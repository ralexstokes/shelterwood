use std::sync::{Arc, Mutex, MutexGuard};

use shelterwood_mailbox::MailboxEffectSink;
use shelterwood_runtime as runtime;

use crate::observe::{SnapshotHub, SnapshotPublication};

/// Shared critical section for one resident tree's observation projection.
///
/// Gate identity, rather than the lock payload, defines tree membership. The
/// lock deliberately tolerates poisoning: a panic in an observation path must
/// not permanently wedge later observation or a subtree handoff.
#[derive(Clone, Debug)]
pub struct ObservationGate(Arc<Mutex<()>>);

impl ObservationGate {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(())))
    }

    pub(super) fn shares_gate(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn same_gate(&self, other: &Self) -> bool {
        self.shares_gate(other)
    }

    pub fn lock(&self) -> MutexGuard<'_, ()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether some thread — possibly this one — is inside the gate.
    ///
    /// The probe a lock-rule test needs: a value's destructor asks whether it
    /// is running inside the critical section, which a same-thread `try_lock`
    /// answers without the reentrant acquisition that would deadlock. A
    /// poisoned but unheld gate reports `false`, matching [`Self::lock`]'s
    /// deliberate poison tolerance.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn is_held(&self) -> bool {
        matches!(self.0.try_lock(), Err(std::sync::TryLockError::WouldBlock))
    }

    /// Whether a panic crossed this gate while its mutex was held.
    ///
    /// This is test instrumentation for assertions that must resume only
    /// after an observation transaction has released its guard.
    #[cfg(any(test, feature = "test-util"))]
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.0.is_poisoned()
    }
}

/// Capability for one observation-gate transaction.
///
/// Every retained control-plane writer takes this token, making an
/// out-of-transaction mutation unavailable by construction. Tokio invokes
/// registered wakers synchronously, so snapshot publications coalesce on the
/// token and are installed once at commit while the gate is still held. Pulses
/// and disposal work then flush only after its gate guard has been released.
/// The same drop path runs during unwind, preventing a poisoned transaction
/// from stranding already-committed wakes.
pub struct ObservationTxn<'a> {
    guard: Option<MutexGuard<'a, ()>>,
    effects: Vec<Box<dyn FnOnce()>>,
    snapshots: Vec<SnapshotPublication>,
}

impl<'a> ObservationTxn<'a> {
    pub(super) fn new(guard: MutexGuard<'a, ()>) -> Self {
        Self {
            guard: Some(guard),
            effects: Vec::new(),
            snapshots: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self {
            guard: None,
            effects: Vec::new(),
            snapshots: Vec::new(),
        }
    }

    pub fn defer(&mut self, operation: impl FnOnce() + 'static) {
        self.effects.push(Box::new(operation));
    }

    /// Defers a watch-channel wake. The driver reaches its own senders
    /// through [`Self::defer`], so this stays inside the cell layer.
    pub(crate) fn pulse<T: 'static>(&mut self, sender: &runtime::WatchSender<T>) {
        let sender = sender.clone();
        self.defer(move || sender.pulse());
    }

    pub(crate) fn snapshot_hub_will_close(&self, hub: &SnapshotHub) -> bool {
        self.snapshots
            .iter()
            .any(|publication| publication.closes(hub))
    }

    pub(crate) fn stage_snapshot(&mut self, publication: SnapshotPublication) {
        let retired = if let Some(staged) = self
            .snapshots
            .iter_mut()
            .find(|staged| staged.same_hub(&publication))
        {
            Some(staged.coalesce(publication))
        } else {
            self.snapshots.push(publication);
            None
        };
        if let Some(retired) = retired {
            // A superseded producer was never run, so nothing is retired but
            // its capture of the publishing scope — which still leaves with
            // the effect list rather than under the gate.
            self.defer(move || drop(retired));
        }
    }

    fn commit(&mut self) {
        let mut panics = runtime::PanicAccumulator::default();
        // Installation precedes the unlock, and the order is load-bearing:
        // released first, two transactions on one gate could interleave as
        // "T1 unlocks, T2 stages and installs a newer cut, T1 installs its
        // stale one", leaving every ungated borrow behind the tree until some
        // later publication corrected it. SPEC §14 promises this ordering.
        // Installations invoke no user code, but keep even their unreachable
        // invariant assertions inside the accumulator. A broken installation
        // must not strand the remaining committed cuts or their queued effects,
        // and its panic resumes only after the guard is gone.
        for mut publication in self.snapshots.drain(..) {
            panics.run(|| publication.install(&mut self.effects));
            // The projection captures its publishing scope. Transfer the
            // whole attempted publication to the post-unlock list whether
            // installation succeeded or panicked.
            self.effects.push(Box::new(move || drop(publication)));
        }
        drop(self.guard.take());
        for effect in self.effects.drain(..) {
            // One hostile waker must not prevent the remaining committed
            // observation edges from notifying their waiters.
            panics.run(effect);
        }
    }
}

impl Drop for ObservationTxn<'_> {
    fn drop(&mut self) {
        self.commit();
    }
}

impl MailboxEffectSink for ObservationTxn<'_> {
    fn defer_mailbox_effect(&mut self, effect: Box<dyn FnOnce()>) {
        self.effects.push(effect);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Wake, Waker},
    };

    use shelterwood_runtime as runtime;

    use super::*;

    struct PanicWake {
        gate: ObservationGate,
        observed_unlocked: Arc<AtomicBool>,
    }

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.observed_unlocked
                .store(!self.gate.is_held(), Ordering::SeqCst);
            std::panic::panic_any("hostile observation pulse");
        }
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn observation_txn_commit_unlocks_before_effects_and_drains_once() {
        let gate = ObservationGate::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut txn = ObservationTxn::new(gate.lock());
        for value in [1, 2] {
            let gate = gate.clone();
            let order = Arc::clone(&order);
            txn.defer(move || {
                assert!(!gate.is_held(), "deferred work runs after the unlock");
                order
                    .lock()
                    .expect("probe mutex remains healthy")
                    .push(value);
            });
        }

        txn.commit();
        assert_eq!(*order.lock().expect("probe mutex remains healthy"), [1, 2]);
        drop(txn);
        assert_eq!(
            *order.lock().expect("probe mutex remains healthy"),
            [1, 2],
            "a second commit from Drop is a no-op"
        );
    }

    #[test]
    fn observation_txn_drop_runs_later_pulses_after_a_hostile_waker() {
        let gate = ObservationGate::new();
        let (hostile_sender, mut hostile_receiver) = runtime::watch(());
        let (later_sender, mut later_receiver) = runtime::watch(());
        let observed_unlocked = Arc::new(AtomicBool::new(false));
        let hostile_waker = Waker::from(Arc::new(PanicWake {
            gate: gate.clone(),
            observed_unlocked: Arc::clone(&observed_unlocked),
        }));
        let later_wakes = Arc::new(AtomicUsize::new(0));
        let later_waker = Waker::from(Arc::new(CountWake(Arc::clone(&later_wakes))));
        let mut hostile_changed = Box::pin(hostile_receiver.changed());
        let mut later_changed = Box::pin(later_receiver.changed());
        assert!(
            hostile_changed
                .as_mut()
                .poll(&mut Context::from_waker(&hostile_waker))
                .is_pending()
        );
        assert!(
            later_changed
                .as_mut()
                .poll(&mut Context::from_waker(&later_waker))
                .is_pending()
        );

        let payload = catch_unwind(AssertUnwindSafe(|| {
            let mut txn = ObservationTxn::new(gate.lock());
            txn.pulse(&hostile_sender);
            txn.pulse(&later_sender);
        }))
        .expect_err("the first hostile pulse is resumed after every effect runs");

        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"hostile observation pulse")
        );
        assert!(
            observed_unlocked.load(Ordering::SeqCst),
            "the hostile waker runs only after the gate is released"
        );
        assert_eq!(
            later_wakes.load(Ordering::SeqCst),
            1,
            "a hostile pulse cannot suppress a later committed pulse"
        );
    }

    #[test]
    fn observation_txn_flushes_during_unwind_without_replacing_the_primary_panic() {
        let gate = ObservationGate::new();
        let effects = Arc::new(Mutex::new(Vec::new()));
        let payload = catch_unwind(AssertUnwindSafe({
            let effects = Arc::clone(&effects);
            let gate = gate.clone();
            move || {
                let mut txn = ObservationTxn::new(gate.lock());
                let first = Arc::clone(&effects);
                let first_gate = gate.clone();
                // An assertion here would be inert: `PanicAccumulator`
                // captures it and `resume_preferred_panic` discards captures
                // raised while a primary unwind is already in flight. The
                // observation is published instead and judged in the body.
                txn.defer(move || {
                    first
                        .lock()
                        .expect("probe mutex remains healthy")
                        .push((1, first_gate.is_held()));
                    std::panic::panic_any("cleanup panic");
                });
                let second = Arc::clone(&effects);
                let second_gate = gate.clone();
                txn.defer(move || {
                    second
                        .lock()
                        .expect("probe mutex remains healthy")
                        .push((2, second_gate.is_held()));
                });
                std::panic::panic_any("primary panic");
            }
        }))
        .expect_err("the original unwind reaches its boundary");

        assert_eq!(payload.downcast_ref::<&str>(), Some(&"primary panic"));
        assert_eq!(
            *effects.lock().expect("probe mutex remains healthy"),
            [(1, false), (2, false)],
            "Drop flushes every committed effect with the gate released, even \
             during an existing unwind"
        );
    }
}
