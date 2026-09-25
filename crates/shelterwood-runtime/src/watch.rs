use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use super::waiters::WaiterRegistry;

/// A change signal: a watch channel whose only content is its version.
pub type Signal = WatchSender<()>;

/// The receiving half of a [`Signal`].
pub type SignalWatcher = WatchReceiver<()>;

/// Shared state for the framework's conflating watch channel.
///
/// The value lock never contains wakers. Versions and endpoint counts are
/// atomic so waiter registration can use the same register/recheck protocol
/// as [`Latch`] without nesting locks.
struct WatchShared<T> {
    value: Mutex<T>,
    version: AtomicU64,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    waiters: WaiterRegistry,
}

impl<T> WatchShared<T> {
    /// Acquires the retained value, tolerating poisoning.
    ///
    /// `modify_silently` and `read_with` run a caller closure under this
    /// guard, and those closures do real work: a lifecycle publication sends
    /// on a broadcast channel here, and a snapshot installation mints a
    /// generation. A panic in any of them would otherwise wedge every later
    /// read, publication, subscription and
    /// terminal wait on the channel. The guarded data is plain framework
    /// state with no invariant spanning the closure, so the surviving value
    /// stays usable; this matches `ObservationGate::lock`, which tolerates
    /// poisoning for the same reason.
    fn value(&self) -> MutexGuard<'_, T> {
        self.value.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Publishing half of a runtime-backed conflating state channel.
pub struct WatchSender<T> {
    shared: Arc<WatchShared<T>>,
}

/// Observing half of a runtime-backed conflating state channel.
pub struct WatchReceiver<T> {
    shared: Arc<WatchShared<T>>,
    seen: u64,
}

fn retain_endpoint(counter: &AtomicUsize, endpoint: &str) {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            count.checked_add(1)
        })
        .unwrap_or_else(|_| panic!("{endpoint} count exhausted"));
}

impl<T> Clone for WatchSender<T> {
    fn clone(&self) -> Self {
        retain_endpoint(&self.shared.senders, "watch sender");
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for WatchSender<T> {
    fn drop(&mut self) {
        let previous = self.shared.senders.fetch_sub(1, Ordering::AcqRel);
        // Diagnostic-only: safe ownership cannot drop one endpoint twice.
        // The wake decision below remains total without this check, and no
        // test depends on the diagnostic panic.
        debug_assert!(previous > 0, "a watch sender is released at most once");
        if previous == 1 {
            self.shared.waiters.wake_all();
        }
    }
}

pub fn watch<T>(initial: T) -> (WatchSender<T>, WatchReceiver<T>) {
    let shared = Arc::new(WatchShared {
        value: Mutex::new(initial),
        version: AtomicU64::new(0),
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
        waiters: WaiterRegistry::default(),
    });
    (
        WatchSender {
            shared: Arc::clone(&shared),
        },
        WatchReceiver { shared, seen: 0 },
    )
}

impl<T: Default> Default for WatchSender<T> {
    fn default() -> Self {
        watch(T::default()).0
    }
}

impl<T> WatchSender<T> {
    /// Whether both senders address the same watch channel.
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    pub fn watcher(&self) -> WatchReceiver<T> {
        retain_endpoint(&self.shared.receivers, "watch receiver");
        WatchReceiver {
            shared: Arc::clone(&self.shared),
            seen: self.shared.version.load(Ordering::Acquire),
        }
    }

    pub fn receiver_count(&self) -> usize {
        self.shared.receivers.load(Ordering::Acquire)
    }

    pub fn pulse(&self) {
        self.shared.version.fetch_add(1, Ordering::AcqRel);
        self.shared.waiters.wake_all();
    }

    /// Mutates the retained value without advancing the watch version.
    ///
    /// This is only for compound publication that must finish another
    /// synchronous state transition before receivers are notified. The caller
    /// must follow a successful logical mutation with [`Self::pulse`]. The
    /// closure runs under the watch value mutex and therefore may only move
    /// plain framework-owned data: it must not call user code, drop user
    /// values, block, panic, or re-enter the framework.
    pub fn modify_silently(&self, update: impl FnOnce(&mut T)) {
        let mut value = self.shared.value();
        update(&mut value);
    }

    /// Reads a projection of the retained value without cloning it.
    ///
    /// `project` runs under the watch's value guard, so it may only inspect
    /// plain framework-owned data. It must not call user code, drop user
    /// values, block, panic, re-enter the framework, or touch this channel.
    pub fn read_with<R>(&self, project: impl FnOnce(&T) -> R) -> R {
        let value = self.shared.value();
        project(&value)
    }
}

impl<T: Clone> WatchSender<T> {
    pub fn read_cloned(&self) -> T {
        self.shared.value().clone()
    }
}

impl<T> Clone for WatchReceiver<T> {
    fn clone(&self) -> Self {
        retain_endpoint(&self.shared.receivers, "watch receiver");
        Self {
            shared: Arc::clone(&self.shared),
            seen: self.seen,
        }
    }
}

impl<T> Drop for WatchReceiver<T> {
    fn drop(&mut self) {
        let previous = self.shared.receivers.fetch_sub(1, Ordering::AcqRel);
        // Diagnostic-only: safe ownership cannot drop one endpoint twice.
        // There is no downstream action whose correctness depends on this
        // check, and no test depends on the diagnostic panic.
        debug_assert!(previous > 0, "a watch receiver is released at most once");
    }
}

enum WatchWaitOutcome {
    Changed,
    Closed,
}

struct WatchWait<'a, T> {
    receiver: &'a mut WatchReceiver<T>,
    identity: u64,
}

impl<'a, T> WatchWait<'a, T> {
    fn new(receiver: &'a mut WatchReceiver<T>) -> Self {
        let identity = receiver.shared.waiters.mint_identity();
        Self { receiver, identity }
    }

    fn poll(&mut self, context: &mut Context<'_>) -> Poll<WatchWaitOutcome> {
        let shared = &self.receiver.shared;
        let seen = &mut self.receiver.seen;
        shared.waiters.poll_registered(self.identity, context, || {
            let version = shared.version.load(Ordering::Acquire);
            if version != *seen {
                *seen = version;
                Some(WatchWaitOutcome::Changed)
            } else if shared.senders.load(Ordering::Acquire) == 0 {
                Some(WatchWaitOutcome::Closed)
            } else {
                None
            }
        })
    }
}

impl<T> Drop for WatchWait<'_, T> {
    fn drop(&mut self) {
        let registered = self.receiver.shared.waiters.remove(self.identity);
        WaiterRegistry::drop_registered([registered]);
    }
}

/// Future returned by [`WatchReceiver::changed`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WatchChanged<'a, T> {
    wait: WatchWait<'a, T>,
}

impl<T> Future for WatchChanged<'_, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().wait.poll(context) {
            Poll::Ready(WatchWaitOutcome::Changed) => Poll::Ready(()),
            Poll::Ready(WatchWaitOutcome::Closed) | Poll::Pending => Poll::Pending,
        }
    }
}

/// Future returned by [`WatchReceiver::changed_or_closed`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WatchChangedOrClosed<'a, T> {
    wait: WatchWait<'a, T>,
}

impl<T> Future for WatchChangedOrClosed<'_, T> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().wait.poll(context) {
            Poll::Ready(WatchWaitOutcome::Changed) => Poll::Ready(true),
            Poll::Ready(WatchWaitOutcome::Closed) => Poll::Ready(false),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> WatchReceiver<T> {
    /// Waits for a new value without treating publisher closure as a change.
    ///
    /// Callers that need to observe publisher closure must use
    /// [`Self::changed_or_closed`] instead. Parking here prevents a loop that
    /// intentionally ignores closure from becoming permanently always-ready.
    pub fn changed(&mut self) -> WatchChanged<'_, T> {
        WatchChanged {
            wait: WatchWait::new(self),
        }
    }

    pub fn changed_or_closed(&mut self) -> WatchChangedOrClosed<'_, T> {
        WatchChangedOrClosed {
            wait: WatchWait::new(self),
        }
    }
}

impl<T: Clone> WatchReceiver<T> {
    pub fn borrow_cloned(&self) -> T {
        self.shared.value().clone()
    }

    pub fn borrow_and_update_cloned(&mut self) -> T {
        let value = self.shared.value();
        // Sampling the version before cloning may produce a harmless extra
        // wake if a pulse races this read, but cannot mark an unseen value as
        // observed. Publication writes the value before advancing the version.
        self.seen = self.shared.version.load(Ordering::Acquire);
        value.clone()
    }
}

impl<T> fmt::Debug for WatchSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WatchSender")
            .field("receivers", &self.receiver_count())
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for WatchReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WatchReceiver")
            .field("seen", &self.seen)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
    };

    use crate::{
        Signal,
        test_wakers::{
            CountPanicWake, CountWake, assert_panic_message, last_drop_panics_waker,
            transition_on_clone_waker,
        },
    };

    struct DebugProbe(Arc<AtomicUsize>);

    impl std::fmt::Debug for DebugProbe {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fetch_add(1, Ordering::SeqCst);
            formatter.write_str("DebugProbe")
        }
    }

    #[test]
    fn signal_pulse_wakes_a_parked_watcher_and_advances_generations() {
        let signal = Signal::default();
        let mut watcher = signal.watcher();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));

        let mut first = Box::pin(watcher.changed());
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        signal.pulse();
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
        drop(first);

        let mut second = Box::pin(watcher.changed());
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        signal.pulse();
        assert_eq!(wakes.load(Ordering::SeqCst), 2);
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[test]
    fn quiet_signal_wait_cancellation_removes_waiter_registration() {
        let signal = Signal::default();
        let mut watcher = signal.watcher();
        // The channel-wide registry length, not the endpoint count: `changed()`
        // never clones its receiver, so an endpoint probe cannot observe a
        // registration a cancelled wait failed to remove.
        assert_eq!(signal.shared.waiters.len(), 0);

        for _ in 0..10_000 {
            let mut changed = Box::pin(watcher.changed());
            let mut context = Context::from_waker(Waker::noop());
            assert!(changed.as_mut().poll(&mut context).is_pending());
            assert_eq!(
                signal.shared.waiters.len(),
                1,
                "a parked wait holds exactly one registration"
            );
            drop(changed);
            assert_eq!(
                signal.shared.waiters.len(),
                0,
                "cancelling the wait removes its registration"
            );
        }
    }

    #[test]
    fn watch_receivers_baseline_the_version_they_were_created_at() {
        fn changed(receiver: &mut super::WatchReceiver<u8>) -> bool {
            let mut changed = Box::pin(receiver.changed());
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        }

        let (sender, mut original) = super::watch(0_u8);
        sender.modify_silently(|value| *value = 1);
        assert!(!changed(&mut original), "a silent write is not a change");
        sender.pulse();

        // A subscription taken after the pulse starts at that version, so the
        // pulse it never missed is not reported to it.
        let mut late = sender.watcher();
        assert!(
            !changed(&mut late),
            "a new watcher baselines the current version"
        );

        // The channel's first receiver baselines the initial version, and a
        // clone inherits its source's baseline rather than the channel's: one
        // taken from the still-behind original owes the same change.
        let mut behind_clone = original.clone();
        assert!(changed(&mut original));
        assert!(!changed(&mut original), "a change is observed once");
        assert!(changed(&mut behind_clone));
        let mut caught_up_clone = original.clone();
        assert!(!changed(&mut caught_up_clone));

        sender.pulse();
        assert_eq!(late.borrow_and_update_cloned(), 1);
        assert!(
            !changed(&mut late),
            "reading with update consumes the change"
        );
        assert_eq!(caught_up_clone.borrow_cloned(), 1);
        assert!(changed(&mut caught_up_clone), "a plain read does not");
    }

    #[test]
    fn closed_watch_change_parks_on_its_first_poll() {
        let (sender, mut receiver) = super::watch(());
        drop(sender);

        let mut changed = Box::pin(receiver.changed());
        let mut context = Context::from_waker(Waker::noop());
        assert!(changed.as_mut().poll(&mut context).is_pending());
        assert!(changed.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    fn closed_watch_remains_observable_when_requested() {
        let (sender, mut receiver) = super::watch(());
        drop(sender);

        let mut changed = Box::pin(receiver.changed_or_closed());
        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(changed.as_mut().poll(&mut context), Poll::Ready(false));
    }

    #[test]
    fn watch_sender_debug_never_formats_the_guarded_value() {
        let formats = Arc::new(AtomicUsize::new(0));
        let (sender, _receiver) = super::watch(DebugProbe(Arc::clone(&formats)));

        // Assert the omission, not the rendering: `Debug` output is not
        // contractual, but formatting the guarded value under the watch mutex
        // would be a lock-rule violation.
        let rendered = format!("{sender:?}");
        assert!(!rendered.contains("DebugProbe"), "rendered as {rendered}");
        assert_eq!(formats.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn watch_receiver_debug_never_formats_the_guarded_value() {
        let formats = Arc::new(AtomicUsize::new(0));
        let (_sender, receiver) = super::watch(DebugProbe(Arc::clone(&formats)));

        let rendered = format!("{receiver:?}");
        assert!(!rendered.contains("DebugProbe"), "rendered as {rendered}");
        assert_eq!(formats.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn watch_rechecks_pulse_before_dropping_a_displaced_hostile_waker() {
        const PANIC: &str = "injected registered waker drop panic";

        let (sender, mut receiver) = super::watch(());
        let hostile_drops = Arc::new(AtomicUsize::new(0));
        let hostile = last_drop_panics_waker(PANIC, Arc::clone(&hostile_drops));
        let mut waiting = Box::pin(receiver.changed());
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        drop(hostile);

        // Advance the same atomic publication edge as `pulse` during the
        // replacement waker's clone. Leaving the registry intact is the
        // deterministic race shape that exercises displaced-waker teardown.
        let shared = Arc::clone(&sender.shared);
        let racing = transition_on_clone_waker(move || {
            shared.version.fetch_add(1, Ordering::AcqRel);
        });
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = waiting.as_mut().poll(&mut Context::from_waker(&racing));
        }))
        .expect_err("destroying the displaced waker still surfaces its panic");

        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_drops.load(Ordering::SeqCst), 1);
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "the pulse version is consumed before the displaced destructor resumes"
        );
    }

    #[test]
    fn hostile_watch_waiter_cannot_strand_a_well_behaved_pulse_waiter() {
        const PANIC: &str = "injected watch pulse waker panic";

        let (sender, mut hostile_receiver) = super::watch(0_u8);
        let mut ordinary_receiver = sender.watcher();
        let hostile_wakes = Arc::new(AtomicUsize::new(0));
        let ordinary_wakes = Arc::new(AtomicUsize::new(0));
        let hostile = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&hostile_wakes),
            message: PANIC,
        }));
        let ordinary = Waker::from(Arc::new(CountWake(Arc::clone(&ordinary_wakes))));
        let mut hostile_wait = Box::pin(hostile_receiver.changed_or_closed());
        let mut ordinary_wait = Box::pin(ordinary_receiver.changed_or_closed());
        assert!(
            hostile_wait
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        assert!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary))
                .is_pending()
        );
        sender.modify_silently(|value| *value = 1);

        let result = catch_unwind(AssertUnwindSafe(|| sender.pulse()));

        let payload = result.expect_err("the hostile watch pulse still surfaces");
        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(ordinary_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary)),
            Poll::Ready(true)
        );
        drop(hostile_wait);
        drop(ordinary_wait);
        assert_eq!(hostile_receiver.borrow_and_update_cloned(), 1);
        assert_eq!(ordinary_receiver.borrow_and_update_cloned(), 1);
    }

    #[test]
    fn hostile_watch_waiter_cannot_strand_a_well_behaved_close_waiter() {
        const PANIC: &str = "injected watch close waker panic";

        let (sender, mut hostile_receiver) = super::watch(());
        let mut ordinary_receiver = sender.watcher();
        let hostile_wakes = Arc::new(AtomicUsize::new(0));
        let ordinary_wakes = Arc::new(AtomicUsize::new(0));
        let hostile = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&hostile_wakes),
            message: PANIC,
        }));
        let ordinary = Waker::from(Arc::new(CountWake(Arc::clone(&ordinary_wakes))));
        let mut hostile_wait = Box::pin(hostile_receiver.changed_or_closed());
        let mut ordinary_wait = Box::pin(ordinary_receiver.changed_or_closed());
        assert!(
            hostile_wait
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        assert!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary))
                .is_pending()
        );

        let result = catch_unwind(AssertUnwindSafe(|| drop(sender)));

        let payload = result.expect_err("the hostile watch close still surfaces");
        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(ordinary_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary)),
            Poll::Ready(false)
        );
    }

    #[test]
    fn a_panicking_watch_closure_leaves_the_channel_usable() {
        let (sender, mut receiver) = super::watch(0u8);
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            sender.modify_silently(|value| {
                *value = 1;
                panic!("injected watch mutation panic");
            });
        }));
        assert!(panicked.is_err());

        // The guard is poisoned; every later reader must still see the
        // surviving framework state rather than inheriting the panic.
        assert_eq!(sender.read_cloned(), 1);
        assert_eq!(sender.read_with(|value| *value), 1);
        assert_eq!(receiver.borrow_cloned(), 1);
        assert_eq!(receiver.borrow_and_update_cloned(), 1);
        sender.modify_silently(|value| *value = 2);
        sender.pulse();
        assert_eq!(receiver.borrow_cloned(), 2);
    }
}
