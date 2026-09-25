use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll},
};

use super::waiters::WaiterRegistry;

/// A one-shot, multi-waiter signal backed by a contained waiter registry.
///
/// The atomic provides a linearizable, idempotent transition and retains the
/// fired state for future waiters. A wait registers before rechecking that
/// state, so it either observes the transition or is present in the registry
/// drained by the publisher. Each drained waker runs independently after the
/// registry lock is released.
///
/// This deliberately does not use `tokio_util::sync::CancellationToken`.
/// Shelterwood also uses latches for readiness and completion, needs `fire` to
/// report which caller performed the transition, and keeps parent and local
/// cancellation as distinct shared latches. The Tokio-util cancellation tree
/// would add allocation, locking, and dependencies without replacing those
/// semantics. A Tokio watch channel similarly adds value locking to the hot
/// `is_fired` path.
#[derive(Clone, Debug, Default)]
pub struct Latch {
    state: Arc<LatchState>,
}

#[derive(Debug, Default)]
struct LatchState {
    fired: AtomicBool,
    waiters: WaiterRegistry,
}

impl Latch {
    pub fn fire(&self) -> bool {
        if self.fire_silently() {
            self.notify();
            true
        } else {
            false
        }
    }

    /// Performs the one-shot transition without waking waiters.
    ///
    /// Splitting the transition from the wake lets an observation-gate
    /// transaction linearize the fire inside its critical section while
    /// deferring the waker-visible [`Self::notify`] until after the gate is
    /// released. Deferral cannot strand a waiter: [`Self::fired`] registers
    /// before rechecking `is_fired`, so a waiter either observes the committed
    /// transition directly or is present for the deferred drain.
    pub fn fire_silently(&self) -> bool {
        self.state
            .fired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Wakes waiters after a [`Self::fire_silently`] transition.
    ///
    /// Idempotent and only meaningful once the latch is fired; callers that
    /// won `fire_silently` inside an observation-gate transaction defer this
    /// wake past the gate release.
    pub fn notify(&self) {
        self.state.waiters.wake_all();
    }

    pub fn is_fired(&self) -> bool {
        self.state.fired.load(Ordering::Acquire)
    }

    pub fn fired(&self) -> LatchWait<'_> {
        LatchWait {
            latch: self,
            identity: self.state.waiters.mint_identity(),
        }
    }
}

/// Future returned by [`Latch::fired`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct LatchWait<'a> {
    latch: &'a Latch,
    identity: u64,
}

impl Future for LatchWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.latch
            .state
            .waiters
            .poll_registered(this.identity, context, || {
                this.latch.is_fired().then_some(())
            })
    }
}

impl Drop for LatchWait<'_> {
    fn drop(&mut self) {
        let registered = self.latch.state.waiters.remove(self.identity);
        WaiterRegistry::drop_registered([registered]);
    }
}

const COMPLETION_GATE_OPEN: u8 = 0;
const COMPLETION_GATE_FIRED: u8 = 1;
const COMPLETION_GATE_CLOSED: u8 = 2;
const COMPLETION_GATE_CLOSED_FIRED: u8 = 3;

/// A one-shot signal whose publication is linearized with a completion edge.
///
/// `fire` wins only while the gate is open. `complete` atomically closes the
/// gate and reports whether the signal won first, so a capability retained by
/// another task cannot publish after completion or disappear between a sample
/// and the completion notification.
///
/// The wake lags the state: both transitions publish their state CAS before
/// firing the corresponding latch, so `is_fired`/`is_completed` can read
/// true while the matching waiter's wake is still in flight. A waiter racing
/// `fired()` against `completed()` must resolve the tie by state or by
/// left-biased selection (`select_two`), never by wake arrival order.
#[derive(Clone, Debug)]
pub struct CompletionGatedLatch {
    state: Arc<AtomicU8>,
    fired: Latch,
    completed: Latch,
}

impl Default for CompletionGatedLatch {
    fn default() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(COMPLETION_GATE_OPEN)),
            fired: Latch::default(),
            completed: Latch::default(),
        }
    }
}

impl CompletionGatedLatch {
    pub fn fire(&self) -> bool {
        if self
            .state
            .compare_exchange(
                COMPLETION_GATE_OPEN,
                COMPLETION_GATE_FIRED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        let transitioned = self.fired.fire();
        assert!(transitioned);
        true
    }

    pub fn is_fired(&self) -> bool {
        matches!(
            self.state.load(Ordering::Acquire),
            COMPLETION_GATE_FIRED | COMPLETION_GATE_CLOSED_FIRED
        )
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn is_completed(&self) -> bool {
        matches!(
            self.state.load(Ordering::Acquire),
            COMPLETION_GATE_CLOSED | COMPLETION_GATE_CLOSED_FIRED
        )
    }

    pub async fn fired(&self) {
        self.fired.fired().await;
    }

    pub fn complete(&self) -> bool {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let (next, fired) = match current {
                COMPLETION_GATE_OPEN => (COMPLETION_GATE_CLOSED, false),
                COMPLETION_GATE_FIRED => (COMPLETION_GATE_CLOSED_FIRED, true),
                COMPLETION_GATE_CLOSED => return false,
                COMPLETION_GATE_CLOSED_FIRED => return true,
                _ => unreachable!("completion-gated latch state is valid"),
            };
            if self
                .state
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let transitioned = self.completed.fire();
                assert!(transitioned);
                return fired;
            }
        }
    }

    pub async fn completed(&self) {
        self.completed.fired().await;
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
        time::Duration,
    };

    use shelterwood_core::exit::JoinOutcome;

    use crate::{
        CompletionGatedLatch, Latch, Timeout, join, spawn,
        test_wakers::{
            CountPanicWake, CountWake, assert_panic_message, last_drop_panics_waker,
            transition_on_clone_waker,
        },
        timeout, yield_now,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exactly_one_concurrent_fire_performs_the_transition() {
        const FIRERS: usize = 32;

        let latch = Latch::default();
        let ready = Arc::new(AtomicUsize::new(0));
        let mut firers = Vec::with_capacity(FIRERS);
        for _ in 0..FIRERS {
            let latch = latch.clone();
            let ready = Arc::clone(&ready);
            firers.push(spawn(async move {
                ready.fetch_add(1, Ordering::AcqRel);
                while ready.load(Ordering::Acquire) != FIRERS {
                    yield_now().await;
                }
                latch.fire()
            }));
        }

        let mut transitions = 0;
        for firer in firers {
            let JoinOutcome::Ok { value } = join(firer).await else {
                panic!("latch firer must complete normally");
            };
            transitions += usize::from(value);
        }

        assert_eq!(transitions, 1);
        assert!(latch.is_fired());
        assert!(!latch.fire());
    }

    #[test]
    fn silent_fire_defers_a_parked_waiters_wake_until_notify() {
        let latch = Latch::default();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut fired = Box::pin(latch.fired());

        assert!(
            fired
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(latch.fire_silently());
        assert!(latch.is_fired());
        assert_eq!(wakes.load(Ordering::SeqCst), 0);

        latch.notify();
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(
            fired
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[test]
    fn latch_rechecks_fire_before_dropping_a_displaced_hostile_waker() {
        const PANIC: &str = "injected registered waker drop panic";

        let latch = Latch::default();
        let hostile_drops = Arc::new(AtomicUsize::new(0));
        let hostile = last_drop_panics_waker(PANIC, Arc::clone(&hostile_drops));
        let mut waiting = Box::pin(latch.fired());
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        // The registered clone remains live, so releasing the caller's raw
        // waker reference does not run its last-reference panic yet.
        drop(hostile);

        let racing_latch = latch.clone();
        let racing = transition_on_clone_waker(move || {
            assert!(racing_latch.fire_silently());
        });
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = waiting.as_mut().poll(&mut Context::from_waker(&racing));
        }))
        .expect_err("destroying the displaced waker still surfaces its panic");

        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_drops.load(Ordering::SeqCst), 1);
        assert!(latch.is_fired());
        assert_eq!(
            latch.state.waiters.len(),
            0,
            "the fire recheck removes the replacement before the displaced destructor resumes"
        );
    }

    #[test]
    fn hostile_latch_waiter_cannot_strand_a_well_behaved_waiter() {
        const PANIC: &str = "injected latch waker panic";

        let latch = Latch::default();
        let hostile_wakes = Arc::new(AtomicUsize::new(0));
        let ordinary_wakes = Arc::new(AtomicUsize::new(0));
        let hostile = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&hostile_wakes),
            message: PANIC,
        }));
        let ordinary = Waker::from(Arc::new(CountWake(Arc::clone(&ordinary_wakes))));
        let mut hostile_wait = Box::pin(latch.fired());
        let mut ordinary_wait = Box::pin(latch.fired());
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

        let result = catch_unwind(AssertUnwindSafe(|| latch.fire()));

        let payload = result.expect_err("the hostile latch wake still surfaces");
        assert_panic_message(&*payload, PANIC);
        assert!(latch.is_fired());
        assert_eq!(hostile_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(ordinary_wakes.load(Ordering::SeqCst), 1);
        assert!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary))
                .is_ready()
        );
    }

    #[test]
    fn completion_waiters_cover_parked_and_already_completed_paths() {
        let parked = CompletionGatedLatch::default();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut waiting = Box::pin(parked.completed());
        let mut context = Context::from_waker(&waker);
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        assert!(!parked.complete());
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(waiting.as_mut().poll(&mut context).is_ready());

        let completed = CompletionGatedLatch::default();
        assert!(!completed.complete());
        let mut immediate = Box::pin(completed.completed());
        assert!(
            immediate
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completion_gated_fire_and_completion_choose_one_order() {
        for _ in 0..256 {
            let latch = CompletionGatedLatch::default();
            let started = Arc::new(AtomicUsize::new(0));
            let fire = spawn({
                let latch = latch.clone();
                let started = Arc::clone(&started);
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    while started.load(Ordering::Acquire) != 2 {
                        yield_now().await;
                    }
                    latch.fire()
                }
            });
            let complete = spawn({
                let latch = latch.clone();
                let started = Arc::clone(&started);
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    while started.load(Ordering::Acquire) != 2 {
                        yield_now().await;
                    }
                    latch.complete()
                }
            });

            let JoinOutcome::Ok { value: fired } = join(fire).await else {
                panic!("signal task must complete normally");
            };
            let JoinOutcome::Ok {
                value: completion_saw_fire,
            } = join(complete).await
            else {
                panic!("completion task must complete normally");
            };
            assert_eq!(fired, completion_saw_fire);
            assert_eq!(latch.is_fired(), completion_saw_fire);
            assert!(!latch.fire(), "completion closes later publication");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_pre_fire_waiters_wake() {
        const WAITERS: usize = 32;

        let latch = Latch::default();
        let parked = Arc::new(AtomicUsize::new(0));
        let mut waiters = Vec::with_capacity(WAITERS);
        for _ in 0..WAITERS {
            let latch = latch.clone();
            let parked = Arc::clone(&parked);
            waiters.push(spawn(async move {
                let mut fired = Box::pin(latch.fired());
                let first_poll =
                    std::future::poll_fn(|context| Poll::Ready(fired.as_mut().poll(context))).await;
                assert!(first_poll.is_pending());
                parked.fetch_add(1, Ordering::Release);
                fired.await;
            }));
        }

        while parked.load(Ordering::Acquire) != WAITERS {
            yield_now().await;
        }
        assert!(latch.fire());

        for waiter in waiters {
            let result = timeout(Duration::from_secs(1), join(waiter)).await;
            assert!(matches!(
                result,
                Timeout::Completed(JoinOutcome::Ok { value: () })
            ));
        }
    }

    #[tokio::test]
    async fn post_fire_waiters_complete_immediately() {
        let latch = Latch::default();
        assert!(latch.fire());

        assert!(matches!(
            timeout(Duration::from_secs(1), latch.fired()).await,
            Timeout::Completed(())
        ));
    }

    #[tokio::test]
    async fn cancelled_waits_do_not_consume_the_signal() {
        let latch = Latch::default();

        for _ in 0..1_024 {
            let mut fired = Box::pin(latch.fired());
            let first_poll =
                std::future::poll_fn(|context| Poll::Ready(fired.as_mut().poll(context))).await;
            assert!(first_poll.is_pending());
            assert_eq!(
                latch.state.waiters.len(),
                1,
                "a parked wait holds exactly one registration"
            );
            drop(fired);
            assert_eq!(
                latch.state.waiters.len(),
                0,
                "cancelling the wait removes its registration"
            );
        }

        let mut live_waiter = Box::pin(latch.fired());
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(live_waiter.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());
        assert_eq!(
            latch.state.waiters.len(),
            1,
            "the surviving wait is still registered"
        );
        assert!(latch.fire());
        assert_eq!(
            latch.state.waiters.len(),
            0,
            "the fire drains every registration before waking"
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), live_waiter).await,
            Timeout::Completed(())
        ));
    }
}
