use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use shelterwood_core::DeadlineBudget;

use crate::{
    mailbox::{MailboxRuntime, ProxiedSleep},
    runtime::PanicAccumulator,
};

/// Which of the two passes an operation is being polled in.
///
/// A `Deadlined` future polls its operation twice per wakeup: once optimistic,
/// and -- once the timer has fired -- again to let the operation arbitrate
/// between a value that landed concurrently and the timeout it would otherwise
/// report. The distinction is the pass, not merely whether the clock has
/// passed the deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeadlinePhase {
    /// Optimistic pass: the operation may only complete on its own terms.
    InitialAttempt,
    /// Post-expiry pass: the operation closes its channel and decides between
    /// a concurrently delivered value and reporting the timeout.
    TimeoutArbitration,
}

pub(super) trait DeadlineOperation {
    type Output;

    /// Polls the operation before or after the shared deadline transition.
    ///
    /// This is a second operation poll after the timer becomes ready so
    /// completion or acceptance at the exact boundary wins before
    /// operation-specific timeout cleanup runs. It may remain pending only
    /// when an atomic completion transition won but has not published its
    /// payload yet; that transition must wake the registered operation waker.
    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        budget: crate::deadline::Deadline,
        phase: DeadlinePhase,
    ) -> Poll<Self::Output>;

    /// Resolves a zero-width budget without ever attempting the operation.
    ///
    /// A zero budget is a short-circuit, not a raced deadline: nothing is
    /// submitted and no completion is observed, so this reports the
    /// operation's timeout outcome and performs only its own cleanup.
    fn short_circuit(&mut self) -> Self::Output;
}

/// First-poll deadline capture shared by every public mailbox deadline future.
pub(super) struct Deadlined<F> {
    pub(super) operation: F,
    runtime: Arc<dyn MailboxRuntime>,
    budget_width: DeadlineBudget,
    budget: Option<crate::deadline::Deadline>,
    timer: Option<ProxiedSleep>,
    pub(super) started: bool,
    phase: DeadlinePhase,
}

impl<F> Deadlined<F> {
    /// Constructs the shared no-attempt deadline policy used by public
    /// mailbox operations.
    pub(super) fn no_attempt(
        operation: F,
        budget_width: impl Into<DeadlineBudget>,
        runtime: Arc<dyn MailboxRuntime>,
    ) -> Self {
        Self {
            operation,
            runtime,
            budget_width: budget_width.into(),
            budget: None,
            timer: None,
            started: false,
            phase: DeadlinePhase::InitialAttempt,
        }
    }

    /// Polls the timer with a stable framework-owned waker.
    ///
    /// The first probe uses the static no-op waker. An already-ready timer
    /// therefore allocates nothing; only a timer that actually parks gets a
    /// proxy and a clone of the caller waker. A completion racing the probe is
    /// observed by the immediate second poll, so the no-op registration
    /// cannot lose a wake.
    ///
    /// An unbounded deadline never reaches either step: it is answered from
    /// the captured budget alone, so it registers nothing to retire.
    fn poll_timer(&mut self, context: &mut Context<'_>) -> Poll<()> {
        if self
            .budget
            .expect("a started deadline future retains its captured budget")
            .instant()
            .is_none()
        {
            // An unrepresentable deadline never arrives, and the capability
            // answers it with `std::future::pending()` -- a future that
            // resolves for nobody and wakes nobody. Registering a caller waker
            // with it would allocate a proxy and dispatch the caller's `clone`
            // vtable for a wake that cannot happen, and then dispatch its
            // `drop` vtable again at retirement. This is the documented
            // unbounded-wait idiom (`docs/observation.md`), not an edge case,
            // so it stays as free as it was before the proxy.
            return Poll::Pending;
        }
        Pin::new(
            self.timer
                .as_mut()
                .expect("an unelapsed deadline future retains its timer"),
        )
        .poll(context)
    }

    /// Retires the caller waker on this thread, then synchronously removes the
    /// framework-only wheel registration.
    ///
    /// This is the poll-path venue. Post-proxy, Tokio's wheel holds only a
    /// framework-owned proxy, so no caller waker is ever destroyed under the
    /// global time-driver mutex on *any* path; the only question left is who
    /// absorbs a hostile destructor. On a poll path that is the caller's own
    /// task, which is the trade #398 ruling 3 already accepted for the reply
    /// delivery seam: retirement here is per-completion hot-path work, and a
    /// lane submission on every completed `send_timeout`/`call`/`recv` is real
    /// cost where a contained drop of a benign waker is nearly free. Only drop
    /// glue -- where the absorbing party is whatever is tearing the future down
    /// -- keeps the lane through `ProxiedSleep`'s drop implementation.
    fn retire_timer_inline(&mut self, panics: &mut PanicAccumulator) {
        let mut timer = self.timer.take();
        if let Some(timer) = timer.as_mut() {
            timer.retire_inline(panics);
        }
        panics.run(|| drop(timer));
    }
}

impl<F: DeadlineOperation + Unpin> Future for Deadlined<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if !this.started {
            this.started = true;
            let budget =
                crate::deadline::Deadline::after(this.runtime.now(), this.budget_width.duration());
            this.budget = Some(budget);
            if !this.budget_width.is_zero()
                && let Some(deadline) = budget.instant()
            {
                let timer = this.runtime.sleep_until(Some(deadline));
                this.timer = Some(ProxiedSleep::new(timer, Arc::clone(&this.runtime)));
            }
        }
        // A zero budget short-circuits: the operation is never attempted, so
        // nothing is submitted and no completion is observed. The expiry
        // boundary below governs only non-zero budgets, and the two rules
        // therefore never compete.
        if this.budget_width.is_zero() {
            return Poll::Ready(this.operation.short_circuit());
        }
        let budget = this
            .budget
            .expect("a started deadline future retains its captured budget");
        if let Poll::Ready(result) =
            this.operation
                .poll_deadlined(context, budget, DeadlinePhase::InitialAttempt)
        {
            // Completion retires the wheel entry here, and only here: a
            // future held past its result would otherwise keep a timer armed
            // with a clone of the caller's waker and deliver that expiry to a
            // caller who already has its answer. Retirement must therefore be
            // synchronous -- handing the timer to isolated disposal leaves the
            // entry armed until a worker thread reaches it, which is the
            // spurious wake this exists to prevent.
            //
            // Tokio now destroys only a framework-owned proxy under the time
            // driver mutex, so the stored caller waker retires here, on this
            // task, rather than through the disposal lane -- the poll-path
            // venue of #398 ruling 3. See `retire_timer_inline`.
            //
            // `result` may own a user value. The retirement is therefore fully
            // drained *before* the handoff: the accumulator is emptied by
            // `take` and its payload discarded, so nothing can unwind out of
            // this frame while it owns the completed output. An escaping
            // cleanup panic would destroy that value mid-unwind and -- with a
            // hostile value destructor -- abort the process. The swallowed
            // diagnostic is the accepted cost, exactly as at the reply
            // delivery seam.
            let mut panics = PanicAccumulator::default();
            this.retire_timer_inline(&mut panics);
            crate::runtime::discard_panic(panics.take());
            return Poll::Ready(result);
        }
        if this.phase == DeadlinePhase::InitialAttempt {
            if this.poll_timer(context).is_pending() {
                return Poll::Pending;
            }
            // The timer is a one-shot future: polling it again after it
            // resolves panics. Latch the transition and release it, so an
            // elapsed poll that stays pending re-polls only the operation.
            this.phase = DeadlinePhase::TimeoutArbitration;
            // The same poll-path venue and the same containment. Contained
            // rather than re-raised because the arbitration poll immediately
            // below can hand back a value that landed at the boundary: letting
            // a caller-waker destructor unwind out of `poll` would tear the
            // whole future down inside that unwind -- operation state, an
            // unsent user message and all -- which is the double-panic abort
            // this seam exists to remove.
            let mut panics = PanicAccumulator::default();
            this.retire_timer_inline(&mut panics);
            crate::runtime::discard_panic(panics.take());
        }
        this.operation
            .poll_deadlined(context, budget, DeadlinePhase::TimeoutArbitration)
    }
}

impl<F> Drop for Deadlined<F> {
    /// Retires an unelapsed timer inside a boundary.
    ///
    /// Tokio's wheel holds only a framework-owned proxy. The real caller
    /// waker retires through the blocking disposal lane before the wheel entry
    /// is synchronously cancelled, so its destructor can neither hold the
    /// global time-driver mutex nor run in this drop glue. The accumulator
    /// still protects capability submission and framework retirement during
    /// an existing unwind.
    ///
    /// This is the half of #398 ruling 3's venue split that keeps the lane:
    /// cancellation has no result to weigh the diagnostic against and no task
    /// of its own to stall, so a destructor that blocks is handed off rather
    /// than run here.
    fn drop(&mut self) {
        drop(self.timer.take());
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
    };

    /// Stays pending on its first elapsed poll, modelling an atomic
    /// completion transition that won without publishing its payload yet.
    #[derive(Default)]
    struct PendingOnFirstExpiry {
        elapsed_polls: usize,
    }

    /// Parks once, then completes strictly before its deadline -- the only
    /// arrangement in which a retained future can still hold an armed wheel
    /// entry carrying a clone of the caller's waker.
    #[derive(Default)]
    struct ReadyAfterParking {
        polls: usize,
    }

    struct ImmediatelyReady;

    impl super::DeadlineOperation for ImmediatelyReady {
        type Output = ();

        fn poll_deadlined(
            &mut self,
            _context: &mut Context<'_>,
            _budget: crate::deadline::Deadline,
            _phase: super::DeadlinePhase,
        ) -> Poll<()> {
            Poll::Ready(())
        }

        fn short_circuit(&mut self) {}
    }

    impl super::DeadlineOperation for ReadyAfterParking {
        type Output = ();

        fn poll_deadlined(
            &mut self,
            _context: &mut Context<'_>,
            _budget: crate::deadline::Deadline,
            _phase: super::DeadlinePhase,
        ) -> Poll<()> {
            self.polls += 1;
            if self.polls < 2 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }

        fn short_circuit(&mut self) {}
    }

    /// Records wakes so an armed wheel entry is observable as the spurious
    /// wake it would deliver rather than as a cleared field.
    #[derive(Default)]
    struct WakeRecorder(AtomicUsize);

    impl Wake for WakeRecorder {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Counts the clones a poll takes of the caller's waker.
    ///
    /// A proxy allocation is not observable after retirement. The artifact a
    /// lazy path must leave untouched is therefore the caller's own `clone`
    /// vtable, which only `WakerProxy::register` reaches.
    #[derive(Default)]
    struct CloneCounter(AtomicUsize);

    unsafe fn clone_counting_waker(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let probe = ManuallyDrop::new(unsafe { Arc::<CloneCounter>::from_raw(data.cast()) });
        probe.0.fetch_add(1, Ordering::SeqCst);
        RawWaker::new(Arc::into_raw(Arc::clone(&probe)).cast(), &COUNTING_VTABLE)
    }

    unsafe fn wake_counting_waker(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<CloneCounter>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_counting_waker(_data: *const ()) {}

    unsafe fn drop_counting_waker(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<CloneCounter>::from_raw(data.cast()) });
    }

    static COUNTING_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_counting_waker,
        wake_counting_waker,
        wake_by_ref_counting_waker,
        drop_counting_waker,
    );

    fn counting_waker(counter: &Arc<CloneCounter>) -> Waker {
        let raw = RawWaker::new(Arc::into_raw(Arc::clone(counter)).cast(), &COUNTING_VTABLE);
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    impl super::DeadlineOperation for PendingOnFirstExpiry {
        type Output = usize;

        fn poll_deadlined(
            &mut self,
            _context: &mut Context<'_>,
            _budget: crate::deadline::Deadline,
            phase: super::DeadlinePhase,
        ) -> Poll<usize> {
            if phase == super::DeadlinePhase::InitialAttempt {
                return Poll::Pending;
            }
            self.elapsed_polls += 1;
            if self.elapsed_polls < 2 {
                Poll::Pending
            } else {
                Poll::Ready(self.elapsed_polls)
            }
        }

        fn short_circuit(&mut self) -> usize {
            0
        }
    }

    #[crate::runtime::test(start_paused = true)]
    async fn an_expired_deadline_future_never_repolls_its_resolved_timer() {
        let width = std::time::Duration::from_secs(1);
        let mut future = Box::pin(super::Deadlined::no_attempt(
            PendingOnFirstExpiry::default(),
            width,
            crate::mailbox::capability::tests::runtime(),
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(future.as_mut().poll(&mut context).is_pending());
        crate::runtime::advance(width * 2).await;
        // The timer resolves here and the operation stays pending, so the
        // scaffold must latch the expiry rather than poll the timer again.
        assert!(future.as_mut().poll(&mut context).is_pending());

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(2)));
    }

    #[crate::runtime::test(start_paused = true)]
    async fn a_completed_operation_leaves_no_timer_to_wake_its_caller() {
        let width = std::time::Duration::from_secs(1);
        let mut future = Box::pin(super::Deadlined::no_attempt(
            ReadyAfterParking::default(),
            width,
            crate::mailbox::capability::tests::runtime(),
        ));
        let recorder = Arc::new(WakeRecorder::default());
        let waker = Waker::from(Arc::clone(&recorder));
        let mut context = Context::from_waker(&waker);

        // Parking arms the timer with a clone of this waker; the operation
        // then completes while that deadline is still in the future.
        assert!(future.as_mut().poll(&mut context).is_pending());
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(())
        ));
        let woken_before = recorder.0.load(Ordering::SeqCst);

        // The direct artifact, asserted first: retirement empties the proxy
        // slot before it drops the timer, so a wheel entry left armed here
        // would fire into an empty slot and stay invisible to the observation
        // below. Only the dropped `Sleep` proves the entry is synchronously
        // gone rather than merely harmless.
        assert!(
            future.timer.is_none(),
            "completion must synchronously cancel the wheel entry, not merely silence it"
        );

        crate::runtime::advance(width * 2).await;

        // The future is still held, so a timer left armed by completion would
        // deliver its expiry to the caller that already has its result.
        assert_eq!(
            recorder.0.load(Ordering::SeqCst),
            woken_before,
            "a completed but retained deadline future must not wake its caller when the deadline elapses"
        );
    }

    #[crate::runtime::test(start_paused = true)]
    async fn an_immediately_ready_operation_never_allocates_a_timer_proxy() {
        let mut future = Box::pin(super::Deadlined::no_attempt(
            ImmediatelyReady,
            std::time::Duration::from_secs(1),
            crate::mailbox::capability::tests::runtime(),
        ));
        let counter = Arc::new(CloneCounter::default());
        let waker = counting_waker(&counter);
        let mut context = Context::from_waker(&waker);

        assert!(future.as_mut().poll(&mut context).is_ready());
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            0,
            "an operation ready before the timer poll never clones the caller waker"
        );
    }

    /// The documented lazy path: a timer that is *already* elapsed when the
    /// operation parks.
    ///
    /// The test above never enters `poll_timer` at all -- its operation is
    /// ready on the first poll -- so the no-op probe it names goes untested
    /// there. Reaching the probe needs an operation that stays pending beside
    /// a timer that is ready, which is what dating the capability clock into
    /// the past arranges: the budget is captured from that stale `now`, so the
    /// deadline it derives is already behind the real clock and
    /// `sleep_until` resolves on its first poll.
    #[crate::runtime::test(start_paused = true)]
    async fn an_already_elapsed_timer_never_clones_the_caller_waker() {
        let stale = crate::runtime::now()
            .checked_sub(std::time::Duration::from_secs(60))
            .expect("the test clock is far enough past its origin to date a budget backwards");
        let runtime =
            Arc::new(crate::mailbox::capability::tests::TestRuntime::new().with_now(move || stale));
        let mut future = Box::pin(super::Deadlined::no_attempt(
            PendingOnFirstExpiry::default(),
            std::time::Duration::from_secs(1),
            runtime,
        ));
        let counter = Arc::new(CloneCounter::default());
        let waker = counting_waker(&counter);
        let mut context = Context::from_waker(&waker);

        // Pending because the operation withholds its first elapsed poll, not
        // because the timer parked.
        assert!(future.as_mut().poll(&mut context).is_pending());

        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            0,
            "an already-ready timer is answered by the static no-op probe, so no proxy allocates and the caller's clone vtable is never dispatched"
        );
        assert!(
            future.timer.is_none(),
            "an elapsed timer is retired at once"
        );
    }

    /// An unbounded deadline is `std::future::pending()`: it wakes nobody, so
    /// registering with it would clone a caller waker for an event that cannot
    /// happen. This is `docs/observation.md`'s unbounded-wait idiom, reached
    /// by every `Duration::MAX` budget in the public API.
    #[crate::runtime::test(start_paused = true)]
    async fn an_unbounded_deadline_never_allocates_a_timer_proxy() {
        let mut future = Box::pin(super::Deadlined::no_attempt(
            PendingOnFirstExpiry::default(),
            std::time::Duration::MAX,
            crate::mailbox::capability::tests::runtime(),
        ));
        let counter = Arc::new(CloneCounter::default());
        let waker = counting_waker(&counter);
        let mut context = Context::from_waker(&waker);

        assert!(future.as_mut().poll(&mut context).is_pending());
        assert!(future.as_mut().poll(&mut context).is_pending());

        assert!(
            future.timer.is_none(),
            "an unbounded deadline constructs no timer or waker proxy"
        );
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            0,
            "an unbounded deadline never dispatches the caller's clone vtable"
        );
    }

    #[test]
    fn a_zero_budget_short_circuits_without_polling_the_operation() {
        let mut future = Box::pin(super::Deadlined::no_attempt(
            PendingOnFirstExpiry::default(),
            shelterwood_core::DeadlineBudget::ZERO,
            crate::mailbox::capability::tests::runtime(),
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(0)));
        assert_eq!(
            future.operation.elapsed_polls, 0,
            "a zero budget never attempts the operation"
        );
    }
}
