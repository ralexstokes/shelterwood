use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use shelterwood_core::DeadlineBudget;

use crate::{
    BoxedSleep, MailboxRuntime,
    cell::waker_slot::{WakerAction, WakerEffects},
    panic::PanicAccumulator,
    waker_proxy::WakerProxy,
};

pub(crate) use shelterwood_core::deadline::Deadline;

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
    timer: Option<BoxedSleep>,
    timer_waker: Option<WakerProxy>,
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
            timer_waker: None,
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
    fn poll_timer(&mut self, context: &mut Context<'_>) -> Poll<()> {
        if self.timer_waker.is_none() {
            let mut probe = Context::from_waker(Waker::noop());
            if self
                .timer
                .as_mut()
                .expect("an unelapsed deadline future retains its timer")
                .as_mut()
                .poll(&mut probe)
                .is_ready()
            {
                return Poll::Ready(());
            }
            self.timer_waker = Some(WakerProxy::new());
        }

        let timer_waker = self
            .timer_waker
            .as_ref()
            .expect("a parked timer retains its waker proxy");
        timer_waker.register(context.waker());
        let mut proxy_context = Context::from_waker(timer_waker.waker());
        self.timer
            .as_mut()
            .expect("an unelapsed deadline future retains its timer")
            .as_mut()
            .poll(&mut proxy_context)
    }

    /// Retires the caller waker through the blocking disposal lane, then
    /// synchronously removes the framework-only wheel registration.
    fn retire_timer(&mut self, panics: &mut PanicAccumulator) {
        let mut effects = WakerEffects::default();
        if let Some(timer_waker) = &self.timer_waker {
            timer_waker.retire(
                WakerAction::Dispose(Arc::clone(&self.runtime)),
                &mut effects,
            );
        }
        effects.flush(panics);

        let timer = self.timer.take();
        panics.run(|| drop(timer));
        let timer_waker = self.timer_waker.take();
        panics.run(|| drop(timer_waker));
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
            if !this.budget_width.is_zero() {
                this.timer = Some(this.runtime.sleep_until(budget.instant()));
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
            // driver mutex. The caller waker may block, so move it to the
            // disposal lane before synchronously removing the wheel entry.
            // `result` may own a user value, so a failure to submit cleanup
            // remains subordinate to returning the completed output.
            let mut panics = PanicAccumulator::default();
            this.retire_timer(&mut panics);
            crate::panic::discard_panic(panics.take());
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
            let mut panics = PanicAccumulator::default();
            this.retire_timer(&mut panics);
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
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        self.retire_timer(&mut panics);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
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
            crate::capability::tests::runtime(),
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
            crate::capability::tests::runtime(),
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
            crate::capability::tests::runtime(),
        ));
        let mut context = Context::from_waker(Waker::noop());

        assert!(future.timer_waker.is_none());
        assert!(future.as_mut().poll(&mut context).is_ready());
        assert!(future.timer_waker.is_none());
    }

    #[test]
    fn a_zero_budget_short_circuits_without_polling_the_operation() {
        let mut future = Box::pin(super::Deadlined::no_attempt(
            PendingOnFirstExpiry::default(),
            shelterwood_core::DeadlineBudget::ZERO,
            crate::capability::tests::runtime(),
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
