use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use shelterwood_core::{DeadlineBudget, ZeroBudgetBehavior, select_zero_budget_behavior};

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
    budget_width: DeadlineBudget,
    budget: Option<crate::deadline::Deadline>,
    timer: Option<crate::runtime::BoxedSleep>,
    pub(super) started: bool,
    phase: DeadlinePhase,
}

impl<F> Deadlined<F> {
    /// Constructs the shared no-attempt deadline policy used by public
    /// mailbox operations.
    pub(super) fn no_attempt(operation: F, budget_width: DeadlineBudget) -> Self {
        Self {
            operation,
            budget_width,
            budget: None,
            timer: None,
            started: false,
            phase: DeadlinePhase::InitialAttempt,
        }
    }
}

impl<F: DeadlineOperation + Unpin> Future for Deadlined<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if !this.started {
            this.started = true;
            let budget = crate::runtime::deadline(this.budget_width.duration());
            this.budget = Some(budget);
            if select_zero_budget_behavior(this.budget_width, ZeroBudgetBehavior::NoAttempt)
                .is_none()
            {
                this.timer = Some(crate::runtime::sleep_deadline(budget));
            }
        }
        // A zero budget short-circuits: the operation is never attempted, so
        // nothing is submitted and no completion is observed. The expiry
        // boundary below governs only non-zero budgets, and the two rules
        // therefore never compete.
        if select_zero_budget_behavior(this.budget_width, ZeroBudgetBehavior::NoAttempt).is_some() {
            return Poll::Ready(this.operation.short_circuit());
        }
        let budget = this
            .budget
            .expect("a started deadline future retains its captured budget");
        if let Poll::Ready(result) =
            this.operation
                .poll_deadlined(context, budget, DeadlinePhase::InitialAttempt)
        {
            return Poll::Ready(result);
        }
        if this.phase == DeadlinePhase::InitialAttempt {
            if this
                .timer
                .as_mut()
                .expect("an unelapsed deadline future retains its timer")
                .as_mut()
                .poll(context)
                .is_pending()
            {
                return Poll::Pending;
            }
            // The timer is a one-shot future: polling it again after it
            // resolves panics. Latch the transition and release it, so an
            // elapsed poll that stays pending re-polls only the operation.
            this.phase = DeadlinePhase::TimeoutArbitration;
            this.timer = None;
        }
        this.operation
            .poll_deadlined(context, budget, DeadlinePhase::TimeoutArbitration)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    /// Stays pending on its first elapsed poll, modelling an atomic
    /// completion transition that won without publishing its payload yet.
    #[derive(Default)]
    struct PendingOnFirstExpiry {
        elapsed_polls: usize,
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
            width.into(),
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

    #[test]
    fn a_zero_budget_short_circuits_without_polling_the_operation() {
        let mut future = Box::pin(super::Deadlined::no_attempt(
            PendingOnFirstExpiry::default(),
            shelterwood_core::DeadlineBudget::ZERO,
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
