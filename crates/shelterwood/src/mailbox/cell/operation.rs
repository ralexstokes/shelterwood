use std::sync::{Arc, Mutex};

use crate::{identity::Incarnation, runtime::PanicAccumulator};
use shelterwood_core::waker::{WakerAction, WakerEffects, WakerSlot};

use super::state::WaiterId;

/// A send operation's outcome. Each message-carrying variant owns its message
/// by value; leaving it moves the message out and installs the successor in
/// the same critical section.
pub(in crate::mailbox) enum OperationOutcome<M> {
    Waiting {
        message: M,
        newest_observed: Option<Incarnation>,
    },
    Accepted(Incarnation),
    Terminated {
        message: M,
        final_incarnation: Option<Incarnation>,
    },
    /// The owning `SendFuture` has taken its outcome: withdrawn, or observed
    /// terminal. The future drops the operation on the same edge.
    Retired,
}

pub(in crate::mailbox) struct OperationState<M> {
    pub(in crate::mailbox) outcome: OperationOutcome<M>,
    pub(super) waker: WakerSlot,
    pub(super) registration: Option<WaiterId>,
}

pub(in crate::mailbox) struct SendOperation<M> {
    pub(in crate::mailbox) state: Mutex<OperationState<M>>,
}

pub(in crate::mailbox) enum OperationPoll<M> {
    Accepted(Incarnation),
    Terminated {
        message: M,
        final_incarnation: Option<Incarnation>,
    },
    Pending,
    NeedsWakerClone,
}

// Lock order is mailbox state, then send-operation state. Code that starts
// from an operation lock must release it before entering the mailbox. Paths
// that take only the operation lock (polling, detached teardown) never reach
// back into mailbox state. This keeps acceptance, withdrawal, and waiter
// registration on one acyclic ordering.

impl<M> SendOperation<M> {
    pub(super) fn new(
        message: M,
        newest_observed: Option<Incarnation>,
        registration: Option<WaiterId>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OperationState {
                outcome: OperationOutcome::Waiting {
                    message,
                    newest_observed,
                },
                waker: WakerSlot::default(),
                registration,
            }),
        })
    }

    pub(super) fn clear_registration(&self) {
        self.state
            .lock()
            .expect("send operation mutex poisoned")
            .registration = None;
    }

    pub(super) fn observe(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        if let OperationOutcome::Waiting {
            newest_observed, ..
        } = &mut state.outcome
        {
            *newest_observed = Some(incarnation);
        }
    }

    /// Leaves `Waiting` for `Accepted`, moving the message out. Any other
    /// outcome is left in place and yields `None`.
    pub(super) fn accept(&self, incarnation: Incarnation, effects: &mut WakerEffects) -> Option<M> {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        match std::mem::replace(&mut state.outcome, OperationOutcome::Accepted(incarnation)) {
            OperationOutcome::Waiting { message, .. } => {
                state.waker.take(WakerAction::Wake, effects);
                Some(message)
            }
            other => {
                state.outcome = other;
                None
            }
        }
    }

    pub(super) fn terminate(&self, final_incarnation: Option<Incarnation>) {
        let mut effects = WakerEffects::default();
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        let outcome = match std::mem::replace(&mut state.outcome, OperationOutcome::Retired) {
            OperationOutcome::Waiting { message, .. } => {
                state.waker.take(WakerAction::Wake, &mut effects);
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                }
            }
            other => other,
        };
        state.outcome = outcome;
    }

    pub(in crate::mailbox) fn poll(
        &self,
        mut replacement: Option<std::task::Waker>,
        current: &std::task::Waker,
    ) -> OperationPoll<M> {
        // Effects precede the guard, so even an unwind drops the guard before
        // invoking a displaced RawWaker vtable.
        let mut effects = WakerEffects::default();
        let result = loop {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let (outcome, result) =
                match std::mem::replace(&mut state.outcome, OperationOutcome::Retired) {
                    // The replacement was cloned outside this lock. Every
                    // outcome but `Waiting` refuses it, so it must be retired
                    // before this frame either moves a terminal message into
                    // the return value or raises the retired diagnostic below.
                    // Both would otherwise destroy the caller waker mid-unwind
                    // -- beside a user value in the first case, inside an
                    // existing panic in the second. A hostile destructor is
                    // contained at this ready seam for the same reason as reply
                    // and timer retirement.
                    outcome @ (OperationOutcome::Accepted(_)
                    | OperationOutcome::Terminated { .. }
                    | OperationOutcome::Retired)
                        if replacement.is_some() =>
                    {
                        (outcome, None)
                    }
                    OperationOutcome::Accepted(incarnation) => (
                        OperationOutcome::Accepted(incarnation),
                        Some(Ok(OperationPoll::Accepted(incarnation))),
                    ),
                    OperationOutcome::Terminated {
                        message,
                        final_incarnation,
                    } => (
                        OperationOutcome::Retired,
                        Some(Ok(OperationPoll::Terminated {
                            message,
                            final_incarnation,
                        })),
                    ),
                    waiting @ OperationOutcome::Waiting { .. } => {
                        let result = if let Some(replacement) = replacement.take() {
                            state.waker.replace(replacement, &mut effects);
                            OperationPoll::Pending
                        } else if state.waker.will_wake(current) {
                            OperationPoll::Pending
                        } else {
                            OperationPoll::NeedsWakerClone
                        };
                        (waiting, Some(Ok(result)))
                    }
                    OperationOutcome::Retired => (
                        OperationOutcome::Retired,
                        Some(Err("a retired send operation was polled")),
                    ),
                };
            state.outcome = outcome;
            drop(state);
            if let Some(result) = result {
                break result;
            }

            // Stage the clone through the structural waker sink, then drain it
            // with no operation lock held and before re-reading the stable
            // non-waiting outcome. `flush` catches its vtable panic; taking and
            // discarding the payload prevents `PanicAccumulator::drop` from
            // resuming it over the eventual by-value result.
            let mut staged = WakerSlot::default();
            staged.replace(
                replacement
                    .take()
                    .expect("a refused clone window retains its replacement waker"),
                &mut effects,
            );
            staged.take(WakerAction::DropInline, &mut effects);
            let mut panics = PanicAccumulator::default();
            effects.flush(&mut panics);
            crate::runtime::discard_panic(panics.take());
        };
        drop(effects);
        match result {
            Ok(result) => result,
            Err(message) => panic!("{message}"),
        }
    }

    #[cfg(test)]
    pub(super) fn install_test_waker(&self, waker: std::task::Waker) {
        let mut effects = WakerEffects::default();
        {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            state.waker.replace(waker, &mut effects);
        }
    }
}
pub(in crate::mailbox) enum Submission<M> {
    Accepted(Incarnation),
    Parked(Arc<SendOperation<M>>),
    Terminated {
        message: M,
        final_incarnation: Option<Incarnation>,
    },
}
#[derive(Clone, Copy)]
pub(in crate::mailbox) enum WithdrawalDisposition {
    Inline,
    Isolated,
}

impl WithdrawalDisposition {
    pub(super) fn action(self) -> WakerAction {
        match self {
            Self::Inline => WakerAction::DropInline,
            Self::Isolated => WakerAction::Run(crate::runtime::dispose_waker),
        }
    }
}

pub(in crate::mailbox) struct Withdrawal<M> {
    pub(super) outcome: Option<WithdrawalOutcome<M>>,
    pub(super) _waker_effects: WakerEffects,
}

impl<M> Withdrawal<M> {
    pub(in crate::mailbox) fn without_effects(outcome: WithdrawalOutcome<M>) -> Self {
        Self {
            outcome: Some(outcome),
            _waker_effects: WakerEffects::default(),
        }
    }

    pub(in crate::mailbox) fn take_outcome(&mut self) -> WithdrawalOutcome<M> {
        self.outcome
            .take()
            .expect("a withdrawal outcome is consumed exactly once")
    }

    pub(in crate::mailbox) fn take_outcome_if_present(&mut self) -> Option<WithdrawalOutcome<M>> {
        self.outcome.take()
    }

    pub(in crate::mailbox) fn finish(self) {}
}

pub(in crate::mailbox) enum WithdrawalOutcome<M> {
    Withdrawn {
        message: M,
        observed: Option<Incarnation>,
    },
    Accepted(Incarnation),
    Terminated {
        message: M,
        observed: Option<Incarnation>,
    },
}
