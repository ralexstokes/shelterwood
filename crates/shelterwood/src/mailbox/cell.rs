use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    identity::{AtomicMonotonicCounter, ChildId, Incarnation},
    mailbox::{
        MailboxBindToken, MailboxClose, MailboxControl, MailboxDisposal, MailboxEffectQueue,
        MailboxEffectSink, MailboxTermination,
    },
    policy::ResolvedMailbox,
    runtime::{Signal, SignalWatcher},
};
use shelterwood_core::waker::WakerEffects;

use super::{SendError, SendErrorKind};

mod effects;
mod operation;
mod state;

use effects::{MailboxTeardown, MailboxTxn, ReturnedMessage, Termination};
pub(super) use operation::{
    OperationOutcome, OperationPoll, SendOperation, Submission, Withdrawal, WithdrawalDisposition,
    WithdrawalOutcome,
};
pub(crate) use state::AcceptedSequence;
use state::{
    Binding, MailboxState, Phase, ReceiveMode, WaiterQueue, accept_locked, promote_waiters,
};

/// Restart-stable mailbox state for one actor membership.
///
/// Dropping the last handle can destroy unread user payloads. Framework owners
/// must therefore close or terminalize the cell and transfer its payload to
/// isolated disposal before releasing their final handle.
pub(crate) struct MailboxCell<M> {
    pub(super) actor_id: ChildId,
    pub(super) state: Mutex<MailboxState<M>>,
    accepted: AtomicMonotonicCounter,
    changed: Signal,
}

impl<M> fmt::Debug for MailboxCell<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailboxCell")
            .field("actor_id", &self.actor_id)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> MailboxCell<M> {
    pub(crate) fn new(actor_id: ChildId) -> Arc<Self> {
        Arc::new(Self {
            actor_id,
            state: Mutex::new(MailboxState {
                kind: None,
                bind_permit: Arc::new(AtomicBool::new(false)),
                phase: Phase::Unbound,
                last_bound: None,
                waiters: WaiterQueue::default(),
                queue: VecDeque::new(),
                latest: None,
            }),
            accepted: AtomicMonotonicCounter::new(),
            changed: Signal::default(),
        })
    }

    pub(super) fn submit(&self, message: M) -> Submission<M> {
        let mut transaction = MailboxTxn::new(self);
        let submission = match transaction.phase {
            Phase::Terminal(final_incarnation) => Submission::Terminated {
                message,
                final_incarnation,
            },
            Phase::Bound(binding) => {
                let accepted = {
                    let (state, effects) = transaction.parts();
                    accept_locked(state, binding, message, &self.accepted, effects)
                };
                match accepted {
                    Ok(incarnation) => Submission::Accepted(incarnation),
                    Err(message) => transaction.park(message, Some(binding.incarnation)),
                }
            }
            Phase::Frozen(binding) => transaction.park(message, Some(binding.incarnation)),
            Phase::Unbound => transaction.park(message, None),
        };
        transaction.finish(submission)
    }

    pub(super) fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        let mut transaction = MailboxTxn::new(self);
        let (incarnation_observed, kind, message) = match transaction.phase {
            Phase::Terminal(final_incarnation) => {
                (final_incarnation, SendErrorKind::Terminated, message)
            }
            Phase::Unbound => (None, SendErrorKind::NotRunning, message),
            Phase::Frozen(binding) => (
                Some(binding.incarnation),
                SendErrorKind::NotRunning,
                message,
            ),
            Phase::Bound(binding) => {
                let accepted = {
                    let (state, effects) = transaction.parts();
                    accept_locked(state, binding, message, &self.accepted, effects)
                };
                match accepted {
                    Ok(incarnation) => return transaction.finish(Ok(incarnation)),
                    Err(message) => (Some(binding.incarnation), SendErrorKind::Full, message),
                }
            }
        };
        transaction.finish(Err(SendError {
            actor_id: self.actor_id.clone(),
            incarnation_observed,
            message,
            kind,
        }))
    }

    fn receive(&self, incarnation: Incarnation, mode: ReceiveMode) -> Option<M> {
        // Declared before the transaction so it drops after it: see
        // `ReturnedMessage`.
        let mut returned = ReturnedMessage { value: None };
        let mut transaction = MailboxTxn::new(self);
        let (binding, live) = match transaction.phase {
            Phase::Bound(binding) if binding.incarnation == incarnation => (binding, true),
            Phase::Frozen(binding)
                if mode == ReceiveMode::Drain && binding.incarnation == incarnation =>
            {
                (binding, false)
            }
            Phase::Unbound | Phase::Bound(_) | Phase::Frozen(_) | Phase::Terminal(_) => {
                return transaction.finish(None);
            }
        };
        let (state, effects) = transaction.parts();
        if let Some(envelope) = state.take_next(binding.kind, mode) {
            returned.value = Some(envelope.message);
            // Receiving frees one slot, so a bound mailbox admits its oldest
            // parked senders. Neither the receipt nor the promotion pulses:
            // the receiving actor is the change signal's only watcher, and it
            // re-reads the accepted sequence before it waits.
            if live {
                promote_waiters(state, binding, &self.accepted, effects);
            }
        }
        transaction.finish(());
        returned.value.take()
    }

    pub(super) fn current_observation(&self) -> Option<Incarnation> {
        self.state
            .lock()
            .expect("mailbox mutex poisoned")
            .phase
            .observation()
    }

    fn watcher(&self) -> SignalWatcher {
        self.changed.watcher()
    }

    fn accepted_sequence(&self) -> AcceptedSequence {
        AcceptedSequence(self.accepted.load(Ordering::Acquire))
    }
}

impl<M: Send + 'static> MailboxCell<M> {
    /// Withdraws a send operation into an explicit post-unlock effect set.
    ///
    /// The registered waker is never returned as a raw value. `WakerSlot`
    /// transfers it directly to this operation's effects, and callers choose
    /// inline or isolated destruction before the locked transition begins.
    pub(super) fn withdraw(
        &self,
        operation: &Arc<SendOperation<M>>,
        disposition: WithdrawalDisposition,
    ) -> Withdrawal<M> {
        // Declare waker effects before the transaction. On every unwind the
        // operation guard drops first, then the mailbox transaction releases
        // its guard, and only then can these effects run.
        let mut waker_effects = WakerEffects::default();
        let mut transaction = MailboxTxn::new(self);
        let phase = transaction.phase;
        let (outcome, registration) = {
            let mut state = operation
                .state
                .lock()
                .expect("send operation mutex poisoned");
            let outcome = match std::mem::replace(&mut state.outcome, OperationOutcome::Retired) {
                // Mailbox terminality linearizes in the phase, while a parked
                // operation linearizes when its own outcome leaves `Waiting`.
                // A terminal teardown may therefore have detached this waiter
                // without discharging it yet; an already-expired withdrawal
                // legitimately wins that operation-local race.
                // The mailbox lock makes this one evidence snapshot: a binding
                // either precedes withdrawal and contributes its incarnation,
                // or follows the completed withdrawal. This also covers an
                // operation first submitted by an elapsed (including
                // zero-duration) deadline poll.
                OperationOutcome::Waiting {
                    message,
                    newest_observed,
                } => Some(WithdrawalOutcome::Withdrawn {
                    message,
                    observed: newest_observed.or(phase.observation()),
                }),
                OperationOutcome::Accepted(incarnation) => {
                    Some(WithdrawalOutcome::Accepted(incarnation))
                }
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => Some(WithdrawalOutcome::Terminated {
                    message,
                    observed: final_incarnation,
                }),
                OperationOutcome::Retired => None,
            };
            // Acceptance and termination took the waker in the critical
            // section that published their outcome, so this is normally
            // empty for them.
            state.waker.take(disposition.action(), &mut waker_effects);
            (outcome, state.registration.take())
        };
        // Promotion clears a registration under this lock before it leaves
        // the queue, so a registration still set here names this operation's
        // own entry — unless terminal teardown already detached the queue.
        let removed = registration
            .and_then(|registration| transaction.state_mut().waiters.remove(registration));
        let detached = registration.is_some() && removed.is_none();
        // The caller's `Arc` keeps the removed entry alive; it is released
        // with no lock held all the same.
        drop(transaction.finish(removed));
        // Drop glue reaches this during unwinds, where a failed assertion
        // would abort rather than diagnose.
        debug_assert!(
            std::thread::panicking() || outcome.is_some(),
            "a send operation is withdrawn at most once"
        );
        debug_assert!(
            std::thread::panicking() || !detached || matches!(phase, Phase::Terminal(_)),
            "only terminal teardown detaches a live waiter registration"
        );
        Withdrawal {
            outcome,
            _waker_effects: waker_effects,
        }
    }
}

impl<M: Send + 'static> MailboxControl for MailboxCell<M> {
    fn configure(
        &self,
        mailbox: ResolvedMailbox,
        effects: &mut dyn MailboxEffectSink,
    ) -> MailboxBindToken {
        let mut transaction = MailboxTxn::deferred(self, effects);
        let mismatch = transaction.configure_kind(mailbox);
        let token = MailboxBindToken::new(Arc::clone(&transaction.bind_permit));
        transaction.finish(());
        if let Some(existing) = mismatch {
            panic!(
                "mailbox configuration changed from {existing:?} to {mailbox:?} after initialization"
            );
        }
        token
    }

    fn bind(
        &self,
        token: MailboxBindToken,
        incarnation: Incarnation,
        effects: &mut dyn MailboxEffectSink,
    ) {
        let mut transaction = MailboxTxn::deferred(self, effects);
        let verdict = match (transaction.phase, transaction.kind) {
            (Phase::Terminal(_), _) => return transaction.finish(()),
            (_, None) => Err("mailbox must be configured before its first bind"),
            (Phase::Bound(_) | Phase::Frozen(_), Some(_)) => {
                Err("mailbox must close the prior incarnation before rebinding")
            }
            (Phase::Unbound, Some(_)) if !token.claim(&transaction.bind_permit) => {
                Err("mailbox bind token is foreign or was already consumed")
            }
            (Phase::Unbound, Some(kind)) => Ok(Binding { incarnation, kind }),
        };
        let binding = match verdict {
            Ok(binding) => binding,
            Err(message) => {
                transaction.finish(());
                std::panic::panic_any(message)
            }
        };
        {
            let (state, effects) = transaction.parts();
            state.phase = Phase::Bound(binding);
            state.last_bound = Some(incarnation);
            // Binding is an observation edge for every operation that remains
            // parked through it, including FIFO overflow that cannot be
            // promoted into the current capacity. Withdrawal takes the mailbox
            // lock before the operation lock, so a concurrent timeout sees
            // either the prior evidence or this incarnation consistently with
            // which edge won.
            state.waiters.observe_all(incarnation);
            promote_waiters(state, binding, &self.accepted, effects);
            effects.pulse();
            effects.isolate_displaced();
        }
        transaction.finish(())
    }

    fn freeze(&self, incarnation: Incarnation, effects: &mut dyn MailboxEffectSink) {
        let mut transaction = MailboxTxn::deferred(self, effects);
        let Phase::Bound(binding) = transaction.phase else {
            return transaction.finish(());
        };
        if binding.incarnation != incarnation {
            return transaction.finish(());
        }
        transaction.set_phase(Phase::Frozen(binding));
        transaction.effects.pulse();
        transaction.finish(())
    }

    fn close(
        &self,
        incarnation: Incarnation,
        effects: &mut dyn MailboxEffectSink,
    ) -> Option<MailboxClose> {
        let mut transaction = MailboxTxn::deferred(self, effects);
        if transaction.phase.observation() != Some(incarnation) {
            return transaction.finish(None);
        }
        // Parked senders stay parked for the next incarnation.
        transaction.set_phase(Phase::Unbound);
        let token = transaction.reset_bind_permit();
        let payload = transaction.take_payload();
        transaction.effects.pulse();
        let disposal = Box::new(payload) as MailboxDisposal;
        // The close result outlives this transaction's effect flush at every
        // caller, so its drop isolates the unread payload if that flush
        // unwinds.
        transaction.finish(Some(MailboxClose::new(token, disposal)))
    }

    fn prepare_termination(
        &self,
        effects: &mut dyn MailboxEffectSink,
    ) -> Option<Box<dyn MailboxTermination>> {
        let mut transaction = MailboxTxn::deferred(self, effects);
        if matches!(transaction.phase, Phase::Terminal(_)) {
            return transaction.finish(None);
        }
        let final_incarnation = transaction.last_bound;
        // This phase transition linearizes mailbox terminality. Each detached
        // waiter is decided separately by its `Waiting ->` outcome
        // transition, so an already-expired withdrawal may beat the deferred
        // discharge even after this mailbox-wide transition.
        transaction.set_phase(Phase::Terminal(final_incarnation));
        let waiters = std::mem::take(&mut transaction.state_mut().waiters);
        let payload = transaction.take_payload();
        let termination = Termination {
            waiters,
            final_incarnation,
        };
        let teardown = Some(Box::new(MailboxTeardown {
            changed: Some(self.changed.clone()),
            payload: Some(payload),
            termination: Some(termination),
        }) as Box<dyn MailboxTermination>);
        transaction.finish(teardown)
    }
}

pub(crate) struct MailboxReceiver<M> {
    mailbox: Arc<MailboxCell<M>>,
    incarnation: Incarnation,
    watcher: SignalWatcher,
}

impl<M: Send + 'static> MailboxReceiver<M> {
    pub(crate) fn new(mailbox: Arc<MailboxCell<M>>, incarnation: Incarnation) -> Self {
        let watcher = mailbox.watcher();
        Self {
            mailbox,
            incarnation,
            watcher,
        }
    }

    pub(crate) fn try_recv(&self) -> Option<M> {
        self.mailbox.receive(self.incarnation, ReceiveMode::Drain)
    }

    pub(crate) fn try_recv_live_through(&self, accepted_sequence: AcceptedSequence) -> Option<M> {
        self.mailbox.receive(
            self.incarnation,
            ReceiveMode::LiveThrough(accepted_sequence),
        )
    }

    pub(crate) fn accepted_sequence(&self) -> AcceptedSequence {
        self.mailbox.accepted_sequence()
    }

    pub(crate) fn freeze(&self) {
        let mut effects = MailboxEffectQueue::default();
        self.mailbox.freeze(self.incarnation, &mut effects);
    }

    /// Waits for mailbox activity in the façade's merged raw-actor event loop.
    ///
    /// This crate-private receiver seam is used by `RawContext`; it is not
    /// reachable from Shelterwood's supported API.
    pub(crate) async fn changed(&mut self) {
        self.watcher.changed().await;
    }
}

#[cfg(test)]
mod stress_tests;
#[cfg(test)]
pub(super) mod tests;
