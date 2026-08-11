use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex, atomic::Ordering},
    task::Waker,
};

use crate::{
    ChildId, Incarnation,
    cells::{MailboxControl, MailboxDisposal, MailboxTermination},
    identity::{AtomicPoisonedCounter, PoisonedCounter},
    policy::ResolvedMailbox,
    runtime::{PanicAccumulator, PanicPayload, Signal, SignalWatcher, resume_panic},
};

use super::{SendError, SendErrorKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindingStatus {
    Unbound,
    Bound(Incarnation),
    Frozen(Incarnation),
    Terminal(Option<Incarnation>),
}

enum MailboxBinding<M> {
    Unbound(WaiterQueue<M>),
    Bound(BoundState<M>),
    Frozen {
        incarnation: Incarnation,
        waiters: WaiterQueue<M>,
    },
    Terminal(Option<Incarnation>),
}

/// A bound mailbox either has no parked senders or is explicitly blocked.
/// Only the blocked variant can own waiters, and it is constructed only when
/// a queue mailbox has reached capacity.
enum BoundState<M> {
    Available(Incarnation),
    Full {
        incarnation: Incarnation,
        waiters: WaiterQueue<M>,
    },
}

impl<M> BoundState<M> {
    fn incarnation(&self) -> Incarnation {
        match self {
            Self::Available(incarnation) | Self::Full { incarnation, .. } => *incarnation,
        }
    }
}

/// Which mailbox binding states may yield accepted messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiveMode {
    LiveOnly,
    IncludeFrozen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MailboxKind {
    Queue(NonZeroUsize),
    Latest,
}

pub(super) struct Envelope<M> {
    message: M,
    accepted_sequence: AcceptedSequence,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct AcceptedSequence(u64);

pub(super) enum OperationOutcome<M> {
    Waiting {
        message: Option<M>,
        newest_observed: Option<Incarnation>,
    },
    Accepted(Incarnation),
    Terminated {
        message: Option<M>,
        final_incarnation: Option<Incarnation>,
    },
    Withdrawn,
}

pub(super) struct OperationState<M> {
    pub(super) outcome: OperationOutcome<M>,
    pub(super) waker: Option<Waker>,
    registration: Option<WaiterId>,
}

pub(super) struct SendOperation<M> {
    pub(super) state: Mutex<OperationState<M>>,
}

// Lock order is mailbox state, then send-operation state. Code that starts
// from an operation lock must release it before entering the mailbox. Paths
// that take only the operation lock (polling, detached teardown) never reach
// back into mailbox state. This keeps acceptance, withdrawal, and waiter
// registration on one acyclic ordering.

impl<M> SendOperation<M> {
    fn new(message: M) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OperationState {
                outcome: OperationOutcome::Waiting {
                    message: Some(message),
                    newest_observed: None,
                },
                waker: None,
                registration: None,
            }),
        })
    }

    fn register(&self, registration: WaiterId) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        debug_assert!(state.registration.is_none());
        debug_assert!(matches!(state.outcome, OperationOutcome::Waiting { .. }));
        state.registration = Some(registration);
    }

    fn clear_registration(&self, registration: WaiterId) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        if state.registration == Some(registration) {
            state.registration = None;
        } else {
            // A cancellation can win after terminal teardown detaches the
            // queue but before it discharges this entry.
            debug_assert!(state.registration.is_none());
        }
    }

    fn observe(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        if let OperationOutcome::Waiting {
            newest_observed, ..
        } = &mut state.outcome
        {
            *newest_observed = Some(incarnation);
        }
    }

    fn accept(&self, incarnation: Incarnation) -> Option<(M, Option<Waker>)> {
        let (message, wake) = {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return None;
            };
            let message = message.take()?;
            state.outcome = OperationOutcome::Accepted(incarnation);
            (message, state.waker.take())
        };
        Some((message, wake))
    }

    fn terminate(&self, final_incarnation: Option<Incarnation>) {
        let wake = {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return;
            };
            let message = message.take();
            state.outcome = OperationOutcome::Terminated {
                message,
                final_incarnation,
            };
            state.waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

pub(super) struct MailboxState<M> {
    kind: Option<MailboxKind>,
    binding: MailboxBinding<M>,
    last_bound: Option<Incarnation>,
    pub(super) queue: VecDeque<Envelope<M>>,
    latest: Option<Envelope<M>>,
}

impl<M> MailboxState<M> {
    fn status(&self) -> BindingStatus {
        match &self.binding {
            MailboxBinding::Unbound(_) => BindingStatus::Unbound,
            MailboxBinding::Bound(bound) => BindingStatus::Bound(bound.incarnation()),
            MailboxBinding::Frozen { incarnation, .. } => BindingStatus::Frozen(*incarnation),
            MailboxBinding::Terminal(incarnation) => BindingStatus::Terminal(*incarnation),
        }
    }

    fn park(&mut self, operation: &Arc<SendOperation<M>>) {
        match &mut self.binding {
            MailboxBinding::Unbound(waiters)
            | MailboxBinding::Frozen { waiters, .. }
            | MailboxBinding::Bound(BoundState::Full { waiters, .. }) => {
                waiters.park(operation);
            }
            MailboxBinding::Bound(BoundState::Available(incarnation)) => {
                let Some(MailboxKind::Queue(capacity)) = self.kind else {
                    unreachable!("only a capacity-bound queue can park while bound")
                };
                debug_assert_eq!(self.queue.len(), capacity.get());
                let incarnation = *incarnation;
                let mut waiters = WaiterQueue::default();
                waiters.park(operation);
                self.binding = MailboxBinding::Bound(BoundState::Full {
                    incarnation,
                    waiters,
                });
            }
            MailboxBinding::Terminal(_) => {
                unreachable!("terminal submissions return their payload directly")
            }
        }
    }

    /// Detaches every waiter from a non-terminal binding before a transition.
    ///
    /// Taking directly from the owning variant avoids temporarily claiming
    /// the mailbox is terminal merely to move its queue out.
    fn take_waiters(&mut self) -> WaiterQueue<M> {
        match &mut self.binding {
            MailboxBinding::Unbound(waiters)
            | MailboxBinding::Frozen { waiters, .. }
            | MailboxBinding::Bound(BoundState::Full { waiters, .. }) => std::mem::take(waiters),
            MailboxBinding::Bound(BoundState::Available(_)) => WaiterQueue::default(),
            MailboxBinding::Terminal(_) => {
                // Terminalization takes the waiters exactly once and no live
                // transition follows it. This runs under the mailbox mutex
                // with no way to release it first, so diagnose in debug rather
                // than poisoning the lock for every sender in release.
                debug_assert!(false, "a terminal mailbox has no live transition");
                WaiterQueue::default()
            }
        }
    }

    fn remove_waiter(&mut self, registration: WaiterId) -> Option<Arc<SendOperation<M>>> {
        let mut available = None;
        let removed = match &mut self.binding {
            MailboxBinding::Unbound(waiters) | MailboxBinding::Frozen { waiters, .. } => {
                waiters.remove(registration)
            }
            MailboxBinding::Bound(BoundState::Full {
                incarnation,
                waiters,
            }) => {
                let removed = waiters.remove(registration);
                if removed.is_some() && waiters.is_empty() {
                    available = Some(*incarnation);
                }
                removed
            }
            MailboxBinding::Bound(BoundState::Available(_)) | MailboxBinding::Terminal(_) => None,
        };
        if let Some(incarnation) = available {
            self.binding = MailboxBinding::Bound(BoundState::Available(incarnation));
        }
        removed
    }

    #[cfg(test)]
    pub(super) fn waiters(&self) -> Option<&WaiterQueue<M>> {
        match &self.binding {
            MailboxBinding::Unbound(waiters)
            | MailboxBinding::Frozen { waiters, .. }
            | MailboxBinding::Bound(BoundState::Full { waiters, .. }) => Some(waiters),
            MailboxBinding::Bound(BoundState::Available(_)) | MailboxBinding::Terminal(_) => None,
        }
    }
}

pub(super) enum Submission<M> {
    Accepted(Incarnation),
    Parked(Arc<SendOperation<M>>),
    Terminated {
        message: M,
        final_incarnation: Option<Incarnation>,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct WaiterId(u64);

impl WaiterId {
    #[cfg(test)]
    const POISON: Self = Self(u64::MAX);
}

/// FIFO registrations with direct removal by a send operation.
///
/// Monotonic keys are insertion order, so the first map entry is the oldest
/// waiter. Keys are never reused within one queue instance; a queue is only
/// replaced when it is empty or by the absorbing Terminal state, so no
/// registration outlives its queue and stale cancellation ids remain harmless.
/// `u64::MAX` is a poison key and is never minted; exhaustion remains poisoned
/// instead of wrapping back into the live id domain.
pub(super) struct WaiterQueue<M> {
    entries: BTreeMap<WaiterId, Arc<SendOperation<M>>>,
    ids: PoisonedCounter,
    #[cfg(test)]
    direct_removals: usize,
}

impl<M> Default for WaiterQueue<M> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            ids: PoisonedCounter::new(),
            #[cfg(test)]
            direct_removals: 0,
        }
    }
}

impl<M> WaiterQueue<M> {
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn push_back(&mut self, operation: Arc<SendOperation<M>>) -> WaiterId {
        let next = WaiterId(
            self.ids
                .mint()
                .expect("mailbox waiter identity space exhausted"),
        );
        let replaced = self.entries.insert(next, operation);
        debug_assert!(replaced.is_none());
        next
    }

    fn park(&mut self, operation: &Arc<SendOperation<M>>) {
        let registration = self.push_back(Arc::clone(operation));
        operation.register(registration);
    }

    fn observe_all(&self, incarnation: Incarnation) {
        for operation in self.entries.values() {
            operation.observe(incarnation);
        }
    }

    fn pop_front(&mut self) -> Option<(WaiterId, Arc<SendOperation<M>>)> {
        let entry = self.entries.pop_first();
        #[cfg(test)]
        if entry.is_some() {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        entry
    }

    fn remove(&mut self, id: WaiterId) -> Option<Arc<SendOperation<M>>> {
        let operation = self.entries.remove(&id)?;
        #[cfg(test)]
        {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        Some(operation)
    }
}

/// A completed locked acceptance whose user payload must be destroyed only
/// after the mailbox mutex is released.
#[must_use = "finish accepted delivery after releasing the mailbox lock"]
struct Acceptance<M> {
    incarnation: Incarnation,
    displaced: Option<Envelope<M>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RejectionReason {
    NotAccepting,
    Unconfigured,
    Full,
}

struct Rejected<M> {
    message: M,
    reason: RejectionReason,
}

impl<M> Acceptance<M> {
    fn finish(self, changed: &Signal) -> Incarnation {
        changed.pulse();
        drop(self.displaced);
        self.incarnation
    }
}

struct Promotion<M: Send + 'static> {
    displaced: Vec<Envelope<M>>,
    wakers: Vec<Waker>,
}

impl<M: Send + 'static> Default for Promotion<M> {
    fn default() -> Self {
        Self {
            displaced: Vec::new(),
            wakers: Vec::new(),
        }
    }
}

impl<M: Send + 'static> Promotion<M> {
    // Drop is the completion path on every ownership edge. `finish` only
    // names the intentional consume point; its Drop still wakes all accepted
    // senders and panic-contains each displaced-message destructor inline.
    fn finish(self) {}

    fn finish_isolated(mut self) {
        let displaced = std::mem::take(&mut self.displaced);
        if !displaced.is_empty() {
            crate::runtime::dispose_detached(MailboxPayload {
                queue: Some(displaced.into()),
                latest: None,
                retired: Vec::new(),
            });
        }
        self.finish();
    }
}

impl<M: Send + 'static> Drop for Promotion<M> {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        for waker in self.wakers.drain(..) {
            panics.run(|| waker.wake());
        }
        for displaced in self.displaced.drain(..) {
            panics.run(|| drop(displaced));
        }
    }
}

struct Termination<M> {
    waiters: WaiterQueue<M>,
    final_incarnation: Option<Incarnation>,
}

impl<M> Termination<M> {
    fn finish(&mut self, retired: &mut Vec<Arc<SendOperation<M>>>) -> Option<PanicPayload> {
        let mut panics = PanicAccumulator::default();
        let final_incarnation = self.final_incarnation;
        while let Some((registration, waiter)) = self.waiters.pop_front() {
            waiter.clear_registration(registration);
            panics.run(|| {
                waiter.terminate(final_incarnation);
            });
            // A withdrawn sender may leave this as the final operation owner,
            // so retain it for the same isolated path as unread messages.
            retired.push(waiter);
        }
        panics.take()
    }
}

struct MailboxPayload<M> {
    queue: Option<VecDeque<Envelope<M>>>,
    latest: Option<Envelope<M>>,
    retired: Vec<Arc<SendOperation<M>>>,
}

impl<M> Drop for MailboxPayload<M> {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        if let Some(mut queue) = self.queue.take() {
            while let Some(envelope) = queue.pop_front() {
                panics.run(|| drop(envelope));
            }
        }
        if let Some(latest) = self.latest.take() {
            panics.run(|| drop(latest));
        }
        for waiter in self.retired.drain(..) {
            panics.run(|| drop(waiter));
        }
    }
}

struct MailboxTeardown<M: Send + 'static> {
    changed: Option<Signal>,
    payload: crate::runtime::Isolated<MailboxPayload<M>>,
    termination: Option<Termination<M>>,
}

impl<M: Send + 'static> MailboxTeardown<M> {
    fn finish_framework(&mut self) -> Option<PanicPayload> {
        let mut panics = PanicAccumulator::default();
        if let Some(changed) = self.changed.take() {
            panics.run(|| changed.pulse());
        }
        if let Some(mut termination) = self.termination.take() {
            panics.record(termination.finish(&mut self.payload.get_mut().retired));
        }
        panics.take()
    }
}

impl<M: Send + 'static> MailboxTermination for MailboxTeardown<M> {
    fn finish(mut self: Box<Self>) -> Option<MailboxDisposal> {
        let panic = self.finish_framework();
        let payload = self
            .payload
            .take()
            .map(|payload| Box::new(payload) as MailboxDisposal);
        if let Some(panic) = panic {
            if let Some(payload) = payload {
                crate::runtime::dispose_detached(payload);
            }
            resume_panic(panic);
        }
        payload
    }
}

impl<M: Send + 'static> Drop for MailboxTeardown<M> {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        panics.record(self.finish_framework());
        if let Some(payload) = self.payload.take() {
            crate::runtime::dispose_detached(payload);
        }
    }
}

pub(crate) struct MailboxCell<M> {
    pub(super) actor_id: ChildId,
    pub(super) state: Mutex<MailboxState<M>>,
    accepted: AtomicPoisonedCounter,
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
                binding: MailboxBinding::Unbound(WaiterQueue::default()),
                last_bound: None,
                queue: VecDeque::new(),
                latest: None,
            }),
            accepted: AtomicPoisonedCounter::new(),
            changed: Signal::default(),
        })
    }

    pub(super) fn submit(&self, message: M) -> Submission<M> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.status() {
            BindingStatus::Terminal(final_incarnation) => Submission::Terminated {
                message,
                final_incarnation,
            },
            BindingStatus::Bound(incarnation) => {
                match accept_locked(&mut state, message, &self.accepted) {
                    Ok(acceptance) => {
                        drop(state);
                        Submission::Accepted(acceptance.finish(&self.changed))
                    }
                    Err(Rejected { message, reason }) => {
                        if !matches!(reason, RejectionReason::Full) {
                            // `Full` is the only rejection a bound, configured
                            // mailbox can produce. Follow `bind`'s convention
                            // and release the lock before panicking, so an
                            // invariant break stays on this thread instead of
                            // poisoning the mutex under every other sender --
                            // and so the rejected payload's destructor does not
                            // run under it either.
                            drop(state);
                            drop(message);
                            unreachable!("a bound configured mailbox rejected as {reason:?}");
                        }
                        let operation = SendOperation::new(message);
                        operation.observe(incarnation);
                        state.park(&operation);
                        Submission::Parked(operation)
                    }
                }
            }
            BindingStatus::Frozen(incarnation) => {
                let operation = SendOperation::new(message);
                operation.observe(incarnation);
                state.park(&operation);
                Submission::Parked(operation)
            }
            BindingStatus::Unbound => {
                let operation = SendOperation::new(message);
                state.park(&operation);
                Submission::Parked(operation)
            }
        }
    }

    pub(super) fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.status() {
            BindingStatus::Terminal(final_incarnation) => Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: final_incarnation,
                message,
                kind: SendErrorKind::Terminated,
            }),
            BindingStatus::Unbound => Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: None,
                message,
                kind: SendErrorKind::NotRunning,
            }),
            BindingStatus::Frozen(incarnation) => Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: Some(incarnation),
                message,
                kind: SendErrorKind::NotRunning,
            }),
            BindingStatus::Bound(incarnation) => {
                match accept_locked(&mut state, message, &self.accepted) {
                    Ok(acceptance) => {
                        drop(state);
                        Ok(acceptance.finish(&self.changed))
                    }
                    Err(Rejected { message, reason }) => Err(SendError {
                        actor_id: self.actor_id.clone(),
                        incarnation_observed: match reason {
                            RejectionReason::Full | RejectionReason::NotAccepting => {
                                Some(incarnation)
                            }
                            RejectionReason::Unconfigured => None,
                        },
                        message,
                        kind: match reason {
                            RejectionReason::Full => SendErrorKind::Full,
                            RejectionReason::Unconfigured | RejectionReason::NotAccepting => {
                                SendErrorKind::NotRunning
                            }
                        },
                    }),
                }
            }
        }
    }

    fn receive(
        &self,
        incarnation: Incarnation,
        mode: ReceiveMode,
        accepted_through: Option<AcceptedSequence>,
    ) -> Option<M> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        let eligible = match state.status() {
            BindingStatus::Bound(current) => current == incarnation,
            BindingStatus::Frozen(current) => {
                mode == ReceiveMode::IncludeFrozen && current == incarnation
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => false,
        };
        if !eligible {
            return None;
        }
        let message = match state.kind {
            Some(MailboxKind::Queue(_)) => {
                let eligible = state.queue.front().is_some_and(|item| {
                    accepted_through.is_none_or(|limit| item.accepted_sequence <= limit)
                });
                eligible
                    .then(|| state.queue.pop_front())
                    .flatten()
                    .map(|item| item.message)
            }
            Some(MailboxKind::Latest) => {
                let eligible = state.latest.as_ref().is_some_and(|item| {
                    accepted_through.is_none_or(|limit| item.accepted_sequence <= limit)
                });
                eligible
                    .then(|| state.latest.take())
                    .flatten()
                    .map(|item| item.message)
            }
            None => None,
        };
        let promotion = if message.is_some() && matches!(state.status(), BindingStatus::Bound(_)) {
            promote_waiters(&mut state, &self.accepted)
        } else {
            Promotion::default()
        };
        drop(state);
        if message.is_some() {
            self.changed.pulse();
        }
        promotion.finish();
        message
    }

    pub(super) fn current_observation(&self) -> Option<Incarnation> {
        match self.state.lock().expect("mailbox mutex poisoned").status() {
            BindingStatus::Bound(incarnation) | BindingStatus::Frozen(incarnation) => {
                Some(incarnation)
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => None,
        }
    }

    fn watcher(&self) -> SignalWatcher {
        self.changed.watcher()
    }

    fn accepted_sequence(&self) -> AcceptedSequence {
        AcceptedSequence(self.accepted.load(Ordering::Acquire))
    }
}

impl<M> MailboxCell<M> {
    pub(super) fn withdraw(&self, operation: &Arc<SendOperation<M>>) -> Withdrawal<M> {
        let mut mailbox = self.state.lock().expect("mailbox mutex poisoned");
        let current_observation = match mailbox.status() {
            BindingStatus::Bound(incarnation) | BindingStatus::Frozen(incarnation) => {
                Some(incarnation)
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => None,
        };
        let (result, registration) = {
            let mut state = operation
                .state
                .lock()
                .expect("send operation mutex poisoned");
            match &mut state.outcome {
                OperationOutcome::Waiting {
                    message,
                    newest_observed,
                } => {
                    let message = message
                        .take()
                        .expect("a waiting operation must retain its message");
                    // The mailbox lock makes this one evidence snapshot: a
                    // binding either precedes withdrawal and contributes its
                    // incarnation, or follows the completed withdrawal. This
                    // also covers an operation first submitted by an elapsed
                    // (including zero-duration) deadline poll.
                    let observed = (*newest_observed).or(current_observation);
                    state.outcome = OperationOutcome::Withdrawn;
                    state.waker = None;
                    (
                        Withdrawal::Withdrawn { message, observed },
                        state.registration.take(),
                    )
                }
                OperationOutcome::Accepted(incarnation) => (
                    Withdrawal::Accepted(*incarnation),
                    state.registration.take(),
                ),
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => (
                    Withdrawal::Terminated {
                        message: message
                            .take()
                            .expect("a terminal operation must retain its message"),
                        observed: *final_incarnation,
                    },
                    state.registration.take(),
                ),
                OperationOutcome::Withdrawn => {
                    panic!("a send operation was withdrawn more than once")
                }
            }
        };
        if let Some(registration) = registration {
            if let Some(removed) = mailbox.remove_waiter(registration) {
                debug_assert!(Arc::ptr_eq(&removed, operation));
            } else {
                debug_assert!(matches!(mailbox.status(), BindingStatus::Terminal(_)));
            }
        }
        result
    }
}

impl<M: Send + 'static> MailboxControl for MailboxCell<M> {
    fn configure(&self, mailbox: ResolvedMailbox) {
        let kind = match mailbox {
            ResolvedMailbox::Queue(capacity) => MailboxKind::Queue(capacity),
            ResolvedMailbox::Latest => MailboxKind::Latest,
        };
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.kind {
            Some(existing) => debug_assert_eq!(existing, kind),
            None => state.kind = Some(kind),
        }
    }

    fn bind(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if matches!(state.status(), BindingStatus::Terminal(_)) {
            return;
        }
        // Driver-contract violations release the lock before panicking so the
        // failure stays on the driver thread instead of poisoning the mutex
        // under every sender.
        let Some(kind) = state.kind else {
            drop(state);
            panic!("mailbox must be configured before its first bind");
        };
        if !matches!(state.status(), BindingStatus::Unbound) {
            drop(state);
            panic!("mailbox must close the prior incarnation before rebinding");
        }
        let mut waiters = state.take_waiters();
        state.last_bound = Some(incarnation);
        // Binding is an observation edge for every operation that remained
        // parked through it, including FIFO overflow that cannot be promoted
        // into the current capacity. Withdrawal takes the mailbox lock before
        // the operation lock, so a concurrent timeout sees either the prior
        // evidence or this incarnation consistently with which edge won.
        waiters.observe_all(incarnation);
        let promotion = {
            let MailboxState { queue, latest, .. } = &mut *state;
            promote_waiter_queue(
                kind,
                incarnation,
                &mut waiters,
                queue,
                latest,
                &self.accepted,
            )
        };
        if waiters.is_empty() {
            state.binding = MailboxBinding::Bound(BoundState::Available(incarnation));
        } else {
            let MailboxKind::Queue(capacity) = kind else {
                unreachable!("a latest mailbox finishes all waiting submissions")
            };
            debug_assert_eq!(state.queue.len(), capacity.get());
            state.binding = MailboxBinding::Bound(BoundState::Full {
                incarnation,
                waiters,
            });
        }
        drop(state);
        self.changed.pulse();
        promotion.finish_isolated();
    }

    fn freeze(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if state.status() != BindingStatus::Bound(incarnation) {
            return;
        }
        let waiters = state.take_waiters();
        state.binding = MailboxBinding::Frozen {
            incarnation,
            waiters,
        };
        drop(state);
        self.changed.pulse();
    }

    fn close(&self, incarnation: Incarnation) -> Option<MailboxDisposal> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if !matches!(
            state.status(),
            BindingStatus::Bound(current) | BindingStatus::Frozen(current)
                if current == incarnation
        ) {
            return None;
        }
        let waiters = state.take_waiters();
        state.binding = MailboxBinding::Unbound(waiters);
        let queue = std::mem::take(&mut state.queue);
        let latest = state.latest.take();
        drop(state);
        self.changed.pulse();
        Some(Box::new(MailboxPayload {
            queue: Some(queue),
            latest,
            retired: Vec::new(),
        }) as MailboxDisposal)
    }

    fn prepare_termination(&self) -> Option<Box<dyn MailboxTermination>> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if matches!(state.status(), BindingStatus::Terminal(_)) {
            return None;
        }
        let final_incarnation = state.last_bound;
        let waiters = state.take_waiters();
        state.binding = MailboxBinding::Terminal(final_incarnation);
        let queue = std::mem::take(&mut state.queue);
        let latest = state.latest.take();
        let termination = Termination {
            waiters,
            final_incarnation,
        };
        drop(state);
        Some(Box::new(MailboxTeardown {
            changed: Some(self.changed.clone()),
            payload: crate::runtime::Isolated::new(MailboxPayload {
                queue: Some(queue),
                latest,
                retired: Vec::new(),
            }),
            termination: Some(termination),
        }))
    }

    #[cfg(debug_assertions)]
    fn bind_order_valid(&self) -> bool {
        let state = self.state.lock().expect("mailbox mutex poisoned");
        state.kind.is_some()
            && matches!(
                state.status(),
                BindingStatus::Unbound | BindingStatus::Terminal(_)
            )
    }
}

fn mint_accepted_sequence(accepted: &AtomicPoisonedCounter) -> AcceptedSequence {
    AcceptedSequence(
        accepted
            .mint(Ordering::Release, Ordering::Relaxed)
            .expect("mailbox accepted-sequence space exhausted"),
    )
}

fn accept_locked<M>(
    state: &mut MailboxState<M>,
    message: M,
    accepted: &AtomicPoisonedCounter,
) -> Result<Acceptance<M>, Rejected<M>> {
    let incarnation = match &state.binding {
        MailboxBinding::Bound(BoundState::Available(incarnation)) => *incarnation,
        MailboxBinding::Bound(BoundState::Full { .. }) => {
            return Err(Rejected {
                message,
                reason: RejectionReason::Full,
            });
        }
        MailboxBinding::Unbound(_)
        | MailboxBinding::Frozen { .. }
        | MailboxBinding::Terminal(_) => {
            return Err(Rejected {
                message,
                reason: RejectionReason::NotAccepting,
            });
        }
    };
    let kind = match state.kind {
        Some(MailboxKind::Queue(capacity)) if state.queue.len() < capacity.get() => {
            MailboxKind::Queue(capacity)
        }
        Some(MailboxKind::Latest) => MailboxKind::Latest,
        Some(MailboxKind::Queue(_)) => {
            return Err(Rejected {
                message,
                reason: RejectionReason::Full,
            });
        }
        None => {
            return Err(Rejected {
                message,
                reason: RejectionReason::Unconfigured,
            });
        }
    };
    let accepted_sequence = mint_accepted_sequence(accepted);
    let displaced = match kind {
        MailboxKind::Queue(_) => {
            state.queue.push_back(Envelope {
                message,
                accepted_sequence,
            });
            None
        }
        MailboxKind::Latest => state.latest.replace(Envelope {
            message,
            accepted_sequence,
        }),
    };
    Ok(Acceptance {
        incarnation,
        displaced,
    })
}

fn promote_waiters<M: Send + 'static>(
    state: &mut MailboxState<M>,
    accepted_sequence: &AtomicPoisonedCounter,
) -> Promotion<M> {
    let Some(kind) = state.kind else {
        return Promotion::default();
    };
    let MailboxBinding::Bound(BoundState::Full {
        incarnation,
        waiters,
    }) = &mut state.binding
    else {
        return Promotion::default();
    };
    let incarnation = *incarnation;
    let promotion = promote_waiter_queue(
        kind,
        incarnation,
        waiters,
        &mut state.queue,
        &mut state.latest,
        accepted_sequence,
    );
    if waiters.is_empty() {
        state.binding = MailboxBinding::Bound(BoundState::Available(incarnation));
    }
    promotion
}

fn promote_waiter_queue<M: Send + 'static>(
    kind: MailboxKind,
    incarnation: Incarnation,
    waiters: &mut WaiterQueue<M>,
    queue: &mut VecDeque<Envelope<M>>,
    latest: &mut Option<Envelope<M>>,
    accepted_sequence: &AtomicPoisonedCounter,
) -> Promotion<M> {
    let mut promotion = Promotion::default();
    let available = match kind {
        MailboxKind::Queue(capacity) => capacity.get().saturating_sub(queue.len()),
        MailboxKind::Latest => usize::MAX,
    };
    let mut accepted = 0usize;
    while accepted < available {
        let Some((registration, operation)) = waiters.pop_front() else {
            break;
        };
        operation.clear_registration(registration);
        operation.observe(incarnation);
        let Some((message, wake)) = operation.accept(incarnation) else {
            continue;
        };
        if let Some(waker) = wake {
            promotion.wakers.push(waker);
        }
        let accepted_sequence = mint_accepted_sequence(accepted_sequence);
        match kind {
            MailboxKind::Queue(_) => queue.push_back(Envelope {
                message,
                accepted_sequence,
            }),
            MailboxKind::Latest => {
                if let Some(displaced) = latest.replace(Envelope {
                    message,
                    accepted_sequence,
                }) {
                    promotion.displaced.push(displaced);
                }
            }
        }
        accepted += 1;
    }
    promotion
}

pub(super) enum Withdrawal<M> {
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
        self.mailbox
            .receive(self.incarnation, ReceiveMode::IncludeFrozen, None)
    }

    pub(crate) fn try_recv_live_through(&self, accepted_sequence: AcceptedSequence) -> Option<M> {
        self.mailbox.receive(
            self.incarnation,
            ReceiveMode::LiveOnly,
            Some(accepted_sequence),
        )
    }

    pub(crate) fn accepted_sequence(&self) -> AcceptedSequence {
        self.mailbox.accepted_sequence()
    }

    pub(crate) fn freeze(&self) {
        self.mailbox.freeze(self.incarnation);
    }

    pub(crate) async fn changed(&mut self) {
        self.watcher.changed().await;
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    use crate::{
        ChildId, SendErrorKind,
        cells::{MailboxControl, MemberCell},
        identity::ScopeIdentity,
        mailbox::ActorRef,
        policy::{ResolvedDefaults, ResolvedMailbox},
    };

    use super::MailboxCell;

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("injected waker panic");
        }
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub(in crate::mailbox) fn actor() -> (Arc<MailboxCell<u8>>, ActorRef<u8>) {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("actor");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        member.attach_mailbox(mailbox.clone());
        (Arc::clone(&mailbox), ActorRef::new(member, mailbox))
    }
    fn park_with(future: &mut std::pin::Pin<Box<crate::mailbox::SendFuture<u8>>>, waker: &Waker) {
        let mut context = Context::from_waker(waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    #[should_panic(expected = "mailbox must be configured before its first bind")]
    fn binding_before_configuration_trips_the_driver_contract() {
        let (mailbox, _) = actor();
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        let incarnation = incarnations.mint().expect("incarnation available");

        MailboxControl::bind(&*mailbox, incarnation);
    }

    #[test]
    #[should_panic(expected = "mailbox must close the prior incarnation before rebinding")]
    fn rebinding_before_close_trips_the_driver_contract() {
        let (mailbox, _) = actor();
        MailboxControl::configure(&*mailbox, ResolvedDefaults::default().mailbox);
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        let first = incarnations.mint().expect("first incarnation available");
        let second = incarnations.mint().expect("second incarnation available");

        MailboxControl::bind(&*mailbox, first);
        MailboxControl::bind(&*mailbox, second);
    }

    #[test]
    fn bound_waiters_exist_only_in_the_full_state() {
        let (mailbox, _) = actor();
        MailboxControl::configure(
            &*mailbox,
            ResolvedMailbox::Queue(
                std::num::NonZeroUsize::new(1).expect("non-zero queue capacity"),
            ),
        );
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        let incarnation = incarnations.mint().expect("incarnation available");
        MailboxControl::bind(&*mailbox, incarnation);

        assert!(matches!(
            mailbox.submit(1),
            super::Submission::Accepted(bound) if bound == incarnation
        ));
        let operation = match mailbox.submit(2) {
            super::Submission::Parked(operation) => operation,
            super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
                panic!("a sender parks behind the full queue")
            }
        };
        assert!(matches!(
            &mailbox.state.lock().expect("mailbox mutex poisoned").binding,
            super::MailboxBinding::Bound(super::BoundState::Full { waiters, .. })
                if !waiters.is_empty()
        ));

        assert!(matches!(
            mailbox.withdraw(&operation),
            super::Withdrawal::Withdrawn { message: 2, .. }
        ));
        assert!(matches!(
            mailbox.state.lock().expect("mailbox mutex poisoned").binding,
            super::MailboxBinding::Bound(super::BoundState::Available(bound))
                if bound == incarnation
        ));
    }

    #[test]
    fn cancelling_many_parked_sends_unlinks_one_registration_each() {
        const SENDS: usize = 16_384;

        let (mailbox, actor) = actor();
        let mut sends = Vec::with_capacity(SENDS);
        for _ in 0..SENDS {
            let mut send = Box::pin(actor.send(1));
            park_with(&mut send, Waker::noop());
            sends.push(Some(send));
        }
        assert_eq!(
            mailbox
                .state
                .lock()
                .expect("mailbox mutex poisoned")
                .waiters()
                .expect("an unbound mailbox owns its parked waiters")
                .len(),
            SENDS
        );

        // Exercise one interior, the head, and the tail explicitly, then a
        // deterministic odd/even permutation. The counter measures queue
        // operations rather than elapsed time or scheduler behavior.
        for index in [SENDS / 2, 0, SENDS - 1] {
            drop(sends[index].take().expect("selected send remains live"));
        }
        for index in (1..SENDS - 1).step_by(2) {
            if let Some(send) = sends[index].take() {
                drop(send);
            }
        }
        for send in sends.into_iter().flatten() {
            drop(send);
        }

        let state = mailbox.state.lock().expect("mailbox mutex poisoned");
        let waiters = state
            .waiters()
            .expect("an unbound mailbox retains its empty waiter queue");
        assert!(waiters.is_empty());
        assert_eq!(
            waiters.direct_removals, SENDS,
            "mass cancellation must do one direct unlink per parked send"
        );
    }

    #[test]
    fn promotion_wakes_every_sender_when_multiple_wakers_panic() {
        let (mailbox, actor) = actor();
        let mut first = Box::pin(actor.send(1));
        let mut second = Box::pin(actor.send(2));
        let mut third = Box::pin(actor.send(3));
        let first_panicking = Waker::from(Arc::new(PanicWake));
        let second_panicking = Waker::from(Arc::new(PanicWake));
        let wakes = Arc::new(AtomicUsize::new(0));
        let counting = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        park_with(&mut first, &first_panicking);
        park_with(&mut second, &second_panicking);
        park_with(&mut third, &counting);
        MailboxControl::configure(&*mailbox, ResolvedDefaults::default().mailbox);
        let mut generations = {
            let mut identity = ScopeIdentity::new();
            let (_, generations) = identity
                .mint_membership(&ChildId::from("actor"))
                .expect("membership available")
                .into_pair();
            generations
        };
        let incarnation = generations.mint().expect("incarnation available");

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                MailboxControl::bind(&*mailbox, incarnation);
            }))
            .is_err()
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            first.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
        assert!(matches!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
        assert!(matches!(
            third.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
    }

    #[test]
    fn termination_discharge_reaches_every_sender_when_multiple_wakers_panic() {
        let (mailbox, actor) = actor();
        let mut first = Box::pin(actor.send(1));
        let mut second = Box::pin(actor.send(2));
        let mut third = Box::pin(actor.send(3));
        let first_panicking = Waker::from(Arc::new(PanicWake));
        let second_panicking = Waker::from(Arc::new(PanicWake));
        let wakes = Arc::new(AtomicUsize::new(0));
        let counting = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        park_with(&mut first, &first_panicking);
        park_with(&mut second, &second_panicking);
        park_with(&mut third, &counting);

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                drop(MailboxControl::prepare_termination(&*mailbox));
            }))
            .is_err()
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        for future in [&mut first, &mut second, &mut third] {
            let Poll::Ready(Err(error)) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("every parked send must be discharged");
            };
            assert_eq!(error.kind, SendErrorKind::Terminated);
        }
    }

    #[test]
    fn cancellation_can_race_detached_terminal_teardown() {
        let (mailbox, actor) = actor();
        let mut send = Box::pin(actor.send(1));
        park_with(&mut send, Waker::noop());

        let teardown = MailboxControl::prepare_termination(&*mailbox)
            .expect("live mailbox prepares terminal teardown");
        // Teardown is deliberately retained: cancellation sees terminal state
        // after the waiter queue was detached but before it was discharged.
        drop(send);
        drop(teardown);

        assert!(
            mailbox
                .state
                .lock()
                .expect("mailbox mutex remains healthy")
                .waiters()
                .is_none()
        );
    }

    #[test]
    fn stale_waiter_id_cannot_unlink_a_later_registration() {
        let mut waiters = super::WaiterQueue::default();
        let first = super::SendOperation::new(1_u8);
        let first_id = waiters.push_back(Arc::clone(&first));
        first.register(first_id);
        let removed = waiters.remove(first_id).expect("first waiter is live");
        removed.clear_registration(first_id);

        let second = super::SendOperation::new(2_u8);
        let second_id = waiters.push_back(Arc::clone(&second));
        second.register(second_id);
        assert_ne!(first_id, second_id, "waiter identities are never reused");
        assert!(
            waiters.remove(first_id).is_none(),
            "a stale cancellation cannot unlink a later waiter"
        );
        let removed = waiters
            .remove(second_id)
            .expect("second waiter remains live");
        assert!(Arc::ptr_eq(&removed, &second));
        removed.clear_registration(second_id);
        assert!(waiters.is_empty());
    }

    #[test]
    fn waiter_queue_preserves_fifo_across_removal() {
        let mut waiters = super::WaiterQueue::default();
        let first = super::SendOperation::new(1_u8);
        let second = super::SendOperation::new(2_u8);
        let third = super::SendOperation::new(3_u8);
        let first_id = waiters.push_back(Arc::clone(&first));
        let second_id = waiters.push_back(Arc::clone(&second));
        let third_id = waiters.push_back(Arc::clone(&third));

        assert!(Arc::ptr_eq(
            &waiters.remove(second_id).expect("middle waiter is live"),
            &second
        ));
        let (popped_first, operation) = waiters.pop_front().expect("head remains live");
        assert_eq!(popped_first, first_id);
        assert!(Arc::ptr_eq(&operation, &first));
        let (popped_third, operation) = waiters.pop_front().expect("tail remains live");
        assert_eq!(popped_third, third_id);
        assert!(Arc::ptr_eq(&operation, &third));
        assert!(waiters.is_empty());
    }

    #[test]
    fn waiter_identity_exhaustion_poison_is_never_minted() {
        let mut waiters = super::WaiterQueue {
            ids: crate::identity::PoisonedCounter::near_exhaustion(),
            ..super::WaiterQueue::default()
        };
        let last = waiters.push_back(super::SendOperation::new(1_u8));
        assert_eq!(last, super::WaiterId(u64::MAX - 1));

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                waiters.push_back(super::SendOperation::new(2_u8));
            }))
            .is_err(),
            "the poison key is never minted"
        );
        assert!(waiters.ids.is_poisoned());
        assert!(!waiters.entries.contains_key(&super::WaiterId::POISON));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                waiters.push_back(super::SendOperation::new(3_u8));
            }))
            .is_err(),
            "the exhausted domain stays poisoned"
        );
    }

    #[test]
    fn accepted_sequence_exhaustion_poison_is_never_minted() {
        let accepted = crate::identity::AtomicPoisonedCounter::near_exhaustion();
        assert_eq!(
            super::mint_accepted_sequence(&accepted),
            super::AcceptedSequence(u64::MAX - 1)
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                super::mint_accepted_sequence(&accepted);
            }))
            .is_err()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                super::mint_accepted_sequence(&accepted);
            }))
            .is_err(),
            "the accepted-sequence domain stays poisoned"
        );
    }
}
