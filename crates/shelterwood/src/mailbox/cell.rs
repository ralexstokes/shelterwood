use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    num::NonZeroUsize,
    ops::Deref,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::{
    identity::{AtomicPoisonedCounter, PoisonedCounter},
    mailbox::{
        ChildId, Incarnation, MailboxBindToken, MailboxClose, MailboxControl, MailboxDisposal,
        MailboxEffectQueue, MailboxEffectSink, MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
        MailboxTermination,
        capability::{dispose, dispose_value},
        panic::{PanicAccumulator, PanicPayload, resume_panic},
    },
    policy::ResolvedMailbox,
};
use shelterwood_core::waker::{WakerAction, WakerEffects, WakerSlot};

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

/// The structurally valid receive domains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiveMode {
    Drain,
    LiveThrough(AcceptedSequence),
}

impl ReceiveMode {
    fn accepts(self, sequence: AcceptedSequence) -> bool {
        match self {
            Self::Drain => true,
            Self::LiveThrough(limit) => sequence <= limit,
        }
    }
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
    waker: WakerSlot,
    registration: Option<WaiterId>,
}

pub(super) struct SendOperation<M> {
    pub(super) state: Mutex<OperationState<M>>,
}

pub(super) enum OperationPoll<M> {
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
    fn new(message: M) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OperationState {
                outcome: OperationOutcome::Waiting {
                    message: Some(message),
                    newest_observed: None,
                },
                waker: WakerSlot::default(),
                registration: None,
            }),
        })
    }

    fn register(&self, registration: WaiterId) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        // Diagnostic-only under the send-operation mutex: a fresh operation
        // is parked once and remains Waiting until the outer mailbox edge
        // completes. Reachable behavior therefore does not depend on either
        // check, and no test expects these diagnostics to panic.
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
            // Diagnostic-only: that race leaves `None`; any other identity is
            // preserved by the total fallback. No test expects this panic.
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

    fn accept(&self, incarnation: Incarnation, effects: &mut WakerEffects) -> Option<M> {
        {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return None;
            };
            let message = message.take()?;
            state.outcome = OperationOutcome::Accepted(incarnation);
            state.waker.take(WakerAction::Wake, effects);
            Some(message)
        }
    }

    fn terminate(&self, final_incarnation: Option<Incarnation>) {
        let mut effects = WakerEffects::default();
        {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return;
            };
            let message = message.take();
            state.outcome = OperationOutcome::Terminated {
                message,
                final_incarnation,
            };
            state.waker.take(WakerAction::Wake, &mut effects);
        }
    }

    pub(super) fn poll(
        &self,
        replacement: Option<std::task::Waker>,
        current: &std::task::Waker,
    ) -> OperationPoll<M> {
        // Effects precede the guard, so even an unwind drops the guard before
        // invoking a displaced RawWaker vtable.
        let mut effects = WakerEffects::default();
        let result = {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            match &mut state.outcome {
                OperationOutcome::Accepted(incarnation) => {
                    Ok(OperationPoll::Accepted(*incarnation))
                }
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => message.take().map_or_else(
                    || Err("a terminal operation retains its message until observed"),
                    |message| {
                        Ok(OperationPoll::Terminated {
                            message,
                            final_incarnation: *final_incarnation,
                        })
                    },
                ),
                OperationOutcome::Waiting { .. } => {
                    if let Some(replacement) = replacement {
                        state.waker.replace(replacement, &mut effects);
                        Ok(OperationPoll::Pending)
                    } else if state.waker.will_wake(current) {
                        Ok(OperationPoll::Pending)
                    } else {
                        Ok(OperationPoll::NeedsWakerClone)
                    }
                }
                OperationOutcome::Withdrawn => Err("a withdrawn send future was polled"),
            }
        };
        drop(effects);
        match result {
            Ok(result) => result,
            Err(message) => panic!("{message}"),
        }
    }

    #[cfg(test)]
    fn install_test_waker(&self, waker: std::task::Waker) {
        let mut effects = WakerEffects::default();
        {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            state.waker.replace(waker, &mut effects);
        }
    }
}

pub(super) struct MailboxState<M> {
    kind: Option<MailboxKind>,
    bind_permit: Arc<AtomicBool>,
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

    fn current_observation(&self) -> Option<Incarnation> {
        match &self.binding {
            MailboxBinding::Bound(bound) => Some(bound.incarnation()),
            MailboxBinding::Frozen { incarnation, .. } => Some(*incarnation),
            MailboxBinding::Unbound(_) | MailboxBinding::Terminal(_) => None,
        }
    }

    /// Replaces one binding only after its waiter identity domain is empty.
    ///
    /// The state-level operation returns a rejected replacement because every
    /// caller holds the mailbox mutex. `MailboxTxn` transfers that binding to
    /// its effects, then raises the invariant panic only after unlock. Thus a
    /// live `WaiterQueue` and its `Arc<SendOperation<M>>`s can never be
    /// destroyed in the critical section.
    ///
    /// At every waiter-carrying call site rejection is unreachable by
    /// construction: `take_waiters` moves the parked senders out of the old
    /// binding into the replacement first, so the domain this check reads is
    /// already empty. The remaining callers pass an `Available` or empty
    /// binding.
    fn replace_binding(&mut self, replacement: MailboxBinding<M>) -> Result<(), MailboxBinding<M>> {
        let replaceable = match &self.binding {
            MailboxBinding::Unbound(waiters)
            | MailboxBinding::Frozen { waiters, .. }
            | MailboxBinding::Bound(BoundState::Full { waiters, .. }) => waiters.is_empty(),
            MailboxBinding::Bound(BoundState::Available(_)) => true,
            MailboxBinding::Terminal(_) => false,
        };
        if !replaceable {
            return Err(replacement);
        }
        self.binding = replacement;
        Ok(())
    }

    fn park(&mut self, operation: &Arc<SendOperation<M>>) -> bool {
        match &mut self.binding {
            MailboxBinding::Unbound(waiters)
            | MailboxBinding::Frozen { waiters, .. }
            | MailboxBinding::Bound(BoundState::Full { waiters, .. }) => {
                return waiters.park(operation);
            }
            MailboxBinding::Bound(BoundState::Available(incarnation)) => {
                let Some(MailboxKind::Queue(capacity)) = self.kind else {
                    unreachable!("only a capacity-bound queue can park while bound")
                };
                // Diagnostic-only under the mailbox mutex: `Available` parks
                // only after the accepting transition filled this configured
                // queue, or after that transition's own incarnation-mismatch
                // fallback, which has already recorded its invariant panic on
                // the effects sink. The Full transition remains total without
                // this check, and no test expects the diagnostic panic.
                debug_assert_eq!(self.queue.len(), capacity.get());
                let incarnation = *incarnation;
                let mut waiters = WaiterQueue::default();
                if !waiters.park(operation) {
                    return false;
                }
                // This arm just matched `Available`, so no waiter identity
                // domain can be displaced by the direct replacement.
                self.binding = MailboxBinding::Bound(BoundState::Full {
                    incarnation,
                    waiters,
                });
            }
            MailboxBinding::Terminal(_) => {
                unreachable!("terminal submissions return their payload directly")
            }
        }
        true
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
                // with no way to release it first, so this is diagnostic-only
                // rather than an always-on panic. Returning an empty queue is
                // the total behavior, and no test expects this diagnostic.
                debug_assert!(false, "a terminal mailbox has no live transition");
                WaiterQueue::default()
            }
        }
    }

    /// Dequeues the next envelope this receive mode is willing to observe.
    fn take_next(&mut self, mode: ReceiveMode) -> Option<Envelope<M>> {
        match self.kind {
            Some(MailboxKind::Queue(_)) => self
                .queue
                .front()
                .is_some_and(|item| mode.accepts(item.accepted_sequence))
                .then(|| self.queue.pop_front())
                .flatten(),
            Some(MailboxKind::Latest) => self
                .latest
                .as_ref()
                .is_some_and(|item| mode.accepts(item.accepted_sequence))
                .then(|| self.latest.take())
                .flatten(),
            None => None,
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

enum SubmitTransition<M> {
    Complete(Submission<M>),
    WaiterIdentityExhausted(Arc<SendOperation<M>>),
    AcceptedSequenceExhausted(M),
}

enum AcceptTransition<M> {
    Accepted(Incarnation),
    Full(M),
    Exhausted(M),
}

enum TrySendTransition<M> {
    Complete(Result<Incarnation, SendError<M>>),
    AcceptedSequenceExhausted(M),
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
/// waiter. Keys are never reused within one queue instance;
/// `MailboxState::replace_binding` permits replacing a queue only after it is
/// empty, so no registration outlives its queue and stale cancellation ids
/// remain harmless. Terminalization detaches the live queue before replacing
/// the binding and discharges those registrations after unlocking.
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

    fn push_back(&mut self, operation: Arc<SendOperation<M>>) -> Option<WaiterId> {
        let next = WaiterId(self.ids.mint()?);
        // Keep a counter regression total. `park` retains the caller's Arc,
        // so declining this clone is refcount traffic and cannot destroy its
        // user message under the mailbox mutex; the resident operation is not
        // displaced at all.
        match self.entries.entry(next) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(operation);
                Some(next)
            }
            std::collections::btree_map::Entry::Occupied(_) => None,
        }
    }

    fn park(&mut self, operation: &Arc<SendOperation<M>>) -> bool {
        let Some(registration) = self.push_back(Arc::clone(operation)) else {
            return false;
        };
        operation.register(registration);
        true
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

/// User-controlled effects produced while reducing one mailbox transition.
///
/// `MailboxTxn` owns this sink beside the guard and drops the guard first.
/// Locked transition code can only enqueue effects; pulse callbacks, waker
/// vtables, payload destructors, and runtime disposal all run during flush.
/// The sink borrows its mailbox rather than cloning the capability handles out
/// of it: it never outlives the `MailboxTxn` that owns it, and every mailbox
/// transition — including the per-message receive path — would otherwise pay
/// two atomic refcount pairs to restate what the transaction already holds.
struct MailboxEffects<'a, 's, M: Send + 'static> {
    cell: &'a MailboxCell<M>,
    external: Option<&'s mut dyn MailboxEffectSink>,
    pulse: bool,
    displaced: Vec<Envelope<M>>,
    isolate_displaced: bool,
    wakers: WakerEffects,
    returned: Option<M>,
    accepted_sequence_exhausted: bool,
    rejected_bindings: Vec<MailboxBinding<M>>,
    invariant_panic: Option<&'static str>,
}

impl<'a, 's, M: Send + 'static> MailboxEffects<'a, 's, M> {
    fn new(cell: &'a MailboxCell<M>) -> Self {
        Self::with_external(cell, None)
    }

    fn deferred(cell: &'a MailboxCell<M>, external: &'s mut dyn MailboxEffectSink) -> Self {
        Self::with_external(cell, Some(external))
    }

    fn with_external(
        cell: &'a MailboxCell<M>,
        external: Option<&'s mut dyn MailboxEffectSink>,
    ) -> Self {
        Self {
            cell,
            external,
            pulse: false,
            displaced: Vec::new(),
            isolate_displaced: false,
            wakers: WakerEffects::default(),
            returned: None,
            accepted_sequence_exhausted: false,
            rejected_bindings: Vec::new(),
            invariant_panic: None,
        }
    }

    fn pulse(&mut self) {
        self.pulse = true;
    }

    fn isolate_displaced(&mut self) {
        self.isolate_displaced = true;
    }
}

impl<M: Send + 'static> Drop for MailboxEffects<'_, '_, M> {
    fn drop(&mut self) {
        if !self.pulse
            && self.displaced.is_empty()
            && self.wakers.is_empty()
            && self.returned.is_none()
            && !self.accepted_sequence_exhausted
            && self.rejected_bindings.is_empty()
            && self.invariant_panic.is_none()
        {
            return;
        }
        let batch = MailboxEffectBatch {
            changed: Arc::clone(&self.cell.changed),
            runtime: Arc::clone(&self.cell.runtime),
            pulse: self.pulse,
            displaced: std::mem::take(&mut self.displaced),
            isolate_displaced: self.isolate_displaced,
            wakers: std::mem::take(&mut self.wakers),
            returned: self.returned.take(),
            accepted_sequence_exhausted: self.accepted_sequence_exhausted,
            rejected_bindings: std::mem::take(&mut self.rejected_bindings),
            invariant_panic: self.invariant_panic.take(),
        };
        if let Some(external) = self.external.take() {
            external.defer_mailbox_effect(Box::new(move || {
                batch.flush();
            }));
            return;
        }
        batch.flush();
    }
}

struct MailboxEffectBatch<M> {
    changed: Arc<dyn MailboxSignal>,
    runtime: Arc<dyn MailboxRuntime>,
    pulse: bool,
    displaced: Vec<Envelope<M>>,
    isolate_displaced: bool,
    wakers: WakerEffects,
    returned: Option<M>,
    accepted_sequence_exhausted: bool,
    rejected_bindings: Vec<MailboxBinding<M>>,
    invariant_panic: Option<&'static str>,
}

/// A received message remains isolated until every post-unlock effect has
/// flushed successfully. If a pulse, waker, displaced payload, or exhaustion
/// verdict panics, unwinding submits the message for detached disposal instead
/// of destroying it on the mailbox caller's stack.
struct ReturnedMessage<M: Send + 'static> {
    value: Option<M>,
    runtime: Arc<dyn MailboxRuntime>,
}

impl<M: Send + 'static> ReturnedMessage<M> {
    fn new(value: Option<M>, runtime: Arc<dyn MailboxRuntime>) -> Self {
        Self { value, runtime }
    }

    fn take(&mut self) -> Option<M> {
        self.value.take()
    }
}

impl<M: Send + 'static> Drop for ReturnedMessage<M> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            dispose(&self.runtime, value);
        }
    }
}

impl<M: Send + 'static> MailboxEffectBatch<M> {
    fn flush(mut self) {
        let mut panics = PanicAccumulator::default();
        if let Some(message) = self.invariant_panic {
            panics.run(|| panic!("{message}"));
        }
        for rejected in self.rejected_bindings.drain(..) {
            // A rejected binding can own parked user messages. Submit every
            // binding separately so neither this caller nor a sibling job's
            // unwind destroys it inline.
            panics.run(|| dispose_value(self.runtime.as_ref(), rejected));
        }
        if self.pulse {
            panics.run(|| self.changed.pulse());
        }
        // Submit displaced latest-value payloads to isolated disposal before
        // waking accepted senders: a woken sender may run immediately and must
        // not race ahead of disposal submission.
        if self.isolate_displaced && !self.displaced.is_empty() {
            let isolated = std::mem::take(&mut self.displaced);
            panics.run(|| {
                dispose_value(
                    self.runtime.as_ref(),
                    MailboxPayload::unread(isolated.into(), None),
                );
            });
        }
        self.wakers.flush(&mut panics);
        if !self.isolate_displaced {
            for envelope in self.displaced.drain(..) {
                panics.run(|| drop(envelope));
            }
        }
        if let Some(returned) = self.returned.take() {
            panics.run(|| drop(returned));
        }
        if self.accepted_sequence_exhausted {
            panics.run(|| panic!("mailbox accepted-sequence space exhausted"));
        }
    }
}

/// A mailbox transition guard paired with its mandatory post-unlock effects.
///
/// It exposes immutable state through `Deref`. Mutation is either a named
/// transition on the transaction or a `parts()` pairing that hands the state
/// out beside its sink, so no mutation happens without the effects sink in
/// scope. That is what the removed `DerefMut` used to allow.
struct MailboxTxn<'a, 's, M: Send + 'static> {
    state: Option<MutexGuard<'a, MailboxState<M>>>,
    effects: MailboxEffects<'a, 's, M>,
}

impl<'a, M: Send + 'static> MailboxTxn<'a, 'static, M> {
    fn new(cell: &'a MailboxCell<M>) -> Self {
        let effects = MailboxEffects::new(cell);
        let state = cell.state.lock().expect("mailbox mutex poisoned");
        Self {
            state: Some(state),
            effects,
        }
    }
}

impl<'a, 's, M: Send + 'static> MailboxTxn<'a, 's, M> {
    fn deferred(cell: &'a MailboxCell<M>, effects: &'s mut dyn MailboxEffectSink) -> Self {
        let effects = MailboxEffects::deferred(cell, effects);
        let state = cell.state.lock().expect("mailbox mutex poisoned");
        Self {
            state: Some(state),
            effects,
        }
    }

    fn parts(&mut self) -> (&mut MailboxState<M>, &mut MailboxEffects<'a, 's, M>) {
        (
            self.state
                .as_deref_mut()
                .expect("a live mailbox transaction retains its guard"),
            &mut self.effects,
        )
    }

    /// The guarded state alone, for transitions whose effects are queued by
    /// the transaction rather than by the caller.
    fn state_mut(&mut self) -> &mut MailboxState<M> {
        self.parts().0
    }

    fn configure_kind(&mut self, kind: MailboxKind) -> Option<MailboxKind> {
        let state = self.state_mut();
        match state.kind {
            Some(existing) => (existing != kind).then_some(existing),
            None => {
                state.kind = Some(kind);
                None
            }
        }
    }

    fn park(&mut self, operation: &Arc<SendOperation<M>>) -> bool {
        self.state_mut().park(operation)
    }

    #[must_use]
    fn remove_waiter(&mut self, registration: WaiterId) -> Option<Arc<SendOperation<M>>> {
        self.state_mut().remove_waiter(registration)
    }

    #[must_use]
    fn take_next(&mut self, mode: ReceiveMode) -> Option<Envelope<M>> {
        self.state_mut().take_next(mode)
    }

    #[must_use]
    fn take_waiters(&mut self) -> WaiterQueue<M> {
        self.state_mut().take_waiters()
    }

    fn set_last_bound(&mut self, incarnation: Incarnation) {
        self.state_mut().last_bound = Some(incarnation);
    }

    fn replace_binding(&mut self, replacement: MailboxBinding<M>) {
        let rejected = self.state_mut().replace_binding(replacement).err();
        if let Some(rejected) = rejected {
            self.effects.rejected_bindings.push(rejected);
            // First-wins, like every other precedence site: the earliest
            // invariant failure in a transaction is the informative one.
            self.effects
                .invariant_panic
                .get_or_insert("mailbox binding replacement requires an empty waiter queue");
        }
    }

    fn bind_available(&mut self, incarnation: Incarnation) {
        self.replace_binding(MailboxBinding::Bound(BoundState::Available(incarnation)));
    }

    fn bind_full(&mut self, incarnation: Incarnation, waiters: WaiterQueue<M>) {
        self.replace_binding(MailboxBinding::Bound(BoundState::Full {
            incarnation,
            waiters,
        }));
    }

    fn freeze_binding(&mut self, incarnation: Incarnation, waiters: WaiterQueue<M>) {
        self.replace_binding(MailboxBinding::Frozen {
            incarnation,
            waiters,
        });
    }

    fn freeze_after_exhaustion(&mut self, incarnation: Incarnation, waiters: WaiterQueue<M>) {
        // This transition carries the waiter identity domain forward, so it
        // deliberately bypasses `replace_binding`'s empty-domain check.
        self.state_mut().binding = MailboxBinding::Frozen {
            incarnation,
            waiters,
        };
    }

    fn unbind(&mut self, waiters: WaiterQueue<M>) {
        self.replace_binding(MailboxBinding::Unbound(waiters));
    }

    fn terminalize(&mut self, final_incarnation: Option<Incarnation>) {
        self.replace_binding(MailboxBinding::Terminal(final_incarnation));
    }

    fn reset_bind_permit(&mut self) -> MailboxBindToken {
        let state = self.state_mut();
        state.bind_permit = Arc::new(AtomicBool::new(false));
        MailboxBindToken::new(Arc::clone(&state.bind_permit))
    }

    /// Detaches the unread payload into the carrier that owns its destruction.
    ///
    /// Naming `MailboxPayload` in the signature keeps the envelopes from ever
    /// being loose: wherever they are finally destroyed, its `Drop` runs each
    /// user destructor through a `PanicAccumulator` instead of letting one
    /// hostile destructor abandon the rest. `#[must_use]` keeps a dropped
    /// temporary from destroying them here, still under the mailbox mutex.
    #[must_use]
    fn take_payload(&mut self) -> MailboxPayload<M> {
        let state = self.state_mut();
        MailboxPayload::unread(std::mem::take(&mut state.queue), state.latest.take())
    }

    fn finish<R>(mut self, output: R) -> R {
        drop(self.state.take());
        drop(self);
        output
    }

    fn finish_returned(mut self) -> Option<M> {
        drop(self.state.take());
        let mut output = ReturnedMessage::new(
            self.effects.returned.take(),
            Arc::clone(&self.effects.cell.runtime),
        );
        drop(self);
        output.take()
    }
}

impl<M: Send + 'static> Deref for MailboxTxn<'_, '_, M> {
    type Target = MailboxState<M>;

    fn deref(&self) -> &Self::Target {
        self.state
            .as_deref()
            .expect("a live mailbox transaction retains its guard")
    }
}

impl<M: Send + 'static> Drop for MailboxTxn<'_, '_, M> {
    fn drop(&mut self) {
        // Rust drops fields after this body. Empty the guard field here so the
        // effects field necessarily flushes with no mailbox mutex held.
        drop(self.state.take());
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

impl<M> MailboxPayload<M> {
    /// Carries unread messages, with no retired operations yet.
    fn unread(queue: VecDeque<Envelope<M>>, latest: Option<Envelope<M>>) -> Self {
        Self {
            queue: Some(queue),
            latest,
            retired: Vec::new(),
        }
    }
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
    runtime: Arc<dyn MailboxRuntime>,
    changed: Option<Arc<dyn MailboxSignal>>,
    payload: Option<MailboxPayload<M>>,
    termination: Option<Termination<M>>,
}

impl<M: Send + 'static> MailboxTeardown<M> {
    fn finish_framework(&mut self) -> Option<PanicPayload> {
        let mut panics = PanicAccumulator::default();
        if let Some(changed) = self.changed.take() {
            panics.run(|| changed.pulse());
        }
        if let Some(mut termination) = self.termination.take() {
            panics.record(
                termination.finish(
                    &mut self
                        .payload
                        .as_mut()
                        .expect("mailbox teardown retains its payload")
                        .retired,
                ),
            );
        }
        panics.take()
    }
}

impl<M: Send + 'static> MailboxTermination for MailboxTeardown<M> {
    fn finish(mut self: Box<Self>) -> MailboxDisposal {
        let panic = self.finish_framework();
        let payload = self
            .payload
            .take()
            .map(|payload| Box::new(payload) as MailboxDisposal)
            .expect("mailbox teardown retains its payload until finish");
        if let Some(panic) = panic {
            self.runtime.dispose(payload);
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
            dispose(&self.runtime, payload);
        }
    }
}

/// Restart-stable mailbox state for one actor membership.
///
/// Dropping the last handle can destroy unread user payloads. Framework owners
/// must therefore close or terminalize the cell and transfer its payload to
/// isolated disposal before releasing their final handle.
pub(crate) struct MailboxCell<M> {
    pub(super) actor_id: ChildId,
    pub(super) state: Mutex<MailboxState<M>>,
    accepted: AtomicPoisonedCounter,
    runtime: Arc<dyn MailboxRuntime>,
    changed: Arc<dyn MailboxSignal>,
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
    // This constructor bridges the lower mailbox crate to the downstream
    // façade and therefore cannot be crate-private. Direct construction is
    // outside the supported API; the façade does not export either this type
    // or the `MailboxRuntime` capability in its signature.
    #[doc(hidden)]
    pub(crate) fn new(actor_id: ChildId, runtime: Arc<dyn MailboxRuntime>) -> Arc<Self> {
        let changed = runtime.signal();
        Arc::new(Self {
            actor_id,
            state: Mutex::new(MailboxState {
                kind: None,
                bind_permit: Arc::new(AtomicBool::new(false)),
                binding: MailboxBinding::Unbound(WaiterQueue::default()),
                last_bound: None,
                queue: VecDeque::new(),
                latest: None,
            }),
            accepted: AtomicPoisonedCounter::new(),
            runtime,
            changed,
        })
    }

    pub(super) fn submit(&self, message: M) -> Submission<M> {
        let mut transaction = MailboxTxn::new(self);
        let transition = match transaction.status() {
            BindingStatus::Terminal(final_incarnation) => {
                SubmitTransition::Complete(Submission::Terminated {
                    message,
                    final_incarnation,
                })
            }
            BindingStatus::Bound(incarnation) => {
                let accepted = {
                    let (state, effects) = transaction.parts();
                    accept_locked(state, incarnation, message, &self.accepted, effects)
                };
                match accepted {
                    AcceptTransition::Accepted(incarnation) => {
                        SubmitTransition::Complete(Submission::Accepted(incarnation))
                    }
                    AcceptTransition::Full(message) => {
                        let operation = SendOperation::new(message);
                        operation.observe(incarnation);
                        if transaction.park(&operation) {
                            SubmitTransition::Complete(Submission::Parked(operation))
                        } else {
                            SubmitTransition::WaiterIdentityExhausted(operation)
                        }
                    }
                    AcceptTransition::Exhausted(message) => {
                        SubmitTransition::AcceptedSequenceExhausted(message)
                    }
                }
            }
            status @ (BindingStatus::Frozen(_) | BindingStatus::Unbound) => {
                let operation = SendOperation::new(message);
                if let BindingStatus::Frozen(incarnation) = status {
                    operation.observe(incarnation);
                }
                if transaction.park(&operation) {
                    SubmitTransition::Complete(Submission::Parked(operation))
                } else {
                    SubmitTransition::WaiterIdentityExhausted(operation)
                }
            }
        };
        match transaction.finish(transition) {
            SubmitTransition::Complete(submission) => submission,
            SubmitTransition::WaiterIdentityExhausted(operation) => {
                dispose(&self.runtime, operation);
                panic!("mailbox waiter identity space exhausted");
            }
            SubmitTransition::AcceptedSequenceExhausted(message) => {
                dispose(&self.runtime, message);
                panic!("mailbox accepted-sequence space exhausted");
            }
        }
    }

    pub(super) fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        let mut transaction = MailboxTxn::new(self);
        let transition = match transaction.status() {
            BindingStatus::Terminal(final_incarnation) => {
                TrySendTransition::Complete(Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: final_incarnation,
                    message,
                    kind: SendErrorKind::Terminated,
                }))
            }
            BindingStatus::Unbound => TrySendTransition::Complete(Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: None,
                message,
                kind: SendErrorKind::NotRunning,
            })),
            BindingStatus::Frozen(incarnation) => TrySendTransition::Complete(Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: Some(incarnation),
                message,
                kind: SendErrorKind::NotRunning,
            })),
            BindingStatus::Bound(incarnation) => {
                let accepted = {
                    let (state, effects) = transaction.parts();
                    accept_locked(state, incarnation, message, &self.accepted, effects)
                };
                match accepted {
                    AcceptTransition::Accepted(incarnation) => {
                        TrySendTransition::Complete(Ok(incarnation))
                    }
                    AcceptTransition::Full(message) => {
                        TrySendTransition::Complete(Err(SendError {
                            actor_id: self.actor_id.clone(),
                            incarnation_observed: Some(incarnation),
                            message,
                            kind: SendErrorKind::Full,
                        }))
                    }
                    AcceptTransition::Exhausted(message) => {
                        TrySendTransition::AcceptedSequenceExhausted(message)
                    }
                }
            }
        };
        match transaction.finish(transition) {
            TrySendTransition::Complete(result) => result,
            TrySendTransition::AcceptedSequenceExhausted(message) => {
                dispose(&self.runtime, message);
                panic!("mailbox accepted-sequence space exhausted");
            }
        }
    }

    fn receive(&self, incarnation: Incarnation, mode: ReceiveMode) -> Option<M> {
        let mut transaction = MailboxTxn::new(self);
        let eligible = match transaction.status() {
            BindingStatus::Bound(current) => current == incarnation,
            BindingStatus::Frozen(current) => mode == ReceiveMode::Drain && current == incarnation,
            BindingStatus::Unbound | BindingStatus::Terminal(_) => false,
        };
        if !eligible {
            return transaction.finish(None);
        }
        let envelope = transaction.take_next(mode);
        if let Some(envelope) = envelope {
            transaction.effects.returned = Some(envelope.message);
            if matches!(transaction.status(), BindingStatus::Bound(_)) {
                let (state, effects) = transaction.parts();
                promote_waiters(state, &self.accepted, effects);
            }
            transaction.effects.pulse();
        }
        transaction.finish_returned()
    }

    pub(super) fn current_observation(&self) -> Option<Incarnation> {
        self.state
            .lock()
            .expect("mailbox mutex poisoned")
            .current_observation()
    }

    fn watcher(&self) -> Box<dyn MailboxSignalWatcher> {
        self.changed.watcher()
    }

    fn accepted_sequence(&self) -> AcceptedSequence {
        AcceptedSequence(self.accepted.load(Ordering::Acquire))
    }
}

impl<M> MailboxCell<M> {
    pub(super) fn runtime(&self) -> Arc<dyn MailboxRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(super) fn now(&self) -> Instant {
        self.runtime.now()
    }

    pub(super) fn dispose<T: Send + 'static>(&self, value: T) {
        dispose_value(self.runtime.as_ref(), value);
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
        let current_observation = transaction.current_observation();
        let transition = {
            let mut state = operation
                .state
                .lock()
                .expect("send operation mutex poisoned");
            match &mut state.outcome {
                OperationOutcome::Waiting {
                    message,
                    newest_observed,
                } => match message.take() {
                    Some(message) => {
                        // Mailbox terminality linearizes in the binding, while a
                        // parked operation linearizes when its own outcome leaves
                        // `Waiting`. A terminal teardown may therefore have
                        // detached this waiter without discharging it yet; an
                        // already-expired withdrawal legitimately wins that
                        // operation-local race.
                        // The mailbox lock makes this one evidence snapshot: a
                        // binding either precedes withdrawal and contributes its
                        // incarnation, or follows the completed withdrawal. This
                        // also covers an operation first submitted by an elapsed
                        // (including zero-duration) deadline poll.
                        let observed = (*newest_observed).or(current_observation);
                        state.outcome = OperationOutcome::Withdrawn;
                        state.waker.take(
                            match disposition {
                                WithdrawalDisposition::Inline => WakerAction::DropInline,
                                WithdrawalDisposition::Isolated => {
                                    WakerAction::Dispose(Arc::clone(&self.runtime))
                                }
                            },
                            &mut waker_effects,
                        );
                        Ok((
                            WithdrawalOutcome::Withdrawn { message, observed },
                            state.registration.take(),
                        ))
                    }
                    None => Err("a waiting operation must retain its message"),
                },
                OperationOutcome::Accepted(incarnation) => {
                    let incarnation = *incarnation;
                    // Acceptance took the waker in the same critical section
                    // that published this outcome. Keep a future regression
                    // total by transferring any leftover waker to the same
                    // post-unlock effect set as an ordinary withdrawal.
                    state.waker.take(
                        match disposition {
                            WithdrawalDisposition::Inline => WakerAction::DropInline,
                            WithdrawalDisposition::Isolated => {
                                WakerAction::Dispose(Arc::clone(&self.runtime))
                            }
                        },
                        &mut waker_effects,
                    );
                    Ok((
                        WithdrawalOutcome::Accepted(incarnation),
                        state.registration.take(),
                    ))
                }
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => match message.take() {
                    Some(message) => {
                        let observed = *final_incarnation;
                        // Termination likewise normally took the waker before
                        // publishing. Transfer a leftover instead of letting a
                        // diagnostic unwind this by-value message under the
                        // operation mutex.
                        state.waker.take(
                            match disposition {
                                WithdrawalDisposition::Inline => WakerAction::DropInline,
                                WithdrawalDisposition::Isolated => {
                                    WakerAction::Dispose(Arc::clone(&self.runtime))
                                }
                            },
                            &mut waker_effects,
                        );
                        Ok((
                            WithdrawalOutcome::Terminated { message, observed },
                            state.registration.take(),
                        ))
                    }
                    None => Err("a terminal operation must retain its message"),
                },
                OperationOutcome::Withdrawn => Err("a send operation was withdrawn more than once"),
            }
        };
        let (outcome, registration) = match transition {
            Ok(transition) => transition,
            Err(message) => {
                transaction.finish(());
                panic!("{message}");
            }
        };
        let mut unexpected_removed = None;
        let mut missing_nonterminal_registration = false;
        if let Some(registration) = registration {
            if let Some(removed) = transaction.remove_waiter(registration) {
                if Arc::ptr_eq(&removed, operation) {
                    // The caller's Arc proves this locked refcount decrement
                    // cannot destroy the operation or its user message.
                    drop(removed);
                } else {
                    unexpected_removed = Some(removed);
                }
            } else {
                missing_nonterminal_registration =
                    !matches!(transaction.status(), BindingStatus::Terminal(_));
            }
        }
        let withdrawal = transaction.finish(Withdrawal {
            outcome: Some(outcome),
            _waker_effects: waker_effects,
        });
        let invariant = if unexpected_removed.is_some() {
            Some("a waiter registration must identify its send operation")
        } else if missing_nonterminal_registration {
            Some("only terminal teardown may detach a live waiter registration")
        } else {
            None
        };
        if let Some(invariant) = invariant {
            // `withdraw` is reached from `SendFuture`'s drop glue, so this can
            // run on an already-unwinding stack. `PanicAccumulator` contains
            // rather than resumes there -- raising the invariant would be a
            // double panic and an abort, which is the outcome this whole lane
            // exists to prevent -- so decide the disposition up front instead
            // of assuming the accumulator will resume.
            let unwinding = std::thread::panicking();
            let mut panics = PanicAccumulator::default();
            // Establish the framework diagnostic first, then submit both the
            // unexpectedly removed operation and this withdrawal's by-value
            // message/waker effects before resuming it. Neither can therefore
            // be destroyed on the invariant panic's stack, and neither can
            // displace the diagnostic: the accumulator keeps the first panic.
            if !unwinding {
                panics.run(|| panic!("{invariant}"));
            }
            if let Some(unexpected_removed) = unexpected_removed {
                panics.run(|| self.dispose(unexpected_removed));
            }
            if !unwinding {
                panics.run(|| self.dispose(withdrawal));
                drop(panics);
                unreachable!("a non-unwinding accumulator resumes its recorded panic");
            }
            // Contained: the caller is already unwinding and still owns this
            // withdrawal's ordinary disposal path, so hand it back rather than
            // taking it over.
            drop(panics);
            return withdrawal;
        }
        withdrawal
    }
}

impl<M: Send + 'static> MailboxControl for MailboxCell<M> {
    fn configure(
        &self,
        mailbox: ResolvedMailbox,
        effects: &mut dyn MailboxEffectSink,
    ) -> MailboxBindToken {
        let kind = match mailbox {
            ResolvedMailbox::Queue(capacity) => MailboxKind::Queue(capacity),
            ResolvedMailbox::Latest => MailboxKind::Latest,
        };
        let mut transaction = MailboxTxn::deferred(self, effects);
        let mismatch = transaction.configure_kind(kind);
        let token = MailboxBindToken::new(Arc::clone(&transaction.bind_permit));
        transaction.finish(());
        if let Some(existing) = mismatch {
            panic!(
                "mailbox configuration changed from {existing:?} to {kind:?} after initialization"
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
        if matches!(transaction.status(), BindingStatus::Terminal(_)) {
            return transaction.finish(());
        }
        let Some(kind) = transaction.kind else {
            transaction.finish(());
            panic!("mailbox must be configured before its first bind")
        };
        if !matches!(transaction.status(), BindingStatus::Unbound) {
            transaction.finish(());
            panic!("mailbox must close the prior incarnation before rebinding")
        }
        if !token.claim(&transaction.bind_permit) {
            transaction.finish(());
            panic!("mailbox bind token is foreign or was already consumed")
        }
        let mut waiters = transaction.take_waiters();
        transaction.set_last_bound(incarnation);
        // Binding is an observation edge for every operation that remained
        // parked through it, including FIFO overflow that cannot be promoted
        // into the current capacity. Withdrawal takes the mailbox lock before
        // the operation lock, so a concurrent timeout sees either the prior
        // evidence or this incarnation consistently with which edge won.
        waiters.observe_all(incarnation);
        {
            let (state, effects) = transaction.parts();
            let MailboxState { queue, latest, .. } = state;
            promote_waiter_queue(
                kind,
                incarnation,
                &mut waiters,
                queue,
                latest,
                &self.accepted,
                effects,
            );
        }
        if transaction.effects.accepted_sequence_exhausted {
            // Exhaustion is the one way promotion stops early, so neither
            // derived verdict below holds: a latest mailbox can still owe
            // waiters and a queue mailbox can still have free capacity.
            // Freezing is the honest state — no further acceptance is
            // possible on this counter — and it keeps the parked senders in
            // mailbox-owned state. Destroying them here would run user
            // message destructors under the mailbox mutex during the
            // exhaustion panic's own unwind; the post-unlock effect raises
            // that panic instead.
            //
            // Assigned directly rather than through `replace_binding`: this
            // is the one transition whose replacement carries the waiter
            // queue forward instead of discarding it, so a declined
            // replacement would destroy exactly what the branch exists to
            // preserve.
            transaction.freeze_after_exhaustion(incarnation, waiters);
        } else if waiters.is_empty() {
            transaction.bind_available(incarnation);
        } else {
            let MailboxKind::Queue(capacity) = kind else {
                unreachable!("a latest mailbox finishes all waiting submissions")
            };
            // Promotion leaves waiters only after filling a configured queue.
            // Binding Full is the total fallback too: diagnosing here would
            // unwind the waiter queue's user messages under the mailbox mutex.
            let _ = capacity;
            transaction.bind_full(incarnation, waiters);
        }
        transaction.effects.pulse();
        transaction.effects.isolate_displaced();
        transaction.finish(())
    }

    fn freeze(&self, incarnation: Incarnation, effects: &mut dyn MailboxEffectSink) {
        let mut transaction = MailboxTxn::deferred(self, effects);
        if transaction.status() != BindingStatus::Bound(incarnation) {
            return transaction.finish(());
        }
        let waiters = transaction.take_waiters();
        transaction.freeze_binding(incarnation, waiters);
        transaction.effects.pulse();
        transaction.finish(())
    }

    fn close(
        &self,
        incarnation: Incarnation,
        effects: &mut dyn MailboxEffectSink,
    ) -> Option<MailboxClose> {
        let mut transaction = MailboxTxn::deferred(self, effects);
        if !matches!(
            transaction.status(),
            BindingStatus::Bound(current) | BindingStatus::Frozen(current)
                if current == incarnation
        ) {
            return transaction.finish(None);
        }
        let waiters = transaction.take_waiters();
        transaction.unbind(waiters);
        let token = transaction.reset_bind_permit();
        let payload = transaction.take_payload();
        transaction.effects.pulse();
        let disposal = Box::new(payload) as MailboxDisposal;
        // The close result outlives this transaction's effect flush at every
        // caller, so it carries the disposal capability that isolates the
        // unread payload if that flush unwinds.
        let runtime = Arc::clone(&transaction.effects.cell.runtime);
        transaction.finish(Some(MailboxClose::new(token, disposal, runtime)))
    }

    fn prepare_termination(
        &self,
        effects: &mut dyn MailboxEffectSink,
    ) -> Option<Box<dyn MailboxTermination>> {
        let mut transaction = MailboxTxn::deferred(self, effects);
        if matches!(transaction.status(), BindingStatus::Terminal(_)) {
            return transaction.finish(None);
        }
        let final_incarnation = transaction.last_bound;
        let waiters = transaction.take_waiters();
        // This binding transition linearizes mailbox terminality. Each
        // detached waiter is decided separately by its `Waiting ->` outcome
        // transition, so an already-expired withdrawal may beat the deferred
        // discharge even after this mailbox-wide transition.
        transaction.terminalize(final_incarnation);
        let payload = transaction.take_payload();
        let termination = Termination {
            waiters,
            final_incarnation,
        };
        let teardown = Some(Box::new(MailboxTeardown {
            runtime: Arc::clone(&self.runtime),
            changed: Some(Arc::clone(&self.changed)),
            payload: Some(payload),
            termination: Some(termination),
        }) as Box<dyn MailboxTermination>);
        transaction.finish(teardown)
    }
}

fn mint_accepted_sequence(accepted: &AtomicPoisonedCounter) -> Option<AcceptedSequence> {
    accepted
        .mint(Ordering::Release, Ordering::Relaxed)
        .map(AcceptedSequence)
}

fn accept_locked<M>(
    state: &mut MailboxState<M>,
    incarnation: Incarnation,
    message: M,
    accepted: &AtomicPoisonedCounter,
    effects: &mut MailboxEffects<'_, '_, M>,
) -> AcceptTransition<M>
where
    M: Send + 'static,
{
    match &state.binding {
        MailboxBinding::Bound(BoundState::Available(current)) => {
            // `incarnation` was read from this same binding by the surrounding
            // transaction, so a mismatch is a framework invariant failure.
            // Keep it total -- the by-value user message returns through the
            // caller's post-unlock path rather than being destroyed under the
            // mailbox mutex -- and raise the diagnostic through the same
            // effects sink, which flushes it after unlock.
            if *current != incarnation {
                effects.invariant_panic =
                    Some("an accepting transition observes its own binding's incarnation");
                return AcceptTransition::Full(message);
            }
        }
        MailboxBinding::Bound(BoundState::Full { .. }) => {
            return AcceptTransition::Full(message);
        }
        MailboxBinding::Unbound(_)
        | MailboxBinding::Frozen { .. }
        | MailboxBinding::Terminal(_) => {
            unreachable!("accept_locked is entered only from a bound mailbox")
        }
    }
    let kind = match state.kind {
        Some(MailboxKind::Queue(capacity)) if state.queue.len() < capacity.get() => {
            MailboxKind::Queue(capacity)
        }
        Some(MailboxKind::Latest) => MailboxKind::Latest,
        Some(MailboxKind::Queue(_)) => return AcceptTransition::Full(message),
        None => unreachable!("a bound mailbox is always configured"),
    };
    let Some(accepted_sequence) = mint_accepted_sequence(accepted) else {
        return AcceptTransition::Exhausted(message);
    };
    match kind {
        MailboxKind::Queue(_) => {
            state.queue.push_back(Envelope {
                message,
                accepted_sequence,
            });
        }
        MailboxKind::Latest => {
            let displaced = state.latest.replace(Envelope {
                message,
                accepted_sequence,
            });
            if let Some(displaced) = displaced {
                effects.displaced.push(displaced);
            }
        }
    }
    effects.pulse();
    AcceptTransition::Accepted(incarnation)
}

fn promote_waiters<M: Send + 'static>(
    state: &mut MailboxState<M>,
    accepted_sequence: &AtomicPoisonedCounter,
    effects: &mut MailboxEffects<'_, '_, M>,
) {
    let Some(kind) = state.kind else {
        return;
    };
    let MailboxBinding::Bound(BoundState::Full {
        incarnation,
        waiters,
    }) = &mut state.binding
    else {
        return;
    };
    let incarnation = *incarnation;
    promote_waiter_queue(
        kind,
        incarnation,
        waiters,
        &mut state.queue,
        &mut state.latest,
        accepted_sequence,
        effects,
    );
    if waiters.is_empty() {
        state.binding = MailboxBinding::Bound(BoundState::Available(incarnation));
    }
}

fn promote_waiter_queue<M: Send + 'static>(
    kind: MailboxKind,
    incarnation: Incarnation,
    waiters: &mut WaiterQueue<M>,
    queue: &mut VecDeque<Envelope<M>>,
    latest: &mut Option<Envelope<M>>,
    accepted_sequence: &AtomicPoisonedCounter,
    effects: &mut MailboxEffects<'_, '_, M>,
) {
    let available = match kind {
        MailboxKind::Queue(capacity) => capacity.get().saturating_sub(queue.len()),
        MailboxKind::Latest => usize::MAX,
    };
    let mut accepted = 0usize;
    while accepted < available {
        if waiters.is_empty() {
            break;
        }
        let Some(accepted_sequence) = mint_accepted_sequence(accepted_sequence) else {
            effects.accepted_sequence_exhausted = true;
            break;
        };
        let Some((registration, operation)) = waiters.pop_front() else {
            break;
        };
        operation.clear_registration(registration);
        operation.observe(incarnation);
        let Some(message) = operation.accept(incarnation, &mut effects.wakers) else {
            continue;
        };
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
                    effects.displaced.push(displaced);
                }
            }
        }
        accepted += 1;
    }
}

#[derive(Clone, Copy)]
pub(super) enum WithdrawalDisposition {
    Inline,
    Isolated,
}

pub(super) struct Withdrawal<M> {
    outcome: Option<WithdrawalOutcome<M>>,
    _waker_effects: WakerEffects,
}

impl<M> Withdrawal<M> {
    pub(super) fn without_effects(outcome: WithdrawalOutcome<M>) -> Self {
        Self {
            outcome: Some(outcome),
            _waker_effects: WakerEffects::default(),
        }
    }

    pub(super) fn take_outcome(&mut self) -> WithdrawalOutcome<M> {
        self.outcome
            .take()
            .expect("a withdrawal outcome is consumed exactly once")
    }

    pub(super) fn finish(self) {}
}

pub(super) enum WithdrawalOutcome<M> {
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
    watcher: Box<dyn MailboxSignalWatcher>,
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
    /// This remains public only as the cross-crate receiver seam used by
    /// `RawContext`; it is not reachable from Shelterwood's supported API.
    pub(crate) async fn changed(&mut self) {
        self.watcher.changed().await;
    }
}

#[cfg(test)]
pub(super) mod tests;
