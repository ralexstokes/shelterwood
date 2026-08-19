use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::{
    ChildId, Incarnation, MailboxBindToken, MailboxClose, MailboxControl, MailboxDisposal,
    MailboxEffectQueue, MailboxEffectSink, MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
    MailboxTermination,
    capability::{dispose, dispose_value},
    identity::{AtomicPoisonedCounter, PoisonedCounter},
    panic::{PanicAccumulator, PanicPayload, resume_panic},
    policy::ResolvedMailbox,
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
pub struct AcceptedSequence(u64);

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

mod waker_slot {
    use std::{sync::Arc, task::Waker};

    use crate::{MailboxRuntime, capability::dispose, panic::PanicAccumulator};

    /// The only storage surface for a caller-owned waker.
    ///
    /// Its value is private even from the parent mailbox module, and every
    /// mutating operation requires an effects sink, so replacing or taking an
    /// `Option<Waker>` and accidentally dropping it beside a guard does not
    /// type-check.
    #[derive(Default)]
    pub(super) struct WakerSlot(Option<Waker>);

    pub(super) enum WakerAction {
        Wake,
        DropInline,
        Dispose(Arc<dyn MailboxRuntime>),
    }

    enum WakerEffect {
        Wake(Waker),
        DropInline(Waker),
        Dispose(Arc<dyn MailboxRuntime>, Waker),
    }

    #[derive(Default)]
    pub(super) struct WakerEffects(Vec<WakerEffect>);

    impl WakerSlot {
        pub(super) fn will_wake(&self, waker: &Waker) -> bool {
            self.0
                .as_ref()
                .is_some_and(|registered| registered.will_wake(waker))
        }

        pub(super) fn replace(&mut self, waker: Waker, effects: &mut WakerEffects) {
            if let Some(displaced) = self.0.replace(waker) {
                effects.push(displaced, WakerAction::DropInline);
            }
        }

        pub(super) fn take(&mut self, action: WakerAction, effects: &mut WakerEffects) {
            if let Some(waker) = self.0.take() {
                effects.push(waker, action);
            }
        }

        pub(super) fn is_empty(&self) -> bool {
            self.0.is_none()
        }
    }

    impl WakerEffects {
        fn push(&mut self, waker: Waker, action: WakerAction) {
            self.0.push(match action {
                WakerAction::Wake => WakerEffect::Wake(waker),
                WakerAction::DropInline => WakerEffect::DropInline(waker),
                WakerAction::Dispose(runtime) => WakerEffect::Dispose(runtime, waker),
            });
        }

        pub(super) fn flush(&mut self, panics: &mut PanicAccumulator) {
            for effect in self.0.drain(..) {
                match effect {
                    WakerEffect::Wake(waker) => panics.run(|| waker.wake()),
                    WakerEffect::DropInline(waker) => panics.run(|| drop(waker)),
                    WakerEffect::Dispose(runtime, waker) => {
                        panics.run(|| dispose(&runtime, waker));
                    }
                }
            }
        }
    }

    impl Drop for WakerEffects {
        fn drop(&mut self) {
            self.flush(&mut PanicAccumulator::default());
        }
    }
}

use waker_slot::{WakerAction, WakerEffects, WakerSlot};

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
                OperationOutcome::Accepted(incarnation) => OperationPoll::Accepted(*incarnation),
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => OperationPoll::Terminated {
                    message: message
                        .take()
                        .expect("a terminal operation retains its message until observed"),
                    final_incarnation: *final_incarnation,
                },
                OperationOutcome::Waiting { .. } => {
                    if let Some(replacement) = replacement {
                        state.waker.replace(replacement, &mut effects);
                        OperationPoll::Pending
                    } else if state.waker.will_wake(current) {
                        OperationPoll::Pending
                    } else {
                        OperationPoll::NeedsWakerClone
                    }
                }
                OperationOutcome::Withdrawn => panic!("a withdrawn send future was polled"),
            }
        };
        drop(effects);
        result
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
                debug_assert_eq!(self.queue.len(), capacity.get());
                let incarnation = *incarnation;
                let mut waiters = WaiterQueue::default();
                if !waiters.park(operation) {
                    return false;
                }
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

    fn push_back(&mut self, operation: Arc<SendOperation<M>>) -> Option<WaiterId> {
        let next = WaiterId(self.ids.mint()?);
        let replaced = self.entries.insert(next, operation);
        debug_assert!(replaced.is_none());
        Some(next)
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
}

impl<'a, 's, M: Send + 'static> MailboxEffects<'a, 's, M> {
    fn new(cell: &'a MailboxCell<M>) -> Self {
        Self {
            cell,
            external: None,
            pulse: false,
            displaced: Vec::new(),
            isolate_displaced: false,
            wakers: WakerEffects::default(),
            returned: None,
            accepted_sequence_exhausted: false,
        }
    }

    fn deferred(cell: &'a MailboxCell<M>, external: &'s mut dyn MailboxEffectSink) -> Self {
        Self {
            cell,
            external: Some(external),
            pulse: false,
            displaced: Vec::new(),
            isolate_displaced: false,
            wakers: WakerEffects::default(),
            returned: None,
            accepted_sequence_exhausted: false,
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
        let batch = MailboxEffectBatch {
            changed: Arc::clone(&self.cell.changed),
            runtime: Arc::clone(&self.cell.runtime),
            pulse: self.pulse,
            displaced: std::mem::take(&mut self.displaced),
            isolate_displaced: self.isolate_displaced,
            wakers: std::mem::take(&mut self.wakers),
            returned: self.returned.take(),
            accepted_sequence_exhausted: self.accepted_sequence_exhausted,
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
                    MailboxPayload {
                        queue: Some(isolated.into()),
                        latest: None,
                        retired: Vec::new(),
                    },
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

impl<M: Send + 'static> DerefMut for MailboxTxn<'_, '_, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state
            .as_deref_mut()
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
    fn finish(mut self: Box<Self>) -> Option<MailboxDisposal> {
        let panic = self.finish_framework();
        let payload = self
            .payload
            .take()
            .map(|payload| Box::new(payload) as MailboxDisposal);
        if let Some(panic) = panic {
            if let Some(payload) = payload {
                dispose(&self.runtime, payload);
            }
            resume_panic(panic);
        }
        payload
    }
}

impl<M: Send + 'static> crate::private::SealedMailboxTermination for MailboxTeardown<M> {}

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
pub struct MailboxCell<M> {
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
    pub fn new(actor_id: ChildId, runtime: Arc<dyn MailboxRuntime>) -> Arc<Self> {
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
            BindingStatus::Frozen(incarnation) => {
                let operation = SendOperation::new(message);
                operation.observe(incarnation);
                if transaction.park(&operation) {
                    SubmitTransition::Complete(Submission::Parked(operation))
                } else {
                    SubmitTransition::WaiterIdentityExhausted(operation)
                }
            }
            BindingStatus::Unbound => {
                let operation = SendOperation::new(message);
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

    fn receive(
        &self,
        incarnation: Incarnation,
        mode: ReceiveMode,
        accepted_through: Option<AcceptedSequence>,
    ) -> Option<M> {
        let mut transaction = MailboxTxn::new(self);
        let eligible = match transaction.status() {
            BindingStatus::Bound(current) => current == incarnation,
            BindingStatus::Frozen(current) => {
                mode == ReceiveMode::IncludeFrozen && current == incarnation
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => false,
        };
        if !eligible {
            return transaction.finish(None);
        }
        let envelope = match transaction.kind {
            Some(MailboxKind::Queue(_)) => {
                let eligible = transaction.queue.front().is_some_and(|item| {
                    accepted_through.is_none_or(|limit| item.accepted_sequence <= limit)
                });
                eligible.then(|| transaction.queue.pop_front()).flatten()
            }
            Some(MailboxKind::Latest) => {
                let eligible = transaction.latest.as_ref().is_some_and(|item| {
                    accepted_through.is_none_or(|limit| item.accepted_sequence <= limit)
                });
                eligible.then(|| transaction.latest.take()).flatten()
            }
            None => None,
        };
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
        match self.state.lock().expect("mailbox mutex poisoned").status() {
            BindingStatus::Bound(incarnation) | BindingStatus::Frozen(incarnation) => {
                Some(incarnation)
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => None,
        }
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
        let current_observation = match transaction.status() {
            BindingStatus::Bound(incarnation) | BindingStatus::Frozen(incarnation) => {
                Some(incarnation)
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => None,
        };
        let (outcome, registration) = {
            let mut state = operation
                .state
                .lock()
                .expect("send operation mutex poisoned");
            match &mut state.outcome {
                OperationOutcome::Waiting {
                    message,
                    newest_observed,
                } => {
                    // Mailbox terminality linearizes in the binding, while a
                    // parked operation linearizes when its own outcome leaves
                    // `Waiting`. A terminal teardown may therefore have
                    // detached this waiter without discharging it yet; an
                    // already-expired withdrawal legitimately wins that
                    // operation-local race.
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
                    state.waker.take(
                        match disposition {
                            WithdrawalDisposition::Inline => WakerAction::DropInline,
                            WithdrawalDisposition::Isolated => {
                                WakerAction::Dispose(Arc::clone(&self.runtime))
                            }
                        },
                        &mut waker_effects,
                    );
                    (
                        WithdrawalOutcome::Withdrawn { message, observed },
                        state.registration.take(),
                    )
                }
                OperationOutcome::Accepted(incarnation) => {
                    let incarnation = *incarnation;
                    // Acceptance took the waker in the same critical section
                    // that published this outcome, so no registration survives
                    // it for withdrawal to release.
                    debug_assert!(state.waker.is_empty());
                    (
                        WithdrawalOutcome::Accepted(incarnation),
                        state.registration.take(),
                    )
                }
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => {
                    let message = message
                        .take()
                        .expect("a terminal operation must retain its message");
                    let observed = *final_incarnation;
                    // Termination likewise took the waker before publishing.
                    debug_assert!(state.waker.is_empty());
                    (
                        WithdrawalOutcome::Terminated { message, observed },
                        state.registration.take(),
                    )
                }
                OperationOutcome::Withdrawn => {
                    panic!("a send operation was withdrawn more than once")
                }
            }
        };
        if let Some(registration) = registration {
            if let Some(removed) = transaction.remove_waiter(registration) {
                debug_assert!(Arc::ptr_eq(&removed, operation));
            } else {
                debug_assert!(matches!(transaction.status(), BindingStatus::Terminal(_)));
            }
        }
        transaction.finish(Withdrawal {
            outcome: Some(outcome),
            _waker_effects: waker_effects,
        })
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
        let mismatch = match transaction.kind {
            Some(existing) => (existing != kind).then_some(existing),
            None => {
                transaction.kind = Some(kind);
                None
            }
        };
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
        transaction.last_bound = Some(incarnation);
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
            transaction.binding = MailboxBinding::Frozen {
                incarnation,
                waiters,
            };
        } else if waiters.is_empty() {
            transaction.binding = MailboxBinding::Bound(BoundState::Available(incarnation));
        } else {
            let MailboxKind::Queue(capacity) = kind else {
                unreachable!("a latest mailbox finishes all waiting submissions")
            };
            debug_assert_eq!(transaction.queue.len(), capacity.get());
            transaction.binding = MailboxBinding::Bound(BoundState::Full {
                incarnation,
                waiters,
            });
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
        transaction.binding = MailboxBinding::Frozen {
            incarnation,
            waiters,
        };
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
        transaction.binding = MailboxBinding::Unbound(waiters);
        transaction.bind_permit = Arc::new(AtomicBool::new(false));
        let queue = std::mem::take(&mut transaction.queue);
        let latest = transaction.latest.take();
        transaction.effects.pulse();
        let disposal = Box::new(MailboxPayload {
            queue: Some(queue),
            latest,
            retired: Vec::new(),
        }) as MailboxDisposal;
        let token = MailboxBindToken::new(Arc::clone(&transaction.bind_permit));
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
        transaction.binding = MailboxBinding::Terminal(final_incarnation);
        let queue = std::mem::take(&mut transaction.queue);
        let latest = transaction.latest.take();
        let termination = Termination {
            waiters,
            final_incarnation,
        };
        let teardown = Some(Box::new(MailboxTeardown {
            runtime: Arc::clone(&self.runtime),
            changed: Some(Arc::clone(&self.changed)),
            payload: Some(MailboxPayload {
                queue: Some(queue),
                latest,
                retired: Vec::new(),
            }),
            termination: Some(termination),
        }) as Box<dyn MailboxTermination>);
        transaction.finish(teardown)
    }
}

impl<M: Send + 'static> crate::private::SealedMailboxControl for MailboxCell<M> {}

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
            debug_assert_eq!(*current, incarnation);
        }
        MailboxBinding::Bound(BoundState::Full {
            incarnation: current,
            ..
        }) => {
            debug_assert_eq!(*current, incarnation);
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
pub struct MailboxReceiver<M> {
    mailbox: Arc<MailboxCell<M>>,
    incarnation: Incarnation,
    watcher: Box<dyn MailboxSignalWatcher>,
}

impl<M: Send + 'static> MailboxReceiver<M> {
    pub fn new(mailbox: Arc<MailboxCell<M>>, incarnation: Incarnation) -> Self {
        let watcher = mailbox.watcher();
        Self {
            mailbox,
            incarnation,
            watcher,
        }
    }

    pub fn try_recv(&self) -> Option<M> {
        self.mailbox
            .receive(self.incarnation, ReceiveMode::IncludeFrozen, None)
    }

    pub fn try_recv_live_through(&self, accepted_sequence: AcceptedSequence) -> Option<M> {
        self.mailbox.receive(
            self.incarnation,
            ReceiveMode::LiveOnly,
            Some(accepted_sequence),
        )
    }

    pub fn accepted_sequence(&self) -> AcceptedSequence {
        self.mailbox.accepted_sequence()
    }

    pub fn freeze(&self) {
        let mut effects = MailboxEffectQueue::default();
        self.mailbox.freeze(self.incarnation, &mut effects);
    }

    /// Waits for mailbox activity in the façade's merged raw-actor event loop.
    ///
    /// This remains public only as the cross-crate receiver seam used by
    /// `RawContext`; it is not reachable from Shelterwood's supported API.
    pub async fn changed(&mut self) {
        self.watcher.changed().await;
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::{
            Arc, Mutex, Weak,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, Wake, Waker},
        time::{Duration, Instant},
    };

    use crate::{
        ActorIdentity, ActorRef, ChildId, Incarnation, MailboxControl, MailboxReceiver,
        SendErrorKind,
        identity::ScopeIdentity,
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

    struct LockCheckingMessage {
        mailbox: Weak<MailboxCell<LockCheckingMessage>>,
        dropped: Option<mpsc::Sender<bool>>,
    }

    impl Drop for LockCheckingMessage {
        fn drop(&mut self) {
            let Some(dropped) = self.dropped.take() else {
                return;
            };
            let unlocked = self
                .mailbox
                .upgrade()
                .is_none_or(|mailbox| mailbox.state.try_lock().is_ok());
            let _ = dropped.send(unlocked);
        }
    }

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum BindEffectEvent {
        SignalPulsed,
        DisposalSubmitted,
        SenderWoken,
    }

    struct BindOrderingRuntime {
        inner: Arc<dyn crate::MailboxRuntime>,
        events: Arc<Mutex<Vec<BindEffectEvent>>>,
    }

    impl crate::MailboxRuntime for BindOrderingRuntime {
        fn oneshot(
            &self,
        ) -> (
            Box<dyn crate::ErasedOneShotSender>,
            Pin<Box<dyn crate::ErasedOneShotReceiver>>,
        ) {
            self.inner.oneshot()
        }

        fn signal(&self) -> Arc<dyn crate::MailboxSignal> {
            Arc::new(BindOrderingSignal {
                inner: self.inner.signal(),
                events: Arc::clone(&self.events),
            })
        }

        fn dispose(&self, value: Box<dyn Send + 'static>) {
            self.events
                .lock()
                .expect("bind effect recorder mutex")
                .push(BindEffectEvent::DisposalSubmitted);
            self.inner.dispose(value);
        }

        fn now(&self) -> Instant {
            self.inner.now()
        }

        fn sleep_until(&self, deadline: Option<Instant>) -> crate::BoxedSleep {
            self.inner.sleep_until(deadline)
        }
    }

    /// Runtime whose change signal panics once armed, so a mailbox effect
    /// flush can be made to unwind on demand.
    struct PanickingPulseRuntime {
        inner: Arc<dyn crate::MailboxRuntime>,
        armed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl crate::MailboxRuntime for PanickingPulseRuntime {
        fn oneshot(
            &self,
        ) -> (
            Box<dyn crate::ErasedOneShotSender>,
            Pin<Box<dyn crate::ErasedOneShotReceiver>>,
        ) {
            self.inner.oneshot()
        }

        fn signal(&self) -> Arc<dyn crate::MailboxSignal> {
            Arc::new(PanickingPulseSignal {
                inner: self.inner.signal(),
                armed: Arc::clone(&self.armed),
            })
        }

        fn dispose(&self, value: Box<dyn Send + 'static>) {
            self.inner.dispose(value);
        }

        fn now(&self) -> Instant {
            self.inner.now()
        }

        fn sleep_until(&self, deadline: Option<Instant>) -> crate::BoxedSleep {
            self.inner.sleep_until(deadline)
        }
    }

    struct PanickingPulseSignal {
        inner: Arc<dyn crate::MailboxSignal>,
        armed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl crate::MailboxSignal for PanickingPulseSignal {
        fn pulse(&self) {
            assert!(
                !self.armed.load(Ordering::SeqCst),
                "injected mailbox pulse panic"
            );
            self.inner.pulse();
        }

        fn watcher(&self) -> Box<dyn crate::MailboxSignalWatcher> {
            self.inner.watcher()
        }
    }

    /// User message recording the thread its destructor ran on.
    struct ThreadRecordingMessage(Option<mpsc::Sender<std::thread::ThreadId>>);

    impl Drop for ThreadRecordingMessage {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(std::thread::current().id());
            }
        }
    }

    struct BindOrderingSignal {
        inner: Arc<dyn crate::MailboxSignal>,
        events: Arc<Mutex<Vec<BindEffectEvent>>>,
    }

    impl crate::MailboxSignal for BindOrderingSignal {
        fn pulse(&self) {
            self.events
                .lock()
                .expect("bind effect recorder mutex")
                .push(BindEffectEvent::SignalPulsed);
            self.inner.pulse();
        }

        fn watcher(&self) -> Box<dyn crate::MailboxSignalWatcher> {
            self.inner.watcher()
        }
    }

    struct BindOrderingWake(Arc<Mutex<Vec<BindEffectEvent>>>);

    impl Wake for BindOrderingWake {
        fn wake(self: Arc<Self>) {
            self.0
                .lock()
                .expect("bind effect recorder mutex")
                .push(BindEffectEvent::SenderWoken);
        }
    }

    struct ReentrantPanicDrop {
        mailbox: Weak<MailboxCell<u8>>,
        operation: Weak<super::SendOperation<u8>>,
        drops: Arc<AtomicUsize>,
    }

    impl Wake for ReentrantPanicDrop {
        fn wake(self: Arc<Self>) {
            panic!("the withdrawal regression only drops its waker")
        }
    }

    impl Drop for ReentrantPanicDrop {
        fn drop(&mut self) {
            let operation = self
                .operation
                .upgrade()
                .expect("the cancelling send retains its operation");
            drop(
                operation
                    .state
                    .try_lock()
                    .expect("waker drop runs after the operation lock is released"),
            );

            let mailbox = self
                .mailbox
                .upgrade()
                .expect("the cancelling send retains its mailbox");
            drop(
                mailbox
                    .state
                    .try_lock()
                    .expect("waker drop runs after the mailbox lock is released"),
            );
            let error = mailbox
                .try_send(2)
                .expect_err("a reentrant try-send observes the unbound mailbox");
            assert_eq!(error.kind, SendErrorKind::NotRunning);
            assert_eq!(error.message, 2);

            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("injected waker drop panic");
        }
    }

    struct TestIdentity {
        id: ChildId,
        membership: crate::Membership,
    }

    impl ActorIdentity for TestIdentity {
        fn id(&self) -> &ChildId {
            &self.id
        }

        fn membership(&self) -> crate::Membership {
            self.membership
        }
    }

    pub(crate) fn actor_for<M: Send + 'static>() -> (Arc<MailboxCell<M>>, ActorRef<M>) {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("actor");
        let member = Arc::new(TestIdentity {
            id: id.clone(),
            membership: identity
                .mint_membership(&id)
                .expect("membership available")
                .into_pair()
                .0,
        });
        let mailbox = MailboxCell::new(id, crate::capability::tests::runtime());
        (
            Arc::clone(&mailbox),
            crate::actor_ref_from_parts(member, mailbox),
        )
    }

    pub(crate) fn actor() -> (Arc<MailboxCell<u8>>, ActorRef<u8>) {
        actor_for()
    }

    fn configure<M: Send + 'static>(
        mailbox: &MailboxCell<M>,
        policy: ResolvedMailbox,
    ) -> crate::MailboxBindToken {
        let mut effects = crate::MailboxEffectQueue::default();
        MailboxControl::configure(mailbox, policy, &mut effects)
    }

    fn bind<M: Send + 'static>(
        mailbox: &MailboxCell<M>,
        token: crate::MailboxBindToken,
        incarnation: Incarnation,
    ) {
        let mut effects = crate::MailboxEffectQueue::default();
        MailboxControl::bind(mailbox, token, incarnation, &mut effects);
    }

    fn prepare_termination<M: Send + 'static>(
        mailbox: &MailboxCell<M>,
    ) -> Option<Box<dyn crate::MailboxTermination>> {
        let mut effects = crate::MailboxEffectQueue::default();
        MailboxControl::prepare_termination(mailbox, &mut effects)
    }

    fn park_with(future: &mut std::pin::Pin<Box<crate::SendFuture<u8>>>, waker: &Waker) {
        let mut context = Context::from_waker(waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    fn configuration_mismatch_panics_after_releasing_the_mailbox_lock() {
        let (mailbox, _) = actor();
        let queue = ResolvedMailbox::Queue(
            std::num::NonZeroUsize::new(1).expect("non-zero queue capacity"),
        );
        let _ = configure(&mailbox, queue);
        let _ = configure(&mailbox, queue);

        let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
            let _ = configure(&mailbox, ResolvedMailbox::Latest);
        })) else {
            panic!("changing mailbox kind must trip the driver contract")
        };
        assert!(
            panic
                .downcast_ref::<String>()
                .is_some_and(|message| message.contains("mailbox configuration changed"))
        );
        assert_eq!(
            mailbox
                .state
                .lock()
                .expect("configuration panic occurs after unlocking")
                .kind,
            Some(super::MailboxKind::Queue(
                std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")
            ))
        );
    }

    #[test]
    fn bound_waiters_exist_only_in_the_full_state() {
        let (mailbox, _) = actor();
        let token = configure(
            &mailbox,
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
        bind(&mailbox, token, incarnation);

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

        let mut withdrawal = mailbox.withdraw(&operation, super::WithdrawalDisposition::Inline);
        assert!(matches!(
            withdrawal.take_outcome(),
            super::WithdrawalOutcome::Withdrawn { message: 2, .. }
        ));
        withdrawal.finish();
        assert!(matches!(
            mailbox.state.lock().expect("mailbox mutex poisoned").binding,
            super::MailboxBinding::Bound(super::BoundState::Available(bound))
                if bound == incarnation
        ));
    }

    #[test]
    fn receive_promotes_multiple_parked_senders_in_fifo_order() {
        let (mailbox, actor) = actor();
        let token = configure(
            &mailbox,
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
        bind(&mailbox, token, incarnation);
        let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);

        assert!(matches!(actor.try_send(0), Ok(bound) if bound == incarnation));
        let mut sends: Vec<_> = (1_u8..=3)
            .map(|message| Box::pin(actor.send(message)))
            .collect();
        for send in &mut sends {
            park_with(send, Waker::noop());
        }

        let send_count = sends.len();
        for (delivered, send) in sends.iter_mut().enumerate() {
            assert_eq!(receiver.try_recv(), Some(delivered as u8));
            assert!(matches!(
                send.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(bound)) if bound == incarnation
            ));

            let remaining = send_count - delivered - 1;
            if remaining > 0 {
                assert!(matches!(
                    &mailbox.state.lock().expect("mailbox mutex poisoned").binding,
                    super::MailboxBinding::Bound(super::BoundState::Full { waiters, .. })
                        if waiters.len() == remaining
                ));
            }
        }

        assert_eq!(receiver.try_recv(), Some(3));
        assert_eq!(receiver.try_recv(), None);
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
    fn withdrawal_releases_its_waker_instead_of_destroying_it_under_the_locks() {
        let (mailbox, _) = actor();
        let operation = match mailbox.submit(1) {
            super::Submission::Parked(operation) => operation,
            super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
                panic!("an unbound mailbox parks its send")
            }
        };
        let drops = Arc::new(AtomicUsize::new(0));
        let hostile = Waker::from(Arc::new(ReentrantPanicDrop {
            mailbox: Arc::downgrade(&mailbox),
            operation: Arc::downgrade(&operation),
            drops: Arc::clone(&drops),
        }));
        operation.install_test_waker(hostile.clone());
        drop(hostile);

        let mut withdrawal = mailbox.withdraw(&operation, super::WithdrawalDisposition::Inline);
        assert!(matches!(
            withdrawal.take_outcome(),
            super::WithdrawalOutcome::Withdrawn { message: 1, .. }
        ));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "withdrawal releases the waker rather than running its destructor"
        );
        // Finishing the explicit effect set runs its waker destructor with
        // neither core lock held, so the reentrant probe inside it succeeds
        // and its panic reaches only that owner.
        let Err(panic) = catch_unwind(AssertUnwindSafe(move || withdrawal.finish())) else {
            panic!("the hostile waker drop panic reaches whoever destroys it")
        };
        assert_eq!(
            panic.downcast_ref::<&'static str>().copied(),
            Some("injected waker drop panic")
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(
            operation
                .state
                .lock()
                .expect("hostile waker drop cannot poison the operation lock"),
        );
        let state = mailbox
            .state
            .lock()
            .expect("hostile waker drop cannot poison the mailbox lock");
        assert!(
            state
                .waiters()
                .expect("an unbound mailbox retains its waiter queue")
                .is_empty()
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
        let token = configure(&mailbox, ResolvedDefaults::default().mailbox);
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
                bind(&mailbox, token, incarnation);
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
    fn bind_submits_displaced_payloads_for_disposal_before_waking_senders() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let runtime = Arc::new(BindOrderingRuntime {
            inner: crate::capability::tests::runtime(),
            events: Arc::clone(&events),
        });
        let mailbox = MailboxCell::new(ChildId::from("actor"), runtime);
        let token = configure(&mailbox, ResolvedMailbox::Latest);

        let first = match mailbox.submit(1) {
            super::Submission::Parked(operation) => operation,
            super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
                panic!("an unbound mailbox parks its send")
            }
        };
        let second = match mailbox.submit(2) {
            super::Submission::Parked(operation) => operation,
            super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
                panic!("an unbound mailbox parks its send")
            }
        };
        first.install_test_waker(Waker::from(Arc::new(BindOrderingWake(Arc::clone(&events)))));
        second.install_test_waker(Waker::from(Arc::new(BindOrderingWake(Arc::clone(&events)))));

        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        bind(
            &mailbox,
            token,
            incarnations.mint().expect("incarnation available"),
        );

        assert_eq!(
            *events.lock().expect("bind effect recorder mutex"),
            [
                BindEffectEvent::SignalPulsed,
                BindEffectEvent::DisposalSubmitted,
                BindEffectEvent::SenderWoken,
                BindEffectEvent::SenderWoken,
            ],
            "binding preserves pulse, disposal-submission, then wake ordering"
        );
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
                drop(prepare_termination(&mailbox));
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

        let teardown =
            prepare_termination(&mailbox).expect("live mailbox prepares terminal teardown");
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

    #[crate::runtime::test(start_paused = true)]
    async fn expired_timeout_can_race_detached_terminal_teardown() {
        let (mailbox, actor) = actor();
        let width = Duration::from_secs(1);
        let mut send = Box::pin(actor.send_timeout(1, width));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        crate::runtime::advance(width * 2).await;

        let teardown =
            prepare_termination(&mailbox).expect("live mailbox prepares terminal teardown");
        // Teardown remains retained: timeout sees terminal binding after the
        // waiter queue was detached but before its waiter was discharged.
        let Poll::Ready(Err(error)) = send.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("the expired send withdraws before deferred discharge");
        };
        assert_eq!(error.kind, SendErrorKind::TimedOut);
        assert_eq!(error.message, 1);
        drop(teardown);

        let retry = actor
            .try_send(2)
            .expect_err("the terminal mailbox rejects a retry");
        assert_eq!(retry.kind, SendErrorKind::Terminated);
        assert_eq!(retry.incarnation_observed, None);
    }

    #[test]
    fn stale_waiter_id_cannot_unlink_a_later_registration() {
        let mut waiters = super::WaiterQueue::default();
        let first = super::SendOperation::new(1_u8);
        let first_id = waiters
            .push_back(Arc::clone(&first))
            .expect("first waiter id available");
        first.register(first_id);
        let removed = waiters.remove(first_id).expect("first waiter is live");
        removed.clear_registration(first_id);

        let second = super::SendOperation::new(2_u8);
        let second_id = waiters
            .push_back(Arc::clone(&second))
            .expect("second waiter id available");
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
        let first_id = waiters.push_back(Arc::clone(&first)).expect("first id");
        let second_id = waiters.push_back(Arc::clone(&second)).expect("second id");
        let third_id = waiters.push_back(Arc::clone(&third)).expect("third id");

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
        assert_eq!(last, Some(super::WaiterId(u64::MAX - 1)));

        assert_eq!(waiters.push_back(super::SendOperation::new(2_u8)), None);
        assert!(waiters.ids.is_poisoned());
        assert!(!waiters.entries.contains_key(&super::WaiterId::POISON));
        assert_eq!(waiters.push_back(super::SendOperation::new(3_u8)), None);
    }

    #[test]
    fn accepted_sequence_exhaustion_poison_is_never_minted() {
        let accepted = crate::identity::AtomicPoisonedCounter::near_exhaustion();
        assert_eq!(
            super::mint_accepted_sequence(&accepted),
            Some(super::AcceptedSequence(u64::MAX - 1))
        );
        assert_eq!(super::mint_accepted_sequence(&accepted), None);
        assert_eq!(super::mint_accepted_sequence(&accepted), None);
    }

    #[test]
    fn waiter_identity_exhaustion_drops_the_message_after_unlock() {
        let mailbox = MailboxCell::new(ChildId::from("actor"), crate::capability::tests::runtime());
        mailbox.state.lock().expect("mailbox state").binding =
            super::MailboxBinding::Unbound(super::WaiterQueue {
                ids: crate::identity::PoisonedCounter::near_exhaustion(),
                ..super::WaiterQueue::default()
            });
        let weak = Arc::downgrade(&mailbox);
        assert!(matches!(
            mailbox.submit(LockCheckingMessage {
                mailbox: Weak::clone(&weak),
                dropped: None,
            }),
            super::Submission::Parked(_)
        ));
        let (dropped, observed) = mpsc::channel();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = mailbox.submit(LockCheckingMessage {
                mailbox: weak,
                dropped: Some(dropped),
            });
        }));
        assert!(panic.is_err(), "exhaustion is reported to the caller");
        assert!(
            observed
                .recv_timeout(Duration::from_secs(5))
                .expect("isolated message destructor reports"),
            "the exhausted message is destroyed outside the mailbox mutex"
        );
    }

    #[test]
    fn accepted_sequence_exhaustion_drops_the_message_after_unlock() {
        let mut mailbox =
            MailboxCell::new(ChildId::from("actor"), crate::capability::tests::runtime());
        Arc::get_mut(&mut mailbox)
            .expect("mailbox is uniquely owned")
            .accepted = crate::identity::AtomicPoisonedCounter::near_exhaustion();
        let token = configure(&mailbox, ResolvedMailbox::Latest);
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        bind(
            &mailbox,
            token,
            incarnations.mint().expect("incarnation available"),
        );
        let weak = Arc::downgrade(&mailbox);
        assert!(matches!(
            mailbox.submit(LockCheckingMessage {
                mailbox: Weak::clone(&weak),
                dropped: None,
            }),
            super::Submission::Accepted(_)
        ));
        let (dropped, observed) = mpsc::channel();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = mailbox.submit(LockCheckingMessage {
                mailbox: weak,
                dropped: Some(dropped),
            });
        }));
        assert!(panic.is_err(), "exhaustion is reported to the caller");
        assert!(
            observed
                .recv_timeout(Duration::from_secs(5))
                .expect("isolated message destructor reports"),
            "the exhausted message is destroyed outside the mailbox mutex"
        );
    }

    #[test]
    fn promotion_sequence_exhaustion_isolates_the_received_message_before_panicking() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let runtime = Arc::new(BindOrderingRuntime {
            inner: crate::capability::tests::runtime(),
            events: Arc::clone(&events),
        });
        let mut mailbox = MailboxCell::new(ChildId::from("actor"), runtime);
        Arc::get_mut(&mut mailbox)
            .expect("mailbox is uniquely owned")
            .accepted = crate::identity::AtomicPoisonedCounter::near_exhaustion();
        let token = configure(
            &mailbox,
            ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero capacity")),
        );
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        let incarnation = incarnations.mint().expect("incarnation available");
        bind(&mailbox, token, incarnation);
        let weak = Arc::downgrade(&mailbox);
        let (dropped, observed) = mpsc::channel();
        assert!(matches!(
            mailbox.submit(LockCheckingMessage {
                mailbox: Weak::clone(&weak),
                dropped: Some(dropped),
            }),
            super::Submission::Accepted(_)
        ));
        let operation = match mailbox.submit(LockCheckingMessage {
            mailbox: weak,
            dropped: None,
        }) {
            super::Submission::Parked(operation) => operation,
            super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
                panic!("the full queue parks the second message")
            }
        };
        let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);

        assert!(catch_unwind(AssertUnwindSafe(|| receiver.try_recv())).is_err());
        drop(
            mailbox
                .state
                .lock()
                .expect("the exhaustion panic occurs after mailbox unlock"),
        );
        assert!(
            events
                .lock()
                .expect("effect recorder mutex")
                .contains(&BindEffectEvent::DisposalSubmitted),
            "the live return value is submitted for isolated disposal before unwind"
        );
        assert!(
            observed
                .recv_timeout(Duration::from_secs(5))
                .expect("isolated returned-message destructor reports"),
            "the returned message is destroyed outside the mailbox mutex"
        );
        let mut withdrawal = mailbox.withdraw(&operation, super::WithdrawalDisposition::Inline);
        assert!(matches!(
            withdrawal.take_outcome(),
            super::WithdrawalOutcome::Withdrawn { .. }
        ));
        withdrawal.finish();
    }

    #[test]
    fn a_panicking_close_flush_isolates_the_unread_payload() {
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let runtime = Arc::new(PanickingPulseRuntime {
            inner: crate::capability::tests::runtime(),
            armed: Arc::clone(&armed),
        });
        let mailbox = MailboxCell::new(ChildId::from("actor"), runtime);
        let token = configure(&mailbox, ResolvedMailbox::Latest);
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        let incarnation = incarnations.mint().expect("incarnation available");
        bind(&mailbox, token, incarnation);
        let (dropped, observed) = mpsc::channel();
        assert!(matches!(
            mailbox.submit(ThreadRecordingMessage(Some(dropped))),
            super::Submission::Accepted(_)
        ));
        armed.store(true, Ordering::SeqCst);

        // The driver shape: the close result is a live local across the
        // effects flush, which wakes registered wakers synchronously and can
        // therefore unwind on user code.
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let mut effects = crate::MailboxEffectQueue::default();
            let closed = MailboxControl::close(&*mailbox, incarnation, &mut effects);
            drop(effects);
            if let Some(closed) = closed {
                let (_token, disposal) = closed.into_parts();
                drop(disposal);
            }
        }));
        assert!(panic.is_err(), "the flush panic reaches the caller");

        let destructor = observed
            .recv_timeout(Duration::from_secs(5))
            .expect("the unread message destructor reports");
        assert_ne!(
            destructor,
            std::thread::current().id(),
            "an unwinding close flush must not destroy unread user messages on the caller's thread"
        );
    }

    #[test]
    fn bind_sequence_exhaustion_retains_parked_senders_after_unlock() {
        let mut mailbox =
            MailboxCell::new(ChildId::from("actor"), crate::capability::tests::runtime());
        Arc::get_mut(&mut mailbox)
            .expect("mailbox is uniquely owned")
            .accepted = crate::identity::AtomicPoisonedCounter::near_exhaustion();
        let token = configure(&mailbox, ResolvedMailbox::Latest);
        let weak = Arc::downgrade(&mailbox);
        let first = match mailbox.submit(LockCheckingMessage {
            mailbox: Weak::clone(&weak),
            dropped: None,
        }) {
            super::Submission::Parked(operation) => operation,
            super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
                panic!("an unbound mailbox parks its send")
            }
        };
        let (dropped, observed) = mpsc::channel();
        let second = match mailbox.submit(LockCheckingMessage {
            mailbox: weak,
            dropped: Some(dropped),
        }) {
            super::Submission::Parked(operation) => operation,
            super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
                panic!("an unbound mailbox parks its send")
            }
        };
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        let incarnation = incarnations.mint().expect("incarnation available");

        // Promotion mints one accepted sequence and then runs out, leaving a
        // latest mailbox with a waiter still parked.
        let panic = catch_unwind(AssertUnwindSafe(|| {
            bind(&mailbox, token, incarnation);
        }));
        assert!(panic.is_err(), "exhaustion is reported to the caller");
        drop(
            mailbox
                .state
                .lock()
                .expect("the exhaustion panic occurs after mailbox unlock"),
        );

        let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);
        assert!(
            receiver.try_recv().is_some(),
            "the promoted message survives the exhaustion verdict"
        );
        let mut withdrawal = mailbox.withdraw(&second, super::WithdrawalDisposition::Inline);
        assert!(
            matches!(
                withdrawal.take_outcome(),
                super::WithdrawalOutcome::Withdrawn { .. }
            ),
            "the unpromotable sender still owns its message"
        );
        withdrawal.finish();
        assert!(
            observed
                .recv_timeout(Duration::from_secs(5))
                .expect("the parked message destructor reports"),
            "the parked message is destroyed outside the mailbox mutex"
        );
        drop(first);
    }
}
