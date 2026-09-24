use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use crate::{
    identity::{AtomicMonotonicCounter, ChildId, Incarnation, MonotonicCounter},
    mailbox::{
        MailboxBindToken, MailboxClose, MailboxControl, MailboxDisposal, MailboxEffectQueue,
        MailboxEffectSink, MailboxRuntime, MailboxSignal, MailboxSignalWatcher, MailboxTermination,
        capability::{dispose, dispose_value},
    },
    policy::ResolvedMailbox,
    runtime::{PanicAccumulator, PanicPayload, resume_panic},
};
use shelterwood_core::waker::{WakerAction, WakerEffects, WakerSlot};

use super::{SendError, SendErrorKind};

/// Where the mailbox is in its binding lifecycle.
///
/// The phase is plain framework data and owns nothing: parked senders live in
/// `MailboxState::waiters` beside it, so no phase change can displace a live
/// waiter, and a "full" bound mailbox is simply `Bound` with waiters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Unbound,
    Bound(Binding),
    Frozen(Binding),
    Terminal(Option<Incarnation>),
}

/// The incarnation a mailbox is bound to, and the kind `bind` verified was
/// configured before admitting it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Binding {
    incarnation: Incarnation,
    kind: ResolvedMailbox,
}

impl Phase {
    fn observation(self) -> Option<Incarnation> {
        match self {
            Self::Bound(binding) | Self::Frozen(binding) => Some(binding.incarnation),
            Self::Unbound | Self::Terminal(_) => None,
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

pub(super) struct Envelope<M> {
    message: M,
    accepted_sequence: AcceptedSequence,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct AcceptedSequence(u64);

/// A send operation's outcome. Each message-carrying variant owns its message
/// by value; leaving it moves the message out and installs the successor in
/// the same critical section.
pub(super) enum OperationOutcome<M> {
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
    fn new(
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

    fn clear_registration(&self) {
        self.state
            .lock()
            .expect("send operation mutex poisoned")
            .registration = None;
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

    /// Leaves `Waiting` for `Accepted`, moving the message out. Any other
    /// outcome is left in place and yields `None`.
    fn accept(&self, incarnation: Incarnation, effects: &mut WakerEffects) -> Option<M> {
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

    fn terminate(&self, final_incarnation: Option<Incarnation>) {
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

    pub(super) fn poll(
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
    fn install_test_waker(&self, waker: std::task::Waker) {
        let mut effects = WakerEffects::default();
        {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            state.waker.replace(waker, &mut effects);
        }
    }
}

pub(super) struct MailboxState<M> {
    kind: Option<ResolvedMailbox>,
    bind_permit: Arc<AtomicBool>,
    phase: Phase,
    last_bound: Option<Incarnation>,
    /// Senders parked behind an unbound, frozen, or full mailbox, in FIFO
    /// order. Terminalization is the only transition that detaches them.
    waiters: WaiterQueue<M>,
    pub(super) queue: VecDeque<Envelope<M>>,
    latest: Option<Envelope<M>>,
}

impl<M> MailboxState<M> {
    /// Dequeues the next envelope this receive mode is willing to observe.
    fn take_next(&mut self, kind: ResolvedMailbox, mode: ReceiveMode) -> Option<Envelope<M>> {
        match kind {
            ResolvedMailbox::Queue(_) => self
                .queue
                .front()
                .is_some_and(|item| mode.accepts(item.accepted_sequence))
                .then(|| self.queue.pop_front())
                .flatten(),
            ResolvedMailbox::Latest => self
                .latest
                .as_ref()
                .is_some_and(|item| mode.accepts(item.accepted_sequence))
                .then(|| self.latest.take())
                .flatten(),
        }
    }

    #[cfg(test)]
    pub(super) fn waiters(&self) -> &WaiterQueue<M> {
        &self.waiters
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

/// FIFO registrations with direct removal by a send operation.
///
/// Monotonic keys are insertion order, so the first map entry is the oldest
/// waiter. The mailbox owns one queue for its whole non-terminal life, so a
/// key is never reused and a stale cancellation id is harmless.
/// Terminalization detaches the queue and discharges those registrations
/// after unlocking; nothing parks behind a terminal mailbox.
pub(super) struct WaiterQueue<M> {
    entries: BTreeMap<WaiterId, Arc<SendOperation<M>>>,
    ids: MonotonicCounter,
    #[cfg(test)]
    direct_removals: usize,
}

impl<M> Default for WaiterQueue<M> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            ids: MonotonicCounter::new(),
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

    /// Parks a fresh operation, registered under a newly minted id.
    fn park(&mut self, message: M, newest_observed: Option<Incarnation>) -> Arc<SendOperation<M>> {
        let registration = WaiterId(self.ids.mint());
        let operation = SendOperation::new(message, newest_observed, Some(registration));
        self.entries.insert(registration, Arc::clone(&operation));
        operation
    }

    fn observe_all(&self, incarnation: Incarnation) {
        for operation in self.entries.values() {
            operation.observe(incarnation);
        }
    }

    fn pop_front(&mut self) -> Option<Arc<SendOperation<M>>> {
        let (_, operation) = self.entries.pop_first()?;
        #[cfg(test)]
        {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        Some(operation)
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

/// The complete movable effect state for one mailbox transition.
///
/// `MailboxEffects` owns this value and moves it wholesale into the eventual
/// batch, so adding an effect cannot require restating its field at that seam.
struct MailboxEffectPayload<M> {
    pulse: bool,
    displaced: Vec<Envelope<M>>,
    isolate_displaced: bool,
    wakers: WakerEffects,
}

impl<M> Default for MailboxEffectPayload<M> {
    fn default() -> Self {
        Self {
            pulse: false,
            displaced: Vec::new(),
            isolate_displaced: false,
            wakers: WakerEffects::default(),
        }
    }
}

impl<M> MailboxEffectPayload<M> {
    fn is_empty(&self) -> bool {
        // Destructured rather than field-accessed: a new effect field is then
        // a compile error here instead of an effect silently discarded
        // whenever it is the only one this transition set.
        let Self {
            pulse,
            displaced,
            // `isolate_displaced` only selects how a nonempty displaced set is
            // flushed; by itself it represents no work.
            isolate_displaced: _,
            wakers,
        } = self;
        !pulse && displaced.is_empty() && wakers.is_empty()
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
    payload: MailboxEffectPayload<M>,
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
            payload: MailboxEffectPayload::default(),
        }
    }

    fn pulse(&mut self) {
        self.payload.pulse = true;
    }

    fn isolate_displaced(&mut self) {
        self.payload.isolate_displaced = true;
    }
}

impl<M: Send + 'static> Deref for MailboxEffects<'_, '_, M> {
    type Target = MailboxEffectPayload<M>;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<M: Send + 'static> DerefMut for MailboxEffects<'_, '_, M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.payload
    }
}

impl<M: Send + 'static> Drop for MailboxEffects<'_, '_, M> {
    fn drop(&mut self) {
        if self.payload.is_empty() {
            return;
        }
        let batch = MailboxEffectBatch {
            changed: Arc::clone(&self.cell.changed),
            runtime: Arc::clone(&self.cell.runtime),
            payload: std::mem::take(&mut self.payload),
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
    payload: MailboxEffectPayload<M>,
}

/// A received message, held outside the transaction that dequeued it.
///
/// `receive` declares it before its `MailboxTxn`, so every unwind — one out of
/// the locked transition as much as a panicking pulse, waker, or displaced
/// payload in the post-unlock flush — drops the transaction first and then
/// submits the message for detached disposal instead of destroying it on the
/// receiving caller's stack.
struct ReturnedMessage<'a, M: Send + 'static> {
    value: Option<M>,
    runtime: &'a Arc<dyn MailboxRuntime>,
}

impl<M: Send + 'static> Drop for ReturnedMessage<'_, M> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            dispose(self.runtime, value);
        }
    }
}

impl<M: Send + 'static> MailboxEffectBatch<M> {
    fn flush(self) {
        let Self {
            changed,
            runtime,
            mut payload,
        } = self;
        let mut panics = PanicAccumulator::default();
        if payload.pulse {
            panics.run(|| changed.pulse());
        }
        // Submit displaced latest-value payloads to isolated disposal before
        // waking accepted senders: a woken sender may run immediately and must
        // not race ahead of disposal submission.
        if payload.isolate_displaced && !payload.displaced.is_empty() {
            let isolated = std::mem::take(&mut payload.displaced);
            panics.run(|| {
                dispose_value(
                    runtime.as_ref(),
                    MailboxPayload::unread(isolated.into(), None),
                );
            });
        }
        payload.wakers.flush(&mut panics);
        if !payload.isolate_displaced {
            // A live latest transition displaces at most the one value it
            // replaced, so per-element containment here is totality for a
            // future multi-displacement caller rather than a tested behavior;
            // `bind`, the only caller that can accumulate several, isolates
            // them into a `MailboxPayload` above instead.
            for envelope in payload.displaced.drain(..) {
                panics.run(|| drop(envelope));
            }
        }
    }
}

/// A mailbox transition guard paired with its mandatory post-unlock effects.
///
/// It exposes immutable state through `Deref`. Mutation is either a named
/// transition on the transaction or a `parts()` pairing that hands the state
/// out beside its sink, so no mutation happens without the effects sink in
/// scope.
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

    fn configure_kind(&mut self, kind: ResolvedMailbox) -> Option<ResolvedMailbox> {
        let state = self.state_mut();
        match state.kind {
            Some(existing) => (existing != kind).then_some(existing),
            None => {
                state.kind = Some(kind);
                None
            }
        }
    }

    /// Parks a fresh send operation behind this mailbox's waiter queue.
    fn park(&mut self, message: M, newest_observed: Option<Incarnation>) -> Submission<M> {
        Submission::Parked(self.state_mut().waiters.park(message, newest_observed))
    }

    fn set_phase(&mut self, phase: Phase) {
        self.state_mut().phase = phase;
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
        while let Some(waiter) = self.waiters.pop_front() {
            waiter.clear_registration();
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
    accepted: AtomicMonotonicCounter,
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
    // Only the façade can pair a mailbox with the runtime capability object
    // selected by its private adapter.
    pub(crate) fn new(actor_id: ChildId, runtime: Arc<dyn MailboxRuntime>) -> Arc<Self> {
        let changed = runtime.signal();
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
            runtime,
            changed,
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
        let mut returned = ReturnedMessage {
            value: None,
            runtime: &self.runtime,
        };
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
            runtime: Arc::clone(&self.runtime),
            changed: Some(Arc::clone(&self.changed)),
            payload: Some(payload),
            termination: Some(termination),
        }) as Box<dyn MailboxTermination>);
        transaction.finish(teardown)
    }
}

fn mint_accepted_sequence(accepted: &AtomicMonotonicCounter) -> AcceptedSequence {
    AcceptedSequence(accepted.mint(Ordering::Release, Ordering::Relaxed))
}

/// Accepts `message` into a mailbox bound to `binding`, or hands it back when
/// the mailbox is full. Parked senders are older than this one, so any waiter
/// makes the mailbox full.
fn accept_locked<M: Send + 'static>(
    state: &mut MailboxState<M>,
    binding: Binding,
    message: M,
    accepted: &AtomicMonotonicCounter,
    effects: &mut MailboxEffects<'_, '_, M>,
) -> Result<Incarnation, M> {
    if !state.waiters.is_empty() {
        return Err(message);
    }
    match binding.kind {
        ResolvedMailbox::Queue(capacity) if state.queue.len() >= capacity.get() => {
            return Err(message);
        }
        ResolvedMailbox::Queue(_) | ResolvedMailbox::Latest => {}
    }
    push_accepted(state, binding.kind, message, accepted, effects);
    effects.pulse();
    Ok(binding.incarnation)
}

/// Appends an accepted message; a latest mailbox displaces its prior value
/// into the effects.
fn push_accepted<M: Send + 'static>(
    state: &mut MailboxState<M>,
    kind: ResolvedMailbox,
    message: M,
    accepted: &AtomicMonotonicCounter,
    effects: &mut MailboxEffects<'_, '_, M>,
) {
    let envelope = Envelope {
        message,
        accepted_sequence: mint_accepted_sequence(accepted),
    };
    match kind {
        ResolvedMailbox::Queue(_) => state.queue.push_back(envelope),
        ResolvedMailbox::Latest => {
            if let Some(displaced) = state.latest.replace(envelope) {
                effects.displaced.push(displaced);
            }
        }
    }
}

/// Accepts parked senders, oldest first, while the bound mailbox has room.
fn promote_waiters<M: Send + 'static>(
    state: &mut MailboxState<M>,
    binding: Binding,
    accepted: &AtomicMonotonicCounter,
    effects: &mut MailboxEffects<'_, '_, M>,
) {
    loop {
        if let ResolvedMailbox::Queue(capacity) = binding.kind
            && state.queue.len() >= capacity.get()
        {
            return;
        }
        let Some(operation) = state.waiters.pop_front() else {
            return;
        };
        operation.clear_registration();
        operation.observe(binding.incarnation);
        if let Some(message) = operation.accept(binding.incarnation, &mut effects.wakers) {
            push_accepted(state, binding.kind, message, accepted, effects);
        }
        // `operation` is the queue's popped reference; its sender still owns
        // one, so this drop under the lock is refcount traffic.
    }
}

#[derive(Clone, Copy)]
pub(super) enum WithdrawalDisposition {
    Inline,
    Isolated,
}

impl WithdrawalDisposition {
    fn action(self) -> WakerAction {
        match self {
            Self::Inline => WakerAction::DropInline,
            Self::Isolated => WakerAction::Run(crate::runtime::dispose_waker),
        }
    }
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

    pub(super) fn take_outcome_if_present(&mut self) -> Option<WithdrawalOutcome<M>> {
        self.outcome.take()
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
