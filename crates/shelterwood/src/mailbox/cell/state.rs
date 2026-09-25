use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    identity::{AtomicMonotonicCounter, Incarnation, MonotonicCounter},
    policy::ResolvedMailbox,
};

use super::{effects::MailboxEffects, operation::SendOperation};

/// Where the mailbox is in its binding lifecycle.
///
/// The phase is plain framework data and owns nothing: parked senders live in
/// `MailboxState::waiters` beside it, so no phase change can displace a live
/// waiter, and a "full" bound mailbox is simply `Bound` with waiters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Unbound,
    Bound(Binding),
    Frozen(Binding),
    Terminal(Option<Incarnation>),
}

/// The incarnation a mailbox is bound to, and the kind `bind` verified was
/// configured before admitting it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Binding {
    pub(super) incarnation: Incarnation,
    pub(super) kind: ResolvedMailbox,
}

impl Phase {
    pub(super) fn observation(self) -> Option<Incarnation> {
        match self {
            Self::Bound(binding) | Self::Frozen(binding) => Some(binding.incarnation),
            Self::Unbound | Self::Terminal(_) => None,
        }
    }
}

/// The structurally valid receive domains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReceiveMode {
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

pub(in crate::mailbox) struct Envelope<M> {
    pub(super) message: M,
    accepted_sequence: AcceptedSequence,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct AcceptedSequence(pub(super) u64);
pub(in crate::mailbox) struct MailboxState<M> {
    pub(super) kind: Option<ResolvedMailbox>,
    pub(super) bind_permit: Arc<AtomicBool>,
    pub(super) phase: Phase,
    pub(super) last_bound: Option<Incarnation>,
    /// Senders parked behind an unbound, frozen, or full mailbox, in FIFO
    /// order. Terminalization is the only transition that detaches them.
    pub(super) waiters: WaiterQueue<M>,
    pub(in crate::mailbox) queue: VecDeque<Envelope<M>>,
    pub(super) latest: Option<Envelope<M>>,
}

impl<M> MailboxState<M> {
    /// Dequeues the next envelope this receive mode is willing to observe.
    pub(super) fn take_next(
        &mut self,
        kind: ResolvedMailbox,
        mode: ReceiveMode,
    ) -> Option<Envelope<M>> {
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
    pub(in crate::mailbox) fn waiters(&self) -> &WaiterQueue<M> {
        &self.waiters
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct WaiterId(u64);

/// FIFO registrations with direct removal by a send operation.
///
/// Monotonic keys are insertion order, so the first map entry is the oldest
/// waiter. The mailbox owns one queue for its whole non-terminal life, so a
/// key is never reused and a stale cancellation id is harmless.
/// Terminalization detaches the queue and discharges those registrations
/// after unlocking; nothing parks behind a terminal mailbox.
pub(in crate::mailbox) struct WaiterQueue<M> {
    entries: BTreeMap<WaiterId, Arc<SendOperation<M>>>,
    ids: MonotonicCounter,
    #[cfg(test)]
    pub(super) direct_removals: usize,
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
    pub(in crate::mailbox) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Parks a fresh operation, registered under a newly minted id.
    pub(super) fn park(
        &mut self,
        message: M,
        newest_observed: Option<Incarnation>,
    ) -> Arc<SendOperation<M>> {
        let registration = WaiterId(self.ids.mint());
        let operation = SendOperation::new(message, newest_observed, Some(registration));
        self.entries.insert(registration, Arc::clone(&operation));
        operation
    }

    pub(super) fn observe_all(&self, incarnation: Incarnation) {
        for operation in self.entries.values() {
            operation.observe(incarnation);
        }
    }

    pub(super) fn pop_front(&mut self) -> Option<Arc<SendOperation<M>>> {
        let (_, operation) = self.entries.pop_first()?;
        #[cfg(test)]
        {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        Some(operation)
    }

    pub(super) fn remove(&mut self, id: WaiterId) -> Option<Arc<SendOperation<M>>> {
        let operation = self.entries.remove(&id)?;
        #[cfg(test)]
        {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        Some(operation)
    }
}
fn mint_accepted_sequence(accepted: &AtomicMonotonicCounter) -> AcceptedSequence {
    AcceptedSequence(accepted.mint(Ordering::Release, Ordering::Relaxed))
}

/// Accepts `message` into a mailbox bound to `binding`, or hands it back when
/// the mailbox is full. Parked senders are older than this one, so any waiter
/// makes the mailbox full.
pub(super) fn accept_locked<M: Send + 'static>(
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
pub(super) fn promote_waiters<M: Send + 'static>(
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
