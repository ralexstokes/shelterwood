use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{Arc, MutexGuard, atomic::AtomicBool},
};

use crate::{
    identity::Incarnation,
    mailbox::{MailboxBindToken, MailboxDisposal, MailboxEffectSink, MailboxTermination},
    policy::ResolvedMailbox,
    runtime::{Signal, dispose_detached},
};
use shelterwood_core::{
    panic::{PanicAccumulator, PanicPayload, resume_panic},
    waker::WakerEffects,
};

use super::{
    MailboxCell,
    operation::{SendOperation, Submission},
    state::{Envelope, MailboxState, Phase, WaiterQueue},
};

/// The complete movable effect state for one mailbox transition.
///
/// `MailboxEffects` owns this value and moves it wholesale into the eventual
/// batch, so adding an effect cannot require restating its field at that seam.
pub(super) struct MailboxEffectPayload<M> {
    pulse: bool,
    pub(super) displaced: Vec<Envelope<M>>,
    isolate_displaced: bool,
    pub(super) wakers: WakerEffects,
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
/// The sink borrows its mailbox rather than cloning the change signal out of
/// it: it never outlives the `MailboxTxn` that owns it, and every mailbox
/// transition — including the per-message receive path — would otherwise pay
/// refcount traffic to restate what the transaction already holds. Only a
/// flush deferred into a wider framework sink, which outlives the borrow,
/// clones the signal handle.
pub(super) struct MailboxEffects<'a, 's, M: Send + 'static> {
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

    pub(super) fn pulse(&mut self) {
        self.payload.pulse = true;
    }

    pub(super) fn isolate_displaced(&mut self) {
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
        let payload = std::mem::take(&mut self.payload);
        if let Some(external) = self.external.take() {
            let changed = self.cell.changed.clone();
            external.defer_mailbox_effect(Box::new(move || {
                payload.flush(&changed);
            }));
            return;
        }
        payload.flush(&self.cell.changed);
    }
}

/// A received message, held outside the transaction that dequeued it.
///
/// `receive` declares it before its `MailboxTxn`, so every unwind — one out of
/// the locked transition as much as a panicking pulse, waker, or displaced
/// payload in the post-unlock flush — drops the transaction first and then
/// submits the message for detached disposal instead of destroying it on the
/// receiving caller's stack.
pub(super) struct ReturnedMessage<M: Send + 'static> {
    pub(super) value: Option<M>,
}

impl<M: Send + 'static> Drop for ReturnedMessage<M> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            dispose_detached(value);
        }
    }
}

impl<M: Send + 'static> MailboxEffectPayload<M> {
    fn flush(self, changed: &Signal) {
        let mut payload = self;
        let mut panics = PanicAccumulator::default();
        if payload.pulse {
            panics.run(|| changed.pulse());
        }
        // Submit displaced latest-value payloads to isolated disposal before
        // waking accepted senders: a woken sender may run immediately and must
        // not race ahead of disposal submission.
        if payload.isolate_displaced && !payload.displaced.is_empty() {
            let isolated = std::mem::take(&mut payload.displaced);
            panics.run(|| dispose_detached(MailboxPayload::unread(isolated.into(), None)));
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
pub(super) struct MailboxTxn<'a, 's, M: Send + 'static> {
    state: Option<MutexGuard<'a, MailboxState<M>>>,
    pub(super) effects: MailboxEffects<'a, 's, M>,
}

impl<'a, M: Send + 'static> MailboxTxn<'a, 'static, M> {
    pub(super) fn new(cell: &'a MailboxCell<M>) -> Self {
        let effects = MailboxEffects::new(cell);
        let state = cell.state.lock().expect("mailbox mutex poisoned");
        Self {
            state: Some(state),
            effects,
        }
    }
}

impl<'a, 's, M: Send + 'static> MailboxTxn<'a, 's, M> {
    pub(super) fn deferred(
        cell: &'a MailboxCell<M>,
        effects: &'s mut dyn MailboxEffectSink,
    ) -> Self {
        let effects = MailboxEffects::deferred(cell, effects);
        let state = cell.state.lock().expect("mailbox mutex poisoned");
        Self {
            state: Some(state),
            effects,
        }
    }

    pub(super) fn parts(&mut self) -> (&mut MailboxState<M>, &mut MailboxEffects<'a, 's, M>) {
        (
            self.state
                .as_deref_mut()
                .expect("a live mailbox transaction retains its guard"),
            &mut self.effects,
        )
    }

    /// The guarded state alone, for transitions whose effects are queued by
    /// the transaction rather than by the caller.
    pub(super) fn state_mut(&mut self) -> &mut MailboxState<M> {
        self.parts().0
    }

    pub(super) fn configure_kind(&mut self, kind: ResolvedMailbox) -> Option<ResolvedMailbox> {
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
    pub(super) fn park(
        &mut self,
        message: M,
        newest_observed: Option<Incarnation>,
    ) -> Submission<M> {
        Submission::Parked(self.state_mut().waiters.park(message, newest_observed))
    }

    pub(super) fn set_phase(&mut self, phase: Phase) {
        self.state_mut().phase = phase;
    }

    pub(super) fn reset_bind_permit(&mut self) -> MailboxBindToken {
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
    pub(super) fn take_payload(&mut self) -> MailboxPayload<M> {
        let state = self.state_mut();
        MailboxPayload::unread(std::mem::take(&mut state.queue), state.latest.take())
    }

    pub(super) fn finish<R>(mut self, output: R) -> R {
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

pub(super) struct Termination<M> {
    pub(super) waiters: WaiterQueue<M>,
    pub(super) final_incarnation: Option<Incarnation>,
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

pub(super) struct MailboxPayload<M> {
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

pub(super) struct MailboxTeardown<M: Send + 'static> {
    pub(super) changed: Option<Signal>,
    pub(super) payload: Option<MailboxPayload<M>>,
    pub(super) termination: Option<Termination<M>>,
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
            dispose_detached(payload);
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
            dispose_detached(payload);
        }
    }
}
