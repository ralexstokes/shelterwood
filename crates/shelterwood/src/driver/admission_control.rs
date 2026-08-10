//! Dynamic membership control-plane transport and bookkeeping.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use crate::{
    ChildId, Membership, ScopeState,
    admission::{NotAdmittingCause, RemoveOutcome, ReserveError},
    cells::{DynamicRoute, MemberStage, ScopeCell},
    plan::{ChildConstruction, SlotCell, checked_id, mint_reserved_slot},
    policy::ScopeFlavor,
    runtime::{self, Latch},
};

use super::{DriverEvent, Obligation};

pub(crate) type RemovalResponse = runtime::OneShotReceiver<RemoveOutcome>;

#[derive(Default)]
pub(super) struct RemovalResponses(pub(super) Vec<runtime::OneShotSender<RemoveOutcome>>);

impl RemovalResponses {
    pub(super) fn subscribe(&mut self) -> RemovalResponse {
        // Callers may re-request removal and drop the returned future while
        // the child is still stopping; discard those abandoned senders so the
        // entry retains space proportional to live waiters only.
        self.0.retain(|sender| !sender.is_closed());
        let (sender, receiver) = runtime::oneshot();
        self.0.push(sender);
        receiver
    }

    fn complete(self, outcome: RemoveOutcome) {
        for sender in self.0 {
            let _ = sender.send(outcome);
        }
    }
}

pub(super) fn complete_removals(responses: RemovalResponses) {
    responses.complete(RemoveOutcome::Removed);
}

fn completed_removal(outcome: RemoveOutcome) -> RemovalResponse {
    let (sender, receiver) = runtime::oneshot();
    sender
        .send(outcome)
        .expect("a fresh removal receiver must be open");
    receiver
}

pub(super) struct DynamicEntry {
    pub(super) slot: Arc<SlotCell>,
    pub(super) admitted: bool,
    pub(super) fused_cancel: Option<Latch>,
    pub(super) removal: Obligation<RemovalResponses>,
    pub(super) removal_started: bool,
}

impl DynamicEntry {
    fn reserved(slot: Arc<SlotCell>) -> Self {
        Self {
            slot,
            admitted: false,
            fused_cancel: None,
            removal: Obligation::new(RemovalResponses::default(), complete_removals),
            removal_started: false,
        }
    }

    fn admitted(slot: Arc<SlotCell>) -> Self {
        Self {
            admitted: true,
            ..Self::reserved(slot)
        }
    }
}

pub(super) struct DynamicState {
    accepting: bool,
    pub(super) entries: HashMap<ChildId, DynamicEntry>,
}

pub(crate) struct DynamicControl {
    events: runtime::MpscSender<DriverEvent>,
    pub(super) state: Mutex<DynamicState>,
    requests: runtime::UnboundedMpscSender<DriverEvent>,
    pub(super) request_forwarder_close: Latch,
    #[cfg(test)]
    pub(super) request_forwarder_ended: Latch,
}

impl DynamicControl {
    pub(super) fn new(events: runtime::MpscSender<DriverEvent>) -> Arc<Self> {
        let (requests, mut request_receiver) = runtime::unbounded_mpsc();
        let forward_events = events.clone();
        let request_forwarder_close = Latch::default();
        let close_forwarder = request_forwarder_close.clone();
        #[cfg(test)]
        let request_forwarder_ended = Latch::default();
        #[cfg(test)]
        let forwarder_ended = request_forwarder_ended.clone();
        let control = Arc::new(Self {
            events,
            state: Mutex::new(DynamicState {
                accepting: true,
                entries: HashMap::new(),
            }),
            requests,
            request_forwarder_close,
            #[cfg(test)]
            request_forwarder_ended,
        });
        // Off-runtime construction (unit tests only) safely skips the
        // forwarder: `reserve_dynamic`/`start_admission` fail closed with
        // `NoRuntime` before anything can enqueue, and a live driver — the
        // only other producer — exists only inside the runtime.
        if runtime::is_available() {
            runtime::spawn(async move {
                loop {
                    let request = match runtime::select_two(
                        close_forwarder.fired(),
                        runtime::unbounded_mpsc_recv(&mut request_receiver),
                    )
                    .await
                    {
                        runtime::Either::Left(()) | runtime::Either::Right(None) => break,
                        runtime::Either::Right(Some(request)) => request,
                    };
                    // A failed admission send drops its response obligation,
                    // completing it with `Terminal`. Explicit closure drops
                    // this request and the queued suffix for the same result.
                    if matches!(
                        runtime::select_two(
                            close_forwarder.fired(),
                            runtime::mpsc_send(&forward_events, request),
                        )
                        .await,
                        runtime::Either::Left(())
                    ) {
                        break;
                    }
                }
                #[cfg(test)]
                forwarder_ended.fire();
            });
        }
        control
    }

    pub(super) fn register_initial<'a>(&self, slots: impl IntoIterator<Item = &'a Arc<SlotCell>>) {
        let mut state = self.state.lock().expect("dynamic-state mutex poisoned");
        for slot in slots {
            state.entries.insert(
                slot.member.id().clone(),
                DynamicEntry::admitted(Arc::clone(slot)),
            );
        }
    }

    pub(super) fn close(&self) -> HashMap<ChildId, DynamicEntry> {
        let mut state = self.state.lock().expect("dynamic-state mutex poisoned");
        state.accepting = false;
        let entries = std::mem::take(&mut state.entries);
        drop(state);
        // Reservation handles may retain this control after scope teardown.
        // Stop the sole per-scope forwarder explicitly instead of waiting for
        // every clone of its unbounded sender to disappear.
        self.request_forwarder_close.fire();
        let mut retained = HashMap::new();
        for (id, entry) in entries {
            if !entry.admitted {
                let definition = entry.slot.take_never_started();
                dispose_definition_then(definition, move || drop(entry));
            } else {
                retained.insert(id, entry);
            }
        }
        // The caller holds admitted entries across member terminality and
        // residency removal. Dropping them then completes every in-flight
        // removal without waking a remover before those observation edges.
        retained
    }
}

pub(crate) struct DynamicReservation {
    pub(crate) slot: Arc<SlotCell>,
    pub(crate) control: Arc<dyn DynamicRoute>,
}

pub(crate) fn reserve_dynamic(
    scope: &Arc<ScopeCell>,
    id: ChildId,
    child_scope: Option<ScopeFlavor>,
) -> Result<DynamicReservation, ReserveError> {
    let id = checked_id(id)?;
    if !runtime::is_available() {
        return Err(ReserveError::NoRuntime);
    }
    if matches!(scope.member.record().stage, MemberStage::Terminal(_)) {
        return Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal));
    }
    let control = scope.dynamic_route().ok_or(ReserveError::NotAdmitting(
        NotAdmittingCause::NoLiveIncarnation,
    ))?;
    match scope.record().state {
        ScopeState::Starting | ScopeState::Running => {}
        ScopeState::Draining => {
            return Err(ReserveError::NotAdmitting(NotAdmittingCause::Draining));
        }
        ScopeState::StartupFailed => {
            return Err(ReserveError::NotAdmitting(NotAdmittingCause::StartupFailed));
        }
        ScopeState::Unstarted | ScopeState::Stopped { .. } => {
            return Err(ReserveError::NotAdmitting(
                NotAdmittingCause::NoLiveIncarnation,
            ));
        }
    }
    let slot = control.reserve(scope, id, child_scope)?;
    Ok(DynamicReservation { slot, control })
}

fn reserve_dynamic_slot(
    control: &DynamicControl,
    scope: &Arc<ScopeCell>,
    id: ChildId,
    child_scope: Option<ScopeFlavor>,
) -> Result<Arc<SlotCell>, ReserveError> {
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    if !state.accepting {
        return Err(ReserveError::NotAdmitting(NotAdmittingCause::Draining));
    }
    if let Some(existing) = state.entries.get(&id) {
        if existing.slot.member.record().removing {
            return Err(ReserveError::RemovalInProgress(id));
        }
        return Err(ReserveError::DuplicateId(id));
    }
    let slot = mint_reserved_slot(scope, &id, child_scope)?;
    state
        .entries
        .insert(id, DynamicEntry::reserved(Arc::clone(&slot)));
    Ok(slot)
}

pub(crate) fn start_admission(
    control: Arc<dyn DynamicRoute>,
    slot: Arc<SlotCell>,
    fused_cancel: Option<Latch>,
) -> Result<runtime::OneShotReceiver<Result<(), ReserveError>>, ReserveError> {
    control.start_admission(slot, fused_cancel)
}

fn start_dynamic_admission(
    control: Arc<DynamicControl>,
    slot: Arc<SlotCell>,
    fused_cancel: Option<Latch>,
) -> Result<runtime::OneShotReceiver<Result<(), ReserveError>>, ReserveError> {
    if !runtime::is_available() {
        return Err(ReserveError::NoRuntime);
    }
    let (sender, response) = runtime::oneshot();
    let request = AdmissionRequest {
        control: Arc::downgrade(&control),
        slot,
        fused_cancel,
        response: Obligation::new(sender, |sender| {
            let _ = sender.send(Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal)));
        }),
    };
    // The driver channel is bounded and a split admission may be dropped
    // right after this first poll (drop detaches), so the send cannot live in
    // the caller-held future. One persistent forwarder per scope drains this
    // unbounded FIFO, keeping pending-admission memory proportional to the
    // requests without a second mutex-protected channel implementation.
    let _ = runtime::unbounded_mpsc_send(&control.requests, DriverEvent::Admission(request));
    Ok(response)
}

pub(super) fn cancel_dynamic_reservation_parts(
    control: &DynamicControl,
    slot: &Arc<SlotCell>,
) -> (
    Option<runtime::Isolated<ChildConstruction>>,
    Option<DynamicEntry>,
) {
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    let id = slot.member.id().clone();
    let cancelled = state.entries.get(&id).is_some_and(|entry| {
        entry.slot.member.membership() == slot.member.membership() && !entry.admitted
    });
    let removed = cancelled.then(|| state.entries.remove(&id)).flatten();
    drop(state);
    let definition = if cancelled {
        slot.take_never_started()
    } else {
        None
    };
    (definition, removed)
}

pub(crate) fn cancel_dynamic_reservation(control: &dyn DynamicRoute, slot: &Arc<SlotCell>) {
    control.cancel_reservation(slot);
}

fn cancel_dynamic_reservation_impl(control: &DynamicControl, slot: &Arc<SlotCell>) {
    let (definition, removed) = cancel_dynamic_reservation_parts(control, slot);
    // The entry's drop completes its removal response; it must follow the
    // member's terminal publication and isolated definition disposal.
    dispose_definition_then(definition, move || drop(removed));
}

pub(crate) fn signal_fused_cancel(
    control: &dyn DynamicRoute,
    membership: Membership,
    latch: &Latch,
) {
    control.signal_fused_cancel(membership, latch);
}

fn signal_fused_cancel_impl(control: &DynamicControl, membership: Membership, latch: &Latch) {
    if latch.fire() {
        queue_driver_event(control, DriverEvent::Removal(membership));
    }
}

pub(crate) fn remove_dynamic(
    scope: &Arc<ScopeCell>,
    id: &ChildId,
    exact: Option<Membership>,
) -> RemovalResponse {
    if matches!(
        scope.record().state,
        ScopeState::Draining | ScopeState::Stopped { .. }
    ) {
        return completed_removal(RemoveOutcome::AlreadyAbsent);
    }
    let Some(control) = scope.dynamic_route() else {
        return completed_removal(RemoveOutcome::AlreadyAbsent);
    };
    control.remove(scope, id, exact)
}

fn remove_dynamic_impl(
    control: &DynamicControl,
    scope: &Arc<ScopeCell>,
    id: &ChildId,
    exact: Option<Membership>,
) -> RemovalResponse {
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    let Some(entry) = state.entries.get_mut(id) else {
        return completed_removal(RemoveOutcome::AlreadyAbsent);
    };
    if exact.is_some_and(|membership| membership != entry.slot.member.membership()) {
        return completed_removal(RemoveOutcome::AlreadyAbsent);
    }
    let response = entry.removal.payload_mut().subscribe();
    if !entry.admitted {
        let entry = state.entries.remove(id).expect("entry was just resolved");
        drop(state);
        let definition = entry.slot.take_never_started();
        dispose_definition_then(definition, move || drop(entry));
        return response;
    }
    // Terminal residents still have a driver registration. Route them
    // through the normal removal path, like live residents, so that
    // registration is reclaimed before the removal response completes.
    let member = Arc::clone(&entry.slot.member);
    let membership = member.membership();
    // Dynamic-state protects admission/removal bookkeeping; the observation
    // gate protects the public projection. Release the former before entering
    // the latter. No path takes the two in the opposite order, so this is not
    // breaking an existing cycle; it keeps an unbounded wait out of the
    // bookkeeping mutex. Any thread may hold the gate across arbitrary
    // observation work, and blocking there while holding dynamic state would
    // stall every concurrent reservation, removal, and driver admission.
    drop(state);
    scope.transition_child(&member, |record| record.removing = true, None);
    if member.removal.fire() {
        queue_driver_event(control, DriverEvent::Removal(membership));
    }
    response
}

impl DynamicRoute for DynamicControl {
    fn reserve(
        &self,
        scope: &Arc<ScopeCell>,
        id: ChildId,
        child_scope: Option<ScopeFlavor>,
    ) -> Result<Arc<SlotCell>, ReserveError> {
        reserve_dynamic_slot(self, scope, id, child_scope)
    }

    fn start_admission(
        self: Arc<Self>,
        slot: Arc<SlotCell>,
        fused_cancel: Option<Latch>,
    ) -> Result<runtime::OneShotReceiver<Result<(), ReserveError>>, ReserveError> {
        start_dynamic_admission(self, slot, fused_cancel)
    }

    fn cancel_reservation(&self, slot: &Arc<SlotCell>) {
        cancel_dynamic_reservation_impl(self, slot);
    }

    fn signal_fused_cancel(&self, membership: Membership, latch: &Latch) {
        signal_fused_cancel_impl(self, membership, latch);
    }

    fn remove(
        &self,
        scope: &Arc<ScopeCell>,
        id: &ChildId,
        exact: Option<Membership>,
    ) -> RemovalResponse {
        remove_dynamic_impl(self, scope, id, exact)
    }

    #[cfg(test)]
    fn request_forwarder_probe(&self) -> (Latch, Latch) {
        (
            self.request_forwarder_close.clone(),
            self.request_forwarder_ended.clone(),
        )
    }
}

fn queue_driver_event(control: &DynamicControl, event: DriverEvent) {
    let Err(event) = runtime::mpsc_try_send(&control.events, event) else {
        return;
    };
    // The unbounded send is synchronous and runtime-independent, so a drop or
    // removal from a foreign thread cannot lose the edge when the bounded
    // driver lane is full. The per-scope forwarder applies backpressure only
    // on that saturated fallback; the normal removal path stays allocation-free.
    let _ = runtime::unbounded_mpsc_send(&control.requests, event);
}

pub(super) struct AdmissionRequest {
    // Concrete so rejection can dispose the reservation through the very
    // control that reserved it, even when it is no longer the live route.
    pub(super) control: Weak<DynamicControl>,
    pub(super) slot: Arc<SlotCell>,
    pub(super) fused_cancel: Option<Latch>,
    response: Obligation<runtime::OneShotSender<Result<(), ReserveError>>>,
}

impl AdmissionRequest {
    pub(super) fn complete(&mut self, result: Result<(), ReserveError>) {
        self.response.complete(|sender| {
            let _ = sender.send(result);
        });
    }
}

pub(super) fn reject_admission_after_disposal(
    mut request: AdmissionRequest,
    definition: Option<runtime::Isolated<ChildConstruction>>,
    removed: Option<DynamicEntry>,
    error: ReserveError,
) {
    dispose_definition_then(definition, move || {
        drop(removed);
        request.complete(Err(error));
    });
}

fn dispose_definition_then(
    mut definition: Option<runtime::Isolated<ChildConstruction>>,
    completion: impl FnOnce() + Send + 'static,
) {
    let construction = definition.as_mut().and_then(runtime::Isolated::take);
    let Some(construction) = construction else {
        completion();
        return;
    };
    runtime::dispose_then(construction, move |_| {
        // A never-admitted definition has no incarnation verdict to publish,
        // but its destructor must still be isolated and complete before any
        // response releases ownership back to the caller.
        completion();
    });
}

#[cfg(test)]
mod tests {
    use crate::RemoveOutcome;

    use super::RemovalResponses;

    #[test]
    fn removal_subscription_discards_abandoned_waiters() {
        let mut responses = RemovalResponses::default();
        for _ in 0..8 {
            drop(responses.subscribe());
        }
        let mut retained = responses.subscribe();
        assert_eq!(
            responses.0.len(),
            1,
            "abandoned removal waiters are pruned on subscription"
        );
        responses.complete(RemoveOutcome::Removed);
        assert_eq!(retained.try_receive(), Some(RemoveOutcome::Removed));
    }
}
