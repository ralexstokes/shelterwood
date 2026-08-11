//! Dynamic membership control-plane transport and bookkeeping.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use crate::{
    ChildId, Membership, ScopeState,
    admission::{NotAdmittingCause, RemoveOutcome, ReserveError},
    cells::{
        DynamicRoute, ErasedDynamicRoute, ErasedDynamicSlot, MemberStage, ObservationTxn, ScopeCell,
    },
    plan::{
        ChildConstruction, SlotCell, checked_id, concrete_dynamic_slot, concrete_dynamic_slot_ref,
        erase_dynamic_slot, mint_reserved_slot,
    },
    policy::ScopeFlavor,
    runtime::{self, Latch},
};

use super::{ChildKey, DriverEvent, Obligation, RemovalRequest};

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
    state: DynamicMembershipState,
    pub(super) removal: Obligation<RemovalResponses>,
}

// The authoritative dynamic control-plane phase. `MemberRecord::membership_status`
// is only its public observation projection; driver decisions use this enum.
// A resident owns its arena key, so removal and restart paths never have to
// rediscover the corresponding `ChildRuntime` with a linear scan.
enum DynamicMembershipState {
    Reserved,
    Resident {
        key: ChildKey,
        fused_cancel: Option<Latch>,
    },
    Removing {
        key: ChildKey,
    },
}

impl DynamicEntry {
    pub(super) fn reserved(slot: Arc<SlotCell>) -> Self {
        Self {
            slot,
            state: DynamicMembershipState::Reserved,
            removal: Obligation::new(RemovalResponses::default(), complete_removals),
        }
    }

    pub(super) fn resident(
        slot: Arc<SlotCell>,
        key: ChildKey,
        fused_cancel: Option<Latch>,
    ) -> Self {
        Self {
            slot,
            state: DynamicMembershipState::Resident { key, fused_cancel },
            removal: Obligation::new(RemovalResponses::default(), complete_removals),
        }
    }

    #[cfg(test)]
    pub(super) fn removing(slot: Arc<SlotCell>, key: ChildKey, removal: RemovalResponses) -> Self {
        Self {
            slot,
            state: DynamicMembershipState::Removing { key },
            removal: Obligation::new(removal, complete_removals),
        }
    }

    pub(super) fn is_reserved(&self) -> bool {
        matches!(self.state, DynamicMembershipState::Reserved)
    }

    pub(super) fn is_removing(&self) -> bool {
        matches!(self.state, DynamicMembershipState::Removing { .. })
    }

    fn removal_requested(&self) -> bool {
        match &self.state {
            DynamicMembershipState::Resident { fused_cancel, .. } => {
                fused_cancel.as_ref().is_some_and(Latch::is_fired)
            }
            DynamicMembershipState::Removing { .. } => true,
            DynamicMembershipState::Reserved => false,
        }
    }

    pub(super) fn key(&self) -> Option<ChildKey> {
        match self.state {
            DynamicMembershipState::Reserved => None,
            DynamicMembershipState::Resident { key, .. }
            | DynamicMembershipState::Removing { key } => Some(key),
        }
    }

    pub(super) fn matches_key(&self, key: ChildKey) -> bool {
        self.key() == Some(key)
    }

    pub(super) fn promote(
        &mut self,
        key: ChildKey,
        fused_cancel: Option<Latch>,
        _txn: &mut ObservationTxn<'_>,
    ) {
        debug_assert!(self.is_reserved(), "only a reservation can become resident");
        self.state = DynamicMembershipState::Resident { key, fused_cancel };
    }

    pub(super) fn mark_removing(&mut self, _txn: &mut ObservationTxn<'_>) -> Option<ChildKey> {
        let DynamicMembershipState::Resident { key, .. } = self.state else {
            return None;
        };
        self.state = DynamicMembershipState::Removing { key };
        Some(key)
    }

    pub(super) fn restart_is_suppressed(&self, key: ChildKey) -> bool {
        match &self.state {
            DynamicMembershipState::Reserved => false,
            DynamicMembershipState::Resident {
                key: resident,
                fused_cancel,
            } => *resident == key && fused_cancel.as_ref().is_some_and(Latch::is_fired),
            DynamicMembershipState::Removing { key: removing } => *removing == key,
        }
    }
}

pub(super) struct DynamicState {
    accepting: bool,
    #[cfg(not(test))]
    entries: HashMap<ChildId, DynamicEntry>,
    #[cfg(test)]
    pub(super) entries: HashMap<ChildId, DynamicEntry>,
}

impl DynamicState {
    pub(super) fn entry(&self, id: &ChildId) -> Option<&DynamicEntry> {
        self.entries.get(id)
    }

    pub(super) fn entry_mut(&mut self, id: &ChildId) -> Option<&mut DynamicEntry> {
        self.entries.get_mut(id)
    }

    fn insert(
        &mut self,
        id: ChildId,
        entry: DynamicEntry,
        _txn: &mut ObservationTxn<'_>,
    ) -> Option<DynamicEntry> {
        self.entries.insert(id, entry)
    }

    pub(super) fn remove(
        &mut self,
        id: &ChildId,
        _txn: &mut ObservationTxn<'_>,
    ) -> Option<DynamicEntry> {
        self.entries.remove(id)
    }

    fn close_admission(&mut self, _txn: &mut ObservationTxn<'_>) {
        self.accepting = false;
    }

    fn take_entries(&mut self, _txn: &mut ObservationTxn<'_>) -> HashMap<ChildId, DynamicEntry> {
        std::mem::take(&mut self.entries)
    }
}

pub(crate) struct DynamicControl {
    requests: runtime::UnboundedMpscSender<DriverEvent>,
    pub(super) state: Mutex<DynamicState>,
}

impl DynamicControl {
    pub(super) fn new(requests: runtime::UnboundedMpscSender<DriverEvent>) -> Arc<Self> {
        Arc::new(Self {
            requests,
            state: Mutex::new(DynamicState {
                accepting: true,
                entries: HashMap::new(),
            }),
        })
    }

    pub(super) fn register_initial<'a>(
        &self,
        slots: impl IntoIterator<Item = (&'a Arc<SlotCell>, ChildKey)>,
        _txn: &mut ObservationTxn<'_>,
    ) {
        let mut state = self.state.lock().expect("dynamic-state mutex poisoned");
        for (slot, key) in slots {
            state.insert(
                slot.member.id().clone(),
                DynamicEntry::resident(Arc::clone(slot), key, None),
                _txn,
            );
        }
    }

    fn close_admission_in(&self, _txn: &mut ObservationTxn<'_>) {
        self.state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .close_admission(_txn);
    }

    pub(super) fn close(
        &self,
        scope: &ScopeCell,
        txn: &mut ObservationTxn<'_>,
    ) -> HashMap<ChildId, DynamicEntry> {
        self.close_admission_in(txn);
        let mut state = self.state.lock().expect("dynamic-state mutex poisoned");
        let entries = state.take_entries(txn);
        drop(state);
        let mut retained = HashMap::new();
        for (id, entry) in entries {
            if entry.is_reserved() {
                let definition = take_terminal_reservation(scope, &entry.slot, txn);
                txn.defer(move || dispose_definition_then(definition, move || drop(entry)));
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

fn take_terminal_reservation(
    scope: &ScopeCell,
    slot: &SlotCell,
    txn: &mut ObservationTxn<'_>,
) -> Option<runtime::Isolated<ChildConstruction>> {
    slot.take_never_started_locked(scope, txn)
}

pub(crate) struct DynamicReservation {
    pub(crate) scope: Arc<ScopeCell>,
    pub(crate) slot: Arc<SlotCell>,
    pub(crate) control: Arc<ErasedDynamicRoute>,
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
    scope.with_observation_gate(|txn| reserve_dynamic_in(scope, id, child_scope, txn))
}

pub(super) fn reserve_dynamic_in(
    scope: &Arc<ScopeCell>,
    id: ChildId,
    child_scope: Option<ScopeFlavor>,
    txn: &mut ObservationTxn<'_>,
) -> Result<DynamicReservation, ReserveError> {
    if matches!(scope.member.record().stage, MemberStage::Terminal(_)) {
        return Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal));
    }
    let control = scope
        .dynamic_route_in(txn)
        .ok_or(ReserveError::NotAdmitting(
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
    let slot = concrete_dynamic_slot(control.reserve(scope, id, child_scope, txn)?);
    Ok(DynamicReservation {
        scope: Arc::clone(scope),
        slot,
        control,
    })
}

fn reserve_dynamic_slot(
    control: &DynamicControl,
    scope: &Arc<ScopeCell>,
    id: ChildId,
    child_scope: Option<ScopeFlavor>,
    _txn: &mut ObservationTxn<'_>,
) -> Result<Arc<SlotCell>, ReserveError> {
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    if !state.accepting {
        return Err(ReserveError::NotAdmitting(NotAdmittingCause::Draining));
    }
    if let Some(existing) = state.entry(&id) {
        if existing.removal_requested() {
            return Err(ReserveError::RemovalInProgress(id));
        }
        return Err(ReserveError::DuplicateId(id));
    }
    let slot = mint_reserved_slot(scope, &id, child_scope)?;
    scope.adopt_child_observation_gate(&slot.member, slot.scope.as_deref(), _txn);
    state.insert(id, DynamicEntry::reserved(Arc::clone(&slot)), _txn);
    Ok(slot)
}

pub(crate) fn start_admission(
    control: Arc<ErasedDynamicRoute>,
    slot: Arc<SlotCell>,
    fused_cancel: Option<Latch>,
) -> Result<runtime::OneShotReceiver<Result<(), ReserveError>>, ReserveError> {
    control.start_admission(erase_dynamic_slot(slot), fused_cancel)
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
    // Queue synchronously so dropping a split-admission future immediately
    // after its first poll cannot cancel the admitted request.
    queue_driver_event(&control, DriverEvent::Admission(request));
    Ok(response)
}

pub(super) fn cancel_dynamic_reservation_parts(
    scope: &ScopeCell,
    control: &DynamicControl,
    slot: &SlotCell,
    txn: &mut ObservationTxn<'_>,
) -> (
    Option<runtime::Isolated<ChildConstruction>>,
    Option<DynamicEntry>,
) {
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    let id = slot.member.id().clone();
    let cancelled = state.entry(&id).is_some_and(|entry| {
        entry.slot.member.membership() == slot.member.membership() && entry.is_reserved()
    });
    let removed = cancelled.then(|| state.remove(&id, txn)).flatten();
    drop(state);
    let definition = cancelled
        .then(|| take_terminal_reservation(scope, slot, txn))
        .flatten();
    (definition, removed)
}

pub(crate) fn cancel_dynamic_reservation(
    scope: &Arc<ScopeCell>,
    control: &ErasedDynamicRoute,
    slot: &Arc<SlotCell>,
) {
    scope.with_observation_gate(|txn| control.cancel_reservation(scope, slot.as_ref(), txn));
}

fn cancel_dynamic_reservation_impl(
    scope: &ScopeCell,
    control: &DynamicControl,
    slot: &SlotCell,
    txn: &mut ObservationTxn<'_>,
) {
    let (definition, removed) = cancel_dynamic_reservation_parts(scope, control, slot, txn);
    // The entry's drop completes its removal response; it must follow the
    // member's terminal publication and isolated definition disposal.
    txn.defer(move || dispose_definition_then(definition, move || drop(removed)));
}

pub(crate) fn signal_fused_cancel(
    scope: &Arc<ScopeCell>,
    control: &ErasedDynamicRoute,
    slot: &Arc<SlotCell>,
    latch: &Latch,
) {
    scope.with_observation_gate(|txn| {
        control.signal_fused_cancel(scope, slot.as_ref(), latch, txn);
    });
}

fn signal_fused_cancel_impl(
    control: &DynamicControl,
    scope: &Arc<ScopeCell>,
    slot: &SlotCell,
    latch: &Latch,
    txn: &mut ObservationTxn<'_>,
) {
    // The fire linearizes inside the transaction so a racing `remove` sees a
    // decided latch, but the waker-visible wake must not run under the gate.
    if !latch.fire_silently() {
        return;
    }
    let latch = latch.clone();
    txn.defer(move || latch.notify());
    // The authoritative state transition owns request publication, so a
    // racing explicit removal cannot queue the same membership again.
    let removal = {
        let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
        state
            .entry_mut(slot.member.id())
            .filter(|entry| entry.slot.member.membership() == slot.member.membership())
            .and_then(|entry| entry.mark_removing(txn))
            .map(|key| RemovalRequest {
                membership: slot.member.membership(),
                key,
            })
    };
    if let Some(removal) = removal {
        scope.set_child_removing_locked(&slot.member, txn);
        defer_driver_event(txn, control, DriverEvent::Removal(removal));
    }
}

pub(crate) fn remove_dynamic(
    scope: &Arc<ScopeCell>,
    id: &ChildId,
    exact: Option<Membership>,
) -> RemovalResponse {
    scope.with_observation_gate(|txn| {
        if matches!(
            scope.record().state,
            ScopeState::Draining | ScopeState::Stopped { .. }
        ) {
            return completed_removal(RemoveOutcome::AlreadyAbsent);
        }
        let Some(control) = scope.dynamic_route_in(txn) else {
            return completed_removal(RemoveOutcome::AlreadyAbsent);
        };
        control.remove(scope, id, exact, txn)
    })
}

fn remove_dynamic_impl(
    control: &DynamicControl,
    scope: &Arc<ScopeCell>,
    id: &ChildId,
    exact: Option<Membership>,
    txn: &mut ObservationTxn<'_>,
) -> RemovalResponse {
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    let Some(entry) = state.entry_mut(id) else {
        return completed_removal(RemoveOutcome::AlreadyAbsent);
    };
    if exact.is_some_and(|membership| membership != entry.slot.member.membership()) {
        return completed_removal(RemoveOutcome::AlreadyAbsent);
    }
    let response = entry.removal.payload_mut().subscribe();
    if entry.is_reserved() {
        let entry = state.remove(id, txn).expect("entry was just resolved");
        drop(state);
        let definition = take_terminal_reservation(scope, &entry.slot, txn);
        txn.defer(move || dispose_definition_then(definition, move || drop(entry)));
        return response;
    }
    // Terminal residents still have a driver registration. Route them
    // through the normal removal path, like live residents, so that
    // registration is reclaimed before the removal response completes.
    let member = Arc::clone(&entry.slot.member);
    let membership = member.membership();
    let removal = entry
        .mark_removing(txn)
        .map(|key| RemovalRequest { membership, key });
    drop(state);
    if removal.is_some() {
        scope.set_child_removing_locked(&member, txn);
    }
    if let Some(removal) = removal {
        defer_driver_event(txn, control, DriverEvent::Removal(removal));
    }
    response
}

impl DynamicRoute for DynamicControl {
    type Slot = ErasedDynamicSlot;

    fn reserve(
        &self,
        scope: &Arc<ScopeCell>,
        id: ChildId,
        child_scope: Option<ScopeFlavor>,
        txn: &mut ObservationTxn<'_>,
    ) -> Result<Arc<Self::Slot>, ReserveError> {
        reserve_dynamic_slot(self, scope, id, child_scope, txn).map(erase_dynamic_slot)
    }

    fn close_admission(&self, txn: &mut ObservationTxn<'_>) {
        self.close_admission_in(txn);
    }

    fn start_admission(
        self: Arc<Self>,
        slot: Arc<Self::Slot>,
        fused_cancel: Option<Latch>,
    ) -> Result<runtime::OneShotReceiver<Result<(), ReserveError>>, ReserveError> {
        start_dynamic_admission(self, concrete_dynamic_slot(slot), fused_cancel)
    }

    fn cancel_reservation(
        &self,
        scope: &Arc<ScopeCell>,
        slot: &Self::Slot,
        txn: &mut ObservationTxn<'_>,
    ) {
        cancel_dynamic_reservation_impl(scope, self, concrete_dynamic_slot_ref(slot), txn);
    }

    fn signal_fused_cancel(
        &self,
        scope: &Arc<ScopeCell>,
        slot: &Self::Slot,
        latch: &Latch,
        txn: &mut ObservationTxn<'_>,
    ) {
        signal_fused_cancel_impl(self, scope, concrete_dynamic_slot_ref(slot), latch, txn);
    }

    fn remove(
        &self,
        scope: &Arc<ScopeCell>,
        id: &ChildId,
        exact: Option<Membership>,
        txn: &mut ObservationTxn<'_>,
    ) -> RemovalResponse {
        remove_dynamic_impl(self, scope, id, exact, txn)
    }
}

fn queue_driver_event(control: &DynamicControl, event: DriverEvent) {
    // Synchronous and runtime-independent: admission detaches at its first
    // poll, and removal may be signalled from a foreign thread.
    let _ = runtime::unbounded_mpsc_send(&control.requests, event);
}

fn defer_driver_event(txn: &mut ObservationTxn<'_>, control: &DynamicControl, event: DriverEvent) {
    let requests = control.requests.clone();
    txn.defer(move || {
        let _ = runtime::unbounded_mpsc_send(&requests, event);
    });
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
