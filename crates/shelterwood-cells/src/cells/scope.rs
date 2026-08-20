use std::{
    any::Any,
    collections::VecDeque,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

#[cfg(any(test, feature = "test-util"))]
use std::sync::atomic::AtomicUsize;

use shelterwood_core::{
    ChildId, Exit, Incarnation, Membership, TotalRestarts,
    engine::{Epoch, MembershipStatus, RequestTarget, ScopeEpochs, ScopeState},
    exit::{StartupError, StopReason, stop_reason_precedence},
    identity::{AtomicPoisonedCounter, MembershipReconciliation, MintedMembership, ScopeIdentity},
    policy::ScopeFlavor,
};
use shelterwood_runtime as runtime;

use crate::observe::{LifecycleEventKind, LifecycleHub, SnapshotHub};

use super::{
    MemberCell, MemberRecord, MemberStage, MemberTransition, ObservationGate, ObservationTxn,
    RetainedExit, RetainedStopReason, StartupDisposition,
};

/// Cross-crate close-admission hook retained by a restart-stable scope cell.
///
/// # Implementation boundary
///
/// This is not a user extension point. Only Shelterwood's façade implements
/// and installs this trait. The callback runs while the restart-stable tree
/// owns its observation gate, so a foreign implementation invalidates the
/// framework's lock-rule guarantees. Requiring the transaction capability in
/// the signature keeps that critical-section boundary explicit.
pub trait DynamicRoute: Any + Send + Sync {
    fn close_admission(&self, txn: &mut ObservationTxn<'_>);
}

/// Driver-independent view of one declaration slot while its membership is
/// resident in a running scope.
#[derive(Clone)]
pub struct ResidentProjection {
    pub member: Arc<MemberCell>,
    pub scope: Option<Arc<ScopeCell>>,
}

impl ResidentProjection {
    pub fn new(member: Arc<MemberCell>, scope: Option<Arc<ScopeCell>>) -> Self {
        Self { member, scope }
    }
}

struct ResidentChild {
    projection: ResidentProjection,
}

impl ResidentChild {
    fn new(projection: ResidentProjection) -> Self {
        Self { projection }
    }
}

#[derive(Debug, Default)]
struct ScopeControl {
    epochs: ScopeEpochs,
    shutdown: Option<ScopeRequest>,
    force: Option<ScopeRequest>,
    events: VecDeque<ScopeControlEvent>,
}

#[derive(Clone, Copy, Debug)]
struct ScopeRequest {
    epoch: Epoch,
    consumed: bool,
}

#[derive(Clone, Copy)]
enum ScopeRequestSlot {
    Shutdown,
    Force,
}

impl ScopeRequestSlot {
    fn get(self, control: &ScopeControl) -> Option<&ScopeRequest> {
        match self {
            Self::Shutdown => control.shutdown.as_ref(),
            Self::Force => control.force.as_ref(),
        }
    }

    fn get_mut(self, control: &mut ScopeControl) -> Option<&mut ScopeRequest> {
        match self {
            Self::Shutdown => control.shutdown.as_mut(),
            Self::Force => control.force.as_mut(),
        }
    }
}

/// One fact published by a scope control-plane transaction for its driver.
///
/// The payload is restart-stable identity rather than a mutable driver key.
/// The driver resolves it through its incrementally maintained membership
/// index, so stale events miss instead of addressing a replacement child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeControlEvent {
    RestartShutdown {
        membership: Membership,
        target: Epoch,
    },
}

/// Shared scope state follows two distinct synchronization regimes.
///
/// One gate per resident tree serializes compound observation-visible
/// transitions across configuration, records, resident children, and parent
/// links. Configuration, residency, and ancestry are plain synchronized state;
/// their individual locks do not make a multi-field transition atomic, so
/// every recursive observation path continues to hold that one tree gate.
///
/// Control requests, the dynamic route, records, residency, and hubs retain
/// their narrow storage locks, but every mutation is subordinate to the tree
/// gate and takes an [`ObservationTxn`] capability. Identity allocation and
/// lifecycle sequence minting remain independent driver-only counters. The
/// member-record watch is intentionally also the driver's wake bus.
struct ScopeObservation {
    config: Mutex<ObservationConfig>,
    record: runtime::WatchSender<ScopeRecord>,
    // Removal paths move residents into transaction effects before emitting
    // their `Removed` edges. A projection can be the last member/mailbox
    // owner, so neither this mutex nor the observation gate may retire one.
    current_children: Mutex<Vec<ResidentChild>>,
    parent: Mutex<Option<Weak<ScopeCell>>>,
    lifecycle_seq: AtomicPoisonedCounter,
    lifecycle: LifecycleHub,
    snapshots: SnapshotHub,
    closed: AtomicBool,
}

pub struct ScopeCell {
    pub member: Arc<MemberCell>,
    pub flavor: ScopeFlavor,
    /// This cell's own handle, for work it defers past the current borrow.
    ///
    /// Snapshot construction is staged during a transaction and runs at its
    /// commit, by which time the `&self` that staged it has returned, so the
    /// producer must own the cell rather than borrow it.
    me: Weak<ScopeCell>,
    child_identity: Mutex<ScopeIdentity>,
    control: Mutex<ScopeControl>,
    dynamic_route: Mutex<Option<Arc<dyn DynamicRoute>>>,
    observation: ScopeObservation,
    #[cfg(any(test, feature = "test-util"))]
    ancestor_parent_reads: AtomicUsize,
    #[cfg(any(test, feature = "test-util"))]
    runtime_storage: Mutex<RuntimeStorage>,
    #[cfg(any(test, feature = "test-util"))]
    gate_capture_probe: Mutex<Option<std::sync::mpsc::Sender<GateCapture>>>,
}

/// One observation-gate capture reported to a test probe.
///
/// A capture is reported after a thread has cloned the gate it is about to
/// acquire and before it blocks on that acquisition. Unit tests use these
/// reports as explicit barriers in place of scheduler or strong-count
/// polling: receiving a capture proves the reporting thread committed to the
/// gate that was current at that instant.
#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateCapture {
    /// [`ScopeCell::with_observation_gate`] captured its current gate.
    Observation,
    /// Gate adoption captured an obsolete gate it must acquire to hand off.
    Adoption,
}

#[cfg(any(test, feature = "test-util"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeStorage {
    pub children: usize,
    pub child_slots: usize,
    pub deadlines: usize,
    pub deadline_slots: usize,
}

mod projection;

use projection::ObservationConfig;
pub use projection::ScopeRecord;

impl ScopeCell {
    pub fn mint_membership(&self, id: &ChildId) -> Option<MintedMembership> {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mint_membership(id)
    }

    pub fn adopt_or_mint_membership(
        &self,
        id: &ChildId,
        provisional: Membership,
    ) -> MembershipReconciliation {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .adopt_or_mint_membership(id, provisional)
    }

    pub fn evict_child_identity(&self, member: &MemberCell) {
        self.child_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .evict(member.id(), member.membership());
    }

    pub fn new(
        member: Arc<MemberCell>,
        flavor: ScopeFlavor,
        child_identity: ScopeIdentity,
    ) -> Arc<Self> {
        let (record, _) = runtime::watch(ScopeRecord {
            state: ScopeState::Unstarted,
            startup: None,
            total_restarts: TotalRestarts::ZERO,
            retained_exits: Arc::new(Vec::new()),
        });
        Arc::new_cyclic(|me| Self {
            member,
            flavor,
            me: me.clone(),
            child_identity: Mutex::new(child_identity),
            control: Mutex::new(ScopeControl::default()),
            dynamic_route: Mutex::new(None),
            observation: ScopeObservation {
                config: Mutex::new(ObservationConfig::default()),
                record,
                current_children: Mutex::new(Vec::new()),
                parent: Mutex::new(None),
                lifecycle_seq: AtomicPoisonedCounter::new(),
                lifecycle: LifecycleHub::default(),
                snapshots: SnapshotHub::default(),
                closed: AtomicBool::new(false),
            },
            #[cfg(any(test, feature = "test-util"))]
            ancestor_parent_reads: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-util"))]
            runtime_storage: Mutex::new(RuntimeStorage::default()),
            #[cfg(any(test, feature = "test-util"))]
            gate_capture_probe: Mutex::new(None),
        })
    }

    /// An owning handle to this cell.
    ///
    /// `ScopeCell::new` is the only constructor and it publishes the cell
    /// inside an `Arc`, so the upgrade can only fail from within this cell's
    /// own destructor — which observes nothing.
    fn owned(&self) -> Arc<ScopeCell> {
        self.me
            .upgrade()
            .expect("a live scope cell owns a handle to itself")
    }

    pub fn resident_projections(&self) -> Vec<ResidentProjection> {
        self.current_children()
            .iter()
            .map(|resident| resident.projection.clone())
            .collect()
    }

    pub fn has_resident_child(&self, member: &MemberCell) -> bool {
        self.current_children()
            .iter()
            .any(|resident| resident.projection.member.membership() == member.membership())
    }

    fn current_children(&self) -> MutexGuard<'_, Vec<ResidentChild>> {
        self.observation
            .current_children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn parent(&self) -> Option<Arc<ScopeCell>> {
        #[cfg(any(test, feature = "test-util"))]
        self.ancestor_parent_reads.fetch_add(1, Ordering::Relaxed);
        self.observation
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn take_ancestor_parent_reads(&self) -> usize {
        self.ancestor_parent_reads.swap(0, Ordering::Relaxed)
    }

    fn set_parent(&self, parent: &Arc<ScopeCell>, txn: &mut ObservationTxn<'_>) {
        *self
            .observation
            .parent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(parent));
        let control = self.control.lock().expect("scope control mutex poisoned");
        let pending_shutdown = control.shutdown.filter(|request| {
            !request.consumed && control.epochs.request_is_pending(request.epoch)
        });
        drop(control);
        if let Some(request) = pending_shutdown {
            parent.publish_control_event_locked(
                ScopeControlEvent::RestartShutdown {
                    membership: self.member.membership(),
                    target: request.epoch,
                },
                txn,
            );
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn runtime_storage(&self) -> RuntimeStorage {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned")
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn record_runtime_storage(&self, storage: RuntimeStorage) {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned") = storage;
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn observation_gate(&self) -> ObservationGate {
        self.current_observation_gate()
    }

    /// Installs a probe reporting every gate capture made through this scope.
    #[cfg(any(test, feature = "test-util"))]
    pub fn probe_gate_captures(&self) -> std::sync::mpsc::Receiver<GateCapture> {
        let (sender, receiver) = std::sync::mpsc::channel();
        *self
            .gate_capture_probe
            .lock()
            .expect("gate capture probe mutex poisoned") = Some(sender);
        receiver
    }

    #[cfg(any(test, feature = "test-util"))]
    fn report_gate_capture(&self, capture: GateCapture) {
        if let Some(probe) = &*self
            .gate_capture_probe
            .lock()
            .expect("gate capture probe mutex poisoned")
        {
            // The probe channel is unbounded, so reporting never blocks and
            // cannot reorder the acquisition it announces.
            let _ = probe.send(capture);
        }
    }

    fn current_observation_gate(&self) -> ObservationGate {
        self.member.current_observation_gate()
    }

    fn adopt_observation_gate(
        &self,
        parent: &ScopeCell,
        gate: &ObservationGate,
        txn: &mut ObservationTxn<'_>,
    ) {
        debug_assert!(
            !std::ptr::eq(self, parent),
            "a scope cannot adopt from itself"
        );
        // The caller holds `gate` through `parent.with_observation_gate`.
        // Re-homing the parent would first have to acquire that same gate, so
        // rereading its installed pointer here cannot race a parent handoff.
        debug_assert!(
            gate.shares_gate(&parent.current_observation_gate()),
            "observation gates are adopted only in the parent-to-child direction"
        );
        self.member.adopt_observation_gate_with(
            gate,
            || {
                #[cfg(any(test, feature = "test-util"))]
                self.report_gate_capture(GateCapture::Adoption);
            },
            |current| {
                debug_assert!(
                    self.dynamic_route_in(txn).is_none(),
                    "a scope with a live dynamic route is never re-homed"
                );
                self.member.install_observation_gate_locked(current, gate);
                self.adopt_descendant_observation_gates_locked(current, gate, txn);
            },
        );
    }

    pub fn adopt_child_observation_gate(
        self: &Arc<Self>,
        member: &MemberCell,
        child: Option<&ScopeCell>,
        txn: &mut ObservationTxn<'_>,
    ) {
        let gate = self.current_observation_gate();
        if let Some(child) = child {
            child.adopt_observation_gate(self, &gate, txn);
        } else {
            member.adopt_observation_gate(&gate, txn);
        }
    }

    /// Re-homes a resident subtree while its prior tree gate is held. The
    /// destination gate is also held, so observers cannot enter either tree
    /// while the handoff is installed recursively. Walking residents is
    /// exhaustive here: a reserved dynamic slot requires the live route that
    /// only a started driver installs, while gate adoption happens before
    /// that driver can run, and no running scope is subsequently re-homed.
    fn adopt_descendant_observation_gates_locked(
        &self,
        previous: &ObservationGate,
        gate: &ObservationGate,
        _txn: &mut ObservationTxn<'_>,
    ) {
        let descendants = self
            .current_children()
            .iter()
            .map(|resident| resident.projection.clone())
            .collect::<Vec<_>>();
        for descendant in descendants {
            descendant
                .member
                .install_observation_gate_locked(previous, gate);
            if let Some(scope) = descendant.scope {
                scope.adopt_descendant_observation_gates_locked(previous, gate, _txn);
            }
        }
    }

    /// Runs against the current resident-tree observation gate. Adoption can
    /// race an early pre-start observer, so an obsolete-gate acquisition is
    /// detected and retried before the operation enters its critical section.
    pub fn with_observation_gate<R>(
        &self,
        operation: impl FnOnce(&mut ObservationTxn<'_>) -> R,
    ) -> R {
        self.member.with_observation_txn_probed(
            || {
                #[cfg(any(test, feature = "test-util"))]
                self.report_gate_capture(GateCapture::Observation);
            },
            operation,
        )
    }

    pub fn set_state(&self, state: ScopeState) {
        self.with_observation_gate(|txn| self.set_state_locked(state, &[], txn));
    }

    pub fn set_state_and_startup(&self, state: ScopeState, startup: Result<(), StartupError>) {
        self.with_observation_gate(|txn| {
            self.set_startup_locked(startup, txn);
            self.set_state_locked(state, &[], txn);
        });
    }

    /// Publishes drain entry together with terminal-cleanup intent selected by
    /// the same driver step.
    ///
    /// A zero-budget shutdown waiter wakes from the state publication and
    /// immediately samples descendants. Installing these markers under the
    /// shared observation gate keeps that sample from splitting drain entry
    /// between the scope transition and an inactive child's cleanup.
    pub fn publish_drain(
        &self,
        state: ScopeState,
        startup: Option<Result<(), StartupError>>,
        terminal_disposals: &[Arc<MemberCell>],
    ) {
        debug_assert!(matches!(state, ScopeState::Draining));
        self.with_observation_gate(|txn| {
            if let Some(startup) = startup {
                self.set_startup_locked(startup, txn);
            }
            self.set_state_locked(state, terminal_disposals, txn);
        });
    }

    fn set_state_locked(
        &self,
        state: ScopeState,
        terminal_disposals: &[Arc<MemberCell>],
        txn: &mut ObservationTxn<'_>,
    ) {
        // The marker slice is part of the state-writer signature so the #270
        // regression guard cannot be reordered at a caller. Every marker is
        // stored before the `Draining` record write whose release edge makes
        // it visible to zero-budget shutdown samplers on other workers.
        for member in terminal_disposals {
            debug_assert!(
                self.current_observation_gate()
                    .shares_gate(&member.current_observation_gate()),
                "a drain entry may mark only a resident member on its observation gate"
            );
            member.set_terminal_disposal_pending(true);
        }
        let mut transient_retained = Vec::new();
        RetainedExit::retain_scope_state(&mut transient_retained, &state);
        if matches!(state, ScopeState::Draining | ScopeState::StartupFailed)
            && let Some(route) = self.dynamic_route_in(txn)
        {
            route.close_admission(txn);
        }
        self.observation.record.modify_silently(|record| {
            record.state = state.clone();
            record.refresh_retained_exits();
        });
        // The scope record and emitted lifecycle event each install their own
        // guards. Avoid an extra disposal submission for this call-local
        // unwind guard.
        for exit in transient_retained {
            drop(exit.into_exit());
        }
        txn.pulse(&self.observation.record);
        txn.pulse(&self.member.record);
        self.emit_locked(txn, LifecycleEventKind::ScopeState { state });
    }

    pub fn set_child_removing_locked(&self, member: &MemberCell, txn: &mut ObservationTxn<'_>) {
        if member.record().membership_status == MembershipStatus::Removing {
            return;
        }
        member.update_locked(txn, |record| {
            record.membership_status = MembershipStatus::Removing;
        });
        self.publish_snapshot_chain_locked(txn);
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn transition_child(
        &self,
        member: &MemberCell,
        update: impl FnOnce(&mut MemberRecord),
        event: Option<LifecycleEventKind>,
    ) {
        self.with_observation_gate(|wakes| {
            member.update_locked(wakes, update);
            if let Some(event) = event {
                self.emit_locked(wakes, event);
            } else {
                self.publish_snapshot_chain_locked(wakes);
            }
        });
    }

    pub fn transition_child_stage(
        &self,
        member: &MemberCell,
        transition: MemberTransition,
        event: Option<LifecycleEventKind>,
    ) {
        // Routed through `transition_locked` rather than a record-only update
        // so a restart schedule's displaced exit leaves the gate on this path
        // too.
        self.with_observation_gate(|wakes| {
            member.transition_locked(wakes, transition);
            if let Some(event) = event {
                self.emit_locked(wakes, event);
            } else {
                self.publish_snapshot_chain_locked(wakes);
            }
        });
    }

    pub fn publish_child_restart(
        &self,
        member: &MemberCell,
        total_restarts: TotalRestarts,
        transition: MemberTransition,
        exited: LifecycleEventKind,
        scheduled: LifecycleEventKind,
    ) {
        self.with_observation_gate(|wakes| {
            self.observation.record.modify_silently(|scope| {
                scope.total_restarts = total_restarts;
            });
            wakes.pulse(&self.observation.record);
            member.transition_locked(wakes, transition);
            self.emit_locked(wakes, exited);
            self.emit_locked(wakes, scheduled);
        });
    }

    pub fn terminalize_child(
        &self,
        member: &MemberCell,
        exit: Exit,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        self.with_observation_gate(|wakes| {
            let record = member.record();
            if matches!(record.stage, MemberStage::Terminal(_)) {
                // The outer guard defines losing supervised terminalization:
                // no record or lifecycle edge is published. Keep the incoming
                // failed exit behind the same isolated-disposal boundary as a
                // losing direct terminalizer, and do not destroy it under the
                // observation gate.
                let exit = RetainedExit::new(exit);
                wakes.defer(move || drop(exit));
                return false;
            }
            // Nested publication and closure are reached only through this
            // residency lookup, so a supervised child must still be resident
            // here when it is terminalized. That holds because residency is
            // installed before the child can be spawned (`set_admitted_children`
            // for a planned incarnation, `admit_child` before dynamic
            // `spawn_child`) and is only withdrawn by pruning, which always
            // follows terminality. A child that has already left residency
            // owns its nested scope through `SlotCell::terminalize_never_started`
            // instead.
            let resident = self
                .current_children()
                .iter()
                .find(|resident| resident.projection.member.membership() == member.membership())
                .map(|resident| resident.projection.clone());
            debug_assert!(
                resident.is_some(),
                "a supervised terminal child must remain in parent residency"
            );
            let nested = resident.and_then(|resident| resident.scope);
            let terminal_exit = member.terminalize_locked(exit, startup, wakes);
            self.evict_child_identity(member);
            if record.last_incarnation.is_none()
                && let Some(scope) = &nested
            {
                scope.publish_stopped_locked(wakes, StopReason::NeverStarted, None, None);
            }
            if let Some(incarnation) = exited_incarnation {
                self.emit_locked(
                    wakes,
                    LifecycleEventKind::Exited {
                        id: member.id().clone(),
                        membership: member.membership(),
                        incarnation,
                        exit: terminal_exit,
                    },
                );
            } else {
                // Terminals without a current incarnation have no Exited
                // event to carry snapshot publication. Publish the final
                // parent projection explicitly before nested observation
                // closes.
                self.publish_snapshot_chain_locked(wakes);
            }
            if let Some(scope) = nested
                && matches!(scope.record().state, ScopeState::Stopped { .. })
            {
                // A parent fallback can terminalize a live nested membership
                // before cancellation drops the nested driver. Keep that
                // scope's stream open until its own epilogue publishes the
                // final Stopped record; otherwise waiters would receive a
                // terminal stream carrying Starting/Running as its payload.
                scope.close_observation_locked(wakes);
            }
            true
        })
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn prune_child(&self, member: &MemberCell) -> bool {
        self.with_observation_gate(|wakes| self.prune_child_locked(member, wakes))
    }

    pub fn prune_child_locked(&self, member: &MemberCell, txn: &mut ObservationTxn<'_>) -> bool {
        let membership = member.membership();
        let resident = {
            let mut children = self.current_children();
            let index = children
                .iter()
                .position(|child| child.projection.member.membership() == membership);
            index.map(|index| children.remove(index))
        };
        let Some(resident) = resident else {
            return false;
        };
        debug_assert_eq!(resident.projection.member.membership(), membership);
        let event = LifecycleEventKind::Removed {
            id: resident.projection.member.id().clone(),
            membership,
            last_incarnation: resident.projection.member.record().last_incarnation,
        };
        // The projection can carry the last member/mailbox owner. Put it in
        // the transaction before the fallible publication path so unwind also
        // retires it only after the observation gate is released.
        txn.defer(move || drop(resident));
        self.emit_locked(txn, event);
        true
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn replace_observation_gate(&self, gate: ObservationGate) {
        *self
            .member
            .observation_gate
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = gate;
    }

    pub fn set_startup(&self, startup: Result<(), StartupError>) {
        self.with_observation_gate(|txn| self.set_startup_locked(startup, txn));
    }

    fn set_startup_locked(&self, startup: Result<(), StartupError>, txn: &mut ObservationTxn<'_>) {
        let mut retained = Vec::new();
        RetainedExit::retain_startup_result(&mut retained, &startup);
        let mut incoming = Some((startup, retained));
        let mut published = false;
        self.observation.record.modify_silently(|record| {
            if record.startup.is_none() {
                let (startup, retained) = incoming
                    .take()
                    .expect("startup result is installed at most once");
                record.startup = Some(startup);
                record.refresh_retained_exits();
                // The record now owns an equivalent retained copy. Convert
                // these transient guards back to raw refcount traffic rather
                // than submitting duplicate disposal jobs under the gate.
                for exit in retained {
                    drop(exit.into_exit());
                }
                published = true;
            }
        });
        // Tuple field order is intentional: a rejected raw startup result is
        // released while its retained guards still exist, then those guards
        // transfer failed destruction to isolated disposal.
        txn.defer(move || drop(incoming));
        if published {
            txn.pulse(&self.member.record);
            txn.pulse(&self.observation.record);
        }
    }

    pub fn begin_incarnation(&self, state: ScopeState) -> Option<Epoch> {
        debug_assert!(
            matches!(state, ScopeState::Starting),
            "a fresh incarnation publishes its lifecycle machine's initial state"
        );
        self.with_observation_gate(|wakes| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            let epoch = control.epochs.begin()?;
            // The idle epoch plane pairs only with a settled projection:
            // `Unstarted` before any mint, `Stopped` after every finish. That
            // pairing is what lets `settled` treat terminal membership
            // plus a settled projection as final without stranding a scope
            // that still owns a live incarnation.
            debug_assert!(
                matches!(
                    self.record().state,
                    ScopeState::Unstarted | ScopeState::Stopped { .. }
                ),
                "an idle scope projection is Unstarted or Stopped before a fresh incarnation mints"
            );
            self.observation.record.modify_silently(|record| {
                record.total_restarts = TotalRestarts::ZERO;
                record.startup = None;
                record.state = state.clone();
                record.refresh_retained_exits();
            });
            // Hold epoch ownership through its observation projection. A
            // stale finish and a newer begin can no longer cross these two
            // state planes in opposite orders.
            drop(control);
            wakes.pulse(&self.observation.record);
            wakes.pulse(&self.member.record);
            self.emit_locked(wakes, LifecycleEventKind::ScopeState { state });
            Some(epoch)
        })
    }

    pub fn finish_incarnation(&self, epoch: Epoch, reason: StopReason) {
        self.finish_incarnation_with_terminal(epoch, reason, None);
    }

    pub fn finish_root_incarnation(&self, epoch: Epoch, reason: StopReason, exit: Exit) {
        self.finish_incarnation_with_terminal(epoch, reason, Some(exit));
    }

    fn finish_incarnation_with_terminal(
        &self,
        epoch: Epoch,
        reason: StopReason,
        terminal_exit: Option<Exit>,
    ) {
        self.with_observation_gate(|wakes| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if !control.epochs.finish(epoch) {
                // A stale driver must not overwrite the observation
                // projection of a newer live incarnation. Membership
                // terminality is not part of that projection: whoever owns a
                // terminal exit still publishes it exactly once, so declining
                // the epoch can never strand `wait_terminal`.
                drop(control);
                if let Some(exit) = terminal_exit {
                    self.member
                        .terminalize_locked(exit, StartupDisposition::Unchanged, wakes);
                    wakes.pulse(&self.member.record);
                    wakes.pulse(&self.observation.record);
                    self.close_observation_locked(wakes);
                }
                // `StopReason::StartupFailed` recursively owns the failed
                // child's user error. A stale verdict is unused, but the
                // framework still owns this copy: retaining it before the
                // deferred drop sends a possibly-blocking or panicking user
                // destructor to `dispose_critical` instead of running it
                // inline on the committing thread once the gate is released.
                let reason = RetainedStopReason::new(reason);
                wakes.defer(move || drop(reason));
                return;
            }
            if control
                .shutdown
                .is_some_and(|request| request.epoch <= epoch)
            {
                control.shutdown = None;
            }
            if control.force.is_some_and(|request| request.epoch <= epoch) {
                control.force = None;
            }
            let terminal = terminal_exit.is_some();
            let membership_terminal =
                matches!(self.member.record().stage, MemberStage::Terminal(_));
            self.publish_stopped_locked(wakes, reason, terminal_exit, Some(control));
            if terminal || membership_terminal {
                // A parent-driver fallback may have terminalized this nested
                // membership while its live scope epilogue was still
                // pending. The epilogue owns the final Stopped projection and
                // closes observation only after publishing it.
                self.close_observation_locked(wakes);
            }
        });
    }

    pub fn finish_live_root_incarnation(&self, reason: StopReason, exit: Exit) {
        let epoch = {
            let control = self.control.lock().expect("scope control mutex poisoned");
            control.epochs.live_epoch()
        };
        if let Some(epoch) = epoch {
            self.finish_root_incarnation(epoch, reason, exit);
        } else {
            self.with_observation_gate(|wakes| {
                self.publish_stopped_locked(wakes, reason, Some(exit), None);
                self.close_observation_locked(wakes);
            });
        }
    }

    pub fn request_shutdown(&self) -> Option<Epoch> {
        self.with_observation_gate(|txn| {
            let control = self.control.lock().expect("scope control mutex poisoned");
            self.request_shutdown_locked(control, txn)
        })
    }

    /// [`Self::request_shutdown`] for destructors: tolerates a poisoned
    /// control mutex so a drop-path request cannot panic — and abort — on a
    /// thread that is already unwinding. Control holds plain request state,
    /// so overwriting a poisoner's partial update is no worse than any other
    /// racing request.
    pub fn request_shutdown_ignoring_poison(&self) -> Option<Epoch> {
        self.with_observation_gate(|txn| {
            let control = self
                .control
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.request_shutdown_locked(control, txn)
        })
    }

    fn request_shutdown_locked(
        &self,
        mut control: MutexGuard<'_, ScopeControl>,
        txn: &mut ObservationTxn<'_>,
    ) -> Option<Epoch> {
        let RequestTarget {
            epoch: target,
            pending_incarnation,
        } = control.epochs.request_target()?;
        let published = control
            .shutdown
            .is_none_or(|request| request.epoch < target);
        if published {
            control.shutdown = Some(ScopeRequest {
                epoch: target,
                consumed: false,
            });
        }
        drop(control);
        if published {
            txn.pulse(&self.member.record);
            if pending_incarnation && let Some(parent) = self.parent() {
                parent.publish_control_event_locked(
                    ScopeControlEvent::RestartShutdown {
                        membership: self.member.membership(),
                        target,
                    },
                    txn,
                );
            }
        }
        Some(target)
    }

    fn publish_control_event_locked(&self, event: ScopeControlEvent, txn: &mut ObservationTxn<'_>) {
        self.control
            .lock()
            .expect("scope control mutex poisoned")
            .events
            .push_back(event);
        txn.pulse(&self.member.record);
    }

    pub fn take_control_events(&self) -> Vec<ScopeControlEvent> {
        if self
            .control
            .lock()
            .expect("scope control mutex poisoned")
            .events
            .is_empty()
        {
            return Vec::new();
        }
        self.with_observation_gate(|_txn| {
            self.control
                .lock()
                .expect("scope control mutex poisoned")
                .events
                .drain(..)
                .collect()
        })
    }

    pub fn has_pending_incarnation_shutdown(&self, target: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.shutdown.is_some_and(|request| {
            request.epoch == target
                && !request.consumed
                && control.epochs.request_is_pending(target)
        })
    }

    pub fn has_stop_request(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control
            .shutdown
            .is_some_and(|request| request.epoch == epoch)
            || control.force.is_some_and(|request| request.epoch == epoch)
    }

    pub fn take_shutdown_request(&self, epoch: Epoch) -> bool {
        self.take_request(ScopeRequestSlot::Shutdown, epoch)
    }

    pub fn force_shutdown(&self, epoch: Epoch) {
        self.with_observation_gate(|txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if control.epochs.is_current(epoch) {
                control.force = Some(ScopeRequest {
                    epoch,
                    consumed: false,
                });
            }
            drop(control);
            txn.pulse(&self.member.record);
        });
    }

    pub fn take_force_request(&self, epoch: Epoch) -> bool {
        self.take_request(ScopeRequestSlot::Force, epoch)
    }

    fn take_request(&self, slot: ScopeRequestSlot, epoch: Epoch) -> bool {
        let pending = slot
            .get(&self.control.lock().expect("scope control mutex poisoned"))
            .is_some_and(|request| request.epoch == epoch && !request.consumed);
        if !pending {
            return false;
        }
        self.with_observation_gate(|_txn| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            match slot.get_mut(&mut control) {
                Some(request) if request.epoch == epoch && !request.consumed => {
                    request.consumed = true;
                    true
                }
                _ => false,
            }
        })
    }

    fn incarnation_complete(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.epochs.finished(epoch)
    }

    fn membership_terminal(&self) -> bool {
        matches!(self.member.record().stage, MemberStage::Terminal(_))
    }

    fn joined(&self) -> bool {
        matches!(
            self.record().state,
            ScopeState::Stopped { .. } | ScopeState::Unstarted
        )
    }

    /// Whether a shutdown wait has crossed the finality fence for its target.
    ///
    /// Membership terminality alone is insufficient: parent-driver
    /// destruction publishes it before the aborted nested driver runs the
    /// scope epilogue that finishes the incarnation and publishes `Stopped`.
    /// `None` is used only by the entry check, before a target epoch exists.
    ///
    /// This predicate is strictly weaker than membership terminality, so
    /// shutdown liveness now rests on two structural invariants. First, every
    /// live epoch has exactly one owner — the pre-driver epoch guard before a
    /// scope runtime exists, the scope runtime itself afterwards — and both
    /// finish it from `Drop`, so an unsettled target always has a pending
    /// finisher. Second, an idle epoch plane implies a settled projection:
    /// `begin_incarnation` is the only mint and it publishes `Starting`
    /// under the control guard,
    /// while [`Self::finish_incarnation`] always publishes `Stopped` under
    /// that same guard, so `ScopeEpochs::Idle`/`Exhausted` can only pair with
    /// `Unstarted` (never begun) or `Stopped`. Together they mean the
    /// terminal-membership arm can never be the *only* reachable settlement
    /// for a scope that still owns work.
    pub fn settled(&self, epoch: Option<Epoch>) -> bool {
        epoch.is_some_and(|epoch| self.incarnation_complete(epoch))
            || (self.membership_terminal() && self.joined())
    }

    pub fn set_admitted_children(self: &Arc<Self>, children: Vec<ResidentProjection>) {
        self.with_observation_gate(|wakes| {
            self.clear_residents_locked(wakes);
            for child in children {
                self.admit_child_locked(child, wakes);
            }
        });
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn admit_child(self: &Arc<Self>, child: ResidentProjection) {
        self.with_observation_gate(|wakes| self.admit_child_locked(child, wakes));
    }

    pub fn admit_child_locked(
        self: &Arc<Self>,
        child: ResidentProjection,
        txn: &mut ObservationTxn<'_>,
    ) {
        let gate = self.current_observation_gate();
        if let Some(scope) = &child.scope {
            scope.adopt_observation_gate(self, &gate, txn);
            scope.set_parent(self, txn);
        } else {
            child.member.adopt_observation_gate(&gate, txn);
        }
        let id = child.member.id().clone();
        let membership = child.member.membership();
        child
            .member
            .transition_locked(txn, MemberTransition::Admitted);
        self.current_children().push(ResidentChild::new(child));
        self.emit_locked(txn, LifecycleEventKind::Added { id, membership });
    }

    pub fn clear_residents(&self) {
        self.with_observation_gate(|wakes| self.clear_residents_locked(wakes));
    }

    pub fn clear_residents_locked(&self, wakes: &mut ObservationTxn<'_>) {
        let residents = {
            let mut children = self.current_children();
            std::mem::take(&mut *children)
        };
        let removals = residents
            .iter()
            .map(|resident| LifecycleEventKind::Removed {
                id: resident.projection.member.id().clone(),
                membership: resident.projection.member.membership(),
                last_incarnation: resident.projection.member.record().last_incarnation,
            })
            .collect::<Vec<_>>();
        // Schedule the whole displaced set before emitting any edge. This
        // both preserves last-owner disposal and makes an unwind retire the
        // untouched suffix after unlock.
        wakes.defer(move || drop(residents));
        for removal in removals {
            self.emit_locked(wakes, removal);
        }
    }

    pub fn set_dynamic_route(&self, route: Option<Arc<dyn DynamicRoute>>) {
        self.with_observation_gate(|txn| {
            self.set_dynamic_route_locked(route, txn);
        });
    }

    pub fn set_dynamic_route_locked(
        &self,
        route: Option<Arc<dyn DynamicRoute>>,
        txn: &mut ObservationTxn<'_>,
    ) {
        let previous = std::mem::replace(
            &mut *self
                .dynamic_route
                .lock()
                .expect("scope dynamic-route mutex poisoned"),
            route,
        );
        txn.defer(move || drop(previous));
        txn.pulse(&self.member.record);
    }

    pub fn dynamic_route_in(&self, _txn: &ObservationTxn<'_>) -> Option<Arc<dyn DynamicRoute>> {
        self.dynamic_route
            .lock()
            .expect("scope dynamic-route mutex poisoned")
            .clone()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn dynamic_route(&self) -> Option<Arc<dyn DynamicRoute>> {
        self.with_observation_gate(|txn| self.dynamic_route_in(txn))
    }

    pub fn signal(&self) -> &runtime::WatchSender<MemberRecord> {
        &self.member.record
    }

    pub async fn wait_started(&self) -> Result<(), StartupError> {
        let mut watcher = self.observation.record.watcher();
        loop {
            if let Some(result) = watcher.borrow_and_update_cloned().startup {
                return result;
            }
            watcher.changed().await;
        }
    }

    pub async fn wait_stopped(&self) -> StopReason {
        self.member.wait_terminal().await;
        // Parent-driver destruction can terminalize a nested membership
        // synchronously before the aborted nested driver runs its own scope
        // epilogue. Membership terminality is therefore the finality fence,
        // not proof that the scope record has already reached `Stopped`.
        let mut watcher = self.observation.record.watcher();
        loop {
            match watcher.borrow_and_update_cloned().state {
                ScopeState::Stopped { reason } => return reason,
                ScopeState::Unstarted => return StopReason::NeverStarted,
                ScopeState::Starting
                | ScopeState::Running
                | ScopeState::StartupFailed
                | ScopeState::Draining => watcher.changed().await,
            }
        }
    }

    /// Commits a stopped-scope projection monotonically and applies its
    /// optional member terminal edge under the resident-tree observation gate.
    ///
    /// Several owners can reach a stop verdict for one incarnation — a
    /// driver's drain epilogue, a join monitor's fallback, a never-started
    /// terminalization — so competing reasons resolve through
    /// `StopPrecedence`, never through arrival order. A publication commits
    /// only when it strictly outranks the recorded reason; equal or weaker
    /// verdicts are idempotent repeats that mutate nothing. An upgrade
    /// republishes the record, the snapshot and a corrected `ScopeState` edge,
    /// so the stream never ends on an event that disagrees with the final
    /// record (SPEC B.4's non-final-`Stopped` rule admits exactly this step).
    ///
    /// Member terminalization prepares mailbox teardown first; its deferred
    /// discharge is therefore queued before either member or scope pulses.
    /// `epoch_owner` carries a live incarnation's control ownership through
    /// both retained record mutations and is released before snapshot and
    /// lifecycle publication, preserving the stop transition's ownership
    /// boundary. A suppressed repeat still terminalizes the member and still
    /// releases the epoch: only the scope-record mutation, snapshot pulse and
    /// lifecycle edge are skipped. Because the ancestor snapshot chain is
    /// republished by `emit_locked`, a suppressed repeat publishes no ancestor
    /// snapshot either — a caller whose member terminal edge must reach an
    /// ancestor projection has to publish that itself. Observation closure
    /// remains a caller decision because a nested scope's final event must
    /// precede its parent's terminal event and only then close the nested
    /// streams; a subscriber attaching after the final event and before that
    /// closure therefore resolves by closure alone, as it already does on the
    /// stale-epoch path above.
    fn publish_stopped_locked(
        &self,
        wakes: &mut ObservationTxn<'_>,
        reason: StopReason,
        terminal_exit: Option<Exit>,
        epoch_owner: Option<MutexGuard<'_, ScopeControl>>,
    ) {
        let incoming = stop_reason_precedence(&reason);
        let state = ScopeState::Stopped { reason };
        let mut transient_retained = Vec::new();
        RetainedExit::retain_scope_state(&mut transient_retained, &state);
        let mut published = false;
        self.observation.record.modify_silently(|record| {
            if let ScopeState::Stopped { reason: recorded } = &record.state
                && incoming <= stop_reason_precedence(recorded)
            {
                return;
            }
            if record.startup.is_none() {
                record.startup = Some(Err(StartupError::ShutdownRequested));
            }
            record.state = state.clone();
            record.refresh_retained_exits();
            published = true;
        });
        if let Some(exit) = terminal_exit {
            self.member
                .terminalize_locked(exit, StartupDisposition::Unchanged, wakes);
        }
        drop(epoch_owner);
        wakes.pulse(&self.member.record);
        // `wait_started` must not observe terminal startup until the member
        // and incarnation-control planes are mutually consistent.
        if published {
            // The record and lifecycle event now retain the raw projection.
            // Surrender the transient guards without scheduling duplicate
            // disposal jobs.
            for exit in transient_retained {
                drop(exit.into_exit());
            }
            wakes.pulse(&self.observation.record);
            self.emit_locked(wakes, LifecycleEventKind::ScopeState { state });
        } else {
            // Release the rejected raw projection before its guards. This is
            // the same field order used by retained framework state.
            wakes.defer(move || {
                drop(state);
                drop(transient_retained);
            });
        }
    }

    pub fn terminalize_never_started(&self) {
        self.with_observation_gate(|txn| self.terminalize_never_started_locked(txn));
    }

    pub fn terminalize_never_started_locked(&self, txn: &mut ObservationTxn<'_>) {
        if self.observation.closed.load(Ordering::Acquire) {
            return;
        }
        self.member
            .terminalize_locked(Exit::never_started(), StartupDisposition::Unchanged, txn);
        self.publish_stopped_locked(txn, StopReason::NeverStarted, None, None);
        self.close_observation_locked(txn);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use shelterwood_core::{
        Cancellation, ExitError, IntensityTrip, StartupFailure, StartupFailureCause,
        identity::ScopeIdentity,
        policy::{ResolvedMailbox, ScopeFlavor},
    };
    use shelterwood_mailbox::{
        MailboxCell, MailboxControl, MailboxEffectQueue, MailboxReceiver, actor_ref_from_parts,
    };

    use super::*;
    use crate::{
        cells::test_support::{TEST_WAIT, ThreadProbe, child_member, child_scope, isolated_scope},
        observe::LifecycleItem,
    };

    struct GateCheckingWake {
        gate: super::ObservationGate,
        woke_after_unlock: Arc<AtomicBool>,
    }

    impl Wake for GateCheckingWake {
        fn wake(self: Arc<Self>) {
            self.woke_after_unlock
                .store(!self.gate.is_held(), Ordering::SeqCst);
        }
    }

    struct GateDropMessage {
        gate: super::ObservationGate,
        dropped: mpsc::SyncSender<bool>,
    }

    impl Drop for GateDropMessage {
        fn drop(&mut self) {
            let _ = self.dropped.send(!self.gate.is_held());
        }
    }

    #[test]
    fn adoption_retries_when_the_captured_child_gate_has_already_been_replaced() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let replacement = isolated_scope("replacement", ScopeFlavor::Ordered);
        let captures = nested.probe_gate_captures();
        let prior_gate = nested.observation_gate();
        let held = prior_gate.lock();
        let adopting_root = Arc::clone(&root);
        let adopting_nested = Arc::clone(&nested);
        let adoption = std::thread::spawn(move || {
            adopting_root.admit_child(ResidentProjection::new(
                Arc::clone(&adopting_nested.member),
                Some(adopting_nested),
            ));
        });

        assert_eq!(
            captures
                .recv_timeout(TEST_WAIT)
                .expect("adoption captures the child's prior gate"),
            GateCapture::Adoption
        );
        nested.replace_observation_gate(replacement.observation_gate());
        drop(held);
        assert_eq!(
            captures
                .recv_timeout(TEST_WAIT)
                .expect("adoption retries after observing the replacement"),
            GateCapture::Adoption
        );
        adoption.join().expect("the retried adoption completes");

        assert!(
            root.observation_gate()
                .same_gate(&nested.observation_gate())
        );
        assert!(root.has_resident_child(&nested.member));
        assert_eq!(captures.try_recv(), Err(mpsc::TryRecvError::Empty));
    }

    #[test]
    fn adopting_a_resident_subtree_rehomes_every_descendant_gate() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
        let leaf_scope = child_scope(&nested, "leaf-scope", ScopeFlavor::Ordered);
        let leaf_member = child_member(&nested, "leaf-member");
        // Depth 2. The one-level loop in
        // `adopt_descendant_observation_gates_locked` re-homes `leaf_scope`'s
        // own member, and `ScopeCell::observation_gate` reads that member, so
        // a subtree of depth 1 cannot tell the loop from the recursion. Only
        // this grandchild is reachable exclusively through the recursive call.
        let grandchild = child_member(&leaf_scope, "grandchild");
        leaf_scope
            .set_admitted_children(vec![ResidentProjection::new(Arc::clone(&grandchild), None)]);
        nested.set_admitted_children(vec![
            ResidentProjection::new(
                Arc::clone(&leaf_scope.member),
                Some(Arc::clone(&leaf_scope)),
            ),
            ResidentProjection::new(Arc::clone(&leaf_member), None),
        ]);

        root.admit_child(ResidentProjection::new(
            Arc::clone(&nested.member),
            Some(Arc::clone(&nested)),
        ));

        let root_gate = root.observation_gate();
        for gate in [
            nested.observation_gate(),
            leaf_scope.observation_gate(),
            leaf_member.observation_gate(),
            grandchild.observation_gate(),
        ] {
            assert!(
                root_gate.same_gate(&gate),
                "every descendant joins the adopting parent's observation gate"
            );
        }
        assert!(Arc::ptr_eq(
            &nested.parent().expect("nested parent is installed"),
            &root
        ));
        assert!(Arc::ptr_eq(
            &leaf_scope.parent().expect("leaf parent is installed"),
            &nested
        ));
    }

    #[test]
    fn drain_publication_installs_terminal_disposal_intent_with_the_state() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let child = child_member(&root, "child");
        root.set_admitted_children(vec![ResidentProjection::new(Arc::clone(&child), None)]);

        root.publish_drain(ScopeState::Draining, None, &[Arc::clone(&child)]);

        assert!(matches!(root.record().state, ScopeState::Draining));
        assert!(child.terminal_or_disposal_pending());
    }

    #[test]
    fn residency_admits_prunes_and_clears_exact_memberships() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = child_scope(&root, "nested", ScopeFlavor::Dynamic);
        let leaf = child_member(&root, "leaf");
        let nested_membership = nested.member.membership();
        let leaf_membership = leaf.membership();
        let mut lifecycle = root.subscribe_lifecycle();

        root.set_admitted_children(vec![
            ResidentProjection::new(Arc::clone(&nested.member), Some(Arc::clone(&nested))),
            ResidentProjection::new(Arc::clone(&leaf), None),
        ]);
        assert_eq!(root.resident_projections().len(), 2);
        assert!(root.has_resident_child(&nested.member));
        assert!(root.has_resident_child(&leaf));
        assert!(matches!(
            nested.member.record().stage,
            MemberStage::Admitted
        ));
        assert!(matches!(leaf.record().stage, MemberStage::Admitted));

        assert!(root.prune_child(&nested.member));
        assert!(!root.prune_child(&nested.member));
        assert!(!root.has_resident_child(&nested.member));
        assert!(root.has_resident_child(&leaf));
        root.clear_residents();
        assert!(root.resident_projections().is_empty());

        let mut edges = Vec::new();
        while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
            match event.kind {
                LifecycleEventKind::Added { membership, .. } => {
                    edges.push(("added", membership));
                }
                LifecycleEventKind::Removed { membership, .. } => {
                    edges.push(("removed", membership));
                }
                _ => panic!("residency mutation emitted an unrelated lifecycle edge"),
            }
        }
        assert_eq!(
            edges,
            [
                ("added", nested_membership),
                ("added", leaf_membership),
                ("removed", nested_membership),
                ("removed", leaf_membership),
            ]
        );
    }

    #[test]
    fn stopped_publication_uses_the_full_precedence_lattice_and_strict_upgrades() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let mut lifecycle = scope.subscribe_lifecycle();
        let ascending = vec![
            StopReason::Finished,
            StopReason::IntensityTripped(IntensityTrip {
                max_restarts: 1,
                observed_restarts: 2,
                within: Duration::from_secs(1),
            }),
            StopReason::StartupFailed(StartupFailure {
                cause: StartupFailureCause::Lowering {
                    undefined: vec![ChildId::from("missing")],
                },
            }),
            StopReason::ShutdownRequested,
            StopReason::NeverStarted,
        ];

        for reason in &ascending {
            scope.with_observation_gate(|txn| {
                scope.publish_stopped_locked(txn, reason.clone(), None, None);
            });
            assert_eq!(
                scope.record().state,
                ScopeState::Stopped {
                    reason: reason.clone()
                }
            );
        }
        for reason in ascending.iter().rev() {
            scope.with_observation_gate(|txn| {
                scope.publish_stopped_locked(txn, reason.clone(), None, None);
            });
        }

        let mut published = Vec::new();
        while let Ok(LifecycleItem::Event(event)) = lifecycle.try_recv() {
            if let LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped { reason },
            } = event.kind
            {
                published.push(reason);
            }
        }
        assert_eq!(
            published, ascending,
            "only strict precedence upgrades publish a stopped-state edge"
        );
        assert_eq!(
            scope.record().state,
            ScopeState::Stopped {
                reason: StopReason::NeverStarted
            }
        );
    }

    #[test]
    fn shutdown_and_force_requests_are_exactly_once_and_epoch_scoped() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let first = scope
            .begin_incarnation(ScopeState::Starting)
            .expect("the first epoch is available");
        assert_eq!(scope.request_shutdown(), Some(first));
        assert!(scope.take_shutdown_request(first));
        assert!(!scope.take_shutdown_request(first));
        scope.force_shutdown(first);
        assert!(scope.take_force_request(first));
        assert!(!scope.take_force_request(first));
        scope.finish_incarnation(first, StopReason::Finished);

        let second = scope
            .begin_incarnation(ScopeState::Starting)
            .expect("the next epoch is available");
        assert_ne!(first, second);
        scope.force_shutdown(first);
        assert!(!scope.take_force_request(first));
        assert!(!scope.take_force_request(second));
        assert_eq!(scope.request_shutdown(), Some(second));
        assert!(!scope.take_shutdown_request(first));
        assert!(scope.take_shutdown_request(second));
        scope.force_shutdown(second);
        assert!(scope.take_force_request(second));
        assert!(!scope.take_force_request(second));
        scope.finish_incarnation(second, StopReason::ShutdownRequested);
    }

    #[test]
    fn a_stale_scope_verdict_disposes_its_nested_exit_off_the_finishing_thread() {
        let finishing_thread = std::thread::current().id();
        let mut identity = ScopeIdentity::new();
        let root_id = ChildId::from("root");
        let root = MemberCell::new(
            root_id.clone(),
            identity
                .mint_membership(&root_id)
                .expect("root membership is available"),
        );
        let scope = ScopeCell::new(root, ScopeFlavor::Ordered, ScopeIdentity::new());
        let child_id = ChildId::from("worker");
        let child = MemberCell::new(
            child_id.clone(),
            identity
                .mint_membership(&child_id)
                .expect("child membership is available"),
        );

        let stale = scope
            .begin_incarnation(ScopeState::Starting)
            .expect("first scope epoch is available");
        scope.finish_incarnation(stale, StopReason::Finished);
        let live = scope
            .begin_incarnation(ScopeState::Starting)
            .expect("second scope epoch is available");

        // The stale epoch is declined, so this structured verdict is never
        // published — but the framework still owns the failed child `Exit` it
        // recursively carries, and must not run that user destructor inline.
        let (dropped, observed) = mpsc::sync_channel(1);
        scope.finish_incarnation(
            stale,
            StopReason::StartupFailed(StartupFailure {
                cause: StartupFailureCause::Child {
                    id: child_id,
                    membership: child.membership(),
                    exit: Exit::failed(
                        ExitError::from(ThreadProbe(dropped)),
                        Cancellation::NotObserved,
                    ),
                },
            }),
        );

        let disposal_thread = observed
            .recv_timeout(Duration::from_secs(10))
            .expect("the stale verdict's nested exit is destroyed");
        assert_ne!(
            disposal_thread, finishing_thread,
            "a declined stop reason must not run its nested user destructor on the \
             finishing thread"
        );
        assert_eq!(
            scope.record().state,
            ScopeState::Starting,
            "the stale verdict must not rewrite the newer incarnation"
        );
        scope.finish_incarnation(live, StopReason::Finished);
    }

    #[test]
    fn destructor_shutdown_tolerates_a_poisoned_control_mutex() {
        let id = ChildId::from("root");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("root membership is available"),
        );
        let scope = ScopeCell::new(member, ScopeFlavor::Dynamic, ScopeIdentity::new());

        let poison = Arc::clone(&scope);
        assert!(
            catch_unwind(AssertUnwindSafe(move || {
                let _control = poison.control.lock().expect("control starts healthy");
                panic!("inject control poison");
            }))
            .is_err()
        );

        assert!(scope.request_shutdown_ignoring_poison().is_some());
    }

    #[test]
    fn mailbox_control_wakes_are_deferred_past_the_observation_gate() {
        let id = ChildId::from("root");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("root membership is available"),
        );
        let mut incarnations = member.take_incarnation_counter();
        let incarnation = incarnations.mint().expect("incarnation available");
        let scope = ScopeCell::new(member, ScopeFlavor::Dynamic, ScopeIdentity::new());
        let gate = scope.observation_gate();
        let mailbox = MailboxCell::<u8>::new(id, shelterwood_runtime::mailbox_runtime());
        let mut effects = MailboxEffectQueue::default();
        let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
        MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
        drop(effects);
        let mut receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);
        let woke_after_unlock = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(GateCheckingWake {
            gate: gate.clone(),
            woke_after_unlock: Arc::clone(&woke_after_unlock),
        }));
        let mut changed = Box::pin(receiver.changed());
        assert!(matches!(
            changed.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));

        scope.with_observation_gate(|txn| {
            MailboxControl::freeze(&*mailbox, incarnation, txn);
        });

        assert!(woke_after_unlock.load(Ordering::SeqCst));
    }

    #[test]
    fn clearing_residents_releases_the_last_mailbox_owner_after_unlock() {
        let root_id = ChildId::from("root");
        let mut root_identity = ScopeIdentity::new();
        let root_member = MemberCell::new(
            root_id.clone(),
            root_identity
                .mint_membership(&root_id)
                .expect("root membership is available"),
        );
        let scope = ScopeCell::new(root_member, ScopeFlavor::Dynamic, ScopeIdentity::new());
        let gate = scope.observation_gate();

        let child_id = ChildId::from("child");
        let mut child_identity = ScopeIdentity::new();
        let child = MemberCell::new(
            child_id.clone(),
            child_identity
                .mint_membership(&child_id)
                .expect("child membership is available"),
        );
        let mut incarnations = child.take_incarnation_counter();
        let incarnation = incarnations.mint().expect("incarnation available");
        let mailbox = MailboxCell::new(child_id, shelterwood_runtime::mailbox_runtime());
        child.attach_mailbox(mailbox.clone());
        let actor = actor_ref_from_parts(Arc::clone(&child), Arc::clone(&mailbox));
        let mut effects = MailboxEffectQueue::default();
        let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
        MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
        drop(effects);
        let (dropped, observed) = mpsc::sync_channel(1);
        actor
            .try_send(GateDropMessage { gate, dropped })
            .expect("bound mailbox accepts the probe");
        scope.admit_child(ResidentProjection::new(Arc::clone(&child), None));
        drop(actor);
        drop(mailbox);
        drop(child);

        scope.clear_residents();

        assert!(
            observed
                .recv_timeout(Duration::from_secs(10))
                .expect("resident mailbox payload destructor reports"),
            "the displaced resident owner is released after the gate unlocks"
        );
    }
}
