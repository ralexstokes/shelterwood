use std::{
    any::Any,
    cell::{Ref, RefCell},
    collections::VecDeque,
    rc::Rc,
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
    panic::{catch_panic, discard_panic},
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

/// By-value admission input protected until residency owns it.
///
/// A projection can be the last owner of a member and its mailbox. Any
/// bookkeeping panic before installation therefore routes the whole graph to
/// detached disposal instead of unwinding it through the observation gate.
///
/// The transaction holds the second `Rc` rather than this guard disposing
/// from its own `Drop`: a bare guard would submit the disposal job with the
/// observation gate still held. That is legal — a submission runs no user
/// code — but the standing preference is that a caller already holding an
/// effects sink flushes through it, because a submission can cost a native
/// thread start. #390 tracks deleting the guard outright by letting residency
/// own the projection across admission's fallible steps.
struct ResidentAdmission(Rc<RefCell<Option<ResidentProjection>>>);

impl ResidentAdmission {
    fn new(projection: ResidentProjection, txn: &mut ObservationTxn<'_>) -> Self {
        let projection = Rc::new(RefCell::new(Some(projection)));
        let deferred = Rc::clone(&projection);
        txn.defer(move || {
            if let Some(projection) = deferred.borrow_mut().take() {
                runtime::dispose_detached(projection);
            }
        });
        Self(projection)
    }

    fn projection(&self) -> Ref<'_, ResidentProjection> {
        Ref::map(self.0.borrow(), |projection| {
            projection
                .as_ref()
                .expect("resident admission was already installed")
        })
    }

    fn install(self) -> ResidentProjection {
        self.0
            .borrow_mut()
            .take()
            .expect("resident admission installs exactly once")
    }
}

/// One slot of a scope's observed child set, with its own unwind boundary.
///
/// A displaced set is disposed as a whole `Vec`, and `Vec`'s slice drop glue
/// keeps destroying the remaining elements after one of them panics. A
/// resident can own the last handle to a mailbox still holding unread user
/// messages, so without a boundary here a second hostile destructor in the
/// same set panics *inside* the first one's unwind, which aborts the process
/// rather than surfacing anywhere. SPEC §5.5 requires this lane to run with
/// per-element panic containment; keeping the boundary on the element rather
/// than on the collection is what makes it hold at every depth of a nested
/// scope's residency, and on `ScopeCell`'s own drop glue, not merely at the
/// displaced root.
///
/// The diagnostic is discarded rather than reported because every venue that
/// destroys a resident already discards it: `dispose_detached` passes an
/// empty completion, and plain drop glue has nobody to report to.
struct ResidentChild {
    /// `None` only while [`Drop`] is destroying the projection.
    projection: Option<ResidentProjection>,
}

impl ResidentChild {
    fn new(projection: ResidentProjection) -> Self {
        Self {
            projection: Some(projection),
        }
    }

    fn projection(&self) -> &ResidentProjection {
        self.projection
            .as_ref()
            .expect("a resident child owns its projection until it is dropped")
    }
}

impl Drop for ResidentChild {
    fn drop(&mut self) {
        let Some(projection) = self.projection.take() else {
            return;
        };
        discard_panic(catch_panic(|| drop(projection)).err());
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
            .map(|resident| resident.projection().clone())
            .collect()
    }

    pub fn has_resident_child(&self, member: &MemberCell) -> bool {
        self.current_children()
            .iter()
            .any(|resident| resident.projection().member.membership() == member.membership())
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

    /// Admits this scope's subtree onto `gate`, or refuses without touching it.
    ///
    /// Legality is probed before anything is committed and applied only after
    /// the recursive handoff has succeeded. Both halves run under the child's
    /// own gate, held across the whole attempt, so no writer can change the
    /// stage between them; and an unwind out of the handoff's `assert!` leaves
    /// the record still reading its pre-admission stage.
    fn admit_observation_gate(
        &self,
        parent: &ScopeCell,
        gate: &ObservationGate,
        txn: &mut ObservationTxn<'_>,
    ) -> bool {
        debug_assert!(
            !std::ptr::eq(self, parent),
            "a scope cannot be admitted into itself"
        );
        // The caller holds `gate` through `parent.with_observation_gate`.
        // Re-homing the parent would first have to acquire that same gate, so
        // rereading its installed pointer here cannot race a parent handoff.
        debug_assert!(
            gate.shares_gate(&parent.current_observation_gate()),
            "observation gates are admitted only in the parent-to-child direction"
        );
        self.member.with_handoff_gate(
            gate,
            || {
                #[cfg(any(test, feature = "test-util"))]
                self.report_gate_capture(GateCapture::Adoption);
            },
            |current| {
                if !self.member.would_accept(&MemberTransition::Admitted) {
                    return false;
                }
                if !current.shares_gate(gate) {
                    // A live dynamic route needs a started driver, which needs
                    // a stage past `Reserved`; the probe above already refused
                    // every such stage, so re-homing one is unconstructible
                    // rather than merely unreached.
                    self.member.install_observation_gate_locked(current, gate);
                    self.adopt_descendant_observation_gates_locked(current, gate, txn);
                }
                let admitted = self
                    .member
                    .transition_locked(txn, MemberTransition::Admitted);
                debug_assert!(
                    admitted,
                    "the probed admission cannot be refused under the same held gate"
                );
                admitted
            },
        )
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
            .map(|resident| resident.projection().clone())
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

    /// Applies `transition` and publishes `event`, or refuses both.
    ///
    /// Returns whether the reducer accepted the transition. A refusal is a
    /// no-op *for this scope*; the caller is responsible for whatever it
    /// committed beforehand.
    #[must_use = "an illegal member transition is rejected, not applied"]
    pub fn transition_child_stage(
        &self,
        member: &MemberCell,
        transition: MemberTransition,
        event: Option<LifecycleEventKind>,
    ) -> bool {
        // Routed through `transition_locked` rather than a record-only update
        // so a restart schedule's displaced exit leaves the gate on this path
        // too.
        self.with_observation_gate(|wakes| {
            if !member.transition_locked(wakes, transition) {
                if let Some(event) = event {
                    wakes.defer(move || runtime::dispose_detached(event));
                }
                return false;
            }
            if let Some(event) = event {
                self.emit_locked(wakes, event);
            } else {
                self.publish_snapshot_chain_locked(wakes);
            }
            true
        })
    }

    /// Publishes one restart schedule, or refuses the whole publication.
    ///
    /// Returns whether the reducer accepted the transition. The restart
    /// bookkeeping its caller already charged is not rolled back here.
    #[must_use = "an illegal member transition is rejected, not applied"]
    pub fn publish_child_restart(
        &self,
        member: &MemberCell,
        total_restarts: TotalRestarts,
        transition: MemberTransition,
        exited: LifecycleEventKind,
        scheduled: LifecycleEventKind,
    ) -> bool {
        self.with_observation_gate(|wakes| {
            if !member.transition_locked(wakes, transition) {
                wakes.defer(move || runtime::dispose_detached((exited, scheduled)));
                return false;
            }
            self.observation.record.modify_silently(|scope| {
                scope.total_restarts = total_restarts;
            });
            wakes.pulse(&self.observation.record);
            self.emit_locked(wakes, exited);
            self.emit_locked(wakes, scheduled);
            true
        })
    }

    pub fn terminalize_child(
        &self,
        member: &MemberCell,
        exit: Exit,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        // Keep a retained owner across the residency assertion and every
        // fallible cell lookup. If an invariant fails, the raw argument can
        // unwind under the gate only as refcount traffic.
        let exit = RetainedExit::new(exit);
        self.with_observation_gate(move |wakes| {
            let record = member.record();
            if matches!(record.stage, MemberStage::Terminal(_)) {
                // The outer guard defines losing supervised terminalization:
                // no record or lifecycle edge is published. Keep the incoming
                // failed exit behind the same isolated-disposal boundary as a
                // losing direct terminalizer, and do not destroy it under the
                // observation gate.
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
                .find(|resident| resident.projection().member.membership() == member.membership())
                .map(|resident| resident.projection().clone());
            debug_assert!(
                resident.is_some(),
                "a supervised terminal child must remain in parent residency"
            );
            let nested = resident.and_then(|resident| resident.scope);
            let terminal_exit = member.terminalize_locked(exit.as_exit().clone(), startup, wakes);
            // The terminal member record now owns the equivalent retained
            // copy, so surrender this transient guard as refcount traffic.
            drop(exit.into_exit());
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
                .position(|child| child.projection().member.membership() == membership);
            index.map(|index| children.remove(index))
        };
        let Some(resident) = resident else {
            return false;
        };
        debug_assert_eq!(resident.projection().member.membership(), membership);
        let event = LifecycleEventKind::Removed {
            id: resident.projection().member.id().clone(),
            membership,
            last_incarnation: resident.projection().member.record().last_incarnation,
        };
        // The projection can carry the last member/mailbox owner. Put it in
        // the transaction before the fallible publication path so unwind also
        // retires it only after the observation gate is released. The detached
        // handoff deliberately makes final member teardown asynchronous.
        txn.defer(move || runtime::dispose_detached(resident));
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
        self.finish_incarnation_with_terminal(epoch, RetainedStopReason::new(reason), None);
    }

    pub fn finish_root_incarnation(&self, epoch: Epoch, reason: StopReason, exit: Exit) {
        self.finish_incarnation_with_terminal(
            epoch,
            RetainedStopReason::new(reason),
            Some(RetainedExit::new(exit)),
        );
    }

    fn finish_incarnation_with_terminal(
        &self,
        epoch: Epoch,
        reason: RetainedStopReason,
        mut terminal_exit: Option<RetainedExit>,
    ) {
        self.with_observation_gate(move |wakes| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if !control.epochs.finish(epoch) {
                // A stale driver must not overwrite the observation
                // projection of a newer live incarnation. Membership
                // terminality is not part of that projection: whoever owns a
                // terminal exit still publishes it exactly once, so declining
                // the epoch can never strand `wait_terminal`.
                drop(control);
                if let Some(exit) = terminal_exit.take() {
                    self.member.terminalize_locked(
                        exit.as_exit().clone(),
                        StartupDisposition::Unchanged,
                        wakes,
                    );
                    drop(exit.into_exit());
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
            self.publish_stopped_locked(
                wakes,
                reason.as_reason().clone(),
                terminal_exit.as_ref().map(|exit| exit.as_exit().clone()),
                Some(control),
            );
            if terminal || membership_terminal {
                // A parent-driver fallback may have terminalized this nested
                // membership while its live scope epilogue was still
                // pending. The epilogue owns the final Stopped projection and
                // closes observation only after publishing it.
                self.close_observation_locked(wakes);
            }
            drop(reason.into_public());
            if let Some(exit) = terminal_exit.take() {
                drop(exit.into_exit());
            }
        });
    }

    pub fn finish_live_root_incarnation(&self, reason: StopReason, exit: Exit) {
        // These wrappers precede the control lookup: a poisoned framework
        // mutex must not retire either user-bearing input on this thread.
        let reason = RetainedStopReason::new(reason);
        let exit = RetainedExit::new(exit);
        let epoch = {
            let control = self.control.lock().expect("scope control mutex poisoned");
            control.epochs.live_epoch()
        };
        if let Some(epoch) = epoch {
            self.finish_incarnation_with_terminal(epoch, reason, Some(exit));
        } else {
            self.with_observation_gate(move |wakes| {
                self.publish_stopped_locked(
                    wakes,
                    reason.as_reason().clone(),
                    Some(exit.as_exit().clone()),
                    None,
                );
                self.close_observation_locked(wakes);
                drop(reason.into_public());
                drop(exit.into_exit());
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

    /// Replaces this scope's residency, returning whether every child was
    /// admitted. A refused child is left out of the residency entirely.
    #[must_use = "a refused admission leaves the child out of the residency"]
    pub fn set_admitted_children(self: &Arc<Self>, children: Vec<ResidentProjection>) -> bool {
        self.with_observation_gate(|wakes| {
            self.clear_residents_locked(wakes);
            let mut admitted = true;
            for child in children {
                admitted &= self.admit_child_locked(child, wakes);
            }
            admitted
        })
    }

    #[cfg(any(test, feature = "test-util"))]
    #[must_use = "a refused admission leaves the child out of the residency"]
    pub fn admit_child(self: &Arc<Self>, child: ResidentProjection) -> bool {
        self.with_observation_gate(|wakes| self.admit_child_locked(child, wakes))
    }

    /// Admits one projection, or refuses it whole.
    ///
    /// Returns whether the member's `Admitted` transition was legal. A refusal
    /// publishes nothing: no gate handoff, no parent wiring, no residency push
    /// and no `Added` event.
    #[must_use = "a refused admission leaves the child out of the residency"]
    pub fn admit_child_locked(
        self: &Arc<Self>,
        child: ResidentProjection,
        txn: &mut ObservationTxn<'_>,
    ) -> bool {
        // Take protected ownership before gate adoption, parent wiring, and
        // reducer validation. Until the final push succeeds this projection
        // may be the last owner of a mailbox-bearing member.
        let child = ResidentAdmission::new(child, txn);
        let projection = child.projection();
        let gate = self.current_observation_gate();
        let admitted = if let Some(scope) = &projection.scope {
            let admitted = scope.admit_observation_gate(self, &gate, txn);
            if admitted {
                scope.set_parent(self, txn);
            }
            admitted
        } else {
            // Same probe-handoff-apply order as the nested-scope path above.
            projection.member.with_handoff_gate(
                &gate,
                || {},
                |current| {
                    if !projection.member.would_accept(&MemberTransition::Admitted) {
                        return false;
                    }
                    if !current.shares_gate(&gate) {
                        projection
                            .member
                            .install_observation_gate_locked(current, &gate);
                    }
                    let admitted = projection
                        .member
                        .transition_locked(txn, MemberTransition::Admitted);
                    debug_assert!(
                        admitted,
                        "the probed admission cannot be refused under the same held gate"
                    );
                    admitted
                },
            )
        };
        if !admitted {
            return false;
        }
        let id = projection.member.id().clone();
        let membership = projection.member.membership();
        drop(projection);
        self.current_children()
            .push(ResidentChild::new(child.install()));
        self.emit_locked(txn, LifecycleEventKind::Added { id, membership });
        true
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
                id: resident.projection().member.id().clone(),
                membership: resident.projection().member.membership(),
                last_incarnation: resident.projection().member.record().last_incarnation,
            })
            .collect::<Vec<_>>();
        // Schedule the whole displaced set before emitting any edge. This
        // both preserves last-owner disposal and makes an unwind retire the
        // untouched suffix after unlock. Detached disposal means final member
        // teardown may complete after this transaction returns -- and that a
        // resident's own destructor can never reach an `ObservationTxn`, so
        // SPEC §15.5's structural `Removed`-on-drop is unavailable on this
        // lane and the edge is emitted explicitly below instead (#389).
        wakes.defer(move || runtime::dispose_detached(residents));
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

    /// Publishes a nested scope body that was spawned but never began its own
    /// incarnation. The parent driver remains responsible for the shared
    /// membership's terminal exit and, unless it already published that exit,
    /// closes observation after the terminal parent projection.
    ///
    /// `Unstarted` is precisely SPEC B.6's "no incarnation has ever spawned"
    /// in the scope plane, and `Stopped { NeverStarted }` is its terminal
    /// twin; publishing that pair is what makes the record agree with
    /// `wait_stopped`'s own `Unstarted` answer. A body dropped before its
    /// *restart* incarnation began is a different case: that scope already
    /// published a real prior-incarnation reason, which is both its final
    /// verdict and terminal, so the fallback must leave it alone. Publishing
    /// `NeverStarted` over it would contradict the shared membership's exit
    /// (`Aborted` with `last_incarnation: Some(..)`) and — because the parent
    /// path can terminalize and close first — would only land in one of two
    /// arrival orders. The fallback therefore supplies a missing terminal
    /// projection and never replaces a published one; closure is the only
    /// effect it owns unconditionally. `total_restarts` and `startup` need no
    /// reset under this gate: an `Unstarted` scope never charged a restart,
    /// and a startup result installed without an incarnation (identity
    /// exhaustion) is the structured cause `wait_started` must keep.
    pub fn close_never_started_body(&self) {
        self.with_observation_gate(|txn| {
            if self.observation.closed.load(Ordering::Acquire) {
                return;
            }
            if matches!(self.record().state, ScopeState::Unstarted) {
                self.publish_stopped_locked(txn, StopReason::NeverStarted, None, None);
            }
            // The same guard `terminalize_child` applies to its trailing
            // close: a stream whose payload is still `Starting`/`Running`
            // must not end on that projection.
            if self.membership_terminal()
                && matches!(self.record().state, ScopeState::Stopped { .. })
            {
                self.close_observation_locked(txn);
            }
        });
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
        Cancellation, ExitError, GracePhase, IntensityTrip, StartupFailure, StartupFailureCause,
        identity::ScopeIdentity,
        policy::{ResolvedMailbox, RestartCount, ScopeFlavor},
    };
    use shelterwood_mailbox::{
        MailboxCell, MailboxControl, MailboxEffectQueue, MailboxReceiver, actor_ref_from_parts,
    };

    use super::*;
    use crate::{
        cells::test_support::{TEST_WAIT, ThreadProbe, child_member, child_scope, isolated_scope},
        observe::{LifecycleItem, LifecycleTryRecvError},
    };

    struct GateCheckingWake {
        gate: super::ObservationGate,
        woke_after_unlock: Arc<AtomicBool>,
    }

    #[test]
    fn never_started_body_publishes_before_membership_closes_observation() {
        let scope = isolated_scope("nested", ScopeFlavor::Ordered);
        let snapshots = scope.subscribe_snapshots();

        scope.close_never_started_body();

        let (snapshot, closed) = snapshots.borrow_latest_and_closed();
        assert!(!closed, "membership terminality owns observation closure");
        assert!(matches!(
            snapshot.state,
            ScopeState::Stopped {
                reason: StopReason::NeverStarted
            }
        ));
        assert_eq!(snapshot.total_restarts, TotalRestarts::ZERO);
        assert!(matches!(
            scope.record().startup,
            Some(Err(StartupError::ShutdownRequested))
        ));
        assert!(
            !matches!(scope.member.record().stage, MemberStage::Terminal(_)),
            "the parent driver still owns membership terminality"
        );

        scope.terminalize_never_started();
        let (_, closed) = snapshots.borrow_latest_and_closed();
        assert!(closed, "terminal membership closes observation");
    }

    /// A body dropped before its *restart* incarnation began keeps the prior
    /// incarnation's published reason, whichever of the two owners of that
    /// scope's terminality runs first.
    ///
    /// The parent path (`terminalize_child`) and the body fallback race on a
    /// current-thread runtime only by a few instructions, and on a
    /// multi-thread runtime genuinely. Publishing `NeverStarted` here would
    /// both depend on that order and contradict the membership exit, which
    /// carries `last_incarnation: Some(..)` — SPEC B.6's stop-reason lattice
    /// requires the projection and the exit to agree in either order.
    #[test]
    fn never_polled_restart_body_keeps_its_prior_reason_in_either_arrival_order() {
        for parent_first in [true, false] {
            let root = isolated_scope("root", ScopeFlavor::Ordered);
            let nested = child_scope(&root, "nested", ScopeFlavor::Ordered);
            assert!(root.admit_child(ResidentProjection::new(
                Arc::clone(&nested.member),
                Some(Arc::clone(&nested)),
            )));
            let mut incarnations = nested.member.take_incarnation_counter();
            let first = incarnations.mint().expect("first incarnation available");
            assert!(
                nested
                    .member
                    .transition(MemberTransition::Starting { incarnation: first })
            );
            let epoch = nested
                .begin_incarnation(ScopeState::Starting)
                .expect("first incarnation begins");
            nested.finish_incarnation(epoch, StopReason::Finished);
            assert!(
                nested
                    .member
                    .transition(MemberTransition::RestartScheduled {
                        exit: Exit::completed(Cancellation::NotObserved),
                        restart_count: RestartCount::ZERO.bump(),
                        restart_at: None,
                    })
            );
            let restarted = incarnations.mint().expect("restart incarnation available");
            assert!(nested.member.transition(MemberTransition::Starting {
                incarnation: restarted,
            }));
            let snapshots = nested.subscribe_snapshots();

            // The restart body is dropped before its first poll, so it never
            // reaches `begin_incarnation` and the parent aborts it.
            let terminalize = || {
                root.terminalize_child(
                    &nested.member,
                    Exit::aborted(GracePhase::WithinGrace, Cancellation::Observed),
                    Some(restarted),
                    StartupDisposition::NotAborted,
                )
            };
            if parent_first {
                terminalize();
                nested.close_never_started_body();
            } else {
                nested.close_never_started_body();
                terminalize();
            }

            let (snapshot, closed) = snapshots.borrow_latest_and_closed();
            assert_eq!(
                snapshot.state,
                ScopeState::Stopped {
                    reason: StopReason::Finished
                },
                "a never-polled restart keeps its prior incarnation's reason \
                 (parent_first={parent_first})"
            );
            assert!(
                closed,
                "membership terminality closes observation (parent_first={parent_first})"
            );
            let record = nested.member.record();
            assert!(
                matches!(record.stage, MemberStage::Terminal(_)),
                "the parent owns the shared membership's terminal exit"
            );
            assert!(
                record.last_incarnation.is_some(),
                "a restarted membership has spawned, so `NeverStarted` cannot describe it"
            );
        }
    }

    /// Identity exhaustion installs a structured startup failure and then
    /// discharges the body obligation without an incarnation. The fallback
    /// supplies the missing terminal projection without overwriting that
    /// cause, so `wait_started` keeps reporting it.
    #[test]
    fn never_started_body_preserves_a_structured_startup_failure() {
        let scope = isolated_scope("nested", ScopeFlavor::Ordered);
        let failure = StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: scope.member.id().clone(),
            },
        };
        scope.set_startup(Err(StartupError::StartupFailed(failure)));

        scope.close_never_started_body();

        assert!(
            matches!(
                scope.record().startup,
                Some(Err(StartupError::StartupFailed(_)))
            ),
            "a structured startup cause survives the never-started fallback: {:?}",
            scope.record().startup
        );
        assert!(matches!(
            scope.record().state,
            ScopeState::Stopped {
                reason: StopReason::NeverStarted
            }
        ));
    }

    impl Wake for GateCheckingWake {
        fn wake(self: Arc<Self>) {
            self.woke_after_unlock
                .store(!self.gate.is_held(), Ordering::SeqCst);
        }
    }

    /// A user payload whose destructor panics, as SPEC §5.5's containment
    /// clause anticipates.
    struct HostileDropMessage {
        entered: mpsc::SyncSender<&'static str>,
        id: &'static str,
    }

    impl Drop for HostileDropMessage {
        fn drop(&mut self) {
            let _ = self.entered.send(self.id);
            panic!("hostile resident payload destructor");
        }
    }

    struct GateDropMessage {
        gate: super::ObservationGate,
        entered: mpsc::SyncSender<(bool, std::thread::ThreadId)>,
        release: mpsc::Receiver<()>,
    }

    impl Drop for GateDropMessage {
        fn drop(&mut self) {
            let _ = self
                .entered
                .send((!self.gate.is_held(), std::thread::current().id()));
            let _ = self.release.recv_timeout(TEST_WAIT);
        }
    }

    // The retention this pins only has an observable effect when the
    // framework invariant it guards is checked, and that check is a
    // `debug_assert!`. Release builds decline the panic entirely, so the test
    // is gated on the profile it can hold in rather than left to fail there.
    #[cfg(debug_assertions)]
    #[test]
    fn nonresident_terminal_exit_is_retained_before_the_residency_assertion() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let member = child_member(&root, "missing");
        let (dropped, observed) = mpsc::sync_channel(1);
        let retiring_thread = std::thread::current().id();
        let exit = Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        );

        catch_unwind(AssertUnwindSafe(|| {
            root.terminalize_child(&member, exit, None, StartupDisposition::Unchanged);
        }))
        .expect_err("a supervised child must remain resident");

        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("failed exit disposal reports"),
            retiring_thread,
            "the incoming error cannot unwind through the observation gate"
        );
    }

    // The retention this pins only has an observable effect when the
    // framework invariant it guards is checked, and that check is a
    // `debug_assert!`. Release builds decline the panic entirely, so the test
    // is gated on the profile it can hold in rather than left to fail there.
    #[cfg(debug_assertions)]
    #[test]
    fn rejected_resident_admission_detaches_its_last_mailbox_owner() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let member = child_member(&root, "invalid");
        let mut incarnations = member.take_incarnation_counter();
        let incarnation = incarnations.mint().expect("incarnation available");
        let mailbox = MailboxCell::new(member.id().clone(), shelterwood_runtime::mailbox_runtime());
        member.attach_mailbox(mailbox.clone());
        let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
        let mut effects = MailboxEffectQueue::default();
        let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
        MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
        drop(effects);
        // Admission below is intentionally illegal, but the projection is
        // still the final owner of this mailbox-bearing member when it fails.
        assert!(member.transition(MemberTransition::Admitted));
        let (dropped, observed) = mpsc::sync_channel(1);
        actor
            .try_send(ThreadProbe(dropped))
            .expect("bound mailbox accepts the probe");
        let projection = ResidentProjection::new(member, None);
        drop(actor);
        drop(mailbox);
        let retiring_thread = std::thread::current().id();

        assert!(
            !root.admit_child(projection),
            "an admitted member cannot be admitted twice"
        );
        assert!(
            root.resident_projections().is_empty(),
            "a rejected admission publishes no residency"
        );

        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("mailbox payload disposal reports"),
            retiring_thread,
            "the by-value projection cannot unwind its mailbox through the observation gate"
        );
    }

    #[test]
    fn rejected_nested_admission_preserves_original_parent_and_gate() {
        let original = isolated_scope("original", ScopeFlavor::Ordered);
        let destination = isolated_scope("destination", ScopeFlavor::Ordered);
        let nested = child_scope(&original, "nested", ScopeFlavor::Dynamic);
        let descendant = child_member(&nested, "descendant");
        assert!(
            nested.set_admitted_children(vec![ResidentProjection::new(
                Arc::clone(&descendant),
                None
            )])
        );
        assert!(original.set_admitted_children(vec![ResidentProjection::new(
            Arc::clone(&nested.member),
            Some(Arc::clone(&nested)),
        )]));
        let original_gate = original.observation_gate();
        assert!(original_gate.same_gate(&nested.observation_gate()));
        assert!(original_gate.same_gate(&descendant.observation_gate()));

        assert!(
            !destination.admit_child(ResidentProjection::new(
                Arc::clone(&nested.member),
                Some(Arc::clone(&nested)),
            )),
            "an already-admitted subtree cannot move to a second parent"
        );

        assert!(original.has_resident_child(&nested.member));
        assert!(destination.resident_projections().is_empty());
        assert!(Arc::ptr_eq(
            &nested
                .parent()
                .expect("the original parent remains installed"),
            &original
        ));
        assert!(original_gate.same_gate(&nested.observation_gate()));
        assert!(original_gate.same_gate(&descendant.observation_gate()));
        assert!(
            !destination
                .observation_gate()
                .same_gate(&nested.observation_gate()),
            "rejection leaves the subtree on its original observation gate"
        );
    }

    #[test]
    fn rejected_stage_transition_suppresses_its_lifecycle_publication() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let member = child_member(&root, "invalid");
        let mut events = root.subscribe_lifecycle();

        assert!(
            !root.transition_child_stage(
                &member,
                MemberTransition::RestartScheduled {
                    exit: Exit::completed(Cancellation::NotObserved),
                    restart_count: RestartCount::ZERO.bump(),
                    restart_at: None,
                },
                Some(LifecycleEventKind::Exited {
                    id: member.id().clone(),
                    membership: member.membership(),
                    incarnation: member
                        .take_incarnation_counter()
                        .mint()
                        .expect("incarnation available"),
                    exit: Exit::completed(Cancellation::NotObserved),
                }),
            ),
            "the Reserved-to-Restarting projection transition is illegal"
        );
        assert!(matches!(member.record().stage, MemberStage::Reserved));
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));
    }

    #[cfg(debug_assertions)]
    struct InertRoute;

    #[cfg(debug_assertions)]
    impl DynamicRoute for InertRoute {
        fn close_admission(&self, _txn: &mut ObservationTxn<'_>) {}
    }

    /// Coverage for the surviving live-route assertion.
    ///
    /// `admit_observation_gate` no longer needs one: its legality probe
    /// refuses every stage a started driver can present, so a re-homed live
    /// route is unconstructible there. The reservation-time adoption path has
    /// no such probe, and this is its regression.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "a scope with a live dynamic route is never re-homed")]
    fn plain_gate_adoption_rejects_a_scope_with_a_live_dynamic_route() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        nested.set_dynamic_route(Some(Arc::new(InertRoute)));

        root.with_observation_gate(|txn| {
            root.adopt_child_observation_gate(&nested.member, Some(&nested), txn);
        });
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
            assert!(adopting_root.admit_child(ResidentProjection::new(
                Arc::clone(&adopting_nested.member),
                Some(adopting_nested),
            )));
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
        assert!(
            leaf_scope.set_admitted_children(vec![ResidentProjection::new(
                Arc::clone(&grandchild),
                None
            )])
        );
        assert!(nested.set_admitted_children(vec![
            ResidentProjection::new(
                Arc::clone(&leaf_scope.member),
                Some(Arc::clone(&leaf_scope)),
            ),
            ResidentProjection::new(Arc::clone(&leaf_member), None),
        ]));

        assert!(root.admit_child(ResidentProjection::new(
            Arc::clone(&nested.member),
            Some(Arc::clone(&nested)),
        )));

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
        assert!(
            root.set_admitted_children(vec![ResidentProjection::new(Arc::clone(&child), None)])
        );

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

        assert!(root.set_admitted_children(vec![
            ResidentProjection::new(Arc::clone(&nested.member), Some(Arc::clone(&nested))),
            ResidentProjection::new(Arc::clone(&leaf), None),
        ]));
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
    fn poisoned_finish_bookkeeping_retires_user_inputs_off_thread() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let epoch = scope
            .begin_incarnation(ScopeState::Starting)
            .expect("the fixture begins one incarnation");
        let poison = Arc::clone(&scope);
        assert!(
            catch_unwind(AssertUnwindSafe(move || {
                let _control = poison.control.lock().expect("control starts healthy");
                panic!("inject control poison");
            }))
            .is_err()
        );

        let retiring_thread = std::thread::current().id();
        let (reason_dropped, reason_observed) = mpsc::sync_channel(1);
        let reason_exit = Exit::failed(
            ExitError::from(ThreadProbe(reason_dropped)),
            Cancellation::NotObserved,
        );
        let reason = StopReason::StartupFailed(StartupFailure {
            cause: StartupFailureCause::Child {
                id: ChildId::from("failed-child"),
                membership: scope.member.membership(),
                exit: reason_exit,
            },
        });
        let (terminal_dropped, terminal_observed) = mpsc::sync_channel(1);
        let terminal_exit = Exit::failed(
            ExitError::from(ThreadProbe(terminal_dropped)),
            Cancellation::NotObserved,
        );

        catch_unwind(AssertUnwindSafe(|| {
            scope.finish_root_incarnation(epoch, reason, terminal_exit);
        }))
        .expect_err("the poisoned control mutex rejects finish bookkeeping");

        for observed in [reason_observed, terminal_observed] {
            assert_ne!(
                observed
                    .recv_timeout(TEST_WAIT)
                    .expect("failed exit disposal reports"),
                retiring_thread,
                "scope-finish inputs cannot unwind through the observation gate"
            );
        }
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
    fn clearing_residents_detaches_the_last_mailbox_owner_after_unlock() {
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
        let (entered, observed) = mpsc::sync_channel(1);
        let (release, release_drop) = mpsc::sync_channel(1);
        actor
            .try_send(GateDropMessage {
                gate,
                entered,
                release: release_drop,
            })
            .expect("bound mailbox accepts the probe");
        assert!(scope.admit_child(ResidentProjection::new(Arc::clone(&child), None)));
        drop(actor);
        drop(mailbox);
        drop(child);

        let (cleared, clear_observed) = mpsc::sync_channel(1);
        let clearing = std::thread::spawn(move || {
            let thread = std::thread::current().id();
            scope.clear_residents();
            cleared
                .send(thread)
                .expect("clear observer remains available");
        });
        let (unlocked, drop_thread) = observed
            .recv_timeout(TEST_WAIT)
            .expect("resident mailbox payload destructor reports");
        let clear_before_release = clear_observed.recv_timeout(Duration::from_millis(100)).ok();
        let returned_before_release = clear_before_release.is_some();
        release
            .send(())
            .expect("the blocking destructor remains parked");
        let clear_thread = clear_before_release.unwrap_or_else(|| {
            clear_observed
                .recv_timeout(TEST_WAIT)
                .expect("resident clearing eventually returns")
        });
        clearing.join().expect("resident clearing thread joins");

        assert!(
            unlocked,
            "the displaced resident owner is released after the gate unlocks"
        );
        assert!(
            returned_before_release,
            "resident clearing must not wait for a blocking user destructor"
        );
        assert_ne!(
            drop_thread, clear_thread,
            "last-owner resident disposal runs on the detached lane"
        );
    }

    #[test]
    fn clearing_residents_contains_every_hostile_payload_destructor() {
        let scope = isolated_scope("root", ScopeFlavor::Dynamic);
        let (entered, observed) = mpsc::sync_channel(2);
        let mut counters = Vec::new();
        for id in ["first", "second"] {
            let child = child_member(&scope, id);
            let mut incarnations = child.take_incarnation_counter();
            let incarnation = incarnations.mint().expect("incarnation available");
            counters.push(incarnations);
            let mailbox =
                MailboxCell::new(child.id().clone(), shelterwood_runtime::mailbox_runtime());
            child.attach_mailbox(mailbox.clone());
            let actor = actor_ref_from_parts(Arc::clone(&child), Arc::clone(&mailbox));
            let mut effects = MailboxEffectQueue::default();
            let token = MailboxControl::configure(&*mailbox, ResolvedMailbox::Latest, &mut effects);
            MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);
            drop(effects);
            actor
                .try_send(HostileDropMessage {
                    entered: entered.clone(),
                    id,
                })
                .expect("bound mailbox accepts the hostile payload");
            assert!(scope.admit_child(ResidentProjection::new(Arc::clone(&child), None)));
            drop(actor);
            drop(mailbox);
            drop(child);
        }
        drop(entered);

        scope.clear_residents();

        // Park behind a job queued after the displaced set. Without a
        // per-resident boundary the second destructor panics inside the first
        // one's unwind through `Vec`'s slice drop glue, and the disposal
        // worker aborts the process before this sentinel can run -- the
        // sequencing is what makes the regression deterministic rather than a
        // race against the test's own return.
        let (sentinel, sequenced) = mpsc::sync_channel(1);
        runtime::dispose_detached(ThreadProbe(sentinel));
        sequenced
            .recv_timeout(TEST_WAIT)
            .expect("the disposal worker survives every hostile resident");

        let mut reported = Vec::new();
        for _ in 0..2 {
            reported.push(
                observed
                    .recv_timeout(TEST_WAIT)
                    .expect("every hostile resident destructor runs"),
            );
        }
        reported.sort_unstable();
        assert_eq!(reported, ["first", "second"]);
    }
}
