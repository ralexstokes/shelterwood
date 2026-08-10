//! Restart-stable shared member and scope state.
//!
//! These cells are the neutral synchronization and observation projection
//! layer shared by public handles, mailboxes, actor/task contexts, and the
//! mutable supervision driver. In particular, this module does not depend on
//! mutable driver state. Its dynamic-route interface names declaration slots
//! only as opaque capability payloads; their state remains owned elsewhere.

use std::{
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::{
    ChildId, Exit, Incarnation, Intensity, Mailbox, Membership, Readiness, RestartCount,
    ScopeState, Strategy, TotalRestarts,
    admission::{RemoveOutcome, ReserveError},
    engine::{Epoch, RequestTarget, ScopeEpochs},
    exit::{StartupError, StopReason},
    identity::ScopeIdentity,
    observe::{
        ChildSnapshot, ChildState, LifecycleEvent, LifecycleEventKind, LifecycleEvents,
        LifecycleHub, LifecycleSeq, MembershipStatus, ScopeKind, ScopeSnapshot, SnapshotHub,
        SnapshotReceiver,
    },
    plan::SlotCell,
    policy::{ResolvedCommonOptions, ScopeFlavor},
    runtime::{self, Latch},
};

/// Isolated payload returned after mailbox termination has synchronously
/// published all waiter outcomes.
pub(crate) type MailboxDisposal = Box<dyn Send>;

/// Prepared terminal mailbox transition. Finishing it wakes terminal waiters
/// before returning unread payload ownership for detached disposal.
pub(crate) trait MailboxTermination: Send {
    fn finish(self: Box<Self>) -> Option<MailboxDisposal>;
}

/// Type-erased mailbox lifecycle surface owned by a member cell.
///
/// The driver must configure a mailbox before its first bind. Every live
/// incarnation must then be closed before a later incarnation is bound; if
/// close is skipped, messages accepted for the prior incarnation can leak
/// into the replacement. Once termination is prepared, later binds are
/// intentionally ignored.
pub(crate) trait MailboxControl: fmt::Debug + Send + Sync {
    /// Installs the declaration-time mailbox policy before the first bind.
    /// Reconfiguration may only repeat the same resolved policy.
    fn configure(&self, mailbox: Mailbox);
    /// Makes one incarnation live after configuration and prior-close cleanup.
    /// A bind after terminal preparation is deliberately ignored because
    /// terminality wins that race permanently.
    fn bind(&self, incarnation: Incarnation);
    /// Stops new acceptance for the matching live incarnation.
    fn freeze(&self, incarnation: Incarnation);
    /// Unbinds the matching incarnation and returns its unread payload.
    /// Every successful bind must be followed by this close before a rebind;
    /// skipping it would deliver the old incarnation's messages to the new.
    fn close(&self, incarnation: Incarnation) -> Option<MailboxDisposal>;
    /// Irreversibly terminalizes the membership and prepares synchronous
    /// waiter completion followed by isolated unread-payload disposal.
    fn prepare_termination(&self) -> Option<Box<dyn MailboxTermination>>;
    /// Debug-only check for the driver's configure/close-before-bind contract.
    #[cfg(debug_assertions)]
    fn bind_order_valid(&self) -> bool;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MemberStage {
    Reserved,
    Admitted,
    Starting,
    Running,
    Restarting,
    Stopping,
    Terminal(Exit),
}

/// Whether a terminal child incarnation failed during aggregate startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupDisposition {
    NotAborted,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemberRecord {
    pub(crate) stage: MemberStage,
    pub(crate) incarnation: Option<Incarnation>,
    pub(crate) last_incarnation: Option<Incarnation>,
    pub(crate) last_exit: Option<Exit>,
    pub(crate) restart_count: RestartCount,
    pub(crate) restart_at: Option<Instant>,
    pub(crate) removing: bool,
    pub(crate) startup_aborted: bool,
}

#[derive(Debug)]
pub(crate) struct MemberCell {
    id: ChildId,
    membership: Membership,
    rebased_membership: OnceLock<Membership>,
    record: runtime::WatchSender<MemberRecord>,
    terminal_disposal_pending: AtomicBool,
    mailbox: Mutex<MemberMailbox>,
    options: OnceLock<ResolvedCommonOptions>,
    pub(crate) removal: Latch,
}

#[derive(Default)]
enum MemberMailbox {
    #[default]
    Unattached,
    Attached(Arc<dyn MailboxControl>),
    Terminal {
        control: Option<Arc<dyn MailboxControl>>,
        exit: Exit,
        teardown: Option<Box<dyn MailboxTermination>>,
    },
}

impl fmt::Debug for MemberMailbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unattached => formatter.write_str("Unattached"),
            Self::Attached(control) => formatter.debug_tuple("Attached").field(control).finish(),
            Self::Terminal {
                control,
                exit,
                teardown,
            } => formatter
                .debug_struct("Terminal")
                .field("control", control)
                .field("exit", exit)
                .field("teardown_pending", &teardown.is_some())
                .finish(),
        }
    }
}

impl MemberCell {
    pub(crate) fn new(id: ChildId, membership: Membership) -> Arc<Self> {
        let (record, _) = runtime::watch(MemberRecord {
            stage: MemberStage::Reserved,
            incarnation: None,
            last_incarnation: None,
            last_exit: None,
            restart_count: RestartCount::ZERO,
            restart_at: None,
            removing: false,
            startup_aborted: false,
        });
        Arc::new(Self {
            id,
            membership,
            rebased_membership: OnceLock::new(),
            record,
            terminal_disposal_pending: AtomicBool::new(false),
            mailbox: Mutex::new(MemberMailbox::default()),
            options: OnceLock::new(),
            removal: Latch::default(),
        })
    }

    pub(crate) fn id(&self) -> &ChildId {
        &self.id
    }

    pub(crate) fn membership(&self) -> Membership {
        self.rebased_membership
            .get()
            .copied()
            .unwrap_or(self.membership)
    }

    pub(crate) fn rebase_membership(&self, membership: Membership) {
        let record = self.record();
        assert!(
            matches!(record.stage, MemberStage::Reserved)
                && record.incarnation.is_none()
                && record.last_incarnation.is_none(),
            "only an unstarted reservation can be rebased"
        );
        self.rebased_membership
            .set(membership)
            .expect("a reservation can be rebased at most once");
    }

    pub(crate) fn record(&self) -> MemberRecord {
        self.record.read_cloned()
    }

    #[cfg(test)]
    pub(crate) fn record_watcher(&self) -> runtime::WatchReceiver<MemberRecord> {
        self.record.watcher()
    }

    #[cfg(test)]
    pub(crate) fn stage_terminal_before_mailbox(&self, exit: Exit) {
        let mut mailbox = self.mailbox.lock().expect("member mailbox mutex poisoned");
        assert!(matches!(*mailbox, MemberMailbox::Unattached));
        *mailbox = MemberMailbox::Terminal {
            control: None,
            exit,
            teardown: None,
        };
    }

    pub(crate) fn terminal_disposal_pending(&self) -> bool {
        self.terminal_disposal_pending.load(Ordering::Acquire)
    }

    pub(crate) fn set_terminal_disposal_pending(&self, pending: bool) {
        self.terminal_disposal_pending
            .store(pending, Ordering::Release);
    }

    /// Mutates a member record and pulses the watch channel.
    ///
    /// The driver also treats this channel as its control-plane wake bus: any
    /// field read by a loop precondition must be changed through a pulsing path
    /// like this one, never by a silent write outside an observation gate.
    pub(crate) fn update(&self, update: impl FnOnce(&mut MemberRecord)) {
        self.record.send_modify(update);
    }

    fn update_locked(&self, update: impl FnOnce(&mut MemberRecord)) {
        self.record.send_modify(update);
    }

    pub(crate) fn set_options(&self, options: ResolvedCommonOptions) {
        self.options
            .set(options)
            .expect("member options are resolved exactly once");
    }

    fn options(&self) -> ResolvedCommonOptions {
        self.options.get().cloned().unwrap_or_else(|| {
            crate::policy::resolve_common(
                &crate::policy::CommonOptions::default(),
                &crate::policy::ResolvedDefaults::default(),
                crate::policy::ChildMode::Restartable,
                Readiness::Immediate,
            )
            .expect("library defaults must be valid")
        })
    }

    pub(crate) fn attach_mailbox(&self, mailbox: Arc<dyn MailboxControl>) {
        let terminal_exit = {
            let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
            match &mut *state {
                MemberMailbox::Unattached => {
                    *state = MemberMailbox::Attached(mailbox);
                    None
                }
                MemberMailbox::Attached(_)
                | MemberMailbox::Terminal {
                    control: Some(_), ..
                } => panic!("a member can own only one mailbox"),
                MemberMailbox::Terminal {
                    control,
                    exit,
                    teardown,
                } => {
                    debug_assert!(teardown.is_none());
                    *teardown = mailbox.prepare_termination();
                    *control = Some(mailbox);
                    Some(exit.clone())
                }
            }
        };
        if let Some(terminal_exit) = terminal_exit {
            runtime::resume_preferred_panic(self.publish_terminal(terminal_exit));
        }
    }

    pub(crate) fn mailbox(&self) -> Option<Arc<dyn MailboxControl>> {
        match &*self.mailbox.lock().expect("member mailbox mutex poisoned") {
            MemberMailbox::Unattached => None,
            MemberMailbox::Attached(control) => Some(Arc::clone(control)),
            MemberMailbox::Terminal { control, .. } => control.clone(),
        }
    }

    pub(crate) fn terminalize(&self, exit: Exit) {
        runtime::resume_preferred_panic(self.terminalize_for_scope(exit));
    }

    fn publish_terminal(&self, terminal_exit: Exit) -> runtime::UnwindPanics {
        let mut published = false;
        self.record.modify_silently(|record| {
            if !matches!(record.stage, MemberStage::Terminal(_)) {
                record.incarnation = None;
                record.restart_at = None;
                record.last_exit = Some(terminal_exit.clone());
                record.stage = MemberStage::Terminal(terminal_exit);
                published = true;
            }
        });
        // The terminal record is stored before mailbox discharge so reentrant
        // mailbox wakers observe the winning exit. The ordering guarantee for
        // notification-driven readers is discharge-before-pulse, not
        // discharge-before-store: a direct borrow can see `Terminal` while
        // teardown is still running. A hostile mailbox waker may panic, but
        // that panic is resumed only after the pulse so it cannot strand a
        // waiter parked on membership terminality.
        let teardown = match &mut *self.mailbox.lock().expect("member mailbox mutex poisoned") {
            MemberMailbox::Terminal { teardown, .. } => teardown.take(),
            MemberMailbox::Unattached | MemberMailbox::Attached(_) => {
                unreachable!("terminal publication requires terminal mailbox state")
            }
        };
        let mut teardown_panic = None;
        if let Some(teardown) = teardown {
            match runtime::catch_panic(|| teardown.finish()) {
                Ok(Some(payload)) => runtime::dispose_detached(payload),
                Ok(None) => {}
                Err(payload) => teardown_panic = Some(payload),
            }
        }
        let pulse_panic = published
            .then(|| runtime::catch_panic(|| self.record.pulse()).err())
            .flatten();
        runtime::UnwindPanics {
            primary: teardown_panic,
            cleanup: pulse_panic,
        }
    }

    fn terminalize_for_scope(&self, exit: Exit) -> runtime::UnwindPanics {
        let terminal_exit = {
            let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
            match &*state {
                MemberMailbox::Terminal {
                    exit: terminal_exit,
                    ..
                } => terminal_exit.clone(),
                MemberMailbox::Unattached => {
                    *state = MemberMailbox::Terminal {
                        control: None,
                        exit: exit.clone(),
                        teardown: None,
                    };
                    exit
                }
                MemberMailbox::Attached(control) => {
                    let control = Arc::clone(control);
                    let teardown = control.prepare_termination();
                    *state = MemberMailbox::Terminal {
                        control: Some(control),
                        exit: exit.clone(),
                        teardown,
                    };
                    exit
                }
            }
        };
        self.publish_terminal(terminal_exit)
    }

    pub(crate) async fn wait_terminal(&self) -> Exit {
        let mut watcher = self.record.watcher();
        loop {
            if let MemberStage::Terminal(exit) = watcher.borrow_cloned().stage {
                return exit;
            }
            watcher.changed().await;
        }
    }
}

/// Observation-only projection of the authoritative engine lifecycle.
/// Driver decisions never read this record back as liveness policy.
#[derive(Clone, Debug)]
pub(crate) struct ScopeRecord {
    pub(crate) state: ScopeState,
    pub(crate) startup: Option<Result<(), StartupError>>,
    pub(crate) total_restarts: TotalRestarts,
}

pub(crate) trait DynamicRoute: Send + Sync {
    fn reserve(
        &self,
        scope: &Arc<ScopeCell>,
        id: ChildId,
        child_scope: Option<ScopeFlavor>,
    ) -> Result<Arc<SlotCell>, ReserveError>;

    fn start_admission(
        self: Arc<Self>,
        slot: Arc<SlotCell>,
        fused_cancel: Option<Latch>,
    ) -> Result<runtime::OneShotReceiver<Result<(), ReserveError>>, ReserveError>;

    fn cancel_reservation(&self, slot: &Arc<SlotCell>);

    fn signal_fused_cancel(&self, membership: Membership, latch: &Latch);

    fn remove(
        &self,
        scope: &Arc<ScopeCell>,
        id: &ChildId,
        exact: Option<Membership>,
    ) -> runtime::OneShotReceiver<RemoveOutcome>;

    #[cfg(test)]
    fn request_forwarder_probe(&self) -> (Latch, Latch);
}

/// Shared critical section for one resident tree's observation projection.
///
/// Gate identity, rather than the lock payload, defines tree membership. The
/// lock deliberately tolerates poisoning: a panic in an observation path must
/// not permanently wedge later observation or a subtree handoff.
#[derive(Clone, Debug)]
pub(crate) struct ObservationGate(Arc<Mutex<()>>);

impl ObservationGate {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(())))
    }

    pub(crate) fn same_gate(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, ()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Driver-independent view of one declaration slot while its membership is
/// resident in a running scope.
#[derive(Clone)]
pub(crate) struct ResidentProjection {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) scope: Option<Arc<ScopeCell>>,
}

impl ResidentProjection {
    pub(crate) fn new(member: Arc<MemberCell>, scope: Option<Arc<ScopeCell>>) -> Self {
        Self { member, scope }
    }
}

struct ResidencyCompletion {
    parent: Weak<ScopeCell>,
    projection: ResidentProjection,
}

struct ResidentChild {
    projection: ResidentProjection,
    removal: Option<ResidencyCompletion>,
}

impl ResidentChild {
    fn new(parent: &Arc<ScopeCell>, projection: ResidentProjection) -> Self {
        Self {
            removal: Some(ResidencyCompletion {
                parent: Arc::downgrade(parent),
                projection: projection.clone(),
            }),
            projection,
        }
    }
}

impl Drop for ResidentChild {
    fn drop(&mut self) {
        let Some(completion) = self.removal.take() else {
            return;
        };
        let Some(parent) = completion.parent.upgrade() else {
            return;
        };
        parent.emit_locked(LifecycleEventKind::Removed {
            id: completion.projection.member.id().clone(),
            membership: completion.projection.member.membership(),
            last_incarnation: completion.projection.member.record().last_incarnation,
        });
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ObservationConfig {
    strategy: Strategy,
    intensity: Intensity,
}

#[derive(Debug, Default)]
struct ScopeControl {
    epochs: ScopeEpochs,
    shutdown: Option<ScopeRequest>,
    force: Option<ScopeRequest>,
}

#[derive(Clone, Copy, Debug)]
struct ScopeRequest {
    epoch: Epoch,
    consumed: bool,
}

/// Shared scope state follows two distinct synchronization regimes.
///
/// One gate per resident tree serializes compound observation-visible
/// transitions across configuration, records, resident children, and parent
/// links. Their watch channels retain independently readable latest values;
/// they do not make a multi-field transition atomic, so every recursive
/// observation path continues to hold that one tree gate.
///
/// Control requests, identity counters, lifecycle sequences, and the dynamic
/// route remain independent synchronization planes. The member-record watch is
/// intentionally also the driver's wake bus.
pub(crate) struct ScopeCell {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) flavor: ScopeFlavor,
    pub(crate) child_identity: Mutex<ScopeIdentity>,
    config: runtime::WatchSender<ObservationConfig>,
    record: runtime::WatchSender<ScopeRecord>,
    control: Mutex<ScopeControl>,
    dynamic_route: Mutex<Option<Arc<dyn DynamicRoute>>>,
    // Dropping a resident emits `Removed` and recursively reads this watch.
    // Every removal path must therefore release the watch guard first:
    // mutation callbacks move removed residents into outer storage, while a
    // wholesale clear uses `take`/replacement, whose old collection emerges
    // only after the channel's internal guard is released. Dropping a resident
    // inside a mutation callback would self-deadlock; adding one there is safe.
    current_children: runtime::WatchSender<Vec<ResidentChild>>,
    parent: runtime::WatchSender<Option<Weak<ScopeCell>>>,
    observation_gate: RwLock<ObservationGate>,
    lifecycle_seq: AtomicU64,
    lifecycle: LifecycleHub,
    snapshots: SnapshotHub,
    observation_closed: AtomicBool,
    #[cfg(test)]
    runtime_storage: Mutex<RuntimeStorage>,
    #[cfg(test)]
    gate_capture_probe: Mutex<Option<std::sync::mpsc::Sender<GateCapture>>>,
}

/// One observation-gate capture reported to a test probe.
///
/// A capture is reported after a thread has cloned the gate it is about to
/// acquire and before it blocks on that acquisition. Unit tests use these
/// reports as explicit barriers in place of scheduler or strong-count
/// polling: receiving a capture proves the reporting thread committed to the
/// gate that was current at that instant.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GateCapture {
    /// [`ScopeCell::with_observation_gate`] captured its current gate.
    Observation,
    /// Gate adoption captured an obsolete gate it must acquire to hand off.
    Adoption,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeStorage {
    pub(crate) children: usize,
    pub(crate) child_slots: usize,
    pub(crate) deadlines: usize,
    pub(crate) deadline_slots: usize,
}

impl ScopeCell {
    pub(crate) fn new(
        member: Arc<MemberCell>,
        flavor: ScopeFlavor,
        child_identity: ScopeIdentity,
    ) -> Arc<Self> {
        let (config, _) = runtime::watch(ObservationConfig::default());
        let (record, _) = runtime::watch(ScopeRecord {
            state: ScopeState::Unstarted,
            startup: None,
            total_restarts: TotalRestarts::ZERO,
        });
        let (current_children, _) = runtime::watch(Vec::new());
        let (parent, _) = runtime::watch(None);
        Arc::new(Self {
            member,
            flavor,
            child_identity: Mutex::new(child_identity),
            config,
            record,
            control: Mutex::new(ScopeControl::default()),
            dynamic_route: Mutex::new(None),
            current_children,
            parent,
            observation_gate: RwLock::new(ObservationGate::new()),
            lifecycle_seq: AtomicU64::new(0),
            lifecycle: LifecycleHub::default(),
            snapshots: SnapshotHub::default(),
            observation_closed: AtomicBool::new(false),
            #[cfg(test)]
            runtime_storage: Mutex::new(RuntimeStorage::default()),
            #[cfg(test)]
            gate_capture_probe: Mutex::new(None),
        })
    }

    pub(crate) fn record(&self) -> ScopeRecord {
        self.record.read_cloned()
    }

    #[cfg(test)]
    pub(crate) fn record_watcher(&self) -> runtime::WatchReceiver<ScopeRecord> {
        self.record.watcher()
    }

    pub(crate) fn resident_projections(&self) -> Vec<ResidentProjection> {
        self.current_children.read_with(|children| {
            children
                .iter()
                .map(|resident| resident.projection.clone())
                .collect()
        })
    }

    #[cfg(test)]
    pub(crate) fn runtime_storage(&self) -> RuntimeStorage {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned")
    }

    #[cfg(test)]
    pub(crate) fn record_runtime_storage(&self, storage: RuntimeStorage) {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned") = storage;
    }

    #[cfg(test)]
    pub(crate) fn observation_gate(&self) -> ObservationGate {
        self.current_observation_gate()
    }

    /// Installs a probe reporting every gate capture made through this scope.
    #[cfg(test)]
    pub(crate) fn probe_gate_captures(&self) -> std::sync::mpsc::Receiver<GateCapture> {
        let (sender, receiver) = std::sync::mpsc::channel();
        *self
            .gate_capture_probe
            .lock()
            .expect("gate capture probe mutex poisoned") = Some(sender);
        receiver
    }

    #[cfg(test)]
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
        self.observation_gate
            .read()
            .expect("observation gate handoff mutex poisoned")
            .clone()
    }

    fn adopt_observation_gate(&self, parent: &ScopeCell, gate: &ObservationGate) {
        debug_assert!(
            !std::ptr::eq(self, parent),
            "a scope cannot adopt from itself"
        );
        // The caller holds `gate` through `parent.with_observation_gate`.
        // Re-homing the parent would first have to acquire that same gate, so
        // rereading its installed pointer here cannot race a parent handoff.
        debug_assert!(
            gate.same_gate(&parent.current_observation_gate()),
            "observation gates are adopted only in the parent-to-child direction"
        );
        loop {
            let current = self.current_observation_gate();
            if current.same_gate(gate) {
                return;
            }

            #[cfg(test)]
            self.report_gate_capture(GateCapture::Adoption);

            // An operation that passed `with_observation_gate`'s pointer
            // check may finish its complete edge before handoff. An operation
            // that merely captured this obsolete gate retries after acquiring
            // it and observing the replacement.
            let current_guard = current.lock();
            let mut installed = self
                .observation_gate
                .write()
                .expect("observation gate handoff mutex poisoned");
            if current.same_gate(&installed) {
                *installed = gate.clone();
                drop(installed);
                self.adopt_descendant_observation_gates_locked(&current, gate);
                drop(current_guard);
                return;
            }
            drop(installed);
            drop(current_guard);
        }
    }

    /// Re-homes a resident subtree while its prior tree gate is held. The
    /// destination gate is also held, so observers cannot enter either tree
    /// while the handoff is installed recursively.
    fn adopt_descendant_observation_gates_locked(
        &self,
        previous: &ObservationGate,
        gate: &ObservationGate,
    ) {
        let descendants = self.current_children.read_with(|children| {
            children
                .iter()
                .filter_map(|resident| resident.projection.scope.as_ref().cloned())
                .collect::<Vec<_>>()
        });
        for descendant in descendants {
            let mut installed = descendant
                .observation_gate
                .write()
                .expect("observation gate handoff mutex poisoned");
            if installed.same_gate(previous) {
                *installed = gate.clone();
            } else {
                assert!(
                    installed.same_gate(gate),
                    "one resident tree must share one observation gate"
                );
            }
            drop(installed);
            descendant.adopt_descendant_observation_gates_locked(previous, gate);
        }
    }

    /// Runs against the current resident-tree observation gate. Adoption can
    /// race an early pre-start observer, so an obsolete-gate acquisition is
    /// detected and retried before the operation enters its critical section.
    pub(crate) fn with_observation_gate<R>(&self, operation: impl FnOnce() -> R) -> R {
        let mut operation = Some(operation);
        loop {
            let gate = self.current_observation_gate();
            #[cfg(test)]
            self.report_gate_capture(GateCapture::Observation);
            let guard = gate.lock();
            if gate.same_gate(&self.current_observation_gate()) {
                let operation = operation
                    .take()
                    .expect("observation operation runs exactly once");
                let result = operation();
                drop(guard);
                return result;
            }
            drop(guard);
        }
    }

    pub(crate) fn set_state(&self, state: ScopeState) {
        self.with_observation_gate(|| {
            self.record.send_modify(|record| {
                if state == ScopeState::Starting {
                    record.total_restarts = TotalRestarts::ZERO;
                }
                record.state = state.clone();
            });
            self.member.record.pulse();
            self.emit_locked(LifecycleEventKind::ScopeState { state });
        });
    }

    pub(crate) fn set_observation_config(&self, strategy: Strategy, intensity: Intensity) {
        self.with_observation_gate(|| {
            self.config.replace(ObservationConfig {
                strategy,
                intensity,
            });
            self.publish_snapshot_chain_locked();
        });
    }

    pub(crate) fn transition_child(
        &self,
        member: &MemberCell,
        update: impl FnOnce(&mut MemberRecord),
        event: Option<LifecycleEventKind>,
    ) {
        self.with_observation_gate(|| {
            member.update_locked(update);
            if let Some(event) = event {
                self.emit_locked(event);
            } else {
                self.publish_snapshot_chain_locked();
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn emit(&self, event: LifecycleEventKind) {
        self.with_observation_gate(|| self.emit_locked(event));
    }

    pub(crate) fn publish_child_restart(
        &self,
        member: &MemberCell,
        total_restarts: TotalRestarts,
        update: impl FnOnce(&mut MemberRecord),
        exited: LifecycleEventKind,
        scheduled: LifecycleEventKind,
    ) {
        self.with_observation_gate(|| {
            self.record.send_modify(|scope| {
                scope.total_restarts = total_restarts;
            });
            member.update_locked(update);
            self.emit_locked(exited);
            self.emit_locked(scheduled);
        });
    }

    pub(crate) fn terminalize_child(
        &self,
        member: &MemberCell,
        exit: Exit,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        self.with_observation_gate(|| {
            let record = member.record();
            if matches!(record.stage, MemberStage::Terminal(_)) {
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
            let nested = self.current_children.read_with(|children| {
                children
                    .iter()
                    .find(|resident| resident.projection.member.membership() == member.membership())
                    .and_then(|resident| resident.projection.scope.as_ref())
                    .cloned()
            });
            member.update_locked(|record| {
                record.startup_aborted = startup == StartupDisposition::Aborted;
            });
            let terminal_panics = member.terminalize_for_scope(exit.clone());
            if record.last_incarnation.is_none()
                && let Some(scope) = &nested
            {
                scope.publish_never_started_locked();
            }
            if let Some(incarnation) = exited_incarnation {
                self.emit_locked(LifecycleEventKind::Exited {
                    id: member.id().clone(),
                    membership: member.membership(),
                    incarnation,
                    exit: exit.clone(),
                });
            } else {
                // Terminals without a current incarnation have no Exited
                // event to carry snapshot publication. Publish the final
                // parent projection explicitly before nested observation
                // closes.
                self.publish_snapshot_chain_locked();
            }
            if let Some(scope) = nested {
                scope.close_observation_locked();
            }
            // A hostile mailbox waker may panic while discharge makes this
            // terminal record observable. Keep that panic authoritative, but
            // defer it until the parent snapshot/lifecycle transaction and
            // nested observation closure are complete.
            runtime::resume_preferred_panic(terminal_panics);
            true
        })
    }

    pub(crate) fn prune_child(&self, member: &MemberCell) -> bool {
        self.with_observation_gate(|| {
            let membership = member.membership();
            let mut resident = None;
            let removed = self.current_children.send_if_modified(|children| {
                let Some(index) = children
                    .iter()
                    .position(|child| child.projection.member.membership() == membership)
                else {
                    return false;
                };
                resident = Some(children.remove(index));
                true
            });
            if !removed {
                return false;
            }
            let resident = resident.expect("a reported removal owns its resident entry");
            debug_assert_eq!(resident.projection.member.membership(), membership);
            // Dropping residency under the observation gate emits the matching
            // Removed edge through its owned completion.
            drop(resident);
            true
        })
    }

    pub(crate) fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.with_observation_gate(|| self.snapshot_locked())
    }

    pub(crate) fn subscribe_snapshots(&self) -> SnapshotReceiver {
        self.with_observation_gate(|| {
            let receiver = self.snapshots.subscribe(self.snapshot_locked());
            if self.observation_closed.load(Ordering::Acquire) {
                self.snapshots.close();
            }
            receiver
        })
    }

    pub(crate) fn subscribe_lifecycle(&self) -> LifecycleEvents {
        self.with_observation_gate(|| {
            let events = self.lifecycle.subscribe();
            if self.observation_closed.load(Ordering::Acquire) {
                self.lifecycle.close();
            }
            events
        })
    }

    fn snapshot_locked(&self) -> Arc<ScopeSnapshot> {
        let record = self.record();
        let config = self.config.read_cloned();
        let children = self.current_children.read_with(|children| {
            children
                .iter()
                .map(|resident| resident.projection.clone())
                .collect::<Vec<_>>()
        });
        let children = children
            .iter()
            .map(|child| self.child_snapshot_locked(child))
            .collect::<Vec<_>>();
        Arc::new(ScopeSnapshot {
            state: record.state,
            kind: match self.flavor {
                ScopeFlavor::Ordered => ScopeKind::Ordered,
                ScopeFlavor::Dynamic => ScopeKind::Dynamic,
            },
            strategy: (self.flavor == ScopeFlavor::Ordered).then_some(config.strategy),
            intensity: config.intensity,
            total_restarts: record.total_restarts,
            lifecycle_seq: LifecycleSeq::new(self.lifecycle_seq.load(Ordering::Acquire)),
            children: children.into(),
        })
    }

    fn child_snapshot_locked(&self, child: &ResidentProjection) -> ChildSnapshot {
        let record = child.member.record();
        let options = child.member.options();
        let terminal = matches!(record.stage, MemberStage::Terminal(_));
        let nested = child.scope.as_ref().and_then(|scope| {
            (record.incarnation.is_some() || terminal).then(|| scope.snapshot_locked())
        });
        ChildSnapshot {
            id: child.member.id().clone(),
            membership: child.member.membership(),
            incarnation: record.incarnation,
            state: match record.stage {
                MemberStage::Reserved | MemberStage::Admitted => ChildState::Admitted,
                MemberStage::Starting => ChildState::Starting,
                MemberStage::Running => ChildState::Running,
                MemberStage::Restarting => ChildState::Restarting,
                MemberStage::Stopping => ChildState::Stopping,
                MemberStage::Terminal(exit) if record.startup_aborted => {
                    ChildState::StartupAborted { exit }
                }
                MemberStage::Terminal(exit) => ChildState::Stopped { exit },
            },
            last_exit: record.last_exit,
            membership_status: if record.removing {
                MembershipStatus::Removing
            } else {
                MembershipStatus::Active
            },
            restart_count: record.restart_count,
            restart_policy: options.restart,
            retention: options.retention,
            restart_at: record.restart_at,
            nested,
            scope_seq: child
                .scope
                .as_ref()
                .map(|scope| LifecycleSeq::new(scope.lifecycle_seq.load(Ordering::Acquire))),
        }
    }

    fn ancestors_locked(&self) -> Vec<Arc<ScopeCell>> {
        let mut ancestors = Vec::new();
        let mut current = self.parent.read_cloned().as_ref().and_then(Weak::upgrade);
        while let Some(scope) = current {
            current = scope.parent.read_cloned().as_ref().and_then(Weak::upgrade);
            ancestors.push(scope);
        }
        ancestors
    }

    fn publish_snapshot_chain_locked(&self) {
        self.snapshots.publish(|| self.snapshot_locked());
        for ancestor in self.ancestors_locked() {
            ancestor.snapshots.publish(|| ancestor.snapshot_locked());
        }
    }

    fn emit_locked(&self, kind: LifecycleEventKind) {
        // The resident-tree observation gate serializes every mint; the
        // atomic is the published watermark as well as the counter, avoiding
        // a second, provably uncontended lock on every lifecycle edge. The
        // mint is still a compare-and-swap so an emit that ever escaped the
        // gate could reorder events but never duplicate a sequence value.
        let seq = self
            .lifecycle_seq
            .try_update(Ordering::Release, Ordering::Relaxed, |current| {
                current.checked_add(1).filter(|seq| *seq != u64::MAX)
            })
            .ok()
            .map(|previous| previous.saturating_add(1));
        let Some(seq) = seq.map(LifecycleSeq::new) else {
            self.lifecycle_seq.store(u64::MAX, Ordering::Release);
            self.publish_snapshot_chain_locked();
            self.lifecycle.publish_lagged(1);
            for ancestor in self.ancestors_locked() {
                ancestor.lifecycle.publish_lagged(1);
            }
            return;
        };
        self.publish_snapshot_chain_locked();

        let scope = self.member.membership();
        let mut event = LifecycleEvent {
            scope_path: Vec::new(),
            scope,
            seq,
            kind,
        };
        self.lifecycle.publish(event.clone());
        let mut child_id = self.member.id().clone();
        for ancestor in self.ancestors_locked() {
            event.scope_path.insert(0, child_id);
            child_id = ancestor.member.id().clone();
            ancestor.lifecycle.publish(event.clone());
        }
    }

    fn close_observation_locked(&self) {
        if self.observation_closed.swap(true, Ordering::AcqRel) {
            return;
        }
        // Closure follows the final state/snapshot/event publication performed
        // by the caller while this same observation gate remains held.
        self.snapshots.close();
        self.lifecycle.close();
    }

    #[cfg(test)]
    pub(crate) fn replace_observation_gate(&self, gate: ObservationGate) {
        *self
            .observation_gate
            .write()
            .expect("observation gate handoff mutex remains healthy") = gate;
    }

    #[cfg(test)]
    pub(crate) fn set_lifecycle_sequence(&self, current: u64) {
        self.lifecycle_seq.store(current, Ordering::Relaxed);
    }

    pub(crate) fn set_startup(&self, startup: Result<(), StartupError>) {
        let mut published = false;
        self.record.modify_silently(|record| {
            if record.startup.is_none() {
                record.startup = Some(startup);
                published = true;
            }
        });
        if published {
            // Preserve the original driver wake boundary before releasing the
            // startup record itself.
            self.member.record.pulse();
            self.record.pulse();
        }
    }

    pub(crate) fn begin_incarnation(&self) -> Option<Epoch> {
        self.with_observation_gate(|| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            let epoch = control.epochs.begin()?;
            let state = ScopeState::Starting;
            self.record.send_modify(|record| {
                record.total_restarts = TotalRestarts::ZERO;
                record.state = state.clone();
            });
            // Hold epoch ownership through its observation projection. A
            // stale finish and a newer begin can no longer cross these two
            // state planes in opposite orders.
            drop(control);
            self.member.record.pulse();
            self.emit_locked(LifecycleEventKind::ScopeState { state });
            Some(epoch)
        })
    }

    pub(crate) fn finish_incarnation(&self, epoch: Epoch, reason: StopReason) {
        self.finish_incarnation_with_terminal(epoch, reason, None);
    }

    pub(crate) fn finish_root_incarnation(&self, epoch: Epoch, reason: StopReason, exit: Exit) {
        self.finish_incarnation_with_terminal(epoch, reason, Some(exit));
    }

    fn finish_incarnation_with_terminal(
        &self,
        epoch: Epoch,
        reason: StopReason,
        terminal_exit: Option<Exit>,
    ) {
        self.with_observation_gate(|| {
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if !control.epochs.finish(epoch) {
                // A stale driver must not overwrite the observation
                // projection of a newer live incarnation. Membership
                // terminality is not part of that projection: whoever owns a
                // terminal exit still publishes it exactly once, so declining
                // the epoch can never strand `wait_terminal`.
                drop(control);
                if let Some(exit) = terminal_exit {
                    self.member.terminalize(exit);
                    self.member.record.pulse();
                    self.record.pulse();
                    self.close_observation_locked();
                }
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
            let state = ScopeState::Stopped {
                reason: reason.clone(),
            };
            self.record.modify_silently(|record| {
                if record.startup.is_none() {
                    record.startup = Some(Err(StartupError::ShutdownRequested));
                }
                record.state = state.clone();
            });
            let terminal = terminal_exit.is_some();
            if let Some(exit) = terminal_exit {
                self.member.terminalize(exit);
            }
            // Keep epoch ownership through the final record mutation so a
            // newer begin cannot race an old driver into publishing Stopped.
            drop(control);
            self.member.record.pulse();
            // `wait_started` must not observe terminal startup until the member
            // and incarnation-control planes are mutually consistent.
            self.record.pulse();
            self.emit_locked(LifecycleEventKind::ScopeState { state });
            if terminal {
                self.close_observation_locked();
            }
        });
    }

    pub(crate) fn finish_live_root_incarnation(&self, reason: StopReason, exit: Exit) {
        let epoch = {
            let control = self.control.lock().expect("scope control mutex poisoned");
            control.epochs.live_epoch()
        };
        if let Some(epoch) = epoch {
            self.finish_root_incarnation(epoch, reason, exit);
        } else {
            self.with_observation_gate(|| {
                let state = ScopeState::Stopped { reason };
                self.record.modify_silently(|record| {
                    if record.startup.is_none() {
                        record.startup = Some(Err(StartupError::ShutdownRequested));
                    }
                    record.state = state.clone();
                });
                self.member.terminalize(exit);
                self.member.record.pulse();
                self.record.pulse();
                self.emit_locked(LifecycleEventKind::ScopeState { state });
                self.close_observation_locked();
            });
        }
    }

    pub(crate) fn request_shutdown(&self) -> Option<Epoch> {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        let RequestTarget {
            epoch: target,
            pending_incarnation,
        } = control.epochs.request_target()?;
        if control
            .shutdown
            .is_none_or(|request| request.epoch < target)
        {
            control.shutdown = Some(ScopeRequest {
                epoch: target,
                consumed: false,
            });
        }
        drop(control);
        self.member.record.pulse();
        if pending_incarnation
            && let Some(parent) = self.parent.read_cloned().as_ref().and_then(Weak::upgrade)
        {
            parent.member.record.pulse();
        }
        Some(target)
    }

    pub(crate) fn has_pending_incarnation_shutdown(&self) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.shutdown.is_some_and(|request| {
            !request.consumed && control.epochs.request_is_pending(request.epoch)
        })
    }

    pub(crate) fn has_stop_request(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control
            .shutdown
            .is_some_and(|request| request.epoch == epoch)
            || control.force.is_some_and(|request| request.epoch == epoch)
    }

    pub(crate) fn take_shutdown_request(&self, epoch: Epoch) -> bool {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        match control.shutdown.as_mut() {
            Some(request) if request.epoch == epoch && !request.consumed => {
                request.consumed = true;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn force_shutdown(&self, epoch: Epoch) {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        if control.epochs.is_current(epoch) {
            control.force = Some(ScopeRequest {
                epoch,
                consumed: false,
            });
        }
        drop(control);
        self.member.record.pulse();
    }

    pub(crate) fn take_force_request(&self, epoch: Epoch) -> bool {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        match control.force.as_mut() {
            Some(request) if request.epoch == epoch && !request.consumed => {
                request.consumed = true;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn incarnation_finished(&self, epoch: Epoch) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.epochs.finished(epoch)
    }

    pub(crate) fn set_admitted_children(self: &Arc<Self>, children: Vec<ResidentProjection>) {
        self.with_observation_gate(|| {
            let gate = self.current_observation_gate();
            self.clear_residents_locked();
            for child in children {
                if let Some(scope) = &child.scope {
                    scope.adopt_observation_gate(self, &gate);
                    scope.parent.replace(Some(Arc::downgrade(self)));
                }
                child
                    .member
                    .update_locked(|record| record.stage = MemberStage::Admitted);
                let id = child.member.id().clone();
                let membership = child.member.membership();
                self.current_children
                    .send_modify(|children| children.push(ResidentChild::new(self, child)));
                self.emit_locked(LifecycleEventKind::Added { id, membership });
            }
        });
    }

    pub(crate) fn admit_child(self: &Arc<Self>, child: ResidentProjection) {
        self.with_observation_gate(|| {
            let gate = self.current_observation_gate();
            if let Some(scope) = &child.scope {
                scope.adopt_observation_gate(self, &gate);
                scope.parent.replace(Some(Arc::downgrade(self)));
            }
            let id = child.member.id().clone();
            let membership = child.member.membership();
            child
                .member
                .update_locked(|record| record.stage = MemberStage::Admitted);
            self.current_children
                .send_modify(|children| children.push(ResidentChild::new(self, child)));
            self.emit_locked(LifecycleEventKind::Added { id, membership });
        });
    }

    pub(crate) fn clear_residents(&self) {
        self.with_observation_gate(|| self.clear_residents_locked());
    }

    fn clear_residents_locked(&self) {
        let residents = self.current_children.take();
        // Each owned residency emits Removed only after the watch guard has
        // been released, so the recursively projected set is already empty.
        drop(residents);
    }

    pub(crate) fn set_dynamic_route(&self, route: Option<Arc<dyn DynamicRoute>>) {
        *self
            .dynamic_route
            .lock()
            .expect("scope dynamic-route mutex poisoned") = route;
        self.member.record.pulse();
    }

    pub(crate) fn dynamic_route(&self) -> Option<Arc<dyn DynamicRoute>> {
        self.dynamic_route
            .lock()
            .expect("scope dynamic-route mutex poisoned")
            .clone()
    }

    pub(crate) fn signal(&self) -> &runtime::WatchSender<MemberRecord> {
        &self.member.record
    }

    pub(crate) async fn wait_started(&self) -> Result<(), StartupError> {
        let mut watcher = self.record.watcher();
        loop {
            if let Some(result) = watcher.borrow_and_update_cloned().startup {
                return result;
            }
            watcher.changed().await;
        }
    }

    pub(crate) async fn wait_stopped(&self) -> StopReason {
        self.member.wait_terminal().await;
        match self.record().state {
            ScopeState::Stopped { reason } => reason,
            ScopeState::Unstarted
            | ScopeState::Starting
            | ScopeState::Running
            | ScopeState::StartupFailed
            | ScopeState::Draining => StopReason::NeverStarted,
        }
    }

    fn publish_never_started_locked(&self) {
        self.record.modify_silently(|record| {
            if record.startup.is_none() {
                record.startup = Some(Err(StartupError::ShutdownRequested));
            }
            record.state = ScopeState::Stopped {
                reason: StopReason::NeverStarted,
            };
        });
        self.member.record.pulse();
        self.record.pulse();
        self.emit_locked(LifecycleEventKind::ScopeState {
            state: ScopeState::Stopped {
                reason: StopReason::NeverStarted,
            },
        });
    }

    pub(crate) fn terminalize_never_started(&self) {
        self.with_observation_gate(|| {
            if self.observation_closed.load(Ordering::Acquire) {
                return;
            }
            self.member.terminalize(Exit::never_started());
            self.publish_never_started_locked();
            self.close_observation_locked();
        });
    }
}
