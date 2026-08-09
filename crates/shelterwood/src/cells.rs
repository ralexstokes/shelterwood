//! Restart-stable shared member and scope state.
//!
//! These cells are the neutral synchronization and observation projection
//! layer shared by public handles, mailboxes, actor/task contexts, and the
//! mutable supervision driver. In particular, this module does not depend on
//! driver state or declaration-plan slots.

use std::{
    any::Any,
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::{
    ChildId, Exit, Incarnation, Intensity, Mailbox, Membership, Readiness, ScopeState, Strategy,
    engine::{Epoch, RequestTarget, ScopeEpochs},
    exit::{StartupError, StopReason},
    identity::{FenceCounter, ScopeIdentity},
    observe::{
        ChildSnapshot, ChildState, LifecycleEvent, LifecycleEventKind, LifecycleEvents,
        LifecycleHub, MembershipStatus, ScopeKind, ScopeSnapshot, SnapshotHub, SnapshotReceiver,
    },
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemberRecord {
    pub(crate) stage: MemberStage,
    pub(crate) incarnation: Option<Incarnation>,
    pub(crate) last_incarnation: Option<Incarnation>,
    pub(crate) last_exit: Option<Exit>,
    pub(crate) restart_count: u64,
    pub(crate) restart_at: Option<Instant>,
    pub(crate) removing: bool,
    pub(crate) startup_aborted: bool,
}

#[derive(Debug)]
pub(crate) struct MemberCell {
    id: ChildId,
    membership: Mutex<Membership>,
    record: runtime::WatchSender<MemberRecord>,
    terminal_disposal_pending: AtomicBool,
    mailbox: Mutex<MemberMailbox>,
    options: Mutex<Option<ResolvedCommonOptions>>,
    pub(crate) removal: Latch,
}

#[derive(Default)]
struct MemberMailbox {
    control: Option<Arc<dyn MailboxControl>>,
    terminal: Option<Exit>,
    teardown: Option<Box<dyn MailboxTermination>>,
}

impl fmt::Debug for MemberMailbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemberMailbox")
            .field("control", &self.control)
            .field("terminal", &self.terminal)
            .field("teardown_pending", &self.teardown.is_some())
            .finish()
    }
}

impl MemberCell {
    pub(crate) fn new(id: ChildId, membership: Membership) -> Arc<Self> {
        let (record, _) = runtime::watch(MemberRecord {
            stage: MemberStage::Reserved,
            incarnation: None,
            last_incarnation: None,
            last_exit: None,
            restart_count: 0,
            restart_at: None,
            removing: false,
            startup_aborted: false,
        });
        Arc::new(Self {
            id,
            membership: Mutex::new(membership),
            record,
            terminal_disposal_pending: AtomicBool::new(false),
            mailbox: Mutex::new(MemberMailbox::default()),
            options: Mutex::new(None),
            removal: Latch::default(),
        })
    }

    pub(crate) fn id(&self) -> &ChildId {
        &self.id
    }

    pub(crate) fn membership(&self) -> Membership {
        *self
            .membership
            .lock()
            .expect("member identity mutex poisoned")
    }

    pub(crate) fn rebase_membership(&self, membership: Membership) {
        let record = self.record();
        assert!(
            matches!(record.stage, MemberStage::Reserved)
                && record.incarnation.is_none()
                && record.last_incarnation.is_none(),
            "only an unstarted reservation can be rebased"
        );
        *self
            .membership
            .lock()
            .expect("member identity mutex poisoned") = membership;
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
        assert!(mailbox.control.is_none());
        mailbox.terminal = Some(exit);
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
        *self.options.lock().expect("member options mutex poisoned") = Some(options);
    }

    fn options(&self) -> ResolvedCommonOptions {
        self.options
            .lock()
            .expect("member options mutex poisoned")
            .clone()
            .unwrap_or_else(|| {
                crate::policy::resolve_common(
                    &crate::policy::CommonOptions::default(),
                    &crate::policy::ResolvedDefaults::default(),
                    false,
                    Readiness::Immediate,
                )
                .expect("library defaults must be valid")
            })
    }

    pub(crate) fn attach_mailbox(&self, mailbox: Arc<dyn MailboxControl>) {
        let terminal_exit = {
            let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
            assert!(state.control.is_none(), "a member can own only one mailbox");
            state.control = Some(Arc::clone(&mailbox));
            let terminal_exit = state.terminal.clone();
            if terminal_exit.is_some() {
                debug_assert!(state.teardown.is_none());
                state.teardown = mailbox.prepare_termination();
            }
            terminal_exit
        };
        if let Some(terminal_exit) = terminal_exit {
            self.publish_terminal(terminal_exit);
        }
    }

    pub(crate) fn mailbox(&self) -> Option<Arc<dyn MailboxControl>> {
        self.mailbox
            .lock()
            .expect("member mailbox mutex poisoned")
            .control
            .clone()
    }

    pub(crate) fn terminalize(&self, exit: Exit) {
        let terminal_exit = {
            let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
            if let Some(terminal_exit) = &state.terminal {
                terminal_exit.clone()
            } else {
                let teardown = state
                    .control
                    .as_ref()
                    .and_then(|mailbox| mailbox.prepare_termination());
                state.terminal = Some(exit.clone());
                state.teardown = teardown;
                exit
            }
        };
        self.publish_terminal(terminal_exit);
    }

    fn publish_terminal(&self, terminal_exit: Exit) {
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
        // Mailbox terminality and waiter completion must become visible before
        // member terminality. The watch pulse is deliberately delayed until
        // teardown has synchronously finished and unread payload ownership has
        // moved to detached disposal.
        let teardown = self
            .mailbox
            .lock()
            .expect("member mailbox mutex poisoned")
            .teardown
            .take();
        if let Some(teardown) = teardown
            && let Some(payload) = teardown.finish()
        {
            runtime::dispose_detached(payload);
        }
        if published {
            self.record.pulse();
        }
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
    pub(crate) total_restarts: u64,
}

#[derive(Clone)]
pub(crate) struct DynamicRoute(Arc<dyn Any + Send + Sync>);

impl fmt::Debug for DynamicRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("DynamicRoute").finish()
    }
}

impl DynamicRoute {
    pub(crate) fn new<T: Any + Send + Sync>(route: Arc<T>) -> Self {
        Self(route)
    }

    pub(crate) fn resolve<T: Any + Send + Sync>(self) -> Result<Arc<T>, Self> {
        Arc::downcast(self.0).map_err(Self)
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
    dynamic_route: Mutex<Option<DynamicRoute>>,
    current_children: runtime::WatchSender<Vec<ResidentChild>>,
    parent: runtime::WatchSender<Option<Weak<ScopeCell>>>,
    observation_gate: Mutex<Arc<Mutex<()>>>,
    lifecycle_sequence: Mutex<FenceCounter>,
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
            total_restarts: 0,
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
            observation_gate: Mutex::new(Arc::new(Mutex::new(()))),
            lifecycle_sequence: Mutex::new(FenceCounter::new(0)),
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
    pub(crate) fn observation_gate(&self) -> Arc<Mutex<()>> {
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

    fn current_observation_gate(&self) -> Arc<Mutex<()>> {
        Arc::clone(
            &self
                .observation_gate
                .lock()
                .expect("observation gate handoff mutex poisoned"),
        )
    }

    fn adopt_observation_gate(&self, gate: &Arc<Mutex<()>>) {
        loop {
            let current = self.current_observation_gate();
            if Arc::ptr_eq(&current, gate) {
                return;
            }

            #[cfg(test)]
            self.report_gate_capture(GateCapture::Adoption);

            // An operation that passed `with_observation_gate`'s pointer
            // check may finish its complete edge before handoff. An operation
            // that merely captured this obsolete gate retries after acquiring
            // it and observing the replacement.
            let current_guard = current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut installed = self
                .observation_gate
                .lock()
                .expect("observation gate handoff mutex poisoned");
            if Arc::ptr_eq(&current, &installed) {
                *installed = Arc::clone(gate);
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
        previous: &Arc<Mutex<()>>,
        gate: &Arc<Mutex<()>>,
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
                .lock()
                .expect("observation gate handoff mutex poisoned");
            if Arc::ptr_eq(&installed, previous) {
                *installed = Arc::clone(gate);
            } else {
                assert!(
                    Arc::ptr_eq(&installed, gate),
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
            let guard = gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if Arc::ptr_eq(&gate, &self.current_observation_gate()) {
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
                    record.total_restarts = 0;
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
        total_restarts: u64,
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
        startup_aborted: bool,
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
            member.update_locked(|record| record.startup_aborted = startup_aborted);
            member.terminalize(exit.clone());
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
            lifecycle_seq: self.lifecycle_seq.load(Ordering::Acquire),
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
                .map(|scope| scope.lifecycle_seq.load(Ordering::Acquire)),
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
        let seq = self
            .lifecycle_sequence
            .lock()
            .expect("lifecycle sequence mutex poisoned")
            .mint_sequence();
        let Some(seq) = seq else {
            self.lifecycle_seq.store(u64::MAX, Ordering::Release);
            self.publish_snapshot_chain_locked();
            self.lifecycle.publish_lagged(1);
            for ancestor in self.ancestors_locked() {
                ancestor.lifecycle.publish_lagged(1);
            }
            return;
        };
        self.lifecycle_seq.store(seq, Ordering::Release);
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
    pub(crate) fn replace_observation_gate(&self, gate: Arc<Mutex<()>>) {
        *self
            .observation_gate
            .lock()
            .expect("observation gate handoff mutex remains healthy") = gate;
    }

    #[cfg(test)]
    pub(crate) fn set_lifecycle_sequence(&self, counter: FenceCounter) {
        *self
            .lifecycle_sequence
            .lock()
            .expect("lifecycle sequence mutex poisoned") = counter;
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
                record.total_restarts = 0;
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
                    scope.adopt_observation_gate(&gate);
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
                scope.adopt_observation_gate(&gate);
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

    pub(crate) fn set_dynamic_route(&self, route: Option<DynamicRoute>) {
        *self
            .dynamic_route
            .lock()
            .expect("scope dynamic-route mutex poisoned") = route;
        self.member.record.pulse();
    }

    pub(crate) fn dynamic_route(&self) -> Option<DynamicRoute> {
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
