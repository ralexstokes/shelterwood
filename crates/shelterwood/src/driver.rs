//! Mutable runtime shell and shared handle state.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    ops::{Index, IndexMut},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use crate::{
    ChildId, Exit, ExitKind, Incarnation, IntensityTrip, Membership, Readiness, ReadinessDeadline,
    ScopeState, ShutdownStraggler, ShutdownTimeout, StartupFailure, StartupFailureCause,
    deadline::Deadline,
    engine::{
        ArbitrationClass, DeadlineHandle, DeadlineQueue, ExitDispatch, IntensityState,
        MembershipMode, ReadinessEffect, ReadinessEvent, ReadinessGate, RestartState, ScopeMode,
        StopAction, StopLadder, arbitrate, dispatch_exit, schedule_restart,
    },
    exit::{JoinVerdict, RecordedOutcome, classify_exit},
    identity::{FenceCounter, ScopeIdentity},
    mailbox::{MailboxControl, MailboxTermination},
    observe::{
        ChildSnapshot, ChildState, LifecycleEvent, LifecycleEventKind, LifecycleEvents,
        LifecycleHub, MembershipStatus, ScopeKind, ScopeSnapshot, SnapshotHub, SnapshotReceiver,
    },
    policy::{DefaultsInheritance, ResolvedDefaults},
    raw::{CatchUnwindFuture, RawRunContext, RawSpawn},
    runtime::{self, Latch},
    task::{OnceTaskBody, TaskContext, TaskFactory},
    tree::{
        BuilderCore, ChildConstruction, ChildPlan, LowerError, NotAdmittingCause, RemoveOutcome,
        ReserveError, ScopeFactory, ScopeFlavor, ScopePlan, ScopeSource, SlotCell, StartupError,
        StopReason,
    },
};

pub(crate) type DriverSleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub(crate) fn deadline(duration: Duration) -> Deadline {
    Deadline::after(runtime::now(), duration)
}

pub(crate) fn sleep_deadline(deadline: Deadline) -> DriverSleep {
    Box::pin(async move {
        match deadline.instant() {
            Some(deadline) => runtime::sleep_until_std(deadline).await,
            None => std::future::pending().await,
        }
    })
}

pub(crate) fn sleep_until(deadline: Instant) -> DriverSleep {
    Box::pin(runtime::sleep_until_std(deadline))
}

pub(crate) fn now() -> Instant {
    runtime::now()
}

/// An exactly-once synchronous completion.
///
/// The orderly path consumes the payload with [`Self::complete`]. If that
/// path is destroyed before doing so, dropping the obligation executes the
/// fail-closed fallback instead. Fallbacks must never await or join.
#[must_use = "dropping an obligation executes its fallback completion"]
struct Obligation<T> {
    payload: Option<T>,
    fallback: fn(T),
}

impl<T> Obligation<T> {
    fn new(payload: T, fallback: fn(T)) -> Self {
        Self {
            payload: Some(payload),
            fallback,
        }
    }

    fn payload_mut(&mut self) -> &mut T {
        self.payload
            .as_mut()
            .expect("a completed obligation has no payload")
    }

    fn complete(&mut self, completion: impl FnOnce(T)) {
        if let Some(payload) = self.payload.take() {
            completion(payload);
        }
    }

    fn discharge(&mut self) {
        if let Some(payload) = self.payload.take() {
            (self.fallback)(payload);
        }
    }
}

impl<T> Drop for Obligation<T> {
    fn drop(&mut self) {
        self.discharge();
    }
}

pub(crate) struct ActorWork {
    handle: Option<runtime::JoinHandle<(), ()>>,
    abort: runtime::AbortHandle,
}

impl ActorWork {
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    pub(crate) async fn join(mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let _ = runtime::join(handle).await;
    }
}

impl Drop for ActorWork {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(crate) fn spawn_actor_work(future: impl Future<Output = ()> + Send + 'static) -> ActorWork {
    let handle = runtime::spawn((), future);
    let abort = handle.abort_handle();
    ActorWork {
        handle: Some(handle),
        abort,
    }
}

pub(crate) struct BlockingWork<T> {
    handle: Option<runtime::JoinHandle<(), T>>,
}

impl<T: Send + 'static> BlockingWork<T> {
    pub(crate) async fn join(mut self) -> T {
        let handle = self
            .handle
            .take()
            .expect("blocking operation was joined more than once");
        runtime::join_resuming(handle).await.1
    }
}

pub(crate) fn spawn_blocking_work<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> BlockingWork<T> {
    BlockingWork {
        handle: Some(runtime::spawn_blocking((), operation)),
    }
}

pub(crate) enum Selected<A, B> {
    First(A),
    Second(B),
}

pub(crate) async fn select<A, B>(first: A, second: B) -> Selected<A::Output, B::Output>
where
    A: Future + Send,
    B: Future + Send,
{
    match runtime::select_two(first, second).await {
        runtime::Either::Left(value) => Selected::First(value),
        runtime::Either::Right(value) => Selected::Second(value),
    }
}

#[derive(Debug)]
pub(crate) struct Signal {
    inner: runtime::WatchSender<()>,
}

impl Default for Signal {
    fn default() -> Self {
        Self {
            inner: runtime::watch(()).0,
        }
    }
}

impl Clone for Signal {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Signal {
    pub(crate) fn pulse(&self) {
        self.inner.pulse();
    }

    pub(crate) fn watcher(&self) -> SignalWatcher {
        SignalWatcher {
            inner: self.inner.watcher(),
            _signal: self.clone(),
        }
    }

    #[cfg(test)]
    fn watcher_count(&self) -> usize {
        self.inner.receiver_count()
    }
}

pub(crate) struct SignalWatcher {
    inner: runtime::WatchReceiver<()>,
    // The previous signal implementation kept the source alive through each
    // watcher. Preserve that ownership so `changed` cannot turn channel
    // closure into a spurious pulse.
    _signal: Signal,
}

impl SignalWatcher {
    pub(crate) async fn changed(&mut self) {
        self.inner.changed().await;
    }
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
    mailbox: Mutex<MemberMailbox>,
    options: Mutex<Option<crate::policy::ResolvedCommonOptions>>,
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

    pub(crate) fn update(&self, update: impl FnOnce(&mut MemberRecord)) {
        self.record.send_modify(update);
    }

    fn update_locked(&self, update: impl FnOnce(&mut MemberRecord)) {
        self.record.send_modify(update);
    }

    pub(crate) fn set_options(&self, options: crate::policy::ResolvedCommonOptions) {
        *self.options.lock().expect("member options mutex poisoned") = Some(options);
    }

    fn options(&self) -> crate::policy::ResolvedCommonOptions {
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
        let (terminal_exit, mailbox) = {
            let mut state = self.mailbox.lock().expect("member mailbox mutex poisoned");
            let terminal_exit = if let Some(terminal_exit) = &state.terminal {
                terminal_exit.clone()
            } else {
                let teardown = state
                    .control
                    .as_ref()
                    .and_then(|mailbox| mailbox.prepare_termination());
                state.terminal = Some(exit.clone());
                state.teardown = teardown;
                exit
            };
            (terminal_exit, state.control.clone())
        };
        self.publish_terminal(terminal_exit);
        if let Some(mailbox) = mailbox {
            let stats = mailbox.stats();
            debug_assert!(stats.delivered <= stats.accepted);
            debug_assert!(stats.conflated <= stats.accepted);
            debug_assert!(stats.depth <= stats.capacity);
            let _ = stats.sends_rejected;
        }
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

struct RecordedReport {
    outcome: Option<RecordedOutcome>,
    cancelled: bool,
}

struct ReportCompletion {
    sender: mpsc::Sender<RecordedReport>,
    shutdown: Latch,
    local_stop: Option<Latch>,
}

pub(crate) struct ReportToken {
    completion: Obligation<ReportCompletion>,
}

pub(crate) struct ReportReceiver(mpsc::Receiver<RecordedReport>);

pub(crate) fn report_channel(
    shutdown: Latch,
    local_stop: Option<Latch>,
) -> (ReportToken, ReportReceiver) {
    let (sender, receiver) = mpsc::channel();
    (
        ReportToken {
            completion: Obligation::new(
                ReportCompletion {
                    sender,
                    shutdown,
                    local_stop,
                },
                |completion| completion.send(None),
            ),
        },
        ReportReceiver(receiver),
    )
}

impl ReportCompletion {
    fn send(self, outcome: Option<RecordedOutcome>) {
        let _ = self.sender.send(RecordedReport {
            outcome,
            cancelled: self.shutdown.is_fired()
                || self.local_stop.as_ref().is_some_and(Latch::is_fired),
        });
    }
}

impl ReportToken {
    pub(crate) fn record(mut self, outcome: RecordedOutcome) {
        self.completion
            .complete(|completion| completion.send(Some(outcome)));
    }
}

impl ReportReceiver {
    fn receive(self) -> RecordedReport {
        self.0
            .recv()
            .expect("owned report token must record or fall back")
    }
}

struct ResidencyCompletion {
    parent: Weak<ScopeCell>,
    slot: Arc<SlotCell>,
}

fn discharge_residency(completion: ResidencyCompletion) {
    let Some(parent) = completion.parent.upgrade() else {
        return;
    };
    parent.emit_locked(LifecycleEventKind::Removed {
        id: completion.slot.member.id().clone(),
        membership: completion.slot.member.membership(),
        last_incarnation: completion.slot.member.record().last_incarnation,
    });
}

struct ResidentChild {
    slot: Arc<SlotCell>,
    _removal: Obligation<ResidencyCompletion>,
}

impl ResidentChild {
    fn new(parent: &Arc<ScopeCell>, slot: Arc<SlotCell>) -> Self {
        Self {
            _removal: Obligation::new(
                ResidencyCompletion {
                    parent: Arc::downgrade(parent),
                    slot: Arc::clone(&slot),
                },
                discharge_residency,
            ),
            slot,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScopeRecord {
    pub(crate) state: ScopeState,
    pub(crate) startup: Option<Result<(), StartupError>>,
    pub(crate) total_restarts: u64,
}

/// Shared scope state follows two distinct synchronization regimes.
///
/// One gate per running tree serializes compound, observation-visible
/// transitions across `config`, `record`, `current_children`, and `parent`,
/// including recursive snapshot projection and lifecycle-event staging. Their
/// watch channels retain the latest independently readable value; they do not
/// make that compound transition atomic, so every multi-field observation path
/// must continue to hold the gate.
///
/// The remaining mutexes are deliberately not collapsed into one state lock.
/// `control` is an epoch-tagged request plane with concurrent callers,
/// `child_identity` and `lifecycle_sequence` mint monotonic identities, and
/// `current_dynamic` owns the independently published dynamic request route.
/// Scope state therefore has multiple writers and no single-writer invariant.
pub(crate) struct ScopeCell {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) flavor: ScopeFlavor,
    pub(crate) child_identity: Mutex<ScopeIdentity>,
    config: runtime::WatchSender<crate::tree::ScopeConfig>,
    record: runtime::WatchSender<ScopeRecord>,
    control: Mutex<ScopeControl>,
    current_dynamic: Mutex<Option<Arc<DynamicControl>>>,
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
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RuntimeStorage {
    children: usize,
    child_slots: usize,
    deadlines: usize,
    deadline_slots: usize,
}

#[derive(Debug, Default)]
struct ScopeControl {
    current_epoch: u64,
    live: bool,
    last_stopped_epoch: u64,
    shutdown: Option<ScopeRequest>,
    force: Option<ScopeRequest>,
}

#[derive(Clone, Copy, Debug)]
struct ScopeRequest {
    epoch: u64,
    consumed: bool,
}

impl ScopeCell {
    pub(crate) fn new(
        member: Arc<MemberCell>,
        flavor: ScopeFlavor,
        child_identity: ScopeIdentity,
    ) -> Arc<Self> {
        let (config, _) = runtime::watch(crate::tree::ScopeConfig::default());
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
            current_dynamic: Mutex::new(None),
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
        })
    }

    pub(crate) fn record(&self) -> ScopeRecord {
        self.record.read_cloned()
    }

    #[cfg(test)]
    fn runtime_storage(&self) -> RuntimeStorage {
        *self
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned")
    }

    fn observation_gate(&self) -> Arc<Mutex<()>> {
        Arc::clone(
            &self
                .observation_gate
                .lock()
                .expect("observation gate handoff mutex poisoned"),
        )
    }

    fn adopt_observation_gate(&self, gate: &Arc<Mutex<()>>) {
        loop {
            let current = self.observation_gate();
            if Arc::ptr_eq(&current, gate) {
                return;
            }

            // An operation that passed `with_observation_gate`'s pointer
            // check is allowed to finish its complete observation edge before
            // the handoff. Conversely, an operation that only captured this
            // gate will see the replacement after acquiring it and retry.
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

    /// Re-homes a resident subtree while its former tree gate is held. The
    /// caller also holds the destination gate, so observers cannot enter
    /// either half of the tree while the handoff is being installed.
    fn adopt_descendant_observation_gates_locked(
        &self,
        previous: &Arc<Mutex<()>>,
        gate: &Arc<Mutex<()>>,
    ) {
        let descendants = self.current_children.read_with(|children| {
            children
                .iter()
                .filter_map(|resident| resident.slot.scope.as_ref().cloned())
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

    /// Runs against the cell's current tree gate. Adoption can race an early
    /// pre-start observer, so a waiter that acquired an obsolete gate retries
    /// before entering the observation critical section.
    fn with_observation_gate<R>(&self, operation: impl FnOnce() -> R) -> R {
        let mut operation = Some(operation);
        loop {
            let gate = self.observation_gate();
            let guard = gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if Arc::ptr_eq(&gate, &self.observation_gate()) {
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

    pub(crate) fn set_config(&self, config: crate::tree::ScopeConfig) {
        self.with_observation_gate(|| {
            self.config.replace(config);
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
            if matches!(member.record().stage, MemberStage::Terminal(_)) {
                return false;
            }
            member.update_locked(|record| record.startup_aborted = startup_aborted);
            member.terminalize(exit.clone());
            if let Some(incarnation) = exited_incarnation {
                self.emit_locked(LifecycleEventKind::Exited {
                    id: member.id().clone(),
                    membership: member.membership(),
                    incarnation,
                    exit: exit.clone(),
                });
            }
            let nested = self.current_children.read_with(|children| {
                children
                    .iter()
                    .find(|resident| resident.slot.member.membership() == member.membership())
                    .and_then(|resident| resident.slot.scope.as_ref())
                    .cloned()
            });
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
                    .position(|child| child.slot.member.membership() == membership)
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
            debug_assert_eq!(resident.slot.member.membership(), membership);
            // Dropping residency while the observation gate is held emits the
            // matching Removed edge through its owned completion.
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
        let children = self
            .current_children
            .read_with(|children| {
                children
                    .iter()
                    .map(|resident| Arc::clone(&resident.slot))
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .map(|slot| self.child_snapshot_locked(&slot))
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

    fn child_snapshot_locked(&self, slot: &SlotCell) -> ChildSnapshot {
        let record = slot.member.record();
        let options = slot.member.options();
        let terminal = matches!(record.stage, MemberStage::Terminal(_));
        let nested = slot.scope.as_ref().and_then(|scope| {
            (record.incarnation.is_some() || terminal).then(|| scope.snapshot_locked())
        });
        ChildSnapshot {
            id: slot.member.id().clone(),
            membership: slot.member.membership(),
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
            scope_seq: slot
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
        self.snapshots.close();
        self.lifecycle.close();
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
            // Before the record became watch-backed, startup waiters shared
            // the member signal. Preserve that ordering boundary: publish the
            // member-plane wake before releasing the startup result itself.
            self.member.record.pulse();
            self.record.pulse();
        }
    }

    fn begin_incarnation(&self) -> u64 {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        control.current_epoch = control.current_epoch.saturating_add(1);
        control.live = true;
        let epoch = control.current_epoch;
        drop(control);
        self.member.record.pulse();
        epoch
    }

    fn finish_incarnation(&self, epoch: u64, reason: StopReason) {
        self.finish_incarnation_with_terminal(epoch, reason, None);
    }

    fn finish_root_incarnation(&self, epoch: u64, reason: StopReason, exit: Exit) {
        self.finish_incarnation_with_terminal(epoch, reason, Some(exit));
    }

    fn finish_incarnation_with_terminal(
        &self,
        epoch: u64,
        reason: StopReason,
        terminal_exit: Option<Exit>,
    ) {
        self.with_observation_gate(|| {
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
            let mut control = self.control.lock().expect("scope control mutex poisoned");
            if control.current_epoch == epoch {
                control.live = false;
                control.last_stopped_epoch = control.last_stopped_epoch.max(epoch);
                if control
                    .shutdown
                    .is_some_and(|request| request.epoch <= epoch)
                {
                    control.shutdown = None;
                }
                if control.force.is_some_and(|request| request.epoch <= epoch) {
                    control.force = None;
                }
            }
            drop(control);
            self.member.record.pulse();
            // `wait_started` must not observe terminal startup until the member
            // and incarnation-control planes above are consistent.
            self.record.pulse();
            self.emit_locked(LifecycleEventKind::ScopeState { state });
            if terminal {
                self.close_observation_locked();
            }
        });
    }

    fn finish_live_root_incarnation(&self, reason: StopReason, exit: Exit) {
        let epoch = {
            let control = self.control.lock().expect("scope control mutex poisoned");
            control.live.then_some(control.current_epoch)
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

    pub(crate) fn request_shutdown(&self) -> u64 {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        let target = if control.live {
            control.current_epoch
        } else {
            control.current_epoch.saturating_add(1)
        };
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
        target
    }

    fn take_shutdown_request(&self, epoch: u64) -> bool {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        match control.shutdown.as_mut() {
            Some(request) if request.epoch == epoch && !request.consumed => {
                request.consumed = true;
                true
            }
            _ => false,
        }
    }

    fn force_shutdown(&self, epoch: u64) {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        if control.live && control.current_epoch == epoch {
            control.force = Some(ScopeRequest {
                epoch,
                consumed: false,
            });
        }
        drop(control);
        self.member.record.pulse();
    }

    fn take_force_request(&self, epoch: u64) -> bool {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        match control.force.as_mut() {
            Some(request) if request.epoch == epoch && !request.consumed => {
                request.consumed = true;
                true
            }
            _ => false,
        }
    }

    fn incarnation_finished(&self, epoch: u64) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.last_stopped_epoch >= epoch
    }

    fn set_admitted_children(self: &Arc<Self>, children: Vec<Arc<SlotCell>>) {
        self.with_observation_gate(|| {
            let gate = self.observation_gate();
            self.clear_residents_locked();
            for child in children {
                if let Some(scope) = &child.scope {
                    scope.adopt_observation_gate(&gate);
                    scope.parent.replace(Some(Arc::downgrade(self)));
                }
                child
                    .member
                    .update_locked(|record| record.stage = MemberStage::Admitted);
                self.current_children.send_modify(|children| {
                    children.push(ResidentChild::new(self, Arc::clone(&child)))
                });
                self.emit_locked(LifecycleEventKind::Added {
                    id: child.member.id().clone(),
                    membership: child.member.membership(),
                });
            }
        });
    }

    fn admit_child(self: &Arc<Self>, child: &Arc<SlotCell>) {
        self.with_observation_gate(|| {
            let gate = self.observation_gate();
            if let Some(scope) = &child.scope {
                scope.adopt_observation_gate(&gate);
                scope.parent.replace(Some(Arc::downgrade(self)));
            }
            self.current_children
                .send_modify(|children| children.push(ResidentChild::new(self, Arc::clone(child))));
            child
                .member
                .update_locked(|record| record.stage = MemberStage::Admitted);
            self.emit_locked(LifecycleEventKind::Added {
                id: child.member.id().clone(),
                membership: child.member.membership(),
            });
        });
    }

    pub(crate) fn clear_residents(&self) {
        self.with_observation_gate(|| self.clear_residents_locked());
    }

    fn clear_residents_locked(&self) {
        let residents = self.current_children.take();
        // Each entry's owned completion emits Removed. Drop the vector only
        // after releasing the child-set watch value guard so snapshot publication can
        // project the now-empty set while this observation gate stays held.
        drop(residents);
    }

    fn set_dynamic(&self, control: Option<Arc<DynamicControl>>) {
        *self
            .current_dynamic
            .lock()
            .expect("scope dynamic-control mutex poisoned") = control;
        self.member.record.pulse();
    }

    fn dynamic(&self) -> Option<Arc<DynamicControl>> {
        self.current_dynamic
            .lock()
            .expect("scope dynamic-control mutex poisoned")
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

    pub(crate) fn terminalize_never_started(&self) {
        self.with_observation_gate(|| {
            if self.observation_closed.load(Ordering::Acquire) {
                return;
            }
            self.member.terminalize(Exit::never_started());
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
            self.close_observation_locked();
        });
    }
}

pub(crate) struct SystemRun {
    pub(crate) root: Arc<ScopeCell>,
    driver: Option<runtime::JoinHandle<(), StopReason>>,
}

pub(crate) type RemovalResponse = runtime::OneShotReceiver<RemoveOutcome>;

#[derive(Default)]
struct RemovalResponses(Vec<runtime::OneShotSender<RemoveOutcome>>);

impl RemovalResponses {
    fn subscribe(&mut self) -> RemovalResponse {
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

fn complete_removals(responses: RemovalResponses) {
    responses.complete(RemoveOutcome::Removed);
}

fn completed_removal(outcome: RemoveOutcome) -> RemovalResponse {
    let (sender, receiver) = runtime::oneshot();
    sender
        .send(outcome)
        .expect("a fresh removal receiver must be open");
    receiver
}

struct DynamicEntry {
    slot: Arc<SlotCell>,
    admitted: bool,
    fused_cancel: Option<Latch>,
    removal: Obligation<RemovalResponses>,
    removal_started: bool,
}

struct DynamicState {
    accepting: bool,
    entries: HashMap<ChildId, DynamicEntry>,
}

pub(crate) struct DynamicControl {
    scope: Weak<ScopeCell>,
    events: runtime::MpscSender<DriverEvent>,
    state: Mutex<DynamicState>,
}

impl DynamicControl {
    fn new(scope: &Arc<ScopeCell>, events: runtime::MpscSender<DriverEvent>) -> Arc<Self> {
        Arc::new(Self {
            scope: Arc::downgrade(scope),
            events,
            state: Mutex::new(DynamicState {
                accepting: true,
                entries: HashMap::new(),
            }),
        })
    }

    fn close(&self) -> HashMap<ChildId, DynamicEntry> {
        let mut state = self.state.lock().expect("dynamic-state mutex poisoned");
        state.accepting = false;
        let entries = std::mem::take(&mut state.entries);
        drop(state);
        for entry in entries.values() {
            if !entry.admitted {
                drop(entry.slot.take_defined());
                entry.slot.member.terminalize(Exit::never_started());
                if let Some(scope) = &entry.slot.scope {
                    scope.terminalize_never_started();
                }
            }
        }
        // The caller holds admitted entries across member terminality and
        // residency removal. Dropping them then completes every in-flight
        // removal without waking a remover before those observation edges.
        entries
    }
}

pub(crate) struct DynamicReservation {
    pub(crate) slot: Arc<SlotCell>,
    pub(crate) control: Arc<DynamicControl>,
}

pub(crate) fn reserve_dynamic(
    scope: &Arc<ScopeCell>,
    id: ChildId,
    child_scope: Option<ScopeFlavor>,
) -> Result<DynamicReservation, ReserveError> {
    if matches!(scope.member.record().stage, MemberStage::Terminal(_)) {
        return Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal));
    }
    let control = scope.dynamic().ok_or(ReserveError::NotAdmitting(
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
    let membership = scope
        .child_identity
        .lock()
        .expect("scope identity mutex poisoned")
        .mint_membership(&id)
        .ok_or(ReserveError::IdentityExhausted)?;
    let member = MemberCell::new(id.clone(), membership);
    let child_scope = child_scope.map(|flavor| {
        let identity = ScopeIdentity::new().expect("global scope identity space exhausted");
        ScopeCell::new(Arc::clone(&member), flavor, identity)
    });
    let slot = SlotCell::new(member, child_scope);
    state.entries.insert(
        id,
        DynamicEntry {
            slot: Arc::clone(&slot),
            admitted: false,
            fused_cancel: None,
            removal: Obligation::new(RemovalResponses::default(), complete_removals),
            removal_started: false,
        },
    );
    Ok(DynamicReservation {
        slot,
        control: Arc::clone(&control),
    })
}

pub(crate) fn start_admission(
    control: Arc<DynamicControl>,
    slot: Arc<SlotCell>,
    fused_cancel: Option<Latch>,
) -> runtime::OneShotReceiver<Result<(), ReserveError>> {
    let (sender, response) = runtime::oneshot();
    let request = AdmissionRequest {
        control: Arc::downgrade(&control),
        slot,
        fused_cancel,
        response: Obligation::new(sender, |sender| {
            let _ = sender.send(Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal)));
        }),
    };
    runtime::spawn((), async move {
        let _ = runtime::mpsc_send(&control.events, DriverEvent::Admission(request)).await;
    });
    response
}

fn cancel_dynamic_reservation_parts(
    control: &Arc<DynamicControl>,
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
        let definition = slot.take_defined();
        slot.member.terminalize(Exit::never_started());
        if let Some(scope) = &slot.scope {
            scope.terminalize_never_started();
        }
        definition
    } else {
        None
    };
    (definition, removed)
}

pub(crate) fn cancel_dynamic_reservation(control: &Arc<DynamicControl>, slot: &Arc<SlotCell>) {
    let (definition, removed) = cancel_dynamic_reservation_parts(control, slot);
    // The entry's drop completes its removal response; it must follow the
    // member's terminal publication and isolated definition disposal.
    dispose_definition_then(definition, move || drop(removed));
}

pub(crate) fn signal_fused_cancel(control: &Arc<DynamicControl>, latch: &Latch) {
    latch.fire();
    if let Some(scope) = control.scope.upgrade() {
        scope.signal().pulse();
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
    let Some(control) = scope.dynamic() else {
        return completed_removal(RemoveOutcome::AlreadyAbsent);
    };
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
        let definition = entry.slot.take_defined();
        entry.slot.member.terminalize(Exit::never_started());
        if let Some(scope) = &entry.slot.scope {
            scope.terminalize_never_started();
        }
        dispose_definition_then(definition, move || drop(entry));
        return response;
    }
    // Terminal residents still have a driver registration. Route them
    // through the normal removal path, like live residents, so that
    // registration is reclaimed before the removal response completes.
    let member = Arc::clone(&entry.slot.member);
    scope.transition_child(&member, |record| record.removing = true, None);
    entry.slot.member.removal.fire();
    drop(state);
    scope.signal().pulse();
    response
}

struct AdmissionRequest {
    control: Weak<DynamicControl>,
    slot: Arc<SlotCell>,
    fused_cancel: Option<Latch>,
    response: Obligation<runtime::OneShotSender<Result<(), ReserveError>>>,
}

impl AdmissionRequest {
    fn complete(&mut self, result: Result<(), ReserveError>) {
        self.response.complete(|sender| {
            let _ = sender.send(result);
        });
    }
}

fn reject_admission_after_disposal(
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
    let disposal = runtime::spawn_blocking((), move || drop(construction));
    runtime::spawn((), async move {
        // A never-admitted definition has no incarnation verdict to publish,
        // but its destructor must still be isolated and complete before any
        // response releases ownership back to the caller.
        let _ = runtime::join(disposal).await;
        completion();
    });
}

enum DriverEvent {
    Child(ChildEvent),
    Admission(AdmissionRequest),
}

impl SystemRun {
    pub(crate) fn request_shutdown(&self) {
        self.root.request_shutdown();
    }

    pub(crate) async fn shutdown(&mut self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        let result = shutdown_scope(Arc::clone(&self.root), timeout).await;
        self.join_driver().await;
        result
    }

    pub(crate) async fn wait(&mut self) -> StopReason {
        let reason = self.root.wait_stopped().await;
        self.join_driver().await;
        reason
    }

    async fn join_driver(&mut self) {
        let Some(driver) = self.driver.take() else {
            return;
        };
        match runtime::join(driver).await {
            runtime::JoinOutcome::Ok { .. } => {}
            runtime::JoinOutcome::Panic { message, .. } => {
                let exit = Exit::new(ExitKind::Panicked { message }, false);
                self.root
                    .finish_live_root_incarnation(StopReason::ShutdownRequested, exit);
            }
            runtime::JoinOutcome::Cancelled => {
                self.root.finish_live_root_incarnation(
                    StopReason::ShutdownRequested,
                    Exit::new(ExitKind::Aborted { after_grace: false }, true),
                );
            }
        }
    }
}

pub(crate) fn runtime_available() -> bool {
    runtime::is_available()
}

fn collect_stragglers(scope: &ScopeCell, prefix: &[ChildId], out: &mut Vec<ShutdownStraggler>) {
    let children = scope.current_children.read_with(|children| {
        children
            .iter()
            .map(|resident| Arc::clone(&resident.slot))
            .collect::<Vec<_>>()
    });
    for child in children {
        if matches!(child.member.record().stage, MemberStage::Terminal(_)) {
            continue;
        }
        let mut path = prefix.to_vec();
        path.push(child.member.id().clone());
        let before = out.len();
        if let Some(nested) = &child.scope {
            collect_stragglers(nested, &path, out);
        }
        if out.len() == before {
            out.push(ShutdownStraggler {
                path,
                membership: child.member.membership(),
            });
        }
    }
}

async fn wait_for_incarnation(scope: &ScopeCell, epoch: u64) {
    let mut watcher = scope.signal().watcher();
    loop {
        if scope.incarnation_finished(epoch)
            || matches!(scope.member.record().stage, MemberStage::Terminal(_))
        {
            return;
        }
        watcher.changed().await;
    }
}

pub(crate) async fn shutdown_scope(
    scope: Arc<ScopeCell>,
    timeout: Duration,
) -> Result<(), ShutdownTimeout> {
    if matches!(scope.member.record().stage, MemberStage::Terminal(_)) {
        return Ok(());
    }
    let epoch = scope.request_shutdown();
    let mut watcher = scope.signal().watcher();
    loop {
        if scope.incarnation_finished(epoch)
            || matches!(scope.member.record().stage, MemberStage::Terminal(_))
        {
            return Ok(());
        }
        if matches!(scope.record().state, ScopeState::Draining) {
            break;
        }
        watcher.changed().await;
    }

    match runtime::timeout(timeout, wait_for_incarnation(&scope, epoch)).await {
        runtime::Timeout::Completed(()) => Ok(()),
        runtime::Timeout::Elapsed => {
            let mut stragglers = Vec::new();
            collect_stragglers(&scope, &[], &mut stragglers);
            scope.force_shutdown(epoch);
            wait_for_incarnation(&scope, epoch).await;
            Err(ShutdownTimeout { stragglers })
        }
    }
}

pub(crate) fn spawn_system(plan: ScopePlan) -> SystemRun {
    let root = Arc::clone(&plan.root);
    let monitor_root = Arc::clone(&root);
    let driver = runtime::spawn((), async move { run_scope(plan, true, None).await });
    let lifecycle = runtime::spawn((), async move {
        match runtime::join(driver).await {
            runtime::JoinOutcome::Ok { value, .. } => value,
            runtime::JoinOutcome::Panic { message, .. } => {
                let exit = Exit::new(ExitKind::Panicked { message }, false);
                monitor_root.finish_live_root_incarnation(StopReason::ShutdownRequested, exit);
                StopReason::ShutdownRequested
            }
            runtime::JoinOutcome::Cancelled => {
                monitor_root.finish_live_root_incarnation(
                    StopReason::ShutdownRequested,
                    Exit::new(ExitKind::Aborted { after_grace: false }, true),
                );
                StopReason::ShutdownRequested
            }
        }
    });
    SystemRun {
        root,
        driver: Some(lifecycle),
    }
}

enum ChildEvent {
    Constructed {
        child: ChildKey,
        incarnation: Incarnation,
        readiness: Readiness,
    },
    Ready {
        child: ChildKey,
        incarnation: Incarnation,
    },
    SelfStop {
        child: ChildKey,
        incarnation: Incarnation,
    },
    Exited {
        child: ChildKey,
        incarnation: Incarnation,
        recorded: Option<RecordedOutcome>,
        join: JoinVerdict,
        cancelled: bool,
    },
    ConstructionDisposed {
        child: ChildKey,
        panic: Option<Option<String>>,
    },
}

enum DeadlineKind {
    Readiness {
        child: ChildKey,
        incarnation: Incarnation,
    },
    Restart {
        child: ChildKey,
    },
    Stop {
        child: ChildKey,
        incarnation: Incarnation,
    },
}

struct ActiveChild {
    incarnation: Incarnation,
    started_at: Instant,
    shutdown: Latch,
    abort: Latch,
    abort_handle: runtime::AbortHandle,
    ladder: Option<StopLadder>,
    forced_outcome: Option<RecordedOutcome>,
    hard_abort_after_grace: Option<bool>,
    readiness: ReadinessGate,
    readiness_deadline: Option<DeadlineHandle>,
    ready_signal: Latch,
    construction_release: Latch,
    framework_abort: Option<Latch>,
    framework_abort_ack: Option<Latch>,
    framework_abort_deadline: Option<Instant>,
    stop_deadline: Option<DeadlineHandle>,
}

struct ChildTerminality {
    root: Arc<ScopeCell>,
    slot: Arc<SlotCell>,
}

fn discharge_child_terminality(completion: ChildTerminality) {
    let record = completion.slot.member.record();
    if matches!(record.stage, MemberStage::Terminal(_)) {
        return;
    }
    let (exit, exited_incarnation) = if record.last_incarnation.is_some() {
        (
            Exit::new(ExitKind::Aborted { after_grace: false }, true),
            record.incarnation,
        )
    } else {
        (Exit::never_started(), None)
    };
    if exited_incarnation.is_none()
        && let Some(scope) = &completion.slot.scope
    {
        scope.terminalize_never_started();
    }
    completion
        .root
        .terminalize_child(&completion.slot.member, exit, exited_incarnation, false);
}

struct ChildRuntime {
    slot: Arc<crate::tree::SlotCell>,
    terminality: Obligation<ChildTerminality>,
    construction: runtime::Isolated<ChildConstruction>,
    pending_terminal: Option<PendingTerminal>,
    options: crate::policy::ResolvedCommonOptions,
    incarnations: FenceCounter,
    restarts: RestartState,
    restart_deadline: Option<DeadlineHandle>,
    active: Option<ActiveChild>,
    initial_ready: bool,
    initial: bool,
    spawned_once: bool,
}

struct PendingTerminal {
    exit: Exit,
    exited_incarnation: Option<Incarnation>,
    startup_aborted: bool,
}

impl ChildRuntime {
    fn from_plan(plan: ChildPlan, scope: &Arc<ScopeCell>) -> Self {
        let ChildPlan {
            slot,
            construction,
            options,
        } = plan;
        // Arm terminality before any fallible setup. If a poisoned lock or
        // mailbox callback unwinds construction, this child has already left
        // ScopePlan and therefore needs its own synchronous fallback.
        let terminality = Obligation::new(
            ChildTerminality {
                root: Arc::clone(scope),
                slot: Arc::clone(&slot),
            },
            discharge_child_terminality,
        );
        let incarnations = scope
            .child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .incarnation_counter(slot.member.membership());
        if let Some(mailbox) = slot.member.mailbox() {
            mailbox.configure(options.mailbox);
        }
        Self {
            terminality,
            slot,
            construction,
            pending_terminal: None,
            options,
            incarnations,
            restarts: RestartState::new(),
            restart_deadline: None,
            active: None,
            initial_ready: false,
            initial: true,
            spawned_once: false,
        }
    }

    fn is_disposing(&self) -> bool {
        self.pending_terminal.is_some()
    }

    fn is_terminal(&self) -> bool {
        matches!(self.slot.member.record().stage, MemberStage::Terminal(_))
    }

    fn terminalize(
        &mut self,
        root: &ScopeCell,
        exit: Exit,
        exited_incarnation: Option<Incarnation>,
        startup_aborted: bool,
    ) -> bool {
        let changed =
            root.terminalize_child(&self.slot.member, exit, exited_incarnation, startup_aborted);
        if matches!(self.slot.member.record().stage, MemberStage::Terminal(_)) {
            self.terminality.complete(drop);
        }
        changed
    }

    fn complete_terminality(&mut self) {
        self.terminality.complete(drop);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupPhase {
    Pending,
    Complete,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Driver-loop lifecycle state. `ScopeRecord` remains the observation projection and
/// `ScopeControl` remains the epoch-tagged request plane; neither duplicates these phases.
enum ScopePhase {
    Starting,
    Running,
    StartupFailed,
    Draining {
        reason: StopReason,
        startup: StartupPhase,
    },
}

impl ScopePhase {
    fn startup_complete(&self) -> bool {
        matches!(
            self,
            Self::Running
                | Self::Draining {
                    startup: StartupPhase::Complete,
                    ..
                }
        )
    }

    fn startup_failed(&self) -> bool {
        matches!(
            self,
            Self::StartupFailed
                | Self::Draining {
                    startup: StartupPhase::Failed,
                    ..
                }
        )
    }

    fn is_draining(&self) -> bool {
        matches!(self, Self::Draining { .. })
    }

    fn draining_reason(&self) -> Option<&StopReason> {
        match self {
            Self::Draining { reason, .. } => Some(reason),
            Self::Starting | Self::Running | Self::StartupFailed => None,
        }
    }

    fn begin_drain(&mut self, reason: StopReason) -> bool {
        let startup = match self {
            Self::Starting => StartupPhase::Pending,
            Self::Running => StartupPhase::Complete,
            Self::StartupFailed => StartupPhase::Failed,
            Self::Draining { .. } => return false,
        };
        *self = Self::Draining { reason, startup };
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ChildKey {
    index: usize,
    generation: u64,
}

struct ChildArenaSlot {
    generation: u64,
    child: Option<ChildRuntime>,
}

#[derive(Default)]
struct ChildArena {
    // Vacancies are reused, but every reuse advances the generation carried
    // by driver events and deadlines. A late event can therefore miss; it
    // can never address the new resident of the same physical slot.
    slots: Vec<ChildArenaSlot>,
    free: Vec<usize>,
    len: usize,
}

impl ChildArena {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: Vec::with_capacity(capacity),
            free: Vec::new(),
            len: 0,
        }
    }

    fn insert(&mut self, child: ChildRuntime) -> ChildKey {
        self.len += 1;
        if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index];
            debug_assert!(slot.child.is_none());
            slot.child = Some(child);
            ChildKey {
                index,
                generation: slot.generation,
            }
        } else {
            let index = self.slots.len();
            self.slots.push(ChildArenaSlot {
                generation: 0,
                child: Some(child),
            });
            ChildKey {
                index,
                generation: 0,
            }
        }
    }

    fn get(&self, key: ChildKey) -> Option<&ChildRuntime> {
        self.slots
            .get(key.index)
            .filter(|slot| slot.generation == key.generation)
            .and_then(|slot| slot.child.as_ref())
    }

    fn get_mut(&mut self, key: ChildKey) -> Option<&mut ChildRuntime> {
        self.slots
            .get_mut(key.index)
            .filter(|slot| slot.generation == key.generation)
            .and_then(|slot| slot.child.as_mut())
    }

    fn remove(&mut self, key: ChildKey) -> Option<ChildRuntime> {
        let slot = self.slots.get_mut(key.index)?;
        if slot.generation != key.generation {
            return None;
        }
        let child = slot.child.take()?;
        self.len -= 1;
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            self.free.push(key.index);
        }
        Some(child)
    }

    fn key_at(&self, index: usize) -> Option<ChildKey> {
        self.slots.get(index).and_then(|slot| {
            slot.child.as_ref().map(|_| ChildKey {
                index,
                generation: slot.generation,
            })
        })
    }

    fn keys(&self) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.slots.iter().enumerate().filter_map(|(index, slot)| {
            slot.child.as_ref().map(|_| ChildKey {
                index,
                generation: slot.generation,
            })
        })
    }

    fn values(&self) -> impl Iterator<Item = &ChildRuntime> {
        self.slots.iter().filter_map(|slot| slot.child.as_ref())
    }

    fn values_mut(&mut self) -> impl Iterator<Item = &mut ChildRuntime> {
        self.slots.iter_mut().filter_map(|slot| slot.child.as_mut())
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.free.clear();
        self.len = 0;
    }

    #[cfg(test)]
    fn storage_len(&self) -> usize {
        self.slots.len()
    }
}

impl Index<ChildKey> for ChildArena {
    type Output = ChildRuntime;

    fn index(&self, key: ChildKey) -> &Self::Output {
        self.get(key).expect("live child key")
    }
}

impl IndexMut<ChildKey> for ChildArena {
    fn index_mut(&mut self, key: ChildKey) -> &mut Self::Output {
        self.get_mut(key).expect("live child key")
    }
}

struct ScopeRuntime {
    root: Arc<ScopeCell>,
    flavor: ScopeFlavor,
    defaults: ResolvedDefaults,
    intensity_policy: crate::Intensity,
    intensity: IntensityState,
    children: ChildArena,
    events: runtime::MpscSender<DriverEvent>,
    deadlines: DeadlineQueue<DeadlineKind>,
    jitter: runtime::JitterRng,
    phase: ScopePhase,
    next_ordered_start: usize,
    is_root: bool,
    parent_ready: Option<Latch>,
    dynamic: Option<Arc<DynamicControl>>,
    epoch: u64,
    ancestor_shutdown: Option<Latch>,
    ancestor_shutdown_seen: bool,
    ancestor_abort: Option<Latch>,
    ancestor_abort_ack: Option<Latch>,
    ancestor_abort_seen: bool,
    completion: Option<ScopeCompletion>,
}

struct ScopeCompletion {
    reason: StopReason,
    root_exit: Option<Exit>,
}

impl Drop for ScopeRuntime {
    fn drop(&mut self) {
        let dynamic_entries = if let Some(dynamic) = &self.dynamic {
            let entries = dynamic.close();
            self.root.set_dynamic(None);
            Some(entries)
        } else {
            None
        };
        for child in self.children.values_mut() {
            if let Some(active) = child.active.take() {
                if let Some(mailbox) = child.slot.member.mailbox() {
                    mailbox.freeze(active.incarnation);
                    if let Some(teardown) = mailbox.close(active.incarnation) {
                        runtime::dispose_detached(teardown);
                    }
                }
                active.shutdown.fire();
                active.abort.fire();
                active.abort_handle.abort();
            }
            // Driver destruction consumes the same owned terminality
            // completion as the orderly path. Its fallback publishes the
            // coarse kill verdict synchronously.
            child.terminality.discharge();
        }
        // Residency owns the matching Removed edges. Clearing the set after
        // terminality discharges them all before the scope's final event.
        self.root.clear_residents();
        // Dynamic entries own removal completions. Keep them armed until the
        // corresponding members are terminal and no longer resident.
        drop(dynamic_entries);
        self.children.clear();
        if !matches!(self.root.record().state, ScopeState::Stopped { .. }) {
            let completion = self.completion.take();
            let reason = completion
                .as_ref()
                .map(|completion| completion.reason.clone())
                .or_else(|| self.phase.draining_reason().cloned())
                .unwrap_or(StopReason::ShutdownRequested);
            if let Some(exit) = completion.and_then(|completion| completion.root_exit) {
                self.root.finish_root_incarnation(self.epoch, reason, exit);
            } else {
                self.root.finish_incarnation(self.epoch, reason);
            }
        }
    }
}

enum SpawnBody {
    Raw {
        spawn: RawSpawn,
        context: RawRunContext,
    },
    TaskRestartable(TaskFactory),
    TaskOnce(Box<dyn FnOnce(TaskContext) -> crate::task::TaskFuture + Send + 'static>),
    ScopeRestartable {
        factory: ScopeFactory,
        scope: Arc<ScopeCell>,
        inherited: ResolvedDefaults,
    },
    ScopeOnce {
        tree: Box<BuilderCore>,
        scope: Arc<ScopeCell>,
        inherited: ResolvedDefaults,
    },
}

struct FactoryGuard(Option<Box<dyn Send + 'static>>);

impl FactoryGuard {
    fn new(factory: Option<Box<dyn Send + 'static>>) -> Self {
        Self(factory)
    }

    fn finish(&mut self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        let factory = self.0.take();
        catch_unwind(AssertUnwindSafe(|| drop(factory))).err()
    }
}

impl Drop for FactoryGuard {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        let panic = self.finish();
        if !already_panicking && let Some(payload) = panic {
            resume_unwind(payload);
        }
    }
}

impl ScopeRuntime {
    #[cfg(test)]
    fn record_storage(&self) {
        *self
            .root
            .runtime_storage
            .lock()
            .expect("runtime-storage mutex poisoned") = RuntimeStorage {
            children: self.children.len(),
            child_slots: self.children.storage_len(),
            deadlines: self.deadlines.len(),
            deadline_slots: self.deadlines.storage_len(),
        };
    }

    fn spawn_child(&mut self, key: ChildKey) {
        let Some(child) = self.children.get(key) else {
            return;
        };
        if self.phase.is_draining() || child.is_terminal() || child.is_disposing() {
            return;
        }
        let child = &mut self.children[key];
        if let Some(deadline) = child.restart_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        let Some(incarnation) = mint_child_incarnation(&child.slot, &mut child.incarnations) else {
            child.complete_terminality();
            // Incarnation exhaustion terminalized the membership (§3.1):
            // publish the observation edges, then route the terminal outcome
            // through the same paths as a terminal exit so ordered startup
            // fails or draining advances instead of wedging the scope.
            self.root.transition_child(&child.slot.member, |_| {}, None);
            if child.options.retention == crate::Retention::Remove {
                self.root.prune_child(&child.slot.member);
            }
            if let Some(construction) = child.construction.take() {
                runtime::dispose_detached(construction);
            }
            let exit = match child.slot.member.record().stage {
                MemberStage::Terminal(exit) => exit,
                _ => Exit::never_started(),
            };
            let pre_ready = child.initial && !self.phase.startup_complete() && !child.initial_ready;
            let removing = child.slot.member.record().removing;
            let retention_remove = child.options.retention == crate::Retention::Remove;
            if removing {
                self.finalize_removal(key);
            } else if pre_ready && !self.phase.is_draining() {
                self.fail_startup(key, exit);
            } else {
                if retention_remove {
                    self.prune_terminal(key);
                }
                if self.phase.is_draining() {
                    self.stop_next_ordered();
                }
            }
            return;
        };

        let shutdown = Latch::default();
        let abort = Latch::default();
        let ready = Latch::default();
        let ended = Latch::default();
        let construction_release = Latch::default();
        let local_stop = Latch::default();
        let id = child.slot.member.id().clone();
        let task_context = TaskContext::new(
            id.clone(),
            incarnation,
            shutdown.clone(),
            abort.clone(),
            ready.clone(),
        );
        let construction = child.construction.get_mut();
        let scope_child = matches!(construction, ChildConstruction::Scope(_));
        let (body, factory_guard, declared_readiness) = match construction {
            ChildConstruction::Raw(definition) => {
                let spawn = definition.take_spawn();
                let factory_guard = spawn.factory_guard();
                (
                    SpawnBody::Raw {
                        spawn,
                        context: RawRunContext {
                            id,
                            incarnation,
                            member: Arc::clone(&child.slot.member),
                            scope: crate::ScopeRef {
                                cell: Arc::clone(&self.root),
                            },
                            shutdown: shutdown.clone(),
                            abort: abort.clone(),
                            ready: ready.clone(),
                            local_stop: local_stop.clone(),
                            mailbox_shutdown: child.options.mailbox_shutdown,
                        },
                    },
                    factory_guard,
                    None,
                )
            }
            ChildConstruction::Task(definition) => {
                let factory = Arc::clone(&definition.factory);
                (
                    SpawnBody::TaskRestartable(Arc::clone(&factory)),
                    Some(Box::new(factory) as Box<dyn Send + 'static>),
                    Some(child.options.readiness),
                )
            }
            ChildConstruction::TaskOnce(definition) => {
                let body = std::mem::replace(&mut definition.body, OnceTaskBody::Spent);
                match body {
                    OnceTaskBody::Available(body) => (
                        SpawnBody::TaskOnce(body),
                        None,
                        Some(child.options.readiness),
                    ),
                    OnceTaskBody::Spent => {
                        panic!("one-shot task construction invoked more than once")
                    }
                }
            }
            ChildConstruction::Scope(definition) => {
                let inherited = match definition.defaults {
                    DefaultsInheritance::Inherit => self.defaults.clone(),
                    DefaultsInheritance::Reset => ResolvedDefaults::default(),
                };
                match &mut definition.source {
                    ScopeSource::Restartable(factory) => (
                        SpawnBody::ScopeRestartable {
                            factory: Arc::clone(factory),
                            scope: Arc::clone(
                                child
                                    .slot
                                    .scope
                                    .as_ref()
                                    .expect("scope construction needs a scope cell"),
                            ),
                            inherited,
                        },
                        Some(Box::new(Arc::clone(factory)) as Box<dyn Send + 'static>),
                        Some(Readiness::Manual),
                    ),
                    ScopeSource::OneShot(_) => {
                        let source = std::mem::replace(&mut definition.source, ScopeSource::Spent);
                        let ScopeSource::OneShot(tree) = source else {
                            unreachable!()
                        };
                        (
                            SpawnBody::ScopeOnce {
                                tree,
                                scope: Arc::clone(
                                    child
                                        .slot
                                        .scope
                                        .as_ref()
                                        .expect("scope construction needs a scope cell"),
                                ),
                                inherited,
                            },
                            None,
                            Some(Readiness::Manual),
                        )
                    }
                    ScopeSource::Spent => {
                        panic!("one-shot subtree construction invoked more than once")
                    }
                }
            }
        };
        let construction_pending = declared_readiness.is_none();
        let gated = scope_child
            || declared_readiness.is_none_or(|readiness| readiness != Readiness::Immediate);

        let now = runtime::now();
        child.spawned_once = true;
        if let Some(mailbox) = child.slot.member.mailbox() {
            mailbox.bind(incarnation);
        }
        self.root.transition_child(
            &child.slot.member,
            |record| {
                record.stage = MemberStage::Starting;
                record.incarnation = Some(incarnation);
                record.last_incarnation = Some(incarnation);
                record.restart_at = None;
            },
            Some(LifecycleEventKind::Started {
                id: child.slot.member.id().clone(),
                membership: child.slot.member.membership(),
                incarnation,
            }),
        );

        let (readiness, readiness_deadline) = if construction_pending {
            (ReadinessGate::Waiting { deadline: None }, None)
        } else if !gated {
            ready.fire();
            child.initial_ready = true;
            self.root.transition_child(
                &child.slot.member,
                |record| record.stage = MemberStage::Running,
                Some(LifecycleEventKind::Ready {
                    id: child.slot.member.id().clone(),
                    membership: child.slot.member.membership(),
                    incarnation,
                }),
            );
            (ReadinessGate::Immediate, None)
        } else {
            let (deadline, handle) = match child.options.readiness_deadline {
                ReadinessDeadline::Bounded(duration) => {
                    let deadline = Deadline::after(now, duration).instant();
                    let handle = deadline.map(|deadline| {
                        self.deadlines.push(
                            deadline,
                            DeadlineKind::Readiness {
                                child: key,
                                incarnation,
                            },
                        )
                    });
                    (deadline, handle)
                }
                ReadinessDeadline::Unbounded | ReadinessDeadline::Inherit => (None, None),
            };
            (ReadinessGate::Waiting { deadline }, handle)
        };

        let (report, report_receiver) = report_channel(shutdown.clone(), Some(local_stop.clone()));
        let nested_ready = ready.clone();
        let nested_cancel = shutdown.clone();
        let framework_abort = Latch::default();
        let nested_abort = framework_abort.clone();
        let framework_abort_ack = Latch::default();
        let nested_abort_ack = framework_abort_ack.clone();
        let constructed_sender = self.events.clone();
        let run_release = construction_release.clone();
        let child_readiness_override = child.options.readiness_override;
        if child.options.restart.is_never() {
            // `body` and `factory_guard` now own every live user value needed
            // by this sole incarnation. Releasing the framework definition
            // while those owners are still local makes its drop metadata-only;
            // the final user destructor runs inside the task boundary below.
            drop(child.construction.take());
        }
        let handle = runtime::spawn(incarnation, async move {
            let mut factory_guard = FactoryGuard::new(factory_guard);
            let body = async move {
                match body {
                    SpawnBody::Raw { spawn, context } => {
                        let instance = spawn.construct();
                        let readiness = match child_readiness_override {
                            Some(readiness) => readiness,
                            None => instance.readiness(),
                        };
                        let _ = runtime::mpsc_send(
                            &constructed_sender,
                            DriverEvent::Child(ChildEvent::Constructed {
                                child: key,
                                incarnation,
                                readiness,
                            }),
                        )
                        .await;
                        run_release.fired().await;
                        instance.run(context, readiness).await
                    }
                    SpawnBody::TaskRestartable(factory) => {
                        let future = factory(task_context);
                        future.await
                    }
                    SpawnBody::TaskOnce(body) => body(task_context).await,
                    SpawnBody::ScopeRestartable {
                        factory,
                        scope,
                        inherited,
                    } => {
                        let tree = factory();
                        run_nested_tree(
                            tree,
                            scope,
                            inherited,
                            nested_ready,
                            nested_cancel,
                            nested_abort,
                            nested_abort_ack,
                        )
                        .await
                    }
                    SpawnBody::ScopeOnce {
                        tree,
                        scope,
                        inherited,
                    } => {
                        run_nested_tree(
                            *tree,
                            scope,
                            inherited,
                            nested_ready,
                            nested_cancel,
                            nested_abort,
                            nested_abort_ack,
                        )
                        .await
                    }
                }
            };
            let outcome = CatchUnwindFuture::new(body).await;
            let factory_panic = factory_guard.finish();
            let result = match outcome {
                Ok(result) => {
                    if let Some(payload) = factory_panic {
                        resume_unwind(payload);
                    }
                    result
                }
                Err(payload) => {
                    // The execution panic is primary over a later factory
                    // capture destructor panic, matching incarnation-owned
                    // state and offload precedence.
                    drop(factory_panic);
                    resume_unwind(payload)
                }
            };
            report.record(RecordedOutcome::Returned(result));
        });
        let abort_handle = handle.abort_handle();
        let exit_sender = self.events.clone();
        let exit_ended = ended.clone();
        runtime::spawn((), async move {
            let join = match runtime::join(handle).await {
                runtime::JoinOutcome::Ok { .. } => JoinVerdict::Completed,
                runtime::JoinOutcome::Panic { message, .. } => JoinVerdict::Panicked { message },
                runtime::JoinOutcome::Cancelled => JoinVerdict::Cancelled { after_grace: false },
            };
            exit_ended.fire();
            let report = report_receiver.receive();
            let _ = runtime::mpsc_send(
                &exit_sender,
                DriverEvent::Child(ChildEvent::Exited {
                    child: key,
                    incarnation,
                    recorded: report.outcome,
                    join,
                    cancelled: report.cancelled,
                }),
            )
            .await;
        });

        let readiness_signal = ready.clone();
        let self_stop_ended = ended.clone();
        if gated {
            let ready_sender = self.events.clone();
            runtime::spawn((), async move {
                if matches!(
                    runtime::select_two(ready.fired(), ended.fired()).await,
                    runtime::Either::Left(())
                ) {
                    let _ = runtime::mpsc_send(
                        &ready_sender,
                        DriverEvent::Child(ChildEvent::Ready {
                            child: key,
                            incarnation,
                        }),
                    )
                    .await;
                }
            });
        }
        let self_stop_sender = self.events.clone();
        runtime::spawn((), async move {
            if matches!(
                runtime::select_two(local_stop.fired(), self_stop_ended.fired()).await,
                runtime::Either::Left(())
            ) {
                let _ = runtime::mpsc_send(
                    &self_stop_sender,
                    DriverEvent::Child(ChildEvent::SelfStop {
                        child: key,
                        incarnation,
                    }),
                )
                .await;
            }
        });

        child.active = Some(ActiveChild {
            incarnation,
            started_at: now,
            shutdown,
            abort,
            abort_handle,
            ladder: None,
            forced_outcome: None,
            hard_abort_after_grace: None,
            readiness,
            readiness_deadline,
            ready_signal: readiness_signal,
            construction_release,
            framework_abort: scope_child.then_some(framework_abort),
            framework_abort_ack: scope_child.then_some(framework_abort_ack),
            framework_abort_deadline: None,
            stop_deadline: None,
        });
        #[cfg(test)]
        self.record_storage();
    }

    fn progress_startup(&mut self) {
        if self.phase != ScopePhase::Starting {
            return;
        }
        match self.flavor {
            ScopeFlavor::Ordered => {
                while self.next_ordered_start < self.children.len() {
                    let key = self
                        .children
                        .key_at(self.next_ordered_start)
                        .expect("ordered children are never reclaimed during startup");
                    if !self.children[key].spawned_once {
                        self.spawn_child(key);
                    }
                    if self.children[key].initial_ready {
                        self.next_ordered_start += 1;
                    } else {
                        return;
                    }
                }
                if self
                    .children
                    .values()
                    .filter(|child| child.initial)
                    .all(|child| child.initial_ready)
                {
                    self.complete_startup();
                }
            }
            ScopeFlavor::Dynamic => {
                if self
                    .children
                    .values()
                    .filter(|child| child.initial)
                    .all(|child| child.initial_ready)
                {
                    self.complete_startup();
                }
            }
        }
    }

    fn complete_startup(&mut self) {
        self.phase = ScopePhase::Running;
        self.root.set_state(ScopeState::Running);
        self.root.set_startup(Ok(()));
        if let Some(parent_ready) = &self.parent_ready {
            parent_ready.fire();
        }
    }

    fn begin_stop_child(&mut self, key: ChildKey, forced: Option<RecordedOutcome>) {
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        if child.is_terminal() || child.is_disposing() {
            return;
        }
        if let Some(active) = &mut child.active {
            if active.ladder.is_some() {
                if forced.is_some() {
                    active.forced_outcome = forced;
                }
                return;
            }
            // Scope drain and planned removal suppress restart. Every
            // restartable spawn retains a matching factory guard in the
            // incarnation task, so releasing the driver's Arc in those modes
            // can only make that panic boundary the final capture owner.
            let restart_suppressed =
                self.phase.is_draining() || child.slot.member.record().removing;
            if restart_suppressed && let Some(construction) = child.construction.take() {
                drop(construction);
            }
            self.root.transition_child(
                &child.slot.member,
                |record| record.stage = MemberStage::Stopping,
                None,
            );
            if let Some(mailbox) = child.slot.member.mailbox() {
                mailbox.freeze(active.incarnation);
            }
            active.forced_outcome = forced;
            if let Some(deadline) = active.readiness_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            if active.forced_outcome.is_none() {
                active.readiness.step(ReadinessEvent::Shutdown);
            }
            active.ladder = Some(StopLadder::new(child.options.shutdown));
            self.advance_ladder(key, runtime::now());
        } else {
            if let Some(deadline) = child.restart_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            let record = child.slot.member.record();
            let exit = record.last_exit.unwrap_or_else(Exit::never_started);
            if record.last_incarnation.is_none() {
                if let Some(scope) = &child.slot.scope {
                    scope.terminalize_never_started();
                }
                // A never-ran terminal is the plain `Stopped { NeverStarted }`
                // state (B.6), not a §6 startup abort. Its definition still
                // owns the only user resource, so join disposal before
                // completing the scope, while publishing the never-started
                // member state synchronously for startup observation.
                self.begin_terminal_disposal(key, exit, None, false);
                self.children[key].terminalize(&self.root, Exit::never_started(), None, false);
            } else {
                // Between incarnations, the recorded exit already owns the
                // child verdict. Do not turn detached factory cleanup into a
                // shutdown straggler solely because there is no incarnation
                // left to host that cleanup.
                child.terminalize(&self.root, exit, None, false);
                if let Some(construction) = child.construction.take() {
                    runtime::dispose_detached(construction);
                }
            }
        }
    }

    fn advance_ladder(&mut self, key: ChildKey, now: Instant) {
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        let Some(active) = &mut child.active else {
            return;
        };
        if let Some(deadline) = active.stop_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        let Some(ladder) = &mut active.ladder else {
            return;
        };
        while let Some(action) = ladder.advance(now) {
            match action {
                StopAction::Cancel => {
                    active.shutdown.fire();
                }
                StopAction::Escalate => {
                    active.abort.fire();
                }
                StopAction::HardAbort { after_grace } => {
                    active.hard_abort_after_grace = Some(after_grace);
                    if let Some(abort) = &active.framework_abort {
                        if active.forced_outcome.is_none() {
                            active.forced_outcome = Some(RecordedOutcome::Aborted { after_grace });
                        }
                        abort.fire();
                        active.framework_abort_deadline =
                            Deadline::after(now, crate::policy::tidy_abort_beat(Duration::ZERO))
                                .instant();
                    } else {
                        active.abort_handle.abort();
                    }
                }
            }
        }
        let ladder_deadline = ladder.deadline();
        if active
            .framework_abort_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            active.framework_abort_deadline = None;
            if active
                .framework_abort_ack
                .as_ref()
                .is_none_or(|ack| !ack.is_fired())
            {
                active.abort_handle.abort();
            }
        }
        if let Some(deadline) = ladder_deadline.or(active.framework_abort_deadline) {
            active.stop_deadline = Some(self.deadlines.push(
                deadline,
                DeadlineKind::Stop {
                    child: key,
                    incarnation: active.incarnation,
                },
            ));
        }
    }

    fn begin_drain(&mut self, reason: StopReason) {
        let startup_pending = self.phase == ScopePhase::Starting;
        if !self.phase.begin_drain(reason) {
            return;
        }
        if startup_pending {
            self.root.set_startup(Err(StartupError::ShutdownRequested));
        }
        self.root.set_state(ScopeState::Draining);
        match self.flavor {
            ScopeFlavor::Ordered => self.stop_next_ordered(),
            ScopeFlavor::Dynamic => {
                let children: Vec<_> = self.children.keys().collect();
                for child in children {
                    self.begin_stop_child(child, None);
                }
            }
        }
    }

    fn stop_next_ordered(&mut self) {
        if self.flavor != ScopeFlavor::Ordered || !self.phase.is_draining() {
            return;
        }
        loop {
            let Some(key) = self
                .children
                .keys()
                .rev()
                .find(|key| !self.children[*key].is_terminal())
            else {
                return;
            };
            self.begin_stop_child(key, None);
            if self.children[key].active.is_some()
                || (self.children[key].is_disposing() && !self.children[key].is_terminal())
            {
                return;
            }
        }
    }

    fn force_all(&mut self) {
        if !self.phase.is_draining() {
            self.begin_drain(StopReason::ShutdownRequested);
        }
        let now = runtime::now();
        let children: Vec<_> = self.children.keys().collect();
        for key in children {
            let child = &mut self.children[key];
            let Some(active) = &mut child.active else {
                continue;
            };
            let mut ladder = StopLadder::new(crate::Shutdown::Abort);
            let _ = ladder.advance(now);
            let _ = ladder.advance(now);
            active.shutdown.fire();
            active.abort.fire();
            active.ladder = Some(ladder);
            self.advance_ladder(key, now);
        }
    }

    fn handle_constructed(
        &mut self,
        key: ChildKey,
        incarnation: Incarnation,
        readiness: Readiness,
    ) {
        let mut became_ready = false;
        let mut deadline_to_arm = None;
        {
            let Some(child) = self.children.get_mut(key) else {
                return;
            };
            let Some(active) = child.active.as_mut() else {
                return;
            };
            if active.incarnation != incarnation {
                return;
            }
            if active.ladder.is_some() {
                active.construction_release.fire();
                return;
            }
            if readiness == Readiness::Immediate {
                active.readiness = ReadinessGate::Immediate;
                active.ready_signal.fire();
                if !self.phase.startup_complete() {
                    child.initial_ready = true;
                }
                self.root.transition_child(
                    &child.slot.member,
                    |record| record.stage = MemberStage::Running,
                    Some(LifecycleEventKind::Ready {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        incarnation,
                    }),
                );
                became_ready = true;
            } else {
                let deadline = match child.options.readiness_deadline {
                    ReadinessDeadline::Bounded(duration) => {
                        Deadline::after(active.started_at, duration).instant()
                    }
                    ReadinessDeadline::Unbounded | ReadinessDeadline::Inherit => None,
                };
                active.readiness = ReadinessGate::Waiting { deadline };
                deadline_to_arm = deadline;
            }
            active.construction_release.fire();
        }
        if let Some(deadline) = deadline_to_arm {
            let handle = self.deadlines.push(
                deadline,
                DeadlineKind::Readiness {
                    child: key,
                    incarnation,
                },
            );
            if let Some(active) = self.children[key].active.as_mut()
                && active.incarnation == incarnation
            {
                active.readiness_deadline = Some(handle);
            } else {
                self.deadlines.cancel(handle);
            }
        }
        if became_ready {
            self.progress_startup();
        }
    }

    fn handle_ready(&mut self, key: ChildKey, incarnation: Incarnation) {
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        let Some(active) = child.active.as_mut() else {
            return;
        };
        if active.incarnation != incarnation
            || !matches!(
                active.readiness.step(ReadinessEvent::Signal),
                Some(ReadinessEffect::BecameReady)
            )
        {
            return;
        }
        if let Some(deadline) = active.readiness_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        if !self.phase.startup_complete() {
            child.initial_ready = true;
        }
        self.root.transition_child(
            &child.slot.member,
            |record| record.stage = MemberStage::Running,
            Some(LifecycleEventKind::Ready {
                id: child.slot.member.id().clone(),
                membership: child.slot.member.membership(),
                incarnation,
            }),
        );
        #[cfg(test)]
        self.record_storage();
        self.progress_startup();
    }

    fn handle_self_stop(&mut self, key: ChildKey, incarnation: Incarnation) {
        if self
            .children
            .get(key)
            .and_then(|child| child.active.as_ref())
            .is_some_and(|active| active.incarnation == incarnation)
        {
            self.begin_stop_child(key, None);
        }
    }

    fn handle_exit(
        &mut self,
        key: ChildKey,
        incarnation: Incarnation,
        recorded: Option<RecordedOutcome>,
        mut join: JoinVerdict,
        cancelled: bool,
    ) {
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        let Some(mut active) = child.active.take() else {
            return;
        };
        if active.incarnation != incarnation {
            child.active = Some(active);
            return;
        }
        if let Some(deadline) = active.readiness_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        if let Some(deadline) = active.stop_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        if let Some(mailbox) = child.slot.member.mailbox()
            && let Some(teardown) = mailbox.close(incarnation)
        {
            runtime::dispose_detached(teardown);
        }
        active.readiness.step(ReadinessEvent::Exit);
        if let (JoinVerdict::Cancelled { .. }, Some(after_grace)) =
            (&join, active.hard_abort_after_grace)
        {
            join = JoinVerdict::Cancelled { after_grace };
        }
        let recorded = active.forced_outcome.or(recorded);
        let exit = classify_exit(recorded, join, cancelled);
        let ran_for = runtime::now().saturating_duration_since(active.started_at);
        if ran_for >= self.intensity_policy.within {
            child.restarts.settled();
        }

        let mode = if self.phase.is_draining() {
            ScopeMode::Draining
        } else {
            ScopeMode::Running
        };
        let member_mode = if child.slot.member.record().removing {
            MembershipMode::Removing
        } else {
            MembershipMode::Active
        };
        match dispatch_exit(&exit, child.options.restart, mode, member_mode) {
            ExitDispatch::Terminal => {
                // §6's startup abort is a startup-sequence property: the
                // membership failed before its *initial* readiness edge. A
                // later incarnation stopped pre-ready (e.g. during drain)
                // does not rewind it.
                let pre_ready =
                    child.initial && !self.phase.startup_complete() && !child.initial_ready;
                self.begin_terminal_disposal(key, exit, Some(incarnation), pre_ready);
            }
            ExitDispatch::ScheduleRestart => {
                if !self.phase.startup_complete() {
                    child.initial_ready = false;
                }
                let sample = self.jitter.sample(0..u64::MAX) as f64 / u64::MAX as f64;
                let now = runtime::now();
                let decision = schedule_restart(
                    &mut child.restarts,
                    &mut self.intensity,
                    self.intensity_policy,
                    child.options.restart,
                    now,
                    sample,
                );
                self.root.publish_child_restart(
                    &child.slot.member,
                    decision.charge.total_restarts,
                    |record| {
                        record.incarnation = None;
                        record.last_exit = Some(exit.clone());
                        record.restart_count = decision.restart_count;
                        // Publish the derived schedule even when intensity
                        // prevents spawning it. `None` remains reserved for
                        // an unrepresentable clock deadline.
                        record.restart_at = decision.restart_at;
                        record.stage = MemberStage::Restarting;
                    },
                    LifecycleEventKind::Exited {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        incarnation,
                        exit: exit.clone(),
                    },
                    LifecycleEventKind::RestartScheduled {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        attempt: decision.attempt,
                        delay: decision.delay,
                    },
                );
                if decision.charge.tripped {
                    let trip = IntensityTrip {
                        max_restarts: self.intensity_policy.max_restarts,
                        observed_restarts: decision.charge.in_window,
                        within: self.intensity_policy.within,
                    };
                    if self.phase == ScopePhase::Starting {
                        self.root
                            .set_startup(Err(StartupError::IntensityTripped(trip.clone())));
                    }
                    self.begin_drain(StopReason::IntensityTripped(trip));
                } else {
                    if let Some(restart_at) = decision.restart_at {
                        child.restart_deadline = Some(
                            self.deadlines
                                .push(restart_at, DeadlineKind::Restart { child: key }),
                        );
                    }
                }
            }
        }
    }

    fn begin_terminal_disposal(
        &mut self,
        key: ChildKey,
        exit: Exit,
        exited_incarnation: Option<Incarnation>,
        startup_aborted: bool,
    ) {
        let construction = {
            let Some(child) = self.children.get_mut(key) else {
                return;
            };
            if child.pending_terminal.is_some() {
                return;
            }
            child.pending_terminal = Some(PendingTerminal {
                exit,
                exited_incarnation,
                startup_aborted,
            });
            child.construction.take()
        };
        let Some(construction) = construction else {
            self.handle_construction_disposed(key, None);
            return;
        };

        // The retained factory is user-owned. Destroy it on the blocking
        // pool and join only its verdict; the scope driver remains free to
        // advance unrelated children while the destructor runs.
        let disposal = runtime::spawn_blocking((), move || drop(construction));
        let sender = self.events.clone();
        runtime::spawn((), async move {
            let panic = match runtime::join(disposal).await {
                runtime::JoinOutcome::Ok { .. } => None,
                runtime::JoinOutcome::Panic { message } => Some(message),
                runtime::JoinOutcome::Cancelled => Some(Some(
                    "factory disposal task was cancelled before completion".to_owned(),
                )),
            };
            let _ = runtime::mpsc_send(
                &sender,
                DriverEvent::Child(ChildEvent::ConstructionDisposed { child: key, panic }),
            )
            .await;
        });
    }

    fn handle_construction_disposed(&mut self, key: ChildKey, panic: Option<Option<String>>) {
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        let Some(mut terminal) = child.pending_terminal.take() else {
            return;
        };
        if let Some(message) = panic
            && !matches!(terminal.exit.kind(), ExitKind::Panicked { .. })
        {
            terminal.exit = Exit::new(ExitKind::Panicked { message }, terminal.exit.cancelled());
        }

        let exit = terminal.exit;
        self.children[key].terminalize(
            &self.root,
            exit.clone(),
            terminal.exited_incarnation,
            terminal.startup_aborted,
        );
        let removing = self.children[key].slot.member.record().removing;
        if removing {
            self.finalize_removal(key);
        } else if terminal.startup_aborted && !self.phase.is_draining() {
            self.fail_startup(key, exit);
            if self.children[key].options.retention == crate::Retention::Remove {
                self.prune_terminal(key);
            }
        } else {
            if self.children[key].options.retention == crate::Retention::Remove {
                self.prune_terminal(key);
            }
            if self.phase.is_draining() {
                self.stop_next_ordered();
            }
        }
    }

    fn fail_startup(&mut self, key: ChildKey, exit: Exit) {
        let child = &self.children[key];
        let failure = StartupFailure {
            cause: StartupFailureCause::Child {
                id: child.slot.member.id().clone(),
                membership: child.slot.member.membership(),
                exit,
            },
        };
        self.phase = ScopePhase::StartupFailed;
        self.root
            .set_startup(Err(StartupError::StartupFailed(failure.clone())));
        if self.flavor == ScopeFlavor::Ordered {
            for later_index in key.index + 1..self.children.len() {
                let later = self
                    .children
                    .key_at(later_index)
                    .expect("ordered children are never reclaimed during startup");
                if !self.children[later].spawned_once
                    && !self.children[later].is_disposing()
                    && !self.children[later].is_terminal()
                {
                    if let Some(scope) = &self.children[later].slot.scope {
                        scope.terminalize_never_started();
                    }
                    self.begin_terminal_disposal(later, Exit::never_started(), None, false);
                    self.children[later].terminalize(
                        &self.root,
                        Exit::never_started(),
                        None,
                        false,
                    );
                }
            }
        }
        if self.is_root {
            self.root.set_state(ScopeState::StartupFailed);
        } else {
            self.begin_drain(StopReason::StartupFailed(failure));
        }
    }

    fn handle_deadline(&mut self, deadline: DeadlineKind) {
        match deadline {
            DeadlineKind::Readiness {
                child: key,
                incarnation,
            } => {
                if self
                    .children
                    .get(key)
                    .and_then(|child| child.active.as_ref())
                    .is_some_and(|active| {
                        active.incarnation == incarnation && active.ready_signal.is_fired()
                    })
                {
                    self.handle_ready(key, incarnation);
                    return;
                }
                let Some(child) = self.children.get_mut(key) else {
                    return;
                };
                let Some(active) = child.active.as_mut() else {
                    return;
                };
                if active.incarnation != incarnation {
                    return;
                }
                let Some(ReadinessEffect::TimedOut { deadline }) = active
                    .readiness
                    .step(ReadinessEvent::Deadline(runtime::now()))
                else {
                    return;
                };
                self.begin_stop_child(key, Some(RecordedOutcome::ReadinessTimedOut { deadline }));
            }
            DeadlineKind::Restart { child } => self.spawn_child(child),
            DeadlineKind::Stop { child, incarnation } => {
                if self
                    .children
                    .get(child)
                    .and_then(|child| child.active.as_ref())
                    .is_some_and(|active| active.incarnation == incarnation)
                {
                    self.advance_ladder(child, runtime::now());
                }
            }
        }
    }

    fn finish_if_ready(&mut self) -> Option<StopReason> {
        if let Some(reason) = self.phase.draining_reason() {
            if self
                .children
                .values()
                .all(|child| child.is_terminal() && !child.is_disposing())
            {
                return Some(reason.clone());
            }
            return None;
        }
        if !self.phase.startup_failed()
            && self.flavor == ScopeFlavor::Ordered
            && !self.children.is_empty()
            && self
                .children
                .values()
                .all(|child| child.is_terminal() && !child.is_disposing())
        {
            return Some(StopReason::Finished);
        }
        None
    }

    fn handle_admission(&mut self, mut request: AdmissionRequest) {
        let Some(control) = request.control.upgrade() else {
            request.complete(Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal)));
            return;
        };
        if self
            .dynamic
            .as_ref()
            .is_none_or(|current| !Arc::ptr_eq(current, &control))
            || self.phase.is_draining()
            || self.phase.startup_failed()
        {
            let cause = if self.phase.is_draining() {
                NotAdmittingCause::Draining
            } else if self.phase.startup_failed() {
                NotAdmittingCause::StartupFailed
            } else {
                NotAdmittingCause::NoLiveIncarnation
            };
            let (definition, removed) = cancel_dynamic_reservation_parts(&control, &request.slot);
            reject_admission_after_disposal(
                request,
                definition,
                removed,
                ReserveError::NotAdmitting(cause),
            );
            return;
        }
        if request.fused_cancel.as_ref().is_some_and(Latch::is_fired) {
            let (definition, removed) = cancel_dynamic_reservation_parts(&control, &request.slot);
            reject_admission_after_disposal(
                request,
                definition,
                removed,
                ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
            );
            return;
        }

        let Some(definition) = request.slot.take_defined() else {
            let (_, removed) = cancel_dynamic_reservation_parts(&control, &request.slot);
            reject_admission_after_disposal(
                request,
                None,
                removed,
                ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
            );
            return;
        };
        let (options, one_shot) = match definition.get() {
            ChildConstruction::Raw(definition) => (&definition.options, definition.one_shot()),
            ChildConstruction::Task(definition) => (&definition.options, false),
            ChildConstruction::TaskOnce(definition) => (&definition.options, true),
            ChildConstruction::Scope(definition) => (&definition.options, definition.one_shot()),
        };
        let resolved =
            crate::policy::resolve_common(options, &self.defaults, one_shot, Readiness::Immediate);
        request.slot.member.set_options(resolved.clone());
        let plan = ChildPlan {
            slot: Arc::clone(&request.slot),
            construction: definition,
            options: resolved,
        };
        {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            let id = request.slot.member.id();
            let matches_reservation = state.entries.get(id).is_some_and(|entry| {
                entry.slot.member.membership() == request.slot.member.membership()
                    && !entry.admitted
            });
            if !matches_reservation || request.fused_cancel.as_ref().is_some_and(Latch::is_fired) {
                let removed = matches_reservation
                    .then(|| state.entries.remove(id))
                    .flatten();
                drop(state);
                request.slot.member.terminalize(Exit::never_started());
                if let Some(scope) = &request.slot.scope {
                    scope.terminalize_never_started();
                }
                // The entry's drop completes its removal response; preserve
                // the same terminality-before-completion ordering as every
                // other reservation-cancellation path. Definition disposal
                // is also complete before either waiter regains ownership.
                let ChildPlan { construction, .. } = plan;
                reject_admission_after_disposal(
                    request,
                    Some(construction),
                    removed,
                    ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
                );
                return;
            }
            let entry = state
                .entries
                .get_mut(id)
                .expect("the matching reservation was just resolved");
            entry.admitted = true;
            entry.fused_cancel = request.fused_cancel.take();
        }
        let mut child = ChildRuntime::from_plan(plan, &self.root);
        child.initial = false;
        self.root.admit_child(&request.slot);
        let key = self.children.insert(child);
        #[cfg(test)]
        self.record_storage();
        request.complete(Ok(()));
        self.spawn_child(key);
    }

    fn pending_removals(&self) -> Vec<Membership> {
        let Some(control) = &self.dynamic else {
            return Vec::new();
        };
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .values()
            .filter(|entry| {
                entry.admitted
                    && !entry.removal_started
                    && (entry.slot.member.removal.is_fired()
                        || entry.fused_cancel.as_ref().is_some_and(Latch::is_fired))
            })
            .map(|entry| entry.slot.member.membership())
            .collect()
    }

    fn handle_removal(&mut self, membership: Membership) {
        if let Some(control) = &self.dynamic {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            if let Some(entry) = state
                .entries
                .values_mut()
                .find(|entry| entry.slot.member.membership() == membership)
            {
                if entry.removal_started {
                    return;
                }
                entry.removal_started = true;
            }
        }
        let Some(key) = self
            .children
            .keys()
            .find(|key| self.children[*key].slot.member.membership() == membership)
        else {
            return;
        };
        self.root.transition_child(
            &self.children[key].slot.member,
            |record| record.removing = true,
            None,
        );
        if self.children[key].is_terminal() {
            self.finalize_removal(key);
        } else {
            self.begin_stop_child(key, None);
            if self.children[key].is_terminal() {
                self.finalize_removal(key);
            }
        }
    }

    fn finalize_removal(&mut self, key: ChildKey) {
        let Some(control) = &self.dynamic else {
            return;
        };
        let member = Arc::clone(&self.children[key].slot.member);
        let id = member.id().clone();
        let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
        if state
            .entries
            .get(&id)
            .is_some_and(|entry| entry.slot.member.membership() == member.membership())
        {
            let entry = state.entries.remove(&id).expect("entry was just resolved");
            drop(state);
            self.root.prune_child(&member);
            self.reclaim_child(key);
            drop(entry);
        }
    }

    fn prune_terminal(&mut self, key: ChildKey) {
        let member = Arc::clone(&self.children[key].slot.member);
        let mut removed = None;
        if let Some(control) = &self.dynamic {
            let id = member.id().clone();
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            if state
                .entries
                .get(&id)
                .is_some_and(|entry| entry.slot.member.membership() == member.membership())
            {
                removed = state.entries.remove(&id);
            }
        }
        self.root.prune_child(&member);
        if self.flavor == ScopeFlavor::Dynamic {
            self.reclaim_child(key);
        }
        // The entry's drop completes any in-flight removal response; it must
        // follow the Removed edge so a woken remover never sees the child
        // resident.
        drop(removed);
    }

    fn reclaim_child(&mut self, key: ChildKey) {
        let Some(mut child) = self.children.remove(key) else {
            return;
        };
        if let Some(deadline) = child.restart_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        if let Some(mut active) = child.active.take() {
            if let Some(deadline) = active.readiness_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            if let Some(deadline) = active.stop_deadline.take() {
                self.deadlines.cancel(deadline);
            }
        }
        #[cfg(test)]
        self.record_storage();
    }
}

fn mint_child_incarnation(slot: &Arc<SlotCell>, counter: &mut FenceCounter) -> Option<Incarnation> {
    let member = &slot.member;
    let incarnation = ScopeIdentity::mint_incarnation(member.membership(), counter);
    if incarnation.is_none() {
        let record = member.record();
        let exit = record.last_exit.unwrap_or_else(Exit::never_started);
        member.terminalize(exit);
        if let Some(scope) = &slot.scope {
            if record.last_incarnation.is_none() {
                scope.terminalize_never_started();
            } else {
                scope.with_observation_gate(|| scope.close_observation_locked());
            }
        }
    }
    incarnation
}

async fn run_nested_tree(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    ready: Latch,
    cancel: Latch,
    abort: Latch,
    abort_ack: Latch,
) -> crate::ExitResult {
    let epoch = scope.begin_incarnation();
    let plan = match tree.lower(inherited, Some(Arc::clone(&scope))) {
        Ok(plan) => plan,
        Err(error) => {
            let failure = StartupFailure {
                cause: match error {
                    LowerError::Undefined(undefined) => StartupFailureCause::Lowering { undefined },
                    LowerError::IdentityExhausted(id) => {
                        StartupFailureCause::IdentityExhausted { id }
                    }
                },
            };
            scope.set_startup(Err(StartupError::StartupFailed(failure.clone())));
            scope.finish_incarnation(epoch, StopReason::StartupFailed(failure.clone()));
            return Err(crate::ExitError::from_startup_failure(failure));
        }
    };
    match run_scope_incarnation(
        plan,
        false,
        Some(ready),
        Some(cancel),
        Some(abort),
        Some(abort_ack),
        epoch,
    )
    .await
    {
        StopReason::Finished | StopReason::ShutdownRequested => Ok(()),
        StopReason::IntensityTripped(trip) => Err(crate::ExitError::from_intensity_trip(trip)),
        StopReason::StartupFailed(failure) => Err(crate::ExitError::from_startup_failure(failure)),
        StopReason::NeverStarted => Err(crate::ExitError::message("nested scope never started")),
    }
}

async fn run_scope(plan: ScopePlan, is_root: bool, parent_ready: Option<Latch>) -> StopReason {
    let root = Arc::clone(&plan.root);
    let epoch = root.begin_incarnation();
    run_scope_incarnation(plan, is_root, parent_ready, None, None, None, epoch).await
}

async fn run_scope_incarnation(
    mut plan: ScopePlan,
    is_root: bool,
    parent_ready: Option<Latch>,
    incarnation_cancel: Option<Latch>,
    incarnation_abort: Option<Latch>,
    incarnation_abort_ack: Option<Latch>,
    epoch: u64,
) -> StopReason {
    let root = Arc::clone(&plan.root);
    root.set_state(ScopeState::Starting);
    if is_root {
        root.member
            .update(|record| record.stage = MemberStage::Running);
    }
    let capacity = plan.children.len().saturating_mul(3).max(64);
    let (events, mut event_receiver) = runtime::bounded_mpsc(capacity);
    let dynamic =
        (plan.flavor == ScopeFlavor::Dynamic).then(|| DynamicControl::new(&root, events.clone()));
    if let Some(control) = &dynamic {
        let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
        for child in &plan.children {
            state.entries.insert(
                child.slot.member.id().clone(),
                DynamicEntry {
                    slot: Arc::clone(&child.slot),
                    admitted: true,
                    fused_cancel: None,
                    removal: Obligation::new(RemovalResponses::default(), complete_removals),
                    removal_started: false,
                },
            );
        }
        drop(state);
        root.set_dynamic(Some(Arc::clone(control)));
    }
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| Arc::clone(&child.slot))
            .collect(),
    );
    // Transfer children one at a time. The not-yet-converted suffix remains
    // owned by ScopePlan, while ChildRuntime::from_plan arms the current
    // child's obligation before fallible setup. Thus a panic at any point has
    // exactly one terminality owner for every child.
    let mut children = ChildArena::with_capacity(plan.children.len());
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children.insert(ChildRuntime::from_plan(child, &root));
    }
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        flavor: plan.flavor,
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: IntensityState::default(),
        children,
        events,
        deadlines: DeadlineQueue::default(),
        jitter: runtime::JitterRng::from_system_entropy(),
        phase: ScopePhase::Starting,
        next_ordered_start: 0,
        is_root,
        parent_ready,
        dynamic,
        epoch,
        ancestor_shutdown: incarnation_cancel,
        ancestor_shutdown_seen: false,
        ancestor_abort: incarnation_abort,
        ancestor_abort_ack: incarnation_abort_ack,
        ancestor_abort_seen: false,
        completion: None,
    };
    #[cfg(test)]
    scope.record_storage();
    plan.armed = false;
    drop(plan);

    match scope.flavor {
        ScopeFlavor::Ordered => scope.progress_startup(),
        ScopeFlavor::Dynamic => {
            let children: Vec<_> = scope.children.keys().collect();
            for child in children {
                scope.spawn_child(child);
            }
            scope.progress_startup();
        }
    }

    let mut signal = root.signal().watcher();
    loop {
        let mut pending = Vec::new();
        if root.take_shutdown_request(epoch) {
            pending.push((ArbitrationClass::ScopeShutdown, Pending::Shutdown));
        }
        if !scope.ancestor_shutdown_seen
            && scope
                .ancestor_shutdown
                .as_ref()
                .is_some_and(Latch::is_fired)
        {
            scope.ancestor_shutdown_seen = true;
            pending.push((ArbitrationClass::ScopeShutdown, Pending::AncestorShutdown));
        }
        if !scope.ancestor_abort_seen && scope.ancestor_abort.as_ref().is_some_and(Latch::is_fired)
        {
            scope.ancestor_abort_seen = true;
            pending.push((ArbitrationClass::ScopeShutdown, Pending::AncestorAbort));
        }
        if root.take_force_request(epoch) {
            pending.push((ArbitrationClass::StopDeadline, Pending::Force));
        }
        for membership in scope.pending_removals() {
            pending.push((
                ArbitrationClass::MembershipRemoval,
                Pending::Removal(membership),
            ));
        }
        while let Some(event) = runtime::mpsc_try_recv(&mut event_receiver) {
            let class = match event {
                DriverEvent::Child(ChildEvent::SelfStop { .. }) => {
                    ArbitrationClass::MembershipRemoval
                }
                DriverEvent::Child(ChildEvent::Constructed { .. })
                | DriverEvent::Child(ChildEvent::Ready { .. }) => ArbitrationClass::ReadinessSignal,
                DriverEvent::Child(
                    ChildEvent::Exited { .. } | ChildEvent::ConstructionDisposed { .. },
                ) => ArbitrationClass::ChildExit,
                DriverEvent::Admission(_) => ArbitrationClass::Admission,
            };
            pending.push((class, Pending::Driver(event)));
        }
        let now = runtime::now();
        while let Some(deadline) = scope.deadlines.pop_due(now) {
            let class = match deadline {
                DeadlineKind::Readiness { .. } => ArbitrationClass::ReadinessDeadline,
                DeadlineKind::Restart { .. } => ArbitrationClass::BackoffDue,
                DeadlineKind::Stop { .. } => ArbitrationClass::StopDeadline,
            };
            pending.push((class, Pending::Deadline(deadline)));
        }

        if pending.is_empty() {
            let ancestor_shutdown = (!scope.ancestor_shutdown_seen)
                .then(|| scope.ancestor_shutdown.clone())
                .flatten();
            let ancestor_abort = (!scope.ancestor_abort_seen)
                .then(|| scope.ancestor_abort.clone())
                .flatten();
            let ancestor_command = async move {
                let shutdown = async move {
                    if let Some(shutdown) = ancestor_shutdown {
                        shutdown.fired().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let abort = async move {
                    if let Some(abort) = ancestor_abort {
                        abort.fired().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let _ = runtime::select_two(shutdown, abort).await;
            };
            match runtime::wait_scope(
                signal.changed(),
                ancestor_command,
                &mut event_receiver,
                scope.deadlines.next(),
            )
            .await
            {
                runtime::ScopeWake::Signal | runtime::ScopeWake::ParentShutdown => continue,
                runtime::ScopeWake::Deadline => {
                    // A producer becoming ready at the same instant owns the
                    // tie over its deadline. Give tasks woken by that clock
                    // edge one turn to publish their retained readiness
                    // latch before collecting due registrations.
                    runtime::yield_now().await;
                    continue;
                }
                runtime::ScopeWake::Message(Some(event)) => {
                    let class = match event {
                        DriverEvent::Child(ChildEvent::SelfStop { .. }) => {
                            ArbitrationClass::MembershipRemoval
                        }
                        DriverEvent::Child(ChildEvent::Constructed { .. })
                        | DriverEvent::Child(ChildEvent::Ready { .. }) => {
                            ArbitrationClass::ReadinessSignal
                        }
                        DriverEvent::Child(
                            ChildEvent::Exited { .. } | ChildEvent::ConstructionDisposed { .. },
                        ) => ArbitrationClass::ChildExit,
                        DriverEvent::Admission(_) => ArbitrationClass::Admission,
                    };
                    pending.push((class, Pending::Driver(event)));
                }
                runtime::ScopeWake::Message(None) => continue,
            }
        }

        arbitrate(&mut pending);
        for (_, event) in pending {
            match event {
                Pending::Shutdown => {
                    if let Some(cancel) = &scope.ancestor_shutdown {
                        cancel.fire();
                        scope.ancestor_shutdown_seen = true;
                    }
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::AncestorShutdown => {
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::AncestorAbort => {
                    if let Some(ack) = &scope.ancestor_abort_ack {
                        ack.fire();
                    }
                    // A scheduled framework driver recursively hard-drains
                    // and joins its children. Its parent only task-aborts it
                    // at the tidy-beat backstop when this acknowledgement is
                    // never published.
                    scope.force_all();
                }
                Pending::Force => {
                    scope.force_all();
                }
                Pending::Removal(membership) => scope.handle_removal(membership),
                Pending::Driver(DriverEvent::Admission(request)) => {
                    scope.handle_admission(request);
                }
                Pending::Driver(DriverEvent::Child(ChildEvent::Constructed {
                    child,
                    incarnation,
                    readiness,
                })) => scope.handle_constructed(child, incarnation, readiness),
                Pending::Driver(DriverEvent::Child(ChildEvent::Ready { child, incarnation })) => {
                    scope.handle_ready(child, incarnation);
                }
                Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop {
                    child,
                    incarnation,
                })) => scope.handle_self_stop(child, incarnation),
                Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancelled,
                })) => scope.handle_exit(child, incarnation, recorded, join, cancelled),
                Pending::Driver(DriverEvent::Child(ChildEvent::ConstructionDisposed {
                    child,
                    panic,
                })) => scope.handle_construction_disposed(child, panic),
                Pending::Deadline(deadline) => scope.handle_deadline(deadline),
            }
        }

        if let Some(reason) = scope.finish_if_ready() {
            let root_exit = is_root.then(|| match &reason {
                StopReason::Finished | StopReason::ShutdownRequested => {
                    Exit::new(ExitKind::Completed, reason == StopReason::ShutdownRequested)
                }
                StopReason::IntensityTripped(trip) => Exit::new(
                    ExitKind::Failed(crate::ExitError::from_intensity_trip(trip.clone())),
                    false,
                ),
                StopReason::StartupFailed(failure) => Exit::new(
                    ExitKind::Failed(crate::ExitError::from_startup_failure(failure.clone())),
                    false,
                ),
                StopReason::NeverStarted => Exit::never_started(),
            });
            // ScopeRuntime's synchronous epilogue clears dynamic state,
            // discharges child obligations and residency, and only then
            // publishes the scope's terminal state.
            scope.completion = Some(ScopeCompletion {
                reason: reason.clone(),
                root_exit,
            });
            return reason;
        }
    }
}

enum Pending {
    Shutdown,
    AncestorShutdown,
    AncestorAbort,
    Force,
    Removal(Membership),
    Driver(DriverEvent),
    Deadline(DeadlineKind),
}

#[cfg(test)]
mod tests {
    use std::{
        future::{self, Future},
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{Arc, Barrier, Mutex},
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use crate::{
        ActorRef, ChildId, DynamicTree, Exit, ExitError, ExitKind, LifecycleEventKind,
        LifecycleItem, LifecycleTryRecvError, Readiness, ReadinessDeadline, RemoveOutcome,
        Retention, ScopeState, SendErrorKind, StartupError, StartupFailureCause, StopReason,
        SubtreeOnceDef, TaskDef, Tree,
        exit::RecordedOutcome,
        identity::{FenceCounter, ScopeIdentity},
        mailbox::MailboxCell,
        runtime::Latch,
        tree::{SlotCell, into_core_for_test, lower_tree_for_test},
    };

    use super::{
        ChildArena, ChildRuntime, DynamicControl, DynamicEntry, MemberCell, MemberStage,
        Obligation, RemovalResponses, RuntimeStorage, ScopeCell, ScopeFlavor, ScopePhase, Signal,
        StartupPhase, complete_removals, mint_child_incarnation, report_channel, run_nested_tree,
        run_scope_incarnation,
    };

    fn isolated_scope(id: &'static str, flavor: ScopeFlavor) -> Arc<ScopeCell> {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from(id);
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        ScopeCell::new(
            member,
            flavor,
            ScopeIdentity::new().expect("child identity available"),
        )
    }

    #[test]
    fn independent_systems_do_not_share_an_observation_critical_section() {
        let first = isolated_scope("first", ScopeFlavor::Ordered);
        let second = isolated_scope("second", ScopeFlavor::Ordered);
        let first_gate = first.observation_gate();
        let second_gate = second.observation_gate();
        assert!(!Arc::ptr_eq(&first_gate, &second_gate));

        let held = first_gate
            .lock()
            .expect("first observation gate starts healthy");
        let (completed, receiver) = std::sync::mpsc::sync_channel(0);
        let worker = std::thread::spawn(move || {
            second.set_state(ScopeState::Starting);
            completed.send(()).expect("test receiver remains available");
        });
        let result = receiver.recv_timeout(Duration::from_secs(2));
        drop(held);
        worker.join().expect("independent transition succeeds");
        assert_eq!(
            result,
            Ok(()),
            "holding one system's gate must not stall another system"
        );
    }

    #[test]
    fn admitted_subtrees_share_their_parent_observation_gate() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));

        root.set_admitted_children(vec![slot]);

        assert!(Arc::ptr_eq(
            &root.observation_gate(),
            &nested.observation_gate()
        ));
    }

    #[test]
    fn admitted_subtree_rehomes_existing_descendants_to_one_gate() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let leaf = isolated_scope("leaf", ScopeFlavor::Ordered);
        let leaf_slot = SlotCell::new(Arc::clone(&leaf.member), Some(Arc::clone(&leaf)));
        nested.set_admitted_children(vec![leaf_slot]);
        assert!(Arc::ptr_eq(
            &nested.observation_gate(),
            &leaf.observation_gate()
        ));

        let nested_slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
        root.set_admitted_children(vec![nested_slot]);

        let root_gate = root.observation_gate();
        assert!(Arc::ptr_eq(&root_gate, &nested.observation_gate()));
        assert!(Arc::ptr_eq(&root_gate, &leaf.observation_gate()));
    }

    #[test]
    fn pre_admission_observer_retries_after_gate_handoff() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let prior_gate = nested.observation_gate();
        let held = prior_gate
            .lock()
            .expect("pre-admission observation gate starts healthy");
        let observer = Arc::clone(&nested);
        let worker = std::thread::spawn(move || observer.set_state(ScopeState::Starting));

        for _ in 0..100_000 {
            if Arc::strong_count(&prior_gate) >= 3 {
                break;
            }
            std::thread::yield_now();
        }
        assert!(
            Arc::strong_count(&prior_gate) >= 3,
            "observer must be waiting on the pre-admission gate"
        );

        // Model the instant at which adoption owns the old gate and publishes
        // the replacement. The waiting observer must acquire the old gate,
        // detect this handoff, and retry on the root gate.
        *nested
            .observation_gate
            .lock()
            .expect("observation gate handoff mutex remains healthy") = root.observation_gate();
        drop(held);
        worker.join().expect("observer follows the gate handoff");

        assert_eq!(nested.record().state, ScopeState::Starting);
        assert!(Arc::ptr_eq(
            &root.observation_gate(),
            &nested.observation_gate()
        ));
    }

    #[test]
    fn scope_phase_preserves_startup_status_while_draining() {
        for (mut phase, startup) in [
            (ScopePhase::Starting, StartupPhase::Pending),
            (ScopePhase::Running, StartupPhase::Complete),
            (ScopePhase::StartupFailed, StartupPhase::Failed),
        ] {
            assert!(phase.begin_drain(StopReason::ShutdownRequested));
            assert_eq!(
                phase,
                ScopePhase::Draining {
                    reason: StopReason::ShutdownRequested,
                    startup,
                }
            );
            assert!(!phase.begin_drain(StopReason::Finished));
        }
    }

    #[test]
    fn gate_handoff_waits_for_an_in_flight_observation_edge() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let prior_gate = nested.observation_gate();
        let (entered, entered_receiver) = std::sync::mpsc::sync_channel(0);
        let (release, release_receiver) = std::sync::mpsc::sync_channel(0);
        let observer = Arc::clone(&nested);
        let observation = std::thread::spawn(move || {
            observer.with_observation_gate(|| {
                entered.send(()).expect("test receiver remains available");
                release_receiver
                    .recv()
                    .expect("test sender releases the observation edge");
            });
        });
        entered_receiver
            .recv()
            .expect("observer enters the pre-admission edge");

        let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
        let adopting_root = Arc::clone(&root);
        let (adopted, adopted_receiver) = std::sync::mpsc::sync_channel(0);
        let adoption = std::thread::spawn(move || {
            adopting_root.set_admitted_children(vec![slot]);
            adopted.send(()).expect("test receiver remains available");
        });

        // The field, this test, and the active observation own three gate
        // references. A fourth proves adoption reached the old gate and is
        // blocked behind the complete observation edge rather than replacing
        // it concurrently.
        for _ in 0..100_000 {
            if Arc::strong_count(&prior_gate) >= 4 {
                break;
            }
            std::thread::yield_now();
        }
        assert!(
            Arc::strong_count(&prior_gate) >= 4,
            "adoption must synchronize through the prior observation gate"
        );
        assert!(matches!(
            adopted_receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));

        release
            .send(())
            .expect("active observation remains available");
        observation.join().expect("observation edge completes");
        adopted_receiver
            .recv()
            .expect("adoption reports completion after the edge");
        adoption.join().expect("gate handoff completes");
        assert!(Arc::ptr_eq(
            &root.observation_gate(),
            &nested.observation_gate()
        ));
    }

    #[test]
    fn quiet_signal_wait_cancellation_keeps_one_watch_registration() {
        let signal = Signal::default();
        let mut watcher = signal.watcher();
        assert_eq!(signal.watcher_count(), 1);

        for _ in 0..10_000 {
            let mut changed = Box::pin(watcher.changed());
            let mut context = Context::from_waker(Waker::noop());
            assert!(changed.as_mut().poll(&mut context).is_pending());
            drop(changed);
            assert_eq!(signal.watcher_count(), 1);
        }
    }

    #[test]
    fn stale_child_keys_cannot_target_reused_slots() {
        let mut tree = Tree::new();
        tree.add_task("worker", TaskDef::new(|_| future::pending()))
            .expect("valid task");
        let mut plan = lower_tree_for_test(tree);
        let child =
            ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &plan.root);
        let mut arena = ChildArena::default();
        let stale = arena.insert(child);
        let child = arena.remove(stale).expect("live key removes its child");
        let current = arena.insert(child);

        assert_eq!(stale.index, current.index, "the vacant slot is reused");
        assert_ne!(stale.generation, current.generation);
        assert!(arena.get(stale).is_none());
        assert!(arena.remove(stale).is_none());
        assert!(arena.get(current).is_some());
    }

    #[test]
    fn exhausted_child_generation_retires_the_slot() {
        let mut tree = Tree::new();
        tree.add_task("worker", TaskDef::new(|_| future::pending()))
            .expect("valid task");
        let mut plan = lower_tree_for_test(tree);
        let child =
            ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &plan.root);
        let mut arena = ChildArena::default();
        let original = arena.insert(child);
        arena.slots[original.index].generation = u64::MAX;
        let exhausted = super::ChildKey {
            index: original.index,
            generation: u64::MAX,
        };

        let child = arena
            .remove(exhausted)
            .expect("the forced exhausted generation is live");
        let current = arena.insert(child);
        assert_ne!(exhausted.index, current.index);
        assert!(arena.get(exhausted).is_none());
        assert!(arena.get(current).is_some());
    }

    #[crate::runtime::test]
    async fn dynamic_high_cycle_add_remove_reuses_runtime_storage() {
        const CYCLES: usize = 1_000;

        let system = DynamicTree::new().spawn().expect("runtime is available");
        system.wait_started().await.expect("dynamic root starts");
        let scope = system.scope();
        let cell = Arc::clone(&scope.as_scope().cell);

        for cycle in 0..CYCLES {
            let task = scope
                .add_task(
                    "worker",
                    TaskDef::new(|_| future::pending())
                        .readiness(Readiness::Manual)
                        .expect("manual readiness is valid")
                        .readiness_deadline(
                            ReadinessDeadline::bounded(Duration::from_secs(60 * 60))
                                .expect("non-zero readiness deadline"),
                        )
                        .shutdown(crate::Shutdown::Abort),
                )
                .await
                .expect("task admission")
                .into_handles();
            assert_eq!(
                cell.runtime_storage(),
                RuntimeStorage {
                    children: 1,
                    child_slots: 1,
                    deadlines: 1,
                    deadline_slots: 1,
                },
                "cycle {cycle} must reuse the sole runtime slot"
            );

            assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
            assert_eq!(
                cell.runtime_storage(),
                RuntimeStorage {
                    children: 0,
                    child_slots: 1,
                    deadlines: 0,
                    deadline_slots: 0,
                },
                "cycle {cycle} must reclaim the removed child"
            );

            let automatic = scope
                .add_task(
                    "worker",
                    TaskDef::new(|_| async { Ok(()) }).retention(Retention::Remove),
                )
                .await
                .expect("auto-removing task admission")
                .into_handles();
            automatic.wait().await;
            assert_eq!(
                cell.runtime_storage(),
                RuntimeStorage {
                    children: 0,
                    child_slots: 1,
                    deadlines: 0,
                    deadline_slots: 0,
                },
                "cycle {cycle} must reclaim Retention::Remove registration"
            );
        }

        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("empty dynamic scope shuts down");
    }

    #[test]
    fn owned_report_token_consumes_or_falls_back_once() {
        let shutdown = Latch::default();
        let (token, receiver) = report_channel(shutdown.clone(), None);
        token.record(RecordedOutcome::Returned(Ok(())));
        shutdown.fire();
        let report = receiver.receive();
        assert!(matches!(
            report.outcome,
            Some(RecordedOutcome::Returned(Ok(())))
        ));
        assert!(!report.cancelled);

        let shutdown = Latch::default();
        let (token, receiver) = report_channel(shutdown.clone(), None);
        shutdown.fire();
        drop(token);
        let report = receiver.receive();
        assert!(report.outcome.is_none());
        assert!(report.cancelled);
    }

    #[test]
    fn owned_report_token_records_prior_cancellation() {
        let shutdown = Latch::default();
        let (token, receiver) = report_channel(shutdown.clone(), None);
        shutdown.fire();
        token.record(RecordedOutcome::Returned(Ok(())));
        let report = receiver.receive();
        assert!(matches!(
            report.outcome,
            Some(RecordedOutcome::Returned(Ok(())))
        ));
        assert!(report.cancelled);
    }

    #[test]
    fn owned_report_token_records_prior_local_stop() {
        let shutdown = Latch::default();
        let local_stop = Latch::default();
        let (token, receiver) = report_channel(shutdown, Some(local_stop.clone()));
        local_stop.fire();
        token.record(RecordedOutcome::Returned(Ok(())));
        assert!(receiver.receive().cancelled);
    }

    #[test]
    fn handle_identity_is_stable_across_membership_rebase() {
        fn hashed(value: &impl std::hash::Hash) -> u64 {
            use std::hash::Hasher;
            let mut hasher = std::hash::DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        }

        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox: Arc<MailboxCell<u8>> = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), mailbox);
        let peer = actor.clone();
        let task = crate::TaskRef::new(Arc::clone(&member));
        let declared = actor.membership();
        let actor_hash = hashed(&actor);
        let task_hash = hashed(&task);

        member.rebase_membership(
            identity
                .mint_membership(&id)
                .expect("successor membership available"),
        );

        assert!(actor.membership().supersedes(declared));
        assert_eq!(actor, peer);
        assert_eq!(hashed(&actor), actor_hash);
        assert_eq!(hashed(&task), task_hash);
    }

    #[crate::runtime::test]
    async fn attaching_after_terminality_closes_the_mailbox() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        let mut parked = Box::pin(actor.send(1));
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(parked.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());

        member.terminalize(Exit::never_started());
        member.attach_mailbox(mailbox);

        let parked = match crate::runtime::timeout(Duration::from_secs(1), parked).await {
            crate::runtime::Timeout::Completed(result) => {
                result.expect_err("parked send is terminated")
            }
            crate::runtime::Timeout::Elapsed => panic!("parked send must not remain pending"),
        };
        assert_eq!(parked.kind, SendErrorKind::Terminated);
        let immediate = actor.try_send(2).expect_err("terminal send is rejected");
        assert_eq!(immediate.kind, SendErrorKind::Terminated);
    }

    #[crate::runtime::test]
    async fn task_aborted_scope_driver_resolves_startup() {
        let mut tree = Tree::new();
        tree.add_task(
            "not-ready",
            TaskDef::new(|_| future::pending())
                .readiness(Readiness::Manual)
                .expect("manual readiness is valid"),
        )
        .expect("valid task");
        let plan = lower_tree_for_test(tree);
        let scope = Arc::clone(&plan.root);
        let epoch = scope.begin_incarnation();
        let driver = crate::runtime::spawn(
            (),
            run_scope_incarnation(plan, false, Some(Latch::default()), None, None, None, epoch),
        );
        let abort = driver.abort_handle();
        let reached_startup = crate::runtime::timeout(Duration::from_secs(1), async {
            while !matches!(scope.record().state, ScopeState::Starting) {
                crate::runtime::yield_now().await;
            }
        })
        .await;
        assert!(matches!(
            reached_startup,
            crate::runtime::Timeout::Completed(())
        ));
        let waiter_scope = Arc::clone(&scope);
        let mut waiter = Box::pin(waiter_scope.wait_started());
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());

        abort.abort();
        assert!(matches!(
            crate::runtime::join(driver).await,
            crate::runtime::JoinOutcome::Cancelled
        ));
        let result = crate::runtime::timeout(Duration::from_secs(1), waiter).await;
        assert!(matches!(
            result,
            crate::runtime::Timeout::Completed(Err(crate::StartupError::ShutdownRequested))
        ));
        assert!(matches!(scope.record().state, ScopeState::Stopped { .. }));
        assert!(scope.record().startup.is_some());
    }

    #[crate::runtime::test]
    async fn dropped_unpolled_scope_plan_terminalizes_its_root() {
        let plan = lower_tree_for_test(Tree::new());
        let root = Arc::clone(&plan.root);

        drop(plan);

        assert_eq!(
            root.wait_started().await,
            Err(crate::StartupError::ShutdownRequested)
        );
        assert_eq!(root.wait_stopped().await, StopReason::NeverStarted);
    }

    #[crate::runtime::test]
    async fn dropped_unpolled_scope_plan_terminalizes_nested_declarations() {
        let mut inner = Tree::new();
        let leaf = inner
            .add_task("leaf", TaskDef::new(|_| future::pending()))
            .expect("valid nested task");
        let mut outer = Tree::new();
        let nested = outer
            .add_subtree_once("nested", SubtreeOnceDef::new(inner))
            .expect("valid nested scope");
        let mut snapshots = nested.subscribe_snapshots();
        let mut events = nested.subscribe_lifecycle();
        let plan = lower_tree_for_test(outer);

        drop(plan);

        assert_eq!(nested.wait_stopped().await, StopReason::NeverStarted);
        snapshots
            .changed()
            .await
            .expect("the final nested snapshot is delivered before closure");
        assert!(matches!(
            snapshots.borrow_latest().state,
            ScopeState::Stopped {
                reason: StopReason::NeverStarted
            }
        ));
        assert!(snapshots.changed().await.is_err());
        assert!(matches!(leaf.wait().await.kind(), ExitKind::NeverStarted));
        let mut saw_stopped = false;
        while let Some(item) = events.recv().await {
            saw_stopped |= matches!(
                item,
                LifecycleItem::Event(crate::LifecycleEvent {
                    kind: LifecycleEventKind::ScopeState {
                        state: ScopeState::Stopped {
                            reason: StopReason::NeverStarted
                        }
                    },
                    ..
                })
            );
        }
        assert!(
            saw_stopped,
            "nested observation closes after its final event"
        );
    }

    #[crate::runtime::test]
    async fn scope_plan_conversion_panic_terminalizes_every_child() {
        let mut tree = Tree::new();
        for id in ["first", "second"] {
            tree.add_task(id, TaskDef::new(|_| future::pending()))
                .expect("valid task");
        }
        let plan = lower_tree_for_test(tree);
        let root = Arc::clone(&plan.root);
        let mut events = root.subscribe_lifecycle();
        let children: Vec<_> = plan
            .children
            .iter()
            .map(|child| Arc::clone(&child.slot.member))
            .collect();
        let epoch = root.begin_incarnation();

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _identity = root
                    .child_identity
                    .lock()
                    .expect("scope identity mutex starts healthy");
                panic!("inject child conversion failure");
            }))
            .is_err()
        );

        let mut driver = Box::pin(run_scope_incarnation(
            plan, false, None, None, None, None, epoch,
        ));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let mut context = Context::from_waker(Waker::noop());
                let _ = driver.as_mut().poll(&mut context);
            }))
            .is_err()
        );
        drop(driver);

        for child in children {
            assert!(
                matches!(child.record().stage, MemberStage::Terminal(_)),
                "every transferred or pending child must terminalize"
            );
        }
        assert_eq!(
            root.wait_started().await,
            Err(crate::StartupError::ShutdownRequested)
        );
        assert_eq!(root.wait_stopped().await, StopReason::NeverStarted);
        assert!(
            root.snapshot().children.is_empty(),
            "the fallback must release every admitted residency"
        );
        let mut removed = 0;
        while let Some(item) = events.recv().await {
            if matches!(
                item,
                LifecycleItem::Event(crate::LifecycleEvent {
                    kind: LifecycleEventKind::Removed { .. },
                    ..
                })
            ) {
                removed += 1;
            }
        }
        assert_eq!(removed, 2, "every Added edge needs a matching Removed edge");
    }

    struct TrySendOnWake {
        actor: ActorRef<u8>,
        observed: Mutex<Option<SendErrorKind>>,
    }

    impl TrySendOnWake {
        fn observe(&self) {
            let error = self
                .actor
                .try_send(1)
                .expect_err("a terminality-derived wake observes a closed mailbox");
            *self.observed.lock().expect("observation mutex poisoned") = Some(error.kind);
        }
    }

    impl Wake for TrySendOnWake {
        fn wake(self: Arc<Self>) {
            self.observe();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.observe();
        }
    }

    struct ObserveMemberOnMailboxWake {
        member: Arc<MemberCell>,
        competing_exit: Exit,
        observed: Mutex<Option<(MemberStage, MemberStage)>>,
    }

    impl ObserveMemberOnMailboxWake {
        fn observe(&self) {
            let before = self.member.record().stage;
            self.member.terminalize(self.competing_exit.clone());
            let after = self.member.record().stage;
            *self.observed.lock().expect("observation mutex poisoned") = Some((before, after));
        }
    }

    impl Wake for ObserveMemberOnMailboxWake {
        fn wake(self: Arc<Self>) {
            self.observe();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.observe();
        }
    }

    struct ObserveScopeOnStartupWake {
        scope: Arc<ScopeCell>,
        epoch: Option<u64>,
        observed: Mutex<Option<(MemberStage, Option<bool>)>>,
    }

    impl ObserveScopeOnStartupWake {
        fn observe(&self) {
            let member = self.scope.member.record().stage;
            let incarnation_finished = self
                .epoch
                .map(|epoch| self.scope.incarnation_finished(epoch));
            *self.observed.lock().expect("observation mutex poisoned") =
                Some((member, incarnation_finished));
        }
    }

    impl Wake for ObserveScopeOnStartupWake {
        fn wake(self: Arc<Self>) {
            self.observe();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.observe();
        }
    }

    #[test]
    fn terminality_signal_follows_mailbox_termination() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        member.attach_mailbox(mailbox);

        let probe = Arc::new(TrySendOnWake {
            actor,
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut context = Context::from_waker(&waker);
        let mut watcher = member.record.watcher();
        let mut changed = Box::pin(watcher.changed());
        assert!(changed.as_mut().poll(&mut context).is_pending());

        member.terminalize(Exit::never_started());

        assert_eq!(
            *probe.observed.lock().expect("observation mutex poisoned"),
            Some(SendErrorKind::Terminated)
        );
        assert!(changed.as_mut().poll(&mut context).is_ready());
    }

    #[test]
    fn mailbox_wake_observes_terminal_record_and_reentrant_terminality_is_idempotent() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        member.attach_mailbox(mailbox);
        let first_exit = Exit::never_started();
        let probe = Arc::new(ObserveMemberOnMailboxWake {
            member: Arc::clone(&member),
            competing_exit: Exit::new(ExitKind::Completed, false),
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut parked = Box::pin(actor.send(1));
        assert!(
            parked
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        member.terminalize(first_exit.clone());

        assert_eq!(
            *probe.observed.lock().expect("observation mutex poisoned"),
            Some((
                MemberStage::Terminal(first_exit.clone()),
                MemberStage::Terminal(first_exit.clone())
            ))
        );
        assert!(matches!(
            member.record().stage,
            MemberStage::Terminal(exit) if exit == first_exit
        ));
    }

    #[test]
    fn attach_during_terminal_publication_finishes_record_before_mailbox_wake() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        let first_exit = Exit::never_started();
        member
            .mailbox
            .lock()
            .expect("member mailbox mutex poisoned")
            .terminal = Some(first_exit.clone());
        let probe = Arc::new(ObserveMemberOnMailboxWake {
            member: Arc::clone(&member),
            competing_exit: Exit::new(ExitKind::Completed, false),
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut parked = Box::pin(actor.send(1));
        assert!(
            parked
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        member.attach_mailbox(mailbox);

        assert_eq!(
            *probe.observed.lock().expect("observation mutex poisoned"),
            Some((
                MemberStage::Terminal(first_exit.clone()),
                MemberStage::Terminal(first_exit.clone())
            ))
        );
        assert!(matches!(
            member.record().stage,
            MemberStage::Terminal(exit) if exit == first_exit
        ));
    }

    #[test]
    fn concurrent_terminalizers_return_after_one_consistent_record_is_visible() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let start = Arc::new(Barrier::new(3));
        let workers = [Exit::never_started(), Exit::new(ExitKind::Completed, false)]
            .into_iter()
            .map(|exit| {
                let member = Arc::clone(&member);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    member.terminalize(exit);
                    assert!(matches!(member.record().stage, MemberStage::Terminal(_)));
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for worker in workers {
            worker.join().expect("terminalizer thread succeeds");
        }

        let record = member.record();
        let MemberStage::Terminal(exit) = record.stage else {
            panic!("one terminal record must be visible");
        };
        assert_eq!(record.last_exit, Some(exit));
    }

    #[test]
    fn terminal_startup_wake_follows_member_and_incarnation_publication() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("root");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let scope = ScopeCell::new(
            member,
            ScopeFlavor::Ordered,
            ScopeIdentity::new().expect("child identity available"),
        );
        let epoch = scope.begin_incarnation();
        let probe = Arc::new(ObserveScopeOnStartupWake {
            scope: Arc::clone(&scope),
            epoch: Some(epoch),
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut watcher = scope.record.watcher();
        let mut changed = Box::pin(watcher.changed());
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        scope.finish_root_incarnation(epoch, StopReason::ShutdownRequested, Exit::never_started());

        let observed = probe
            .observed
            .lock()
            .expect("observation mutex poisoned")
            .clone()
            .expect("terminal startup wakes the scope watcher");
        assert!(matches!(observed.0, MemberStage::Terminal(_)));
        assert_eq!(observed.1, Some(true));
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[test]
    fn no_live_root_startup_wake_follows_member_publication() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("root");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let scope = ScopeCell::new(
            member,
            ScopeFlavor::Ordered,
            ScopeIdentity::new().expect("child identity available"),
        );
        let probe = Arc::new(ObserveScopeOnStartupWake {
            scope: Arc::clone(&scope),
            epoch: None,
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut watcher = scope.record.watcher();
        let mut changed = Box::pin(watcher.changed());
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        scope.finish_live_root_incarnation(StopReason::ShutdownRequested, Exit::never_started());

        let observed = probe
            .observed
            .lock()
            .expect("observation mutex poisoned")
            .clone()
            .expect("terminal startup wakes the scope watcher");
        assert!(matches!(observed.0, MemberStage::Terminal(_)));
        assert_eq!(observed.1, None);
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

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

    #[test]
    fn dynamic_close_holds_removal_completion_through_observation_cleanup() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let root_id = ChildId::from("root");
        let root_member = MemberCell::new(
            root_id.clone(),
            identity
                .mint_membership(&root_id)
                .expect("root membership available"),
        );
        let root = ScopeCell::new(
            root_member,
            ScopeFlavor::Dynamic,
            ScopeIdentity::new().expect("child identity available"),
        );
        let child_id = ChildId::from("worker");
        let member = MemberCell::new(
            child_id.clone(),
            root.child_identity
                .lock()
                .expect("scope identity mutex poisoned")
                .mint_membership(&child_id)
                .expect("child membership available"),
        );
        let slot = SlotCell::new(Arc::clone(&member), None);
        root.set_admitted_children(vec![Arc::clone(&slot)]);
        let (events, _receiver) = crate::runtime::bounded_mpsc(1);
        let control = DynamicControl::new(&root, events);
        let (sender, mut response) = crate::runtime::oneshot();
        let mut responses = RemovalResponses::default();
        responses.0.push(sender);
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .insert(
                ChildId::from("worker"),
                DynamicEntry {
                    slot,
                    admitted: true,
                    fused_cancel: None,
                    removal: Obligation::new(responses, complete_removals),
                    removal_started: true,
                },
            );

        let entries = control.close();
        assert!(
            response.try_receive().is_none(),
            "closing admission must not complete removal before teardown"
        );
        member.terminalize(Exit::never_started());
        assert!(root.prune_child(&member));
        assert!(
            response.try_receive().is_none(),
            "terminality and Removed precede removal completion"
        );
        drop(entries);
        assert_eq!(response.try_receive(), Some(RemoveOutcome::Removed));
    }

    #[crate::runtime::test]
    async fn system_shutdown_joins_root_driver_teardown() {
        let system = DynamicTree::new().spawn().expect("runtime is available");
        let root = system.scope();
        system.wait_started().await.expect("dynamic root starts");
        let control = root
            .as_scope()
            .cell
            .dynamic()
            .expect("running dynamic root has a control");
        let weak = Arc::downgrade(&control);
        drop(control);

        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("empty dynamic root shuts down");

        assert!(
            weak.upgrade().is_none(),
            "shutdown returns only after root driver teardown drops dynamic state"
        );
    }

    #[test]
    fn incarnation_exhaustion_terminalizes_the_membership_as_never_restart() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let membership = identity.mint_membership(&id).expect("membership available");
        let member = MemberCell::new(id, membership);
        let previous = Exit::new(
            ExitKind::Failed(ExitError::message("last completed incarnation")),
            false,
        );
        member.update(|record| {
            record.stage = MemberStage::Restarting;
            record.last_exit = Some(previous.clone());
        });
        let slot = SlotCell::new(Arc::clone(&member), None);
        let mut counter = FenceCounter::near_exhaustion(71);

        assert!(mint_child_incarnation(&slot, &mut counter).is_some());
        assert!(mint_child_incarnation(&slot, &mut counter).is_none());
        assert!(matches!(
            member.record().stage,
            MemberStage::Terminal(ref exit) if exit == &previous
        ));
        assert!(mint_child_incarnation(&slot, &mut counter).is_none());
    }

    #[crate::runtime::test]
    async fn nested_membership_exhaustion_is_structured_and_fail_closed() {
        let nested_id = ChildId::from("nested");
        let mut parent_identity = ScopeIdentity::new().expect("parent identity available");
        let nested_membership = parent_identity
            .mint_membership(&nested_id)
            .expect("nested membership available");
        let nested_member = MemberCell::new(nested_id, nested_membership);

        let worker_id = ChildId::from("worker");
        let mut child_identity =
            ScopeIdentity::with_counter(worker_id.clone(), FenceCounter::near_exhaustion(7));
        child_identity
            .mint_membership(&worker_id)
            .expect("last usable membership is minted before the rebuild");
        let scope = ScopeCell::new(nested_member, ScopeFlavor::Ordered, child_identity);

        let mut tree = Tree::new();
        let worker = tree
            .add_task(
                worker_id.clone(),
                TaskDef::new(|_| future::pending::<crate::ExitResult>()),
            )
            .expect("provisional declaration succeeds");
        let ready = Latch::default();
        let error = run_nested_tree(
            into_core_for_test(tree),
            Arc::clone(&scope),
            crate::policy::ResolvedDefaults::default(),
            ready.clone(),
            Latch::default(),
            Latch::default(),
            Latch::default(),
        )
        .await
        .expect_err("the stable child-id domain is exhausted");

        let failure = error
            .startup_failure()
            .expect("framework provenance is retained");
        assert!(matches!(
            failure.cause,
            StartupFailureCause::IdentityExhausted { ref id } if id == &worker_id
        ));
        assert!(matches!(
            scope.record().startup,
            Some(Err(StartupError::StartupFailed(ref failure)))
                if matches!(failure.cause, StartupFailureCause::IdentityExhausted { ref id } if id == &worker_id)
        ));
        assert!(matches!(
            scope.record().state,
            ScopeState::Stopped {
                reason: StopReason::StartupFailed(_)
            }
        ));
        assert!(!ready.is_fired());
        assert!(matches!(worker.wait().await.kind(), ExitKind::NeverStarted));
    }

    #[crate::runtime::test]
    async fn scope_incarnation_exhaustion_closes_nested_observation() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("nested");
        let membership = identity.mint_membership(&id).expect("membership available");
        let member = MemberCell::new(id, membership);
        let scope = ScopeCell::new(
            Arc::clone(&member),
            ScopeFlavor::Ordered,
            ScopeIdentity::new().expect("child identity available"),
        );
        let slot = SlotCell::new(Arc::clone(&member), Some(Arc::clone(&scope)));
        let mut snapshots = scope.subscribe_snapshots();
        let mut events = scope.subscribe_lifecycle();
        let mut counter = FenceCounter::near_exhaustion(83);
        let first = mint_child_incarnation(&slot, &mut counter).expect("last incarnation mints");
        member.update(|record| {
            record.stage = MemberStage::Restarting;
            record.last_incarnation = Some(first);
            record.last_exit = Some(Exit::new(ExitKind::Completed, false));
        });
        scope.set_state(ScopeState::Stopped {
            reason: StopReason::Finished,
        });

        assert!(mint_child_incarnation(&slot, &mut counter).is_none());
        assert!(matches!(
            events.try_recv(),
            Ok(LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::Stopped {
                        reason: StopReason::Finished
                    }
                },
                ..
            }))
        ));
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
        snapshots
            .changed()
            .await
            .expect("the prior incarnation's final snapshot remains observable");
        assert!(matches!(
            snapshots.borrow_latest().state,
            ScopeState::Stopped {
                reason: StopReason::Finished
            }
        ));
        assert!(snapshots.changed().await.is_err());
    }

    #[test]
    fn lifecycle_sequence_exhaustion_poison_is_never_minted_and_becomes_lag() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("scope");
        let membership = identity.mint_membership(&id).expect("membership available");
        let member = MemberCell::new(id, membership);
        let scope = ScopeCell::new(
            member,
            ScopeFlavor::Ordered,
            ScopeIdentity::new().expect("child identity available"),
        );
        *scope
            .lifecycle_sequence
            .lock()
            .expect("lifecycle sequence mutex poisoned") = FenceCounter::near_exhaustion(0);
        let mut events = scope.subscribe_lifecycle();

        scope.emit(LifecycleEventKind::ScopeState {
            state: ScopeState::Starting,
        });
        scope.emit(LifecycleEventKind::ScopeState {
            state: ScopeState::Running,
        });
        scope.emit(LifecycleEventKind::ScopeState {
            state: ScopeState::Draining,
        });

        assert_eq!(events.try_recv(), Ok(LifecycleItem::Lagged { dropped: 2 }));
        let LifecycleItem::Event(event) = events.try_recv().expect("last mintable event remains")
        else {
            panic!("expected the final mintable event");
        };
        assert_eq!(event.seq, u64::MAX - 1);
        assert_eq!(scope.snapshot().lifecycle_seq, u64::MAX);
    }
}
