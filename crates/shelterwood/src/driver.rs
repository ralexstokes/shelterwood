//! Mutable runtime shell and shared handle state.

mod admission_control;

use std::{
    collections::BTreeMap,
    ops::{Bound, Index, IndexMut},
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use crate::{
    Cancellation, ChildId, Exit, ExitKind, GracePhase, Incarnation, IntensityTrip, JitterSample,
    Membership, Readiness, ReadinessDeadline, ScopeState, ShutdownStraggler, ShutdownTimeout,
    StartupFailure, StartupFailureCause,
    admission::{NotAdmittingCause, ReserveError},
    cells::{MailboxControl, MemberStage, ResidentProjection, ScopeCell, StartupDisposition},
    deadline::Deadline,
    engine::{
        ArbitrationClass, ChildCompletionState, DeadlineHandle, DeadlineQueue, Epoch, ExitDispatch,
        IncarnationRun, IntensityState, MembershipMode, ReadinessEffect, ReadinessEvent,
        ReadinessGate, RestartState, ScopeLifecycle, ScopeMode, StopAction, StopLadder, arbitrate,
        dispatch_exit, schedule_restart,
    },
    exit::{
        JoinVerdict, RecordedOutcome, StartupError, StopReason, classify_exit,
        reconcile_recorded_outcomes,
    },
    identity::IncarnationCounter,
    observe::LifecycleEventKind,
    plan::{
        BuilderCore, ChildConstruction, ChildPlan, LowerError, ScopeFactory, ScopePlan, SlotCell,
    },
    policy::{DefaultsInheritance, ResolvedDefaults, ScopeFlavor},
    raw::{CatchUnwindFuture, RawRunContext, RawSpawn},
    runtime::{self, CompletionGatedLatch, Latch},
    task::{TaskContext, TaskContextLatches, TaskFactory},
};

use admission_control::{
    AdmissionRequest, DynamicControl, DynamicEntry, cancel_dynamic_reservation_parts,
    reject_admission_after_disposal,
};
pub(crate) use admission_control::{
    DynamicReservation, RemovalResponse, cancel_dynamic_reservation, remove_dynamic,
    reserve_dynamic, signal_fused_cancel, start_admission,
};

#[cfg(test)]
use crate::cells::{GateCapture, MemberCell, RuntimeStorage};
#[cfg(test)]
use admission_control::RemovalResponses;

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

struct RecordedReport {
    outcome: Option<RecordedOutcome>,
    cancellation: Cancellation,
    readiness_signal_seen: bool,
}

struct ReportCompletion {
    sender: mpsc::Sender<RecordedReport>,
    shutdown: Latch,
    local_stop: Option<Latch>,
    readiness: CompletionGatedLatch,
}

pub(crate) struct ReportToken {
    completion: Obligation<ReportCompletion>,
}

pub(crate) struct ReportReceiver(mpsc::Receiver<RecordedReport>);

/// Couples the child task's outcome report to its join verdict without an
/// asynchronous handoff race.
///
/// `ReportToken` is owned by the child task and its fail-closed `Drop` sends a
/// fallback synchronously. The runtime resolves `runtime::join` only after the
/// spawned future has been destroyed (a tokio `JoinHandle` guarantee, not a
/// language one — any replacement executor behind `runtime` must preserve it),
/// so the exit joiner may require an immediately available report: one has
/// already been sent on every return, panic, and cancellation edge. The
/// shutdown/local-stop and readiness latches are sampled by that same send,
/// making the report and its completion-boundary evidence one ordered
/// observation.
pub(crate) fn report_channel(
    shutdown: Latch,
    local_stop: Option<Latch>,
    readiness: CompletionGatedLatch,
) -> (ReportToken, ReportReceiver) {
    let (sender, receiver) = mpsc::channel();
    (
        ReportToken {
            completion: Obligation::new(
                ReportCompletion {
                    sender,
                    shutdown,
                    local_stop,
                    readiness,
                },
                |completion| completion.send(None),
            ),
        },
        ReportReceiver(receiver),
    )
}

impl ReportCompletion {
    fn send(self, outcome: Option<RecordedOutcome>) {
        let cancellation =
            if self.shutdown.is_fired() || self.local_stop.as_ref().is_some_and(Latch::is_fired) {
                Cancellation::Observed
            } else {
                Cancellation::NotObserved
            };
        let readiness_signal_seen = self.readiness.complete();
        let report = RecordedReport {
            outcome,
            cancellation,
            readiness_signal_seen,
        };
        let _ = self.sender.send(report);
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
            .try_recv()
            .expect("owned report token must record or fall back before its task joins")
    }
}

pub(crate) struct SystemRun {
    pub(crate) root: Arc<ScopeCell>,
    driver: Option<runtime::JoinHandle<StopReason>>,
}

fn resident_projection(slot: &SlotCell) -> ResidentProjection {
    ResidentProjection::new(Arc::clone(&slot.member), slot.scope.clone())
}

enum DriverEvent {
    Child(ChildEvent),
    Admission(AdmissionRequest),
    Removal(RemovalRequest),
}

#[derive(Clone, Copy)]
struct RemovalRequest {
    membership: Membership,
    key: ChildKey,
}

impl SystemRun {
    pub(crate) fn request_shutdown(&self) {
        let _ = self.root.request_shutdown();
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
                let exit = Exit::new(ExitKind::Panicked { message }, Cancellation::NotObserved);
                self.root
                    .finish_live_root_incarnation(StopReason::ShutdownRequested, exit);
            }
            runtime::JoinOutcome::Cancelled => {
                self.root.finish_live_root_incarnation(
                    StopReason::ShutdownRequested,
                    Exit::new(
                        ExitKind::Aborted {
                            phase: GracePhase::WithinGrace,
                        },
                        Cancellation::Observed,
                    ),
                );
            }
        }
    }
}

fn collect_stragglers(scope: &ScopeCell, prefix: &[ChildId], out: &mut Vec<ShutdownStraggler>) {
    let children = scope.resident_projections();
    for child in children {
        if matches!(child.member.record().stage, MemberStage::Terminal(_))
            || child.member.terminal_disposal_pending()
        {
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

async fn wait_for_incarnation(scope: &ScopeCell, epoch: Epoch) {
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
    let Some(epoch) = scope.request_shutdown() else {
        // An exhausted idle scope has no incarnation that can remain live.
        return Ok(());
    };
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
            if stragglers.is_empty() {
                Ok(())
            } else {
                Err(ShutdownTimeout { stragglers })
            }
        }
    }
}

pub(crate) fn spawn_system(plan: ScopePlan) -> SystemRun {
    let root = Arc::clone(&plan.root);
    let monitor_root = Arc::clone(&root);
    let driver = runtime::spawn(async move { run_scope(plan, ScopeRole::Root).await });
    let lifecycle = runtime::spawn(async move {
        match runtime::join(driver).await {
            runtime::JoinOutcome::Ok { value, .. } => value,
            runtime::JoinOutcome::Panic { message, .. } => {
                let exit = Exit::new(ExitKind::Panicked { message }, Cancellation::NotObserved);
                monitor_root.finish_live_root_incarnation(StopReason::ShutdownRequested, exit);
                StopReason::ShutdownRequested
            }
            runtime::JoinOutcome::Cancelled => {
                monitor_root.finish_live_root_incarnation(
                    StopReason::ShutdownRequested,
                    Exit::new(
                        ExitKind::Aborted {
                            phase: GracePhase::WithinGrace,
                        },
                        Cancellation::Observed,
                    ),
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
        cancellation: Cancellation,
        readiness_signal_seen: bool,
    },
    ConstructionDisposed {
        child: ChildKey,
        panic: Option<runtime::DisposalPanic>,
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
    hard_abort_phase: Option<GracePhase>,
    readiness: ReadinessGate,
    readiness_deadline: Option<DeadlineHandle>,
    ready_signal: CompletionGatedLatch,
    construction_release: Latch,
    framework_abort: Option<Latch>,
    framework_abort_ack: Option<Latch>,
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
            Exit::new(
                ExitKind::Aborted {
                    phase: GracePhase::WithinGrace,
                },
                Cancellation::Observed,
            ),
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
    completion.root.terminalize_child(
        &completion.slot.member,
        exit,
        exited_incarnation,
        StartupDisposition::NotAborted,
    );
}

struct ChildRuntime {
    slot: Arc<SlotCell>,
    mailbox: Option<Arc<dyn MailboxControl>>,
    terminality: Obligation<ChildTerminality>,
    construction: runtime::Isolated<ChildConstruction>,
    pending_terminal: Option<PendingTerminal>,
    options: crate::policy::ResolvedCommonOptions,
    incarnations: IncarnationCounter,
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
    startup: StartupDisposition,
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
        let mailbox = slot.member.mailbox();
        if let Some(mailbox) = &mailbox {
            mailbox.configure(options.mailbox);
        }
        Self {
            terminality,
            slot,
            mailbox,
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
        startup: StartupDisposition,
    ) -> bool {
        let terminalized = runtime::catch_panic(|| {
            root.terminalize_child(&self.slot.member, exit, exited_incarnation, startup)
        });
        if matches!(self.slot.member.record().stage, MemberStage::Terminal(_)) {
            self.terminality.complete(drop);
        }
        match terminalized {
            Ok(changed) => changed,
            Err(payload) => runtime::resume_panic(payload),
        }
    }

    fn complete_terminality(&mut self) {
        self.terminality.complete(drop);
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ChildKey(u64);

#[derive(Default)]
struct ChildArena {
    // Keys are insertion-order ids and are never reused. A late event can
    // therefore miss; it can never address a subsequently inserted child.
    children: BTreeMap<ChildKey, ChildRuntime>,
    // `u64::MAX` is poison and is never minted. Once exhausted, every later
    // insertion fails closed instead of wrapping into the live key domain.
    next_key: u64,
}

impl ChildArena {
    fn insert(&mut self, child: ChildRuntime) -> Result<ChildKey, Box<ChildRuntime>> {
        let Some(next) = self.next_key.checked_add(1) else {
            return Err(Box::new(child));
        };
        if next == u64::MAX {
            self.next_key = u64::MAX;
            return Err(Box::new(child));
        }
        self.next_key = next;
        let key = ChildKey(next);
        let replaced = self.children.insert(key, child);
        debug_assert!(replaced.is_none(), "monotonic child keys are never reused");
        Ok(key)
    }

    fn get(&self, key: ChildKey) -> Option<&ChildRuntime> {
        self.children.get(&key)
    }

    fn get_mut(&mut self, key: ChildKey) -> Option<&mut ChildRuntime> {
        self.children.get_mut(&key)
    }

    fn remove(&mut self, key: ChildKey) -> Option<ChildRuntime> {
        self.children.remove(&key)
    }

    fn keys(&self) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children.keys().copied()
    }

    fn keys_after(&self, key: ChildKey) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children
            .range((Bound::Excluded(key), Bound::Unbounded))
            .map(|(key, _)| *key)
    }

    fn previous_key(&self, key: ChildKey) -> Option<ChildKey> {
        self.children.range(..key).next_back().map(|(key, _)| *key)
    }

    fn values(&self) -> impl Iterator<Item = &ChildRuntime> {
        self.children.values()
    }

    fn values_mut(&mut self) -> impl Iterator<Item = &mut ChildRuntime> {
        self.children.values_mut()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.children.len()
    }

    fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    fn clear(&mut self) {
        self.children.clear();
    }

    #[cfg(test)]
    fn storage_len(&self) -> usize {
        self.children.len()
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

struct AncestorCommandLatches {
    shutdown: Latch,
    abort: Latch,
    abort_ack: Latch,
}

struct NestedScopeLatches {
    parent_ready: CompletionGatedLatch,
    ancestor: AncestorCommandLatches,
}

enum ScopeRole {
    Root,
    Nested(NestedScopeLatches),
}

impl ScopeRole {
    fn is_root(&self) -> bool {
        matches!(self, Self::Root)
    }

    fn parent_ready(&self) -> Option<&CompletionGatedLatch> {
        match self {
            Self::Root => None,
            Self::Nested(latches) => Some(&latches.parent_ready),
        }
    }

    fn ancestor(&self) -> Option<&AncestorCommandLatches> {
        match self {
            Self::Root => None,
            Self::Nested(latches) => Some(&latches.ancestor),
        }
    }
}

struct ScopeRuntime {
    root: Arc<ScopeCell>,
    defaults: ResolvedDefaults,
    intensity_policy: crate::Intensity,
    intensity: IntensityState,
    children: ChildArena,
    events: runtime::MpscSender<DriverEvent>,
    disposal_events: runtime::UnboundedMpscSender<DriverEvent>,
    deadlines: DeadlineQueue<DeadlineKind>,
    jitter: runtime::JitterRng,
    lifecycle: ScopeLifecycle,
    next_ordered_start: Option<ChildKey>,
    role: ScopeRole,
    dynamic: Option<Arc<DynamicControl>>,
    epoch: Epoch,
    ancestor_shutdown_seen: bool,
    ancestor_abort_seen: bool,
    hard_forced: bool,
    ordered_stop_progressing: bool,
    ordered_stop_cursor: Option<ChildKey>,
    ordered_stop_waiting: Option<ChildKey>,
    #[cfg(test)]
    ordered_stop_inspections: usize,
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
            self.root.set_dynamic_route(None);
            Some(entries)
        } else {
            None
        };
        for child in self.children.values_mut() {
            if let Some(active) = child.active.take() {
                if let Some(mailbox) = &child.mailbox {
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
                .or_else(|| self.lifecycle.draining_reason().cloned())
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
    TaskRestartable {
        factory: TaskFactory,
        context: TaskContext,
    },
    TaskOnce {
        body: Box<dyn FnOnce(TaskContext) -> crate::task::TaskFuture + Send + 'static>,
        context: TaskContext,
    },
    ScopeRestartable {
        factory: ScopeFactory,
        scope: Arc<ScopeCell>,
        inherited: ResolvedDefaults,
        latches: NestedScopeLatches,
    },
    ScopeOnce {
        tree: Box<BuilderCore>,
        scope: Arc<ScopeCell>,
        inherited: ResolvedDefaults,
        latches: NestedScopeLatches,
    },
}

struct SpawnDispatch {
    body: SpawnBody,
    declared_readiness: Option<Readiness>,
    construction_spent: bool,
    scope_child: bool,
}

/// Latches have deliberately separate ownership:
///
/// - `shutdown`/`abort` are the child-facing cooperative ladder;
/// - `framework_abort`/`framework_abort_ack` bound a nested scope driver's
///   recursive drain before its task is aborted;
/// - `construction_release` prevents a raw actor from running before the
///   driver has installed the readiness mode reported by construction;
/// - `ready` also carries the completion edge that makes readiness and
///   self-stop watcher tasks finite.
struct SpawnLatches {
    shutdown: Latch,
    abort: Latch,
    ready: CompletionGatedLatch,
    construction_release: Latch,
    local_stop: Latch,
    framework_abort: Latch,
    framework_abort_ack: Latch,
}

impl SpawnLatches {
    fn task_context(&self) -> TaskContextLatches {
        TaskContextLatches {
            shutdown: self.shutdown.clone(),
            abort: self.abort.clone(),
            ready: self.ready.clone(),
        }
    }

    fn nested_scope(&self) -> NestedScopeLatches {
        NestedScopeLatches {
            parent_ready: self.ready.clone(),
            ancestor: AncestorCommandLatches {
                shutdown: self.shutdown.clone(),
                abort: self.framework_abort.clone(),
                abort_ack: self.framework_abort_ack.clone(),
            },
        }
    }
}

struct ChildTaskLaunch {
    events: runtime::MpscSender<DriverEvent>,
    key: ChildKey,
    incarnation: Incarnation,
    body: SpawnBody,
    readiness_override: Option<Readiness>,
    watch_readiness: bool,
    shutdown: Latch,
    ready: CompletionGatedLatch,
    construction_release: Latch,
    local_stop: Latch,
}

fn dispatch_child_construction(
    child: &mut ChildRuntime,
    root: &Arc<ScopeCell>,
    defaults: &ResolvedDefaults,
    incarnation: Incarnation,
    latches: &SpawnLatches,
) -> SpawnDispatch {
    let id = child.slot.member.id().clone();
    let construction = child.construction.get_mut();
    match construction {
        ChildConstruction::Raw(definition) => {
            let construction_spent = definition.one_shot();
            SpawnDispatch {
                body: SpawnBody::Raw {
                    spawn: definition.take_spawn(),
                    context: RawRunContext {
                        id,
                        incarnation,
                        member: Arc::clone(&child.slot.member),
                        scope: crate::ScopeRef {
                            cell: Arc::clone(root),
                        },
                        shutdown: latches.shutdown.clone(),
                        abort: latches.abort.clone(),
                        ready: latches.ready.clone(),
                        local_stop: latches.local_stop.clone(),
                        mailbox_shutdown: child.options.mailbox_shutdown,
                    },
                },
                declared_readiness: None,
                construction_spent,
                scope_child: false,
            }
        }
        ChildConstruction::Task(definition) => SpawnDispatch {
            body: SpawnBody::TaskRestartable {
                factory: Arc::clone(&definition.factory),
                context: TaskContext::new(id, incarnation, latches.task_context()),
            },
            declared_readiness: Some(child.options.readiness),
            construction_spent: false,
            scope_child: false,
        },
        ChildConstruction::TaskOnce(definition) => SpawnDispatch {
            body: SpawnBody::TaskOnce {
                body: definition.take_body(),
                context: TaskContext::new(id, incarnation, latches.task_context()),
            },
            declared_readiness: Some(child.options.readiness),
            construction_spent: true,
            scope_child: false,
        },
        ChildConstruction::Scope(definition) => {
            let inherited = match definition.defaults {
                DefaultsInheritance::Inherit => defaults.clone(),
                DefaultsInheritance::Reset => ResolvedDefaults::default(),
            };
            let scope = Arc::clone(
                child
                    .slot
                    .scope
                    .as_ref()
                    .expect("scope construction needs a scope cell"),
            );
            let (body, construction_spent) = if let Some(factory) = definition.restartable() {
                (
                    SpawnBody::ScopeRestartable {
                        factory: Arc::clone(factory),
                        scope,
                        inherited,
                        latches: latches.nested_scope(),
                    },
                    false,
                )
            } else {
                (
                    SpawnBody::ScopeOnce {
                        tree: definition
                            .take_one_shot()
                            .expect("one-shot subtree construction invoked more than once"),
                        scope,
                        inherited,
                        latches: latches.nested_scope(),
                    },
                    true,
                )
            };
            SpawnDispatch {
                body,
                declared_readiness: Some(Readiness::Manual),
                construction_spent,
                scope_child: true,
            }
        }
    }
}

fn spawn_child_tasks(launch: ChildTaskLaunch) -> runtime::AbortHandle {
    let ChildTaskLaunch {
        events,
        key,
        incarnation,
        body,
        readiness_override,
        watch_readiness,
        shutdown,
        ready,
        construction_release,
        local_stop,
    } = launch;
    let (report, report_receiver) =
        report_channel(shutdown, Some(local_stop.clone()), ready.clone());
    let constructed_sender = events.clone();
    let handle = runtime::spawn(async move {
        let body = async move {
            match body {
                SpawnBody::Raw { spawn, context } => {
                    let instance = spawn.construct();
                    let readiness = readiness_override.unwrap_or_else(|| instance.readiness());
                    let _ = runtime::mpsc_send(
                        &constructed_sender,
                        DriverEvent::Child(ChildEvent::Constructed {
                            child: key,
                            incarnation,
                            readiness,
                        }),
                    )
                    .await;
                    construction_release.fired().await;
                    instance.run(context, readiness).await
                }
                SpawnBody::TaskRestartable { factory, context } => factory(context).await,
                SpawnBody::TaskOnce { body, context } => body(context).await,
                SpawnBody::ScopeRestartable {
                    factory,
                    scope,
                    inherited,
                    latches,
                } => run_nested_factory(factory, scope, inherited, latches).await,
                SpawnBody::ScopeOnce {
                    tree,
                    scope,
                    inherited,
                    latches,
                } => run_nested_tree(*tree, scope, inherited, latches).await,
            }
        };
        let outcome = CatchUnwindFuture::new(body).await;
        let result = match outcome {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        };
        report.record(RecordedOutcome::Returned(result));
    });
    let abort_handle = handle.abort_handle();

    let exit_sender = events.clone();
    runtime::spawn(async move {
        let join = match runtime::join(handle).await {
            runtime::JoinOutcome::Ok { .. } => JoinVerdict::Completed,
            runtime::JoinOutcome::Panic { message, .. } => JoinVerdict::Panicked { message },
            runtime::JoinOutcome::Cancelled => JoinVerdict::Cancelled {
                phase: GracePhase::WithinGrace,
            },
        };
        // The task owns `report`, whose explicit record or Drop fallback runs
        // before the join completes. `receive` therefore asserts immediate
        // post-join availability without ever blocking this runtime worker.
        let report = report_receiver.receive();
        let _ = runtime::mpsc_send(
            &exit_sender,
            DriverEvent::Child(ChildEvent::Exited {
                child: key,
                incarnation,
                recorded: report.outcome,
                join,
                cancellation: report.cancellation,
                readiness_signal_seen: report.readiness_signal_seen,
            }),
        )
        .await;
    });

    let completion = ready.clone();
    if watch_readiness {
        let ready_sender = events.clone();
        let ready_completion = ready.clone();
        runtime::spawn(async move {
            if matches!(
                runtime::select_two(ready.fired(), ready_completion.completed()).await,
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

    runtime::spawn(async move {
        if matches!(
            runtime::select_two(local_stop.fired(), completion.completed()).await,
            runtime::Either::Left(())
        ) {
            let _ = runtime::mpsc_send(
                &events,
                DriverEvent::Child(ChildEvent::SelfStop {
                    child: key,
                    incarnation,
                }),
            )
            .await;
        }
    });

    abort_handle
}

impl ScopeRuntime {
    fn dynamic_membership_is_removing(&self, key: ChildKey) -> bool {
        let Some(child) = self.children.get(key) else {
            return false;
        };
        self.dynamic.as_ref().is_some_and(|control| {
            control
                .state
                .lock()
                .expect("dynamic-state mutex poisoned")
                .entries
                .get(child.slot.member.id())
                .filter(|entry| entry.slot.member.membership() == child.slot.member.membership())
                .is_some_and(|entry| entry.is_removing() && entry.matches_key(key))
        })
    }

    fn publish_dynamic_removal(&self, key: ChildKey) {
        let Some(member) = self
            .children
            .get(key)
            .map(|child| Arc::clone(&child.slot.member))
        else {
            return;
        };
        if !member.record().removing {
            self.root
                .transition_child(&member, |record| record.removing = true, None);
        }
    }

    /// Reports whether a *removal* source has latched for this membership:
    /// the dynamic entry's authoritative `Removing` control-plane state or a
    /// fired fused-cancel latch on its `Resident` state. Scope-level stop
    /// sources (drain, force, latched shutdown requests, ancestor latches)
    /// are deliberately excluded: each of those has a guaranteed follow-up
    /// event that owns the scope verdict, so exit dispatch must not
    /// reclassify the membership as `Removing` on their behalf.
    fn membership_is_removing(&self, key: ChildKey) -> bool {
        let Some(child) = self.children.get(key) else {
            return true;
        };
        self.dynamic.as_ref().is_some_and(|control| {
            control
                .state
                .lock()
                .expect("dynamic-state mutex poisoned")
                .entries
                .get(child.slot.member.id())
                .filter(|entry| entry.slot.member.membership() == child.slot.member.membership())
                .is_some_and(|entry| entry.restart_is_suppressed(key))
        })
    }

    /// Reports whether any level-triggered stop source forbids constructing
    /// a new incarnation: a removal source for the membership itself, or a
    /// scope-level stop (drain, force, a latched shutdown request, or an
    /// ancestor latch). Every scope-level source has a guaranteed follow-up
    /// event, so this broad consult belongs only at sites that would
    /// otherwise invoke user construction — not at exit dispatch, where it
    /// would misclassify the membership and reroute the scope verdict.
    fn restart_is_suppressed(&self, key: ChildKey) -> bool {
        self.lifecycle.is_draining()
            || self.hard_forced
            || self.root.has_stop_request(self.epoch)
            || self
                .role
                .ancestor()
                .is_some_and(|latches| latches.shutdown.is_fired() || latches.abort.is_fired())
            || self.membership_is_removing(key)
    }

    fn pending_restart_shutdowns(&self) -> Vec<ChildKey> {
        self.children
            .keys()
            .filter(|key| {
                let child = &self.children[*key];
                // Only a nested scope can hold a pending-incarnation stop, and
                // only its own control plane can answer whether one exists.
                // Both are cheap, so they gate the dynamic-state lookup.
                child.active.is_none()
                    && matches!(child.slot.member.record().stage, MemberStage::Restarting)
                    && child
                        .slot
                        .scope
                        .as_ref()
                        .is_some_and(|scope| scope.has_pending_incarnation_shutdown())
                    && !self.restart_is_suppressed(*key)
            })
            .collect()
    }

    fn expedite_restart_shutdown(&mut self, key: ChildKey) {
        // Collection and execution are separated by arbitration. Recheck
        // every level-triggered stop source so teardown/removal latched in the
        // same batch suppresses user construction immediately.
        if !self.restart_is_suppressed(key) {
            self.spawn_child(key);
        }
    }

    #[cfg(test)]
    fn record_storage(&self) {
        self.root.record_runtime_storage(RuntimeStorage {
            children: self.children.len(),
            child_slots: self.children.storage_len(),
            deadlines: self.deadlines.len(),
            deadline_slots: self.deadlines.storage_len(),
        });
    }

    fn spawn_child(&mut self, key: ChildKey) {
        let Some(child) = self.children.get(key) else {
            return;
        };
        if self.lifecycle.is_draining()
            || child.active.is_some()
            || child.is_terminal()
            || child.is_disposing()
        {
            return;
        }
        let child = &mut self.children[key];
        if let Some(deadline) = child.restart_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        let Some(incarnation) = child.incarnations.mint() else {
            let exit = child
                .slot
                .member
                .record()
                .last_exit
                .unwrap_or_else(Exit::never_started);
            let startup =
                if child.initial && !self.lifecycle.startup_complete() && !child.initial_ready {
                    StartupDisposition::Aborted
                } else {
                    StartupDisposition::NotAborted
                };
            // Exhaustion is a terminal outcome, not an exceptional cleanup
            // path. Join retained-definition disposal before terminality,
            // retention, removal completion, or ordered-scope progression.
            self.begin_terminal_disposal(key, exit, None, startup);
            return;
        };

        // Per-incarnation latch topology:
        // - shutdown/abort flow from the ladder into application code;
        // - ready and local_stop flow from application code back to helpers;
        // - ready's completion edge terminates those helpers when the child
        //   exits first and orders late retained readiness capabilities;
        // - construction_release keeps a raw actor behind the driver-owned
        //   readiness transition after its construction report is accepted;
        // - framework_abort/ack join nested-scope escalation before exit.
        // Each edge is level-triggered, so helper startup cannot lose a pulse.
        let latches = SpawnLatches {
            shutdown: Latch::default(),
            abort: Latch::default(),
            ready: CompletionGatedLatch::default(),
            construction_release: Latch::default(),
            local_stop: Latch::default(),
            framework_abort: Latch::default(),
            framework_abort_ack: Latch::default(),
        };
        let SpawnDispatch {
            body,
            declared_readiness,
            construction_spent,
            scope_child,
        } = dispatch_child_construction(child, &self.root, &self.defaults, incarnation, &latches);
        let now = runtime::now();
        child.spawned_once = true;
        if let Some(mailbox) = &child.mailbox {
            #[cfg(debug_assertions)]
            debug_assert!(
                mailbox.bind_order_valid(),
                "driver must configure before bind and close before rebind"
            );
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

        let mut readiness = ReadinessGate::new();
        let readiness_effect = declared_readiness.and_then(|readiness_mode| {
            let deadline = match child.options.readiness_deadline {
                ReadinessDeadline::Bounded(duration) => Deadline::after(now, duration).instant(),
                ReadinessDeadline::Unbounded | ReadinessDeadline::Inherit => None,
            };
            readiness.step(ReadinessEvent::Configure {
                readiness: readiness_mode,
                deadline,
            })
        });
        let gated = readiness.needs_signal_watch();

        let child_readiness_override = child.options.readiness_override;
        if construction_spent {
            // One-shot actor/task/subtree state has moved into `body`; the
            // retained construction is now framework-only spent metadata.
            // Release it without adding a blocking-pool scheduling edge to
            // terminal publication or restart-window arbitration.
            drop(child.construction.take());
        }
        let abort_handle = spawn_child_tasks(ChildTaskLaunch {
            events: self.events.clone(),
            key,
            incarnation,
            body,
            readiness_override: child_readiness_override,
            watch_readiness: gated,
            shutdown: latches.shutdown.clone(),
            ready: latches.ready.clone(),
            construction_release: latches.construction_release.clone(),
            local_stop: latches.local_stop.clone(),
        });

        child.active = Some(ActiveChild {
            incarnation,
            started_at: now,
            shutdown: latches.shutdown,
            abort: latches.abort,
            abort_handle,
            ladder: None,
            forced_outcome: None,
            hard_abort_phase: None,
            readiness,
            readiness_deadline: None,
            ready_signal: latches.ready,
            construction_release: latches.construction_release,
            framework_abort: scope_child.then_some(latches.framework_abort),
            framework_abort_ack: scope_child.then_some(latches.framework_abort_ack),
            stop_deadline: None,
        });
        if let Some(effect) = readiness_effect {
            // `progress_startup` already owns this ordered-startup loop. Do
            // not re-enter it synchronously for an immediate child.
            let _ = self.apply_readiness_effect(key, incarnation, effect);
        }
        #[cfg(test)]
        self.record_storage();
    }

    fn apply_readiness_effect(
        &mut self,
        key: ChildKey,
        incarnation: Incarnation,
        effect: ReadinessEffect,
    ) -> bool {
        match effect {
            ReadinessEffect::ArmDeadline { deadline } => {
                let handle = self.deadlines.push(
                    deadline,
                    DeadlineKind::Readiness {
                        child: key,
                        incarnation,
                    },
                );
                if let Some(active) = self
                    .children
                    .get_mut(key)
                    .and_then(|child| child.active.as_mut())
                    && active.incarnation == incarnation
                {
                    active.readiness_deadline = Some(handle);
                } else {
                    self.deadlines.cancel(handle);
                }
                false
            }
            ReadinessEffect::BecameReady => {
                let Some(child) = self.children.get_mut(key) else {
                    return false;
                };
                let Some(active) = child.active.as_mut() else {
                    return false;
                };
                if active.incarnation != incarnation {
                    return false;
                }
                if let Some(deadline) = active.readiness_deadline.take() {
                    self.deadlines.cancel(deadline);
                }
                active.ready_signal.fire();
                if !self.lifecycle.startup_complete() {
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
                true
            }
            ReadinessEffect::TimedOut { deadline } => {
                self.begin_stop_child(key, Some(RecordedOutcome::ReadinessTimedOut { deadline }));
                false
            }
            ReadinessEffect::Disarmed => false,
        }
    }

    fn progress_startup(&mut self) {
        if !self.lifecycle.is_starting() {
            return;
        }
        match self.root.flavor {
            ScopeFlavor::Ordered => {
                while let Some(key) = self.next_ordered_start {
                    if !self.children[key].spawned_once {
                        self.spawn_child(key);
                    }
                    if self.children[key].initial_ready {
                        self.next_ordered_start = self.children.keys_after(key).next();
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
        if !self.lifecycle.complete_startup() {
            return;
        }
        self.root.set_state(ScopeState::Running);
        self.root.set_startup(Ok(()));
        if let Some(parent_ready) = self.role.parent_ready() {
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
            self.root.transition_child(
                &child.slot.member,
                |record| record.stage = MemberStage::Stopping,
                None,
            );
            if let Some(mailbox) = &child.mailbox {
                mailbox.freeze(active.incarnation);
            }
            active.forced_outcome = forced;
            if let Some(deadline) = active.readiness_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            if active.forced_outcome.is_none() {
                active.readiness.step(ReadinessEvent::Shutdown);
            }
            active.ladder = Some(if active.framework_abort.is_some() {
                StopLadder::for_framework_driver(child.options.shutdown)
            } else {
                StopLadder::new(child.options.shutdown)
            });
            self.advance_ladder(key, runtime::now());
        } else {
            if let Some(deadline) = child.restart_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            let record = child.slot.member.record();
            let exit = record.last_exit.unwrap_or_else(Exit::never_started);
            // A never-ran child and a child stopped between restart
            // incarnations share the same post-disposal terminal route. Hard
            // shutdown still detaches disposal through `hard_forced` below.
            self.begin_terminal_disposal(key, exit, None, StartupDisposition::NotAborted);
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
        if active
            .framework_abort_ack
            .as_ref()
            .is_some_and(Latch::is_fired)
        {
            ladder.acknowledge_framework_abort();
        }
        while let Some(action) = ladder.advance(now) {
            match action {
                StopAction::Cancel => {
                    active.shutdown.fire();
                }
                StopAction::Escalate => {
                    active.abort.fire();
                }
                StopAction::AbortFramework { phase } => {
                    active.hard_abort_phase = Some(phase);
                    if active.forced_outcome.is_none() {
                        active.forced_outcome = Some(RecordedOutcome::Aborted { phase });
                    }
                    active
                        .framework_abort
                        .as_ref()
                        .expect("framework action belongs only to a framework driver")
                        .fire();
                }
                StopAction::HardAbort { phase } => {
                    active.hard_abort_phase = Some(phase);
                    active.abort_handle.abort();
                }
            }
        }
        let ladder_deadline = ladder.deadline();
        if let Some(deadline) = ladder_deadline {
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
        let Some(effect) = self.lifecycle.begin_drain(reason) else {
            return;
        };
        if effect.startup_pending {
            self.root.set_startup(Err(StartupError::ShutdownRequested));
        }
        self.root.set_state(ScopeState::Draining);
        match self.root.flavor {
            ScopeFlavor::Ordered => {
                self.ordered_stop_cursor = self.children.keys().next_back();
                self.stop_next_ordered();
            }
            ScopeFlavor::Dynamic => {
                let children: Vec<_> = self.children.keys().collect();
                for child in children {
                    self.begin_stop_child(child, None);
                }
            }
        }
    }

    fn stop_next_ordered(&mut self) {
        if self.root.flavor != ScopeFlavor::Ordered
            || !self.lifecycle.is_draining()
            || self.ordered_stop_progressing
        {
            return;
        }
        self.ordered_stop_progressing = true;
        if let Some(key) = self.ordered_stop_waiting {
            let waiting = self
                .children
                .get(key)
                .is_some_and(|child| !child.is_terminal() || child.is_disposing());
            if waiting {
                self.ordered_stop_progressing = false;
                return;
            }
            self.ordered_stop_waiting = None;
        }
        while let Some(key) = self.ordered_stop_cursor {
            self.ordered_stop_cursor = self.children.previous_key(key);
            #[cfg(test)]
            {
                self.ordered_stop_inspections += 1;
            }
            // The cursor key is held across await boundaries, so never index
            // the arena with it: a reclaimed slot is treated as already gone.
            let Some(child) = self.children.get(key) else {
                continue;
            };
            if child.is_terminal() && !child.is_disposing() {
                continue;
            }
            self.begin_stop_child(key, None);
            let Some(child) = self.children.get(key) else {
                continue;
            };
            if child.active.is_some() || child.is_disposing() {
                self.ordered_stop_waiting = Some(key);
                break;
            }
        }
        self.ordered_stop_progressing = false;
    }

    fn force_all(&mut self) {
        self.hard_forced = true;
        if !self.lifecycle.is_draining() {
            self.begin_drain(StopReason::ShutdownRequested);
        }
        let now = runtime::now();
        let children: Vec<_> = self.children.keys().collect();
        for key in children {
            // Every live membership enters the same stop funnel first. That
            // owns mailbox freeze, readiness disarm, ordered-child handling,
            // and the initial cooperative action.
            self.begin_stop_child(key, None);
            if let Some(ladder) = self
                .children
                .get_mut(key)
                .and_then(|child| child.active.as_mut())
                .and_then(|active| active.ladder.as_mut())
            {
                ladder.force(now);
            }
            self.advance_ladder(key, now);
        }
        let disposing = self
            .children
            .keys()
            .filter(|key| self.children[*key].pending_terminal.is_some())
            .collect::<Vec<_>>();
        for key in disposing {
            // The incarnation has already exited; only its retained factory
            // remains. Hard escalation detaches that cleanup, but must not
            // rewrite the actor's recorded verdict.
            self.handle_construction_disposed(key, None);
        }
    }

    fn handle_constructed(
        &mut self,
        key: ChildKey,
        incarnation: Incarnation,
        readiness: Readiness,
    ) {
        let effect = {
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
            let deadline = match child.options.readiness_deadline {
                ReadinessDeadline::Bounded(duration) => {
                    Deadline::after(active.started_at, duration).instant()
                }
                ReadinessDeadline::Unbounded | ReadinessDeadline::Inherit => None,
            };
            active.readiness.step(ReadinessEvent::Configure {
                readiness,
                deadline,
            })
        };
        let became_ready = effect
            .map(|effect| self.apply_readiness_effect(key, incarnation, effect))
            .unwrap_or(false);
        if let Some(active) = self
            .children
            .get(key)
            .and_then(|child| child.active.as_ref())
            .filter(|active| active.incarnation == incarnation)
        {
            // Readiness state and all shell effects are installed before raw
            // actor execution is released.
            active.construction_release.fire();
        }
        if became_ready {
            self.progress_startup();
        }
    }

    fn handle_ready(&mut self, key: ChildKey, incarnation: Incarnation) {
        let effect = self
            .children
            .get_mut(key)
            .and_then(|child| child.active.as_mut())
            .filter(|active| active.incarnation == incarnation)
            .and_then(|active| active.readiness.step(ReadinessEvent::Signal));
        let became_ready = effect
            .map(|effect| self.apply_readiness_effect(key, incarnation, effect))
            .unwrap_or(false);
        if became_ready {
            self.progress_startup();
        }
        #[cfg(test)]
        self.record_storage();
    }

    fn handle_self_stop(&mut self, key: ChildKey, incarnation: Incarnation) {
        let ready_before_stop = self
            .children
            .get(key)
            .and_then(|child| child.active.as_ref())
            .is_some_and(|active| {
                active.incarnation == incarnation && active.ready_signal.is_fired()
            });
        if ready_before_stop {
            // A local stop is reported on a separate helper task. Preserve
            // the application task's mark-ready-before-stop order even when
            // arbitration observes the stop before the readiness event.
            // An inverted `stop(); mark_ready()` sequence may also count as
            // ready here when its latch fires before the driver observes the
            // stop — licensed by the spec's "fired before ... a clean
            // self-stop is observed" wording (§6).
            self.handle_ready(key, incarnation);
        }
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
        cancellation: Cancellation,
        readiness_signal_seen: bool,
    ) {
        let readiness_effect = self
            .children
            .get_mut(key)
            .and_then(|child| child.active.as_mut())
            .filter(|active| active.incarnation == incarnation)
            .and_then(|active| {
                active.readiness.step(ReadinessEvent::Exit {
                    signal_seen: readiness_signal_seen,
                })
            });
        let became_ready = readiness_effect
            .map(|effect| self.apply_readiness_effect(key, incarnation, effect))
            .unwrap_or(false);
        if became_ready {
            // Match the natural signal-before-exit order: ordered startup may
            // advance, and a sole ready child completes aggregate startup
            // before its post-ready exit is classified.
            self.progress_startup();
        }

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
        if let Some(mailbox) = &child.mailbox
            && let Some(teardown) = mailbox.close(incarnation)
        {
            runtime::dispose_detached(teardown);
        }
        if let (JoinVerdict::Cancelled { .. }, Some(phase)) = (&join, active.hard_abort_phase) {
            join = JoinVerdict::Cancelled { phase };
        }
        let recorded = reconcile_recorded_outcomes(recorded, active.forced_outcome);
        let exit = classify_exit(recorded, join, cancellation);
        child.restarts.settle_if_stable(
            IncarnationRun {
                started_at: active.started_at,
                stopped_at: runtime::now(),
            },
            self.intensity_policy.within,
        );

        // Fused cancellation is a level-triggered source. It can linearize
        // before the forwarded Removal event or its public `removing`
        // projection reaches this driver, so exit dispatch must consult the
        // removal sources directly before charging or publishing a restart.
        // Only removal sources classify the membership here: a latched but
        // unprocessed scope stop (shutdown request or ancestor latch) must
        // not turn this exit Terminal, or a restartable initial child
        // failing pre-ready would publish `StartupFailed` where the stop's
        // own follow-up event owns the verdict. The broader
        // `restart_is_suppressed` still gates the restart deadline arm,
        // where every suppression source has a guaranteed follow-up event.
        let membership_removing = self.membership_is_removing(key);
        let child = self
            .children
            .get_mut(key)
            .expect("the exiting child remains registered");

        let mode = if self.lifecycle.is_draining() {
            ScopeMode::Draining
        } else {
            ScopeMode::Running
        };
        let member_mode = if membership_removing {
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
                let startup = if child.initial
                    && !self.lifecycle.startup_complete()
                    && !child.initial_ready
                {
                    StartupDisposition::Aborted
                } else {
                    StartupDisposition::NotAborted
                };
                self.begin_terminal_disposal(key, exit, Some(incarnation), startup);
            }
            ExitDispatch::ScheduleRestart => {
                if !self.lifecycle.startup_complete() {
                    child.initial_ready = false;
                }
                let sample =
                    JitterSample::from_u64_ratio(self.jitter.sample(0..u64::MAX), u64::MAX);
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
                        // prevents spawning it. `None` means the exact clock
                        // point cannot be represented and armed; no substitute
                        // restart is scheduled.
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
                    let trip = IntensityTrip::new(self.intensity_policy, decision.charge);
                    if self.lifecycle.is_starting() {
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
        startup: StartupDisposition,
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
                startup,
            });
            child.slot.member.set_terminal_disposal_pending(true);
            child.construction.take()
        };
        let Some(construction) = construction else {
            self.handle_construction_disposed(key, None);
            return;
        };

        if self.hard_forced {
            runtime::dispose_detached(construction);
            self.handle_construction_disposed(key, None);
            return;
        }

        // The retained factory is user-owned. Destroy it on the blocking
        // pool. The disposal job itself owns completion, so cancellation or
        // failure to spawn an auxiliary async joiner cannot strand the child.
        let sender = self.disposal_events.clone();
        let signal = self.root.signal().clone();
        runtime::dispose_then(construction, move |panic| {
            if runtime::unbounded_mpsc_send(
                &sender,
                DriverEvent::Child(ChildEvent::ConstructionDisposed { child: key, panic }),
            )
            .is_ok()
            {
                signal.pulse();
            }
        });
    }

    fn handle_construction_disposed(
        &mut self,
        key: ChildKey,
        panic: Option<runtime::DisposalPanic>,
    ) {
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        let Some(mut terminal) = child.pending_terminal.take() else {
            return;
        };
        child.slot.member.set_terminal_disposal_pending(false);
        if terminal.exited_incarnation.is_some()
            && let Some(runtime::DisposalPanic { message }) = panic
            && !matches!(terminal.exit.kind(), ExitKind::Panicked { .. })
        {
            // Only an exited incarnation can own a destructor failure. A
            // never-started child or a child between restart incarnations
            // keeps its already-authoritative verdict while disposal remains
            // ordered ahead of terminal routing.
            terminal.exit = Exit::new(ExitKind::Panicked { message }, terminal.exit.cancellation());
        }

        let exit = terminal.exit;
        // §6's `StartupAborted` is a startup-sequence property of a
        // membership that *ran* and failed before its initial readiness
        // edge. A terminal without an exited incarnation never ran, so it
        // publishes the plain `Stopped { NeverStarted }` verdict (B.6) even
        // when its pre-readiness position still routes the scope's startup
        // failure below. Incarnation exhaustion is the reachable case:
        // it terminalizes an unspawned membership while `pre_ready` holds.
        let startup = if terminal.exited_incarnation.is_some() {
            terminal.startup
        } else {
            StartupDisposition::NotAborted
        };
        self.children[key].terminalize(
            &self.root,
            exit.clone(),
            terminal.exited_incarnation,
            startup,
        );
        if self.dynamic_membership_is_removing(key) {
            // A foreign remover may have committed the control-plane state
            // and be waiting for this thread's observation gate. Publish the
            // Removing projection before pruning so the public lifecycle
            // cannot skip directly from resident to Removed.
            self.publish_dynamic_removal(key);
            self.finalize_removal(key);
        } else if terminal.startup == StartupDisposition::Aborted && !self.lifecycle.is_draining() {
            self.fail_startup(key, exit);
            if self.children[key].options.retention == crate::Retention::Remove {
                self.prune_terminal(key);
            }
        } else {
            if self.children[key].options.retention == crate::Retention::Remove {
                self.prune_terminal(key);
            }
            if self.lifecycle.is_draining() {
                self.stop_next_ordered();
            }
        }
    }

    fn fail_startup(&mut self, key: ChildKey, exit: Exit) {
        // Several initial children can fail in one arbitration batch. The
        // first failure owns the startup verdict and its sole lifecycle edge;
        // later exits are still terminalized, but cannot republish the scope
        // transition or replace the authoritative cause.
        let child = &self.children[key];
        let failure = StartupFailure {
            cause: StartupFailureCause::Child {
                id: child.slot.member.id().clone(),
                membership: child.slot.member.membership(),
                exit,
            },
        };
        let first_failure = self.lifecycle.fail_startup();
        if !first_failure {
            return;
        }
        self.root
            .set_startup(Err(StartupError::StartupFailed(failure.clone())));
        if self.root.flavor == ScopeFlavor::Ordered {
            let later_children: Vec<_> = self.children.keys_after(key).collect();
            for later in later_children {
                if !self.children[later].spawned_once
                    && !self.children[later].is_disposing()
                    && !self.children[later].is_terminal()
                {
                    self.begin_terminal_disposal(
                        later,
                        Exit::never_started(),
                        None,
                        StartupDisposition::NotAborted,
                    );
                }
            }
        }
        if self.role.is_root() {
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
                let Some(child) = self.children.get_mut(key) else {
                    return;
                };
                let Some(active) = child.active.as_mut() else {
                    return;
                };
                if active.incarnation != incarnation {
                    return;
                }
                // The queue already consumed this registration. Feed the
                // retained latch into the engine so signal-at-deadline policy
                // is decided in exactly one place.
                active.readiness_deadline.take();
                let effect = active.readiness.step(ReadinessEvent::Deadline {
                    now: runtime::now(),
                    signal_seen: active.ready_signal.is_fired(),
                });
                if effect
                    .map(|effect| self.apply_readiness_effect(key, incarnation, effect))
                    .unwrap_or(false)
                {
                    self.progress_startup();
                }
            }
            DeadlineKind::Restart { child } => {
                // A removal or scope stop can latch after the exit scheduled
                // this deadline but before the deadline's batch runs. Recheck
                // the level-triggered sources at execution time so a stale
                // backoff edge never invokes user construction.
                if self.restart_is_suppressed(child) {
                    if let Some(child) = self.children.get_mut(child) {
                        child.restart_deadline.take();
                    }
                } else {
                    self.spawn_child(child);
                    // A restart-deadline caller is outside `progress_startup`'s
                    // ordered loop. Revisit the aggregate in case this spawn's
                    // immediate-readiness effect released its last gate.
                    self.progress_startup();
                }
            }
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
        let all_terminal = self
            .children
            .values()
            .all(|child| child.is_terminal() && !child.is_disposing());
        self.lifecycle.finish_if_ready(
            self.root.flavor,
            ChildCompletionState {
                has_children: !self.children.is_empty(),
                all_terminal,
            },
        )
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
            || self.lifecycle.is_draining()
            || self.lifecycle.startup_failed()
        {
            let cause = if self.lifecycle.is_draining() {
                NotAdmittingCause::Draining
            } else if self.lifecycle.startup_failed() {
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

        let (definition, resolved) = match request.slot.resolve_and_take_defined(&self.defaults) {
            Ok(Some(claimed)) => claimed,
            Ok(None) => {
                let (_, removed) = cancel_dynamic_reservation_parts(&control, &request.slot);
                reject_admission_after_disposal(
                    request,
                    None,
                    removed,
                    ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
                );
                return;
            }
            Err(invalid) => {
                let (definition, removed) =
                    cancel_dynamic_reservation_parts(&control, &request.slot);
                reject_admission_after_disposal(
                    request,
                    definition,
                    removed,
                    ReserveError::InvalidPolicy(invalid),
                );
                return;
            }
        };
        let plan = ChildPlan::with_options(Arc::clone(&request.slot), definition, resolved);
        // Conversion can unwind while acquiring child identity or configuring
        // the mailbox. Keep that fallible work outside the control-plane lock
        // so driver teardown can still close reservations and removals.
        let mut child = ChildRuntime::from_plan(plan, &self.root);
        child.initial = false;
        let key = {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            let id = request.slot.member.id();
            let matches_reservation = state.entries.get(id).is_some_and(|entry| {
                entry.slot.member.membership() == request.slot.member.membership()
                    && entry.is_reserved()
            });
            if !matches_reservation || request.fused_cancel.as_ref().is_some_and(Latch::is_fired) {
                let removed = matches_reservation
                    .then(|| state.entries.remove(id))
                    .flatten();
                drop(state);
                request.slot.terminalize_never_started();
                // The entry's drop completes its removal response; preserve
                // the same terminality-before-completion ordering as every
                // other reservation-cancellation path. Definition disposal
                // is also complete before either waiter regains ownership.
                child.complete_terminality();
                let ChildRuntime { construction, .. } = child;
                reject_admission_after_disposal(
                    request,
                    Some(construction),
                    removed,
                    ReserveError::NotAdmitting(NotAdmittingCause::ReservationEnded),
                );
                return;
            }
            // The control-plane lock makes arena insertion and promotion one
            // state transition: an exact remover sees either the reservation
            // or a resident carrying its live arena key, never an unindexed
            // admitted intermediate.
            let key = match self.children.insert(child) {
                Ok(key) => key,
                Err(child) => {
                    let removed = state.entries.remove(id);
                    drop(state);
                    let mut child = *child;
                    request.slot.terminalize_never_started();
                    child.complete_terminality();
                    let ChildRuntime { construction, .. } = child;
                    reject_admission_after_disposal(
                        request,
                        Some(construction),
                        removed,
                        ReserveError::IdentityExhausted,
                    );
                    return;
                }
            };
            let entry = state
                .entries
                .get_mut(id)
                .expect("the matching reservation was just resolved");
            entry.promote(key, request.fused_cancel.take());
            key
        };
        self.root.admit_child(resident_projection(&request.slot));
        #[cfg(test)]
        self.record_storage();
        request.complete(Ok(()));
        self.spawn_child(key);
    }

    fn handle_removal(&mut self, removal: RemovalRequest) {
        let RemovalRequest { membership, key } = removal;
        let Some(member) = self
            .children
            .get(key)
            .map(|child| Arc::clone(&child.slot.member))
            .filter(|member| member.membership() == membership)
        else {
            return;
        };
        let Some(control) = &self.dynamic else {
            return;
        };
        let tracked = {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            state
                .entries
                .get_mut(member.id())
                .filter(|entry| entry.slot.member.membership() == membership)
                .and_then(DynamicEntry::mark_removing)
                .is_some_and(|tracked| tracked == key)
        };
        if !tracked {
            return;
        }
        // A fused drop and an explicit `remove` each queue one
        // `RemovalRequest` for the same membership, and `mark_removing`
        // deliberately re-succeeds on an already-Removing entry, so a second
        // delivery reaches this point. Every step below is idempotent:
        // `publish_dynamic_removal` is guarded by the record's `removing`
        // flag, `begin_stop_child` by its ladder/disposal guards, and
        // `finalize_removal` removes the entry it matched.
        self.publish_dynamic_removal(key);
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
        if state.entries.get(&id).is_some_and(|entry| {
            entry.slot.member.membership() == member.membership()
                && entry.matches_key(key)
                && entry.is_removing()
        }) {
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
            if state.entries.get(&id).is_some_and(|entry| {
                entry.slot.member.membership() == member.membership() && entry.matches_key(key)
            }) {
                removed = state.entries.remove(&id);
            }
        }
        self.root.prune_child(&member);
        if self.root.flavor == ScopeFlavor::Dynamic {
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

/// Owns a scope epoch until a `ScopeRuntime` has taken over its teardown.
///
/// Nested lowering can await isolated disposal before a driver exists. If
/// that setup future is cancelled or unwinds, dropping this guard retires the
/// epoch so a later restart cannot mistake the still-live reservation for
/// identity exhaustion.
struct ScopeEpochGuard {
    scope: Arc<ScopeCell>,
    epoch: Option<Epoch>,
}

impl ScopeEpochGuard {
    fn begin(scope: &Arc<ScopeCell>) -> Option<Self> {
        let epoch = scope.begin_incarnation()?;
        Some(Self {
            scope: Arc::clone(scope),
            epoch: Some(epoch),
        })
    }

    fn finish(mut self, reason: StopReason) {
        let epoch = self
            .epoch
            .take()
            .expect("an owned scope epoch finishes at most once");
        self.scope.finish_incarnation(epoch, reason);
    }

    fn transfer(mut self) -> Epoch {
        self.epoch
            .take()
            .expect("an owned scope epoch transfers at most once")
    }
}

impl Drop for ScopeEpochGuard {
    fn drop(&mut self) {
        if let Some(epoch) = self.epoch.take() {
            self.scope
                .finish_incarnation(epoch, StopReason::ShutdownRequested);
        }
    }
}

async fn run_nested_tree(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
) -> crate::ExitResult {
    let epoch = begin_nested_incarnation(&scope)?;
    run_nested_tree_with_epoch(tree, scope, inherited, latches, epoch).await
}

/// Begins the nested epoch before invoking its synchronous restartable
/// factory. Tokio cancellation cannot interrupt one in-progress poll, so a
/// factory that overlaps parent-driver destruction must already own the
/// epilogue that makes `wait_stopped` final.
async fn run_nested_factory(
    factory: ScopeFactory,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
) -> crate::ExitResult {
    let epoch = begin_nested_incarnation(&scope)?;
    let tree = factory();
    run_nested_tree_with_epoch(tree, scope, inherited, latches, epoch).await
}

fn begin_nested_incarnation(scope: &Arc<ScopeCell>) -> Result<ScopeEpochGuard, crate::ExitError> {
    ScopeEpochGuard::begin(scope).ok_or_else(|| {
        let failure = StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: scope.member.id().clone(),
            },
        };
        scope.set_startup(Err(StartupError::StartupFailed(failure.clone())));
        crate::ExitError::from_startup_failure(failure)
    })
}

async fn run_nested_tree_with_epoch(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
    epoch: ScopeEpochGuard,
) -> crate::ExitResult {
    let plan = match tree.lower(inherited, Some(Arc::clone(&scope))) {
        Ok(plan) => plan,
        Err(error) => {
            let (cause, disposal) = match error {
                LowerError::Undefined { paths, disposal } => {
                    (StartupFailureCause::Lowering { undefined: paths }, disposal)
                }
                LowerError::IdentityExhausted { id, disposal } => {
                    (StartupFailureCause::IdentityExhausted { id }, disposal)
                }
                LowerError::InvalidPolicy { invalid, disposal } => {
                    // `InvalidPolicy::path` is relative to the scope being
                    // lowered, which here is this nested scope itself. The
                    // owning child id is already carried by the enclosing
                    // `StartupFailureCause::Child`, so prepending it here
                    // would double-count this frame.
                    (StartupFailureCause::InvalidPolicy(invalid), disposal)
                }
            };
            // Lowering never created a nested driver to own teardown. Keep
            // its isolated definitions attached to this incarnation until
            // they finish; hard-aborting the incarnation still detaches the
            // cancellation-safe disposal jobs.
            disposal.fired().await;
            let failure = StartupFailure { cause };
            scope.set_startup(Err(StartupError::StartupFailed(failure.clone())));
            epoch.finish(StopReason::StartupFailed(failure.clone()));
            return Err(crate::ExitError::from_startup_failure(failure));
        }
    };
    match run_scope_incarnation(plan, ScopeRole::Nested(latches), epoch).await {
        StopReason::Finished | StopReason::ShutdownRequested => Ok(()),
        StopReason::IntensityTripped(trip) => Err(crate::ExitError::from_intensity_trip(trip)),
        StopReason::StartupFailed(failure) => Err(crate::ExitError::from_startup_failure(failure)),
        StopReason::NeverStarted => Err(crate::ExitError::message("nested scope never started")),
    }
}

async fn run_scope(plan: ScopePlan, role: ScopeRole) -> StopReason {
    let root = Arc::clone(&plan.root);
    let Some(epoch) = ScopeEpochGuard::begin(&root) else {
        // Dropping the still-armed plan terminalizes every never-started
        // declaration and the root; no aliased driver epoch is created.
        drop(plan);
        return StopReason::NeverStarted;
    };
    run_scope_incarnation(plan, role, epoch).await
}

async fn run_scope_incarnation(
    mut plan: ScopePlan,
    role: ScopeRole,
    epoch: ScopeEpochGuard,
) -> StopReason {
    let root = Arc::clone(&plan.root);
    if role.is_root() {
        root.member
            .update(|record| record.stage = MemberStage::Running);
    }
    let capacity = plan.children.len().saturating_mul(3).max(64);
    let (events, mut event_receiver) = runtime::bounded_mpsc(capacity);
    let (disposal_events, mut disposal_event_receiver) = runtime::unbounded_mpsc();
    let dynamic =
        (plan.root.flavor == ScopeFlavor::Dynamic).then(|| DynamicControl::new(events.clone()));
    // Transfer children one at a time. The not-yet-converted suffix remains
    // owned by ScopePlan, while ChildRuntime::from_plan arms the current
    // child's obligation before fallible setup. Thus a panic at any point has
    // exactly one terminality owner for every child.
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        let child = ChildRuntime::from_plan(child, &root);
        if children.insert(child).is_err() {
            unreachable!("a fresh child-key domain accommodates an in-memory child collection");
        }
    }
    if let Some(control) = &dynamic {
        control.register_initial(
            children
                .children
                .iter()
                .map(|(key, child)| (&child.slot, *key)),
        );
    }
    let next_ordered_start = children.keys().next();
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: DeadlineQueue::default(),
        jitter: runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::starting(),
        next_ordered_start,
        role,
        dynamic,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        #[cfg(test)]
        ordered_stop_inspections: 0,
        completion: None,
        // Transfer last: every fallible setup expression above remains
        // covered by the pre-driver guard, and completed construction moves
        // the raw epoch directly into ScopeRuntime's synchronous epilogue.
        epoch: epoch.transfer(),
    };
    plan.armed = false;
    drop(plan);

    // ScopeRuntime owns teardown before the route becomes public. If either
    // route notification or initial-child publication unwinds, its epilogue
    // closes dynamic state, terminalizes every child, and clears any resident
    // prefix. Install the fully keyed route before publishing Added so a
    // synchronous observer never sees membership without its control plane.
    if let Some(control) = &scope.dynamic {
        scope.root.set_dynamic_route(Some(control.clone()));
    }
    scope.root.set_admitted_children(
        scope
            .children
            .values()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    #[cfg(test)]
    scope.record_storage();

    match scope.root.flavor {
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
        if root.take_shutdown_request(scope.epoch) {
            pending.push(Pending::Shutdown.classified());
        }
        for child in scope.pending_restart_shutdowns() {
            pending.push(restart_shutdown_work(child));
        }
        if !scope.ancestor_shutdown_seen
            && scope
                .role
                .ancestor()
                .is_some_and(|latches| latches.shutdown.is_fired())
        {
            scope.ancestor_shutdown_seen = true;
            pending.push(Pending::AncestorShutdown.classified());
        }
        if !scope.ancestor_abort_seen
            && scope
                .role
                .ancestor()
                .is_some_and(|latches| latches.abort.is_fired())
        {
            scope.ancestor_abort_seen = true;
            pending.push(Pending::AncestorAbort.classified());
        }
        if root.take_force_request(scope.epoch) {
            // Force owns shutdown arbitration: readiness from the same wake
            // cannot publish Running after the stop boundary.
            pending.push(Pending::Force.classified());
        }
        while let Some(event) = runtime::mpsc_try_recv(&mut event_receiver) {
            pending.push(Pending::Driver(event).classified());
        }
        // Disposal completions drain after the bounded lane, and `arbitrate`
        // sorts stably, so a `ConstructionDisposed` always trails every
        // same-class `Exited` collected in the same wake — even one produced
        // later. A disposal is therefore a batch-tail event: the exit it
        // trails may begin a drain first, after which
        // `handle_construction_disposed` sees `is_draining` and routes the
        // disposed child through stop progression instead of `fail_startup`.
        // That is a widening of an order that was already reachable, not a new
        // one: disposal runs on the blocking pool, so its completion never had
        // a fixed position relative to concurrent exits.
        while let Some(event) = runtime::unbounded_mpsc_try_recv(&mut disposal_event_receiver) {
            pending.push(Pending::Driver(event).classified());
        }
        let now = runtime::now();
        while let Some(deadline) = scope.deadlines.pop_due(now) {
            pending.push(Pending::Deadline(deadline).classified());
        }

        if pending.is_empty() {
            let ancestor_shutdown = scope
                .role
                .ancestor()
                .filter(|_| !scope.ancestor_shutdown_seen)
                .map(|latches| latches.shutdown.clone());
            let ancestor_abort = scope
                .role
                .ancestor()
                .filter(|_| !scope.ancestor_abort_seen)
                .map(|latches| latches.abort.clone());
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
                runtime::ScopeWait {
                    signal: signal.changed(),
                    parent_shutdown: ancestor_command,
                },
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
                    pending.push(Pending::Driver(event).classified());
                }
                runtime::ScopeWake::Message(None) => continue,
            }
        }

        arbitrate(&mut pending);
        for (_, event) in pending {
            match event {
                Pending::Shutdown => {
                    if let Some(latches) = scope.role.ancestor() {
                        latches.shutdown.fire();
                        scope.ancestor_shutdown_seen = true;
                    }
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::RestartShutdown(child) => scope.expedite_restart_shutdown(child),
                Pending::AncestorShutdown => {
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::AncestorAbort => {
                    if let Some(latches) = scope.role.ancestor() {
                        latches.abort_ack.fire();
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
                Pending::Driver(DriverEvent::Removal(removal)) => scope.handle_removal(removal),
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
                    cancellation,
                    readiness_signal_seen,
                })) => scope.handle_exit(
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                ),
                Pending::Driver(DriverEvent::Child(ChildEvent::ConstructionDisposed {
                    child,
                    panic,
                })) => scope.handle_construction_disposed(child, panic),
                Pending::Deadline(deadline) => scope.handle_deadline(deadline),
            }
        }

        if let Some(reason) = scope.finish_if_ready() {
            let root_exit = scope.role.is_root().then(|| match &reason {
                StopReason::Finished | StopReason::ShutdownRequested => {
                    let cancellation = if reason == StopReason::ShutdownRequested {
                        Cancellation::Observed
                    } else {
                        Cancellation::NotObserved
                    };
                    Exit::new(ExitKind::Completed, cancellation)
                }
                StopReason::IntensityTripped(trip) => Exit::new(
                    ExitKind::Failed(crate::ExitError::from_intensity_trip(trip.clone())),
                    Cancellation::NotObserved,
                ),
                StopReason::StartupFailed(failure) => Exit::new(
                    ExitKind::Failed(crate::ExitError::from_startup_failure(failure.clone())),
                    Cancellation::NotObserved,
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
    RestartShutdown(ChildKey),
    AncestorShutdown,
    AncestorAbort,
    Force,
    Driver(DriverEvent),
    Deadline(DeadlineKind),
}

impl Pending {
    fn class(&self) -> ArbitrationClass {
        match self {
            Self::Shutdown | Self::AncestorShutdown | Self::AncestorAbort | Self::Force => {
                ArbitrationClass::ScopeShutdown
            }
            Self::RestartShutdown(_) => ArbitrationClass::BackoffDue,
            Self::Driver(event) => driver_event_class(event),
            Self::Deadline(DeadlineKind::Readiness { .. }) => ArbitrationClass::ReadinessDeadline,
            Self::Deadline(DeadlineKind::Restart { .. }) => ArbitrationClass::BackoffDue,
            Self::Deadline(DeadlineKind::Stop { .. }) => ArbitrationClass::StopDeadline,
        }
    }

    fn classified(self) -> (ArbitrationClass, Self) {
        (self.class(), self)
    }
}

fn restart_shutdown_work(child: ChildKey) -> (ArbitrationClass, Pending) {
    // This starts a pending incarnation, so it is restart work, not a
    // scope-shutdown transition. A child exit collected in the same wake must
    // first get the chance to trip intensity or fail startup; the
    // execution-time suppression check then observes that drain.
    Pending::RestartShutdown(child).classified()
}

fn driver_event_class(event: &DriverEvent) -> ArbitrationClass {
    match event {
        DriverEvent::Child(ChildEvent::SelfStop { .. }) => ArbitrationClass::MembershipRemoval,
        DriverEvent::Removal(_) => ArbitrationClass::MembershipRemoval,
        DriverEvent::Child(ChildEvent::Constructed { .. } | ChildEvent::Ready { .. }) => {
            ArbitrationClass::ReadinessSignal
        }
        DriverEvent::Child(ChildEvent::Exited { .. } | ChildEvent::ConstructionDisposed { .. }) => {
            ArbitrationClass::ChildExit
        }
        DriverEvent::Admission(_) => ArbitrationClass::Admission,
    }
}

#[cfg(test)]
pub(crate) use tests::exercise_saturated_fused_drop_before_exit;

#[cfg(test)]
mod tests {
    use std::{
        future::{self, Future},
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc, Barrier, Condvar, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use crate::{
        ActorRef, Backoff, Cancellation, ChildId, ChildState, DynamicTree, Exit, ExitError,
        ExitKind, GracePhase, Intensity, LifecycleEventKind, LifecycleItem, LifecycleTryRecvError,
        Mailbox, RawOnceDef, Readiness, ReadinessDeadline, RemoveOutcome, ReserveError,
        RestartCondition, RestartPolicy, Retention, ScopeRef, ScopeState, SendErrorKind,
        StartupError, StartupFailureCause, StopReason, SubtreeDef, SubtreeOnceDef, TaskDef, Tree,
        engine::{Epoch, ScopeLifecycle, StopLadder, arbitrate},
        exit::{JoinVerdict, RecordedOutcome},
        identity::{IncarnationCounter, ScopeIdentity},
        mailbox::MailboxCell,
        plan::{ChildConstruction, SlotCell},
        policy::ResolvedDefaults,
        runtime::{CompletionGatedLatch, Latch},
    };

    use super::{
        AncestorCommandLatches, ChildArena, ChildEvent, ChildKey, ChildRuntime, DriverEvent,
        DynamicControl, DynamicEntry, GateCapture, MemberCell, MemberStage, NestedScopeLatches,
        Pending, RemovalRequest, RemovalResponses, ResidentProjection, RuntimeStorage, ScopeCell,
        ScopeEpochGuard, ScopeFlavor, ScopeRole, ScopeRuntime, StartupDisposition,
        cancel_dynamic_reservation, report_channel, reserve_dynamic, resident_projection,
        restart_shutdown_work, run_nested_factory, run_nested_tree, run_scope_incarnation,
    };

    /// Bounds every gate-capture probe wait. The probe sender lives inside
    /// the scope cell for the whole test, so the channel can never
    /// disconnect: a regression that keeps a thread from reaching its gate
    /// must time out with a diagnostic rather than hang the test on `recv`.
    const CAPTURE_PROBE_WAIT: Duration = Duration::from_secs(10);

    fn isolated_scope(id: &'static str, flavor: ScopeFlavor) -> Arc<ScopeCell> {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from(id);
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        ScopeCell::new(member, flavor, ScopeIdentity::new())
    }

    #[derive(Debug, Default)]
    struct FactoryGate {
        entered: AtomicBool,
        released: Mutex<bool>,
        changed: Condvar,
    }

    impl FactoryGate {
        fn block(&self) {
            self.entered.store(true, Ordering::Release);
            let mut released = self
                .released
                .lock()
                .expect("factory gate mutex remains healthy");
            self.changed.notify_all();
            while !*released {
                released = self
                    .changed
                    .wait(released)
                    .expect("factory gate mutex remains healthy while blocked");
            }
        }

        async fn wait_entered(&self) {
            while !self.entered.load(Ordering::Acquire) {
                crate::runtime::yield_now().await;
            }
        }

        fn release(&self) {
            let mut released = self
                .released
                .lock()
                .expect("factory gate mutex remains healthy");
            *released = true;
            self.changed.notify_all();
        }
    }

    fn pending_tree() -> Tree {
        let mut tree = Tree::new();
        tree.add_task("pending", TaskDef::new(|_| future::pending()))
            .expect("pending child is valid");
        tree
    }

    fn finished_tree() -> Tree {
        let mut tree = Tree::new();
        tree.add_task(
            "finished",
            TaskDef::new(|_| async { Ok::<_, ExitError>(()) }),
        )
        .expect("finished child is valid");
        tree
    }

    struct SnapshotReentryWake {
        scope: ScopeRef,
        observed: std::sync::mpsc::Sender<ScopeState>,
    }

    impl Wake for SnapshotReentryWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.observed.send(self.scope.snapshot().state.clone());
        }
    }

    fn snapshot_reentry_waker(
        scope: &Arc<ScopeCell>,
    ) -> (Waker, std::sync::mpsc::Receiver<ScopeState>) {
        let (observed, receiver) = std::sync::mpsc::channel();
        (
            Waker::from(Arc::new(SnapshotReentryWake {
                scope: ScopeRef {
                    cell: Arc::clone(scope),
                },
                observed,
            })),
            receiver,
        )
    }

    struct PendingRaw;

    impl crate::RawActor for PendingRaw {
        type Msg = u8;

        fn readiness(&self) -> Readiness {
            Readiness::Manual
        }

        async fn run(&mut self, _: &mut crate::RawContext<Self::Msg>) -> crate::ExitResult {
            future::pending().await
        }
    }

    struct PanicWake(&'static str);

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(self.0);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            std::panic::panic_any(self.0);
        }
    }

    struct PanicDropProbe(Arc<AtomicUsize>);

    impl Drop for PanicDropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct CountedPanicWake(Arc<AtomicUsize>);

    impl Wake for CountedPanicWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(PanicDropProbe(Arc::clone(&self.0)));
        }

        fn wake_by_ref(self: &Arc<Self>) {
            std::panic::panic_any(PanicDropProbe(Arc::clone(&self.0)));
        }
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ObserveWakeCount {
        wakes: Arc<AtomicUsize>,
        observed: Mutex<Option<usize>>,
    }

    impl Wake for ObserveWakeCount {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            *self.observed.lock().expect("observation mutex poisoned") =
                Some(self.wakes.load(Ordering::SeqCst));
        }
    }

    struct ExitOnSignalRaw {
        exit: Latch,
    }

    impl crate::RawActor for ExitOnSignalRaw {
        type Msg = u8;

        async fn run(&mut self, _: &mut crate::RawContext<Self::Msg>) -> crate::ExitResult {
            self.exit.fired().await;
            Ok(())
        }
    }

    #[test]
    fn independent_systems_do_not_share_an_observation_critical_section() {
        let first = isolated_scope("first", ScopeFlavor::Ordered);
        let second = isolated_scope("second", ScopeFlavor::Ordered);
        let first_gate = first.observation_gate();
        let second_gate = second.observation_gate();
        assert!(!first_gate.same_gate(&second_gate));

        let held = first_gate.lock();
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
    fn snapshot_subscription_waker_can_reenter_snapshot() {
        let scope = isolated_scope("scope", ScopeFlavor::Ordered);
        let handle = ScopeRef {
            cell: Arc::clone(&scope),
        };
        let mut snapshots = handle.subscribe_snapshots();
        let (waker, observed) = snapshot_reentry_waker(&scope);
        let mut changed = Box::pin(snapshots.changed());
        assert!(matches!(
            changed.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));

        let publisher = std::thread::spawn(move || scope.set_state(ScopeState::Starting));
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(2)),
            Ok(ScopeState::Starting),
            "the watch waker must run only after snapshot can reacquire the gate"
        );
        publisher.join().expect("snapshot publication completes");
        assert!(matches!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
    }

    #[test]
    fn lifecycle_subscription_waker_can_reenter_snapshot() {
        let scope = isolated_scope("scope", ScopeFlavor::Ordered);
        let handle = ScopeRef {
            cell: Arc::clone(&scope),
        };
        let mut events = handle.subscribe_lifecycle();
        let (waker, observed) = snapshot_reentry_waker(&scope);
        let mut next = Box::pin(events.recv());
        assert!(matches!(
            next.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));

        let publisher = std::thread::spawn(move || scope.set_state(ScopeState::Starting));
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(2)),
            Ok(ScopeState::Starting),
            "the lifecycle waker must run only after snapshot can reacquire the gate"
        );
        publisher.join().expect("lifecycle publication completes");
        assert!(matches!(
            next.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Some(_))
        ));
    }

    #[test]
    fn scope_wait_waker_can_reenter_snapshot_at_terminality() {
        let scope = isolated_scope("scope", ScopeFlavor::Ordered);
        let handle = ScopeRef {
            cell: Arc::clone(&scope),
        };
        let mut stopped = Box::pin(handle.wait_stopped());
        let (stopped_waker, stopped_observed) = snapshot_reentry_waker(&scope);
        assert!(matches!(
            stopped
                .as_mut()
                .poll(&mut Context::from_waker(&stopped_waker)),
            Poll::Pending
        ));

        let terminalizer = std::thread::spawn(move || scope.terminalize_never_started());
        assert!(matches!(
            stopped_observed.recv_timeout(Duration::from_secs(2)),
            Ok(ScopeState::Stopped { .. })
        ));
        terminalizer.join().expect("terminal publication completes");
        assert!(matches!(
            stopped
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(_)
        ));
    }

    #[test]
    fn observation_gate_poison_does_not_wedge_later_observation() {
        let scope = isolated_scope("scope", ScopeFlavor::Ordered);
        let gate = scope.observation_gate();
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _held = gate.lock();
                panic!("inject observation failure");
            }))
            .is_err()
        );

        scope.set_state(ScopeState::Starting);
        assert_eq!(scope.record().state, ScopeState::Starting);
    }

    #[test]
    fn stale_scope_driver_cannot_stop_a_newer_live_incarnation_projection() {
        let scope = isolated_scope("scope", ScopeFlavor::Ordered);
        let first = scope
            .begin_incarnation()
            .expect("first scope epoch is available");
        scope.finish_incarnation(first, StopReason::Finished);
        let second = scope
            .begin_incarnation()
            .expect("second scope epoch is available");
        scope.set_state(ScopeState::Running);

        scope.finish_incarnation(first, StopReason::Finished);
        assert_eq!(scope.record().state, ScopeState::Running);
        assert!(!scope.incarnation_finished(second));

        scope.finish_incarnation(second, StopReason::Finished);
        assert_eq!(
            scope.record().state,
            ScopeState::Stopped {
                reason: StopReason::Finished,
            }
        );
        assert!(scope.incarnation_finished(second));
    }

    #[crate::runtime::test]
    async fn terminal_scope_waits_for_its_live_incarnation_to_stop() {
        let parent = isolated_scope("parent", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Ordered);
        let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
        parent.set_admitted_children(vec![resident_projection(&slot)]);

        let epoch = nested
            .begin_incarnation()
            .expect("nested scope epoch is available");
        let mut incarnations = ScopeIdentity::new().incarnation_counter(nested.member.membership());
        let incarnation = incarnations.mint().expect("child incarnation is available");
        nested.member.update(|record| {
            record.stage = MemberStage::Running;
            record.incarnation = Some(incarnation);
            record.last_incarnation = Some(incarnation);
        });
        nested.set_state(ScopeState::Running);

        assert!(parent.terminalize_child(
            &nested.member,
            Exit::new(
                ExitKind::Aborted {
                    phase: GracePhase::WithinGrace,
                },
                Cancellation::Observed,
            ),
            Some(incarnation),
            StartupDisposition::NotAborted,
        ));

        let mut waiter = Box::pin(nested.wait_stopped());
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
        assert!(
            first_poll.is_pending(),
            "membership terminality does not imply that its live scope incarnation stopped"
        );

        nested.finish_incarnation(epoch, StopReason::ShutdownRequested);
        assert_eq!(waiter.await, StopReason::ShutdownRequested);
    }

    #[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocked_initial_scope_factory_owns_its_stop_epilogue() {
        let gate = Arc::new(FactoryGate::default());
        let mut tree = Tree::new();
        let nested = tree
            .add_subtree(
                "nested",
                SubtreeDef::factory({
                    let gate = Arc::clone(&gate);
                    move || {
                        gate.block();
                        pending_tree()
                    }
                }),
            )
            .expect("nested scope is valid");
        let plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = ScopeEpochGuard::begin(&root).expect("parent epoch is available");
        let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
        let abort = driver.abort_handle();

        assert!(matches!(
            crate::runtime::timeout(Duration::from_secs(2), gate.wait_entered()).await,
            crate::runtime::Timeout::Completed(())
        ));
        let factory_state = nested.snapshot().state.clone();

        abort.abort();
        let parent_join = crate::runtime::join(driver).await;
        let mut waiter = Box::pin(nested.wait_stopped());
        let before_release =
            std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
        gate.release();
        assert_eq!(factory_state, ScopeState::Starting);
        assert!(matches!(
            parent_join,
            crate::runtime::JoinOutcome::Cancelled
        ));
        assert!(
            before_release.is_pending(),
            "an executing initial factory still owns the final scope epilogue"
        );
        assert!(matches!(
            crate::runtime::timeout(Duration::from_secs(2), waiter).await,
            crate::runtime::Timeout::Completed(StopReason::ShutdownRequested)
        ));
    }

    #[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocked_restart_scope_factory_supersedes_the_stale_stopped_projection() {
        let gate = Arc::new(FactoryGate::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tree = Tree::new();
        let nested = tree
            .add_subtree(
                "nested",
                SubtreeDef::factory({
                    let gate = Arc::clone(&gate);
                    let calls = Arc::clone(&calls);
                    move || {
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            finished_tree()
                        } else {
                            gate.block();
                            pending_tree()
                        }
                    }
                })
                .restart(RestartPolicy::new(
                    RestartCondition::Always,
                    Backoff::Immediate,
                )),
            )
            .expect("restartable nested scope is valid");
        let plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = ScopeEpochGuard::begin(&root).expect("parent epoch is available");
        let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
        let abort = driver.abort_handle();

        assert!(matches!(
            crate::runtime::timeout(Duration::from_secs(2), gate.wait_entered()).await,
            crate::runtime::Timeout::Completed(())
        ));
        let factory_calls = calls.load(Ordering::SeqCst);
        let factory_state = nested.snapshot().state.clone();

        abort.abort();
        let parent_join = crate::runtime::join(driver).await;
        let mut waiter = Box::pin(nested.wait_stopped());
        let before_release =
            std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
        gate.release();
        assert_eq!(factory_calls, 2);
        assert_eq!(
            factory_state,
            ScopeState::Starting,
            "the second epoch supersedes the first incarnation's Stopped projection before its factory runs"
        );
        assert!(matches!(
            parent_join,
            crate::runtime::JoinOutcome::Cancelled
        ));
        assert!(
            before_release.is_pending(),
            "an executing restart factory still owns the final scope epilogue"
        );
        assert!(matches!(
            crate::runtime::timeout(Duration::from_secs(2), waiter).await,
            crate::runtime::Timeout::Completed(StopReason::ShutdownRequested)
        ));
    }

    #[crate::runtime::test]
    async fn panicking_nested_factory_releases_its_pre_driver_epoch() {
        let scope = isolated_scope("nested", ScopeFlavor::Ordered);
        let driver_scope = Arc::clone(&scope);
        let driver = crate::runtime::spawn(async move {
            let factory = Arc::new(|| -> crate::plan::BuilderCore {
                panic!("injected nested factory panic");
            });
            run_nested_factory(
                factory,
                driver_scope,
                crate::policy::ResolvedDefaults::default(),
                NestedScopeLatches {
                    parent_ready: CompletionGatedLatch::default(),
                    ancestor: AncestorCommandLatches {
                        shutdown: Latch::default(),
                        abort: Latch::default(),
                        abort_ack: Latch::default(),
                    },
                },
            )
            .await
        });

        assert!(matches!(
            crate::runtime::join(driver).await,
            crate::runtime::JoinOutcome::Panic { .. }
        ));
        assert_eq!(
            scope.record().state,
            ScopeState::Stopped {
                reason: StopReason::ShutdownRequested,
            }
        );
        let successor = ScopeEpochGuard::begin(&scope)
            .expect("factory unwind retires the reserved scope epoch");
        successor.finish(StopReason::NeverStarted);
    }

    #[test]
    fn pre_driver_epoch_guard_releases_on_cancellation_and_unwind() {
        let cancelled = isolated_scope("cancelled", ScopeFlavor::Ordered);
        let guard = ScopeEpochGuard::begin(&cancelled).expect("first epoch is available");
        let mut setup = Box::pin(async move {
            let _guard = guard;
            future::pending::<()>().await;
        });
        assert!(
            setup
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(setup);
        let successor = ScopeEpochGuard::begin(&cancelled)
            .expect("cancelling pre-driver setup retires its epoch");
        successor.finish(StopReason::NeverStarted);

        let unwound = isolated_scope("unwound", ScopeFlavor::Ordered);
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _guard = ScopeEpochGuard::begin(&unwound).expect("unwind epoch is available");
                panic!("injected pre-driver unwind");
            }))
            .is_err()
        );
        let successor =
            ScopeEpochGuard::begin(&unwound).expect("unwinding pre-driver setup retires its epoch");
        successor.finish(StopReason::NeverStarted);
    }

    #[test]
    fn a_declined_epoch_still_publishes_its_owned_terminal_exit() {
        let scope = isolated_scope("scope", ScopeFlavor::Ordered);
        let epoch = scope.begin_incarnation().expect("scope epoch is available");
        // The orderly finisher retires the epoch without a terminal exit, so
        // a second owner still holds the only membership verdict.
        scope.finish_incarnation(epoch, StopReason::Finished);
        assert!(!matches!(
            scope.member.record().stage,
            MemberStage::Terminal(_)
        ));

        scope.finish_root_incarnation(
            epoch,
            StopReason::ShutdownRequested,
            Exit::new(
                ExitKind::Aborted {
                    phase: GracePhase::WithinGrace,
                },
                Cancellation::Observed,
            ),
        );
        assert!(matches!(
            scope.member.record().stage,
            MemberStage::Terminal(_)
        ));
        assert_eq!(
            scope.record().state,
            ScopeState::Stopped {
                reason: StopReason::Finished,
            },
            "a declined epoch must not rewrite the retired stop reason"
        );
    }

    #[test]
    fn admitted_subtrees_share_their_parent_observation_gate() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));

        root.set_admitted_children(vec![resident_projection(&slot)]);

        assert!(
            root.observation_gate()
                .same_gate(&nested.observation_gate())
        );
    }

    #[test]
    fn admitted_subtree_rehomes_existing_descendants_to_one_gate() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let leaf = isolated_scope("leaf", ScopeFlavor::Ordered);
        let leaf_slot = SlotCell::new(Arc::clone(&leaf.member), Some(Arc::clone(&leaf)));
        nested.set_admitted_children(vec![resident_projection(&leaf_slot)]);
        assert!(
            nested
                .observation_gate()
                .same_gate(&leaf.observation_gate())
        );

        let nested_slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
        root.set_admitted_children(vec![resident_projection(&nested_slot)]);

        let root_gate = root.observation_gate();
        assert!(root_gate.same_gate(&nested.observation_gate()));
        assert!(root_gate.same_gate(&leaf.observation_gate()));
    }

    #[test]
    fn pre_admission_observer_retries_after_gate_handoff() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let captures = nested.probe_gate_captures();
        let prior_gate = nested.observation_gate();
        let held = prior_gate.lock();
        let observer = Arc::clone(&nested);
        let worker = std::thread::spawn(move || observer.set_state(ScopeState::Starting));

        // The capture report proves the observer committed to the
        // pre-admission gate, which the held guard keeps it from acquiring.
        assert_eq!(
            captures
                .recv_timeout(CAPTURE_PROBE_WAIT)
                .expect("the observer reports its capture within the bound"),
            GateCapture::Observation
        );

        // Model the instant at which adoption owns the old gate and publishes
        // the replacement. The waiting observer must acquire the old gate,
        // detect this handoff, and retry on the root gate.
        nested.replace_observation_gate(root.observation_gate());
        drop(held);
        worker.join().expect("observer follows the gate handoff");

        assert_eq!(
            captures
                .recv_timeout(CAPTURE_PROBE_WAIT)
                .expect("the observer reports its retry within the bound"),
            GateCapture::Observation,
            "the handoff forces one retry capture on the root gate"
        );
        assert_eq!(nested.record().state, ScopeState::Starting);
        assert!(
            root.observation_gate()
                .same_gate(&nested.observation_gate())
        );
    }

    #[crate::runtime::test]
    async fn force_uses_the_stop_funnel_for_every_ordered_child() {
        let mut tree = Tree::new();
        let first = tree
            .add_raw("first", crate::RawDef::factory(|| PendingRaw))
            .expect("valid first actor");
        let second = tree
            .add_raw("second", crate::RawDef::factory(|| PendingRaw))
            .expect("valid second actor");
        let mut plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
        let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let mut children = ChildArena::default();
        plan.children.reverse();
        while let Some(child) = plan.children.pop() {
            children
                .insert(ChildRuntime::from_plan(child, &root))
                .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
        }
        let keys = children.keys().collect::<Vec<_>>();
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: plan.defaults.clone(),
            intensity_policy: plan.config.intensity,
            intensity: super::IntensityState::default(),
            children,
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::running(),
            next_ordered_start: None,
            role: ScopeRole::Root,
            dynamic: None,
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };
        plan.armed = false;
        drop(plan);

        for key in &keys {
            scope.spawn_child(*key);
        }
        let incarnations = keys
            .iter()
            .map(|key| {
                scope.children[*key]
                    .active
                    .as_ref()
                    .expect("child is active")
                    .incarnation
            })
            .collect::<Vec<_>>();

        scope.force_all();

        for key in &keys {
            let active = scope.children[*key]
                .active
                .as_ref()
                .expect("forced child remains active through the tidy beat");
            assert!(active.shutdown.is_fired(), "force sends cancellation");
            assert!(active.abort.is_fired(), "force immediately escalates");
        }
        assert_eq!(
            first.try_send(1).expect_err("first mailbox freezes").kind,
            SendErrorKind::NotRunning
        );
        assert_eq!(
            second.try_send(2).expect_err("second mailbox freezes").kind,
            SendErrorKind::NotRunning
        );

        // Model readiness messages that shared the driver's wake with force.
        // The force boundary disarmed both gates before either can publish a
        // late Running transition.
        for (key, incarnation) in keys.iter().zip(incarnations) {
            scope.handle_ready(*key, incarnation);
            assert!(matches!(
                scope.children[*key].slot.member.record().stage,
                MemberStage::Stopping
            ));
        }

        let deadlines = keys
            .iter()
            .map(|key| {
                scope.children[*key]
                    .active
                    .as_ref()
                    .and_then(|active| active.ladder)
                    .and_then(StopLadder::deadline)
                    .expect("each ladder retains its tidy deadline")
            })
            .collect::<Vec<_>>();
        scope.force_all();
        for (key, deadline) in keys.iter().zip(deadlines) {
            assert_eq!(
                scope.children[*key]
                    .active
                    .as_ref()
                    .and_then(|active| active.ladder)
                    .and_then(StopLadder::deadline),
                Some(deadline),
                "repeated force cannot rewind or skip the ladder"
            );
        }
    }

    #[test]
    fn forced_ordered_drain_advances_an_inactive_suffix_iteratively() {
        const CHILDREN: usize = 1_024;

        let mut tree = Tree::new();
        for index in 0..CHILDREN {
            tree.add_task(
                format!("inactive-{index}"),
                TaskDef::new(|_| future::pending()),
            )
            .expect("unique child declaration");
        }
        let mut plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
        let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let mut children = ChildArena::default();
        plan.children.reverse();
        while let Some(child) = plan.children.pop() {
            children
                .insert(ChildRuntime::from_plan(child, &root))
                .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
        }
        // Model restart-window children: no incarnation and no retained
        // construction remains, so forced terminalization completes inline.
        // This used to re-enter `stop_next_ordered` once per child.
        for child in children.values_mut() {
            drop(child.construction.take());
        }
        let mut scope = ScopeRuntime {
            root,
            defaults: plan.defaults.clone(),
            intensity_policy: plan.config.intensity,
            intensity: super::IntensityState::default(),
            children,
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::running(),
            next_ordered_start: None,
            role: ScopeRole::Root,
            dynamic: None,
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: true,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };
        plan.armed = false;
        drop(plan);

        scope.begin_drain(StopReason::ShutdownRequested);

        assert!(scope.children.values().all(ChildRuntime::is_terminal));
        assert!(!scope.ordered_stop_progressing);
        assert_eq!(scope.ordered_stop_waiting, None);
        assert_eq!(
            scope.ordered_stop_inspections, CHILDREN,
            "the reverse cursor inspects each ordered child exactly once"
        );
    }

    #[crate::runtime::test]
    async fn same_batch_intensity_exit_suppresses_real_expedited_factory() {
        let factories = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut tree = Tree::new();
        tree.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
        tree.add_subtree(
            "nested",
            SubtreeDef::factory({
                let factories = Arc::clone(&factories);
                move || {
                    factories.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Tree::new()
                }
            }),
        )
        .expect("valid subtree");
        tree.add_task("trip", TaskDef::new(|_| future::pending()))
            .expect("valid task");

        let mut plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
        let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let mut children = ChildArena::default();
        plan.children.reverse();
        while let Some(child) = plan.children.pop() {
            children
                .insert(ChildRuntime::from_plan(child, &root))
                .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
        }
        let nested = children
            .keys()
            .find(|key| children[*key].slot.member.id().as_str() == "nested")
            .expect("nested child key");
        let trip = children
            .keys()
            .find(|key| children[*key].slot.member.id().as_str() == "trip")
            .expect("tripping child key");
        let next_ordered_start = children.keys().next();
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: plan.defaults.clone(),
            intensity_policy: plan.config.intensity,
            intensity: super::IntensityState::default(),
            children,
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::running(),
            next_ordered_start,
            role: ScopeRole::Root,
            dynamic: None,
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };
        plan.armed = false;
        drop(plan);

        root.transition_child(
            &scope.children[nested].slot.member,
            |record| {
                record.incarnation = None;
                record.stage = MemberStage::Restarting;
            },
            None,
        );
        let _ = scope.children[nested]
            .slot
            .scope
            .as_ref()
            .expect("nested scope cell")
            .request_shutdown();
        assert_eq!(scope.pending_restart_shutdowns(), vec![nested]);

        scope.spawn_child(trip);
        let incarnation = scope.children[trip]
            .active
            .as_ref()
            .expect("tripping child is active")
            .incarnation;
        scope.children[trip]
            .active
            .as_ref()
            .expect("tripping child is active")
            .abort_handle
            .abort();
        let exit = DriverEvent::Child(ChildEvent::Exited {
            child: trip,
            incarnation,
            recorded: Some(RecordedOutcome::Returned(Err(ExitError::message(
                "trip intensity",
            )))),
            join: JoinVerdict::Completed,
            cancellation: Cancellation::NotObserved,
            readiness_signal_seen: false,
        });
        let mut pending = [
            restart_shutdown_work(nested),
            Pending::Driver(exit).classified(),
        ];
        arbitrate(&mut pending);
        for (_, event) in pending {
            match event {
                Pending::RestartShutdown(child) => scope.expedite_restart_shutdown(child),
                Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                })) => scope.handle_exit(
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                ),
                _ => unreachable!("the fixture queues only exit and restart work"),
            }
        }

        crate::runtime::yield_now().await;
        crate::runtime::yield_now().await;

        assert!(scope.lifecycle.is_draining());
        assert_eq!(
            factories.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the production guard must suppress the expedited factory after intensity drain"
        );
    }

    /// A `mark_ready(); stop()` child reports its local stop and exit on
    /// helper tasks, so one driver wake can collect both while the fired
    /// readiness latch's Ready event is still undrained — and arbitration
    /// orders the stop ahead of the readiness signal. `handle_self_stop`
    /// must consult the fired latch before `begin_stop_child`'s Shutdown
    /// step disarms the gate, or the clean post-ready exit is misread as a
    /// pre-ready stop and spuriously aborts startup.
    #[crate::runtime::test]
    async fn same_batch_self_stop_preserves_fired_readiness_for_startup() {
        let mut tree = Tree::new();
        tree.add_task(
            "ready-then-stop",
            TaskDef::new(|_| future::pending::<crate::ExitResult>())
                .readiness(Readiness::Manual)
                .expect("manual readiness is valid")
                .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid task");
        let mut plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
        let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
        let mut children = ChildArena::default();
        let key = children
            .insert(child)
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: plan.defaults.clone(),
            intensity_policy: plan.config.intensity,
            intensity: super::IntensityState::default(),
            children,
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::starting(),
            next_ordered_start: Some(key),
            role: ScopeRole::Root,
            dynamic: None,
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };
        plan.armed = false;
        drop(plan);

        scope.spawn_child(key);
        let active = scope.children[key]
            .active
            .as_ref()
            .expect("spawned child is active");
        let incarnation = active.incarnation;
        // The application task fired its readiness latch before stopping;
        // the driver has not yet drained the corresponding Ready event.
        assert!(active.ready_signal.fire());
        active.abort_handle.abort();

        let mut pending = [
            Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                child: key,
                incarnation,
                recorded: Some(RecordedOutcome::Returned(Ok(()))),
                join: JoinVerdict::Completed,
                cancellation: Cancellation::NotObserved,
                readiness_signal_seen: true,
            }))
            .classified(),
            Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop {
                child: key,
                incarnation,
            }))
            .classified(),
        ];
        arbitrate(&mut pending);
        assert!(
            matches!(
                pending[0].1,
                Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop { .. }))
            ),
            "the regression premise: arbitration orders the stop ahead of the exit"
        );
        for (_, event) in pending {
            match event {
                Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop {
                    child,
                    incarnation,
                })) => scope.handle_self_stop(child, incarnation),
                Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                })) => scope.handle_exit(
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                ),
                _ => unreachable!("the fixture queues only the stop and the exit"),
            }
        }

        let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
            disposal_event_receiver
                .recv()
                .await
                .expect("disposal reports completion")
        else {
            panic!("only construction disposal was armed")
        };
        scope.handle_construction_disposed(child, panic);

        assert!(
            scope.lifecycle.startup_complete(),
            "the ready-before-stop child completes startup"
        );
        assert!(
            matches!(root.record().startup, Some(Ok(()))),
            "a fired readiness latch must survive a same-batch local stop: {:?}",
            root.record().startup
        );
        assert_eq!(root.record().state, ScopeState::Running);
        assert!(matches!(
            scope.children[key].slot.member.record().stage,
            MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::Completed)
        ));
        assert!(
            !scope.children[key].slot.member.record().startup_aborted,
            "a post-ready clean self-stop is not a startup abort"
        );
    }

    #[test]
    fn gate_handoff_waits_for_an_in_flight_observation_edge() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
        let captures = nested.probe_gate_captures();
        let (entered, entered_receiver) = std::sync::mpsc::sync_channel(0);
        let (release, release_receiver) = std::sync::mpsc::sync_channel(0);
        let observer = Arc::clone(&nested);
        let observation = std::thread::spawn(move || {
            observer.with_observation_gate(|_| {
                entered.send(()).expect("test receiver remains available");
                release_receiver
                    .recv()
                    .expect("test sender releases the observation edge");
            });
        });
        entered_receiver
            .recv()
            .expect("observer enters the pre-admission edge");
        assert_eq!(
            captures
                .recv_timeout(CAPTURE_PROBE_WAIT)
                .expect("the observation edge reports its capture within the bound"),
            GateCapture::Observation
        );

        let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
        let adopting_root = Arc::clone(&root);
        let (adopted, adopted_receiver) = std::sync::mpsc::sync_channel(0);
        let adoption = std::thread::spawn(move || {
            adopting_root.set_admitted_children(vec![resident_projection(&slot)]);
            adopted.send(()).expect("test receiver remains available");
        });

        // The adoption capture proves handoff committed to the prior gate and
        // is blocked behind the complete observation edge rather than
        // replacing it concurrently, so adoption cannot yet have completed.
        assert_eq!(
            captures
                .recv_timeout(CAPTURE_PROBE_WAIT)
                .expect("adoption reports its capture within the bound"),
            GateCapture::Adoption
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
        assert!(
            root.observation_gate()
                .same_gate(&nested.observation_gate())
        );
    }

    #[test]
    fn removed_child_keys_are_never_reused() {
        let mut tree = Tree::new();
        tree.add_task("worker", TaskDef::new(|_| future::pending()))
            .expect("valid task");
        let mut plan = tree.lower_for_test();
        let child =
            ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &plan.root);
        let mut arena = ChildArena::default();
        let stale = arena.insert(child).unwrap_or_else(|_| panic!("key mints"));
        let child = arena.remove(stale).expect("live key removes its child");
        let current = arena.insert(child).unwrap_or_else(|_| panic!("key mints"));

        assert!(current > stale, "keys advance monotonically across removal");
        assert!(arena.get(stale).is_none());
        assert!(arena.remove(stale).is_none());
        assert!(arena.get(current).is_some());
    }

    #[test]
    fn child_key_exhaustion_poison_is_never_minted() {
        let mut tree = Tree::new();
        tree.add_task("worker", TaskDef::new(|_| future::pending()))
            .expect("valid task");
        let mut plan = tree.lower_for_test();
        let child =
            ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &plan.root);
        let mut arena = ChildArena {
            next_key: u64::MAX - 2,
            ..ChildArena::default()
        };
        let last = arena
            .insert(child)
            .unwrap_or_else(|_| panic!("the last usable key mints"));
        assert_eq!(last, ChildKey(u64::MAX - 1));
        let child = arena.remove(last).expect("the last usable key is live");

        let child = *arena
            .insert(child)
            .expect_err("the poison key is never minted");
        assert_eq!(arena.next_key, u64::MAX);
        assert!(arena.get(ChildKey(u64::MAX)).is_none());
        assert!(
            arena.insert(child).is_err(),
            "the exhausted domain stays poisoned"
        );
    }

    #[crate::runtime::test(start_paused = true)]
    async fn dynamic_high_cycle_add_remove_keeps_only_live_runtime_storage() {
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
                "cycle {cycle} stores only the live child"
            );

            assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
            assert_eq!(
                cell.runtime_storage(),
                RuntimeStorage {
                    children: 0,
                    child_slots: 0,
                    deadlines: 0,
                    deadline_slots: 0,
                },
                "cycle {cycle} must release removed-child storage"
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
                    child_slots: 0,
                    deadlines: 0,
                    deadline_slots: 0,
                },
                "cycle {cycle} must release Retention::Remove storage"
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
        let readiness = CompletionGatedLatch::default();
        let (token, receiver) = report_channel(shutdown.clone(), None, readiness.clone());
        token.record(RecordedOutcome::Returned(Ok(())));
        shutdown.fire();
        readiness.fire();
        let report = receiver.receive();
        assert!(matches!(
            report.outcome,
            Some(RecordedOutcome::Returned(Ok(())))
        ));
        assert_eq!(report.cancellation, Cancellation::NotObserved);
        assert!(!report.readiness_signal_seen);

        let shutdown = Latch::default();
        let (token, receiver) =
            report_channel(shutdown.clone(), None, CompletionGatedLatch::default());
        shutdown.fire();
        drop(token);
        let report = receiver.receive();
        assert!(report.outcome.is_none());
        assert_eq!(report.cancellation, Cancellation::Observed);
    }

    #[test]
    fn owned_report_token_records_prior_cancellation() {
        let shutdown = Latch::default();
        let (token, receiver) =
            report_channel(shutdown.clone(), None, CompletionGatedLatch::default());
        shutdown.fire();
        token.record(RecordedOutcome::Returned(Ok(())));
        let report = receiver.receive();
        assert!(matches!(
            report.outcome,
            Some(RecordedOutcome::Returned(Ok(())))
        ));
        assert_eq!(report.cancellation, Cancellation::Observed);
    }

    #[test]
    fn owned_report_token_records_prior_local_stop() {
        let shutdown = Latch::default();
        let local_stop = Latch::default();
        let (token, receiver) = report_channel(
            shutdown,
            Some(local_stop.clone()),
            CompletionGatedLatch::default(),
        );
        local_stop.fire();
        token.record(RecordedOutcome::Returned(Ok(())));
        assert_eq!(receiver.receive().cancellation, Cancellation::Observed);
    }

    #[test]
    fn owned_report_token_records_readiness_at_completion() {
        let readiness = CompletionGatedLatch::default();
        let (token, receiver) = report_channel(Latch::default(), None, readiness.clone());
        readiness.fire();
        token.record(RecordedOutcome::Returned(Ok(())));
        assert!(!readiness.fire(), "completion closes retained capabilities");
        assert!(receiver.receive().readiness_signal_seen);
    }

    #[test]
    fn handle_identity_is_stable_across_membership_rebase() {
        fn hashed(value: &impl std::hash::Hash) -> u64 {
            use std::hash::Hasher;
            let mut hasher = std::hash::DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        }

        let mut identity = ScopeIdentity::new();
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
        let mut identity = ScopeIdentity::new();
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
        let plan = tree.lower_for_test();
        let scope = Arc::clone(&plan.root);
        let epoch = ScopeEpochGuard::begin(&scope).expect("test scope epoch is available");
        let driver = crate::runtime::spawn(run_scope_incarnation(
            plan,
            ScopeRole::Nested(NestedScopeLatches {
                parent_ready: CompletionGatedLatch::default(),
                ancestor: AncestorCommandLatches {
                    shutdown: Latch::default(),
                    abort: Latch::default(),
                    abort_ack: Latch::default(),
                },
            }),
            epoch,
        ));
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
        let plan = Tree::new().lower_for_test();
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
        let plan = outer.lower_for_test();

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
        let plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let mut events = root.subscribe_lifecycle();
        let children: Vec<_> = plan
            .children
            .iter()
            .map(|child| Arc::clone(&child.slot.member))
            .collect();
        let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");

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
            plan,
            ScopeRole::Nested(NestedScopeLatches {
                parent_ready: CompletionGatedLatch::default(),
                ancestor: AncestorCommandLatches {
                    shutdown: Latch::default(),
                    abort: Latch::default(),
                    abort_ack: Latch::default(),
                },
            }),
            epoch,
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
        assert_eq!(
            removed, 0,
            "conversion must finish before publication begins"
        );
    }

    struct ReserveOnLifecycleWake {
        scope: Arc<ScopeCell>,
        result: Mutex<Option<Result<(), ReserveError>>>,
        observed: Latch,
    }

    impl ReserveOnLifecycleWake {
        fn observe(&self) {
            let mut result = self.result.lock().expect("observation mutex poisoned");
            if result.is_none() {
                *result = Some(
                    reserve_dynamic(&self.scope, ChildId::from("reentrant"), None).map(
                        |reservation| {
                            cancel_dynamic_reservation(
                                reservation.control.as_ref(),
                                &reservation.slot,
                            );
                        },
                    ),
                );
                self.observed.fire();
            }
        }
    }

    impl Wake for ReserveOnLifecycleWake {
        fn wake(self: Arc<Self>) {
            self.observe();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.observe();
        }
    }

    #[crate::runtime::test]
    async fn initial_added_wake_observes_the_keyed_dynamic_route() {
        let mut tree = DynamicTree::new();
        tree.add_task("initial", TaskDef::new(|_| future::pending()))
            .expect("valid task");
        let plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let mut events = root.subscribe_lifecycle();
        let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
        assert!(events.recv().await.is_some(), "Starting is observed first");

        let probe = Arc::new(ReserveOnLifecycleWake {
            scope: Arc::clone(&root),
            result: Mutex::new(None),
            observed: Latch::default(),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut added = Box::pin(events.recv());
        assert!(
            added
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
        let abort = driver.abort_handle();
        assert!(matches!(
            crate::runtime::timeout(Duration::from_secs(1), probe.observed.fired()).await,
            crate::runtime::Timeout::Completed(())
        ));
        drop(added);
        assert!(matches!(
            probe
                .result
                .lock()
                .expect("observation mutex poisoned")
                .as_ref(),
            Some(Ok(()))
        ));
        abort.abort();
        assert!(matches!(
            crate::runtime::join(driver).await,
            crate::runtime::JoinOutcome::Cancelled
        ));
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
        epoch: Option<Epoch>,
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

    #[derive(Clone, Copy, Debug)]
    enum TerminalStopPath {
        LiveEpoch,
        NoLiveEpoch,
        NeverStarted,
    }

    #[crate::runtime::test]
    async fn terminal_stop_paths_share_one_complete_observation_transition() {
        for (path, reason) in [
            (TerminalStopPath::LiveEpoch, StopReason::ShutdownRequested),
            (TerminalStopPath::NoLiveEpoch, StopReason::ShutdownRequested),
            (TerminalStopPath::NeverStarted, StopReason::NeverStarted),
        ] {
            let scope = isolated_scope("root", ScopeFlavor::Ordered);
            let epoch = matches!(path, TerminalStopPath::LiveEpoch).then(|| {
                scope
                    .begin_incarnation()
                    .expect("test scope epoch is available")
            });
            let handle = ScopeRef {
                cell: Arc::clone(&scope),
            };
            let mut snapshots = handle.subscribe_snapshots();
            let mut events = handle.subscribe_lifecycle();

            match path {
                TerminalStopPath::LiveEpoch => scope.finish_root_incarnation(
                    epoch.expect("live path owns an epoch"),
                    reason.clone(),
                    Exit::never_started(),
                ),
                TerminalStopPath::NoLiveEpoch => {
                    scope.finish_live_root_incarnation(reason.clone(), Exit::never_started())
                }
                TerminalStopPath::NeverStarted => scope.terminalize_never_started(),
            }

            let expected_state = ScopeState::Stopped {
                reason: reason.clone(),
            };
            assert!(matches!(
                scope.member.record().stage,
                MemberStage::Terminal(_)
            ));
            assert_eq!(scope.record().state, expected_state);
            assert_eq!(
                epoch.map(|epoch| scope.incarnation_finished(epoch)),
                epoch.map(|_| true)
            );
            assert_eq!(snapshots.borrow_latest().state, expected_state);
            assert!(matches!(
                events.try_recv(),
                Ok(LifecycleItem::Event(crate::LifecycleEvent {
                    kind: LifecycleEventKind::ScopeState { state },
                    ..
                })) if state == expected_state
            ));
            assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));

            snapshots
                .changed()
                .await
                .expect("the final snapshot precedes observation closure");
            assert!(snapshots.changed().await.is_err());
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
    fn stopped_publication_keeps_mailbox_panic_primary_and_finishes_observation() {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let epoch = scope
            .begin_incarnation()
            .expect("test scope epoch is available");
        let handle = ScopeRef {
            cell: Arc::clone(&scope),
        };
        let mut events = handle.subscribe_lifecycle();
        let mailbox = MailboxCell::new(scope.member.id().clone());
        let actor: ActorRef<u8> = ActorRef::new(Arc::clone(&scope.member), Arc::clone(&mailbox));
        scope.member.attach_mailbox(mailbox);

        let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
        let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
            &mailbox_payload_dropped,
        ))));
        let mut parked_send = Box::pin(actor.send(1));
        assert!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(&mailbox_waker))
                .is_pending()
        );

        let terminal_payload_dropped = Arc::new(AtomicUsize::new(0));
        let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
            &terminal_payload_dropped,
        ))));
        let mut terminal = Box::pin(scope.member.wait_terminal());
        assert!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(&terminal_waker))
                .is_pending()
        );

        let payload = catch_unwind(AssertUnwindSafe(|| {
            scope.finish_root_incarnation(
                epoch,
                StopReason::ShutdownRequested,
                Exit::never_started(),
            );
        }))
        .expect_err("the primary mailbox panic still surfaces");
        assert_eq!(
            mailbox_payload_dropped.load(Ordering::SeqCst),
            0,
            "the mailbox panic payload remains owned by the caller"
        );
        assert!(
            terminal_payload_dropped.load(Ordering::SeqCst) > 0,
            "later member-pulse panics are contained as cleanup"
        );
        assert!(matches!(
            scope.record().state,
            ScopeState::Stopped {
                reason: StopReason::ShutdownRequested
            }
        ));
        assert!(scope.incarnation_finished(epoch));
        assert!(matches!(
            events.try_recv(),
            Ok(LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::Stopped {
                        reason: StopReason::ShutdownRequested
                    }
                },
                ..
            }))
        ));
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
        assert!(matches!(
            scope.member.record().stage,
            MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        drop(payload);
        assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn panicking_mailbox_waker_cannot_skip_the_terminal_pulse() {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        member.attach_mailbox(mailbox);

        let mut parked_send = Box::pin(actor.send(1));
        let panicking_waker = Waker::from(Arc::new(PanicWake("injected mailbox waker panic")));
        assert!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(&panicking_waker))
                .is_pending()
        );

        let wakes = Arc::new(AtomicUsize::new(0));
        let terminal_waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut terminal = Box::pin(member.wait_terminal());
        assert!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(&terminal_waker))
                .is_pending()
        );

        catch_unwind(AssertUnwindSafe(|| {
            member.terminalize(Exit::never_started());
        }))
        .expect_err("the hostile mailbox waker still surfaces its panic");
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "membership terminality is pulsed before the mailbox panic resumes"
        );
        assert!(matches!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        assert!(matches!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
        ));
    }

    #[test]
    fn mailbox_teardown_panic_precedes_a_terminal_pulse_panic() {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        member.attach_mailbox(mailbox);

        let mut parked_send = Box::pin(actor.send(1));
        let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
        let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
            &mailbox_payload_dropped,
        ))));
        assert!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(&mailbox_waker))
                .is_pending()
        );

        let mut terminal = Box::pin(member.wait_terminal());
        let pulse_payload_dropped = Arc::new(AtomicUsize::new(0));
        let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
            &pulse_payload_dropped,
        ))));
        assert!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(&terminal_waker))
                .is_pending()
        );

        let payload = catch_unwind(AssertUnwindSafe(|| {
            member.terminalize(Exit::never_started());
        }))
        .expect_err("the primary mailbox panic still surfaces");
        assert_eq!(
            mailbox_payload_dropped.load(Ordering::SeqCst),
            0,
            "the primary mailbox payload is retained for the caller"
        );
        assert_eq!(
            pulse_payload_dropped.load(Ordering::SeqCst),
            1,
            "the membership-pulse panic is cleanup and is contained"
        );
        drop(payload);
        assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
        assert!(matches!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        assert!(matches!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
        ));
    }

    #[crate::runtime::test]
    async fn mailbox_waker_panic_is_contained_without_wedging_system_completion() {
        let exit = Latch::default();
        let mut tree = Tree::new();
        let actor = tree
            .add_raw_once(
                "worker",
                RawOnceDef::new(ExitOnSignalRaw { exit: exit.clone() })
                    .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
            )
            .expect("valid actor");
        let plan = tree.lower_for_test();
        let member = Arc::clone(&plan.children[0].slot.member);
        let root = Arc::clone(&plan.root);
        let mut events = root.subscribe_lifecycle();
        let mut system = super::spawn_system(plan);
        root.wait_started().await.expect("actor starts");
        actor
            .try_send(1)
            .expect("the first message fills the queue");

        let mut parked_send = Box::pin(actor.send(2));
        let panicking_waker = Waker::from(Arc::new(PanicWake("injected mailbox waker panic")));
        assert!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(&panicking_waker))
                .is_pending()
        );

        let waiter_started = Latch::default();
        let terminal_waiter = crate::runtime::spawn({
            let member = Arc::clone(&member);
            let waiter_started = waiter_started.clone();
            async move {
                waiter_started.fire();
                member.wait_terminal().await
            }
        });
        waiter_started.fired().await;
        exit.fire();

        let member_exit = match crate::runtime::timeout(
            Duration::from_secs(2),
            crate::runtime::join(terminal_waiter),
        )
        .await
        {
            crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Ok { value }) => value,
            crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Panic { message }) => {
                panic!("the terminal waiter panicked: {message:?}")
            }
            crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Cancelled) => {
                panic!("the terminal waiter was cancelled")
            }
            crate::runtime::Timeout::Elapsed => {
                panic!("the terminal waiter was not pulsed after the mailbox panic")
            }
        };
        assert!(matches!(member_exit.kind(), ExitKind::Completed));

        let reason = match crate::runtime::timeout(Duration::from_secs(2), system.wait()).await {
            crate::runtime::Timeout::Completed(reason) => reason,
            crate::runtime::Timeout::Elapsed => {
                panic!("the system monitor did not contain the driver unwind")
            }
        };
        assert_eq!(reason, StopReason::ShutdownRequested);
        let root_exit = root.member.wait_terminal().await;
        assert!(matches!(
            root_exit.kind(),
            ExitKind::Panicked { message }
                if message.as_deref() == Some("injected mailbox waker panic")
        ));
        let mut terminal_trace = Vec::new();
        while let Some(item) = events.recv().await {
            let LifecycleItem::Event(event) = item else {
                panic!("the single-child terminal trace cannot lag");
            };
            match event.kind {
                LifecycleEventKind::Exited { .. } => terminal_trace.push("exited"),
                LifecycleEventKind::Removed { .. } => terminal_trace.push("removed"),
                _ => {}
            }
        }
        assert_eq!(
            terminal_trace,
            ["exited", "removed"],
            "mailbox panic resumes only after the terminal event, pruning edge, and stream closure"
        );
    }

    #[test]
    fn terminality_signal_follows_mailbox_termination() {
        let mut identity = ScopeIdentity::new();
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
        let mut watcher = member.record_watcher();
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
    fn supervised_terminality_pulse_follows_mailbox_termination() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        member.attach_mailbox(mailbox);

        let mailbox_wakes = Arc::new(AtomicUsize::new(0));
        let mailbox_waker = Waker::from(Arc::new(CountWake(Arc::clone(&mailbox_wakes))));
        let mut parked_send = Box::pin(actor.send(1));
        assert!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(&mailbox_waker))
                .is_pending()
        );

        let probe = Arc::new(ObserveWakeCount {
            wakes: Arc::clone(&mailbox_wakes),
            observed: Mutex::new(None),
        });
        let terminal_waker = Waker::from(Arc::clone(&probe));
        let mut terminal = Box::pin(member.wait_terminal());
        assert!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(&terminal_waker))
                .is_pending()
        );

        assert!(root.terminalize_child(
            &member,
            Exit::never_started(),
            None,
            StartupDisposition::Aborted,
        ));

        assert_eq!(
            *probe.observed.lock().expect("observation mutex poisoned"),
            Some(1),
            "the supervised terminal pulse must follow parked-sender discharge"
        );
        assert!(member.record().startup_aborted);
        assert!(matches!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        assert!(matches!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
        ));
    }

    #[test]
    fn supervised_mailbox_teardown_panic_precedes_terminal_pulse_panic() {
        let root = isolated_scope("root", ScopeFlavor::Ordered);
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        member.attach_mailbox(mailbox);

        let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
        let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
            &mailbox_payload_dropped,
        ))));
        let mut parked_send = Box::pin(actor.send(1));
        assert!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(&mailbox_waker))
                .is_pending()
        );

        let terminal_payload_dropped = Arc::new(AtomicUsize::new(0));
        let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
            &terminal_payload_dropped,
        ))));
        let mut terminal = Box::pin(member.wait_terminal());
        assert!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(&terminal_waker))
                .is_pending()
        );

        let payload = catch_unwind(AssertUnwindSafe(|| {
            root.terminalize_child(
                &member,
                Exit::never_started(),
                None,
                StartupDisposition::Aborted,
            );
        }))
        .expect_err("the primary mailbox panic still surfaces");
        assert_eq!(
            mailbox_payload_dropped.load(Ordering::SeqCst),
            0,
            "the primary mailbox payload is retained for the caller"
        );
        assert_eq!(
            terminal_payload_dropped.load(Ordering::SeqCst),
            1,
            "the later terminal-pulse panic is contained as cleanup"
        );
        drop(payload);
        assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
        assert!(matches!(
            terminal
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        assert!(matches!(
            parked_send
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
        ));
    }

    #[test]
    fn mailbox_wake_observes_terminal_record_and_reentrant_terminality_is_idempotent() {
        let mut identity = ScopeIdentity::new();
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
            competing_exit: Exit::new(ExitKind::Completed, Cancellation::NotObserved),
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
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
        let first_exit = Exit::never_started();
        member.stage_terminal_before_mailbox(first_exit.clone());
        let probe = Arc::new(ObserveMemberOnMailboxWake {
            member: Arc::clone(&member),
            competing_exit: Exit::new(ExitKind::Completed, Cancellation::NotObserved),
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
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let start = Arc::new(Barrier::new(3));
        let workers = [
            Exit::never_started(),
            Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        ]
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
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("root");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
        let epoch = scope
            .begin_incarnation()
            .expect("test scope epoch is available");
        let probe = Arc::new(ObserveScopeOnStartupWake {
            scope: Arc::clone(&scope),
            epoch: Some(epoch),
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut watcher = scope.record_watcher();
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
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("root");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
        let probe = Arc::new(ObserveScopeOnStartupWake {
            scope: Arc::clone(&scope),
            epoch: None,
            observed: Mutex::new(None),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut watcher = scope.record_watcher();
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
    fn dynamic_close_holds_removal_completion_through_observation_cleanup() {
        let mut identity = ScopeIdentity::new();
        let root_id = ChildId::from("root");
        let root_member = MemberCell::new(
            root_id.clone(),
            identity
                .mint_membership(&root_id)
                .expect("root membership available"),
        );
        let root = ScopeCell::new(root_member, ScopeFlavor::Dynamic, ScopeIdentity::new());
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
        root.set_admitted_children(vec![resident_projection(&slot)]);
        let (events, _receiver) = crate::runtime::bounded_mpsc(1);
        let control = DynamicControl::new(events);
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
                DynamicEntry::removing(slot, ChildKey(1), responses),
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
    async fn retained_unadmitted_slot_does_not_retain_the_request_forwarder() {
        let system = DynamicTree::new().spawn().expect("runtime is available");
        system.wait_started().await.expect("root starts");
        let scope = system.scope();
        let slot = scope
            .reserve_task("retained")
            .expect("unadmitted reservation is retained");
        let control = scope
            .as_scope()
            .cell
            .dynamic_route()
            .expect("the live dynamic scope exposes its control");
        let (forwarder_close, forwarder_ended) = control.request_forwarder_probe();

        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("driver teardown completes");

        assert!(forwarder_close.is_fired());
        assert!(matches!(
            crate::runtime::timeout(Duration::from_secs(1), forwarder_ended.fired()).await,
            crate::runtime::Timeout::Completed(())
        ));
        drop(slot);
        drop(control);
    }

    #[test]
    fn reserve_dynamic_rejects_an_empty_id_at_the_driver_boundary() {
        let root = isolated_scope("root", ScopeFlavor::Dynamic);

        assert!(matches!(
            super::reserve_dynamic(&root, ChildId::from(""), None),
            Err(crate::ReserveError::EmptyId)
        ));
    }

    #[test]
    fn dynamic_removal_releases_state_before_waiting_for_the_observation_gate() {
        let root = isolated_scope("root", ScopeFlavor::Dynamic);
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
        root.set_admitted_children(vec![resident_projection(&slot)]);
        let (events, _receiver) = crate::runtime::bounded_mpsc(1);
        let control = DynamicControl::new(events);
        let key = ChildKey(1);
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .insert(child_id.clone(), DynamicEntry::resident(slot, key, None));
        root.set_dynamic_route(Some(control.clone()));

        let captures = root.probe_gate_captures();
        let gate = root.observation_gate();
        let held_gate = gate.lock();
        let removal_root = Arc::clone(&root);
        let removal_id = child_id.clone();
        let worker =
            std::thread::spawn(move || super::remove_dynamic(&removal_root, &removal_id, None));

        // The capture report proves removal committed to the held observation
        // gate for its removing transition. Dynamic state cannot change again
        // until that gate is released, so a single acquisition attempt decides
        // whether removal reached the gate while still holding the state.
        assert_eq!(
            captures
                .recv_timeout(CAPTURE_PROBE_WAIT)
                .expect("removal reports its gate capture within the bound"),
            GateCapture::Observation
        );
        let state = control
            .state
            .try_lock()
            .expect("a removal waiting on observation must release dynamic state");
        let entry = state
            .entries
            .get(&child_id)
            .expect("the removal keeps its resident registration");
        assert!(entry.is_removing());
        assert!(entry.matches_key(key));
        drop(state);

        let route = root
            .dynamic_route()
            .expect("the fixture exposes its dynamic route");
        assert!(matches!(
            route.reserve(&root, child_id.clone(), None),
            Err(crate::ReserveError::RemovalInProgress(id)) if id == child_id
        ));

        drop(held_gate);
        let response = worker.join().expect("removal transition completes");
        drop(response);
    }

    #[crate::runtime::test]
    async fn saturated_removal_from_a_foreign_thread_reaches_the_driver() {
        let mut identity = ScopeIdentity::new();
        let first_id = ChildId::from("first");
        let second_id = ChildId::from("second");
        let first = identity
            .mint_membership(&first_id)
            .expect("first membership available");
        let second = identity
            .mint_membership(&second_id)
            .expect("second membership available");
        let member = MemberCell::new(second_id.clone(), second);
        let slot = SlotCell::new(Arc::clone(&member), None);
        let second_key = ChildKey(2);
        let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
        assert!(
            events
                .try_send(DriverEvent::Removal(RemovalRequest {
                    membership: first,
                    key: ChildKey(1),
                }))
                .is_ok(),
            "the fixture saturates the bounded driver lane"
        );
        let control = DynamicControl::new(events);
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .insert(
                second_id,
                DynamicEntry::resident(Arc::clone(&slot), second_key, Some(Latch::default())),
            );
        let foreign_control = Arc::clone(&control);
        let foreign_slot = Arc::clone(&slot);
        std::thread::spawn(move || {
            assert!(
                !crate::runtime::is_available(),
                "Tokio context is not inherited by a foreign thread"
            );
            super::signal_fused_cancel(foreign_control.as_ref(), &foreign_slot, &Latch::default());
        })
        .join()
        .expect("foreign-thread removal signaling succeeds");

        let Some(DriverEvent::Removal(observed_first)) = event_receiver.recv().await else {
            panic!("the saturated event remains first");
        };
        assert_eq!(observed_first.membership, first);
        let second_event =
            match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
                crate::runtime::Timeout::Completed(event) => {
                    event.expect("the control forwarder remains open")
                }
                crate::runtime::Timeout::Elapsed => {
                    panic!("the off-runtime removal edge must not be lost")
                }
            };
        let DriverEvent::Removal(observed_second) = second_event else {
            panic!("the forwarded event is the requested removal");
        };
        assert_eq!(observed_second.membership, second);
        assert_eq!(observed_second.key, second_key);
    }

    #[crate::runtime::test]
    async fn admission_conversion_panic_does_not_poison_dynamic_cleanup() {
        let root = isolated_scope("root", ScopeFlavor::Dynamic);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.member
            .update(|record| record.stage = MemberStage::Running);
        root.set_state(ScopeState::Running);
        root.set_startup(Ok(()));

        let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
        let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let control = DynamicControl::new(events.clone());
        root.set_dynamic_route(Some(control.clone()));
        root.set_admitted_children(Vec::new());
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: ResolvedDefaults::default(),
            intensity_policy: Intensity::default(),
            intensity: super::IntensityState::default(),
            children: ChildArena::default(),
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::running(),
            next_ordered_start: None,
            role: ScopeRole::Root,
            dynamic: Some(control.clone()),
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };

        let reservation = super::reserve_dynamic(&root, ChildId::from("worker"), None)
            .expect("running dynamic scope reserves the child");
        let member = Arc::clone(&reservation.slot.member);
        reservation
            .slot
            .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
        let response = super::start_admission(
            Arc::clone(&reservation.control),
            Arc::clone(&reservation.slot),
            None,
        )
        .expect("admission starts inside the runtime");
        let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
            panic!("the admission forwarder submits the request")
        };

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _identity = root
                    .child_identity
                    .lock()
                    .expect("scope identity mutex starts healthy");
                panic!("inject admission conversion failure");
            }))
            .is_err()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| scope.handle_admission(request))).is_err(),
            "the poisoned child identity injects the conversion panic"
        );
        assert!(matches!(
            response.receive().await,
            Some(Err(ReserveError::NotAdmitting(
                crate::NotAdmittingCause::Terminal
            )))
        ));
        assert!(matches!(member.record().stage, MemberStage::Terminal(_)));

        let cleanup = catch_unwind(AssertUnwindSafe(|| drop(scope)));
        assert!(
            cleanup.is_ok(),
            "conversion failure must not poison dynamic cleanup"
        );
        assert!(
            control
                .state
                .lock()
                .expect("dynamic-state mutex remains healthy")
                .entries
                .is_empty(),
            "cleanup discharges the stranded reservation"
        );
    }

    pub(crate) async fn exercise_saturated_fused_drop_before_exit<A>(
        make_admission: impl FnOnce(super::DynamicReservation) -> A,
    ) where
        A: Future,
    {
        let root = isolated_scope("root", ScopeFlavor::Dynamic);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.member
            .update(|record| record.stage = MemberStage::Running);
        root.set_state(ScopeState::Running);
        root.set_startup(Ok(()));

        let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
        let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let control = DynamicControl::new(events.clone());
        root.set_dynamic_route(Some(control.clone()));
        root.set_admitted_children(Vec::new());
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: ResolvedDefaults::default(),
            intensity_policy: Intensity::default(),
            intensity: super::IntensityState::default(),
            children: ChildArena::default(),
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::running(),
            next_ordered_start: None,
            role: ScopeRole::Root,
            dynamic: Some(control.clone()),
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };

        let release_failure = Latch::default();
        let starts = Arc::new(AtomicUsize::new(0));
        let reservation = super::reserve_dynamic(&root, ChildId::from("worker"), None)
            .expect("running dynamic scope reserves the child");
        let member = Arc::clone(&reservation.slot.member);
        reservation
            .slot
            .define(ChildConstruction::Task(TaskDef::new({
                let release_failure = release_failure.clone();
                let starts = Arc::clone(&starts);
                move |_| {
                    let release_failure = release_failure.clone();
                    let invocation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                    async move {
                        if invocation == 1 {
                            release_failure.fired().await;
                            Err(ExitError::message("first incarnation failed"))
                        } else {
                            future::pending().await
                        }
                    }
                }
            })));
        let membership = member.membership();
        let mut admission = Box::pin(make_admission(reservation));
        assert!(
            admission
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "first poll submits the fused admission"
        );
        let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
            panic!("the admission forwarder submits the request")
        };
        scope.handle_admission(request);

        for _ in 0..64 {
            if starts.load(Ordering::SeqCst) == 1 {
                break;
            }
            crate::runtime::yield_now().await;
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);

        release_failure.fire();
        let exit = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv())
            .await
        {
            crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            }))) => (
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            ),
            crate::runtime::Timeout::Completed(_) => panic!("the first incarnation reports exit"),
            crate::runtime::Timeout::Elapsed => panic!("the first incarnation exit must arrive"),
        };
        let key = exit.0;
        assert!(
            scope
                .events
                .try_send(DriverEvent::Removal(RemovalRequest {
                    membership: root.member.membership(),
                    key: ChildKey(u64::MAX - 1),
                }))
                .is_ok(),
            "the fixture saturates the bounded driver lane"
        );
        drop(admission);
        assert!(
            control
                .state
                .lock()
                .expect("dynamic-state mutex poisoned")
                .entries
                .get(member.id())
                .is_some_and(|entry| entry.is_removing() && entry.matches_key(key)),
            "fused drop marks the indexed membership removing before its queued edge advances"
        );

        scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
        assert!(scope.children[key].restart_deadline.is_none());
        assert_eq!(
            root.snapshot().total_restarts,
            crate::TotalRestarts::ZERO,
            "cancellation that linearized before exit incurs no restart charge"
        );
        for _ in 0..16 {
            crate::runtime::yield_now().await;
        }
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "exit dispatch consults fused cancellation before restart construction"
        );

        assert!(matches!(
            event_receiver.recv().await,
            Some(DriverEvent::Removal(queued))
                if queued.membership == root.member.membership()
        ));
        let forwarded =
            match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
                crate::runtime::Timeout::Completed(Some(event)) => event,
                crate::runtime::Timeout::Completed(None) => panic!("the driver lane remains open"),
                crate::runtime::Timeout::Elapsed => panic!("the fused removal edge is forwarded"),
            };
        let DriverEvent::Removal(removal) = forwarded else {
            panic!("the forwarded event is the fused removal")
        };
        assert_eq!(removal.membership, membership);
        assert_eq!(removal.key, key);
        scope.handle_removal(removal);
        let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
            crate::runtime::unbounded_mpsc_recv(&mut disposal_event_receiver).await
        else {
            panic!("removal joins retained construction disposal")
        };
        scope.handle_construction_disposed(child, panic);
        assert!(scope.children.get(key).is_none());

        control.request_forwarder_close.fire();
        control.request_forwarder_ended.fired().await;
    }

    /// Exercises the `DeadlineKind::Restart` suppression gate on its own:
    /// the restart deadline is scheduled first (no stop source latched at
    /// exit time), then the fused cancellation lands before the deadline's
    /// batch runs. The gate must clear the stale backoff edge without
    /// invoking user construction.
    #[crate::runtime::test]
    async fn restart_deadline_gate_suppresses_a_fused_cancel_landing_after_scheduling() {
        let root = isolated_scope("root", ScopeFlavor::Dynamic);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.member
            .update(|record| record.stage = MemberStage::Running);
        root.set_state(ScopeState::Running);
        root.set_startup(Ok(()));

        let (events, mut event_receiver) = crate::runtime::bounded_mpsc(64);
        let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let control = DynamicControl::new(events.clone());
        root.set_dynamic_route(Some(control.clone()));
        root.set_admitted_children(Vec::new());
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: ResolvedDefaults::default(),
            intensity_policy: Intensity::default(),
            intensity: super::IntensityState::default(),
            children: ChildArena::default(),
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::running(),
            next_ordered_start: None,
            role: ScopeRole::Root,
            dynamic: Some(control.clone()),
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };

        let starts = Arc::new(AtomicUsize::new(0));
        let reservation = super::reserve_dynamic(&root, ChildId::from("worker"), None)
            .expect("running dynamic scope reserves the child");
        let member = Arc::clone(&reservation.slot.member);
        reservation
            .slot
            .define(ChildConstruction::Task(TaskDef::new({
                let starts = Arc::clone(&starts);
                move |_| {
                    let invocation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                    async move {
                        if invocation == 1 {
                            Err(ExitError::message("first incarnation failed"))
                        } else {
                            future::pending().await
                        }
                    }
                }
            })));
        let membership = member.membership();
        let fused_cancel = Latch::default();
        let mut response = super::start_admission(
            Arc::clone(&reservation.control),
            Arc::clone(&reservation.slot),
            Some(fused_cancel.clone()),
        )
        .expect("the running scope accepts the admission");
        let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
            panic!("the admission forwarder submits the request")
        };
        scope.handle_admission(request);
        assert!(matches!(response.try_receive(), Some(Ok(()))));

        let exit = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv())
            .await
        {
            crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            }))) => (
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            ),
            crate::runtime::Timeout::Completed(_) => panic!("the first incarnation reports exit"),
            crate::runtime::Timeout::Elapsed => panic!("the first incarnation exit must arrive"),
        };
        let key = exit.0;
        scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
        assert!(
            scope.children[key].restart_deadline.is_some(),
            "a live fused admission does not suppress the restart at exit dispatch"
        );
        assert!(matches!(
            scope.children[key].slot.member.record().stage,
            MemberStage::Restarting
        ));

        // The fused admission handle drops only now: the cancellation latch
        // fires after the backoff was scheduled, and its Removal edge queues
        // behind the already-due restart deadline.
        super::signal_fused_cancel(control.as_ref(), &reservation.slot, &fused_cancel);
        assert!(fused_cancel.is_fired());

        let deadline = scope
            .deadlines
            .pop_due(crate::runtime::now() + Duration::from_secs(60 * 60))
            .expect("the immediate-backoff restart deadline is registered");
        assert!(matches!(deadline, super::DeadlineKind::Restart { .. }));
        scope.handle_deadline(deadline);

        assert!(
            scope.children[key].restart_deadline.is_none(),
            "the gate clears the stale backoff edge"
        );
        assert!(scope.children[key].active.is_none());
        for _ in 0..16 {
            crate::runtime::yield_now().await;
        }
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "the restart deadline arm rechecks level-triggered stop sources"
        );

        let forwarded =
            match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
                crate::runtime::Timeout::Completed(Some(event)) => event,
                crate::runtime::Timeout::Completed(None) => panic!("the driver lane remains open"),
                crate::runtime::Timeout::Elapsed => panic!("the fused removal edge is forwarded"),
            };
        let DriverEvent::Removal(removal) = forwarded else {
            panic!("the forwarded event is the fused removal");
        };
        assert_eq!(removal.membership, membership);
        assert_eq!(removal.key, key);
        scope.handle_removal(removal);
        let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
            crate::runtime::unbounded_mpsc_recv(&mut disposal_event_receiver).await
        else {
            panic!("removal joins retained construction disposal")
        };
        scope.handle_construction_disposed(child, panic);
        assert!(scope.children.get(key).is_none());

        control.request_forwarder_close.fire();
        control.request_forwarder_ended.fired().await;
    }

    #[crate::runtime::test]
    async fn saturated_admissions_share_one_forwarder_task_and_all_resolve() {
        const ADMISSIONS: usize = 64;

        let root = isolated_scope("root", ScopeFlavor::Dynamic);
        let (events, event_receiver) = crate::runtime::bounded_mpsc(1);
        let control = DynamicControl::new(events);
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

        let baseline = crate::runtime::alive_task_count();
        let mut responses = Vec::with_capacity(ADMISSIONS);
        for _ in 0..ADMISSIONS {
            responses.push(
                super::start_admission(control.clone(), Arc::clone(&slot), None)
                    .expect("runtime is available"),
            );
        }

        // Let the forwarder run until it parks on the saturated driver
        // channel; pending admissions must not each hold a live sender task.
        for _ in 0..64 {
            crate::runtime::yield_now().await;
        }
        assert!(
            crate::runtime::alive_task_count() <= baseline + 1,
            "saturated admissions share one forwarder task instead of spawning one each"
        );

        // Ending the driver channel answers every queued admission through
        // its response obligation.
        drop(event_receiver);
        for response in responses {
            assert!(matches!(
                response.receive().await,
                Some(Err(crate::ReserveError::NotAdmitting(
                    crate::NotAdmittingCause::Terminal
                )))
            ));
        }

        let mut alive = crate::runtime::alive_task_count();
        for _ in 0..1_024 {
            if alive == baseline {
                break;
            }
            crate::runtime::yield_now().await;
            alive = crate::runtime::alive_task_count();
        }
        assert_eq!(alive, baseline, "the forwarder exits once its queue drains");
    }

    #[crate::runtime::test]
    async fn system_shutdown_joins_root_driver_teardown() {
        let system = DynamicTree::new().spawn().expect("runtime is available");
        let root = system.scope();
        system.wait_started().await.expect("dynamic root starts");
        let control = root
            .as_scope()
            .cell
            .dynamic_route()
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
    fn incarnation_mint_exhaustion_has_no_terminal_side_effects() {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let membership = identity.mint_membership(&id).expect("membership available");
        let member = MemberCell::new(id, membership);
        let previous = Exit::new(
            ExitKind::Failed(ExitError::message("last completed incarnation")),
            Cancellation::NotObserved,
        );
        member.update(|record| {
            record.stage = MemberStage::Restarting;
            record.last_exit = Some(previous.clone());
        });
        let mut counter = IncarnationCounter::near_exhaustion(membership);

        assert!(counter.mint().is_some());
        assert!(counter.mint().is_none());
        assert!(matches!(member.record().stage, MemberStage::Restarting));
        assert_eq!(member.record().last_exit, Some(previous));
        assert!(counter.mint().is_none());
    }

    #[crate::runtime::test]
    async fn incarnation_exhaustion_uses_post_disposal_retention_routing() {
        let mut tree = Tree::new();
        tree.add_task(
            "worker",
            TaskDef::new(|_| future::pending::<crate::ExitResult>()).retention(Retention::Remove),
        )
        .expect("valid task");
        let mut plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
        let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
        let mut children = ChildArena::default();
        let key = children
            .insert(child)
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: plan.defaults.clone(),
            intensity_policy: plan.config.intensity,
            intensity: super::IntensityState::default(),
            children,
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::running(),
            next_ordered_start: Some(key),
            role: ScopeRole::Root,
            dynamic: None,
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };
        plan.armed = false;
        drop(plan);

        scope.children[key].incarnations =
            IncarnationCounter::near_exhaustion(scope.children[key].slot.member.membership());
        let first = scope.children[key]
            .incarnations
            .mint()
            .expect("the last usable incarnation mints");
        let previous = Exit::new(
            ExitKind::Failed(ExitError::message("last completed incarnation")),
            Cancellation::NotObserved,
        );
        root.transition_child(
            &scope.children[key].slot.member,
            |record| {
                record.incarnation = None;
                record.last_incarnation = Some(first);
                record.last_exit = Some(previous.clone());
                record.stage = MemberStage::Restarting;
            },
            None,
        );

        assert!(
            scope
                .events
                .try_send(DriverEvent::Child(ChildEvent::Ready {
                    child: key,
                    incarnation: first,
                }))
                .is_ok(),
            "the bounded driver queue is deliberately saturated"
        );

        scope.spawn_child(key);
        assert!(scope.children[key].is_disposing());
        assert!(matches!(
            scope.children[key].slot.member.record().stage,
            MemberStage::Restarting
        ));
        assert_eq!(root.snapshot().children.len(), 1);

        let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
            disposal_event_receiver
                .recv()
                .await
                .expect("disposal reports completion")
        else {
            panic!("only construction disposal was armed")
        };
        assert!(matches!(
            event_receiver.try_recv(),
            Ok(DriverEvent::Child(ChildEvent::Ready { .. }))
        ));
        scope.handle_construction_disposed(child, panic);

        assert!(!scope.children[key].is_disposing());
        assert!(matches!(
            scope.children[key].slot.member.record().stage,
            MemberStage::Terminal(ref exit) if exit == &previous
        ));
        assert!(
            root.snapshot().children.is_empty(),
            "retention pruning follows joined disposal"
        );
    }

    /// Exhaustion terminalizes a membership that never spawned. B.6 makes
    /// that the plain `Stopped { NeverStarted }` verdict; §6's
    /// `StartupAborted` stays reserved for a membership that ran and failed
    /// before its initial readiness edge. The pre-readiness position still
    /// has to route the scope's startup failure.
    #[crate::runtime::test]
    async fn first_spawn_exhaustion_stops_without_reporting_a_startup_abort() {
        let mut tree = Tree::new();
        tree.add_task(
            "worker",
            TaskDef::new(|_| future::pending::<crate::ExitResult>()),
        )
        .expect("valid task");
        let mut plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
        let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
        let mut children = ChildArena::default();
        let key = children
            .insert(child)
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: plan.defaults.clone(),
            intensity_policy: plan.config.intensity,
            intensity: super::IntensityState::default(),
            children,
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::starting(),
            next_ordered_start: Some(key),
            role: ScopeRole::Root,
            dynamic: None,
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };
        plan.armed = false;
        drop(plan);

        // Burn the counter's last usable generation without touching the
        // member record: the child is still an unspawned initial member, so
        // its very first `spawn_child` exhausts before any incarnation runs.
        scope.children[key].incarnations =
            IncarnationCounter::near_exhaustion(scope.children[key].slot.member.membership());
        assert!(scope.children[key].incarnations.mint().is_some());
        assert!(scope.children[key].initial);
        assert!(!scope.children[key].initial_ready);
        assert_eq!(
            scope.children[key].slot.member.record().last_incarnation,
            None
        );

        scope.spawn_child(key);
        assert!(scope.children[key].is_disposing());

        let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
            disposal_event_receiver
                .recv()
                .await
                .expect("disposal reports completion")
        else {
            panic!("only construction disposal was armed")
        };
        scope.handle_construction_disposed(child, panic);

        assert!(matches!(
            scope.children[key].slot.member.record().stage,
            MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        let snapshot = root.snapshot();
        let published = snapshot
            .child("worker")
            .expect("a retained exhausted child stays resident");
        assert!(
            matches!(
                published.state,
                ChildState::Stopped { ref exit } if matches!(exit.kind(), ExitKind::NeverStarted)
            ),
            "an unspawned exhausted membership is Stopped, not StartupAborted: {:?}",
            published.state
        );
        assert!(
            !scope.children[key].slot.member.record().startup_aborted,
            "§6's startup-abort flag belongs to a membership that ran"
        );
        assert!(
            matches!(
                root.record().startup,
                Some(Err(StartupError::StartupFailed(_)))
            ),
            "the pre-readiness position still routes the scope's startup failure"
        );
    }

    /// Pins main's linearization for a scope stop latched cross-batch: a
    /// restartable initial child failing pre-ready still dispatches
    /// `ScheduleRestart`, and the latched stop's own follow-up event owns
    /// the startup verdict (`ShutdownRequested`). Exit dispatch must not
    /// consult latched-but-unprocessed scope-stop sources for its membership
    /// classification, or the failure would be rerouted into
    /// `StartupFailed` while restart suppression claims the stop was first.
    #[crate::runtime::test]
    async fn latched_shutdown_keeps_the_startup_verdict_for_its_follow_up_event() {
        let mut tree = Tree::new();
        tree.add_task(
            "worker",
            TaskDef::new(|_| async { Err(ExitError::message("failed before readiness")) })
                .readiness(Readiness::Manual)
                .expect("manual readiness is valid"),
        )
        .expect("valid task");
        let mut plan = tree.lower_for_test();
        let root = Arc::clone(&plan.root);
        let epoch = root
            .begin_incarnation()
            .expect("test scope epoch is available");
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| resident_projection(&child.slot))
                .collect(),
        );
        let (events, mut event_receiver) = crate::runtime::bounded_mpsc(64);
        let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
        let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
        let mut children = ChildArena::default();
        let key = children
            .insert(child)
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
        let mut scope = ScopeRuntime {
            root: Arc::clone(&root),
            defaults: plan.defaults.clone(),
            intensity_policy: plan.config.intensity,
            intensity: super::IntensityState::default(),
            children,
            events,
            disposal_events,
            deadlines: super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: ScopeLifecycle::starting(),
            next_ordered_start: Some(key),
            role: ScopeRole::Root,
            dynamic: None,
            epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: false,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        };
        plan.armed = false;
        drop(plan);

        assert!(scope.children[key].initial);
        scope.spawn_child(key);
        assert!(!scope.children[key].initial_ready);
        let exit = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv())
            .await
        {
            crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            }))) => (
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            ),
            crate::runtime::Timeout::Completed(_) => panic!("the pre-ready failure reports exit"),
            crate::runtime::Timeout::Elapsed => panic!("the pre-ready failure exit must arrive"),
        };

        // The stop request latches after this batch was collected: it is
        // visible to `has_stop_request`, but its `Pending::Shutdown` follow-up
        // event belongs to the next batch.
        assert!(root.request_shutdown().is_some());
        assert!(root.has_stop_request(scope.epoch));

        scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
        assert!(
            scope.children[key].restart_deadline.is_some(),
            "a latched scope stop does not reclassify exit dispatch"
        );
        assert!(matches!(
            scope.children[key].slot.member.record().stage,
            MemberStage::Restarting
        ));
        assert!(
            root.record().startup.is_none(),
            "the pre-ready failure must not claim the startup verdict: {:?}",
            root.record().startup
        );

        // The latched stop's guaranteed follow-up event runs in the next
        // batch and owns the verdict, exactly as an unlatched scope would.
        assert!(root.take_shutdown_request(scope.epoch));
        scope.begin_drain(StopReason::ShutdownRequested);
        assert!(
            matches!(
                root.record().startup,
                Some(Err(StartupError::ShutdownRequested))
            ),
            "the latched stop owns the startup verdict: {:?}",
            root.record().startup
        );
        assert!(scope.children[key].restart_deadline.is_none());

        let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
            crate::runtime::unbounded_mpsc_recv(&mut disposal_event_receiver).await
        else {
            panic!("only construction disposal was armed")
        };
        scope.handle_construction_disposed(child, panic);
        assert!(matches!(
            scope.children[key].slot.member.record().stage,
            MemberStage::Terminal(_)
        ));
        assert!(
            !scope.children[key].slot.member.record().startup_aborted,
            "shutdown-first linearization publishes no startup abort"
        );
        assert!(matches!(
            root.record().startup,
            Some(Err(StartupError::ShutdownRequested))
        ));
    }

    #[crate::runtime::test]
    async fn nested_membership_exhaustion_is_structured_and_fail_closed() {
        let nested_id = ChildId::from("nested");
        let mut parent_identity = ScopeIdentity::new();
        let nested_membership = parent_identity
            .mint_membership(&nested_id)
            .expect("nested membership available");
        let nested_member = MemberCell::new(nested_id, nested_membership);

        let worker_id = ChildId::from("worker");
        let mut child_identity = ScopeIdentity::near_exhaustion(worker_id.clone(), 7);
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
        let ready = CompletionGatedLatch::default();
        let error = run_nested_tree(
            tree.into_core_for_test(),
            Arc::clone(&scope),
            crate::policy::ResolvedDefaults::default(),
            NestedScopeLatches {
                parent_ready: ready.clone(),
                ancestor: AncestorCommandLatches {
                    shutdown: Latch::default(),
                    abort: Latch::default(),
                    abort_ack: Latch::default(),
                },
            },
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
        let parent = isolated_scope("parent", ScopeFlavor::Ordered);
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("nested");
        let membership = identity.mint_membership(&id).expect("membership available");
        let member = MemberCell::new(id, membership);
        let scope = ScopeCell::new(
            Arc::clone(&member),
            ScopeFlavor::Ordered,
            ScopeIdentity::new(),
        );
        let slot = SlotCell::new(Arc::clone(&member), Some(Arc::clone(&scope)));
        parent.set_admitted_children(vec![resident_projection(&slot)]);
        let mut snapshots = scope.subscribe_snapshots();
        let mut events = scope.subscribe_lifecycle();
        let mut counter = IncarnationCounter::near_exhaustion(membership);
        let first = counter.mint().expect("last incarnation mints");
        member.update(|record| {
            record.stage = MemberStage::Restarting;
            record.last_incarnation = Some(first);
            record.last_exit = Some(Exit::new(ExitKind::Completed, Cancellation::NotObserved));
        });
        scope.set_state(ScopeState::Stopped {
            reason: StopReason::Finished,
        });

        assert!(counter.mint().is_none());
        assert!(matches!(member.record().stage, MemberStage::Restarting));
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
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));
        assert!(parent.terminalize_child(
            &member,
            Exit::new(ExitKind::Completed, Cancellation::NotObserved),
            None,
            StartupDisposition::NotAborted,
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

    #[crate::runtime::test]
    async fn never_started_nested_terminal_publishes_one_final_parent_snapshot() {
        let parent = isolated_scope("parent", ScopeFlavor::Ordered);
        let nested = isolated_scope("nested", ScopeFlavor::Ordered);
        let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
        parent.set_admitted_children(vec![resident_projection(&slot)]);
        let mut snapshots = parent.subscribe_snapshots();

        assert!(parent.terminalize_child(
            &nested.member,
            Exit::never_started(),
            None,
            StartupDisposition::NotAborted,
        ));
        snapshots
            .changed()
            .await
            .expect("no-incarnation terminal publishes the parent projection");

        assert!(matches!(
            snapshots
                .borrow_latest()
                .child("nested")
                .expect("retained nested child remains resident")
                .state,
            ChildState::Stopped { ref exit } if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        assert!(matches!(
            nested.record().state,
            ScopeState::Stopped {
                reason: StopReason::NeverStarted
            }
        ));
    }

    #[test]
    fn lifecycle_sequence_exhaustion_poison_is_never_minted_and_becomes_lag() {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("scope");
        let membership = identity.mint_membership(&id).expect("membership available");
        let member = MemberCell::new(id, membership);
        let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
        scope.set_lifecycle_sequence(u64::MAX - 2);
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
        assert_eq!(event.seq.get(), u64::MAX - 1);
        assert_eq!(
            scope.snapshot().lifecycle_seq,
            crate::LifecycleSeq::EXHAUSTED
        );
    }
}
