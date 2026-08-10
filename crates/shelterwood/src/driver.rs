//! Mutable runtime shell and shared handle state.

mod admission_control;

use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

mod storage;

use storage::{ChildArena, ChildKey, Obligation};

use crate::{
    Cancellation, ChildId, Exit, ExitKind, GracePhase, Incarnation, IntensityTrip, JitterSample,
    Membership, Readiness, ReadinessDeadline, ScopeState, ShutdownStraggler, ShutdownTimeout,
    StartupFailure, StartupFailureCause,
    admission::{NotAdmittingCause, ReserveError},
    cells::{
        MailboxControl, MemberStage, MemberTransition, ResidentProjection, ScopeCell,
        StartupDisposition,
    },
    deadline::Deadline,
    engine::{
        ArbitrationClass, ChildCompletionState, DeadlineHandle, DeadlineQueue, Epoch, ExitDispatch,
        IncarnationRun, IntensityState, MembershipStatus, ReadinessEffect, ReadinessEvent,
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

struct RecordedReport {
    outcome: Option<RecordedOutcome>,
    cancellation: Cancellation,
    readiness_signal_seen: bool,
}

struct ReportCompletion {
    report: Arc<OnceLock<RecordedReport>>,
    shutdown: Latch,
    local_stop: Option<Latch>,
    readiness: CompletionGatedLatch,
}

pub(crate) struct ReportToken {
    completion: Obligation<ReportCompletion>,
}

pub(crate) struct ReportClaim(Arc<OnceLock<RecordedReport>>);

/// Couples the child task's outcome report to its join verdict without an
/// asynchronous handoff race.
///
/// `ReportToken` is owned by the child task and its fail-closed `Drop` fills a
/// shared cell synchronously. The runtime resolves `runtime::join` only after
/// the spawned future has been destroyed (a tokio `JoinHandle` guarantee, not
/// a language one — any replacement executor behind `runtime` must preserve
/// it), so the exit joiner may consume the cell immediately: its claim is the
/// sole surviving owner and the cell is initialized on every return, panic,
/// and cancellation edge. The shutdown/local-stop and readiness latches are
/// sampled by that same initialization, making the report and its
/// completion-boundary evidence one ordered observation.
pub(crate) fn report_slot(
    shutdown: Latch,
    local_stop: Option<Latch>,
    readiness: CompletionGatedLatch,
) -> (ReportToken, ReportClaim) {
    let report = Arc::new(OnceLock::new());
    (
        ReportToken {
            completion: Obligation::new(
                ReportCompletion {
                    report: Arc::clone(&report),
                    shutdown,
                    local_stop,
                    readiness,
                },
                |completion| completion.fill(None),
            ),
        },
        ReportClaim(report),
    )
}

impl ReportCompletion {
    fn fill(self, outcome: Option<RecordedOutcome>) {
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
        // Ownership supplies exactly one `ReportCompletion`; ignoring the
        // impossible occupied-cell result keeps the Drop fallback infallible.
        let _ = self.report.set(report);
    }
}

impl ReportToken {
    pub(crate) fn record(mut self, outcome: RecordedOutcome) {
        self.completion
            .complete(|completion| completion.fill(Some(outcome)));
    }
}

impl ReportClaim {
    fn receive(self) -> RecordedReport {
        Arc::try_unwrap(self.0)
            .unwrap_or_else(|_| {
                panic!("owned report token must be destroyed before its task joins")
            })
            .into_inner()
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
    children: ChildArena<ChildRuntime>,
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
                        scope: crate::scope::ScopeRef {
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
    let (report, report_claim) = report_slot(shutdown, Some(local_stop.clone()), ready.clone());
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
        report.record(RecordedOutcome::returned(result));
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
        // before the join completes. `receive` therefore asserts sole
        // ownership and immediate post-join availability without ever
        // blocking this runtime worker.
        let report = report_claim.receive();
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
            self.root.set_child_removing(&member);
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
        self.root.transition_child_stage(
            &child.slot.member,
            MemberTransition::Starting { incarnation },
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
                self.root.transition_child_stage(
                    &child.slot.member,
                    MemberTransition::Running,
                    Some(LifecycleEventKind::Ready {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        incarnation,
                    }),
                );
                true
            }
            ReadinessEffect::TimedOut { deadline } => {
                self.begin_stop_child(key, Some(RecordedOutcome::readiness_timed_out(deadline)));
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
            self.root
                .transition_child_stage(&child.slot.member, MemberTransition::Stopping, None);
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
                        active.forced_outcome = Some(RecordedOutcome::aborted(phase));
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
        let membership_status = if membership_removing {
            MembershipStatus::Removing
        } else {
            MembershipStatus::Active
        };
        match dispatch_exit(&exit, child.options.restart, mode, membership_status) {
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
                    MemberTransition::RestartScheduled {
                        exit: exit.clone(),
                        restart_count: decision.restart_count,
                        // Publish the derived schedule even when intensity prevents spawning it.
                        // `None` means the exact clock point cannot be represented and armed; no
                        // substitute restart is scheduled.
                        restart_at: decision.restart_at,
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
        root.member.transition(MemberTransition::Running);
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
        control.register_initial(children.iter().map(|(key, child)| (&child.slot, key)));
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
mod tests;
