use super::*;

pub(super) struct RecordedReport {
    pub(super) outcome: Option<RetainedRecordedOutcome>,
    pub(super) cancellation: Cancellation,
    pub(super) readiness_signal_seen: bool,
}

struct ReportCompletion {
    report: Arc<OnceLock<RecordedReport>>,
    shutdown: Latch,
    local_stop: Option<Latch>,
    readiness: CompletionGatedLatch,
}

pub(super) struct ReportToken {
    completion: Obligation<ReportCompletion>,
}

pub(super) struct ReportClaim(Arc<OnceLock<RecordedReport>>);

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
pub(super) fn report_slot(
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
        // Retain here rather than at the send site. From `set` until the exit
        // joiner's `receive`, the joiner's claim is the cell's sole owner; a
        // joiner dropped un-polled at runtime teardown would otherwise run the
        // application error's destructor inline on the teardown thread.
        let outcome = outcome.map(RetainedRecordedOutcome::new);
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
    pub(super) fn record(mut self, outcome: RecordedOutcome) {
        self.completion
            .complete(|completion| completion.fill(Some(outcome)));
    }
}

impl ReportClaim {
    pub(super) fn receive(self) -> RecordedReport {
        Arc::try_unwrap(self.0)
            .unwrap_or_else(|_| {
                panic!("owned report token must be destroyed before its task joins")
            })
            .into_inner()
            .expect("owned report token must record or fall back before its task joins")
    }
}
pub(super) struct ActiveChild {
    pub(super) incarnation: Incarnation,
    pub(super) started_at: Instant,
    pub(super) shutdown: Latch,
    pub(super) abort: Latch,
    pub(super) abort_handle: runtime::AbortHandle,
    pub(super) ladder: Option<StopLadder>,
    pub(super) forced_outcome: Option<RecordedOutcome>,
    pub(super) hard_abort_phase: Option<GracePhase>,
    pub(super) readiness: ReadinessGate,
    pub(super) readiness_deadline: Option<DeadlineHandle>,
    pub(super) ready_signal: CompletionGatedLatch,
    pub(super) framework_shutdown: Option<Latch>,
    pub(super) framework_abort: Option<Latch>,
    pub(super) framework_abort_ack: Option<Latch>,
    pub(super) stop_deadline: Option<DeadlineHandle>,
}

pub(super) struct ChildTerminality {
    pub(super) root: Arc<ScopeCell>,
    pub(super) slot: Arc<SlotCell>,
}

pub(super) fn discharge_child_terminality(completion: ChildTerminality) {
    let record = completion.slot.member.record();
    if matches!(record.stage, MemberStage::Terminal(_)) {
        return;
    }
    let never_started = record.last_incarnation.is_none();
    let (exit, exited_incarnation) = if never_started {
        (Exit::never_started(), None)
    } else {
        (
            classify_exit_retaining(
                None,
                runtime::JoinOutcome::Cancelled,
                None,
                Cancellation::Observed,
            ),
            record.incarnation,
        )
    };
    // Initial-child conversion precedes residency publication. If a later
    // conversion unwinds, use the slot-owned gate for the converted prefix;
    // `terminalize_child` cannot discover those slots through the parent's
    // resident list yet. Once resident, the parent path synthesizes a nested
    // NeverStarted scope stop in the same observation transaction as the
    // membership edge. A restarting scope already published its real prior-
    // incarnation reason.
    if never_started && !completion.root.has_resident_child(&completion.slot.member) {
        // Every terminalization evicts the lineage; the slot-owned path
        // passes the owning scope so a restart's rebuild adopts a fresh,
        // incomparable membership instead of an ordered successor.
        completion.slot.terminalize_never_started(&completion.root);
        return;
    }
    completion.root.terminalize_child(
        &completion.slot.member,
        exit,
        exited_incarnation,
        StartupDisposition::NotAborted,
    );
}

pub(super) struct ChildRuntime {
    pub(super) slot: Arc<SlotCell>,
    pub(super) mailbox: Option<Arc<dyn MailboxControl>>,
    pub(super) mailbox_bind: Option<MailboxBindToken>,
    pub(super) terminality: Obligation<ChildTerminality>,
    pub(super) construction: runtime::Isolated<ChildConstruction>,
    pub(super) pending_terminal: Option<PendingTerminal>,
    pub(super) options: crate::policy::ResolvedCommonOptions,
    pub(super) incarnations: IncarnationCounter,
    pub(super) restarts: RestartState,
    pub(super) restart_deadline: Option<DeadlineHandle>,
    pub(super) restart_shutdown_pending: Option<Epoch>,
    pub(super) active: Option<ActiveChild>,
}

pub(super) struct PendingTerminal {
    exit: RetainedExit,
    exited_incarnation: Option<Incarnation>,
    startup: StartupDisposition,
}

impl ChildRuntime {
    pub(super) fn from_plan(plan: ChildPlan, scope: &Arc<ScopeCell>) -> Self {
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
        let incarnations = slot.member.take_incarnation_counter();
        let mailbox = slot.member.mailbox();
        let mailbox_bind = if let Some(mailbox) = &mailbox {
            let mut effects = MailboxEffectQueue::default();
            Some(mailbox.configure(options.mailbox, &mut effects))
        } else {
            None
        };
        Self {
            terminality,
            slot,
            mailbox,
            mailbox_bind,
            construction,
            pending_terminal: None,
            options,
            incarnations,
            restarts: RestartState::new(),
            restart_deadline: None,
            restart_shutdown_pending: None,
            active: None,
        }
    }

    #[cfg(test)]
    pub(super) fn is_disposing(&self) -> bool {
        self.pending_terminal.is_some()
    }

    #[cfg(test)]
    pub(super) fn is_terminal(&self) -> bool {
        matches!(self.slot.member.record().stage, MemberStage::Terminal(_))
    }

    pub(super) fn terminalize(
        &mut self,
        root: &ScopeCell,
        exit: RetainedExit,
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

    pub(super) fn complete_terminality(&mut self) {
        self.terminality.complete(drop);
    }
}
enum SpawnBody {
    Raw {
        spawn: RawSpawn,
        context: RawRunContext,
        /// Definition-resolved readiness mode handed to the incarnation.
        readiness: Readiness,
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
        start: NestedScopeStart,
    },
    ScopeOnce {
        tree: Box<BuilderCore>,
        scope: Arc<ScopeCell>,
        inherited: ResolvedDefaults,
        latches: NestedScopeLatches,
        start: NestedScopeStart,
    },
}

enum PendingSpawnBody {
    /// Restartable bodies hold only framework data and clones of state that
    /// remains retained by `ChildConstruction`, so unwinding can only release
    /// a non-last owner.
    Retained(SpawnBody),
    /// One-shot bodies carry the sole user-owned construction payload.
    Isolated(runtime::Isolated<SpawnBody>),
}

impl PendingSpawnBody {
    fn restartable(body: SpawnBody) -> Self {
        Self::Retained(body)
    }

    fn one_shot(body: SpawnBody) -> Self {
        Self::Isolated(runtime::Isolated::new(body))
    }

    fn into_body(self) -> SpawnBody {
        match self {
            Self::Retained(body) => body,
            Self::Isolated(mut body) => body
                .take()
                .expect("a child spawn retains its one-shot construction body"),
        }
    }
}

struct SpawnDispatch {
    body: PendingSpawnBody,
    construction_spent: bool,
}

/// Latches have deliberately separate ownership:
///
/// - `shutdown`/`abort` are the child-facing cooperative ladder;
/// - `framework_shutdown` is the nested scope driver's private observation
///   edge, separate from user-installable shutdown-token waiters;
/// - `framework_abort`/`framework_abort_ack` bound that driver's recursive
///   drain before its task is aborted;
/// - `ready` also carries the completion edge that makes readiness and
///   self-stop watcher tasks finite.
struct SpawnLatches {
    shutdown: Latch,
    abort: Latch,
    ready: CompletionGatedLatch,
    local_stop: Latch,
    framework_shutdown: Option<Latch>,
    framework_abort: Option<Latch>,
    framework_abort_ack: Option<Latch>,
}

impl SpawnLatches {
    fn new(scope_child: bool) -> Self {
        Self {
            shutdown: Latch::default(),
            abort: Latch::default(),
            ready: CompletionGatedLatch::default(),
            local_stop: Latch::default(),
            framework_shutdown: scope_child.then(Latch::default),
            framework_abort: scope_child.then(Latch::default),
            framework_abort_ack: scope_child.then(Latch::default),
        }
    }

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
            child_shutdown: self.shutdown.clone(),
            ancestor: AncestorCommandLatches {
                framework_shutdown: self
                    .framework_shutdown
                    .clone()
                    .expect("scope incarnations own a framework-shutdown latch"),
                abort: self
                    .framework_abort
                    .clone()
                    .expect("scope incarnations own a framework-abort latch"),
                abort_ack: self
                    .framework_abort_ack
                    .clone()
                    .expect("scope incarnations own a framework-abort acknowledgement"),
            },
        }
    }
}

pub(super) fn fire_shutdown_edges(shutdown: &Latch, framework_shutdown: Option<&Latch>) {
    let mut panics = runtime::PanicAccumulator::default();
    // Commit the child-facing cancellation evidence before waking the nested
    // driver. That observer may finish on another worker and have completion
    // sample this bit as soon as its wake runs. User waiters remain last so a
    // hostile one cannot strand framework progress.
    let notify_shutdown = shutdown.fire_silently();
    if let Some(framework_shutdown) = framework_shutdown {
        panics.run(|| {
            framework_shutdown.fire();
        });
    }
    if notify_shutdown {
        panics.run(|| shutdown.notify());
    }
}

struct ChildTaskLaunch {
    events: runtime::UnboundedMpscSender<DriverEvent>,
    key: ChildKey,
    incarnation: Incarnation,
    body: SpawnBody,
    watch_readiness: bool,
    shutdown: Latch,
    ready: CompletionGatedLatch,
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
            let body = SpawnBody::Raw {
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
                readiness: child.options.readiness,
            };
            SpawnDispatch {
                body: if construction_spent {
                    PendingSpawnBody::one_shot(body)
                } else {
                    PendingSpawnBody::restartable(body)
                },
                construction_spent,
            }
        }
        ChildConstruction::Task(definition) => {
            let context = TaskContext::new(id, incarnation, latches.task_context());
            let (body, construction_spent) = if let Some(factory) = definition.restartable() {
                (
                    SpawnBody::TaskRestartable {
                        factory: Arc::clone(factory),
                        context,
                    },
                    false,
                )
            } else {
                (
                    SpawnBody::TaskOnce {
                        body: definition
                            .take_one_shot()
                            .expect("one-shot task construction invoked more than once"),
                        context,
                    },
                    true,
                )
            };
            SpawnDispatch {
                body: if construction_spent {
                    PendingSpawnBody::one_shot(body)
                } else {
                    PendingSpawnBody::restartable(body)
                },
                construction_spent,
            }
        }
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
                        start: nested_scope_start(&scope),
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
                        start: nested_scope_start(&scope),
                        scope,
                        inherited,
                        latches: latches.nested_scope(),
                    },
                    true,
                )
            };
            SpawnDispatch {
                body: if construction_spent {
                    PendingSpawnBody::one_shot(body)
                } else {
                    PendingSpawnBody::restartable(body)
                },
                construction_spent,
            }
        }
    }
}

/// Test-only entry point to the construction dispatch, which the driver
/// otherwise reaches only through `spawn_child`. `spawn_child` releases a
/// spent construction before it can be dispatched again, so this is the one
/// way to exercise the one-shot invariant panics directly.
#[cfg(test)]
pub(super) fn dispatch_child_construction_for_test(
    child: &mut ChildRuntime,
    root: &Arc<ScopeCell>,
    defaults: &ResolvedDefaults,
    incarnation: Incarnation,
) {
    let latches = SpawnLatches::new(matches!(
        child.construction.get_mut(),
        ChildConstruction::Scope(_)
    ));
    drop(dispatch_child_construction(
        child,
        root,
        defaults,
        incarnation,
        &latches,
    ));
}

fn spawn_child_tasks(launch: ChildTaskLaunch) -> runtime::AbortHandle {
    let ChildTaskLaunch {
        events,
        key,
        incarnation,
        body,
        watch_readiness,
        shutdown,
        ready,
        local_stop,
    } = launch;
    let (report, report_claim) = report_slot(shutdown, Some(local_stop.clone()), ready.clone());
    let handle = runtime::spawn(async move {
        let body = async move {
            match body {
                SpawnBody::Raw {
                    spawn,
                    context,
                    readiness,
                } => spawn.run(context, readiness).await,
                SpawnBody::TaskRestartable { factory, context } => factory(context).await,
                SpawnBody::TaskOnce { body, context } => body(context).await,
                SpawnBody::ScopeRestartable {
                    factory,
                    scope,
                    inherited,
                    latches,
                    start,
                } => run_nested_factory(factory, scope, inherited, latches, start).await,
                SpawnBody::ScopeOnce {
                    tree,
                    scope,
                    inherited,
                    latches,
                    start,
                } => run_nested_tree(*tree, scope, inherited, latches, start).await,
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
        let join = runtime::join(handle).await;
        // The task owns `report`, whose explicit record or Drop fallback runs
        // before the join completes. `receive` therefore asserts sole
        // ownership and immediate post-join availability without ever
        // blocking this runtime worker.
        let report = report_claim.receive();
        let _ = exit_sender.send(DriverEvent::Child(ChildEvent::Exited {
            child: key,
            incarnation,
            recorded: report.outcome,
            join,
            cancellation: report.cancellation,
            readiness_signal_seen: report.readiness_signal_seen,
        }));
    });

    let completion = ready.clone();
    if watch_readiness {
        let ready_sender = events.clone();
        let ready_completion = ready.clone();
        // The latch publishes state before wake, so a child that fired and
        // completed may deliver both wakes together; select_two's left bias
        // is what keeps the fired edge winning that tie.
        runtime::spawn(async move {
            if matches!(
                runtime::select_two(ready.fired(), ready_completion.completed()).await,
                runtime::Either::Left(())
            ) {
                let _ = ready_sender.send(DriverEvent::Child(ChildEvent::Ready {
                    child: key,
                    incarnation,
                }));
            }
        });
    }

    runtime::spawn(async move {
        if matches!(
            runtime::select_two(local_stop.fired(), completion.completed()).await,
            runtime::Either::Left(())
        ) {
            let _ = events.send(DriverEvent::Child(ChildEvent::SelfStop {
                child: key,
                incarnation,
            }));
        }
    });

    abort_handle
}

impl ScopeRuntime {
    pub(super) fn terminalize_child(
        &mut self,
        key: ChildKey,
        exit: impl Into<RetainedExit>,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        // Protect the raw user error before every resource lookup and reducer
        // invariant. A malformed key may still be diagnosed, but it cannot
        // unwind the Exit payload on the driver stack.
        let mut exit = Some(exit.into());
        // Production terminal publication follows joined construction
        // disposal.  A few structural test/fallback paths synthesize that
        // already-joined boundary directly, so normalize them through the
        // same reducer predecessor instead of allowing `Terminalized` to
        // skip arbitrary incarnation states.
        if !self.supervisor.is_disposing(key) && !self.supervisor.joined(key) {
            self.reduce(SupervisorEvent::DisposalStarted { child: key });
        }
        let changed = self
            .children
            .get_mut(key)
            .expect("terminalized child remains registered")
            .terminalize(
                &self.root,
                exit.take()
                    .expect("terminal publication consumes its retained exit once"),
                exited_incarnation,
                startup,
            );
        self.reduce(SupervisorEvent::Terminalized { child: key });
        // The reducer drops an event whose predecessor never ran, which keeps
        // `step` total but leaves the shell no return channel. A child that
        // published terminality without reaching `Joined` would never count
        // toward completion, so the scope would simply never finish. Assert
        // the transition landed rather than discovering it as a stall.
        assert!(
            self.supervisor.joined(key),
            "terminal publication must leave the reducer's membership joined"
        );
        changed
    }

    pub(super) fn spawn_child(&mut self, key: ChildKey) {
        // A queued start effect can outlive the synchronous removal latch
        // that it was computed against. Re-sample that source at the single
        // construction funnel so initial and freshly admitted children obey
        // the same execution-time suppression rule as restart deadlines.
        // Scope-stop sources remain owned by their ordered control event; this
        // gate is the membership-local rule from SPEC §8.
        if self.removal_latched(key) {
            self.reduce(SupervisorEvent::RemovalSampled { child: key });
            return;
        }
        let Some(child) = self.children.get(key) else {
            return;
        };
        if self.supervisor.lifecycle().is_draining()
            || child.active.is_some()
            || self.supervisor.joined(key)
            || self.supervisor.is_disposing(key)
        {
            return;
        }
        let startup = self.terminal_startup_disposition(key);
        let child = self
            .children
            .get_mut(key)
            .expect("the spawnable child remains registered");
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
            // Exhaustion is a terminal outcome, not an exceptional cleanup
            // path. Join retained-definition disposal before terminality,
            // retention, removal completion, or ordered-scope progression.
            self.begin_terminal_disposal(key, RetainedExit::new(exit), None, startup);
            return;
        };

        // Per-incarnation latch topology:
        // - shutdown/abort flow from the ladder into application code;
        // - ready and local_stop flow from application code back to helpers;
        // - ready's completion edge terminates those helpers when the child
        //   exits first and orders late retained readiness capabilities;
        // - framework_shutdown keeps the nested driver observer separate from
        //   user waiters, while framework_abort/ack joins escalation before
        //   exit.
        // Each edge is level-triggered, so helper startup cannot lose a pulse.
        let scope_child = matches!(child.construction.get_mut(), ChildConstruction::Scope(_));
        let latches = SpawnLatches::new(scope_child);
        let SpawnDispatch {
            body,
            construction_spent,
        } = dispatch_child_construction(child, &self.root, &self.defaults, incarnation, &latches);
        let now = runtime::now();
        if let Some(mailbox) = &child.mailbox {
            let mut effects = MailboxEffectQueue::default();
            let token = child
                .mailbox_bind
                .take()
                .expect("configuration or close supplies each bind token");
            mailbox.bind(token, incarnation, &mut effects);
        }
        // A spawn reaches here only from `Admitted` (first incarnation) or
        // `Restarting` (a scheduled restart), both accepted sources. The body
        // and the mailbox bind above are already committed, so a refusal would
        // run the incarnation with no `Started` edge — see the partial-effect
        // note on issue #392.
        let started = self.root.transition_child_stage(
            &child.slot.member,
            MemberTransition::Starting { incarnation },
            Some(LifecycleEventKind::Started {
                id: child.slot.member.id().clone(),
                membership: child.slot.member.membership(),
                incarnation,
            }),
        );
        assert!(
            started,
            "a spawn starts an admitted or restarting member's projection"
        );

        let mut readiness = ReadinessGate::new();
        let deadline = child
            .options
            .readiness_deadline()
            .and_then(|duration| Deadline::after(now, duration).instant());
        let readiness_effect = readiness.step(ReadinessEvent::Configure {
            readiness: child.options.readiness,
            deadline,
        });
        let gated = readiness.needs_signal_watch();

        if construction_spent {
            // One-shot actor/task/subtree state has moved into `body`; the
            // retained construction is now framework-only spent metadata.
            // Release it without adding a blocking-pool scheduling edge to
            // terminal publication or restart-window arbitration.
            drop(child.construction.take());
        }
        // Binding and Started publication can both resume hostile waker
        // panics. Keep one-shot construction state behind isolated disposal
        // until those effects have completed, then transfer it at the task
        // launch boundary.
        let body = body.into_body();
        let abort_handle = spawn_child_tasks(ChildTaskLaunch {
            events: self.events.clone(),
            key,
            incarnation,
            body,
            watch_readiness: gated,
            shutdown: latches.shutdown.clone(),
            ready: latches.ready.clone(),
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
            framework_shutdown: latches.framework_shutdown,
            framework_abort: latches.framework_abort,
            framework_abort_ack: latches.framework_abort_ack,
            stop_deadline: None,
        });
        self.reduce(SupervisorEvent::Spawned { child: key });
        if let Some(effect) = readiness_effect {
            // `progress_startup` already owns this ordered-startup loop. Do
            // not re-enter it synchronously for an immediate child.
            let _ = self.apply_readiness_effect(key, incarnation, effect);
        }
        #[cfg(test)]
        self.record_storage();
    }

    pub(super) fn begin_stop_child(&mut self, key: ChildKey, forced: Option<RecordedOutcome>) {
        let Some(child) = self.children.get(key) else {
            return;
        };
        if self.supervisor.joined(key) || self.supervisor.is_disposing(key) {
            return;
        }
        if child
            .active
            .as_ref()
            .is_some_and(|active| active.ladder.is_some())
        {
            if let Some(active) = self
                .children
                .get_mut(key)
                .and_then(|child| child.active.as_mut())
                && forced.is_some()
            {
                active.forced_outcome = forced;
            }
            return;
        }
        if child.active.is_some() {
            self.reduce(SupervisorEvent::StopStarted { child: key });
            let child = self
                .children
                .get_mut(key)
                .expect("the stopped child remains registered");
            let active = child
                .active
                .as_mut()
                .expect("the stopped child remains active");
            // The stop ladder is armed only for an active incarnation, whose
            // projection is `Starting` or `Running`.
            let stopping = self.root.transition_child_stage(
                &child.slot.member,
                MemberTransition::Stopping,
                None,
            );
            assert!(
                stopping,
                "a stop ladder begins on a starting or running member"
            );
            if let Some(mailbox) = &child.mailbox {
                let mut effects = MailboxEffectQueue::default();
                mailbox.freeze(active.incarnation, &mut effects);
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
            let child = self
                .children
                .get_mut(key)
                .expect("the inactive child remains registered");
            if let Some(deadline) = child.restart_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            let record = child.slot.member.record();
            let exit = record.last_exit.unwrap_or_else(Exit::never_started);
            // A never-ran child and a child stopped between restart
            // incarnations share the same post-disposal terminal route. Hard
            // shutdown still detaches disposal through `hard_forced` below.
            self.begin_terminal_disposal(
                key,
                RetainedExit::new(exit),
                None,
                StartupDisposition::NotAborted,
            );
        }
    }

    pub(super) fn advance_ladder(&mut self, key: ChildKey, now: Instant) {
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
                    // Publish the framework-only edge independently so a
                    // hostile user cancellation waiter cannot strand the
                    // nested scope driver. The latch implementation itself
                    // finishes every waiter before this call resumes a panic.
                    fire_shutdown_edges(&active.shutdown, active.framework_shutdown.as_ref());
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

    pub(super) fn handle_self_stop(&mut self, key: ChildKey, incarnation: Incarnation) {
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
            // self-stop is observed" wording (§7).
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

    pub(super) fn handle_exit(
        &mut self,
        key: ChildKey,
        incarnation: Incarnation,
        recorded: Option<RetainedRecordedOutcome>,
        join: runtime::JoinOutcome<()>,
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
        if let Some(mailbox) = &child.mailbox {
            let mut effects = MailboxEffectQueue::default();
            let closed = mailbox.close(incarnation, &mut effects);
            drop(effects);
            if let Some(closed) = closed {
                let (token, teardown) = closed.into_parts();
                child.mailbox_bind = Some(token);
                runtime::dispose_detached(teardown);
            }
        }
        let recorded = reconcile_recorded_outcomes_retaining(recorded, active.forced_outcome);
        let exit = RetainedExit::new(classify_exit_retaining(
            recorded,
            join,
            active.hard_abort_phase,
            cancellation,
        ));
        child.restarts.settle_if_stable(
            IncarnationRun {
                started_at: active.started_at,
                stopped_at: runtime::now(),
            },
            self.intensity_policy.within(),
        );
        self.reduce(SupervisorEvent::IncarnationComplete { child: key });

        // Fused cancellation is a level-triggered source. It can linearize
        // before the forwarded Removal event or its public status projection
        // reaches this driver, so exit dispatch must consult the
        // removal sources directly before charging or publishing a restart.
        // Only removal sources classify the membership here: a latched but
        // unprocessed scope stop (shutdown request or ancestor latch) must
        // not turn this exit Terminal, or a restartable initial child
        // failing pre-ready would publish `StartupFailed` where the stop's
        // own follow-up event owns the verdict. The broader
        // `construction_is_suppressed` still gates the restart deadline arm,
        // where every suppression source has a guaranteed follow-up event.
        let membership_status = self.dispatch_membership_status(key);
        let startup = self.terminal_startup_disposition(key);
        let child = self
            .children
            .get_mut(key)
            .expect("the exiting child remains registered");

        let mode = if self.supervisor.lifecycle().is_draining() {
            ScopeMode::Draining
        } else {
            ScopeMode::Running
        };
        match dispatch_exit(
            exit.as_exit(),
            child.options.restart,
            mode,
            membership_status,
        ) {
            ExitDispatch::Terminal => {
                self.begin_terminal_disposal(key, exit, Some(incarnation), startup);
            }
            ExitDispatch::ScheduleRestart => {
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
                // The exiting incarnation's projection is `Starting`,
                // `Running` or `Stopping`, all accepted sources.
                // `schedule_restart` above has already charged this attempt
                // against the child and the intensity window, so a refusal
                // would restart the child with neither `Exited` nor
                // `RestartScheduled` published — see the partial-effect note
                // on issue #392.
                let raw_exit = exit.as_exit().clone();
                let published = self.root.publish_child_restart(
                    &child.slot.member,
                    decision.total_restarts(),
                    exit,
                    MemberTransition::RestartScheduled {
                        exit: raw_exit.clone(),
                        restart_count: decision.restart_count(),
                        // Publish the derived schedule even when intensity prevents spawning it.
                        // `None` means the exact clock point cannot be represented and armed; no
                        // substitute restart is scheduled.
                        restart_at: decision.restart_at(),
                    },
                    LifecycleEventKind::Exited {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        incarnation,
                        exit: raw_exit,
                    },
                    LifecycleEventKind::RestartScheduled {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        attempt: decision.attempt(),
                        delay: decision.delay(),
                    },
                );
                assert!(
                    published,
                    "a restart is scheduled from an active incarnation's exit"
                );
                let trip = decision.intensity_trip();
                if trip.is_none()
                    && let Some(restart_at) = decision.restart_at()
                {
                    child.restart_deadline = Some(
                        self.deadlines
                            .push(restart_at, DeadlineKind::Restart { child: key }),
                    );
                }
                let startup_pending = self.supervisor.lifecycle().is_starting();
                self.reduce(SupervisorEvent::RestartPending { child: key });
                if let Some(trip) = trip {
                    if startup_pending {
                        self.begin_drain_with_startup(
                            StopReason::IntensityTripped(trip.clone()),
                            Err(StartupError::IntensityTripped(trip)),
                        );
                    } else {
                        self.begin_drain(StopReason::IntensityTripped(trip));
                    }
                }
            }
        }
        if let Some(target) = self
            .children
            .get(key)
            .and_then(|child| child.restart_shutdown_pending)
        {
            // A subject-carrying control event can beat the corresponding
            // child exit into an earlier driver batch. Retry the retained
            // fact now that the old incarnation is inactive; otherwise the
            // one-shot event would be consumed while `spawn_child` still
            // rejects the active child and the requested expedite is lost.
            // Queue the retry rather than expediting synchronously: it
            // re-enters arbitration on the next wake, so a later exit
            // collected in this same batch first gets the chance to trip
            // intensity or fail startup, and the execution-time suppression
            // re-check then observes that drain.
            self.restart_shutdown_retries.push((key, target));
        }
    }

    fn terminal_startup_disposition(&self, key: ChildKey) -> StartupDisposition {
        // §7's startup abort is a startup-sequence property: the membership
        // failed before its *initial* readiness edge. A later incarnation
        // stopped pre-ready (for example during drain) does not rewind it.
        if self.supervisor.is_initial(key)
            && !self.supervisor.lifecycle().startup_complete()
            && !self.supervisor.initial_ready(key)
        {
            StartupDisposition::Aborted
        } else {
            StartupDisposition::NotAborted
        }
    }

    pub(super) fn begin_terminal_disposal(
        &mut self,
        key: ChildKey,
        exit: RetainedExit,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) {
        // Keep the caller's guard through every refusal and invariant verdict.
        // A failed Exit can own hostile user error drop glue, so no call-site
        // argument window or local path may unwind or return it directly on
        // the driver thread.
        let mut exit = Some(exit);
        if !self.supervisor.contains(key)
            || self.supervisor.is_disposing(key)
            || self.supervisor.joined(key)
        {
            return;
        }
        self.reduce(SupervisorEvent::DisposalStarted { child: key });
        // Same reasoning as `terminalize_child`: a dropped `DisposalStarted`
        // would make the later `Terminalized` unreachable too, stranding the
        // membership short of `Joined` with no loud failure.
        assert!(
            self.supervisor.is_disposing(key),
            "terminal disposal must leave the reducer's incarnation disposing"
        );
        let construction = {
            let Some(child) = self.children.get_mut(key) else {
                return;
            };
            if child.pending_terminal.is_some() {
                return;
            }
            child.pending_terminal = Some(PendingTerminal {
                exit: exit
                    .take()
                    .expect("terminal disposal installs its retained exit once"),
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

        if self.supervisor.hard_forced() {
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
            if sender
                .send(DriverEvent::Child(ChildEvent::ConstructionDisposed {
                    child: key,
                    panic,
                }))
                .is_ok()
            {
                signal.pulse();
            }
        });
    }

    pub(super) fn handle_construction_disposed(
        &mut self,
        key: ChildKey,
        panic: Option<runtime::DisposalPanic>,
    ) {
        let Some(child) = self.children.get_mut(key) else {
            return;
        };
        let Some(terminal) = child.pending_terminal.take() else {
            return;
        };
        let member = Arc::clone(&child.slot.member);
        let mut exit = terminal.exit;
        if terminal.exited_incarnation.is_some()
            && let Some(runtime::DisposalPanic { message }) = panic
        {
            // Only an exited incarnation can own a destructor failure. A
            // never-started child or a child between restart incarnations
            // keeps its already-authoritative verdict while disposal remains
            // ordered ahead of terminal routing.
            exit = classify_disposal_panic_retaining(exit, message);
        }
        // §7's `StartupAborted` is a startup-sequence property of a
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
        // Hand the publication seam a guarded clone rather than a raw one, so
        // no window between here and the cell layer's own retention holds the
        // user error unguarded. The cell layer surrenders its copy inside the
        // publishing transaction.
        self.terminalize_child(key, exit.clone(), terminal.exited_incarnation, startup);
        // Keep the marker installed until terminal publication has committed.
        // A concurrent shutdown sampler then sees either pending cleanup or a
        // terminal member, never the gap between those two representations.
        //
        // Argued, not pinned: clearing the marker before `terminalize_child`
        // reopens that gap for a few instructions, which no test in the suite
        // can provoke deterministically.
        member.set_terminal_disposal_pending(false);
        if self.supervisor.membership_status(key) == MembershipStatus::Removing {
            self.flush_supervisor_effects();
        } else {
            if terminal.startup == StartupDisposition::Aborted
                && !self.supervisor.lifecycle().is_draining()
            {
                self.fail_startup(key, &exit);
            }
            if self.children[key].options.retention == crate::Retention::Remove {
                self.prune_terminal(key);
            }
        }
        // Both routes above are fallible, so the guard retires once, here, by
        // falling out of scope whichever route ran. Issue #455 removed the
        // escape hatch that let a driver-layer caller surrender a guard on a
        // conventional co-owner proof, and the driver owns no observation
        // transaction to surrender into, so `RetainedExit::drop` is the venue:
        // it retires a failed user error through critical disposal at the cost
        // of one blocking-pool job. The copy that retires as refcount traffic
        // is the clone handed to `terminalize_child` above, surrendered inside
        // the publishing transaction where the terminal member record is its
        // structural co-owner.
    }
}

#[cfg(test)]
mod latch_topology_tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Wake, Waker},
    };

    use super::{SpawnLatches, fire_shutdown_edges};

    struct PanicWake(&'static str);

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(self.0);
        }
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ObserveShutdownWake {
        shutdown: crate::runtime::Latch,
        observed: Arc<AtomicBool>,
    }

    impl Wake for ObserveShutdownWake {
        fn wake(self: Arc<Self>) {
            self.observed
                .store(self.shutdown.is_fired(), Ordering::SeqCst);
        }
    }

    #[test]
    fn nested_scope_shutdown_observer_is_not_a_user_cancellation_waiter() {
        let latches = SpawnLatches::new(true);
        let nested = latches.nested_scope();

        assert!(latches.shutdown.fire());
        assert!(latches.shutdown.is_fired());
        assert!(
            !nested.ancestor.framework_shutdown.is_fired(),
            "firing the user cancellation edge cannot fire the framework observer"
        );

        let latches = SpawnLatches::new(true);
        let nested = latches.nested_scope();
        assert!(nested.ancestor.framework_shutdown.fire());
        assert!(nested.ancestor.framework_shutdown.is_fired());
        assert!(
            !latches.shutdown.is_fired(),
            "firing the framework observer cannot publish user cancellation"
        );
    }

    #[test]
    fn non_scope_children_do_not_allocate_framework_shutdown_observers() {
        let latches = SpawnLatches::new(false);

        assert!(latches.framework_shutdown.is_none());
        assert!(latches.framework_abort.is_none());
        assert!(latches.framework_abort_ack.is_none());
    }

    #[test]
    fn hostile_user_shutdown_waiter_cannot_strand_the_framework_observer() {
        const PANIC: &str = "injected user cancellation waker panic";

        let latches = SpawnLatches::new(true);
        let nested = latches.nested_scope();
        let mut user_wait = Box::pin(latches.shutdown.fired());
        let mut framework_wait = Box::pin(nested.ancestor.framework_shutdown.fired());
        let hostile = Waker::from(Arc::new(PanicWake(PANIC)));
        let framework_wakes = Arc::new(AtomicUsize::new(0));
        let framework = Waker::from(Arc::new(CountWake(Arc::clone(&framework_wakes))));
        assert!(
            user_wait
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        assert!(
            framework_wait
                .as_mut()
                .poll(&mut Context::from_waker(&framework))
                .is_pending()
        );

        let result = catch_unwind(AssertUnwindSafe(|| {
            fire_shutdown_edges(&latches.shutdown, Some(&nested.ancestor.framework_shutdown));
        }));

        let payload = result.expect_err("the hostile user wake still surfaces");
        assert_eq!(payload.downcast_ref::<&'static str>().copied(), Some(PANIC));
        assert!(latches.shutdown.is_fired());
        assert!(nested.ancestor.framework_shutdown.is_fired());
        assert_eq!(framework_wakes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn framework_shutdown_wake_observes_child_cancellation_already_committed() {
        let latches = SpawnLatches::new(true);
        let nested = latches.nested_scope();
        let observed = Arc::new(AtomicBool::new(false));
        let mut framework_wait = Box::pin(nested.ancestor.framework_shutdown.fired());
        let framework = Waker::from(Arc::new(ObserveShutdownWake {
            shutdown: latches.shutdown.clone(),
            observed: Arc::clone(&observed),
        }));
        assert!(
            framework_wait
                .as_mut()
                .poll(&mut Context::from_waker(&framework))
                .is_pending()
        );

        fire_shutdown_edges(&latches.shutdown, Some(&nested.ancestor.framework_shutdown));

        assert!(
            observed.load(Ordering::SeqCst),
            "the nested driver cannot run before child cancellation is visible"
        );
    }
}
