//! Constructing one child incarnation and launching its tasks.

use super::*;
use shelterwood_core::panic::PanicAccumulator;

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

impl SpawnDispatch {
    fn new(body: SpawnBody, construction_spent: bool) -> Self {
        Self {
            body: if construction_spent {
                PendingSpawnBody::one_shot(body)
            } else {
                PendingSpawnBody::restartable(body)
            },
            construction_spent,
        }
    }
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
    let mut panics = PanicAccumulator::default();
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
            SpawnDispatch::new(body, construction_spent)
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
            SpawnDispatch::new(body, construction_spent)
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
            SpawnDispatch::new(body, construction_spent)
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
        let Some(child) = self.children.get(&key) else {
            return;
        };
        if self.supervisor.lifecycle().is_draining()
            || child.active.is_some()
            || self.supervisor.joined(key)
            || self.supervisor.is_disposing(key)
        {
            return;
        }
        let nested = child.slot.scope.as_ref().map(Arc::clone);
        let restart_stopped = self.restarts_a_stopped_incarnation(key);
        let incarnation = self
            .children
            .get_mut(&key)
            .expect("the spawnable child remains registered")
            .incarnations
            .mint();
        // The pending-stop decision and Starting publication share the gate
        // with request_shutdown. A separate peek leaves a race in which an
        // idle request could be carried into a newly constructed body.
        if let Some(nested) = &nested
            && !self
                .root
                .start_scope_child(nested, incarnation, restart_stopped)
        {
            let startup = self.terminal_startup_disposition(key);
            self.terminate_inactive(key, startup);
            return;
        }
        let child = self
            .children
            .get_mut(&key)
            .expect("the spawnable child remains registered");
        if let Some(deadline) = child.restart_deadline.take() {
            self.deadlines.cancel(deadline);
        }

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
        // Non-scope children publish after mailbox binding. Nested members
        // already committed Starting together with their stop decision.
        // A spawn reaches here only from `Admitted` (first incarnation) or
        // `Restarting` (a scheduled restart), both accepted sources. The body
        // and the mailbox bind above are already committed, so a refusal would
        // run the incarnation with no `Started` edge.
        if nested.is_none() {
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
        }

        let deadline = child
            .options
            .readiness_deadline()
            .and_then(|duration| Deadline::after(now, duration).instant());
        let (readiness, readiness_effect) =
            ReadinessGate::configure(child.options.readiness, deadline);
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
            // `settle_supervisor` already owns this ordered-startup loop. Do
            // not re-enter it synchronously for an immediate child.
            let _ = self.apply_readiness_effect(key, incarnation, effect);
        }
        #[cfg(test)]
        self.record_storage();
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
