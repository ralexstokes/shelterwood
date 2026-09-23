mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, ReleaseGate, SHUTDOWN_BUDGET, advance_time, assert_eventually,
    assert_eventually_frozen, assert_quiet,
    policy::never,
    waiting::{
        cancellation_signalled_waiting_task, construction_signalled_waiting_task,
        gate_released_manual_ready_task, signalled_waiting_task, start_signalled_waiting_task,
        task as waiting_task,
    },
};
use shelterwood::{
    Actor, ActorDef, ActorOnceDef, Backoff, Cancellation, ChildState, Context, DynamicTree,
    ExitError, ExitKind, ExitResult, Jitter, RawActor, RawContext, RawDef, RawOnceDef, Readiness,
    ReadinessDeadline, RestartCondition, RestartPolicy, Retention, ScopeState, Shutdown,
    StartupError, StartupFailureCause, SubtreeOnceDef, TaskDef, Tree,
};

struct ReadyThenStop;

impl RawActor for ReadyThenStop {
    type Msg = ();

    fn readiness() -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context.mark_ready();
        context.stop();
        Ok(())
    }
}

struct GatedStopInInit;

impl Actor for GatedStopInInit {
    type Msg = ();
    type Args = (ReleaseGate, ReleaseGate);

    async fn init(
        (entered, release): Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        context.stop();
        entered.release();
        release.wait().await;
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct RepeatingStopInInit;

impl Actor for RepeatingStopInInit {
    type Msg = ();
    type Args = Arc<AtomicUsize>;

    async fn init(starts: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        starts.fetch_add(1, Ordering::SeqCst);
        context.stop();
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct FailAfterStopInInit;

impl Actor for FailAfterStopInInit {
    type Msg = ();
    type Args = ();

    async fn init((): (), context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        context.stop();
        Err(ExitError::message("init failed after requesting stop"))
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct PanicAfterStopInInit;

impl Actor for PanicAfterStopInInit {
    type Msg = ();
    type Args = ();

    async fn init((): (), context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        context.stop();
        panic!("init panicked after requesting stop");
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct DelayedStopDecorator {
    inner: GatedStopInInit,
}

impl Actor for DelayedStopDecorator {
    type Msg = ();
    type Args = ((ReleaseGate, ReleaseGate), ReleaseGate, ReleaseGate);

    async fn init(
        (inner_args, decorator_entered, release_decorator): Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        let inner = {
            let mut inner_context = context.for_actor::<GatedStopInInit>();
            GatedStopInInit::init(inner_args, &mut inner_context).await?
        };
        decorator_entered.release();
        release_decorator.wait().await;
        Ok(Self { inner })
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        self.inner
            .handle((), &mut context.for_actor::<GatedStopInInit>())
            .await
    }
}

#[tokio::test]
async fn readiness_fired_before_clean_exit_counts_for_startup() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "ready-then-complete",
            TaskDef::new(|context| async move {
                context.mark_ready();
                Ok(())
            })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("ready-before-exit completes startup");
    assert!(matches!(task.wait().await.kind(), ExitKind::Completed));
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test(start_paused = true)]
async fn readiness_fired_before_failure_makes_the_failure_post_ready() {
    let backoff = Duration::from_secs(30);
    let mut tree = Tree::new();
    tree.add_task(
        "ready-then-fail",
        TaskDef::new(|context| async move {
            context.mark_ready();
            Err(ExitError::message("post-ready failure"))
        })
        .restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(backoff, Jitter::None).expect("non-zero backoff"),
        ))
        .shutdown(Shutdown::Abort)
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    assert_eventually_frozen!(|| {
        system
            .scope()
            .child("ready-then-fail")
            .is_some_and(|child| matches!(child.state, ChildState::Restarting))
    })
    .await;
    assert_eq!(
        system.scope().snapshot().state,
        ScopeState::Running,
        "the ready edge must complete startup before restart backoff"
    );
    system
        .wait_started()
        .await
        .expect("post-ready failure does not abort or re-gate startup");
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("a child in backoff has no straggler");
}

#[tokio::test]
async fn readiness_fired_before_clean_self_stop_counts_for_startup() {
    let mut tree = Tree::new();
    tree.add_raw_once("ready-then-stop", RawOnceDef::new(ReadyThenStop))
        .expect("valid raw actor");

    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("ready-before-stop completes startup");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test]
async fn after_init_stop_waits_for_success_then_advances_the_ordered_suffix() {
    let init_entered = ReleaseGate::default();
    let release_init = ReleaseGate::default();
    let decorator_entered = ReleaseGate::default();
    let release_decorator = ReleaseGate::default();
    let suffix_started = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_actor_once(
        "stop-in-init",
        ActorOnceDef::<DelayedStopDecorator>::new((
            (init_entered.clone(), release_init.clone()),
            decorator_entered.clone(),
            release_decorator.clone(),
        ))
        .retention(Retention::Retain),
    )
    .expect("valid actor");
    tree.add_task(
        "suffix",
        TaskDef::new({
            let suffix_started = suffix_started.clone();
            move |context| {
                let suffix_started = suffix_started.clone();
                async move {
                    suffix_started.release();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("valid suffix");

    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    init_entered.wait().await;
    assert_quiet(Duration::from_millis(20), || {
        scope
            .child("suffix")
            .is_some_and(|child| !matches!(child.state, ChildState::Admitted))
    })
    .await;

    release_init.release();
    decorator_entered.wait().await;
    assert_quiet(Duration::from_millis(20), || {
        scope
            .child("suffix")
            .is_some_and(|child| !matches!(child.state, ChildState::Admitted))
    })
    .await;
    release_decorator.release();
    suffix_started.wait().await;
    system
        .wait_started()
        .await
        .expect("successful AfterInit initialization publishes readiness before self-stop");
    let stopped = scope
        .wait_for_child(
            "stop-in-init",
            |child| child.state.is_terminal(),
            Duration::from_secs(1),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "self-stopping actor terminalizes: {error:?}; snapshot: {:?}",
                scope.snapshot()
            )
        });
    let ChildState::Stopped { exit } = stopped.state else {
        panic!("ready self-stop is not a startup abort: {stopped:?}");
    };
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(exit.cancellation(), Cancellation::Observed);

    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("ordered suffix cooperates");
}

#[tokio::test]
async fn immediate_stop_in_init_is_published_before_the_initializer_returns() {
    let init_entered = ReleaseGate::default();
    let release_init = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_actor_once(
        "immediate-stop-in-init",
        ActorOnceDef::<GatedStopInInit>::new((init_entered.clone(), release_init.clone()))
            .readiness(Readiness::Immediate),
    )
    .expect("valid immediate actor");

    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    init_entered.wait().await;
    system
        .wait_started()
        .await
        .expect("immediate readiness does not wait for initialization");
    scope
        .wait_for_child(
            "immediate-stop-in-init",
            |child| matches!(child.state, ChildState::Stopping),
            POLL_TIMEOUT,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "an Immediate override publishes self-stop before init returns: {error:?}; snapshot: {:?}",
                scope.snapshot()
            )
        });

    release_init.release();
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test]
async fn manual_stop_in_init_remains_a_terminal_pre_ready_failure() {
    let init_entered = ReleaseGate::default();
    let release_init = ReleaseGate::default();
    let suffix_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_actor_once(
        "manual-stop-in-init",
        ActorOnceDef::<GatedStopInInit>::new((init_entered.clone(), release_init.clone()))
            .readiness(Readiness::Manual)
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid manually gated actor");
    let suffix = tree
        .add_task(
            "suffix",
            TaskDef::new({
                let suffix_started = Arc::clone(&suffix_started);
                move |_| {
                    suffix_started.store(true, Ordering::SeqCst);
                    async { Ok(()) }
                }
            }),
        )
        .expect("valid suffix");

    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    init_entered.wait().await;
    scope
        .wait_for_child(
            "manual-stop-in-init",
            |child| matches!(child.state, ChildState::Stopping),
            POLL_TIMEOUT,
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "a Manual override publishes self-stop before init returns: {error:?}; snapshot: {:?}",
                scope.snapshot()
            )
        });
    release_init.release();
    let (id, exit) = crate::common::startup_failed_child(
        system
            .wait_started()
            .await
            .expect_err("manual pre-ready self-stop still aborts startup"),
    );
    assert_eq!(id.as_str(), "manual-stop-in-init");
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(exit.cancellation(), Cancellation::Observed);
    assert!(!suffix_started.load(Ordering::SeqCst));
    assert!(matches!(suffix.wait().await.kind(), ExitKind::NeverStarted));
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("failed tree has no straggler");
}

#[tokio::test]
async fn after_init_stop_survives_init_panic_without_manufacturing_readiness() {
    let mut tree = Tree::new();
    tree.add_actor_once(
        "panicked-stop-in-init",
        ActorOnceDef::<PanicAfterStopInInit>::new(()),
    )
    .expect("valid actor");

    let system = tree.spawn().expect("runtime is available");
    let (id, exit) = crate::common::startup_failed_child(
        system
            .wait_started()
            .await
            .expect_err("an init panic remains a terminal pre-ready failure"),
    );
    assert_eq!(id.as_str(), "panicked-stop-in-init");
    assert!(
        matches!(exit.kind(), ExitKind::Panicked { message: Some(message) } if message.contains("init panicked after requesting stop")),
        "the initializer panic remains authoritative: {exit:?}"
    );
    assert_eq!(
        exit.cancellation(),
        Cancellation::Observed,
        "dropping the initializer context publishes its deferred stop"
    );
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("failed tree has no straggler");
}

#[tokio::test]
async fn after_init_stop_does_not_publish_readiness_when_init_fails() {
    let suffix_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_actor_once(
        "failed-stop-in-init",
        ActorOnceDef::<FailAfterStopInInit>::new(()),
    )
    .expect("valid actor");
    let suffix = tree
        .add_task(
            "suffix",
            TaskDef::new({
                let suffix_started = Arc::clone(&suffix_started);
                move |_| {
                    suffix_started.store(true, Ordering::SeqCst);
                    async { Ok(()) }
                }
            }),
        )
        .expect("valid suffix");

    let system = tree.spawn().expect("runtime is available");
    let (id, exit) = crate::common::startup_failed_child(
        system
            .wait_started()
            .await
            .expect_err("only a successful AfterInit initializer becomes ready"),
    );
    assert_eq!(id.as_str(), "failed-stop-in-init");
    assert!(matches!(exit.kind(), ExitKind::Failed(_)));
    assert_eq!(exit.cancellation(), Cancellation::Observed);
    assert!(!suffix_started.load(Ordering::SeqCst));
    assert!(matches!(suffix.wait().await.kind(), ExitKind::NeverStarted));
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("failed tree has no straggler");
}

#[tokio::test]
async fn repeated_after_init_self_stops_are_post_ready_until_intensity_trips() {
    let starts = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_actor(
        "restarting-stop-in-init",
        ActorDef::<RepeatingStopInInit>::cloned(Arc::clone(&starts)).restart(RestartPolicy::new(
            RestartCondition::Always,
            Backoff::Immediate,
        )),
    )
    .expect("valid restarting actor");

    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("the first successful init establishes aggregate readiness");
    let reason = system.wait().await;
    assert!(
        matches!(reason, shelterwood::StopReason::IntensityTripped(_)),
        "the restart loop ends through intensity, not StartupFailed: {reason:?}"
    );
    assert!(starts.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn readiness_fired_after_task_completion_cannot_reclassify_the_exit() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "late-readiness",
            TaskDef::new(|context| async move {
                let late_context = context.clone();
                tokio::spawn(async move {
                    late_context.mark_ready();
                });
                Ok(())
            })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    let startup = system
        .wait_started()
        .await
        .expect_err("readiness after future completion is stale");
    assert!(matches!(
        startup,
        StartupError::StartupFailed(ref failure)
            if matches!(failure.cause, StartupFailureCause::Child { ref id, .. } if id.as_str() == "late-readiness")
    ));
    assert!(matches!(task.wait().await.kind(), ExitKind::Completed));
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("the completed child has no straggler");
}

#[tokio::test(start_paused = true)]
async fn immediate_restart_deadline_rechecks_aggregate_startup() {
    let backoff = Duration::from_secs(30);
    let incarnations = Arc::new(AtomicUsize::new(0));
    let release_manual = ReleaseGate::default();
    let manual_ready = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "restarting-immediate",
        TaskDef::new({
            let incarnations = Arc::clone(&incarnations);
            move |context| {
                let incarnation = incarnations.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if incarnation == 1 {
                        return Err(ExitError::message("restart after sibling starts"));
                    }
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(backoff, Jitter::None).expect("non-zero backoff"),
        )),
    )
    .expect("valid immediate task");
    tree.add_task(
        "manual-sibling",
        TaskDef::new({
            let release_manual = release_manual.clone();
            let manual_ready = manual_ready.clone();
            move |context| {
                let release_manual = release_manual.clone();
                let manual_ready = manual_ready.clone();
                async move {
                    release_manual.wait().await;
                    context.mark_ready();
                    manual_ready.release();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid manual sibling");

    let system = tree.spawn().expect("runtime is available");
    assert_eventually_frozen!(|| {
        system
            .scope()
            .child("restarting-immediate")
            .is_some_and(|child| matches!(child.state, ChildState::Restarting))
    })
    .await;
    release_manual.release();
    manual_ready.wait().await;
    assert_eq!(system.scope().snapshot().state, ScopeState::Starting);

    advance_time(backoff).await;
    assert_eventually_frozen!(|| incarnations.load(Ordering::SeqCst) == 2).await;
    system
        .wait_started()
        .await
        .expect("immediate restart releases the last aggregate gate");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("restarted task cooperates");
}

#[tokio::test(start_paused = true)]
async fn ordered_startup_waits_for_manual_readiness() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let release_ready = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gated",
        gate_released_manual_ready_task(release_ready.clone(), {
            let order = Arc::clone(&order);
            move || order.lock().expect("order mutex poisoned").push("gated")
        })
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    tree.add_task(
        "later",
        TaskDef::new({
            let order = Arc::clone(&order);
            move |context| {
                let order = Arc::clone(&order);
                async move {
                    order.lock().expect("order mutex poisoned").push("later");
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    assert_eventually_frozen!(|| {
        order.lock().expect("order mutex poisoned").as_slice() == ["gated"]
    })
    .await;
    assert_quiet(Duration::from_millis(20), || {
        order.lock().expect("order mutex poisoned").len() > 1
    })
    .await;
    release_ready.release();
    system
        .wait_started()
        .await
        .expect("readiness releases startup");
    assert_eq!(
        *order.lock().expect("order mutex poisoned"),
        ["gated", "later"]
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("clean shutdown");
}

#[tokio::test]
async fn shutdown_racing_startup_reports_shutdown_requested() {
    let entered = ReleaseGate::default();
    let cancellation_seen = ReleaseGate::default();
    let release_ready = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gated",
        TaskDef::new({
            let entered = entered.clone();
            let cancellation_seen = cancellation_seen.clone();
            let release_ready = release_ready.clone();
            move |context| {
                let entered = entered.clone();
                let cancellation_seen = cancellation_seen.clone();
                let release_ready = release_ready.clone();
                async move {
                    entered.release();
                    context.shutdown_token().cancelled().await;
                    cancellation_seen.release();
                    release_ready.wait().await;
                    context.mark_ready();
                    Ok(())
                }
            }
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");

    let system = tree.spawn().expect("runtime is available");
    entered.wait().await;
    system.scope().request_shutdown();
    cancellation_seen.wait().await;
    release_ready.release();

    assert_eq!(
        system.wait_started().await,
        Err(StartupError::ShutdownRequested)
    );
    assert_eq!(
        system.wait().await,
        shelterwood::StopReason::ShutdownRequested
    );
}

#[tokio::test(start_paused = true)]
async fn readiness_deadline_is_typed_and_absolute() {
    let deadline_width = Duration::from_secs(10);
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "gated",
            TaskDef::new(|_| std::future::pending())
                .restart(never())
                .shutdown(Shutdown::Abort)
                .readiness(Readiness::Manual)
                .expect("manual readiness")
                .readiness_deadline(
                    ReadinessDeadline::bounded(deadline_width).expect("non-zero deadline"),
                ),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    let before = tokio::time::Instant::now().into_std();
    advance_time(deadline_width).await;
    let startup = system.wait_started().await.expect_err("startup times out");
    let exit = task.wait().await;
    let ExitKind::ReadinessTimedOut { deadline } = exit.kind() else {
        panic!("expected a typed readiness timeout, got {exit:?}");
    };
    assert!(*deadline >= before + deadline_width);
    assert!(matches!(
        startup,
        StartupError::StartupFailed(ref failure)
            if matches!(failure.cause, StartupFailureCause::Child { ref id, .. } if id.as_str() == "gated")
    ));
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("terminal child leaves no straggler");
}

#[tokio::test(start_paused = true)]
async fn ready_at_deadline_wins_and_shutdown_disarms_the_gate() {
    let width = Duration::from_secs(10);
    let ready_started = Arc::new(AtomicBool::new(false));
    let ready_marked = ReleaseGate::default();
    let mut ready_tree = Tree::new();
    let ready_task = ready_tree
        .add_task(
            "edge",
            TaskDef::new({
                let ready_started = Arc::clone(&ready_started);
                let ready_marked = ready_marked.clone();
                move |context| {
                    let ready_started = Arc::clone(&ready_started);
                    let ready_marked = ready_marked.clone();
                    async move {
                        ready_started.store(true, Ordering::SeqCst);
                        tokio::time::sleep(width).await;
                        context.mark_ready();
                        ready_marked.release();
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(ReadinessDeadline::bounded(width).expect("non-zero deadline")),
        )
        .expect("valid task");
    let ready_system = ready_tree.spawn().expect("runtime is available");
    assert_eventually_frozen!(|| ready_started.load(Ordering::SeqCst)).await;
    advance_time(width).await;
    ready_marked.wait().await;
    ready_system
        .wait_started()
        .await
        .expect("readiness signal wins the deadline tie");
    ready_system
        .shutdown(Duration::ZERO)
        .await
        .expect("ready task cooperates");
    assert!(!matches!(
        ready_task.wait().await.kind(),
        ExitKind::ReadinessTimedOut { .. }
    ));

    let mut shutdown_tree = Tree::new();
    let shutdown_task = shutdown_tree
        .add_task(
            "edge",
            waiting_task()
                .readiness(Readiness::Manual)
                .expect("manual readiness")
                .readiness_deadline(ReadinessDeadline::bounded(width).expect("non-zero deadline")),
        )
        .expect("valid task");
    let shutdown_system = shutdown_tree.spawn().expect("runtime is available");
    shutdown_system
        .shutdown(Duration::ZERO)
        .await
        .expect("shutdown disarms readiness");
    advance_time(width).await;
    let exit = shutdown_task.wait().await;
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(exit.cancellation(), Cancellation::Observed);
}

#[tokio::test(start_paused = true)]
async fn restart_before_aggregate_readiness_rearms_the_gate() {
    let incarnation = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let ready_again = ReleaseGate::default();
    let release_second = ReleaseGate::default();
    let later_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_task(
        "restarting",
        TaskDef::new({
            let incarnation = Arc::clone(&incarnation);
            let fail_first = fail_first.clone();
            let ready_again = ready_again.clone();
            move |context| {
                let current = incarnation.fetch_add(1, Ordering::SeqCst) + 1;
                let fail_first = fail_first.clone();
                let ready_again = ready_again.clone();
                async move {
                    if current == 1 {
                        context.mark_ready();
                        fail_first.wait().await;
                        return Err(ExitError::message("retry"));
                    }
                    ready_again.wait().await;
                    context.mark_ready();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    tree.add_task(
        "later",
        gate_released_manual_ready_task(release_second.clone(), {
            let later_started = Arc::clone(&later_started);
            move || later_started.store(true, Ordering::SeqCst)
        })
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    assert_eventually_frozen!(|| later_started.load(Ordering::SeqCst)).await;
    fail_first.release();
    assert_eventually_frozen!(|| incarnation.load(Ordering::SeqCst) == 2).await;
    release_second.release();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), system.wait_started())
            .await
            .is_err(),
        "the replacement incarnation must re-gate aggregate readiness"
    );
    ready_again.release();
    system
        .wait_started()
        .await
        .expect("replacement becomes ready");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("clean shutdown");
}

#[tokio::test(start_paused = true)]
async fn ordered_terminal_pre_ready_exit_parks_the_root_and_marks_suffix_never_started() {
    let prefix_cancelled = Arc::new(AtomicBool::new(false));
    let suffix_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_task(
        "prefix",
        cancellation_signalled_waiting_task(Arc::clone(&prefix_cancelled)),
    )
    .expect("valid prefix");
    tree.add_task(
        "failure",
        TaskDef::new(|_| async { Err(ExitError::message("startup failure")) })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
    )
    .expect("valid failing task");
    let suffix = tree
        .add_task(
            "suffix",
            TaskDef::new({
                let suffix_started = Arc::clone(&suffix_started);
                move |_| {
                    suffix_started.store(true, Ordering::SeqCst);
                    async { Ok(()) }
                }
            }),
        )
        .expect("valid suffix");
    let system = tree.spawn().expect("runtime is available");
    let startup = system.wait_started().await.expect_err("startup fails");
    assert!(matches!(
        startup,
        StartupError::StartupFailed(ref failure)
            if matches!(failure.cause, StartupFailureCause::Child { ref id, .. } if id.as_str() == "failure")
    ));
    assert!(matches!(suffix.wait().await.kind(), ExitKind::NeverStarted));
    assert!(!suffix_started.load(Ordering::SeqCst));
    assert_quiet(Duration::from_millis(20), || {
        prefix_cancelled.load(Ordering::SeqCst)
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("owner rolls back live prefix");
}

#[tokio::test(start_paused = true)]
async fn dynamic_startup_failure_keeps_other_initial_members_supervised() {
    let sibling_cancelled = Arc::new(AtomicBool::new(false));
    let sibling_started = Arc::new(AtomicBool::new(false));
    let mut tree = DynamicTree::new();
    tree.add_task(
        "failure",
        TaskDef::new(|_| async { Err(ExitError::message("startup failure")) })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
    )
    .expect("valid failing task");
    tree.add_task(
        "sibling",
        signalled_waiting_task(Arc::clone(&sibling_started), Arc::clone(&sibling_cancelled))
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid sibling");
    let system = tree.spawn().expect("runtime is available");
    let startup = system.wait_started().await.expect_err("startup fails");
    assert!(matches!(
        startup,
        StartupError::StartupFailed(ref failure)
            if matches!(failure.cause, StartupFailureCause::Child { ref id, .. } if id.as_str() == "failure")
    ));
    assert!(sibling_started.load(Ordering::SeqCst));
    assert_quiet(Duration::from_millis(20), || {
        sibling_cancelled.load(Ordering::SeqCst)
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("owner rolls back sibling");
}

/// A `Manual` member that never marks ready, so it gates its scope's
/// aggregate until it is removed.
fn unbounded_manual_gate() -> TaskDef {
    waiting_task()
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded)
}

/// A `Manual` member that marks ready immediately and then parks.
fn unbounded_manual_ready() -> TaskDef {
    TaskDef::new(|context| async move {
        context.mark_ready();
        context.shutdown_token().cancelled().await;
        Ok(())
    })
    .readiness(Readiness::Manual)
    .expect("manual readiness")
    .readiness_deadline(ReadinessDeadline::Unbounded)
}

#[tokio::test(start_paused = true)]
async fn dynamic_startup_completes_after_removing_sole_unready_initial_member() {
    let mut tree = DynamicTree::new();
    tree.add_task("gate", unbounded_manual_gate())
        .expect("valid initial member");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert_eventually_frozen!(|| scope.as_scope().child("gate").is_some()).await;

    assert_eq!(
        scope.remove("gate").await,
        shelterwood::RemoveOutcome::Removed
    );
    tokio::time::timeout(Duration::from_secs(60), system.wait_started())
        .await
        .expect("removing the sole initial gate completes startup")
        .expect("removal is not a startup failure");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("empty root stops");
}

#[tokio::test(start_paused = true)]
async fn dynamic_startup_completes_after_removing_last_unready_initial_member() {
    let mut tree = DynamicTree::new();
    tree.add_task("ready", unbounded_manual_ready())
        .expect("valid ready sibling");
    tree.add_task("gate", unbounded_manual_gate())
        .expect("valid unready member");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert_eventually_frozen!(|| {
        scope
            .as_scope()
            .child("ready")
            .is_some_and(|child| matches!(child.state, ChildState::Running))
            && scope.as_scope().child("gate").is_some()
    })
    .await;

    assert_eq!(
        scope.remove("gate").await,
        shelterwood::RemoveOutcome::Removed
    );
    tokio::time::timeout(Duration::from_secs(60), system.wait_started())
        .await
        .expect("removing the final initial gate completes startup")
        .expect("removal is not a startup failure");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("ready sibling stops");
}

/// The declared set can empty out: two removals in flight at once leave no
/// initial member at all, and the aggregate completes on the vacuous set.
#[tokio::test(start_paused = true)]
async fn dynamic_startup_completes_after_removing_every_initial_member() {
    let mut tree = DynamicTree::new();
    tree.add_task("first", unbounded_manual_gate())
        .expect("valid unready member");
    tree.add_task("second", unbounded_manual_gate())
        .expect("valid unready member");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert_eventually_frozen!(|| {
        scope.as_scope().child("first").is_some() && scope.as_scope().child("second").is_some()
    })
    .await;

    let (first, second) = tokio::join!(scope.remove("first"), scope.remove("second"));
    assert_eq!(first, shelterwood::RemoveOutcome::Removed);
    assert_eq!(second, shelterwood::RemoveOutcome::Removed);
    tokio::time::timeout(Duration::from_secs(60), system.wait_started())
        .await
        .expect("an emptied declared set completes startup")
        .expect("removal is not a startup failure");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("empty root stops");
}

/// Completion has to publish upwards, not just locally: the nested dynamic
/// scope's aggregate is the ordered parent's gate, so removal must release
/// the sibling declared after it.
#[tokio::test(start_paused = true)]
async fn removal_completed_nested_startup_releases_the_ordered_sibling() {
    let mut nested = DynamicTree::new();
    nested
        .add_task("gate", unbounded_manual_gate())
        .expect("valid unready member");
    let mut root = Tree::new();
    let nested_scope = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid nested dynamic scope");
    root.add_task("after", unbounded_manual_ready())
        .expect("valid gated sibling");

    let system = root.spawn().expect("runtime is available");
    let scope = system.scope();
    assert_eventually_frozen!(|| nested_scope.as_scope().child("gate").is_some()).await;
    assert_quiet(Duration::from_secs(5), || {
        scope
            .child("after")
            .is_some_and(|child| matches!(child.state, ChildState::Running))
    })
    .await;

    assert_eq!(
        nested_scope.remove("gate").await,
        shelterwood::RemoveOutcome::Removed
    );
    tokio::time::timeout(Duration::from_secs(60), system.wait_started())
        .await
        .expect("nested completion releases the ordered gate")
        .expect("removal is not a startup failure");
    assert!(
        scope
            .child("after")
            .is_some_and(|child| matches!(child.state, ChildState::Running)),
        "the gated sibling starts once removal completes the nested aggregate"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

/// The negative that pins the recomputation's guard: removal recomputes the
/// aggregate, it does not assert it. Dropping an already-ready member cannot
/// complete startup while an unready one is still declared.
#[tokio::test(start_paused = true)]
async fn removing_a_ready_initial_member_leaves_startup_pending() {
    let mut tree = DynamicTree::new();
    tree.add_task("ready", unbounded_manual_ready())
        .expect("valid ready member");
    tree.add_task("gate", unbounded_manual_gate())
        .expect("valid unready member");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert_eventually_frozen!(|| {
        scope
            .as_scope()
            .child("ready")
            .is_some_and(|child| matches!(child.state, ChildState::Running))
            && scope.as_scope().child("gate").is_some()
    })
    .await;

    assert_eq!(
        scope.remove("ready").await,
        shelterwood::RemoveOutcome::Removed
    );
    assert_quiet(Duration::from_secs(30), || {
        !matches!(scope.as_scope().snapshot().state, ScopeState::Starting)
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("the still-gated root stops");
}

#[tokio::test]
async fn runtime_dynamic_additions_never_join_aggregate_readiness() {
    let initial_release = ReleaseGate::default();
    let initial_started = Arc::new(AtomicBool::new(false));
    let runtime_started = Arc::new(AtomicBool::new(false));
    let mut tree = DynamicTree::new();
    tree.add_task(
        "initial",
        gate_released_manual_ready_task(initial_release.clone(), {
            let initial_started = Arc::clone(&initial_started);
            move || initial_started.store(true, Ordering::SeqCst)
        })
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid initial member");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert_eventually!(|| initial_started.load(Ordering::SeqCst)).await;
    let runtime_task = scope
        .add_task(
            "runtime",
            start_signalled_waiting_task(Arc::clone(&runtime_started))
                .readiness(Readiness::Manual)
                .expect("manual readiness")
                .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .await
        .expect("runtime member is admitted");
    assert_eventually!(|| runtime_started.load(Ordering::SeqCst)).await;
    initial_release.release();
    // The regression this test targets — a runtime addition joining the
    // aggregate — would park `wait_started` forever behind the unbounded
    // readiness deadline of a member that never marks ready. Bounding the
    // wait turns that hang into a diagnostic outside nextest's kill timer.
    tokio::time::timeout(POLL_TIMEOUT, system.wait_started())
        .await
        .expect("aggregate readiness does not wait on the runtime addition")
        .expect("only the initial set gates aggregate readiness");
    assert_eq!(
        scope.remove_task(&runtime_task).await,
        shelterwood::RemoveOutcome::Removed
    );
    system.shutdown(SHUTDOWN_BUDGET).await.expect("root stops");
}

#[tokio::test]
async fn nested_dynamic_startup_failure_rolls_back_and_preserves_inner_cause() {
    let sibling_cancelled = Arc::new(AtomicBool::new(false));
    let mut nested = DynamicTree::new();
    nested
        .add_task(
            "inner-failure",
            TaskDef::new(|_| async { Err(ExitError::message("startup failure")) })
                .restart(never())
                .readiness(Readiness::Manual)
                .expect("manual readiness"),
        )
        .expect("valid failing task");
    nested
        .add_task(
            "inner-sibling",
            cancellation_signalled_waiting_task(Arc::clone(&sibling_cancelled))
                .readiness(Readiness::Manual)
                .expect("manual readiness")
                .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid sibling");

    let mut root = Tree::new();
    let nested_scope = root
        .add_subtree_once(
            "nested",
            SubtreeOnceDef::new(nested).shutdown(Shutdown::Abort),
        )
        .expect("valid nested scope");
    let system = root.spawn().expect("runtime is available");
    let startup = system
        .wait_started()
        .await
        .expect_err("nested startup fails");
    let outer_source =
        std::error::Error::source(&startup).expect("startup error exposes its outer child failure");
    assert_eq!(
        outer_source.to_string(),
        "child `nested` failed during startup"
    );
    let inner_source = outer_source
        .source()
        .expect("the child exit exposes the nested structured failure");
    assert_eq!(
        inner_source.to_string(),
        "child `inner-failure` failed during startup"
    );
    assert_eq!(
        inner_source
            .source()
            .expect("the nested child failure exposes its application error")
            .to_string(),
        "startup failure"
    );
    let outer_failure = match startup {
        StartupError::StartupFailed(failure) => failure,
        other => panic!("unexpected startup result: {other:?}"),
    };
    let StartupFailureCause::Child { id, exit, .. } = outer_failure.cause else {
        panic!("expected child startup failure");
    };
    assert_eq!(id.as_str(), "nested");
    let ExitKind::Failed(error) = exit.kind() else {
        panic!("nested scope must fail structurally");
    };
    let inner = error
        .startup_failure()
        .expect("nested failure retains framework provenance");
    assert!(matches!(
        inner.cause,
        StartupFailureCause::Child { ref id, .. } if id.as_str() == "inner-failure"
    ));
    assert!(sibling_cancelled.load(Ordering::SeqCst));
    assert!(matches!(
        nested_scope.as_scope().wait_stopped().await,
        shelterwood::StopReason::StartupFailed(_)
    ));
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("failed root rolls back");
}

#[tokio::test]
async fn nested_ordered_startup_failure_rolls_back_only_the_started_prefix() {
    let prefix_cancelled = Arc::new(AtomicBool::new(false));
    let suffix_started = Arc::new(AtomicBool::new(false));
    let mut nested = Tree::new();
    nested
        .add_task(
            "prefix",
            cancellation_signalled_waiting_task(Arc::clone(&prefix_cancelled)),
        )
        .expect("valid prefix");
    nested
        .add_task(
            "failure",
            TaskDef::new(|_| async { Err(ExitError::message("nested ordered failure")) })
                .restart(never())
                .readiness(Readiness::Manual)
                .expect("manual readiness"),
        )
        .expect("valid failure");
    let suffix = nested
        .add_task(
            "suffix",
            TaskDef::new({
                let suffix_started = Arc::clone(&suffix_started);
                move |_| {
                    suffix_started.store(true, Ordering::SeqCst);
                    async { Ok(()) }
                }
            }),
        )
        .expect("valid suffix");

    let mut root = Tree::new();
    let nested_scope = root
        .add_subtree_once(
            "nested",
            SubtreeOnceDef::new(nested).shutdown(Shutdown::Abort),
        )
        .expect("valid nested scope");
    let system = root.spawn().expect("runtime is available");
    let error = system
        .wait_started()
        .await
        .expect_err("the ordered child failure aborts nested startup");
    assert!(matches!(
        error,
        StartupError::StartupFailed(ref failure)
            if matches!(failure.cause, StartupFailureCause::Child { ref id, .. } if id.as_str() == "nested")
    ));
    assert!(prefix_cancelled.load(Ordering::SeqCst));
    assert!(!suffix_started.load(Ordering::SeqCst));
    assert!(matches!(suffix.wait().await.kind(), ExitKind::NeverStarted));
    assert!(matches!(
        nested_scope.wait_stopped().await,
        shelterwood::StopReason::StartupFailed(_)
    ));
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("failed root rolls back");
}

struct ExplicitInitReady;

impl Actor for ExplicitInitReady {
    type Msg = ();
    type Args = (ReleaseGate, ReleaseGate);

    async fn init(
        (ready, release_init): Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        context.mark_ready();
        context.mark_ready();
        ready.release();
        release_init.wait().await;
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn earliest_mark_ready_wins_and_later_readiness_edges_are_no_ops() {
    let explicit_ready = ReleaseGate::default();
    let release_init = ReleaseGate::default();
    let sibling_started = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_actor_once(
        "actor",
        ActorOnceDef::<ExplicitInitReady>::new((explicit_ready.clone(), release_init.clone()))
            .readiness(Readiness::AfterInit),
    )
    .expect("valid actor");
    tree.add_task(
        "sibling",
        TaskDef::new({
            let sibling_started = sibling_started.clone();
            move |context| {
                let sibling_started = sibling_started.clone();
                async move {
                    context.mark_ready();
                    context.mark_ready();
                    sibling_started.release();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("valid sibling");

    let system = tree.spawn().expect("runtime is available");
    explicit_ready.wait().await;
    tokio::time::timeout(POLL_TIMEOUT, sibling_started.wait())
        .await
        .expect("explicit readiness advances ordered startup before init returns");
    release_init.release();
    system
        .wait_started()
        .await
        .expect("automatic and duplicate readiness signals are harmless no-ops");
    system.shutdown(SHUTDOWN_BUDGET).await.expect("tree stops");
}

#[tokio::test]
async fn start_or_shutdown_preserves_startup_error_and_rolls_back_the_prefix() {
    let prefix_cancelled = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_task(
        "prefix",
        cancellation_signalled_waiting_task(Arc::clone(&prefix_cancelled)),
    )
    .expect("valid prefix");
    tree.add_task(
        "failure",
        TaskDef::new(|_| async { Err(ExitError::message("startup failure")) })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
    )
    .expect("valid failure");
    let system = tree.spawn().expect("runtime is available");
    let error = system
        .start_or_shutdown(SHUTDOWN_BUDGET)
        .await
        .expect_err("startup failure is returned after rollback");
    assert!(matches!(error.startup, StartupError::StartupFailed(_)));
    assert!(error.rollback_timeout.is_none());
    assert!(prefix_cancelled.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn start_or_shutdown_rollback_timeout_preserves_the_startup_cause_and_stragglers() {
    let mut tree = Tree::new();
    let prefix = tree
        .add_task(
            "prefix",
            TaskDef::new(|_| std::future::pending::<shelterwood::ExitResult>())
                .shutdown(Shutdown::graceful(Duration::from_secs(60)).expect("grace is non-zero")),
        )
        .expect("valid prefix");
    tree.add_task(
        "failure",
        TaskDef::new(|_| async { Err(ExitError::message("original startup cause")) })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
    )
    .expect("valid failure");

    let error = tree
        .spawn()
        .expect("runtime is available")
        .start_or_shutdown(Duration::ZERO)
        .await
        .expect_err("startup fails and zero-budget rollback reports its live prefix");

    let StartupError::StartupFailed(failure) = error.startup else {
        panic!("expected the original structured startup failure");
    };
    assert!(matches!(
        failure.cause,
        StartupFailureCause::Child { ref id, ref exit, .. }
            if id.as_str() == "failure"
                && matches!(exit.kind(), ExitKind::Failed(cause) if cause.to_string() == "original startup cause")
    ));
    let rollback = error
        .rollback_timeout
        .expect("the still-live prefix is retained as rollback evidence");
    assert_eq!(rollback.stragglers.len(), 1);
    assert_eq!(rollback.stragglers[0].path[0].as_str(), "prefix");
    assert_eq!(rollback.stragglers[0].membership, prefix.membership());
    assert!(matches!(
        prefix.wait().await.kind(),
        ExitKind::Aborted { .. }
    ));
}

#[tokio::test(start_paused = true)]
async fn aggregate_readiness_stays_monotonic_after_a_ready_child_restarts() {
    let incarnation = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let second_started = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new({
            let incarnation = Arc::clone(&incarnation);
            let fail_first = fail_first.clone();
            let second_started = second_started.clone();
            move |context| {
                let current = incarnation.fetch_add(1, Ordering::SeqCst) + 1;
                let fail_first = fail_first.clone();
                let second_started = second_started.clone();
                async move {
                    if current == 1 {
                        context.mark_ready();
                        fail_first.wait().await;
                        return Err(ExitError::message("restart after aggregate readiness"));
                    }
                    second_started.release();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded)
        .shutdown(Shutdown::Abort),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("the first incarnation releases aggregate readiness");

    fail_first.release();
    second_started.wait().await;
    let snapshot = system.scope().snapshot();
    assert_eq!(snapshot.state, ScopeState::Running);
    assert!(matches!(
        snapshot
            .child("worker")
            .expect("worker remains resident")
            .state,
        ChildState::Starting
    ));
    system
        .wait_started()
        .await
        .expect("published aggregate readiness never regresses");

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn nested_startup_rollback_includes_runtime_added_members() {
    let failure_entered = ReleaseGate::default();
    let fail = ReleaseGate::default();
    let runtime_started = ReleaseGate::default();
    let runtime_cancelled = ReleaseGate::default();
    let mut nested = DynamicTree::new();
    nested
        .add_task(
            "failure",
            TaskDef::new({
                let failure_entered = failure_entered.clone();
                let fail = fail.clone();
                move |_| {
                    let failure_entered = failure_entered.clone();
                    let fail = fail.clone();
                    async move {
                        failure_entered.release();
                        fail.wait().await;
                        Err(ExitError::message("nested startup failure"))
                    }
                }
            })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid failure");
    let mut root = Tree::new();
    let nested = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid nested scope");
    let system = root.spawn().expect("runtime is available");
    failure_entered.wait().await;

    let runtime = nested
        .add_task(
            "runtime",
            TaskDef::new({
                let runtime_started = runtime_started.clone();
                let runtime_cancelled = runtime_cancelled.clone();
                move |context| {
                    let runtime_started = runtime_started.clone();
                    let runtime_cancelled = runtime_cancelled.clone();
                    async move {
                        runtime_started.release();
                        context.shutdown_token().cancelled().await;
                        runtime_cancelled.release();
                        Ok(())
                    }
                }
            }),
        )
        .await
        .expect("runtime member is admitted during nested startup");
    runtime_started.wait().await;

    fail.release();
    assert!(matches!(
        system.wait_started().await,
        Err(StartupError::StartupFailed(_))
    ));
    runtime_cancelled.wait().await;
    let runtime_exit = runtime.wait().await;
    assert!(matches!(runtime_exit.kind(), ExitKind::Completed));
    assert_eq!(runtime_exit.cancellation(), Cancellation::Observed);
    assert!(matches!(
        nested.as_scope().wait_stopped().await,
        shelterwood::StopReason::StartupFailed(_)
    ));

    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("failed root rolls back");
}

struct NeverConstructed;

impl RawActor for NeverConstructed {
    type Msg = ();

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        unreachable!("the factory panics before an incarnation exists")
    }
}

#[tokio::test]
async fn immediate_raw_construction_panic_classifies_post_ready() {
    // Resolved-`Immediate` readiness publishes at spawn, before the raw
    // factory runs (SPEC §7): a construction panic is a post-ready failure,
    // so ordered startup has already advanced past the child and later
    // siblings still start.
    let sibling_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_raw(
        "constructs-never",
        RawDef::factory(|| -> NeverConstructed { panic!("raw construction panic") })
            .restart(never()),
    )
    .expect("valid raw actor");
    tree.add_task(
        "sibling",
        construction_signalled_waiting_task(Arc::clone(&sibling_started)),
    )
    .expect("valid sibling");

    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("spawn-time readiness classifies the construction panic post-ready");
    assert!(sibling_started.load(Ordering::SeqCst));
    assert_eventually!(
        || {
            system
                .scope()
                .child("constructs-never")
                .is_some_and(|child| matches!(
                    &child.state,
                    ChildState::Stopped { exit } if matches!(exit.kind(), ExitKind::Panicked { .. })
                ))
        },
        "the terminal panic is an ordinary post-ready stop, not a startup abort"
    )
    .await;
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("clean shutdown");
}

/// Blocks its destructor until released, holding a retained construction's
/// disposal open on the blocking pool.
struct SlowDrop {
    disposing: Option<std::sync::mpsc::Sender<()>>,
    release: Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl Drop for SlowDrop {
    fn drop(&mut self) {
        if let Some(disposing) = self.disposing.take() {
            let _ = disposing.send(());
        }
        let _ = self
            .release
            .lock()
            .expect("release lock")
            .recv_timeout(Duration::from_secs(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_failure_is_decided_at_dispatch_not_after_construction_disposal() {
    // §7: the scope leaves `Starting` the moment the terminal pre-ready exit
    // is dispatched. A shutdown request landing while the failed child's
    // retained factory is still being destroyed must not replace the verdict.
    let (disposing_sender, disposing) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let slow = SlowDrop {
        disposing: Some(disposing_sender),
        release: Arc::new(Mutex::new(blocked)),
    };
    let mut tree = Tree::new();
    tree.add_task(
        "failure",
        TaskDef::new(move |_| {
            let _captured = &slow;
            async { Err(ExitError::message("pre-ready failure")) }
        })
        .restart(never())
        .readiness(Readiness::Manual)
        .expect("manual readiness"),
    )
    .expect("valid failing task");
    let system = tree.spawn().expect("runtime is available");

    tokio::task::spawn_blocking(move || disposing.recv_timeout(POLL_TIMEOUT))
        .await
        .expect("the wait does not panic")
        .expect("the failed child's construction is being disposed");
    system.scope().request_shutdown();

    let startup = system.wait_started().await.expect_err("startup fails");
    assert!(
        matches!(
            startup,
            StartupError::StartupFailed(ref failure)
                if matches!(failure.cause, StartupFailureCause::Child { ref id, .. } if id.as_str() == "failure")
        ),
        "{startup:?}"
    );
    // §12: the named child is already terminal, with exactly the payload's
    // exit, while its factory's destruction is still blocked.
    let StartupError::StartupFailed(ref failure) = startup else {
        unreachable!("matched above")
    };
    let StartupFailureCause::Child {
        exit: ref reported, ..
    } = failure.cause
    else {
        unreachable!("matched above")
    };
    assert!(
        matches!(
            system.scope().snapshot().child("failure").map(|child| &child.state),
            Some(ChildState::StartupAborted { exit }) if exit == reported
        ),
        "{:?}",
        system.scope().snapshot().child("failure")
    );
    release.send(()).expect("the disposal is still blocked");
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("the requested shutdown completes");
}

/// A started task that, once cancelled, holds its stop open until `release`,
/// so an ordered drain's reverse walk parks on it and the members it already
/// stopped stay resident and observable before scope exit clears them.
fn drain_hold(release: ReleaseGate) -> TaskDef {
    TaskDef::new(move |context| {
        let release = release.clone();
        async move {
            context.shutdown_token().cancelled().await;
            release.wait().await;
            Ok(())
        }
    })
    .shutdown(Shutdown::graceful(Duration::from_secs(60)).expect("grace is non-zero"))
}

/// B.6 reserves `StartupAborted` for §7's terminal pre-ready failure. An owner
/// shutdown during `Starting` stops the root's still-starting child, and the
/// ancestor shutdown reaching the nested scope stops its gated child; neither
/// stop is a startup failure, so both terminals are `Stopped`.
#[tokio::test]
async fn shutdown_during_starting_stops_gated_children_without_startup_abort() {
    let started = Arc::new(AtomicBool::new(false));
    let root_hold = ReleaseGate::default();
    let nested_hold = ReleaseGate::default();
    let mut nested = Tree::new();
    nested
        .add_task("hold", drain_hold(nested_hold.clone()))
        .expect("valid held task");
    nested
        .add_task(
            "gate",
            start_signalled_waiting_task(Arc::clone(&started))
                .readiness(Readiness::Manual)
                .expect("manual readiness")
                .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .expect("valid gated task");
    let mut tree = Tree::new();
    tree.add_task("hold", drain_hold(root_hold.clone()))
        .expect("valid held task");
    tree.add_subtree_once(
        "inner",
        SubtreeOnceDef::new(nested)
            .readiness_deadline(ReadinessDeadline::Unbounded)
            .retention(Retention::Retain),
    )
    .expect("valid nested scope");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert_eventually!(|| started.load(Ordering::SeqCst)).await;
    assert!(matches!(scope.snapshot().state, ScopeState::Starting));

    scope.request_shutdown();
    let inner = scope
        .wait_for_child(
            "inner",
            |child| {
                child
                    .nested
                    .as_ref()
                    .and_then(|nested| nested.child("gate"))
                    .is_some_and(|gate| gate.state.is_terminal())
            },
            POLL_TIMEOUT,
        )
        .await
        .expect("the nested gate terminalizes while the nested hold drains");
    let gate = inner
        .nested
        .as_ref()
        .and_then(|nested| nested.child("gate"))
        .expect("the predicate matched the nested gate");
    assert!(
        matches!(gate.state, ChildState::Stopped { .. }),
        "an ancestor shutdown during Starting is not a startup abort: {gate:?}"
    );

    nested_hold.release();
    let inner = scope
        .wait_for_child("inner", |child| child.state.is_terminal(), POLL_TIMEOUT)
        .await
        .expect("the nested scope terminalizes while the root hold drains");
    assert!(
        matches!(inner.state, ChildState::Stopped { .. }),
        "an owner shutdown during Starting is not a startup abort: {inner:?}"
    );

    root_hold.release();
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("the requested shutdown completes");
}

/// A root parked in `StartupFailed` keeps dispatching its started prefix as
/// `Running` (§12). A prefix child that was ready, fails, and restarts during
/// the park has spent its initial readiness edge, so an owner shutdown that
/// stops the restarted incarnation before it marks ready is not a startup
/// abort (B.6).
#[tokio::test]
async fn parked_prefix_restart_stopped_by_shutdown_is_not_a_startup_abort() {
    let hold = ReleaseGate::default();
    let fail = ReleaseGate::default();
    let tree = parked_prefix_tree(hold.clone(), fail.clone(), |context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    });
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert!(matches!(
        system.wait_started().await,
        Err(StartupError::StartupFailed(_))
    ));

    fail.release();
    assert_eventually!(|| scope.child("prefix").is_some_and(|child| {
        child.restart_count.get() == 1 && matches!(child.state, ChildState::Starting)
    }))
    .await;
    assert!(matches!(scope.snapshot().state, ScopeState::StartupFailed));

    scope.request_shutdown();
    let prefix = scope
        .wait_for_child("prefix", |child| child.state.is_terminal(), POLL_TIMEOUT)
        .await
        .expect("the prefix terminalizes while the hold drains");
    assert!(
        matches!(prefix.state, ChildState::Stopped { .. }),
        "a restarted prefix stopped by shutdown is not a startup abort: {prefix:?}"
    );

    hold.release();
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("the requested shutdown completes");
}

/// The park's exit funnel dispatches as `Running` (§12), so a restarted prefix
/// incarnation that completes before marking ready is an ordinary terminal
/// exit under its restart policy. Its initial readiness edge already fired
/// before the park; the restart does not rewind it (§7, B.6).
#[tokio::test]
async fn parked_prefix_restart_completing_pre_ready_is_not_a_startup_abort() {
    let hold = ReleaseGate::default();
    let fail = ReleaseGate::default();
    let tree = parked_prefix_tree(hold.clone(), fail.clone(), |_| async { Ok(()) });
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert!(matches!(
        system.wait_started().await,
        Err(StartupError::StartupFailed(_))
    ));

    fail.release();
    let prefix = scope
        .wait_for_child("prefix", |child| child.state.is_terminal(), POLL_TIMEOUT)
        .await
        .expect("the restarted prefix completes");
    assert!(
        matches!(
            &prefix.state,
            ChildState::Stopped { exit } if matches!(exit.kind(), ExitKind::Completed)
        ),
        "a restarted prefix completing in the park is not a startup abort: {prefix:?}"
    );
    assert!(matches!(scope.snapshot().state, ScopeState::StartupFailed));

    hold.release();
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("the parked root stops");
}

/// An ordered root led by a [`drain_hold`], then a `prefix` that marks ready,
/// fails once `fail` is released and restarts under `OnFailure` into
/// `restarted`, then a sibling whose terminal pre-ready failure parks the root
/// in `StartupFailed`.
fn parked_prefix_tree<F, Fut>(hold: ReleaseGate, fail: ReleaseGate, restarted: F) -> Tree
where
    F: Fn(shelterwood::TaskContext) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = ExitResult> + Send + 'static,
{
    let runs = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_task("hold", drain_hold(hold))
        .expect("valid held task");
    tree.add_task(
        "prefix",
        TaskDef::new(move |context| {
            let first = runs.fetch_add(1, Ordering::SeqCst) == 0;
            let fail = fail.clone();
            let restarted = restarted.clone();
            async move {
                if !first {
                    return restarted(context).await;
                }
                context.mark_ready();
                fail.wait().await;
                Err(ExitError::message("post-ready failure"))
            }
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded)
        .restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::Immediate,
        )),
    )
    .expect("valid prefix task");
    tree.add_task(
        "failing",
        TaskDef::new(|_| async { Err(ExitError::message("pre-ready failure")) })
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .restart(never()),
    )
    .expect("valid failing task");
    tree
}
