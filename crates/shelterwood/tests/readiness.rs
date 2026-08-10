use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, ReleaseGate, advance_time, assert_quiet, policy::never, poll_until,
};
use shelterwood::{
    Backoff, Cancellation, ChildState, DynamicTree, ExitError, ExitKind, ExitResult, Jitter,
    RawActor, RawContext, RawDef, RawOnceDef, Readiness, ReadinessDeadline, RestartCondition,
    RestartPolicy, ScopeState, Shutdown, StartupError, StartupFailureCause, SubtreeOnceDef,
    TaskDef, Tree,
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            system
                .scope()
                .child("ready-then-fail")
                .is_some_and(|child| matches!(child.state, ChildState::Restarting))
        })
        .await
    );
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            system
                .scope()
                .child("restarting-immediate")
                .is_some_and(|child| matches!(child.state, ChildState::Restarting))
        })
        .await
    );
    release_manual.release();
    manual_ready.wait().await;
    assert_eq!(system.scope().snapshot().state, ScopeState::Starting);

    advance_time(backoff).await;
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            incarnations.load(Ordering::SeqCst) == 2
        })
        .await
    );
    system
        .wait_started()
        .await
        .expect("immediate restart releases the last aggregate gate");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("restarted task cooperates");
}

#[tokio::test]
async fn ordered_startup_waits_for_manual_readiness() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let release_ready = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gated",
        TaskDef::new({
            let order = Arc::clone(&order);
            let release_ready = release_ready.clone();
            move |context| {
                let order = Arc::clone(&order);
                let release_ready = release_ready.clone();
                async move {
                    order.lock().expect("order mutex poisoned").push("gated");
                    release_ready.wait().await;
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            order.lock().expect("order mutex poisoned").as_slice() == ["gated"]
        })
        .await
    );
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
    let before = std::time::Instant::now();
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            ready_started.load(Ordering::SeqCst)
        })
        .await
    );
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
            TaskDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            })
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

#[tokio::test]
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
        TaskDef::new({
            let later_started = Arc::clone(&later_started);
            let release_second = release_second.clone();
            move |context| {
                let later_started = Arc::clone(&later_started);
                let release_second = release_second.clone();
                async move {
                    later_started.store(true, Ordering::SeqCst);
                    release_second.wait().await;
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
    let system = tree.spawn().expect("runtime is available");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            later_started.load(Ordering::SeqCst)
        })
        .await
    );
    fail_first.release();
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            incarnation.load(Ordering::SeqCst) == 2
        })
        .await
    );
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

#[tokio::test]
async fn ordered_terminal_pre_ready_exit_parks_the_root_and_marks_suffix_never_started() {
    let prefix_cancelled = Arc::new(AtomicBool::new(false));
    let suffix_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_task(
        "prefix",
        TaskDef::new({
            let prefix_cancelled = Arc::clone(&prefix_cancelled);
            move |context| {
                let prefix_cancelled = Arc::clone(&prefix_cancelled);
                async move {
                    context.shutdown_token().cancelled().await;
                    prefix_cancelled.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        }),
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

#[tokio::test]
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
        TaskDef::new({
            let sibling_cancelled = Arc::clone(&sibling_cancelled);
            let sibling_started = Arc::clone(&sibling_started);
            move |context| {
                let sibling_cancelled = Arc::clone(&sibling_cancelled);
                let sibling_started = Arc::clone(&sibling_started);
                async move {
                    sibling_started.store(true, Ordering::SeqCst);
                    context.shutdown_token().cancelled().await;
                    sibling_cancelled.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        })
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

#[tokio::test]
async fn runtime_dynamic_additions_never_join_aggregate_readiness() {
    let initial_release = ReleaseGate::default();
    let initial_started = Arc::new(AtomicBool::new(false));
    let runtime_started = Arc::new(AtomicBool::new(false));
    let mut tree = DynamicTree::new();
    tree.add_task(
        "initial",
        TaskDef::new({
            let initial_release = initial_release.clone();
            let initial_started = Arc::clone(&initial_started);
            move |context| {
                let initial_release = initial_release.clone();
                let initial_started = Arc::clone(&initial_started);
                async move {
                    initial_started.store(true, Ordering::SeqCst);
                    initial_release.wait().await;
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
    .expect("valid initial member");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            initial_started.load(Ordering::SeqCst)
        })
        .await
    );
    let runtime_task = scope
        .add_task(
            "runtime",
            TaskDef::new({
                let runtime_started = Arc::clone(&runtime_started);
                move |context| {
                    let runtime_started = Arc::clone(&runtime_started);
                    async move {
                        runtime_started.store(true, Ordering::SeqCst);
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(ReadinessDeadline::Unbounded),
        )
        .await
        .expect("runtime member is admitted");
    assert!(runtime_started.load(Ordering::SeqCst));
    initial_release.release();
    system
        .wait_started()
        .await
        .expect("only the initial set gates aggregate readiness");
    assert_eq!(
        scope.remove_task(&runtime_task).await,
        shelterwood::RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
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
            TaskDef::new({
                let sibling_cancelled = Arc::clone(&sibling_cancelled);
                move |context| {
                    let sibling_cancelled = Arc::clone(&sibling_cancelled);
                    async move {
                        context.shutdown_token().cancelled().await;
                        sibling_cancelled.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                }
            })
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
        "child `nested` failed during startup: child `inner-failure` failed during startup: startup failure"
    );
    let inner_source = outer_source
        .source()
        .expect("the child exit exposes the nested structured failure");
    assert_eq!(
        inner_source.to_string(),
        "child `inner-failure` failed during startup: startup failure"
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
        nested_scope.wait_stopped().await,
        shelterwood::StopReason::StartupFailed(_)
    ));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
}

#[tokio::test]
async fn start_or_shutdown_preserves_startup_error_and_rolls_back_the_prefix() {
    let prefix_cancelled = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_task(
        "prefix",
        TaskDef::new({
            let prefix_cancelled = Arc::clone(&prefix_cancelled);
            move |context| {
                let prefix_cancelled = Arc::clone(&prefix_cancelled);
                async move {
                    context.shutdown_token().cancelled().await;
                    prefix_cancelled.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        }),
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
        .start_or_shutdown(Duration::from_secs(1))
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
            TaskDef::new(|_| std::future::pending::<shelterwood::ExitResult>()).shutdown(
                Shutdown::Graceful {
                    grace: Duration::from_secs(60),
                },
            ),
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
        nested.wait_stopped().await,
        shelterwood::StopReason::StartupFailed(_)
    ));

    system
        .shutdown(Duration::from_secs(1))
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
    // factory runs (SPEC §6): a construction panic is a post-ready failure,
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
        TaskDef::new({
            let sibling_started = Arc::clone(&sibling_started);
            move |context| {
                sibling_started.store(true, Ordering::SeqCst);
                async move {
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("valid sibling");

    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("spawn-time readiness classifies the construction panic post-ready");
    assert!(sibling_started.load(Ordering::SeqCst));
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            system
                .scope()
                .child("constructs-never")
                .is_some_and(|child| matches!(
                    &child.state,
                    ChildState::Stopped { exit } if matches!(exit.kind(), ExitKind::Panicked { .. })
                ))
        })
        .await,
        "the terminal panic is an ordinary post-ready stop, not a startup abort"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("clean shutdown");
}
