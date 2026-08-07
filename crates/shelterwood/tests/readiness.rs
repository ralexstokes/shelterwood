use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Backoff, DynamicTree, ExitError, ExitKind, Readiness, ReadinessDeadline, RestartCondition,
    RestartPolicy, Shutdown, StartupError, StartupFailureCause, SubtreeOnceDef, TaskDef, Tree,
};
use shelterwood_test_support::{ReleaseGate, advance_time, assert_quiet, poll_until};

fn never() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
    let mut ready_tree = Tree::new();
    let ready_task = ready_tree
        .add_task(
            "edge",
            TaskDef::new({
                let ready_started = Arc::clone(&ready_started);
                move |context| {
                    let ready_started = Arc::clone(&ready_started);
                    async move {
                        ready_started.store(true, Ordering::SeqCst);
                        tokio::time::sleep(width).await;
                        context.mark_ready();
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            ready_started.load(Ordering::SeqCst)
        })
        .await
    );
    advance_time(width).await;
    tokio::task::yield_now().await;
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
    assert!(exit.cancelled());
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            later_started.load(Ordering::SeqCst)
        })
        .await
    );
    fail_first.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
        .expect("runtime member is admitted")
        .into_handles();
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
