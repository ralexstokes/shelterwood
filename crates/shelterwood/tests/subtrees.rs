use std::{
    future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Backoff, DynamicTree, ExitError, ExitKind, Intensity, RestartCondition, RestartPolicy,
    Shutdown, StartupError, StartupFailureCause, StopReason, SubtreeDef, SubtreeOnceDef, TaskDef,
    TaskOnceDef, TaskRef, Tree,
};
use shelterwood_test_support::{advance_time, poll_until};

#[tokio::test]
async fn a_restartable_subtree_can_heal_an_unfilled_lowering_failure() {
    let calls = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(Mutex::new(None::<TaskRef>));
    let definition = SubtreeDef::factory({
        let calls = Arc::clone(&calls);
        let rejected = Arc::clone(&rejected);
        move || {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            let mut tree = Tree::new();
            if call == 0 {
                let slot = tree.reserve_task("undefined").expect("reservation");
                *rejected.lock().expect("rejected mutex poisoned") = Some(slot.task_ref());
            } else {
                tree.add_task(
                    "healthy",
                    TaskDef::new(|context| async move {
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }),
                )
                .expect("valid task");
            }
            tree
        }
    });
    let mut root = Tree::new();
    root.intensity(Intensity::new(5, Duration::from_secs(1)).expect("valid intensity"));
    root.add_subtree("nested", definition)
        .expect("valid subtree edge");
    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("second subtree declaration heals startup");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let rejected = rejected
        .lock()
        .expect("rejected mutex poisoned")
        .clone()
        .expect("first factory exposed its cell");
    assert!(matches!(
        rejected.wait().await.kind(),
        ExitKind::NeverStarted
    ));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn deterministic_unfilled_subtree_churns_to_the_scope_intensity_trip() {
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = SubtreeDef::factory({
        let calls = Arc::clone(&calls);
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            let mut tree = Tree::new();
            let _undefined = tree.reserve_task("undefined").expect("reservation");
            tree
        }
    });
    let mut root = Tree::new();
    root.intensity(Intensity::new(1, Duration::from_secs(10)).expect("valid intensity"));
    root.add_subtree("nested", definition)
        .expect("valid subtree edge");
    let system = root.spawn().expect("runtime is available");
    let startup = system.wait_started().await.expect_err("budget trips");
    let trip = match startup {
        StartupError::IntensityTripped(trip) => trip,
        other => panic!("unexpected startup error: {other:?}"),
    };
    assert_eq!(trip.observed_restarts, 2);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "tripping restart never spawns"
    );
    assert!(matches!(
        system.wait().await,
        StopReason::IntensityTripped(_)
    ));
}

#[tokio::test]
async fn one_shot_subtree_lowering_failure_retains_structured_provenance() {
    let mut nested = Tree::new();
    let _undefined = nested.reserve_task("missing").expect("reservation");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree edge");
    let system = root.spawn().expect("runtime is available");
    let StartupError::StartupFailed(failure) = system
        .wait_started()
        .await
        .expect_err("nested lowering fails")
    else {
        panic!("expected structured startup failure");
    };
    let StartupFailureCause::Child { id, exit, .. } = failure.cause else {
        panic!("root failure must name its nested child");
    };
    assert_eq!(id.as_str(), "nested");
    let ExitKind::Failed(error) = exit.kind() else {
        panic!("nested lowering is a child failure");
    };
    let nested = error
        .startup_failure()
        .expect("framework provenance is retained");
    assert!(matches!(
        nested.cause,
        StartupFailureCause::Lowering { ref undefined }
            if undefined.len() == 1 && undefined[0][0].as_str() == "missing"
    ));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
}

#[tokio::test]
async fn over_budget_restart_is_charged_but_never_spawned() {
    let starts = Arc::new(AtomicUsize::new(0));
    let mut root = Tree::new();
    root.intensity(Intensity::new(2, Duration::from_secs(10)).expect("valid intensity"));
    root.add_task(
        "failing",
        TaskDef::new({
            let starts = Arc::clone(&starts);
            move |_| {
                starts.fetch_add(1, Ordering::SeqCst);
                async { Err(ExitError::message("retry")) }
            }
        }),
    )
    .expect("valid task");
    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("immediate readiness starts root");
    let reason = system.wait().await;
    let StopReason::IntensityTripped(trip) = reason else {
        panic!("expected intensity trip");
    };
    assert_eq!(trip.observed_restarts, 3);
    assert_eq!(
        starts.load(Ordering::SeqCst),
        3,
        "fourth spawn is suppressed"
    );
}

#[tokio::test]
async fn dynamic_terminal_pruning_cannot_mask_an_intensity_trip() {
    let starts = Arc::new(AtomicUsize::new(0));
    let mut root = DynamicTree::new();
    root.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
    let task = root
        .add_task(
            "failing",
            TaskDef::new({
                let starts = Arc::clone(&starts);
                move |_| {
                    starts.fetch_add(1, Ordering::SeqCst);
                    async { Err(ExitError::message("retry")) }
                }
            })
            .retention(shelterwood::Retention::Remove),
        )
        .expect("valid task");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    assert!(matches!(
        system.wait().await,
        StopReason::IntensityTripped(_)
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(matches!(task.wait().await.kind(), ExitKind::Failed(_)));
}

#[tokio::test(start_paused = true)]
async fn intensity_window_ages_out_between_restart_schedules() {
    let starts = Arc::new(AtomicUsize::new(0));
    let mut root = Tree::new();
    root.intensity(Intensity::new(1, Duration::from_secs(10)).expect("valid intensity"));
    root.add_task(
        "failing-slowly",
        TaskDef::new({
            let starts = Arc::clone(&starts);
            move |context| {
                let current = starts.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if current >= 3 {
                        context.shutdown_token().cancelled().await;
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_secs(11)).await;
                    Err(ExitError::message("retry outside window"))
                }
            }
        }),
    )
    .expect("valid task");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    advance_time(Duration::from_secs(11)).await;
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) >= 2
        })
        .await
    );
    advance_time(Duration::from_secs(11)).await;
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) >= 3
        })
        .await
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("budget did not trip");
}

#[tokio::test]
async fn recursive_shutdown_reaches_nested_descendants() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut nested = Tree::new();
    nested
        .add_task(
            "leaf",
            TaskDef::new({
                let cancelled = Arc::clone(&cancelled);
                move |context| {
                    let cancelled = Arc::clone(&cancelled);
                    async move {
                        context.shutdown_token().cancelled().await;
                        cancelled.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid leaf");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("recursive shutdown joins");
    assert!(cancelled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn zero_timeout_reports_recursive_straggler_paths_and_joins_them() {
    let live = Arc::new(AtomicBool::new(true));
    struct Clears(Arc<AtomicBool>);
    impl Drop for Clears {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }

    let mut nested = Tree::new();
    nested
        .add_task(
            "leaf",
            TaskDef::new({
                let live = Arc::clone(&live);
                move |_| {
                    let guard = Clears(Arc::clone(&live));
                    async move {
                        let _guard = guard;
                        future::pending().await
                    }
                }
            })
            .shutdown(Shutdown::Abort),
        )
        .expect("valid leaf");
    let mut root = Tree::new();
    root.add_subtree_once(
        "nested",
        SubtreeOnceDef::new(nested).shutdown(Shutdown::Abort),
    )
    .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let timeout = system
        .shutdown(Duration::ZERO)
        .await
        .expect_err("zero timeout escalates live descendants");
    assert_eq!(timeout.stragglers.len(), 1);
    assert_eq!(
        timeout.stragglers[0]
            .path
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        ["nested", "leaf"]
    );
    assert!(
        !live.load(Ordering::SeqCst),
        "shutdown returns only after join"
    );
}

#[tokio::test(start_paused = true)]
async fn ordered_graces_sum_while_dynamic_graces_overlap() {
    fn stubborn() -> TaskDef {
        TaskDef::new(|_| future::pending()).shutdown(Shutdown::Graceful {
            grace: Duration::from_secs(10),
        })
    }

    let mut ordered = Tree::new();
    ordered.add_task("one", stubborn()).expect("valid task");
    ordered.add_task("two", stubborn()).expect("valid task");
    let ordered = ordered.spawn().expect("runtime is available");
    ordered.wait_started().await.expect("ordered starts");
    let started = tokio::time::Instant::now();
    ordered
        .shutdown(Duration::from_secs(60))
        .await
        .expect("child policies bound shutdown");
    let ordered_elapsed = tokio::time::Instant::now() - started;
    assert!(ordered_elapsed >= Duration::from_secs(20));

    let mut dynamic = DynamicTree::new();
    dynamic.add_task("one", stubborn()).expect("valid task");
    dynamic.add_task("two", stubborn()).expect("valid task");
    let dynamic = dynamic.spawn().expect("runtime is available");
    dynamic.wait_started().await.expect("dynamic starts");
    let started = tokio::time::Instant::now();
    dynamic
        .shutdown(Duration::from_secs(60))
        .await
        .expect("child policies bound shutdown");
    let dynamic_elapsed = tokio::time::Instant::now() - started;
    assert!(dynamic_elapsed >= Duration::from_secs(10));
    assert!(dynamic_elapsed < Duration::from_secs(15));
}

#[tokio::test]
async fn dynamic_and_always_members_do_not_finish_naturally() {
    let mut dynamic = DynamicTree::new();
    let (_task, _completion) = dynamic
        .add_task_once(
            "once",
            TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }),
        )
        .expect("valid task");
    let dynamic = dynamic.spawn().expect("runtime is available");
    dynamic.wait_started().await.expect("dynamic starts");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), dynamic.scope().wait_stopped())
            .await
            .is_err()
    );
    dynamic
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic stops by owner");

    let starts = Arc::new(AtomicUsize::new(0));
    let mut ordered = Tree::new();
    ordered.intensity(Intensity::new(100, Duration::from_secs(1)).expect("valid intensity"));
    ordered
        .add_task(
            "always",
            TaskDef::new({
                let starts = Arc::clone(&starts);
                move |context| {
                    let current = starts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if current == 0 {
                            Ok(())
                        } else {
                            context.shutdown_token().cancelled().await;
                            Ok(())
                        }
                    }
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Immediate,
            )),
        )
        .expect("valid task");
    let ordered = ordered.spawn().expect("runtime is available");
    ordered.wait_started().await.expect("ordered starts");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) >= 2
        })
        .await
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), ordered.scope().wait_stopped())
            .await
            .is_err()
    );
    ordered
        .shutdown(Duration::from_secs(1))
        .await
        .expect("owner stops always member");
}

#[tokio::test]
async fn hard_aborted_subtree_descendants_still_publish_exits() {
    let mut nested = Tree::new();
    let leaf = nested
        .add_task(
            "leaf",
            TaskDef::new(|_| future::pending()).shutdown(Shutdown::Graceful {
                grace: Duration::from_secs(60),
            }),
        )
        .expect("valid leaf");
    let mut root = Tree::new();
    root.add_subtree_once(
        "nested",
        SubtreeOnceDef::new(nested).shutdown(Shutdown::Abort),
    )
    .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    system
        .shutdown(Duration::from_secs(5))
        .await
        .expect("the subtree's abort policy bounds teardown");
    let exit = tokio::time::timeout(Duration::from_secs(1), leaf.wait())
        .await
        .expect("hard-aborted descendants terminalize");
    assert!(matches!(exit.kind(), ExitKind::Aborted { .. }));
    assert!(exit.cancelled());
}

#[tokio::test]
async fn shutdown_and_wait_wakes_when_a_parent_drain_terminalizes_a_restarting_subtree() {
    let gate = shelterwood_test_support::ReleaseGate::default();
    let inner = Arc::new(Mutex::new(None::<TaskRef>));
    let definition = SubtreeDef::factory({
        let gate = gate.clone();
        let inner = Arc::clone(&inner);
        move || {
            let mut tree = Tree::new();
            tree.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
            let task = tree
                .add_task(
                    "failing",
                    TaskDef::new({
                        let gate = gate.clone();
                        move |_| {
                            let gate = gate.clone();
                            async move {
                                gate.wait().await;
                                Err(ExitError::message("trip the nested budget"))
                            }
                        }
                    }),
                )
                .expect("valid task");
            inner
                .lock()
                .expect("handle mutex poisoned")
                .get_or_insert(task);
            tree
        }
    })
    .restart(RestartPolicy::new(
        RestartCondition::Always,
        Backoff::fixed(Duration::from_secs(60), shelterwood::Jitter::None)
            .expect("non-zero backoff"),
    ));
    let mut root = Tree::new();
    let sub = root
        .add_subtree("nested", definition)
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let inner = inner
        .lock()
        .expect("handle mutex poisoned")
        .clone()
        .expect("first subtree incarnation exposed its task");
    gate.release();
    // The nested budget trips and the incarnation finishes; the parent then
    // schedules the subtree restart far in the future, so the membership
    // sits in its restart window with no live incarnation.
    assert!(matches!(inner.wait().await.kind(), ExitKind::Failed(_)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let waiter = tokio::spawn({
        let sub = sub.clone();
        async move { sub.shutdown_and_wait(Duration::from_secs(1)).await }
    });
    // Give the waiter time to park on the scope signal before draining.
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(system);
    tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("parent drain terminalizes the restarting subtree and wakes waiters")
        .expect("waiter joins")
        .expect("teardown completes in bound");
}
