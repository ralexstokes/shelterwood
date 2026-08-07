use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use shelterwood::{
    Backoff, DynamicTree, ExitError, ExitKind, NotAdmittingCause, Readiness, RemoveOutcome,
    ReserveError, RestartCondition, RestartPolicy, Retention, Shutdown, StopReason, SubtreeDef,
    SubtreeOnceDef, TaskDef, TaskOnceDef, Tree,
};
use shelterwood_test_support::{ReleaseGate, advance_time, assert_quiet, poll_until};

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn never() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

fn waiting_task() -> TaskDef {
    TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    })
}

fn waiting_tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task("worker", waiting_task()).expect("valid task");
    tree
}

#[tokio::test]
async fn exact_handles_reject_cross_scope_and_same_id_successors() {
    let left = DynamicTree::new().spawn().expect("runtime is available");
    let right = DynamicTree::new().spawn().expect("runtime is available");
    left.wait_started().await.expect("left starts");
    right.wait_started().await.expect("right starts");
    let left_scope = left.scope();
    let right_scope = right.scope();
    let left_task = left_scope
        .add_task("same", waiting_task())
        .await
        .expect("left admission")
        .into_handles();
    let right_task = right_scope
        .add_task("same", waiting_task())
        .await
        .expect("right admission")
        .into_handles();

    assert_eq!(
        right_scope.remove_task(&left_task).await,
        RemoveOutcome::AlreadyAbsent
    );
    assert_eq!(
        left_scope.remove_task(&left_task).await,
        RemoveOutcome::Removed
    );
    let replacement = left_scope
        .add_task("same", waiting_task())
        .await
        .expect("replacement admission")
        .into_handles();
    assert!(replacement.membership().supersedes(left_task.membership()));
    assert_eq!(
        left_scope.remove_task(&left_task).await,
        RemoveOutcome::AlreadyAbsent
    );
    assert_eq!(
        left_scope.remove_task(&replacement).await,
        RemoveOutcome::Removed
    );
    assert_eq!(
        right_scope.remove_task(&right_task).await,
        RemoveOutcome::Removed
    );
    left.shutdown(Duration::from_secs(1))
        .await
        .expect("left stops");
    right
        .shutdown(Duration::from_secs(1))
        .await
        .expect("right stops");
}

#[tokio::test]
async fn exact_scope_removal_does_not_touch_a_same_id_successor() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let root = system.scope();
    let first = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("first subtree admitted")
        .into_handles();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), root.remove_scope(&first))
            .await
            .expect("first subtree removal completes"),
        RemoveOutcome::Removed
    );
    let second = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("second subtree admitted")
        .into_handles();
    assert!(second.membership().supersedes(first.membership()));
    assert_eq!(
        root.remove_scope(&first).await,
        RemoveOutcome::AlreadyAbsent
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), root.remove_scope(&second))
            .await
            .expect("second subtree removal completes"),
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn tombstones_occupy_ids_until_explicit_removal() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let task = scope
        .add_task(
            "tombstone",
            TaskDef::new(|_| async { Ok(()) })
                .restart(never())
                .retention(Retention::Retain),
        )
        .await
        .expect("task admitted")
        .into_handles();
    task.wait().await;
    assert!(matches!(
        scope.add_task("tombstone", waiting_task()).await,
        Err(ReserveError::DuplicateId(ref id)) if id.as_str() == "tombstone"
    ));
    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    let replacement = scope
        .add_task("tombstone", waiting_task())
        .await
        .expect("removal frees id")
        .into_handles();
    assert_eq!(
        scope.remove_task(&replacement).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn removal_is_synchronous_detached_and_shared() {
    let gate = ReleaseGate::default();
    let cancelled = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let task = scope
        .add_task(
            "worker",
            TaskDef::new({
                let gate = gate.clone();
                let cancelled = Arc::clone(&cancelled);
                move |context| {
                    let gate = gate.clone();
                    let cancelled = Arc::clone(&cancelled);
                    async move {
                        context.shutdown_token().cancelled().await;
                        cancelled.store(true, Ordering::SeqCst);
                        gate.wait().await;
                        Ok(())
                    }
                }
            }),
        )
        .await
        .expect("task admitted")
        .into_handles();

    let first = scope.remove_task(&task);
    let second = scope.remove_task(&task);
    assert!(matches!(
        scope.reserve_task("worker"),
        Err(ReserveError::RemovalInProgress(ref id)) if id.as_str() == "worker"
    ));
    drop(first);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    gate.release();
    assert_eq!(second.await, RemoveOutcome::Removed);
    let replacement = scope
        .add_task("worker", waiting_task())
        .await
        .expect("id is free after detached removal")
        .into_handles();
    assert_eq!(
        scope.remove_task(&replacement).await,
        RemoveOutcome::Removed
    );
    let shared = scope
        .add_task("shared", waiting_task())
        .await
        .expect("shared-removal task admitted")
        .into_handles();
    let (left, right) = tokio::join!(scope.remove_task(&shared), scope.remove_task(&shared));
    assert_eq!(left, RemoveOutcome::Removed);
    assert_eq!(right, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn reserved_cell_removal_wins_a_queued_split_definition() {
    let factory_dropped = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let slot = scope
        .reserve_task("reserved")
        .expect("reservation succeeds");
    let task = slot.task_ref();
    let removed = scope.remove("reserved");
    let admission = slot.define(TaskDef::new({
        let probe = DropProbe(Arc::clone(&factory_dropped));
        move |context| {
            let _ = &probe;
            async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    }));
    assert_eq!(removed.await, RemoveOutcome::Removed);
    assert!(matches!(
        admission.await,
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::ReservationEnded
        ))
    ));
    assert!(matches!(
        task.wait().await.kind(),
        shelterwood::ExitKind::NeverStarted
    ));
    assert!(factory_dropped.load(Ordering::SeqCst));

    let survivor = scope
        .add_task("survivor", waiting_task())
        .await
        .expect("scope remains admitting")
        .into_handles();
    assert_eq!(scope.remove_task(&survivor).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn fused_drop_withdraws_or_removes_while_split_drop_detaches() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    drop(scope.add_task("fused-before-poll", waiting_task()));
    let reused = scope
        .add_task("fused-before-poll", waiting_task())
        .await
        .expect("never-polled fused add withdraws")
        .into_handles();
    assert_eq!(scope.remove_task(&reused).await, RemoveOutcome::Removed);

    let fused_started = Arc::new(AtomicBool::new(false));
    let fused_cancelled = Arc::new(AtomicBool::new(false));
    let mut fused = Box::pin(scope.add_task(
        "fused-after-admission",
        TaskDef::new({
            let fused_started = Arc::clone(&fused_started);
            let fused_cancelled = Arc::clone(&fused_cancelled);
            move |context| {
                let fused_started = Arc::clone(&fused_started);
                let fused_cancelled = Arc::clone(&fused_cancelled);
                async move {
                    fused_started.store(true, Ordering::SeqCst);
                    context.shutdown_token().cancelled().await;
                    fused_cancelled.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        }),
    ));
    assert!(poll_once(fused.as_mut()).is_pending());
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            fused_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(fused);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            fused_cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    let reused = scope
        .add_task("fused-after-admission", waiting_task())
        .await
        .expect("post-admission fused drop removes")
        .into_handles();
    assert_eq!(scope.remove_task(&reused).await, RemoveOutcome::Removed);

    let split_started = Arc::new(AtomicBool::new(false));
    let split_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope.reserve_task("split").expect("split reservation");
    let split_task = slot.task_ref();
    let mut split = Box::pin(slot.define(TaskDef::new({
        let split_started = Arc::clone(&split_started);
        let split_cancelled = Arc::clone(&split_cancelled);
        move |context| {
            let split_started = Arc::clone(&split_started);
            let split_cancelled = Arc::clone(&split_cancelled);
            async move {
                split_started.store(true, Ordering::SeqCst);
                context.shutdown_token().cancelled().await;
                split_cancelled.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    })));
    assert!(poll_once(split.as_mut()).is_pending());
    drop(split);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            split_started.load(Ordering::SeqCst)
        })
        .await
    );
    assert_quiet(Duration::from_millis(20), || {
        split_cancelled.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(scope.remove_task(&split_task).await, RemoveOutcome::Removed);

    let split_after_started = Arc::new(AtomicBool::new(false));
    let split_after_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope
        .reserve_task("split-after-admission")
        .expect("split reservation");
    let split_after_task = slot.task_ref();
    let mut split_after = Box::pin(slot.define(TaskDef::new({
        let split_after_started = Arc::clone(&split_after_started);
        let split_after_cancelled = Arc::clone(&split_after_cancelled);
        move |context| {
            let split_after_started = Arc::clone(&split_after_started);
            let split_after_cancelled = Arc::clone(&split_after_cancelled);
            async move {
                split_after_started.store(true, Ordering::SeqCst);
                context.shutdown_token().cancelled().await;
                split_after_cancelled.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    })));
    assert!(poll_once(split_after.as_mut()).is_pending());
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            split_after_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(split_after);
    assert_quiet(Duration::from_millis(20), || {
        split_after_cancelled.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(
        scope.remove_task(&split_after_task).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn removing_a_member_releases_its_factory_before_scope_shutdown() {
    let started = Arc::new(AtomicBool::new(false));
    let factory_dropped = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let task = scope
        .add_task(
            "worker",
            TaskDef::new({
                let started = Arc::clone(&started);
                let probe = DropProbe(Arc::clone(&factory_dropped));
                move |context| {
                    let _ = &probe;
                    started.store(true, Ordering::SeqCst);
                    async move {
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }),
        )
        .await
        .expect("task admitted")
        .into_handles();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            started.load(Ordering::SeqCst)
        })
        .await
    );

    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    assert!(factory_dropped.load(Ordering::SeqCst));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn pre_spawn_shutdown_waits_for_teardown_to_exist() {
    let gate = ReleaseGate::default();
    let mut nested = Tree::new();
    nested
        .add_task(
            "worker",
            TaskDef::new({
                let gate = gate.clone();
                move |context| {
                    let gate = gate.clone();
                    async move {
                        context.shutdown_token().cancelled().await;
                        gate.wait().await;
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid task");
    let mut root = Tree::new();
    let slot = root.reserve_subtree::<Tree>("nested").expect("reservation");
    let scope = slot.scope_ref();
    let scope_for_wait = scope.clone();
    let waiter = tokio::spawn(async move {
        scope_for_wait
            .shutdown_and_wait(Duration::from_millis(10))
            .await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !waiter.is_finished(),
        "timeout must not arm before an incarnation starts"
    );
    let _nested_handle = slot.define_once(SubtreeOnceDef::new(nested));
    let system = root.spawn().expect("runtime is available");
    let timeout = waiter
        .await
        .expect("waiter joins")
        .expect_err("live teardown exceeds its bound");
    assert_eq!(timeout.stragglers.len(), 1);
    assert_eq!(timeout.stragglers[0].path[0].as_str(), "worker");
    gate.release();
    assert_eq!(scope.wait_stopped().await, StopReason::ShutdownRequested);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn pre_spawn_shutdown_resolves_if_the_tree_is_dropped_unspawned() {
    let mut root = Tree::new();
    let slot = root.reserve_subtree::<Tree>("nested").expect("reservation");
    let scope = slot.scope_ref();
    let wait_scope = scope.clone();
    let waiter =
        tokio::spawn(async move { wait_scope.shutdown_and_wait(Duration::from_millis(1)).await });
    drop(slot);
    drop(root);
    waiter
        .await
        .expect("waiter joins")
        .expect("already stopped");
    assert_eq!(scope.wait_stopped().await, StopReason::NeverStarted);
}

#[tokio::test]
async fn pre_spawn_dynamic_handles_reject_reservations_without_a_live_incarnation() {
    let mut root = Tree::new();
    let slot = root
        .reserve_subtree::<DynamicTree>("dynamic")
        .expect("reservation");
    let scope = slot.scope_ref();
    assert!(matches!(
        scope.reserve_task("too-early"),
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::NoLiveIncarnation
        ))
    ));
    drop(slot);
    drop(root);
    assert_eq!(scope.wait_stopped().await, StopReason::NeverStarted);
}

#[tokio::test]
async fn dropping_undefined_dynamic_slots_terminalizes_cells_and_frees_ids() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let task_slot = scope.reserve_task("worker").expect("task reservation");
    let abandoned_task = task_slot.task_ref();
    drop(task_slot);
    assert!(matches!(
        abandoned_task.wait().await.kind(),
        ExitKind::NeverStarted
    ));

    let subtree_slot = scope
        .reserve_subtree::<Tree>("nested")
        .expect("subtree reservation");
    let abandoned_scope = subtree_slot.scope_ref();
    drop(subtree_slot);
    assert_eq!(
        abandoned_scope.wait_stopped().await,
        StopReason::NeverStarted
    );

    let worker = scope
        .add_task("worker", waiting_task())
        .await
        .expect("task id was released")
        .into_handles();
    assert_eq!(scope.remove_task(&worker).await, RemoveOutcome::Removed);
    let nested = scope
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("subtree id was released")
        .into_handles();
    assert_eq!(scope.remove_scope(&nested).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn dynamic_scope_rejects_reservations_between_incarnations() {
    let factories = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(AtomicBool::new(false));
    let width = Duration::from_secs(10);
    let subtree = SubtreeDef::factory({
        let factories = Arc::clone(&factories);
        let first_started = Arc::clone(&first_started);
        move || {
            let generation = factories.fetch_add(1, Ordering::SeqCst) + 1;
            let mut tree = DynamicTree::new();
            if generation == 1 {
                tree.add_task(
                    "fails-before-ready",
                    TaskDef::new({
                        let first_started = Arc::clone(&first_started);
                        move |_| {
                            first_started.store(true, Ordering::SeqCst);
                            async { Err(ExitError::message("first incarnation fails startup")) }
                        }
                    })
                    .restart(never())
                    .readiness(Readiness::Manual)
                    .expect("manual readiness"),
                )
                .expect("valid task");
            } else {
                tree.add_task("worker", waiting_task()).expect("valid task");
            }
            tree
        }
    })
    .restart(RestartPolicy::new(
        RestartCondition::OnFailure,
        Backoff::fixed(width, shelterwood::Jitter::None).expect("non-zero backoff"),
    ));
    let mut root = Tree::new();
    let scope = root.add_subtree("dynamic", subtree).expect("valid subtree");
    let system = root.spawn().expect("runtime is available");

    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            first_started.load(Ordering::SeqCst)
        })
        .await
    );
    advance_time(Duration::from_secs(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    assert!(matches!(
        scope.reserve_task("too-early"),
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::NoLiveIncarnation
        ))
    ));

    advance_time(width).await;
    system
        .wait_started()
        .await
        .expect("replacement incarnation starts");
    assert_eq!(factories.load(Ordering::SeqCst), 2);
    let runtime_task = scope
        .add_task("runtime", waiting_task())
        .await
        .expect("the replacement incarnation admits")
        .into_handles();
    assert_eq!(
        scope.remove_task(&runtime_task).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn pending_restart_window_stop_targets_the_next_incarnation_once() {
    let factories = Arc::new(AtomicUsize::new(0));
    let starts = Arc::new(AtomicUsize::new(0));
    let width = Duration::from_secs(10);
    let subtree = SubtreeDef::factory({
        let factories = Arc::clone(&factories);
        let starts = Arc::clone(&starts);
        move || {
            let generation = factories.fetch_add(1, Ordering::SeqCst) + 1;
            let mut tree = Tree::new();
            if generation == 1 {
                let (_task, _completion) = tree
                    .add_task_once(
                        "worker",
                        TaskOnceDef::new({
                            let starts = Arc::clone(&starts);
                            move |_| async move {
                                starts.fetch_add(1, Ordering::SeqCst);
                                Ok::<_, ExitError>(())
                            }
                        }),
                    )
                    .expect("valid task");
            } else {
                tree.add_task(
                    "worker",
                    TaskDef::new({
                        let starts = Arc::clone(&starts);
                        move |context| {
                            let starts = Arc::clone(&starts);
                            async move {
                                starts.fetch_add(1, Ordering::SeqCst);
                                context.shutdown_token().cancelled().await;
                                Ok(())
                            }
                        }
                    }),
                )
                .expect("valid task");
            }
            tree
        }
    })
    .restart(RestartPolicy::new(
        RestartCondition::Always,
        Backoff::fixed(width, shelterwood::Jitter::None).expect("non-zero backoff"),
    ));
    let mut root = Tree::new();
    let nested = root.add_subtree("nested", subtree).expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("first incarnation starts");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) == 1
        })
        .await
    );
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let nested_wait = nested.clone();
    let stop =
        tokio::spawn(async move { nested_wait.shutdown_and_wait(Duration::from_secs(1)).await });
    tokio::task::yield_now().await;
    assert!(
        !stop.is_finished(),
        "restart-window request waits for the next incarnation"
    );
    advance_time(width).await;
    stop.await
        .expect("stop waiter joins")
        .expect("next incarnation cooperates");
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    advance_time(width / 2).await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "a consumed per-incarnation latch must not cause a stop/restart storm"
    );
    system.shutdown(Duration::ZERO).await.expect("root stops");
}

#[tokio::test]
async fn draining_scopes_reject_admission_and_treat_removal_as_absent() {
    let gate = ReleaseGate::default();
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut tree = DynamicTree::new();
    tree.add_task(
        "worker",
        TaskDef::new({
            let gate = gate.clone();
            let cancelled = Arc::clone(&cancelled);
            move |context| {
                let gate = gate.clone();
                let cancelled = Arc::clone(&cancelled);
                async move {
                    context.shutdown_token().cancelled().await;
                    cancelled.store(true, Ordering::SeqCst);
                    gate.wait().await;
                    Ok(())
                }
            }
        })
        .shutdown(Shutdown::Graceful {
            grace: Duration::from_secs(1),
        }),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(2)));
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    assert!(matches!(
        scope.add_task("late", waiting_task()).await,
        Err(ReserveError::NotAdmitting(NotAdmittingCause::Draining))
    ));
    assert_eq!(scope.remove("worker").await, RemoveOutcome::AlreadyAbsent);
    gate.release();
    shutdown
        .await
        .expect("shutdown joins")
        .expect("clean shutdown");
}

#[tokio::test]
async fn removal_of_a_polled_split_definition_keeps_the_scope_admitting() {
    let factory_dropped = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let slot = scope.reserve_task("worker").expect("reservation succeeds");
    let task = slot.task_ref();
    let mut admission = Box::pin(slot.define(TaskDef::new({
        let probe = DropProbe(Arc::clone(&factory_dropped));
        move |context| {
            let _ = &probe;
            async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })));
    // The first poll queues the admission with the driver; the removal then
    // lands before the driver dequeues it — §13.12's mandated race, which
    // must resolve `ReservationEnded` while the scope keeps admitting.
    assert!(poll_once(admission.as_mut()).is_pending());
    assert_eq!(scope.remove("worker").await, RemoveOutcome::Removed);
    assert!(matches!(
        admission.await,
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::ReservationEnded
        ))
    ));
    assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    assert!(factory_dropped.load(Ordering::SeqCst));

    let survivor = scope
        .add_task("worker", waiting_task())
        .await
        .expect("the scope keeps admitting and the id is free")
        .into_handles();
    assert_eq!(scope.remove_task(&survivor).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}
