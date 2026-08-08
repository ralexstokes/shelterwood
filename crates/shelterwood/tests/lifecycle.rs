use std::{
    future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, PanicOnDrop, ReleaseGate, assert_quiet, policy::never, poll_until,
};
use shelterwood::{
    Actor, ActorOnceDef, Context, DynamicTree, ExitError, ExitKind, ExitResult, Intensity,
    LifecycleEventKind, LifecycleItem, Readiness, Retention, ScopeState, Shutdown, StartupError,
    StartupFailureCause, StopReason, SubtreeOnceDef, TaskDef, TaskOnceDef, Tree,
};

#[tokio::test]
async fn non_owners_are_quiet_and_an_empty_root_needs_its_owner() {
    let empty = Tree::new().spawn().expect("runtime is available");
    empty.wait_started().await.expect("empty root starts");
    let empty_scope = empty.scope();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), empty_scope.wait_stopped())
            .await
            .is_err(),
        "a zero-child root must not finish naturally"
    );
    empty
        .shutdown(Duration::from_secs(1))
        .await
        .expect("empty root shuts down");

    let cancelled = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "worker",
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
        .expect("valid task");
    let spare = task.clone();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    drop(task);
    drop(spare);
    assert_quiet(Duration::from_millis(20), || {
        cancelled.load(Ordering::SeqCst)
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("owner shuts down");
}

#[tokio::test]
async fn fire_and_forget_owner_drop_still_tears_down() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "worker",
            TaskDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }),
        )
        .expect("valid task");
    drop(tree.spawn().expect("runtime is available"));
    let exit = tokio::time::timeout(Duration::from_secs(1), task.wait())
        .await
        .expect("owner drop completes teardown");
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert!(exit.cancelled());
}

#[tokio::test]
async fn framework_task_verdicts_remain_typed() {
    let mut timeout_tree = Tree::new();
    let timeout_task = timeout_tree
        .add_task(
            "not-ready",
            TaskDef::new(|_| future::pending())
                .restart(never())
                .shutdown(Shutdown::Abort)
                .readiness(Readiness::Manual)
                .expect("manual readiness")
                .readiness_deadline(
                    shelterwood::ReadinessDeadline::bounded(Duration::from_millis(10))
                        .expect("non-zero deadline"),
                ),
        )
        .expect("valid task");
    let timeout_system = timeout_tree.spawn().expect("runtime is available");
    assert!(timeout_system.wait_started().await.is_err());
    assert!(matches!(
        timeout_task.wait().await.kind(),
        ExitKind::ReadinessTimedOut { .. }
    ));
    timeout_system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed startup can be rolled back");

    let mut abort_tree = Tree::new();
    let abort_task = abort_tree
        .add_task(
            "stubborn",
            TaskDef::new(|_| future::pending()).shutdown(Shutdown::Graceful {
                grace: Duration::from_millis(5),
            }),
        )
        .expect("valid task");
    let abort_system = abort_tree.spawn().expect("runtime is available");
    abort_system.wait_started().await.expect("tree starts");
    abort_system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("child grace bounds shutdown");
    assert!(matches!(
        abort_task.wait().await.kind(),
        ExitKind::Aborted { after_grace: true }
    ));
}

#[tokio::test]
async fn task_destructor_panic_is_one_post_join_panic_exit() {
    let mut tree = Tree::new();
    let task = tree
        .add_task_once(
            "panic-on-drop",
            TaskOnceDef::new(|_| async move {
                let _value = PanicOnDrop::new("task destructor panic");
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task")
        .0;
    let system = tree.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_ok());
    let exit = task.wait().await;
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message: Some(message) } if message == "task destructor panic"
    ));
    assert_eq!(system.wait().await, StopReason::Finished);
}

struct CompletedThenDropPanic;

impl Drop for CompletedThenDropPanic {
    fn drop(&mut self) {
        panic!("actor destructor panic");
    }
}

impl Actor for CompletedThenDropPanic {
    type Msg = ();
    type Args = ();

    async fn init(_: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        context.stop();
        Ok(())
    }
}

#[tokio::test]
async fn actor_destructor_panic_supersedes_the_completed_run_outcome() {
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<CompletedThenDropPanic>::new(()).readiness(Readiness::Manual),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(())
        .await
        .expect("mailbox accepts before readiness");
    let error = system
        .wait_started()
        .await
        .expect_err("destructor panic prevents readiness");
    let StartupError::StartupFailed(failure) = error else {
        panic!("expected child startup failure");
    };
    let StartupFailureCause::Child { exit, .. } = failure.cause else {
        panic!("expected child failure");
    };
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message } if message.as_deref() == Some("actor destructor panic")
    ));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
}

#[tokio::test]
async fn replacement_starts_only_after_the_old_future_is_destroyed() {
    struct DropOrder(Arc<Mutex<Vec<&'static str>>>);
    impl Drop for DropOrder {
        fn drop(&mut self) {
            self.0.lock().expect("order mutex poisoned").push("drop-1");
        }
    }

    let starts = Arc::new(AtomicUsize::new(0));
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(10, Duration::from_secs(1)).expect("valid intensity"));
    tree.add_task(
        "worker",
        TaskDef::new({
            let starts = Arc::clone(&starts);
            let order = Arc::clone(&order);
            move |context| {
                let generation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                order
                    .lock()
                    .expect("order mutex poisoned")
                    .push(if generation == 1 {
                        "start-1"
                    } else {
                        "start-2"
                    });
                let order = Arc::clone(&order);
                async move {
                    if generation == 1 {
                        let _drop_order = DropOrder(order);
                        panic!("restart me");
                    }
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("initial incarnation starts");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) >= 2
        })
        .await
    );
    assert_eq!(
        *order.lock().expect("order mutex poisoned"),
        ["start-1", "drop-1", "start-2"]
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test]
async fn ordered_teardown_is_reverse_and_joins_before_advancing() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let gates = [
        ReleaseGate::default(),
        ReleaseGate::default(),
        ReleaseGate::default(),
    ];
    let mut tree = Tree::new();
    for (index, gate) in gates.iter().cloned().enumerate() {
        let order = Arc::clone(&order);
        tree.add_task(
            format!("child-{index}"),
            TaskDef::new(move |context| {
                let order = Arc::clone(&order);
                let gate = gate.clone();
                async move {
                    context.shutdown_token().cancelled().await;
                    order.lock().expect("order mutex poisoned").push(index);
                    gate.wait().await;
                    Ok(())
                }
            }),
        )
        .expect("valid task");
    }
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(2)));

    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            order.lock().expect("order mutex poisoned").as_slice() == [2]
        })
        .await
    );
    assert_quiet(Duration::from_millis(15), || {
        order.lock().expect("order mutex poisoned").len() > 1
    })
    .await;
    gates[2].release();
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            order.lock().expect("order mutex poisoned").as_slice() == [2, 1]
        })
        .await
    );
    gates[1].release();
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            order.lock().expect("order mutex poisoned").as_slice() == [2, 1, 0]
        })
        .await
    );
    gates[0].release();
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("clean shutdown");
}

#[tokio::test]
async fn dynamic_teardown_cancels_children_concurrently() {
    let cancelled = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
    let gate = ReleaseGate::default();
    let mut tree = DynamicTree::new();
    for index in 0..2 {
        tree.add_task(
            format!("child-{index}"),
            TaskDef::new({
                let cancelled = Arc::clone(&cancelled);
                let gate = gate.clone();
                move |context| {
                    let cancelled = Arc::clone(&cancelled);
                    let gate = gate.clone();
                    async move {
                        context.shutdown_token().cancelled().await;
                        cancelled[index].store(true, Ordering::SeqCst);
                        gate.wait().await;
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid task");
    }
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(2)));
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            cancelled.iter().all(|flag| flag.load(Ordering::SeqCst))
        })
        .await
    );
    gate.release();
    gate.release();
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("clean shutdown");
}

#[tokio::test]
async fn concurrent_initial_failures_publish_one_startup_failed_scope_edge() {
    let release = Arc::new(tokio::sync::Barrier::new(3));
    let mut tree = DynamicTree::new();
    for index in 0..2 {
        tree.add_task(
            format!("child-{index}"),
            TaskDef::new({
                let release = Arc::clone(&release);
                move |_| {
                    let release = Arc::clone(&release);
                    async move {
                        release.wait().await;
                        Err(ExitError::message(format!("failure-{index}")))
                    }
                }
            })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
        )
        .expect("valid task");
    }

    let system = tree.spawn().expect("runtime is available");
    let mut lifecycle = system.scope().subscribe_lifecycle();
    release.wait().await;

    let mut exits = 0;
    let mut startup_failed_edges = 0;
    while exits < 2 {
        let item = tokio::time::timeout(Duration::from_secs(2), lifecycle.recv())
            .await
            .expect("both concurrent exits are bounded")
            .expect("the root lifecycle remains open");
        let LifecycleItem::Event(event) = item else {
            panic!("the small fixture cannot lag");
        };
        match event.kind {
            LifecycleEventKind::Exited { .. } => exits += 1,
            LifecycleEventKind::ScopeState {
                state: ScopeState::StartupFailed,
            } => startup_failed_edges += 1,
            _ => {}
        }
    }
    while let Ok(item) = lifecycle.try_recv() {
        if matches!(
            item,
            LifecycleItem::Event(shelterwood::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::StartupFailed,
                },
                ..
            })
        ) {
            startup_failed_edges += 1;
        }
    }

    assert_eq!(
        startup_failed_edges, 1,
        "the first initial failure owns the root startup transition"
    );
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed dynamic root shuts down");
}

#[tokio::test]
async fn ordered_one_shot_subtrees_finish_naturally() {
    let mut leaf = Tree::new();
    let (_leaf_task, _leaf_completion) = leaf
        .add_task_once(
            "leaf",
            TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }),
        )
        .expect("valid leaf");

    let mut middle = Tree::new();
    let middle_scope = middle
        .add_subtree_once("leaf-scope", SubtreeOnceDef::new(leaf))
        .expect("valid subtree");

    let mut root = Tree::new();
    let root_scope = root
        .add_subtree_once("middle-scope", SubtreeOnceDef::new(middle))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    assert_eq!(middle_scope.wait_stopped().await, StopReason::Finished);
    assert_eq!(root_scope.wait_stopped().await, StopReason::Finished);
    assert_eq!(system.wait().await, StopReason::Finished);
}

#[tokio::test]
async fn retained_terminal_children_count_toward_ordered_completion() {
    let mut tree = Tree::new();
    let (_task, _completion) = tree
        .add_task_once(
            "retained",
            TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }).retention(Retention::Retain),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    assert_eq!(system.wait().await, StopReason::Finished);
}

#[tokio::test]
async fn abort_policy_task_exits_aborted_without_grace() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "stubborn",
            TaskDef::new(|_| future::pending()).shutdown(Shutdown::Abort),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("abort policy bounds teardown");
    let exit = task.wait().await;
    assert!(
        matches!(exit.kind(), ExitKind::Aborted { after_grace: false }),
        "no grace ever ran, so the abort is not after-grace: {exit:?}"
    );
    assert!(exit.cancelled());
}

#[tokio::test]
async fn completion_during_the_tidy_beat_is_not_reclassified_as_abort() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "prompt",
            TaskDef::new(|context| async move {
                context.abort_token().cancelled().await;
                Ok(())
            })
            .shutdown(Shutdown::Abort),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("completion within the tidy beat bounds teardown");
    let exit = task.wait().await;
    assert!(
        matches!(exit.kind(), ExitKind::Completed),
        "the policy does not pre-decide the classification: {exit:?}"
    );
    assert!(exit.cancelled());
}
