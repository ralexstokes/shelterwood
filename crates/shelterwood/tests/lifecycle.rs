mod common;

use std::{
    future::{self, Future},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context as FutureContext, Poll},
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, ReleaseGate, assert_eventually, assert_quiet, next_event,
    policy::never,
    poll_once,
    waiting::{cancellation_signalled_waiting_task, task as waiting_task},
};
use shelterwood::{
    Actor, ActorOnceDef, Cancellation, Context, DynamicTree, ExitError, ExitKind, ExitResult,
    GracePhase, Intensity, LifecycleEventKind, LifecycleItem, Readiness, Retention, ScopeState,
    Shutdown, StartupError, StartupFailureCause, StopReason, SubtreeOnceDef, TaskDef, TaskOnceDef,
    Tree,
};

#[tokio::test(start_paused = true)]
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
            cancellation_signalled_waiting_task(Arc::clone(&cancelled)),
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
    let task = tree.add_task("worker", waiting_task()).expect("valid task");
    drop(tree.spawn().expect("runtime is available"));
    let exit = tokio::time::timeout(POLL_TIMEOUT, task.wait())
        .await
        .expect("owner drop completes teardown");
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(exit.cancellation(), Cancellation::Observed);
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
            TaskDef::new(|_| future::pending())
                .shutdown(Shutdown::graceful(Duration::from_millis(5)).expect("grace is non-zero")),
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
        ExitKind::Aborted {
            phase: GracePhase::AfterGrace
        }
    ));
}

struct CompletedTaskFuture {
    returned: bool,
}

impl Future for CompletedTaskFuture {
    type Output = Result<(), ExitError>;

    fn poll(mut self: Pin<&mut Self>, _: &mut FutureContext<'_>) -> Poll<Self::Output> {
        assert!(
            !self.returned,
            "completed task future must not be re-polled"
        );
        self.returned = true;
        Poll::Ready(Ok(()))
    }
}

impl Drop for CompletedTaskFuture {
    fn drop(&mut self) {
        assert!(
            self.returned,
            "fixture must first produce a completed output"
        );
        panic!("task future destructor panic");
    }
}

#[tokio::test]
async fn task_future_destructor_panic_folds_after_its_completed_output() {
    let mut tree = Tree::new();
    let task = tree
        .add_task_once(
            "panic-on-drop",
            TaskOnceDef::new(|_| CompletedTaskFuture { returned: false }),
        )
        .expect("valid task")
        .0;
    let system = tree.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_ok());
    let exit = task.wait().await;
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message: Some(message) } if message == "task future destructor panic"
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
    assert_eventually!(|| starts.load(Ordering::SeqCst) >= 2).await;
    assert_eq!(
        *order.lock().expect("order mutex poisoned"),
        ["start-1", "drop-1", "start-2"]
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test(start_paused = true)]
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

    assert_eventually!(|| order.lock().expect("order mutex poisoned").as_slice() == [2]).await;
    assert_quiet(Duration::from_millis(15), || {
        order.lock().expect("order mutex poisoned").len() > 1
    })
    .await;
    gates[2].release();
    assert_eventually!(|| order.lock().expect("order mutex poisoned").as_slice() == [2, 1]).await;
    gates[1].release();
    assert_eventually!(|| { order.lock().expect("order mutex poisoned").as_slice() == [2, 1, 0] })
        .await;
    gates[0].release();
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("clean shutdown");
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn grace_expiry_mid_ladder_joins_the_abort_before_advancing() {
    let last_stopping = ReleaseGate::default();
    let first_stopping = ReleaseGate::default();
    let last_dropped = Arc::new(AtomicBool::new(false));
    let joined_before_advance = Arc::new(AtomicBool::new(false));
    let last_handle = Arc::new(Mutex::new(None::<shelterwood::TaskRef>));
    let grace = Duration::from_secs(10);
    let mut tree = Tree::new();
    tree.add_task(
        "first",
        TaskDef::new({
            let first_stopping = first_stopping.clone();
            let last_dropped = Arc::clone(&last_dropped);
            let joined_before_advance = Arc::clone(&joined_before_advance);
            let last_handle = Arc::clone(&last_handle);
            move |context| {
                let first_stopping = first_stopping.clone();
                let last_dropped = Arc::clone(&last_dropped);
                let joined_before_advance = Arc::clone(&joined_before_advance);
                let last_handle = Arc::clone(&last_handle);
                async move {
                    context.shutdown_token().cancelled().await;
                    let last = last_handle
                        .lock()
                        .expect("last handle mutex poisoned")
                        .clone()
                        .expect("last handle is installed before spawn");
                    let mut terminal = Box::pin(last.wait());
                    joined_before_advance.store(
                        poll_once(terminal.as_mut()).is_ready()
                            && last_dropped.load(Ordering::SeqCst),
                        Ordering::SeqCst,
                    );
                    first_stopping.release();
                    Ok(())
                }
            }
        }),
    )
    .expect("valid first task");
    let last = tree
        .add_task(
            "last",
            TaskDef::new({
                let last_stopping = last_stopping.clone();
                let last_dropped = Arc::clone(&last_dropped);
                move |context| {
                    let last_stopping = last_stopping.clone();
                    let last_dropped = Arc::clone(&last_dropped);
                    async move {
                        let _drop_signal = DropSignal(last_dropped);
                        context.shutdown_token().cancelled().await;
                        last_stopping.release();
                        future::pending::<ExitResult>().await
                    }
                }
            })
            .shutdown(Shutdown::graceful(grace).expect("grace is non-zero")),
        )
        .expect("valid last task");
    *last_handle.lock().expect("last handle mutex poisoned") = Some(last.clone());

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(30)));
    last_stopping.wait().await;
    assert!(!last_dropped.load(Ordering::SeqCst));

    tokio::time::advance(grace).await;
    tokio::time::timeout(POLL_TIMEOUT, first_stopping.wait())
        .await
        .expect("the teardown ladder advances after the abort joins");
    assert!(last_dropped.load(Ordering::SeqCst));
    assert!(joined_before_advance.load(Ordering::SeqCst));
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("shutdown completes within its owner budget");
    assert!(matches!(
        last.wait().await.kind(),
        ExitKind::Aborted {
            phase: GracePhase::AfterGrace
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn ordered_teardown_keeps_its_frontier_when_an_earlier_child_exits() {
    let first_stopping = Arc::new(AtomicBool::new(false));
    let middle_exit = ReleaseGate::default();
    let last_stopping = ReleaseGate::default();
    let last_exit = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "first",
        cancellation_signalled_waiting_task(Arc::clone(&first_stopping)),
    )
    .expect("valid first task");
    let middle = tree
        .add_task(
            "middle",
            TaskDef::new({
                let middle_exit = middle_exit.clone();
                move |_| {
                    let middle_exit = middle_exit.clone();
                    async move {
                        middle_exit.wait().await;
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid middle task");
    tree.add_task(
        "last",
        TaskDef::new({
            let last_stopping = last_stopping.clone();
            let last_exit = last_exit.clone();
            move |context| {
                let last_stopping = last_stopping.clone();
                let last_exit = last_exit.clone();
                async move {
                    context.shutdown_token().cancelled().await;
                    last_stopping.release();
                    last_exit.wait().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("valid last task");

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(2)));
    tokio::time::timeout(POLL_TIMEOUT, last_stopping.wait())
        .await
        .expect("the reverse-order frontier starts stopping");

    middle_exit.release();
    tokio::time::timeout(POLL_TIMEOUT, middle.wait())
        .await
        .expect("the earlier child exits independently");
    assert_quiet(Duration::from_millis(15), || {
        first_stopping.load(Ordering::SeqCst)
    })
    .await;

    last_exit.release();
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("clean shutdown");
    assert!(first_stopping.load(Ordering::SeqCst));
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
    assert_eventually!(|| cancelled.iter().all(|flag| flag.load(Ordering::SeqCst))).await;
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
    let mut lifecycle = system.scope().as_scope().subscribe_lifecycle();
    release.wait().await;

    let mut exits = 0;
    let mut startup_failed_edges = 0;
    while exits < 2 {
        let event = next_event(&mut lifecycle).await;
        match event.kind {
            LifecycleEventKind::Exited { .. } => exits += 1,
            LifecycleEventKind::ScopeState {
                state: ScopeState::StartupFailed,
            } => startup_failed_edges += 1,
            _ => {}
        }
    }
    while let Ok(item) = lifecycle.try_recv() {
        let LifecycleItem::Event(event) = item else {
            panic!("unexpected lifecycle lag while draining the startup trace")
        };
        if matches!(
            event,
            shelterwood::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::StartupFailed,
                },
                ..
            }
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
async fn plain_restart_publishes_exited_old_before_started_new() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new({
            let attempts = Arc::clone(&attempts);
            let fail_first = fail_first.clone();
            move |context| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let fail_first = fail_first.clone();
                async move {
                    if attempt == 0 {
                        fail_first.wait().await;
                        return Err(ExitError::message("restart me"));
                    }
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        }),
    )
    .expect("valid worker");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("first incarnation starts");
    let first = system
        .scope()
        .child("worker")
        .and_then(|child| child.incarnation)
        .expect("running child has an incarnation");
    let mut lifecycle = system.scope().subscribe_lifecycle();
    fail_first.release();

    loop {
        let event = next_event(&mut lifecycle).await;
        match event.kind {
            LifecycleEventKind::Exited { incarnation, .. } if incarnation == first => break,
            _ => {}
        }
    }
    let second = loop {
        let event = next_event(&mut lifecycle).await;
        if let LifecycleEventKind::Started { incarnation, .. } = event.kind
            && incarnation.supersedes(first)
        {
            break incarnation;
        }
    };
    assert!(second.supersedes(first));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("replacement stops");
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
        matches!(
            exit.kind(),
            ExitKind::Aborted {
                phase: GracePhase::WithinGrace
            }
        ),
        "no grace ever ran, so the abort is not after-grace: {exit:?}"
    );
    assert_eq!(exit.cancellation(), Cancellation::Observed);
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
    assert_eq!(exit.cancellation(), Cancellation::Observed);
}
