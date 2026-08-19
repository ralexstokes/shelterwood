mod common;

use std::{
    future,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, ReleaseGate, advance_time, assert_eventually, assert_quiet, next_event,
    poll_once, poll_until, startup_failed_child, waiting::task as waiting_task,
};
use shelterwood::{
    Actor, ActorOnceDef, Backoff, CallErrorKind, Cancellation, ChildState, Context,
    DefaultsInheritance, DynamicTree, ExitError, ExitKind, ExitResult, GracePhase, Intensity,
    LifecycleEventKind, LifecycleItem, LifecycleTryRecvError, Mailbox, MailboxShutdown, RawActor,
    RawContext, RawOnceDef, Readiness, ReadinessDeadline, Reply, RestartAttempt, RestartCondition,
    RestartPolicy, ScopeDefaults, ScopeRef, SendErrorKind, Shutdown, StartupError,
    StartupFailureCause, StopReason, SubtreeDef, SubtreeOnceDef, TaskDef, TaskOnceDef, TaskRef,
    Tree,
};

#[tokio::test]
async fn static_subtree_slot_preserves_its_handle_through_definition_and_spawn() {
    let mut nested = Tree::new();
    nested
        .add_task("worker", waiting_task())
        .expect("valid nested task");

    let mut root = Tree::new();
    let slot = root
        .reserve_subtree::<Tree>("nested")
        .expect("static subtree reservation succeeds");
    let reserved = slot.scope_ref();
    let defined = slot.define_once(SubtreeOnceDef::new(nested));
    assert_eq!(defined.membership(), reserved.membership());

    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("reserved subtree starts");
    let snapshot = reserved.snapshot();
    assert!(matches!(snapshot.state, shelterwood::ScopeState::Running));
    assert!(snapshot.child("worker").is_some());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("reserved subtree shuts down");
}

enum ParkedMailboxMessage {
    Value,
    Ask { _reply: Reply<()> },
}

struct NeverReceives;

impl RawActor for NeverReceives {
    type Msg = ParkedMailboxMessage;

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        future::pending().await
    }
}

#[tokio::test]
async fn owner_drop_and_nested_driver_abort_resolve_already_parked_mailbox_futures() {
    let mut nested = Tree::new();
    let actor = nested
        .add_raw_once(
            "mailbox",
            RawOnceDef::new(NeverReceives).mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid nested actor");
    let mut root = Tree::new();
    root.add_subtree_once(
        "nested",
        SubtreeOnceDef::new(nested).shutdown(Shutdown::Abort),
    )
    .expect("valid aborting subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("nested actor starts");

    actor
        .try_send(ParkedMailboxMessage::Value)
        .expect("the queue is filled");
    let mut send = Box::pin(actor.send(ParkedMailboxMessage::Value));
    let mut call = Box::pin(actor.call(
        |reply| ParkedMailboxMessage::Ask { _reply: reply },
        Duration::from_secs(30),
    ));
    assert!(poll_once(send.as_mut()).is_pending());
    assert!(poll_once(call.as_mut()).is_pending());

    drop(system);
    let send_error = tokio::time::timeout(POLL_TIMEOUT, send)
        .await
        .expect("owner drop cannot strand the parked send")
        .expect_err("the aborted nested mailbox terminalizes");
    assert_eq!(send_error.kind, SendErrorKind::Terminated);
    let call_error = tokio::time::timeout(POLL_TIMEOUT, call)
        .await
        .expect("nested-driver abort cannot strand the parked call")
        .expect_err("the aborted nested mailbox terminalizes");
    assert_eq!(call_error.kind, CallErrorKind::Terminated);
}

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
                tree.add_task("healthy", waiting_task())
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
    let (id, exit) = startup_failed_child(
        system
            .wait_started()
            .await
            .expect_err("nested lowering fails"),
    );
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
async fn subtree_intensity_trip_retains_structured_provenance() {
    // The nested scope's zero budget trips on the first failure, before its
    // manually gated task ever becomes ready, so the parent deterministically
    // observes the subtree child terminalize during startup.
    let mut nested = Tree::new();
    nested.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
    nested
        .add_task(
            "failing",
            TaskDef::new(|_| async { Err(ExitError::message("retry")) })
                .readiness(Readiness::Manual)
                .expect("manual readiness"),
        )
        .expect("valid task");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree edge");
    let system = root.spawn().expect("runtime is available");
    let (id, exit) = startup_failed_child(
        system
            .wait_started()
            .await
            .expect_err("nested budget trips during startup"),
    );
    assert_eq!(id.as_str(), "nested");
    let ExitKind::Failed(error) = exit.kind() else {
        panic!("a tripped subtree is a child failure");
    };
    let trip = error
        .intensity_trip()
        .expect("framework provenance is retained");
    assert_eq!(trip.max_restarts, 0);
    assert_eq!(trip.observed_restarts, 1);
    assert_eq!(trip.within, Duration::from_secs(10));
    assert!(
        error.startup_failure().is_none(),
        "an intensity trip is not a startup failure"
    );
    assert_eq!(
        error.as_error().to_string(),
        error.to_string(),
        "as_error exposes the same erased failure that Display renders"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
}

#[tokio::test]
async fn over_budget_restart_is_charged_but_never_spawned() {
    let starts = Arc::new(AtomicUsize::new(0));
    let releases = Arc::new([
        ReleaseGate::default(),
        ReleaseGate::default(),
        ReleaseGate::default(),
    ]);
    let mut root = Tree::new();
    root.intensity(Intensity::new(2, Duration::from_secs(10)).expect("valid intensity"));
    root.add_task(
        "failing",
        TaskDef::new({
            let starts = Arc::clone(&starts);
            let releases = Arc::clone(&releases);
            move |_| {
                let incarnation = starts.fetch_add(1, Ordering::SeqCst);
                let release = releases
                    .get(incarnation)
                    .cloned()
                    .expect("the over-budget restart is never spawned");
                async move {
                    release.wait().await;
                    Err(ExitError::message("retry"))
                }
            }
        }),
    )
    .expect("valid task");
    let system = root.spawn().expect("runtime is available");
    let scope = system.scope();
    let mut events = scope.subscribe_lifecycle();
    system
        .wait_started()
        .await
        .expect("immediate readiness starts root");
    releases[0].release();
    let mut trace = Vec::new();
    let mut scheduled = 0usize;
    loop {
        let event = next_event(&mut events).await;
        match event.kind {
            LifecycleEventKind::Exited { .. } => trace.push("exited"),
            LifecycleEventKind::RestartScheduled { attempt, .. } => {
                scheduled += 1;
                trace.push("scheduled");
                let expected_attempt =
                    (0..scheduled).fold(RestartAttempt::ZERO, |attempt, _| attempt.bump());
                let expected_total =
                    (0..scheduled).fold(shelterwood::TotalRestarts::ZERO, |total, _| total.bump());
                let expected_count =
                    (0..scheduled).fold(shelterwood::RestartCount::ZERO, |count, _| count.bump());
                assert_eq!(attempt, expected_attempt);
                let snapshot = scope.snapshot();
                assert_eq!(
                    snapshot.total_restarts, expected_total,
                    "the public scope counter advances with each scheduling event"
                );
                if scheduled < releases.len() {
                    // The next incarnation is still gated, so the member is
                    // guaranteed to remain resident while we inspect it.
                    assert_eq!(
                        snapshot
                            .child("failing")
                            .expect("the restartable child is retained")
                            .restart_count,
                        expected_count,
                        "the membership counter includes the scheduling charge"
                    );
                    releases[scheduled].release();
                } else if let Some(child) = snapshot.child("failing") {
                    // No gate holds the tripping charge: the driver drains and
                    // tears the scope down without waiting for this subscriber,
                    // so the member may already be pruned by the time event #3
                    // is processed. Only pin its counter while it is resident;
                    // `total_restarts` above stays authoritative either way.
                    assert_eq!(
                        child.restart_count, expected_count,
                        "the membership counter includes the tripping charge"
                    );
                }
            }
            LifecycleEventKind::ScopeState {
                state: shelterwood::ScopeState::Draining,
            } => {
                trace.push("draining");
                break;
            }
            _ => {}
        }
    }
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
    assert_eq!(scheduled, 3);
    assert_eq!(
        scope.snapshot().total_restarts,
        shelterwood::TotalRestarts::ZERO.bump().bump().bump(),
        "the tripping scheduling charge remains visible after failure"
    );
    assert_eq!(
        trace,
        [
            "exited",
            "scheduled",
            "exited",
            "scheduled",
            "exited",
            "scheduled",
            "draining"
        ],
        "the tripping charge is emitted before the scope failure"
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
    assert_eventually!(|| starts.load(Ordering::SeqCst) >= 2).await;
    advance_time(Duration::from_secs(11)).await;
    assert_eventually!(|| starts.load(Ordering::SeqCst) >= 3).await;
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
        TaskDef::new(|_| future::pending())
            .shutdown(Shutdown::graceful(Duration::from_secs(10)).expect("grace is non-zero"))
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
    assert!(ordered_elapsed < Duration::from_secs(25));

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

#[tokio::test(start_paused = true)]
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
    let dynamic_scope = dynamic.scope();
    let mut dynamic_stopped = Box::pin(dynamic_scope.as_scope().wait_stopped());
    assert_quiet(Duration::from_millis(20), || {
        poll_once(dynamic_stopped.as_mut()).is_ready()
    })
    .await;
    drop(dynamic_stopped);
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
    assert_eventually!(|| starts.load(Ordering::SeqCst) >= 2).await;
    let ordered_scope = ordered.scope();
    let mut ordered_stopped = Box::pin(ordered_scope.wait_stopped());
    assert_quiet(Duration::from_millis(20), || {
        poll_once(ordered_stopped.as_mut()).is_ready()
    })
    .await;
    drop(ordered_stopped);
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
            TaskDef::new(|_| future::pending())
                .shutdown(Shutdown::graceful(Duration::from_secs(60)).expect("grace is non-zero")),
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
    assert_eq!(exit.cancellation(), Cancellation::Observed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owning_shutdown_joins_recursively_aborted_scope_drivers() {
    let held = Arc::new(());
    let weak = Arc::downgrade(&held);
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let mut inner = Tree::new();
    // Keep the probe exclusively in the live future. Restartable factories
    // are retained definitions whose cleanup is intentionally detached after
    // hard escalation, so they cannot witness the recursive driver join.
    let (_leaf, _completion) = inner
        .add_task_once(
            "leaf",
            TaskOnceDef::new({
                let held = Arc::clone(&held);
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                move |_| async move {
                    let _held = held;
                    started.store(true, Ordering::Release);
                    let (released, wake) = &*release;
                    let mut released = released.lock().expect("release mutex poisoned");
                    while !*released {
                        released = wake.wait(released).expect("release mutex poisoned");
                    }
                    Ok(())
                }
            })
            .shutdown(Shutdown::graceful(Duration::from_secs(60)).expect("grace is non-zero")),
        )
        .expect("valid leaf");
    drop(held);

    let mut outer = Tree::new();
    outer
        .add_subtree_once("inner", SubtreeOnceDef::new(inner))
        .expect("valid inner subtree");
    let mut root = Tree::new();
    root.add_subtree_once(
        "outer",
        SubtreeOnceDef::new(outer).shutdown(Shutdown::Abort),
    )
    .expect("valid outer subtree");

    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    assert_eventually!(|| started.load(Ordering::Acquire), "leaf starts polling").await;
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(5)));
    let returned_before_leaf_released =
        poll_until(Duration::from_millis(50), Duration::from_millis(1), || {
            shutdown.is_finished()
        })
        .await;
    {
        let (released, wake) = &*release;
        *released.lock().expect("release mutex poisoned") = true;
        wake.notify_all();
    }
    let shutdown_result = shutdown.await.expect("shutdown task does not panic");

    assert!(
        !returned_before_leaf_released,
        "shutdown must not return while a recursively aborted child future is still polling"
    );
    shutdown_result.expect("root shuts down");

    assert!(
        weak.upgrade().is_none(),
        "shutdown returns only after every nested driver and active future drops"
    );
}

#[tokio::test]
async fn subtree_shutdown_and_wait_zero_escalates_and_joins_its_target_incarnation() {
    let mut nested = Tree::new();
    let leaf = nested
        .add_task(
            "leaf",
            TaskDef::new(|_| future::pending()).shutdown(Shutdown::Abort),
        )
        .expect("valid leaf");
    let mut root = Tree::new();
    let subtree = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("subtree starts");

    let timeout = subtree
        .shutdown_and_wait(Duration::ZERO)
        .await
        .expect_err("zero budget immediately escalates the live leaf");
    assert_eq!(timeout.stragglers.len(), 1);
    assert_eq!(
        timeout.stragglers[0]
            .path
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        ["leaf"],
        "scope-handle timeout paths are relative to the targeted subtree"
    );
    assert!(matches!(leaf.wait().await.kind(), ExitKind::Aborted { .. }));
    assert_eq!(system.wait().await, StopReason::Finished);
}

#[tokio::test]
async fn shutdown_and_wait_wakes_when_a_parent_drain_terminalizes_a_restarting_subtree() {
    let gate = ReleaseGate::default();
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
    system
        .scope()
        .wait_for_child(
            "nested",
            |child| matches!(child.state, ChildState::Restarting),
            Duration::from_secs(1),
        )
        .await
        .expect("the parent publishes the subtree restart window");
    let mut waiter = Box::pin(sub.shutdown_and_wait(Duration::from_secs(1)));
    assert!(
        poll_once(waiter.as_mut()).is_pending(),
        "the restart-window stop request is registered before parent teardown"
    );
    drop(system);
    tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("parent drain terminalizes the restarting subtree and wakes waiters")
        .expect("teardown completes in bound");
}

/// A subtree that shuts itself down records a cancelled exit: the stop
/// request was observed before the outcome, whether it came from an
/// ancestor's latch or the scope's own `request_scope_shutdown` (§7's
/// `cancelled` definition is observation, not provenance).
#[tokio::test]
async fn locally_requested_subtree_shutdown_reads_cancelled() {
    let started = Arc::new(AtomicBool::new(false));
    let mut nested = Tree::new();
    nested
        .add_task(
            "parked",
            TaskDef::new({
                let started = Arc::clone(&started);
                move |context| {
                    let started = Arc::clone(&started);
                    async move {
                        started.store(true, Ordering::SeqCst);
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            })
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
        )
        .expect("valid task");
    let mut root = Tree::new();
    let sub = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    assert_eventually!(|| started.load(Ordering::SeqCst)).await;
    // The stop request comes from the subtree's own handle, not an
    // ancestor's ladder.
    sub.request_shutdown();
    let startup = system
        .wait_started()
        .await
        .expect_err("the nested self-shutdown aborts parent startup pre-ready");
    let (id, exit) = startup_failed_child(startup);
    assert_eq!(id.as_str(), "nested");
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(
        exit.cancellation(),
        Cancellation::Observed,
        "a locally requested shutdown is still a stop request: {exit:?}"
    );
}

/// The pre-driver lowering-failure path resolves the same verdict as the loop
/// above, including the cancellation evidence: a stop request latched against
/// the first (still pending) incarnation outranks the lowering failure, so the
/// subtree exits `Completed`/`Observed` rather than `Failed`.
#[tokio::test]
async fn shutdown_latched_before_a_subtree_lowering_failure_reads_cancelled() {
    let mut nested = Tree::new();
    let _undefined = nested.reserve_task("missing").expect("reservation");
    let mut root = Tree::new();
    let sub = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree edge");
    // Declaration-time request: it latches against the incarnation the nested
    // driver has not minted yet, so lowering fails with the stop already held.
    sub.request_shutdown();
    let system = root.spawn().expect("runtime is available");
    let startup = system
        .wait_started()
        .await
        .expect_err("the nested stop aborts parent startup pre-ready");
    let (id, exit) = startup_failed_child(startup);
    assert_eq!(id.as_str(), "nested");
    assert!(
        matches!(exit.kind(), ExitKind::Completed),
        "the latched stop outranks the lowering failure: {exit:?}"
    );
    assert_eq!(
        exit.cancellation(),
        Cancellation::Observed,
        "a pre-loop stop request is still a stop request: {exit:?}"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("the stopped root rolls back");
}

#[tokio::test(start_paused = true)]
async fn restart_attempt_resets_after_a_ready_incarnation_settles() {
    let starts = Arc::new(AtomicUsize::new(0));
    let replacement_started = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(100, Duration::from_secs(10)).expect("valid intensity"));
    tree.add_task(
        "worker",
        TaskDef::new({
            let starts = Arc::clone(&starts);
            let replacement_started = replacement_started.clone();
            move |context| {
                let generation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                let replacement_started = replacement_started.clone();
                async move {
                    match generation {
                        1 => Err(ExitError::message("pre-ready failure")),
                        2 => {
                            context.mark_ready();
                            tokio::time::sleep(Duration::from_secs(11)).await;
                            Err(ExitError::message("post-ready failure"))
                        }
                        _ => {
                            replacement_started.release();
                            context.mark_ready();
                            context.shutdown_token().cancelled().await;
                            Ok(())
                        }
                    }
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
    let mut events = system.scope().subscribe_lifecycle();
    system
        .wait_started()
        .await
        .expect("the second incarnation settles aggregate readiness");

    advance_time(Duration::from_secs(11)).await;
    replacement_started.wait().await;

    let mut attempts = Vec::new();
    loop {
        match events.try_recv() {
            Ok(LifecycleItem::Event(event)) => {
                if let LifecycleEventKind::RestartScheduled { attempt, .. } = event.kind {
                    attempts.push(attempt);
                }
            }
            Ok(LifecycleItem::Lagged { dropped }) => {
                panic!("short restart trace unexpectedly lagged by {dropped}")
            }
            Err(LifecycleTryRecvError::Empty) => break,
            Err(LifecycleTryRecvError::Closed) => panic!("live root stream closed"),
        }
    }
    assert_eq!(
        attempts,
        [RestartAttempt::ZERO.bump(), RestartAttempt::ZERO.bump()]
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[derive(Clone, Copy)]
enum CapacityMessage {
    Hold,
    Queued,
}

struct CapacityActor {
    entered: ReleaseGate,
    release: ReleaseGate,
}

impl Actor for CapacityActor {
    type Msg = CapacityMessage;
    type Args = (ReleaseGate, ReleaseGate);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            entered: args.0,
            release: args.1,
        })
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        if matches!(message, CapacityMessage::Hold) {
            self.entered.release();
            self.release.wait().await;
        }
        Ok(())
    }
}

fn add_inherit_reset_subtrees(
    root: &mut Tree,
    inherited: Tree,
    reset: Tree,
) -> (ScopeRef, ScopeRef) {
    let inherited = root
        .add_subtree_once(
            "inherited",
            SubtreeOnceDef::new(inherited).defaults(DefaultsInheritance::Inherit),
        )
        .expect("valid inherited subtree");
    let reset = root
        .add_subtree_once(
            "reset",
            SubtreeOnceDef::new(reset).defaults(DefaultsInheritance::Reset),
        )
        .expect("valid reset subtree");
    (inherited, reset)
}

#[tokio::test]
async fn subtree_defaults_inherit_or_reset_end_to_end() {
    let inherited_entered = ReleaseGate::default();
    let inherited_release = ReleaseGate::default();
    let reset_entered = ReleaseGate::default();
    let reset_release = ReleaseGate::default();

    let mut inherited_tree = Tree::new();
    let inherited_actor = inherited_tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<CapacityActor>::new((
                inherited_entered.clone(),
                inherited_release.clone(),
            )),
        )
        .expect("valid inherited actor");
    let mut reset_tree = Tree::new();
    let reset_actor = reset_tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<CapacityActor>::new((reset_entered.clone(), reset_release.clone())),
        )
        .expect("valid reset actor");

    let mut root = Tree::new();
    root.defaults(ScopeDefaults {
        mailbox: Some(Mailbox::queue(1).expect("valid inherited capacity")),
        ..ScopeDefaults::default()
    });
    add_inherit_reset_subtrees(&mut root, inherited_tree, reset_tree);
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("both subtrees start");

    inherited_actor
        .send(CapacityMessage::Hold)
        .await
        .expect("inherited actor accepts hold");
    reset_actor
        .send(CapacityMessage::Hold)
        .await
        .expect("reset actor accepts hold");
    inherited_entered.wait().await;
    reset_entered.wait().await;

    inherited_actor
        .try_send(CapacityMessage::Queued)
        .expect("the inherited one-slot queue accepts one pending message");
    let inherited_full = inherited_actor
        .try_send(CapacityMessage::Queued)
        .expect_err("the second pending message observes the inherited capacity");
    assert_eq!(inherited_full.kind, SendErrorKind::Full);

    reset_actor
        .try_send(CapacityMessage::Queued)
        .expect("reset uses the library queue capacity");
    reset_actor
        .try_send(CapacityMessage::Queued)
        .expect("reset does not inherit the parent's one-slot capacity");

    inherited_release.release();
    reset_release.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn three_level_mailbox_capacity_walk_honors_inherit_and_reset() {
    let inherited_entered = ReleaseGate::default();
    let inherited_release = ReleaseGate::default();
    let reset_entered = ReleaseGate::default();
    let reset_release = ReleaseGate::default();

    let mut inherited_leaf = Tree::new();
    let inherited_actor = inherited_leaf
        .add_actor_once(
            "actor",
            ActorOnceDef::<CapacityActor>::new((
                inherited_entered.clone(),
                inherited_release.clone(),
            ))
            .mailbox(Mailbox::queue_inherit()),
        )
        .expect("valid inherited actor");
    let mut reset_leaf = Tree::new();
    let reset_actor = reset_leaf
        .add_actor_once(
            "actor",
            ActorOnceDef::<CapacityActor>::new((reset_entered.clone(), reset_release.clone()))
                .mailbox(Mailbox::queue_inherit()),
        )
        .expect("valid reset actor");

    let mut middle = Tree::new();
    middle.defaults(ScopeDefaults {
        mailbox: Some(Mailbox::latest()),
        ..ScopeDefaults::default()
    });
    add_inherit_reset_subtrees(&mut middle, inherited_leaf, reset_leaf);

    let mut root = Tree::new();
    root.defaults(ScopeDefaults {
        mailbox: Some(Mailbox::queue(1).expect("valid outer capacity")),
        ..ScopeDefaults::default()
    });
    root.add_subtree_once(
        "middle",
        SubtreeOnceDef::new(middle).defaults(DefaultsInheritance::Inherit),
    )
    .expect("valid middle subtree");

    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("all scopes start");

    inherited_actor
        .send(CapacityMessage::Hold)
        .await
        .expect("inherited actor accepts hold");
    reset_actor
        .send(CapacityMessage::Hold)
        .await
        .expect("reset actor accepts hold");
    inherited_entered.wait().await;
    reset_entered.wait().await;

    inherited_actor
        .try_send(CapacityMessage::Queued)
        .expect("the inherited outer capacity accepts one pending message");
    let inherited_full = inherited_actor
        .try_send(CapacityMessage::Queued)
        .expect_err("latest is passed over and the outer one-slot queue is full");
    assert_eq!(inherited_full.kind, SendErrorKind::Full);

    reset_actor
        .try_send(CapacityMessage::Queued)
        .expect("reset uses the library queue capacity");
    reset_actor
        .try_send(CapacityMessage::Queued)
        .expect("reset severs the outer one-slot queue capacity");

    inherited_release.release();
    reset_release.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn subtree_restart_defaults_inherit_or_reset_end_to_end() {
    let inherited_starts = Arc::new(AtomicUsize::new(0));
    let inherited_fail = ReleaseGate::default();
    let reset_starts = Arc::new(AtomicUsize::new(0));
    let reset_fail = ReleaseGate::default();
    let reset_restarted = ReleaseGate::default();

    let mut inherited_tree = Tree::new();
    let inherited_task = inherited_tree
        .add_task(
            "worker",
            TaskDef::new({
                let starts = Arc::clone(&inherited_starts);
                let fail = inherited_fail.clone();
                move |context| {
                    let starts = Arc::clone(&starts);
                    let fail = fail.clone();
                    async move {
                        starts.fetch_add(1, Ordering::SeqCst);
                        context.mark_ready();
                        fail.wait().await;
                        Err(ExitError::message("inherited restart default"))
                    }
                }
            })
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
        )
        .expect("valid inherited worker");

    let mut reset_tree = Tree::new();
    let reset_task = reset_tree
        .add_task(
            "worker",
            TaskDef::new({
                let starts = Arc::clone(&reset_starts);
                let fail = reset_fail.clone();
                let restarted = reset_restarted.clone();
                move |context| {
                    let starts = Arc::clone(&starts);
                    let fail = fail.clone();
                    let restarted = restarted.clone();
                    async move {
                        let attempt = starts.fetch_add(1, Ordering::SeqCst);
                        context.mark_ready();
                        if attempt == 0 {
                            fail.wait().await;
                            Err(ExitError::message("reset restart default"))
                        } else {
                            restarted.release();
                            context.shutdown_token().cancelled().await;
                            Ok(())
                        }
                    }
                }
            })
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
        )
        .expect("valid reset worker");

    let mut root = Tree::new();
    root.defaults(ScopeDefaults {
        child_restart: Some(RestartPolicy::new(
            RestartCondition::Never,
            Backoff::Immediate,
        )),
        ..ScopeDefaults::default()
    });
    add_inherit_reset_subtrees(&mut root, inherited_tree, reset_tree);
    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("both workers become ready");

    inherited_fail.release();
    reset_fail.release();
    assert!(matches!(
        inherited_task.wait().await.kind(),
        ExitKind::Failed(cause) if cause.to_string() == "inherited restart default"
    ));
    reset_restarted.wait().await;
    assert_eq!(inherited_starts.load(Ordering::SeqCst), 1);
    assert_eq!(reset_starts.load(Ordering::SeqCst), 2);
    let mut reset_wait = Box::pin(reset_task.wait());
    assert!(poll_once(reset_wait.as_mut()).is_pending());
    drop(reset_wait);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn subtree_shutdown_defaults_inherit_or_reset_end_to_end() {
    fn stubborn_tree(started: ReleaseGate) -> (Tree, TaskRef) {
        let mut tree = Tree::new();
        let task = tree
            .add_task(
                "worker",
                TaskDef::new(move |context| {
                    let started = started.clone();
                    async move {
                        started.release();
                        context.shutdown_token().cancelled().await;
                        std::future::pending::<ExitResult>().await
                    }
                }),
            )
            .expect("valid stubborn worker");
        (tree, task)
    }

    let inherited_started = ReleaseGate::default();
    let reset_started = ReleaseGate::default();
    let (inherited_tree, inherited_task) = stubborn_tree(inherited_started.clone());
    let (reset_tree, reset_task) = stubborn_tree(reset_started.clone());
    let mut root = Tree::new();
    root.defaults(ScopeDefaults {
        child_shutdown: Some(Shutdown::Abort),
        ..ScopeDefaults::default()
    });
    let (inherited_scope, reset_scope) =
        add_inherit_reset_subtrees(&mut root, inherited_tree, reset_tree);
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("both workers start");
    inherited_started.wait().await;
    reset_started.wait().await;

    inherited_scope
        .shutdown_and_wait(Duration::from_secs(30))
        .await
        .expect("inherited abort policy stops immediately");
    reset_scope
        .shutdown_and_wait(Duration::from_secs(30))
        .await
        .expect("reset library grace eventually escalates");
    assert!(matches!(
        inherited_task.wait().await.kind(),
        ExitKind::Aborted {
            phase: GracePhase::WithinGrace
        }
    ));
    assert!(matches!(
        reset_task.wait().await.kind(),
        ExitKind::Aborted {
            phase: GracePhase::AfterGrace
        }
    ));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

struct DefaultMailboxActor {
    entered: ReleaseGate,
    release: ReleaseGate,
    handled: Arc<AtomicUsize>,
}

impl Actor for DefaultMailboxActor {
    type Msg = CapacityMessage;
    type Args = (ReleaseGate, ReleaseGate, ReleaseGate, Arc<AtomicUsize>);

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        let shutdown = context.shutdown_token();
        let shutdown_seen = args.2.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            shutdown_seen.release();
        });
        Ok(Self {
            entered: args.0,
            release: args.1,
            handled: args.3,
        })
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        match message {
            CapacityMessage::Hold => {
                self.entered.release();
                self.release.wait().await;
            }
            CapacityMessage::Queued => {
                self.handled.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn subtree_mailbox_shutdown_defaults_inherit_or_reset_end_to_end() {
    let inherited_entered = ReleaseGate::default();
    let inherited_release = ReleaseGate::default();
    let inherited_shutdown = ReleaseGate::default();
    let inherited_handled = Arc::new(AtomicUsize::new(0));
    let reset_entered = ReleaseGate::default();
    let reset_release = ReleaseGate::default();
    let reset_shutdown = ReleaseGate::default();
    let reset_handled = Arc::new(AtomicUsize::new(0));

    let mut inherited_tree = Tree::new();
    let inherited_actor = inherited_tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<DefaultMailboxActor>::new((
                inherited_entered.clone(),
                inherited_release.clone(),
                inherited_shutdown.clone(),
                Arc::clone(&inherited_handled),
            )),
        )
        .expect("valid inherited actor");
    let mut reset_tree = Tree::new();
    let reset_actor = reset_tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<DefaultMailboxActor>::new((
                reset_entered.clone(),
                reset_release.clone(),
                reset_shutdown.clone(),
                Arc::clone(&reset_handled),
            )),
        )
        .expect("valid reset actor");

    let mut root = Tree::new();
    root.defaults(ScopeDefaults {
        mailbox_shutdown: Some(MailboxShutdown::Discard),
        ..ScopeDefaults::default()
    });
    let (inherited_scope, reset_scope) =
        add_inherit_reset_subtrees(&mut root, inherited_tree, reset_tree);
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("both actors start");
    inherited_actor
        .send(CapacityMessage::Hold)
        .await
        .expect("inherited hold is accepted");
    reset_actor
        .send(CapacityMessage::Hold)
        .await
        .expect("reset hold is accepted");
    inherited_entered.wait().await;
    reset_entered.wait().await;
    inherited_actor
        .send(CapacityMessage::Queued)
        .await
        .expect("inherited prefix is accepted");
    reset_actor
        .send(CapacityMessage::Queued)
        .await
        .expect("reset prefix is accepted");

    inherited_scope.request_shutdown();
    reset_scope.request_shutdown();
    inherited_shutdown.wait().await;
    reset_shutdown.wait().await;
    inherited_release.release();
    reset_release.release();
    inherited_scope.wait_stopped().await;
    reset_scope.wait_stopped().await;
    assert_eq!(inherited_handled.load(Ordering::SeqCst), 0);
    assert_eq!(reset_handled.load(Ordering::SeqCst), 1);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn subtree_readiness_deadline_defaults_inherit_or_reset_end_to_end() {
    fn manual_tree(started: ReleaseGate) -> (Tree, TaskRef) {
        let mut tree = Tree::new();
        let task = tree
            .add_task(
                "manual",
                TaskDef::new(move |context| {
                    let started = started.clone();
                    async move {
                        started.release();
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                })
                .restart(RestartPolicy::new(
                    RestartCondition::Never,
                    Backoff::Immediate,
                ))
                .readiness(Readiness::Manual)
                .expect("manual readiness"),
            )
            .expect("valid manual worker");
        (tree, task)
    }

    let inherited_started = ReleaseGate::default();
    let reset_started = ReleaseGate::default();
    let (inherited_tree, inherited_task) = manual_tree(inherited_started.clone());
    let (reset_tree, reset_task) = manual_tree(reset_started.clone());
    let mut root = DynamicTree::new();
    root.defaults(ScopeDefaults {
        readiness_deadline: ReadinessDeadline::Unbounded,
        ..ScopeDefaults::default()
    });
    let inherited_scope = root
        .add_subtree_once(
            "inherited",
            SubtreeOnceDef::new(inherited_tree).defaults(DefaultsInheritance::Inherit),
        )
        .expect("valid inherited subtree");
    root.add_subtree_once(
        "reset",
        SubtreeOnceDef::new(reset_tree).defaults(DefaultsInheritance::Reset),
    )
    .expect("valid reset subtree");
    let system = root.spawn().expect("runtime is available");
    inherited_started.wait().await;
    reset_started.wait().await;

    assert!(matches!(
        reset_task.wait().await.kind(),
        ExitKind::ReadinessTimedOut { .. }
    ));
    assert!(matches!(
        inherited_scope
            .child("manual")
            .expect("inherited manual worker remains resident")
            .state,
        ChildState::Starting
    ));
    let mut inherited_wait = Box::pin(inherited_task.wait());
    assert!(poll_once(inherited_wait.as_mut()).is_pending());
    drop(inherited_wait);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}
