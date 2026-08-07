use std::{fmt::Debug, time::Duration};

use shelterwood::{
    ActorRef, ChildSnapshot, ChildState, DynamicTree, ExitError, ExitKind, ExitResult, Guard,
    Mailbox, RawActor, RawContext, RawOnceDef, Readiness, Rejected, RejectedKind, Retention,
    RunUntilAllResult, SiblingReadyError, TaskCompletion, TaskOnceDef, Tree,
};
use shelterwood_test_support::ReleaseGate;
use tokio::sync::oneshot;

fn assert_clone_eq_debug_send_sync<T: Clone + Eq + Debug + Send + Sync>() {}

#[test]
fn milestone_eleven_convenience_data_keeps_its_trait_contracts() {
    assert_clone_eq_debug_send_sync::<TaskCompletion>();
    assert_clone_eq_debug_send_sync::<RunUntilAllResult>();
    assert_clone_eq_debug_send_sync::<SiblingReadyError>();
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Tick(u64);

struct ParkedTarget(ReleaseGate);

impl RawActor for ParkedTarget {
    type Msg = Tick;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.0.wait().await;
        while context.recv().await.is_some() {}
        Ok(())
    }
}

enum TimerCommand {
    After {
        target: ActorRef<Tick>,
        message: Tick,
        after: Duration,
        reply: oneshot::Sender<Result<Guard, Rejected<Tick>>>,
    },
    Interval {
        target: ActorRef<Tick>,
        message: Tick,
        period: Duration,
        reply: oneshot::Sender<Result<Guard, Rejected<Tick>>>,
    },
    Stop,
}

struct TimerSource;

impl RawActor for TimerSource {
    type Msg = TimerCommand;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        while let Some(command) = context.recv().await {
            match command {
                TimerCommand::After {
                    target,
                    message,
                    after,
                    reply,
                } => {
                    let _ = reply.send(context.send_after_to(&target, message, after));
                }
                TimerCommand::Interval {
                    target,
                    message,
                    period,
                    reply,
                } => {
                    let _ = reply.send(context.interval_to(&target, message, period));
                }
                TimerCommand::Stop => break,
            }
        }
        Ok(())
    }
}

async fn arm_after(
    source: &ActorRef<TimerCommand>,
    target: &ActorRef<Tick>,
    message: Tick,
    after: Duration,
) -> Result<Guard, Rejected<Tick>> {
    let (reply, response) = oneshot::channel();
    source
        .send(TimerCommand::After {
            target: target.clone(),
            message,
            after,
            reply,
        })
        .await
        .expect("timer source accepts command");
    response.await.expect("timer source replies")
}

async fn arm_interval(
    source: &ActorRef<TimerCommand>,
    target: &ActorRef<Tick>,
    message: Tick,
    period: Duration,
) -> Result<Guard, Rejected<Tick>> {
    let (reply, response) = oneshot::channel();
    source
        .send(TimerCommand::Interval {
            target: target.clone(),
            message,
            period,
            reply,
        })
        .await
        .expect("timer source accepts command");
    response.await.expect("timer source replies")
}

#[tokio::test(start_paused = true)]
async fn cross_actor_timers_use_mailbox_semantics_and_incarnation_ownership() {
    let target_gate = ReleaseGate::default();
    let mut tree = Tree::new();
    let target = tree
        .add_raw_once(
            "target",
            RawOnceDef::new(ParkedTarget(target_gate.clone()))
                .mailbox(Mailbox::latest())
                .retention(Retention::Retain),
        )
        .expect("target id is valid");
    let source = tree
        .add_raw_once(
            "source",
            RawOnceDef::new(TimerSource).retention(Retention::Retain),
        )
        .expect("source id is valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actors start");

    let after = arm_after(&source, &target, Tick(1), Duration::from_secs(2))
        .await
        .expect("one-shot timer arms");
    assert_eq!(target.stats().stats.messages_accepted, 0);
    tokio::time::advance(Duration::from_secs(2)).await;
    after.finished().await;
    assert_eq!(target.stats().stats.messages_accepted, 1);
    assert_eq!(target.stats().stats.mailbox_depth, 1);

    let cancelled = arm_after(&source, &target, Tick(2), Duration::from_secs(20))
        .await
        .expect("cancelled timer arms");
    drop(cancelled);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(20)).await;
    tokio::task::yield_now().await;
    assert_eq!(target.stats().stats.messages_accepted, 1);

    let zero = arm_interval(&source, &target, Tick(3), Duration::ZERO)
        .await
        .expect_err("zero interval is rejected eagerly");
    assert_eq!(zero.kind, RejectedKind::ZeroPeriod);
    assert_eq!(zero.into_payload(), Some(Tick(3)));

    let interval = arm_interval(&source, &target, Tick(4), Duration::from_secs(1))
        .await
        .expect("interval arms");
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(target.stats().stats.messages_accepted, 4);
    assert_eq!(target.stats().stats.messages_conflated, 3);
    interval.cancel();

    let detached = arm_after(&source, &target, Tick(5), Duration::from_secs(10))
        .await
        .expect("detached timer arms");
    detached.detach();
    source
        .send(TimerCommand::Stop)
        .await
        .expect("source accepts stop");
    system
        .scope()
        .wait_for_child(
            "source",
            |child| child.state.is_terminal(),
            Duration::from_secs(1),
        )
        .await
        .expect("source membership terminalizes");
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        target.stats().stats.messages_accepted,
        4,
        "sender incarnation owns even a detached timer"
    );

    target_gate.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("timer tree stops");
}

struct ImmediateTarget;

impl RawActor for ImmediateTarget {
    type Msg = Tick;

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_target_finishes_a_delayed_guard_without_waiting_for_deadline() {
    let mut tree = Tree::new();
    let target = tree
        .add_raw_once(
            "target",
            RawOnceDef::new(ImmediateTarget).retention(Retention::Retain),
        )
        .expect("target id is valid");
    let source = tree
        .add_raw_once(
            "source",
            RawOnceDef::new(TimerSource).retention(Retention::Retain),
        )
        .expect("source id is valid");
    let system = tree.spawn().expect("runtime is available");
    system
        .scope()
        .wait_for_child(
            "target",
            |child| child.state.is_terminal(),
            Duration::from_secs(1),
        )
        .await
        .expect("target terminalizes");

    let guard = arm_after(&source, &target, Tick(9), Duration::from_secs(100))
        .await
        .expect("timer registration itself is valid");
    guard.finished().await;
    assert_eq!(target.stats().stats.messages_accepted, 0);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree stops");
}

struct EarlierSibling {
    release: ReleaseGate,
}

impl RawActor for EarlierSibling {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.release.wait().await;
        context.mark_ready();
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

struct SiblingWaiter {
    id: &'static str,
    result: Option<oneshot::Sender<Result<ChildSnapshot, SiblingReadyError>>>,
}

impl RawActor for SiblingWaiter {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        let result = context
            .await_sibling_ready(self.id, Duration::from_secs(5))
            .await;
        let _ = self.result.take().expect("waiter runs once").send(result);
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn sibling_barrier_observes_earlier_readiness_and_rejects_later_waits() {
    let release = ReleaseGate::default();
    let (result, mut readiness) = oneshot::channel();
    let mut tree = Tree::new();
    let earlier = tree
        .add_raw_once(
            "earlier",
            RawOnceDef::new(EarlierSibling {
                release: release.clone(),
            })
            .readiness(Readiness::Manual)
            .expect("manual raw readiness is valid"),
        )
        .expect("earlier id is valid");
    tree.add_raw_once(
        "waiter",
        RawOnceDef::new(SiblingWaiter {
            id: "earlier",
            result: Some(result),
        }),
    )
    .expect("waiter id is valid");
    let system = tree.spawn().expect("runtime is available");
    assert!(
        readiness.try_recv().is_err(),
        "later child is not spawned before earlier readiness"
    );
    release.release();
    system
        .wait_started()
        .await
        .expect("ordered startup completes");
    let snapshot = readiness
        .await
        .expect("waiter reports")
        .expect("earlier sibling is ready");
    assert_eq!(snapshot.membership, earlier.membership());
    assert_eq!(snapshot.state, ChildState::Running);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("sibling tree stops");

    let (result, deadlock) = oneshot::channel();
    let later_release = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "first",
        RawOnceDef::new(SiblingWaiter {
            id: "later",
            result: Some(result),
        }),
    )
    .expect("first id is valid");
    tree.add_raw_once(
        "later",
        RawOnceDef::new(EarlierSibling {
            release: later_release.clone(),
        }),
    )
    .expect("later id is valid");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(
        deadlock.await.expect("first child reports"),
        Err(SiblingReadyError::WouldDeadlock)
    );
    later_release.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("deadlock fixture stops");
}

#[tokio::test(start_paused = true)]
async fn dynamic_sibling_barrier_waits_for_later_admission() {
    let (result, readiness) = oneshot::channel();
    let mut tree = DynamicTree::new();
    tree.add_raw_once(
        "waiter",
        RawOnceDef::new(SiblingWaiter {
            id: "late",
            result: Some(result),
        }),
    )
    .expect("waiter id is valid");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("initial dynamic tree starts");

    let release = ReleaseGate::default();
    let target = system
        .scope()
        .add_raw_once(
            "late",
            RawOnceDef::new(EarlierSibling {
                release: release.clone(),
            })
            .readiness(Readiness::Manual)
            .expect("manual raw readiness is valid"),
        )
        .await
        .expect("late sibling is admitted");
    release.release();
    let snapshot = readiness
        .await
        .expect("waiter reports")
        .expect("dynamic wait survives absence until admission");
    assert_eq!(snapshot.membership, target.membership());
    assert_eq!(snapshot.state, ChildState::Running);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic sibling tree stops");
}

struct DrainWaiter(Option<oneshot::Sender<Result<ChildSnapshot, SiblingReadyError>>>);

impl RawActor for DrainWaiter {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        while context.recv().await.is_some() {}
        let result = context
            .await_sibling_ready("missing", Duration::from_secs(1))
            .await;
        let _ = self.0.take().expect("waiter runs once").send(result);
        Ok(())
    }
}

#[tokio::test]
async fn sibling_barrier_rejects_the_raw_drain_stage() {
    let (result, drained) = oneshot::channel();
    let mut tree = Tree::new();
    tree.add_raw_once("draining", RawOnceDef::new(DrainWaiter(Some(result))))
        .expect("draining id is valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("drain waiter starts");
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(1)));
    assert_eq!(
        drained.await.expect("drain waiter reports"),
        Err(SiblingReadyError::Draining)
    );
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("drain waiter stops");
}

#[tokio::test]
async fn in_flight_dynamic_sibling_wait_is_cancelled_by_shutdown() {
    let (result, waiting) = oneshot::channel();
    let mut tree = DynamicTree::new();
    tree.add_raw_once(
        "waiting",
        RawOnceDef::new(SiblingWaiter {
            id: "never-admitted",
            result: Some(result),
        }),
    )
    .expect("waiter id is valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic waiter starts");
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(1)));
    assert_eq!(
        waiting.await.expect("waiter reports cancellation"),
        Err(SiblingReadyError::Draining)
    );
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("in-flight wait stops promptly");
}

#[tokio::test]
async fn run_until_all_retains_input_order_and_every_task_exit() {
    let first_release = ReleaseGate::default();
    let second_release = ReleaseGate::default();
    let mut tree = Tree::new();
    let (first, _) = tree
        .add_task_once(
            "first",
            TaskOnceDef::<()>::new({
                let release = first_release.clone();
                move |_| async move {
                    release.wait().await;
                    Ok(())
                }
            }),
        )
        .expect("first id is valid");
    let (second, _) = tree
        .add_task_once(
            "second",
            TaskOnceDef::<()>::new({
                let release = second_release.clone();
                move |_| async move {
                    release.wait().await;
                    Err(ExitError::message("selected failure"))
                }
            }),
        )
        .expect("second id is valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tasks start");

    let runner =
        tokio::spawn(system.run_until_all([second.clone(), first.clone()], Duration::from_secs(1)));
    first_release.release();
    tokio::task::yield_now().await;
    assert!(
        !runner.is_finished(),
        "all-of waits for the other selected task"
    );
    second_release.release();
    let result = runner.await.expect("run-until task joins");
    assert!(result.shutdown.is_ok());
    assert_eq!(
        result
            .tasks
            .iter()
            .map(|completion| completion.membership)
            .collect::<Vec<_>>(),
        [second.membership(), first.membership()]
    );
    assert!(matches!(result.tasks[0].exit.kind(), ExitKind::Failed(_)));
    assert!(matches!(result.tasks[1].exit.kind(), ExitKind::Completed));
}

#[tokio::test]
async fn run_until_all_accepts_an_empty_selection() {
    let release = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_raw_once("idle", RawOnceDef::new(ParkedTarget(release.clone())))
        .expect("idle id is valid");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("idle child starts");
    release.release();
    let result = system.run_until_all([], Duration::from_secs(1)).await;
    assert!(result.tasks.is_empty());
    assert!(result.shutdown.is_ok());
}
