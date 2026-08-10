use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::common::{
    ConsumeCount, ConsumeGuard, POLL_TIMEOUT, ReleaseGate, policy::never, poll_once, poll_until,
};
use shelterwood::{
    Actor, ActorOnceDef, Context, DynamicTree, ExitError, ExitResult, RawActor, RawContext,
    RawOnceDef, Readiness, ReadinessDeadline, SubtreeOnceDef, TaskDef, TaskOnceDef, Tree,
};

#[derive(Clone, Copy)]
enum RawResourceMode {
    Normal,
    StartupFailure,
}

struct ResourceRawActor {
    _guard: ConsumeGuard,
    mode: RawResourceMode,
}

impl RawActor for ResourceRawActor {
    type Msg = ();

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        match self.mode {
            RawResourceMode::Normal => Ok(()),
            RawResourceMode::StartupFailure => {
                Err(ExitError::message("raw actor failed before ready"))
            }
        }
    }
}

struct ReadinessPanicRawActor {
    _guard: ConsumeGuard,
}

impl RawActor for ReadinessPanicRawActor {
    type Msg = ();

    fn readiness() -> Readiness {
        panic!("raw readiness panic")
    }

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        unreachable!("readiness metadata prevents admission")
    }
}

fn resource_raw_actor(count: &ConsumeCount, mode: RawResourceMode) -> ResourceRawActor {
    ResourceRawActor {
        _guard: count.guard(),
        mode,
    }
}

#[tokio::test]
async fn one_shot_raw_resource_drops_once_on_normal_exit() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "raw",
        RawOnceDef::new(resource_raw_actor(&count, RawResourceMode::Normal)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    count.assert_once();
}

#[test]
fn one_shot_raw_resource_drops_once_on_readiness_definition_panic() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = tree.add_raw_once(
            "raw",
            RawOnceDef::new(ReadinessPanicRawActor {
                _guard: count.guard(),
            }),
        );
    }));
    assert!(result.is_err());
    count.assert_once();
}

#[tokio::test]
async fn dynamic_one_shot_raw_resource_drops_once_on_readiness_definition_panic() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let count = ConsumeCount::default();

    let result = catch_unwind(AssertUnwindSafe(|| {
        drop(scope.add_raw_once(
            "raw",
            RawOnceDef::new(ReadinessPanicRawActor {
                _guard: count.guard(),
            }),
        ));
    }));

    assert!(result.is_err());
    count.assert_once();
    drop(
        scope
            .reserve_task("raw")
            .expect("the panicking dynamic definition releases its id"),
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root stops");
}

#[tokio::test]
async fn one_shot_raw_resource_drops_once_on_startup_failure() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_raw_once(
        "raw",
        RawOnceDef::new(resource_raw_actor(&count, RawResourceMode::StartupFailure))
            .readiness(Readiness::Manual)
            .expect("manual raw readiness"),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_raw_resource_drops_once_on_shutdown_before_start() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .restart(never())
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid gate");
    tree.add_raw_once(
        "never-spawned",
        RawOnceDef::new(resource_raw_actor(&count, RawResourceMode::Normal)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown completes");
    count.assert_once();
}

#[derive(Clone, Copy)]
enum ActorResourceMode {
    Normal,
    InitPanic,
    StartupFailure,
}

struct ResourceArgs {
    guard: ConsumeGuard,
    mode: ActorResourceMode,
}

enum ResourceMessage {
    Stop,
}

struct ResourceActor {
    _guard: ConsumeGuard,
}

impl Actor for ResourceActor {
    type Msg = ResourceMessage;
    type Args = ResourceArgs;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        match args.mode {
            ActorResourceMode::Normal => Ok(Self { _guard: args.guard }),
            ActorResourceMode::InitPanic => panic!("actor init panic"),
            ActorResourceMode::StartupFailure => Err(ExitError::message("actor init failed")),
        }
    }

    async fn handle(
        &mut self,
        ResourceMessage::Stop: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        context.stop();
        Ok(())
    }
}

#[tokio::test]
async fn one_shot_actor_args_drop_once_on_normal_exit() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<ResourceActor>::new(ResourceArgs {
                guard: count.guard(),
                mode: ActorResourceMode::Normal,
            }),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(ResourceMessage::Stop).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    count.assert_once();
}

async fn assert_actor_init_path_drops_once(mode: ActorResourceMode) {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_actor_once(
        "actor",
        ActorOnceDef::<ResourceActor>::new(ResourceArgs {
            guard: count.guard(),
            mode,
        }),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect_err("init path fails startup");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_actor_args_drop_once_on_init_panic_and_startup_failure() {
    assert_actor_init_path_drops_once(ActorResourceMode::InitPanic).await;
    assert_actor_init_path_drops_once(ActorResourceMode::StartupFailure).await;
}

#[tokio::test]
async fn one_shot_actor_args_drop_once_when_shutdown_prevents_start() {
    let count = ConsumeCount::default();
    let gate_started = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new({
            let gate_started = gate_started.clone();
            move |context| {
                let gate_started = gate_started.clone();
                async move {
                    gate_started.release();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })
        .readiness(Readiness::Manual)
        .expect("manual task readiness is valid"),
    )
    .expect("valid gate");
    tree.add_actor_once(
        "actor",
        ActorOnceDef::<ResourceActor>::new(ResourceArgs {
            guard: count.guard(),
            mode: ActorResourceMode::Normal,
        }),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    gate_started.wait().await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_normal_exit() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    let (_task, completion) = tree
        .add_task_once(
            "task",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    completion.wait().await.expect("task completes");
    system.wait().await;
    count.assert_once();
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_panic() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    let (task, _completion) = tree
        .add_task_once(
            "task",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                panic!("construction body panic");
                #[allow(unreachable_code)]
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    task.wait().await;
    system.wait().await;
    count.assert_once();
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_startup_failure() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    let (_task, _completion) = tree
        .add_task_once(
            "task",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Err::<(), _>(ExitError::message("failed before ready"))
            })
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_shutdown_before_start() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .restart(never())
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid gate");
    let (_task, _completion) = tree
        .add_task_once(
            "never-spawned",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown completes");
    count.assert_once();
}

fn one_shot_subtree_with_guard(count: &ConsumeCount, mode: &'static str) -> Tree {
    let guard = count.guard();
    let mut tree = Tree::new();
    let definition = match mode {
        "normal" => TaskOnceDef::new(move |_| async move {
            let _guard = guard;
            Ok::<_, ExitError>(())
        }),
        "panic" => TaskOnceDef::new(move |_| async move {
            let _guard = guard;
            panic!("subtree construction panic");
            #[allow(unreachable_code)]
            Ok::<_, ExitError>(())
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness"),
        _ => unreachable!("known fixture mode"),
    };
    let (_task, _completion) = tree
        .add_task_once("resource", definition)
        .expect("valid task");
    tree
}

struct CancelledResourceActor {
    _guard: ConsumeGuard,
}

impl Actor for CancelledResourceActor {
    type Msg = ();
    type Args = (ConsumeGuard, Arc<AtomicBool>);

    async fn init(
        (guard, started): Self::Args,
        _: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        started.store(true, Ordering::SeqCst);
        Ok(Self { _guard: guard })
    }

    async fn handle(&mut self, _: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        std::future::pending().await
    }
}

struct CancelledResourceRawActor {
    _guard: ConsumeGuard,
    started: Arc<AtomicBool>,
}

impl RawActor for CancelledResourceRawActor {
    type Msg = ();

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        self.started.store(true, Ordering::SeqCst);
        std::future::pending().await
    }
}

fn cancelled_one_shot_subtree(count: &ConsumeCount, started: Arc<AtomicBool>) -> Tree {
    let guard = count.guard();
    let mut tree = Tree::new();
    let (_task, _completion) = tree
        .add_task_once(
            "resource",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                started.store(true, Ordering::SeqCst);
                std::future::pending::<Result<(), ExitError>>().await
            }),
        )
        .expect("valid task");
    tree
}

#[tokio::test]
async fn cancelling_inflight_one_shot_adds_drops_every_kind_resource_once() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();

    let task_count = ConsumeCount::default();
    let task_started = Arc::new(AtomicBool::new(false));
    let task_guard = task_count.guard();
    // The `!started` assertions below are deterministic only on the
    // current_thread runtime: no await separates each enqueueing poll from its
    // fused drop, so the driver cannot process the admission first (under
    // multi_thread this would race).
    let mut task = Box::pin(scope.add_task_once(
        "task",
        TaskOnceDef::new({
            let task_started = Arc::clone(&task_started);
            move |_| async move {
                let _guard = task_guard;
                task_started.store(true, Ordering::SeqCst);
                std::future::pending::<Result<(), ExitError>>().await
            }
        }),
    ));
    assert!(poll_once(task.as_mut()).is_pending());
    drop(task);

    let actor_count = ConsumeCount::default();
    let actor_started = Arc::new(AtomicBool::new(false));
    let mut actor = Box::pin(scope.add_actor_once(
        "actor",
        ActorOnceDef::<CancelledResourceActor>::new((
            actor_count.guard(),
            Arc::clone(&actor_started),
        )),
    ));
    assert!(poll_once(actor.as_mut()).is_pending());
    drop(actor);

    let raw_count = ConsumeCount::default();
    let raw_started = Arc::new(AtomicBool::new(false));
    let mut raw = Box::pin(scope.add_raw_once(
        "raw",
        RawOnceDef::new(CancelledResourceRawActor {
            _guard: raw_count.guard(),
            started: Arc::clone(&raw_started),
        }),
    ));
    assert!(poll_once(raw.as_mut()).is_pending());
    drop(raw);

    let subtree_count = ConsumeCount::default();
    let subtree_started = Arc::new(AtomicBool::new(false));
    let mut subtree = Box::pin(scope.add_subtree_once(
        "subtree",
        SubtreeOnceDef::new(cancelled_one_shot_subtree(
            &subtree_count,
            Arc::clone(&subtree_started),
        )),
    ));
    assert!(poll_once(subtree.as_mut()).is_pending());
    drop(subtree);

    for (kind, count, started) in [
        ("task", &task_count, &task_started),
        ("actor", &actor_count, &actor_started),
        ("raw actor", &raw_count, &raw_started),
        ("subtree", &subtree_count, &subtree_started),
    ] {
        assert!(
            poll_until(POLL_TIMEOUT, Duration::from_millis(1), || count.get() == 1).await,
            "cancelled {kind} admission disposes its resource"
        );
        count.assert_once();
        assert!(
            !started.load(Ordering::SeqCst),
            "cancelled {kind} admission never starts construction"
        );
    }

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("cancelled admissions leave no stragglers");
    task_count.assert_once();
    actor_count.assert_once();
    raw_count.assert_once();
    subtree_count.assert_once();
    assert!(!task_started.load(Ordering::SeqCst));
    assert!(!actor_started.load(Ordering::SeqCst));
    assert!(!raw_started.load(Ordering::SeqCst));
    assert!(!subtree_started.load(Ordering::SeqCst));
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_normal_exit() {
    let count = ConsumeCount::default();
    let nested = one_shot_subtree_with_guard(&count, "normal");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait().await;
    count.assert_once();
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_panic() {
    let count = ConsumeCount::default();
    let nested = one_shot_subtree_with_guard(&count, "panic");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_lowering_failure() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut nested = Tree::new();
    let _undefined = nested.reserve_task("undefined").expect("reservation");
    let (_task, _completion) = nested
        .add_task_once(
            "resource",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_shutdown_before_start() {
    let count = ConsumeCount::default();
    let nested = one_shot_subtree_with_guard(&count, "normal");
    let mut root = Tree::new();
    root.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid gate");
    root.add_subtree_once("never-spawned", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown completes");
    count.assert_once();
}
