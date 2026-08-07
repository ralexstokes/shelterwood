use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorDef, ActorOnceDef, Backoff, Context, DynamicTree, ExitError, ExitKind, ExitResult,
    Readiness, RestartCondition, RestartPolicy, Shutdown, StartupError, StartupFailureCause,
    TaskDef, Tree,
};
use shelterwood_test_support::{ConsumeCount, ConsumeGuard, ReleaseGate, poll_until};

#[derive(Clone, Copy)]
enum ResourceMode {
    Normal,
    InitPanic,
    StartupFailure,
}

struct ResourceArgs {
    guard: ConsumeGuard,
    mode: ResourceMode,
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
            ResourceMode::Normal => Ok(Self { _guard: args.guard }),
            ResourceMode::InitPanic => panic!("actor init panic"),
            ResourceMode::StartupFailure => Err(ExitError::message("actor init failed")),
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
                mode: ResourceMode::Normal,
            }),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(ResourceMessage::Stop).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    count.assert_once();
}

async fn assert_actor_init_path_drops_once(mode: ResourceMode) {
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
    assert_actor_init_path_drops_once(ResourceMode::InitPanic).await;
    assert_actor_init_path_drops_once(ResourceMode::StartupFailure).await;
}

#[tokio::test]
async fn one_shot_actor_args_drop_once_when_shutdown_prevents_start() {
    let count = ConsumeCount::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .readiness(Readiness::Manual)
        .expect("manual task readiness is valid"),
    )
    .expect("valid gate");
    tree.add_actor_once(
        "actor",
        ActorOnceDef::<ResourceActor>::new(ResourceArgs {
            guard: count.guard(),
            mode: ResourceMode::Normal,
        }),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    tokio::task::yield_now().await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
    count.assert_once();
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

enum RestartMessage {
    Poison,
    Fresh,
    StaleTimer,
    StaleOffload,
}

struct RestartActor {
    generation: usize,
    stale_seen: Arc<AtomicBool>,
}

impl Actor for RestartActor {
    type Msg = RestartMessage;
    type Args = (usize, Arc<AtomicBool>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            generation: args.0,
            stale_seen: args.1,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            RestartMessage::Poison => {
                assert_eq!(self.generation, 1);
                context
                    .set_timeout("stale", RestartMessage::StaleTimer, Duration::ZERO)
                    .expect("timer accepted");
                context
                    .offload(async {}, |_| RestartMessage::StaleOffload, Duration::MAX)
                    .expect("offload accepted");
                Err(ExitError::message("poisoned incarnation"))
            }
            RestartMessage::Fresh => {
                assert_eq!(self.generation, 2);
                context.stop();
                Ok(())
            }
            RestartMessage::StaleTimer | RestartMessage::StaleOffload => {
                self.stale_seen.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    }
}

#[tokio::test]
async fn timers_and_offload_completions_never_cross_an_incarnation_boundary() {
    let generations = Arc::new(AtomicUsize::new(0));
    let stale_seen = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let next = Arc::clone(&generations);
    let stale = Arc::clone(&stale_seen);
    let actor = tree
        .add_actor(
            "actor",
            ActorDef::<RestartActor>::factory(move || {
                let generation = next.fetch_add(1, Ordering::SeqCst) + 1;
                (generation, Arc::clone(&stale))
            })
            .restart(RestartPolicy::new(
                RestartCondition::OnFailure,
                Backoff::Immediate,
            )),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    actor
        .send(RestartMessage::Poison)
        .await
        .expect("actor live");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            generations.load(Ordering::SeqCst) >= 2
        })
        .await
    );
    actor
        .send(RestartMessage::Fresh)
        .await
        .expect("replacement live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(!stale_seen.load(Ordering::SeqCst));
}

struct GatedDynamicActor;

impl Actor for GatedDynamicActor {
    type Msg = ();
    type Args = ReleaseGate;

    async fn init(gate: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        gate.wait().await;
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn dynamic_actor_add_resolves_at_admission_without_awaiting_init() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let gate = ReleaseGate::default();
    let receipt = tokio::time::timeout(
        Duration::from_secs(1),
        scope.add_actor_once(
            "gated",
            ActorOnceDef::<GatedDynamicActor>::new(gate)
                .readiness(Readiness::Manual)
                .shutdown(Shutdown::Abort),
        ),
    )
    .await
    .expect("admission does not wait for init")
    .expect("actor admitted");
    let actor = receipt.into_handles();
    actor.send(()).await.expect("admitted mailbox is usable");
    assert_eq!(
        scope.remove_actor(&actor).await,
        shelterwood::RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}
