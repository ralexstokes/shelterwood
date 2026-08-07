use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorOnceDef, Context, DeadlineElapsed, ExitError, ExitKind, ExitResult, Readiness,
    StartupError, StartupFailureCause, Tree,
};
use shelterwood_test_support::poll_until;

enum ZeroMessage {
    Done,
}

struct ZeroDeadlineActor;

impl Actor for ZeroDeadlineActor {
    type Msg = ZeroMessage;
    type Args = Arc<AtomicBool>;

    async fn init(polled: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        let actor_task = format!("{:?}", tokio::task::id());
        context
            .offload(
                async move {
                    polled.store(true, Ordering::SeqCst);
                    7usize
                },
                move |result| {
                    assert_eq!(result, Err(DeadlineElapsed));
                    assert_eq!(format!("{:?}", tokio::task::id()), actor_task);
                    ZeroMessage::Done
                },
                Duration::ZERO,
            )
            .expect("live offload accepted");
        Ok(Self)
    }

    async fn handle(
        &mut self,
        ZeroMessage::Done: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        context.stop();
        Ok(())
    }
}

#[tokio::test]
async fn zero_budget_offload_never_polls_work_and_times_out_on_actor_task() {
    let polled = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_actor_once(
        "zero",
        ActorOnceDef::<ZeroDeadlineActor>::new(Arc::clone(&polled)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(!polled.load(Ordering::SeqCst));
}

enum DeadlineMessage {
    Start,
    Completed(Result<usize, DeadlineElapsed>),
}

struct ExactDeadlineActor {
    armed: Arc<AtomicBool>,
    result: Arc<Mutex<Option<Result<usize, DeadlineElapsed>>>>,
}

impl Actor for ExactDeadlineActor {
    type Msg = DeadlineMessage;
    type Args = (
        Arc<AtomicBool>,
        Arc<Mutex<Option<Result<usize, DeadlineElapsed>>>>,
    );

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            armed: args.0,
            result: args.1,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            DeadlineMessage::Start => {
                context
                    .offload(
                        async {
                            tokio::time::sleep(Duration::from_secs(10)).await;
                            42usize
                        },
                        DeadlineMessage::Completed,
                        Duration::from_secs(10),
                    )
                    .expect("offload accepted");
                self.armed.store(true, Ordering::SeqCst);
            }
            DeadlineMessage::Completed(result) => {
                *self.result.lock().expect("result mutex poisoned") = Some(result);
                context.stop();
            }
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn offload_completion_wins_at_the_exact_deadline() {
    let armed = Arc::new(AtomicBool::new(false));
    let result = Arc::new(Mutex::new(None));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "deadline",
            ActorOnceDef::<ExactDeadlineActor>::new((Arc::clone(&armed), Arc::clone(&result))),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(DeadlineMessage::Start)
        .await
        .expect("actor live");
    while !armed.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(*result.lock().expect("result mutex poisoned"), Some(Ok(42)));
}

enum GuardMessage {
    Start,
    Unexpected,
    Stop,
}

struct GuardedActor {
    deliveries: Arc<AtomicUsize>,
}

impl Actor for GuardedActor {
    type Msg = GuardMessage;
    type Args = Arc<AtomicUsize>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { deliveries: args })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            GuardMessage::Start => {
                let guard = context
                    .offload_scoped(
                        async { 1usize },
                        |_| GuardMessage::Unexpected,
                        Duration::MAX,
                    )
                    .expect("guarded offload accepted");
                drop(guard);
            }
            GuardMessage::Unexpected => {
                self.deliveries.fetch_add(1, Ordering::SeqCst);
            }
            GuardMessage::Stop => context.stop(),
        }
        Ok(())
    }
}

#[tokio::test]
async fn dropping_scoped_guard_suppresses_the_continuation() {
    let deliveries = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "guarded",
            ActorOnceDef::<GuardedActor>::new(Arc::clone(&deliveries)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(GuardMessage::Start).await.expect("actor live");
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    actor.send(GuardMessage::Stop).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(deliveries.load(Ordering::SeqCst), 0);
}

struct DropLog {
    name: &'static str,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for DropLog {
    fn drop(&mut self) {
        self.log
            .lock()
            .expect("drop log mutex poisoned")
            .push(self.name);
    }
}

enum TeardownMessage {
    Start,
}

struct TeardownActor {
    _drop: DropLog,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for TeardownActor {
    type Msg = TeardownMessage;
    type Args = Arc<Mutex<Vec<&'static str>>>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            _drop: DropLog {
                name: "actor",
                log: Arc::clone(&args),
            },
            log: args,
        })
    }

    async fn handle(
        &mut self,
        TeardownMessage::Start: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        let drop = DropLog {
            name: "offload",
            log: Arc::clone(&self.log),
        };
        context
            .offload(
                async move {
                    let _drop = drop;
                    std::future::pending::<()>().await;
                },
                |_| TeardownMessage::Start,
                Duration::MAX,
            )
            .expect("offload accepted");
        context.stop();
        Ok(())
    }
}

#[tokio::test]
async fn incarnation_offloads_are_destroyed_before_actor_state() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "teardown",
            ActorOnceDef::<TeardownActor>::new(Arc::clone(&log)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(TeardownMessage::Start)
        .await
        .expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *log.lock().expect("drop log mutex poisoned"),
        ["offload", "actor"]
    );
}

enum BlockingMessage {
    Run,
}

struct BlockingActor {
    observed: Arc<AtomicUsize>,
}

impl Actor for BlockingActor {
    type Msg = BlockingMessage;
    type Args = Arc<AtomicUsize>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { observed: args })
    }

    async fn handle(
        &mut self,
        BlockingMessage::Run: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        let value = context.run_blocking(|token| {
            assert!(!token.is_cancelled());
            73usize
        });
        self.observed.store(value.await, Ordering::SeqCst);
        context.stop();
        Ok(())
    }
}

#[tokio::test]
async fn run_blocking_returns_on_the_actor_task() {
    let observed = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "blocking",
            ActorOnceDef::<BlockingActor>::new(Arc::clone(&observed)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(BlockingMessage::Run).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(observed.load(Ordering::SeqCst), 73);
}

enum CancelBlockingMessage {
    Run,
}

struct CancelBlockingActor {
    cancelled: Arc<AtomicBool>,
}

impl Actor for CancelBlockingActor {
    type Msg = CancelBlockingMessage;
    type Args = Arc<AtomicBool>;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { cancelled: args })
    }

    async fn handle(
        &mut self,
        CancelBlockingMessage::Run: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        let observed = Arc::clone(&self.cancelled);
        let work = context.run_blocking(move |token| {
            while !token.is_cancelled() {
                std::thread::yield_now();
            }
            observed.store(true, Ordering::SeqCst);
        });
        drop(work);
        context.stop();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_run_blocking_future_cancels_and_detaches_its_thread() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "blocking",
            ActorOnceDef::<CancelBlockingActor>::new(Arc::clone(&cancelled)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(CancelBlockingMessage::Run)
        .await
        .expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            cancelled.load(Ordering::SeqCst)
        })
        .await
    );
}

#[derive(Clone, Copy)]
enum PanicMode {
    Future,
    Continuation,
    Blocking,
    HandlerAndDrop,
}

enum PanicMessage {
    Trigger,
    Delivery,
}

struct PanicActor {
    mode: PanicMode,
}

impl Drop for PanicActor {
    fn drop(&mut self) {
        if matches!(self.mode, PanicMode::HandlerAndDrop) {
            panic!("secondary destructor panic");
        }
    }
}

impl Actor for PanicActor {
    type Msg = PanicMessage;
    type Args = PanicMode;

    async fn init(mode: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        match mode {
            PanicMode::Future => context
                .offload(
                    async {
                        panic!("offload future panic");
                        #[allow(unreachable_code)]
                        ()
                    },
                    |_| PanicMessage::Delivery,
                    Duration::MAX,
                )
                .expect("offload accepted"),
            PanicMode::Continuation => context
                .offload(
                    async {},
                    |_| panic!("offload continuation panic"),
                    Duration::MAX,
                )
                .expect("offload accepted"),
            PanicMode::Blocking | PanicMode::HandlerAndDrop => {}
        }
        Ok(Self { mode })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match (self.mode, message) {
            (PanicMode::Blocking, PanicMessage::Trigger) => {
                context
                    .run_blocking(|_| -> () { panic!("blocking panic") })
                    .await;
            }
            (PanicMode::HandlerAndDrop, PanicMessage::Trigger) => {
                panic!("primary handler panic");
            }
            (_, PanicMessage::Delivery | PanicMessage::Trigger) => {}
        }
        Ok(())
    }
}

fn startup_panic_message(error: StartupError) -> Option<String> {
    let StartupError::StartupFailed(failure) = error else {
        panic!("expected child startup failure");
    };
    let StartupFailureCause::Child { exit, .. } = failure.cause else {
        panic!("expected child failure");
    };
    let ExitKind::Panicked { message } = exit.kind() else {
        panic!("expected panic exit, got {:?}", exit.kind());
    };
    message.clone()
}

async fn assert_pre_ready_panic(mode: PanicMode, expected: &str, trigger: bool) {
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "panic",
            ActorOnceDef::<PanicActor>::new(mode).readiness(Readiness::Manual),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    if trigger {
        actor
            .send(PanicMessage::Trigger)
            .await
            .expect("mailbox accepts before readiness");
    }
    let message = startup_panic_message(
        system
            .wait_started()
            .await
            .expect_err("panic before readiness fails startup"),
    );
    assert_eq!(message.as_deref(), Some(expected));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
}

#[tokio::test]
async fn offload_future_and_continuation_panics_resume_on_actor_task() {
    assert_pre_ready_panic(PanicMode::Future, "offload future panic", false).await;
    assert_pre_ready_panic(PanicMode::Continuation, "offload continuation panic", false).await;
}

#[tokio::test]
async fn run_blocking_panic_resumes_where_awaited() {
    assert_pre_ready_panic(PanicMode::Blocking, "blocking panic", true).await;
}

#[tokio::test]
async fn primary_callback_panic_survives_a_panicking_actor_destructor() {
    assert_pre_ready_panic(PanicMode::HandlerAndDrop, "primary handler panic", true).await;
}
