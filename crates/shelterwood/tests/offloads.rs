mod common;

use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{POLL_TIMEOUT, PanicOnDrop, ReleaseGate, assert_eventually, assert_quiet};
use shelterwood::{
    Actor, ActorDef, ActorOnceDef, Context, DeadlineElapsed, ExitError, ExitKind, ExitResult,
    Guard, Handler, LifecycleEventKind, LifecycleItem, Mailbox, MailboxShutdown, RawActor,
    RawContext, RawOnceDef, Readiness, Shutdown, StartupError, StartupFailureCause, StopContext,
    Tree,
};

enum ZeroMessage {
    Done,
}

struct ZeroDeadlineActor;

#[derive(Debug, Eq, PartialEq)]
struct ZeroDeadlineObservation {
    result: Result<usize, DeadlineElapsed>,
    continuation_task_matches: bool,
}

struct ZeroDeadlinePanickingDrop {
    polled: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}

impl std::future::Future for ZeroDeadlinePanickingDrop {
    type Output = usize;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.polled.store(true, Ordering::SeqCst);
        std::task::Poll::Pending
    }
}

impl Drop for ZeroDeadlinePanickingDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("zero-budget offload destructor panic");
    }
}

struct ZeroDeadlinePanickingDropActor;

impl Actor for ZeroDeadlineActor {
    type Msg = ZeroMessage;
    type Args = (
        Arc<AtomicBool>,
        std::sync::mpsc::Sender<ZeroDeadlineObservation>,
    );

    async fn init(
        (polled, observed): Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        let actor_task = format!("{:?}", tokio::task::id());
        context
            .offload(
                async move {
                    polled.store(true, Ordering::SeqCst);
                    7usize
                },
                move |result| {
                    observed
                        .send(ZeroDeadlineObservation {
                            result,
                            continuation_task_matches: format!("{:?}", tokio::task::id())
                                == actor_task,
                        })
                        .expect("the test retains the observation receiver");
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

impl Actor for ZeroDeadlinePanickingDropActor {
    type Msg = ZeroMessage;
    type Args = (Arc<AtomicBool>, Arc<AtomicUsize>);

    async fn init(
        (polled, drops): Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        context
            .offload(
                ZeroDeadlinePanickingDrop { polled, drops },
                |_| ZeroMessage::Done,
                Duration::ZERO,
            )
            .expect("live offload accepted");
        Ok(Self)
    }

    async fn handle(
        &mut self,
        ZeroMessage::Done: Self::Msg,
        _: &mut Context<'_, Self>,
    ) -> ExitResult {
        panic!("cleanup panic must precede continuation delivery");
    }
}

#[tokio::test]
async fn zero_budget_offload_never_polls_work_and_times_out_on_actor_task() {
    let polled = Arc::new(AtomicBool::new(false));
    let (observed, observations) = std::sync::mpsc::channel();
    let mut tree = Tree::new();
    tree.add_actor_once(
        "zero",
        ActorOnceDef::<ZeroDeadlineActor>::new((Arc::clone(&polled), observed)),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(
        observations
            .recv_timeout(POLL_TIMEOUT)
            .expect("the continuation publishes its result"),
        ZeroDeadlineObservation {
            result: Err(DeadlineElapsed),
            continuation_task_matches: true,
        },
        "a zero-budget offload times out without polling and its continuation runs on the actor task"
    );
    assert!(
        observations.try_recv().is_err(),
        "the continuation publishes exactly one observation"
    );
}

#[tokio::test]
async fn zero_budget_offload_contains_and_classifies_its_work_destructor_panic() {
    let polled = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_actor_once(
        "zero-drop-panic",
        ActorOnceDef::<ZeroDeadlinePanickingDropActor>::new((
            Arc::clone(&polled),
            Arc::clone(&drops),
        ))
        .readiness(Readiness::Manual),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");

    let message = startup_panic_message(
        system
            .wait_started()
            .await
            .expect_err("the contained pre-ready panic fails startup"),
    );
    assert_eq!(
        message.as_deref(),
        Some("zero-budget offload destructor panic")
    );
    assert!(!polled.load(Ordering::SeqCst));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
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
            .restart(Default::default()),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    actor
        .send(RestartMessage::Poison)
        .await
        .expect("actor live");
    assert_eventually!(|| generations.load(Ordering::SeqCst) >= 2).await;
    actor
        .send(RestartMessage::Fresh)
        .await
        .expect("replacement live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(!stale_seen.load(Ordering::SeqCst));
}

enum DetachedBlockingMessage {
    Poison,
    Fresh,
    Stop,
    StaleCompletion,
}

struct DetachedBlockingResult(ReleaseGate);

impl Drop for DetachedBlockingResult {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Default)]
struct BlockingThreadRelease {
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl BlockingThreadRelease {
    fn gate(&self) -> Arc<(Mutex<bool>, Condvar)> {
        Arc::clone(&self.gate)
    }

    fn release(&self) {
        let (released, changed) = &*self.gate;
        *released.lock().expect("release mutex poisoned") = true;
        changed.notify_all();
    }
}

impl Drop for BlockingThreadRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct DetachedBlockingActor {
    generation: usize,
    thread_started: ReleaseGate,
    thread_release: Arc<(Mutex<bool>, Condvar)>,
    result_disposed: ReleaseGate,
    fresh_seen: ReleaseGate,
    stale_seen: Arc<AtomicBool>,
    map_ran: Arc<AtomicBool>,
}

impl Actor for DetachedBlockingActor {
    type Msg = DetachedBlockingMessage;
    type Args = (
        usize,
        ReleaseGate,
        Arc<(Mutex<bool>, Condvar)>,
        ReleaseGate,
        ReleaseGate,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    );

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            generation: args.0,
            thread_started: args.1,
            thread_release: args.2,
            result_disposed: args.3,
            fresh_seen: args.4,
            stale_seen: args.5,
            map_ran: args.6,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            DetachedBlockingMessage::Poison => {
                assert_eq!(self.generation, 1);
                let thread_started = self.thread_started.clone();
                let thread_release = Arc::clone(&self.thread_release);
                let result_disposed = self.result_disposed.clone();
                let work = context.run_blocking(move |cancellation| {
                    thread_started.release();
                    let (released, changed) = &*thread_release;
                    let mut released = released.lock().expect("release mutex poisoned");
                    while !*released {
                        released = changed
                            .wait(released)
                            .expect("release mutex poisoned while waiting");
                    }
                    assert!(
                        cancellation.is_cancelled(),
                        "the old incarnation cancels detached blocking work"
                    );
                    DetachedBlockingResult(result_disposed)
                });
                self.thread_started.wait().await;
                context
                    .offload(
                        work,
                        {
                            let map_ran = Arc::clone(&self.map_ran);
                            move |_| {
                                map_ran.store(true, Ordering::SeqCst);
                                DetachedBlockingMessage::StaleCompletion
                            }
                        },
                        Duration::MAX,
                    )
                    .expect("blocking future is accepted before failure");
                Err(ExitError::message("poisoned incarnation"))
            }
            DetachedBlockingMessage::Fresh => {
                assert_eq!(self.generation, 2);
                self.fresh_seen.release();
                Ok(())
            }
            DetachedBlockingMessage::Stop => {
                assert_eq!(self.generation, 2);
                context.stop();
                Ok(())
            }
            DetachedBlockingMessage::StaleCompletion => {
                self.stale_seen.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_run_blocking_completion_stays_with_its_old_incarnation() {
    let generations = Arc::new(AtomicUsize::new(0));
    let thread_started = ReleaseGate::default();
    // A failed assertion must not leave Tokio's blocking pool stuck waiting
    // while the test runtime tries to shut down.
    let thread_release = BlockingThreadRelease::default();
    let result_disposed = ReleaseGate::default();
    let fresh_seen = ReleaseGate::default();
    let stale_seen = Arc::new(AtomicBool::new(false));
    let map_ran = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor(
            "actor",
            ActorDef::<DetachedBlockingActor>::factory({
                let generations = Arc::clone(&generations);
                let thread_started = thread_started.clone();
                let thread_release = thread_release.gate();
                let result_disposed = result_disposed.clone();
                let fresh_seen = fresh_seen.clone();
                let stale_seen = Arc::clone(&stale_seen);
                let map_ran = Arc::clone(&map_ran);
                move || {
                    (
                        generations.fetch_add(1, Ordering::SeqCst) + 1,
                        thread_started.clone(),
                        Arc::clone(&thread_release),
                        result_disposed.clone(),
                        fresh_seen.clone(),
                        Arc::clone(&stale_seen),
                        Arc::clone(&map_ran),
                    )
                }
            }),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    actor
        .send(DetachedBlockingMessage::Poison)
        .await
        .expect("actor accepts poison");
    assert_eventually!(
        || generations.load(Ordering::SeqCst) >= 2,
        "replacement starts while the old blocking thread remains detached"
    )
    .await;

    thread_release.release();
    // The gate only witnesses that the result's destructor ran somewhere; the
    // `map_ran` assertions below pin down that disposal, not the map closure,
    // consumed it.
    tokio::time::timeout(POLL_TIMEOUT, result_disposed.wait())
        .await
        .expect("the detached blocking result's destructor runs");
    assert!(
        !map_ran.load(Ordering::SeqCst),
        "the detached completion is disposed without running the map closure"
    );

    actor
        .send(DetachedBlockingMessage::Fresh)
        .await
        .expect("replacement remains live");
    tokio::time::timeout(POLL_TIMEOUT, fresh_seen.wait())
        .await
        .expect("replacement processes the synchronization message");
    assert_quiet(Duration::from_millis(20), || {
        stale_seen.load(Ordering::SeqCst)
    })
    .await;
    actor
        .send(DetachedBlockingMessage::Stop)
        .await
        .expect("replacement accepts the final stop");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(!stale_seen.load(Ordering::SeqCst));
    assert!(
        !map_ran.load(Ordering::SeqCst),
        "the stale completion was never mapped"
    );
}

enum DeadlineMessage {
    Start,
    Completed(Result<usize, DeadlineElapsed>),
}

struct ExactDeadlineActor {
    armed: Arc<AtomicBool>,
    offload_started: Arc<AtomicBool>,
    result: Arc<Mutex<Option<Result<usize, DeadlineElapsed>>>>,
}

impl Actor for ExactDeadlineActor {
    type Msg = DeadlineMessage;
    type Args = (
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        Arc<Mutex<Option<Result<usize, DeadlineElapsed>>>>,
    );

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            armed: args.0,
            offload_started: args.1,
            result: args.2,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            DeadlineMessage::Start => {
                let offload_started = Arc::clone(&self.offload_started);
                context
                    .offload(
                        async move {
                            offload_started.store(true, Ordering::SeqCst);
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
    let offload_started = Arc::new(AtomicBool::new(false));
    let result = Arc::new(Mutex::new(None));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "deadline",
            ActorOnceDef::<ExactDeadlineActor>::new((
                Arc::clone(&armed),
                Arc::clone(&offload_started),
                Arc::clone(&result),
            )),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(DeadlineMessage::Start)
        .await
        .expect("actor live");
    assert_eventually!(
        || armed.load(Ordering::SeqCst) && offload_started.load(Ordering::SeqCst),
        "the offload is polled and its deadline is registered before time moves"
    )
    .await;
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
    guard_dropped: Arc<AtomicBool>,
}

impl Actor for GuardedActor {
    type Msg = GuardMessage;
    type Args = (Arc<AtomicUsize>, Arc<AtomicBool>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            deliveries: args.0,
            guard_dropped: args.1,
        })
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
                self.guard_dropped.store(true, Ordering::SeqCst);
            }
            GuardMessage::Unexpected => {
                self.deliveries.fetch_add(1, Ordering::SeqCst);
            }
            GuardMessage::Stop => context.stop(),
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn dropping_scoped_guard_suppresses_the_continuation() {
    let deliveries = Arc::new(AtomicUsize::new(0));
    let guard_dropped = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "guarded",
            ActorOnceDef::<GuardedActor>::new((
                Arc::clone(&deliveries),
                Arc::clone(&guard_dropped),
            )),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(GuardMessage::Start).await.expect("actor live");
    assert_eventually!(
        || guard_dropped.load(Ordering::SeqCst),
        "the actor drops the guard before the negative window begins"
    )
    .await;
    assert_quiet(Duration::from_secs(1), || {
        deliveries.load(Ordering::SeqCst) != 0
    })
    .await;
    actor.send(GuardMessage::Stop).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(deliveries.load(Ordering::SeqCst), 0);
}

struct ExportGuardActor;

impl Actor for ExportGuardActor {
    type Msg = ();
    type Args = tokio::sync::oneshot::Sender<Guard>;

    async fn init(
        guard_sender: Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        let guard = context
            .offload_scoped(std::future::pending::<()>(), |_| (), Duration::MAX)
            .expect("scoped offload accepted");
        guard_sender
            .send(guard)
            .expect("test still awaits cancellation guard");
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn guard_reports_incarnation_cancellation() {
    let (guard_sender, guard_receiver) = tokio::sync::oneshot::channel();
    let mut tree = Tree::new();
    tree.add_actor_once(
        "guard-exporter",
        ActorOnceDef::<ExportGuardActor>::new(guard_sender),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let guard = guard_receiver.await.expect("actor exports guard");
    assert!(!guard.is_cancelled());

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
    assert!(guard.is_cancelled());
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

/// What the handler observed around its `run_blocking` call. A handler
/// cannot judge this itself: supervision contains a handler panic, so an
/// in-handler assertion never reaches the test. The evidence is published
/// instead, and the test body is the only place that judges it.
struct BlockingObservation {
    entered_on: tokio::task::Id,
    resumed_on: tokio::task::Id,
    cancelled_while_blocking: bool,
}

struct BlockingActor {
    observed: Arc<AtomicUsize>,
    observation: Arc<Mutex<Option<BlockingObservation>>>,
}

impl Actor for BlockingActor {
    type Msg = BlockingMessage;
    type Args = (Arc<AtomicUsize>, Arc<Mutex<Option<BlockingObservation>>>);

    async fn init(
        (observed, observation): Self::Args,
        _: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        Ok(Self {
            observed,
            observation,
        })
    }

    async fn handle(
        &mut self,
        BlockingMessage::Run: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        let entered_on = tokio::task::id();
        let value = context.run_blocking(|token| (73usize, token.is_cancelled()));
        let (value, cancelled_while_blocking) = value.await;
        self.observed.store(value, Ordering::SeqCst);
        *self
            .observation
            .lock()
            .expect("blocking observation mutex poisoned") = Some(BlockingObservation {
            entered_on,
            resumed_on: tokio::task::id(),
            cancelled_while_blocking,
        });
        context.stop();
        Ok(())
    }
}

#[tokio::test]
async fn run_blocking_returns_on_the_actor_task() {
    let observed = Arc::new(AtomicUsize::new(0));
    let observation = Arc::new(Mutex::new(None));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "blocking",
            ActorOnceDef::<BlockingActor>::new((Arc::clone(&observed), Arc::clone(&observation))),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(BlockingMessage::Run).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(observed.load(Ordering::SeqCst), 73);
    let observation = observation
        .lock()
        .expect("blocking observation mutex poisoned")
        .take()
        .expect("the handler ran to completion");
    assert!(!observation.cancelled_while_blocking);
    assert_eq!(
        observation.resumed_on, observation.entered_on,
        "run_blocking resumes its handler on the actor task, not the blocking thread"
    );
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
    assert_eventually!(|| cancelled.load(Ordering::SeqCst)).await;
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

#[derive(Clone, Copy)]
enum CancellationDropMode {
    Cleanup,
    PolledCancellation,
    Actor,
}

struct PendingDropFuture {
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
    polled: Option<Arc<tokio::sync::Notify>>,
    dropped: Option<Arc<tokio::sync::Notify>>,
}

impl std::future::Future for PendingDropFuture {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Some(polled) = &self.polled {
            polled.notify_one();
        }
        std::task::Poll::Pending
    }
}

impl Drop for PendingDropFuture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(dropped) = &self.dropped {
            dropped.notify_one();
        }
        if self.panic_on_drop {
            panic!("offload cancellation destructor panic");
        }
    }
}

struct ExternallyCancelledOffloadActor;

impl Actor for ExternallyCancelledOffloadActor {
    type Msg = ();
    type Args = (
        tokio::sync::oneshot::Sender<Guard>,
        Arc<tokio::sync::Notify>,
        Arc<AtomicUsize>,
    );

    async fn init(
        (guard_sender, polled, drops): Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        let guard = context
            .offload_scoped(
                PendingDropFuture {
                    drops,
                    panic_on_drop: true,
                    polled: Some(polled),
                    dropped: None,
                },
                |_| (),
                Duration::MAX,
            )
            .expect("offload accepted");
        guard_sender
            .send(guard)
            .expect("test receives the cancellation guard");
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[tokio::test]
async fn cancellation_destructor_panic_wakes_an_otherwise_idle_actor() {
    let (guard_sender, guard_receiver) = tokio::sync::oneshot::channel();
    let polled = Arc::new(tokio::sync::Notify::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_actor_once(
        "idle-offload",
        ActorOnceDef::<ExternallyCancelledOffloadActor>::new((
            guard_sender,
            Arc::clone(&polled),
            Arc::clone(&drops),
        )),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("actor becomes idle");
    let guard = guard_receiver.await.expect("actor exports its guard");
    polled.notified().await;

    guard.cancel();

    let exit = tokio::time::timeout(POLL_TIMEOUT, async {
        loop {
            let item = events.recv().await.expect("lifecycle remains open");
            let LifecycleItem::Event(event) = item else {
                panic!("small fixture must not lag");
            };
            if let LifecycleEventKind::Exited { id, exit, .. } = event.kind
                && id.as_str() == "idle-offload"
            {
                break exit;
            }
        }
    })
    .await
    .expect("the retained panic wakes the idle actor");
    let ExitKind::Panicked { message } = exit.kind() else {
        panic!("expected destructor panic, got {:?}", exit.kind());
    };
    assert_eq!(
        message.as_deref(),
        Some("offload cancellation destructor panic")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed actor scope shuts down");
}

struct CancellationDropActor;

impl Actor for CancellationDropActor {
    type Msg = ();
    type Args = (CancellationDropMode, Arc<AtomicUsize>, Arc<AtomicBool>);

    async fn init(
        (mode, drops, signalled): Self::Args,
        context: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        let polled = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let first = context
            .offload_scoped(
                PendingDropFuture {
                    drops: Arc::clone(&drops),
                    panic_on_drop: true,
                    polled: Some(Arc::clone(&polled)),
                    dropped: Some(Arc::clone(&dropped)),
                },
                |_| (),
                Duration::MAX,
            )
            .expect("first offload accepted");
        let second = context
            .offload_scoped(
                PendingDropFuture {
                    drops,
                    panic_on_drop: false,
                    polled: None,
                    dropped: None,
                },
                |_| (),
                Duration::MAX,
            )
            .expect("second offload accepted");

        match mode {
            CancellationDropMode::Cleanup => {
                context.stop();
                signalled.store(
                    first.is_finished() && second.is_finished(),
                    Ordering::SeqCst,
                );
                first.detach();
                second.detach();
                Ok(Self)
            }
            CancellationDropMode::PolledCancellation => {
                polled.notified().await;
                first.cancel();
                dropped.notified().await;
                context.stop();
                signalled.store(second.is_finished(), Ordering::SeqCst);
                second.detach();
                Ok(Self)
            }
            CancellationDropMode::Actor => panic!("primary actor panic"),
        }
    }

    async fn handle(&mut self, (): Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
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

async fn assert_cancellation_drop_panic(mode: CancellationDropMode, expected: &str) {
    let drops = Arc::new(AtomicUsize::new(0));
    let signalled = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_actor_once(
        "cancellation-drop",
        ActorOnceDef::<CancellationDropActor>::new((
            mode,
            Arc::clone(&drops),
            Arc::clone(&signalled),
        ))
        .readiness(Readiness::Manual),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    let message = startup_panic_message(
        system
            .wait_started()
            .await
            .expect_err("the pre-ready panic fails startup"),
    );
    assert_eq!(message.as_deref(), Some(expected));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    if !matches!(mode, CancellationDropMode::Actor) {
        assert!(
            signalled.load(Ordering::SeqCst),
            "every cancellation signals completion despite one destructor panic"
        );
    }
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
}

#[tokio::test]
async fn cancellation_destructor_panics_do_not_poison_or_short_circuit_cleanup() {
    assert_cancellation_drop_panic(
        CancellationDropMode::Cleanup,
        "offload cancellation destructor panic",
    )
    .await;
}

#[tokio::test]
async fn polled_cancellation_destructor_panic_does_not_poison_offload_state() {
    assert_cancellation_drop_panic(
        CancellationDropMode::PolledCancellation,
        "offload cancellation destructor panic",
    )
    .await;
}

#[tokio::test]
async fn cancellation_destructor_panic_does_not_mask_actor_panic() {
    assert_cancellation_drop_panic(CancellationDropMode::Actor, "primary actor panic").await;
}

#[tokio::test]
async fn offload_future_and_continuation_panics_resume_on_actor_task() {
    assert_pre_ready_panic(PanicMode::Future, "offload future panic", false).await;
    assert_pre_ready_panic(PanicMode::Continuation, "offload continuation panic", false).await;
}

#[derive(Clone, Copy)]
enum QueuedPanicMode {
    Stop,
    HandlerError,
    HardAbort,
}

struct QueuedPanicActor {
    mode: QueuedPanicMode,
    queued: Arc<AtomicBool>,
}

impl Actor for QueuedPanicActor {
    type Msg = PanicMessage;
    type Args = (QueuedPanicMode, Arc<AtomicBool>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            mode: args.0,
            queued: args.1,
        })
    }

    async fn handle(&mut self, _: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let guard = context
            .offload_scoped(
                async {
                    panic!("owned offload panic");
                    #[allow(unreachable_code)]
                    ()
                },
                |_| PanicMessage::Delivery,
                Duration::MAX,
            )
            .expect("offload accepted");
        guard.finished().await;
        self.queued.store(true, Ordering::SeqCst);
        match self.mode {
            QueuedPanicMode::Stop => {
                context.stop();
                Ok(())
            }
            QueuedPanicMode::HandlerError => Err(ExitError::message("secondary handler error")),
            QueuedPanicMode::HardAbort => std::future::pending().await,
        }
    }
}

async fn assert_queued_panic_beats_orderly_exit(mode: QueuedPanicMode) {
    let queued = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "panic",
            ActorOnceDef::<QueuedPanicActor>::new((mode, Arc::clone(&queued)))
                .readiness(Readiness::Manual),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(PanicMessage::Trigger)
        .await
        .expect("mailbox accepts before readiness");
    let message = startup_panic_message(
        system
            .wait_started()
            .await
            .expect_err("queued offload panic fails startup"),
    );
    assert_eq!(message.as_deref(), Some("owned offload panic"));
    assert!(queued.load(Ordering::SeqCst));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
}

#[tokio::test]
async fn queued_offload_panic_survives_stop_and_handler_error() {
    assert_queued_panic_beats_orderly_exit(QueuedPanicMode::Stop).await;
    assert_queued_panic_beats_orderly_exit(QueuedPanicMode::HandlerError).await;
}

struct StopPathPanicObservations {
    on_stop_ran: AtomicBool,
    further_delivery_ran: AtomicBool,
    continuation_queued: AtomicBool,
    decorator_result: Mutex<Option<&'static str>>,
}

impl StopPathPanicObservations {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            on_stop_ran: AtomicBool::new(false),
            further_delivery_ran: AtomicBool::new(false),
            continuation_queued: AtomicBool::new(false),
            decorator_result: Mutex::new(None),
        })
    }
}

struct StopPathPanicActor {
    observations: Arc<StopPathPanicObservations>,
}

impl Actor for StopPathPanicActor {
    type Msg = PanicMessage;
    type Args = (Arc<StopPathPanicObservations>, ReleaseGate);

    async fn init(
        (observations, release): Self::Args,
        _: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        release.wait().await;
        Ok(Self { observations })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            PanicMessage::Trigger => {
                let guard = context
                    .offload_scoped(
                        async {
                            panic!("stop-path offload panic");
                        },
                        |_| PanicMessage::Delivery,
                        Duration::MAX,
                    )
                    .expect("offload accepted");
                guard.finished().await;
                context.stop();
            }
            PanicMessage::Delivery => {
                self.observations
                    .further_delivery_ran
                    .store(true, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {
        self.observations.on_stop_ran.store(true, Ordering::SeqCst);
    }
}

struct StopPathPanicDecorator<A: Actor> {
    inner: Handler<A>,
    observations: Arc<StopPathPanicObservations>,
}

impl<A: Actor> RawActor for StopPathPanicDecorator<A> {
    type Msg = A::Msg;

    fn readiness() -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        let result = self.inner.run(context).await;
        *self
            .observations
            .decorator_result
            .lock()
            .expect("decorator result mutex poisoned") =
            Some(if result.is_ok() { "ok" } else { "err" });
        result
    }
}

async fn assert_pending_offload_panic_stops_before_handler_teardown(
    mailbox_shutdown: MailboxShutdown,
) {
    let release_init = ReleaseGate::default();
    let observations = StopPathPanicObservations::new();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "stop-path-panic",
            RawOnceDef::new(StopPathPanicDecorator {
                inner: Handler::<StopPathPanicActor>::new((
                    Arc::clone(&observations),
                    release_init.clone(),
                )),
                observations: Arc::clone(&observations),
            })
            .mailbox_shutdown(mailbox_shutdown),
        )
        .expect("valid decorated actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(PanicMessage::Trigger)
        .await
        .expect("mailbox accepts before readiness");
    actor
        .send(PanicMessage::Delivery)
        .await
        .expect("frozen prefix message is accepted before readiness");
    release_init.release();

    let message = startup_panic_message(
        system
            .wait_started()
            .await
            .expect_err("the queued offload panic fails startup"),
    );
    assert_eq!(message.as_deref(), Some("stop-path offload panic"));
    assert!(
        !observations.on_stop_ran.load(Ordering::SeqCst),
        "on_stop must not run after an incarnation-owned panic"
    );
    assert!(
        !observations.further_delivery_ran.load(Ordering::SeqCst),
        "the frozen prefix must not drain after an incarnation-owned panic"
    );
    assert_eq!(
        *observations
            .decorator_result
            .lock()
            .expect("decorator result mutex poisoned"),
        None,
        "the offload panic must unwind through the Handler composition point"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
}

#[tokio::test]
async fn pending_offload_panic_stops_before_drain_on_stop_and_decorator_resume() {
    assert_pending_offload_panic_stops_before_handler_teardown(MailboxShutdown::Drain).await;
}

#[tokio::test]
async fn pending_offload_panic_resumes_from_local_recv_before_discard_teardown() {
    // Discard bypasses Handler's `try_recv` drain. This pins the local-stop
    // branch of `recv` independently of the separate draining check.
    assert_pending_offload_panic_stops_before_handler_teardown(MailboxShutdown::Discard).await;
}

struct ExternalStoppingRecvPanicActor {
    panic_queued: Arc<AtomicBool>,
}

impl RawActor for ExternalStoppingRecvPanicActor {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        let guard = context
            .offload_scoped(
                async {
                    panic!("external-stop recv offload panic");
                },
                |_| (),
                Duration::MAX,
            )
            .expect("offload accepted");
        guard.finished().await;
        self.panic_queued.store(true, Ordering::SeqCst);
        context.shutdown_token().cancelled().await;
        let _ = context.recv().await;
        unreachable!("externally stopped recv must resume the retained offload panic")
    }
}

#[tokio::test]
async fn externally_stopped_recv_resumes_a_pending_offload_panic() {
    let panic_queued = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_raw_once(
        "external-stop-recv-panic",
        RawOnceDef::new(ExternalStoppingRecvPanicActor {
            panic_queued: Arc::clone(&panic_queued),
        }),
    )
    .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("raw actor starts");
    assert_eventually!(
        || panic_queued.load(Ordering::SeqCst),
        "offload panic is queued before external shutdown"
    )
    .await;

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed actor shuts down");

    let mut panic_message = None;
    while let Some(item) = events.recv().await {
        let LifecycleItem::Event(event) = item else {
            panic!("small fixture must not lag");
        };
        if let LifecycleEventKind::Exited { id, exit, .. } = event.kind
            && id.as_str() == "external-stop-recv-panic"
            && let ExitKind::Panicked { message } = exit.kind()
        {
            panic_message = message.clone();
        }
    }
    assert_eq!(
        panic_message.as_deref(),
        Some("external-stop recv offload panic")
    );
}

struct StoppingTryRecvPanicActor {
    enter: ReleaseGate,
}

impl RawActor for StoppingTryRecvPanicActor {
    type Msg = ();

    fn readiness() -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.enter.wait().await;
        let guard = context
            .offload_scoped(
                async {
                    panic!("stopping try_recv offload panic");
                },
                |_| (),
                Duration::MAX,
            )
            .expect("offload accepted");
        guard.finished().await;
        context.stop();
        let _ = context.try_recv();
        unreachable!("stopping try_recv must resume the retained offload panic")
    }
}

#[tokio::test]
async fn stopping_try_recv_resumes_a_pending_offload_panic_before_drain() {
    let enter = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "stopping-try-recv-panic",
            RawOnceDef::new(StoppingTryRecvPanicActor {
                enter: enter.clone(),
            }),
        )
        .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(())
        .await
        .expect("frozen prefix message is accepted before readiness");
    enter.release();

    let message = startup_panic_message(
        system
            .wait_started()
            .await
            .expect_err("the queued offload panic fails startup"),
    );
    assert_eq!(message.as_deref(), Some("stopping try_recv offload panic"));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
}

const DISCARDED_CONTINUATION_PANIC: &str = "discarded continuation destructor panic";

enum DiscardedContinuationMessage {
    Trigger,
    Continuation(PanicOnDrop),
}

/// Queues a continuation whose destructor panics, then stops. The stop's
/// freeze discards the continuation into the incarnation's shared disposal
/// slot, which the following receive boundary resumes: SPEC §5.2's exclusion
/// covers any incarnation-owned disposal panic, not offload work alone.
struct DiscardedContinuationPanicActor {
    observations: Arc<StopPathPanicObservations>,
}

impl Actor for DiscardedContinuationPanicActor {
    type Msg = DiscardedContinuationMessage;
    type Args = Arc<StopPathPanicObservations>;

    async fn init(observations: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { observations })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            DiscardedContinuationMessage::Trigger => {
                let queued = context.continue_with(DiscardedContinuationMessage::Continuation(
                    PanicOnDrop::new(DISCARDED_CONTINUATION_PANIC),
                ));
                self.observations
                    .continuation_queued
                    .store(queued.is_ok(), Ordering::SeqCst);
                // A rejection would hand the panicking payload back here; leak
                // it rather than unwinding inside the handler, and let the test
                // body judge the published flag.
                if let Err(rejected) = queued {
                    std::mem::forget(rejected);
                }
                context.stop();
            }
            DiscardedContinuationMessage::Continuation(payload) => {
                self.observations
                    .further_delivery_ran
                    .store(true, Ordering::SeqCst);
                // Unreachable: the freeze inside `stop` discards queued
                // continuations. Leak the payload so a regression reports
                // through the flag instead of a second panic from in here.
                std::mem::forget(payload);
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {
        self.observations.on_stop_ran.store(true, Ordering::SeqCst);
    }
}

async fn assert_discarded_continuation_panic_stops_before_handler_teardown(
    mailbox_shutdown: MailboxShutdown,
) {
    let observations = StopPathPanicObservations::new();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "discarded-continuation-panic",
            RawOnceDef::new(StopPathPanicDecorator {
                inner: Handler::<DiscardedContinuationPanicActor>::new(Arc::clone(&observations)),
                observations: Arc::clone(&observations),
            })
            .mailbox_shutdown(mailbox_shutdown),
        )
        .expect("valid decorated actor");
    let system = tree.spawn().expect("runtime is available");
    actor
        .send(DiscardedContinuationMessage::Trigger)
        .await
        .expect("mailbox accepts before readiness");

    let message = startup_panic_message(
        system
            .wait_started()
            .await
            .expect_err("the discarded continuation destructor panic fails startup"),
    );
    assert_eq!(message.as_deref(), Some(DISCARDED_CONTINUATION_PANIC));
    assert!(
        observations.continuation_queued.load(Ordering::SeqCst),
        "the continuation must be queued before the stop freezes it"
    );
    assert!(
        !observations.on_stop_ran.load(Ordering::SeqCst),
        "on_stop must not run after an incarnation-owned disposal panic"
    );
    assert!(
        !observations.further_delivery_ran.load(Ordering::SeqCst),
        "a discarded continuation must never be delivered"
    );
    assert_eq!(
        *observations
            .decorator_result
            .lock()
            .expect("decorator result mutex poisoned"),
        None,
        "the disposal panic must unwind through the Handler composition point"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root shuts down");
}

#[tokio::test]
async fn discarded_continuation_destructor_panic_resumes_before_drain_teardown() {
    assert_discarded_continuation_panic_stops_before_handler_teardown(MailboxShutdown::Drain).await;
}

#[tokio::test]
async fn discarded_continuation_destructor_panic_resumes_before_discard_teardown() {
    // Discard skips Handler's `try_recv` drain, so only `recv`'s local-stop
    // branch can carry the freeze-time destructor panic out of the loop.
    assert_discarded_continuation_panic_stops_before_handler_teardown(MailboxShutdown::Discard)
        .await;
}

#[tokio::test]
async fn queued_offload_panic_survives_hard_abort() {
    let queued = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "panic",
            ActorOnceDef::<QueuedPanicActor>::new((
                QueuedPanicMode::HardAbort,
                Arc::clone(&queued),
            ))
            .readiness(Readiness::Manual)
            .shutdown(Shutdown::Abort),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    actor
        .send(PanicMessage::Trigger)
        .await
        .expect("mailbox accepts before readiness");
    assert_eventually!(
        || queued.load(Ordering::SeqCst),
        "offload panic is queued before hard abort"
    )
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("hard abort bounds shutdown");

    let mut panic_message = None;
    while let Some(item) = events.recv().await {
        let LifecycleItem::Event(event) = item else {
            panic!("small fixture must not lag");
        };
        if let LifecycleEventKind::Exited { id, exit, .. } = event.kind
            && id.as_str() == "panic"
            && let ExitKind::Panicked { message } = exit.kind()
        {
            panic_message = message.clone();
        }
    }
    assert_eq!(panic_message.as_deref(), Some("owned offload panic"));
}

struct HandlerOffloadDoublePanicActor {
    queued: Arc<AtomicBool>,
}

impl Actor for HandlerOffloadDoublePanicActor {
    type Msg = ();
    type Args = Arc<AtomicBool>;

    async fn init(queued: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { queued })
    }

    async fn handle(&mut self, (): Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        let guard = context
            .offload_scoped(
                async {
                    panic!("handler owned offload panic");
                },
                |_| (),
                Duration::MAX,
            )
            .expect("offload accepted");
        guard.finished().await;
        self.queued.store(true, Ordering::SeqCst);
        std::future::pending().await
    }
}

impl Drop for HandlerOffloadDoublePanicActor {
    fn drop(&mut self) {
        panic!("handler destructor panic");
    }
}

#[tokio::test]
async fn hard_abort_preserves_owned_offload_panic_over_handler_destructor() {
    let queued = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "handler-double-panic",
            ActorOnceDef::<HandlerOffloadDoublePanicActor>::new(Arc::clone(&queued))
                .shutdown(Shutdown::Abort),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("actor starts");
    actor.send(()).await.expect("actor accepts trigger");
    assert_eventually!(
        || queued.load(Ordering::SeqCst),
        "offload panic is owned before hard abort"
    )
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("the two panics remain contained");

    let mut panic_message = None;
    while let Some(item) = events.recv().await {
        let LifecycleItem::Event(event) = item else {
            panic!("small fixture must not lag");
        };
        if let LifecycleEventKind::Exited { id, exit, .. } = event.kind
            && id.as_str() == "handler-double-panic"
            && let ExitKind::Panicked { message } = exit.kind()
        {
            panic_message = message.clone();
        }
    }
    assert_eq!(
        panic_message.as_deref(),
        Some("handler owned offload panic")
    );
}

#[tokio::test]
async fn run_blocking_panic_resumes_where_awaited() {
    assert_pre_ready_panic(PanicMode::Blocking, "blocking panic", true).await;
}

#[tokio::test]
async fn primary_callback_panic_survives_a_panicking_actor_destructor() {
    assert_pre_ready_panic(PanicMode::HandlerAndDrop, "primary handler panic", true).await;
}

enum CrashTeardownMessage {
    Start,
}

struct CrashTeardownActor {
    _drop: DropLog,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for CrashTeardownActor {
    type Msg = CrashTeardownMessage;
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
        CrashTeardownMessage::Start: Self::Msg,
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
                |_| CrashTeardownMessage::Start,
                Duration::MAX,
            )
            .expect("offload accepted");
        panic!("injected handle panic");
    }
}

/// §5.5's teardown order holds on the crash path too: a `handle` panic joins
/// and destroys in-flight offload work before actor state is dropped.
#[tokio::test]
async fn incarnation_offloads_are_destroyed_before_actor_state_on_panic() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "crash-teardown",
            ActorOnceDef::<CrashTeardownActor>::new(Arc::clone(&log)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(CrashTeardownMessage::Start)
        .await
        .expect("actor live");
    assert_eventually!(|| log.lock().expect("drop log mutex poisoned").len() == 2).await;
    assert_eq!(
        *log.lock().expect("drop log mutex poisoned"),
        ["offload", "actor"]
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

enum FailTeardownMessage {
    Start,
}

struct FailTeardownActor {
    _drop: DropLog,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for FailTeardownActor {
    type Msg = FailTeardownMessage;
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
        FailTeardownMessage::Start: Self::Msg,
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
                |_| FailTeardownMessage::Start,
                Duration::MAX,
            )
            .expect("offload accepted");
        Err(ExitError::message("injected handler failure"))
    }
}

const FLOOD: usize = 256;

enum FloodMessage {
    Start,
    Completed(usize),
}

struct FloodActor {
    delivered: usize,
    completed_work: Arc<AtomicUsize>,
    delivered_indices: Arc<Mutex<Vec<usize>>>,
    release: ReleaseGate,
}

impl Actor for FloodActor {
    type Msg = FloodMessage;
    type Args = (Arc<AtomicUsize>, Arc<Mutex<Vec<usize>>>, ReleaseGate);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            delivered: 0,
            completed_work: args.0,
            delivered_indices: args.1,
            release: args.2,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            FloodMessage::Start => {
                for index in 0..FLOOD {
                    let completed_work = Arc::clone(&self.completed_work);
                    context
                        .offload(
                            async move {
                                completed_work.fetch_add(1, Ordering::SeqCst);
                                index
                            },
                            move |result| {
                                FloodMessage::Completed(
                                    result.expect("flooded offload completes within its budget"),
                                )
                            },
                            Duration::MAX,
                        )
                        .expect("live offload accepted");
                }
                // Hold the loop here so every completion queues before any
                // is consumed: completion storage is 1:1 with the offloads
                // this incarnation started, never dropped or conflated.
                self.release.wait().await;
                Ok(())
            }
            FloodMessage::Completed(index) => {
                self.delivered_indices
                    .lock()
                    .expect("delivery evidence mutex remains healthy")
                    .push(index);
                self.delivered += 1;
                if self.delivered == FLOOD {
                    context.stop();
                }
                Ok(())
            }
        }
    }
}

/// A completion flood: every offload finishes while the loop is held, so all
/// completions queue in incarnation-internal storage before one is consumed,
/// and every one of them is still delivered exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offload_completion_flood_is_absorbed_and_fully_delivered() {
    let completed_work = Arc::new(AtomicUsize::new(0));
    let delivered_indices = Arc::new(Mutex::new(Vec::new()));
    let release = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "flood",
            ActorOnceDef::<FloodActor>::new((
                Arc::clone(&completed_work),
                Arc::clone(&delivered_indices),
                release.clone(),
            )),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(FloodMessage::Start).await.expect("actor live");
    assert_eventually!(|| completed_work.load(Ordering::SeqCst) == FLOOD).await;
    release.release();
    assert_eq!(
        tokio::time::timeout(POLL_TIMEOUT, system.wait())
            .await
            .expect("every flooded completion is delivered"),
        shelterwood::StopReason::Finished
    );
    let mut delivered = delivered_indices
        .lock()
        .expect("delivery evidence mutex remains healthy")
        .clone();
    delivered.sort_unstable();
    assert_eq!(
        delivered,
        (0..FLOOD).collect::<Vec<_>>(),
        "every flooded completion identity is delivered exactly once"
    );
}

const CYCLES: usize = 64;

enum CycleMessage {
    Start,
    Completed(usize),
}

struct CycleActor {
    next: usize,
    delivered: Arc<Mutex<Vec<usize>>>,
}

impl CycleActor {
    fn offload_next(&self, context: &mut Context<'_, Self>) {
        let value = self.next;
        context
            .offload(
                async move { value },
                move |result| CycleMessage::Completed(result.expect("cycle offload completes")),
                Duration::MAX,
            )
            .expect("live offload accepted");
    }
}

impl Actor for CycleActor {
    type Msg = CycleMessage;
    type Args = Arc<Mutex<Vec<usize>>>;

    async fn init(delivered: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { next: 0, delivered })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            CycleMessage::Start => self.offload_next(context),
            CycleMessage::Completed(value) => {
                self.delivered
                    .lock()
                    .expect("cycle evidence mutex remains healthy")
                    .push(value);
                self.next += 1;
                if self.next == CYCLES {
                    context.stop();
                } else {
                    self.offload_next(context);
                }
            }
        }
        Ok(())
    }
}

/// Steady-state offload churn: the loop goes idle between completions, so
/// finished-offload bookkeeping is reclaimed continuously rather than only
/// when the next offload starts or the incarnation tears down, and every
/// cycle's completion is delivered.
#[tokio::test]
async fn sequential_offload_cycles_deliver_every_completion() {
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "cycles",
            ActorOnceDef::<CycleActor>::new(Arc::clone(&delivered)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(CycleMessage::Start).await.expect("actor live");
    assert_eq!(
        tokio::time::timeout(POLL_TIMEOUT, system.wait())
            .await
            .expect("every sequential completion is delivered"),
        shelterwood::StopReason::Finished
    );
    assert_eq!(
        *delivered
            .lock()
            .expect("cycle evidence mutex remains healthy"),
        (0..CYCLES).collect::<Vec<_>>(),
        "steady-state cycles deliver every completion exactly once and in order"
    );
}

const SATURATE: usize = 64;

enum SaturateMessage {
    Ping,
    Completed,
}

struct SaturatedActor {
    issued: usize,
    delivered: usize,
    peak_backlog: Arc<AtomicUsize>,
    timed_out: Arc<AtomicUsize>,
}

impl Actor for SaturatedActor {
    type Msg = SaturateMessage;
    type Args = (ReleaseGate, Arc<AtomicUsize>, Arc<AtomicUsize>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.wait().await;
        Ok(Self {
            issued: 0,
            delivered: 0,
            peak_backlog: args.1,
            timed_out: args.2,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            SaturateMessage::Ping => {
                self.peak_backlog
                    .fetch_max(self.issued - self.delivered, Ordering::SeqCst);
                self.issued += 1;
                let timed_out = Arc::clone(&self.timed_out);
                context
                    .offload(
                        async {},
                        move |result| {
                            if result == Err(DeadlineElapsed) {
                                timed_out.fetch_add(1, Ordering::SeqCst);
                            }
                            SaturateMessage::Completed
                        },
                        Duration::ZERO,
                    )
                    .expect("live offload accepted");
            }
            SaturateMessage::Completed => {
                self.delivered += 1;
                if self.delivered == SATURATE {
                    context.stop();
                }
            }
        }
        Ok(())
    }
}

/// A continuously nonempty mailbox cannot starve queued offload completions:
/// bounded arbitration turns interleave the captured completion prefix with
/// mailbox delivery, so the completion backlog tracks the actor's own
/// in-flight issuance instead of growing with mailbox history. Only that
/// relation is asserted — a backlog reaching the whole queued history means
/// completions were starved until the mailbox drained, while the exact
/// transient width and the cross-source interleaving are unspecified.
#[tokio::test]
async fn saturated_mailbox_does_not_grow_the_completion_backlog() {
    let gate = ReleaseGate::default();
    let peak_backlog = Arc::new(AtomicUsize::new(0));
    let timed_out = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "saturated",
            ActorOnceDef::<SaturatedActor>::new((
                gate.clone(),
                Arc::clone(&peak_backlog),
                Arc::clone(&timed_out),
            ))
            .mailbox(Mailbox::queue(SATURATE).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    for _ in 0..SATURATE {
        actor
            .send(SaturateMessage::Ping)
            .await
            .expect("mailbox accepts during init");
    }
    gate.release();
    system.wait_started().await.expect("actor starts");
    // Termination is itself the liveness half: the actor stops only once all
    // SATURATE completions land.
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(
        peak_backlog.load(Ordering::SeqCst) < SATURATE - 1,
        "the completion backlog must not reach the queued mailbox history"
    );
    assert_eq!(
        timed_out.load(Ordering::SeqCst),
        SATURATE,
        "every zero-budget offload is classified as deadline elapsed"
    );
}

/// §5.5's teardown order holds on the error path too: a handler `Err` joins
/// and destroys in-flight offload work before actor state is dropped.
#[tokio::test]
async fn incarnation_offloads_are_destroyed_before_actor_state_on_error() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "fail-teardown",
            ActorOnceDef::<FailTeardownActor>::new(Arc::clone(&log)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(FailTeardownMessage::Start)
        .await
        .expect("actor live");
    assert_eventually!(|| log.lock().expect("drop log mutex poisoned").len() == 2).await;
    assert_eq!(
        *log.lock().expect("drop log mutex poisoned"),
        ["offload", "actor"]
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}
