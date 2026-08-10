use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{POLL_TIMEOUT, ReleaseGate, assert_quiet, poll_until};
use shelterwood::{
    Actor, ActorDef, ActorOnceDef, Context, DeadlineElapsed, ExitError, ExitKind, ExitResult,
    Guard, LifecycleEventKind, LifecycleItem, Mailbox, Readiness, Shutdown, StartupError,
    StartupFailureCause, Tree,
};

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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            generations.load(Ordering::SeqCst) >= 2
        })
        .await,
        "replacement starts while the old blocking thread remains detached"
    );

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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            armed.load(Ordering::SeqCst) && offload_started.load(Ordering::SeqCst)
        })
        .await,
        "the offload is polled and its deadline is registered before time moves"
    );
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            guard_dropped.load(Ordering::SeqCst)
        })
        .await,
        "the actor drops the guard before the negative window begins"
    );
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
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
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

    let exit = tokio::time::timeout(Duration::from_secs(1), async {
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            queued.load(Ordering::SeqCst)
        })
        .await,
        "offload panic is queued before hard abort"
    );
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            queued.load(Ordering::SeqCst)
        })
        .await,
        "offload panic is owned before hard abort"
    );
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            log.lock().expect("drop log mutex poisoned").len() == 2
        })
        .await
    );
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
    seen: Vec<bool>,
    delivered: usize,
    completed_work: Arc<AtomicUsize>,
    release: ReleaseGate,
}

impl Actor for FloodActor {
    type Msg = FloodMessage;
    type Args = (Arc<AtomicUsize>, ReleaseGate);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            seen: vec![false; FLOOD],
            delivered: 0,
            completed_work: args.0,
            release: args.1,
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
                assert!(
                    !self.seen[index],
                    "each flooded completion is delivered exactly once"
                );
                self.seen[index] = true;
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
    let release = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "flood",
            ActorOnceDef::<FloodActor>::new((Arc::clone(&completed_work), release.clone())),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(FloodMessage::Start).await.expect("actor live");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            completed_work.load(Ordering::SeqCst) == FLOOD
        })
        .await
    );
    release.release();
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

const CYCLES: usize = 64;

enum CycleMessage {
    Start,
    Completed(usize),
}

struct CycleActor {
    next: usize,
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
    type Args = ();

    async fn init((): Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { next: 0 })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            CycleMessage::Start => self.offload_next(context),
            CycleMessage::Completed(value) => {
                assert_eq!(value, self.next, "steady-state cycles deliver in order");
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
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once("cycles", ActorOnceDef::<CycleActor>::new(()))
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.send(CycleMessage::Start).await.expect("actor live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
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
}

impl Actor for SaturatedActor {
    type Msg = SaturateMessage;
    type Args = (ReleaseGate, Arc<AtomicUsize>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.wait().await;
        Ok(Self {
            issued: 0,
            delivered: 0,
            peak_backlog: args.1,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            SaturateMessage::Ping => {
                self.peak_backlog
                    .fetch_max(self.issued - self.delivered, Ordering::SeqCst);
                self.issued += 1;
                context
                    .offload(
                        async {},
                        |result| {
                            assert_eq!(result, Err(DeadlineElapsed));
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
/// every bounded arbitration turn admits at most one mailbox delivery before
/// its captured completion prefix, so the completion backlog stays
/// proportional to the actor's own in-flight issuance instead of growing with
/// mailbox history.
#[tokio::test]
async fn saturated_mailbox_does_not_grow_the_completion_backlog() {
    let gate = ReleaseGate::default();
    let peak_backlog = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "saturated",
            ActorOnceDef::<SaturatedActor>::new((gate.clone(), Arc::clone(&peak_backlog)))
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
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert!(
        peak_backlog.load(Ordering::SeqCst) <= 2,
        "bounded arbitration turns drain completions instead of accumulating \
         them while the mailbox stays nonempty"
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
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            log.lock().expect("drop log mutex poisoned").len() == 2
        })
        .await
    );
    assert_eq!(
        *log.lock().expect("drop log mutex poisoned"),
        ["offload", "actor"]
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}
