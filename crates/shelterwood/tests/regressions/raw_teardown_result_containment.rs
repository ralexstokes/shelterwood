use std::{error::Error, fmt, sync::mpsc, thread::ThreadId, time::Duration};

use crate::common::{SHUTDOWN_BUDGET, next_exit_of};
use shelterwood::{
    Actor, ActorOnceDef, Context, ExitError, ExitKind, ExitResult, Guard, Handler, RawActor,
    RawContext, RawOnceDef, Shutdown, StopReason, Tree,
};

const ACTOR_DROP_PANIC: &str = "injected actor-state destructor panic";
const PROBE_WAIT: Duration = Duration::from_secs(10);

type ThreadProbe = mpsc::SyncSender<ThreadId>;

#[derive(Debug)]
struct HostileError(ThreadProbe);

impl fmt::Display for HostileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("hostile application error")
    }
}

impl Error for HostileError {}

impl Drop for HostileError {
    fn drop(&mut self) {
        let _ = self.0.send(std::thread::current().id());
        panic!("injected application-error destructor panic");
    }
}

/// Asserts that the epilogue moved the application error off the very thread
/// that ran teardown, rather than off whichever thread happens to host the
/// test. The two coincide only under the default current-thread flavour, so
/// comparing against the teardown thread keeps the probe honest if the actor
/// is ever scheduled elsewhere.
fn assert_disposed_off_the_teardown_thread(
    teardown: &mpsc::Receiver<ThreadId>,
    disposal: &mpsc::Receiver<ThreadId>,
) {
    let teardown_thread = teardown
        .recv_timeout(PROBE_WAIT)
        .expect("the actor's destructor runs during teardown");
    let disposal_thread = disposal
        .recv_timeout(PROBE_WAIT)
        .expect("the hostile error is eventually disposed");
    assert_ne!(
        disposal_thread, teardown_thread,
        "the teardown unwind must not destroy the application error inline"
    );
}

/// Returns an application error, then panics from actor destruction while the
/// raw incarnation epilogue still owns that result.
struct RawReturnsHostileErrorThenPanicsOnDrop {
    error: Option<HostileError>,
    teardown: ThreadProbe,
}

impl RawActor for RawReturnsHostileErrorThenPanicsOnDrop {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        let _ = context.recv().await;
        Err(ExitError::from(
            self.error.take().expect("hostile error is returned once"),
        ))
    }
}

impl Drop for RawReturnsHostileErrorThenPanicsOnDrop {
    fn drop(&mut self) {
        let _ = self.teardown.send(std::thread::current().id());
        panic!("{ACTOR_DROP_PANIC}");
    }
}

#[tokio::test]
async fn raw_error_is_retained_while_the_teardown_panic_unwinds() {
    let (dropped, observed) = mpsc::sync_channel(1);
    let (torn_down, teardown) = mpsc::sync_channel(1);
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "hostile-raw",
            RawOnceDef::new(RawReturnsHostileErrorThenPanicsOnDrop {
                error: Some(HostileError(dropped)),
                teardown: torn_down,
            }),
        )
        .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("raw actor starts");

    actor.send(()).await.expect("trigger is accepted");

    let exit = next_exit_of(&mut events, "hostile-raw").await;
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message } if message.as_deref() == Some(ACTOR_DROP_PANIC)
    ));
    assert_eq!(system.wait().await, StopReason::Finished);
    assert_disposed_off_the_teardown_thread(&teardown, &observed);
}

/// The supported callback-oriented actor surface reaches the same raw
/// epilogue through `Handler<A>`.
struct HandlerReturnsHostileError {
    error: Option<HostileError>,
    teardown: ThreadProbe,
}

impl Actor for HandlerReturnsHostileError {
    type Msg = ();
    type Args = (HostileError, ThreadProbe);

    async fn init(
        (error, teardown): Self::Args,
        _: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        Ok(Self {
            error: Some(error),
            teardown,
        })
    }

    async fn handle(&mut self, (): Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Err(ExitError::from(
            self.error.take().expect("hostile error is returned once"),
        ))
    }
}

impl Drop for HandlerReturnsHostileError {
    fn drop(&mut self) {
        let _ = self.teardown.send(std::thread::current().id());
        panic!("{ACTOR_DROP_PANIC}");
    }
}

#[tokio::test]
async fn handler_error_is_retained_while_the_teardown_panic_unwinds() {
    let (dropped, observed) = mpsc::sync_channel(1);
    let (torn_down, teardown) = mpsc::sync_channel(1);
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "hostile-handler",
            ActorOnceDef::<HandlerReturnsHostileError>::new((HostileError(dropped), torn_down)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("handler actor starts");

    actor.send(()).await.expect("trigger is accepted");

    let exit = next_exit_of(&mut events, "hostile-handler").await;
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message } if message.as_deref() == Some(ACTOR_DROP_PANIC)
    ));
    assert_eq!(system.wait().await, StopReason::Finished);
    assert_disposed_off_the_teardown_thread(&teardown, &observed);
}

struct ErrorDuringCleanup<const INIT: bool> {
    release: Option<mpsc::Receiver<()>>,
    guard: Option<tokio::sync::oneshot::Sender<Guard>>,
    error_dropped: ThreadProbe,
}

impl<const INIT: bool> ErrorDuringCleanup<INIT> {
    async fn fail(&mut self, context: &mut Context<'_, Self>) -> ExitError {
        let release = self.release.take().expect("offload starts once");
        let (started, running) = tokio::sync::oneshot::channel();
        let guard = context
            .offload_scoped(
                async move {
                    // Notify from outside the executor: waking the actor from
                    // this worker could put it in Tokio's unstealable LIFO slot
                    // immediately before this deliberately blocked poll.
                    std::thread::spawn(move || started.send(()).expect("actor awaits start"))
                        .join()
                        .expect("start notifier returns");
                    release
                        .recv_timeout(PROBE_WAIT)
                        .expect("test releases the blocked offload");
                },
                |_| (),
                Duration::MAX,
            )
            .expect("live callback accepts offload");
        running
            .await
            .expect("offload is polling before callback fails");
        self.guard
            .take()
            .expect("guard is exported once")
            .send(guard)
            .expect("test receives completion guard");
        ExitError::from(HostileError(self.error_dropped.clone()))
    }
}

impl<const INIT: bool> Actor for ErrorDuringCleanup<INIT> {
    type Msg = ();
    type Args = Self;

    async fn init(mut args: Self, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        if INIT {
            Err(args.fail(context).await)
        } else {
            Ok(args)
        }
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        Err(self.fail(context).await)
    }
}

/// The root raw value survives even an initializer failure, so its destructor
/// witnesses the cancellation thread in both callback paths.
struct CleanupProbe<const INIT: bool> {
    handler: Handler<ErrorDuringCleanup<INIT>>,
    teardown: ThreadProbe,
}

impl<const INIT: bool> RawActor for CleanupProbe<INIT> {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<()>) -> ExitResult {
        self.handler.run(context).await
    }
}

impl<const INIT: bool> Drop for CleanupProbe<INIT> {
    fn drop(&mut self) {
        let _ = self.teardown.send(std::thread::current().id());
    }
}

async fn assert_callback_error_is_retained_during_cancelled_cleanup<const INIT: bool>() {
    let (release, blocked) = mpsc::channel();
    let (guard, completion) = tokio::sync::oneshot::channel();
    let (dropped, disposal) = mpsc::sync_channel(1);
    let (torn_down, teardown) = mpsc::sync_channel(1);
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "cleanup",
            RawOnceDef::new(CleanupProbe {
                handler: Handler::new(ErrorDuringCleanup::<INIT> {
                    release: Some(blocked),
                    guard: Some(guard),
                    error_dropped: dropped,
                }),
                teardown: torn_down,
            })
            .shutdown(Shutdown::Abort),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    if !INIT {
        actor.send(()).await.expect("handler trigger is accepted");
    }
    let guard = tokio::time::timeout(PROBE_WAIT, completion)
        .await
        .expect("callback starts offload")
        .expect("callback exports guard");
    tokio::time::timeout(PROBE_WAIT, guard.finished())
        .await
        .expect("error cleanup freezes resources");
    // Work is still inside its poll. Finished proves that error cleanup has
    // requested cancellation; its join cannot finish until we release work.
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("hard abort completes despite the pending resource join");
    release
        .send(())
        .expect("offload remains blocked until release");
    assert_disposed_off_the_teardown_thread(&teardown, &disposal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initializer_error_is_retained_when_cleanup_is_hard_aborted() {
    assert_callback_error_is_retained_during_cancelled_cleanup::<true>().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handler_error_is_retained_when_cleanup_is_hard_aborted() {
    assert_callback_error_is_retained_during_cancelled_cleanup::<false>().await;
}
