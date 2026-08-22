mod common;

use std::{error::Error, fmt, sync::mpsc, thread::ThreadId, time::Duration};

use crate::common::next_exit_of;
use shelterwood::{
    Actor, ActorOnceDef, Context, ExitError, ExitKind, ExitResult, RawActor, RawContext,
    RawOnceDef, StopReason, Tree,
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
