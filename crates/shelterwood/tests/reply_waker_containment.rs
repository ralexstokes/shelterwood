mod common;

use std::{
    future::Future,
    sync::Arc,
    task::{Context as TaskContext, Poll, Wake, Waker},
    time::Duration,
};

use crate::common::next_exit_of;
use shelterwood::{
    Actor, ActorOnceDef, Context, ExitError, ExitKind, ExitResult, Reply, StopReason, Tree,
};

const HANDLER_PANIC: &str = "injected reply-owning handler panic";

enum Message {
    Crash(Reply<u8>),
}

struct PanickingActor;

impl Actor for PanickingActor {
    type Msg = Message;
    type Args = ();

    async fn init((): (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, message: Message, _: &mut Context<'_, Self>) -> ExitResult {
        let Message::Crash(_unanswered) = message;
        panic!("{HANDLER_PANIC}");
    }
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("injected reply receiver waker panic");
    }

    fn wake_by_ref(self: &Arc<Self>) {
        panic!("injected reply receiver waker panic");
    }
}

/// Drops an unanswered [`Reply`] while its owning handler unwinds, with a
/// hostile waker registered on the matching receiver.
///
/// This runs in process rather than behind a subprocess harness. The failure
/// this pins is an uncontained hostile wake: either the waker panic escapes
/// onto the unwind path and double-panics into `abort`, or it replaces the
/// handler panic that the actor must publish. Every test binary already runs
/// under nextest's process-per-test isolation, so an abort surfaces as a
/// signal on this test alone and *is* the failure signal — the harness that
/// used to re-implement that isolation could only ever fail open, since
/// libtest exits zero when its `--exact` filter matches nothing.
#[tokio::test(flavor = "multi_thread")]
async fn unanswered_reply_drop_during_handler_panic_contains_hostile_waker() {
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "panicking-reply-owner",
            ActorOnceDef::<PanickingActor>::new(()),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    let mut events = system.scope().subscribe_lifecycle();
    system.wait_started().await.expect("actor starts");

    let (reply, receiver) = actor.reply_channel::<u8>();
    let hostile = Waker::from(Arc::new(PanicWake));
    let mut receive = Box::pin(receiver.recv(Duration::from_secs(30)));
    assert!(matches!(
        receive
            .as_mut()
            .poll(&mut TaskContext::from_waker(&hostile)),
        Poll::Pending
    ));

    actor
        .send(Message::Crash(reply))
        .await
        .expect("crashing message is accepted");

    let exit = next_exit_of(&mut events, "panicking-reply-owner").await;
    assert!(matches!(
        exit.kind(),
        ExitKind::Panicked { message: Some(message) } if message == HANDLER_PANIC
    ));
    assert_eq!(system.wait().await, StopReason::Finished);

    // Keep the receiver future and its registered hostile waker alive across
    // sender destruction and authoritative exit publication. If it were
    // released sooner, Tokio would observe a closed receiver and skip the
    // hostile wake that this regression must exercise.
    drop(receive);
}
