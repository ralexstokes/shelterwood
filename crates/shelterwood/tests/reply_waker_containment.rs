use std::{
    future::Future,
    process::Command,
    sync::Arc,
    task::{Context as TaskContext, Poll, Wake, Waker},
    time::Duration,
};

use shelterwood::{
    Actor, ActorOnceDef, Context, ExitError, ExitKind, ExitResult, LifecycleEventKind,
    LifecycleItem, Reply, StopReason, Tree,
};

const CHILD_ENV: &str = "SHELTERWOOD_HOSTILE_REPLY_WAKER_CHILD";
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

async fn run_hostile_reply_waker_child() {
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

    let exit = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await {
                Some(LifecycleItem::Event(event)) => {
                    if let LifecycleEventKind::Exited { id, exit, .. } = event.kind
                        && id.as_str() == "panicking-reply-owner"
                    {
                        return exit;
                    }
                }
                Some(LifecycleItem::Lagged { dropped }) => {
                    panic!("unexpected lifecycle lag marker dropping {dropped}");
                }
                None => panic!("lifecycle stream closed before the actor exit"),
            }
        }
    })
    .await
    .expect("the actor publishes its contained exit");
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

#[test]
fn unanswered_reply_drop_during_handler_panic_contains_hostile_waker() {
    if std::env::var_os(CHILD_ENV).is_some() {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(run_hostile_reply_waker_child());
        return;
    }

    let output = Command::new(std::env::current_exe().expect("integration-test executable"))
        .arg("--exact")
        .arg("unanswered_reply_drop_during_handler_panic_contains_hostile_waker")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_ENV, "1")
        .output()
        .expect("hostile-waker subprocess starts");

    assert!(
        output.status.success(),
        "hostile-waker subprocess must survive and publish the actor exit\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
