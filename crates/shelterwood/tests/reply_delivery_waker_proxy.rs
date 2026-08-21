mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    sync::Arc,
    task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

use common::{POLL_TIMEOUT, ReleaseGate};
use shelterwood::{Actor, ActorOnceDef, Context, ExitError, ExitResult, Reply, Tree};

unsafe fn clone_panicking_drop_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let state = ManuallyDrop::new(unsafe { Arc::<()>::from_raw(data.cast()) });
    RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &PANICKING_DROP_WAKER_VTABLE,
    )
}

unsafe fn wake_panicking_drop_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<()>::from_raw(data.cast()) });
}

unsafe fn wake_by_ref_panicking_drop_waker(_data: *const ()) {}

unsafe fn drop_panicking_drop_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<()>::from_raw(data.cast()) });
    panic!("injected reply caller-waker drop panic");
}

static PANICKING_DROP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_panicking_drop_waker,
    wake_panicking_drop_waker,
    wake_by_ref_panicking_drop_waker,
    drop_panicking_drop_waker,
);

fn panicking_drop_waker() -> Waker {
    let raw = RawWaker::new(
        Arc::into_raw(Arc::new(())).cast(),
        &PANICKING_DROP_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    unsafe { Waker::from_raw(raw) }
}

enum Message {
    Ask(Reply<u8>),
}

struct ReplyingActor {
    allow_reply: ReleaseGate,
    delivered: tokio::sync::mpsc::UnboundedSender<()>,
}

impl Actor for ReplyingActor {
    type Msg = Message;
    type Args = (ReleaseGate, tokio::sync::mpsc::UnboundedSender<()>);

    async fn init(
        (allow_reply, delivered): Self::Args,
        _: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        Ok(Self {
            allow_reply,
            delivered,
        })
    }

    async fn handle(&mut self, message: Message, _: &mut Context<'_, Self>) -> ExitResult {
        let Message::Ask(reply) = message;
        self.allow_reply.wait().await;
        reply.send(7);
        let _ = self.delivered.send(());
        Ok(())
    }
}

/// Drives the ordinary public `call` success path after ensuring its one-shot
/// has parked with a caller waker whose drop vtable panics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_call_contains_reply_caller_waker_retirement() {
    let allow_reply = ReleaseGate::default();
    let (delivered_tx, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "reply-waker-call",
            ActorOnceDef::<ReplyingActor>::new((allow_reply.clone(), delivered_tx)),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    let hostile = ManuallyDrop::new(panicking_drop_waker());
    let mut call = Box::pin(actor.call(Message::Ask, Duration::from_secs(30)));
    assert!(matches!(
        call.as_mut().poll(&mut TaskContext::from_waker(&hostile)),
        Poll::Pending
    ));
    allow_reply.release();
    tokio::time::timeout(POLL_TIMEOUT, delivered_rx.recv())
        .await
        .expect("the actor delivers its reply")
        .expect("the delivery signal remains open");

    let Poll::Ready(Ok(replied)) = call.as_mut().poll(&mut TaskContext::from_waker(&hostile))
    else {
        panic!("the successful call returns its reply")
    };
    assert_eq!(replied.value, 7);

    drop(call);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

struct HostileReply;

impl Drop for HostileReply {
    fn drop(&mut self) {
        panic!("injected delivered-value drop panic");
    }
}

/// Combines the hostile registered-waker destructor with a delivered value
/// whose own destructor panics. Before the proxy, Tokio 1.53.1 dropped the
/// registered caller waker while its ready result owned this value; the two
/// unwinds aborted the process. Nextest's process-per-test isolation makes
/// that SIGABRT a failure of this exact regression.
#[tokio::test]
async fn successful_recv_cannot_double_panic_with_hostile_waker_and_value_drops() {
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "reply-waker-recv",
            ActorOnceDef::<ReplyingActor>::new((
                ReleaseGate::default(),
                tokio::sync::mpsc::unbounded_channel().0,
            )),
        )
        .expect("valid actor");
    let (reply, receiver) = actor.reply_channel::<HostileReply>();
    let hostile = ManuallyDrop::new(panicking_drop_waker());
    let mut receive = Box::pin(receiver.recv(Duration::from_secs(30)));
    assert!(matches!(
        receive
            .as_mut()
            .poll(&mut TaskContext::from_waker(&hostile)),
        Poll::Pending
    ));

    reply.send(HostileReply);
    let Poll::Ready(Ok(value)) = receive
        .as_mut()
        .poll(&mut TaskContext::from_waker(&hostile))
    else {
        panic!("the successful receive returns its reply")
    };
    // Its destructor is the abort's second half. A passing implementation
    // returns it intact, so the test intentionally leaves it undestroyed.
    std::mem::forget(value);
    drop(receive);
}
