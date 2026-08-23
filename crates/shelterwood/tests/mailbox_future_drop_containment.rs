//! Containment regressions for the mailbox futures' drop and completion
//! paths.
//!
//! Every test here injects a destructor that panics where the framework must
//! not let one escape, so the failure these pin is a double panic and a
//! process abort rather than an assertion. That needs no subprocess harness:
//! `just test` runs the integration targets under `cargo nextest`, which
//! executes each test in its own process, so an abort fails exactly the test
//! that caused it and leaves the rest of the run intact. Under a shared-process
//! runner (`cargo test`) an abort would instead take the whole binary down --
//! still a loud failure, but an unattributed one.

mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};

use common::{ordinal_drop_waker, probe_waker};
use shelterwood::{Actor, ActorOnceDef, Context, ExitError, ExitResult, Reply, Tree};

const OUTER_PANIC: &str = "injected outer panic";

enum Message {
    Value,
    Hold(Reply<u8>),
}

struct HoldingActor {
    reply: Option<Reply<u8>>,
}

impl Actor for HoldingActor {
    type Msg = Message;
    type Args = ();

    async fn init((): (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { reply: None })
    }

    async fn handle(&mut self, message: Message, _: &mut Context<'_, Self>) -> ExitResult {
        if let Message::Hold(reply) = message {
            self.reply = Some(reply);
        }
        Ok(())
    }
}

fn panicking_drop_waker() -> Waker {
    let first = Arc::new(std::sync::atomic::AtomicBool::new(false));
    probe_waker(
        || {},
        move || {
            if !first.swap(true, std::sync::atomic::Ordering::SeqCst) {
                panic!("injected waker drop panic");
            }
        },
    )
}

fn assert_pending_then_unwind<F: Future>(mut future: Pin<Box<F>>) {
    // The caller-owned raw waker is deliberately leaked. Only the clones the
    // public future registers are under test.
    let hostile = ManuallyDrop::new(panicking_drop_waker());
    assert!(matches!(
        future.as_mut().poll(&mut TaskContext::from_waker(&hostile)),
        Poll::Pending
    ));

    let payload = catch_unwind(AssertUnwindSafe(move || {
        let _future = future;
        std::panic::panic_any(OUTER_PANIC);
    }))
    .expect_err("the original unwind reaches its boundary");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&OUTER_PANIC));
}

fn actor_ref() -> (Tree, shelterwood::ActorRef<Message>) {
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once("waker-drop", ActorOnceDef::<HoldingActor>::new(()))
        .expect("valid actor");
    (tree, actor)
}

#[tokio::test]
async fn send_timeout_drop_contains_timer_waker_during_unwind() {
    let (_tree, actor) = actor_ref();
    assert_pending_then_unwind(Box::pin(
        actor.send_timeout(Message::Value, Duration::from_secs(30)),
    ));
}

#[tokio::test]
async fn call_acceptance_phase_drop_contains_timer_waker_during_unwind() {
    let (_tree, actor) = actor_ref();
    // A bounded budget, deliberately: `Duration::MAX` overflows to an
    // unbounded deadline, which registers no timer waker at all and would
    // leave this test named after a clone it never mints.
    assert_pending_then_unwind(Box::pin(actor.call(Message::Hold, Duration::from_secs(30))));
}

#[tokio::test]
async fn call_reply_phase_drop_contains_caller_wakers_during_unwind() {
    let (tree, actor) = actor_ref();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    assert_pending_then_unwind(Box::pin(actor.call(Message::Hold, Duration::MAX)));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test]
async fn recv_drop_contains_receiver_waker_during_unwind() {
    let (_tree, actor) = actor_ref();
    let (_reply, receiver) = actor.reply_channel::<u8>();
    assert_pending_then_unwind(Box::pin(receiver.recv(Duration::MAX)));
}

const TIMER_CALLER_WAKER_ORDINAL: usize = 2;

/// Stands in for a user reply whose destructor is hostile. Only the abort
/// half of the regression destroys one; the success path forgets it.
struct HostileReply;

impl Drop for HostileReply {
    fn drop(&mut self) {
        panic!("injected reply destructor panic");
    }
}

/// A deadline future that completes *after* parking still holds a caller-waker
/// clone behind its timer proxy, and the completion path retires that clone
/// **on this thread** -- the poll-path half of #398 ruling 3's venue split.
/// The destructor injected below therefore runs synchronously inside
/// `Deadlined::poll`, while the delivered reply is a live local in the same
/// frame. An escaping cleanup panic would destroy that value mid-unwind and --
/// with the hostile value destructor this test installs -- abort the process.
///
/// Judged by the ordinary return: the poll must hand back `Ready` with the
/// delivered value. Wrapping the poll in `catch_unwind` would make the test
/// vacuous -- it would pass on a framework that contains nothing, because the
/// harness would be doing the containing.
#[tokio::test]
async fn recv_ready_after_parking_contains_the_retired_timer_waker() {
    let (_tree, actor) = actor_ref();
    let (reply, receiver) = actor.reply_channel::<HostileReply>();
    let mut future = Box::pin(receiver.recv(Duration::from_secs(30)));
    // The caller-owned waker is deliberately leaked. Only the clones the
    // public future registers are under test.
    // `Deadlined::poll` registers the operation's clone first and the timer
    // proxy's stored caller second. The timer itself sees only the stable
    // framework-owned proxy, so it does not mint another hostile clone.
    let (hostile, state) = ordinal_drop_waker(TIMER_CALLER_WAKER_ORDINAL, || {
        panic!("injected timer-proxy caller-waker drop panic")
    });
    let hostile = ManuallyDrop::new(hostile);

    // Parking registers one clone in the reply channel and one behind the
    // timer proxy; only the proxy-owned caller clone's destructor is hostile.
    assert!(matches!(
        future.as_mut().poll(&mut TaskContext::from_waker(&hostile)),
        Poll::Pending
    ));

    assert_eq!(
        state.clones(),
        TIMER_CALLER_WAKER_ORDINAL,
        "parking must register exactly the operation and timer-proxy caller clones"
    );

    reply.send(HostileReply);
    let completed = future.as_mut().poll(&mut TaskContext::from_waker(&hostile));

    let Poll::Ready(Ok(value)) = completed else {
        panic!("the delivered reply must reach the caller")
    };
    // Its destructor is the abort's second half; the point of the test is
    // that it was never reached.
    std::mem::forget(value);
    drop::<Pin<Box<_>>>(future);
}
