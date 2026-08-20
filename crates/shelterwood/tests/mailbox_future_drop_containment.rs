use std::{
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

use shelterwood::{Actor, ActorOnceDef, Context, ExitError, ExitResult, Reply, Tree};

const CHILD_ENV: &str = "SHELTERWOOD_MAILBOX_FUTURE_DROP_CHILD";
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

struct FirstWakerDropPanics(AtomicBool);

unsafe fn clone_panicking_drop_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the matching
    // type. ManuallyDrop preserves the reference represented by data; the
    // returned raw waker owns only the new clone.
    let probe = ManuallyDrop::new(unsafe { Arc::<FirstWakerDropPanics>::from_raw(data.cast()) });
    RawWaker::new(
        Arc::into_raw(Arc::clone(&probe)).cast(),
        &PANICKING_DROP_WAKER_VTABLE,
    )
}

unsafe fn wake_panicking_drop_waker(data: *const ()) {
    // SAFETY: wake consumes the reference represented by this raw waker.
    drop(unsafe { Arc::<FirstWakerDropPanics>::from_raw(data.cast()) });
    panic!("injected waker wake panic");
}

unsafe fn wake_by_ref_panicking_drop_waker(_data: *const ()) {
    panic!("injected waker wake panic");
}

unsafe fn drop_panicking_drop_waker(data: *const ()) {
    // SAFETY: drop consumes the reference represented by this raw waker.
    let probe = unsafe { Arc::<FirstWakerDropPanics>::from_raw(data.cast()) };
    if !probe.0.swap(true, Ordering::SeqCst) {
        panic!("injected waker drop panic");
    }
}

static PANICKING_DROP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_panicking_drop_waker,
    wake_panicking_drop_waker,
    wake_by_ref_panicking_drop_waker,
    drop_panicking_drop_waker,
);

fn panicking_drop_waker() -> Waker {
    let raw = RawWaker::new(
        Arc::into_raw(Arc::new(FirstWakerDropPanics(AtomicBool::new(false)))).cast(),
        &PANICKING_DROP_WAKER_VTABLE,
    );
    // SAFETY: raw owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    unsafe { Waker::from_raw(raw) }
}

fn assert_pending_then_unwind<F: Future>(mut future: std::pin::Pin<Box<F>>) {
    // The caller-owned raw waker is deliberately leaked in this subprocess.
    // Only the clones registered by the public future are under test.
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

fn run_subprocess(test_name: &str) {
    let output = Command::new(std::env::current_exe().expect("integration-test executable"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_ENV, "1")
        .output()
        .expect("hostile-waker subprocess starts");

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    assert!(
        output.status.success(),
        "hostile-waker subprocess must preserve the original panic instead of aborting\nstatus: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status,
    );
    // A libtest filter that matches nothing also exits zero, so a renamed or
    // mistyped test would let this assertion pass without ever running the
    // child body. Require the child to report exactly one executed test.
    assert!(
        stdout.contains("1 passed"),
        "the child must actually run {test_name}, not filter it away\nstdout:\n{stdout}\nstderr:\n{stderr}",
    );
}

#[test]
fn send_timeout_drop_contains_timer_waker_during_unwind() {
    if std::env::var_os(CHILD_ENV).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let (_tree, actor) = actor_ref();
                assert_pending_then_unwind(Box::pin(
                    actor.send_timeout(Message::Value, Duration::from_secs(30)),
                ));
            });
        return;
    }

    run_subprocess("send_timeout_drop_contains_timer_waker_during_unwind");
}

#[test]
fn call_drop_contains_reply_receiver_waker_during_unwind() {
    if std::env::var_os(CHILD_ENV).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let (tree, actor) = actor_ref();
                let system = tree.spawn().expect("runtime is available");
                system.wait_started().await.expect("actor starts");
                assert_pending_then_unwind(Box::pin(actor.call(Message::Hold, Duration::MAX)));
                system
                    .shutdown(Duration::from_secs(1))
                    .await
                    .expect("actor stops");
            });
        return;
    }

    run_subprocess("call_drop_contains_reply_receiver_waker_during_unwind");
}

#[test]
fn recv_drop_contains_receiver_waker_during_unwind() {
    if std::env::var_os(CHILD_ENV).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let (_tree, actor) = actor_ref();
                let (_reply, receiver) = actor.reply_channel::<u8>();
                assert_pending_then_unwind(Box::pin(receiver.recv(Duration::MAX)));
            });
        return;
    }

    run_subprocess("recv_drop_contains_receiver_waker_during_unwind");
}

/// Numbers the clones a single poll hands out so the regression can aim at
/// one of them. `Deadlined::poll` registers the operation's clone first and
/// the timer's second, so ordinal two is the timer's.
static CLONES_MINTED: AtomicUsize = AtomicUsize::new(0);
const TIMER_WAKER_ORDINAL: usize = 2;

unsafe fn clone_ordinal_waker(_data: *const ()) -> RawWaker {
    let ordinal = CLONES_MINTED.fetch_add(1, Ordering::SeqCst) + 1;
    RawWaker::new(std::ptr::without_provenance(ordinal), &ORDINAL_WAKER_VTABLE)
}

unsafe fn wake_ordinal_waker(_data: *const ()) {}

unsafe fn wake_by_ref_ordinal_waker(_data: *const ()) {}

unsafe fn drop_ordinal_waker(data: *const ()) {
    if data.addr() == TIMER_WAKER_ORDINAL {
        panic!("injected timer waker drop panic");
    }
}

static ORDINAL_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_ordinal_waker,
    wake_ordinal_waker,
    wake_by_ref_ordinal_waker,
    drop_ordinal_waker,
);

fn ordinal_waker() -> Waker {
    // SAFETY: the vtable never dereferences its data pointer -- it carries a
    // clone ordinal, not an address -- so every operation on it is a no-op, a
    // counter bump, or a panic.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &ORDINAL_WAKER_VTABLE)) }
}

/// Stands in for a user reply whose destructor is hostile. Only the abort
/// half of the regression destroys one; the success path forgets it.
struct HostileReply;

impl Drop for HostileReply {
    fn drop(&mut self) {
        panic!("injected reply destructor panic");
    }
}

/// A deadline future that completes *after* parking still holds an armed
/// timer carrying a clone of the caller's waker. Retiring that timer on the
/// ready return runs a `RawWaker` destructor while the completed reply is a
/// live local, so an unwinding one would destroy the delivered value and --
/// with a hostile value destructor -- abort the process.
#[test]
fn recv_ready_after_parking_contains_the_retired_timer_waker() {
    if std::env::var_os(CHILD_ENV).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(async {
                let (_tree, actor) = actor_ref();
                let (reply, receiver) = actor.reply_channel::<HostileReply>();
                let mut future = Box::pin(receiver.recv(Duration::from_secs(30)));
                // The caller-owned waker is deliberately leaked in this
                // subprocess. Only the clones the public future registers are
                // under test.
                let hostile = ManuallyDrop::new(ordinal_waker());

                // Parking registers one clone in the reply channel and one in
                // the timer; only the timer's destructor is hostile.
                assert!(matches!(
                    future.as_mut().poll(&mut TaskContext::from_waker(&hostile)),
                    Poll::Pending
                ));

                assert_eq!(
                    CLONES_MINTED.load(Ordering::SeqCst),
                    TIMER_WAKER_ORDINAL,
                    "parking must register exactly the operation and timer clones"
                );

                reply.send(HostileReply);
                let completed = future.as_mut().poll(&mut TaskContext::from_waker(&hostile));

                let Poll::Ready(Ok(value)) = completed else {
                    panic!("the delivered reply must reach the caller")
                };
                // Its destructor is the abort's second half; the point of the
                // test is that it was never reached.
                std::mem::forget(value);
                drop::<Pin<Box<_>>>(future);
            });
        return;
    }

    run_subprocess("recv_ready_after_parking_contains_the_retired_timer_waker");
}
