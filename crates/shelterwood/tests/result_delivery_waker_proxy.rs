mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{Arc, mpsc},
    task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker},
    time::Instant,
};

use common::POLL_TIMEOUT;
use shelterwood::{Actor, ActorOnceDef, Blocking, Context, ExitError, ExitResult, Tree};

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
    panic!("injected result caller-waker drop panic");
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

struct DeliveredValue {
    panic_on_drop: bool,
}

impl Drop for DeliveredValue {
    fn drop(&mut self) {
        if self.panic_on_drop {
            panic!("injected delivered-value drop panic");
        }
    }
}

fn poll_until_ready<F: Future>(mut future: Pin<&mut F>, waker: &Waker) -> F::Output {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut TaskContext::from_waker(waker)) {
            return output;
        }
        assert!(
            Instant::now() < deadline,
            "result future becomes ready before the test deadline"
        );
        std::thread::yield_now();
    }
}

enum BlockingMessage {
    Start {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
        panic_on_drop: bool,
    },
}

struct BlockingSource {
    work: mpsc::Sender<Blocking<DeliveredValue>>,
}

impl Actor for BlockingSource {
    type Msg = BlockingMessage;
    type Args = mpsc::Sender<Blocking<DeliveredValue>>;

    async fn init(work: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { work })
    }

    async fn handle(
        &mut self,
        message: BlockingMessage,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        let BlockingMessage::Start {
            entered,
            release,
            panic_on_drop,
        } = message;
        let work = context.run_blocking(move |_| {
            let _ = entered.send(());
            release
                .recv_timeout(POLL_TIMEOUT)
                .expect("the test releases the blocking operation");
            DeliveredValue { panic_on_drop }
        });
        self.work
            .send(work)
            .unwrap_or_else(|_| panic!("the test retains the blocking future receiver"));
        Ok(())
    }
}

async fn drive_run_blocking_delivery(panic_on_drop: bool) {
    let (work_tx, work_rx) = mpsc::channel();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "blocking-result-waker",
            ActorOnceDef::<BlockingSource>::new(work_tx),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(BlockingMessage::Start {
            entered: entered_tx,
            release: release_rx,
            panic_on_drop,
        })
        .await
        .expect("actor accepts the blocking request");

    let mut work = Box::pin(
        work_rx
            .recv_timeout(POLL_TIMEOUT)
            .expect("the actor returns the public blocking future"),
    );
    entered_rx
        .recv_timeout(POLL_TIMEOUT)
        .expect("the blocking operation starts");
    let hostile = ManuallyDrop::new(panicking_drop_waker());
    assert!(matches!(
        work.as_mut().poll(&mut TaskContext::from_waker(&hostile)),
        Poll::Pending
    ));

    release_tx
        .send(())
        .expect("the blocking operation retains its release lane");
    let value = poll_until_ready(work.as_mut(), &hostile);
    assert_eq!(value.panic_on_drop, panic_on_drop);
    if panic_on_drop {
        // Its destructor is the abort's second half. A passing implementation
        // returns it intact, so this regression intentionally leaves it live.
        std::mem::forget(value);
    } else {
        drop(value);
    }
    drop(work);

    system
        .shutdown(POLL_TIMEOUT)
        .await
        .expect("the actor stops");
}

/// Drives the public `Context::run_blocking` future after parking it with a
/// caller waker whose drop vtable panics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_run_blocking_contains_completion_caller_waker_retirement() {
    drive_run_blocking_delivery(false).await;
}

/// Combines the hostile registered-waker destructor with a blocking result
/// whose destructor also panics. Before the proxy, Tokio 1.53.1 destroyed the
/// raw caller waker while its ready frame owned this result, so unwinding that
/// result aborted the process. Nextest's process-per-test isolation makes that
/// SIGABRT a failure of this exact regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_run_blocking_cannot_double_panic_with_hostile_waker_and_value_drops() {
    drive_run_blocking_delivery(true).await;
}
