mod common;

use std::{
    future::Future,
    sync::mpsc,
    task::{Context as TaskContext, Poll},
};

use common::{DestructorGate, POLL_TIMEOUT, hostile_waker, poll_until_ready};
use shelterwood::{Actor, ActorOnceDef, Blocking, Context, ExitError, ExitResult, Tree};

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

enum BlockingMessage {
    Start {
        operation: DestructorGate,
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
            operation,
            panic_on_drop,
        } = message;
        let blocker = operation.blocker();
        let work = context.run_blocking(move |_| {
            drop(blocker);
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
    let operation = DestructorGate::default();
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
            operation: operation.clone(),
            panic_on_drop,
        })
        .await
        .expect("actor accepts the blocking request");

    let mut work = Box::pin(
        work_rx
            .recv_timeout(POLL_TIMEOUT)
            .expect("the actor returns the public blocking future"),
    );
    operation.wait_entered();
    let hostile = hostile_waker("injected result caller-waker drop panic");
    assert!(matches!(
        work.as_mut().poll(&mut TaskContext::from_waker(&hostile)),
        Poll::Pending
    ));

    operation.release();
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
