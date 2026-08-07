use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use shelterwood::{
    CallErrorKind, ExitResult, Mailbox, MailboxShutdown, RawActor, RawContext, RawDef, Reply,
    ReplyError, SendErrorKind, Tree,
};
use shelterwood_test_support::{ReleaseGate, advance_time, assert_quiet, poll_until};

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

#[derive(Debug)]
enum Message {
    Value(usize),
    Ask(usize, Reply<usize>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplyMode {
    Answer,
    Drop,
    Hold,
}

struct Recorder {
    gate: Option<ReleaseGate>,
    values: Arc<Mutex<Vec<usize>>>,
    asks: Arc<AtomicUsize>,
    reply_mode: ReplyMode,
    held: Option<Reply<usize>>,
}

impl Recorder {
    fn handle(&mut self, message: Message) {
        match message {
            Message::Value(value) => self
                .values
                .lock()
                .expect("values mutex poisoned")
                .push(value),
            Message::Ask(value, reply) => {
                self.asks.fetch_add(1, Ordering::SeqCst);
                match self.reply_mode {
                    ReplyMode::Answer => reply.send(value * 2),
                    ReplyMode::Drop => drop(reply),
                    ReplyMode::Hold => self.held = Some(reply),
                }
            }
        }
    }
}

impl RawActor for Recorder {
    type Msg = Message;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if let Some(gate) = self.gate.take() {
            gate.wait().await;
        }
        while let Some(message) = context.recv().await {
            self.handle(message);
        }
        if context.mailbox_shutdown() == MailboxShutdown::Drain {
            while let Some(message) = context.try_recv() {
                self.handle(message);
            }
        }
        Ok(())
    }
}

fn recorder(
    gate: Option<ReleaseGate>,
    values: &Arc<Mutex<Vec<usize>>>,
    asks: &Arc<AtomicUsize>,
    reply_mode: ReplyMode,
) -> Recorder {
    Recorder {
        gate,
        values: Arc::clone(values),
        asks: Arc::clone(asks),
        reply_mode,
        held: None,
    }
}

#[tokio::test]
async fn queue_backpressure_and_send_error_identity_are_exact() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let asks = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "recorder",
            RawDef::factory({
                let gate = gate.clone();
                let values = Arc::clone(&values);
                let asks = Arc::clone(&asks);
                move || recorder(Some(gate.clone()), &values, &asks, ReplyMode::Answer)
            })
            .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");

    let pre_spawn = actor
        .try_send(Message::Value(0))
        .expect_err("pre-spawn actor is unbound");
    assert_eq!(pre_spawn.kind, SendErrorKind::NotRunning);
    assert_eq!(pre_spawn.incarnation_observed, None);

    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let accepting = actor
        .try_send(Message::Value(1))
        .expect("first message fits");
    let full = actor
        .try_send(Message::Value(2))
        .expect_err("second message sees full queue");
    assert_eq!(full.kind, SendErrorKind::Full);
    assert_eq!(full.incarnation_observed, Some(accepting));

    let waiting_actor = actor.clone();
    let waiting = tokio::spawn(async move { waiting_actor.send(full.message).await });
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(!waiting.is_finished(), "send applies real backpressure");
    gate.release();
    assert_eq!(
        waiting
            .await
            .expect("send task joins")
            .expect("send accepts"),
        accepting
    );
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values.lock().expect("values mutex poisoned").as_slice() == [1, 2]
        })
        .await
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
    let terminal = actor
        .send(Message::Value(3))
        .await
        .expect_err("terminal membership rejects send");
    assert_eq!(terminal.kind, SendErrorKind::Terminated);
    assert_eq!(terminal.incarnation_observed, Some(accepting));
}

#[tokio::test]
async fn latest_mailbox_keeps_only_the_newest_accepted_value() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let asks = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "latest",
            RawDef::factory({
                let gate = gate.clone();
                let values = Arc::clone(&values);
                let asks = Arc::clone(&asks);
                move || recorder(Some(gate.clone()), &values, &asks, ReplyMode::Answer)
            })
            .mailbox(Mailbox::latest()),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    actor.try_send(Message::Value(1)).expect("accept one");
    actor.try_send(Message::Value(2)).expect("replace one");
    actor.try_send(Message::Value(3)).expect("replace two");
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values.lock().expect("values mutex poisoned").as_slice() == [3]
        })
        .await
    );
    assert_quiet(Duration::from_millis(20), || {
        values.lock().expect("values mutex poisoned").len() > 1
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test(start_paused = true)]
async fn timed_send_withdraws_and_recovers_the_message() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let asks = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "timed",
            RawDef::factory({
                let gate = gate.clone();
                let values = Arc::clone(&values);
                let asks = Arc::clone(&asks);
                move || recorder(Some(gate.clone()), &values, &asks, ReplyMode::Answer)
            })
            .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let accepting = actor.try_send(Message::Value(1)).expect("queue fills");

    let width = Duration::from_secs(10);
    let timed_actor = actor.clone();
    let timed =
        tokio::spawn(async move { timed_actor.send_timeout(Message::Value(2), width).await });
    tokio::task::yield_now().await;
    advance_time(width).await;
    let error = timed
        .await
        .expect("timed send task joins")
        .expect_err("send withdraws");
    assert_eq!(error.kind, SendErrorKind::TimedOut);
    assert_eq!(error.incarnation_observed, Some(accepting));

    let mut cancelled = Box::pin(actor.send(Message::Value(3)));
    assert!(poll_once(cancelled.as_mut()).is_pending());
    drop(cancelled);
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values.lock().expect("values mutex poisoned").as_slice() == [1]
        })
        .await
    );
    actor
        .send(error.message)
        .await
        .expect("recovered message is safe to resend");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values.lock().expect("values mutex poisoned").as_slice() == [1, 2]
        })
        .await
    );
    assert_quiet(Duration::from_millis(20), || {
        values.lock().expect("values mutex poisoned").contains(&3)
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test(start_paused = true)]
async fn call_distinguishes_success_drop_and_response_timeout() {
    for (mode, expected) in [
        (ReplyMode::Drop, Some(CallErrorKind::ReplyDropped)),
        (ReplyMode::Hold, Some(CallErrorKind::ResponseTimedOut)),
        (ReplyMode::Answer, None),
    ] {
        let values = Arc::new(Mutex::new(Vec::new()));
        let asks = Arc::new(AtomicUsize::new(0));
        let mut tree = Tree::new();
        let actor = tree
            .add_raw_once(
                "caller",
                shelterwood::RawOnceDef::new(recorder(None, &values, &asks, mode)),
            )
            .expect("valid actor");
        let system = tree.spawn().expect("runtime is available");
        system.wait_started().await.expect("actor starts");
        let accepting = actor
            .try_send(Message::Value(0))
            .expect("identity probe accepts");
        let width = Duration::from_secs(10);
        let call_actor = actor.clone();
        let call =
            tokio::spawn(
                async move { call_actor.call(|reply| Message::Ask(7, reply), width).await },
            );
        assert!(
            poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
                asks.load(Ordering::SeqCst) == 1
            })
            .await
        );
        if mode == ReplyMode::Hold {
            advance_time(width).await;
        }
        let result = call.await.expect("call task joins");
        match expected {
            Some(kind) => {
                let error = result.expect_err("call fails in this mode");
                assert_eq!(error.kind, kind);
                assert_eq!(error.incarnation_observed, Some(accepting));
            }
            None => {
                let replied = result.expect("answer arrives");
                assert_eq!(replied.value, 14);
                assert_eq!(replied.incarnation, accepting);
            }
        }
        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("actor stops");
    }
}

#[tokio::test(start_paused = true)]
async fn reply_receiver_is_consuming_and_late_replies_are_discarded() {
    let width = Duration::from_secs(10);
    let (reply, receiver) = Reply::channel();
    let waiter = tokio::spawn(async move { receiver.recv(width).await });
    tokio::task::yield_now().await;
    advance_time(width).await;
    assert_eq!(
        waiter.await.expect("receiver task joins"),
        Err(ReplyError::Timeout)
    );
    reply.send(1);

    let (reply, receiver) = Reply::<usize>::channel();
    drop(receiver);
    reply.send(2);
}
