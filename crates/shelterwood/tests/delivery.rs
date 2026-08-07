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
    CallErrorKind, ExitError, ExitResult, Mailbox, RawActor, RawContext, RawDef, RawOnceDef, Reply,
    SendErrorKind, Tree,
};
use shelterwood_test_support::{ReleaseGate, assert_quiet, poll_until};

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

struct PrefixActor {
    generation: usize,
    park: bool,
    gate: ReleaseGate,
    log: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl RawActor for PrefixActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.park || self.generation == 1 {
            self.gate.wait().await;
        }
        while let Some(value) = context.recv().await {
            self.log
                .lock()
                .expect("prefix log mutex poisoned")
                .push((self.generation, value));
            if self.generation == 1 {
                return Err(ExitError::message("poison first incarnation"));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn accepted_but_undelivered_prefix_never_crosses_an_incarnation() {
    let generation = Arc::new(AtomicUsize::new(0));
    let gate = ReleaseGate::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "prefix",
            RawDef::factory({
                let generation = Arc::clone(&generation);
                let gate = gate.clone();
                let log = Arc::clone(&log);
                move || PrefixActor {
                    generation: generation.fetch_add(1, Ordering::SeqCst) + 1,
                    park: false,
                    gate: gate.clone(),
                    log: Arc::clone(&log),
                }
            })
            .mailbox(Mailbox::queue(4).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    actor.try_send(1).expect("poison accepts");
    actor.try_send(2).expect("remainder accepts");
    actor.try_send(3).expect("remainder accepts");
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            generation.load(Ordering::SeqCst) == 2
        })
        .await
    );
    actor
        .send(4)
        .await
        .expect("fresh message reaches replacement");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            log.lock().expect("prefix log mutex poisoned").as_slice() == [(1, 1), (2, 4)]
        })
        .await
    );
    assert_quiet(Duration::from_millis(20), || {
        log.lock()
            .expect("prefix log mutex poisoned")
            .iter()
            .any(|(_, value)| *value == 2 || *value == 3)
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[derive(Debug)]
enum CallMessage {
    Marker,
    Ask(Reply<usize>),
    Value,
}

struct KilledCallActor {
    gate: ReleaseGate,
}

impl RawActor for KilledCallActor {
    type Msg = CallMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.gate.wait().await;
        while let Some(message) = context.recv().await {
            match message {
                CallMessage::Marker | CallMessage::Value => {}
                CallMessage::Ask(reply) => {
                    drop(reply);
                    return Err(ExitError::message("accepted call killed"));
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn accepted_then_killed_call_reports_reply_dropped_with_accepting_identity() {
    let gate = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "killed-call",
            RawOnceDef::new(KilledCallActor { gate: gate.clone() })
                .mailbox(Mailbox::queue(2).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let accepting = actor.try_send(CallMessage::Marker).expect("marker accepts");
    let call_actor = actor.clone();
    let call = tokio::spawn(async move {
        call_actor
            .call(CallMessage::Ask, Duration::from_secs(1))
            .await
    });
    tokio::task::yield_now().await;
    gate.release();
    let error = call
        .await
        .expect("call task joins")
        .expect_err("killed call loses its reply");
    assert_eq!(error.kind, CallErrorKind::ReplyDropped);
    assert_eq!(error.incarnation_observed, Some(accepting));
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrderedMessage {
    sender: char,
    sequence: usize,
}

struct OrderedActor {
    gate: ReleaseGate,
    log: Arc<Mutex<Vec<OrderedMessage>>>,
}

impl RawActor for OrderedActor {
    type Msg = OrderedMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.gate.wait().await;
        while let Some(message) = context.recv().await {
            self.log
                .lock()
                .expect("order log mutex poisoned")
                .push(message);
        }
        Ok(())
    }
}

#[tokio::test]
async fn queue_preserves_per_sender_fifo_under_interleaved_senders() {
    let gate = ReleaseGate::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "ordered",
            RawOnceDef::new(OrderedActor {
                gate: gate.clone(),
                log: Arc::clone(&log),
            })
            .mailbox(Mailbox::queue(8).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    let (a_tx, mut a_rx) =
        tokio::sync::mpsc::unbounded_channel::<(usize, tokio::sync::oneshot::Sender<()>)>();
    let (b_tx, mut b_rx) =
        tokio::sync::mpsc::unbounded_channel::<(usize, tokio::sync::oneshot::Sender<()>)>();
    let a_actor = actor.clone();
    let sender_a = tokio::spawn(async move {
        while let Some((sequence, accepted)) = a_rx.recv().await {
            a_actor
                .send(OrderedMessage {
                    sender: 'a',
                    sequence,
                })
                .await
                .expect("sender a message accepts");
            let _ = accepted.send(());
        }
    });
    let b_actor = actor.clone();
    let sender_b = tokio::spawn(async move {
        while let Some((sequence, accepted)) = b_rx.recv().await {
            b_actor
                .send(OrderedMessage {
                    sender: 'b',
                    sequence,
                })
                .await
                .expect("sender b message accepts");
            let _ = accepted.send(());
        }
    });

    for (sender, sequence) in [(&a_tx, 1), (&b_tx, 1), (&a_tx, 2), (&b_tx, 2)] {
        let (accepted, wait) = tokio::sync::oneshot::channel();
        sender
            .send((sequence, accepted))
            .expect("sender task is live");
        wait.await.expect("message is accepted");
    }
    drop(a_tx);
    drop(b_tx);
    sender_a.await.expect("sender a joins");
    sender_b.await.expect("sender b joins");
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            log.lock().expect("order log mutex poisoned").len() == 4
        })
        .await
    );
    let log = log.lock().expect("order log mutex poisoned").clone();
    let a: Vec<_> = log
        .iter()
        .filter(|message| message.sender == 'a')
        .map(|message| message.sequence)
        .collect();
    let b: Vec<_> = log
        .iter()
        .filter(|message| message.sender == 'b')
        .map(|message| message.sequence)
        .collect();
    assert_eq!(a, [1, 2]);
    assert_eq!(b, [1, 2]);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

struct CallRecorder {
    gate: ReleaseGate,
    calls: Arc<AtomicUsize>,
}

impl RawActor for CallRecorder {
    type Msg = CallMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.gate.wait().await;
        while let Some(message) = context.recv().await {
            if let CallMessage::Ask(reply) = message {
                self.calls.fetch_add(1, Ordering::SeqCst);
                reply.send(7);
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn latest_conflation_drops_replaced_call_and_keeps_newest_value() {
    let gate = ReleaseGate::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "latest-call",
            RawOnceDef::new(CallRecorder {
                gate: gate.clone(),
                calls: Arc::clone(&calls),
            })
            .mailbox(Mailbox::latest()),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let accepting = actor
        .try_send(CallMessage::Value)
        .expect("identity probe accepts");
    let mut call = Box::pin(actor.call(CallMessage::Ask, Duration::from_secs(1)));
    assert!(poll_once(call.as_mut()).is_pending());
    assert_eq!(
        actor
            .try_send(CallMessage::Value)
            .expect("newest replaces call"),
        accepting
    );
    let error = match poll_once(call.as_mut()) {
        Poll::Ready(Err(error)) => error,
        _ => panic!("replaced reply capability must resolve as dropped"),
    };
    assert_eq!(error.kind, CallErrorKind::ReplyDropped);
    assert_eq!(error.incarnation_observed, Some(accepting));
    gate.release();
    assert_quiet(Duration::from_millis(20), || {
        calls.load(Ordering::SeqCst) != 0
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test]
async fn send_cancellation_withdraws_before_acceptance_but_not_after() {
    let gate = ReleaseGate::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "cancel-send",
            RawOnceDef::new(PrefixActor {
                generation: 2,
                park: true,
                gate: gate.clone(),
                log: Arc::clone(&log),
            })
            .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor.try_send(1).expect("queue fills");
    let mut before = Box::pin(actor.send(2));
    assert!(poll_once(before.as_mut()).is_pending());
    drop(before);
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            log.lock().expect("send log mutex poisoned").as_slice() == [(2, 1)]
        })
        .await
    );
    let mut after = Box::pin(actor.send(3));
    assert!(matches!(poll_once(after.as_mut()), Poll::Ready(Ok(_))));
    drop(after);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            log.lock().expect("send log mutex poisoned").as_slice() == [(2, 1), (2, 3)]
        })
        .await
    );
    assert_quiet(Duration::from_millis(20), || {
        log.lock()
            .expect("send log mutex poisoned")
            .iter()
            .any(|(_, value)| *value == 2)
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test]
async fn call_cancellation_withdraws_before_acceptance_but_processes_after() {
    for accepted_before_drop in [false, true] {
        let gate = ReleaseGate::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tree = Tree::new();
        let actor = tree
            .add_raw_once(
                "cancel-call",
                RawOnceDef::new(CallRecorder {
                    gate: gate.clone(),
                    calls: Arc::clone(&calls),
                })
                .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
            )
            .expect("valid actor");
        let system = tree.spawn().expect("runtime is available");
        system.wait_started().await.expect("actor starts");
        if !accepted_before_drop {
            actor.try_send(CallMessage::Value).expect("queue fills");
        }
        let mut call = Box::pin(actor.call(CallMessage::Ask, Duration::from_secs(1)));
        assert!(poll_once(call.as_mut()).is_pending());
        if accepted_before_drop {
            let full = actor
                .try_send(CallMessage::Value)
                .expect_err("accepted call occupies the queue");
            assert_eq!(full.kind, SendErrorKind::Full);
        }
        drop(call);
        gate.release();
        let expected = usize::from(accepted_before_drop);
        if accepted_before_drop {
            assert!(
                poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
                    calls.load(Ordering::SeqCst) == expected
                })
                .await
            );
        } else {
            assert_quiet(Duration::from_millis(20), || {
                calls.load(Ordering::SeqCst) != expected
            })
            .await;
        }
        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("actor stops");
    }
}
