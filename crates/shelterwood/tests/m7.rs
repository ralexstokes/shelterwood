use std::{
    cell::Cell,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    ActorRef, AttemptEnd, Backoff, ExitError, ExitResult, IdempotentCallErrorKind,
    IdempotentCallFuture, Mailbox, NextIncarnation, NextIncarnationError, PinnedRef, RawActor,
    RawContext, RawDef, RawOnceDef, Reply, RetryPolicy, SendErrorKind, SendPayload, Tree,
};
use shelterwood_test_support::{ReleaseGate, advance_time, poll_until};

enum MappingMessage {
    Value(u32),
    Ask(u32, Reply<u32>),
}

struct MappingActor {
    gate: ReleaseGate,
    values: Arc<Mutex<Vec<u32>>>,
}

fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
fn assert_send<T: Send>() {}

#[test]
fn m7_public_handles_and_futures_obey_the_trait_matrix() {
    assert_clone_send_sync::<ActorRef<u32>>();
    assert_clone_send_sync::<PinnedRef<u32>>();
    assert_send::<NextIncarnation>();
    assert_send::<IdempotentCallFuture<u32>>();
}

impl RawActor for MappingActor {
    type Msg = MappingMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.gate.wait().await;
        while let Some(message) = context.recv().await {
            match message {
                MappingMessage::Value(value) => self
                    .values
                    .lock()
                    .expect("mapping values mutex poisoned")
                    .push(value),
                MappingMessage::Ask(value, reply) => reply.send(value * 2),
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn contramap_shares_identity_mailbox_and_projected_error_model() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "mapped",
            RawOnceDef::new(MappingActor {
                gate: gate.clone(),
                values: Arc::clone(&values),
            })
            .mailbox(Mailbox::latest()),
        )
        .expect("valid mapped actor");
    let first_hops = Arc::new(AtomicUsize::new(0));
    let second_hops = Arc::new(AtomicUsize::new(0));
    let numbers = actor.contramap({
        let first_hops = Arc::clone(&first_hops);
        move |value: u32| {
            first_hops.fetch_add(1, Ordering::SeqCst);
            MappingMessage::Value(value)
        }
    });
    let strings = numbers.contramap({
        let second_hops = Arc::clone(&second_hops);
        move |value: String| {
            second_hops.fetch_add(1, Ordering::SeqCst);
            value.parse::<u32>().expect("test value is numeric")
        }
    });

    assert_eq!(strings.id(), actor.id());
    assert_eq!(strings.membership(), actor.membership());
    let panicking = actor.contramap(|_: &'static str| -> MappingMessage {
        panic!("mapping panic stays on the sender stack")
    });
    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = panicking.try_send("boom");
    }));
    assert!(panic.is_err());
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("mapped actor starts");
    assert_eq!(system.scope().snapshot().children.len(), 1);

    strings.try_send("1".to_owned()).expect("accept one");
    strings.try_send("2".to_owned()).expect("replace one");
    strings.try_send("3".to_owned()).expect("replace two");
    assert_eq!(first_hops.load(Ordering::SeqCst), 3);
    assert_eq!(second_hops.load(Ordering::SeqCst), 3);
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values
                .lock()
                .expect("mapping values mutex poisoned")
                .as_slice()
                == [3]
        })
        .await
    );

    let calls =
        actor.contramap(|(value, reply): (u32, Reply<u32>)| MappingMessage::Ask(value, reply));
    let reply = calls
        .call(|reply| (7, reply), Duration::from_secs(1))
        .await
        .expect("mapped call replies");
    assert_eq!(reply.value, 14);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("mapped actor stops");
    let projected = strings
        .send("4".to_owned())
        .await
        .expect_err("terminal mapped ref rejects");
    assert_eq!(projected.kind, SendErrorKind::Terminated);
    assert_eq!(projected.payload, SendPayload::Projected);
    assert_eq!(projected.into_message(), None);

    let recovered = actor
        .send(MappingMessage::Value(4))
        .await
        .expect_err("terminal direct ref rejects");
    assert!(matches!(
        recovered.payload,
        SendPayload::Recovered(MappingMessage::Value(4))
    ));
}

enum RestartMessage {
    Crash,
    Value(usize),
}

struct RestartActor {
    values: Arc<Mutex<Vec<usize>>>,
}

impl RawActor for RestartActor {
    type Msg = RestartMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        while let Some(message) = context.recv().await {
            match message {
                RestartMessage::Crash => return Err(ExitError::message("restart now")),
                RestartMessage::Value(value) => self
                    .values
                    .lock()
                    .expect("restart values mutex poisoned")
                    .push(value),
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn pinned_ref_fails_after_restart_while_membership_ref_rides_through() {
    let values = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "restart",
            RawDef::factory({
                let values = Arc::clone(&values);
                move || RestartActor {
                    values: Arc::clone(&values),
                }
            }),
        )
        .expect("valid restart actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("restart actor starts");
    let first = actor
        .try_send(RestartMessage::Crash)
        .expect("crash accepted");
    let pinned = actor.pinned(first);
    assert_eq!(pinned.unpinned(), actor);

    let second = actor
        .next_incarnation(first, Duration::from_secs(1))
        .await
        .expect("replacement becomes accepting");
    assert!(second.supersedes(first));
    let error = pinned
        .try_send(RestartMessage::Value(1))
        .expect_err("old pin fails fast");
    assert_eq!(error.incarnation_observed, Some(second));
    assert_eq!(
        error.kind,
        SendErrorKind::Superseded {
            pinned: first,
            newest_observed: Some(second),
        }
    );
    assert!(matches!(
        error.into_message(),
        Some(RestartMessage::Value(1))
    ));

    assert_eq!(
        actor
            .send(RestartMessage::Value(2))
            .await
            .expect("membership ref follows replacement"),
        second
    );
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values
                .lock()
                .expect("restart values mutex poisoned")
                .as_slice()
                == [2]
        })
        .await
    );
    assert_eq!(
        actor.next_incarnation(second, Duration::ZERO).await,
        Err(NextIncarnationError::TimedOut)
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("restart actor stops");
    assert_eq!(
        actor.next_incarnation(second, Duration::from_secs(1)).await,
        Err(NextIncarnationError::Terminated { last: Some(second) })
    );
}

struct ParkedRestartActor {
    generation: usize,
    fail_first: ReleaseGate,
    values: Arc<Mutex<Vec<usize>>>,
}

impl RawActor for ParkedRestartActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.generation == 1 {
            self.fail_first.wait().await;
            return Err(ExitError::message("replace parked incarnation"));
        }
        while let Some(value) = context.recv().await {
            self.values
                .lock()
                .expect("parked values mutex poisoned")
                .push(value);
        }
        Ok(())
    }
}

#[tokio::test]
async fn parked_pinned_send_stops_at_the_incarnation_boundary() {
    let generations = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "parked-pin",
            RawDef::factory({
                let generations = Arc::clone(&generations);
                let fail_first = fail_first.clone();
                let values = Arc::clone(&values);
                move || ParkedRestartActor {
                    generation: generations.fetch_add(1, Ordering::SeqCst) + 1,
                    fail_first: fail_first.clone(),
                    values: Arc::clone(&values),
                }
            })
            .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid parked-pin actor");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("parked-pin actor starts");
    let first = actor.try_send(0).expect("first mailbox fills");
    let pinned = actor.pinned(first);
    let pinned_send = tokio::spawn(async move { pinned.send(1).await });
    let membership_send = {
        let actor = actor.clone();
        tokio::spawn(async move { actor.send(2).await })
    };
    tokio::task::yield_now().await;
    assert!(!pinned_send.is_finished());
    assert!(!membership_send.is_finished());

    fail_first.release();
    let pinned_error = pinned_send
        .await
        .expect("pinned send task joins")
        .expect_err("pinned send cannot ride through intake freeze");
    assert_eq!(pinned_error.kind, SendErrorKind::NotRunning);
    assert_eq!(pinned_error.incarnation_observed, Some(first));
    assert_eq!(pinned_error.into_message(), Some(1));

    let second = actor
        .next_incarnation(first, Duration::from_secs(1))
        .await
        .expect("replacement becomes accepting");
    assert_eq!(
        membership_send
            .await
            .expect("membership send task joins")
            .expect("membership send rides through restart"),
        second
    );
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values
                .lock()
                .expect("parked values mutex poisoned")
                .as_slice()
                == [2]
        })
        .await
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("parked-pin actor stops");
}

enum IdempotentMessage {
    Ask(Reply<usize>),
}

struct ReplyDropActor {
    generation: usize,
    seen: Arc<Mutex<Vec<shelterwood::Incarnation>>>,
}

impl RawActor for ReplyDropActor {
    type Msg = IdempotentMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        let Some(IdempotentMessage::Ask(reply)) = context.recv().await else {
            return Ok(());
        };
        self.seen
            .lock()
            .expect("reply-drop log mutex poisoned")
            .push(context.incarnation());
        if self.generation == 1 {
            drop(reply);
            Err(ExitError::message("drop first reply"))
        } else {
            reply.send(99);
            while context.recv().await.is_some() {}
            Ok(())
        }
    }
}

#[tokio::test]
async fn idempotent_reply_drop_retries_only_on_a_superseding_incarnation() {
    let generations = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "reply-drop",
            RawDef::factory({
                let generations = Arc::clone(&generations);
                let seen = Arc::clone(&seen);
                move || ReplyDropActor {
                    generation: generations.fetch_add(1, Ordering::SeqCst) + 1,
                    seen: Arc::clone(&seen),
                }
            }),
        )
        .expect("valid reply-drop actor");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("reply-drop actor starts");

    let constructions = Cell::new(0usize);
    let reply = actor
        .call_idempotent(
            move |reply| {
                constructions.set(constructions.get() + 1);
                IdempotentMessage::Ask(reply)
            },
            RetryPolicy::new(Duration::from_secs(1), Backoff::Immediate)
                .expect("valid retry policy"),
            Duration::from_secs(5),
        )
        .await
        .expect("idempotent helper retries reply loss");
    assert_eq!(reply.value, 99);
    {
        let seen = seen.lock().expect("reply-drop log mutex poisoned");
        assert_eq!(seen.len(), 2);
        assert!(seen[1].supersedes(seen[0]));
        assert_eq!(reply.incarnation, seen[1]);
    }
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("reply-drop actor stops");
}

#[tokio::test]
async fn idempotent_constructor_time_cannot_extend_the_overall_budget() {
    let gate = ReleaseGate::default();
    gate.release();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "constructor-budget",
            RawOnceDef::new(MappingActor {
                gate,
                values: Arc::new(Mutex::new(Vec::new())),
            }),
        )
        .expect("valid constructor-budget actor");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("constructor-budget actor starts");

    let error = actor
        .call_idempotent(
            |reply| {
                std::thread::sleep(Duration::from_millis(20));
                MappingMessage::Ask(1, reply)
            },
            RetryPolicy::new(Duration::from_secs(1), Backoff::Immediate)
                .expect("valid retry policy"),
            Duration::from_millis(1),
        )
        .await
        .expect_err("message construction cannot create time beyond the overall budget");
    assert_eq!(error.kind, IdempotentCallErrorKind::BudgetExhausted);
    assert!(
        error.attempts.is_empty(),
        "no request reached actor ingress"
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("constructor-budget actor stops");
}

enum SliceMessage {
    Fill,
    Ask(Reply<usize>),
}

struct SliceActor {
    start: ReleaseGate,
}

impl RawActor for SliceActor {
    type Msg = SliceMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.start.wait().await;
        while let Some(message) = context.recv().await {
            if let SliceMessage::Ask(reply) = message {
                reply.send(7);
            }
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn per_attempt_slice_makes_acceptance_timeout_retryable_before_overall_expiry() {
    let start = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "slice",
            RawOnceDef::new(SliceActor {
                start: start.clone(),
            })
            .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid slice actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("slice actor starts");
    actor.try_send(SliceMessage::Fill).expect("queue fills");

    let constructions = Arc::new(AtomicUsize::new(0));
    let call = {
        let actor = actor.clone();
        let constructions = Arc::clone(&constructions);
        tokio::spawn(async move {
            actor
                .call_idempotent(
                    move |reply| {
                        constructions.fetch_add(1, Ordering::SeqCst);
                        SliceMessage::Ask(reply)
                    },
                    RetryPolicy::new(Duration::from_secs(5), Backoff::Immediate)
                        .expect("valid retry policy"),
                    Duration::from_secs(20),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    advance_time(Duration::from_secs(5)).await;
    for _ in 0..8 {
        if constructions.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        constructions.load(Ordering::SeqCst),
        2,
        "the per-attempt slice retries while fifteen seconds remain"
    );
    start.release();
    assert_eq!(
        call.await
            .expect("slice call task joins")
            .expect("second attempt is accepted")
            .value,
        7
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("slice actor stops");
}

struct HoldReplyActor {
    accepted: ReleaseGate,
    calls: Arc<AtomicUsize>,
    held: Option<Reply<usize>>,
}

impl RawActor for HoldReplyActor {
    type Msg = IdempotentMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        while let Some(IdempotentMessage::Ask(reply)) = context.recv().await {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.held = Some(reply);
            self.accepted.release();
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn idempotent_response_timeout_is_terminal_after_exactly_one_send_attempt() {
    let accepted = ReleaseGate::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "response-timeout",
            RawOnceDef::new(HoldReplyActor {
                accepted: accepted.clone(),
                calls: Arc::clone(&calls),
                held: None,
            }),
        )
        .expect("valid response-timeout actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("response actor starts");
    let call = {
        let actor = actor.clone();
        tokio::spawn(async move {
            actor
                .call_idempotent(
                    IdempotentMessage::Ask,
                    RetryPolicy::new(Duration::from_secs(5), Backoff::Immediate)
                        .expect("valid retry policy"),
                    Duration::from_secs(20),
                )
                .await
        })
    };
    accepted.wait().await;
    advance_time(Duration::from_secs(5)).await;
    let error = call
        .await
        .expect("response-timeout task joins")
        .expect_err("accepted response timeout is terminal");
    assert_eq!(error.kind, IdempotentCallErrorKind::ResponseTimedOut);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(error.attempts.len(), 1);
    assert_eq!(error.attempts[0].ended, AttemptEnd::ResponseTimedOut);
    assert!(error.attempts[0].incarnation.is_some());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("response actor stops");
}

#[test]
fn retry_policy_rejects_a_zero_attempt_slice() {
    assert!(RetryPolicy::new(Duration::ZERO, Backoff::Immediate).is_err());
}

#[allow(dead_code)]
fn actor_refs_remain_send_sync<M: Send + 'static>() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ActorRef<M>>();
}
