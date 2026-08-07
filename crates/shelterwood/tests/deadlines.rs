use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use crate::common::{ReleaseGate, advance_time, assert_quiet, poll_once, poll_until};
use shelterwood::{
    Backoff, CallErrorKind, ChildState, ExitError, ExitResult, Jitter, Mailbox, RawActor,
    RawContext, RawOnceDef, Readiness, ReadinessDeadline, Reply, ReplyError, RestartCondition,
    RestartPolicy, SendErrorKind, Shutdown, TaskDef, Tree,
};

#[derive(Debug)]
enum Message {
    Value(usize),
    Ask(Reply<usize>),
}

struct BoundaryActor {
    gate: Option<ReleaseGate>,
    values: Arc<Mutex<Vec<usize>>>,
    calls: Arc<AtomicUsize>,
    hold_reply: bool,
    held: Option<Reply<usize>>,
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl RawActor for BoundaryActor {
    type Msg = Message;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if let Some(gate) = self.gate.take() {
            gate.wait().await;
        }
        while let Some(message) = context.recv().await {
            match message {
                Message::Value(value) => self
                    .values
                    .lock()
                    .expect("values mutex poisoned")
                    .push(value),
                Message::Ask(reply) => {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    if self.hold_reply {
                        self.held = Some(reply);
                    } else {
                        reply.send(17);
                    }
                }
            }
        }
        Ok(())
    }
}

fn boundary_actor(
    gate: Option<ReleaseGate>,
    values: &Arc<Mutex<Vec<usize>>>,
    calls: &Arc<AtomicUsize>,
    hold_reply: bool,
) -> BoundaryActor {
    BoundaryActor {
        gate,
        values: Arc::clone(values),
        calls: Arc::clone(calls),
        hold_reply,
        held: None,
    }
}

#[tokio::test(start_paused = true)]
async fn acceptance_winning_the_deadline_withdrawal_race_succeeds() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "deadline-race",
            RawOnceDef::new(boundary_actor(Some(gate.clone()), &values, &calls, false))
                .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let accepting = actor.try_send(Message::Value(1)).expect("queue fills");
    let deadline = Duration::from_secs(10);
    let mut timed = Box::pin(actor.send_timeout(Message::Value(2), deadline));
    assert!(poll_once(timed.as_mut()).is_pending());

    advance_time(deadline).await;
    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values.lock().expect("values mutex poisoned").contains(&1)
        })
        .await
    );
    assert!(
        matches!(poll_once(timed.as_mut()), Poll::Ready(Ok(value)) if value == accepting),
        "acceptance is checked before successful withdrawal at the boundary"
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
}

#[tokio::test(start_paused = true)]
async fn zero_deadlines_short_circuit_without_acceptance_or_message_construction() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let constructed = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "zero",
            RawOnceDef::new(boundary_actor(Some(gate.clone()), &values, &calls, false))
                .mailbox(Mailbox::queue(2).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let accepting = actor
        .try_send(Message::Value(1))
        .expect("identity probe accepts");

    let timed = actor
        .send_timeout(Message::Value(2), Duration::ZERO)
        .await
        .expect_err("zero send deadline short-circuits");
    assert_eq!(timed.kind, SendErrorKind::TimedOut);
    assert_eq!(timed.incarnation_observed, Some(accepting));
    let constructed_in_call = Arc::clone(&constructed);
    let call = actor
        .call(
            move |reply| {
                constructed_in_call.store(true, Ordering::SeqCst);
                Message::Ask(reply)
            },
            Duration::ZERO,
        )
        .await
        .expect_err("zero call deadline short-circuits");
    assert_eq!(call.kind, CallErrorKind::AcceptanceTimedOut);
    assert_eq!(call.incarnation_observed, Some(accepting));
    assert!(!constructed.load(Ordering::SeqCst));

    gate.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            values.lock().expect("values mutex poisoned").as_slice() == [1]
        })
        .await
    );
    assert_quiet(Duration::from_millis(20), || {
        values.lock().expect("values mutex poisoned").contains(&2)
            || calls.load(Ordering::SeqCst) != 0
    })
    .await;
    assert!(matches!(timed.message, Message::Value(2)));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test(start_paused = true)]
async fn preacceptance_expiry_and_terminality_follow_the_identity_table() {
    let values = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "unbound",
            RawOnceDef::new(boundary_actor(None, &values, &calls, false)),
        )
        .expect("valid actor");
    let width = Duration::from_secs(10);

    let timed_actor = actor.clone();
    let timed =
        tokio::spawn(async move { timed_actor.send_timeout(Message::Value(1), width).await });
    tokio::task::yield_now().await;
    advance_time(width).await;
    let timed = timed
        .await
        .expect("timed send joins")
        .expect_err("unbound send expires");
    assert_eq!(timed.kind, SendErrorKind::TimedOut);
    assert_eq!(timed.incarnation_observed, None);

    let call_actor = actor.clone();
    let call = tokio::spawn(async move { call_actor.call(Message::Ask, width).await });
    tokio::task::yield_now().await;
    advance_time(width).await;
    let call = call
        .await
        .expect("call task joins")
        .expect_err("unbound call expires");
    assert_eq!(call.kind, CallErrorKind::AcceptanceTimedOut);
    assert_eq!(call.incarnation_observed, None);

    drop(tree);
    let terminal_send = actor
        .send(timed.message)
        .await
        .expect_err("never-started actor is terminal");
    assert_eq!(terminal_send.kind, SendErrorKind::Terminated);
    assert_eq!(terminal_send.incarnation_observed, None);
    let terminal_call = actor
        .call(Message::Ask, width)
        .await
        .expect_err("never-started call is terminal");
    assert_eq!(terminal_call.kind, CallErrorKind::Terminated);
    assert_eq!(terminal_call.incarnation_observed, None);
}

#[tokio::test(start_paused = true)]
async fn call_uses_one_budget_across_acceptance_and_response() {
    let gate = ReleaseGate::default();
    let values = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "one-budget",
            RawOnceDef::new(boundary_actor(Some(gate.clone()), &values, &calls, true))
                .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let accepting = actor.try_send(Message::Value(1)).expect("queue fills");
    let budget = Duration::from_secs(10);
    let call_actor = actor.clone();
    let call = tokio::spawn(async move { call_actor.call(Message::Ask, budget).await });
    tokio::task::yield_now().await;

    advance_time(Duration::from_secs(6)).await;
    gate.release();
    for _ in 0..32 {
        if calls.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        !call.is_finished(),
        "response still has the remaining budget"
    );
    advance_time(Duration::from_secs(4)).await;
    let error = call
        .await
        .expect("call task joins")
        .expect_err("one overall budget expires");
    assert_eq!(error.kind, CallErrorKind::ResponseTimedOut);
    assert_eq!(error.incarnation_observed, Some(accepting));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test(start_paused = true)]
async fn message_construction_consumes_the_call_budget() {
    let values = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "constructor-budget",
            RawOnceDef::new(boundary_actor(None, &values, &calls, false))
                .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    let budget = Duration::from_secs(10);
    let mut call = Box::pin(actor.call(
        move |reply| {
            // Advance the paused clock synchronously while the user
            // constructor is running. Its first poll advances before the
            // yield point, so this does not depend on scheduler timing.
            let mut advance = Box::pin(tokio::time::advance(budget + Duration::from_nanos(1)));
            assert!(poll_once(advance.as_mut()).is_pending());
            Message::Ask(reply)
        },
        budget,
    ));

    let Poll::Ready(result) = poll_once(call.as_mut()) else {
        panic!("construction itself exhausts the captured budget");
    };
    let error = result.expect_err("the over-budget call times out");
    assert_eq!(error.kind, CallErrorKind::AcceptanceTimedOut);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "an available mailbox must not accept work after construction exhausts the budget"
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test(start_paused = true)]
async fn overflowing_readiness_deadline_remains_pending() {
    let mut tree = Tree::new();
    tree.add_task(
        "manual",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::bounded(Duration::MAX).expect("non-zero deadline")),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");

    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    let mut startup = Box::pin(system.wait_started());
    assert!(poll_once(startup.as_mut()).is_pending());
    assert!(matches!(
        system
            .scope()
            .child("manual")
            .expect("manual task is resident")
            .state,
        ChildState::Starting
    ));
    drop(startup);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("cooperative task stops");
}

#[tokio::test(start_paused = true)]
async fn overflowing_shutdown_grace_does_not_escalate() {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_task(
        "stubborn",
        TaskDef::new({
            let dropped = Arc::clone(&dropped);
            move |context| {
                let dropped = Arc::clone(&dropped);
                async move {
                    let _drop = DropFlag(dropped);
                    context.shutdown_token().cancelled().await;
                    std::future::pending::<ExitResult>().await
                }
            }
        })
        .shutdown(Shutdown::Graceful {
            grace: Duration::MAX,
        }),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");

    system.scope().request_shutdown();
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert!(
        !dropped.load(Ordering::SeqCst),
        "an overflowing grace must not trigger immediate escalation"
    );

    let shutdown = system.shutdown(Duration::ZERO).await;
    assert!(shutdown.is_err(), "forced cleanup reports the straggler");
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn overflowing_restart_delay_never_restarts_immediately() {
    let release_failure = ReleaseGate::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.add_task(
        "restart",
        TaskDef::new({
            let release_failure = release_failure.clone();
            let starts = Arc::clone(&starts);
            move |_| {
                let release_failure = release_failure.clone();
                let attempt = starts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        release_failure.wait().await;
                        Err(ExitError::message("restart me"))
                    } else {
                        std::future::pending().await
                    }
                }
            }
        })
        .restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(Duration::MAX, Jitter::None).expect("non-zero backoff"),
        )),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("first incarnation starts");
    release_failure.release();

    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            system.scope().child("restart").is_some_and(|child| {
                matches!(child.state, ChildState::Restarting) && child.restart_at.is_none()
            })
        })
        .await
    );
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("restart window shuts down");
}

#[tokio::test(start_paused = true)]
async fn zero_duration_wait_observes_an_already_satisfied_child() {
    let mut tree = Tree::new();
    tree.add_task(
        "ready",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        }),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");

    let child = system
        .scope()
        .wait_for_child(
            "ready",
            |child| matches!(child.state, ChildState::Running),
            Duration::ZERO,
        )
        .await
        .expect("the current snapshot satisfies the predicate");
    assert_eq!(child.id.as_str(), "ready");

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("task stops");
}

#[tokio::test]
async fn reply_receiver_reports_drop_and_is_safe_to_abandon() {
    let (reply, receiver) = Reply::<usize>::channel();
    drop(reply);
    assert_eq!(
        receiver.recv(Duration::from_secs(1)).await,
        Err(ReplyError::Dropped)
    );

    let (reply, receiver) = Reply::<usize>::channel();
    drop(receiver);
    reply.send(1);
}
