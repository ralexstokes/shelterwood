mod common;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    DestructorBlocker, DestructorGate, ReleaseGate, SHUTDOWN_BUDGET, advance_time,
    assert_eventually, assert_eventually_frozen, policy::never, poll_once,
};
use shelterwood::{
    Backoff, CallErrorKind, ExitError, ExitResult, Incarnation, Jitter, Mailbox, RawActor,
    RawContext, RawDef, Reply, RestartCondition, RestartPolicy, SendErrorKind, Tree,
};

struct RestartActor {
    generation: usize,
    fail_first: ReleaseGate,
    _blocker: Option<DestructorBlocker>,
    deliveries: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl RawActor for RestartActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.generation == 1 {
            self.fail_first.wait().await;
            return Err(ExitError::message("replace first incarnation"));
        }
        while let Some(message) = context.recv().await {
            self.deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .push((self.generation, message));
        }
        Ok(())
    }
}

fn restarting_definition(
    factories: &Arc<AtomicUsize>,
    fail_first: &ReleaseGate,
    destructor: &DestructorGate,
    deliveries: &Arc<Mutex<Vec<(usize, usize)>>>,
    block_first: bool,
    restart: RestartPolicy,
) -> RawDef<RestartActor> {
    RawDef::factory({
        let factories = Arc::clone(factories);
        let fail_first = fail_first.clone();
        let destructor = destructor.clone();
        let deliveries = Arc::clone(deliveries);
        move || {
            let generation = factories.fetch_add(1, Ordering::SeqCst) + 1;
            RestartActor {
                generation,
                fail_first: fail_first.clone(),
                _blocker: (block_first && generation == 1).then(|| destructor.blocker()),
                deliveries: Arc::clone(&deliveries),
            }
        }
    })
    .mailbox(Mailbox::queue(1).expect("non-zero capacity"))
    .restart(restart)
}

enum EvidenceMessage {
    Value,
    Ask(Reply<usize>),
}

struct EvidenceRestartActor {
    generation: usize,
    fail_first: ReleaseGate,
    hold_replacement: ReleaseGate,
    complete_first: bool,
}

impl RawActor for EvidenceRestartActor {
    type Msg = EvidenceMessage;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.generation == 1 {
            self.fail_first.wait().await;
            return if self.complete_first {
                Ok(())
            } else {
                Err(ExitError::message("replace first incarnation"))
            };
        }
        self.hold_replacement.wait().await;
        while let Some(message) = context.recv().await {
            match message {
                EvidenceMessage::Value => {}
                EvidenceMessage::Ask(reply) => reply.send(17),
            }
        }
        Ok(())
    }
}

fn evidence_restarting_definition(
    factories: &Arc<AtomicUsize>,
    fail_first: &ReleaseGate,
    hold_replacement: &ReleaseGate,
    complete_first: bool,
    restart: RestartPolicy,
) -> RawDef<EvidenceRestartActor> {
    RawDef::factory({
        let factories = Arc::clone(factories);
        let fail_first = fail_first.clone();
        let hold_replacement = hold_replacement.clone();
        move || EvidenceRestartActor {
            generation: factories.fetch_add(1, Ordering::SeqCst) + 1,
            fail_first: fail_first.clone(),
            hold_replacement: hold_replacement.clone(),
            complete_first,
        }
    })
    .mailbox(Mailbox::queue(1).expect("non-zero capacity"))
    .restart(restart)
}

async fn wait_for_destructor(destructor: &DestructorGate) {
    let destructor = destructor.clone();
    tokio::task::spawn_blocking(move || destructor.wait_entered())
        .await
        .expect("destructor waiter joins");
}

#[tokio::test(start_paused = true)]
async fn rebind_refreshes_all_overflow_waiter_incarnation_evidence() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let hold_replacement = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "evidence-worker",
            evidence_restarting_definition(
                &factories,
                &fail_first,
                &hold_replacement,
                false,
                RestartPolicy::default(),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    let first = actor
        .try_send(EvidenceMessage::Value)
        .expect("first incarnation queue fills");
    let width = Duration::from_secs(10);

    let mut promoted = Box::pin(actor.send_timeout(EvidenceMessage::Value, width));
    let mut overflow = Box::pin(actor.send_timeout(EvidenceMessage::Value, width));
    let mut call = Box::pin(actor.call(EvidenceMessage::Ask, width));
    assert!(poll_once(promoted.as_mut()).is_pending());
    assert!(poll_once(overflow.as_mut()).is_pending());
    assert!(poll_once(call.as_mut()).is_pending());

    fail_first.release();
    let mut replacement = None;
    assert_eventually_frozen!(|| {
        if let std::task::Poll::Ready(result) = poll_once(promoted.as_mut()) {
            replacement = Some(result.expect("first waiter enters replacement capacity"));
            true
        } else {
            false
        }
    })
    .await;
    let replacement = replacement.expect("replacement bind promotes the first waiter");
    assert!(replacement.supersedes(first));
    assert_eq!(factories.load(Ordering::SeqCst), 2);
    assert!(poll_once(overflow.as_mut()).is_pending());
    assert!(poll_once(call.as_mut()).is_pending());

    tokio::time::advance(width).await;
    let std::task::Poll::Ready(Err(send_error)) = poll_once(overflow.as_mut()) else {
        panic!("overflow send times out");
    };
    assert_eq!(send_error.kind, SendErrorKind::TimedOut);
    assert_eq!(send_error.incarnation_observed, Some(replacement));
    let std::task::Poll::Ready(Err(call_error)) = poll_once(call.as_mut()) else {
        panic!("overflow call times out before acceptance");
    };
    assert_eq!(call_error.kind, CallErrorKind::AcceptanceTimedOut);
    assert_eq!(call_error.incarnation_observed, Some(replacement));

    hold_replacement.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("actor stops");
}

#[tokio::test]
async fn always_restart_carries_parked_send_and_call_across_a_clean_exit() {
    let factories = Arc::new(AtomicUsize::new(0));
    let finish_first = ReleaseGate::default();
    let hold_replacement = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "always-worker",
            evidence_restarting_definition(
                &factories,
                &finish_first,
                &hold_replacement,
                true,
                RestartPolicy::new(RestartCondition::Always, Backoff::Immediate),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    let first = actor
        .try_send(EvidenceMessage::Value)
        .expect("first incarnation queue fills");
    let mut send = Box::pin(actor.send(EvidenceMessage::Value));
    let mut call = Box::pin(actor.call(EvidenceMessage::Ask, Duration::from_secs(30)));
    assert!(poll_once(send.as_mut()).is_pending());
    assert!(poll_once(call.as_mut()).is_pending());

    finish_first.release();
    assert_eventually!(|| factories.load(Ordering::SeqCst) == 2).await;
    hold_replacement.release();
    let accepting = send.await.expect("parked send enters the replacement");
    let replied = call.await.expect("parked call enters the replacement");
    assert!(accepting.supersedes(first));
    assert_eq!(replied.incarnation, accepting);
    assert_eq!(replied.value, 17);
    assert_eq!(factories.load(Ordering::SeqCst), 2);
    system
        .shutdown(SHUTDOWN_BUDGET)
        .await
        .expect("replacement stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_rides_the_frozen_destructor_and_rebind_window() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                true,
                RestartPolicy::default(),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    let first = actor.try_send(1).expect("first incarnation accepts");
    let mut parked = Box::pin(actor.send(42));
    assert!(poll_once(parked.as_mut()).is_pending());

    fail_first.release();
    wait_for_destructor(&destructor).await;
    let frozen = actor
        .try_send(2)
        .expect_err("frozen intake rejects try_send");
    assert_eq!(frozen.kind, SendErrorKind::NotRunning);
    assert_eq!(frozen.incarnation_observed, Some(first));
    assert!(poll_once(parked.as_mut()).is_pending());

    destructor.release();
    let accepting = parked.await.expect("send rides into replacement");
    assert!(accepting.supersedes(first));
    assert_eventually!(|| {
        deliveries
            .lock()
            .expect("deliveries mutex poisoned")
            .as_slice()
            == [(2, 42)]
    })
    .await;
    assert_eq!(
        factories.load(Ordering::SeqCst),
        2,
        "only one replacement starts"
    );
    system.shutdown(SHUTDOWN_BUDGET).await.expect("actor stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_restart_turns_a_frozen_parked_send_into_terminal() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                true,
                never(),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let first = actor.try_send(1).expect("first incarnation accepts");
    let mut parked = Box::pin(actor.send(42));
    assert!(poll_once(parked.as_mut()).is_pending());
    fail_first.release();
    wait_for_destructor(&destructor).await;
    assert!(poll_once(parked.as_mut()).is_pending());
    destructor.release();
    let error = parked.await.expect_err("membership terminalizes");
    assert_eq!(error.kind, SendErrorKind::Terminated);
    assert_eq!(error.incarnation_observed, Some(first));
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
}

#[tokio::test(start_paused = true)]
async fn timed_send_withdraws_while_replacement_is_in_backoff() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let backoff = Duration::from_secs(30);
    let timeout = Duration::from_secs(10);
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                false,
                RestartPolicy::new(
                    RestartCondition::OnFailure,
                    Backoff::fixed(backoff, Jitter::None).expect("non-zero backoff"),
                ),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    let first = actor.try_send(1).expect("first incarnation accepts");
    let mut timed = Box::pin(actor.send_timeout(42, timeout));
    assert!(poll_once(timed.as_mut()).is_pending());
    fail_first.release();
    let mut observed_rebind = None;
    assert_eventually_frozen!(|| {
        match actor.try_send(0) {
            Err(error) if error.kind == SendErrorKind::NotRunning => {
                observed_rebind = Some(error.incarnation_observed);
                true
            }
            _ => false,
        }
    })
    .await;
    assert_eq!(observed_rebind, Some(None));
    tokio::time::advance(timeout).await;
    let error = timed.await.expect_err("backoff outlives timeout");
    assert_eq!(error.kind, SendErrorKind::TimedOut);
    assert_eq!(error.incarnation_observed, Some(first));
    assert_eq!(error.message, 42);
    system.shutdown(Duration::ZERO).await.expect("root stops");
}

/// Dropping a parked send while the mailbox is unbound (the rebind window)
/// withdraws it: the replacement incarnation never sees the message even
/// though promotion runs at the next bind (§5.2's withdrawal rule).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_parked_send_in_the_rebind_window_withdraws_it() {
    let factories = Arc::new(AtomicUsize::new(0));
    let fail_first = ReleaseGate::default();
    let destructor = DestructorGate::default();
    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "worker",
            restarting_definition(
                &factories,
                &fail_first,
                &destructor,
                &deliveries,
                true,
                RestartPolicy::default(),
            ),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    actor.try_send(1).expect("first incarnation accepts");
    let mut parked = Box::pin(actor.send(42));
    assert!(poll_once(parked.as_mut()).is_pending());

    fail_first.release();
    wait_for_destructor(&destructor).await;
    // Park a second send inside the frozen rebind window, then drop it
    // before the replacement binds.
    let mut doomed = Box::pin(actor.send(43));
    assert!(poll_once(doomed.as_mut()).is_pending());
    drop(doomed);

    destructor.release();
    parked.await.expect("the retained send rides the window");
    actor.send(44).await.expect("replacement accepts");
    assert_eventually!(
        || {
            deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .as_slice()
                == [(2, 42), (2, 44)]
        },
        "the withdrawn send must never be promoted at bind: {:?}",
        deliveries.lock().expect("deliveries mutex poisoned")
    )
    .await;
    system.shutdown(SHUTDOWN_BUDGET).await.expect("actor stops");
}

/// A restartable actor whose rebind window is held open by a fixed restart
/// backoff under a paused clock, and whose replacement does not read until
/// released.
///
/// Holding the window on virtual time, rather than on a blocked destructor,
/// keeps every step on one thread and puts the rebind at an exact virtual
/// instant. Holding the replacement's first receive means anything the
/// replacement's mailbox holds when the test looks was accepted by bind
/// promotion, not by a receive making room.
struct WindowActor {
    generation: usize,
    fail_first: ReleaseGate,
    hold_replacement: ReleaseGate,
    deliveries: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl RawActor for WindowActor {
    type Msg = usize;

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        if self.generation == 1 {
            self.fail_first.wait().await;
            return Err(ExitError::message("open the rebind window"));
        }
        self.hold_replacement.wait().await;
        while let Some(message) = context.recv().await {
            self.deliveries
                .lock()
                .expect("deliveries mutex poisoned")
                .push((self.generation, message));
        }
        Ok(())
    }
}

struct WindowFixture {
    fail_first: ReleaseGate,
    hold_replacement: ReleaseGate,
    deliveries: Arc<Mutex<Vec<(usize, usize)>>>,
    factories: Arc<AtomicUsize>,
}

impl WindowFixture {
    fn new() -> Self {
        Self {
            fail_first: ReleaseGate::default(),
            hold_replacement: ReleaseGate::default(),
            deliveries: Arc::new(Mutex::new(Vec::new())),
            factories: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn definition(&self, mailbox: Mailbox, backoff: Duration) -> RawDef<WindowActor> {
        RawDef::factory({
            let factories = Arc::clone(&self.factories);
            let fail_first = self.fail_first.clone();
            let hold_replacement = self.hold_replacement.clone();
            let deliveries = Arc::clone(&self.deliveries);
            move || WindowActor {
                generation: factories.fetch_add(1, Ordering::SeqCst) + 1,
                fail_first: fail_first.clone(),
                hold_replacement: hold_replacement.clone(),
                deliveries: Arc::clone(&deliveries),
            }
        })
        .mailbox(mailbox)
        .restart(RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(backoff, Jitter::None).expect("non-zero backoff"),
        ))
    }

    fn deliveries(&self) -> Vec<(usize, usize)> {
        self.deliveries
            .lock()
            .expect("deliveries mutex poisoned")
            .clone()
    }
}

/// Fails the first incarnation and waits, without moving virtual time, until
/// the membership is unbound: the rebind window proper, not the frozen intake
/// that precedes it.
async fn open_rebind_window(actor: &shelterwood::ActorRef<usize>, fail_first: &ReleaseGate) {
    fail_first.release();
    assert_eventually_frozen!(|| matches!(
        actor.try_send(0),
        Err(error) if error.kind == SendErrorKind::NotRunning
            && error.incarnation_observed.is_none()
    ))
    .await;
}

/// Polls parked sends front to back, collecting each accepting incarnation,
/// and reports whether every one has resolved.
fn resolve_in_order<F>(
    parked: &mut Vec<std::pin::Pin<Box<F>>>,
    accepted: &mut Vec<Incarnation>,
) -> bool
where
    F: std::future::Future<Output = Result<Incarnation, shelterwood::SendError<usize>>>,
{
    while let Some(send) = parked.first_mut() {
        match poll_once(send.as_mut()) {
            std::task::Poll::Ready(result) => {
                accepted.push(result.expect("parked send enters the replacement"));
                drop(parked.remove(0));
            }
            std::task::Poll::Pending => break,
        }
    }
    parked.is_empty()
}

/// Senders parked while the membership is unbound are accepted at bind in
/// the order they parked (review T-11). Capacity equals the number of parked
/// senders, so bind promotes every one of them before the replacement's first
/// receive, and the replacement's delivery order is exactly the promotion
/// order.
#[tokio::test(start_paused = true)]
async fn bind_promotes_senders_parked_in_the_rebind_window_in_arrival_order() {
    let fixture = WindowFixture::new();
    let backoff = Duration::from_secs(5);
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "fifo",
            fixture.definition(Mailbox::queue(4).expect("non-zero capacity"), backoff),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    open_rebind_window(&actor, &fixture.fail_first).await;

    let mut parked: Vec<_> = (10..14).map(|value| Box::pin(actor.send(value))).collect();
    for send in &mut parked {
        assert!(poll_once(send.as_mut()).is_pending(), "unbound sends park");
    }

    advance_time(backoff).await;
    let mut accepted = Vec::new();
    assert_eventually_frozen!(|| resolve_in_order(&mut parked, &mut accepted)).await;
    assert_eq!(fixture.factories.load(Ordering::SeqCst), 2);
    let replacement = accepted[0];
    assert!(
        accepted
            .iter()
            .all(|&incarnation| incarnation == replacement),
        "every parked sender is accepted by the one replacement: {accepted:?}"
    );
    assert!(fixture.deliveries().is_empty(), "nothing is received yet");

    fixture.hold_replacement.release();
    assert_eventually_frozen!(
        || fixture.deliveries().len() == 4,
        "deliveries so far: {:?}",
        fixture.deliveries()
    )
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("replacement stops");
    assert_eq!(
        fixture.deliveries(),
        [(2, 10), (2, 11), (2, 12), (2, 13)],
        "bind promotion accepts parked senders in arrival order"
    );
}

/// A `latest()` mailbox never parks a sender for capacity, only for binding.
/// Senders parked across a restart are all accepted by the replacement at
/// bind, each conflating the one before, so every send succeeds and the
/// newest parked value is the only one delivered (review T-11).
#[tokio::test(start_paused = true)]
async fn latest_mailbox_accepts_senders_parked_across_a_restart_and_keeps_the_newest() {
    let fixture = WindowFixture::new();
    let backoff = Duration::from_secs(5);
    let mut tree = Tree::new();
    let actor = tree
        .add_raw("latest", fixture.definition(Mailbox::latest(), backoff))
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    let first = actor
        .try_send(1)
        .expect("the first incarnation accepts into its slot");
    open_rebind_window(&actor, &fixture.fail_first).await;

    let mut parked: Vec<_> = (21..24).map(|value| Box::pin(actor.send(value))).collect();
    for send in &mut parked {
        assert!(poll_once(send.as_mut()).is_pending(), "unbound sends park");
    }

    advance_time(backoff).await;
    let mut accepted = Vec::new();
    assert_eventually_frozen!(|| resolve_in_order(&mut parked, &mut accepted)).await;
    let replacement = accepted[0];
    assert!(replacement.supersedes(first));
    assert!(
        accepted
            .iter()
            .all(|&incarnation| incarnation == replacement),
        "a conflated sender was still accepted, by the replacement: {accepted:?}"
    );

    fixture.hold_replacement.release();
    assert_eventually_frozen!(
        || !fixture.deliveries().is_empty(),
        "no delivery reached the replacement"
    )
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("replacement stops");
    // The complete history proves the first incarnation's slot did not cross
    // the restart and that conflation destroyed the older parked values
    // rather than delaying them.
    assert_eq!(
        fixture.deliveries(),
        [(2, 23)],
        "only the newest parked value survives bind-time conflation"
    );
}

/// SPEC §5.2 and Appendix B's expiry boundary: an acceptance that wins the
/// race at the deadline instant resolves `send_timeout` successfully; the
/// tie is decided by the withdrawal race, never by clock comparison (review
/// T-11).
///
/// The restart backoff equals the send budget and both are armed at one
/// frozen virtual instant, so the replacement binds exactly when the send's
/// deadline elapses. The send is not polled until after that bind, when its
/// timer has already fired: bind promotion accepted it first, so withdrawal
/// finds it accepted and the send succeeds. The twin in which the rebind
/// comes later is `timed_send_withdraws_while_replacement_is_in_backoff`.
#[tokio::test(start_paused = true)]
async fn bind_promotion_at_the_exact_deadline_resolves_send_timeout_successfully() {
    let fixture = WindowFixture::new();
    let width = Duration::from_secs(10);
    let mut tree = Tree::new();
    let actor = tree
        .add_raw(
            "tie",
            fixture.definition(Mailbox::queue(1).expect("non-zero capacity"), width),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("first actor starts");
    let first = actor.try_send(1).expect("first incarnation accepts");
    open_rebind_window(&actor, &fixture.fail_first).await;

    let started = tokio::time::Instant::now();
    let mut timed = Box::pin(actor.send_timeout(42, width));
    assert!(poll_once(timed.as_mut()).is_pending(), "unbound send parks");

    advance_time(width).await;
    // Bind evidence that leaves the timed send untouched: once bound, the
    // capacity-1 queue is full, holding the promoted message, and a probe is
    // refused and handed back without side effects.
    let mut bound = None;
    assert_eventually_frozen!(|| match actor.try_send(0) {
        Err(error) if error.kind == SendErrorKind::Full => {
            bound = error.incarnation_observed;
            true
        }
        _ => false,
    })
    .await;
    assert_eq!(
        tokio::time::Instant::now(),
        started + width,
        "the send is resolved at its exact deadline instant"
    );

    let std::task::Poll::Ready(result) = poll_once(timed.as_mut()) else {
        panic!("a send whose deadline elapsed resolves on its next poll");
    };
    let accepting = result.expect("acceptance at the deadline instant wins the tie");
    assert!(accepting.supersedes(first));
    assert_eq!(bound, Some(accepting));

    fixture.hold_replacement.release();
    assert_eventually_frozen!(
        || fixture.deliveries() == [(2, 42)],
        "deliveries so far: {:?}",
        fixture.deliveries()
    )
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("replacement stops");
}
