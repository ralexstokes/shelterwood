//! Multithreaded mailbox stress (review gap T-1a).
//!
//! Many senders race one receiver and one lifecycle controller through bind,
//! freeze, close and terminate. Every message carries a unique id and a
//! ledger entry, and the assertions are about each message's fate, not the
//! schedule: whatever interleaving ran, each message must be destroyed exactly
//! once and must end in exactly one of the SPEC §5 outcomes — delivered to the
//! incarnation that accepted it, returned to its sender in a `SendError`, or
//! (only when accepted and unread, or withdrawn by cancellation) disposed by
//! the framework. A lost wake shows up as a sender that never resolves, which
//! the per-task deadline turns into a failure instead of a hang.

use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use super::{
    MailboxCell, MailboxReceiver,
    tests::{bind, close, configure, freeze, prepare_termination},
};
use crate::{
    mailbox::{ActorRef, Incarnation, SendError, SendErrorKind},
    policy::ResolvedMailbox,
    runtime::{JoinOutcome, Timeout, join, spawn, timeout, yield_now},
    test_support::mint_actor_membership,
};

const SENDERS: usize = 8;
const MESSAGES_PER_SENDER: usize = 400;
/// The controller terminates once senders have started this share of their
/// messages, so the remainder races a terminal mailbox.
const TERMINATE_AFTER_PERCENT: usize = 75;
/// Generous next to the test's real runtime (well under a second); only a
/// lost wake or a deadlock should ever reach it.
/// Independent rounds per policy; each is a fresh mailbox and ledger.
const ROUNDS: usize = 25;
const STALL: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Style {
    /// Plain `send`: resolves only through acceptance or termination, so a
    /// lost wake strands it.
    Send,
    /// `send_timeout` with a budget short enough to expire regularly.
    ShortTimeout,
    /// `send_timeout` with a budget that should almost never expire.
    LongTimeout,
    /// `send` polled a few times and then dropped mid-wait.
    Cancelled,
}

impl Style {
    fn of(sequence: usize) -> Self {
        match sequence % 4 {
            0 => Self::Send,
            1 => Self::ShortTimeout,
            2 => Self::LongTimeout,
            _ => Self::Cancelled,
        }
    }
}

#[derive(Default)]
struct Record {
    drops: usize,
    deliveries: Vec<(Incarnation, usize)>,
    accepted: Option<Incarnation>,
    returned: Option<SendErrorKind>,
    cancelled_pending: bool,
    resolved: bool,
}

struct Ledger {
    records: Vec<Mutex<Record>>,
    dropped: AtomicUsize,
    delivery_order: AtomicUsize,
    started: AtomicUsize,
}

impl Ledger {
    fn new(total: usize) -> Arc<Self> {
        Arc::new(Self {
            records: (0..total).map(|_| Mutex::new(Record::default())).collect(),
            dropped: AtomicUsize::new(0),
            delivery_order: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
        })
    }

    fn record(&self, id: usize) -> std::sync::MutexGuard<'_, Record> {
        self.records[id].lock().expect("ledger record mutex")
    }
}

/// A message whose destruction is recorded in the ledger. Delivery and
/// return are recorded explicitly by whoever received the value, so a drop
/// that neither preceded is a framework disposal.
struct Probe {
    id: usize,
    ledger: Arc<Ledger>,
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.ledger.record(self.id).drops += 1;
        self.ledger.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// Minimal xorshift so delays vary per sender without a dependency.
struct Jitter(u64);

impl Jitter {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

async fn yields(count: u64) {
    for _ in 0..count {
        yield_now().await;
    }
}

fn record_error(ledger: &Ledger, id: usize, style: Style, error: SendError<Probe>) {
    assert_eq!(error.message.id, id, "a send error returns its own message");
    let allowed = match style {
        Style::Send | Style::Cancelled => error.kind == SendErrorKind::Terminated,
        Style::ShortTimeout | Style::LongTimeout => matches!(
            error.kind,
            SendErrorKind::Terminated | SendErrorKind::TimedOut
        ),
    };
    assert!(allowed, "{style:?} send {id} failed with {:?}", error.kind);
    {
        let mut record = ledger.record(id);
        assert!(record.returned.is_none(), "message {id} returned twice");
        record.returned = Some(error.kind);
        record.resolved = true;
    }
    drop(error);
}

async fn run_sender(sender: usize, actor: ActorRef<Probe>, ledger: Arc<Ledger>) {
    let mut jitter = Jitter(0x9e37_79b9_7f4a_7c15 ^ (sender as u64 + 1));
    for sequence in 0..MESSAGES_PER_SENDER {
        let id = sender * MESSAGES_PER_SENDER + sequence;
        let probe = Probe {
            id,
            ledger: Arc::clone(&ledger),
        };
        let style = Style::of(sequence + sender);
        ledger.started.fetch_add(1, Ordering::Relaxed);
        let outcome = match style {
            Style::Send => Some(actor.send(probe).await),
            Style::ShortTimeout => {
                let budget = Duration::from_micros(jitter.next(200));
                Some(actor.send_timeout(probe, budget).await)
            }
            Style::LongTimeout => Some(actor.send_timeout(probe, STALL).await),
            Style::Cancelled => {
                let polls = 1 + jitter.next(4);
                let mut send: Pin<Box<_>> = Box::pin(actor.send(probe));
                let mut outcome = None;
                for _ in 0..polls {
                    outcome = std::future::poll_fn(|context| {
                        Poll::Ready(match send.as_mut().poll(context) {
                            Poll::Ready(result) => Some(result),
                            Poll::Pending => None,
                        })
                    })
                    .await;
                    if outcome.is_some() {
                        break;
                    }
                    yield_now().await;
                }
                if outcome.is_none() {
                    ledger.record(id).cancelled_pending = true;
                    // Dropping the parked future withdraws it; the message is
                    // then disposed unless acceptance already won.
                    drop(send);
                }
                outcome
            }
        };
        match outcome {
            Some(Ok(incarnation)) => {
                let mut record = ledger.record(id);
                assert!(record.accepted.is_none(), "message {id} accepted twice");
                record.accepted = Some(incarnation);
                record.resolved = true;
            }
            Some(Err(error)) => record_error(&ledger, id, style, error),
            None => {}
        }
        yields(jitter.next(3)).await;
    }
}

async fn run_receiver(
    mailbox: Arc<MailboxCell<Probe>>,
    current: Arc<Mutex<Option<Incarnation>>>,
    done: Arc<AtomicBool>,
    ledger: Arc<Ledger>,
) {
    let mut receiver: Option<MailboxReceiver<Probe>> = None;
    let mut pass = 0usize;
    while !done.load(Ordering::Acquire) {
        pass += 1;
        let incarnation = *current.lock().expect("current incarnation mutex");
        if let Some(incarnation) = incarnation
            && receiver.as_ref().map(|r| r.incarnation) != Some(incarnation)
        {
            receiver = Some(MailboxReceiver::new(Arc::clone(&mailbox), incarnation));
        }
        // A receiver for a closed incarnation stays in use until the
        // controller publishes the next one; the cell must refuse it.
        let Some(active) = receiver.as_ref() else {
            yield_now().await;
            continue;
        };
        let message = if pass % 3 == 0 {
            active.try_recv_live_through(active.accepted_sequence())
        } else {
            active.try_recv()
        };
        match message {
            Some(message) => {
                let order = ledger.delivery_order.fetch_add(1, Ordering::SeqCst);
                ledger
                    .record(message.id)
                    .deliveries
                    .push((active.incarnation, order));
                drop(message);
            }
            None => yield_now().await,
        }
        // Keep the receiver slower than the senders so queues fill, senders
        // park, and closes find unread payload.
        if pass % 2 == 0 {
            yield_now().await;
        }
    }
}

async fn run_controller(
    mailbox: Arc<MailboxCell<Probe>>,
    policy: ResolvedMailbox,
    current: Arc<Mutex<Option<Incarnation>>>,
    ledger: Arc<Ledger>,
) {
    let (_, mut incarnations) = mint_actor_membership();
    let mut token = configure(&mailbox, policy);
    let terminate_at = SENDERS * MESSAGES_PER_SENDER * TERMINATE_AFTER_PERCENT / 100;
    for cycle in 0.. {
        let incarnation = incarnations.mint().expect("incarnation available");
        bind(&mailbox, token, incarnation);
        *current.lock().expect("current incarnation mutex") = Some(incarnation);
        yields(8 + (cycle as u64 % 7) * 4).await;
        if cycle % 2 == 0 {
            freeze(&mailbox, incarnation);
            yields(4).await;
        }
        if ledger.started.load(Ordering::Relaxed) >= terminate_at {
            // Terminate straight from a live binding: the queued payload and
            // every parked sender leave through termination, not close.
            break;
        }
        let closed = close(&mailbox, incarnation).expect("the live incarnation closes");
        *current.lock().expect("current incarnation mutex") = None;
        let (next, payload) = closed.into_parts();
        token = next;
        // Unread payload: disposed without delivery or return.
        drop(payload);
        yields(cycle as u64 % 5).await;
    }
    let teardown = prepare_termination(&mailbox).expect("a live mailbox terminalizes once");
    drop(teardown.finish());
}

/// Per-round counts of each message fate, in [`FATES`] order.
type Fates = [usize; 5];

const FATES: [&str; 5] = [
    "delivered",
    "disposed unread",
    "withdrawn by cancellation",
    "timed out",
    "terminated",
];

async fn stress(policy: ResolvedMailbox) {
    let started = Instant::now();
    let mut totals: Fates = [0; 5];
    for _ in 0..ROUNDS {
        let fates = stress_round(policy).await;
        for (total, count) in totals.iter_mut().zip(fates) {
            *total += count;
        }
    }
    eprintln!(
        "{policy:?}: {ROUNDS} rounds in {:?}: {totals:?}",
        started.elapsed()
    );
    // Non-vacuity: the schedules exercised every fate the assertions classify.
    for (fate, count) in FATES.iter().zip(totals) {
        assert!(count > 0, "no message was {fate} ({policy:?})");
    }
}

async fn stress_round(policy: ResolvedMailbox) -> Fates {
    let total = SENDERS * MESSAGES_PER_SENDER;
    let ledger = Ledger::new(total);
    let (mailbox, actor) = super::tests::actor_for::<Probe>();
    let current = Arc::new(Mutex::new(None));
    let done = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let receiver = spawn(run_receiver(
        Arc::clone(&mailbox),
        Arc::clone(&current),
        Arc::clone(&done),
        Arc::clone(&ledger),
    ));
    let senders: Vec<_> = (0..SENDERS)
        .map(|sender| spawn(run_sender(sender, actor.clone(), Arc::clone(&ledger))))
        .collect();
    let controller = spawn(run_controller(
        Arc::clone(&mailbox),
        policy,
        Arc::clone(&current),
        Arc::clone(&ledger),
    ));

    let deadline = started + STALL;
    let joined = |name: &'static str, outcome: Timeout<JoinOutcome<()>>| match outcome {
        Timeout::Completed(JoinOutcome::Ok { value: () }) => {}
        Timeout::Completed(_) => panic!("{name} task did not complete"),
        Timeout::Elapsed => panic!("{name} task stalled: a lost wake or a deadlock"),
    };
    joined(
        "controller",
        timeout(
            deadline.saturating_duration_since(Instant::now()),
            join(controller),
        )
        .await,
    );
    for sender in senders {
        joined(
            "sender",
            timeout(
                deadline.saturating_duration_since(Instant::now()),
                join(sender),
            )
            .await,
        );
    }
    done.store(true, Ordering::Release);
    joined(
        "receiver",
        timeout(
            deadline.saturating_duration_since(Instant::now()),
            join(receiver),
        )
        .await,
    );
    drop(actor);
    drop(mailbox);

    // Withdrawn and unread messages may still be on the detached disposal
    // lane; wait for it, bounded by the same stall budget.
    while ledger.dropped.load(Ordering::SeqCst) < total {
        assert!(
            Instant::now() < deadline,
            "{} of {total} messages were never destroyed",
            total - ledger.dropped.load(Ordering::SeqCst)
        );
        yield_now().await;
    }

    let mut delivered = 0;
    let mut disposed_unread = 0;
    let mut withdrawn = 0;
    let mut timed_out = 0;
    let mut terminated = 0;
    let mut last_delivery = vec![None::<(usize, Incarnation)>; SENDERS];
    let mut deliveries: Vec<(usize, usize, Incarnation)> = Vec::new();
    for (id, record) in ledger.records.iter().enumerate() {
        let record = record.lock().expect("ledger record mutex");
        assert_eq!(
            record.drops, 1,
            "message {id} destroyed {} times",
            record.drops
        );
        assert!(
            record.deliveries.len() <= 1,
            "message {id} delivered {} times",
            record.deliveries.len()
        );
        assert!(
            record.resolved || record.cancelled_pending,
            "message {id}'s send neither resolved nor was cancelled"
        );
        if let Some(kind) = record.returned {
            assert!(
                record.deliveries.is_empty() && record.accepted.is_none(),
                "message {id} was returned ({kind:?}) after acceptance or delivery"
            );
            match kind {
                SendErrorKind::TimedOut => timed_out += 1,
                _ => terminated += 1,
            }
        }
        match (record.deliveries.first(), record.accepted) {
            (Some(&(receiver, order)), accepted) => {
                assert!(
                    accepted.is_none_or(|accepted| accepted == receiver),
                    "message {id} accepted by {accepted:?} but delivered to {receiver:?}"
                );
                assert!(
                    accepted.is_some() || record.cancelled_pending,
                    "message {id} delivered without an acceptance"
                );
                deliveries.push((order, id, receiver));
                delivered += 1;
            }
            (None, Some(_)) => disposed_unread += 1,
            (None, None) if record.returned.is_none() => {
                assert!(record.cancelled_pending);
                withdrawn += 1;
            }
            (None, None) => {}
        }
    }
    // Each sender submits sequentially, so its delivered messages must arrive
    // in submission order and never regress to an older incarnation.
    deliveries.sort_unstable_by_key(|&(order, _, _)| order);
    for (_, id, incarnation) in deliveries {
        let sender = id / MESSAGES_PER_SENDER;
        if let Some((previous, previous_incarnation)) = last_delivery[sender] {
            assert!(
                previous < id,
                "sender {sender} delivered {id} after {previous}"
            );
            assert!(
                incarnation == previous_incarnation || incarnation.supersedes(previous_incarnation),
                "sender {sender}'s delivery regressed to an older incarnation"
            );
        }
        last_delivery[sender] = Some((id, incarnation));
    }

    [delivered, disposed_unread, withdrawn, timed_out, terminated]
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_queue_senders_resolve_every_message_exactly_once() {
    stress(ResolvedMailbox::Queue(
        NonZeroUsize::new(2).expect("non-zero queue capacity"),
    ))
    .await;
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_latest_senders_resolve_every_message_exactly_once() {
    stress(ResolvedMailbox::Latest).await;
}
