use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, panic_any},
    pin::Pin,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use super::{
    OffloadPoll, OffloadResource, PanicSlot, RawContext, RawRunContext, Rejected,
    SharedOffloadFuture, SharedOffloadState, TimerMessage,
    resources::{EventQueue, QueuedEvent, RawResources},
};
use crate::{
    ChildId, MailboxShutdown, Readiness,
    cells::{MemberCell, ScopeCell},
    identity::ScopeIdentity,
    mailbox::{ActorRef, MailboxCell, MailboxControl, MailboxEffectQueue, actor_ref_from_parts},
    policy::{ResolvedDefaults, ScopeFlavor},
    raw::disposal::RawDisposal,
    runtime::{CompletionGatedLatch, Latch, Signal},
    scope::ScopeRef,
};
use shelterwood_core::panic::{PanicPayload, UnwindPanics, resume_preferred_panic};

/// Builds a live raw incarnation context whose mailbox is configured and
/// bound, so `next_ready` can take the busy path without a driver. The
/// returned latch is the context's own shutdown token.
fn bound_raw_context_for<M: Send + 'static>() -> (RawContext<M>, ActorRef<M>, Latch) {
    let (context, actor, shutdown, _abort) = bound_raw_context_with_abort();
    (context, actor, shutdown)
}

/// [`bound_raw_context_for`], also returning the context's escalation
/// latch.
fn bound_raw_context_with_abort<M: Send + 'static>() -> (RawContext<M>, ActorRef<M>, Latch, Latch) {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("raw-actor");
    let member = MemberCell::new(identity.mint_membership(&id));
    let mailbox = MailboxCell::new(id.clone());
    member.attach_mailbox(mailbox.clone());
    let mut effects = MailboxEffectQueue::default();
    let token = MailboxControl::configure(
        &*mailbox,
        ResolvedDefaults::default().mailbox(),
        &mut effects,
    );
    let incarnation = member.take_incarnation_counter().mint();
    MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);

    let mut scope_identity = ScopeIdentity::new();
    let scope_id = ChildId::from("scope");
    let scope_member = MemberCell::new(scope_identity.mint_membership(&scope_id));
    let scope = ScopeCell::new(scope_member, ScopeFlavor::Ordered, ScopeIdentity::new());

    let myself = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    let shutdown = Latch::default();
    let abort = Latch::default();
    let context = RawContext::new(
        RawRunContext {
            id,
            incarnation,
            member,
            scope: ScopeRef { cell: scope },
            shutdown: shutdown.clone(),
            abort: abort.clone(),
            ready: CompletionGatedLatch::default(),
            local_stop: Latch::default(),
            mailbox_shutdown: MailboxShutdown::Drain,
        },
        myself.clone(),
        mailbox,
        Readiness::Immediate,
    );
    (context, myself, shutdown, abort)
}

fn bound_raw_context() -> (RawContext<u8>, ActorRef<u8>) {
    let (context, actor, _shutdown) = bound_raw_context_for();
    (context, actor)
}

fn marker(value: usize) -> QueuedEvent<usize> {
    QueuedEvent {
        cancellation: Latch::default(),
        make_message: Box::new(move || value),
    }
}

fn value(event: QueuedEvent<usize>) -> usize {
    (event.make_message)()
}

fn panic_message(payload: &PanicPayload) -> Option<&str> {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
}

#[test]
fn primary_panic_precedence_discards_a_secondary_cleanup_panic() {
    let payload = catch_unwind(AssertUnwindSafe(|| {
        resume_preferred_panic(UnwindPanics {
            primary: Some(Box::new("primary actor panic")),
            cleanup: Some(Box::new("secondary cleanup panic")),
        });
    }))
    .expect_err("the primary panic is resumed");
    assert_eq!(panic_message(&payload), Some("primary actor panic"));
}

#[test]
fn the_incarnation_return_path_resumes_a_primary_panic_it_solely_owns() {
    let payload = catch_unwind(AssertUnwindSafe(|| {
        resume_preferred_panic(UnwindPanics {
            primary: Some(Box::new("primary actor panic")),
            cleanup: None,
        });
    }))
    .expect_err("a sole-owned primary panic is never contained");
    assert_eq!(panic_message(&payload), Some("primary actor panic"));

    let payload = catch_unwind(AssertUnwindSafe(|| {
        resume_preferred_panic(UnwindPanics {
            primary: None,
            cleanup: Some(Box::new("cleanup panic")),
        });
    }))
    .expect_err("cleanup stands in when there is no primary panic");
    assert_eq!(panic_message(&payload), Some("cleanup panic"));

    resume_preferred_panic(UnwindPanics {
        primary: None,
        cleanup: None,
    });
}

struct BlockingPollDrop {
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}

struct PanickingDrop(Arc<AtomicUsize>);

struct PanickingWake(&'static str);

/// How long a bounded handshake waits before failing its test.
///
/// Generous enough to survive a loaded shared machine, finite so that a
/// mutation which suppresses the fire under test reports instead of
/// wedging the runner.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A single-use, timeout-bounded signal between two threads.
///
/// `Barrier` is the wrong oracle for these tests: a mutation that stops
/// the completion latch firing at all would block the waiting side
/// forever, turning a regression into a hang. Every wait here expires and
/// panics on the waiting thread instead, where nothing swallows it.
struct Gate {
    sender: mpsc::SyncSender<()>,
    receiver: Mutex<mpsc::Receiver<()>>,
}

impl Gate {
    fn new() -> Arc<Self> {
        let (sender, receiver) = mpsc::sync_channel(1);
        Arc::new(Self {
            sender,
            receiver: Mutex::new(receiver),
        })
    }

    fn open(&self) {
        // A dropped peer means its wait already expired and failed the
        // test; there is nothing left to report here.
        let _ = self.sender.try_send(());
    }

    fn wait(&self, expected: &str) {
        self.receiver
            .lock()
            .expect("gate receiver mutex poisoned")
            .recv_timeout(HANDSHAKE_TIMEOUT)
            .unwrap_or_else(|_| panic!("timed out waiting for {expected}"));
    }
}

/// A wake panic payload that is deliberately not a string.
///
/// An uncontained wake panic reaches the incarnation only as the offload
/// task's join result, which the framework can preserve solely as a
/// `String`. Panicking with an opaque payload is therefore what lets these
/// tests tell the caller's own payload apart from that stand-in.
#[derive(Debug, Eq, PartialEq)]
struct OpaqueWakePanic(&'static str);

struct BlockingPanickingWake {
    entered: Arc<Gate>,
    release: Arc<Gate>,
    message: &'static str,
}

impl Wake for PanickingWake {
    fn wake(self: Arc<Self>) {
        panic!("{}", self.0);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        panic!("{}", self.0);
    }
}

impl BlockingPanickingWake {
    fn block_then_panic(&self) {
        self.entered.open();
        self.release.wait("the test to release the completion wake");
        panic_any(OpaqueWakePanic(self.message));
    }
}

impl Wake for BlockingPanickingWake {
    fn wake(self: Arc<Self>) {
        self.block_then_panic();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.block_then_panic();
    }
}

impl Drop for PanickingDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("contained raw payload destructor panic");
    }
}

struct CountedDrop {
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}

#[derive(Debug)]
struct PanicOnceClone {
    clones: Arc<AtomicUsize>,
    value: u8,
}

impl Clone for PanicOnceClone {
    fn clone(&self) -> Self {
        if self.clones.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("interval message clone panic");
        }
        Self {
            clones: Arc::clone(&self.clones),
            value: self.value,
        }
    }
}

impl Drop for CountedDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("queued continuation destructor panic");
        }
    }
}

impl Future for BlockingPollDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.entered.wait();
        self.release.wait();
        Poll::Pending
    }
}

impl Drop for BlockingPollDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("unit offload destructor panic");
        }
    }
}

#[test]
fn concurrent_pushes_and_timer_watermark_share_one_linearization_point() {
    let queue = Arc::new(EventQueue::default());
    let first_entered = Arc::new(Barrier::new(2));
    let release_first = Arc::new(Barrier::new(2));
    let first = {
        let queue = Arc::clone(&queue);
        let first_entered = Arc::clone(&first_entered);
        let release_first = Arc::clone(&release_first);
        thread::spawn(move || {
            queue.insert_with(marker(1), || {
                first_entered.wait();
                release_first.wait();
            });
        })
    };
    first_entered.wait();

    assert!(
        queue.queue.try_lock().is_err(),
        "FIFO insertion and timer watermarking share one lock"
    );

    let race = Arc::new(Barrier::new(3));
    let second = {
        let queue = Arc::clone(&queue);
        let race = Arc::clone(&race);
        thread::spawn(move || {
            race.wait();
            queue.push(marker(2));
        })
    };
    let watermark = {
        let queue = Arc::clone(&queue);
        let race = Arc::clone(&race);
        thread::spawn(move || {
            race.wait();
            queue.watermark()
        })
    };
    race.wait();
    release_first.wait();
    first.join().expect("first producer completes");
    second.join().expect("second producer completes");
    let mut watermark = watermark.join().expect("watermark reader completes");

    assert!(matches!(watermark, 1 | 2));
    assert_eq!(
        value(queue.pop_through(&mut watermark).expect("first is covered")),
        1
    );
    if watermark == 1 {
        assert_eq!(
            value(
                queue
                    .pop_through(&mut watermark)
                    .expect("second is covered")
            ),
            2
        );
    } else {
        assert!(queue.pop_through(&mut watermark).is_none());
        assert_eq!(value(queue.pop().expect("second follows watermark")), 2);
    }
    assert!(queue.pop().is_none());
}

#[test]
fn timer_watermark_drains_exactly_the_preexisting_fifo_prefix() {
    let queue = EventQueue::default();
    queue.push(marker(1));
    queue.push(marker(2));
    let mut watermark = queue.watermark();
    queue.push(marker(3));

    assert_eq!(
        value(queue.pop_through(&mut watermark).expect("first is covered")),
        1
    );
    assert_eq!(
        value(
            queue
                .pop_through(&mut watermark)
                .expect("second is covered")
        ),
        2
    );
    assert!(queue.pop_through(&mut watermark).is_none());
    assert_eq!(
        value(queue.pop().expect("post-watermark event remains queued")),
        3
    );
}

#[crate::runtime::test]
async fn event_visibility_precedes_signal_without_losing_the_wakeup() {
    let queue = Arc::new(EventQueue::default());
    let mut watcher = queue.signal.watcher();
    let inserted = Arc::new(Barrier::new(2));
    let release_signal = Arc::new(Barrier::new(2));
    let producer = {
        let queue = Arc::clone(&queue);
        let inserted = Arc::clone(&inserted);
        let release_signal = Arc::clone(&release_signal);
        thread::spawn(move || {
            queue.push_with_hooks(
                marker(7),
                || {},
                || {
                    inserted.wait();
                    release_signal.wait();
                },
            );
        })
    };
    inserted.wait();

    let mut watermark = queue.watermark();
    assert_eq!(watermark, 1);
    assert_eq!(
        value(
            queue
                .pop_through(&mut watermark)
                .expect("inserted event is visible before its signal")
        ),
        7
    );
    release_signal.wait();
    producer.join().expect("producer completes");
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(1), watcher.changed()).await,
        crate::runtime::Timeout::Completed(())
    ));
}

#[test]
fn polling_cancellation_drops_outside_the_mutex_and_is_idempotent() {
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let drops = Arc::new(AtomicUsize::new(0));
    let panic = Arc::new(PanicSlot::default());
    let finished = Latch::default();
    let state = SharedOffloadState::new(
        Box::pin(BlockingPollDrop {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            drops: Arc::clone(&drops),
            panic_on_drop: true,
        }),
        RawDisposal {
            panic: Arc::clone(&panic),
            signal: Signal::default(),
        },
        finished.clone(),
    );
    let poller = {
        let state = Arc::clone(&state);
        thread::spawn(move || {
            let mut future = SharedOffloadFuture(state);
            let waker = Waker::noop();
            let mut context = Context::from_waker(waker);
            assert!(Pin::new(&mut future).poll(&mut context).is_ready());
        })
    };
    entered.wait();

    state.cancel();
    assert!(
        finished.is_fired(),
        "cancellation always signals completion"
    );
    state.cancel();
    release.wait();
    poller.join().expect("the destructor panic stays contained");

    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(state.state.lock().is_ok(), "offload mutex is not poisoned");
    let payload = panic.take().expect("destructor panic is retained");
    assert_eq!(
        panic_message(&payload),
        Some("unit offload destructor panic")
    );
}

#[test]
fn absent_offload_future_does_not_claim_a_poller() {
    let state = SharedOffloadState::new(
        Box::pin(std::future::pending()),
        RawDisposal::default(),
        Latch::default(),
    );
    let future = state.take_for_poll().expect("fixture future is present");
    let dispose = state.finish_poll(future, OffloadPoll::Finished);
    state.dispose(dispose);

    assert!(state.take_for_poll().is_none());
    assert!(
        !state
            .state
            .lock()
            .expect("offload future mutex starts healthy")
            .polling,
        "a contract-violating re-poll with no future cannot leave a poller claimed"
    );
}

#[test]
fn finished_offload_ledger_entries_are_reclaimed() {
    let mut resources = RawResources::<()>::default();
    for finished in [true, false, true] {
        let completion = Latch::default();
        if finished {
            completion.fire();
        }
        resources.offloads.push(OffloadResource {
            cancellation: Latch::default(),
            finished: completion,
            state: None,
            task: None,
        });
    }

    resources.reclaim_finished();

    assert_eq!(
        resources.offloads.len(),
        1,
        "reclamation retains exactly the unfinished offloads"
    );
    assert!(!resources.offloads[0].finished.is_fired());
}

/// The ordinary completion path — no freeze, no cancellation — retains a
/// `Guard::finished()` waiter's wake panic, and the ledger may not retire
/// the work until it has.
///
/// The publication ordering itself is pinned by
/// `offload::tests::finished_publication_releases_wake_effect_to_reclaimer`;
/// this test covers panic retention, while the companion below covers the
/// `task`-bearing chain this one omits.
#[test]
fn ordinary_offload_completion_retains_a_finished_waiter_wake_panic() {
    let mut resources = RawResources::<()>::default();
    let panic = Arc::clone(&resources.disposal.panic);
    let finished = Latch::default();
    let mut waiter = Box::pin(finished.fired());
    let entered = Gate::new();
    let release = Gate::new();
    let hostile = Waker::from(Arc::new(BlockingPanickingWake {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        message: "ordinary finished wake panic",
    }));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );

    let state = SharedOffloadState::new(
        Box::pin(async {}),
        resources.disposal.clone(),
        finished.clone(),
    );
    resources.offloads.push(OffloadResource {
        cancellation: Latch::default(),
        finished: finished.clone(),
        state: Some(Arc::clone(&state)),
        task: None,
    });
    let poller = thread::spawn(move || {
        let mut work = SharedOffloadFuture(state);
        assert!(
            Pin::new(&mut work)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready(),
            "a caller wake panic does not escape through the offload task"
        );
    });
    entered.wait("the completion wake to reach the hostile waiter");

    // Sample the mid-wake state, then release before asserting on it: an
    // assertion that fires while the wake is still parked would strand the
    // peer on its timeout and report the expiry instead of the mismatch.
    let fired_mid_wake = finished.is_fired();
    resources.reclaim_finished();
    let pinned_mid_wake = resources.offloads.len();
    let recorded_mid_wake = panic.take();
    release.open();

    assert!(fired_mid_wake, "all completion waiters were released");
    assert_eq!(
        pinned_mid_wake, 1,
        "the fired bit cannot retire work before its hostile wake is contained"
    );
    assert!(
        recorded_mid_wake.is_none(),
        "an actor turn can precede the blocked wake's panic"
    );

    poller.join().expect("the caller wake panic is contained");
    resources.reclaim_finished();
    assert!(
        resources.offloads.is_empty(),
        "the ledger retires work after notification publication"
    );
    let payload = panic
        .take()
        .expect("the caller wake panic survives ledger reclamation");
    assert_eq!(
        payload.downcast_ref::<OpaqueWakePanic>(),
        Some(&OpaqueWakePanic("ordinary finished wake panic")),
        "the caller's original payload is retained, not a stringified stand-in"
    );
}

/// The same chain with a real task handle: the
/// ledger keeps the entry while the completion wake is in flight, so
/// teardown still owns the `ActorWork` it must join, and the caller's own
/// payload — not the task join's stringified panic — is what the
/// cleanup slot holds after the join.
///
/// Multi-threaded on purpose: the test thread blocks on the handshake
/// while the offload task runs the hostile wake on a worker.
#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_completion_wake_panic_survives_the_offload_task_join() {
    let mut resources = RawResources::<()>::default();
    let finished = Latch::default();
    let mut waiter = Box::pin(finished.fired());
    let entered = Gate::new();
    let release = Gate::new();
    let hostile = Waker::from(Arc::new(BlockingPanickingWake {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        message: "joined finished wake panic",
    }));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );

    let state = SharedOffloadState::new(
        Box::pin(async {}),
        resources.disposal.clone(),
        finished.clone(),
    );
    resources.offloads.push(OffloadResource {
        cancellation: Latch::default(),
        finished: finished.clone(),
        state: Some(Arc::clone(&state)),
        task: Some(crate::runtime::spawn_actor_work(SharedOffloadFuture(state))),
    });

    entered.wait("the completion wake to reach the hostile waiter");
    let fired_mid_wake = finished.is_fired();
    resources.reclaim_finished();
    let pinned_mid_wake = resources.offloads.len();
    release.open();

    assert!(fired_mid_wake, "all completion waiters were released");
    assert_eq!(
        pinned_mid_wake, 1,
        "the ledger keeps the task handle teardown must join while the wake runs"
    );

    resources.join_offloads().await;
    assert!(
        resources.offloads.is_empty(),
        "joining clears the ledger it pinned"
    );

    let payload = resources
        .disposal
        .panic
        .take()
        .expect("the caller wake panic is in the cleanup slot after the join");
    assert_eq!(
        payload.downcast_ref::<OpaqueWakePanic>(),
        Some(&OpaqueWakePanic("joined finished wake panic")),
        "the caller's original payload is retained, not the task join's stringified result"
    );
}

/// `freeze` is the drain that normally empties the continuation queue, but
/// it is not a guaranteed one: `Drop for RawResources` skips it once the
/// incarnation is already frozen, and runs it under `PanicSlot::run` so an
/// earlier cleanup step failing can cut it short. Either way the queue's
/// own destructor must still route its payloads through the disposal
/// funnel instead of letting them unwind out of incarnation cleanup.
#[test]
fn a_skipped_freeze_still_contains_queued_continuation_destructors() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut resources = RawResources::<CountedDrop>::default();
    let panic = Arc::clone(&resources.disposal.panic);
    resources.accepting = false;
    for panic_on_drop in [true, false] {
        resources.continuations.push_back(CountedDrop {
            drops: Arc::clone(&drops),
            panic_on_drop,
        });
    }

    catch_unwind(AssertUnwindSafe(|| drop(resources)))
        .expect("a hostile continuation destructor never escapes incarnation cleanup");

    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "one hostile destructor cannot skip the rest of the queue"
    );
    assert_eq!(
        panic.take().as_ref().and_then(panic_message),
        Some("queued continuation destructor panic"),
        "the contained destructor panic is retained as cleanup evidence"
    );
}

/// The ledger's retention bound rests on the reclaim point inside
/// `next_ready`, not on the one in `wait_for_event`: an actor whose
/// mailbox never empties returns from every receive without going idle,
/// so the idle reclaim never runs and finished task handles would
/// otherwise accumulate for the lifetime of the incarnation.
#[test]
fn a_busy_receive_turn_reclaims_finished_offload_ledger_entries() {
    let (mut context, actor) = bound_raw_context();
    actor
        .try_send(1)
        .expect("a bound mailbox accepts the message");
    actor
        .try_send(2)
        .expect("a bound mailbox keeps the actor busy");
    for finished in [true, false, true] {
        let completion = Latch::default();
        if finished {
            completion.fire();
        }
        context.resources.offloads.push(OffloadResource {
            cancellation: Latch::default(),
            finished: completion,
            state: None,
            task: None,
        });
    }

    assert_eq!(
        context.try_recv(),
        Some(1),
        "a readable mailbox returns from ready selection without going idle"
    );

    assert_eq!(
        context.resources.offloads.len(),
        1,
        "the ready-selection turn reclaimed both finished ledger entries"
    );
    assert!(!context.resources.offloads[0].finished.is_fired());
}

#[test]
fn abort_first_stops_a_raw_context() {
    let (mut context, _actor, shutdown, abort) = bound_raw_context_with_abort::<u8>();

    assert!(abort.fire());
    assert!(context.is_stopping());
    context.mark_ready();
    assert!(
        !context.ready.is_fired(),
        "escalation suppresses readiness publication, as on TaskContext"
    );
    assert_eq!(
        context.continue_with(1).map_err(Rejected::into_inner),
        Err(1),
        "escalation rejects new incarnation work"
    );
    assert!(!shutdown.is_fired());
}

#[crate::runtime::test]
async fn recv_freezes_its_mailbox_when_it_observes_shutdown() {
    let (mut context, actor, shutdown) = bound_raw_context_for::<u8>();
    actor
        .try_send(1)
        .expect("the live mailbox accepts before shutdown");
    shutdown.fire();

    assert_eq!(context.recv().await, None);
    assert_eq!(
        actor
            .try_send(2)
            .expect_err("recv's local freeze closes the acceptance boundary")
            .kind,
        crate::SendErrorKind::NotRunning
    );
}

#[test]
fn try_recv_freezes_its_mailbox_when_it_observes_shutdown() {
    let (mut context, actor, shutdown) = bound_raw_context_for::<u8>();
    actor
        .try_send(1)
        .expect("the live mailbox accepts before shutdown");
    shutdown.fire();

    assert_eq!(
        context.try_recv(),
        Some(1),
        "drain mode still reads the prefix accepted before the local freeze"
    );
    assert_eq!(
        actor
            .try_send(2)
            .expect_err("try_recv's local freeze closes the acceptance boundary")
            .kind,
        crate::SendErrorKind::NotRunning
    );
}

#[test]
fn a_disposal_panic_retained_during_selection_fails_the_receive() {
    let (mut context, _actor, _shutdown) = bound_raw_context_for::<u8>();
    let cancellation = Latch::default();
    cancellation.fire();
    let drops = Arc::new(AtomicUsize::new(0));
    let event_payload = PanickingDrop(Arc::clone(&drops));
    context.resources.events.push(QueuedEvent {
        cancellation,
        make_message: Box::new(move || {
            drop(event_payload);
            7
        }),
    });
    context.resources.events.push(QueuedEvent {
        cancellation: Latch::default(),
        make_message: Box::new(|| 9),
    });

    let result = catch_unwind(AssertUnwindSafe(|| context.try_recv()));

    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let panic = result.expect_err(
        "a destructor panic retained by this receive fails it before the next delivery",
    );
    assert_eq!(
        panic_message(&panic),
        Some("contained raw payload destructor panic")
    );
    assert!(context.resources.disposal.panic.take().is_none());
}

#[test]
fn a_fired_timer_key_destructor_panic_fails_the_receive() {
    #[derive(Hash, PartialEq, Eq)]
    struct PanickingKey;

    impl Drop for PanickingKey {
        fn drop(&mut self) {
            panic!("timer key destructor panic");
        }
    }

    let (mut context, _actor, _shutdown) = bound_raw_context_for::<u8>();
    context
        .set_timeout(PanickingKey, 5, Duration::ZERO)
        .unwrap_or_else(|_| panic!("the timer is armed"));

    let result = catch_unwind(AssertUnwindSafe(|| context.try_recv()));

    let panic = result.expect_err("the fired timer's message is not delivered");
    assert_eq!(panic_message(&panic), Some("timer key destructor panic"));
    assert_eq!(context.try_recv(), None, "the timer was consumed");
}

#[test]
fn cancelled_queued_event_is_disposed_without_materializing_its_message() {
    let (mut context, _actor, _shutdown) = bound_raw_context_for::<u8>();
    let cancellation = Latch::default();
    cancellation.fire();
    let drops = Arc::new(AtomicUsize::new(0));
    let invoked = Arc::new(AtomicUsize::new(0));
    let event_payload = PanickingDrop(Arc::clone(&drops));
    let invoked_by_event = Arc::clone(&invoked);
    context.resources.events.push(QueuedEvent {
        cancellation,
        make_message: Box::new(move || {
            invoked_by_event.fetch_add(1, Ordering::SeqCst);
            drop(event_payload);
            7
        }),
    });

    let payload = catch_unwind(AssertUnwindSafe(|| context.try_recv()))
        .expect_err("an empty receive must surface selection's disposal panic");
    assert_eq!(
        invoked.load(Ordering::SeqCst),
        0,
        "the cancelled completion never runs its message builder"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the queued completion is disposed at materialization time"
    );
    assert!(context.resources.disposal.panic.take().is_none());
    assert_eq!(
        panic_message(&payload),
        Some("contained raw payload destructor panic")
    );
}

#[test]
fn a_caught_mailbox_wake_panic_preserves_fired_timers_and_their_cutoff() {
    let (mut context, actor) = bound_raw_context();
    for message in 0..64 {
        actor.try_send(message).expect("fill the default mailbox");
    }
    let mut pending = Box::pin(actor.send(64));
    let hostile = Waker::from(Arc::new(PanickingWake("mailbox sender wake panic")));
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );
    context.set_timeout("timer", 100, Duration::ZERO).unwrap();

    let panic = catch_unwind(AssertUnwindSafe(|| context.try_recv()))
        .expect_err("promoting the sender resumes its wake panic");
    assert_eq!(panic_message(&panic), Some("mailbox sender wake panic"));
    for expected in 1..64 {
        assert_eq!(context.try_recv(), Some(expected));
    }
    assert_eq!(
        context.try_recv(),
        Some(100),
        "the fired timer precedes post-cutoff promotion"
    );
    assert_eq!(context.try_recv(), Some(64));
    assert_eq!(context.try_recv(), None);
    assert!(
        !context.clear_timer(&"timer"),
        "the one-shot timer was delivered"
    );
}

#[test]
fn a_caught_mailbox_wake_panic_spends_the_steady_mailbox_turn() {
    let (mut context, actor) = bound_raw_context();
    for message in 0..64 {
        actor.try_send(message).expect("fill the default mailbox");
    }
    let mut pending = Box::pin(actor.send(64));
    let hostile = Waker::from(Arc::new(PanickingWake("mailbox sender wake panic")));
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );
    context.resources.events.push(QueuedEvent {
        cancellation: Latch::default(),
        make_message: Box::new(|| 100),
    });

    let panic = catch_unwind(AssertUnwindSafe(|| context.try_recv()))
        .expect_err("promoting the sender resumes its wake panic");
    assert_eq!(panic_message(&panic), Some("mailbox sender wake panic"));
    assert_eq!(
        context.try_recv(),
        Some(100),
        "consumed mailbox input spends its fairness turn even when its wake panics"
    );
    assert_eq!(context.try_recv(), Some(1));
}

#[test]
fn a_caught_interval_clone_panic_preserves_the_fired_batch_for_retry() {
    let clones = Arc::new(AtomicUsize::new(0));
    let mut context = bound_raw_context_for::<PanicOnceClone>().0;
    let now = crate::runtime::now();
    let arming = context.resources.timers.replace(
        "interval",
        Some(now),
        TimerMessage::Interval {
            message: PanicOnceClone {
                clones: Arc::clone(&clones),
                value: 7,
            },
            clone: Clone::clone,
            period: Duration::from_secs(1),
        },
    );

    let panic = catch_unwind(AssertUnwindSafe(|| context.try_recv()))
        .expect_err("the first interval clone panic escapes the receive call");
    assert_eq!(panic_message(&panic), Some("interval message clone panic"));
    assert!(
        context
            .resources
            .ready_batch
            .as_ref()
            .is_some_and(|batch| batch.next_arming() == Some(arming)),
        "the caught panic leaves the due arming in its installed batch"
    );

    let message = context
        .try_recv()
        .expect("the next receive retries the same interval firing");
    assert_eq!(message.value, 7);
    assert_eq!(clones.load(Ordering::SeqCst), 2);
    assert!(
        context.resources.timers.next_deadline().is_some(),
        "a successful retry rearms the interval"
    );
}

#[test]
fn a_caught_offload_continuation_panic_preserves_later_fired_work() {
    let mut context = bound_raw_context_for::<u8>().0;
    let now = crate::runtime::now();
    let arming = context
        .resources
        .timers
        .replace("timer", Some(now), TimerMessage::Once(7));
    context.resources.events.push(QueuedEvent {
        cancellation: Latch::default(),
        make_message: Box::new(|| panic!("offload continuation panic")),
    });

    let panic = catch_unwind(AssertUnwindSafe(|| context.try_recv()))
        .expect_err("the offload continuation panic escapes the receive call");
    assert_eq!(panic_message(&panic), Some("offload continuation panic"));
    assert!(
        context
            .resources
            .ready_batch
            .as_ref()
            .is_some_and(|batch| batch.next_arming() == Some(arming)),
        "the caught continuation panic leaves later timer work in the batch"
    );
    assert_eq!(
        context.try_recv(),
        Some(7),
        "the next receive completes the fired batch instead of losing it"
    );
}

#[test]
fn clearing_an_elapsed_undelivered_timer_skips_its_captured_arming() {
    let mut context = bound_raw_context_for::<u8>().0;
    let now = crate::runtime::now();
    context
        .resources
        .timers
        .replace("first", Some(now), TimerMessage::Once(1));
    context
        .resources
        .timers
        .replace("second", Some(now), TimerMessage::Once(2));

    assert_eq!(context.try_recv(), Some(1));
    assert!(
        context.clear_timer(&"second"),
        "an elapsed timer remains clearable before delivery"
    );
    assert_eq!(
        context.try_recv(),
        None,
        "the fired batch skips an arming removed after its cut was captured"
    );
}

#[test]
fn resident_raw_collections_do_not_clone_disposal_per_element() {
    let mut resources = RawResources::<()>::default();
    let baseline = Arc::strong_count(&resources.disposal.panic);

    resources.continuations.push_back(());
    resources.events.push(QueuedEvent {
        cancellation: Latch::default(),
        make_message: Box::new(|| ()),
    });
    resources.timers.replace(7_u8, None, TimerMessage::Once(()));

    assert_eq!(
        Arc::strong_count(&resources.disposal.panic),
        baseline,
        "continuations, events, and timers store raw elements without disposal clones"
    );
}

#[crate::runtime::test]
async fn joining_offloads_retains_a_framework_task_panic() {
    let mut resources = RawResources::<()>::default();
    resources.offloads.push(OffloadResource {
        cancellation: Latch::default(),
        finished: Latch::default(),
        state: None,
        task: Some(crate::runtime::spawn_actor_work(async {
            panic!("unit framework offload panic");
        })),
    });

    resources.join_offloads().await;

    let payload = resources
        .disposal
        .panic
        .take()
        .expect("the framework panic is retained for incarnation teardown");
    assert_eq!(
        panic_message(&payload),
        Some("unit framework offload panic")
    );
}

#[test]
fn raw_resources_drop_cancels_every_offload_after_one_destructor_panics() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut resources = RawResources::<()>::default();
    let mut states = Vec::new();
    let mut finished = Vec::new();
    for panic_on_drop in [true, false] {
        let completion = Latch::default();
        let state = SharedOffloadState::new(
            Box::pin(BlockingPollDrop {
                entered: Arc::new(Barrier::new(1)),
                release: Arc::new(Barrier::new(1)),
                drops: Arc::clone(&drops),
                panic_on_drop,
            }),
            resources.disposal.clone(),
            completion.clone(),
        );
        resources.offloads.push(OffloadResource {
            cancellation: Latch::default(),
            finished: completion.clone(),
            state: Some(Arc::clone(&state)),
            task: None,
        });
        states.push(state);
        finished.push(completion);
    }

    let slot = Arc::clone(&resources.disposal.panic);
    catch_unwind(AssertUnwindSafe(|| drop(resources)))
        .expect("drop leaves cleanup evidence in the slot");
    let payload = slot.take().expect("the first destructor panic is retained");
    assert_eq!(
        panic_message(&payload),
        Some("unit offload destructor panic")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(finished.iter().all(Latch::is_fired));
    assert!(states.iter().all(|state| state.state.lock().is_ok()));
    for state in states {
        state.cancel();
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "repeat cancellation is inert"
    );
}

#[test]
fn freeze_preserves_an_offload_wake_panic_ahead_of_later_collection_disposal() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut resources = RawResources::<PanickingDrop>::default();
    resources
        .continuations
        .push_back(PanickingDrop(Arc::clone(&drops)));

    let finished = Latch::default();
    let mut waiter = Box::pin(finished.fired());
    let hostile = Waker::from(Arc::new(PanickingWake("first finished wake panic")));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );
    resources.offloads.push(OffloadResource {
        cancellation: Latch::default(),
        finished: finished.clone(),
        state: None,
        task: None,
    });

    resources.freeze();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let payload = resources
        .disposal
        .panic
        .take()
        .expect("the first cleanup failure is retained");
    assert_eq!(panic_message(&payload), Some("first finished wake panic"));
}

#[test]
fn freeze_preserves_future_disposal_ahead_of_its_later_finished_wake() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut resources = RawResources::<()>::default();
    let finished = Latch::default();
    let mut waiter = Box::pin(finished.fired());
    let hostile = Waker::from(Arc::new(PanickingWake("later finished wake panic")));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&hostile))
            .is_pending()
    );
    let state = SharedOffloadState::new(
        Box::pin(BlockingPollDrop {
            entered: Arc::new(Barrier::new(1)),
            release: Arc::new(Barrier::new(1)),
            drops: Arc::clone(&drops),
            panic_on_drop: true,
        }),
        resources.disposal.clone(),
        finished.clone(),
    );
    resources.offloads.push(OffloadResource {
        cancellation: Latch::default(),
        finished: finished.clone(),
        state: Some(state),
        task: None,
    });

    resources.freeze();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let payload = resources
        .disposal
        .panic
        .take()
        .expect("the first cleanup failure is retained");
    assert_eq!(
        panic_message(&payload),
        Some("unit offload destructor panic")
    );
}

#[test]
fn repeated_freeze_preserves_the_first_failure_without_repeating_disposal() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut resources = RawResources::<PanickingDrop>::default();
    resources
        .continuations
        .push_back(PanickingDrop(Arc::clone(&drops)));

    resources.freeze();
    resources.freeze();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the freeze transition is one-shot"
    );

    let slot = Arc::clone(&resources.disposal.panic);
    catch_unwind(AssertUnwindSafe(|| drop(resources)))
        .expect("drop leaves cleanup evidence in the slot");
    let payload = slot
        .take()
        .expect("the failure retained by the first freeze survives drop");
    assert_eq!(
        panic_message(&payload),
        Some("contained raw payload destructor panic")
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "drop does not re-run already completed disposal"
    );
}

#[test]
fn freeze_drains_each_raw_collection_with_independent_containment() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut resources = RawResources::<PanickingDrop>::default();
    for _ in 0..2 {
        resources
            .continuations
            .push_back(PanickingDrop(Arc::clone(&drops)));
    }
    let event_payload = PanickingDrop(Arc::clone(&drops));
    resources.events.push(QueuedEvent {
        cancellation: Latch::default(),
        make_message: Box::new(move || {
            drop(event_payload);
            unreachable!("a disposed event is never materialized")
        }),
    });
    resources.timers.replace(
        1_u8,
        None,
        TimerMessage::Once(PanickingDrop(Arc::clone(&drops))),
    );

    resources.freeze();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        4,
        "one hostile destructor cannot skip later collection elements or drains"
    );
    let payload = resources
        .disposal
        .panic
        .take()
        .expect("the first cleanup panic is retained");
    assert_eq!(
        panic_message(&payload),
        Some("contained raw payload destructor panic")
    );
}

/// A callback actor whose initialization always fails, so `Handler::run`
/// takes the error path that returns without installing an actor.
struct RefusingInit;

impl crate::Actor for RefusingInit {
    type Msg = u8;
    type Args = ();

    async fn init(
        _args: Self::Args,
        _context: &mut crate::Context<'_, Self>,
    ) -> Result<Self, crate::ExitError> {
        Err(crate::ExitError::message("initialization refused"))
    }

    async fn handle(
        &mut self,
        _message: Self::Msg,
        _context: &mut crate::Context<'_, Self>,
    ) -> crate::ExitResult {
        Ok(())
    }
}

// The handler lives in `crate::actor`, but its one-run contract is only
// observable against a live raw incarnation, which this module's fixture
// owns. A failed initialization spends the handler exactly as a
// successful one does.
#[crate::runtime::test]
#[should_panic(expected = "handler actor initialization invoked more than once")]
async fn handler_initialization_runs_at_most_once() {
    let (mut context, _myself) = bound_raw_context();
    let mut handler = crate::actor::Handler::<RefusingInit>::new(());
    crate::RawActor::run(&mut handler, &mut context)
        .await
        .expect_err("initialization failure exits the incarnation");
    let _ = crate::RawActor::run(&mut handler, &mut context).await;
}
