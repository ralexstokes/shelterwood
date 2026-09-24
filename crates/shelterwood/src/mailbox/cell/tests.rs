use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use crate::{
    mailbox::{ActorRef, ChildId, Incarnation, MailboxControl, MailboxReceiver, SendErrorKind},
    policy::{ResolvedDefaults, ResolvedMailbox},
    test_support::{mint_actor_incarnation, mint_actor_membership},
};

use super::MailboxCell;

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("injected waker panic");
    }
}

struct CountWake(Arc<AtomicUsize>);

struct LatestDisplacementMessage {
    value: u8,
    mailbox: Weak<MailboxCell<LatestDisplacementMessage>>,
    displaced: Option<mpsc::Sender<(bool, Option<u8>)>>,
}

struct PanickingDisplacementMessage {
    value: u8,
    mailbox: Weak<MailboxCell<PanickingDisplacementMessage>>,
    displaced: Option<mpsc::Sender<bool>>,
}

impl Drop for PanickingDisplacementMessage {
    fn drop(&mut self) {
        let Some(displaced) = self.displaced.take() else {
            return;
        };
        let unlocked = self
            .mailbox
            .upgrade()
            .is_none_or(|mailbox| mailbox.state.try_lock().is_ok());
        let _ = displaced.send(unlocked);
        panic!("injected displaced-message destructor panic");
    }
}

impl Drop for LatestDisplacementMessage {
    fn drop(&mut self) {
        let Some(displaced) = self.displaced.take() else {
            return;
        };
        let observation = self.mailbox.upgrade().and_then(|mailbox| {
            mailbox.state.try_lock().ok().map(|state| {
                (
                    true,
                    state.latest.as_ref().map(|latest| latest.message.value),
                )
            })
        });
        let _ = displaced.send(observation.unwrap_or((false, None)));
    }
}

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindEffectEvent {
    SignalPulsed,
    DisposalSubmitted,
    SenderWoken,
}

struct BindOrderingRuntime {
    inner: Arc<dyn crate::mailbox::MailboxRuntime>,
    events: Arc<Mutex<Vec<BindEffectEvent>>>,
}

impl crate::mailbox::MailboxRuntime for BindOrderingRuntime {
    fn oneshot(
        &self,
    ) -> (
        Box<dyn shelterwood_core::ErasedOneShotSender>,
        Pin<Box<dyn shelterwood_core::ErasedOneShotReceiver>>,
    ) {
        self.inner.oneshot()
    }

    fn signal(&self) -> Arc<dyn crate::mailbox::MailboxSignal> {
        Arc::new(BindOrderingSignal {
            inner: self.inner.signal(),
            events: Arc::clone(&self.events),
        })
    }

    fn dispose(&self, value: Box<dyn Send + 'static>) {
        self.events
            .lock()
            .expect("bind effect recorder mutex")
            .push(BindEffectEvent::DisposalSubmitted);
        self.inner.dispose(value);
    }

    fn now(&self) -> Instant {
        self.inner.now()
    }

    fn sleep_until(&self, deadline: Option<Instant>) -> shelterwood_core::BoxedSleep {
        self.inner.sleep_until(deadline)
    }
}

/// Runtime whose change signal panics once armed, so a mailbox effect
/// flush can be made to unwind on demand.
struct PanickingPulseRuntime {
    inner: Arc<dyn crate::mailbox::MailboxRuntime>,
    armed: Arc<AtomicBool>,
    disposals: Arc<AtomicUsize>,
}

impl crate::mailbox::MailboxRuntime for PanickingPulseRuntime {
    fn oneshot(
        &self,
    ) -> (
        Box<dyn shelterwood_core::ErasedOneShotSender>,
        Pin<Box<dyn shelterwood_core::ErasedOneShotReceiver>>,
    ) {
        self.inner.oneshot()
    }

    fn signal(&self) -> Arc<dyn crate::mailbox::MailboxSignal> {
        Arc::new(PanickingPulseSignal {
            inner: self.inner.signal(),
            armed: Arc::clone(&self.armed),
        })
    }

    fn dispose(&self, value: Box<dyn Send + 'static>) {
        self.disposals.fetch_add(1, Ordering::SeqCst);
        self.inner.dispose(value);
    }

    fn now(&self) -> Instant {
        self.inner.now()
    }

    fn sleep_until(&self, deadline: Option<Instant>) -> shelterwood_core::BoxedSleep {
        self.inner.sleep_until(deadline)
    }
}

struct PanickingPulseSignal {
    inner: Arc<dyn crate::mailbox::MailboxSignal>,
    armed: Arc<AtomicBool>,
}

impl crate::mailbox::MailboxSignal for PanickingPulseSignal {
    fn pulse(&self) {
        assert!(
            !self.armed.load(Ordering::SeqCst),
            "injected mailbox pulse panic"
        );
        self.inner.pulse();
    }

    fn watcher(&self) -> Box<dyn crate::mailbox::MailboxSignalWatcher> {
        self.inner.watcher()
    }
}

/// User message recording the thread its destructor ran on.
struct ThreadRecordingMessage(Option<mpsc::Sender<std::thread::ThreadId>>);

impl Drop for ThreadRecordingMessage {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(std::thread::current().id());
        }
    }
}

struct BindOrderingSignal {
    inner: Arc<dyn crate::mailbox::MailboxSignal>,
    events: Arc<Mutex<Vec<BindEffectEvent>>>,
}

impl crate::mailbox::MailboxSignal for BindOrderingSignal {
    fn pulse(&self) {
        self.events
            .lock()
            .expect("bind effect recorder mutex")
            .push(BindEffectEvent::SignalPulsed);
        self.inner.pulse();
    }

    fn watcher(&self) -> Box<dyn crate::mailbox::MailboxSignalWatcher> {
        self.inner.watcher()
    }
}

struct BindOrderingWake(Arc<Mutex<Vec<BindEffectEvent>>>);

impl Wake for BindOrderingWake {
    fn wake(self: Arc<Self>) {
        self.0
            .lock()
            .expect("bind effect recorder mutex")
            .push(BindEffectEvent::SenderWoken);
    }
}

struct ReentrantPanicDrop {
    mailbox: Weak<MailboxCell<u8>>,
    operation: Weak<super::SendOperation<u8>>,
    drops: Arc<AtomicUsize>,
}

impl Wake for ReentrantPanicDrop {
    fn wake(self: Arc<Self>) {
        panic!("the withdrawal regression only drops its waker")
    }
}

impl Drop for ReentrantPanicDrop {
    fn drop(&mut self) {
        let operation = self
            .operation
            .upgrade()
            .expect("the cancelling send retains its operation");
        drop(
            operation
                .state
                .try_lock()
                .expect("waker drop runs after the operation lock is released"),
        );

        let mailbox = self
            .mailbox
            .upgrade()
            .expect("the cancelling send retains its mailbox");
        drop(
            mailbox
                .state
                .try_lock()
                .expect("waker drop runs after the mailbox lock is released"),
        );
        let error = mailbox
            .try_send(2)
            .expect_err("a reentrant try-send observes the unbound mailbox");
        assert_eq!(error.kind, SendErrorKind::NotRunning);
        assert_eq!(error.message, 2);

        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("injected waker drop panic");
    }
}

pub(crate) fn actor_for_with_runtime<M: Send + 'static>(
    runtime: Arc<dyn crate::mailbox::MailboxRuntime>,
) -> (Arc<MailboxCell<M>>, ActorRef<M>) {
    let id = ChildId::from("actor");
    let identity = crate::identity::ScopeIdentity::new().mint_membership(&id);
    let member = crate::cells::MemberCell::new(identity);
    let mailbox = MailboxCell::new(id, runtime);
    (
        Arc::clone(&mailbox),
        crate::mailbox::actor_ref_from_parts(member, mailbox),
    )
}

pub(crate) fn actor_for<M: Send + 'static>() -> (Arc<MailboxCell<M>>, ActorRef<M>) {
    actor_for_with_runtime(crate::mailbox::capability::tests::runtime())
}

pub(crate) fn actor() -> (Arc<MailboxCell<u8>>, ActorRef<u8>) {
    actor_for()
}

pub(crate) fn configure<M: Send + 'static>(
    mailbox: &MailboxCell<M>,
    policy: ResolvedMailbox,
) -> crate::mailbox::MailboxBindToken {
    let mut effects = crate::mailbox::MailboxEffectQueue::default();
    MailboxControl::configure(mailbox, policy, &mut effects)
}

pub(crate) fn bind<M: Send + 'static>(
    mailbox: &MailboxCell<M>,
    token: crate::mailbox::MailboxBindToken,
    incarnation: Incarnation,
) {
    let mut effects = crate::mailbox::MailboxEffectQueue::default();
    MailboxControl::bind(mailbox, token, incarnation, &mut effects);
}

pub(crate) fn prepare_termination<M: Send + 'static>(
    mailbox: &MailboxCell<M>,
) -> Option<Box<dyn crate::mailbox::MailboxTermination>> {
    let mut effects = crate::mailbox::MailboxEffectQueue::default();
    MailboxControl::prepare_termination(mailbox, &mut effects)
}

fn park_with(future: &mut std::pin::Pin<Box<crate::SendFuture<u8>>>, waker: &Waker) {
    let mut context = Context::from_waker(waker);
    assert!(future.as_mut().poll(&mut context).is_pending());
}

pub(crate) fn freeze<M: Send + 'static>(mailbox: &MailboxCell<M>, incarnation: Incarnation) {
    let mut effects = crate::mailbox::MailboxEffectQueue::default();
    MailboxControl::freeze(mailbox, incarnation, &mut effects);
}

/// Closes in the driver's shape: the result stays live across the effect
/// flush, so a panicking waker cannot strand the unread payload.
pub(crate) fn close<M: Send + 'static>(
    mailbox: &MailboxCell<M>,
    incarnation: Incarnation,
) -> Option<crate::mailbox::MailboxClose> {
    let mut effects = crate::mailbox::MailboxEffectQueue::default();
    MailboxControl::close(mailbox, incarnation, &mut effects)
}

fn two_incarnations() -> (Incarnation, Incarnation) {
    let (_, mut incarnations) = mint_actor_membership();
    (incarnations.mint(), incarnations.mint())
}

struct TrackedMessage {
    value: u8,
    drops: Arc<AtomicUsize>,
}

impl Drop for TrackedMessage {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn freeze_and_close_preserve_waiters_payloads_and_incarnation_boundaries() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (first, second) = two_incarnations();
    let mailbox = MailboxCell::new(
        ChildId::from("actor"),
        crate::mailbox::capability::tests::runtime(),
    );
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")),
    );

    freeze(&mailbox, first);
    assert!(close(&mailbox, first).is_none());
    bind(&mailbox, token, first);
    assert!(matches!(
        mailbox.submit(TrackedMessage {
            value: 1,
            drops: Arc::clone(&drops),
        }),
        super::Submission::Accepted(bound) if bound == first
    ));
    let second_message = match mailbox.submit(TrackedMessage {
        value: 2,
        drops: Arc::clone(&drops),
    }) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("the second message parks behind the full queue")
        }
    };

    freeze(&mailbox, second);
    assert!(close(&mailbox, second).is_none());
    {
        let state = mailbox.state.lock().expect("mailbox mutex poisoned");
        assert_eq!(state.phase.observation(), Some(first));
        assert!(matches!(state.phase, super::Phase::Bound(_)));
        assert_eq!(state.waiters.len(), 1);
    }

    freeze(&mailbox, first);
    let third_message = match mailbox.submit(TrackedMessage {
        value: 3,
        drops: Arc::clone(&drops),
    }) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("frozen intake parks new messages")
        }
    };
    {
        let state = mailbox.state.lock().expect("mailbox mutex poisoned");
        assert_eq!(state.phase.observation(), Some(first));
        assert!(matches!(state.phase, super::Phase::Frozen(_)));
        assert_eq!(state.waiters.len(), 2);
    }

    let closed =
        close(&mailbox, first).expect("the matching close returns the unread queue payload");
    {
        let state = mailbox.state.lock().expect("mailbox mutex poisoned");
        assert_eq!(state.phase, super::Phase::Unbound);
        assert_eq!(state.waiters.len(), 2);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let (token, payload) = closed.into_parts();
    drop(payload);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "close transfers the unread payload instead of dropping it inline"
    );

    bind(&mailbox, token, second);
    assert!(matches!(
        second_message.poll(None, Waker::noop()),
        super::OperationPoll::Accepted(bound) if bound == second
    ));
    assert!(matches!(
        third_message.poll(None, Waker::noop()),
        super::OperationPoll::NeedsWakerClone
    ));
    let receiver = MailboxReceiver::new(Arc::clone(&mailbox), second);
    let message = receiver.try_recv().expect("the oldest waiter was promoted");
    assert_eq!(message.value, 2);
    drop(message);
    assert!(matches!(
        third_message.poll(None, Waker::noop()),
        super::OperationPoll::Accepted(bound) if bound == second
    ));
    let message = receiver
        .try_recv()
        .expect("the remaining waiter was promoted");
    assert_eq!(message.value, 3);
    drop(message);
    assert_eq!(drops.load(Ordering::SeqCst), 3);

    freeze(&mailbox, first);
    assert!(close(&mailbox, first).is_none());
    drop(close(&mailbox, second));
    let teardown = prepare_termination(&mailbox).expect("an unbound mailbox can terminalize");
    drop(teardown.finish());
    freeze(&mailbox, second);
    assert!(close(&mailbox, second).is_none());

    let latest_drops = Arc::new(AtomicUsize::new(0));
    let latest = MailboxCell::new(
        ChildId::from("latest"),
        crate::mailbox::capability::tests::runtime(),
    );
    let latest_token = configure(&latest, ResolvedMailbox::Latest);
    bind(&latest, latest_token, first);
    assert!(matches!(
        latest.submit(TrackedMessage {
            value: 4,
            drops: Arc::clone(&latest_drops),
        }),
        super::Submission::Accepted(bound) if bound == first
    ));
    freeze(&latest, first);
    let closed = close(&latest, first).expect("close returns the frozen latest payload");
    assert_eq!(latest_drops.load(Ordering::SeqCst), 0);
    let (_token, payload) = closed.into_parts();
    drop(payload);
    assert_eq!(latest_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn accepted_sequence_boundary_is_inclusive_and_live_only_refuses_frozen_input() {
    let (mailbox, actor_ref) = actor();
    let (incarnation, _) = two_incarnations();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(4).expect("non-zero queue capacity")),
    );
    bind(&mailbox, token, incarnation);
    let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);

    assert!(matches!(actor_ref.try_send(1), Ok(bound) if bound == incarnation));
    assert!(matches!(actor_ref.try_send(2), Ok(bound) if bound == incarnation));
    let through = receiver.accepted_sequence();
    assert!(matches!(actor_ref.try_send(3), Ok(bound) if bound == incarnation));
    assert_eq!(receiver.try_recv_live_through(through), Some(1));
    assert_eq!(
        receiver.try_recv_live_through(through),
        Some(2),
        "the accepted-sequence boundary itself is included"
    );
    assert_eq!(receiver.try_recv_live_through(through), None);

    receiver.freeze();
    let frozen_through = receiver.accepted_sequence();
    assert_eq!(receiver.try_recv_live_through(frozen_through), None);
    assert_eq!(
        receiver.try_recv(),
        Some(3),
        "Drain receives the same frozen payload LiveThrough refuses"
    );

    let (latest_mailbox, latest_actor) = actor();
    let latest_token = configure(&latest_mailbox, ResolvedMailbox::Latest);
    bind(&latest_mailbox, latest_token, incarnation);
    let latest_receiver = MailboxReceiver::new(latest_mailbox, incarnation);
    assert!(matches!(
        latest_actor.try_send(4),
        Ok(bound) if bound == incarnation
    ));
    let latest_through = latest_receiver.accepted_sequence();
    assert_eq!(
        latest_receiver.try_recv_live_through(latest_through),
        Some(4),
        "latest mailboxes also include the exact accepted-sequence boundary"
    );
    assert!(matches!(
        latest_actor.try_send(5),
        Ok(bound) if bound == incarnation
    ));
    assert_eq!(latest_receiver.try_recv_live_through(latest_through), None);
    assert_eq!(latest_receiver.try_recv(), Some(5));
}

#[test]
fn termination_that_wins_before_withdrawal_reports_the_terminal_outcome() {
    let (mailbox, _) = actor();
    let (incarnation, _) = two_incarnations();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")),
    );
    bind(&mailbox, token, incarnation);
    assert!(matches!(
        mailbox.submit(1),
        super::Submission::Accepted(bound) if bound == incarnation
    ));
    let operation = match mailbox.submit(2) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("the second message parks")
        }
    };

    let teardown = prepare_termination(&mailbox).expect("the mailbox prepares termination");
    let payload = teardown.finish();
    let mut withdrawal = mailbox.withdraw(&operation, super::WithdrawalDisposition::Inline);
    assert!(matches!(
        withdrawal.take_outcome(),
        super::WithdrawalOutcome::Terminated {
            message: 2,
            observed: Some(final_incarnation),
        } if final_incarnation == incarnation
    ));
    withdrawal.finish();
    drop(payload);
}

#[test]
fn termination_finish_completes_waiters_and_isolates_payload_after_signal_panic() {
    let armed = Arc::new(AtomicBool::new(false));
    let disposals = Arc::new(AtomicUsize::new(0));
    let runtime = Arc::new(PanickingPulseRuntime {
        inner: crate::mailbox::capability::tests::runtime(),
        armed: Arc::clone(&armed),
        disposals: Arc::clone(&disposals),
    });
    let (mailbox, actor) = actor_for_with_runtime(runtime);
    let (incarnation, _) = two_incarnations();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")),
    );
    bind(&mailbox, token, incarnation);
    assert!(matches!(actor.try_send(1), Ok(bound) if bound == incarnation));
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    let mut second = Box::pin(actor.send(2));
    let mut third = Box::pin(actor.send(3));
    park_with(&mut second, &waker);
    park_with(&mut third, &waker);

    let teardown = prepare_termination(&mailbox).expect("the mailbox prepares termination");
    armed.store(true, Ordering::SeqCst);
    let panic = catch_unwind(AssertUnwindSafe(move || {
        let _ = teardown.finish();
    }))
    .expect_err("the primary signal panic resumes after teardown");
    assert_eq!(
        panic.downcast_ref::<&'static str>().copied(),
        Some("injected mailbox pulse panic")
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 2);
    assert_eq!(
        disposals.load(Ordering::SeqCst),
        1,
        "a panicking finish submits the unread payload for isolated disposal"
    );
    for send in [&mut second, &mut third] {
        let Poll::Ready(Err(error)) = send.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("every waiter is terminal before the primary panic resumes")
        };
        assert_eq!(error.kind, SendErrorKind::Terminated);
        assert_eq!(error.incarnation_observed, Some(incarnation));
    }
}

#[test]
fn receive_modes_pin_live_cutoffs_and_frozen_drain() {
    let (mailbox, actor) = actor();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(2).expect("non-zero queue capacity")),
    );
    let incarnation = mint_actor_incarnation();
    bind(&mailbox, token, incarnation);
    let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);

    actor.try_send(1).expect("first message accepts");
    let through_first = receiver.accepted_sequence();
    actor.try_send(2).expect("second message accepts");
    assert_eq!(receiver.try_recv_live_through(through_first), Some(1));
    assert_eq!(receiver.try_recv_live_through(through_first), None);

    let through_all = receiver.accepted_sequence();
    receiver.freeze();
    assert_eq!(
        receiver.try_recv_live_through(through_all),
        None,
        "live reception never enters a frozen mailbox"
    );
    assert_eq!(receiver.try_recv(), Some(2));
    assert_eq!(receiver.try_recv(), None);
}

#[test]
fn stale_receivers_cannot_drain_a_rebound_or_frozen_mailbox() {
    let (mailbox, actor) = actor();
    let (first, second) = two_incarnations();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")),
    );
    bind(&mailbox, token, first);
    let stale = MailboxReceiver::new(Arc::clone(&mailbox), first);

    let closed = close(&mailbox, first).expect("the first incarnation closes");
    let (token, payload) = closed.into_parts();
    drop(payload);
    bind(&mailbox, token, second);
    actor.try_send(7).expect("the rebound mailbox accepts");
    let through = stale.accepted_sequence();

    assert_eq!(stale.try_recv(), None);
    assert_eq!(stale.try_recv_live_through(through), None);
    freeze(&mailbox, second);
    assert_eq!(stale.try_recv(), None);
    assert_eq!(stale.try_recv_live_through(through), None);

    let current = MailboxReceiver::new(mailbox, second);
    assert_eq!(
        current.try_recv(),
        Some(7),
        "stale reads leave the payload intact"
    );
}

#[test]
fn live_latest_displacement_drops_after_unlock_and_replacement_visibility() {
    let (mailbox, actor): (
        Arc<MailboxCell<LatestDisplacementMessage>>,
        ActorRef<LatestDisplacementMessage>,
    ) = actor_for();
    let token = configure(&mailbox, ResolvedMailbox::Latest);
    let incarnation = mint_actor_incarnation();
    bind(&mailbox, token, incarnation);
    let weak = Arc::downgrade(&mailbox);
    let (displaced, observed) = mpsc::channel();

    actor
        .try_send(LatestDisplacementMessage {
            value: 1,
            mailbox: Weak::clone(&weak),
            displaced: Some(displaced),
        })
        .expect("the first latest value accepts");
    actor
        .try_send(LatestDisplacementMessage {
            value: 2,
            mailbox: weak,
            displaced: None,
        })
        .expect("the replacement latest value accepts");

    assert_eq!(
        observed
            .recv_timeout(Duration::from_secs(5))
            .expect("the displaced value reports its drop context"),
        (true, Some(2)),
        "displacement drops after unlock and after publishing the replacement"
    );
}

#[test]
fn latest_displacement_contains_a_panicking_payload_destructor() {
    let mailbox = MailboxCell::new(
        ChildId::from("actor"),
        crate::mailbox::capability::tests::runtime(),
    );
    let token = configure(&mailbox, ResolvedMailbox::Latest);
    let incarnation = mint_actor_incarnation();
    bind(&mailbox, token, incarnation);
    let weak = Arc::downgrade(&mailbox);
    let (displaced, observed) = mpsc::channel();

    assert!(matches!(
        mailbox.submit(PanickingDisplacementMessage {
            value: 1,
            mailbox: Weak::clone(&weak),
            displaced: Some(displaced),
        }),
        super::Submission::Accepted(bound) if bound == incarnation
    ));
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = mailbox.submit(PanickingDisplacementMessage {
            value: 2,
            mailbox: weak,
            displaced: None,
        });
    }));
    assert!(
        panic.is_err(),
        "the hostile destructor panic reaches the caller"
    );
    assert!(
        observed
            .recv_timeout(Duration::from_secs(5))
            .expect("the displaced payload reports its drop context"),
        "the displaced payload is destroyed after mailbox unlock"
    );

    let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);
    let replacement = receiver
        .try_recv()
        .expect("the replacement remains readable after displacement panics");
    assert_eq!(replacement.value, 2);
}

#[test]
fn bind_observes_waiters_that_remain_parked_beyond_capacity() {
    let (mailbox, _) = actor();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")),
    );
    let first = match mailbox.submit(1) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("an unbound mailbox parks the first send")
        }
    };
    let second = match mailbox.submit(2) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("an unbound mailbox parks overflow sends")
        }
    };
    let incarnation = mint_actor_incarnation();
    bind(&mailbox, token, incarnation);
    assert!(matches!(
        first.poll(None, Waker::noop()),
        super::OperationPoll::Accepted(bound) if bound == incarnation
    ));
    let closed = close(&mailbox, incarnation).expect("the bound incarnation closes");
    let (_token, payload) = closed.into_parts();
    drop(payload);

    let mut withdrawal = mailbox.withdraw(&second, super::WithdrawalDisposition::Inline);
    assert!(matches!(
        withdrawal.take_outcome(),
        super::WithdrawalOutcome::Withdrawn {
            message: 2,
            observed: Some(bound),
        } if bound == incarnation
    ));
    withdrawal.finish();
}

#[test]
fn operation_invariant_panics_release_the_operation_guard() {
    let retired = super::SendOperation::new(1_u8, None, None);
    retired.state.lock().expect("operation state").outcome = super::OperationOutcome::Retired;
    assert!(catch_unwind(AssertUnwindSafe(|| retired.poll(None, Waker::noop()))).is_err());
    drop(
        retired
            .state
            .lock()
            .expect("retired poll panic occurs after unlock"),
    );
}

#[test]
fn configuration_mismatch_panics_after_releasing_the_mailbox_lock() {
    let (mailbox, _) = actor();
    let queue =
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity"));
    let _ = configure(&mailbox, queue);
    let _ = configure(&mailbox, queue);

    let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
        let _ = configure(&mailbox, ResolvedMailbox::Latest);
    })) else {
        panic!("changing mailbox kind must trip the driver contract")
    };
    assert!(
        panic
            .downcast_ref::<String>()
            .is_some_and(|message| message.contains("mailbox configuration changed"))
    );
    assert_eq!(
        mailbox
            .state
            .lock()
            .expect("configuration panic occurs after unlocking")
            .kind,
        Some(ResolvedMailbox::Queue(
            std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")
        ))
    );
}

#[test]
fn bind_after_terminal_is_silently_ignored() {
    let (mailbox, _) = actor();
    let token = configure(&mailbox, ResolvedDefaults::default().mailbox());
    let teardown = prepare_termination(&mailbox).expect("the mailbox terminalizes");

    bind(&mailbox, token, mint_actor_incarnation());
    assert_eq!(
        mailbox.state.lock().expect("mailbox state").phase,
        super::Phase::Terminal(None)
    );
    drop(teardown.finish());
}

#[test]
fn bind_requires_prior_configuration_without_poisoning_the_mailbox() {
    let (mailbox, _) = actor();
    let token = {
        let state = mailbox.state.lock().expect("mailbox state");
        crate::mailbox::MailboxBindToken::new(Arc::clone(&state.bind_permit))
    };

    let panic = catch_unwind(AssertUnwindSafe(|| {
        bind(&mailbox, token, mint_actor_incarnation());
    }))
    .expect_err("an unconfigured mailbox rejects bind");
    assert_eq!(
        panic.downcast_ref::<&'static str>().copied(),
        Some("mailbox must be configured before its first bind")
    );
    drop(
        mailbox
            .state
            .lock()
            .expect("the configuration guard panics after unlock"),
    );
}

#[test]
fn bind_requires_the_prior_incarnation_to_close() {
    let (mailbox, _) = actor();
    let token = configure(&mailbox, ResolvedDefaults::default().mailbox());
    let first = mint_actor_incarnation();
    bind(&mailbox, token, first);
    let (foreign, _) = actor();
    let foreign_token = configure(&foreign, ResolvedDefaults::default().mailbox());

    let panic = catch_unwind(AssertUnwindSafe(|| {
        bind(&mailbox, foreign_token, mint_actor_incarnation());
    }))
    .expect_err("a live incarnation rejects rebinding");
    assert_eq!(
        panic.downcast_ref::<&'static str>().copied(),
        Some("mailbox must close the prior incarnation before rebinding")
    );
    assert_eq!(
        mailbox
            .state
            .lock()
            .expect("the prior-close guard panics after unlock")
            .phase
            .observation(),
        Some(first)
    );
}

#[test]
fn bind_rejects_a_foreign_token_without_consuming_the_valid_one() {
    let (mailbox, _) = actor();
    let valid = configure(&mailbox, ResolvedDefaults::default().mailbox());
    let (foreign, _) = actor();
    let foreign_token = configure(&foreign, ResolvedDefaults::default().mailbox());

    let panic = catch_unwind(AssertUnwindSafe(|| {
        bind(&mailbox, foreign_token, mint_actor_incarnation());
    }))
    .expect_err("a foreign token cannot bind the mailbox");
    assert_eq!(
        panic.downcast_ref::<&'static str>().copied(),
        Some("mailbox bind token is foreign or was already consumed")
    );

    bind(&mailbox, valid, mint_actor_incarnation());
}

#[test]
fn repeated_configuration_tokens_share_one_consumable_permit() {
    let (mailbox, _) = actor();
    let policy = ResolvedDefaults::default().mailbox();
    let first_token = configure(&mailbox, policy);
    let alias = configure(&mailbox, policy);
    let first = mint_actor_incarnation();
    bind(&mailbox, first_token, first);
    let closed = close(&mailbox, first).expect("the first incarnation closes");
    let (next_token, payload) = closed.into_parts();
    drop(payload);

    let panic = catch_unwind(AssertUnwindSafe(|| {
        bind(&mailbox, alias, mint_actor_incarnation());
    }))
    .expect_err("the alias cannot bind after the shared permit was consumed");
    assert_eq!(
        panic.downcast_ref::<&'static str>().copied(),
        Some("mailbox bind token is foreign or was already consumed")
    );

    bind(&mailbox, next_token, mint_actor_incarnation());
}

#[test]
fn receive_promotes_multiple_parked_senders_in_fifo_order() {
    let (mailbox, actor) = actor();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")),
    );
    let incarnation = mint_actor_incarnation();
    bind(&mailbox, token, incarnation);
    let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);

    assert!(matches!(actor.try_send(0), Ok(bound) if bound == incarnation));
    let mut sends: Vec<_> = (1_u8..=3)
        .map(|message| Box::pin(actor.send(message)))
        .collect();
    for send in &mut sends {
        park_with(send, Waker::noop());
    }

    let send_count = sends.len();
    for (delivered, send) in sends.iter_mut().enumerate() {
        assert_eq!(receiver.try_recv(), Some(delivered as u8));
        assert!(matches!(
            send.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(bound)) if bound == incarnation
        ));

        let remaining = send_count - delivered - 1;
        if remaining > 0 {
            assert_eq!(
                mailbox
                    .state
                    .lock()
                    .expect("mailbox mutex poisoned")
                    .waiters
                    .len(),
                remaining
            );
        }
    }

    assert_eq!(receiver.try_recv(), Some(3));
    assert_eq!(receiver.try_recv(), None);
}

#[test]
fn cancelling_many_parked_sends_unlinks_one_registration_each() {
    const SENDS: usize = 16_384;

    let (mailbox, actor) = actor();
    let mut sends = Vec::with_capacity(SENDS);
    for _ in 0..SENDS {
        let mut send = Box::pin(actor.send(1));
        park_with(&mut send, Waker::noop());
        sends.push(Some(send));
    }
    assert_eq!(
        mailbox
            .state
            .lock()
            .expect("mailbox mutex poisoned")
            .waiters()
            .len(),
        SENDS
    );

    // Exercise one interior, the head, and the tail explicitly, then a
    // deterministic odd/even permutation. The counter measures queue
    // operations rather than elapsed time or scheduler behavior.
    for index in [SENDS / 2, 0, SENDS - 1] {
        drop(sends[index].take().expect("selected send remains live"));
    }
    for index in (1..SENDS - 1).step_by(2) {
        if let Some(send) = sends[index].take() {
            drop(send);
        }
    }
    for send in sends.into_iter().flatten() {
        drop(send);
    }

    let state = mailbox.state.lock().expect("mailbox mutex poisoned");
    let waiters = state.waiters();
    assert!(waiters.is_empty());
    assert_eq!(
        waiters.direct_removals, SENDS,
        "mass cancellation must do one direct unlink per parked send"
    );
}

#[test]
fn withdrawal_releases_its_waker_instead_of_destroying_it_under_the_locks() {
    let (mailbox, _) = actor();
    let operation = match mailbox.submit(1) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("an unbound mailbox parks its send")
        }
    };
    let drops = Arc::new(AtomicUsize::new(0));
    let hostile = Waker::from(Arc::new(ReentrantPanicDrop {
        mailbox: Arc::downgrade(&mailbox),
        operation: Arc::downgrade(&operation),
        drops: Arc::clone(&drops),
    }));
    operation.install_test_waker(hostile.clone());
    drop(hostile);

    let mut withdrawal = mailbox.withdraw(&operation, super::WithdrawalDisposition::Inline);
    assert!(matches!(
        withdrawal.take_outcome(),
        super::WithdrawalOutcome::Withdrawn { message: 1, .. }
    ));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "withdrawal releases the waker rather than running its destructor"
    );
    // Finishing the explicit effect set runs its waker destructor with
    // neither core lock held, so the reentrant probe inside it succeeds
    // and its panic reaches only that owner.
    let Err(panic) = catch_unwind(AssertUnwindSafe(move || withdrawal.finish())) else {
        panic!("the hostile waker drop panic reaches whoever destroys it")
    };
    assert_eq!(
        panic.downcast_ref::<&'static str>().copied(),
        Some("injected waker drop panic")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(
        operation
            .state
            .lock()
            .expect("hostile waker drop cannot poison the operation lock"),
    );
    let state = mailbox
        .state
        .lock()
        .expect("hostile waker drop cannot poison the mailbox lock");
    assert!(state.waiters().is_empty());
}

#[test]
fn promotion_wakes_every_sender_when_multiple_wakers_panic() {
    let (mailbox, actor) = actor();
    let mut first = Box::pin(actor.send(1));
    let mut second = Box::pin(actor.send(2));
    let mut third = Box::pin(actor.send(3));
    let first_panicking = Waker::from(Arc::new(PanicWake));
    let second_panicking = Waker::from(Arc::new(PanicWake));
    let wakes = Arc::new(AtomicUsize::new(0));
    let counting = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    park_with(&mut first, &first_panicking);
    park_with(&mut second, &second_panicking);
    park_with(&mut third, &counting);
    let token = configure(&mailbox, ResolvedDefaults::default().mailbox());
    let incarnation = mint_actor_incarnation();

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            bind(&mailbox, token, incarnation);
        }))
        .is_err()
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        first.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
    assert!(matches!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
    assert!(matches!(
        third.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn bind_submits_displaced_payloads_for_disposal_before_waking_senders() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let runtime = Arc::new(BindOrderingRuntime {
        inner: crate::mailbox::capability::tests::runtime(),
        events: Arc::clone(&events),
    });
    let mailbox = MailboxCell::new(ChildId::from("actor"), runtime);
    let token = configure(&mailbox, ResolvedMailbox::Latest);

    let first = match mailbox.submit(1) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("an unbound mailbox parks its send")
        }
    };
    let second = match mailbox.submit(2) {
        super::Submission::Parked(operation) => operation,
        super::Submission::Accepted(_) | super::Submission::Terminated { .. } => {
            panic!("an unbound mailbox parks its send")
        }
    };
    first.install_test_waker(Waker::from(Arc::new(BindOrderingWake(Arc::clone(&events)))));
    second.install_test_waker(Waker::from(Arc::new(BindOrderingWake(Arc::clone(&events)))));

    bind(&mailbox, token, mint_actor_incarnation());

    assert_eq!(
        *events.lock().expect("bind effect recorder mutex"),
        [
            BindEffectEvent::SignalPulsed,
            BindEffectEvent::DisposalSubmitted,
            BindEffectEvent::SenderWoken,
            BindEffectEvent::SenderWoken,
        ],
        "binding preserves pulse, disposal-submission, then wake ordering"
    );
}

#[test]
fn termination_discharge_reaches_every_sender_when_multiple_wakers_panic() {
    let (mailbox, actor) = actor();
    let mut first = Box::pin(actor.send(1));
    let mut second = Box::pin(actor.send(2));
    let mut third = Box::pin(actor.send(3));
    let first_panicking = Waker::from(Arc::new(PanicWake));
    let second_panicking = Waker::from(Arc::new(PanicWake));
    let wakes = Arc::new(AtomicUsize::new(0));
    let counting = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    park_with(&mut first, &first_panicking);
    park_with(&mut second, &second_panicking);
    park_with(&mut third, &counting);

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            drop(prepare_termination(&mailbox));
        }))
        .is_err()
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    for future in [&mut first, &mut second, &mut third] {
        let Poll::Ready(Err(error)) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("every parked send must be discharged");
        };
        assert_eq!(error.kind, SendErrorKind::Terminated);
    }
}

#[test]
fn cancellation_can_race_detached_terminal_teardown() {
    let (mailbox, actor) = actor();
    let mut send = Box::pin(actor.send(1));
    park_with(&mut send, Waker::noop());

    let teardown = prepare_termination(&mailbox).expect("live mailbox prepares terminal teardown");
    // Teardown is deliberately retained: cancellation sees terminal state
    // after the waiter queue was detached but before it was discharged.
    drop(send);
    drop(teardown);

    assert!(
        mailbox
            .state
            .lock()
            .expect("mailbox mutex remains healthy")
            .waiters()
            .is_empty(),
        "terminal teardown detaches every waiter"
    );
}

#[crate::runtime::test(start_paused = true)]
async fn expired_timeout_can_race_detached_terminal_teardown() {
    let (mailbox, actor) = actor();
    let width = Duration::from_secs(1);
    let mut send = Box::pin(actor.send_timeout(1, width));
    assert!(
        send.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    crate::runtime::advance(width * 2).await;

    let teardown = prepare_termination(&mailbox).expect("live mailbox prepares terminal teardown");
    // Teardown remains retained: timeout sees terminal binding after the
    // waiter queue was detached but before its waiter was discharged.
    let Poll::Ready(Err(error)) = send.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("the expired send withdraws before deferred discharge");
    };
    assert_eq!(error.kind, SendErrorKind::TimedOut);
    assert_eq!(error.message, 1);
    drop(teardown);

    let retry = actor
        .try_send(2)
        .expect_err("the terminal mailbox rejects a retry");
    assert_eq!(retry.kind, SendErrorKind::Terminated);
    assert_eq!(retry.incarnation_observed, None);
}

fn registration(operation: &super::SendOperation<u8>) -> super::WaiterId {
    operation
        .state
        .lock()
        .expect("operation state")
        .registration
        .expect("a parked operation is registered")
}

#[test]
fn stale_waiter_id_cannot_unlink_a_later_registration() {
    let mut waiters = super::WaiterQueue::default();
    let first = waiters.park(1_u8, None);
    let first_id = registration(&first);
    let removed = waiters.remove(first_id).expect("first waiter is live");
    removed.clear_registration();

    let second = waiters.park(2_u8, None);
    let second_id = registration(&second);
    assert_ne!(first_id, second_id, "waiter identities are never reused");
    assert!(
        waiters.remove(first_id).is_none(),
        "a stale cancellation cannot unlink a later waiter"
    );
    let removed = waiters
        .remove(second_id)
        .expect("second waiter remains live");
    assert!(Arc::ptr_eq(&removed, &second));
    assert!(waiters.is_empty());
}

#[test]
fn waiter_queue_preserves_fifo_across_removal() {
    let mut waiters = super::WaiterQueue::default();
    let first = waiters.park(1_u8, None);
    let second = waiters.park(2_u8, None);
    let third = waiters.park(3_u8, None);

    assert!(Arc::ptr_eq(
        &waiters
            .remove(registration(&second))
            .expect("middle waiter is live"),
        &second
    ));
    let operation = waiters.pop_front().expect("head remains live");
    assert!(Arc::ptr_eq(&operation, &first));
    let operation = waiters.pop_front().expect("tail remains live");
    assert!(Arc::ptr_eq(&operation, &third));
    assert!(waiters.is_empty());
}

#[test]
fn a_panicking_close_flush_isolates_the_unread_payload() {
    let armed = Arc::new(AtomicBool::new(false));
    let runtime = Arc::new(PanickingPulseRuntime {
        inner: crate::mailbox::capability::tests::runtime(),
        armed: Arc::clone(&armed),
        disposals: Arc::new(AtomicUsize::new(0)),
    });
    let mailbox = MailboxCell::new(ChildId::from("actor"), runtime);
    let token = configure(&mailbox, ResolvedMailbox::Latest);
    let incarnation = mint_actor_incarnation();
    bind(&mailbox, token, incarnation);
    let (dropped, observed) = mpsc::channel();
    assert!(matches!(
        mailbox.submit(ThreadRecordingMessage(Some(dropped))),
        super::Submission::Accepted(_)
    ));
    armed.store(true, Ordering::SeqCst);

    // The driver shape: the close result is a live local across the
    // effects flush, which wakes registered wakers synchronously and can
    // therefore unwind on user code.
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let mut effects = crate::mailbox::MailboxEffectQueue::default();
        let closed = MailboxControl::close(&*mailbox, incarnation, &mut effects);
        drop(effects);
        if let Some(closed) = closed {
            let (_token, disposal) = closed.into_parts();
            drop(disposal);
        }
    }));
    assert!(panic.is_err(), "the flush panic reaches the caller");

    let destructor = observed
        .recv_timeout(Duration::from_secs(5))
        .expect("the unread message destructor reports");
    assert_ne!(
        destructor,
        std::thread::current().id(),
        "an unwinding close flush must not destroy unread user messages on the caller's thread"
    );
}

#[test]
fn a_panicking_receive_flush_isolates_the_received_message() {
    let (mailbox, actor) = actor_for::<ThreadRecordingMessage>();
    let token = configure(
        &mailbox,
        ResolvedMailbox::Queue(std::num::NonZeroUsize::new(1).expect("non-zero queue capacity")),
    );
    let incarnation = mint_actor_incarnation();
    bind(&mailbox, token, incarnation);
    let (dropped, observed) = mpsc::channel();
    assert!(matches!(
        actor.try_send(ThreadRecordingMessage(Some(dropped))),
        Ok(bound) if bound == incarnation
    ));
    // Receiving promotes this sender, whose wake panics in the flush.
    let mut parked = Box::pin(actor.send(ThreadRecordingMessage(None)));
    let panicking = Waker::from(Arc::new(PanicWake));
    assert!(
        parked
            .as_mut()
            .poll(&mut Context::from_waker(&panicking))
            .is_pending()
    );
    let receiver = MailboxReceiver::new(Arc::clone(&mailbox), incarnation);

    let panic = catch_unwind(AssertUnwindSafe(|| receiver.try_recv().is_some()))
        .expect_err("the promoted sender's wake panic reaches the receiver");
    assert_eq!(
        panic.downcast_ref::<&'static str>().copied(),
        Some("injected waker panic")
    );
    assert_ne!(
        observed
            .recv_timeout(Duration::from_secs(5))
            .expect("the received message reaches detached disposal"),
        std::thread::current().id(),
        "the unwind does not destroy the received message on the receiver's stack"
    );
    assert!(matches!(
        parked
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(bound)) if bound == incarnation
    ));
}
