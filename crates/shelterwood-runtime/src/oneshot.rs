use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
};

use tokio::sync::oneshot;

use super::{PanicAccumulator, dispose_detached, waker_proxy::ProxiedPoll};

const ONESHOT_OPEN: u8 = 0;
const ONESHOT_SENDING: u8 = 1;
const ONESHOT_SENT: u8 = 2;
const ONESHOT_SENDER_CLOSED: u8 = 3;
const ONESHOT_RECEIVER_CLOSED: u8 = 4;
#[cfg(test)]
pub(crate) const ONESHOT_REPOLL_PANIC: &str =
    "shelterwood one-shot receiver polled after completion";

/// Sending half of a runtime-backed single-delivery channel.
pub struct OneShotSender<T> {
    channel: Option<oneshot::Sender<T>>,
    state: Arc<AtomicU8>,
}

/// Receiving half of a runtime-backed single-delivery channel.
pub struct OneShotReceiver<T> {
    channel: oneshot::Receiver<T>,
    state: Arc<AtomicU8>,
    /// Whether a receive edge has already consumed Tokio's receiver.
    ///
    /// In the pinned Tokio 1.53.1, `Receiver::poll` clears its `Inner` once it
    /// yields `Ready`, and `try_recv` clears it on every outcome but `Empty`;
    /// a later `poll` then panics with a message naming neither Shelterwood
    /// nor this seam. Every terminal edge here records that instead, so the
    /// re-poll diagnostic is framework-owned. A bare [`Self::close`] is not
    /// terminal — Tokio keeps the receiver pollable — so it does not set this.
    /// Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
    completed: bool,
}

/// Outcome after atomically closing a single-delivery receive side.
pub enum OneShotClose<T> {
    /// A value was sent before the receiver closed.
    Value(T),
    /// The sender was dropped before the receiver closed.
    SenderClosed,
    /// The receiver closed while the sender was still live and empty.
    Empty,
    /// The send transition won but has not published its value yet.
    Pending,
}

pub fn oneshot<T>() -> (OneShotSender<T>, OneShotReceiver<T>) {
    let (channel_sender, channel_receiver) = oneshot::channel();
    let state = Arc::new(AtomicU8::new(ONESHOT_OPEN));
    (
        OneShotSender {
            channel: Some(channel_sender),
            state: Arc::clone(&state),
        },
        OneShotReceiver {
            channel: channel_receiver,
            state,
            completed: false,
        },
    )
}

/// Test-only publication half for staging the `ONESHOT_SENDING` window.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub struct OneShotSending<T> {
    channel: Option<oneshot::Sender<T>>,
    state: Arc<AtomicU8>,
}

/// Builds a one-shot whose send transition has won but whose value has not
/// been published yet.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn oneshot_sending_for_test<T>() -> (OneShotSending<T>, OneShotReceiver<T>) {
    let (channel, receiver) = oneshot::channel();
    let state = Arc::new(AtomicU8::new(ONESHOT_SENDING));
    (
        OneShotSending {
            channel: Some(channel),
            state: Arc::clone(&state),
        },
        OneShotReceiver {
            channel: receiver,
            state,
            completed: false,
        },
    )
}

#[cfg(any(test, feature = "test-util"))]
impl<T> OneShotSending<T> {
    /// Publishes the value and completes the staged send transition.
    pub fn publish(mut self, value: T) -> Result<(), T> {
        let channel = self
            .channel
            .take()
            .expect("a staged one-shot send publishes at most once");
        publish_oneshot(channel, &self.state, value)
    }
}

/// Publishes into a one-shot channel whose `ONESHOT_SENDING` window the
/// caller already owns, then closes that window with the outcome.
///
/// `OneShotSender::send` and the staged `OneShotSending::publish` differ only
/// in how they enter the window — `send` wins it with a CAS from
/// `ONESHOT_OPEN`, the test half is constructed inside it — so they share
/// this tail.
fn publish_oneshot<T>(channel: oneshot::Sender<T>, state: &AtomicU8, value: T) -> Result<(), T> {
    match channel.send(value) {
        Ok(()) => {
            state.store(ONESHOT_SENT, Ordering::Release);
            Ok(())
        }
        Err(value) => {
            state.store(ONESHOT_RECEIVER_CLOSED, Ordering::Release);
            Err(value)
        }
    }
}

impl<T> OneShotSender<T> {
    pub fn send(mut self, value: T) -> Result<(), T> {
        if self
            .state
            .compare_exchange(
                ONESHOT_OPEN,
                ONESHOT_SENDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(value);
        }
        let sender = self
            .channel
            .take()
            .expect("a live one-shot sender retains its channel");
        publish_oneshot(sender, &self.state, value)
    }

    pub fn is_closed(&self) -> bool {
        // Diagnostic-only: this is observed under the observation gate and
        // the dynamic-state mutex
        // (`RemovalResponses::subscribe`), so the missing-channel verdict
        // cannot be raised as a panic without poisoning both for every later
        // caller. No correctness property depends on the diagnostic: the
        // total form reports the taken channel as closed, which is what a
        // sender past `send` is, and no test expects this assertion to fire.
        debug_assert!(
            self.channel.is_some(),
            "an observable one-shot sender retains its channel"
        );
        self.channel.as_ref().is_none_or(oneshot::Sender::is_closed)
    }
}

impl<T> Drop for OneShotSender<T> {
    fn drop(&mut self) {
        if self.channel.is_some() {
            let _ = self.state.compare_exchange(
                ONESHOT_OPEN,
                ONESHOT_SENDER_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        // Tokio closes the channel synchronously and may invoke a receiver's
        // caller-supplied waker from the sender's drop glue. Surface that panic
        // to an ordinary dropper, but contain it during an existing unwind so
        // an unanswered reply cannot abort the process with a double panic.
        let channel = self.channel.take();
        let mut panics = PanicAccumulator::default();
        panics.run(|| drop(channel));
    }
}

impl<T> OneShotReceiver<T> {
    fn assert_not_completed(&self) {
        assert!(
            !self.completed,
            "shelterwood one-shot receiver polled after completion"
        );
    }

    pub fn poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        self.assert_not_completed();
        let result = Pin::new(&mut self.channel).poll(context).map(Result::ok);
        if result.is_ready() {
            self.completed = true;
        }
        result
    }

    /// Closes the receive side unless send or sender-drop won first.
    ///
    /// The shared transition word distinguishes sender-drop from receiver
    /// close, which Tokio's post-close `try_recv` result alone cannot do. A
    /// send that wins but is preempted before publishing returns `Pending`;
    /// the channel poll in that branch registers the wake for its completion.
    ///
    /// Every outcome but `Pending` is terminal for this receiver: the close
    /// has been arbitrated, so a later receive edge is a caller bug and gets
    /// the framework diagnostic rather than a fresh arbitration.
    pub fn close_and_poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> OneShotClose<T> {
        self.assert_not_completed();
        let result = match self.state.compare_exchange(
            ONESHOT_OPEN,
            ONESHOT_RECEIVER_CLOSED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.channel.close();
                OneShotClose::Empty
            }
            Err(ONESHOT_SENDER_CLOSED) => OneShotClose::SenderClosed,
            Err(ONESHOT_SENDING) | Err(ONESHOT_SENT) => {
                match Pin::new(&mut self.channel).poll(context) {
                    std::task::Poll::Ready(Ok(value)) => OneShotClose::Value(value),
                    std::task::Poll::Ready(Err(_)) => OneShotClose::SenderClosed,
                    std::task::Poll::Pending => OneShotClose::Pending,
                }
            }
            Err(ONESHOT_RECEIVER_CLOSED) => OneShotClose::Empty,
            Err(other) => unreachable!("unknown one-shot transition state {other}"),
        };
        if !matches!(&result, OneShotClose::Pending) {
            self.completed = true;
        }
        result
    }

    pub fn close(&mut self) {
        let _ = self.state.compare_exchange(
            ONESHOT_OPEN,
            ONESHOT_RECEIVER_CLOSED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.channel.close();
    }

    /// Closes the receive side and recovers a value stored before the close.
    ///
    /// Tokio retains a value sent before `close`, so this is the cancellation
    /// hook that lets callers route an unclaimed stored value through isolated
    /// disposal instead of destroying it in their own drop glue.
    pub fn close_and_take(&mut self) -> Option<T> {
        self.close();
        let value = self.channel.try_recv().ok();
        // Closing makes every outcome terminal, including the staged-send
        // window where publication has not yet observed the receiver close.
        // Record that edge so an erased receiver cannot later re-enter
        // Tokio's completed receiver poll.
        self.completed = true;
        value
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn receive(self) -> Option<T> {
        self.assert_not_completed();
        self.channel.await.ok()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn try_receive(&mut self) -> Option<T> {
        match self.channel.try_recv() {
            Ok(value) => {
                self.completed = true;
                Some(value)
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                self.completed = true;
                None
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
        }
    }
}

/// One-shot receive state that keeps user value destruction off framework and
/// holder drop glue.
///
/// A value stored before the receive side is cancelled is user-owned:
/// destroying it inline could block or panic whoever dropped the holder. The
/// disposal function is captured at construction, where the value type's
/// `Send + 'static` bounds hold, so unbounded holders can still route an
/// unclaimed stored value through isolated disposal on drop.
///
/// The erasure is deliberate: `Drop` must repeat
/// whatever bounds the struct declares, so bounding this type would push
/// `T: Send + 'static` onto the definition of the public `OneShotTaskRef`
/// wrapper that holds it and force downstream generic declarations to carry a
/// bound they never asked for. Reply and call wrappers hold the separate
/// `DisposingReceiver` in the façade's mailbox capability module, which wraps
/// this crate's [`OneShotReceiver`] but retires a cancelled caller waker inline
/// rather than on the disposal lane; the two boundary types preserve the same
/// constructor-only bound. Execution bounds belong on constructors and
/// operational impls here.
pub struct DisposingReceiver<T> {
    inner: Option<OneShotReceiver<T>>,
    dispose: fn(T),
    caller_poll: ProxiedPoll,
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub fn new(inner: OneShotReceiver<T>) -> Self {
        Self {
            inner: Some(inner),
            dispose: dispose_detached::<T>,
            caller_poll: ProxiedPoll::new(),
        }
    }
}

impl<T> DisposingReceiver<T> {
    pub fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        // In pinned Tokio 1.53.1, `Receiver::poll` obtains the result before
        // clearing its `Inner`; the last `Inner::drop` then calls
        // `rx_task.drop_task` while that result can own the delivered value.
        // Probe with a framework waker, then leave only the stable proxy
        // registered across a pending return so Tokio never destroys the raw
        // caller waker at that delivery seam.
        // Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
        self.caller_poll.poll(
            self.inner
                .as_mut()
                .expect("a live disposing receiver retains its channel"),
            context,
            OneShotReceiver::poll_receive,
            Poll::is_pending,
        )
    }
}

impl<T> Drop for DisposingReceiver<T> {
    fn drop(&mut self) {
        let mut inner = self
            .inner
            .take()
            .expect("a live disposing receiver retains its channel");
        let mut value = None;
        let mut panics = PanicAccumulator::default();

        // Recover and dispatch an unclaimed value before either receiver or
        // caller-waker retirement can fail. The waker Tokio's close/drop work
        // below destroys is only ever a framework proxy clone. If recovery
        // itself unwinds, the value stays in the channel and `inner`'s own
        // drop glue destroys it inline rather than through `dispose`:
        // accepted, because reaching it requires a destructor that has
        // already panicked, and the alternative is retrying a step that just
        // failed.
        panics.run(|| value = inner.close_and_take());
        panics.run(|| {
            if let Some(value) = value {
                (self.dispose)(value);
            }
        });
        panics.run(|| drop(inner));
        self.caller_poll.retire_detached(&mut panics);
    }
}

#[cfg(test)]
#[allow(unsafe_code)] // raw-waker test doubles
mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
        thread::ThreadId,
        time::Duration,
    };

    use crate::{
        DisposingReceiver, OneShotClose, oneshot, oneshot_sending_for_test,
        test_support::DISPOSAL_THREAD,
        test_wakers::{CountPanicWake, CountWake, assert_panic_message},
    };

    use super::ONESHOT_REPOLL_PANIC;

    struct RecordPanickingDrop(mpsc::Sender<(ThreadId, Option<String>)>);

    unsafe fn clone_record_panicking_drop(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the represented reference;
        // the returned raw waker owns only the new clone.
        let probe = ManuallyDrop::new(unsafe { Arc::<RecordPanickingDrop>::from_raw(data.cast()) });
        RawWaker::new(
            Arc::into_raw(Arc::clone(&probe)).cast(),
            &RECORD_PANICKING_DROP_VTABLE,
        )
    }

    unsafe fn wake_record_panicking_drop(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<RecordPanickingDrop>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_record_panicking_drop(_data: *const ()) {}

    unsafe fn drop_record_panicking_drop(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let probe = unsafe { Arc::<RecordPanickingDrop>::from_raw(data.cast()) };
        let _ = probe.0.send((
            std::thread::current().id(),
            std::thread::current().name().map(str::to_owned),
        ));
        drop(probe);
        panic!("injected disposing-receiver caller-waker drop panic");
    }

    static RECORD_PANICKING_DROP_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_record_panicking_drop,
        wake_record_panicking_drop,
        wake_by_ref_record_panicking_drop,
        drop_record_panicking_drop,
    );

    fn record_panicking_drop_waker(dropped: mpsc::Sender<(ThreadId, Option<String>)>) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(RecordPanickingDrop(dropped))).cast(),
            &RECORD_PANICKING_DROP_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    #[test]
    fn closing_oneshot_distinguishes_value_sender_drop_and_receiver_win() {
        let mut context = Context::from_waker(Waker::noop());
        let (sender, mut receiver) = oneshot();
        sender.send(1_u8).expect("receiver is live");
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Value(1)
        ));

        let (sender, mut receiver) = oneshot::<u8>();
        drop(sender);
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::SenderClosed
        ));

        let (sender, mut receiver) = oneshot::<u8>();
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Empty
        ));
        assert_eq!(sender.send(1), Err(1));
    }

    #[test]
    fn oneshot_repoll_uses_a_framework_owned_diagnostic() {
        let mut context = Context::from_waker(Waker::noop());
        let (sender, mut receiver) = oneshot();
        sender.send(1_u8).expect("receiver is live");
        assert!(matches!(
            receiver.poll_receive(&mut context),
            Poll::Ready(Some(1))
        ));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("a completed one-shot cannot be polled twice");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let mut receive = Box::pin(receiver.receive());
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receive.as_mut().poll(&mut context);
        }))
        .expect_err("receive cannot re-poll an already delivered receiver");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, receiver) = oneshot();
        sender.send(2_u8).expect("receiver is live");
        let mut receiver = DisposingReceiver::new(receiver);
        assert!(matches!(
            receiver.poll_receive(&mut context),
            Poll::Ready(Some(2))
        ));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("a completed disposing receiver cannot be polled twice");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, mut receiver) = oneshot();
        sender.send(3_u8).expect("receiver is live");
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Value(3)
        ));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.close_and_poll_receive(&mut context);
        }))
        .expect_err("a completed close poll cannot be repeated");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, mut receiver) = oneshot();
        sender.send(4_u8).expect("receiver is live");
        assert_eq!(receiver.close_and_take(), Some(4));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("close-and-take terminality prevents a later poll");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, mut receiver) = oneshot();
        sender.send(5_u8).expect("receiver is live");
        assert_eq!(receiver.try_receive(), Some(5));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.close_and_poll_receive(&mut context);
        }))
        .expect_err("try-receive terminality prevents a later close poll");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        // Tokio's `try_recv` consumes its receiver on every outcome but
        // `Empty`, so an empty sender-closed take is terminal too.
        let (sender, mut receiver) = oneshot::<u8>();
        drop(sender);
        assert_eq!(receiver.try_receive(), None);
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("an exhausted try-receive prevents a later poll");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        // A bare close is not a receive edge: the receiver stays pollable and
        // reports the sender-closed result.
        let (sender, mut receiver) = oneshot::<u8>();
        receiver.close();
        drop(sender);
        assert!(matches!(
            receiver.poll_receive(&mut context),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn closing_oneshot_waits_for_a_send_that_won_before_publication() {
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut context = Context::from_waker(&waker);
        let (sending, mut receiver) = oneshot_sending_for_test();

        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Pending
        ));
        sending.publish(7_u8).expect("the staged receiver is live");
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Value(7)
        ));
    }

    #[test]
    fn closing_oneshot_during_sending_bounces_the_value_to_the_publisher() {
        let (sending, mut receiver) = oneshot_sending_for_test();

        receiver.close();

        assert_eq!(sending.publish(7_u8), Err(7));
        assert_eq!(receiver.try_receive(), None);
    }

    #[test]
    fn dropping_disposing_receiver_detaches_caller_waker_retirement() {
        let dropping_thread = std::thread::current().id();
        let (_sender, receiver) = oneshot::<u8>();
        let mut receiver = DisposingReceiver::new(receiver);
        let (dropped, observed_drop) = mpsc::channel();
        let caller = ManuallyDrop::new(record_panicking_drop_waker(dropped));

        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(&caller)),
            Poll::Pending
        ));
        drop(receiver);

        let (destructor_thread, destructor_name) = observed_drop
            .recv_timeout(Duration::from_secs(1))
            .expect("the caller waker reaches detached disposal");
        assert_ne!(destructor_thread, dropping_thread);
        assert_eq!(
            destructor_name.as_deref(),
            Some(DISPOSAL_THREAD),
            "drop glue must not destroy a caller waker on the holder's thread"
        );
    }

    #[test]
    fn close_and_take_during_sending_leaves_the_value_with_the_publisher() {
        let (sending, mut receiver) = oneshot_sending_for_test();

        assert_eq!(receiver.close_and_take(), None);

        assert_eq!(sending.publish(7_u8), Err(7));
        assert_eq!(receiver.try_receive(), None);
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut Context::from_waker(Waker::noop()));
        }))
        .expect_err("close-and-take completes the staged-send receiver");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);
    }

    #[test]
    fn dropping_oneshot_sender_surfaces_a_hostile_receiver_waker_once() {
        const PANIC: &str = "injected one-shot receiver waker panic";

        let (sender, mut receiver) = oneshot::<u8>();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&wakes),
            message: PANIC,
        }));
        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));

        let payload = catch_unwind(AssertUnwindSafe(|| drop(sender)))
            .expect_err("an ordinary sender drop surfaces the hostile wake");

        assert_panic_message(&*payload, PANIC);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(None)
        ));
    }
}
