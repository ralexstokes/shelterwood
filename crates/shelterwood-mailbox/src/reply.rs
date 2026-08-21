use std::{
    fmt,
    sync::Arc,
    task::{Context, Poll},
};

use shelterwood_core::DeadlineBudget;

use crate::{
    MailboxRuntime,
    capability::{DisposingReceiver, OneShotClose, OneShotSender, dispose, oneshot},
};

use super::{
    ReplyError, ReplyReceive,
    deadline::{DeadlineOperation, DeadlinePhase, Deadlined},
};

/// A consuming, infallible reply capability.
///
/// Dropping an unanswered capability is completion: its receiver observes
/// [`ReplyError::Dropped`]. Dropping or timing out the receiver instead closes
/// the channel, so a late [`Reply::send`] safely discards its value through
/// isolated disposal.
pub struct Reply<T> {
    sender: OneShotSender<T>,
    runtime: Arc<dyn MailboxRuntime>,
}

impl<T> fmt::Debug for Reply<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reply")
            // `Reply::send` consumes the capability, so every observable
            // `Reply` is necessarily unanswered. Preserve the public Debug
            // shape without storing that derivable state.
            .field("answered", &false)
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> Reply<T> {
    pub(crate) fn channel(runtime: Arc<dyn MailboxRuntime>) -> (Self, ReplyReceiver<T>) {
        let (sender, receiver) = oneshot(&runtime);
        (
            Self {
                sender,
                runtime: Arc::clone(&runtime),
            },
            ReplyReceiver {
                receiver: DisposingReceiver::new(receiver, runtime),
            },
        )
    }

    /// Consumes the capability and delivers or discards the reply.
    pub fn send(self, value: T) {
        // A cancelled receiver rejects the value. Destroying it inline would
        // run a possibly blocking or panicking user destructor on the replying
        // actor; route the discard through isolated disposal instead.
        if let Err(unclaimed) = self.sender.send(value) {
            dispose(&self.runtime, unclaimed);
        }
    }
}

/// The owned, non-cloneable receive half of
/// [`ActorRef::reply_channel`](crate::ActorRef::reply_channel).
pub struct ReplyReceiver<T> {
    pub(super) receiver: DisposingReceiver<T>,
}

impl<T> fmt::Debug for ReplyReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyReceiver")
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> ReplyReceiver<T> {
    /// Consumes the receiver and waits within one response-only budget.
    ///
    /// A zero budget does not observe an already-published response; it closes
    /// this receive capability and reports [`ReplyError::Timeout`].
    pub fn recv(self, deadline: impl Into<DeadlineBudget>) -> ReplyReceive<T> {
        let runtime = self.receiver.runtime();
        ReplyReceive {
            deadlined: Deadlined::no_attempt(
                ReplyOperation {
                    receiver: self.receiver,
                },
                deadline,
                runtime,
            ),
        }
    }
}

pub(super) struct ReplyOperation<T> {
    receiver: DisposingReceiver<T>,
}

pub(super) enum ReplyPoll<T> {
    Value(T),
    SenderClosed,
    TimedOut,
}

pub(super) fn poll_reply<T: Send + 'static>(
    receiver: &mut DisposingReceiver<T>,
    context: &mut Context<'_>,
    phase: DeadlinePhase,
) -> Poll<ReplyPoll<T>> {
    match receiver.poll_receive(context) {
        Poll::Ready(Some(value)) => Poll::Ready(ReplyPoll::Value(value)),
        Poll::Ready(None) => Poll::Ready(ReplyPoll::SenderClosed),
        Poll::Pending if phase == DeadlinePhase::TimeoutArbitration => {
            match receiver.close_and_poll_receive(context) {
                OneShotClose::Value(value) => Poll::Ready(ReplyPoll::Value(value)),
                OneShotClose::SenderClosed => Poll::Ready(ReplyPoll::SenderClosed),
                OneShotClose::Empty => Poll::Ready(ReplyPoll::TimedOut),
                OneShotClose::Pending => Poll::Pending,
            }
        }
        Poll::Pending => Poll::Pending,
    }
}

impl<T: Send + 'static> DeadlineOperation for ReplyOperation<T> {
    type Output = Result<T, ReplyError>;

    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        _budget: crate::deadline::Deadline,
        phase: DeadlinePhase,
    ) -> Poll<Self::Output> {
        match poll_reply(&mut self.receiver, context, phase) {
            Poll::Ready(ReplyPoll::Value(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(ReplyPoll::SenderClosed) => Poll::Ready(Err(ReplyError::Dropped)),
            Poll::Ready(ReplyPoll::TimedOut) => Poll::Ready(Err(ReplyError::Timeout)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn short_circuit(&mut self) -> Self::Output {
        self.receiver.close();
        Err(ReplyError::Timeout)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
        time::Duration,
    };

    use crate::{
        ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue,
        capability::{DisposingReceiver, OneShotClose, oneshot},
    };

    use super::super::cell::tests::{actor, actor_for_with_runtime};

    struct RejectingSender;

    impl ErasedOneShotSender for RejectingSender {
        fn send(self: Box<Self>, value: ErasedValue) -> Result<(), ErasedValue> {
            Err(value)
        }
    }

    struct SeamReceiver {
        pending_polls: usize,
        value: Option<ErasedValue>,
        value_on_close: bool,
    }

    impl ErasedOneShotReceiver for SeamReceiver {
        fn poll_receive(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<ErasedValue>> {
            if self.pending_polls > 0 {
                self.pending_polls -= 1;
                Poll::Pending
            } else {
                Poll::Ready(self.value.take())
            }
        }

        fn close_and_poll_receive(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> ErasedOneShotClose {
            if self.value_on_close {
                self.value
                    .take()
                    .map_or(ErasedOneShotClose::SenderClosed, ErasedOneShotClose::Value)
            } else {
                ErasedOneShotClose::Pending
            }
        }

        fn close(self: Pin<&mut Self>) {}

        fn close_and_take(mut self: Pin<&mut Self>) -> Option<ErasedValue> {
            self.value.take()
        }
    }

    struct DropCounter {
        drops: Arc<AtomicUsize>,
        hostile: bool,
    }

    unsafe fn clone_counted_drop_waker(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the represented reference;
        // the returned raw waker owns only the new clone.
        let state = ManuallyDrop::new(unsafe { Arc::<DropCounter>::from_raw(data.cast()) });
        RawWaker::new(
            Arc::into_raw(Arc::clone(&state)).cast(),
            &COUNTED_DROP_WAKER_VTABLE,
        )
    }

    unsafe fn wake_counted_drop_waker(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<DropCounter>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_counted_drop_waker(_data: *const ()) {}

    unsafe fn drop_counted_drop_waker(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<DropCounter>::from_raw(data.cast()) };
        state.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!state.hostile, "injected reply caller-waker drop panic");
    }

    static COUNTED_DROP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_counted_drop_waker,
        wake_counted_drop_waker,
        wake_by_ref_counted_drop_waker,
        drop_counted_drop_waker,
    );

    fn drop_waker(drops: Arc<AtomicUsize>, hostile: bool) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(DropCounter { drops, hostile })).cast(),
            &COUNTED_DROP_WAKER_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    fn counted_drop_waker(drops: Arc<AtomicUsize>) -> Waker {
        drop_waker(drops, false)
    }

    fn hostile_drop_waker(drops: Arc<AtomicUsize>) -> Waker {
        drop_waker(drops, true)
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn reply_debug_still_reports_the_derived_unanswered_state() {
        let (_, actor) = actor();
        let (reply, receiver) = actor.reply_channel::<u8>();

        let rendered = format!("{reply:?}");
        assert!(rendered.contains("Reply"));
        // The leading space pins the field name exactly: a hypothetical
        // `unanswered: false` field must not satisfy this assertion.
        assert!(rendered.contains(" answered: false"));

        drop(reply);
        drop(receiver);
    }

    #[crate::runtime::test]
    async fn reply_halves_preserve_success_drop_and_cancellation_lifecycles() {
        let deadline = std::time::Duration::from_secs(1);
        let (_, actor) = actor();

        let (reply, receiver) = actor.reply_channel();
        reply.send(7_u8);
        assert_eq!(receiver.recv(deadline).await, Ok(7));

        let (reply, receiver) = actor.reply_channel::<u8>();
        drop(reply);
        assert_eq!(
            receiver.recv(deadline).await,
            Err(super::ReplyError::Dropped)
        );

        // Exercises `Reply::send`'s rejection branch for a panic only: the
        // value it discards is unobservable from here. That the discard runs
        // through isolated disposal rather than inline is asserted in
        // `late_reply_send_disposes_unclaimed_value_off_the_sender`
        // (`crates/shelterwood/tests/disposal.rs`), which owns that claim.
        let (reply, receiver) = actor.reply_channel::<u8>();
        drop(receiver);
        reply.send(9);
    }

    #[crate::runtime::test]
    async fn ready_race_retires_the_reply_caller_waker_before_returning() {
        let runtime = Arc::new(
            crate::capability::tests::TestRuntime::new().with_oneshot(|| {
                (
                    Box::new(RejectingSender),
                    Box::pin(SeamReceiver {
                        pending_polls: 1,
                        value: Some(Box::new(7_u8)),
                        value_on_close: false,
                    }),
                )
            }),
        );
        let (_, actor) = actor_for_with_runtime::<()>(runtime);
        let (_reply, receiver) = actor.reply_channel::<u8>();
        let mut receive = Box::pin(receiver.recv(Duration::from_secs(1)));
        let drops = Arc::new(AtomicUsize::new(0));
        let caller = ManuallyDrop::new(counted_drop_waker(Arc::clone(&drops)));

        assert!(matches!(
            receive.as_mut().poll(&mut Context::from_waker(&caller)),
            Poll::Ready(Ok(7))
        ));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the proxy's caller clone retires synchronously at the ready seam"
        );
    }

    #[test]
    fn timeout_arbitration_value_retires_the_reply_caller_waker_before_returning() {
        let runtime = Arc::new(
            crate::capability::tests::TestRuntime::new().with_oneshot(|| {
                (
                    Box::new(RejectingSender),
                    Box::pin(SeamReceiver {
                        pending_polls: usize::MAX,
                        value: Some(Box::new(9_u8)),
                        value_on_close: true,
                    }),
                )
            }),
        );
        let (_, inner) = oneshot::<u8>(&(runtime.clone() as Arc<dyn crate::MailboxRuntime>));
        let mut receiver = DisposingReceiver::new(inner, runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let caller = ManuallyDrop::new(counted_drop_waker(Arc::clone(&drops)));
        let mut context = Context::from_waker(&caller);

        assert!(receiver.poll_receive(&mut context).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Value(9)
        ));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "timeout arbitration retires the proxy's caller clone before returning the value"
        );
    }

    #[test]
    fn close_retires_the_reply_caller_waker_and_contains_a_hostile_destructor() {
        let runtime = Arc::new(
            crate::capability::tests::TestRuntime::new().with_oneshot(|| {
                (
                    Box::new(RejectingSender),
                    Box::pin(SeamReceiver {
                        pending_polls: usize::MAX,
                        value: None,
                        value_on_close: false,
                    }),
                )
            }),
        );
        let (_, inner) = oneshot::<u8>(&(runtime.clone() as Arc<dyn crate::MailboxRuntime>));
        let mut receiver = DisposingReceiver::new(inner, runtime);
        let drops = Arc::new(AtomicUsize::new(0));
        let caller = ManuallyDrop::new(hostile_drop_waker(Arc::clone(&drops)));
        let mut context = Context::from_waker(&caller);

        assert!(receiver.poll_receive(&mut context).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        // Returning normally is the assertion: `close` is reached from frames
        // that own a live user value (`CallOperation::fail_send` holds the
        // recovered message), so a hostile caller-waker destructor has to be
        // contained here rather than re-raised. A `catch_unwind` around this
        // call would pass even if containment were removed.
        receiver.close();

        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "close retires the proxy's caller clone"
        );
    }

    #[crate::runtime::test(start_paused = true)]
    async fn timeout_arbitration_waits_for_a_winning_send_to_publish() {
        let publisher = Arc::new(Mutex::new(None));
        let staged_publisher = Arc::clone(&publisher);
        let runtime = Arc::new(crate::capability::tests::TestRuntime::new().with_oneshot(
            move || {
                let (publisher, receiver) = crate::runtime::oneshot_sending_for_test();
                let displaced = staged_publisher
                    .lock()
                    .expect("staged reply publisher mutex")
                    .replace(publisher);
                assert!(displaced.is_none(), "the test creates one reply channel");
                (
                    Box::new(RejectingSender),
                    Box::pin(crate::capability::tests::AdapterOneShotReceiver::new(
                        receiver,
                    )),
                )
            },
        ));
        let (_, actor) = actor_for_with_runtime::<()>(runtime.clone());
        let (reply, receiver) = actor.reply_channel::<u8>();
        let width = Duration::from_secs(1);
        let mut receive = Box::pin(receiver.recv(width));
        assert!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );

        crate::runtime::advance(width * 2).await;
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        assert!(
            receive
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending(),
            "OneShotClose::Pending defers the timeout verdict"
        );
        let publisher = publisher
            .lock()
            .expect("staged reply publisher mutex")
            .take()
            .expect("reply channel installed its publisher");
        // Measured as a delta, not an absolute. The expired timer already
        // woke the *previous* poll's waker, and the waker proxy replays that
        // record into whichever caller registers next -- a spurious wake the
        // `Future` contract permits, and the price of never losing a real one.
        // The property under test is that publishing the winning value wakes
        // the deferred caller exactly once.
        let woken_before_publish = wakes.load(Ordering::SeqCst);
        publisher
            .publish(Box::new(7_u8))
            .unwrap_or_else(|_| panic!("the staged receiver remains live"));
        assert_eq!(wakes.load(Ordering::SeqCst), woken_before_publish + 1);
        assert!(matches!(
            receive.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(7))
        ));
        drop(reply);
    }
}
