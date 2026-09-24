use std::{
    fmt,
    task::{Context, Poll},
};

use shelterwood_core::DeadlineBudget;

use crate::{
    mailbox::capability::{DisposingReceiver, OneShotReceive},
    runtime::{OneShotClose, OneShotReceiver, OneShotSender, dispose_detached, oneshot},
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
    pub(crate) fn channel() -> (Self, ReplyReceiver<T>) {
        let (sender, receiver) = oneshot();
        (
            Self { sender },
            ReplyReceiver {
                receiver: DisposingReceiver::new(receiver),
            },
        )
    }

    /// Consumes the capability and delivers or discards the reply.
    pub fn send(self, value: T) {
        // A cancelled receiver rejects the value. Destroying it inline would
        // run a possibly blocking or panicking user destructor on the replying
        // actor; route the discard through isolated disposal instead.
        if let Err(unclaimed) = self.sender.send(value) {
            dispose_detached(unclaimed);
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
        ReplyReceive {
            deadlined: Deadlined::no_attempt(
                ReplyOperation {
                    receiver: self.receiver,
                },
                deadline,
            ),
        }
    }
}

pub(super) struct ReplyOperation<T, R: OneShotReceive<T> = OneShotReceiver<T>> {
    receiver: DisposingReceiver<T, R>,
}

pub(super) enum ReplyPoll<T> {
    Value(T),
    SenderClosed,
    TimedOut,
}

pub(super) fn poll_reply<T: Send + 'static, R: OneShotReceive<T>>(
    receiver: &mut DisposingReceiver<T, R>,
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

impl<T: Send + 'static, R: OneShotReceive<T>> DeadlineOperation for ReplyOperation<T, R> {
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
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use crate::{
        mailbox::capability::{DisposingReceiver, OneShotReceive},
        runtime::OneShotClose,
        test_support::probe_waker,
    };

    use super::{
        super::{cell::tests::actor, deadline::Deadlined},
        ReplyOperation, ReplyReceiver,
    };

    /// A scripted receive edge: the ready, close, and arbitration outcomes the
    /// adapter's one-shot produces only under races.
    struct SeamReceiver {
        pending_polls: usize,
        value: Option<u8>,
        value_on_close: bool,
    }

    impl OneShotReceive<u8> for SeamReceiver {
        fn poll_receive(&mut self, _context: &mut Context<'_>) -> Poll<Option<u8>> {
            if self.pending_polls > 0 {
                self.pending_polls -= 1;
                Poll::Pending
            } else {
                Poll::Ready(self.value.take())
            }
        }

        fn close_and_poll_receive(&mut self, _context: &mut Context<'_>) -> OneShotClose<u8> {
            if self.value_on_close {
                self.value
                    .take()
                    .map_or(OneShotClose::SenderClosed, OneShotClose::Value)
            } else {
                OneShotClose::Pending
            }
        }

        fn close(&mut self) {}

        fn close_and_take(&mut self) -> Option<u8> {
            self.value.take()
        }
    }

    fn drop_waker(drops: Arc<AtomicUsize>, hostile: bool) -> Waker {
        probe_waker(
            || {},
            move || {
                drops.fetch_add(1, Ordering::SeqCst);
                assert!(!hostile, "injected reply caller-waker drop panic");
            },
        )
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
        // `ReplyReceiver::recv`'s shape over a scripted receiver: the deadline
        // scaffold around one reply operation.
        let mut receive = Box::pin(Deadlined::no_attempt(
            ReplyOperation {
                receiver: DisposingReceiver::new(SeamReceiver {
                    pending_polls: 1,
                    value: Some(7),
                    value_on_close: false,
                }),
            },
            Duration::from_secs(1),
        ));
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
        let mut receiver = DisposingReceiver::new(SeamReceiver {
            pending_polls: usize::MAX,
            value: Some(9),
            value_on_close: true,
        });
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
        let mut receiver = DisposingReceiver::new(SeamReceiver {
            pending_polls: usize::MAX,
            value: None,
            value_on_close: false,
        });
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
        // The adapter's own staged-send receiver, so the arbitration edge is
        // the real one-shot's rather than a script.
        let (publisher, receiver) = crate::runtime::oneshot_sending_for_test::<u8>();
        let receiver = ReplyReceiver {
            receiver: DisposingReceiver::new(receiver),
        };
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
        // Measured as a delta, not an absolute. The expired timer already
        // woke the *previous* poll's waker, and the waker proxy replays that
        // record into whichever caller registers next -- a spurious wake the
        // `Future` contract permits, and the price of never losing a real one.
        // The property under test is that publishing the winning value wakes
        // the deferred caller exactly once.
        let woken_before_publish = wakes.load(Ordering::SeqCst);
        publisher
            .publish(7)
            .unwrap_or_else(|_| panic!("the staged receiver remains live"));
        assert_eq!(wakes.load(Ordering::SeqCst), woken_before_publish + 1);
        assert!(matches!(
            receive.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Ready(Ok(7))
        ));
    }
}
