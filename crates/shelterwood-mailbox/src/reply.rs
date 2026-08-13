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
    use super::super::cell::tests::actor;

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
}
