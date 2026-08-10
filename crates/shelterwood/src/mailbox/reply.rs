use std::{
    fmt,
    task::{Context, Poll},
    time::Duration,
};

use crate::runtime::{DisposingReceiver, OneShotClose, OneShotSender, dispose_detached};

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
    /// Creates a reply capability and its single owned receiver.
    #[must_use]
    pub fn channel() -> (Self, ReplyReceiver<T>) {
        let (sender, receiver) = crate::runtime::oneshot();
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

/// The owned, non-cloneable receive half of [`Reply::channel`].
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
    pub fn recv(self, deadline: Duration) -> ReplyReceive<T> {
        ReplyReceive {
            deadlined: Deadlined::new(
                ReplyOperation {
                    receiver: self.receiver,
                },
                deadline,
            ),
        }
    }
}

pub(super) struct ReplyOperation<T> {
    receiver: DisposingReceiver<T>,
}

impl<T> DeadlineOperation for ReplyOperation<T> {
    type Output = Result<T, ReplyError>;

    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        _budget: crate::deadline::Deadline,
        phase: DeadlinePhase,
    ) -> Poll<Self::Output> {
        match self.receiver.inner.poll_receive(context) {
            Poll::Ready(Some(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(None) => Poll::Ready(Err(ReplyError::Dropped)),
            Poll::Pending if phase == DeadlinePhase::Elapsed => {
                match self.receiver.inner.close_and_poll_receive(context) {
                    OneShotClose::Value(value) => Poll::Ready(Ok(value)),
                    OneShotClose::SenderClosed => Poll::Ready(Err(ReplyError::Dropped)),
                    OneShotClose::Empty => Poll::Ready(Err(ReplyError::Timeout)),
                    OneShotClose::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn short_circuit(&mut self) -> Self::Output {
        self.receiver.inner.close();
        Err(ReplyError::Timeout)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn reply_debug_still_reports_the_derived_unanswered_state() {
        let (reply, receiver) = super::Reply::<u8>::channel();

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

        let (reply, receiver) = super::Reply::channel();
        reply.send(7_u8);
        assert_eq!(receiver.recv(deadline).await, Ok(7));

        let (reply, receiver) = super::Reply::<u8>::channel();
        drop(reply);
        assert_eq!(
            receiver.recv(deadline).await,
            Err(super::ReplyError::Dropped)
        );

        let (reply, receiver) = super::Reply::<u8>::channel();
        drop(receiver);
        assert!(reply.sender.is_closed());
        reply.send(9);
    }
}
