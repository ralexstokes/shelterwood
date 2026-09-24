use std::future::Future;

use super::{Timeout, UnboundedMpscReceiver, timeout_at};

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

/// Polls two futures and resolves with the first to become ready.
///
/// Ties are contractual, not incidental: when both are ready in the same
/// poll, `Left` always wins. Callers order a "won" edge before a
/// "closed"/"completed" edge on exactly this bias — a latch that fired and
/// completed must still report the fired side.
pub async fn select_two<A, B>(left: A, right: B) -> Either<A::Output, B::Output>
where
    A: Future + Send,
    B: Future + Send,
{
    tokio::pin!(left);
    tokio::pin!(right);
    tokio::select! {
        biased;
        value = &mut left => Either::Left(value),
        value = &mut right => Either::Right(value),
    }
}

pub enum ScopeWake<T> {
    Signal,
    ParentShutdown,
    Message(Option<T>),
    ControlMessage(Option<T>),
    Deadline,
}

pub struct ScopeWait<S, C> {
    pub signal: S,
    pub parent_shutdown: C,
}

pub async fn wait_scope<S, C, T>(
    wait: ScopeWait<S, C>,
    receiver: &mut UnboundedMpscReceiver<T>,
    control_receiver: Option<&mut UnboundedMpscReceiver<T>>,
    deadline: Option<std::time::Instant>,
) -> ScopeWake<T>
where
    S: Future<Output = ()> + Send,
    C: Future<Output = ()> + Send,
{
    let ScopeWait {
        signal,
        parent_shutdown,
    } = wait;
    let event = async move {
        tokio::pin!(signal);
        tokio::pin!(parent_shutdown);
        let control_message = async move {
            if let Some(receiver) = control_receiver {
                receiver.recv().await
            } else {
                std::future::pending().await
            }
        };
        tokio::pin!(control_message);
        tokio::select! {
            biased;
            () = &mut signal => ScopeWake::Signal,
            () = &mut parent_shutdown => ScopeWake::ParentShutdown,
            message = receiver.recv() => ScopeWake::Message(message),
            message = &mut control_message => ScopeWake::ControlMessage(message),
        }
    };
    // The deadline stays outside the whole event selection so every event
    // winner retires the timer through `timeout_at`'s synchronous poll-path
    // boundary. Burying the sleep in a select arm would run its drop-glue
    // disposal venue every time another arm won -- one blocking-lane
    // submission per driver wakeup while any deadline is armed.
    match deadline {
        Some(deadline) => match timeout_at(deadline, event).await {
            Timeout::Completed(wake) => wake,
            Timeout::Elapsed => ScopeWake::Deadline,
        },
        None => event.await,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        task::{Context, Waker},
        time::Duration,
    };

    #[tokio::test]
    async fn scope_wait_prefers_signal_when_both_control_futures_are_ready() {
        let (_sender, mut receiver) = crate::unbounded_mpsc::<()>();

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::ready(()),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            None,
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::Signal));
    }

    #[tokio::test]
    async fn scope_wait_prefers_a_primary_event_over_a_control_backlog() {
        let (sender, mut receiver) = crate::unbounded_mpsc();
        let (control_sender, mut control_receiver) = crate::unbounded_mpsc();
        for value in 0..128 {
            assert!(control_sender.send(value).is_ok());
        }
        assert!(sender.send(999).is_ok());

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::Message(Some(999))));
        assert_eq!(control_receiver.try_recv(), Some(0));
    }

    #[tokio::test]
    async fn scope_wait_reports_parent_shutdown_directly() {
        let (_sender, mut receiver) = crate::unbounded_mpsc::<()>();
        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            None,
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::ParentShutdown));
    }

    /// Each arm alone only pins its own `ScopeWake` mapping; the `biased;`
    /// precedence is a property of ties. Walk the chain
    /// `signal > parent_shutdown > message > control > deadline` with every
    /// weaker arm simultaneously ready, so dropping `biased;` cannot pass.
    #[tokio::test]
    async fn scope_wait_resolves_simultaneous_arms_in_declaration_order() {
        let elapsed = crate::now();
        let (sender, mut receiver) = crate::unbounded_mpsc();
        let (control_sender, mut control_receiver) = crate::unbounded_mpsc();
        assert!(sender.send(1_u8).is_ok());
        assert!(control_sender.send(2_u8).is_ok());

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::ready(()),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Signal));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::ParentShutdown));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Message(Some(1))));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::ControlMessage(Some(2))));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Deadline));
    }

    #[tokio::test(start_paused = true)]
    async fn scope_wait_reports_deadline_and_keeps_an_absent_deadline_pending() {
        let (_sender, mut receiver) = crate::unbounded_mpsc::<()>();
        let deadline = crate::now() + Duration::from_secs(10);
        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            None,
            Some(deadline),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Deadline));

        let (sender, mut receiver) = crate::unbounded_mpsc();
        let mut waiting = Box::pin(super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            None,
            None,
        ));
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(sender.send(7_u8).is_ok());
        assert!(matches!(waiting.await, super::ScopeWake::Message(Some(7))));
    }

    #[tokio::test]
    async fn select_two_covers_both_sides_and_biases_ready_ties_left() {
        assert!(matches!(
            super::select_two(std::future::ready(1_u8), std::future::pending::<u8>()).await,
            super::Either::Left(1)
        ));
        assert!(matches!(
            super::select_two(std::future::pending::<u8>(), std::future::ready(2_u8)).await,
            super::Either::Right(2)
        ));
        assert!(matches!(
            super::select_two(std::future::ready(3_u8), std::future::ready(4_u8)).await,
            super::Either::Left(3)
        ));
    }
}
