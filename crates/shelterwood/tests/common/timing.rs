use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

/// Shared wall-clock budget for eventually-consistent test observations.
pub(crate) const POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// Polls a pinned future once with a no-op waker.
pub(crate) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

/// Advances Tokio's paused clock.
pub(crate) async fn advance_time(duration: Duration) {
    tokio::time::advance(duration).await;
}

/// Polls a synchronous observation until it succeeds or the deadline expires.
pub(crate) async fn poll_until(
    timeout: Duration,
    interval: Duration,
    mut predicate: impl FnMut() -> bool,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if predicate() {
                return;
            }
            tokio::time::sleep(interval).await;
        }
    })
    .await
    .is_ok()
}

/// Asserts that a predicate remains false for a bounded quiet window.
pub(crate) async fn assert_quiet(duration: Duration, mut predicate: impl FnMut() -> bool) {
    const POLL_INTERVAL: Duration = Duration::from_millis(1);

    let deadline = tokio::time::Instant::now() + duration;
    loop {
        assert!(!predicate(), "quiet-window predicate became true");
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL.min(deadline - now)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{future, task::Poll, time::Duration};

    use super::{assert_quiet, poll_once};

    #[test]
    fn one_poll_uses_a_noop_waker() {
        let mut future = Box::pin(future::ready(7));
        assert_eq!(poll_once(future.as_mut()), Poll::Ready(7));
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_window_completes_with_paused_time() {
        let duration = Duration::from_secs(1);
        let started_at = tokio::time::Instant::now();

        assert_quiet(duration, || false).await;

        assert_eq!(tokio::time::Instant::now() - started_at, duration);
    }
}
