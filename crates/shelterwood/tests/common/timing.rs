use std::{
    future::Future,
    panic::Location,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

/// Shared wall-clock budget for eventually-consistent test observations.
pub(crate) const POLL_TIMEOUT: Duration = Duration::from_secs(5);

const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Polls a pinned future once with a no-op waker.
pub(crate) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

/// Spin-polls a pinned future with a supplied test waker until it is ready.
pub(crate) fn poll_until_ready<F: Future>(mut future: Pin<&mut F>, waker: &Waker) -> F::Output {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut Context::from_waker(waker)) {
            return output;
        }
        assert!(
            Instant::now() < deadline,
            "future becomes ready before the test deadline"
        );
        std::thread::yield_now();
    }
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

/// Waits for an eventually-consistent predicate and reports its source text.
#[track_caller]
pub(crate) fn assert_eventually_predicate(
    expression: &'static str,
    predicate: impl FnMut() -> bool,
    context: impl FnOnce() -> Option<String>,
) -> impl Future<Output = ()> {
    let caller = Location::caller();
    async move {
        if poll_until(POLL_TIMEOUT, POLL_INTERVAL, predicate).await {
            return;
        }
        match context() {
            Some(context) => panic!(
                "predicate `{expression}` did not become true within {POLL_TIMEOUT:?} at {caller}: {context}"
            ),
            None => panic!(
                "predicate `{expression}` did not become true within {POLL_TIMEOUT:?} at {caller}"
            ),
        }
    }
}

/// Asserts that a predicate is false at 1 ms samples across a bounded quiet
/// window; it does not observe continuous truth between samples.
pub(crate) async fn assert_quiet(duration: Duration, mut predicate: impl FnMut() -> bool) {
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
