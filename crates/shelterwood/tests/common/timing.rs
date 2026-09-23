use std::{
    future::Future,
    panic::Location,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

/// Shared wall-clock budget for eventually-consistent test observations.
pub(crate) const POLL_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared cooperative-teardown budget for real-clock shutdowns that expect
/// no stragglers.
///
/// A green shutdown returns as soon as teardown finishes, so a generous
/// budget costs nothing; sizing it near the expected latency makes scheduler
/// starvation on a loaded machine indistinguishable from a straggler. Tests
/// whose budget is part of the property under test — an asserted
/// `ShutdownTimeout`, a paused clock, or a race against an outer wall-clock
/// bound — keep their own literal.
pub(crate) const SHUTDOWN_BUDGET: Duration = Duration::from_secs(10);

const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Polls a pinned future once with a no-op waker.
pub(crate) fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

/// Spin-polls a pinned future with a supplied test waker until it is ready.
///
/// The spinning thread never yields to an async runtime, so readiness must
/// arrive from outside it — another worker or a blocking-pool thread. On a
/// `current_thread` flavor whose readiness needs same-thread task progress,
/// this helper spins out the full deadline and fails instead; use an
/// `await`-based wait there.
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
///
/// Under a paused clock each unsatisfied probe auto-advances virtual time by
/// 1 ms. That is how a predicate waiting on a timer makes progress. A
/// predicate that needs no timer should use
/// [`assert_eventually_frozen_predicate`] instead.
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

/// Waits for an eventually-consistent predicate without moving a paused
/// virtual clock, and reports its source text.
///
/// [`assert_eventually_predicate`] paces with a 1 ms `sleep`. Under
/// `start_paused` an idle runtime auto-advances to that sleep's deadline, so
/// every unsatisfied probe moves virtual time. Work Tokio does not track, such
/// as a native thread, then burns the whole virtual budget in a few real
/// milliseconds, and any probe can fire a framework deadline mid-assertion.
/// This variant yields instead. A yielding task keeps the runtime busy, so the
/// clock cannot auto-advance, and the budget is [`POLL_TIMEOUT`] of wall
/// time. It also asserts that virtual time did not move while it waited,
/// which makes it a paused-clock-only helper.
///
/// It is for paused-clock waits whose predicate needs task or thread progress
/// but no timer. A predicate that needs a timer to fire must use
/// `assert_eventually!`, or advance time explicitly first.
#[track_caller]
pub(crate) fn assert_eventually_frozen_predicate(
    expression: &'static str,
    mut predicate: impl FnMut() -> bool,
    context: impl FnOnce() -> Option<String>,
) -> impl Future<Output = ()> {
    let caller = Location::caller();
    async move {
        let frozen_at = tokio::time::Instant::now();
        let deadline = Instant::now() + POLL_TIMEOUT;
        let satisfied = loop {
            if predicate() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            // Give OS threads the CPU too: the work being awaited may run on
            // one, and this loop otherwise spins its runtime thread.
            std::thread::yield_now();
            tokio::task::yield_now().await;
        };
        assert_eq!(
            tokio::time::Instant::now(),
            frozen_at,
            "virtual time moved while `{expression}` was awaited at {caller}"
        );
        if satisfied {
            return;
        }
        match context() {
            Some(context) => panic!(
                "predicate `{expression}` did not become true within {POLL_TIMEOUT:?} of wall time at {caller}: {context}"
            ),
            None => panic!(
                "predicate `{expression}` did not become true within {POLL_TIMEOUT:?} of wall time at {caller}"
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
