mod common;

use std::{cell::Cell, future, task::Poll, time::Duration};

use common::{ConsumeCount, LiveFlag, ReleaseGate, assert_quiet, poll_once};

#[tokio::test]
async fn release_gate_stores_one_permit() {
    let gate = ReleaseGate::default();
    gate.release();
    gate.wait().await;
}

#[test]
#[should_panic(expected = "cannot run more than one release ahead of its waiters")]
fn release_gate_rejects_a_release_no_waiter_asked_for() {
    let gate = ReleaseGate::default();
    gate.release();
    gate.release();
}

#[tokio::test]
async fn release_gate_serves_every_wait_that_has_begun() {
    let gate = ReleaseGate::default();
    let mut first = Box::pin(gate.wait());
    let mut second = Box::pin(gate.wait());
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());

    // Back-to-back releases for two already-parked waiters are ordinary
    // usage: the check counts demand, not whichever permits the semaphore
    // happens to be storing at this instant.
    gate.release();
    gate.release();

    first.await;
    second.await;
}

#[tokio::test]
async fn cancelling_a_wait_returns_its_release_to_the_gate() {
    let gate = ReleaseGate::default();
    let mut waiting = Box::pin(gate.wait());
    assert!(poll_once(waiting.as_mut()).is_pending());

    gate.release();
    // Tokio hands the permit to the parked waiter and takes it back when
    // that future is dropped. The wait still counts as demand, so the
    // replacement release is not an over-release.
    drop(waiting);
    gate.release();

    gate.wait().await;
}

#[test]
fn guards_report_drop_and_consumption() {
    let (flag, guard) = LiveFlag::guarded();
    assert!(flag.is_live());
    drop(guard);
    assert!(!flag.is_live());

    let count = ConsumeCount::default();
    count.guard().consume();
    count.assert_once();
}

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

#[tokio::test]
async fn eventual_assertion_context_is_evaluated_only_on_failure() {
    let evaluated = Cell::new(false);

    crate::common::assert_eventually!(|| true, "{}", {
        evaluated.set(true);
        "unexpected context evaluation"
    })
    .await;

    assert!(!evaluated.get());
}
