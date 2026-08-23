#[path = "mod.rs"]
mod common;

use std::{
    cell::Cell,
    future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::Duration,
};

use common::{
    ConsumeCount, LiveFlag, ReleaseGate, assert_quiet, hostile_waker, poll_once,
    probe_waker_with_wake,
};

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

/// The integration-side raw-waker probe is a hand-written vtable restated from
/// the crate-internal twin, and every fixture built on it -- `hostile_waker`
/// above all -- is consumed by abort-class regressions that assert only that
/// the process survives. A probe that silently stopped invoking its callbacks
/// would therefore leave those suites passing with nothing injected. These
/// pins are what makes that drift loud.
#[test]
fn probe_waker_routes_clone_wake_and_drop_without_leaking() {
    let clones = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let retained = Arc::new(());

    let clone_count = Arc::clone(&clones);
    let clone_retained = Arc::clone(&retained);
    let wake_count = Arc::clone(&wakes);
    let wake_retained = Arc::clone(&retained);
    let drop_count = Arc::clone(&drops);
    let drop_retained = Arc::clone(&retained);
    let waker = probe_waker_with_wake(
        move || {
            let _retained = &clone_retained;
            clone_count.fetch_add(1, Ordering::SeqCst);
        },
        move || {
            let _retained = &wake_retained;
            wake_count.fetch_add(1, Ordering::SeqCst);
        },
        move || {
            let _retained = &drop_retained;
            drop_count.fetch_add(1, Ordering::SeqCst);
        },
    );

    let by_ref = waker.clone();
    by_ref.wake_by_ref();
    drop(by_ref);
    let consumed = waker.clone();
    consumed.wake();
    drop(waker);

    assert_eq!(clones.load(Ordering::SeqCst), 2);
    assert_eq!(wakes.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(Arc::strong_count(&retained), 1);
}

#[test]
fn panicking_consuming_wake_still_retires_its_raw_reference() {
    let first_wake = Arc::new(AtomicBool::new(true));
    let drops = Arc::new(AtomicUsize::new(0));
    let retained = Arc::new(());

    let wake_once = Arc::clone(&first_wake);
    let wake_retained = Arc::clone(&retained);
    let drop_count = Arc::clone(&drops);
    let drop_retained = Arc::clone(&retained);
    let waker = probe_waker_with_wake(
        || {},
        move || {
            let _retained = &wake_retained;
            if wake_once.swap(false, Ordering::SeqCst) {
                panic!("injected wake panic");
            }
        },
        move || {
            let _retained = &drop_retained;
            drop_count.fetch_add(1, Ordering::SeqCst);
        },
    );
    let consumed = waker.clone();

    let panic = catch_unwind(AssertUnwindSafe(|| consumed.wake()));
    assert!(
        panic.is_err(),
        "the wake callback panic reaches the harness"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(waker);

    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&retained), 1);
}

/// `hostile_waker`'s whole contract is the destructor it installs on every
/// registration the framework clones. The suites that consume it judge the
/// absence of an abort, so the injection itself is pinned here.
#[test]
fn hostile_waker_registrations_panic_with_their_named_payload() {
    const INJECTED: &str = "injected fixture caller-waker drop panic";

    let hostile = hostile_waker(INJECTED);
    // `ManuallyDrop`'s own `Clone` would yield another leaked handle, so clone
    // the `Waker` itself: only a real registration reaches the drop vtable.
    let registration = Waker::clone(&hostile);

    let payload = catch_unwind(AssertUnwindSafe(move || drop(registration)))
        .expect_err("a cloned hostile registration panics when it is destroyed");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&INJECTED));
}
