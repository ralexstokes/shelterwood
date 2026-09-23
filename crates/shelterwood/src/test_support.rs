mod waker;

pub(crate) use waker::{probe_waker, probe_waker_with_wake};

use std::time::Duration;

use shelterwood_core::{ChildId, Incarnation, IncarnationCounter, Membership, ScopeIdentity};

/// Shared cooperative-teardown budget for real-clock shutdowns that expect
/// no stragglers. A green shutdown returns as soon as teardown finishes, so a
/// generous budget costs nothing; sizing it near the expected latency makes
/// scheduler starvation on a loaded machine indistinguishable from a
/// straggler. Paused-clock tests and tests whose budget is under test keep
/// their own literal.
pub(crate) const SHUTDOWN_BUDGET: Duration = Duration::from_secs(10);

pub(crate) fn mint_actor_membership() -> (Membership, IncarnationCounter) {
    ScopeIdentity::new()
        .mint_membership(&ChildId::from("actor"))
        .expect("membership available")
        .into_pair()
}

pub(crate) fn mint_actor_incarnation() -> Incarnation {
    let (_, mut incarnations) = mint_actor_membership();
    incarnations.mint().expect("incarnation available")
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use super::probe_waker_with_wake;

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
}
