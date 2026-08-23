use std::{
    mem::ManuallyDrop,
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use shelterwood_core::{ChildId, Incarnation, IncarnationCounter, Membership, ScopeIdentity};

struct WakerProbe {
    on_clone: Box<dyn Fn() + Send + Sync>,
    on_wake: Box<dyn Fn() + Send + Sync>,
    on_drop: Box<dyn Fn() + Send + Sync>,
}

unsafe fn clone_probe(data: *const ()) -> RawWaker {
    // SAFETY: `probe_waker` creates every pointer paired with this vtable from
    // `Arc<WakerProbe>`. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns exactly the newly cloned reference.
    let probe = ManuallyDrop::new(unsafe { Arc::<WakerProbe>::from_raw(data.cast()) });
    (probe.on_clone)();
    RawWaker::new(Arc::into_raw(Arc::clone(&probe)).cast(), &PROBE_VTABLE)
}

unsafe fn wake_probe(data: *const ()) {
    // SAFETY: wake consumes exactly the Arc reference represented by `data`.
    let probe = unsafe { Arc::<WakerProbe>::from_raw(data.cast()) };
    (probe.on_wake)();
}

unsafe fn wake_probe_by_ref(data: *const ()) {
    // SAFETY: `data` remains owned by the caller, so this temporary reference
    // neither consumes nor clones its Arc reference.
    let probe = unsafe { &*data.cast::<WakerProbe>() };
    (probe.on_wake)();
}

unsafe fn drop_probe(data: *const ()) {
    // SAFETY: drop consumes exactly the Arc reference represented by `data`.
    let probe = unsafe { Arc::<WakerProbe>::from_raw(data.cast()) };
    (probe.on_drop)();
}

static PROBE_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_probe, wake_probe, wake_probe_by_ref, drop_probe);

/// Builds a test-only waker whose clone and drop vtable entries are observable.
///
/// The shared raw-waker ownership implementation lives here so individual
/// regressions only supply the behavior they need to observe.
pub(crate) fn probe_waker(
    on_clone: impl Fn() + Send + Sync + 'static,
    on_drop: impl Fn() + Send + Sync + 'static,
) -> Waker {
    probe_waker_with_wake(on_clone, || {}, on_drop)
}

/// Builds a test-only waker that additionally observes wake operations.
pub(crate) fn probe_waker_with_wake(
    on_clone: impl Fn() + Send + Sync + 'static,
    on_wake: impl Fn() + Send + Sync + 'static,
    on_drop: impl Fn() + Send + Sync + 'static,
) -> Waker {
    let probe = Arc::new(WakerProbe {
        on_clone: Box::new(on_clone),
        on_wake: Box::new(on_wake),
        on_drop: Box::new(on_drop),
    });
    let raw = RawWaker::new(Arc::into_raw(probe).cast(), &PROBE_VTABLE);
    // SAFETY: `raw` owns one Arc reference and `PROBE_VTABLE` preserves or
    // consumes exactly one reference for each RawWaker operation.
    unsafe { Waker::from_raw(raw) }
}

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
