use std::{
    mem::ManuallyDrop,
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use shelterwood_core::{ChildId, Incarnation, IncarnationCounter, Membership, ScopeIdentity};

struct WakerProbe {
    on_clone: Box<dyn Fn() + Send + Sync>,
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
    drop(unsafe { Arc::<WakerProbe>::from_raw(data.cast()) });
}

unsafe fn wake_probe_by_ref(_data: *const ()) {}

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
    let probe = Arc::new(WakerProbe {
        on_clone: Box::new(on_clone),
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
