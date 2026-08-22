use std::{
    mem::ManuallyDrop,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{RawWaker, RawWakerVTable, Waker},
};

#[derive(Default)]
pub(crate) struct LiveWakerCounter(AtomicUsize);

impl LiveWakerCounter {
    pub(crate) fn live(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

unsafe fn clone_counting_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let state = ManuallyDrop::new(unsafe { Arc::<LiveWakerCounter>::from_raw(data.cast()) });
    state.0.fetch_add(1, Ordering::SeqCst);
    RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &COUNTING_WAKER_VTABLE,
    )
}

unsafe fn wake_counting_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    let state = unsafe { Arc::<LiveWakerCounter>::from_raw(data.cast()) };
    state.0.fetch_sub(1, Ordering::SeqCst);
}

unsafe fn wake_by_ref_counting_waker(_data: *const ()) {}

unsafe fn drop_counting_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    let state = unsafe { Arc::<LiveWakerCounter>::from_raw(data.cast()) };
    state.0.fetch_sub(1, Ordering::SeqCst);
}

static COUNTING_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_counting_waker,
    wake_counting_waker,
    wake_by_ref_counting_waker,
    drop_counting_waker,
);

pub(crate) fn counting_waker(counter: &Arc<LiveWakerCounter>) -> Waker {
    // Include the caller-owned waker in the live count so every handle using
    // this vtable has the same balanced clone/drop accounting.
    counter.0.fetch_add(1, Ordering::SeqCst);
    let raw = RawWaker::new(
        Arc::into_raw(Arc::clone(counter)).cast(),
        &COUNTING_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    unsafe { Waker::from_raw(raw) }
}

struct OrdinalWaker {
    ordinal: usize,
    shared: Arc<OrdinalWakerState>,
}

pub(crate) struct OrdinalWakerState {
    clones: AtomicUsize,
    target: AtomicUsize,
    action: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl OrdinalWakerState {
    pub(crate) fn clones(&self) -> usize {
        self.clones.load(Ordering::SeqCst)
    }

    pub(crate) fn target_latest_clone(&self, path: &str) {
        let target = self.clones();
        assert_ne!(target, 0, "{path} registers a caller waker");
        self.target.store(target, Ordering::SeqCst);
    }
}

unsafe fn clone_ordinal_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let current = ManuallyDrop::new(unsafe { Arc::<OrdinalWaker>::from_raw(data.cast()) });
    let ordinal = current.shared.clones.fetch_add(1, Ordering::SeqCst) + 1;
    RawWaker::new(
        Arc::into_raw(Arc::new(OrdinalWaker {
            ordinal,
            shared: Arc::clone(&current.shared),
        }))
        .cast(),
        &ORDINAL_WAKER_VTABLE,
    )
}

unsafe fn wake_ordinal_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    unsafe { retire_ordinal_waker(data) };
}

unsafe fn wake_by_ref_ordinal_waker(_data: *const ()) {}

unsafe fn retire_ordinal_waker(data: *const ()) {
    // SAFETY: retirement consumes the Arc reference represented by this raw
    // waker.
    let current = unsafe { Arc::<OrdinalWaker>::from_raw(data.cast()) };
    if current.ordinal != current.shared.target.load(Ordering::SeqCst) {
        return;
    }
    let action = current
        .shared
        .action
        .lock()
        .expect("ordinal waker mutex poisoned")
        .take();
    if let Some(action) = action {
        action();
    }
}

static ORDINAL_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_ordinal_waker,
    wake_ordinal_waker,
    wake_by_ref_ordinal_waker,
    retire_ordinal_waker,
);

pub(crate) fn ordinal_waker(
    target: usize,
    action: impl FnOnce() + Send + 'static,
) -> (Waker, Arc<OrdinalWakerState>) {
    let shared = Arc::new(OrdinalWakerState {
        clones: AtomicUsize::new(0),
        target: AtomicUsize::new(target),
        action: Mutex::new(Some(Box::new(action))),
    });
    let raw = RawWaker::new(
        Arc::into_raw(Arc::new(OrdinalWaker {
            ordinal: 0,
            shared: Arc::clone(&shared),
        }))
        .cast(),
        &ORDINAL_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    (unsafe { Waker::from_raw(raw) }, shared)
}

struct HostileWakerState {
    drop_panic: &'static str,
}

unsafe fn clone_hostile_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let state = ManuallyDrop::new(unsafe { Arc::<HostileWakerState>::from_raw(data.cast()) });
    RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &HOSTILE_WAKER_VTABLE,
    )
}

unsafe fn wake_hostile_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<HostileWakerState>::from_raw(data.cast()) });
}

unsafe fn wake_by_ref_hostile_waker(_data: *const ()) {}

unsafe fn drop_hostile_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    let state = unsafe { Arc::<HostileWakerState>::from_raw(data.cast()) };
    let drop_panic = state.drop_panic;
    drop(state);
    std::panic::panic_any(drop_panic);
}

static HOSTILE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_hostile_waker,
    wake_hostile_waker,
    wake_by_ref_hostile_waker,
    drop_hostile_waker,
);

/// Creates a deliberately leaked caller waker whose cloned registrations
/// panic when destroyed.
pub(crate) fn hostile_waker(drop_panic: &'static str) -> ManuallyDrop<Waker> {
    let state = Arc::new(HostileWakerState { drop_panic });
    let raw = RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &HOSTILE_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    let waker = unsafe { Waker::from_raw(raw) };
    ManuallyDrop::new(waker)
}
