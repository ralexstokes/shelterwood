use std::{
    mem::ManuallyDrop,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{RawWaker, RawWakerVTable, Waker},
};

struct WakerProbe {
    on_clone: Box<dyn Fn() + Send + Sync>,
    on_drop: Box<dyn Fn() + Send + Sync>,
}

unsafe fn clone_probe(data: *const ()) -> RawWaker {
    // SAFETY: `probe_waker` creates every pointer paired with this vtable from
    // `Arc<WakerProbe>`. ManuallyDrop preserves the represented reference;
    // the returned raw waker owns exactly the newly cloned reference.
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

/// Builds a shared integration-test waker whose clone and drop entries are observable.
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
    action_on_wake: bool,
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
    let current = unsafe { Arc::<OrdinalWaker>::from_raw(data.cast()) };
    if current.shared.action_on_wake {
        run_ordinal_action(&current);
    }
}

unsafe fn wake_by_ref_ordinal_waker(_data: *const ()) {}

unsafe fn retire_ordinal_waker(data: *const ()) {
    // SAFETY: retirement consumes the Arc reference represented by this raw
    // waker.
    let current = unsafe { Arc::<OrdinalWaker>::from_raw(data.cast()) };
    run_ordinal_action(&current);
}

fn run_ordinal_action(current: &OrdinalWaker) {
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

/// Creates an ordinal waker whose target action runs when the target is
/// retired by either consuming `wake` or `drop`.
pub(crate) fn ordinal_waker(
    target: usize,
    action: impl FnOnce() + Send + 'static,
) -> (Waker, Arc<OrdinalWakerState>) {
    make_ordinal_waker(target, true, action)
}

/// Creates an ordinal waker whose target action runs only from the vtable's
/// `drop` operation, not its consuming `wake` operation.
pub(crate) fn ordinal_drop_waker(
    target: usize,
    action: impl FnOnce() + Send + 'static,
) -> (Waker, Arc<OrdinalWakerState>) {
    make_ordinal_waker(target, false, action)
}

fn make_ordinal_waker(
    target: usize,
    action_on_wake: bool,
    action: impl FnOnce() + Send + 'static,
) -> (Waker, Arc<OrdinalWakerState>) {
    let shared = Arc::new(OrdinalWakerState {
        clones: AtomicUsize::new(0),
        target: AtomicUsize::new(target),
        action_on_wake,
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

/// Creates a deliberately leaked caller waker whose cloned registrations
/// panic when destroyed.
pub(crate) fn hostile_waker(drop_panic: &'static str) -> ManuallyDrop<Waker> {
    ManuallyDrop::new(probe_waker(
        || {},
        move || std::panic::panic_any(drop_panic),
    ))
}
