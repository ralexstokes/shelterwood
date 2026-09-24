use std::{
    mem::ManuallyDrop,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{RawWaker, RawWakerVTable, Wake, Waker},
};

pub(crate) struct CountWake(pub(crate) Arc<AtomicUsize>);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) struct CountPanicWake {
    pub(crate) wakes: Arc<AtomicUsize>,
    pub(crate) message: &'static str,
}

impl Wake for CountPanicWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(self.message);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(self.message);
    }
}

struct LastWakerDropPanics {
    drops: Arc<AtomicUsize>,
    message: &'static str,
}

unsafe fn clone_last_drop_panics(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let probe = ManuallyDrop::new(unsafe { Arc::<LastWakerDropPanics>::from_raw(data.cast()) });
    RawWaker::new(
        Arc::into_raw(Arc::clone(&probe)).cast(),
        &LAST_DROP_PANICS_VTABLE,
    )
}

unsafe fn wake_last_drop_panics(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this waker.
    drop(unsafe { Arc::<LastWakerDropPanics>::from_raw(data.cast()) });
}

unsafe fn wake_by_ref_last_drop_panics(_data: *const ()) {}

unsafe fn drop_last_drop_panics(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this waker.
    let probe = unsafe { Arc::<LastWakerDropPanics>::from_raw(data.cast()) };
    let last = Arc::strong_count(&probe) == 1;
    if last {
        probe.drops.fetch_add(1, Ordering::SeqCst);
    }
    let message = probe.message;
    drop(probe);
    if last {
        std::panic::panic_any(message);
    }
}

static LAST_DROP_PANICS_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_last_drop_panics,
    wake_last_drop_panics,
    wake_by_ref_last_drop_panics,
    drop_last_drop_panics,
);

pub(crate) fn last_drop_panics_waker(message: &'static str, drops: Arc<AtomicUsize>) -> Waker {
    let raw = RawWaker::new(
        Arc::into_raw(Arc::new(LastWakerDropPanics { drops, message })).cast(),
        &LAST_DROP_PANICS_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    unsafe { Waker::from_raw(raw) }
}

struct TransitionOnClone {
    transition: Box<dyn Fn() + Send + Sync>,
}

unsafe fn clone_transition_on_clone(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let probe = ManuallyDrop::new(unsafe { Arc::<TransitionOnClone>::from_raw(data.cast()) });
    (probe.transition)();
    RawWaker::new(
        Arc::into_raw(Arc::clone(&probe)).cast(),
        &TRANSITION_ON_CLONE_VTABLE,
    )
}

unsafe fn wake_transition_on_clone(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this waker.
    drop(unsafe { Arc::<TransitionOnClone>::from_raw(data.cast()) });
}

unsafe fn wake_by_ref_transition_on_clone(_data: *const ()) {}

unsafe fn drop_transition_on_clone(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this waker.
    drop(unsafe { Arc::<TransitionOnClone>::from_raw(data.cast()) });
}

static TRANSITION_ON_CLONE_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_transition_on_clone,
    wake_transition_on_clone,
    wake_by_ref_transition_on_clone,
    drop_transition_on_clone,
);

pub(crate) fn transition_on_clone_waker(transition: impl Fn() + Send + Sync + 'static) -> Waker {
    let raw = RawWaker::new(
        Arc::into_raw(Arc::new(TransitionOnClone {
            transition: Box::new(transition),
        }))
        .cast(),
        &TRANSITION_ON_CLONE_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    unsafe { Waker::from_raw(raw) }
}

pub(crate) fn assert_panic_message(payload: &(dyn std::any::Any + Send), expected: &'static str) {
    assert_eq!(
        payload.downcast_ref::<&'static str>().copied(),
        Some(expected)
    );
}
