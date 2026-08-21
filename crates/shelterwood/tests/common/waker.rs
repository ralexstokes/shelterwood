use std::{
    mem::ManuallyDrop,
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

/// State shared by a caller waker whose drop vtable always panics.
#[derive(Default)]
pub(crate) struct HostileWakerState {
    woken: tokio::sync::Notify,
    drop_panic: &'static str,
}

impl HostileWakerState {
    /// Waits until a clone of the hostile waker is woken.
    pub(crate) async fn wait_until_woken(&self, timeout: Duration) {
        tokio::time::timeout(timeout, self.woken.notified())
            .await
            .expect("the proxied operation wakes its public waiter");
    }
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
    let state = unsafe { Arc::<HostileWakerState>::from_raw(data.cast()) };
    state.woken.notify_one();
}

unsafe fn wake_by_ref_hostile_waker(data: *const ()) {
    // SAFETY: ManuallyDrop preserves the reference represented by `data`.
    let state = ManuallyDrop::new(unsafe { Arc::<HostileWakerState>::from_raw(data.cast()) });
    state.woken.notify_one();
}

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
/// panic when destroyed, plus observable wake state for synchronization.
pub(crate) fn hostile_waker(
    drop_panic: &'static str,
) -> (ManuallyDrop<Waker>, Arc<HostileWakerState>) {
    let state = Arc::new(HostileWakerState {
        woken: tokio::sync::Notify::new(),
        drop_panic,
    });
    let raw = RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &HOSTILE_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    let waker = unsafe { Waker::from_raw(raw) };
    (ManuallyDrop::new(waker), state)
}
