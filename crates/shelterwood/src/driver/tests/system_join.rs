use std::{
    mem::ManuallyDrop,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use super::support::*;
use crate::{cells::RetainedStopReason, policy::ScopeFlavor};

#[derive(Default)]
struct HostileWakeState {
    woken: tokio::sync::Notify,
}

unsafe fn clone_hostile_waker(data: *const ()) -> RawWaker {
    // SAFETY: every pointer using this vtable came from an Arc of the
    // matching type. ManuallyDrop preserves the reference represented by
    // `data`; the returned raw waker owns only the new clone.
    let state = ManuallyDrop::new(unsafe { Arc::<HostileWakeState>::from_raw(data.cast()) });
    RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &HOSTILE_WAKER_VTABLE,
    )
}

unsafe fn wake_hostile_waker(data: *const ()) {
    // SAFETY: wake consumes the Arc reference represented by this raw waker.
    let state = unsafe { Arc::<HostileWakeState>::from_raw(data.cast()) };
    state.woken.notify_one();
}

unsafe fn wake_by_ref_hostile_waker(data: *const ()) {
    // SAFETY: ManuallyDrop preserves the reference represented by `data`.
    let state = ManuallyDrop::new(unsafe { Arc::<HostileWakeState>::from_raw(data.cast()) });
    state.woken.notify_one();
}

unsafe fn drop_hostile_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<HostileWakeState>::from_raw(data.cast()) });
    panic!("injected SystemRun join caller-waker drop panic");
}

static HOSTILE_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_hostile_waker,
    wake_hostile_waker,
    wake_by_ref_hostile_waker,
    drop_hostile_waker,
);

fn hostile_waker() -> (ManuallyDrop<Waker>, Arc<HostileWakeState>) {
    let state = Arc::new(HostileWakeState::default());
    let raw = RawWaker::new(
        Arc::into_raw(Arc::clone(&state)).cast(),
        &HOSTILE_WAKER_VTABLE,
    );
    // SAFETY: `raw` owns one Arc reference and its vtable maintains that
    // ownership across clone, wake, and drop.
    let waker = unsafe { Waker::from_raw(raw) };
    (ManuallyDrop::new(waker), state)
}

/// `SystemRun::wait` is the exact common seam reached by public
/// `System::wait`, `System::shutdown`, and startup rollback after their
/// operation-specific waits. Publish terminal scope state up front and hold a
/// synthetic driver behind a latch, so the first poll reaches a provably
/// pending driver join without relying on the scheduler interval between the
/// real driver's terminal publication and completion.
#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_run_wait_proxies_a_pending_driver_join_caller_waker() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    root.terminalize_never_started();

    let release = Latch::default();
    let driver_release = release.clone();
    let driver = crate::runtime::spawn(async move {
        driver_release.fired().await;
        RetainedStopReason::new(StopReason::NeverStarted)
    });
    let mut run = super::super::SystemRun {
        root,
        driver: Some(driver),
    };
    let mut wait = Box::pin(run.wait());
    let (hostile, state) = hostile_waker();

    assert!(matches!(
        wait.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Pending
    ));
    assert!(release.fire(), "the synthetic driver is released once");
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, state.woken.notified()).await,
        crate::runtime::Timeout::Completed(())
    ));
    assert!(matches!(
        wait.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Ready(StopReason::NeverStarted)
    ));
}

/// `SystemRun::shutdown` is the exact join seam reached by both public
/// `System::shutdown` and failed-startup rollback. Settle the root up front and
/// hold a synthetic driver behind a latch so shutdown crosses its operation
/// wait and parks in a provably pending driver join. This avoids relying on
/// the scheduler interval between a real driver's terminal publication and
/// its monitor task completing.
#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_run_shutdown_proxies_a_pending_driver_join_caller_waker() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    root.terminalize_never_started();

    let release = Latch::default();
    let driver_release = release.clone();
    let driver = crate::runtime::spawn(async move {
        driver_release.fired().await;
        RetainedStopReason::new(StopReason::NeverStarted)
    });
    let mut run = super::super::SystemRun {
        root,
        driver: Some(driver),
    };
    let mut shutdown = Box::pin(run.shutdown(crate::DeadlineBudget::ZERO));
    let (hostile, state) = hostile_waker();

    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Pending
    ));
    assert!(release.fire(), "the synthetic driver is released once");
    assert!(matches!(
        crate::runtime::timeout(DRIVER_PROGRESS_WAIT, state.woken.notified()).await,
        crate::runtime::Timeout::Completed(())
    ));
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Ready(Ok(()))
    ));
}
