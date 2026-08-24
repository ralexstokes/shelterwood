use std::{
    mem::ManuallyDrop,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use super::support::*;
use crate::{cells::RetainedStopReason, policy::ScopeFlavor, test_support::probe_waker_with_wake};

#[derive(Default)]
struct HostileWakeState {
    woken: tokio::sync::Notify,
}

fn hostile_waker() -> (ManuallyDrop<Waker>, Arc<HostileWakeState>) {
    let state = Arc::new(HostileWakeState::default());
    let wake_state = Arc::clone(&state);
    let waker = probe_waker_with_wake(
        || {},
        move || wake_state.woken.notify_one(),
        || panic!("injected SystemRun join caller-waker drop panic"),
    );
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
        driver_joined: false,
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

#[crate::runtime::test]
async fn system_run_wait_reloads_after_self_healing_a_cancelled_monitor() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.finish_root_incarnation(
        epoch,
        StopReason::Finished,
        Exit::completed(Cancellation::NotObserved),
    );

    let driver = crate::runtime::spawn(future::pending::<RetainedStopReason>());
    driver.abort_handle().abort();
    let mut run = super::super::SystemRun {
        root: Arc::clone(&root),
        driver: Some(driver),
        driver_joined: false,
    };

    assert_eq!(run.wait().await, StopReason::ShutdownRequested);
    assert_eq!(
        root.record().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        },
        "wait reloads the record after its join fallback upgrades the verdict"
    );
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
        driver_joined: false,
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
