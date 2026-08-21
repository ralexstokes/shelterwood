//! Public-path pins that a system join never parks the caller's raw waker in
//! Tokio's join trailer: reverting `join_driver` to the raw `runtime::join`
//! makes each fixture fail at hostile caller-waker destruction. On the fixed
//! path every driver completion consumes the installed clone through the
//! proxy wake, so the hostile drop vtable never fires here — contained
//! ready-path destruction and the detached cancellation venue are pinned by
//! `shelterwood-runtime`'s `spawn` unit tests instead.

mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use common::{HostileWakerState, POLL_TIMEOUT, hostile_waker, policy::never, waiting};
use shelterwood::{ExitError, Readiness, ScopeRef, ScopeState, StartupError, TaskDef, Tree};

#[derive(Default)]
struct BlockingWakeState {
    started: tokio::sync::Notify,
    release: Mutex<WakeRelease>,
    released: Condvar,
}

#[derive(Default)]
struct WakeRelease {
    started: usize,
    released: usize,
    shutdown: bool,
}

impl BlockingWakeState {
    fn run(&self) {
        let mut release = self.release.lock().expect("release mutex is not poisoned");
        release.started += 1;
        let sequence = release.started;
        self.started.notify_one();
        while release.released < sequence && !release.shutdown {
            release = self
                .released
                .wait(release)
                .expect("release mutex is not poisoned");
        }
    }
}

impl Wake for BlockingWakeState {
    fn wake(self: Arc<Self>) {
        self.run();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.run();
    }
}

struct WakeController(Arc<BlockingWakeState>);

impl WakeController {
    fn new() -> (Self, Waker) {
        let state = Arc::new(BlockingWakeState::default());
        (Self(Arc::clone(&state)), Waker::from(state))
    }

    async fn wait_until_wake_is_blocked(&self) {
        tokio::time::timeout(POLL_TIMEOUT, async {
            loop {
                // Register before sampling the counter so a wake starting
                // between those operations leaves either a visible counter
                // edge or a stored notification permit.
                let started = self.0.started.notified();
                {
                    let release = self
                        .0
                        .release
                        .lock()
                        .expect("release mutex is not poisoned");
                    assert!(!release.shutdown, "the wake gate remains live");
                    if release.started > release.released {
                        return;
                    }
                }
                started.await;
            }
        })
        .await
        .expect("the system future is woken before the progress bound");
    }

    fn release_one(&self) {
        let mut release = self
            .0
            .release
            .lock()
            .expect("release mutex is not poisoned");
        assert!(
            release.released < release.started,
            "a blocked wake exists before it is released"
        );
        release.released += 1;
        // Every waiter rechecks its own sequence, so only the earliest
        // unreleased wake can leave even though all are notified.
        self.0.released.notify_all();
    }

    fn release_all_started(&self) {
        let mut release = self
            .0
            .release
            .lock()
            .expect("release mutex is not poisoned");
        assert!(
            release.released < release.started,
            "a blocked wake exists before the blocked prefix is released"
        );
        release.released = release.started;
        self.0.released.notify_all();
    }
}

impl Drop for WakeController {
    fn drop(&mut self) {
        let mut release = self
            .0
            .release
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        release.shutdown = true;
        self.0.released.notify_all();
    }
}

/// Drives a public system future to the narrow interval after its root scope
/// has published Stopped but before its root driver can return. The benign
/// waker is deliberately blocked inside that terminal publication. Wake calls
/// are released strictly in arrival order, and each poll happens before the
/// corresponding state sample. If that poll crosses into the join while
/// Stopped is being published, the hostile replacement is therefore installed
/// before the blocked publisher is released.
async fn park_hostile_waker_in_driver_join<F: Future>(
    mut future: Pin<&mut F>,
    scope: &ScopeRef,
    controller: &WakeController,
    benign: &Waker,
) -> (ManuallyDrop<Waker>, Arc<HostileWakerState>) {
    for _ in 0..16 {
        controller.wait_until_wake_is_blocked().await;
        assert!(matches!(
            future.as_mut().poll(&mut Context::from_waker(benign)),
            Poll::Pending
        ));
        if matches!(scope.snapshot().state, ScopeState::Stopped { .. }) {
            let (hostile, state) = hostile_waker("injected system-join caller-waker drop panic");
            assert!(matches!(
                future.as_mut().poll(&mut Context::from_waker(&hostile)),
                Poll::Pending
            ));
            return (hostile, state);
        }

        controller.release_one();
    }
    panic!("the system did not publish its terminal scope state");
}

/// `System::shutdown` performs the same root-driver join after its bounded
/// shutdown wait, so it must carry the proxy even though the public output is
/// the timeout verdict rather than the root stop reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_shutdown_never_parks_its_caller_waker_in_the_driver_join() {
    let system = waiting::tree().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let (controller, benign) = WakeController::new();
    let mut shutdown = Box::pin(system.shutdown(Duration::from_secs(1)));
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(&benign)),
        Poll::Pending
    ));

    let (hostile, state) =
        park_hostile_waker_in_driver_join(shutdown.as_mut(), &scope, &controller, &benign).await;
    controller.release_all_started();
    state.wait_until_woken(POLL_TIMEOUT).await;
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Ready(Ok(()))
    ));
}

/// Startup rollback consumes the same system owner and reaches the same join
/// seam only after preserving the startup error. This pins both pieces: proxy
/// proxied join cannot displace the error, and rollback remains successful.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_or_shutdown_never_parks_its_caller_waker_in_the_driver_join() {
    let mut tree = Tree::new();
    tree.add_task(
        "failure",
        TaskDef::new(|_| async { Err(ExitError::message("startup failure")) })
            .restart(never())
            .readiness(Readiness::Manual)
            .expect("manual readiness is supported"),
    )
    .expect("valid failing task");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    let (controller, benign) = WakeController::new();
    let mut rollback = Box::pin(system.start_or_shutdown(Duration::from_secs(1)));
    assert!(matches!(
        rollback.as_mut().poll(&mut Context::from_waker(&benign)),
        Poll::Pending
    ));

    let (hostile, state) =
        park_hostile_waker_in_driver_join(rollback.as_mut(), &scope, &controller, &benign).await;
    controller.release_all_started();
    state.wait_until_woken(POLL_TIMEOUT).await;
    let Poll::Ready(Err(error)) = rollback.as_mut().poll(&mut Context::from_waker(&hostile)) else {
        panic!("startup failure is returned after rollback")
    };
    assert!(matches!(error.startup, StartupError::StartupFailed(_)));
    assert!(error.rollback_timeout.is_none());
}
