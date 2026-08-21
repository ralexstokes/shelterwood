mod common;

use std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
    time::Duration,
};

use common::{POLL_TIMEOUT, policy::never, waiting};
use shelterwood::{
    ExitError, Readiness, ScopeRef, ScopeState, StartupError, StopReason, TaskDef, Tree,
};

#[derive(Default)]
struct BlockingWakeState {
    started: tokio::sync::Notify,
    release: Mutex<(usize, bool)>,
    released: Condvar,
}

impl BlockingWakeState {
    fn run(&self) {
        self.started.notify_one();
        let mut release = self.release.lock().expect("release mutex is not poisoned");
        while release.0 == 0 && !release.1 {
            release = self
                .released
                .wait(release)
                .expect("release mutex is not poisoned");
        }
        if !release.1 {
            release.0 -= 1;
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
        tokio::time::timeout(POLL_TIMEOUT, self.0.started.notified())
            .await
            .expect("the system future is woken before the progress bound");
    }

    fn release_one(&self) {
        let mut release = self
            .0
            .release
            .lock()
            .expect("release mutex is not poisoned");
        release.0 += 1;
        self.0.released.notify_one();
    }
}

impl Drop for WakeController {
    fn drop(&mut self) {
        let mut release = self
            .0
            .release
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        release.1 = true;
        self.0.released.notify_all();
    }
}

#[derive(Default)]
struct HostileWakeState {
    woken: tokio::sync::Notify,
}

impl HostileWakeState {
    fn wake(&self) {
        self.woken.notify_one();
    }
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
    state.wake();
}

unsafe fn wake_by_ref_hostile_waker(data: *const ()) {
    // SAFETY: ManuallyDrop preserves the reference represented by `data`.
    let state = ManuallyDrop::new(unsafe { Arc::<HostileWakeState>::from_raw(data.cast()) });
    state.wake();
}

unsafe fn drop_hostile_waker(data: *const ()) {
    // SAFETY: drop consumes the Arc reference represented by this raw waker.
    drop(unsafe { Arc::<HostileWakeState>::from_raw(data.cast()) });
    panic!("injected system-join caller-waker drop panic");
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

/// Drives a public system future to the narrow interval after its root scope
/// has published Stopped but before its root driver can return. The benign
/// waker is deliberately blocked inside that terminal publication, making the
/// following poll deterministically reach the still-pending Tokio join handle.
async fn park_hostile_waker_in_driver_join<F: Future>(
    mut future: Pin<&mut F>,
    scope: &ScopeRef,
    controller: &WakeController,
    benign: &Waker,
) -> (ManuallyDrop<Waker>, Arc<HostileWakeState>) {
    for _ in 0..16 {
        controller.wait_until_wake_is_blocked().await;
        if matches!(scope.snapshot().state, ScopeState::Stopped { .. }) {
            let (hostile, state) = hostile_waker();
            assert!(matches!(
                future.as_mut().poll(&mut Context::from_waker(&hostile)),
                Poll::Pending
            ));
            return (hostile, state);
        }

        assert!(matches!(
            future.as_mut().poll(&mut Context::from_waker(benign)),
            Poll::Pending
        ));
        controller.release_one();
    }
    panic!("the system did not publish its terminal scope state");
}

async fn wait_for_join_wake(state: &HostileWakeState) {
    tokio::time::timeout(POLL_TIMEOUT, state.woken.notified())
        .await
        .expect("the joined root driver wakes its public waiter");
}

/// `System::wait` reaches the shared `join_driver` seam after the natural
/// terminal wait. A hostile caller-waker destructor must be contained before
/// the joined output is handed back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_wait_contains_join_waker_retirement() {
    let system = waiting::tree().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let (controller, benign) = WakeController::new();
    let mut wait = Box::pin(system.wait());
    assert!(matches!(
        wait.as_mut().poll(&mut Context::from_waker(&benign)),
        Poll::Pending
    ));
    // Request from a foreign thread because this test intentionally blocks
    // the registered waker inside the synchronous request publication.
    let shutdown_scope = scope.clone();
    let requester = std::thread::spawn(move || shutdown_scope.request_shutdown());

    let (hostile, state) =
        park_hostile_waker_in_driver_join(wait.as_mut(), &scope, &controller, &benign).await;
    controller.release_one();
    wait_for_join_wake(&state).await;
    assert!(matches!(
        wait.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Ready(StopReason::ShutdownRequested)
    ));
    requester.join().expect("shutdown requester returns");
}

/// `System::shutdown` performs the same root-driver join after its bounded
/// shutdown wait, so it must carry the proxy even though the public output is
/// the timeout verdict rather than the root stop reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_shutdown_contains_join_waker_retirement() {
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
    controller.release_one();
    wait_for_join_wake(&state).await;
    assert!(matches!(
        shutdown.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Ready(Ok(()))
    ));
}

/// Startup rollback consumes the same system owner and reaches the same join
/// seam only after preserving the startup error. This pins both pieces: proxy
/// retirement cannot replace the error, and rollback remains successful.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_or_shutdown_contains_join_waker_retirement() {
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
    controller.release_one();
    wait_for_join_wake(&state).await;
    let Poll::Ready(Err(error)) = rollback.as_mut().poll(&mut Context::from_waker(&hostile)) else {
        panic!("startup failure is returned after rollback")
    };
    assert!(matches!(error.startup, StartupError::StartupFailed(_)));
    assert!(error.rollback_timeout.is_none());
}
