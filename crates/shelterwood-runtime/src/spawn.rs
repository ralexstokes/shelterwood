#[cfg(any(test, feature = "test-util"))]
use std::panic::resume_unwind;
use std::{
    future::{Future, poll_fn},
    task::Poll,
};

use tokio::task;

use shelterwood_core::exit::JoinOutcome;

use super::{contain_panic_payload, waker_proxy::ProxiedPoll};

/// Counts the runtime's currently alive spawned tasks, keeping runtime
/// metrics access in this module.
#[cfg(any(test, feature = "test-util"))]
pub fn alive_task_count() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Whether an ambient runtime is reachable.
///
/// This cannot tell whether that runtime has its time driver enabled: the
/// pinned Tokio exposes no non-panicking probe for it (the handle's time
/// accessor is `tokio_unstable`-only), and provoking the panic would run the
/// user's panic hook. `BuildError::NoRuntime` documents the requirement at
/// the public surface instead.
/// Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
pub fn is_available() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

/// One task hosted on a runtime the test can destroy underneath it.
///
/// Runtime teardown is the only way to produce a genuinely cancelled spawned
/// task, and it cannot be staged from inside the runtime being torn down:
/// dropping a runtime blocks until its workers stop, and a test awaiting the
/// consequences of that teardown is itself a task on some runtime. So the task
/// under test gets its own runtime on its own thread, while the assertions stay
/// on the caller's runtime, and the cancellation edge between them is real.
///
/// The teardown signal is the request channel closing, which covers explicit
/// [`shutdown`](Self::shutdown) and dropping this handle alike — a test that
/// panics before tearing down still releases the thread.
#[cfg(any(test, feature = "test-util"))]
pub struct DedicatedRuntime {
    teardown: std::sync::mpsc::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}

#[cfg(any(test, feature = "test-util"))]
impl DedicatedRuntime {
    /// Two workers: one for the hosted task, one so a task it spawns or wakes
    /// still makes progress while the first is parked.
    const WORKER_THREADS: usize = 2;

    /// Spawns `task` onto a fresh dedicated runtime.
    pub fn spawn<F>(task: F) -> (Self, JoinHandle<F::Output>)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(Self::WORKER_THREADS)
            .enable_all()
            .build()
            .expect("a dedicated runtime builds");
        let handle = JoinHandle {
            inner: runtime.spawn(task),
        };
        let (teardown, request) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            // Both teardown edges arrive as this receive returning: a request
            // to shut down, or the disconnect from a dropped handle.
            let _ = request.recv();
            drop(runtime);
        });
        (Self { teardown, thread }, handle)
    }

    /// Destroys the runtime, cancelling the hosted task, and waits for the
    /// teardown to finish.
    ///
    /// The wait is offloaded because a dropping runtime blocks its thread until
    /// every worker has stopped; awaiting that here keeps the caller's own
    /// worker free.
    pub async fn shutdown(self) {
        let Self { teardown, thread } = self;
        drop(teardown);
        join_resuming(spawn_blocking(move || {
            thread.join().expect("dedicated runtime teardown completes")
        }))
        .await;
    }
}

pub struct ActorWork {
    handle: Option<JoinHandle<()>>,
}

impl ActorWork {
    pub fn abort(&self) {
        // Diagnostic-only: a handle is taken only by `join`, which consumes
        // the work, so the slot is populated on every reachable call. This
        // can be sampled from a locked control path, so aborting nothing is
        // the total release behavior and no test depends on the diagnostic.
        debug_assert!(
            self.handle.is_some(),
            "actor work retains its join handle until join"
        );
        if let Some(handle) = &self.handle {
            handle.inner.abort();
        }
    }

    pub async fn join(mut self) -> JoinOutcome<()> {
        let handle = self
            .handle
            .take()
            .expect("actor work retains its join handle until join");
        join(handle).await
    }
}

impl Drop for ActorWork {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.inner.abort();
        }
    }
}

pub fn spawn_actor_work(future: impl Future<Output = ()> + Send + 'static) -> ActorWork {
    let handle = spawn(future);
    ActorWork {
        handle: Some(handle),
    }
}

/// A spawned operation owned by the library.
pub struct JoinHandle<T> {
    inner: task::JoinHandle<T>,
}

#[derive(Clone)]
pub struct AbortHandle(task::AbortHandle);

impl AbortHandle {
    pub fn abort(&self) {
        self.0.abort();
    }
}

impl<T> JoinHandle<T> {
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle(self.inner.abort_handle())
    }
}

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle {
        inner: task::spawn(future),
    }
}

#[cfg(any(test, feature = "test-util"))]
fn spawn_blocking<F, T>(operation: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    JoinHandle {
        inner: task::spawn_blocking(operation),
    }
}

/// Joins a task polled only from framework-task venues.
///
/// The waker parked raw in Tokio's join trailer here is the polling
/// executor's own, so its destruction stays framework traffic. A future that
/// a public API caller polls supplies that caller's waker instead and must
/// join through [`join_user_polled`].
pub async fn join<T>(handle: JoinHandle<T>) -> JoinOutcome<T> {
    let JoinHandle { inner } = handle;
    classify_join_result(inner.await)
}

/// Joins a task from a future polled directly by a public API caller.
///
/// Tokio retains the polling task's raw waker in the join trailer. In pinned
/// Tokio 1.53.1 that waker survives task completion until the `JoinHandle` is
/// dropped, when the awaiting frame can already own a `JoinError::Panic` and
/// its opaque panic payload. Park only a stable framework proxy in Tokio,
/// retire the real caller waker synchronously and with containment before the
/// ready result crosses this boundary, then let the handle destroy the proxy.
///
/// Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
///
/// The ordinary [`join`] remains the lower-cost path for framework-task
/// venues, whose executor wakers are not supplied by a public caller.
pub async fn join_user_polled<T>(handle: JoinHandle<T>) -> JoinOutcome<T> {
    let JoinHandle { inner } = handle;
    classify_join_result(poll_join_user_waker(inner).await)
}

async fn poll_join_user_waker<T>(mut inner: task::JoinHandle<T>) -> Result<T, task::JoinError> {
    let mut caller_poll = ProxiedPoll::new();
    let result = poll_fn(|context| {
        caller_poll.poll(
            &mut inner,
            context,
            |inner, context| std::pin::Pin::new(inner).poll(context),
            Poll::is_pending,
        )
    })
    .await;

    // `result` can own Tokio's opaque panic payload. The real caller waker was
    // already retired above, so dropping the completed handle dispatches only
    // framework proxy vtables while that payload is live.
    drop(inner);
    result
}

fn classify_join_result<T>(result: Result<T, task::JoinError>) -> JoinOutcome<T> {
    match result {
        Ok(value) => JoinOutcome::Ok { value },
        Err(error) if error.is_panic() => JoinOutcome::Panic {
            message: contain_panic_payload(error.into_panic()),
        },
        Err(error) => {
            assert!(error.is_cancelled());
            JoinOutcome::Cancelled
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
pub async fn join_resuming<T>(handle: JoinHandle<T>) -> T {
    let JoinHandle { inner } = handle;
    match inner.await {
        Ok(value) => value,
        Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
        Err(error) => {
            assert!(error.is_cancelled());
            panic!("library-owned operation task was unexpectedly cancelled")
        }
    }
}

pub async fn yield_now() {
    task::yield_now().await;
}

#[cfg(test)]
#[allow(unsafe_code)] // raw-waker test doubles
mod tests {
    use std::{
        mem::ManuallyDrop,
        panic,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
        thread,
        time::{Duration, Instant},
    };

    use crate::test_support::{DISPOSAL_THREAD, PanickingDrop, RecordingDrop};

    struct JoinWakeState {
        woken: AtomicBool,
        dropped: mpsc::Sender<crate::test_support::ThreadDescription>,
        clone_action: Mutex<Option<CloneJoinAction>>,
    }

    struct CloneJoinAction {
        release: tokio::sync::oneshot::Sender<()>,
        finished: super::AbortHandle,
    }

    unsafe fn clone_join_waker(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let state = ManuallyDrop::new(unsafe { Arc::<JoinWakeState>::from_raw(data.cast()) });
        let action = state
            .clone_action
            .lock()
            .expect("join clone-action mutex is not poisoned")
            .take();
        if let Some(CloneJoinAction { release, finished }) = action {
            release
                .send(())
                .expect("the join task still awaits its release");
            let deadline = Instant::now() + Duration::from_secs(1);
            while !finished.0.is_finished() {
                assert!(
                    Instant::now() < deadline,
                    "the released join task completes while its no-op waker is parked"
                );
                thread::yield_now();
            }
        }
        RawWaker::new(Arc::into_raw(Arc::clone(&state)).cast(), &JOIN_WAKER_VTABLE)
    }

    unsafe fn wake_join_waker(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<JoinWakeState>::from_raw(data.cast()) };
        state.woken.store(true, Ordering::SeqCst);
    }

    unsafe fn wake_by_ref_join_waker(data: *const ()) {
        // SAFETY: ManuallyDrop preserves the reference represented by `data`.
        let state = ManuallyDrop::new(unsafe { Arc::<JoinWakeState>::from_raw(data.cast()) });
        state.woken.store(true, Ordering::SeqCst);
    }

    unsafe fn drop_join_waker(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let state = unsafe { Arc::<JoinWakeState>::from_raw(data.cast()) };
        let current = thread::current();
        let _ = state
            .dropped
            .send((current.id(), current.name().map(str::to_owned)));
        drop(state);
        panic!("hostile public-join waker destructor");
    }

    static JOIN_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_join_waker,
        wake_join_waker,
        wake_by_ref_join_waker,
        drop_join_waker,
    );

    unsafe fn panic_clone_join_waker(_data: *const ()) -> RawWaker {
        panic!("an already-ready join must not clone its caller waker")
    }

    unsafe fn no_op_join_waker(_data: *const ()) {}

    static PANIC_CLONE_JOIN_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        panic_clone_join_waker,
        no_op_join_waker,
        no_op_join_waker,
        no_op_join_waker,
    );

    fn panic_clone_join_waker_value() -> Waker {
        let raw = RawWaker::new(std::ptr::null(), &PANIC_CLONE_JOIN_WAKER_VTABLE);
        // SAFETY: the vtable never dereferences or owns its null data pointer.
        unsafe { Waker::from_raw(raw) }
    }

    fn panicking_drop_join_waker(
        clone_action: Option<CloneJoinAction>,
    ) -> (
        ManuallyDrop<Waker>,
        Arc<JoinWakeState>,
        mpsc::Receiver<crate::test_support::ThreadDescription>,
    ) {
        let (dropped, observed_drop) = mpsc::channel();
        let state = Arc::new(JoinWakeState {
            woken: AtomicBool::new(false),
            dropped,
            clone_action: Mutex::new(clone_action),
        });
        let raw = RawWaker::new(Arc::into_raw(Arc::clone(&state)).cast(), &JOIN_WAKER_VTABLE);
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        let waker = unsafe { Waker::from_raw(raw) };
        (ManuallyDrop::new(waker), state, observed_drop)
    }

    #[tokio::test]
    async fn dropping_actor_work_aborts_its_task() {
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (dropped, dropped_rx) = mpsc::channel();
        let work = super::spawn_actor_work(async move {
            let _notice = RecordingDrop(dropped);
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("the actor work starts");

        drop(work);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match dropped_rx.try_recv() {
                    Ok(_) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("the aborted actor work did not drop its task state")
                    }
                }
            }
        })
        .await
        .expect("actor-work drop cancellation completes");
    }

    #[tokio::test]
    async fn actor_work_abort_is_idempotent_and_join_reports_cancelled() {
        let work = super::spawn_actor_work(std::future::pending::<()>());
        work.abort();
        work.abort();

        assert!(matches!(work.join().await, super::JoinOutcome::Cancelled));
    }

    /// Tokio's ready join result can own the task's opaque panic payload while
    /// dropping the caller waker retained in the handle trailer. Combining a
    /// panicking payload destructor with a panicking waker destructor used to
    /// make that geometry abort the process. This test intentionally installs
    /// both; nextest's process isolation turns a regression into this test's
    /// failure rather than taking the whole suite with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn user_polled_join_separates_hostile_waker_and_panic_payload_destruction() {
        let polling_thread = thread::current().id();
        let (release, released) = tokio::sync::oneshot::channel();
        let (payload_dropped, payload_dropped_rx) = mpsc::channel();
        let handle = super::spawn(async move {
            let _ = released.await;
            panic::panic_any(PanickingDrop(payload_dropped));
        });
        let finished = handle.abort_handle();
        let mut join = Box::pin(super::join_user_polled(handle));
        let (hostile, _state, observed_waker_drop) =
            panicking_drop_join_waker(Some(CloneJoinAction { release, finished }));

        // The first no-op probe parks Pending. Cloning the caller waker then
        // releases the task and waits until Tokio marks it complete, so the
        // second probe in the same poll returns Ready while the caller waker
        // is still installed in the framework proxy. This pins the narrow
        // ready-retirement path rather than relying on a scheduler race.
        assert!(matches!(
            join.as_mut().poll(&mut Context::from_waker(&hostile)),
            Poll::Ready(super::JoinOutcome::Panic { message: None })
        ));
        let (waker_drop_thread, _) = observed_waker_drop
            .recv_timeout(Duration::from_secs(1))
            .expect("ready joins retire the caller waker before returning");
        assert_eq!(
            waker_drop_thread, polling_thread,
            "ready joins retire the caller waker synchronously before handle teardown"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match payload_dropped_rx.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("the hostile panic payload was not destroyed")
                    }
                }
            }
        })
        .await
        .expect("the hostile panic payload reaches detached disposal");
    }

    #[tokio::test]
    async fn already_ready_user_polled_join_never_clones_the_caller_waker() {
        let handle = super::spawn(async { 17_u8 });
        let finished = handle.abort_handle();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !finished.0.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the ready task completes before its first join poll");

        let mut join = Box::pin(super::join_user_polled(handle));
        let caller = panic_clone_join_waker_value();
        assert!(matches!(
            join.as_mut().poll(&mut Context::from_waker(&caller)),
            Poll::Ready(super::JoinOutcome::Ok { value: 17 })
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn user_polled_join_preserves_runtime_cancellation() {
        let (runtime, handle) = super::DedicatedRuntime::spawn(std::future::pending::<()>());
        let mut join = Box::pin(super::join_user_polled(handle));
        assert!(matches!(
            join.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));

        runtime.shutdown().await;

        assert!(matches!(
            join.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(super::JoinOutcome::Cancelled)
        ));
    }

    /// Cancelling a pending public join has no ready-result handoff point.
    /// Its caller waker may block on destruction, so the future's drop glue
    /// must transfer retirement to the detached disposal lane instead of
    /// running it on the holder's thread.
    #[tokio::test]
    async fn dropping_pending_user_polled_join_detaches_caller_waker_retirement() {
        let dropping_thread = thread::current().id();
        let handle = super::spawn(std::future::pending::<()>());
        let abort = handle.abort_handle();
        let mut join = Box::pin(super::join_user_polled(handle));
        let (hostile, _state, observed_drop) = panicking_drop_join_waker(None);

        assert!(matches!(
            join.as_mut().poll(&mut Context::from_waker(&hostile)),
            Poll::Pending
        ));
        drop(join);

        let (destructor_thread, destructor_name) = observed_drop
            .recv_timeout(Duration::from_secs(1))
            .expect("the pending join caller waker reaches detached disposal");
        assert_ne!(destructor_thread, dropping_thread);
        assert!(
            destructor_name.as_deref() == Some(DISPOSAL_THREAD)
                || destructor_name.as_deref() == Some("tokio-rt-worker"),
            "pending join drop uses either Tokio's blocking pool or the fallback disposal lane"
        );
        abort.abort();
    }
}
