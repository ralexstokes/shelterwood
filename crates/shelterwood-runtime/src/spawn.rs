use std::{
    any::Any,
    future::{Future, poll_fn},
    ops::RangeBounds,
    panic::resume_unwind,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::Poll,
};

use tokio::{sync::mpsc, task};

use shelterwood_core::exit::JoinOutcome;

use super::{
    DisposingReceiver, OneShotReceiver, OneShotSender, PanicPayload, Timeout, catch_panic,
    discard_panic, dispose_detached, oneshot, timeout_at, waker_proxy::ProxiedPoll,
};

const BLOCKING_FALLBACK_THREAD: &str = "shelterwood-blocking";

type BlockingOutcome<T> = Result<T, PanicPayload>;
type BlockingCompletion<T> = OneShotSender<BlockingOutcome<T>>;

/// Counts the runtime's currently alive spawned tasks, keeping runtime
/// metrics access in this module.
#[cfg(any(test, feature = "test-util"))]
pub fn alive_task_count() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

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

pub fn spawn_blocking_work<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> impl Future<Output = T> + Send {
    let (completion, receiver) = oneshot();
    let job = BlockingJob::new(operation, completion);

    if !submit_blocking_job(&job) {
        // A blocking operation cannot share disposal's single fallback queue:
        // one legitimately long operation would strand every later job. This
        // path exists only for runtime teardown, so one detached thread per
        // rejected operation is the appropriate degradation.
        spawn_blocking_fallback_with(&job, |worker| {
            std::thread::Builder::new()
                .name(BLOCKING_FALLBACK_THREAD.to_owned())
                .spawn(move || worker.run())
        });
    }
    drop(job);

    receive_blocking(receiver)
}

/// Awaits one blocking outcome, disposing it if the awaiting future is dropped.
///
/// The disposing wrapper is taken here rather than inside the returned future
/// because a future dropped before its first poll never runs its own body: the
/// operation can still have stored a user value by then, and reclaiming it
/// would run that user destructor in the awaiting task's drop glue.
fn receive_blocking<T: Send + 'static>(
    receiver: OneShotReceiver<BlockingOutcome<T>>,
) -> impl Future<Output = T> + Send {
    let receiver = DisposingReceiver::new(receiver);
    async move {
        let mut receiver = receiver;
        match poll_fn(|context| receiver.poll_receive(context)).await {
            Some(Ok(value)) => value,
            Some(Err(payload)) => resume_unwind(payload),
            None => panic!("blocking operation was cancelled during runtime teardown"),
        }
    }
}

/// Gives one rejected job to a native fallback thread.
///
/// The injected spawner makes the failure ownership edge directly testable:
/// `std::thread::Builder::spawn` consumes and destroys its closure before it
/// returns `Err`, so either spawner outcome has consumed `worker`. The
/// submitter's `job` reference remains authoritative until this function
/// returns.
fn spawn_blocking_fallback_with<F, T>(
    job: &Arc<BlockingJob<F, T>>,
    spawn: impl FnOnce(Arc<BlockingJob<F, T>>) -> std::io::Result<std::thread::JoinHandle<()>>,
) where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let worker = Arc::clone(job);
    match spawn(worker) {
        Ok(handle) => drop(handle),
        Err(error) => {
            // `job` still owns the operation. Its Drop implementation routes
            // the captured closure through disposal, which retries isolation
            // independently and closes the completion lane if it cannot run.
            drop(error);
        }
    }
}

/// A blocking closure plus the completion lane that outlives its Tokio task.
///
/// Tokio can synchronously destroy a rejected `spawn_blocking` closure while
/// still returning a join handle. Keeping the user closure behind an `Arc`
/// lets the submitter detect that outcome and move the same job to a fallback
/// thread without ever reclaiming the captured state inline.
struct BlockingJob<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    pending: Mutex<Option<(F, BlockingCompletion<T>)>>,
}

impl<F, T> BlockingJob<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    fn new(operation: F, completion: BlockingCompletion<T>) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(Some((operation, completion))),
        })
    }

    /// Recovers the pending slot even after an injected or future panic.
    ///
    /// `Drop` can run during an unrelated unwind, so poisoning must not turn
    /// its capture-disposal fallback into a double panic. The mutex protects
    /// only an ownership move; recovery never observes a partially-mutated
    /// user value.
    fn lock_pending(&self) -> MutexGuard<'_, Option<(F, BlockingCompletion<T>)>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<F, T> BlockingPoolJob for BlockingJob<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    fn run(&self) {
        let Some((operation, completion)) = self.lock_pending().take() else {
            return;
        };
        let outcome = catch_panic(operation);
        match catch_panic(|| completion.send(outcome)) {
            Ok(Ok(())) => {}
            Ok(Err(unclaimed)) => {
                // The returned future was dropped. We are already on a
                // blocking worker, but still contain a hostile
                // result/panic-payload destructor so it cannot unwind through
                // the worker entry point.
                discard_panic(catch_panic(|| drop(unclaimed)).err());
            }
            Err(waker_panic) => {
                // Tokio publishes the value before waking the receiver. A
                // hostile executor waker must not unwind this detached worker;
                // the receiver still owns the authoritative outcome.
                discard_panic(Some(waker_panic));
            }
        }
    }

    fn is_pending(&self) -> bool {
        self.lock_pending().is_some()
    }
}

impl<F, T> Drop for BlockingJob<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    fn drop(&mut self) {
        let Some((operation, completion)) = self.lock_pending().take() else {
            return;
        };
        // A cancelled or unstartable job must wake its waiter, but a hostile
        // waiter waker must not interrupt isolation of the captured closure.
        discard_panic(catch_panic(|| drop(completion)).err());
        dispose_detached(operation);
    }
}

/// Work held behind a shared owner so its submitter can tell whether Tokio
/// took it.
pub(crate) trait BlockingPoolJob: Send + Sync + 'static {
    /// Runs the pending work, if this job still holds any.
    fn run(&self);

    /// Reports whether the work is still waiting to be run.
    fn is_pending(&self) -> bool;
}

/// Submits `job` to Tokio's blocking pool, reporting whether Tokio took it.
///
/// On `false` the caller is the sole owner of still-pending work and must
/// place it elsewhere.
pub(crate) fn submit_blocking_job<J: BlockingPoolJob>(job: &Arc<J>) -> bool {
    if !is_available() {
        return false;
    }
    let worker = Arc::clone(job);
    match catch_panic(|| task::spawn_blocking(move || worker.run())) {
        Ok(handle) => {
            drop(handle);
            blocking_pool_accepted(job)
        }
        Err(payload) => {
            discard_panic(Some(payload));
            false
        }
    }
}

/// Returns whether Tokio took ownership of a submitted blocking job.
///
/// Tokio returns a join handle even when the blocking pool is already shutting
/// down, having synchronously destroyed the submitted closure. Ownership, not
/// the handle, is therefore the acceptance signal: sole ownership proves Tokio
/// no longer holds the closure, and the pending check distinguishes that
/// rejection from a job that ran to completion before the submitter sampled
/// the reference count — rerouting the latter would place already-empty jobs
/// behind live ones.
///
/// This pins Tokio 1.53.1's `spawn_task` shutdown path: a rejected closure is
/// destroyed synchronously, before `spawn_blocking` returns. The workspace
/// pins that exact release so an upgrade requires an explicit re-audit. A
/// future Tokio that deferred that drop would leave the count at two and
/// degrade fail-safe to the old inline behavior rather than misroute a live
/// closure. The end-to-end regressions in this crate pin the behavior we rely
/// on.
pub(crate) fn blocking_pool_accepted<J: BlockingPoolJob>(job: &Arc<J>) -> bool {
    Arc::strong_count(job) > 1 || !job.is_pending()
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

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

/// Polls two futures and resolves with the first to become ready.
///
/// Ties are contractual, not incidental: when both are ready in the same
/// poll, `Left` always wins. Callers order a "won" edge before a
/// "closed"/"completed" edge on exactly this bias — a latch that fired and
/// completed must still report the fired side.
pub async fn select_two<A, B>(left: A, right: B) -> Either<A::Output, B::Output>
where
    A: Future + Send,
    B: Future + Send,
{
    tokio::pin!(left);
    tokio::pin!(right);
    tokio::select! {
        biased;
        value = &mut left => Either::Left(value),
        value = &mut right => Either::Right(value),
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
/// join through [`join_user_polled`] — reaching for this helper from a public
/// seam is exactly the class #409 closed.
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

pub(super) fn contain_panic_payload(payload: PanicPayload) -> Option<String> {
    let message = match catch_panic(|| panic_message(payload.as_ref())) {
        Ok(message) => message,
        Err(inspection_panic) => {
            discard_panic(Some(inspection_panic));
            None
        }
    };
    // A custom panic payload is user-owned too. Its destructor may panic or
    // block, so retire it on the detached disposal lane before publishing
    // completion.
    dispose_detached(DiscardedPanicPayload(Some(payload)));
    message
}

/// Terminal, off-executor destruction for an opaque user panic payload.
///
/// The wrapper is load-bearing, not ceremony: submitting a bare payload to
/// [`dispose_detached`] closes a cycle. `DisposalJob::finish` classifies a
/// destructor panic by calling [`contain_panic_payload`], which submits the
/// replacement payload for detached disposal, whose destructor panics again.
/// A self-regenerating payload — one whose `Drop` `panic_any`s a fresh copy
/// of itself — then spins the disposal lane for the life of the process,
/// firing the panic hook every iteration.
///
/// [`discard_panic`] is what breaks the cycle, and does so by construction:
/// it catches the replacement and `mem::forget`s it precisely because
/// dropping the replacement would recurse outside the boundary
/// (`shelterwood_core::panic`, pinned by that module's
/// `discarding_a_recursively_hostile_panic_payload_is_contained`). Running it
/// from this destructor keeps the venue guarantee — the payload is still
/// destroyed off the exit-publishing executor — while the disposal job
/// itself never observes a panic to classify.
///
/// Removing this wrapper reintroduces the unbounded spin; the regression is
/// pinned by `blocking_panic_payload_does_not_stall_current_thread_exit_publication`'s
/// sibling `a_self_regenerating_panic_payload_is_destroyed_a_bounded_number_of_times`.
struct DiscardedPanicPayload(Option<PanicPayload>);

impl Drop for DiscardedPanicPayload {
    fn drop(&mut self) {
        discard_panic(self.0.take());
    }
}

fn panic_message(payload: &(dyn Any + Send + 'static)) -> Option<String> {
    if let Some(message) = payload.downcast_ref::<&str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}

pub async fn yield_now() {
    task::yield_now().await;
}

/// Runtime-neutral publishing half of an unbounded driver event lane.
pub struct UnboundedMpscSender<T>(mpsc::UnboundedSender<T>);

impl<T> Clone for UnboundedMpscSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> UnboundedMpscSender<T> {
    /// Sends one value, returning it when the receive lane is closed.
    pub fn send(&self, value: T) -> Result<(), T> {
        self.0.send(value).map_err(|error| error.0)
    }
}

/// Runtime-neutral receiving half of an unbounded driver event lane.
pub struct UnboundedMpscReceiver<T>(mpsc::UnboundedReceiver<T>);

impl<T> UnboundedMpscReceiver<T> {
    /// Waits for the next value, or returns `None` when every sender is gone.
    pub async fn recv(&mut self) -> Option<T> {
        self.0.recv().await
    }

    /// Receives one immediately available value.
    pub fn try_recv(&mut self) -> Option<T> {
        self.0.try_recv().ok()
    }

    /// Reports whether the receive lane currently contains no values.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub fn unbounded_mpsc<T>() -> (UnboundedMpscSender<T>, UnboundedMpscReceiver<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (UnboundedMpscSender(sender), UnboundedMpscReceiver(receiver))
}

pub enum ScopeWake<T> {
    Signal,
    ParentShutdown,
    Message(Option<T>),
    ControlMessage(Option<T>),
    Deadline,
}

pub struct ScopeWait<S, C> {
    pub signal: S,
    pub parent_shutdown: C,
}

pub async fn wait_scope<S, C, T>(
    wait: ScopeWait<S, C>,
    receiver: &mut UnboundedMpscReceiver<T>,
    control_receiver: Option<&mut UnboundedMpscReceiver<T>>,
    deadline: Option<std::time::Instant>,
) -> ScopeWake<T>
where
    S: Future<Output = ()> + Send,
    C: Future<Output = ()> + Send,
{
    let ScopeWait {
        signal,
        parent_shutdown,
    } = wait;
    let event = async move {
        tokio::pin!(signal);
        tokio::pin!(parent_shutdown);
        let control_message = async move {
            if let Some(receiver) = control_receiver {
                receiver.recv().await
            } else {
                std::future::pending().await
            }
        };
        tokio::pin!(control_message);
        tokio::select! {
            biased;
            () = &mut signal => ScopeWake::Signal,
            () = &mut parent_shutdown => ScopeWake::ParentShutdown,
            message = receiver.recv() => ScopeWake::Message(message),
            message = &mut control_message => ScopeWake::ControlMessage(message),
        }
    };
    // The deadline stays outside the whole event selection so every event
    // winner retires the timer through `timeout_at`'s synchronous poll-path
    // boundary. Burying the sleep in a select arm would run its drop-glue
    // disposal venue every time another arm won -- one blocking-lane
    // submission per driver wakeup while any deadline is armed.
    match deadline {
        Some(deadline) => match timeout_at(deadline, event).await {
            Timeout::Completed(wake) => wake,
            Timeout::Elapsed => ScopeWake::Deadline,
        },
        None => event.await,
    }
}

#[derive(Debug)]
pub struct JitterRng(fastrand::Rng);

impl JitterRng {
    pub fn new() -> Self {
        // `fastrand::Rng::new` seeds from the thread-local generator, falling
        // back to a fixed seed if that is unavailable (during TLS teardown).
        // Restart jitter would then be deterministic and correlated across
        // scopes -- degraded spread, never a correctness break.
        Self(fastrand::Rng::new())
    }

    pub fn sample<R>(&mut self, range: R) -> u64
    where
        R: RangeBounds<u64>,
    {
        self.0.u64(range)
    }
}

impl Default for JitterRng {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        mem::ManuallyDrop,
        panic,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
        thread,
        time::{Duration, Instant},
    };

    use super::BlockingPoolJob;
    use crate::test_support::{
        BlockingDrop, DESTRUCTOR_ESCAPE as WAIT, DISPOSAL_THREAD, RecordingDrop,
        assert_blocking_pool_outcomes, drop_gate, release, submit_during_blocking_pool_shutdown,
    };

    struct PanickingDrop(mpsc::Sender<()>);

    impl Drop for PanickingDrop {
        fn drop(&mut self) {
            let _ = self.0.send(());
            panic!("hostile blocking outcome destructor");
        }
    }

    struct PanicWake(Arc<AtomicUsize>);

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("hostile blocking-result waker");
        }
    }

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

    #[test]
    fn unbounded_sender_returns_the_value_after_receiver_close() {
        let (sender, receiver) = super::unbounded_mpsc();
        drop(receiver);

        assert_eq!(
            sender.send(String::from("returned")),
            Err(String::from("returned"))
        );
    }

    #[test]
    fn jitter_rng_respects_requested_bounds() {
        let mut rng = super::JitterRng::new();
        for _ in 0..128 {
            assert!((7..11).contains(&rng.sample(7..11)));
        }
    }

    #[test]
    fn blocking_job_recovers_poison_for_pending_checks_and_drop() {
        let (captured_dropped, captured_dropped_rx) = mpsc::channel();
        let captured = RecordingDrop(captured_dropped);
        let (completion, _receiver) = super::oneshot();
        let job = super::BlockingJob::new(move || drop(captured), completion);
        let injected = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let _guard = job.pending.lock().expect("fresh blocking-job mutex");
            panic!("inject blocking-job mutex poison");
        }));
        assert!(injected.is_err());
        assert!(
            job.is_pending(),
            "poison recovery preserves the pending job"
        );
        assert!(
            panic::catch_unwind(panic::AssertUnwindSafe(|| drop(job))).is_ok(),
            "blocking-job drop stays panic-free after poison"
        );
        captured_dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("poison recovery still retires the captured operation");
    }

    #[tokio::test]
    async fn scope_wait_prefers_signal_when_both_control_futures_are_ready() {
        let (_sender, mut receiver) = super::unbounded_mpsc::<()>();

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::ready(()),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            None,
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::Signal));
    }

    #[tokio::test]
    async fn scope_wait_prefers_a_primary_event_over_a_control_backlog() {
        let (sender, mut receiver) = super::unbounded_mpsc();
        let (control_sender, mut control_receiver) = super::unbounded_mpsc();
        for value in 0..128 {
            assert!(control_sender.send(value).is_ok());
        }
        assert!(sender.send(999).is_ok());

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::Message(Some(999))));
        assert_eq!(control_receiver.try_recv(), Some(0));
    }

    #[tokio::test]
    async fn scope_wait_reports_parent_shutdown_directly() {
        let (_sender, mut receiver) = super::unbounded_mpsc::<()>();
        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            None,
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::ParentShutdown));
    }

    /// Each arm alone only pins its own `ScopeWake` mapping; the `biased;`
    /// precedence is a property of ties. Walk the chain
    /// `signal > parent_shutdown > message > control > deadline` with every
    /// weaker arm simultaneously ready, so dropping `biased;` cannot pass.
    #[tokio::test]
    async fn scope_wait_resolves_simultaneous_arms_in_declaration_order() {
        let elapsed = crate::now();
        let (sender, mut receiver) = super::unbounded_mpsc();
        let (control_sender, mut control_receiver) = super::unbounded_mpsc();
        assert!(sender.send(1_u8).is_ok());
        assert!(control_sender.send(2_u8).is_ok());

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::ready(()),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Signal));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::ParentShutdown));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Message(Some(1))));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::ControlMessage(Some(2))));

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            Some(&mut control_receiver),
            Some(elapsed),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Deadline));
    }

    #[tokio::test(start_paused = true)]
    async fn scope_wait_reports_deadline_and_keeps_an_absent_deadline_pending() {
        let (_sender, mut receiver) = super::unbounded_mpsc::<()>();
        let deadline = crate::now() + Duration::from_secs(10);
        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            None,
            Some(deadline),
        )
        .await;
        assert!(matches!(wake, super::ScopeWake::Deadline));

        let (sender, mut receiver) = super::unbounded_mpsc();
        let mut waiting = Box::pin(super::wait_scope(
            super::ScopeWait {
                signal: std::future::pending(),
                parent_shutdown: std::future::pending(),
            },
            &mut receiver,
            None,
            None,
        ));
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(sender.send(7_u8).is_ok());
        assert!(matches!(waiting.await, super::ScopeWake::Message(Some(7))));
    }

    #[tokio::test]
    async fn select_two_covers_both_sides_and_biases_ready_ties_left() {
        assert!(matches!(
            super::select_two(std::future::ready(1_u8), std::future::pending::<u8>()).await,
            super::Either::Left(1)
        ));
        assert!(matches!(
            super::select_two(std::future::pending::<u8>(), std::future::ready(2_u8)).await,
            super::Either::Right(2)
        ));
        assert!(matches!(
            super::select_two(std::future::ready(3_u8), std::future::ready(4_u8)).await,
            super::Either::Left(3)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_work_preserves_values_and_panics() {
        assert_eq!(super::spawn_blocking_work(|| 42_u8).await, 42);

        let panicking = super::spawn(async {
            super::spawn_blocking_work(|| panic!("blocking work panic")).await
        });
        assert!(matches!(
            super::join(panicking).await,
            super::JoinOutcome::Panic {
                message: Some(message)
            } if message == "blocking work panic"
        ));
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

    #[test]
    fn fallback_detection_distinguishes_owned_pending_and_completed_jobs() {
        let (accepted_completion, _accepted_receiver) = super::oneshot();
        let accepted = super::BlockingJob::new(|| 1_u8, accepted_completion);
        let (rejected_completion, _rejected_receiver) = super::oneshot();
        let rejected = super::BlockingJob::new(|| 2_u8, rejected_completion);
        let (completed_completion, _completed_receiver) = super::oneshot();
        let completed = super::BlockingJob::new(|| 3_u8, completed_completion);
        assert_blocking_pool_outcomes(accepted, rejected, completed);
    }

    #[test]
    fn completion_contains_a_panicking_receiver_waker() {
        let (completion, mut receiver) = super::oneshot();
        let job = super::BlockingJob::new(|| 42_u8, completion);
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(PanicWake(Arc::clone(&wakes))));
        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));

        assert!(
            super::catch_panic(|| job.run()).is_ok(),
            "a receiver waker cannot unwind the detached worker"
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Some(Ok(42)))
        ));
    }

    #[test]
    fn unclaimed_result_and_panic_payload_destructors_are_contained() {
        let (result_dropped, result_dropped_rx) = mpsc::channel();
        let (result_completion, result_receiver) = super::oneshot();
        drop(result_receiver);
        let result_job =
            super::BlockingJob::new(move || PanickingDrop(result_dropped), result_completion);
        assert!(
            super::catch_panic(|| result_job.run()).is_ok(),
            "an unclaimed result destructor cannot unwind the worker"
        );
        result_dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the unclaimed result is destroyed");

        let (payload_dropped, payload_dropped_rx) = mpsc::channel();
        let (panic_completion, panic_receiver) = super::oneshot();
        drop(panic_receiver);
        let panic_job = super::BlockingJob::new(
            move || -> () { panic::panic_any(PanickingDrop(payload_dropped)) },
            panic_completion,
        );
        assert!(
            super::catch_panic(|| panic_job.run()).is_ok(),
            "an unclaimed panic-payload destructor cannot unwind the worker"
        );
        payload_dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the unclaimed panic payload is destroyed");
    }

    #[test]
    fn dropping_an_unpolled_future_isolates_a_stored_outcome() {
        let waiter_thread = thread::current().id();
        let (result_dropped, result_dropped_rx) = mpsc::channel();
        let (completion, receiver) = super::oneshot();
        let job = super::BlockingJob::new(move || RecordingDrop(result_dropped), completion);
        let worker = Arc::clone(&job);
        drop(job);

        // The awaiting future is built but never polled, so only its own
        // construction can decide who destroys an outcome that lands first.
        let future = super::receive_blocking(receiver);
        worker.run();
        drop(future);

        let (destructor_thread, destructor_name) = result_dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the unclaimed result is destroyed");
        assert_ne!(destructor_thread, waiter_thread);
        assert_eq!(
            destructor_name.as_deref(),
            Some(DISPOSAL_THREAD),
            "an unclaimed result must not be destroyed on the awaiting task's thread"
        );
    }

    #[test]
    fn accepted_then_cancelled_job_isolates_capture_and_reports_teardown() {
        let (captured_dropped, captured_dropped_rx) = mpsc::channel();
        let captured = RecordingDrop(captured_dropped);
        let (completion, receiver) = super::oneshot();
        let job = super::BlockingJob::new(
            move || {
                drop(captured);
                42_u8
            },
            completion,
        );
        let accepted_worker = Arc::clone(&job);
        drop(job);

        let (cancelled_on, cancelled_on_rx) = mpsc::channel();
        thread::Builder::new()
            .name("tokio-canceller".to_owned())
            .spawn(move || {
                cancelled_on
                    .send(thread::current().id())
                    .expect("test observes cancellation");
                drop(accepted_worker);
            })
            .expect("cancellation thread starts")
            .join()
            .expect("cancellation thread completes");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("consumer runtime");
        let cancellation =
            super::catch_panic(|| runtime.block_on(super::receive_blocking(receiver)))
                .expect_err("an unstarted accepted job reports cancellation");
        assert_eq!(
            cancellation.downcast_ref::<&'static str>().copied(),
            Some("blocking operation was cancelled during runtime teardown")
        );

        let cancellation_thread = cancelled_on_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the cancellation thread is recorded");
        let (destructor_thread, destructor_name) = captured_dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the cancelled capture reaches disposal");
        assert_ne!(destructor_thread, cancellation_thread);
        assert_eq!(
            destructor_name.as_deref(),
            Some(DISPOSAL_THREAD),
            "accepted cancellation must isolate closure destruction"
        );
    }

    #[test]
    fn failed_native_fallback_isolates_capture_and_reports_teardown() {
        let submitting_thread = thread::current().id();
        let (captured_dropped, captured_dropped_rx) = mpsc::channel();
        let captured = RecordingDrop(captured_dropped);
        let (completion, receiver) = super::oneshot();
        let job = super::BlockingJob::new(
            move || {
                drop(captured);
                42_u8
            },
            completion,
        );

        super::spawn_blocking_fallback_with(&job, |_worker| {
            Err(std::io::Error::other("injected native thread exhaustion"))
        });
        drop(job);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("consumer runtime");
        let cancellation =
            super::catch_panic(|| runtime.block_on(super::receive_blocking(receiver)))
                .expect_err("a fallback that cannot start reports cancellation");
        assert_eq!(
            cancellation.downcast_ref::<&'static str>().copied(),
            Some("blocking operation was cancelled during runtime teardown")
        );

        let (destructor_thread, destructor_name) = captured_dropped_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the unstartable capture reaches disposal");
        assert_ne!(destructor_thread, submitting_thread);
        assert_eq!(
            destructor_name.as_deref(),
            Some(DISPOSAL_THREAD),
            "native fallback failure must retry through disposal isolation"
        );
    }

    #[test]
    fn shut_down_blocking_pool_runs_rejected_work_off_the_submitting_thread() {
        let (future_tx, future_rx) = mpsc::channel();
        let (submitted, submitted_rx) = mpsc::channel();
        let (returned, returned_rx) = mpsc::channel();
        let (entered, entered_rx) = mpsc::channel();
        let gate = drop_gate();
        let captured = BlockingDrop::new(entered, Arc::clone(&gate));
        // This outer task stays queued behind the occupied worker. Tokio runs
        // it while draining shutdown, so its nested blocking submission is
        // synchronously rejected even though `spawn_blocking` returns a handle.
        submit_during_blocking_pool_shutdown(move || {
            let submitting_thread = thread::current().id();
            let future = super::spawn_blocking_work(move || {
                drop(captured);
                42_u8
            });
            submitted
                .send(submitting_thread)
                .expect("test observes the submitting thread");
            future_tx
                .send(future)
                .expect("test receives the blocking-work future");
            returned.send(()).expect("test observes submission return");
        });

        let submitting_thread = submitted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("runtime teardown submits blocking work");
        let future = future_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("a rejected submission still returns its future");
        let (operation_thread, operation_name) = entered_rx
            .recv_timeout(WAIT + Duration::from_secs(1))
            .expect("the rejected operation starts");
        // This arrives while the captured destructor is still blocked. On the
        // regression path Tokio destroys the closure inline and submission
        // cannot return until the escape hatch fires.
        let submission_returned = returned_rx.recv_timeout(Duration::from_secs(1));

        release(&gate);
        submission_returned.expect("captured destruction must not block submission");

        let consumer = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("consumer runtime");
        assert_eq!(consumer.block_on(future), 42);
        assert_ne!(operation_thread, submitting_thread);
        assert_eq!(
            operation_name.as_deref(),
            Some(super::BLOCKING_FALLBACK_THREAD),
            "a rejected operation must land on Shelterwood's fallback thread"
        );
    }
}
