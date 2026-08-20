use std::{
    any::Any,
    future::{Future, poll_fn},
    ops::RangeBounds,
    panic::resume_unwind,
    sync::{Arc, Mutex},
};

use tokio::{sync::mpsc, task};

use shelterwood_core::exit::JoinOutcome;

use super::{
    DisposingReceiver, OneShotReceiver, OneShotSender, PanicPayload, catch_panic, discard_panic,
    dispose_detached, oneshot, sleep_until_std,
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
        // A handle is taken only by `join`, which consumes the work, so the
        // slot is populated on every reachable call. Assert rather than
        // panic: this shares its shape with the locked callers of
        // `OneShotSender::is_closed`, and aborting nothing is the harmless
        // reading of an already-joined handle.
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
}

impl<F, T> BlockingPoolJob for BlockingJob<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    fn run(&self) {
        let Some((operation, completion)) = self
            .pending
            .lock()
            .expect("blocking job mutex poisoned")
            .take()
        else {
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
        self.pending
            .lock()
            .expect("blocking job mutex poisoned")
            .is_some()
    }
}

impl<F, T> Drop for BlockingJob<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    fn drop(&mut self) {
        let Some((operation, completion)) = self
            .pending
            .lock()
            .expect("blocking job mutex poisoned")
            .take()
        else {
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

pub async fn join<T>(handle: JoinHandle<T>) -> JoinOutcome<T> {
    let JoinHandle { inner } = handle;
    match inner.await {
        Ok(value) => JoinOutcome::Ok { value },
        Err(error) if error.is_panic() => JoinOutcome::Panic {
            message: contain_panic_payload(error.into_panic()),
        },
        Err(error) => {
            debug_assert!(error.is_cancelled());
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
            debug_assert!(error.is_cancelled());
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
    dispose_detached(payload);
    message
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
    tokio::pin!(signal);
    tokio::pin!(parent_shutdown);
    let deadline = async move {
        if let Some(deadline) = deadline {
            sleep_until_std(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let control_message = async move {
        if let Some(receiver) = control_receiver {
            receiver.recv().await
        } else {
            std::future::pending().await
        }
    };
    tokio::pin!(deadline);
    tokio::pin!(control_message);
    tokio::select! {
        biased;
        () = &mut signal => ScopeWake::Signal,
        () = &mut parent_shutdown => ScopeWake::ParentShutdown,
        message = receiver.recv() => ScopeWake::Message(message),
        message = &mut control_message => ScopeWake::ControlMessage(message),
        () = &mut deadline => ScopeWake::Deadline,
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
        panic,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, Wake, Waker},
        thread,
        time::Duration,
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
