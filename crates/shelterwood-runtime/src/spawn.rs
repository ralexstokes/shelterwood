use std::{
    any::Any,
    future::{Future, poll_fn},
    ops::RangeBounds,
    panic::resume_unwind,
    sync::{Arc, Mutex},
};

use tokio::{sync::mpsc, task};

use shelterwood_core::exit::JoinVerdict as JoinOutcome;

use super::{
    DisposingReceiver, OneShotSender, PanicPayload, catch_panic, discard_panic, dispose_detached,
    oneshot, sleep_until_std,
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
    abort: AbortHandle,
}

impl ActorWork {
    pub fn abort(&self) {
        self.abort.abort();
    }

    pub async fn join(mut self) -> JoinOutcome<()> {
        let Some(handle) = self.handle.take() else {
            return JoinOutcome::Cancelled;
        };
        join(handle).await
    }
}

impl Drop for ActorWork {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub fn spawn_actor_work(future: impl Future<Output = ()> + Send + 'static) -> ActorWork {
    let handle = spawn(future);
    let abort = handle.abort_handle();
    ActorWork {
        handle: Some(handle),
        abort,
    }
}

pub fn spawn_blocking_work<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> impl Future<Output = T> + Send {
    let (completion, receiver) = oneshot();
    let mut receiver = DisposingReceiver::new(receiver);
    let job = BlockingJob::new(operation, completion);
    let worker = Arc::clone(&job);

    let mut needs_fallback = true;
    match catch_panic(|| task::spawn_blocking(move || worker.run())) {
        Ok(handle) => {
            drop(handle);
            // Tokio returns a handle even when the blocking pool is already
            // shutting down. In that case it synchronously destroys the
            // closure, leaving `job` as the only reference with pending work.
            needs_fallback = blocking_spawn_needs_fallback(&job);
        }
        Err(payload) => discard_panic(Some(payload)),
    }

    if needs_fallback {
        let worker = Arc::clone(&job);
        // A blocking operation cannot share disposal's single fallback queue:
        // one legitimately long operation would strand every later job. This
        // path exists only for runtime teardown, so one detached thread per
        // rejected operation is the appropriate degradation.
        let fallback = std::thread::Builder::new()
            .name(BLOCKING_FALLBACK_THREAD.to_owned())
            .spawn(move || worker.run());
        if let Err(error) = fallback {
            // `job` still owns the operation. Its Drop implementation routes
            // the captured closure through isolated disposal, so even native
            // thread exhaustion cannot destroy user state on this submitter.
            drop(error);
        }
    }
    drop(job);

    async move {
        match poll_fn(|context| receiver.poll_receive(context)).await {
            Some(Ok(value)) => value,
            Some(Err(payload)) => resume_unwind(payload),
            None => panic!("blocking operation was cancelled during runtime teardown"),
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
        if let Err(unclaimed) = completion.send(outcome) {
            // The returned future was dropped. We are already on a blocking
            // worker, but still contain a hostile result/panic-payload
            // destructor so it cannot unwind through the worker entry point.
            discard_panic(catch_panic(|| drop(unclaimed)).err());
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

/// Returns whether Tokio synchronously rejected a blocking work submission.
///
/// Sole ownership proves that Tokio no longer holds the submitted closure;
/// the pending check distinguishes rejection from a fast task that completed
/// before the submitter sampled the reference count.
fn blocking_spawn_needs_fallback<F, T>(job: &Arc<BlockingJob<F, T>>) -> bool
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Arc::strong_count(job) == 1 && job.is_pending()
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

pub fn spawn_blocking<F, T>(operation: F) -> JoinHandle<T>
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
    // A custom panic payload is user-owned too. Its destructor may panic, so
    // discard it under a fresh unwind boundary before publishing completion.
    discard_panic(Some(payload));
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

pub type UnboundedMpscSender<T> = mpsc::UnboundedSender<T>;
pub type UnboundedMpscReceiver<T> = mpsc::UnboundedReceiver<T>;

pub fn unbounded_mpsc<T>() -> (UnboundedMpscSender<T>, UnboundedMpscReceiver<T>) {
    mpsc::unbounded_channel()
}

pub fn unbounded_mpsc_send<T>(sender: &UnboundedMpscSender<T>, value: T) -> Result<(), T> {
    sender.send(value).map_err(|error| error.0)
}

pub fn unbounded_mpsc_try_recv<T>(receiver: &mut UnboundedMpscReceiver<T>) -> Option<T> {
    receiver.try_recv().ok()
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
        sync::{Arc, Condvar, Mutex, mpsc},
        thread::{self, ThreadId},
        time::{Duration, Instant},
    };

    const WAIT: Duration = Duration::from_secs(5);

    type ThreadDescription = (ThreadId, Option<String>);

    fn describe_current_thread() -> ThreadDescription {
        let current = thread::current();
        (current.id(), current.name().map(str::to_owned))
    }

    struct BlockingDrop {
        entered: mpsc::Sender<ThreadDescription>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            let _ = self.entered.send(describe_current_thread());
            let (released, wake) = &*self.release;
            let mut released = released.lock().expect("release mutex available");
            let deadline = Instant::now() + WAIT;
            while !*released {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                released = wake
                    .wait_timeout(released, remaining)
                    .expect("release mutex available")
                    .0;
            }
        }
    }

    fn release(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, wake) = &**gate;
        *released.lock().expect("release mutex available") = true;
        wake.notify_all();
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
            assert!(super::unbounded_mpsc_send(&control_sender, value).is_ok());
        }
        assert!(super::unbounded_mpsc_send(&sender, 999).is_ok());

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
        assert_eq!(control_receiver.try_recv(), Ok(0));
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
    fn shut_down_blocking_pool_runs_rejected_work_off_the_submitting_thread() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .expect("test runtime");
        let (worker_started, worker_started_rx) = mpsc::channel();
        let (release_worker, release_worker_rx) = mpsc::channel();
        drop(runtime.spawn_blocking(move || {
            worker_started
                .send(())
                .expect("test observes the occupied blocking worker");
            release_worker_rx
                .recv()
                .expect("test releases the occupied blocking worker");
        }));
        worker_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the sole blocking worker starts");

        let (future_tx, future_rx) = mpsc::channel();
        let (submitted, submitted_rx) = mpsc::channel();
        let (returned, returned_rx) = mpsc::channel();
        let (entered, entered_rx) = mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let captured = BlockingDrop {
            entered,
            release: Arc::clone(&gate),
        };
        // This outer task stays queued behind the occupied worker. Tokio runs
        // it while draining shutdown, so its nested blocking submission is
        // synchronously rejected even though `spawn_blocking` returns a handle.
        drop(runtime.spawn_blocking(move || {
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
        }));
        runtime.shutdown_background();
        release_worker
            .send(())
            .expect("the blocking-pool teardown may proceed");

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
