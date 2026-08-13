use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{Latch, catch_panic, contain_panic_payload, discard_panic};
use crate::spawn::{BlockingPoolJob, submit_blocking_job};

/// Ownership wrapper for user values retained by framework state.
///
/// Dropping the wrapper transfers the value to an isolated blocking task, so
/// framework futures never run user destruction as part of their own drop
/// glue. Callers that need to classify destruction can take the value and
/// join a dedicated blocking task explicitly.
pub struct Isolated<T: Send + 'static> {
    value: Option<T>,
}

impl<T: Send + 'static> Isolated<T> {
    pub const fn new(value: T) -> Self {
        Self { value: Some(value) }
    }

    pub fn get(&self) -> &T {
        self.value
            .as_ref()
            .expect("isolated user value was already taken")
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.value
            .as_mut()
            .expect("isolated user value was already taken")
    }

    pub fn take(&mut self) -> Option<T> {
        self.value.take()
    }
}

impl<T: Send + 'static> Drop for Isolated<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            dispose_detached(value);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisposalPanic {
    pub message: Option<String>,
}

struct DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    state: Mutex<Option<(T, C)>>,
}

impl<T, C> DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn new(value: T, completion: C) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some((value, completion))),
        })
    }

    fn finish(&self) {
        let Some((value, completion)) = self
            .state
            .lock()
            .expect("disposal job mutex poisoned")
            .take()
        else {
            return;
        };
        let panic = match catch_panic(|| drop(value)) {
            Ok(()) => None,
            Err(payload) => Some(DisposalPanic {
                message: contain_panic_payload(payload),
            }),
        };
        // Completion is framework bookkeeping. Contain it as well so a
        // hostile waker or a runtime teardown race cannot unwind a blocking
        // worker or double-panic while the job is being dropped.
        discard_panic(catch_panic(|| completion(panic)).err());
    }
}

impl<T, C> Drop for DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn drop(&mut self) {
        self.finish();
    }
}

impl<T, C> BlockingPoolJob for DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn run(&self) {
        self.finish();
    }

    fn is_pending(&self) -> bool {
        self.state
            .lock()
            .expect("disposal job mutex poisoned")
            .is_some()
    }
}

/// Jobs awaiting the shared non-runtime disposal thread.
///
/// `worker_live` is only cleared by the worker after observing an empty queue
/// under this lock, and submitters push and consult it under the same lock, so
/// a queued job always has a live worker destined to drain it.
struct FallbackDisposals {
    queue: VecDeque<Arc<dyn BlockingPoolJob>>,
    worker_live: bool,
}

static FALLBACK_DISPOSALS: Mutex<FallbackDisposals> = Mutex::new(FallbackDisposals {
    queue: VecDeque::new(),
    worker_live: false,
});

/// Queues a disposal job for the shared fallback thread, lazily starting it.
/// Returns `false` when no worker exists and none could be started; the
/// caller must then finish the job itself.
fn enqueue_fallback_disposal(job: Arc<dyn BlockingPoolJob>) -> bool {
    let mut state = FALLBACK_DISPOSALS
        .lock()
        .expect("fallback disposal queue mutex poisoned");
    state.queue.push_back(job);
    if state.worker_live {
        return true;
    }
    // Spawning under the lock makes queueing and worker liveness one atomic
    // decision: no submitter can observe a queued job without a worker.
    match std::thread::Builder::new()
        .name("shelterwood-disposal".to_owned())
        .spawn(run_fallback_disposals)
    {
        Ok(worker) => {
            drop(worker);
            state.worker_live = true;
            true
        }
        Err(_) => {
            // The queue was empty before this push (no live worker implies an
            // empty queue), so the popped entry is exactly the failed job.
            let rejected = state.queue.pop_back();
            debug_assert!(rejected.is_some());
            false
        }
    }
}

fn run_fallback_disposals() {
    loop {
        let job = {
            let mut state = FALLBACK_DISPOSALS
                .lock()
                .expect("fallback disposal queue mutex poisoned");
            let Some(job) = state.queue.pop_front() else {
                state.worker_live = false;
                return;
            };
            job
        };
        // `DisposalJob::finish` contains destructor and completion panics
        // internally; this outer boundary keeps even an unforeseen framework
        // panic from stranding `worker_live` and the queued jobs behind it.
        discard_panic(catch_panic(|| job.run()).err());
    }
}

fn dispatch_disposal<T, C>(job: Arc<DisposalJob<T, C>>)
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    // A rejected submission falls through so the fallback thread, rather than
    // this runtime-teardown thread, owns user destruction.
    if submit_blocking_job(&job) {
        return;
    }

    // Outside a runtime, one shared lazily started thread drains a queue of
    // disposal jobs, so dropping N values costs at most one thread rather
    // than one thread per value, while a blocking or panicking destructor
    // still never runs on (or unwinds into) the submitting thread. The queue
    // is unbounded on purpose: applying a bound would block the submitter on
    // user destructors, exactly what isolation must prevent. Serialization is
    // the accepted trade: one blocking destructor delays later fallback
    // disposals instead of consuming another native thread.
    if enqueue_fallback_disposal(Arc::clone(&job) as Arc<dyn BlockingPoolJob>) {
        return;
    }

    // Exhausted task and thread creation must not strand completion or expose
    // a destructor panic. Blocking here is the only remaining safe fallback.
    job.finish();
}

/// Runs potentially blocking user destruction away from the caller and then
/// invokes framework completion with the contained panic diagnostic.
///
/// Inside a Tokio runtime this uses the blocking pool. Outside one, jobs are
/// funneled through a single shared disposal thread, so destroying many
/// values (for example dropping a large unspawned tree) never creates one
/// native thread per value.
pub fn dispose_then<T, C>(value: T, completion: C)
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    dispatch_disposal(DisposalJob::new(value, completion));
}

/// Detaches potentially blocking or panicking user destruction from the
/// caller. The guard also contains a panic if task/thread creation itself
/// fails and drops the closure on the submitting thread.
pub fn dispose_detached<T: Send + 'static>(value: T) {
    dispose_then(value, |_| {});
}

/// Starts isolated disposal for every value and fires once all jobs finish.
///
/// Each value gets its own unwind boundary, so one destructor panic cannot
/// prevent the remaining values or the aggregate completion from running.
pub fn dispose_all<T: Send + 'static>(values: Vec<T>) -> Latch {
    let completion = Latch::default();
    if values.is_empty() {
        completion.fire();
        return completion;
    }

    let remaining = Arc::new(AtomicUsize::new(values.len()));
    for value in values {
        let remaining = Arc::clone(&remaining);
        let value_completion = completion.clone();
        dispose_then(value, move |_| {
            if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                value_completion.fire();
            }
        });
    }
    completion
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread::{self, ThreadId},
        time::{Duration, Instant},
    };

    use super::{DisposalJob, DisposalPanic, Isolated, dispose_detached};
    use crate::spawn::{BlockingPoolJob, blocking_pool_accepted};

    /// Name of the shared non-runtime disposal thread, asserted on to pin
    /// *where* isolated destruction lands rather than merely where it does not.
    const FALLBACK_THREAD: &str = "shelterwood-disposal";

    /// Deadlock escape hatch for the hostile destructors below. A correct run
    /// releases them in microseconds and never reaches this bound; a
    /// regression that runs them inline reaches it and then fails the thread
    /// assertions, so the suite reports a diagnosis instead of a hang.
    const DESTRUCTOR_ESCAPE: Duration = Duration::from_secs(5);

    /// The thread a user destructor ran on.
    type DestructorThread = (ThreadId, Option<String>);

    fn describe_current_thread() -> DestructorThread {
        let current = thread::current();
        (current.id(), current.name().map(str::to_owned))
    }

    struct PanickingDrop(Arc<AtomicUsize>);

    impl Drop for PanickingDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("cancelled disposal job payload");
        }
    }

    #[test]
    fn dropping_an_unstarted_disposal_job_contains_panic_and_completes_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let diagnostic = Arc::new(Mutex::new(None));
        let completion_diagnostic = Arc::clone(&diagnostic);
        let job = DisposalJob::new(PanickingDrop(Arc::clone(&drops)), move |panic| {
            *completion_diagnostic
                .lock()
                .expect("diagnostic mutex poisoned") = Some(panic);
        });

        drop(job);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let diagnostic = diagnostic.lock().expect("diagnostic mutex poisoned");
        assert!(matches!(
            diagnostic.as_ref(),
            Some(Some(DisposalPanic {
                message: Some(message)
            })) if message == "cancelled disposal job payload"
        ));
    }

    #[test]
    fn fallback_detection_distinguishes_blocking_spawn_outcomes() {
        let accepted = DisposalJob::new((), |_| {});
        let accepted_worker = Arc::clone(&accepted);
        assert!(
            blocking_pool_accepted(&accepted),
            "an accepted closure still owned by Tokio must stay on its blocking pool"
        );
        drop(accepted_worker);

        let rejected = DisposalJob::new((), |_| {});
        let rejected_worker = Arc::clone(&rejected);
        drop(rejected_worker);
        assert!(
            !blocking_pool_accepted(&rejected),
            "a synchronously dropped closure must move to the fallback queue"
        );

        let completed = DisposalJob::new((), |_| {});
        let completed_worker = Arc::clone(&completed);
        completed_worker.run();
        drop(completed_worker);
        assert!(
            blocking_pool_accepted(&completed),
            "an already-completed closure must not enqueue an empty job"
        );
    }

    struct BlockingDrop {
        entered: mpsc::Sender<DestructorThread>,
        release: Arc<(Mutex<bool>, Condvar)>,
        finished: mpsc::Sender<()>,
    }

    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            let _ = self.entered.send(describe_current_thread());
            let (released, wake) = &*self.release;
            let mut released = released.lock().expect("release mutex available");
            let deadline = Instant::now() + DESTRUCTOR_ESCAPE;
            while !*released {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                released = wake
                    .wait_timeout(released, remaining)
                    .expect("release mutex available")
                    .0;
            }
            let _ = self.finished.send(());
        }
    }

    fn release(gate: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, wake) = &**gate;
        *released.lock().expect("release mutex available") = true;
        wake.notify_all();
    }

    struct DisposeOnDrop {
        value: Option<BlockingDrop>,
        submitted: mpsc::Sender<ThreadId>,
        returned: mpsc::Sender<()>,
    }

    impl Drop for DisposeOnDrop {
        fn drop(&mut self) {
            let _ = self.submitted.send(thread::current().id());
            dispose_detached(
                self.value
                    .take()
                    .expect("teardown submits the disposal exactly once"),
            );
            let _ = self.returned.send(());
        }
    }

    #[test]
    fn shut_down_blocking_pool_falls_back_off_the_teardown_thread() {
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

        let (submitted, submitted_rx) = mpsc::channel();
        let (returned, returned_rx) = mpsc::channel();
        let (entered, entered_rx) = mpsc::channel();
        let (finished, finished_rx) = mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let trigger = DisposeOnDrop {
            value: Some(BlockingDrop {
                entered,
                release: Arc::clone(&gate),
                finished,
            }),
            submitted,
            returned,
        };
        // With the only worker occupied, this outer task stays queued. After
        // shutdown begins, Tokio runs it while draining the worker queue; its
        // nested disposal submission is then synchronously rejected.
        drop(runtime.spawn_blocking(move || drop(trigger)));
        runtime.shutdown_background();
        release_worker
            .send(())
            .expect("the blocking-pool teardown may proceed");

        let teardown_thread = submitted_rx.recv_timeout(Duration::from_secs(1));
        let destructor_thread = entered_rx.recv_timeout(Duration::from_secs(1));
        // This must arrive while the hostile destructor is still blocked. On
        // the regression path dispose_detached runs the destructor inline, so
        // the blocking-pool teardown cannot return from submission.
        let submission_returned = returned_rx.recv_timeout(Duration::from_secs(1));

        release(&gate);
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the fallback destructor finishes after release");

        let teardown_thread = teardown_thread.expect("runtime teardown submits disposal");
        let (destructor_thread, destructor_name) =
            destructor_thread.expect("the hostile destructor starts");
        assert_ne!(destructor_thread, teardown_thread);
        assert_eq!(
            destructor_name.as_deref(),
            Some(FALLBACK_THREAD),
            "a rejected blocking submission must land on the shared fallback thread"
        );
        submission_returned.expect("a hostile destructor must not block runtime teardown");
    }

    /// The embedding-host shape of #205: a live task owns an [`Isolated`]
    /// value and the host tears its runtime down from its own thread.
    ///
    /// `shutdown_background` shuts the blocking pool down *before* Tokio drops
    /// the task, so the drop glue submits into an already shut-down pool. The
    /// regression runs the user destructor inline, parking the host inside
    /// `shutdown_background` — which is precisely the mitigation the shutdown
    /// docs direct hosts to, so it must not be the thing that hangs.
    #[test]
    fn embedder_runtime_teardown_isolates_a_task_held_value() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let embedder_thread = thread::current().id();
        let (entered, entered_rx) = mpsc::channel();
        let (finished, finished_rx) = mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let held = Isolated::new(BlockingDrop {
            entered,
            release: Arc::clone(&gate),
            finished,
        });
        runtime.spawn(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        // Poll the task once so the runtime, not this thread, owns the value.
        runtime.block_on(async { tokio::task::yield_now().await });

        runtime.shutdown_background();
        // Returning here at all is half the regression: on the inline path
        // this thread is still inside `shutdown_background` running the
        // hostile destructor, and only the escape hatch frees it.
        let (destructor_thread, destructor_name) = entered_rx
            .recv_timeout(DESTRUCTOR_ESCAPE + Duration::from_secs(1))
            .expect("the hostile destructor starts");
        release(&gate);
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the isolated destructor finishes after release");

        assert_ne!(
            destructor_thread, embedder_thread,
            "runtime teardown must not run a user destructor on the host thread"
        );
        assert_eq!(
            destructor_name.as_deref(),
            Some(FALLBACK_THREAD),
            "a shut-down blocking pool must hand disposal to the fallback thread"
        );
    }
}
