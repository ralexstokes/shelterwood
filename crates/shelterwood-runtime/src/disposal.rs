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
    /// Whether the submitter may never destroy this payload itself.
    critical: bool,
}

impl<T, C> DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn lock_state(&self) -> std::sync::MutexGuard<'_, Option<(T, C)>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn new(value: T, completion: C) -> Arc<Self> {
        Self::with_criticality(value, completion, false)
    }

    /// Builds a job whose last owner re-routes instead of finishing inline.
    fn critical(value: T, completion: C) -> Arc<Self> {
        Self::with_criticality(value, completion, true)
    }

    fn with_criticality(value: T, completion: C, critical: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some((value, completion))),
            critical,
        })
    }

    /// Claims the pending work, if this job still holds any.
    ///
    /// Poison is tolerated rather than raised: the only work under this mutex
    /// is taking the payload out, and the critical drop path below must stay
    /// panic-free because it can run inside an unwind.
    fn take_pending(&self) -> Option<(T, C)> {
        self.lock_state().take()
    }

    fn finish(&self) {
        let Some((value, completion)) = self.take_pending() else {
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
        if !self.critical {
            self.finish();
            return;
        }
        // This runs only when the last reference dies, and for a critical job
        // that owner is not allowed to destroy the payload: Tokio can drop an
        // accepted closure after `submit_blocking_job` sampled acceptance,
        // which leaves the submitting thread -- possibly inside a framework
        // critical section -- holding the last still-pending reference. Hand
        // the work to a fresh job on the fallback queue instead. The queued
        // copy is only ever dropped after the worker has run it, so this
        // cannot re-enter.
        let Some((value, completion)) = self.take_pending() else {
            return;
        };
        retain_fallback_disposal(Self::critical(value, completion) as Arc<dyn BlockingPoolJob>);
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
        self.lock_state().is_some()
    }
}

/// Jobs awaiting the shared non-runtime disposal thread.
///
/// `worker_live` is only cleared by the worker after observing an empty queue
/// under this lock, and submitters push and consult it under the same lock, so
/// a successful worker start cannot miss queued work. Critical-section
/// disposal is allowed to remain queued without a worker after native thread
/// creation fails; every later submission retries the worker start.
#[derive(Default)]
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
    enqueue_fallback_disposal_with(&FALLBACK_DISPOSALS, job, spawn_fallback_worker)
}

/// Queues a disposal that must never fall back to the submitting thread.
///
/// The `()` return is the contract: every path transfers ownership. If native
/// thread creation is temporarily exhausted, the static queue keeps the job
/// and a later disposal submission retries the worker start. The fail-safe
/// degradation is unreclaimed memory, never user destruction in a framework
/// critical section.
fn retain_fallback_disposal(job: Arc<dyn BlockingPoolJob>) {
    retain_fallback_disposal_with(&FALLBACK_DISPOSALS, job, spawn_fallback_worker);
}

fn spawn_fallback_worker() -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("shelterwood-disposal".to_owned())
        .spawn(run_fallback_disposals)
}

fn enqueue_fallback_disposal_with(
    disposals: &Mutex<FallbackDisposals>,
    job: Arc<dyn BlockingPoolJob>,
    spawn: impl FnOnce() -> std::io::Result<std::thread::JoinHandle<()>>,
) -> bool {
    let mut state = disposals
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.queue.push_back(job);
    if start_worker_locked(&mut state, spawn) {
        return true;
    }
    // The push and this pop share one guard, so the reclaimed entry is exactly
    // the job just appended even when older critical jobs are still queued.
    let rejected = state.queue.pop_back();
    drop(state);
    debug_assert!(rejected.is_some());
    drop(rejected);
    false
}

fn retain_fallback_disposal_with(
    disposals: &Mutex<FallbackDisposals>,
    job: Arc<dyn BlockingPoolJob>,
    spawn: impl FnOnce() -> std::io::Result<std::thread::JoinHandle<()>>,
) {
    let mut state = disposals
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.queue.push_back(job);
    // A failed worker start leaves the job queued deliberately; the next
    // submission of any kind retries the start and drains it.
    start_worker_locked(&mut state, spawn);
}

/// Reports whether a worker is live, starting one under the caller's guard.
///
/// Spawning under the lock makes queueing and worker liveness one atomic
/// decision, so no submitter can observe a queued job without a worker that a
/// later submission will start.
fn start_worker_locked(
    state: &mut FallbackDisposals,
    spawn: impl FnOnce() -> std::io::Result<std::thread::JoinHandle<()>>,
) -> bool {
    if state.worker_live {
        return true;
    }
    match spawn() {
        Ok(worker) => {
            drop(worker);
            state.worker_live = true;
            true
        }
        Err(_) => false,
    }
}

fn run_fallback_disposals() {
    loop {
        let job = {
            let mut state = FALLBACK_DISPOSALS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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

/// Detaches user destruction from a framework critical section.
///
/// Unlike [`dispose_detached`], no path destroys the value on the submitting
/// thread. Exhausted task and native-thread creation leaves the job in a
/// static queue until a later submission can start the shared disposal
/// worker, and the accepted path is covered too: acceptance is sampled from a
/// reference count, so a runtime shut down right after that sample can leave
/// this thread holding the last still-pending reference. `DisposalJob`'s drop
/// re-routes that payload instead of finishing it, which keeps the lock rule
/// under both resource exhaustion and teardown races.
pub fn dispose_critical<T: Send + 'static>(value: T) {
    let job = DisposalJob::critical(value, |_| {});
    if submit_blocking_job(&job) {
        return;
    }
    retain_fallback_disposal(Arc::clone(&job) as Arc<dyn BlockingPoolJob>);
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
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        thread::{self, ThreadId},
        time::Duration,
    };

    use super::{
        DisposalJob, DisposalPanic, FallbackDisposals, Isolated, dispose_detached,
        enqueue_fallback_disposal_with, retain_fallback_disposal_with,
    };
    use crate::{
        spawn::BlockingPoolJob,
        test_support::{
            BlockingDrop, DESTRUCTOR_ESCAPE, DISPOSAL_THREAD as FALLBACK_THREAD, RecordingDrop,
            assert_blocking_pool_outcomes, drop_gate, release,
            submit_during_blocking_pool_shutdown,
        },
    };

    struct PanickingDrop(Arc<AtomicUsize>);

    struct LockCheckingJob {
        disposals: Arc<Mutex<FallbackDisposals>>,
        dropped_after_unlock: Arc<AtomicBool>,
    }

    impl Drop for LockCheckingJob {
        fn drop(&mut self) {
            self.dropped_after_unlock
                .store(self.disposals.try_lock().is_ok(), Ordering::SeqCst);
        }
    }

    impl BlockingPoolJob for LockCheckingJob {
        fn run(&self) {}

        fn is_pending(&self) -> bool {
            true
        }
    }

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
    fn critical_disposal_stays_queued_when_the_fallback_thread_cannot_start() {
        let drops = Arc::new(AtomicUsize::new(0));
        let job = DisposalJob::critical(PanickingDrop(Arc::clone(&drops)), |_| {});
        let disposals = Mutex::new(FallbackDisposals::default());

        retain_fallback_disposal_with(
            &disposals,
            Arc::clone(&job) as Arc<dyn BlockingPoolJob>,
            || Err(std::io::Error::other("injected thread exhaustion")),
        );
        drop(job);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "thread-creation failure must not reclaim critical payloads inline"
        );

        let queued = disposals
            .lock()
            .expect("local disposal queue remains healthy")
            .queue
            .pop_front()
            .expect("failed spawn keeps critical disposal queued");
        queued.run();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn rejected_fallback_job_is_released_after_unlock() {
        let disposals = Arc::new(Mutex::new(FallbackDisposals::default()));
        let dropped_after_unlock = Arc::new(AtomicBool::new(false));
        let job = Arc::new(LockCheckingJob {
            disposals: Arc::clone(&disposals),
            dropped_after_unlock: Arc::clone(&dropped_after_unlock),
        });

        assert!(!enqueue_fallback_disposal_with(&disposals, job, || Err(
            std::io::Error::other("injected thread exhaustion")
        ),));
        assert!(dropped_after_unlock.load(Ordering::SeqCst));
    }

    #[test]
    fn pending_query_tolerates_a_poisoned_disposal_job() {
        let job = DisposalJob::new((), |_| {});
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = job.state.lock().expect("state starts healthy");
            panic!("inject disposal state poison");
        }));

        assert!(job.is_pending());
    }

    /// The teardown race `dispose_critical` cannot sample its way out of:
    /// Tokio may drop an accepted closure after acceptance was observed,
    /// leaving the submitter with the last still-pending reference. Dropping
    /// the sole reference here stands in for that owner, and must re-route
    /// rather than destroy the payload on this thread.
    #[test]
    fn dropping_the_last_critical_job_reroutes_off_the_owning_thread() {
        let owning_thread = thread::current().id();
        let (destroyed, destroyed_rx) = mpsc::channel();
        let job = DisposalJob::critical(RecordingDrop(destroyed), |_| {});

        drop(job);

        let (destructor_thread, destructor_name) = destroyed_rx
            .recv_timeout(DESTRUCTOR_ESCAPE)
            .expect("the re-routed payload is destroyed");
        assert_ne!(
            destructor_thread, owning_thread,
            "a critical payload must never be destroyed by its last owner"
        );
        assert_eq!(
            destructor_name.as_deref(),
            Some(FALLBACK_THREAD),
            "the re-routed payload lands on the shared fallback thread"
        );
    }

    #[test]
    fn fallback_detection_distinguishes_blocking_spawn_outcomes() {
        let accepted = DisposalJob::new((), |_| {});
        let rejected = DisposalJob::new((), |_| {});
        let completed = DisposalJob::new((), |_| {});
        assert_blocking_pool_outcomes(accepted, rejected, completed);
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
        let (submitted, submitted_rx) = mpsc::channel();
        let (returned, returned_rx) = mpsc::channel();
        let (entered, entered_rx) = mpsc::channel();
        let (finished, finished_rx) = mpsc::channel();
        let gate = drop_gate();
        let trigger = DisposeOnDrop {
            value: Some(BlockingDrop::with_completion(
                entered,
                Arc::clone(&gate),
                finished,
            )),
            submitted,
            returned,
        };
        // With the only worker occupied, this outer task stays queued. After
        // shutdown begins, Tokio runs it while draining the worker queue; its
        // nested disposal submission is then synchronously rejected.
        submit_during_blocking_pool_shutdown(move || drop(trigger));

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
        let gate = drop_gate();
        let held = Isolated::new(BlockingDrop::with_completion(
            entered,
            Arc::clone(&gate),
            finished,
        ));
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
