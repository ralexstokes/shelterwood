use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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

struct DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce() + Send + 'static,
{
    state: Mutex<Option<(T, C)>>,
    /// Whether the submitter may never destroy this payload itself.
    critical: bool,
}

impl<T, C> DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce() + Send + 'static,
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
        // A destructor panic is a disposal fault: contained here and never
        // reported to the completion, which learns only that destruction
        // finished. The payload itself is user-owned, so it retires through
        // the same detached venue as any other contained panic payload.
        if let Err(payload) = catch_panic(|| drop(value)) {
            let _ = contain_panic_payload(payload);
        }
        // Completion is framework bookkeeping. Contain it as well so a
        // hostile waker or a runtime teardown race cannot unwind a blocking
        // worker or double-panic while the job is being dropped.
        discard_panic(catch_panic(completion).err());
    }
}

impl<T, C> Drop for DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce() + Send + 'static,
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
    C: FnOnce() + Send + 'static,
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

/// The fallback queue plus a lock-free hint that it holds stranded work.
///
/// `stranded` mirrors "jobs queued and no worker live" and is rewritten under
/// `state` at every transition that can change it. It lets a submission the
/// blocking pool accepted skip the mutex entirely in the common case, while
/// still retrying the worker start once thread exhaustion has stranded a
/// critical job. A stale read only delays that retry to a later submission.
struct FallbackQueue {
    stranded: AtomicBool,
    state: Mutex<FallbackDisposals>,
}

impl FallbackQueue {
    const fn new() -> Self {
        Self {
            stranded: AtomicBool::new(false),
            state: Mutex::new(FallbackDisposals {
                queue: VecDeque::new(),
                worker_live: false,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FallbackDisposals> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Republishes the stranded hint from the state the caller still guards.
    fn settle_locked(&self, state: &FallbackDisposals) {
        self.stranded.store(
            !state.worker_live && !state.queue.is_empty(),
            Ordering::Release,
        );
    }
}

static FALLBACK_DISPOSALS: FallbackQueue = FallbackQueue::new();

/// Queues a disposal that must never fall back to the submitting thread.
///
/// The `()` return is the contract: every path transfers ownership. If native
/// thread creation is temporarily exhausted, the static queue keeps the job
/// and a later disposal submission retries the worker start. The fail-safe
/// degradation is unreclaimed memory, never user destruction in a framework
/// critical section.
fn retain_fallback_disposal(job: Arc<dyn BlockingPoolJob>) {
    queue_fallback_disposal_with(&FALLBACK_DISPOSALS, job, spawn_fallback_worker, true);
}

fn spawn_fallback_worker() -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("shelterwood-disposal".to_owned())
        .spawn(|| run_fallback_disposals(&FALLBACK_DISPOSALS))
}

/// Appends `job` and reports whether a worker is live to drain it.
///
/// When the worker cannot start, `retain` decides the job's fate: a critical
/// job stays queued for a later submission's retry, while any other job is
/// popped back out and released after unlock so its submitter can finish it.
fn queue_fallback_disposal_with(
    disposals: &FallbackQueue,
    job: Arc<dyn BlockingPoolJob>,
    spawn: impl FnOnce() -> std::io::Result<std::thread::JoinHandle<()>>,
    retain: bool,
) -> bool {
    let mut state = disposals.lock();
    state.queue.push_back(job);
    let live = start_worker_locked(&mut state, spawn);
    // The push and this pop share one guard, so the reclaimed entry is exactly
    // the job just appended even when older critical jobs are still queued.
    let rejected = if live || retain {
        None
    } else {
        state.queue.pop_back()
    };
    disposals.settle_locked(&state);
    drop(state);
    // Keep even a future queue regression total. The caller retains the
    // original job Arc and will finish it on the ordinary fallback path; an
    // assertion here could instead unwind that user payload through disposal
    // infrastructure.
    drop(rejected);
    live
}

/// Retries the worker start for jobs a failed start left without a worker.
///
/// Called after the blocking pool accepts a submission, which is otherwise
/// the one path that never consults the fallback queue. The atomic hint keeps
/// that path lock-free until something is actually stranded. Nothing but the
/// spawn attempt runs under the guard: queued jobs are neither run nor
/// dropped here.
fn retry_stranded_fallback_with(
    disposals: &FallbackQueue,
    spawn: impl FnOnce() -> std::io::Result<std::thread::JoinHandle<()>>,
) {
    if !disposals.stranded.load(Ordering::Acquire) {
        return;
    }
    let mut state = disposals.lock();
    if !state.queue.is_empty() {
        start_worker_locked(&mut state, spawn);
    }
    disposals.settle_locked(&state);
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

fn run_fallback_disposals(disposals: &FallbackQueue) {
    loop {
        let job = {
            let mut state = disposals.lock();
            let Some(job) = state.queue.pop_front() else {
                state.worker_live = false;
                disposals.settle_locked(&state);
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
    C: FnOnce() + Send + 'static,
{
    dispatch_disposal_with(job, &FALLBACK_DISPOSALS, spawn_fallback_worker);
}

fn dispatch_disposal_with<T, C>(
    job: Arc<DisposalJob<T, C>>,
    disposals: &FallbackQueue,
    spawn: impl FnOnce() -> std::io::Result<std::thread::JoinHandle<()>>,
) where
    T: Send + 'static,
    C: FnOnce() + Send + 'static,
{
    // A rejected submission falls through so the fallback thread, rather than
    // this runtime-teardown thread, owns user destruction.
    if submit_blocking_job(&job) {
        retry_stranded_fallback_with(disposals, spawn);
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
    if queue_fallback_disposal_with(
        disposals,
        Arc::clone(&job) as Arc<dyn BlockingPoolJob>,
        spawn,
        false,
    ) {
        return;
    }

    // Exhausted task and thread creation must not strand completion or expose
    // a destructor panic. Blocking here is the only remaining safe fallback.
    job.finish();
}

/// Runs potentially blocking user destruction away from the caller and then
/// invokes framework completion. A destructor panic is contained and does not
/// reach the completion.
///
/// Inside a Tokio runtime this uses the blocking pool. Outside one, jobs are
/// funneled through a single shared disposal thread, so destroying many
/// values (for example dropping a large unspawned tree) never creates one
/// native thread per value.
pub fn dispose_then<T, C>(value: T, completion: C)
where
    T: Send + 'static,
    C: FnOnce() + Send + 'static,
{
    dispatch_disposal(DisposalJob::new(value, completion));
}

/// Detaches potentially blocking or panicking user destruction from the
/// caller. The guard also contains a panic if task/thread creation itself
/// fails and drops the closure on the submitting thread.
pub fn dispose_detached<T: Send + 'static>(value: T) {
    dispose_then(value, || {});
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
    dispose_critical_with(value, &FALLBACK_DISPOSALS, spawn_fallback_worker);
}

fn dispose_critical_with<T: Send + 'static>(
    value: T,
    disposals: &FallbackQueue,
    spawn: impl FnOnce() -> std::io::Result<std::thread::JoinHandle<()>>,
) {
    let job = DisposalJob::critical(value, || {});
    if submit_blocking_job(&job) {
        // Spawning is the only work this can add, and it runs no user code
        // and takes only the fallback queue's leaf lock, so the retry is as
        // legal inside a framework critical section as the submission is.
        retry_stranded_fallback_with(disposals, spawn);
        return;
    }
    queue_fallback_disposal_with(
        disposals,
        Arc::clone(&job) as Arc<dyn BlockingPoolJob>,
        spawn,
        true,
    );
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
        dispose_then(value, move || {
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
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        thread::{self, ThreadId},
        time::{Duration, Instant},
    };

    use super::{
        DisposalJob, FallbackQueue, Isolated, dispatch_disposal_with, dispose_critical_with,
        dispose_detached, queue_fallback_disposal_with, run_fallback_disposals,
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
        disposals: Arc<FallbackQueue>,
        dropped_after_unlock: Arc<AtomicBool>,
    }

    impl Drop for LockCheckingJob {
        fn drop(&mut self) {
            self.dropped_after_unlock
                .store(self.disposals.state.try_lock().is_ok(), Ordering::SeqCst);
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
        let completions = Arc::new(AtomicUsize::new(0));
        let completed = Arc::clone(&completions);
        let job = DisposalJob::new(PanickingDrop(Arc::clone(&drops)), move || {
            completed.fetch_add(1, Ordering::SeqCst);
        });

        drop(job);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            completions.load(Ordering::SeqCst),
            1,
            "a contained destructor panic still completes the job exactly once"
        );
    }

    #[test]
    fn critical_disposal_stays_queued_when_the_fallback_thread_cannot_start() {
        let drops = Arc::new(AtomicUsize::new(0));
        let job = DisposalJob::critical(PanickingDrop(Arc::clone(&drops)), || {});
        let disposals = FallbackQueue::new();

        assert!(!queue_fallback_disposal_with(
            &disposals,
            Arc::clone(&job) as Arc<dyn BlockingPoolJob>,
            || Err(std::io::Error::other("injected thread exhaustion")),
            true,
        ));
        drop(job);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "thread-creation failure must not reclaim critical payloads inline"
        );

        assert!(
            disposals.stranded.load(Ordering::SeqCst),
            "a job left without a worker is advertised for retry"
        );
        let queued = disposals
            .state
            .lock()
            .expect("local disposal queue remains healthy")
            .queue
            .pop_front()
            .expect("failed spawn keeps critical disposal queued");
        queued.run();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    /// Strands one critical job behind an injected worker-start failure on a
    /// leaked (hence `'static`) queue a real worker can drain.
    fn strand_critical_job() -> (
        &'static FallbackQueue,
        mpsc::Receiver<(ThreadId, Option<String>)>,
    ) {
        let disposals: &'static FallbackQueue = Box::leak(Box::new(FallbackQueue::new()));
        let (destroyed, destroyed_rx) = mpsc::channel();
        let job = DisposalJob::critical(RecordingDrop(destroyed), || {});
        assert!(!queue_fallback_disposal_with(
            disposals,
            Arc::clone(&job) as Arc<dyn BlockingPoolJob>,
            || Err(std::io::Error::other("injected thread exhaustion")),
            true,
        ));
        drop(job);
        assert!(disposals.stranded.load(Ordering::SeqCst));
        assert!(
            destroyed_rx.try_recv().is_err(),
            "a failed worker start keeps the critical job queued"
        );
        (disposals, destroyed_rx)
    }

    fn start_local_worker(
        disposals: &'static FallbackQueue,
    ) -> std::io::Result<thread::JoinHandle<()>> {
        thread::Builder::new()
            .name(FALLBACK_THREAD.to_owned())
            .spawn(move || run_fallback_disposals(disposals))
    }

    fn assert_stranded_job_reclaimed(
        disposals: &FallbackQueue,
        destroyed_rx: &mpsc::Receiver<(ThreadId, Option<String>)>,
    ) {
        let (_, destructor_name) = destroyed_rx
            .recv_timeout(DESTRUCTOR_ESCAPE)
            .expect("an accepted submission retries the stranded worker start");
        assert_eq!(destructor_name.as_deref(), Some(FALLBACK_THREAD));
        let deadline = Instant::now() + DESTRUCTOR_ESCAPE;
        while disposals.stranded.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "the stranded hint clears");
            thread::yield_now();
        }
    }

    /// P2-5: once thread exhaustion strands a critical job, a later
    /// submission the blocking pool accepts must still retry the fallback
    /// worker rather than leak the job for the life of the process.
    #[test]
    fn accepted_disposal_retries_a_stranded_fallback_worker() {
        let (disposals, destroyed_rx) = strand_critical_job();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .expect("test runtime");
        let _entered = runtime.enter();

        dispatch_disposal_with(DisposalJob::new((), || {}), disposals, || {
            start_local_worker(disposals)
        });

        assert_stranded_job_reclaimed(disposals, &destroyed_rx);
    }

    #[test]
    fn accepted_critical_disposal_retries_a_stranded_fallback_worker() {
        let (disposals, destroyed_rx) = strand_critical_job();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .expect("test runtime");
        let _entered = runtime.enter();
        let (accepted, accepted_rx) = mpsc::channel();

        dispose_critical_with(RecordingDrop(accepted), disposals, || {
            start_local_worker(disposals)
        });

        let (_, accepted_name) = accepted_rx
            .recv_timeout(DESTRUCTOR_ESCAPE)
            .expect("the accepted critical payload is destroyed");
        assert_ne!(
            accepted_name.as_deref(),
            Some(FALLBACK_THREAD),
            "a live runtime runs critical disposal on its blocking pool"
        );
        assert_stranded_job_reclaimed(disposals, &destroyed_rx);
    }

    #[test]
    fn accepted_disposal_skips_the_fallback_queue_when_nothing_is_stranded() {
        let disposals = FallbackQueue::new();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .expect("test runtime");
        let _entered = runtime.enter();
        // Holding the queue lock proves the common path never touches it: a
        // retry that locked would deadlock here instead of returning.
        let _held = disposals.state.lock().expect("local queue starts healthy");

        dispose_critical_with((), &disposals, || {
            panic!("no stranded work means no worker start")
        });
    }

    #[test]
    fn rejected_fallback_job_is_released_after_unlock() {
        let disposals = Arc::new(FallbackQueue::new());
        let dropped_after_unlock = Arc::new(AtomicBool::new(false));
        let job = Arc::new(LockCheckingJob {
            disposals: Arc::clone(&disposals),
            dropped_after_unlock: Arc::clone(&dropped_after_unlock),
        });

        assert!(!queue_fallback_disposal_with(
            &disposals,
            job,
            || Err(std::io::Error::other("injected thread exhaustion")),
            false,
        ));
        assert!(dropped_after_unlock.load(Ordering::SeqCst));
    }

    #[test]
    fn exhausted_runtime_and_thread_creation_finishes_disposal_inline() {
        let drops = Arc::new(AtomicUsize::new(0));
        let completions = Arc::new(AtomicUsize::new(0));
        let completed = Arc::clone(&completions);
        let disposals = FallbackQueue::new();
        let job = DisposalJob::new(PanickingDrop(Arc::clone(&drops)), move || {
            completed.fetch_add(1, Ordering::SeqCst);
        });

        // A plain test has no Tokio context, so blocking-pool submission is
        // rejected. The injected spawner then reaches the final synchronous
        // degradation path without depending on actual resource exhaustion.
        dispatch_disposal_with(job, &disposals, || {
            Err(std::io::Error::other("injected thread exhaustion"))
        });

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            disposals
                .state
                .lock()
                .expect("local disposal queue remains healthy")
                .queue
                .is_empty(),
            "a non-critical job rejected by the spawner returns to its submitter"
        );
        assert_eq!(
            completions.load(Ordering::SeqCst),
            1,
            "a contained destructor panic still completes the job exactly once"
        );
    }

    #[test]
    fn pending_query_tolerates_a_poisoned_disposal_job() {
        let job = DisposalJob::new((), || {});
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
        let job = DisposalJob::critical(RecordingDrop(destroyed), || {});

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
        let accepted = DisposalJob::new((), || {});
        let rejected = DisposalJob::new((), || {});
        let completed = DisposalJob::new((), || {});
        assert_blocking_pool_outcomes(accepted, rejected, completed);
    }

    struct AggregateDrop {
        id: u8,
        dropped: mpsc::Sender<u8>,
        gate: Option<Arc<(Mutex<bool>, Condvar)>>,
        panic: bool,
    }

    impl Drop for AggregateDrop {
        fn drop(&mut self) {
            let _ = self.dropped.send(self.id);
            if let Some(gate) = &self.gate {
                let (released, wake) = &**gate;
                let mut released = released.lock().expect("aggregate gate remains healthy");
                let deadline = Instant::now() + DESTRUCTOR_ESCAPE;
                while !*released {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        break;
                    };
                    released = wake
                        .wait_timeout(released, remaining)
                        .expect("aggregate gate remains healthy")
                        .0;
                }
            }
            assert!(!self.panic, "injected aggregate destructor panic");
        }
    }

    #[test]
    fn dispose_all_empty_fires_synchronously() {
        let completion = super::dispose_all::<u8>(Vec::new());
        assert!(completion.is_fired());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispose_all_fires_only_after_every_value_finishes() {
        let (dropped, dropped_rx) = mpsc::channel();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let values = (0_u8..3)
            .map(|id| AggregateDrop {
                id,
                dropped: dropped.clone(),
                gate: (id == 2).then(|| Arc::clone(&gate)),
                panic: false,
            })
            .collect();
        let completion = super::dispose_all(values);

        let mut observed = Vec::new();
        for _ in 0..3 {
            observed.push(
                dropped_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("every destructor starts"),
            );
        }
        observed.sort_unstable();
        assert_eq!(observed, [0, 1, 2]);
        assert!(matches!(
            crate::timeout(Duration::from_millis(100), completion.fired()).await,
            crate::Timeout::Elapsed
        ));
        release(&gate);
        assert!(matches!(
            crate::timeout(Duration::from_secs(1), completion.fired()).await,
            crate::Timeout::Completed(())
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispose_all_contains_one_panic_and_completes_the_rest() {
        let (dropped, dropped_rx) = mpsc::channel();
        let values = (0_u8..3)
            .map(|id| AggregateDrop {
                id,
                dropped: dropped.clone(),
                gate: None,
                panic: id == 1,
            })
            .collect();
        let completion = super::dispose_all(values);

        assert!(matches!(
            crate::timeout(Duration::from_secs(1), completion.fired()).await,
            crate::Timeout::Completed(())
        ));
        let mut observed = Vec::new();
        for _ in 0..3 {
            observed.push(
                dropped_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("one panicking destructor cannot suppress another"),
            );
        }
        observed.sort_unstable();
        assert_eq!(observed, [0, 1, 2]);
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
    /// Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
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
