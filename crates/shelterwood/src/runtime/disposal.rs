use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::task;

use super::{Latch, catch_panic, contain_panic_payload, discard_panic, is_available};

/// Ownership wrapper for user values retained by framework state.
///
/// Dropping the wrapper transfers the value to an isolated blocking task, so
/// framework futures never run user destruction as part of their own drop
/// glue. Callers that need to classify destruction can take the value and
/// join a dedicated blocking task explicitly.
pub(crate) struct Isolated<T: Send + 'static> {
    value: Option<T>,
}

impl<T: Send + 'static> Isolated<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self { value: Some(value) }
    }

    pub(crate) fn get(&self) -> &T {
        self.value
            .as_ref()
            .expect("isolated user value was already taken")
    }

    pub(crate) fn get_mut(&mut self) -> &mut T {
        self.value
            .as_mut()
            .expect("isolated user value was already taken")
    }

    pub(crate) fn take(&mut self) -> Option<T> {
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
pub(crate) struct DisposalPanic {
    pub(crate) message: Option<String>,
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

/// Erased view of a queued disposal job for the shared fallback thread.
trait QueuedDisposal: Send + Sync {
    fn run(&self);
}

impl<T, C> QueuedDisposal for DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn run(&self) {
        self.finish();
    }
}

/// Jobs awaiting the shared non-runtime disposal thread.
///
/// `worker_live` is only cleared by the worker after observing an empty queue
/// under this lock, and submitters push and consult it under the same lock, so
/// a queued job always has a live worker destined to drain it.
struct FallbackDisposals {
    queue: VecDeque<Arc<dyn QueuedDisposal>>,
    worker_live: bool,
}

static FALLBACK_DISPOSALS: Mutex<FallbackDisposals> = Mutex::new(FallbackDisposals {
    queue: VecDeque::new(),
    worker_live: false,
});

/// Queues a disposal job for the shared fallback thread, lazily starting it.
/// Returns `false` when no worker exists and none could be started; the
/// caller must then finish the job itself.
fn enqueue_fallback_disposal(job: Arc<dyn QueuedDisposal>) -> bool {
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
    if is_available() {
        let worker = Arc::clone(&job);
        match catch_panic(|| task::spawn_blocking(move || worker.finish())) {
            Ok(handle) => {
                drop(handle);
                return;
            }
            Err(payload) => discard_panic(Some(payload)),
        }
    }

    // Outside a runtime, one shared lazily started thread drains a queue of
    // disposal jobs, so dropping N values costs at most one thread rather
    // than one thread per value, while a blocking or panicking destructor
    // still never runs on (or unwinds into) the submitting thread. The queue
    // is unbounded on purpose: applying a bound would block the submitter on
    // user destructors, exactly what isolation must prevent. Serialization is
    // the accepted trade: one blocking destructor delays later fallback
    // disposals instead of consuming another native thread.
    if enqueue_fallback_disposal(Arc::clone(&job) as Arc<dyn QueuedDisposal>) {
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
pub(crate) fn dispose_then<T, C>(value: T, completion: C)
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    dispatch_disposal(DisposalJob::new(value, completion));
}

/// Detaches potentially blocking or panicking user destruction from the
/// caller. The guard also contains a panic if task/thread creation itself
/// fails and drops the closure on the submitting thread.
pub(crate) fn dispose_detached<T: Send + 'static>(value: T) {
    dispose_then(value, |_| {});
}

/// Starts isolated disposal for every value and fires once all jobs finish.
///
/// Each value gets its own unwind boundary, so one destructor panic cannot
/// prevent the remaining values or the aggregate completion from running.
pub(crate) fn dispose_all<T: Send + 'static>(values: Vec<T>) -> Latch {
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
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use super::{DisposalJob, DisposalPanic};

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
}
