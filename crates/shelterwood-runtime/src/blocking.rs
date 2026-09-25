use std::{
    future::{Future, poll_fn},
    panic::resume_unwind,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::task;

use super::{
    DisposingReceiver, OneShotReceiver, OneShotSender, PanicPayload, catch_panic, discard_panic,
    dispose_detached, is_available, oneshot,
};

const BLOCKING_FALLBACK_THREAD: &str = "shelterwood-blocking";

type BlockingOutcome<T> = Result<T, PanicPayload>;
type BlockingCompletion<T> = OneShotSender<BlockingOutcome<T>>;

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
/// This relies on Tokio 1.53.1's `spawn_task` shutdown path: a rejected
/// closure is destroyed synchronously, before `spawn_blocking` returns. A
/// Tokio that deferred that drop would leave the count at two, so the job
/// would count as accepted and Tokio would destroy the closure itself: that
/// fails safe, and never misroutes a live closure. The end-to-end regressions
/// in this crate pin the behavior relied on.
/// Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
pub(crate) fn blocking_pool_accepted<J: BlockingPoolJob>(job: &Arc<J>) -> bool {
    Arc::strong_count(job) > 1 || !job.is_pending()
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
        BlockingDrop, DESTRUCTOR_ESCAPE as WAIT, DISPOSAL_THREAD, PanickingDrop, RecordingDrop,
        assert_blocking_pool_outcomes, drop_gate, release, submit_during_blocking_pool_shutdown,
    };

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_work_preserves_values_and_panics() {
        assert_eq!(super::spawn_blocking_work(|| 42_u8).await, 42);

        let panicking = crate::spawn(async {
            super::spawn_blocking_work(|| panic!("blocking work panic")).await
        });
        assert!(matches!(
            crate::join(panicking).await,
            shelterwood_core::exit::JoinOutcome::Panic {
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
