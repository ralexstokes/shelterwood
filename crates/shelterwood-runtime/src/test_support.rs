use std::{
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

use crate::spawn::{BlockingPoolJob, blocking_pool_accepted};

/// Deadlock escape hatch for hostile test destructors.
pub(crate) const DESTRUCTOR_ESCAPE: Duration = Duration::from_secs(5);
pub(crate) const DISPOSAL_THREAD: &str = "shelterwood-disposal";

pub(crate) type ThreadDescription = (ThreadId, Option<String>);

fn describe_current_thread() -> ThreadDescription {
    let current = thread::current();
    (current.id(), current.name().map(str::to_owned))
}

pub(crate) struct RecordingDrop(pub(crate) mpsc::Sender<ThreadDescription>);

impl Drop for RecordingDrop {
    fn drop(&mut self) {
        let _ = self.0.send(describe_current_thread());
    }
}

pub(crate) type DropGate = Arc<(Mutex<bool>, Condvar)>;

pub(crate) fn drop_gate() -> DropGate {
    Arc::new((Mutex::new(false), Condvar::new()))
}

pub(crate) struct BlockingDrop {
    entered: mpsc::Sender<ThreadDescription>,
    release: DropGate,
    finished: Option<mpsc::Sender<()>>,
}

impl BlockingDrop {
    pub(crate) fn new(entered: mpsc::Sender<ThreadDescription>, release: DropGate) -> Self {
        Self {
            entered,
            release,
            finished: None,
        }
    }

    pub(crate) fn with_completion(
        entered: mpsc::Sender<ThreadDescription>,
        release: DropGate,
        finished: mpsc::Sender<()>,
    ) -> Self {
        Self {
            entered,
            release,
            finished: Some(finished),
        }
    }
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
        if let Some(finished) = &self.finished {
            let _ = finished.send(());
        }
    }
}

pub(crate) fn release(gate: &DropGate) {
    let (released, wake) = &**gate;
    *released.lock().expect("release mutex available") = true;
    wake.notify_all();
}

pub(crate) fn assert_blocking_pool_outcomes<
    A: BlockingPoolJob,
    R: BlockingPoolJob,
    C: BlockingPoolJob,
>(
    accepted: Arc<A>,
    rejected: Arc<R>,
    completed: Arc<C>,
) {
    let accepted_worker = Arc::clone(&accepted);
    assert!(
        blocking_pool_accepted(&accepted),
        "an accepted closure still owned by Tokio must stay on its blocking pool"
    );
    drop(accepted_worker);

    let rejected_worker = Arc::clone(&rejected);
    drop(rejected_worker);
    assert!(
        !blocking_pool_accepted(&rejected),
        "a synchronously dropped closure must move to fallback"
    );

    let completed_worker = Arc::clone(&completed);
    completed_worker.run();
    drop(completed_worker);
    assert!(
        blocking_pool_accepted(&completed),
        "an already-completed closure must not enqueue an empty job"
    );
}

/// Submits `task` behind an occupied blocking worker, starts runtime teardown,
/// then frees the worker so Tokio rejects nested blocking submissions in
/// `task` synchronously.
pub(crate) fn submit_during_blocking_pool_shutdown(task: impl FnOnce() + Send + 'static) {
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

    drop(runtime.spawn_blocking(task));
    runtime.shutdown_background();
    release_worker
        .send(())
        .expect("the blocking-pool teardown may proceed");
}
