use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::Semaphore;

#[derive(Debug)]
struct ReleaseState {
    permits: Semaphore,
    // Demand and supply, owned by the gate rather than read back from
    // `Semaphore::available_permits`. The semaphore reports *storage*, not
    // intent: it hides a release the instant a waiter is enqueued and
    // re-stores one when a waiter's `acquire` future is cancelled, so a check
    // against it both misses and invents over-releases depending on when the
    // waiting task happened to be polled.
    waits: AtomicUsize,
    releases: AtomicUsize,
}

impl Default for ReleaseState {
    fn default() -> Self {
        Self {
            permits: Semaphore::new(0),
            waits: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
        }
    }
}

/// A one-permit asynchronous gate used to sequence child progress.
///
/// A release never collapses the way `Notify::notify_one` does — the extra
/// permit is stored and the next waiter claims it — but running more than one
/// release ahead of the waits that have begun is still a test bug, so the gate
/// fails loudly instead of letting the surplus go unnoticed.
#[derive(Clone, Debug, Default)]
pub(crate) struct ReleaseGate(Arc<ReleaseState>);

impl ReleaseGate {
    /// Waits until the gate is released.
    pub(crate) async fn wait(&self) {
        self.0.waits.fetch_add(1, Ordering::SeqCst);
        self.0
            .permits
            .acquire()
            .await
            .expect("test release gates are never closed")
            .forget();
    }

    /// Releases one current or future waiter.
    #[track_caller]
    pub(crate) fn release(&self) {
        let issued = self.0.releases.fetch_add(1, Ordering::SeqCst) + 1;
        let begun = self.0.waits.load(Ordering::SeqCst);
        assert!(
            issued <= begun + 1,
            "a ReleaseGate cannot run more than one release ahead of its waiters \
             ({issued} releases against {begun} waits)"
        );
        self.0.permits.add_permits(1);
    }
}

#[derive(Debug, Default)]
struct DestructorState {
    entered: bool,
    released: bool,
}

/// Controls a fixture whose destructor blocks at an exact test window.
#[derive(Clone, Debug, Default)]
pub(crate) struct DestructorGate(Arc<(Mutex<DestructorState>, Condvar)>);

impl DestructorGate {
    /// Creates a value that blocks in `Drop` until this gate is released.
    pub(crate) fn blocker(&self) -> DestructorBlocker {
        DestructorBlocker(Arc::clone(&self.0))
    }

    /// Blocks the calling test thread until the destructor has started.
    pub(crate) fn wait_entered(&self) {
        let (state, changed) = &*self.0;
        let mut state = state.lock().expect("destructor gate mutex poisoned");
        while !state.entered {
            state = changed
                .wait(state)
                .expect("destructor gate mutex poisoned while waiting");
        }
    }

    /// Releases the blocked destructor.
    pub(crate) fn release(&self) {
        let (state, changed) = &*self.0;
        let mut state = state.lock().expect("destructor gate mutex poisoned");
        state.released = true;
        changed.notify_all();
    }
}

/// A value that blocks in `Drop` under a [`DestructorGate`].
#[derive(Debug)]
pub(crate) struct DestructorBlocker(Arc<(Mutex<DestructorState>, Condvar)>);

impl Drop for DestructorBlocker {
    fn drop(&mut self) {
        // A stuck-test backstop, not an assertion: well above any legitimate
        // wait (POLL_TIMEOUT and every gate driven by it) so a passing test
        // can never reach it.
        const MAX_BLOCK: Duration = Duration::from_secs(30);

        let (state, changed) = &*self.0;
        let mut state = state.lock().expect("destructor gate mutex poisoned");
        state.entered = true;
        changed.notify_all();
        let deadline = Instant::now() + MAX_BLOCK;
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                // This destructor also runs while other panics unwind, where
                // a nested panic aborts with no diagnostic at all. Report and
                // abort deliberately instead.
                eprintln!("destructor gate was not released within {MAX_BLOCK:?}");
                if std::thread::panicking() {
                    std::process::abort();
                }
                panic!("destructor gate was not released within {MAX_BLOCK:?}");
            }
            state = changed
                .wait_timeout(state, remaining)
                .expect("destructor gate mutex poisoned while blocking")
                .0;
        }
    }
}
