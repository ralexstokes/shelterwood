use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::Notify;

/// A one-permit asynchronous gate used to sequence child progress.
#[derive(Clone, Debug, Default)]
pub(crate) struct ReleaseGate(Arc<Notify>);

impl ReleaseGate {
    /// Waits until the gate is released.
    pub(crate) async fn wait(&self) {
        self.0.notified().await;
    }

    /// Releases one current or future waiter.
    pub(crate) fn release(&self) {
        self.0.notify_one();
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

#[cfg(test)]
mod tests {
    use super::ReleaseGate;

    #[tokio::test]
    async fn release_gate_stores_one_permit() {
        let gate = ReleaseGate::default();
        gate.release();
        gate.wait().await;
    }
}
