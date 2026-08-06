//! Deterministic fixtures shared by Shelterwood's conformance tests.

use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::Notify;

/// An observable flag paired with an owned liveness guard.
#[derive(Clone, Debug)]
pub struct LiveFlag(Arc<AtomicBool>);

impl LiveFlag {
    /// Creates a live flag and the guard whose drop clears it.
    pub fn guarded() -> (Self, LiveGuard) {
        let value = Arc::new(AtomicBool::new(true));
        (Self(Arc::clone(&value)), LiveGuard(value))
    }

    /// Reports whether the paired guard is still live.
    pub fn is_live(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Clears its paired [`LiveFlag`] when dropped.
#[derive(Debug)]
pub struct LiveGuard(Arc<AtomicBool>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Counts consumption and fallback drops of owned one-shot resources.
#[derive(Clone, Debug, Default)]
pub struct ConsumeCount(Arc<AtomicUsize>);

impl ConsumeCount {
    /// Creates a guard that increments this count when consumed or dropped.
    pub fn guard(&self) -> ConsumeGuard {
        ConsumeGuard(Some(Arc::clone(&self.0)))
    }

    /// Returns the number of observed consumptions.
    pub fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    /// Asserts that exactly one consumption occurred.
    #[track_caller]
    pub fn assert_once(&self) {
        assert_eq!(self.get(), 1, "resource must be consumed exactly once");
    }
}

/// A consume-once witness whose fallback effect runs on drop.
#[derive(Debug)]
pub struct ConsumeGuard(Option<Arc<AtomicUsize>>);

impl ConsumeGuard {
    /// Consumes the witness and records its effect immediately.
    pub fn consume(mut self) {
        self.record();
    }

    fn record(&mut self) {
        if let Some(count) = self.0.take() {
            count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Drop for ConsumeGuard {
    fn drop(&mut self) {
        self.record();
    }
}

/// A one-permit asynchronous gate used to sequence child progress.
#[derive(Clone, Debug, Default)]
pub struct ReleaseGate(Arc<Notify>);

impl ReleaseGate {
    /// Waits until the gate is released.
    pub async fn wait(&self) {
        self.0.notified().await;
    }

    /// Releases one current or future waiter.
    pub fn release(&self) {
        self.0.notify_one();
    }
}

/// Coordinates acceptance before a fixture begins receiving work.
#[derive(Clone, Debug, Default)]
pub struct ParkBeforeReceive {
    accepted: Arc<Notify>,
    release: ReleaseGate,
}

impl ParkBeforeReceive {
    /// Marks the fixture as having accepted its input.
    pub fn mark_accepted(&self) {
        self.accepted.notify_one();
    }

    /// Waits for the fixture to report acceptance.
    pub async fn wait_accepted(&self) {
        self.accepted.notified().await;
    }

    /// Parks until receiving is explicitly released.
    pub async fn park(&self) {
        self.release.wait().await;
    }

    /// Allows the fixture to begin receiving.
    pub fn release(&self) {
        self.release.release();
    }
}

#[derive(Debug, Default)]
struct DestructorState {
    entered: bool,
    released: bool,
}

/// Controls a fixture whose destructor blocks at an exact test window.
#[derive(Clone, Debug, Default)]
pub struct DestructorGate(Arc<(Mutex<DestructorState>, Condvar)>);

impl DestructorGate {
    /// Creates a value that blocks in `Drop` until this gate is released.
    pub fn blocker(&self) -> DestructorBlocker {
        DestructorBlocker(Arc::clone(&self.0))
    }

    /// Blocks the calling test thread until the destructor has started.
    pub fn wait_entered(&self) {
        let (state, changed) = &*self.0;
        let mut state = state.lock().expect("destructor gate mutex poisoned");
        while !state.entered {
            state = changed
                .wait(state)
                .expect("destructor gate mutex poisoned while waiting");
        }
    }

    /// Releases the blocked destructor.
    pub fn release(&self) {
        let (state, changed) = &*self.0;
        let mut state = state.lock().expect("destructor gate mutex poisoned");
        state.released = true;
        changed.notify_all();
    }
}

/// A value that blocks in `Drop` under a [`DestructorGate`].
#[derive(Debug)]
pub struct DestructorBlocker(Arc<(Mutex<DestructorState>, Condvar)>);

impl Drop for DestructorBlocker {
    fn drop(&mut self) {
        let (state, changed) = &*self.0;
        let mut state = state.lock().expect("destructor gate mutex poisoned");
        state.entered = true;
        changed.notify_all();
        while !state.released {
            state = changed
                .wait(state)
                .expect("destructor gate mutex poisoned while blocking");
        }
    }
}

/// A fixture that panics when dropped.
#[derive(Debug, Default)]
pub struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("intentional destructor panic");
    }
}

/// Pauses Tokio's clock for deterministic timing tests.
pub fn pause_time() {
    tokio::time::pause();
}

/// Advances Tokio's paused clock.
pub async fn advance_time(duration: Duration) {
    tokio::time::advance(duration).await;
}

/// Polls a synchronous observation until it succeeds or the deadline expires.
pub async fn poll_until(
    timeout: Duration,
    interval: Duration,
    mut predicate: impl FnMut() -> bool,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if predicate() {
                return;
            }
            tokio::time::sleep(interval).await;
        }
    })
    .await
    .is_ok()
}

/// Asserts that a predicate remains false for a bounded quiet window.
pub async fn assert_quiet(duration: Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + duration;
    loop {
        assert!(!predicate(), "quiet-window predicate became true");
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsumeCount, LiveFlag, ReleaseGate};

    #[test]
    fn guards_report_drop_and_consumption() {
        let (flag, guard) = LiveFlag::guarded();
        assert!(flag.is_live());
        drop(guard);
        assert!(!flag.is_live());

        let count = ConsumeCount::default();
        count.guard().consume();
        count.assert_once();
    }

    #[tokio::test]
    async fn release_gate_stores_one_permit() {
        let gate = ReleaseGate::default();
        gate.release();
        gate.wait().await;
    }
}
