use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

/// An observable flag paired with an owned liveness guard.
#[derive(Clone, Debug)]
pub(crate) struct LiveFlag(Arc<AtomicBool>);

impl LiveFlag {
    /// Creates a live flag and the guard whose drop clears it.
    pub(crate) fn guarded() -> (Self, LiveGuard) {
        let value = Arc::new(AtomicBool::new(true));
        (Self(Arc::clone(&value)), LiveGuard(value))
    }

    /// Reports whether the paired guard is still live.
    pub(crate) fn is_live(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Clears its paired [`LiveFlag`] when dropped.
#[derive(Debug)]
pub(crate) struct LiveGuard(Arc<AtomicBool>);

impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Counts consumption and fallback drops of owned one-shot resources.
#[derive(Clone, Debug, Default)]
pub(crate) struct ConsumeCount(Arc<AtomicUsize>);

impl ConsumeCount {
    /// Creates a guard that increments this count when consumed or dropped.
    pub(crate) fn guard(&self) -> ConsumeGuard {
        ConsumeGuard(Some(Arc::clone(&self.0)))
    }

    /// Returns the number of observed consumptions.
    pub(crate) fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    /// Asserts that exactly one consumption occurred.
    #[track_caller]
    pub(crate) fn assert_once(&self) {
        assert_eq!(self.get(), 1, "resource must be consumed exactly once");
    }
}

/// A consume-once witness whose fallback effect runs on drop.
#[derive(Debug)]
pub(crate) struct ConsumeGuard(Option<Arc<AtomicUsize>>);

impl ConsumeGuard {
    /// Consumes the witness and records its effect immediately.
    pub(crate) fn consume(mut self) {
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

/// A fixture that panics with a chosen message when dropped.
#[derive(Debug)]
pub(crate) struct PanicOnDrop(&'static str);

impl PanicOnDrop {
    /// Creates a drop fixture with the supplied panic message.
    pub(crate) const fn new(message: &'static str) -> Self {
        Self(message)
    }
}

impl Default for PanicOnDrop {
    fn default() -> Self {
        Self::new("intentional destructor panic")
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("{}", self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsumeCount, LiveFlag};

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
}
