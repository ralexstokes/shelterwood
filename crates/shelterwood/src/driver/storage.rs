//! Fail-closed completion storage used by the runtime shell.

use crate::runtime;

/// An exactly-once synchronous completion.
///
/// The orderly path consumes the payload with [`Self::complete`]. If that
/// path is destroyed before doing so, dropping the obligation executes the
/// fail-closed fallback instead. Fallbacks must never await or join.
///
/// Every fallback reaches code the framework did not schedule — a one-shot
/// send wakes the caller's waker inline, and terminality publication runs
/// through the resident tree — so destruction contains it. A fallback panic
/// resumes from an ordinary drop, keeping the diagnostic authoritative. A
/// fallback panic raised while the obligation is destroyed during an existing
/// unwind is discarded instead: losing that diagnostic is the lesser outcome
/// against the double panic that would abort the process. Discharging the
/// obligation explicitly ([`Self::discharge`]) contains nothing, so a caller
/// on a destruction path supplies its own accumulator.
#[must_use = "dropping an obligation executes its fallback completion"]
pub(super) struct Obligation<T> {
    payload: Option<T>,
    fallback: fn(T),
}

impl<T> Obligation<T> {
    pub(super) fn new(payload: T, fallback: fn(T)) -> Self {
        Self {
            payload: Some(payload),
            fallback,
        }
    }

    pub(super) fn payload_mut(&mut self) -> &mut T {
        self.payload
            .as_mut()
            .expect("a completed obligation has no payload")
    }

    pub(super) fn complete(&mut self, completion: impl FnOnce(T)) {
        if let Some(payload) = self.payload.take() {
            completion(payload);
        }
    }

    pub(super) fn discharge(&mut self) {
        if let Some(payload) = self.payload.take() {
            (self.fallback)(payload);
        }
    }
}

impl<T> Drop for Obligation<T> {
    fn drop(&mut self) {
        let mut panics = runtime::PanicAccumulator::default();
        panics.run(|| self.discharge());
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::Obligation;

    fn count_fallback(fallbacks: Arc<AtomicUsize>) {
        fallbacks.fetch_add(1, Ordering::SeqCst);
    }

    fn panic_after_counting(fallbacks: Arc<AtomicUsize>) {
        fallbacks.fetch_add(1, Ordering::SeqCst);
        panic!("injected fallback panic");
    }

    fn panic_fallback((): ()) {
        panic!("injected fallback panic");
    }

    #[test]
    fn obligation_completes_or_falls_back_exactly_once() {
        let fallbacks = Arc::new(AtomicUsize::new(0));
        let mut completed = false;
        let mut orderly = Obligation::new(Arc::clone(&fallbacks), count_fallback);
        orderly.complete(|_| completed = true);
        drop(orderly);
        assert!(completed);
        assert_eq!(fallbacks.load(Ordering::SeqCst), 0);

        drop(Obligation::new(Arc::clone(&fallbacks), count_fallback));
        assert_eq!(fallbacks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn panicking_fallback_consumes_the_payload_before_unwinding() {
        let fallbacks = Arc::new(AtomicUsize::new(0));
        let mut obligation = Obligation::new(Arc::clone(&fallbacks), panic_after_counting);
        assert!(
            catch_unwind(AssertUnwindSafe(|| obligation.discharge())).is_err(),
            "the injected fallback panic reaches the caller"
        );
        drop(obligation);
        assert_eq!(
            fallbacks.load(Ordering::SeqCst),
            1,
            "drop cannot repeat a fallback that panicked"
        );
    }

    /// Runs in process because every runner that reaches this test isolates
    /// it in one: `just test` and the authoritative Nix check both invoke
    /// `cargo nextest run` for `--lib --bins --tests --examples`, and nextest
    /// executes each test as its own process. A regressed containment
    /// therefore aborts that process, which the runner reports as this test
    /// failing on SIGABRT. No in-process assertion can observe an abort, so
    /// the assertions below pin the other half instead: that the *original*
    /// panic, not the fallback's, is what reaches the boundary.
    #[test]
    fn obligation_fallback_panic_is_contained_during_an_existing_unwind() {
        let panic = catch_unwind(|| {
            let _obligation = Obligation::new((), panic_fallback);
            panic!("outer panic");
        })
        .expect_err("the original unwind reaches its boundary");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"outer panic"));
    }
}
