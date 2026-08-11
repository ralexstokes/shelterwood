//! Fail-closed completion storage used by the runtime shell.

/// An exactly-once synchronous completion.
///
/// The orderly path consumes the payload with [`Self::complete`]. If that
/// path is destroyed before doing so, dropping the obligation executes the
/// fail-closed fallback instead. Fallbacks must never await or join.
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
        self.discharge();
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
}
