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
        let mut panics = crate::runtime::PanicAccumulator::default();
        panics.run(|| self.discharge());
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        process::Command,
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

    #[test]
    fn obligation_fallback_panic_is_contained_during_an_existing_unwind() {
        const CHILD_ENV: &str = "SHELTERWOOD_OBLIGATION_UNWIND_CHILD";
        const TEST_NAME: &str = "driver::storage::tests::obligation_fallback_panic_is_contained_during_an_existing_unwind";

        if std::env::var_os(CHILD_ENV).is_some() {
            let panic = catch_unwind(|| {
                let _obligation = Obligation::new((), panic_fallback);
                panic!("outer panic");
            })
            .expect_err("the original unwind reaches its boundary");
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"outer panic"));
            return;
        }

        let output = Command::new(std::env::current_exe().expect("unit-test executable"))
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_ENV, "1")
            .output()
            .expect("nested-unwind subprocess starts");

        assert!(
            output.status.success(),
            "nested-unwind subprocess must preserve the original panic instead of aborting\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
