//! Unwind payload capture, precedence, and resumption.
//!
//! Built entirely on `std::panic`, so it belongs to the runtime-neutral core
//! even though every caller reaches it while crossing a runtime boundary. The
//! adapter re-exports it as one of its own facilities.

use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
};

/// The framework's spelling for an unwind payload crossing a boundary.
pub type PanicPayload = Box<dyn Any + Send + 'static>;

/// Panic payloads crossing an unwind boundary, named by precedence.
pub struct UnwindPanics {
    pub primary: Option<PanicPayload>,
    pub cleanup: Option<PanicPayload>,
}

/// Catches application code without requiring every caller to repeat the
/// `AssertUnwindSafe` boundary vocabulary.
pub fn catch_panic<T>(operation: impl FnOnce() -> T) -> Result<T, PanicPayload> {
    catch_unwind(AssertUnwindSafe(operation))
}

/// Discards an optional panic payload without trusting its destructor.
pub fn discard_panic(payload: Option<PanicPayload>) {
    if let Some(payload) = payload
        && let Err(hostile_payload) = catch_panic(|| drop(payload))
    {
        // A payload whose own destructor panics cannot be dropped safely:
        // dropping the replacement payload would merely recurse outside the
        // boundary. Leak only this already-panicking diagnostic.
        std::mem::forget(hostile_payload);
    }
}

/// Resumes one captured panic payload at the framework boundary.
pub fn resume_panic(payload: PanicPayload) -> ! {
    resume_unwind(payload)
}

/// Retains the first panic and safely discards a later cleanup panic.
pub fn keep_first_panic(first: &mut Option<PanicPayload>, candidate: Option<PanicPayload>) {
    if first.is_none() {
        *first = candidate;
    } else {
        discard_panic(candidate);
    }
}

/// Resumes the primary panic, or the cleanup panic when there is no primary.
/// During an existing unwind both are contained to prevent a double panic.
///
/// Containment is only correct where losing the diagnostic is the lesser
/// outcome, which is true in a destructor and false on a normal return path.
/// Callers that own the sole surviving copy of an authoritative panic must use
/// [`resume_preferred_panic_outside_unwind`] instead.
pub fn resume_preferred_panic(panics: UnwindPanics) {
    let UnwindPanics { primary, cleanup } = panics;
    if std::thread::panicking() {
        discard_panic(primary);
        discard_panic(cleanup);
    } else if let Some(payload) = primary {
        discard_panic(cleanup);
        resume_panic(payload);
    } else if let Some(payload) = cleanup {
        resume_panic(payload);
    }
}

/// Resumes exactly as [`resume_preferred_panic`], but never contains the
/// payload.
///
/// This is the variant for call sites that are not destructors and have
/// already taken sole ownership of the panic. Silently discarding there would
/// erase the authoritative diagnostic and let the caller continue past a
/// failure it believes it re-raised, so the caller's non-unwinding precondition
/// is asserted rather than absorbed.
pub fn resume_preferred_panic_outside_unwind(panics: UnwindPanics) {
    let UnwindPanics { primary, cleanup } = panics;
    debug_assert!(
        !std::thread::panicking(),
        "an unwinding caller must contain its payloads with resume_preferred_panic"
    );
    if let Some(payload) = primary {
        discard_panic(cleanup);
        resume_panic(payload);
    } else if let Some(payload) = cleanup {
        resume_panic(payload);
    }
}

/// Collects independent cleanup panics while allowing every cleanup step to
/// run. Dropping the accumulator resumes the first panic unless another unwind
/// is already in progress; callers that need to defer that decision can
/// [`take`](Self::take) the payload.
#[derive(Default)]
pub struct PanicAccumulator {
    first: Option<PanicPayload>,
}

impl PanicAccumulator {
    pub fn run(&mut self, operation: impl FnOnce()) {
        self.record(catch_panic(operation).err());
    }

    pub fn record(&mut self, candidate: Option<PanicPayload>) {
        keep_first_panic(&mut self.first, candidate);
    }

    pub fn take(&mut self) -> Option<PanicPayload> {
        self.first.take()
    }
}

impl Drop for PanicAccumulator {
    fn drop(&mut self) {
        resume_preferred_panic(UnwindPanics {
            primary: None,
            cleanup: self.first.take(),
        });
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

    use super::{
        PanicAccumulator, PanicPayload, UnwindPanics, discard_panic, keep_first_panic,
        resume_preferred_panic, resume_preferred_panic_outside_unwind,
    };

    fn panic_message(payload: &PanicPayload) -> Option<&str> {
        payload
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    }

    struct RecursivelyPanickingPayload;

    impl Drop for RecursivelyPanickingPayload {
        fn drop(&mut self) {
            std::panic::panic_any(RecursivelyPanickingPayload);
        }
    }

    #[test]
    fn discarding_a_recursively_hostile_panic_payload_is_contained() {
        discard_panic(Some(Box::new(RecursivelyPanickingPayload)));
    }

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ResumeDuringUnwind {
        primary_drops: Arc<AtomicUsize>,
        cleanup_drops: Arc<AtomicUsize>,
    }

    impl Drop for ResumeDuringUnwind {
        fn drop(&mut self) {
            resume_preferred_panic(UnwindPanics {
                primary: Some(Box::new(DropCount(Arc::clone(&self.primary_drops)))),
                cleanup: Some(Box::new(DropCount(Arc::clone(&self.cleanup_drops)))),
            });
        }
    }

    #[test]
    fn preferred_panics_are_discarded_during_an_existing_unwind() {
        let primary_drops = Arc::new(AtomicUsize::new(0));
        let cleanup_drops = Arc::new(AtomicUsize::new(0));
        let payload = catch_unwind(AssertUnwindSafe({
            let primary_drops = Arc::clone(&primary_drops);
            let cleanup_drops = Arc::clone(&cleanup_drops);
            move || {
                let _resume = ResumeDuringUnwind {
                    primary_drops,
                    cleanup_drops,
                };
                std::panic::panic_any("outer panic");
            }
        }))
        .expect_err("the original unwind reaches its boundary");

        assert_eq!(payload.downcast_ref::<&str>(), Some(&"outer panic"));
        assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
        assert_eq!(cleanup_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn primary_panic_takes_precedence_over_cleanup_outside_an_unwind() {
        let payload = catch_unwind(AssertUnwindSafe(|| {
            resume_preferred_panic(UnwindPanics {
                primary: Some(Box::new("primary panic")),
                cleanup: Some(Box::new("cleanup panic")),
            });
        }))
        .expect_err("the primary panic is resumed");
        assert_eq!(panic_message(&payload), Some("primary panic"));
    }

    #[test]
    fn outside_unwind_resumption_preserves_primary_and_cleanup_precedence() {
        let primary = catch_unwind(AssertUnwindSafe(|| {
            resume_preferred_panic_outside_unwind(UnwindPanics {
                primary: Some(Box::new("primary panic")),
                cleanup: Some(Box::new("cleanup panic")),
            });
        }))
        .expect_err("the primary panic is resumed");
        assert_eq!(panic_message(&primary), Some("primary panic"));

        let cleanup = catch_unwind(AssertUnwindSafe(|| {
            resume_preferred_panic_outside_unwind(UnwindPanics {
                primary: None,
                cleanup: Some(Box::new("cleanup panic")),
            });
        }))
        .expect_err("cleanup stands in when there is no primary panic");
        assert_eq!(panic_message(&cleanup), Some("cleanup panic"));

        resume_preferred_panic_outside_unwind(UnwindPanics {
            primary: None,
            cleanup: None,
        });
    }

    struct TaggedPanic {
        tag: u8,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for TaggedPanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn tagged_panic(tag: u8, drops: &Arc<AtomicUsize>) -> PanicPayload {
        Box::new(TaggedPanic {
            tag,
            drops: Arc::clone(drops),
        })
    }

    #[test]
    fn keep_first_panic_retains_the_initial_candidate() {
        let first_drops = Arc::new(AtomicUsize::new(0));
        let second_drops = Arc::new(AtomicUsize::new(0));
        let mut retained = None;

        keep_first_panic(&mut retained, Some(tagged_panic(1, &first_drops)));
        keep_first_panic(&mut retained, Some(tagged_panic(2, &second_drops)));

        assert_eq!(
            retained
                .as_ref()
                .and_then(|payload| payload.downcast_ref::<TaggedPanic>())
                .map(|panic| panic.tag),
            Some(1)
        );
        assert_eq!(first_drops.load(Ordering::SeqCst), 0);
        assert_eq!(second_drops.load(Ordering::SeqCst), 1);
        drop(retained);
        assert_eq!(first_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn accumulator_drop_resumes_the_first_panic_after_discarding_later_ones() {
        let first_drops = Arc::new(AtomicUsize::new(0));
        let second_drops = Arc::new(AtomicUsize::new(0));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let mut panics = PanicAccumulator::default();
            panics.record(Some(tagged_panic(1, &first_drops)));
            panics.record(Some(tagged_panic(2, &second_drops)));
        }))
        .expect_err("dropping the accumulator resumes its first panic");

        assert_eq!(
            payload.downcast_ref::<TaggedPanic>().map(|panic| panic.tag),
            Some(1)
        );
        assert_eq!(first_drops.load(Ordering::SeqCst), 0);
        assert_eq!(second_drops.load(Ordering::SeqCst), 1);
        drop(payload);
        assert_eq!(first_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn taking_an_accumulated_panic_disarms_drop_resumption() {
        let drops = Arc::new(AtomicUsize::new(0));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let mut panics = PanicAccumulator::default();
            panics.record(Some(tagged_panic(7, &drops)));
            let payload = panics.take().expect("the first panic is retained");
            assert!(panics.take().is_none(), "take empties the accumulator");
            drop(panics);
            payload
        }))
        .expect("a taken panic is not resumed when the accumulator drops");

        assert_eq!(
            payload.downcast_ref::<TaggedPanic>().map(|panic| panic.tag),
            Some(7)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(payload);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
