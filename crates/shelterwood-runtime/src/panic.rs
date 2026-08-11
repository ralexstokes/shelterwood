use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
};

/// Runtime-owned spelling for an unwind payload crossing a framework boundary.
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
    use super::discard_panic;

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
}
