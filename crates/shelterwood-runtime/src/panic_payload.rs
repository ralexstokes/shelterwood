use std::any::Any;

use super::{PanicPayload, discard_panic, dispose_detached};

pub(super) fn contain_panic_payload(payload: PanicPayload) -> Option<String> {
    // Inspection runs no user code: `downcast_ref` reads the compiler's
    // `Any::type_id`, and copying a `str` only allocates.
    let message = panic_message(payload.as_ref());
    // A custom panic payload is user-owned too. Its destructor may panic or
    // block, so retire it on the detached disposal lane before publishing
    // completion.
    dispose_detached(DiscardedPanicPayload(Some(payload)));
    message
}

/// Terminal, off-executor destruction for an opaque user panic payload.
///
/// The wrapper is load-bearing, not ceremony: submitting a bare payload to
/// [`dispose_detached`] closes a cycle. `DisposalJob::finish` contains a
/// destructor panic by calling [`contain_panic_payload`], which submits the
/// replacement payload for detached disposal, whose destructor panics again.
/// A self-regenerating payload — one whose `Drop` `panic_any`s a fresh copy
/// of itself — then spins the disposal lane for the life of the process,
/// firing the panic hook every iteration.
///
/// [`discard_panic`] is what breaks the cycle, and does so by construction:
/// it catches the replacement and `mem::forget`s it precisely because
/// dropping the replacement would recurse outside the boundary
/// (`shelterwood_core::panic`, pinned by that module's
/// `discarding_a_recursively_hostile_panic_payload_is_contained`). Running it
/// from this destructor keeps the venue guarantee — the payload is still
/// destroyed off the exit-publishing executor — while the disposal job
/// itself never observes a panic to classify.
///
/// Removing this wrapper reintroduces the unbounded spin; the regression is
/// pinned by `blocking_panic_payload_does_not_stall_current_thread_exit_publication`'s
/// sibling `a_self_regenerating_panic_payload_is_destroyed_a_bounded_number_of_times`.
struct DiscardedPanicPayload(Option<PanicPayload>);

impl Drop for DiscardedPanicPayload {
    fn drop(&mut self) {
        discard_panic(self.0.take());
    }
}

fn panic_message(payload: &(dyn Any + Send + 'static)) -> Option<String> {
    if let Some(message) = payload.downcast_ref::<&str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}
