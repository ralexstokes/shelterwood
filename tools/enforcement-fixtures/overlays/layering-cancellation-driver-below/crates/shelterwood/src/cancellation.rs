// Known-bad fixture: the shared cancellation layer must not reference the
// driver above it.
pub fn request_stop() {
    crate::driver::request_stop();
}
