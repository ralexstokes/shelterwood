// Known-bad fixture: a direct tokio_util reference outside the runtime
// module must be flagged by the runtime-path check.
pub fn tighten() {
    let _token = tokio_util::sync::CancellationToken::new();
}
