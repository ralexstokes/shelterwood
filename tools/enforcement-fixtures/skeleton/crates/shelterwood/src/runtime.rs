// Known-good fixture: runtime.rs is the one file allowed to name the runtime
// and randomness crates, so these references must never be flagged.
pub fn spawn_supervised() {
    tokio::spawn(async {});
    let _token = tokio_util::sync::CancellationToken::new();
    let _seed = fastrand::u64(..);
}
