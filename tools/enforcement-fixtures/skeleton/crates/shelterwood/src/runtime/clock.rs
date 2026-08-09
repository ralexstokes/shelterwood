// Known-good fixture: files under runtime/ share runtime.rs's exemption, so
// this reference must never be flagged.
pub async fn pause() {
    tokio::time::sleep(std::time::Duration::ZERO).await;
}
