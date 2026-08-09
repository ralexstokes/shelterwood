// Known-bad fixture: a direct tokio reference outside the runtime module
// must be flagged by the runtime-path check.
pub fn bind() {
    tokio::spawn(async {});
}
