// Known-bad fixture: a direct fastrand reference outside the runtime module
// must be flagged by the runtime-path check.
pub fn sample() {
    let _jitter = fastrand::u64(..);
}
