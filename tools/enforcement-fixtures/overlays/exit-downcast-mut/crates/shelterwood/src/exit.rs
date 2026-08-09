// Known-bad fixture: a mutable downcast on the exit path must be flagged by
// the exit-path check, including the whitespace-split turbofish form.
pub fn classify() {
    let _failure = payload.downcast_mut ::<Failure>();
}
