// Known-bad fixture: a consuming downcast on the exit path must be flagged
// by the exit-path check.
pub fn shell() {
    let _panic = payload.downcast::<Panic>();
}
