// Known-bad fixture: a borrowing downcast on the exit path must be flagged
// by the exit-path check, including the argument-inference form.
pub fn tally() {
    let _message = payload.downcast_ref(marker);
}
