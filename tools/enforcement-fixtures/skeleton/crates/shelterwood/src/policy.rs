// Known-good fixture: an identifier that merely ends in a forbidden crate
// name must not match the word-bounded runtime-path pattern.
pub fn sample() {
    not_fastrand::seed();
}
