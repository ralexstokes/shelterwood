// Known-good fixture: the word "downcast" without a method call must not
// match the exit-path pattern, which requires a receiver and an argument
// list or turbofish.
pub fn classify() {
    let downcast_hint = false;
    let _ = downcast_hint;
}
