// Known-good fixture: a method name that merely starts with "downcast_ref"
// must not match the exit-path pattern.
pub fn tally() {
    metrics.downcast_ref_count();
}
