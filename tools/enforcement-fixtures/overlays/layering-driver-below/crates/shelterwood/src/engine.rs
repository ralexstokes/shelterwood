// Known-bad fixture: an upward driver reference below the driver layer must
// be flagged by the layering check.
pub fn tally() {
    crate::driver::request_stop();
}
