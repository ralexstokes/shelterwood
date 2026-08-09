// Known-bad fixture: an upward tree reference below the driver layer must be
// flagged by the layering check.
pub fn graft() {
    tree::Snapshot::project();
}
