// Known-bad fixture: an upward tree reference in the driver layer must be
// flagged by the layering check.
pub fn shell() {
    tree::Lowered::admit();
}
