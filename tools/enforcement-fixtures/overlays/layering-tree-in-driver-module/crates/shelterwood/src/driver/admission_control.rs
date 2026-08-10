// Known-bad fixture: nested driver modules remain part of the driver layer,
// so an upward tree reference here must be flagged by the layering check.
pub fn shell() {
    tree::Lowered::admit();
}
