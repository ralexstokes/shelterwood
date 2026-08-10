// Known-good nested driver fixture: method names that merely start with the
// forbidden token and identifiers that merely end with a layering token must
// remain accepted when enforcement scans driver submodules recursively.
pub fn route() {
    settings.downcast_settings(defaults);
    metrics.downcast_ref_count();
    subtree::route();
    resolver.resolve_commonality();
}
