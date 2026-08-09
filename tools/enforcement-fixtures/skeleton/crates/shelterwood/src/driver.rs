// Known-good fixture for the driver layer: near-misses for every pattern the
// exit-path and layering checks apply to this file.
pub fn shell() {
    // Method names that merely start with "downcast" are not runtime type
    // recovery and must not match the exit-path pattern.
    settings.downcast_settings(defaults);
    // "subtree" merely ends in "tree" and must not match the word-bounded
    // upward-reference pattern.
    subtree::route();
    // A name that merely extends the plan-funnel helper's name must not
    // match its word-bounded pattern.
    resolve_commonality();
}
