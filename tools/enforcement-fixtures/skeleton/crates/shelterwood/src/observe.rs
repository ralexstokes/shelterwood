// Known-good fixture: an identifier that merely ends in "tree" must not
// match the word-bounded upward-layering pattern below the driver layer.
pub fn graft() {
    subtree::graft();
}
