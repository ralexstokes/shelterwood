// Known-bad fixture: a nested file needs two `super` segments to reach the
// crate root, and that spelling must not bypass a derived tree export.
use super::super::DerivedTreeExport;
