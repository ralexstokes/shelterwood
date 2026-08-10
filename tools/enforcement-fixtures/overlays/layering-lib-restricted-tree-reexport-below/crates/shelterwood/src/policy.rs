// Known-bad fixture: crate-visible imports in lib.rs remain visible to lower
// modules and therefore belong in the derived tree-root pattern.
use crate::InternalTreeExport;
