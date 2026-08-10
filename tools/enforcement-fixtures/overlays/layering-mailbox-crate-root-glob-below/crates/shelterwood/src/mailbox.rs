// Known-bad fixture: a crate-root glob exposes tree-layer exports in every
// below-driver module without naming them at the import.
use crate::*;

fn retain_export(value: DerivedTreeExport) {
    drop(value);
}
