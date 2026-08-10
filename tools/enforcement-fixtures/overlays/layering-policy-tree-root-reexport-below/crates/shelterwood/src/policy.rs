// Known-bad fixture: a tree-layer export discovered only from lib.rs remains
// an upward dependency from every below-driver layer.
use crate::DerivedTreeExport;

fn retain_export(value: DerivedTreeExport) {
    drop(value);
}
