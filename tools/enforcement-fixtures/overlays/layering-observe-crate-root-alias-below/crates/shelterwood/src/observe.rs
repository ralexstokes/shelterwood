// Known-bad fixture: aliasing the crate root must not hide a tree-layer export
// outside the scope module.
use crate as root;

fn retain_export(value: root::DerivedTreeExport) {
    drop(value);
}
