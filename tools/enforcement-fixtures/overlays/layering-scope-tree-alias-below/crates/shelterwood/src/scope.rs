// Known-bad fixture: aliasing the tree module must not hide an upward
// dependency from the shared scope-handle layer.
use crate::tree as upper;

fn retain_system(system: upper::System) {
    drop(system);
}
