// Known-bad fixture: aliasing the crate root must not hide an upward
// dependency from the shared scope-handle layer.
use crate as root;

fn retain_system(system: root::System) {
    drop(system);
}
