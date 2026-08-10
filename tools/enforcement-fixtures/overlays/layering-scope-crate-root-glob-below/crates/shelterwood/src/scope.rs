// Known-bad fixture: a glob import of the crate root pulls every tree-layer
// re-export into the shared scope-handle layer without naming any of them.
use crate::*;

fn retain_system(system: System) {
    drop(system);
}
