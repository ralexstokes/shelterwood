// Known-bad fixture: grouped imports through the parent crate cannot hide a
// tree-layer root re-export from the scope-layer check.
use super::{System as RootSystem};

fn retain_system(system: RootSystem) {
    drop(system);
}
