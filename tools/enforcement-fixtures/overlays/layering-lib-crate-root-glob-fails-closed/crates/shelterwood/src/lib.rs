// Known-bad fixture: a glob of the crate root would re-import every tree
// export without naming it, so the derivation fails closed. rustc rejects
// this spelling too (E0432); the fixture pins the checker's own diagnostic.
mod actor;
mod tree;

pub use actor::Actor;
pub use tree::{OriginalTreeExport as DerivedTreeExport, System};

use crate as root;
pub use root::*;
