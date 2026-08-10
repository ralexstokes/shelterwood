// Known-bad fixture: grouped access through a crate alias must not bypass the
// tree-root export restriction.
use crate as root;
use root::{DerivedTreeExport, System};
