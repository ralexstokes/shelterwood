// Known-bad fixture: glob access through a crate alias must not bypass the
// tree-root export restriction.
use crate as root;
use root::*;
