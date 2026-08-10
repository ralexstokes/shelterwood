// Known-bad fixture: `extern crate self` is another crate-root alias spelling
// and must be rejected where the alias is declared.
extern crate self as root;
use root::DerivedTreeExport;
