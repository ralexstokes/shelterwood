// Known-bad fixture: a `self` alias nested inside another use group is still
// an alias of the crate root.
use crate::{{self as root}};
