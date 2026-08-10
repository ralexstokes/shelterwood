// Known-good fixture: an identifier that merely ends in a forbidden crate
// name must not match the word-bounded runtime-path pattern.
use crate::{policy::System, policy::*};

pub fn sample() {
    not_fastrand::seed();
}

mod nested {
    // One `super` reaches only the containing policy module here, not the
    // crate root, even though the imported name shadows a tree export.
    use super::{System, *};
}
