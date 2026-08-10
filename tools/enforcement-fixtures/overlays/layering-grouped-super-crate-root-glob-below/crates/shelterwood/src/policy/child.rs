// Known-bad fixture: a `super` chain continued inside a use group can also
// end in a glob of the crate root.
use super::{super::*};
