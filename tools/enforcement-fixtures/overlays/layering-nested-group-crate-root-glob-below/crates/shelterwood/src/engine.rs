// Known-bad fixture: a glob nested inside another use group is still a glob
// import of the crate root.
use crate::{{*}};
