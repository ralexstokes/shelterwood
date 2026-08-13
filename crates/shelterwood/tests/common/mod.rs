register_test_modules!(
    "tests/common";
    mod gates;
    mod ownership;
    pub(crate) mod policy;
    mod timing;
    pub(crate) mod waiting;
);

pub(crate) use gates::{DestructorBlocker, DestructorGate, ReleaseGate};
pub(crate) use ownership::{ConsumeCount, ConsumeGuard, LiveFlag, PanicOnDrop};
pub(crate) use timing::{POLL_TIMEOUT, advance_time, assert_quiet, poll_once, poll_until};
