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
pub(crate) use timing::{
    POLL_TIMEOUT, advance_time, assert_eventually_predicate, assert_quiet, poll_once, poll_until,
};

macro_rules! assert_eventually {
    ($predicate:expr $(,)?) => {
        $crate::common::assert_eventually_predicate(stringify!($predicate), $predicate, || None)
    };
    ($predicate:expr, $($context:tt)+) => {
        $crate::common::assert_eventually_predicate(
            stringify!($predicate),
            $predicate,
            || Some(format!($($context)+)),
        )
    };
}

pub(crate) use assert_eventually;
