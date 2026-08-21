// Every integration-test crate imports this module independently and uses a
// different subset of its fixtures.
#![allow(dead_code, unused_imports, unused_macros)]

mod gates;
mod lifecycle;
mod ownership;
pub(crate) mod policy;
mod recorder;
mod startup;
mod timing;
pub(crate) mod waiting;
mod waker;

pub(crate) use gates::{DestructorBlocker, DestructorGate, ReleaseGate};
pub(crate) use lifecycle::{last_panic_message, next_event, next_item};
pub(crate) use ownership::{ConsumeCount, ConsumeGuard, LiveFlag, PanicOnDrop};
pub(crate) use recorder::{GatedRecorder, MessageRecorder};
pub(crate) use startup::startup_failed_child;
pub(crate) use timing::{
    POLL_TIMEOUT, advance_time, assert_eventually_predicate, assert_quiet, poll_once, poll_until,
    poll_until_ready,
};
pub(crate) use waker::hostile_waker;

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
