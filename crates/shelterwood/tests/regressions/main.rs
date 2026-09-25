//! Single-purpose regression suites, folded into one target so the façade is
//! linked once rather than once per suite. `cargo nextest` still runs every
//! test in its own process, so abort-class regressions stay ordinary
//! `#[test]`s: an abort fails only the test that caused it.

#[path = "../common/mod.rs"]
mod common;

mod admission_removal_waker_proxy;
mod mailbox_future_drop_containment;
mod raw_teardown_result_containment;
mod reply_delivery_waker_proxy;
mod reply_waker_containment;
mod result_delivery_waker_proxy;
mod runtime_teardown_root_terminality;
mod system_wait_stale_reason;
mod timer_waker_proxy;
