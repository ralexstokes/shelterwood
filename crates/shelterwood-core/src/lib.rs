#![allow(missing_docs, unreachable_pub)]
// All `unsafe` in this crate is test-only raw-waker construction. `forbid`
// cannot be relaxed by a nested `allow`, so test builds `deny` instead and the
// test modules that build raw wakers opt in with `#[allow(unsafe_code)]`.
#![cfg_attr(not(test), forbid(unsafe_code))]
#![cfg_attr(test, deny(unsafe_code))]

//! Runtime-independent supervision types, waker proxies, and state machines.
//!
//! This implementation crate deliberately exposes protocol seams needed by
//! the façade and adapter crates. Those items are not part of the supported
//! `shelterwood` API, so this crate permits `unreachable_pub`; the public
//! façade retains the workspace's `unreachable_pub` lint.

pub mod deadline;
pub mod engine;
pub mod exit;
pub mod identity;
pub mod panic;
pub mod policy;
#[doc(hidden)]
pub mod proxied_sleep;
pub mod supervisor;
#[cfg(any(test, feature = "test-util"))]
pub mod test_support;
#[doc(hidden)]
pub mod waker;
mod waker_proxy;

pub use deadline::*;
pub use engine::{MembershipStatus, ScopeState};
pub use exit::*;
pub use identity::*;
pub use policy::*;
#[doc(hidden)]
pub use proxied_sleep::{BoxedSleep, ProxiedSleep};
#[doc(hidden)]
pub use waker_proxy::ProxiedPoll;
