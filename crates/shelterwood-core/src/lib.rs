#![allow(missing_docs, unreachable_pub)]

//! Runtime-independent supervision types, capabilities, and state machines.
//!
//! This implementation crate deliberately exposes protocol seams needed by
//! the façade and adapter crates. Those items are not part of the supported
//! `shelterwood` API, so this crate permits `unreachable_pub`; the public
//! façade retains the workspace's `unreachable_pub` lint.

#[doc(hidden)]
pub mod capability;
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
#[doc(hidden)]
pub mod waker_proxy;

#[doc(hidden)]
pub use capability::*;
pub use deadline::*;
pub use engine::{MembershipStatus, ScopeState};
pub use exit::*;
pub use identity::*;
pub use policy::*;
#[doc(hidden)]
pub use proxied_sleep::ProxiedSleep;
#[doc(hidden)]
pub use waker_proxy::{ProxiedPoll, WakerProxy};
