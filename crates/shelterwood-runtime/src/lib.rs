#![allow(missing_docs, unreachable_pub)]

//! Tokio-backed runtime facilities used by Shelterwood's public façade.
//!
//! The broad exports are implementation seams for sibling crates. Runtime
//! types remain unreachable from the supported `shelterwood` public API.

mod disposal;
mod mailbox;
mod spawn;
mod sync;
#[cfg(test)]
mod test_support;
mod timer;

pub use disposal::*;
pub use mailbox::*;
// Unwind handling is plain `std::panic`, so it lives in the runtime-neutral
// core. Re-exported here because the adapter's own modules and the façade
// reach it as a runtime facility.
pub use shelterwood_core::{exit::JoinVerdict as JoinOutcome, panic::*};
pub use spawn::*;
pub use sync::*;
pub use timer::*;
#[cfg(feature = "test-util")]
pub use tokio::test;
