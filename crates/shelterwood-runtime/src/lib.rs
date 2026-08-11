#![allow(missing_docs, unreachable_pub)]

//! Tokio-backed runtime facilities used by Shelterwood's public façade.
//!
//! The broad exports are implementation seams for sibling crates. Runtime
//! types remain unreachable from the supported `shelterwood` public API.

mod disposal;
mod panic;
mod spawn;
mod sync;
mod timer;

pub use disposal::*;
pub use panic::*;
pub use shelterwood_core::exit::JoinVerdict as JoinOutcome;
pub use spawn::*;
pub use sync::*;
pub use timer::*;
pub use tokio::test;
