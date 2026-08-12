#![allow(missing_docs, unreachable_pub)]

//! Restart-stable state, cancellation, and observation projections.
//!
//! This implementation crate is structurally below Shelterwood's mutable
//! driver. Its broader public surface consists of cross-crate implementation
//! seams; the supported API remains the `shelterwood` façade.

mod admission;
mod cancellation;
// Load-bearing, not cosmetic: rustdoc omits hidden items from its JSON, so
// this is what keeps the restart-stable cell seam — whose signatures name
// runtime watch channels and latches — outside the API reachability walk that
// CI runs over this crate. What keeps that seam out of the *public* API is the
// façade's `pub(crate)` shim over it, which makes a public re-export a
// compile error rather than a check failure.
#[doc(hidden)]
mod cells;
mod observe;

pub use admission::*;
pub use cancellation::*;
#[doc(hidden)]
pub use cells::*;
pub use observe::*;

pub(crate) use shelterwood_core::{
    ChildId, Exit, Incarnation, Intensity, Membership, RestartAttempt, RestartCount, RestartPolicy,
    Retention, Strategy, TotalRestarts,
};

mod engine {
    pub(crate) use shelterwood_core::engine::*;
}

mod exit {
    pub(crate) use shelterwood_core::exit::*;
}

mod identity {
    pub(crate) use shelterwood_core::identity::*;
}

mod mailbox {
    pub(crate) use shelterwood_mailbox::*;
}

mod policy {
    pub(crate) use shelterwood_core::policy::*;
}

mod runtime {
    pub(crate) use shelterwood_runtime::*;
}
