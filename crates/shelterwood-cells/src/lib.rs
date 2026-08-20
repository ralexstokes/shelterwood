#![allow(missing_docs, unreachable_pub)]

//! Restart-stable state, cancellation, and observation projections.
//!
//! This implementation crate is structurally below Shelterwood's mutable
//! driver. Its broader public surface consists of cross-crate implementation
//! seams; the supported API remains the `shelterwood` façade.
//!
//! Direct dependencies on this crate are unsupported. In particular,
//! [`DynamicRoute`] and the public methods that install it exist so the
//! downstream façade crate can complete the dependency inversion; they are
//! not user extension points. A foreign implementation may be called while a
//! framework observation mutex is held and therefore invalidates the
//! framework's lock-rule guarantees.

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
pub use cells::{
    DynamicRoute, MemberCell, MemberStage, MemberTransition, ObservationGate, ObservationTxn,
    ResidentProjection, RetainedExit, RetainedRecordedOutcome, RetainedStopReason, ScopeCell,
    ScopeControlEvent, StartupDisposition,
};
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub use cells::{GateCapture, RuntimeStorage};
pub use observe::*;
