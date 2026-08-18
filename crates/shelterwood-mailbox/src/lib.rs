#![allow(missing_docs, unreachable_pub)]

//! Mailbox state machines and public messaging primitives.
//!
//! Tokio details remain behind `shelterwood-runtime`: this crate declares the
//! runtime capabilities its futures need and never names an executor, in tests
//! as well as in production. Cross-crate lifecycle and identity capabilities
//! are public implementation seams, not supported façade API.
//!
//! Direct dependencies on this crate are unsupported. Its public capability
//! traits and their installation helpers exist only so sibling Shelterwood
//! crates can complete the dependency inversion. [`MailboxControl`] and
//! [`MailboxTermination`] are structurally sealed because their implementations
//! live here. The remaining cross-crate traits are not user extension points:
//! foreign implementations may be called from framework critical sections and
//! therefore invalidate the framework's lock-rule guarantees.

use std::{fmt, sync::Arc};

use shelterwood_core::policy::ResolvedMailbox;
pub use shelterwood_core::{ChildId, Incarnation, Membership};

mod capability;
mod cell;
mod deadline;
mod errors;
mod futures;
mod reply;

#[doc(hidden)]
pub use capability::{
    BoxedSleep, ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue,
    MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
};
pub use cell::*;
pub use errors::*;
pub use futures::*;
pub use reply::*;

mod identity {
    pub(crate) use shelterwood_core::identity::*;
}

mod private {
    pub trait SealedMailboxControl {}
    pub trait SealedMailboxTermination {}
}

mod panic {
    pub(crate) use shelterwood_core::panic::*;
}

mod policy {
    pub(crate) use shelterwood_core::policy::*;
}

/// Isolated payload returned after mailbox termination has synchronously
/// published all waiter outcomes.
pub type MailboxDisposal = Box<dyn Send>;

/// Prepared terminal mailbox transition. Finishing it wakes terminal waiters
/// before returning unread payload ownership for detached disposal.
///
/// # Implementation boundary
///
/// This trait is structurally sealed to Shelterwood's mailbox state machine.
/// It is public solely because sibling implementation crates retain it through
/// type erasure; it is not a user extension point.
pub trait MailboxTermination: private::SealedMailboxTermination + Send {
    fn finish(self: Box<Self>) -> Option<MailboxDisposal>;
}

/// Type-erased mailbox lifecycle surface owned by a member cell.
///
/// The driver must configure a mailbox before its first bind. Every live
/// incarnation must then be closed before a later incarnation is bound; if
/// close is skipped, messages accepted for the prior incarnation can leak
/// into the replacement. Once termination is prepared, later binds are
/// intentionally ignored.
///
/// # Implementation boundary
///
/// This trait is structurally sealed to Shelterwood's mailbox state machine.
/// Framework code can invoke it while holding the member mailbox mutex, so
/// preventing foreign implementations is part of the lock-rule boundary.
pub trait MailboxControl: private::SealedMailboxControl + fmt::Debug + Send + Sync {
    /// Installs the declaration-time mailbox policy before the first bind.
    /// Reconfiguration may only repeat the same resolved policy; a mismatch
    /// panics after the mailbox lock has been released.
    fn configure(&self, mailbox: ResolvedMailbox);
    /// Makes one incarnation live after configuration and prior-close cleanup.
    /// A bind after terminal preparation is deliberately ignored because
    /// terminality wins that race permanently.
    fn bind(&self, incarnation: Incarnation);
    /// Stops new acceptance for the matching live incarnation.
    fn freeze(&self, incarnation: Incarnation);
    /// Unbinds the matching incarnation and returns its unread payload.
    /// Every successful bind must be followed by this close before a rebind;
    /// skipping it would deliver the old incarnation's messages to the new.
    fn close(&self, incarnation: Incarnation) -> Option<MailboxDisposal>;
    /// Irreversibly terminalizes the membership and prepares synchronous
    /// waiter completion followed by isolated unread-payload disposal.
    fn prepare_termination(&self) -> Option<Box<dyn MailboxTermination>>;

    /// Debug-only check for the driver's configure/close-before-bind contract.
    #[cfg(debug_assertions)]
    fn bind_order_valid(&self) -> bool;
}

/// Restart-stable identity capability retained by an actor handle.
///
/// # Implementation boundary
///
/// This trait is implemented only by Shelterwood's restart-stable member
/// cell. It is public solely to bridge the mailbox and cell crates and is not
/// a user extension point.
pub trait ActorIdentity: Send + Sync {
    fn id(&self) -> &ChildId;
    fn membership(&self) -> Membership;
}

impl<T: ActorIdentity + ?Sized> ActorIdentity for Arc<T> {
    fn id(&self) -> &ChildId {
        (**self).id()
    }

    fn membership(&self) -> Membership {
        (**self).membership()
    }
}

/// The Tokio adapter, reachable only as a dev-dependency.
///
/// `shelterwood-runtime` depends on this crate, so this is a dev-only cycle
/// that leaves the production graph inverted (`cargo tree -e normal` shows
/// `shelterwood-core` alone). Tests take their capability object and their
/// runtime attribute from the real adapter rather than a stand-in, keeping
/// tokio unnamed here.
#[cfg(test)]
mod runtime {
    pub(crate) use shelterwood_runtime::*;
}
