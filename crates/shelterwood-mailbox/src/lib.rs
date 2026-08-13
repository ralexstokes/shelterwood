#![allow(missing_docs, unreachable_pub)]

//! Mailbox state machines and public messaging primitives.
//!
//! Tokio details remain behind `shelterwood-runtime`; this crate names only
//! the adapter operations its futures need. Cross-crate lifecycle and identity
//! capabilities are public implementation seams, not supported façade API.

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
pub trait MailboxTermination: Send {
    fn finish(self: Box<Self>) -> Option<MailboxDisposal>;
}

/// Type-erased mailbox lifecycle surface owned by a member cell.
///
/// The driver must configure a mailbox before its first bind. Every live
/// incarnation must then be closed before a later incarnation is bound; if
/// close is skipped, messages accepted for the prior incarnation can leak
/// into the replacement. Once termination is prepared, later binds are
/// intentionally ignored.
pub trait MailboxControl: fmt::Debug + Send + Sync {
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

#[cfg(test)]
mod runtime {
    pub(crate) use tokio::{test, time::advance};
}
