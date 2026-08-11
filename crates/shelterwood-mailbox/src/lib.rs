#![allow(missing_docs, unreachable_pub)]

//! Mailbox state machines and public messaging primitives.
//!
//! Tokio details remain behind `shelterwood-runtime`; this crate names only
//! the adapter operations its futures need. Cross-crate lifecycle and identity
//! capabilities are public implementation seams, not supported façade API.

use std::{fmt, sync::Arc};

use shelterwood_core::policy::ResolvedMailbox;
pub use shelterwood_core::{ChildId, Incarnation, Membership};

mod cell;
mod deadline;
mod errors;
mod futures;
mod reply;

pub use cell::*;
pub use errors::*;
pub use futures::*;
pub use reply::*;

pub mod identity {
    pub use shelterwood_core::identity::*;
}

pub mod policy {
    pub use shelterwood_core::policy::*;
}

pub mod runtime {
    pub use shelterwood_runtime::*;
}

pub mod core_deadline {
    pub use shelterwood_core::deadline::*;
}

/// Isolated payload returned after mailbox termination has synchronously
/// published all waiter outcomes.
pub type MailboxDisposal = Box<dyn Send>;

/// Prepared terminal mailbox transition.
pub trait MailboxTermination: Send {
    fn finish(self: Box<Self>) -> Option<MailboxDisposal>;
}

/// Type-erased mailbox lifecycle surface owned by a member cell.
pub trait MailboxControl: fmt::Debug + Send + Sync {
    fn configure(&self, mailbox: ResolvedMailbox);
    fn bind(&self, incarnation: Incarnation);
    fn freeze(&self, incarnation: Incarnation);
    fn close(&self, incarnation: Incarnation) -> Option<MailboxDisposal>;
    fn prepare_termination(&self) -> Option<Box<dyn MailboxTermination>>;

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
