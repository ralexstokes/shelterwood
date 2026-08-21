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

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use shelterwood_core::policy::ResolvedMailbox;
pub use shelterwood_core::{ChildId, Incarnation, Membership};

mod capability;
mod cell;
mod deadline;
mod errors;
mod futures;
mod reply;
#[cfg(test)]
mod test_support;
mod waker_proxy;

#[doc(hidden)]
pub use capability::{
    BoxedSleep, ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue,
    MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
};
pub use cell::*;
pub use errors::*;
pub use futures::*;
pub use reply::*;
#[doc(hidden)]
pub use waker_proxy::WakerProxy;

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

/// Linear permission to bind one mailbox incarnation.
///
/// Configuration mints the initial permission and a successful close returns
/// the next one. The token is intentionally neither `Clone` nor constructible
/// outside the mailbox crate.
#[must_use = "binding permission must be consumed by the next mailbox bind"]
pub struct MailboxBindToken {
    permit: Arc<AtomicBool>,
}

impl MailboxBindToken {
    pub(crate) fn new(permit: Arc<AtomicBool>) -> Self {
        Self { permit }
    }

    pub(crate) fn claim(&self, expected: &Arc<AtomicBool>) -> bool {
        Arc::ptr_eq(&self.permit, expected)
            && self
                .permit
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }
}

/// Result of successfully closing one incarnation.
///
/// The unread payload stays isolated until the caller has taken it apart.
/// Closing rides a non-empty effect batch — at minimum the change pulse that
/// wakes registered wakers synchronously — and callers hold this value across
/// that flush. If any effect panics, unwinding submits the payload for
/// detached disposal instead of destroying every unread user message on the
/// caller's stack, which for the driver is the driver task itself.
#[must_use = "closing returns both the next bind permission and unread payload disposal"]
pub struct MailboxClose {
    bind: Option<MailboxBindToken>,
    disposal: Option<MailboxDisposal>,
    runtime: Arc<dyn MailboxRuntime>,
}

impl MailboxClose {
    pub(crate) fn new(
        bind: MailboxBindToken,
        disposal: MailboxDisposal,
        runtime: Arc<dyn MailboxRuntime>,
    ) -> Self {
        Self {
            bind: Some(bind),
            disposal: Some(disposal),
            runtime,
        }
    }

    pub fn into_parts(mut self) -> (MailboxBindToken, MailboxDisposal) {
        let bind = self
            .bind
            .take()
            .expect("a mailbox close is taken apart exactly once");
        let disposal = self
            .disposal
            .take()
            .expect("a mailbox close is taken apart exactly once");
        (bind, disposal)
    }
}

impl Drop for MailboxClose {
    fn drop(&mut self) {
        if let Some(disposal) = self.disposal.take() {
            self.runtime.dispose(disposal);
        }
    }
}

/// Post-unlock destination for effects produced by mailbox control changes.
///
/// Control callers that hold a wider framework lock implement this trait on
/// that lock's transaction. Callers without a wider lock use
/// [`MailboxEffectQueue`], whose drop flushes every effect after the mailbox
/// operation has returned.
pub trait MailboxEffectSink {
    fn defer_mailbox_effect(&mut self, effect: Box<dyn FnOnce()>);
}

/// Immediate-scope mailbox effect sink for callers holding no wider lock.
#[must_use = "mailbox effects flush when the queue is dropped"]
#[derive(Default)]
pub struct MailboxEffectQueue(Vec<Box<dyn FnOnce()>>);

impl MailboxEffectSink for MailboxEffectQueue {
    fn defer_mailbox_effect(&mut self, effect: Box<dyn FnOnce()>) {
        self.0.push(effect);
    }
}

impl Drop for MailboxEffectQueue {
    fn drop(&mut self) {
        let mut panics = crate::panic::PanicAccumulator::default();
        for effect in self.0.drain(..) {
            panics.run(effect);
        }
    }
}

/// Prepared terminal mailbox transition. Finishing it wakes terminal waiters
/// before returning unread payload ownership for detached disposal.
///
/// # Implementation boundary
///
/// This trait is structurally sealed to Shelterwood's mailbox state machine.
/// It is public solely because sibling implementation crates retain it through
/// type erasure; it is not a user extension point.
pub trait MailboxTermination: private::SealedMailboxTermination + Send {
    #[must_use = "finishing hands back the unread payload for detached disposal"]
    fn finish(self: Box<Self>) -> MailboxDisposal;
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
    fn configure(
        &self,
        mailbox: ResolvedMailbox,
        effects: &mut dyn MailboxEffectSink,
    ) -> MailboxBindToken;
    /// Makes one incarnation live after configuration and prior-close cleanup.
    /// A bind after terminal preparation is deliberately ignored because
    /// terminality wins that race permanently.
    fn bind(
        &self,
        token: MailboxBindToken,
        incarnation: Incarnation,
        effects: &mut dyn MailboxEffectSink,
    );
    /// Stops new acceptance for the matching live incarnation.
    fn freeze(&self, incarnation: Incarnation, effects: &mut dyn MailboxEffectSink);
    /// Unbinds the matching incarnation and returns its unread payload.
    /// Every successful bind must be followed by this close before a rebind;
    /// skipping it would deliver the old incarnation's messages to the new.
    fn close(
        &self,
        incarnation: Incarnation,
        effects: &mut dyn MailboxEffectSink,
    ) -> Option<MailboxClose>;
    /// Irreversibly terminalizes the membership and prepares synchronous
    /// waiter completion followed by isolated unread-payload disposal.
    fn prepare_termination(
        &self,
        effects: &mut dyn MailboxEffectSink,
    ) -> Option<Box<dyn MailboxTermination>>;
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
