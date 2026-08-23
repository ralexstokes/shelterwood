//! Membership-owned actor mailboxes and request/reply capabilities.
//!
//! Tokio details remain behind `shelterwood-runtime`; this module consumes the
//! runtime-neutral capability interface declared by `shelterwood-core`.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::identity::{ChildId, Incarnation, Membership};
use shelterwood_core::policy::ResolvedMailbox;
pub(crate) use shelterwood_core::{
    MailboxRuntime, MailboxSignal, MailboxSignalWatcher, ProxiedSleep,
};

mod capability;
mod cell;
mod deadline;
mod errors;
mod futures;
mod reply;
#[cfg(test)]
mod timer;
pub(crate) use cell::*;
pub use errors::*;
pub use futures::*;
pub use reply::*;

/// Isolated payload returned after mailbox termination has synchronously
/// published all waiter outcomes.
pub(crate) type MailboxDisposal = Box<dyn Send>;

/// Linear permission to bind one mailbox incarnation.
///
/// Configuration mints the initial permission and a successful close returns
/// the next one. The token is intentionally neither `Clone` nor constructible
/// outside the mailbox module.
#[must_use = "binding permission must be consumed by the next mailbox bind"]
pub(crate) struct MailboxBindToken {
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
pub(crate) struct MailboxClose {
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

    pub(crate) fn into_parts(mut self) -> (MailboxBindToken, MailboxDisposal) {
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
pub(crate) trait MailboxEffectSink {
    fn defer_mailbox_effect(&mut self, effect: Box<dyn FnOnce()>);
}

/// Immediate-scope mailbox effect sink for callers holding no wider lock.
#[must_use = "mailbox effects flush when the queue is dropped"]
#[derive(Default)]
pub(crate) struct MailboxEffectQueue(Vec<Box<dyn FnOnce()>>);

impl MailboxEffectSink for MailboxEffectQueue {
    fn defer_mailbox_effect(&mut self, effect: Box<dyn FnOnce()>) {
        self.0.push(effect);
    }
}

impl Drop for MailboxEffectQueue {
    fn drop(&mut self) {
        let mut panics = crate::runtime::PanicAccumulator::default();
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
/// This trait is private to Shelterwood's mailbox state machine and cannot be
/// implemented by an application.
pub(crate) trait MailboxTermination: Send {
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
/// This trait is private to the façade. Framework code can invoke it while
/// holding the member mailbox mutex, so its visibility is part of the
/// lock-rule boundary.
pub(crate) trait MailboxControl: fmt::Debug + Send + Sync {
    /// Installs the declaration-time mailbox policy before the first bind.
    /// Reconfiguration may only repeat the same resolved policy; a mismatch
    /// panics after the mailbox lock has been released. Tokens returned by
    /// repeated compatible configuration share one one-shot permit, so only
    /// the first token presented can bind; later aliases are rejected.
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
/// This trait is implemented only by Shelterwood's restart-stable member cell
/// and is not a user extension point.
pub(crate) trait ActorIdentity: Send + Sync {
    fn id(&self) -> &ChildId;
    fn membership(&self) -> Membership;
}
