use std::task::{Context, Waker};

use shelterwood_mailbox::ProxiedPoll as MailboxProxiedPoll;

use crate::{PanicAccumulator, discard_panic, dispose_detached};

/// Runtime-facing proxied-poll protocol for an external primitive.
///
/// The runtime-neutral implementation lives beside the mailbox state machines,
/// which already need the same lost-wake and post-unlock-effect guarantees.
/// Ready retirement is part of every poll there: a ready result may own a user
/// value (or a panic payload that owns one), so a hostile caller-waker
/// destructor is contained inline, subordinate to returning that result
/// intact. This wrapper supplies the runtime-owned venue for the other half —
/// drop glue transfers a still-registered caller waker to the blocking
/// disposal lane.
///
/// That venue is a per-seam adjudication, not a universal rule: the mailbox
/// reply receiver retires cancellation inline (`shelterwood-mailbox`'s
/// `DisposingReceiver`), accepting that a slow caller-waker destructor stalls
/// the abandoning holder alone. This wrapper backs the one-shot task and
/// blocking-offload seams — and, downstream, the public admission, removal,
/// and system-join futures — where cancellation is cold, so one disposal-lane
/// submission per abandoned pending registration buys drop glue that cannot
/// block or stall the holder's thread. A hot cancellation path added here
/// should revisit the reply receiver's ruling rather than inherit this one.
pub(crate) struct ProxiedPoll(MailboxProxiedPoll);

impl ProxiedPoll {
    pub(crate) fn new() -> Self {
        Self(MailboxProxiedPoll::new())
    }

    /// Probes, proxy-polls, and retires a ready caller registration inline.
    pub(crate) fn poll<T, R>(
        &mut self,
        target: &mut T,
        context: &mut Context<'_>,
        poll: impl FnMut(&mut T, &mut Context<'_>) -> R,
        is_pending: impl Fn(&R) -> bool,
    ) -> R {
        self.0.poll(target, context, poll, is_pending)
    }

    /// Transfers the stored caller waker to the blocking disposal lane.
    pub(crate) fn retire_detached(&mut self, panics: &mut PanicAccumulator) {
        self.0.retire_with(dispose_waker, panics);
    }
}

impl Drop for ProxiedPoll {
    /// Transfers any still-registered caller waker to detached disposal.
    ///
    /// Ordinary delivery retires inline before returning its value. Drop glue
    /// has no such safe handoff point: the surrounding frame may itself be
    /// unwinding or destroying user state, and a caller-waker destructor may
    /// block as well as panic.
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        self.retire_detached(&mut panics);
        discard_panic(panics.take());
    }
}

fn dispose_waker(waker: Waker) {
    dispose_detached(waker);
}
