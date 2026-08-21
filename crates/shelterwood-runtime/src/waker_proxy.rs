use std::task::{Context, Waker};

use shelterwood_mailbox::ProxiedPoll as MailboxProxiedPoll;

use crate::{PanicAccumulator, discard_panic, dispose_detached};

/// Where a caller waker removed from the stable proxy is destroyed.
pub(crate) enum WakerRetirement {
    /// Destroy synchronously, under a caller-owned panic boundary.
    Inline,
    /// Transfer destruction to the runtime's blocking disposal lane.
    Detached,
}

/// Runtime-facing proxied-poll protocol for an external primitive.
///
/// The runtime-neutral implementation lives beside the mailbox state machines,
/// which already need the same lost-wake and post-unlock-effect guarantees.
/// This wrapper supplies the runtime-owned retirement venues and makes ready
/// retirement part of every poll rather than a tail each caller must repeat.
pub(crate) struct ProxiedPoll(MailboxProxiedPoll);

impl ProxiedPoll {
    pub(crate) fn new() -> Self {
        Self(MailboxProxiedPoll::new())
    }

    /// Probes, proxy-polls, and retires a ready caller registration.
    pub(crate) fn poll<T, R>(
        &mut self,
        target: &mut T,
        context: &mut Context<'_>,
        mut poll: impl FnMut(&mut T, &mut Context<'_>) -> R,
        is_pending: impl Fn(&R) -> bool,
        retirement: WakerRetirement,
    ) -> R {
        let result = self.0.poll(target, context, &mut poll, &is_pending);
        if !is_pending(&result) {
            let mut panics = PanicAccumulator::default();
            self.retire(retirement, &mut panics);
            discard_panic(panics.take());
        }
        result
    }

    /// Retires the stored caller waker through an explicit post-unlock venue.
    pub(crate) fn retire(&mut self, retirement: WakerRetirement, panics: &mut PanicAccumulator) {
        let effect = match retirement {
            WakerRetirement::Inline => drop_waker,
            WakerRetirement::Detached => dispose_waker,
        };
        self.0.retire_with(effect, panics);
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
        self.retire(WakerRetirement::Detached, &mut panics);
        discard_panic(panics.take());
    }
}

fn drop_waker(waker: Waker) {
    drop(waker);
}

fn dispose_waker(waker: Waker) {
    dispose_detached(waker);
}
