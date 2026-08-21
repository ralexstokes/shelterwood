use std::task::{Context, Waker};

use shelterwood_mailbox::WakerProxy as MailboxWakerProxy;

use crate::{PanicAccumulator, discard_panic, dispose_detached};

/// Where a caller waker removed from the stable proxy is destroyed.
pub(crate) enum WakerRetirement {
    /// Destroy synchronously, under a caller-owned panic boundary.
    Inline,
    /// Transfer destruction to the runtime's blocking disposal lane.
    Detached,
}

/// Runtime-facing stable waker registered with an external primitive.
///
/// The runtime-neutral implementation lives beside the mailbox state machines,
/// which already need the same lost-wake and post-unlock-effect guarantees.
/// This wrapper supplies the runtime-owned retirement venues without exposing
/// caller wakers as values across the crate boundary.
pub(crate) struct WakerProxy(MailboxWakerProxy);

impl WakerProxy {
    pub(crate) fn new() -> Self {
        Self(MailboxWakerProxy::new())
    }

    pub(crate) fn register(&self, context: &Context<'_>) {
        self.0.register(context.waker());
    }

    pub(crate) fn waker(&self) -> &Waker {
        self.0.waker()
    }

    /// Retires the stored caller waker through an explicit post-unlock venue.
    pub(crate) fn retire(&self, retirement: WakerRetirement, panics: &mut PanicAccumulator) {
        let effect = match retirement {
            WakerRetirement::Inline => drop_waker,
            WakerRetirement::Detached => dispose_waker,
        };
        self.0.retire_with(effect, panics);
    }
}

impl Drop for WakerProxy {
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
