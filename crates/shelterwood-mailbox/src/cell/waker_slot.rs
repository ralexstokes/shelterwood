use std::{sync::Arc, task::Waker};

use crate::{MailboxRuntime, capability::dispose, panic::PanicAccumulator};

/// The only storage surface for a caller-owned waker.
///
/// Its value is private even from the parent mailbox module, and every
/// mutating operation requires an effects sink, so replacing or taking an
/// `Option<Waker>` and accidentally dropping it beside a guard does not
/// type-check.
#[derive(Default)]
pub(crate) struct WakerSlot(Option<Waker>);

pub(crate) enum WakerAction {
    Wake,
    DropInline,
    Dispose(Arc<dyn MailboxRuntime>),
}

enum WakerEffect {
    Wake(Waker),
    DropInline(Waker),
    Dispose(Arc<dyn MailboxRuntime>, Waker),
}

#[derive(Default)]
pub(crate) struct WakerEffects(Vec<WakerEffect>);

impl WakerSlot {
    pub(crate) fn will_wake(&self, waker: &Waker) -> bool {
        self.0
            .as_ref()
            .is_some_and(|registered| registered.will_wake(waker))
    }

    pub(crate) fn replace(&mut self, waker: Waker, effects: &mut WakerEffects) {
        if let Some(displaced) = self.0.replace(waker) {
            effects.push(displaced, WakerAction::DropInline);
        }
    }

    pub(crate) fn take(&mut self, action: WakerAction, effects: &mut WakerEffects) {
        if let Some(waker) = self.0.take() {
            effects.push(waker, action);
        }
    }
}

impl WakerEffects {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn push(&mut self, waker: Waker, action: WakerAction) {
        self.0.push(match action {
            WakerAction::Wake => WakerEffect::Wake(waker),
            WakerAction::DropInline => WakerEffect::DropInline(waker),
            WakerAction::Dispose(runtime) => WakerEffect::Dispose(runtime, waker),
        });
    }

    pub(crate) fn flush(&mut self, panics: &mut PanicAccumulator) {
        for effect in self.0.drain(..) {
            match effect {
                WakerEffect::Wake(waker) => panics.run(|| waker.wake()),
                WakerEffect::DropInline(waker) => panics.run(|| drop(waker)),
                WakerEffect::Dispose(runtime, waker) => {
                    panics.run(|| dispose(&runtime, waker));
                }
            }
        }
    }
}

impl Drop for WakerEffects {
    fn drop(&mut self) {
        self.flush(&mut PanicAccumulator::default());
    }
}
