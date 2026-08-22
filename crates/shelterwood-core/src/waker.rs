use std::{sync::Arc, task::Waker};

use crate::{MailboxRuntime, panic::PanicAccumulator};

/// The only storage surface for a caller-owned waker.
///
/// Its value is private even from the parent mailbox module, and every
/// mutating operation requires an effects sink, so replacing or taking an
/// `Option<Waker>` and accidentally dropping it beside a guard does not
/// type-check.
#[derive(Default)]
#[doc(hidden)]
pub struct WakerSlot(Option<Waker>);

#[doc(hidden)]
pub enum WakerAction {
    Wake,
    DropInline,
    Dispose(Arc<dyn MailboxRuntime>),
    Run(fn(Waker)),
}

enum WakerEffect {
    Wake(Waker),
    DropInline(Waker),
    Dispose(Arc<dyn MailboxRuntime>, Waker),
    Run(fn(Waker), Waker),
}

#[derive(Default)]
#[doc(hidden)]
pub struct WakerEffects(Vec<WakerEffect>);

impl WakerSlot {
    pub fn will_wake(&self, waker: &Waker) -> bool {
        self.0
            .as_ref()
            .is_some_and(|registered| registered.will_wake(waker))
    }

    pub fn replace(&mut self, waker: Waker, effects: &mut WakerEffects) {
        if let Some(displaced) = self.0.replace(waker) {
            effects.push(displaced, WakerAction::DropInline);
        }
    }

    pub fn take(&mut self, action: WakerAction, effects: &mut WakerEffects) {
        if let Some(waker) = self.0.take() {
            effects.push(waker, action);
        }
    }
}

impl WakerEffects {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn push(&mut self, waker: Waker, action: WakerAction) {
        self.0.push(match action {
            WakerAction::Wake => WakerEffect::Wake(waker),
            WakerAction::DropInline => WakerEffect::DropInline(waker),
            WakerAction::Dispose(runtime) => WakerEffect::Dispose(runtime, waker),
            WakerAction::Run(effect) => WakerEffect::Run(effect, waker),
        });
    }

    pub fn flush(&mut self, panics: &mut PanicAccumulator) {
        for effect in self.0.drain(..) {
            match effect {
                WakerEffect::Wake(waker) => panics.run(|| waker.wake()),
                WakerEffect::DropInline(waker) => panics.run(|| drop(waker)),
                WakerEffect::Dispose(runtime, waker) => {
                    panics.run(|| runtime.dispose(Box::new(waker)));
                }
                WakerEffect::Run(effect, waker) => panics.run(|| effect(waker)),
            }
        }
    }
}

impl Drop for WakerEffects {
    fn drop(&mut self) {
        self.flush(&mut PanicAccumulator::default());
    }
}
