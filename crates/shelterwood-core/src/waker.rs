use std::{sync::Arc, task::Waker};

use crate::{MailboxRuntime, panic::PanicAccumulator};

/// The only storage surface for a caller-owned waker.
///
/// Its value is private even from the rest of this crate, and every mutating
/// operation requires an effects sink, so replacing or taking an
/// `Option<Waker>` and accidentally dropping it beside a guard does not
/// type-check.
///
/// # Implementation boundary
///
/// `WakerSlot`, [`WakerAction`], and [`WakerEffects`] are doc-hidden
/// cross-crate seams for the façade's mailbox and proxy code, not user
/// extension points. A direct `shelterwood-core` dependent that constructs
/// them is outside the supported façade contract, and the supported façade
/// re-exports none of them.
#[derive(Default)]
#[doc(hidden)]
pub struct WakerSlot(Option<Waker>);

/// Post-unlock disposition for a waker leaving a [`WakerSlot`].
///
/// See [`WakerSlot`]'s implementation boundary.
#[doc(hidden)]
pub enum WakerAction {
    Wake,
    DropInline,
    Dispose(Arc<dyn MailboxRuntime>),
    Run(fn(Waker)),
}

struct WakerEffect {
    waker: Waker,
    action: WakerAction,
}

/// Deferred waker effects, flushed only with no framework mutex held.
///
/// See [`WakerSlot`]'s implementation boundary. `is_empty` is `pub` because
/// the façade's mailbox effect batches probe their collected sinks.
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
        self.0.push(WakerEffect { waker, action });
    }

    pub fn flush(&mut self, panics: &mut PanicAccumulator) {
        for WakerEffect { waker, action } in self.0.drain(..) {
            match action {
                WakerAction::Wake => panics.run(|| waker.wake()),
                WakerAction::DropInline => panics.run(|| drop(waker)),
                WakerAction::Dispose(runtime) => {
                    panics.run(|| runtime.dispose(Box::new(waker)));
                }
                WakerAction::Run(effect) => panics.run(|| effect(waker)),
            }
        }
    }
}

impl Drop for WakerEffects {
    fn drop(&mut self) {
        self.flush(&mut PanicAccumulator::default());
    }
}
