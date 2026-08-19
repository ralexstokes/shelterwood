//! Panic containment and isolated disposal for a raw incarnation.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskPollContext, Poll},
};

use crate::runtime::{PanicAccumulator, PanicPayload, Signal, catch_panic, discard_panic};

/// Future panic boundary that owns and destroys its inner future.
///
/// Once the inner future returns `Ready`, it is destroyed before the output is
/// released. If that destruction panics, the already-produced output is
/// discarded and the destructor panic becomes this future's error.
pub(crate) struct CatchUnwindFuture<F> {
    future: Option<Pin<Box<F>>>,
}

impl<F> CatchUnwindFuture<F> {
    pub(crate) fn new(future: F) -> Self {
        Self {
            future: Some(Box::pin(future)),
        }
    }
}

impl<F: Future> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output, PanicPayload>;

    fn poll(self: Pin<&mut Self>, context: &mut TaskPollContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polled = catch_panic(|| {
            this.future
                .as_mut()
                .expect("a completed panic boundary was polled again")
                .as_mut()
                .poll(context)
        });
        match polled {
            Ok(Poll::Ready(value)) => {
                let future = this.future.take();
                match catch_panic(|| drop(future)) {
                    Ok(()) => Poll::Ready(Ok(value)),
                    Err(payload) => Poll::Ready(Err(payload)),
                }
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                let future = this.future.take();
                discard_panic(catch_panic(|| drop(future)).err());
                Poll::Ready(Err(payload))
            }
        }
    }
}

impl<F> Drop for CatchUnwindFuture<F> {
    fn drop(&mut self) {
        let future = self.future.take();
        let mut panics = PanicAccumulator::default();
        panics.run(|| drop(future));
    }
}

#[derive(Default)]
pub(super) struct PanicSlot {
    payload: Mutex<Option<PanicPayload>>,
}

impl PanicSlot {
    pub(super) fn record(&self, payload: PanicPayload) {
        let rejected = {
            let mut pending = self.payload.lock().expect("offload panic mutex poisoned");
            if pending.is_none() {
                *pending = Some(payload);
                None
            } else {
                Some(payload)
            }
        };
        discard_panic(rejected);
    }

    pub(super) fn take(&self) -> Option<PanicPayload> {
        self.payload
            .lock()
            .expect("offload panic mutex poisoned")
            .take()
    }
}

/// The one cleanup route for values owned by a raw incarnation.
///
/// Collection drains and offload futures route user payloads through this
/// funnel. Collections keep their resident elements raw and dispose each one
/// explicitly when draining, avoiding a cloned disposal handle per element.
/// A destructor panic is retained as cleanup evidence and wakes an idle actor;
/// it never unwinds through another user destructor.
#[derive(Clone)]
pub(super) struct RawDisposal {
    pub(super) panic: Arc<PanicSlot>,
    pub(super) signal: Signal,
}

/// Test-only: mints an orphan disposal whose panic slot and signal nothing
/// observes. Production wiring threads one shared disposal per incarnation
/// through the container constructors.
#[cfg(test)]
impl Default for RawDisposal {
    fn default() -> Self {
        Self {
            panic: Arc::new(PanicSlot::default()),
            signal: Signal::default(),
        }
    }
}

impl RawDisposal {
    pub(super) fn record(&self, payload: PanicPayload) {
        self.panic.record(payload);
        if let Err(payload) = catch_panic(|| self.signal.pulse()) {
            self.panic.record(payload);
        }
    }

    pub(super) fn dispose<T>(&self, value: T) {
        if let Err(payload) = catch_panic(|| drop(value)) {
            self.record(payload);
        }
    }
}

#[must_use = "contained user ownership must be consumed or disposed"]
pub(super) struct Contained<T> {
    value: Option<T>,
    disposal: RawDisposal,
}

impl<T> Contained<T> {
    pub(super) fn new(value: T, disposal: RawDisposal) -> Self {
        Self {
            value: Some(value),
            disposal,
        }
    }

    pub(super) fn get(&self) -> &T {
        self.value
            .as_ref()
            .expect("contained ownership is consumed once")
    }

    pub(super) fn into_inner(mut self) -> T {
        self.value
            .take()
            .expect("contained ownership is consumed once")
    }
}

impl<T> Drop for Contained<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            self.disposal.dispose(value);
        }
    }
}
