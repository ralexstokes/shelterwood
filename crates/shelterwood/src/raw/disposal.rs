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
                    Err(payload) => {
                        discard_panic(catch_panic(|| drop(value)).err());
                        Poll::Ready(Err(payload))
                    }
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

    /// Restores a payload already established to precede anything that can
    /// have reached this slot in the meantime.
    ///
    /// Raw-resource freeze temporarily folds slot-retained disposal failures
    /// into its cleanup-wide accumulator. An offload task may publish another
    /// failure before that transaction installs its winner, so ordinary
    /// first-wins [`Self::record`] would invert the observed cleanup order.
    pub(super) fn restore_first(&self, payload: PanicPayload) {
        let displaced = {
            let mut pending = self.payload.lock().expect("offload panic mutex poisoned");
            pending.replace(payload)
        };
        discard_panic(displaced);
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

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::panic_any,
        pin::Pin,
        process::Command,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
    };

    use super::{CatchUnwindFuture, PanicPayload, discard_panic};

    struct PanickingOutput {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for PanickingOutput {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("injected output destructor panic");
        }
    }

    struct RecursivelyPanickingPayload;

    impl Drop for RecursivelyPanickingPayload {
        fn drop(&mut self) {
            panic_any(RecursivelyPanickingPayload);
        }
    }

    #[derive(Clone, Copy)]
    enum FutureDropPayload {
        Message,
        Recursive,
    }

    struct ReadyThenDropPanics {
        output: Option<PanickingOutput>,
        drops: Arc<AtomicUsize>,
        payload: FutureDropPayload,
    }

    impl Future for ReadyThenDropPanics {
        type Output = PanickingOutput;

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.output.take().expect("the test future is polled once"))
        }
    }

    impl Drop for ReadyThenDropPanics {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            match self.payload {
                FutureDropPayload::Message => panic!("injected future destructor panic"),
                FutureDropPayload::Recursive => panic_any(RecursivelyPanickingPayload),
            }
        }
    }

    struct PollThenDropPanics {
        drops: Arc<AtomicUsize>,
    }

    impl Future for PollThenDropPanics {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("injected poll panic");
        }
    }

    impl Drop for PollThenDropPanics {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("injected future destructor panic");
        }
    }

    fn ready_error<T>(polled: Poll<Result<T, PanicPayload>>) -> PanicPayload {
        match polled {
            Poll::Ready(Err(payload)) => payload,
            Poll::Ready(Ok(_)) => panic!("the hostile future unexpectedly succeeded"),
            Poll::Pending => panic!("the hostile future unexpectedly remained pending"),
        }
    }

    fn panic_message(payload: &PanicPayload) -> Option<&str> {
        payload
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    }

    #[test]
    fn completed_future_drop_panic_wins_and_output_drop_is_contained() {
        let future_drops = Arc::new(AtomicUsize::new(0));
        let output_drops = Arc::new(AtomicUsize::new(0));
        let mut boundary = Box::pin(CatchUnwindFuture::new(ReadyThenDropPanics {
            output: Some(PanickingOutput {
                drops: Arc::clone(&output_drops),
            }),
            drops: Arc::clone(&future_drops),
            payload: FutureDropPayload::Message,
        }));

        let payload = ready_error(
            boundary
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
        );
        assert_eq!(
            panic_message(&payload),
            Some("injected future destructor panic")
        );
        discard_panic(Some(payload));
        assert_eq!(future_drops.load(Ordering::SeqCst), 1);
        assert_eq!(output_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn poll_panic_wins_and_future_drop_panic_is_contained() {
        let future_drops = Arc::new(AtomicUsize::new(0));
        let mut boundary = Box::pin(CatchUnwindFuture::new(PollThenDropPanics {
            drops: Arc::clone(&future_drops),
        }));

        let payload = ready_error(
            boundary
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
        );
        assert_eq!(panic_message(&payload), Some("injected poll panic"));
        discard_panic(Some(payload));
        assert_eq!(future_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn hostile_output_cannot_abort_while_a_hostile_future_panic_is_retained() {
        const CHILD_ENV: &str = "SHELTERWOOD_CATCH_UNWIND_OUTPUT_CHILD";
        const TEST_NAME: &str = "raw::disposal::tests::hostile_output_cannot_abort_while_a_hostile_future_panic_is_retained";

        if std::env::var_os(CHILD_ENV).is_some() {
            let future_drops = Arc::new(AtomicUsize::new(0));
            let output_drops = Arc::new(AtomicUsize::new(0));
            let mut boundary = Box::pin(CatchUnwindFuture::new(ReadyThenDropPanics {
                output: Some(PanickingOutput {
                    drops: Arc::clone(&output_drops),
                }),
                drops: Arc::clone(&future_drops),
                payload: FutureDropPayload::Recursive,
            }));

            let payload = ready_error(
                boundary
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
            );
            discard_panic(Some(payload));
            assert_eq!(future_drops.load(Ordering::SeqCst), 1);
            assert_eq!(output_drops.load(Ordering::SeqCst), 1);
            return;
        }

        let output = Command::new(std::env::current_exe().expect("unit-test executable"))
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_ENV, "1")
            .output()
            .expect("hostile-output subprocess starts");

        assert!(
            output.status.success(),
            "hostile-output subprocess must return the retained panic instead of aborting\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
