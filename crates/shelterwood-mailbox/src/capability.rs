use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[cfg(test)]
use shelterwood_core::{BoxedSleep, MailboxSignal, MailboxSignalWatcher};
use shelterwood_core::{
    ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue, MailboxRuntime,
    ProxiedPoll,
    waker::{WakerAction, WakerEffects},
};

use crate::panic::PanicAccumulator;

fn downcast<T: Send + 'static>(value: ErasedValue) -> T {
    *value
        .downcast::<T>()
        .unwrap_or_else(|_| panic!("mailbox runtime returned a mismatched one-shot value"))
}

pub(crate) struct OneShotSender<T> {
    inner: Box<dyn ErasedOneShotSender>,
    marker: PhantomData<fn(T)>,
}

pub(crate) struct OneShotReceiver<T> {
    inner: Pin<Box<dyn ErasedOneShotReceiver>>,
    marker: PhantomData<fn(T)>,
}

pub(crate) enum OneShotClose<T> {
    Value(T),
    SenderClosed,
    Empty,
    Pending,
}

pub(crate) fn oneshot<T: Send + 'static>(
    runtime: &Arc<dyn MailboxRuntime>,
) -> (OneShotSender<T>, OneShotReceiver<T>) {
    let (sender, receiver) = runtime.oneshot();
    (
        OneShotSender {
            inner: sender,
            marker: PhantomData,
        },
        OneShotReceiver {
            inner: receiver,
            marker: PhantomData,
        },
    )
}

impl<T: Send + 'static> OneShotSender<T> {
    pub(crate) fn send(self, value: T) -> Result<(), T> {
        self.inner.send(Box::new(value)).map_err(downcast::<T>)
    }
}

impl<T: Send + 'static> OneShotReceiver<T> {
    pub(crate) fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner
            .as_mut()
            .poll_receive(context)
            .map(|value| value.map(downcast::<T>))
    }

    pub(crate) fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T> {
        match self.inner.as_mut().close_and_poll_receive(context) {
            ErasedOneShotClose::Value(value) => OneShotClose::Value(downcast(value)),
            ErasedOneShotClose::SenderClosed => OneShotClose::SenderClosed,
            ErasedOneShotClose::Empty => OneShotClose::Empty,
            ErasedOneShotClose::Pending => OneShotClose::Pending,
        }
    }
}

impl<T> OneShotReceiver<T> {
    pub(crate) fn close(&mut self) {
        self.inner.as_mut().close();
    }

    fn close_and_take_erased(&mut self) -> Option<ErasedValue> {
        self.inner.as_mut().close_and_take()
    }
}

pub(crate) fn dispose_value<T: Send + 'static>(runtime: &dyn MailboxRuntime, value: T) {
    runtime.dispose(Box::new(value));
}

pub(crate) fn dispose<T: Send + 'static>(runtime: &Arc<dyn MailboxRuntime>, value: T) {
    dispose_value(runtime.as_ref(), value);
}

/// Receive state that keeps an unclaimed user value out of holder drop glue.
pub(crate) struct DisposingReceiver<T> {
    inner: Option<OneShotReceiver<T>>,
    runtime: Arc<dyn MailboxRuntime>,
    reply_poll: ProxiedPoll,
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub(crate) fn new(inner: OneShotReceiver<T>, runtime: Arc<dyn MailboxRuntime>) -> Self {
        Self {
            inner: Some(inner),
            runtime,
            reply_poll: ProxiedPoll::new(),
        }
    }
}

impl<T> DisposingReceiver<T> {
    fn inner_mut(&mut self) -> &mut OneShotReceiver<T> {
        self.inner
            .as_mut()
            .expect("a live disposing receiver retains its channel")
    }

    pub(crate) fn runtime(&self) -> Arc<dyn MailboxRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn close(&mut self) {
        self.inner_mut().close();
        // Contained rather than re-raised, for the same reason as the delivery
        // seams: `close`'s callers can own a live user value while they call
        // it. `CallOperation::fail_send` holds the recovered `SendError<M>`
        // message in its frame, so letting a hostile caller-waker destructor
        // unwind out of here would put that panic in flight while the message
        // is still to be destroyed -- the double-panic abort this proxy exists
        // to remove.
        let mut panics = PanicAccumulator::default();
        self.retire_reply_waker(&mut panics);
        crate::panic::discard_panic(panics.take());
    }

    /// Takes the caller waker out of the proxy and queues its destructor into
    /// an effects sink, so it runs with no proxy mutex held.
    ///
    /// The delivery seams then *discard* whatever that destructor raises. Two
    /// costs ride on that, both accepted by #398 ruling 3:
    ///
    /// * The panic is swallowed with no diagnostic. A delivered user value has
    ///   to be returned by value, so there is no point after the handoff at
    ///   which a retained payload could be resumed -- resuming before it would
    ///   destroy the value the caller is owed. `shelterwood-core`'s `panic`
    ///   module holds that containment is false on a normal return path; this
    ///   is the deliberate exception to that guidance, not an oversight.
    /// * A caller-waker destructor that *blocks* stalls the delivering task
    ///   synchronously. The same ruling weighed a per-delivery disposal-lane
    ///   submission against it: delivery is the hot path of every successful
    ///   `call` and `recv`, and a lane submission there is real cost on every
    ///   reply, where a contained drop of a benign waker is nearly free.
    fn retire_reply_waker(&mut self, panics: &mut PanicAccumulator) {
        let mut effects = WakerEffects::default();
        self.reply_poll
            .retire(WakerAction::DropInline, &mut effects);
        effects.flush(panics);
    }
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub(crate) fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        // In pinned Tokio 1.53.1, `Receiver::poll` first obtains the result,
        // then clears its `Inner`; the last `Inner::drop` calls
        // `rx_task.drop_task` while that result can own the delivered value.
        // Probe with a framework waker, then leave only the proxy registered
        // across a pending return so Tokio never destroys a caller waker at
        // that seam.
        // A ready result may own a user value; `ProxiedPoll::poll` retires
        // the caller registration synchronously and contains any hostile
        // destructor panic before returning it. See `retire_reply_waker` for
        // the two costs that ride on the discard.
        self.reply_poll.poll(
            self.inner
                .as_mut()
                .expect("a live disposing receiver retains its channel"),
            context,
            OneShotReceiver::poll_receive,
            Poll::is_pending,
        )
    }

    pub(crate) fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T> {
        // Timeout arbitration can return a concurrently delivered user value,
        // so its ready edge uses the same synchronous contained retirement as
        // the ordinary delivery path, and accepts the same two costs.
        self.reply_poll.poll(
            self.inner
                .as_mut()
                .expect("a live disposing receiver retains its channel"),
            context,
            OneShotReceiver::close_and_poll_receive,
            |result| matches!(result, OneShotClose::Pending),
        )
    }
}

impl<T> Drop for DisposingReceiver<T> {
    fn drop(&mut self) {
        let mut inner = self
            .inner
            .take()
            .expect("a live disposing receiver retains its channel");
        let mut value = None;
        let mut panics = PanicAccumulator::default();
        // Cancellation never required the timer path's blocking-disposal
        // venue, which is a claim about venue only: closing a one-shot runs on
        // the receiver's own thread with no shared driver or wheel mutex held,
        // so a slow caller-waker destructor there stalls this future alone
        // rather than every timer registration in the process. It is not that
        // close touches no wakers -- in the pinned 1.53.1, `Inner::close`
        // wakes a set tx task and calls `rx_task.drop_task()` when the channel
        // is not yet complete, so pre-proxy this path did destroy a caller
        // waker inline. The old ruling that the venue argument justified
        // leaving the one-shot registration unproxied is superseded by #398:
        // reply polling registers a proxy uniformly because delivery, not
        // cancellation, is the abort-class seam, so the waker Tokio drops here
        // is now only ever a framework proxy clone. Cancellation inherits that
        // containment without retaining a special raw-waker path of its own.
        // Recovery runs first so an unclaimed value reaches isolated disposal
        // before the receiver -- and therefore before the waker clone it
        // registered -- is retired; a hostile waker destructor can neither
        // divert nor destroy it. If recovery itself unwinds, the value stays
        // in the channel and `inner`'s own drop glue destroys it inline
        // rather than through the isolated lane: accepted, because reaching
        // it requires a destructor that has already panicked, and the
        // alternative is retrying a step that just failed.
        panics.run(|| value = inner.close_and_take_erased());
        // `dispose` can fall back to destroying the value on this thread when
        // task and native-thread creation are exhausted, so submission belongs
        // inside the boundary too.
        panics.run(|| {
            if let Some(value) = value {
                self.runtime.dispose(value);
            }
        });
        panics.run(|| drop(inner));
        self.retire_reply_waker(&mut panics);
    }
}

/// The capability object this crate's own tests run against.
///
/// The binding is built only as delegation to the same adapter primitives
/// production uses: restating one-shot, signal, or clock semantics in a
/// hand-written double would let a divergence from the adapter read as a
/// passing test. The wrapper exists because focused mailbox tests need to
/// replace one capability while every other operation keeps the real adapter.
///
/// The dev-dependency does not weaken the inversion, which is a claim about
/// the production graph. Core itself retains no dev-dependencies.
#[cfg(test)]
pub(crate) mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::Instant,
    };

    use crate::runtime::{
        OneShotClose, OneShotReceiver, OneShotSender, Signal, SignalWatcher, dispose_detached, now,
        oneshot, raw_sleep_until,
    };

    use super::{
        BoxedSleep, ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue,
        MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
    };

    type ErasedOneShot = (
        Box<dyn ErasedOneShotSender>,
        Pin<Box<dyn ErasedOneShotReceiver>>,
    );

    type OneShotHook = dyn Fn() -> ErasedOneShot + Send + Sync;
    type NowHook = dyn Fn() -> Instant + Send + Sync;

    /// The one mailbox runtime used by this crate's tests. Optional hooks let
    /// a focused test control one capability while every other method keeps
    /// using the real runtime adapter primitives below.
    pub(crate) struct TestRuntime {
        oneshot: Option<Box<OneShotHook>>,
        now: Option<Box<NowHook>>,
    }

    impl TestRuntime {
        pub(crate) fn new() -> Self {
            Self {
                oneshot: None,
                now: None,
            }
        }

        pub(crate) fn with_oneshot(
            mut self,
            oneshot: impl Fn() -> ErasedOneShot + Send + Sync + 'static,
        ) -> Self {
            self.oneshot = Some(Box::new(oneshot));
            self
        }

        pub(crate) fn with_now(
            mut self,
            now: impl Fn() -> Instant + Send + Sync + 'static,
        ) -> Self {
            self.now = Some(Box::new(now));
            self
        }
    }

    impl MailboxRuntime for TestRuntime {
        fn oneshot(
            &self,
        ) -> (
            Box<dyn ErasedOneShotSender>,
            Pin<Box<dyn ErasedOneShotReceiver>>,
        ) {
            if let Some(oneshot) = &self.oneshot {
                return oneshot();
            }
            let (sender, receiver) = oneshot();
            (
                Box::new(AdapterOneShotSender(sender)),
                Box::pin(AdapterOneShotReceiver(receiver)),
            )
        }

        fn signal(&self) -> Arc<dyn MailboxSignal> {
            Arc::new(AdapterSignal(Signal::default()))
        }

        fn dispose(&self, value: Box<dyn Send + 'static>) {
            dispose_detached(value);
        }

        fn now(&self) -> Instant {
            self.now.as_ref().map_or_else(now, |now| now())
        }

        fn sleep_until(&self, deadline: Option<Instant>) -> BoxedSleep {
            deadline.map_or_else(
                || Box::pin(std::future::pending()) as BoxedSleep,
                raw_sleep_until,
            )
        }
    }

    struct AdapterOneShotSender(OneShotSender<ErasedValue>);

    impl ErasedOneShotSender for AdapterOneShotSender {
        fn send(self: Box<Self>, value: ErasedValue) -> Result<(), ErasedValue> {
            self.0.send(value)
        }
    }

    pub(crate) struct AdapterOneShotReceiver(OneShotReceiver<ErasedValue>);

    impl AdapterOneShotReceiver {
        pub(crate) fn new(receiver: OneShotReceiver<ErasedValue>) -> Self {
            Self(receiver)
        }
    }

    impl ErasedOneShotReceiver for AdapterOneShotReceiver {
        fn poll_receive(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<ErasedValue>> {
            self.0.poll_receive(context)
        }

        fn close_and_poll_receive(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> ErasedOneShotClose {
            match self.0.close_and_poll_receive(context) {
                OneShotClose::Value(value) => ErasedOneShotClose::Value(value),
                OneShotClose::SenderClosed => ErasedOneShotClose::SenderClosed,
                OneShotClose::Empty => ErasedOneShotClose::Empty,
                OneShotClose::Pending => ErasedOneShotClose::Pending,
            }
        }

        fn close(mut self: Pin<&mut Self>) {
            self.0.close();
        }

        fn close_and_take(mut self: Pin<&mut Self>) -> Option<ErasedValue> {
            self.0.close_and_take()
        }
    }

    struct AdapterSignal(Signal);

    impl MailboxSignal for AdapterSignal {
        fn pulse(&self) {
            self.0.pulse();
        }

        fn watcher(&self) -> Box<dyn MailboxSignalWatcher> {
            Box::new(AdapterSignalWatcher(self.0.watcher()))
        }
    }

    struct AdapterSignalWatcher(SignalWatcher);

    impl MailboxSignalWatcher for AdapterSignalWatcher {
        fn changed(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(self.0.changed())
        }
    }

    pub(crate) fn runtime() -> Arc<dyn MailboxRuntime> {
        Arc::new(TestRuntime::new())
    }
}
