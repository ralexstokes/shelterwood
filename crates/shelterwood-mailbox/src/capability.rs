use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

pub type BoxedSleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type ErasedValue = Box<dyn Any + Send + 'static>;

/// Runtime-neutral single-delivery send capability.
#[doc(hidden)]
pub trait ErasedOneShotSender: Send {
    fn send(self: Box<Self>, value: ErasedValue) -> Result<(), ErasedValue>;
}

/// Runtime-neutral single-delivery receive capability.
#[doc(hidden)]
pub trait ErasedOneShotReceiver: Send {
    fn poll_receive(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<ErasedValue>>;
    fn close_and_poll_receive(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> ErasedOneShotClose;
    fn close(self: Pin<&mut Self>);
    fn close_and_take(self: Pin<&mut Self>) -> Option<ErasedValue>;
}

#[doc(hidden)]
pub enum ErasedOneShotClose {
    Value(ErasedValue),
    SenderClosed,
    Empty,
    Pending,
}

/// Runtime-neutral one-shot change notification.
#[doc(hidden)]
pub trait MailboxSignal: Send + Sync {
    fn pulse(&self);
    fn watcher(&self) -> Box<dyn MailboxSignalWatcher>;
}

/// Runtime-neutral wait side of [`MailboxSignal`].
#[doc(hidden)]
pub trait MailboxSignalWatcher: Send {
    fn changed(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// The four runtime capabilities needed by the mailbox shell.
///
/// The public façade installs one object per mailbox. Type erasure keeps the
/// adapter out of `ActorRef`'s type parameters while this crate remains free of
/// Tokio and every other concrete executor.
///
/// # Implementation boundary
///
/// This is an implementation seam for Shelterwood's runtime-adapter crate,
/// not a user-supplied executor interface. Foreign implementations and direct
/// construction of mailbox cells are outside the supported façade contract.
#[doc(hidden)]
pub trait MailboxRuntime: Send + Sync {
    fn oneshot(
        &self,
    ) -> (
        Box<dyn ErasedOneShotSender>,
        Pin<Box<dyn ErasedOneShotReceiver>>,
    );
    fn signal(&self) -> Arc<dyn MailboxSignal>;
    fn dispose(&self, value: Box<dyn Send + 'static>);
    fn now(&self) -> Instant;
    fn sleep_until(&self, deadline: Option<Instant>) -> BoxedSleep;
}

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
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub(crate) fn new(inner: OneShotReceiver<T>, runtime: Arc<dyn MailboxRuntime>) -> Self {
        Self {
            inner: Some(inner),
            runtime,
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
    }
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub(crate) fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner_mut().poll_receive(context)
    }

    pub(crate) fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T> {
        self.inner_mut().close_and_poll_receive(context)
    }
}

impl<T> Drop for DisposingReceiver<T> {
    fn drop(&mut self) {
        let mut inner = self
            .inner
            .take()
            .expect("a live disposing receiver retains its channel");
        let mut value = None;
        let mut panics = crate::panic::PanicAccumulator::default();
        panics.run(|| value = inner.close_and_take_erased());
        if let Some(value) = value {
            self.runtime.dispose(value);
        }
        panics.run(|| drop(inner));
    }
}

/// The capability object this crate's own tests run against.
///
/// `shelterwood-runtime` depends on this crate, so its `mailbox_runtime()`
/// implements the trait belonging to the *non-test* build of this crate and
/// cannot satisfy the `cfg(test)` one. The binding is therefore rebuilt here,
/// but only as delegation to the same adapter primitives production uses:
/// restating one-shot, signal, or clock semantics in a hand-written double
/// would let a divergence from the adapter read as a passing test.
///
/// The dev-dependency does not weaken the inversion, which is a claim about
/// the production graph — `cargo tree -p shelterwood-mailbox -e normal`.
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
        oneshot, sleep_until,
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
                sleep_until,
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
