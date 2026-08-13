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
    fn is_closed(&self) -> bool;
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

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.is_closed()
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

fn dispose_value<T: Send + 'static>(runtime: &dyn MailboxRuntime, value: T) {
    runtime.dispose(Box::new(value));
}

pub(crate) fn dispose<T: Send + 'static>(runtime: &Arc<dyn MailboxRuntime>, value: T) {
    dispose_value(runtime.as_ref(), value);
}

/// Receive state that keeps an unclaimed user value out of holder drop glue.
pub(crate) struct DisposingReceiver<T> {
    inner: OneShotReceiver<T>,
    runtime: Arc<dyn MailboxRuntime>,
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub(crate) fn new(inner: OneShotReceiver<T>, runtime: Arc<dyn MailboxRuntime>) -> Self {
        Self { inner, runtime }
    }
}

impl<T> DisposingReceiver<T> {
    pub(crate) fn runtime(&self) -> Arc<dyn MailboxRuntime> {
        Arc::clone(&self.runtime)
    }

    pub(crate) fn close(&mut self) {
        self.inner.close();
    }
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub(crate) fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner.poll_receive(context)
    }

    pub(crate) fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T> {
        self.inner.close_and_poll_receive(context)
    }
}

impl<T> Drop for DisposingReceiver<T> {
    fn drop(&mut self) {
        if let Some(value) = self.inner.close_and_take_erased() {
            self.runtime.dispose(value);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicU8, Ordering},
        },
        task::{Context, Poll},
        time::Instant,
    };

    use tokio::sync::{oneshot, watch};

    use super::{
        BoxedSleep, ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue,
        MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
    };

    const OPEN: u8 = 0;
    const SENDING: u8 = 1;
    const SENT: u8 = 2;
    const SENDER_CLOSED: u8 = 3;
    const RECEIVER_CLOSED: u8 = 4;

    struct TestSender {
        channel: Option<oneshot::Sender<ErasedValue>>,
        state: Arc<AtomicU8>,
    }

    impl ErasedOneShotSender for TestSender {
        fn send(mut self: Box<Self>, value: ErasedValue) -> Result<(), ErasedValue> {
            if self
                .state
                .compare_exchange(OPEN, SENDING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(value);
            }
            match self
                .channel
                .take()
                .expect("a live test sender retains its channel")
                .send(value)
            {
                Ok(()) => {
                    self.state.store(SENT, Ordering::Release);
                    Ok(())
                }
                Err(value) => {
                    self.state.store(RECEIVER_CLOSED, Ordering::Release);
                    Err(value)
                }
            }
        }

        fn is_closed(&self) -> bool {
            self.channel.as_ref().is_none_or(oneshot::Sender::is_closed)
        }
    }

    impl Drop for TestSender {
        fn drop(&mut self) {
            if self.channel.is_some() {
                let _ = self.state.compare_exchange(
                    OPEN,
                    SENDER_CLOSED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
        }
    }

    struct TestReceiver {
        channel: oneshot::Receiver<ErasedValue>,
        state: Arc<AtomicU8>,
    }

    impl ErasedOneShotReceiver for TestReceiver {
        fn poll_receive(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<ErasedValue>> {
            Pin::new(&mut self.channel).poll(context).map(Result::ok)
        }

        fn close_and_poll_receive(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> ErasedOneShotClose {
            match self.state.compare_exchange(
                OPEN,
                RECEIVER_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.channel.close();
                    ErasedOneShotClose::Empty
                }
                Err(SENDER_CLOSED) => ErasedOneShotClose::SenderClosed,
                Err(SENDING) | Err(SENT) => match Pin::new(&mut self.channel).poll(context) {
                    Poll::Ready(Ok(value)) => ErasedOneShotClose::Value(value),
                    Poll::Ready(Err(_)) => ErasedOneShotClose::SenderClosed,
                    Poll::Pending => ErasedOneShotClose::Pending,
                },
                Err(RECEIVER_CLOSED) => ErasedOneShotClose::Empty,
                Err(other) => unreachable!("unknown test one-shot state {other}"),
            }
        }

        fn close(mut self: Pin<&mut Self>) {
            let _ = self.state.compare_exchange(
                OPEN,
                RECEIVER_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            self.channel.close();
        }

        fn close_and_take(mut self: Pin<&mut Self>) -> Option<ErasedValue> {
            self.as_mut().close();
            self.channel.try_recv().ok()
        }
    }

    struct TestSignal(watch::Sender<()>);

    impl MailboxSignal for TestSignal {
        fn pulse(&self) {
            self.0.send_modify(|_| {});
        }

        fn watcher(&self) -> Box<dyn MailboxSignalWatcher> {
            Box::new(TestWatcher(self.0.subscribe()))
        }
    }

    struct TestWatcher(watch::Receiver<()>);

    impl MailboxSignalWatcher for TestWatcher {
        fn changed(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                let _ = self.0.changed().await;
            })
        }
    }

    #[derive(Debug)]
    struct TestRuntime;

    impl MailboxRuntime for TestRuntime {
        fn oneshot(
            &self,
        ) -> (
            Box<dyn ErasedOneShotSender>,
            Pin<Box<dyn ErasedOneShotReceiver>>,
        ) {
            let (sender, receiver) = oneshot::channel();
            let state = Arc::new(AtomicU8::new(OPEN));
            (
                Box::new(TestSender {
                    channel: Some(sender),
                    state: Arc::clone(&state),
                }),
                Box::pin(TestReceiver {
                    channel: receiver,
                    state,
                }),
            )
        }

        fn signal(&self) -> Arc<dyn MailboxSignal> {
            Arc::new(TestSignal(watch::channel(()).0))
        }

        fn dispose(&self, value: Box<dyn Send + 'static>) {
            let worker = std::thread::spawn(move || {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)));
            });
            drop(worker);
        }

        fn now(&self) -> Instant {
            tokio::time::Instant::now().into_std()
        }

        fn sleep_until(&self, deadline: Option<Instant>) -> BoxedSleep {
            Box::pin(async move {
                match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }
            })
        }
    }

    pub(crate) fn runtime() -> Arc<dyn MailboxRuntime> {
        Arc::new(TestRuntime)
    }
}
