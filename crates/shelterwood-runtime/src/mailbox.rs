use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Instant,
};

use shelterwood_mailbox::{
    BoxedSleep, ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue,
    MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
};

use crate::{
    OneShotClose, OneShotReceiver, OneShotSender, Signal, SignalWatcher, dispose_detached, now,
    oneshot, sleep_until,
};

struct TokioMailboxRuntime;

/// Returns the Tokio capability object installed into façade-owned mailboxes.
pub fn mailbox_runtime() -> Arc<dyn MailboxRuntime> {
    static RUNTIME: OnceLock<Arc<dyn MailboxRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(|| Arc::new(TokioMailboxRuntime)))
}

impl MailboxRuntime for TokioMailboxRuntime {
    fn oneshot(
        &self,
    ) -> (
        Box<dyn ErasedOneShotSender>,
        Pin<Box<dyn ErasedOneShotReceiver>>,
    ) {
        let (sender, receiver) = oneshot();
        (
            Box::new(TokioOneShotSender(sender)),
            Box::pin(TokioOneShotReceiver(receiver)),
        )
    }

    fn signal(&self) -> Arc<dyn MailboxSignal> {
        Arc::new(TokioSignal(Signal::default()))
    }

    fn dispose(&self, value: Box<dyn Send + 'static>) {
        dispose_detached(value);
    }

    fn now(&self) -> Instant {
        now()
    }

    fn sleep_until(&self, deadline: Option<Instant>) -> BoxedSleep {
        deadline.map_or_else(
            || Box::pin(std::future::pending()) as BoxedSleep,
            sleep_until,
        )
    }
}

struct TokioOneShotSender(OneShotSender<ErasedValue>);

impl ErasedOneShotSender for TokioOneShotSender {
    fn send(self: Box<Self>, value: ErasedValue) -> Result<(), ErasedValue> {
        self.0.send(value)
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

struct TokioOneShotReceiver(OneShotReceiver<ErasedValue>);

impl ErasedOneShotReceiver for TokioOneShotReceiver {
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

struct TokioSignal(Signal);

impl MailboxSignal for TokioSignal {
    fn pulse(&self) {
        self.0.pulse();
    }

    fn watcher(&self) -> Box<dyn MailboxSignalWatcher> {
        Box::new(TokioSignalWatcher(self.0.watcher()))
    }
}

struct TokioSignalWatcher(SignalWatcher);

impl MailboxSignalWatcher for TokioSignalWatcher {
    fn changed(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.0.changed())
    }
}
