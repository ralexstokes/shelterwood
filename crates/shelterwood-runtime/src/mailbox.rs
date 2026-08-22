use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Instant,
};

use shelterwood_core::{
    BoxedSleep, ErasedOneShotClose, ErasedOneShotReceiver, ErasedOneShotSender, ErasedValue,
    MailboxRuntime, MailboxSignal, MailboxSignalWatcher,
};

use crate::{
    OneShotClose, OneShotReceiver, OneShotSender, Signal, SignalWatcher, dispose_detached, now,
    oneshot, raw_sleep_until,
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
            raw_sleep_until,
        )
    }
}

struct TokioOneShotSender(OneShotSender<ErasedValue>);

impl ErasedOneShotSender for TokioOneShotSender {
    fn send(self: Box<Self>, value: ErasedValue) -> Result<(), ErasedValue> {
        self.0.send(value)
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

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, Wake, Waker},
        thread::ThreadId,
        time::Duration,
    };

    use shelterwood_core::{ErasedOneShotClose, ErasedOneShotReceiver, ErasedValue};

    use super::{TokioOneShotReceiver, mailbox_runtime};

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct RecordDrop(mpsc::Sender<ThreadId>);

    impl Drop for RecordDrop {
        fn drop(&mut self) {
            let _ = self.0.send(std::thread::current().id());
        }
    }

    #[test]
    fn adapter_oneshot_maps_value_sender_close_receiver_close_and_pending_send() {
        let runtime = mailbox_runtime();
        let mut context = Context::from_waker(Waker::noop());

        let (sender, mut receiver) = runtime.oneshot();
        sender
            .send(Box::new(7_u8))
            .unwrap_or_else(|_| panic!("the adapter receiver is live"));
        let Poll::Ready(Some(value)) = receiver.as_mut().poll_receive(&mut context) else {
            panic!("the adapter publishes the sent value")
        };
        assert_eq!(*value.downcast::<u8>().expect("value type is preserved"), 7);

        let (sender, mut receiver) = runtime.oneshot();
        drop(sender);
        assert!(matches!(
            receiver.as_mut().close_and_poll_receive(&mut context),
            ErasedOneShotClose::SenderClosed
        ));

        let (sender, mut receiver) = runtime.oneshot();
        assert!(matches!(
            receiver.as_mut().close_and_poll_receive(&mut context),
            ErasedOneShotClose::Empty
        ));
        assert!(sender.send(Box::new(8_u8)).is_err());

        let (sender, mut receiver) = runtime.oneshot();
        receiver.as_mut().close();
        assert!(sender.send(Box::new(9_u8)).is_err());

        let (sender, mut receiver) = runtime.oneshot();
        sender
            .send(Box::new(10_u8))
            .unwrap_or_else(|_| panic!("the adapter receiver is live"));
        let value = receiver
            .as_mut()
            .close_and_take()
            .expect("close-and-take recovers an already published value");
        assert_eq!(
            *value.downcast::<u8>().expect("value type is preserved"),
            10
        );

        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut context = Context::from_waker(&waker);
        let (sending, receiver) = crate::oneshot_sending_for_test::<ErasedValue>();
        let mut receiver = Box::pin(TokioOneShotReceiver(receiver));
        assert!(matches!(
            ErasedOneShotReceiver::close_and_poll_receive(receiver.as_mut(), &mut context),
            ErasedOneShotClose::Pending
        ));
        sending
            .publish(Box::new(11_u8))
            .unwrap_or_else(|_| panic!("the staged adapter receiver remains live"));
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        let ErasedOneShotClose::Value(value) =
            ErasedOneShotReceiver::close_and_poll_receive(receiver.as_mut(), &mut context)
        else {
            panic!("the adapter returns the value after staged publication")
        };
        assert_eq!(
            *value.downcast::<u8>().expect("value type is preserved"),
            11
        );
    }

    #[test]
    fn adapter_oneshot_rejects_poll_after_close_and_take_with_framework_diagnostic() {
        const REPOLL: &str = "shelterwood one-shot receiver polled after completion";

        let runtime = mailbox_runtime();
        let (sender, mut receiver) = runtime.oneshot();
        sender
            .send(Box::new(12_u8))
            .unwrap_or_else(|_| panic!("the adapter receiver is live"));
        let value = receiver
            .as_mut()
            .close_and_take()
            .expect("close-and-take recovers the published erased value");
        assert_eq!(
            *value.downcast::<u8>().expect("value type is preserved"),
            12
        );

        let mut context = Context::from_waker(Waker::noop());
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.as_mut().poll_receive(&mut context);
        }))
        .expect_err("an erased receiver cannot poll after terminal take");
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some(REPOLL)
        );
    }

    #[test]
    fn adapter_signal_wakes_an_already_parked_watcher() {
        let signal = mailbox_runtime().signal();
        let mut watcher = signal.watcher();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut changed = watcher.changed();

        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        signal.pulse();
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(
            changed
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[test]
    fn adapter_disposal_runs_on_an_isolated_thread() {
        let caller = std::thread::current().id();
        let (dropped, dropped_rx) = mpsc::channel();
        mailbox_runtime().dispose(Box::new(RecordDrop(dropped)));

        let destructor = dropped_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("adapter disposal destroys the value");
        assert_ne!(destructor, caller);
    }

    #[tokio::test(start_paused = true)]
    async fn adapter_clock_and_sleep_follow_the_runtime_clock() {
        let runtime = mailbox_runtime();
        let now = runtime.now();
        assert_eq!(now, crate::now());

        let mut bounded = runtime.sleep_until(Some(now + Duration::from_secs(1)));
        assert!(
            bounded
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            bounded
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );

        let mut absent = runtime.sleep_until(None);
        assert!(
            absent
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(
            absent
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
}
