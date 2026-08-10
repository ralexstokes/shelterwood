use std::{
    fmt,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};

use tokio::sync::{Notify, broadcast, oneshot, watch};

use super::dispose_detached;

#[derive(Debug)]
pub(crate) struct Signal {
    inner: WatchSender<()>,
}

impl Default for Signal {
    fn default() -> Self {
        Self { inner: watch(()).0 }
    }
}

impl Clone for Signal {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Signal {
    pub(crate) fn pulse(&self) {
        self.inner.pulse();
    }

    pub(crate) fn watcher(&self) -> SignalWatcher {
        SignalWatcher {
            inner: self.inner.watcher(),
            _signal: self.clone(),
        }
    }

    #[cfg(test)]
    fn watcher_count(&self) -> usize {
        self.inner.receiver_count()
    }
}

pub(crate) struct SignalWatcher {
    inner: WatchReceiver<()>,
    // Retain the source through every watcher so channel closure cannot turn
    // into a spurious pulse.
    _signal: Signal,
}

impl SignalWatcher {
    pub(crate) async fn changed(&mut self) {
        self.inner.changed().await;
    }
}
/// A one-shot, multi-waiter signal backed by Tokio's waiter queue.
///
/// The atomic provides a linearizable, idempotent transition and retains the
/// fired state for future waiters. [`Notify`] wakes the waiters that already
/// exist and removes cancelled waits from its intrusive queue. Creating the
/// notification before rechecking the atomic is essential: Tokio guarantees
/// that such a notification observes a subsequent `notify_waiters`, even when
/// it has not been polled yet.
///
/// This deliberately does not use `tokio_util::sync::CancellationToken`.
/// Shelterwood also uses latches for readiness and completion, needs `fire` to
/// report which caller performed the transition, and keeps parent and local
/// cancellation as distinct shared latches. The Tokio-util cancellation tree
/// would add allocation, locking, and dependencies without replacing those
/// semantics. A Tokio watch channel similarly adds value locking to the hot
/// `is_fired` path.
#[derive(Clone, Debug, Default)]
pub(crate) struct Latch {
    state: Arc<LatchState>,
}

#[derive(Debug, Default)]
struct LatchState {
    fired: AtomicBool,
    notify: Notify,
}

impl Latch {
    pub(crate) fn fire(&self) -> bool {
        if self
            .state
            .fired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.state.notify.notify_waiters();
            true
        } else {
            false
        }
    }

    pub(crate) fn is_fired(&self) -> bool {
        self.state.fired.load(Ordering::Acquire)
    }

    pub(crate) async fn fired(&self) {
        if self.is_fired() {
            return;
        }

        let notified = self.state.notify.notified();
        if self.is_fired() {
            return;
        }
        notified.await;
        debug_assert!(self.is_fired());
    }
}

const COMPLETION_GATE_OPEN: u8 = 0;
const COMPLETION_GATE_FIRED: u8 = 1;
const COMPLETION_GATE_CLOSED: u8 = 2;
const COMPLETION_GATE_CLOSED_FIRED: u8 = 3;

/// A one-shot signal whose publication is linearized with a completion edge.
///
/// `fire` wins only while the gate is open. `complete` atomically closes the
/// gate and reports whether the signal won first, so a capability retained by
/// another task cannot publish after completion or disappear between a sample
/// and the completion notification.
#[derive(Clone, Debug)]
pub(crate) struct CompletionGatedLatch {
    state: Arc<AtomicU8>,
    fired: Latch,
    completed: Latch,
}

impl Default for CompletionGatedLatch {
    fn default() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(COMPLETION_GATE_OPEN)),
            fired: Latch::default(),
            completed: Latch::default(),
        }
    }
}

impl CompletionGatedLatch {
    pub(crate) fn fire(&self) -> bool {
        if self
            .state
            .compare_exchange(
                COMPLETION_GATE_OPEN,
                COMPLETION_GATE_FIRED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }
        let transitioned = self.fired.fire();
        debug_assert!(transitioned);
        true
    }

    pub(crate) fn is_fired(&self) -> bool {
        matches!(
            self.state.load(Ordering::Acquire),
            COMPLETION_GATE_FIRED | COMPLETION_GATE_CLOSED_FIRED
        )
    }

    pub(crate) async fn fired(&self) {
        self.fired.fired().await;
    }

    pub(crate) fn complete(&self) -> bool {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let (next, fired) = match current {
                COMPLETION_GATE_OPEN => (COMPLETION_GATE_CLOSED, false),
                COMPLETION_GATE_FIRED => (COMPLETION_GATE_CLOSED_FIRED, true),
                COMPLETION_GATE_CLOSED => return false,
                COMPLETION_GATE_CLOSED_FIRED => return true,
                _ => unreachable!("completion-gated latch state is valid"),
            };
            if self
                .state
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let transitioned = self.completed.fire();
                debug_assert!(transitioned);
                return fired;
            }
        }
    }

    pub(crate) async fn completed(&self) {
        self.completed.fired().await;
    }
}

const ONESHOT_OPEN: u8 = 0;
const ONESHOT_SENDING: u8 = 1;
const ONESHOT_SENT: u8 = 2;
const ONESHOT_SENDER_CLOSED: u8 = 3;
const ONESHOT_RECEIVER_CLOSED: u8 = 4;

/// Sending half of a runtime-backed single-delivery channel.
pub(crate) struct OneShotSender<T> {
    channel: Option<oneshot::Sender<T>>,
    state: Arc<AtomicU8>,
}

/// Receiving half of a runtime-backed single-delivery channel.
pub(crate) struct OneShotReceiver<T> {
    channel: oneshot::Receiver<T>,
    state: Arc<AtomicU8>,
}

/// Outcome after atomically closing a single-delivery receive side.
pub(crate) enum OneShotClose<T> {
    /// A value was sent before the receiver closed.
    Value(T),
    /// The sender was dropped before the receiver closed.
    SenderClosed,
    /// The receiver closed while the sender was still live and empty.
    Empty,
    /// The send transition won but has not published its value yet.
    Pending,
}

pub(crate) fn oneshot<T>() -> (OneShotSender<T>, OneShotReceiver<T>) {
    let (channel_sender, channel_receiver) = oneshot::channel();
    let state = Arc::new(AtomicU8::new(ONESHOT_OPEN));
    (
        OneShotSender {
            channel: Some(channel_sender),
            state: Arc::clone(&state),
        },
        OneShotReceiver {
            channel: channel_receiver,
            state,
        },
    )
}

impl<T> OneShotSender<T> {
    pub(crate) fn send(mut self, value: T) -> Result<(), T> {
        if self
            .state
            .compare_exchange(
                ONESHOT_OPEN,
                ONESHOT_SENDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(value);
        }
        let sender = self
            .channel
            .take()
            .expect("a live one-shot sender retains its channel");
        match sender.send(value) {
            Ok(()) => {
                self.state.store(ONESHOT_SENT, Ordering::Release);
                Ok(())
            }
            Err(value) => {
                self.state.store(ONESHOT_RECEIVER_CLOSED, Ordering::Release);
                Err(value)
            }
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.channel.as_ref().is_none_or(oneshot::Sender::is_closed)
    }
}

impl<T> Drop for OneShotSender<T> {
    fn drop(&mut self) {
        if self.channel.is_some() {
            let _ = self.state.compare_exchange(
                ONESHOT_OPEN,
                ONESHOT_SENDER_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}

impl<T> OneShotReceiver<T> {
    pub(crate) fn poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        Pin::new(&mut self.channel).poll(context).map(Result::ok)
    }

    /// Closes the receive side unless send or sender-drop won first.
    ///
    /// The shared transition word distinguishes sender-drop from receiver
    /// close, which Tokio's post-close `try_recv` result alone cannot do. A
    /// send that wins but is preempted before publishing returns `Pending`;
    /// the preceding channel poll registered the wake for its completion.
    pub(crate) fn close_and_poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> OneShotClose<T> {
        match self.state.compare_exchange(
            ONESHOT_OPEN,
            ONESHOT_RECEIVER_CLOSED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                self.channel.close();
                OneShotClose::Empty
            }
            Err(ONESHOT_SENDER_CLOSED) => OneShotClose::SenderClosed,
            Err(ONESHOT_SENDING) | Err(ONESHOT_SENT) => {
                match Pin::new(&mut self.channel).poll(context) {
                    std::task::Poll::Ready(Ok(value)) => OneShotClose::Value(value),
                    std::task::Poll::Ready(Err(_)) => OneShotClose::SenderClosed,
                    std::task::Poll::Pending => OneShotClose::Pending,
                }
            }
            Err(ONESHOT_RECEIVER_CLOSED) => OneShotClose::Empty,
            Err(other) => unreachable!("unknown one-shot transition state {other}"),
        }
    }

    pub(crate) fn close(&mut self) {
        let _ = self.state.compare_exchange(
            ONESHOT_OPEN,
            ONESHOT_RECEIVER_CLOSED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.channel.close();
    }

    /// Closes the receive side and recovers a value stored before the close.
    ///
    /// Tokio retains a value sent before `close`, so this is the cancellation
    /// hook that lets callers route an unclaimed stored value through isolated
    /// disposal instead of destroying it in their own drop glue.
    pub(crate) fn close_and_take(&mut self) -> Option<T> {
        self.close();
        self.channel.try_recv().ok()
    }

    pub(crate) async fn receive(self) -> Option<T> {
        self.channel.await.ok()
    }

    #[cfg(test)]
    pub(crate) fn try_receive(&mut self) -> Option<T> {
        self.channel.try_recv().ok()
    }
}

/// One-shot receive state that keeps user value destruction off framework and
/// holder drop glue.
///
/// A value stored before the receive side is cancelled is user-owned:
/// destroying it inline could block or panic whoever dropped the holder. The
/// disposal function is captured at construction, where the value type's
/// `Send + 'static` bounds hold, so unbounded holders can still route an
/// unclaimed stored value through isolated disposal on drop.
pub(crate) struct DisposingReceiver<T> {
    pub(crate) inner: OneShotReceiver<T>,
    dispose: fn(T),
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub(crate) fn new(inner: OneShotReceiver<T>) -> Self {
        Self {
            inner,
            dispose: dispose_detached::<T>,
        }
    }
}

impl<T> Drop for DisposingReceiver<T> {
    fn drop(&mut self) {
        if let Some(value) = self.inner.close_and_take() {
            (self.dispose)(value);
        }
    }
}

/// Publishing half of a runtime-backed conflating state channel.
pub(crate) struct WatchSender<T>(watch::Sender<T>);

/// Observing half of a runtime-backed conflating state channel.
pub(crate) struct WatchReceiver<T>(watch::Receiver<T>);

impl<T> Clone for WatchSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub(crate) fn watch<T>(initial: T) -> (WatchSender<T>, WatchReceiver<T>) {
    let (sender, receiver) = watch::channel(initial);
    (WatchSender(sender), WatchReceiver(receiver))
}

impl<T> WatchSender<T> {
    pub(crate) fn watcher(&self) -> WatchReceiver<T> {
        WatchReceiver(self.0.subscribe())
    }

    pub(crate) fn receiver_count(&self) -> usize {
        self.0.receiver_count()
    }

    pub(crate) fn pulse(&self) {
        self.0.send_modify(|_| {});
    }

    pub(crate) fn send_modify(&self, update: impl FnOnce(&mut T)) {
        self.0.send_modify(update);
    }

    /// The remaining production writers pulse or replace whole values; only
    /// test-side publication needs conditional notification.
    #[cfg(test)]
    pub(crate) fn send_if_modified(&self, update: impl FnOnce(&mut T) -> bool) -> bool {
        self.0.send_if_modified(update)
    }

    /// Mutates the retained value without advancing the watch version.
    ///
    /// This is only for compound publication that must finish another
    /// synchronous state transition before receivers are notified. The caller
    /// must follow a successful logical mutation with [`Self::pulse`].
    pub(crate) fn modify_silently(&self, update: impl FnOnce(&mut T)) {
        let notified = self.0.send_if_modified(|value| {
            update(value);
            false
        });
        debug_assert!(!notified);
    }
}

impl<T: Clone> WatchSender<T> {
    pub(crate) fn read_cloned(&self) -> T {
        self.0.borrow().clone()
    }
}

impl<T> Clone for WatchReceiver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> WatchReceiver<T> {
    /// Waits for a new value without treating publisher closure as a change.
    ///
    /// Callers that need to observe publisher closure must use
    /// [`Self::changed_or_closed`] instead. Parking here prevents a loop that
    /// intentionally ignores closure from becoming permanently always-ready.
    pub(crate) async fn changed(&mut self) {
        if self.0.changed().await.is_err() {
            std::future::pending().await
        }
    }

    pub(crate) async fn changed_or_closed(&mut self) -> bool {
        self.0.changed().await.is_ok()
    }
}

impl<T: Clone> WatchReceiver<T> {
    pub(crate) fn borrow_cloned(&self) -> T {
        self.0.borrow().clone()
    }

    pub(crate) fn borrow_and_update_cloned(&mut self) -> T {
        self.0.borrow_and_update().clone()
    }
}

impl<T: fmt::Debug> fmt::Debug for WatchSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WatchSender")
            .field("value", &*self.0.borrow())
            .field("receivers", &self.0.receiver_count())
            .finish()
    }
}

impl<T: fmt::Debug> fmt::Debug for WatchReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WatchReceiver")
            .field("value", &*self.0.borrow())
            .finish_non_exhaustive()
    }
}

/// Result of receiving from a runtime-backed broadcast channel.
pub(crate) enum BroadcastReceive<T> {
    Item(T),
    Empty,
    Closed,
    Lagged(u64),
}

/// Publishing half of a bounded runtime-backed broadcast channel.
pub(crate) struct BroadcastSender<T>(broadcast::Sender<T>);

/// Per-subscriber receiving half of a bounded runtime-backed broadcast channel.
pub(crate) struct BroadcastReceiver<T>(broadcast::Receiver<T>);

pub(crate) fn broadcast<T: Clone>(capacity: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let (sender, receiver) = broadcast::channel(capacity);
    (BroadcastSender(sender), BroadcastReceiver(receiver))
}

impl<T: Clone> BroadcastSender<T> {
    pub(crate) fn subscribe(&self) -> BroadcastReceiver<T> {
        BroadcastReceiver(self.0.subscribe())
    }

    pub(crate) fn send(&self, value: T) -> Result<usize, T> {
        self.0.send(value).map_err(|error| error.0)
    }

    pub(crate) fn receiver_count(&self) -> usize {
        self.0.receiver_count()
    }
}

impl<T: Clone> BroadcastReceiver<T> {
    pub(crate) fn try_receive(&mut self) -> BroadcastReceive<T> {
        match self.0.try_recv() {
            Ok(value) => BroadcastReceive::Item(value),
            Err(broadcast::error::TryRecvError::Empty) => BroadcastReceive::Empty,
            Err(broadcast::error::TryRecvError::Closed) => BroadcastReceive::Closed,
            Err(broadcast::error::TryRecvError::Lagged(dropped)) => {
                BroadcastReceive::Lagged(dropped)
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T> fmt::Debug for BroadcastSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BroadcastSender")
            .field("receivers", &self.0.receiver_count())
            .finish()
    }
}

impl<T> fmt::Debug for BroadcastReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BroadcastReceiver")
            .field("queued", &self.0.len())
            .finish()
    }
}
#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use crate::runtime::{
        CompletionGatedLatch, JoinOutcome, Latch, OneShotClose, Signal, Timeout, join, oneshot,
        spawn, timeout, yield_now,
    };

    #[test]
    fn closing_oneshot_distinguishes_value_sender_drop_and_receiver_win() {
        let mut context = Context::from_waker(Waker::noop());
        let (sender, mut receiver) = oneshot();
        sender.send(1_u8).expect("receiver is live");
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Value(1)
        ));

        let (sender, mut receiver) = oneshot::<u8>();
        drop(sender);
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::SenderClosed
        ));

        let (sender, mut receiver) = oneshot::<u8>();
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Empty
        ));
        assert_eq!(sender.send(1), Err(1));
    }

    #[test]
    fn quiet_signal_wait_cancellation_keeps_one_watch_registration() {
        let signal = Signal::default();
        let mut watcher = signal.watcher();
        assert_eq!(signal.watcher_count(), 1);

        for _ in 0..10_000 {
            let mut changed = Box::pin(watcher.changed());
            let mut context = Context::from_waker(Waker::noop());
            assert!(changed.as_mut().poll(&mut context).is_pending());
            drop(changed);
            assert_eq!(signal.watcher_count(), 1);
        }
    }

    #[test]
    fn closed_watch_change_parks_on_its_first_poll() {
        let (sender, mut receiver) = super::watch(());
        drop(sender);

        let mut changed = Box::pin(receiver.changed());
        let mut context = Context::from_waker(Waker::noop());
        assert!(changed.as_mut().poll(&mut context).is_pending());
        assert!(changed.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    fn closed_watch_remains_observable_when_requested() {
        let (sender, mut receiver) = super::watch(());
        drop(sender);

        let mut changed = Box::pin(receiver.changed_or_closed());
        let mut context = Context::from_waker(Waker::noop());
        assert_eq!(changed.as_mut().poll(&mut context), Poll::Ready(false));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exactly_one_concurrent_fire_performs_the_transition() {
        const FIRERS: usize = 32;

        let latch = Latch::default();
        let ready = Arc::new(AtomicUsize::new(0));
        let mut firers = Vec::with_capacity(FIRERS);
        for _ in 0..FIRERS {
            let latch = latch.clone();
            let ready = Arc::clone(&ready);
            firers.push(spawn(async move {
                ready.fetch_add(1, Ordering::AcqRel);
                while ready.load(Ordering::Acquire) != FIRERS {
                    yield_now().await;
                }
                latch.fire()
            }));
        }

        let mut transitions = 0;
        for firer in firers {
            let JoinOutcome::Ok { value } = join(firer).await else {
                panic!("latch firer must complete normally");
            };
            transitions += usize::from(value);
        }

        assert_eq!(transitions, 1);
        assert!(latch.is_fired());
        assert!(!latch.fire());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completion_gated_fire_and_completion_choose_one_order() {
        for _ in 0..256 {
            let latch = CompletionGatedLatch::default();
            let started = Arc::new(AtomicUsize::new(0));
            let fire = spawn({
                let latch = latch.clone();
                let started = Arc::clone(&started);
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    while started.load(Ordering::Acquire) != 2 {
                        yield_now().await;
                    }
                    latch.fire()
                }
            });
            let complete = spawn({
                let latch = latch.clone();
                let started = Arc::clone(&started);
                async move {
                    started.fetch_add(1, Ordering::AcqRel);
                    while started.load(Ordering::Acquire) != 2 {
                        yield_now().await;
                    }
                    latch.complete()
                }
            });

            let JoinOutcome::Ok { value: fired } = join(fire).await else {
                panic!("signal task must complete normally");
            };
            let JoinOutcome::Ok {
                value: completion_saw_fire,
            } = join(complete).await
            else {
                panic!("completion task must complete normally");
            };
            assert_eq!(fired, completion_saw_fire);
            assert_eq!(latch.is_fired(), completion_saw_fire);
            assert!(!latch.fire(), "completion closes later publication");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_pre_fire_waiters_wake() {
        const WAITERS: usize = 32;

        let latch = Latch::default();
        let parked = Arc::new(AtomicUsize::new(0));
        let mut waiters = Vec::with_capacity(WAITERS);
        for _ in 0..WAITERS {
            let latch = latch.clone();
            let parked = Arc::clone(&parked);
            waiters.push(spawn(async move {
                let mut fired = Box::pin(latch.fired());
                let first_poll =
                    std::future::poll_fn(|context| Poll::Ready(fired.as_mut().poll(context))).await;
                assert!(first_poll.is_pending());
                parked.fetch_add(1, Ordering::Release);
                fired.await;
            }));
        }

        while parked.load(Ordering::Acquire) != WAITERS {
            yield_now().await;
        }
        assert!(latch.fire());

        for waiter in waiters {
            let result = timeout(Duration::from_secs(1), join(waiter)).await;
            assert!(matches!(
                result,
                Timeout::Completed(JoinOutcome::Ok { value: () })
            ));
        }
    }

    #[tokio::test]
    async fn post_fire_waiters_complete_immediately() {
        let latch = Latch::default();
        assert!(latch.fire());

        assert!(matches!(
            timeout(Duration::from_secs(1), latch.fired()).await,
            Timeout::Completed(())
        ));
    }

    #[tokio::test]
    async fn cancelled_waits_do_not_consume_the_signal() {
        let latch = Latch::default();

        for _ in 0..1_024 {
            let mut fired = Box::pin(latch.fired());
            let first_poll =
                std::future::poll_fn(|context| Poll::Ready(fired.as_mut().poll(context))).await;
            assert!(first_poll.is_pending());
            drop(fired);
        }

        let mut live_waiter = Box::pin(latch.fired());
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(live_waiter.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());
        assert!(latch.fire());
        assert!(matches!(
            timeout(Duration::from_secs(1), live_waiter).await,
            Timeout::Completed(())
        ));
    }
}
