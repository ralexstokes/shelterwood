use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use tokio::sync::{broadcast, oneshot};

use super::{PanicAccumulator, dispose_detached};

/// A caller-waker registry whose lock protects only inert storage changes.
///
/// Registration clones happen before entering the registry. Removal and
/// draining only move wakers out; their vtables run after unlock, one behind
/// each accumulator boundary. The opaque identity is retained by its waiter,
/// so its `Arc` traffic under the lock cannot destroy even framework data.
#[derive(Default)]
struct WaiterRegistry {
    waiters: Mutex<Vec<RegisteredWaker>>,
}

struct RegisteredWaker {
    identity: Arc<()>,
    waker: Waker,
}

impl WaiterRegistry {
    fn register(&self, identity: &Arc<()>, waker: Waker) -> Option<RegisteredWaker> {
        let mut waiters = self
            .waiters
            .lock()
            .expect("waiter registry lock is never held across caller code");
        if let Some(index) = waiters
            .iter()
            .position(|waiter| Arc::ptr_eq(&waiter.identity, identity))
        {
            Some(std::mem::replace(
                &mut waiters[index],
                RegisteredWaker {
                    identity: Arc::clone(identity),
                    waker,
                },
            ))
        } else {
            waiters.push(RegisteredWaker {
                identity: Arc::clone(identity),
                waker,
            });
            None
        }
    }

    fn remove(&self, identity: &Arc<()>) -> Option<RegisteredWaker> {
        let mut waiters = self
            .waiters
            .lock()
            .expect("waiter registry lock is never held across caller code");
        waiters
            .iter()
            .position(|waiter| Arc::ptr_eq(&waiter.identity, identity))
            .map(|index| waiters.swap_remove(index))
    }

    fn wake_all(&self) {
        let waiters = {
            let mut waiters = self
                .waiters
                .lock()
                .expect("waiter registry lock is never held across caller code");
            std::mem::take(&mut *waiters)
        };
        let mut panics = PanicAccumulator::default();
        for RegisteredWaker { waker, .. } in waiters {
            panics.run(|| waker.wake());
        }
    }

    fn drop_registered(waiters: impl IntoIterator<Item = Option<RegisteredWaker>>) {
        let mut panics = PanicAccumulator::default();
        for waiter in waiters.into_iter().flatten() {
            panics.run(|| drop(waiter));
        }
    }

    fn len(&self) -> usize {
        self.waiters
            .lock()
            .expect("waiter registry lock is never held across caller code")
            .len()
    }
}

impl fmt::Debug for WaiterRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WaiterRegistry")
            .field("waiters", &self.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct Signal {
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
    pub fn pulse(&self) {
        self.inner.pulse();
    }

    pub fn watcher(&self) -> SignalWatcher {
        SignalWatcher {
            inner: self.inner.watcher(),
        }
    }

    #[cfg(test)]
    fn watcher_count(&self) -> usize {
        self.inner.receiver_count()
    }
}

pub struct SignalWatcher {
    inner: WatchReceiver<()>,
}

impl SignalWatcher {
    pub async fn changed(&mut self) {
        self.inner.changed().await;
    }
}
/// A one-shot, multi-waiter signal backed by a contained waiter registry.
///
/// The atomic provides a linearizable, idempotent transition and retains the
/// fired state for future waiters. A wait registers before rechecking that
/// state, so it either observes the transition or is present in the registry
/// drained by the publisher. Each drained waker runs independently after the
/// registry lock is released.
///
/// This deliberately does not use `tokio_util::sync::CancellationToken`.
/// Shelterwood also uses latches for readiness and completion, needs `fire` to
/// report which caller performed the transition, and keeps parent and local
/// cancellation as distinct shared latches. The Tokio-util cancellation tree
/// would add allocation, locking, and dependencies without replacing those
/// semantics. A Tokio watch channel similarly adds value locking to the hot
/// `is_fired` path.
#[derive(Clone, Debug, Default)]
pub struct Latch {
    state: Arc<LatchState>,
}

#[derive(Debug, Default)]
struct LatchState {
    fired: AtomicBool,
    waiters: WaiterRegistry,
}

impl Latch {
    pub fn fire(&self) -> bool {
        if self.fire_silently() {
            self.notify();
            true
        } else {
            false
        }
    }

    /// Performs the one-shot transition without waking waiters.
    ///
    /// Splitting the transition from the wake lets an observation-gate
    /// transaction linearize the fire inside its critical section while
    /// deferring the waker-visible [`Self::notify`] until after the gate is
    /// released. Deferral cannot strand a waiter: [`Self::fired`] registers
    /// before rechecking `is_fired`, so a waiter either observes the committed
    /// transition directly or is present for the deferred drain.
    pub fn fire_silently(&self) -> bool {
        self.state
            .fired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Wakes waiters after a [`Self::fire_silently`] transition.
    ///
    /// Idempotent and only meaningful once the latch is fired; callers that
    /// won `fire_silently` inside an observation-gate transaction defer this
    /// wake past the gate release.
    pub fn notify(&self) {
        self.state.waiters.wake_all();
    }

    pub fn is_fired(&self) -> bool {
        self.state.fired.load(Ordering::Acquire)
    }

    pub fn fired(&self) -> LatchWait<'_> {
        LatchWait {
            latch: self,
            identity: Arc::new(()),
        }
    }
}

/// Future returned by [`Latch::fired`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct LatchWait<'a> {
    latch: &'a Latch,
    identity: Arc<()>,
}

impl Future for LatchWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.latch.is_fired() {
            let registered = this.latch.state.waiters.remove(&this.identity);
            WaiterRegistry::drop_registered([registered]);
            return Poll::Ready(());
        }

        // Caller code runs before the registry lock is acquired.
        let waker = context.waker().clone();
        let displaced = this.latch.state.waiters.register(&this.identity, waker);
        let fired = this.latch.is_fired();
        let registered = fired
            .then(|| this.latch.state.waiters.remove(&this.identity))
            .flatten();
        // Finish the register/recheck protocol before a hostile displaced
        // waker destructor is allowed to resume its panic.
        WaiterRegistry::drop_registered([displaced, registered]);
        if fired {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for LatchWait<'_> {
    fn drop(&mut self) {
        let registered = self.latch.state.waiters.remove(&self.identity);
        WaiterRegistry::drop_registered([registered]);
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
pub struct CompletionGatedLatch {
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
    pub fn fire(&self) -> bool {
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

    pub fn is_fired(&self) -> bool {
        matches!(
            self.state.load(Ordering::Acquire),
            COMPLETION_GATE_FIRED | COMPLETION_GATE_CLOSED_FIRED
        )
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn is_completed(&self) -> bool {
        matches!(
            self.state.load(Ordering::Acquire),
            COMPLETION_GATE_CLOSED | COMPLETION_GATE_CLOSED_FIRED
        )
    }

    pub async fn fired(&self) {
        self.fired.fired().await;
    }

    pub fn complete(&self) -> bool {
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

    pub async fn completed(&self) {
        self.completed.fired().await;
    }
}

const ONESHOT_OPEN: u8 = 0;
const ONESHOT_SENDING: u8 = 1;
const ONESHOT_SENT: u8 = 2;
const ONESHOT_SENDER_CLOSED: u8 = 3;
const ONESHOT_RECEIVER_CLOSED: u8 = 4;

/// Sending half of a runtime-backed single-delivery channel.
pub struct OneShotSender<T> {
    channel: Option<oneshot::Sender<T>>,
    state: Arc<AtomicU8>,
}

/// Receiving half of a runtime-backed single-delivery channel.
pub struct OneShotReceiver<T> {
    channel: oneshot::Receiver<T>,
    state: Arc<AtomicU8>,
}

/// Outcome after atomically closing a single-delivery receive side.
pub enum OneShotClose<T> {
    /// A value was sent before the receiver closed.
    Value(T),
    /// The sender was dropped before the receiver closed.
    SenderClosed,
    /// The receiver closed while the sender was still live and empty.
    Empty,
    /// The send transition won but has not published its value yet.
    Pending,
}

pub fn oneshot<T>() -> (OneShotSender<T>, OneShotReceiver<T>) {
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

/// Test-only publication half for staging the `ONESHOT_SENDING` window.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub struct OneShotSending<T> {
    channel: Option<oneshot::Sender<T>>,
    state: Arc<AtomicU8>,
}

/// Builds a one-shot whose send transition has won but whose value has not
/// been published yet.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn oneshot_sending_for_test<T>() -> (OneShotSending<T>, OneShotReceiver<T>) {
    let (channel, receiver) = oneshot::channel();
    let state = Arc::new(AtomicU8::new(ONESHOT_SENDING));
    (
        OneShotSending {
            channel: Some(channel),
            state: Arc::clone(&state),
        },
        OneShotReceiver {
            channel: receiver,
            state,
        },
    )
}

#[cfg(any(test, feature = "test-util"))]
impl<T> OneShotSending<T> {
    /// Publishes the value and completes the staged send transition.
    pub fn publish(mut self, value: T) -> Result<(), T> {
        let channel = self
            .channel
            .take()
            .expect("a staged one-shot send publishes at most once");
        match channel.send(value) {
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
}

impl<T> OneShotSender<T> {
    pub fn send(mut self, value: T) -> Result<(), T> {
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

    pub fn is_closed(&self) -> bool {
        // Observed under the observation gate and the dynamic-state mutex
        // (`RemovalResponses::subscribe`), so the missing-channel verdict
        // cannot be raised as a panic without poisoning both for every later
        // caller. The total form reports the taken channel as closed, which
        // is what a sender past `send` is.
        debug_assert!(
            self.channel.is_some(),
            "an observable one-shot sender retains its channel"
        );
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
    pub fn poll_receive(
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
    /// the channel poll in that branch registers the wake for its completion.
    pub fn close_and_poll_receive(
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

    pub fn close(&mut self) {
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
    pub fn close_and_take(&mut self) -> Option<T> {
        self.close();
        self.channel.try_recv().ok()
    }

    pub async fn receive(self) -> Option<T> {
        self.channel.await.ok()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn try_receive(&mut self) -> Option<T> {
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
///
/// The erasure is load-bearing API design, not incidental: `Drop` must repeat
/// whatever bounds the struct declares, so bounding this type would push
/// `T: Send + 'static` onto the *definitions* of the public wrappers that hold
/// it (`ReplyReceiver`, `ReplyReceive`, `CallFuture`, `OneShotTaskRef`) and
/// force downstream generic declarations to carry a bound they never asked
/// for. Execution bounds belong on constructors and operational impls here.
pub struct DisposingReceiver<T> {
    inner: OneShotReceiver<T>,
    dispose: fn(T),
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub fn new(inner: OneShotReceiver<T>) -> Self {
        Self {
            inner,
            dispose: dispose_detached::<T>,
        }
    }
}

impl<T> DisposingReceiver<T> {
    pub fn poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        self.inner.poll_receive(context)
    }

    pub fn close_and_poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> OneShotClose<T> {
        self.inner.close_and_poll_receive(context)
    }

    pub fn close(&mut self) {
        self.inner.close();
    }
}

impl<T> Drop for DisposingReceiver<T> {
    fn drop(&mut self) {
        if let Some(value) = self.inner.close_and_take() {
            (self.dispose)(value);
        }
    }
}

/// Shared state for the framework's conflating watch channel.
///
/// The value lock never contains wakers. Versions and endpoint counts are
/// atomic so waiter registration can use the same register/recheck protocol
/// as [`Latch`] without nesting locks.
struct WatchShared<T> {
    value: Mutex<T>,
    version: AtomicU64,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    waiters: WaiterRegistry,
}

impl<T> WatchShared<T> {
    /// Acquires the retained value, tolerating poisoning.
    ///
    /// `modify_silently` and `read_with` run a caller closure under this
    /// guard, and those closures do real work: a lifecycle publication sends
    /// on a broadcast channel here, and a snapshot installation evaluates a
    /// `debug_assert!` and mints a generation. A panic in any of them would
    /// otherwise wedge every later read, publication, subscription and
    /// terminal wait on the channel. The guarded data is plain framework
    /// state with no invariant spanning the closure, so the surviving value
    /// stays usable; this matches `ObservationGate::lock`, which tolerates
    /// poisoning for the same reason.
    fn value(&self) -> MutexGuard<'_, T> {
        self.value.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Publishing half of a runtime-backed conflating state channel.
pub struct WatchSender<T> {
    shared: Arc<WatchShared<T>>,
}

/// Observing half of a runtime-backed conflating state channel.
pub struct WatchReceiver<T> {
    shared: Arc<WatchShared<T>>,
    seen: u64,
}

fn retain_endpoint(counter: &AtomicUsize, endpoint: &str) {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            count.checked_add(1)
        })
        .unwrap_or_else(|_| panic!("{endpoint} count exhausted"));
}

impl<T> Clone for WatchSender<T> {
    fn clone(&self) -> Self {
        retain_endpoint(&self.shared.senders, "watch sender");
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for WatchSender<T> {
    fn drop(&mut self) {
        let previous = self.shared.senders.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "a watch sender is released at most once");
        if previous == 1 {
            self.shared.waiters.wake_all();
        }
    }
}

pub fn watch<T>(initial: T) -> (WatchSender<T>, WatchReceiver<T>) {
    let shared = Arc::new(WatchShared {
        value: Mutex::new(initial),
        version: AtomicU64::new(0),
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
        waiters: WaiterRegistry::default(),
    });
    (
        WatchSender {
            shared: Arc::clone(&shared),
        },
        WatchReceiver { shared, seen: 0 },
    )
}

impl<T> WatchSender<T> {
    /// Whether both senders address the same watch channel.
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    pub fn watcher(&self) -> WatchReceiver<T> {
        retain_endpoint(&self.shared.receivers, "watch receiver");
        WatchReceiver {
            shared: Arc::clone(&self.shared),
            seen: self.shared.version.load(Ordering::Acquire),
        }
    }

    pub fn receiver_count(&self) -> usize {
        self.shared.receivers.load(Ordering::Acquire)
    }

    pub fn pulse(&self) {
        self.shared.version.fetch_add(1, Ordering::AcqRel);
        self.shared.waiters.wake_all();
    }

    /// Mutates the retained value without advancing the watch version.
    ///
    /// This is only for compound publication that must finish another
    /// synchronous state transition before receivers are notified. The caller
    /// must follow a successful logical mutation with [`Self::pulse`].
    pub fn modify_silently(&self, update: impl FnOnce(&mut T)) {
        let mut value = self.shared.value();
        update(&mut value);
    }

    /// Reads a projection of the retained value without cloning it.
    ///
    /// `project` runs under the watch's value guard, so it must stay cheap and
    /// must not touch the same channel.
    pub fn read_with<R>(&self, project: impl FnOnce(&T) -> R) -> R {
        let value = self.shared.value();
        project(&value)
    }
}

impl<T: Clone> WatchSender<T> {
    pub fn read_cloned(&self) -> T {
        self.shared.value().clone()
    }
}

impl<T> Clone for WatchReceiver<T> {
    fn clone(&self) -> Self {
        retain_endpoint(&self.shared.receivers, "watch receiver");
        Self {
            shared: Arc::clone(&self.shared),
            seen: self.seen,
        }
    }
}

impl<T> Drop for WatchReceiver<T> {
    fn drop(&mut self) {
        let previous = self.shared.receivers.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "a watch receiver is released at most once");
    }
}

enum WatchWaitOutcome {
    Changed,
    Closed,
}

struct WatchWait<'a, T> {
    receiver: &'a mut WatchReceiver<T>,
    identity: Arc<()>,
}

impl<T> WatchWait<'_, T> {
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<WatchWaitOutcome> {
        let version = self.receiver.shared.version.load(Ordering::Acquire);
        if version != self.receiver.seen {
            self.receiver.seen = version;
            let registered = self.receiver.shared.waiters.remove(&self.identity);
            WaiterRegistry::drop_registered([registered]);
            return Poll::Ready(WatchWaitOutcome::Changed);
        }
        if self.receiver.shared.senders.load(Ordering::Acquire) == 0 {
            let registered = self.receiver.shared.waiters.remove(&self.identity);
            WaiterRegistry::drop_registered([registered]);
            return Poll::Ready(WatchWaitOutcome::Closed);
        }

        // Caller code runs before the registry lock is acquired.
        let waker = context.waker().clone();
        let displaced = self.receiver.shared.waiters.register(&self.identity, waker);
        let version = self.receiver.shared.version.load(Ordering::Acquire);
        let outcome = if version != self.receiver.seen {
            self.receiver.seen = version;
            Some(WatchWaitOutcome::Changed)
        } else if self.receiver.shared.senders.load(Ordering::Acquire) == 0 {
            Some(WatchWaitOutcome::Closed)
        } else {
            None
        };
        let registered = outcome
            .is_some()
            .then(|| self.receiver.shared.waiters.remove(&self.identity))
            .flatten();
        // Complete the publication recheck before a hostile displaced waker
        // destructor is allowed to resume its panic.
        WaiterRegistry::drop_registered([displaced, registered]);
        outcome.map_or(Poll::Pending, Poll::Ready)
    }
}

impl<T> Drop for WatchWait<'_, T> {
    fn drop(&mut self) {
        let registered = self.receiver.shared.waiters.remove(&self.identity);
        WaiterRegistry::drop_registered([registered]);
    }
}

/// Future returned by [`WatchReceiver::changed`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WatchChanged<'a, T> {
    wait: WatchWait<'a, T>,
}

impl<T> Future for WatchChanged<'_, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().wait.poll(context) {
            Poll::Ready(WatchWaitOutcome::Changed) => Poll::Ready(()),
            Poll::Ready(WatchWaitOutcome::Closed) | Poll::Pending => Poll::Pending,
        }
    }
}

/// Future returned by [`WatchReceiver::changed_or_closed`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WatchChangedOrClosed<'a, T> {
    wait: WatchWait<'a, T>,
}

impl<T> Future for WatchChangedOrClosed<'_, T> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().wait.poll(context) {
            Poll::Ready(WatchWaitOutcome::Changed) => Poll::Ready(true),
            Poll::Ready(WatchWaitOutcome::Closed) => Poll::Ready(false),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> WatchReceiver<T> {
    /// Waits for a new value without treating publisher closure as a change.
    ///
    /// Callers that need to observe publisher closure must use
    /// [`Self::changed_or_closed`] instead. Parking here prevents a loop that
    /// intentionally ignores closure from becoming permanently always-ready.
    pub fn changed(&mut self) -> WatchChanged<'_, T> {
        WatchChanged {
            wait: WatchWait {
                receiver: self,
                identity: Arc::new(()),
            },
        }
    }

    pub fn changed_or_closed(&mut self) -> WatchChangedOrClosed<'_, T> {
        WatchChangedOrClosed {
            wait: WatchWait {
                receiver: self,
                identity: Arc::new(()),
            },
        }
    }
}

impl<T: Clone> WatchReceiver<T> {
    pub fn borrow_cloned(&self) -> T {
        self.shared.value().clone()
    }

    pub fn borrow_and_update_cloned(&mut self) -> T {
        let value = self.shared.value();
        // Sampling the version before cloning may produce a harmless extra
        // wake if a pulse races this read, but cannot mark an unseen value as
        // observed. Publication writes the value before advancing the version.
        self.seen = self.shared.version.load(Ordering::Acquire);
        value.clone()
    }
}

impl<T> fmt::Debug for WatchSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WatchSender")
            .field("receivers", &self.receiver_count())
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for WatchReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WatchReceiver")
            .field("seen", &self.seen)
            .finish_non_exhaustive()
    }
}

/// Result of receiving from a runtime-backed broadcast channel.
pub enum BroadcastReceive<T> {
    Item(T),
    Empty,
    Closed,
    Lagged(u64),
}

/// Publishing half of a bounded runtime-backed broadcast channel.
pub struct BroadcastSender<T>(broadcast::Sender<T>);

/// Per-subscriber receiving half of a bounded runtime-backed broadcast channel.
pub struct BroadcastReceiver<T>(broadcast::Receiver<T>);

pub fn broadcast<T: Clone>(capacity: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let (sender, receiver) = broadcast::channel(capacity);
    (BroadcastSender(sender), BroadcastReceiver(receiver))
}

impl<T: Clone> BroadcastSender<T> {
    pub fn subscribe(&self) -> BroadcastReceiver<T> {
        BroadcastReceiver(self.0.subscribe())
    }

    pub fn send(&self, value: T) -> Result<usize, T> {
        self.0.send(value).map_err(|error| error.0)
    }

    pub fn receiver_count(&self) -> usize {
        self.0.receiver_count()
    }
}

impl<T: Clone> BroadcastReceiver<T> {
    pub fn try_receive(&mut self) -> BroadcastReceive<T> {
        match self.0.try_recv() {
            Ok(value) => BroadcastReceive::Item(value),
            Err(broadcast::error::TryRecvError::Empty) => BroadcastReceive::Empty,
            Err(broadcast::error::TryRecvError::Closed) => BroadcastReceive::Closed,
            Err(broadcast::error::TryRecvError::Lagged(dropped)) => {
                BroadcastReceive::Lagged(dropped)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use crate::{
        BroadcastReceive, CompletionGatedLatch, JoinOutcome, Latch, OneShotClose, Signal, Timeout,
        broadcast, join, oneshot, oneshot_sending_for_test, spawn, timeout, yield_now,
    };

    struct CountWake(Arc<AtomicUsize>);

    struct DebugProbe(Arc<AtomicUsize>);

    impl std::fmt::Debug for DebugProbe {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fetch_add(1, Ordering::SeqCst);
            formatter.write_str("DebugProbe")
        }
    }

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct CountPanicWake {
        wakes: Arc<AtomicUsize>,
        message: &'static str,
    }

    impl Wake for CountPanicWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(self.message);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(self.message);
        }
    }

    fn assert_panic_message(payload: &(dyn std::any::Any + Send), expected: &'static str) {
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some(expected)
        );
    }

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
    fn closing_oneshot_waits_for_a_send_that_won_before_publication() {
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut context = Context::from_waker(&waker);
        let (sending, mut receiver) = oneshot_sending_for_test();

        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Pending
        ));
        sending.publish(7_u8).expect("the staged receiver is live");
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Value(7)
        ));
    }

    #[test]
    fn broadcast_wrapper_maps_items_empty_close_and_exact_lag() {
        let (sender, mut receiver) = broadcast(2);
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Empty));
        assert_eq!(sender.send(0_u8), Ok(1));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Item(0)));

        for value in 1_u8..=5 {
            assert_eq!(sender.send(value), Ok(1));
        }
        assert!(matches!(
            receiver.try_receive(),
            BroadcastReceive::Lagged(3)
        ));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Item(4)));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Item(5)));
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Empty));
        drop(sender);
        assert!(matches!(receiver.try_receive(), BroadcastReceive::Closed));
    }

    #[test]
    fn signal_pulse_wakes_a_parked_watcher_and_advances_generations() {
        let signal = Signal::default();
        let mut watcher = signal.watcher();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));

        let mut first = Box::pin(watcher.changed());
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        signal.pulse();
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
        drop(first);

        let mut second = Box::pin(watcher.changed());
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        signal.pulse();
        assert_eq!(wakes.load(Ordering::SeqCst), 2);
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
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

    #[test]
    fn watch_sender_debug_never_formats_the_guarded_value() {
        let formats = Arc::new(AtomicUsize::new(0));
        let (sender, _receiver) = super::watch(DebugProbe(Arc::clone(&formats)));

        // Assert the omission, not the rendering: `Debug` output is not
        // contractual, but formatting the guarded value under the watch mutex
        // would be a lock-rule violation.
        let rendered = format!("{sender:?}");
        assert!(!rendered.contains("DebugProbe"), "rendered as {rendered}");
        assert_eq!(formats.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn watch_receiver_debug_never_formats_the_guarded_value() {
        let formats = Arc::new(AtomicUsize::new(0));
        let (_sender, receiver) = super::watch(DebugProbe(Arc::clone(&formats)));

        let rendered = format!("{receiver:?}");
        assert!(!rendered.contains("DebugProbe"), "rendered as {rendered}");
        assert_eq!(formats.load(Ordering::SeqCst), 0);
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

    #[test]
    fn silent_fire_defers_a_parked_waiters_wake_until_notify() {
        let latch = Latch::default();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut fired = Box::pin(latch.fired());

        assert!(
            fired
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(latch.fire_silently());
        assert!(latch.is_fired());
        assert_eq!(wakes.load(Ordering::SeqCst), 0);

        latch.notify();
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(
            fired
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_ready()
        );
    }

    #[test]
    fn hostile_latch_waiter_cannot_strand_a_well_behaved_waiter() {
        const PANIC: &str = "injected latch waker panic";

        let latch = Latch::default();
        let hostile_wakes = Arc::new(AtomicUsize::new(0));
        let ordinary_wakes = Arc::new(AtomicUsize::new(0));
        let hostile = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&hostile_wakes),
            message: PANIC,
        }));
        let ordinary = Waker::from(Arc::new(CountWake(Arc::clone(&ordinary_wakes))));
        let mut hostile_wait = Box::pin(latch.fired());
        let mut ordinary_wait = Box::pin(latch.fired());
        assert!(
            hostile_wait
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        assert!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary))
                .is_pending()
        );

        let result = catch_unwind(AssertUnwindSafe(|| latch.fire()));

        let payload = result.expect_err("the hostile latch wake still surfaces");
        assert_panic_message(&*payload, PANIC);
        assert!(latch.is_fired());
        assert_eq!(hostile_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(ordinary_wakes.load(Ordering::SeqCst), 1);
        assert!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary))
                .is_ready()
        );
    }

    #[test]
    fn hostile_watch_waiter_cannot_strand_a_well_behaved_pulse_waiter() {
        const PANIC: &str = "injected watch pulse waker panic";

        let (sender, mut hostile_receiver) = super::watch(0_u8);
        let mut ordinary_receiver = sender.watcher();
        let hostile_wakes = Arc::new(AtomicUsize::new(0));
        let ordinary_wakes = Arc::new(AtomicUsize::new(0));
        let hostile = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&hostile_wakes),
            message: PANIC,
        }));
        let ordinary = Waker::from(Arc::new(CountWake(Arc::clone(&ordinary_wakes))));
        let mut hostile_wait = Box::pin(hostile_receiver.changed_or_closed());
        let mut ordinary_wait = Box::pin(ordinary_receiver.changed_or_closed());
        assert!(
            hostile_wait
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        assert!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary))
                .is_pending()
        );
        sender.modify_silently(|value| *value = 1);

        let result = catch_unwind(AssertUnwindSafe(|| sender.pulse()));

        let payload = result.expect_err("the hostile watch pulse still surfaces");
        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(ordinary_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary)),
            Poll::Ready(true)
        );
        drop(hostile_wait);
        drop(ordinary_wait);
        assert_eq!(hostile_receiver.borrow_and_update_cloned(), 1);
        assert_eq!(ordinary_receiver.borrow_and_update_cloned(), 1);
    }

    #[test]
    fn hostile_watch_waiter_cannot_strand_a_well_behaved_close_waiter() {
        const PANIC: &str = "injected watch close waker panic";

        let (sender, mut hostile_receiver) = super::watch(());
        let mut ordinary_receiver = sender.watcher();
        let hostile_wakes = Arc::new(AtomicUsize::new(0));
        let ordinary_wakes = Arc::new(AtomicUsize::new(0));
        let hostile = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&hostile_wakes),
            message: PANIC,
        }));
        let ordinary = Waker::from(Arc::new(CountWake(Arc::clone(&ordinary_wakes))));
        let mut hostile_wait = Box::pin(hostile_receiver.changed_or_closed());
        let mut ordinary_wait = Box::pin(ordinary_receiver.changed_or_closed());
        assert!(
            hostile_wait
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        assert!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary))
                .is_pending()
        );

        let result = catch_unwind(AssertUnwindSafe(|| drop(sender)));

        let payload = result.expect_err("the hostile watch close still surfaces");
        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(ordinary_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(
            ordinary_wait
                .as_mut()
                .poll(&mut Context::from_waker(&ordinary)),
            Poll::Ready(false)
        );
    }

    #[test]
    fn completion_waiters_cover_parked_and_already_completed_paths() {
        let parked = CompletionGatedLatch::default();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        let mut waiting = Box::pin(parked.completed());
        let mut context = Context::from_waker(&waker);
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        assert!(!parked.complete());
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(waiting.as_mut().poll(&mut context).is_ready());

        let completed = CompletionGatedLatch::default();
        assert!(!completed.complete());
        let mut immediate = Box::pin(completed.completed());
        assert!(
            immediate
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
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

    #[test]
    fn a_panicking_watch_closure_leaves_the_channel_usable() {
        let (sender, mut receiver) = super::watch(0u8);
        let panicked = catch_unwind(AssertUnwindSafe(|| {
            sender.modify_silently(|value| {
                *value = 1;
                panic!("injected watch mutation panic");
            });
        }));
        assert!(panicked.is_err());

        // The guard is poisoned; every later reader must still see the
        // surviving framework state rather than inheriting the panic.
        assert_eq!(sender.read_cloned(), 1);
        assert_eq!(sender.read_with(|value| *value), 1);
        assert_eq!(receiver.borrow_cloned(), 1);
        assert_eq!(receiver.borrow_and_update_cloned(), 1);
        sender.modify_silently(|value| *value = 2);
        sender.pulse();
        assert_eq!(receiver.borrow_cloned(), 2);
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
