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

use super::{PanicAccumulator, dispose_detached, waker_proxy::ProxiedPoll};

/// A caller-waker registry whose lock protects only inert storage changes.
///
/// Registration clones happen before entering the registry. Removal and
/// draining only move wakers out; their vtables run after unlock, one behind
/// each accumulator boundary. The opaque identity is retained by its waiter,
/// so its `Arc` traffic under the lock cannot destroy even framework data.
/// Cancellation destroys a removed caller-waker clone inline on the thread
/// dropping `LatchWait` or `WatchWait`: unlike the external-primitive proxy
/// family, the registry owns the waker directly and has no foreign drop seam
/// that requires detached retirement. That inherits the reply receiver's
/// ruling rather than the proxy wrapper's — a slow caller-waker destructor
/// stalls the abandoning waiter alone, and a hostile one is contained by the
/// accumulator above.
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

    /// Polls one register/recheck waiter protocol around `ready`.
    ///
    /// Caller waker cloning happens before the registry lock is acquired. A
    /// displaced registration and the registration removed after a successful
    /// recheck are both destroyed only after that recheck completes, so a
    /// hostile waker destructor cannot strand an already-published outcome.
    fn poll_registered<T>(
        &self,
        identity: &Arc<()>,
        context: &Context<'_>,
        mut ready: impl FnMut() -> Option<T>,
    ) -> Poll<T> {
        if let Some(output) = ready() {
            let registered = self.remove(identity);
            Self::drop_registered([registered]);
            return Poll::Ready(output);
        }

        // Caller code runs before the registry lock is acquired.
        let waker = context.waker().clone();
        let displaced = self.register(identity, waker);
        let output = ready();
        let registered = output.is_some().then(|| self.remove(identity)).flatten();
        // Complete the publication recheck before a hostile displaced waker
        // destructor is allowed to resume its panic.
        Self::drop_registered([displaced, registered]);
        output.map_or(Poll::Pending, Poll::Ready)
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

    /// The channel-wide waiter-registry length.
    ///
    /// This is deliberately not the endpoint count: `changed()` never clones
    /// its receiver, so an endpoint probe cannot observe a registration a
    /// cancelled wait failed to remove.
    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.inner.shared.waiters.len()
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
        this.latch
            .state
            .waiters
            .poll_registered(&this.identity, context, || {
                this.latch.is_fired().then_some(())
            })
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
///
/// The wake lags the state: both transitions publish their state CAS before
/// firing the corresponding latch, so `is_fired`/`is_completed` can read
/// true while the matching waiter's wake is still in flight. A waiter racing
/// `fired()` against `completed()` must resolve the tie by state or by
/// left-biased selection (`select_two`), never by wake arrival order.
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
        assert!(transitioned);
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
                assert!(transitioned);
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
#[cfg(test)]
pub(crate) const ONESHOT_REPOLL_PANIC: &str =
    "shelterwood one-shot receiver polled after completion";

/// Sending half of a runtime-backed single-delivery channel.
pub struct OneShotSender<T> {
    channel: Option<oneshot::Sender<T>>,
    state: Arc<AtomicU8>,
}

/// Receiving half of a runtime-backed single-delivery channel.
pub struct OneShotReceiver<T> {
    channel: oneshot::Receiver<T>,
    state: Arc<AtomicU8>,
    /// Whether a receive edge has already consumed Tokio's receiver.
    ///
    /// In the pinned Tokio 1.53.1, `Receiver::poll` clears its `Inner` once it
    /// yields `Ready`, and `try_recv` clears it on every outcome but `Empty`;
    /// a later `poll` then panics with a message naming neither Shelterwood
    /// nor this seam. Every terminal edge here records that instead, so the
    /// re-poll diagnostic is framework-owned. A bare [`Self::close`] is not
    /// terminal — Tokio keeps the receiver pollable — so it does not set this.
    completed: bool,
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
            completed: false,
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
            completed: false,
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
        publish_oneshot(channel, &self.state, value)
    }
}

/// Publishes into a one-shot channel whose `ONESHOT_SENDING` window the
/// caller already owns, then closes that window with the outcome.
///
/// `OneShotSender::send` and the staged `OneShotSending::publish` differ only
/// in how they enter the window — `send` wins it with a CAS from
/// `ONESHOT_OPEN`, the test half is constructed inside it — so they share the
/// tail rather than restating it.
fn publish_oneshot<T>(channel: oneshot::Sender<T>, state: &AtomicU8, value: T) -> Result<(), T> {
    match channel.send(value) {
        Ok(()) => {
            state.store(ONESHOT_SENT, Ordering::Release);
            Ok(())
        }
        Err(value) => {
            state.store(ONESHOT_RECEIVER_CLOSED, Ordering::Release);
            Err(value)
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
        publish_oneshot(sender, &self.state, value)
    }

    pub fn is_closed(&self) -> bool {
        // Diagnostic-only: this is observed under the observation gate and
        // the dynamic-state mutex
        // (`RemovalResponses::subscribe`), so the missing-channel verdict
        // cannot be raised as a panic without poisoning both for every later
        // caller. No correctness property depends on the diagnostic: the
        // total form reports the taken channel as closed, which is what a
        // sender past `send` is, and no test expects this assertion to fire.
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
        // Tokio closes the channel synchronously and may invoke a receiver's
        // caller-supplied waker from the sender's drop glue. Surface that panic
        // to an ordinary dropper, but contain it during an existing unwind so
        // an unanswered reply cannot abort the process with a double panic.
        // This accumulator is single-candidate by construction: dropping the
        // channel is its only fallible step, so `keep_first_panic` precedence
        // is exercised by the multi-waiter registry cleanup instead.
        let channel = self.channel.take();
        let mut panics = PanicAccumulator::default();
        panics.run(|| drop(channel));
    }
}

impl<T> OneShotReceiver<T> {
    fn assert_not_completed(&self) {
        assert!(
            !self.completed,
            "shelterwood one-shot receiver polled after completion"
        );
    }

    pub fn poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        self.assert_not_completed();
        let result = Pin::new(&mut self.channel).poll(context).map(Result::ok);
        if result.is_ready() {
            self.completed = true;
        }
        result
    }

    /// Closes the receive side unless send or sender-drop won first.
    ///
    /// The shared transition word distinguishes sender-drop from receiver
    /// close, which Tokio's post-close `try_recv` result alone cannot do. A
    /// send that wins but is preempted before publishing returns `Pending`;
    /// the channel poll in that branch registers the wake for its completion.
    ///
    /// Every outcome but `Pending` is terminal for this receiver: the close
    /// has been arbitrated, so a later receive edge is a caller bug and gets
    /// the framework diagnostic rather than a fresh arbitration.
    pub fn close_and_poll_receive(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> OneShotClose<T> {
        self.assert_not_completed();
        let result = match self.state.compare_exchange(
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
        };
        if !matches!(&result, OneShotClose::Pending) {
            self.completed = true;
        }
        result
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
        let value = self.channel.try_recv().ok();
        // Closing makes every outcome terminal, including the staged-send
        // window where publication has not yet observed the receiver close.
        // Record that edge so an erased receiver cannot later re-enter
        // Tokio's completed receiver poll.
        self.completed = true;
        value
    }

    pub async fn receive(self) -> Option<T> {
        self.assert_not_completed();
        self.channel.await.ok()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn try_receive(&mut self) -> Option<T> {
        match self.channel.try_recv() {
            Ok(value) => {
                self.completed = true;
                Some(value)
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                self.completed = true;
                None
            }
            Err(oneshot::error::TryRecvError::Empty) => None,
        }
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
/// `T: Send + 'static` onto the definition of the public `OneShotTaskRef`
/// wrapper that holds it and force downstream generic declarations to carry a
/// bound they never asked for. Reply and call wrappers hold the separate
/// erased `DisposingReceiver` in the façade's mailbox capability module; the two
/// boundary types preserve the same constructor-only bound. Execution bounds
/// belong on constructors and operational impls here.
pub struct DisposingReceiver<T> {
    inner: Option<OneShotReceiver<T>>,
    dispose: fn(T),
    caller_poll: ProxiedPoll,
}

impl<T: Send + 'static> DisposingReceiver<T> {
    pub fn new(inner: OneShotReceiver<T>) -> Self {
        Self::with_dispose(inner, dispose_detached::<T>)
    }
}

impl<T> DisposingReceiver<T> {
    fn with_dispose(inner: OneShotReceiver<T>, dispose: fn(T)) -> Self {
        Self {
            inner: Some(inner),
            dispose,
            caller_poll: ProxiedPoll::new(),
        }
    }

    pub fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        // In pinned Tokio 1.53.1, `Receiver::poll` obtains the result before
        // clearing its `Inner`; the last `Inner::drop` then calls
        // `rx_task.drop_task` while that result can own the delivered value.
        // Probe with a framework waker, then leave only the stable proxy
        // registered across a pending return so Tokio never destroys the raw
        // caller waker at that delivery seam.
        self.caller_poll.poll(
            self.inner
                .as_mut()
                .expect("a live disposing receiver retains its channel"),
            context,
            OneShotReceiver::poll_receive,
            Poll::is_pending,
        )
    }

    /// Staged parity with the mailbox receiver's timeout arbitration: no
    /// production path closes a runtime `DisposingReceiver` today. It matters
    /// to the venue split anyway, because a receiver-initiated close is the
    /// one ready edge no sender wake precedes — the proxy can still hold an
    /// installed caller clone when inline retirement runs, which every
    /// sender-side completion consumes through the wake first. The
    /// ready-edge containment test below is that path's pin.
    pub fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T> {
        self.caller_poll.poll(
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

        // Recover and dispatch an unclaimed value before either receiver or
        // caller-waker retirement can fail. The waker Tokio's close/drop work
        // below destroys is only ever a framework proxy clone. If recovery
        // itself unwinds, the value stays in the channel and `inner`'s own
        // drop glue destroys it inline rather than through `dispose`:
        // accepted, because reaching it requires a destructor that has
        // already panicked, and the alternative is retrying a step that just
        // failed.
        panics.run(|| value = inner.close_and_take());
        panics.run(|| {
            if let Some(value) = value {
                (self.dispose)(value);
            }
        });
        panics.run(|| drop(inner));
        self.caller_poll.retire_detached(&mut panics);
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
    /// on a broadcast channel here, and a snapshot installation mints a
    /// generation. A panic in any of them would otherwise wedge every later
    /// read, publication, subscription and
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
        // Diagnostic-only: safe ownership cannot drop one endpoint twice.
        // The wake decision below remains total without this check, and no
        // test depends on the diagnostic panic.
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
    /// must follow a successful logical mutation with [`Self::pulse`]. The
    /// closure runs under the watch value mutex and therefore may only move
    /// plain framework-owned data: it must not call user code, drop user
    /// values, block, panic, or re-enter the framework.
    pub fn modify_silently(&self, update: impl FnOnce(&mut T)) {
        let mut value = self.shared.value();
        update(&mut value);
    }

    /// Reads a projection of the retained value without cloning it.
    ///
    /// `project` runs under the watch's value guard, so it may only inspect
    /// plain framework-owned data. It must not call user code, drop user
    /// values, block, panic, re-enter the framework, or touch this channel.
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
        // Diagnostic-only: safe ownership cannot drop one endpoint twice.
        // There is no downstream action whose correctness depends on this
        // check, and no test depends on the diagnostic panic.
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
        let shared = &self.receiver.shared;
        let seen = &mut self.receiver.seen;
        shared.waiters.poll_registered(&self.identity, context, || {
            let version = shared.version.load(Ordering::Acquire);
            if version != *seen {
                *seen = version;
                Some(WatchWaitOutcome::Changed)
            } else if shared.senders.load(Ordering::Acquire) == 0 {
                Some(WatchWaitOutcome::Closed)
            } else {
                None
            }
        })
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
        mem::ManuallyDrop,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
        thread::ThreadId,
        time::Duration,
    };

    use crate::{
        BroadcastReceive, CompletionGatedLatch, DisposingReceiver, JoinOutcome, Latch,
        OneShotClose, Signal, Timeout, broadcast, join, oneshot, oneshot_sending_for_test, spawn,
        test_support::DISPOSAL_THREAD, timeout, yield_now,
    };

    use super::{ONESHOT_REPOLL_PANIC, RegisteredWaker, WaiterRegistry};

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

    struct LastWakerDropPanics {
        drops: Arc<AtomicUsize>,
        message: &'static str,
    }

    unsafe fn clone_last_drop_panics(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let probe = ManuallyDrop::new(unsafe { Arc::<LastWakerDropPanics>::from_raw(data.cast()) });
        RawWaker::new(
            Arc::into_raw(Arc::clone(&probe)).cast(),
            &LAST_DROP_PANICS_VTABLE,
        )
    }

    unsafe fn wake_last_drop_panics(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<LastWakerDropPanics>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_last_drop_panics(_data: *const ()) {}

    unsafe fn drop_last_drop_panics(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let probe = unsafe { Arc::<LastWakerDropPanics>::from_raw(data.cast()) };
        let last = Arc::strong_count(&probe) == 1;
        if last {
            probe.drops.fetch_add(1, Ordering::SeqCst);
        }
        let message = probe.message;
        drop(probe);
        if last {
            std::panic::panic_any(message);
        }
    }

    static LAST_DROP_PANICS_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_last_drop_panics,
        wake_last_drop_panics,
        wake_by_ref_last_drop_panics,
        drop_last_drop_panics,
    );

    fn last_drop_panics_waker(message: &'static str, drops: Arc<AtomicUsize>) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(LastWakerDropPanics { drops, message })).cast(),
            &LAST_DROP_PANICS_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    struct RecordPanickingDrop(mpsc::Sender<(ThreadId, Option<String>)>);

    unsafe fn clone_record_panicking_drop(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the represented reference;
        // the returned raw waker owns only the new clone.
        let probe = ManuallyDrop::new(unsafe { Arc::<RecordPanickingDrop>::from_raw(data.cast()) });
        RawWaker::new(
            Arc::into_raw(Arc::clone(&probe)).cast(),
            &RECORD_PANICKING_DROP_VTABLE,
        )
    }

    unsafe fn wake_record_panicking_drop(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<RecordPanickingDrop>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_record_panicking_drop(_data: *const ()) {}

    unsafe fn drop_record_panicking_drop(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        let probe = unsafe { Arc::<RecordPanickingDrop>::from_raw(data.cast()) };
        let _ = probe.0.send((
            std::thread::current().id(),
            std::thread::current().name().map(str::to_owned),
        ));
        drop(probe);
        panic!("injected disposing-receiver caller-waker drop panic");
    }

    static RECORD_PANICKING_DROP_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_record_panicking_drop,
        wake_record_panicking_drop,
        wake_by_ref_record_panicking_drop,
        drop_record_panicking_drop,
    );

    fn record_panicking_drop_waker(dropped: mpsc::Sender<(ThreadId, Option<String>)>) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(RecordPanickingDrop(dropped))).cast(),
            &RECORD_PANICKING_DROP_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    struct TransitionOnClone {
        transition: Box<dyn Fn() + Send + Sync>,
    }

    unsafe fn clone_transition_on_clone(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let probe = ManuallyDrop::new(unsafe { Arc::<TransitionOnClone>::from_raw(data.cast()) });
        (probe.transition)();
        RawWaker::new(
            Arc::into_raw(Arc::clone(&probe)).cast(),
            &TRANSITION_ON_CLONE_VTABLE,
        )
    }

    unsafe fn wake_transition_on_clone(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<TransitionOnClone>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_transition_on_clone(_data: *const ()) {}

    unsafe fn drop_transition_on_clone(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by this waker.
        drop(unsafe { Arc::<TransitionOnClone>::from_raw(data.cast()) });
    }

    static TRANSITION_ON_CLONE_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_transition_on_clone,
        wake_transition_on_clone,
        wake_by_ref_transition_on_clone,
        drop_transition_on_clone,
    );

    fn transition_on_clone_waker(transition: impl Fn() + Send + Sync + 'static) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(TransitionOnClone {
                transition: Box::new(transition),
            }))
            .cast(),
            &TRANSITION_ON_CLONE_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    fn assert_panic_message(payload: &(dyn std::any::Any + Send), expected: &'static str) {
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some(expected)
        );
    }

    #[test]
    fn registered_waker_cleanup_keeps_the_first_panic_and_attempts_every_drop() {
        const FIRST: &str = "first registered waker drop panic";
        const SECOND: &str = "second registered waker drop panic";

        let first_drops = Arc::new(AtomicUsize::new(0));
        let second_drops = Arc::new(AtomicUsize::new(0));
        let first = RegisteredWaker {
            identity: Arc::new(()),
            waker: last_drop_panics_waker(FIRST, Arc::clone(&first_drops)),
        };
        let second = RegisteredWaker {
            identity: Arc::new(()),
            waker: last_drop_panics_waker(SECOND, Arc::clone(&second_drops)),
        };

        let payload = catch_unwind(AssertUnwindSafe(|| {
            WaiterRegistry::drop_registered([Some(first), Some(second)]);
        }))
        .expect_err("registered-waker cleanup resumes its first panic");

        assert_panic_message(&*payload, FIRST);
        assert_eq!(first_drops.load(Ordering::SeqCst), 1);
        assert_eq!(second_drops.load(Ordering::SeqCst), 1);
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
    fn oneshot_repoll_uses_a_framework_owned_diagnostic() {
        let mut context = Context::from_waker(Waker::noop());
        let (sender, mut receiver) = oneshot();
        sender.send(1_u8).expect("receiver is live");
        assert!(matches!(
            receiver.poll_receive(&mut context),
            Poll::Ready(Some(1))
        ));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("a completed one-shot cannot be polled twice");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let mut receive = Box::pin(receiver.receive());
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receive.as_mut().poll(&mut context);
        }))
        .expect_err("receive cannot re-poll an already delivered receiver");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, receiver) = oneshot();
        sender.send(2_u8).expect("receiver is live");
        let mut receiver = DisposingReceiver::new(receiver);
        assert!(matches!(
            receiver.poll_receive(&mut context),
            Poll::Ready(Some(2))
        ));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("a completed disposing receiver cannot be polled twice");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, mut receiver) = oneshot();
        sender.send(3_u8).expect("receiver is live");
        assert!(matches!(
            receiver.close_and_poll_receive(&mut context),
            OneShotClose::Value(3)
        ));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.close_and_poll_receive(&mut context);
        }))
        .expect_err("a completed close poll cannot be repeated");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, mut receiver) = oneshot();
        sender.send(4_u8).expect("receiver is live");
        assert_eq!(receiver.close_and_take(), Some(4));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("close-and-take terminality prevents a later poll");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        let (sender, mut receiver) = oneshot();
        sender.send(5_u8).expect("receiver is live");
        assert_eq!(receiver.try_receive(), Some(5));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.close_and_poll_receive(&mut context);
        }))
        .expect_err("try-receive terminality prevents a later close poll");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        // Tokio's `try_recv` consumes its receiver on every outcome but
        // `Empty`, so an empty sender-closed take is terminal too.
        let (sender, mut receiver) = oneshot::<u8>();
        drop(sender);
        assert_eq!(receiver.try_receive(), None);
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut context);
        }))
        .expect_err("an exhausted try-receive prevents a later poll");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);

        // A bare close is not a receive edge: the receiver stays pollable and
        // reports the sender-closed result.
        let (sender, mut receiver) = oneshot::<u8>();
        receiver.close();
        drop(sender);
        assert!(matches!(
            receiver.poll_receive(&mut context),
            Poll::Ready(None)
        ));
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
    fn closing_oneshot_during_sending_bounces_the_value_to_the_publisher() {
        let (sending, mut receiver) = oneshot_sending_for_test();

        receiver.close();

        assert_eq!(sending.publish(7_u8), Err(7));
        assert_eq!(receiver.try_receive(), None);
    }

    #[test]
    fn dropping_disposing_receiver_detaches_caller_waker_retirement() {
        let dropping_thread = std::thread::current().id();
        let (_sender, receiver) = oneshot::<u8>();
        let mut receiver = DisposingReceiver::new(receiver);
        let (dropped, observed_drop) = mpsc::channel();
        let caller = ManuallyDrop::new(record_panicking_drop_waker(dropped));

        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(&caller)),
            Poll::Pending
        ));
        drop(receiver);

        let (destructor_thread, destructor_name) = observed_drop
            .recv_timeout(Duration::from_secs(1))
            .expect("the caller waker reaches detached disposal");
        assert_ne!(destructor_thread, dropping_thread);
        assert_eq!(
            destructor_name.as_deref(),
            Some(DISPOSAL_THREAD),
            "drop glue must not destroy a caller waker on the holder's thread"
        );
    }

    #[test]
    fn receiver_close_ready_edge_contains_the_installed_caller_waker() {
        let closing_thread = std::thread::current().id();
        let (_sender, receiver) = oneshot::<u8>();
        let mut receiver = DisposingReceiver::new(receiver);
        let (dropped, observed_drop) = mpsc::channel();
        let caller = ManuallyDrop::new(record_panicking_drop_waker(dropped));

        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(&caller)),
            Poll::Pending
        ));
        // A receiver-initiated close reaches the ready edge with the caller
        // clone still installed: no sender wake preceded it to consume the
        // slot. Retirement must destroy that hostile clone synchronously on
        // this thread and contain its panic so the close outcome is handed
        // back intact.
        assert!(matches!(
            receiver.close_and_poll_receive(&mut Context::from_waker(&caller)),
            OneShotClose::Empty
        ));

        let (destructor_thread, _destructor_name) = observed_drop
            .recv_timeout(Duration::from_secs(1))
            .expect("the ready edge retires the installed caller clone");
        assert_eq!(
            destructor_thread, closing_thread,
            "ready-path retirement is synchronous on the closing thread"
        );
    }

    #[test]
    fn close_and_take_during_sending_leaves_the_value_with_the_publisher() {
        let (sending, mut receiver) = oneshot_sending_for_test();

        assert_eq!(receiver.close_and_take(), None);

        assert_eq!(sending.publish(7_u8), Err(7));
        assert_eq!(receiver.try_receive(), None);
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = receiver.poll_receive(&mut Context::from_waker(Waker::noop()));
        }))
        .expect_err("close-and-take completes the staged-send receiver");
        assert_panic_message(&*payload, ONESHOT_REPOLL_PANIC);
    }

    #[test]
    fn dropping_oneshot_sender_surfaces_a_hostile_receiver_waker_once() {
        const PANIC: &str = "injected one-shot receiver waker panic";

        let (sender, mut receiver) = oneshot::<u8>();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountPanicWake {
            wakes: Arc::clone(&wakes),
            message: PANIC,
        }));
        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));

        let payload = catch_unwind(AssertUnwindSafe(|| drop(sender)))
            .expect_err("an ordinary sender drop surfaces the hostile wake");

        assert_panic_message(&*payload, PANIC);
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            receiver.poll_receive(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(None)
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
    fn quiet_signal_wait_cancellation_removes_waiter_registration() {
        let signal = Signal::default();
        let mut watcher = signal.watcher();
        assert_eq!(signal.waiter_count(), 0);

        for _ in 0..10_000 {
            let mut changed = Box::pin(watcher.changed());
            let mut context = Context::from_waker(Waker::noop());
            assert!(changed.as_mut().poll(&mut context).is_pending());
            assert_eq!(
                signal.waiter_count(),
                1,
                "a parked wait holds exactly one registration"
            );
            drop(changed);
            assert_eq!(
                signal.waiter_count(),
                0,
                "cancelling the wait removes its registration"
            );
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
    fn latch_rechecks_fire_before_dropping_a_displaced_hostile_waker() {
        const PANIC: &str = "injected registered waker drop panic";

        let latch = Latch::default();
        let hostile_drops = Arc::new(AtomicUsize::new(0));
        let hostile = last_drop_panics_waker(PANIC, Arc::clone(&hostile_drops));
        let mut waiting = Box::pin(latch.fired());
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        // The registered clone remains live, so releasing the caller's raw
        // waker reference does not run its last-reference panic yet.
        drop(hostile);

        let racing_latch = latch.clone();
        let racing = transition_on_clone_waker(move || {
            assert!(racing_latch.fire_silently());
        });
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = waiting.as_mut().poll(&mut Context::from_waker(&racing));
        }))
        .expect_err("destroying the displaced waker still surfaces its panic");

        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_drops.load(Ordering::SeqCst), 1);
        assert!(latch.is_fired());
        assert_eq!(
            latch.state.waiters.len(),
            0,
            "the fire recheck removes the replacement before the displaced destructor resumes"
        );
    }

    #[test]
    fn watch_rechecks_pulse_before_dropping_a_displaced_hostile_waker() {
        const PANIC: &str = "injected registered waker drop panic";

        let (sender, mut receiver) = super::watch(());
        let hostile_drops = Arc::new(AtomicUsize::new(0));
        let hostile = last_drop_panics_waker(PANIC, Arc::clone(&hostile_drops));
        let mut waiting = Box::pin(receiver.changed());
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        drop(hostile);

        // Advance the same atomic publication edge as `pulse` during the
        // replacement waker's clone. Leaving the registry intact is the
        // deterministic race shape that exercises displaced-waker teardown.
        let shared = Arc::clone(&sender.shared);
        let racing = transition_on_clone_waker(move || {
            shared.version.fetch_add(1, Ordering::AcqRel);
        });
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = waiting.as_mut().poll(&mut Context::from_waker(&racing));
        }))
        .expect_err("destroying the displaced waker still surfaces its panic");

        assert_panic_message(&*payload, PANIC);
        assert_eq!(hostile_drops.load(Ordering::SeqCst), 1);
        assert!(
            waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "the pulse version is consumed before the displaced destructor resumes"
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
            assert_eq!(
                latch.state.waiters.len(),
                1,
                "a parked wait holds exactly one registration"
            );
            drop(fired);
            assert_eq!(
                latch.state.waiters.len(),
                0,
                "cancelling the wait removes its registration"
            );
        }

        let mut live_waiter = Box::pin(latch.fired());
        let first_poll =
            std::future::poll_fn(|context| Poll::Ready(live_waiter.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());
        assert_eq!(
            latch.state.waiters.len(),
            1,
            "the surviving wait is still registered"
        );
        assert!(latch.fire());
        assert_eq!(
            latch.state.waiters.len(),
            0,
            "the fire drains every registration before waking"
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), live_waiter).await,
            Timeout::Completed(())
        ));
    }
}
