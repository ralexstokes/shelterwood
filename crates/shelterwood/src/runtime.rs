//! The only boundary between the library and its async runtime.

use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    future::Future,
    ops::RangeBounds,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, broadcast, mpsc, oneshot, watch},
    task, time,
};

use crate::deadline::Deadline;

#[cfg(test)]
pub(crate) use tokio::test;

/// Advances a paused test clock, keeping timer control in this module.
#[cfg(test)]
pub(crate) async fn advance(duration: Duration) {
    time::advance(duration).await;
}

/// Counts the runtime's currently alive spawned tasks, keeping runtime
/// metrics access in this module.
#[cfg(test)]
pub(crate) fn alive_task_count() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Runtime-owned spelling for an unwind payload crossing a framework boundary.
pub(crate) type PanicPayload = Box<dyn Any + Send + 'static>;

/// Panic payloads crossing an unwind boundary, named by precedence.
pub(crate) struct UnwindPanics {
    pub(crate) primary: Option<PanicPayload>,
    pub(crate) cleanup: Option<PanicPayload>,
}

/// Catches application code without requiring every caller to repeat the
/// `AssertUnwindSafe` boundary vocabulary.
pub(crate) fn catch_panic<T>(operation: impl FnOnce() -> T) -> Result<T, PanicPayload> {
    catch_unwind(AssertUnwindSafe(operation))
}

/// Discards an optional panic payload without trusting its destructor.
pub(crate) fn discard_panic(payload: Option<PanicPayload>) {
    if let Some(payload) = payload
        && let Err(hostile_payload) = catch_panic(|| drop(payload))
    {
        // A payload whose own destructor panics cannot be dropped safely:
        // dropping the replacement payload would merely recurse outside the
        // boundary. Leak only this already-panicking diagnostic.
        std::mem::forget(hostile_payload);
    }
}

/// Resumes one captured panic payload at the framework boundary.
pub(crate) fn resume_panic(payload: PanicPayload) -> ! {
    resume_unwind(payload)
}

/// Retains the first panic and safely discards a later cleanup panic.
pub(crate) fn keep_first_panic(first: &mut Option<PanicPayload>, candidate: Option<PanicPayload>) {
    if first.is_none() {
        *first = candidate;
    } else {
        discard_panic(candidate);
    }
}

/// Resumes the primary panic, or the cleanup panic when there is no primary.
/// During an existing unwind both are contained to prevent a double panic.
///
/// Containment is only correct where losing the diagnostic is the lesser
/// outcome, which is true in a destructor and false on a normal return path.
/// Callers that own the sole surviving copy of an authoritative panic must use
/// [`resume_preferred_panic_outside_unwind`] instead.
pub(crate) fn resume_preferred_panic(panics: UnwindPanics) {
    let UnwindPanics { primary, cleanup } = panics;
    if std::thread::panicking() {
        discard_panic(primary);
        discard_panic(cleanup);
    } else if let Some(payload) = primary {
        discard_panic(cleanup);
        resume_panic(payload);
    } else if let Some(payload) = cleanup {
        resume_panic(payload);
    }
}

/// Resumes exactly as [`resume_preferred_panic`], but never contains the
/// payload.
///
/// This is the variant for call sites that are not destructors and have
/// already taken sole ownership of the panic. Silently discarding there would
/// erase the authoritative diagnostic and let the caller continue past a
/// failure it believes it re-raised, so the caller's non-unwinding precondition
/// is asserted rather than absorbed.
pub(crate) fn resume_preferred_panic_outside_unwind(panics: UnwindPanics) {
    let UnwindPanics { primary, cleanup } = panics;
    debug_assert!(
        !std::thread::panicking(),
        "an unwinding caller must contain its payloads with resume_preferred_panic"
    );
    if let Some(payload) = primary {
        discard_panic(cleanup);
        resume_panic(payload);
    } else if let Some(payload) = cleanup {
        resume_panic(payload);
    }
}

/// Collects independent cleanup panics while allowing every cleanup step to
/// run. Dropping the accumulator resumes the first panic unless another unwind
/// is already in progress; callers that need to defer that decision can
/// [`take`](Self::take) the payload.
#[derive(Default)]
pub(crate) struct PanicAccumulator {
    first: Option<PanicPayload>,
}

impl PanicAccumulator {
    pub(crate) fn run(&mut self, operation: impl FnOnce()) {
        self.record(catch_panic(operation).err());
    }

    pub(crate) fn record(&mut self, candidate: Option<PanicPayload>) {
        keep_first_panic(&mut self.first, candidate);
    }

    pub(crate) fn take(&mut self) -> Option<PanicPayload> {
        self.first.take()
    }
}

impl Drop for PanicAccumulator {
    fn drop(&mut self) {
        resume_preferred_panic(UnwindPanics {
            primary: None,
            cleanup: self.first.take(),
        });
    }
}

pub(crate) fn now() -> std::time::Instant {
    time::Instant::now().into_std()
}

// Keep each timer registration comfortably inside tokio's millisecond tick
// range. Tokio caps instants beyond its private `u64::MAX - 3` tick sentinel,
// which would otherwise make a valid but very distant std Instant fire early.
// Rechecking the original absolute point after bounded slices preserves exact
// never-early semantics without coupling this crate to that private constant.
const MAX_TIMER_SLICE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

fn next_timer_deadline(
    current: std::time::Instant,
    requested: std::time::Instant,
) -> Option<std::time::Instant> {
    if requested <= current {
        return None;
    }
    let slice = requested.duration_since(current).min(MAX_TIMER_SLICE);
    current.checked_add(slice)
}

pub(crate) fn is_available() -> bool {
    tokio::runtime::Handle::try_current().is_ok()
}

pub(crate) type BoxedSleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub(crate) fn deadline(duration: Duration) -> Deadline {
    Deadline::after(now(), duration)
}

pub(crate) fn sleep_deadline(deadline: Deadline) -> BoxedSleep {
    Box::pin(async move {
        match deadline.instant() {
            Some(deadline) => sleep_until_std(deadline).await,
            None => std::future::pending().await,
        }
    })
}

pub(crate) fn sleep_until(deadline: std::time::Instant) -> BoxedSleep {
    Box::pin(sleep_until_std(deadline))
}

pub(crate) struct ActorWork {
    handle: Option<JoinHandle<()>>,
    abort: AbortHandle,
}

impl ActorWork {
    pub(crate) fn abort(&self) {
        self.abort.abort();
    }

    pub(crate) async fn join(mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let _ = join(handle).await;
    }
}

impl Drop for ActorWork {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(crate) fn spawn_actor_work(future: impl Future<Output = ()> + Send + 'static) -> ActorWork {
    let handle = spawn(future);
    let abort = handle.abort_handle();
    ActorWork {
        handle: Some(handle),
        abort,
    }
}

pub(crate) struct BlockingWork<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T: Send + 'static> BlockingWork<T> {
    pub(crate) async fn join(mut self) -> T {
        let handle = self
            .handle
            .take()
            .expect("blocking operation was joined more than once");
        join_resuming(handle).await
    }
}

pub(crate) fn spawn_blocking_work<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> BlockingWork<T> {
    BlockingWork {
        handle: Some(spawn_blocking(operation)),
    }
}

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

/// Ownership wrapper for user values retained by framework state.
///
/// Dropping the wrapper transfers the value to an isolated blocking task, so
/// framework futures never run user destruction as part of their own drop
/// glue. Callers that need to classify destruction can take the value and
/// join a dedicated blocking task explicitly.
pub(crate) struct Isolated<T: Send + 'static> {
    value: Option<T>,
}

impl<T: Send + 'static> Isolated<T> {
    pub(crate) const fn new(value: T) -> Self {
        Self { value: Some(value) }
    }

    pub(crate) fn get(&self) -> &T {
        self.value
            .as_ref()
            .expect("isolated user value was already taken")
    }

    pub(crate) fn get_mut(&mut self) -> &mut T {
        self.value
            .as_mut()
            .expect("isolated user value was already taken")
    }

    pub(crate) fn take(&mut self) -> Option<T> {
        self.value.take()
    }
}

impl<T: Send + 'static> Drop for Isolated<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            dispose_detached(value);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DisposalPanic {
    pub(crate) message: Option<String>,
}

struct DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    state: Mutex<Option<(T, C)>>,
}

impl<T, C> DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn new(value: T, completion: C) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some((value, completion))),
        })
    }

    fn finish(&self) {
        let Some((value, completion)) = self
            .state
            .lock()
            .expect("disposal job mutex poisoned")
            .take()
        else {
            return;
        };
        let panic = match catch_panic(|| drop(value)) {
            Ok(()) => None,
            Err(payload) => Some(DisposalPanic {
                message: contain_panic_payload(payload),
            }),
        };
        // Completion is framework bookkeeping. Contain it as well so a
        // hostile waker or a runtime teardown race cannot unwind a blocking
        // worker or double-panic while the job is being dropped.
        discard_panic(catch_panic(|| completion(panic)).err());
    }
}

impl<T, C> Drop for DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn drop(&mut self) {
        self.finish();
    }
}

/// Erased view of a queued disposal job for the shared fallback thread.
trait QueuedDisposal: Send + Sync {
    fn run(&self);
}

impl<T, C> QueuedDisposal for DisposalJob<T, C>
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    fn run(&self) {
        self.finish();
    }
}

/// Jobs awaiting the shared non-runtime disposal thread.
///
/// `worker_live` is only cleared by the worker after observing an empty queue
/// under this lock, and submitters push and consult it under the same lock, so
/// a queued job always has a live worker destined to drain it.
struct FallbackDisposals {
    queue: VecDeque<Arc<dyn QueuedDisposal>>,
    worker_live: bool,
}

static FALLBACK_DISPOSALS: Mutex<FallbackDisposals> = Mutex::new(FallbackDisposals {
    queue: VecDeque::new(),
    worker_live: false,
});

/// Queues a disposal job for the shared fallback thread, lazily starting it.
/// Returns `false` when no worker exists and none could be started; the
/// caller must then finish the job itself.
fn enqueue_fallback_disposal(job: Arc<dyn QueuedDisposal>) -> bool {
    let mut state = FALLBACK_DISPOSALS
        .lock()
        .expect("fallback disposal queue mutex poisoned");
    state.queue.push_back(job);
    if state.worker_live {
        return true;
    }
    // Spawning under the lock makes queueing and worker liveness one atomic
    // decision: no submitter can observe a queued job without a worker.
    match std::thread::Builder::new()
        .name("shelterwood-disposal".to_owned())
        .spawn(run_fallback_disposals)
    {
        Ok(worker) => {
            drop(worker);
            state.worker_live = true;
            true
        }
        Err(_) => {
            // The queue was empty before this push (no live worker implies an
            // empty queue), so the popped entry is exactly the failed job.
            let rejected = state.queue.pop_back();
            debug_assert!(rejected.is_some());
            false
        }
    }
}

fn run_fallback_disposals() {
    loop {
        let job = {
            let mut state = FALLBACK_DISPOSALS
                .lock()
                .expect("fallback disposal queue mutex poisoned");
            let Some(job) = state.queue.pop_front() else {
                state.worker_live = false;
                return;
            };
            job
        };
        // `DisposalJob::finish` contains destructor and completion panics
        // internally; this outer boundary keeps even an unforeseen framework
        // panic from stranding `worker_live` and the queued jobs behind it.
        discard_panic(catch_panic(|| job.run()).err());
    }
}

fn dispatch_disposal<T, C>(job: Arc<DisposalJob<T, C>>)
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    if is_available() {
        let worker = Arc::clone(&job);
        match catch_panic(|| task::spawn_blocking(move || worker.finish())) {
            Ok(handle) => {
                drop(handle);
                return;
            }
            Err(payload) => discard_panic(Some(payload)),
        }
    }

    // Outside a runtime, one shared lazily started thread drains a queue of
    // disposal jobs, so dropping N values costs at most one thread rather
    // than one thread per value, while a blocking or panicking destructor
    // still never runs on (or unwinds into) the submitting thread. The queue
    // is unbounded on purpose: applying a bound would block the submitter on
    // user destructors, exactly what isolation must prevent. Serialization is
    // the accepted trade: one blocking destructor delays later fallback
    // disposals instead of consuming another native thread.
    if enqueue_fallback_disposal(Arc::clone(&job) as Arc<dyn QueuedDisposal>) {
        return;
    }

    // Exhausted task and thread creation must not strand completion or expose
    // a destructor panic. Blocking here is the only remaining safe fallback.
    job.finish();
}

/// Runs potentially blocking user destruction away from the caller and then
/// invokes framework completion with the contained panic diagnostic.
///
/// Inside a Tokio runtime this uses the blocking pool. Outside one, jobs are
/// funneled through a single shared disposal thread, so destroying many
/// values (for example dropping a large unspawned tree) never creates one
/// native thread per value.
pub(crate) fn dispose_then<T, C>(value: T, completion: C)
where
    T: Send + 'static,
    C: FnOnce(Option<DisposalPanic>) + Send + 'static,
{
    dispatch_disposal(DisposalJob::new(value, completion));
}

/// Detaches potentially blocking or panicking user destruction from the
/// caller. The guard also contains a panic if task/thread creation itself
/// fails and drops the closure on the submitting thread.
pub(crate) fn dispose_detached<T: Send + 'static>(value: T) {
    dispose_then(value, |_| {});
}

/// Starts isolated disposal for every value and fires once all jobs finish.
///
/// Each value gets its own unwind boundary, so one destructor panic cannot
/// prevent the remaining values or the aggregate completion from running.
pub(crate) fn dispose_all<T: Send + 'static>(values: Vec<T>) -> Latch {
    let completion = Latch::default();
    if values.is_empty() {
        completion.fire();
        return completion;
    }

    let remaining = Arc::new(AtomicUsize::new(values.len()));
    for value in values {
        let remaining = Arc::clone(&remaining);
        let value_completion = completion.clone();
        dispose_then(value, move |_| {
            if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                value_completion.fire();
            }
        });
    }
    completion
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

    pub(crate) fn read_with<R>(&self, read: impl FnOnce(&T) -> R) -> R {
        read(&self.0.borrow())
    }

    pub(crate) fn pulse(&self) {
        self.0.send_modify(|_| {});
    }

    pub(crate) fn send_modify(&self, update: impl FnOnce(&mut T)) {
        self.0.send_modify(update);
    }

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

    pub(crate) fn replace(&self, value: T) {
        self.0.send_replace(value);
    }
}

impl<T: Default> WatchSender<T> {
    pub(crate) fn take(&self) -> T {
        self.0.send_replace(T::default())
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
    pub(crate) async fn changed(&mut self) {
        let _ = self.0.changed().await;
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

/// A spawned operation owned by the library.
pub(crate) struct JoinHandle<T> {
    inner: task::JoinHandle<T>,
}

#[derive(Clone)]
pub(crate) struct AbortHandle(task::AbortHandle);

impl AbortHandle {
    pub(crate) fn abort(&self) {
        self.0.abort();
    }
}

impl<T> JoinHandle<T> {
    pub(crate) fn abort_handle(&self) -> AbortHandle {
        AbortHandle(self.inner.abort_handle())
    }
}

pub(crate) enum Either<L, R> {
    Left(L),
    Right(R),
}

pub(crate) async fn select_two<A, B>(left: A, right: B) -> Either<A::Output, B::Output>
where
    A: Future + Send,
    B: Future + Send,
{
    tokio::pin!(left);
    tokio::pin!(right);
    tokio::select! {
        biased;
        value = &mut left => Either::Left(value),
        value = &mut right => Either::Right(value),
    }
}

/// The runtime-level outcome consumed by the exit classifier.
pub(crate) enum JoinOutcome<T> {
    Ok { value: T },
    Panic { message: Option<String> },
    Cancelled,
}

pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle {
        inner: task::spawn(future),
    }
}

pub(crate) fn spawn_blocking<F, T>(operation: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    JoinHandle {
        inner: task::spawn_blocking(operation),
    }
}

pub(crate) async fn join<T>(handle: JoinHandle<T>) -> JoinOutcome<T> {
    let JoinHandle { inner } = handle;
    match inner.await {
        Ok(value) => JoinOutcome::Ok { value },
        Err(error) if error.is_panic() => JoinOutcome::Panic {
            message: contain_panic_payload(error.into_panic()),
        },
        Err(error) => {
            debug_assert!(error.is_cancelled());
            JoinOutcome::Cancelled
        }
    }
}

pub(crate) async fn join_resuming<T>(handle: JoinHandle<T>) -> T {
    let JoinHandle { inner } = handle;
    match inner.await {
        Ok(value) => value,
        Err(error) if error.is_panic() => resume_unwind(error.into_panic()),
        Err(error) => {
            debug_assert!(error.is_cancelled());
            panic!("library-owned operation task was unexpectedly cancelled")
        }
    }
}

fn contain_panic_payload(payload: PanicPayload) -> Option<String> {
    let message = match catch_panic(|| panic_message(payload.as_ref())) {
        Ok(message) => message,
        Err(inspection_panic) => {
            discard_panic(Some(inspection_panic));
            None
        }
    };
    // A custom panic payload is user-owned too. Its destructor may panic, so
    // discard it under a fresh unwind boundary before publishing completion.
    discard_panic(Some(payload));
    message
}

fn panic_message(payload: &(dyn Any + Send + 'static)) -> Option<String> {
    if let Some(message) = payload.downcast_ref::<&str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}

pub(crate) async fn yield_now() {
    task::yield_now().await;
}

pub(crate) async fn sleep_until_std(deadline: std::time::Instant) {
    // Every absolute-deadline arming crosses the runtime boundary here:
    // tokio rounds the deadline up to the next whole millisecond with a
    // panicking add before tick conversion, so a deadline flush against
    // the clock limit would panic at arming time rather than when it was
    // computed. Absolute instants that arrive by another route obey the same
    // never-substitute rule as relative budgets: if this exact point cannot
    // be armed, it never arrives.
    loop {
        let current = now();
        let Some(next) = next_timer_deadline(current, deadline) else {
            return;
        };
        match Deadline::at(next).instant() {
            Some(next) => time::sleep_until(time::Instant::from_std(next)).await,
            None => std::future::pending().await,
        }
    }
}

pub(crate) enum Timeout<T> {
    Completed(T),
    Elapsed,
}

pub(crate) async fn timeout<F>(duration: Duration, future: F) -> Timeout<F::Output>
where
    F: Future,
{
    // tokio's timeout only falls back to its internal far future when the
    // deadline addition overflows outright; a representable deadline flush
    // against the clock limit would still panic at arming. Route the
    // budget through Deadline so an unarmable timeout never elapses,
    // matching sleep_deadline's overflow semantics.
    let Some(deadline) = deadline(duration).instant() else {
        return Timeout::Completed(future.await);
    };
    if duration <= MAX_TIMER_SLICE {
        return match time::timeout(duration, future).await {
            Ok(value) => Timeout::Completed(value),
            Err(_) => Timeout::Elapsed,
        };
    }
    let sleep = sleep_until_std(deadline);
    tokio::pin!(future);
    tokio::pin!(sleep);
    tokio::select! {
        // Match tokio::time::timeout's boundary rule: the operation receives
        // the first poll when it and a zero-duration timer are both ready.
        biased;
        value = &mut future => Timeout::Completed(value),
        () = &mut sleep => Timeout::Elapsed,
    }
}

pub(crate) type MpscSender<T> = mpsc::Sender<T>;
pub(crate) type MpscReceiver<T> = mpsc::Receiver<T>;
pub(crate) type UnboundedMpscSender<T> = mpsc::UnboundedSender<T>;
pub(crate) type UnboundedMpscReceiver<T> = mpsc::UnboundedReceiver<T>;

pub(crate) fn bounded_mpsc<T>(capacity: usize) -> (MpscSender<T>, MpscReceiver<T>) {
    mpsc::channel(capacity)
}

pub(crate) fn unbounded_mpsc<T>() -> (UnboundedMpscSender<T>, UnboundedMpscReceiver<T>) {
    mpsc::unbounded_channel()
}

pub(crate) async fn mpsc_send<T>(sender: &MpscSender<T>, value: T) -> Result<(), T> {
    sender.send(value).await.map_err(|error| error.0)
}

pub(crate) fn mpsc_try_send<T>(sender: &MpscSender<T>, value: T) -> Result<(), T> {
    sender.try_send(value).map_err(|error| error.into_inner())
}

pub(crate) fn mpsc_try_recv<T>(receiver: &mut MpscReceiver<T>) -> Option<T> {
    receiver.try_recv().ok()
}

pub(crate) fn unbounded_mpsc_send<T>(sender: &UnboundedMpscSender<T>, value: T) -> Result<(), T> {
    sender.send(value).map_err(|error| error.0)
}

pub(crate) async fn unbounded_mpsc_recv<T>(receiver: &mut UnboundedMpscReceiver<T>) -> Option<T> {
    receiver.recv().await
}

pub(crate) fn unbounded_mpsc_try_recv<T>(receiver: &mut UnboundedMpscReceiver<T>) -> Option<T> {
    receiver.try_recv().ok()
}

pub(crate) enum ScopeWake<T> {
    Signal,
    ParentShutdown,
    Message(Option<T>),
    Deadline,
}

pub(crate) struct ScopeWait<S, C> {
    pub(crate) signal: S,
    pub(crate) parent_shutdown: C,
}

pub(crate) async fn wait_scope<S, C, T>(
    wait: ScopeWait<S, C>,
    receiver: &mut MpscReceiver<T>,
    deadline: Option<std::time::Instant>,
) -> ScopeWake<T>
where
    S: Future<Output = ()> + Send,
    C: Future<Output = ()> + Send,
{
    let ScopeWait {
        signal,
        parent_shutdown,
    } = wait;
    tokio::pin!(signal);
    tokio::pin!(parent_shutdown);
    let deadline = async move {
        if let Some(deadline) = deadline {
            sleep_until_std(deadline).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        () = &mut signal => ScopeWake::Signal,
        () = &mut parent_shutdown => ScopeWake::ParentShutdown,
        message = receiver.recv() => ScopeWake::Message(message),
        () = &mut deadline => ScopeWake::Deadline,
    }
}

#[derive(Debug)]
pub(crate) struct JitterRng(fastrand::Rng);

impl JitterRng {
    pub(crate) fn from_system_entropy() -> Self {
        Self(fastrand::Rng::with_seed(fastrand::u64(..)))
    }

    pub(crate) fn sample<R>(&mut self, range: R) -> u64
    where
        R: RangeBounds<u64>,
    {
        self.0.u64(range)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use super::{
        DisposalJob, DisposalPanic, JoinOutcome, Latch, MAX_TIMER_SLICE, OneShotClose, Signal,
        Timeout, discard_panic, join, next_timer_deadline, oneshot, spawn, timeout, yield_now,
    };

    fn latest_representable(started_at: std::time::Instant) -> std::time::Instant {
        let mut low = Duration::ZERO;
        let mut high = Duration::MAX;
        assert!(started_at.checked_add(high).is_none());
        while high - low > Duration::from_nanos(1) {
            let mid = low + (high - low) / 2;
            if started_at.checked_add(mid).is_some() {
                low = mid;
            } else {
                high = mid;
            }
        }
        started_at + low
    }

    #[tokio::test(start_paused = true)]
    async fn unarmable_absolute_deadline_stays_pending_without_substitution() {
        let now = std::time::Instant::now();
        let flush = latest_representable(now);
        let mut sleep = std::pin::pin!(super::sleep_until_std(flush));
        let mut context = Context::from_waker(Waker::noop());
        // The timer registers on first poll: passing this instant to tokio
        // would panic during its millisecond round-up rather than parking.
        assert!(sleep.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    fn deadline_beyond_tokios_tick_range_is_armed_in_a_bounded_slice() {
        // Tokio 1.53 reserves the top three u64 millisecond ticks. The exact
        // value is test evidence only: production uses a small stable slice
        // rather than depending on tokio's private sentinel.
        let beyond_tokio_ticks = Duration::from_millis(u64::MAX - 2);
        let current = std::time::Instant::now();
        let Some(requested) = current.checked_add(beyond_tokio_ticks) else {
            // Some platforms have a narrower Instant domain than Tokio's
            // u64 millisecond tick range, so this boundary cannot be tested.
            return;
        };

        assert_eq!(
            next_timer_deadline(current, requested),
            current.checked_add(MAX_TIMER_SLICE)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_beyond_tokios_tick_range_does_not_fire_at_the_first_slice() {
        let current = super::now();
        let Some(requested) = current.checked_add(Duration::from_millis(u64::MAX - 2)) else {
            // See `deadline_beyond_tokios_tick_range_is_armed_in_a_bounded_slice`.
            return;
        };
        let mut sleep = std::pin::pin!(super::sleep_until_std(requested));
        let mut context = Context::from_waker(Waker::noop());

        assert!(sleep.as_mut().poll(&mut context).is_pending());
        tokio::time::advance(MAX_TIMER_SLICE).await;
        assert!(
            sleep.as_mut().poll(&mut context).is_pending(),
            "finishing an internal slice must not finish the requested sleep"
        );
    }

    #[test]
    fn already_due_clock_limit_needs_no_timer_arm() {
        let edge = latest_representable(std::time::Instant::now());

        assert_eq!(next_timer_deadline(edge, edge), None);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_with_an_unarmable_budget_never_elapses() {
        let now = super::now();
        let flush = latest_representable(now);
        // The paused clock is frozen, so the budget reconstructs the flush
        // deadline exactly and the unarmable-budget guard must engage.
        let budget = flush - now;
        let mut timeout = std::pin::pin!(timeout(budget, std::future::pending::<()>()));
        let mut context = Context::from_waker(Waker::noop());
        // Without the guard, tokio's timeout armed this representable
        // deadline and panicked inside the millisecond round-up.
        assert!(timeout.as_mut().poll(&mut context).is_pending());
    }

    #[tokio::test]
    async fn scope_wait_prefers_signal_when_both_control_futures_are_ready() {
        let (_sender, mut receiver) = super::bounded_mpsc::<()>(1);

        let wake = super::wait_scope(
            super::ScopeWait {
                signal: std::future::ready(()),
                parent_shutdown: std::future::ready(()),
            },
            &mut receiver,
            None,
        )
        .await;

        assert!(matches!(wake, super::ScopeWake::Signal));
    }

    struct RecursivelyPanickingPayload;

    impl Drop for RecursivelyPanickingPayload {
        fn drop(&mut self) {
            std::panic::panic_any(RecursivelyPanickingPayload);
        }
    }

    #[test]
    fn discarding_a_recursively_hostile_panic_payload_is_contained() {
        discard_panic(Some(Box::new(RecursivelyPanickingPayload)));
    }

    struct PanickingDrop(Arc<AtomicUsize>);

    impl Drop for PanickingDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("cancelled disposal job payload");
        }
    }

    #[test]
    fn dropping_an_unstarted_disposal_job_contains_panic_and_completes_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let diagnostic = Arc::new(Mutex::new(None));
        let completion_diagnostic = Arc::clone(&diagnostic);
        let job = DisposalJob::new(PanickingDrop(Arc::clone(&drops)), move |panic| {
            *completion_diagnostic
                .lock()
                .expect("diagnostic mutex poisoned") = Some(panic);
        });

        drop(job);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let diagnostic = diagnostic.lock().expect("diagnostic mutex poisoned");
        assert!(matches!(
            diagnostic.as_ref(),
            Some(Some(DisposalPanic {
                message: Some(message)
            })) if message == "cancelled disposal job payload"
        ));
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
