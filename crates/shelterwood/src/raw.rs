//! Minimal loop-owning raw actors and their incarnation context.

use std::{
    any::{Any, TypeId},
    collections::{BTreeSet, HashMap, VecDeque, hash_map::RandomState},
    fmt,
    future::Future,
    hash::{BuildHasher, Hash},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskPollContext, Poll},
    time::{Duration, Instant},
};

use crate::{
    ActorRef, CancellationToken, ChildId, ExitResult, Incarnation, Mailbox, MailboxShutdown,
    PolicyError, Readiness, ReadinessDeadline, RestartPolicy, Retention, ScopeRef, Shutdown,
    driver::{ActorWork, Signal, SignalWatcher},
    mailbox::{MailboxCell, MailboxControl, MailboxReceiver},
    policy::CommonOptions,
    runtime::Latch,
};

type PanicPayload = Box<dyn Any + Send + 'static>;
type DeferredMessage<M> = Box<dyn FnOnce() -> M + Send + 'static>;
type OffloadFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type SharedWork = Arc<SharedOffloadState>;

fn discard_panic(payload: Option<PanicPayload>) {
    if let Some(payload) = payload {
        let _ = catch_unwind(AssertUnwindSafe(|| drop(payload)));
    }
}

fn keep_first_panic(first: &mut Option<PanicPayload>, candidate: Option<PanicPayload>) {
    if first.is_none() {
        *first = candidate;
    } else {
        discard_panic(candidate);
    }
}

fn resume_preferred_panic(primary: Option<PanicPayload>, cleanup: Option<PanicPayload>) {
    if let Some(payload) = primary {
        discard_panic(cleanup);
        resume_unwind(payload);
    }
    if let Some(payload) = cleanup {
        resume_unwind(payload);
    }
}

/// Marker returned to an offload continuation when its one deadline expires.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the offload deadline elapsed")]
pub struct DeadlineElapsed;

/// An operation rejected because the actor incarnation is already stopping.
#[derive(Eq, PartialEq)]
pub struct Rejected<T> {
    payload: T,
}

impl<T> fmt::Debug for Rejected<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Rejected").finish_non_exhaustive()
    }
}

impl<T> Rejected<T> {
    pub(crate) fn new(payload: T) -> Self {
        Self { payload }
    }

    /// Recovers the operation payload that was never accepted.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.payload
    }
}

/// An owned cancel-on-drop lease for a scoped offload.
#[must_use = "dropping the guard cancels its offload; call detach to keep only incarnation ownership"]
pub struct Guard {
    cancellation: Latch,
    finished: Latch,
    armed: bool,
}

impl fmt::Debug for Guard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Guard")
            .field("detached", &!self.armed)
            .finish()
    }
}

impl Guard {
    /// Reports whether cancellation has been requested for this lease.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_fired()
    }

    /// Reports whether the work completed or incarnation teardown requested cancellation.
    ///
    /// A teardown notification is not a join: under hard abort the task may
    /// still be unwinding when this becomes true.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished.is_fired()
    }

    /// Waits for work completion or an incarnation-teardown cancellation request.
    ///
    /// This notification does not join work that is being hard-aborted.
    pub async fn finished(&self) {
        self.finished.fired().await;
    }

    /// Cancels the guarded offload immediately and consumes the guard.
    pub fn cancel(mut self) {
        self.cancellation.fire();
        self.armed = false;
    }

    /// Releases this lease's cancel-on-drop behavior.
    pub fn detach(mut self) {
        self.armed = false;
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.fire();
        }
    }
}

/// A blocking operation whose thread cooperatively observes actor cancellation.
///
/// Cancellation cannot forcibly stop a blocking thread. Dropping this future
/// or hard-aborting its actor detaches the thread after requesting cooperative
/// cancellation; Shelterwood does not join it, and any later value or panic is
/// discarded. The operation must therefore be safe to outlive its actor.
#[must_use = "dropping this future requests cooperative cancellation and detaches the thread"]
pub struct Blocking<T> {
    future: Pin<Box<dyn Future<Output = T> + Send + 'static>>,
    cancellation: Latch,
    completed: bool,
}

impl<T> fmt::Debug for Blocking<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Blocking")
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}

impl<T> Future for Blocking<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskPollContext<'_>) -> Poll<Self::Output> {
        match self.future.as_mut().poll(context) {
            Poll::Ready(value) => {
                self.completed = true;
                Poll::Ready(value)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for Blocking<T> {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellation.fire();
        }
    }
}

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
        let polled = catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("a completed panic boundary was polled again")
                .as_mut()
                .poll(context)
        }));
        match polled {
            Ok(Poll::Ready(value)) => {
                let future = this.future.take();
                match catch_unwind(AssertUnwindSafe(|| drop(future))) {
                    Ok(()) => Poll::Ready(Ok(value)),
                    Err(payload) => Poll::Ready(Err(payload)),
                }
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                let future = this.future.take();
                let _ = catch_unwind(AssertUnwindSafe(|| drop(future)));
                Poll::Ready(Err(payload))
            }
        }
    }
}

impl<F> Drop for CatchUnwindFuture<F> {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        let future = self.future.take();
        let panic = catch_unwind(AssertUnwindSafe(|| drop(future))).err();
        if !already_panicking && let Some(payload) = panic {
            resume_unwind(payload);
        }
    }
}

enum QueuedEvent<M> {
    Deliver {
        cancellation: Latch,
        make_message: DeferredMessage<M>,
    },
}

#[derive(Default)]
struct PanicSlot {
    payload: Mutex<Option<PanicPayload>>,
}

impl PanicSlot {
    fn record(&self, payload: PanicPayload) {
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

    fn take(&self) -> Option<PanicPayload> {
        self.payload
            .lock()
            .expect("offload panic mutex poisoned")
            .take()
    }
}

struct EventQueue<M> {
    // Timer batches snapshot the number of currently queued events. Insertion
    // and the snapshot share this lock, so FIFO order itself is the sequence
    // and there is no integer counter whose saturation could blur a boundary.
    queue: Mutex<VecDeque<QueuedEvent<M>>>,
    signal: Signal,
}

impl<M> Default for EventQueue<M> {
    fn default() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            signal: Signal::default(),
        }
    }
}

impl<M> EventQueue<M> {
    fn push(&self, event: QueuedEvent<M>) {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .push_back(event);
        self.signal.pulse();
    }

    #[cfg(test)]
    fn insert_with(&self, event: QueuedEvent<M>, before_insert: impl FnOnce()) {
        let mut queue = self.queue.lock().expect("actor event queue mutex poisoned");
        before_insert();
        queue.push_back(event);
    }

    #[cfg(test)]
    fn push_with_hooks(
        &self,
        event: QueuedEvent<M>,
        before_insert: impl FnOnce(),
        after_insert: impl FnOnce(),
    ) {
        self.insert_with(event, before_insert);
        after_insert();
        self.signal.pulse();
    }

    fn watermark(&self) -> usize {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .len()
    }

    fn pop(&self) -> Option<QueuedEvent<M>> {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .pop_front()
    }

    fn pop_through(&self, remaining: &mut usize) -> Option<QueuedEvent<M>> {
        if *remaining == 0 {
            None
        } else {
            let event = self
                .queue
                .lock()
                .expect("actor event queue mutex poisoned")
                .pop_front();
            debug_assert!(event.is_some(), "a timer watermark covers queued events");
            if event.is_some() {
                *remaining -= 1;
            } else {
                *remaining = 0;
            }
            event
        }
    }

    fn clear(&self) {
        let queue = {
            let mut queue = self.queue.lock().expect("actor event queue mutex poisoned");
            std::mem::take(&mut *queue)
        };
        drop(queue);
    }
}

trait ErasedTimerKey: Send {
    fn as_any(&self) -> &dyn Any;
}

struct StoredTimerKey<K>(K);

impl<K: Send + 'static> ErasedTimerKey for StoredTimerKey<K> {
    fn as_any(&self) -> &dyn Any {
        &self.0
    }
}

enum TimerMessage<M> {
    Once(Option<M>),
    Interval(Box<dyn Fn() -> M + Send + 'static>),
}

struct TimerEntry<M> {
    key: Box<dyn ErasedTimerKey>,
    /// `None` when the requested delay overflows the clock: a deadline that
    /// never arrives, mirroring the offload path — never "due now".
    deadline: Option<Instant>,
    arming_order: u64,
    message: TimerMessage<M>,
    period: Option<Duration>,
}

/// Type-aware keyed timer lookup paired with an independently ordered
/// deadline index.
///
/// Type identity participates in the key hash, so equal values of different
/// key types remain distinct. A hash bucket still verifies erased equality to
/// preserve `Eq` semantics in the unlikely event of a collision.
struct TimerStore<M> {
    key_hasher: RandomState,
    keyed: HashMap<u64, Vec<TimerEntry<M>>>,
    armings: HashMap<u64, u64>,
    deadlines: BTreeSet<(Instant, u64)>,
    #[cfg(test)]
    lookup_probes: usize,
}

impl<M> Default for TimerStore<M> {
    fn default() -> Self {
        Self {
            key_hasher: RandomState::new(),
            keyed: HashMap::new(),
            armings: HashMap::new(),
            deadlines: BTreeSet::new(),
            #[cfg(test)]
            lookup_probes: 0,
        }
    }
}

impl<M> TimerStore<M> {
    fn hash_key<K: Hash + 'static>(&self, key: &K) -> u64 {
        self.key_hasher.hash_one((TypeId::of::<K>(), key))
    }

    fn is_empty(&self) -> bool {
        self.armings.is_empty()
    }

    fn clear(&mut self) {
        self.keyed.clear();
        self.armings.clear();
        self.deadlines.clear();
    }

    fn replace<K>(
        &mut self,
        key: K,
        deadline: Option<Instant>,
        arming_order: u64,
        message: TimerMessage<M>,
        period: Option<Duration>,
    ) where
        K: Hash + Eq + Send + 'static,
    {
        let _ = self.remove(&key);
        let hash = self.hash_key(&key);
        self.keyed.entry(hash).or_default().push(TimerEntry {
            key: Box::new(StoredTimerKey(key)),
            deadline,
            arming_order,
            message,
            period,
        });
        let previous = self.armings.insert(arming_order, hash);
        debug_assert!(previous.is_none());
        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, arming_order));
        }
    }

    fn remove<K>(&mut self, key: &K) -> Option<TimerEntry<M>>
    where
        K: Hash + Eq + 'static,
    {
        let hash = self.hash_key(key);
        let (entry, empty) = {
            let bucket = self.keyed.get_mut(&hash)?;
            #[cfg(test)]
            let mut probes = 0;
            let index = bucket.iter().position(|entry| {
                #[cfg(test)]
                {
                    probes += 1;
                }
                entry.key.as_any().downcast_ref::<K>() == Some(key)
            })?;
            #[cfg(test)]
            {
                self.lookup_probes = self.lookup_probes.saturating_add(probes);
            }
            let entry = bucket.swap_remove(index);
            (entry, bucket.is_empty())
        };
        if empty {
            self.keyed.remove(&hash);
        }
        self.armings.remove(&entry.arming_order);
        if let Some(deadline) = entry.deadline {
            self.deadlines.remove(&(deadline, entry.arming_order));
        }
        Some(entry)
    }

    fn remove_arming(&mut self, arming_order: u64) -> Option<TimerEntry<M>> {
        let hash = self.armings.remove(&arming_order)?;
        let (entry, empty) = {
            let bucket = self
                .keyed
                .get_mut(&hash)
                .expect("an arming index must reference a key bucket");
            let index = bucket
                .iter()
                .position(|entry| entry.arming_order == arming_order)
                .expect("an arming index must reference a timer");
            let entry = bucket.swap_remove(index);
            (entry, bucket.is_empty())
        };
        if empty {
            self.keyed.remove(&hash);
        }
        if let Some(deadline) = entry.deadline {
            self.deadlines.remove(&(deadline, arming_order));
        }
        Some(entry)
    }

    fn entry_mut(&mut self, arming_order: u64) -> Option<&mut TimerEntry<M>> {
        let hash = *self.armings.get(&arming_order)?;
        let entry = self
            .keyed
            .get_mut(&hash)
            .expect("an arming index must reference a key bucket")
            .iter_mut()
            .find(|entry| entry.arming_order == arming_order)
            .expect("an arming index must reference a timer");
        Some(entry)
    }

    fn take_due(&mut self, now: Instant) -> VecDeque<u64> {
        let due = self
            .deadlines
            .range(..=(now, u64::MAX))
            .copied()
            .collect::<Vec<_>>();
        for deadline in &due {
            self.deadlines.remove(deadline);
        }
        due.into_iter().map(|(_, arming)| arming).collect()
    }

    fn arm_deadline(&mut self, arming_order: u64, deadline: Option<Instant>) {
        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, arming_order));
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }
}

struct FiredTimerBatch {
    armings: VecDeque<u64>,
    continuations_remaining: usize,
    mailbox_through: u64,
    mailbox_complete: bool,
    offloads_remaining: usize,
    offloads_complete: bool,
}

struct OffloadFutureState {
    future: Option<OffloadFuture>,
    polling: bool,
    cancelled: bool,
}

struct SharedOffloadState {
    // Polling takes the future out of this mutex. Cancellation either takes
    // an idle future or marks an in-progress poll so that the poller disposes
    // it, always after releasing the lock.
    state: Mutex<OffloadFutureState>,
    panic: Arc<PanicSlot>,
    signal: Signal,
    finished: Latch,
}

impl SharedOffloadState {
    fn new(
        future: OffloadFuture,
        panic: Arc<PanicSlot>,
        signal: Signal,
        finished: Latch,
    ) -> SharedWork {
        Arc::new(Self {
            state: Mutex::new(OffloadFutureState {
                future: Some(future),
                polling: false,
                cancelled: false,
            }),
            panic,
            signal,
            finished,
        })
    }

    fn take_for_poll(&self) -> Option<OffloadFuture> {
        let mut state = self.state.lock().expect("offload future mutex poisoned");
        if state.cancelled {
            return None;
        }
        debug_assert!(!state.polling, "offload work must have one poller");
        state.polling = true;
        state.future.take()
    }

    fn finish_poll(&self, future: OffloadFuture, pending: bool) -> Option<OffloadFuture> {
        let mut state = self.state.lock().expect("offload future mutex poisoned");
        state.polling = false;
        if pending && !state.cancelled {
            debug_assert!(state.future.is_none());
            state.future = Some(future);
            None
        } else {
            Some(future)
        }
    }

    fn record(&self, payload: PanicPayload) {
        self.panic.record(payload);
        // Dropping a losing or cancelled operation can panic after its body
        // has stopped running, so every retained panic must wake the actor's
        // control plane independently of ordinary event delivery.
        self.signal.pulse();
    }

    fn dispose(&self, future: Option<OffloadFuture>) {
        if let Some(future) = future {
            match catch_unwind(AssertUnwindSafe(|| drop(future))) {
                Ok(()) => {}
                Err(payload) => self.record(payload),
            }
        }
    }

    fn cancel(&self) {
        let future = {
            let mut state = self.state.lock().expect("offload future mutex poisoned");
            state.cancelled = true;
            if state.polling {
                None
            } else {
                state.future.take()
            }
        };
        self.dispose(future);
        self.finished.fire();
    }
}

struct SharedOffloadFuture(SharedWork);

impl Future for SharedOffloadFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut TaskPollContext<'_>) -> Poll<Self::Output> {
        let Some(mut future) = self.0.take_for_poll() else {
            return Poll::Ready(());
        };
        let polled = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context)));
        match polled {
            Ok(Poll::Pending) => {
                let dispose = self.0.finish_poll(future, true);
                if dispose.is_some() {
                    self.0.dispose(dispose);
                    self.0.finished.fire();
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            Ok(Poll::Ready(())) => {
                let dispose = self.0.finish_poll(future, false);
                self.0.dispose(dispose);
                self.0.finished.fire();
                Poll::Ready(())
            }
            Err(payload) => {
                let dispose = self.0.finish_poll(future, false);
                self.0.record(payload);
                self.0.dispose(dispose);
                self.0.finished.fire();
                Poll::Ready(())
            }
        }
    }
}

struct OffloadResource {
    cancellation: Latch,
    finished: Latch,
    state: Option<SharedWork>,
    task: Option<ActorWork>,
}

impl OffloadResource {
    fn cancel(&mut self) {
        self.cancellation.fire();
        if let Some(state) = &self.state {
            state.cancel();
        }
        if let Some(task) = &self.task {
            task.abort();
        }
        self.finished.fire();
    }
}

struct RawResources<M> {
    accepting: bool,
    continuations: VecDeque<M>,
    continuation_needs_external: bool,
    timers: TimerStore<M>,
    next_timer_order: u64,
    fired_batch: Option<FiredTimerBatch>,
    events: Arc<EventQueue<M>>,
    panic: Arc<PanicSlot>,
    event_watcher: SignalWatcher,
    offloads: Vec<OffloadResource>,
}

impl<M> Default for RawResources<M> {
    fn default() -> Self {
        let events = Arc::new(EventQueue::default());
        let event_watcher = events.signal.watcher();
        Self {
            accepting: true,
            continuations: VecDeque::new(),
            continuation_needs_external: false,
            timers: TimerStore::default(),
            next_timer_order: 0,
            fired_batch: None,
            events,
            panic: Arc::new(PanicSlot::default()),
            event_watcher,
            offloads: Vec::new(),
        }
    }
}

impl<M> RawResources<M> {
    fn freeze(&mut self) -> usize {
        if !self.accepting {
            return 0;
        }
        self.accepting = false;
        let dropped_continuations = self.continuations.len();
        // Cancellation catches each future's destructor independently, so a
        // failure cannot prevent later offloads from being cancelled/signalled.
        for offload in &mut self.offloads {
            offload.cancel();
        }
        self.continuations.clear();
        self.timers.clear();
        self.fired_batch = None;
        self.events.clear();
        dropped_continuations
    }

    fn resume_pending_panic(&self) {
        if let Some(payload) = self.panic.take() {
            resume_unwind(payload);
        }
    }

    async fn join_offloads(&mut self) {
        for offload in &mut self.offloads {
            if let Some(task) = offload.task.take() {
                task.join().await;
            }
        }
        self.events.clear();
        self.offloads.clear();
    }
}

impl<M> Drop for RawResources<M> {
    fn drop(&mut self) {
        let freeze_panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = self.freeze();
        }))
        .err();
        let mut cleanup_panic = self.panic.take();
        keep_first_panic(&mut cleanup_panic, freeze_panic);
        if std::thread::panicking() {
            discard_panic(cleanup_panic);
        } else if let Some(payload) = cleanup_panic {
            resume_unwind(payload);
        }
    }
}

/// Minimal actor contract for application-owned receive loops.
pub trait RawActor: Send + 'static {
    /// Message accepted by this actor.
    type Msg: Send + 'static;

    /// Declares when this actor becomes ready. Read before `run` is polled.
    fn readiness(&self) -> Readiness {
        Readiness::Immediate
    }

    /// Runs one incarnation using the membership-owned mailbox binding.
    ///
    /// [`RawContext::recv`] freezes external intake and returns `None` when
    /// shutdown begins. A raw loop must then honor
    /// [`RawContext::mailbox_shutdown`]: for
    /// [`MailboxShutdown::Drain`], repeatedly call [`RawContext::try_recv`] to
    /// handle the frozen accepted prefix; for [`MailboxShutdown::Discard`],
    /// return without draining. The high-level [`crate::Actor`] loop implements
    /// this policy automatically.
    fn run(
        &mut self,
        context: &mut RawContext<Self::Msg>,
    ) -> impl Future<Output = ExitResult> + Send;
}

/// Per-incarnation capabilities supplied to a [`RawActor`].
pub struct RawContext<M> {
    id: ChildId,
    incarnation: Incarnation,
    myself: ActorRef<M>,
    scope: ScopeRef,
    shutdown: CancellationToken,
    abort: CancellationToken,
    ready: Latch,
    local_stop: Latch,
    readiness: Readiness,
    mailbox_shutdown: MailboxShutdown,
    mailbox: Arc<MailboxCell<M>>,
    receiver: MailboxReceiver<M>,
    resources: RawResources<M>,
}

impl<M> fmt::Debug for RawContext<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawContext")
            .field("id", &self.id)
            .field("incarnation", &self.incarnation)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> RawContext<M> {
    fn new(
        run: RawRunContext,
        myself: ActorRef<M>,
        mailbox: Arc<MailboxCell<M>>,
        readiness: Readiness,
    ) -> Self {
        Self {
            id: run.id,
            incarnation: run.incarnation,
            myself,
            scope: run.scope,
            shutdown: CancellationToken::from_latch(run.shutdown),
            abort: CancellationToken::from_latch(run.abort),
            ready: run.ready,
            local_stop: run.local_stop,
            readiness,
            mailbox_shutdown: run.mailbox_shutdown,
            mailbox: Arc::clone(&mailbox),
            receiver: MailboxReceiver::new(mailbox, run.incarnation),
            resources: RawResources::default(),
        }
    }

    /// Returns this actor's child id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        &self.id
    }

    /// Returns this actor's current incarnation.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Returns a membership-addressed handle to this actor.
    #[must_use]
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
    }

    /// Returns the actor's supervising scope.
    #[must_use]
    pub fn scope(&self) -> ScopeRef {
        self.scope.clone()
    }

    /// Returns the cooperative shutdown token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Returns the escalation token.
    #[must_use]
    pub fn abort_token(&self) -> CancellationToken {
        self.abort.clone()
    }

    /// Requests shutdown of the supervising scope without waiting.
    pub fn request_scope_shutdown(&self) {
        self.scope.request_shutdown();
    }

    /// Returns the resolved frozen-prefix shutdown policy.
    #[must_use]
    pub fn mailbox_shutdown(&self) -> MailboxShutdown {
        self.mailbox_shutdown
    }

    /// Returns the engine-resolved effective readiness mode for this
    /// incarnation: the definition-level override when one was given,
    /// otherwise the actor's declared mode. This is the single source the
    /// gate is driven by (§6) — decorators and the blanket handler loop
    /// consult it rather than re-deriving their own.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        self.readiness
    }

    /// Releases this incarnation's readiness gate.
    pub fn mark_ready(&self) {
        if !self.is_stopping() {
            self.ready.fire();
        }
    }

    /// Requests a clean self-stop of this incarnation.
    ///
    /// External intake freezes at this call — the drained set is exactly the
    /// already-accepted prefix (§5.1's close point) — queued continuations
    /// and timers are discarded, and `recv` begins yielding the frozen
    /// prefix followed by `None`. Idempotent. This is the primitive the
    /// blanket handler loop's `Context::stop` is built on (§1 principle 5);
    /// the child's configured §10 ladder bounds the stop.
    pub fn stop(&mut self) {
        self.mailbox.freeze(self.incarnation);
        self.freeze_and_report();
        self.local_stop.fire();
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.local_stop.is_fired() || self.shutdown.is_cancelled()
    }

    /// Queues an actor-local continuation ahead of external input.
    pub fn continue_with(&mut self, message: M) -> Result<(), Rejected<M>> {
        if self.is_stopping() || !self.resources.accepting {
            return Err(Rejected::new(message));
        }
        self.resources.continuations.push_back(message);
        Ok(())
    }

    /// Arms or replaces a one-shot keyed timer.
    pub fn set_timeout<K>(
        &mut self,
        key: K,
        message: M,
        after: Duration,
    ) -> Result<(), Rejected<(K, M)>>
    where
        K: Hash + Eq + Send + 'static,
    {
        if self.is_stopping() || !self.resources.accepting {
            return Err(Rejected::new((key, message)));
        }
        self.replace_timer(key, TimerMessage::Once(Some(message)), after, None);
        Ok(())
    }

    /// Arms or replaces a keyed interval; a zero period clears the key.
    pub fn set_interval<K>(
        &mut self,
        key: K,
        message: M,
        period: Duration,
    ) -> Result<(), Rejected<(K, M)>>
    where
        K: Hash + Eq + Send + 'static,
        M: Clone,
    {
        if self.is_stopping() || !self.resources.accepting {
            return Err(Rejected::new((key, message)));
        }
        if period.is_zero() {
            let _ = self.clear_timer(&key);
            return Ok(());
        }
        let make_message = Box::new(move || message.clone());
        self.replace_timer(
            key,
            TimerMessage::Interval(make_message),
            period,
            Some(period),
        );
        Ok(())
    }

    /// Retracts a keyed timer, including an elapsed timer not yet delivered.
    pub fn clear_timer<K>(&mut self, key: &K) -> bool
    where
        K: Hash + Eq + Send + 'static,
    {
        self.resources.timers.remove(key).is_some()
    }

    /// Starts incarnation-owned async work with one total deadline budget.
    pub fn offload<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: Duration,
    ) -> Result<(), Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> M + Send + 'static,
    {
        self.start_offload(work, continuation, deadline, false)
            .map(|_| ())
    }

    /// Starts guarded incarnation-owned async work with one deadline budget.
    pub fn offload_scoped<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: Duration,
    ) -> Result<Guard, Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> M + Send + 'static,
    {
        self.start_offload(work, continuation, deadline, true)
            .map(|guard| guard.expect("scoped offload must produce a guard"))
    }

    /// Starts blocking work with cancellation tied to shutdown and future drop.
    ///
    /// Cancellation is cooperative. If this future is dropped or its actor is
    /// hard-aborted, the OS thread detaches and can outlive the incarnation.
    pub fn run_blocking<F, T>(&self, operation: F) -> Blocking<T>
    where
        F: FnOnce(CancellationToken) -> T + Send + 'static,
        T: Send + 'static,
    {
        let cancellation = Latch::default();
        let token = self.shutdown.child(cancellation.clone());
        let work = crate::driver::spawn_blocking_work(move || operation(token));
        Blocking {
            future: Box::pin(async move { work.join().await }),
            cancellation,
            completed: false,
        }
    }

    /// Receives the next accepted message, biased toward shutdown.
    pub async fn recv(&mut self) -> Option<M> {
        loop {
            if self.local_stop.is_fired() {
                self.freeze_and_report();
                self.shutdown.cancelled().await;
                return None;
            }
            if self.shutdown.is_cancelled() {
                self.freeze_and_report();
                return None;
            }
            if let Some(message) = self.next_ready(false) {
                return Some(message);
            }
            self.wait_for_event().await;
        }
    }

    /// Receives one ready event without awaiting or consulting shutdown.
    pub fn try_recv(&mut self) -> Option<M> {
        if self.is_stopping() {
            self.freeze_and_report();
            self.receiver.try_recv()
        } else {
            self.next_ready(false)
        }
    }

    fn replace_timer<K>(
        &mut self,
        key: K,
        message: TimerMessage<M>,
        after: Duration,
        period: Option<Duration>,
    ) where
        K: Hash + Eq + Send + 'static,
    {
        self.resources.next_timer_order = self.resources.next_timer_order.saturating_add(1);
        let now = crate::driver::now();
        let deadline = crate::deadline::Deadline::after(now, after).instant();
        self.resources.timers.replace(
            key,
            deadline,
            self.resources.next_timer_order,
            message,
            period,
        );
    }

    fn start_offload<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: Duration,
        scoped: bool,
    ) -> Result<Option<Guard>, Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> M + Send + 'static,
    {
        if self.is_stopping() || !self.resources.accepting {
            return Err(Rejected::new((work, continuation)));
        }
        // Completed offloads no longer need their resources; prune them here
        // so a long-lived incarnation's ledger stays O(in-flight), not
        // O(offloads-ever-issued).
        self.resources
            .offloads
            .retain(|offload| !offload.finished.is_fired());

        let cancellation = Latch::default();
        let finished = Latch::default();
        let guard = scoped.then(|| Guard {
            cancellation: cancellation.clone(),
            finished: finished.clone(),
            armed: true,
        });
        let events = Arc::clone(&self.resources.events);
        let panic = Arc::clone(&self.resources.panic);
        if deadline.is_zero() {
            drop(work);
            events.push(QueuedEvent::Deliver {
                cancellation: cancellation.clone(),
                make_message: Box::new(move || continuation(Err(DeadlineElapsed))),
            });
            finished.fire();
            self.resources.offloads.push(OffloadResource {
                cancellation,
                finished,
                state: None,
                task: None,
            });
            return Ok(guard);
        }

        let token = self.shutdown.child(cancellation.clone());
        let started_at = crate::driver::now();
        let expires_at = crate::deadline::Deadline::after(started_at, deadline).instant();
        let event_cancellation = cancellation.clone();
        let operation = async move {
            let completion = async move {
                let work = CatchUnwindFuture::new(work);
                if let Some(expires_at) = expires_at {
                    match crate::driver::select(work, crate::driver::sleep_until(expires_at)).await
                    {
                        crate::driver::Selected::First(result) => result.map(Ok),
                        crate::driver::Selected::Second(()) => Ok(Err(DeadlineElapsed)),
                    }
                } else {
                    work.await.map(Ok)
                }
            };
            match crate::driver::select(token.cancelled(), completion).await {
                crate::driver::Selected::First(()) => {}
                crate::driver::Selected::Second(Ok(result)) => {
                    events.push(QueuedEvent::Deliver {
                        cancellation: event_cancellation,
                        make_message: Box::new(move || continuation(result)),
                    });
                }
                crate::driver::Selected::Second(Err(payload)) => {
                    panic.record(payload);
                    events.signal.pulse();
                }
            }
        };
        let state = SharedOffloadState::new(
            Box::pin(operation),
            Arc::clone(&self.resources.panic),
            self.resources.events.signal.clone(),
            finished.clone(),
        );
        let task = crate::driver::spawn_actor_work(SharedOffloadFuture(Arc::clone(&state)));
        self.resources.offloads.push(OffloadResource {
            cancellation,
            finished,
            state: Some(state),
            task: Some(task),
        });
        Ok(guard)
    }

    fn next_ready(&mut self, allow_frozen_mailbox: bool) -> Option<M> {
        loop {
            self.resources.resume_pending_panic();
            self.begin_fired_batch();
            if let Some(mut batch) = self.resources.fired_batch.take() {
                if !self.resources.continuation_needs_external
                    && batch.continuations_remaining > 0
                    && let Some(message) = self.resources.continuations.pop_front()
                {
                    batch.continuations_remaining -= 1;
                    self.resources.continuation_needs_external = true;
                    self.resources.fired_batch = Some(batch);
                    return Some(message);
                }

                if !batch.mailbox_complete {
                    let message = if allow_frozen_mailbox {
                        self.receiver.try_recv()
                    } else {
                        self.receiver.try_recv_live_through(batch.mailbox_through)
                    };
                    if let Some(message) = message {
                        self.resources.continuation_needs_external = false;
                        self.resources.fired_batch = Some(batch);
                        return Some(message);
                    }
                    batch.mailbox_complete = true;
                }

                if !batch.offloads_complete {
                    while let Some(event) = self
                        .resources
                        .events
                        .pop_through(&mut batch.offloads_remaining)
                    {
                        if let Some(message) = Self::materialize_event(event) {
                            self.resources.continuation_needs_external = false;
                            self.resources.fired_batch = Some(batch);
                            return Some(message);
                        }
                    }
                    batch.offloads_complete = true;
                }

                if batch.continuations_remaining > 0
                    && let Some(message) = self.resources.continuations.pop_front()
                {
                    batch.continuations_remaining -= 1;
                    self.resources.continuation_needs_external = true;
                    self.resources.fired_batch = Some(batch);
                    return Some(message);
                }
                batch.continuations_remaining = 0;

                while let Some(arming) = batch.armings.pop_front() {
                    if let Some(message) = self.deliver_timer(arming) {
                        self.resources.continuation_needs_external = false;
                        self.resources.fired_batch = Some(batch);
                        return Some(message);
                    }
                }
                self.resources.fired_batch = None;
                continue;
            }

            if !self.resources.continuation_needs_external
                && let Some(message) = self.resources.continuations.pop_front()
            {
                self.resources.continuation_needs_external = true;
                return Some(message);
            }

            let mailbox = if allow_frozen_mailbox {
                self.receiver.try_recv()
            } else {
                self.receiver.try_recv_live()
            };
            if let Some(message) = mailbox {
                self.resources.continuation_needs_external = false;
                return Some(message);
            }

            while let Some(event) = self.resources.events.pop() {
                if let Some(message) = Self::materialize_event(event) {
                    self.resources.continuation_needs_external = false;
                    return Some(message);
                }
            }

            if let Some(message) = self.resources.continuations.pop_front() {
                self.resources.continuation_needs_external = true;
                return Some(message);
            }
            return None;
        }
    }

    fn materialize_event(event: QueuedEvent<M>) -> Option<M> {
        match event {
            QueuedEvent::Deliver {
                cancellation,
                make_message,
            } => (!cancellation.is_fired()).then(make_message),
        }
    }

    fn begin_fired_batch(&mut self) {
        if self.resources.fired_batch.is_some() || self.resources.timers.is_empty() {
            return;
        }
        let now = crate::driver::now();
        let armings = self.resources.timers.take_due(now);
        if armings.is_empty() {
            return;
        }
        self.resources.fired_batch = Some(FiredTimerBatch {
            armings,
            continuations_remaining: self.resources.continuations.len(),
            mailbox_through: self.receiver.accepted_sequence(),
            mailbox_complete: false,
            offloads_remaining: self.resources.events.watermark(),
            offloads_complete: false,
        });
    }

    fn deliver_timer(&mut self, arming: u64) -> Option<M> {
        let entry = self.resources.timers.entry_mut(arming)?;
        if let Some(period) = entry.period {
            let deadline = crate::deadline::Deadline::after(crate::driver::now(), period).instant();
            entry.deadline = deadline;
            let TimerMessage::Interval(make_message) = &entry.message else {
                unreachable!("an interval timer must own a message factory")
            };
            let message = make_message();
            self.resources.timers.arm_deadline(arming, deadline);
            return Some(message);
        }

        let entry = self
            .resources
            .timers
            .remove_arming(arming)
            .expect("a due one-shot timer remains registered");
        let TimerMessage::Once(message) = entry.message else {
            unreachable!("a non-interval timer must own a one-shot message")
        };
        message
    }

    fn next_timer_deadline(&self) -> Option<Instant> {
        self.resources.timers.next_deadline()
    }

    async fn wait_for_event(&mut self) {
        let sleep = self.next_timer_deadline().map_or_else(
            || Box::pin(std::future::pending()) as crate::driver::DriverSleep,
            crate::driver::sleep_until,
        );
        let shutdown = self.shutdown.clone();
        let local_stop = self.local_stop.clone();
        let mailbox = &mut self.receiver;
        let event_watcher = &mut self.resources.event_watcher;
        let delivery = async move {
            let _ = crate::driver::select(
                mailbox.changed(),
                crate::driver::select(event_watcher.changed(), sleep),
            )
            .await;
        };
        let _ = crate::driver::select(
            shutdown.cancelled(),
            crate::driver::select(local_stop.fired(), delivery),
        )
        .await;
    }

    pub(crate) fn freeze_resources(&mut self) {
        self.freeze_and_report();
    }

    /// Freezes incarnation resources, reporting §5.2's discarded
    /// continuations on the exit path.
    fn freeze_and_report(&mut self) {
        let dropped = self.resources.freeze();
        if dropped > 0 {
            tracing::debug!(
                id = %self.id,
                incarnation = ?self.incarnation,
                dropped_continuations = dropped,
                "queued continuations discarded at the stop freeze"
            );
        }
    }

    pub(crate) async fn join_resources(&mut self) {
        self.resources.join_offloads().await;
    }

    fn take_resource_panic(&self) -> Option<PanicPayload> {
        self.resources.panic.take()
    }
}

/// Restartable raw-actor definition.
pub struct RawDef<R: RawActor> {
    factory: Arc<dyn Fn() -> R + Send + Sync + 'static>,
    pub(crate) options: CommonOptions,
}

impl<R: RawActor> fmt::Debug for RawDef<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawDef")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<R: RawActor> RawDef<R> {
    /// Creates a restartable definition from a repeatable actor factory.
    pub fn factory(factory: impl Fn() -> R + Send + Sync + 'static) -> Self {
        Self {
            factory: Arc::new(factory),
            options: CommonOptions::default(),
        }
    }

    /// Overrides the restart policy.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.options.restart = Some(restart);
        self
    }

    /// Overrides the shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.options.shutdown = Some(shutdown);
        self
    }

    /// Overrides the actor mailbox kind and capacity.
    #[must_use]
    pub fn mailbox(mut self, mailbox: Mailbox) -> Self {
        self.options.mailbox = Some(mailbox);
        self
    }

    /// Overrides frozen-prefix drain versus discard behavior.
    #[must_use]
    pub fn mailbox_shutdown(mut self, shutdown: MailboxShutdown) -> Self {
        self.options.mailbox_shutdown = Some(shutdown);
        self
    }

    /// Overrides the actor's declared readiness mode.
    pub fn readiness(mut self, readiness: Readiness) -> Result<Self, PolicyError> {
        if readiness == Readiness::AfterInit {
            return Err(PolicyError::UnsupportedReadiness);
        }
        self.options.readiness = Some(readiness);
        Ok(self)
    }

    /// Overrides the structural readiness deadline.
    #[must_use]
    pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
        self.options.readiness_deadline = deadline;
        self
    }

    /// Overrides terminal-membership retention.
    #[must_use]
    pub fn retention(mut self, retention: Retention) -> Self {
        self.options.retention = Some(retention);
        self
    }

    pub(crate) fn erase(self, mailbox: Arc<MailboxCell<R::Msg>>) -> RawConstruction {
        let factory = self.factory;
        RawConstruction {
            source: RawSource::Restartable(Arc::new(move || {
                let actor = factory();
                Box::new(RawInstance {
                    actor,
                    mailbox: Arc::clone(&mailbox),
                })
            })),
            options: self.options,
        }
    }
}

/// Consuming one-shot raw-actor definition.
pub struct RawOnceDef<R: RawActor> {
    actor: R,
    pub(crate) options: CommonOptions,
}

impl<R: RawActor> fmt::Debug for RawOnceDef<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawOnceDef")
            .field("actor", &"<owned raw actor>")
            .field("options", &self.options)
            .finish()
    }
}

impl<R: RawActor> RawOnceDef<R> {
    /// Creates a one-shot definition from an owned actor value.
    pub fn new(actor: R) -> Self {
        Self {
            actor,
            options: CommonOptions::default(),
        }
    }

    /// Overrides the shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.options.shutdown = Some(shutdown);
        self
    }

    /// Overrides the actor mailbox kind and capacity.
    #[must_use]
    pub fn mailbox(mut self, mailbox: Mailbox) -> Self {
        self.options.mailbox = Some(mailbox);
        self
    }

    /// Overrides frozen-prefix drain versus discard behavior.
    #[must_use]
    pub fn mailbox_shutdown(mut self, shutdown: MailboxShutdown) -> Self {
        self.options.mailbox_shutdown = Some(shutdown);
        self
    }

    /// Overrides the actor's declared readiness mode.
    pub fn readiness(mut self, readiness: Readiness) -> Result<Self, PolicyError> {
        if readiness == Readiness::AfterInit {
            return Err(PolicyError::UnsupportedReadiness);
        }
        self.options.readiness = Some(readiness);
        Ok(self)
    }

    /// Overrides the structural readiness deadline.
    #[must_use]
    pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
        self.options.readiness_deadline = deadline;
        self
    }

    /// Overrides terminal-membership retention.
    #[must_use]
    pub fn retention(mut self, retention: Retention) -> Self {
        self.options.retention = Some(retention);
        self
    }

    pub(crate) fn erase(self, mailbox: Arc<MailboxCell<R::Msg>>) -> RawConstruction {
        RawConstruction {
            source: RawSource::OneShot(Some(Box::new(RawInstance {
                actor: self.actor,
                mailbox,
            }))),
            options: self.options,
        }
    }
}

pub(crate) type RawFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
type RawFactory = Arc<dyn Fn() -> Box<dyn ErasedRawInstance> + Send + Sync + 'static>;

pub(crate) trait ErasedRawInstance: Send {
    fn readiness(&self) -> Readiness;
    fn run(self: Box<Self>, context: RawRunContext, readiness: Readiness) -> RawFuture;
}

struct RawInstance<R: RawActor> {
    actor: R,
    mailbox: Arc<MailboxCell<R::Msg>>,
}

struct RawIncarnationOwner<R: RawActor> {
    raw: Option<RawContext<R::Msg>>,
    actor: Option<R>,
    primary_panic: Option<PanicPayload>,
}

impl<R: RawActor> RawIncarnationOwner<R> {
    fn new(raw: RawContext<R::Msg>, actor: R) -> Self {
        Self {
            raw: Some(raw),
            actor: Some(actor),
            primary_panic: None,
        }
    }

    fn parts(&mut self) -> (&mut R, &mut RawContext<R::Msg>) {
        let actor = self.actor.as_mut().expect("raw actor owner is armed");
        let raw = self.raw.as_mut().expect("raw context owner is armed");
        (actor, raw)
    }

    fn raw(&mut self) -> &mut RawContext<R::Msg> {
        self.raw.as_mut().expect("raw context owner is armed")
    }

    fn drop_raw(&mut self) {
        drop(self.raw.take());
    }

    fn drop_actor(&mut self) {
        drop(self.actor.take());
    }

    fn record_primary_panic(&mut self, payload: PanicPayload) {
        debug_assert!(self.primary_panic.is_none());
        self.primary_panic = Some(payload);
    }

    fn take_primary_panic(&mut self) -> Option<PanicPayload> {
        self.primary_panic.take()
    }
}

impl<R: RawActor> Drop for RawIncarnationOwner<R> {
    fn drop(&mut self) {
        // A hard abort destroys the incarnation future instead of polling its
        // teardown epilogue. Preserve §5.5's resource-before-actor order, but
        // put a boundary around each destructor so two panics cannot abort the
        // process. The resource panic is primary: it may be an owned offload
        // panic that completed before cancellation was requested.
        let primary_panic = self.take_primary_panic();
        let mut cleanup_panic = catch_unwind(AssertUnwindSafe(|| self.drop_raw())).err();
        let actor_panic = catch_unwind(AssertUnwindSafe(|| self.drop_actor())).err();
        keep_first_panic(&mut cleanup_panic, actor_panic);
        if std::thread::panicking() {
            discard_panic(primary_panic);
            discard_panic(cleanup_panic);
        } else {
            resume_preferred_panic(primary_panic, cleanup_panic);
        }
    }
}

impl<R: RawActor> ErasedRawInstance for RawInstance<R> {
    fn readiness(&self) -> Readiness {
        self.actor.readiness()
    }

    fn run(self: Box<Self>, context: RawRunContext, readiness: Readiness) -> RawFuture {
        Box::pin(async move {
            let Self { actor, mailbox } = *self;
            let incarnation = context.incarnation;
            let myself = ActorRef::new(Arc::clone(&context.member), Arc::clone(&mailbox));
            let raw = RawContext::new(context, myself, Arc::clone(&mailbox), readiness);
            let mut owner = RawIncarnationOwner::new(raw, actor);
            let outcome = {
                let (actor, raw) = owner.parts();
                CatchUnwindFuture::new(actor.run(raw)).await
            };
            let result = match outcome {
                Ok(result) => Some(result),
                Err(payload) => {
                    // Keep the actor's diagnostic in the owned epilogue so a
                    // hard abort during async teardown cannot replace it with
                    // a later cancellation or destructor panic.
                    owner.record_primary_panic(payload);
                    None
                }
            };
            let freeze_panic = catch_unwind(AssertUnwindSafe(|| {
                mailbox.freeze(incarnation);
                owner.raw().freeze_resources();
            }))
            .err();
            let mut cleanup_panic = owner.raw().take_resource_panic();
            keep_first_panic(&mut cleanup_panic, freeze_panic);

            let joined = CatchUnwindFuture::new(owner.raw().join_resources()).await;
            keep_first_panic(&mut cleanup_panic, joined.err());
            let pending = owner.raw().take_resource_panic();
            keep_first_panic(&mut cleanup_panic, pending);
            let raw_drop = catch_unwind(AssertUnwindSafe(|| owner.drop_raw())).err();
            keep_first_panic(&mut cleanup_panic, raw_drop);

            let actor_drop = catch_unwind(AssertUnwindSafe(|| owner.drop_actor())).err();
            keep_first_panic(&mut cleanup_panic, actor_drop);
            // Once actor execution has panicked, teardown is secondary: never
            // replace the actor's original diagnostic.
            resume_preferred_panic(owner.take_primary_panic(), cleanup_panic);
            result.expect("an incarnation without a primary panic returns a result")
        })
    }
}

pub(crate) struct RawConstruction {
    pub(crate) source: RawSource,
    pub(crate) options: CommonOptions,
}

impl RawConstruction {
    pub(crate) fn one_shot(&self) -> bool {
        matches!(self.source, RawSource::OneShot(_) | RawSource::Spent)
    }

    pub(crate) fn take_spawn(&mut self) -> RawSpawn {
        match &mut self.source {
            RawSource::Restartable(factory) => RawSpawn::Restartable(Arc::clone(factory)),
            RawSource::OneShot(instance) => {
                let instance = instance
                    .take()
                    .expect("one-shot raw actor construction invoked more than once");
                self.source = RawSource::Spent;
                RawSpawn::OneShot(instance)
            }
            RawSource::Spent => panic!("one-shot raw actor construction invoked more than once"),
        }
    }
}

pub(crate) enum RawSpawn {
    Restartable(RawFactory),
    OneShot(Box<dyn ErasedRawInstance>),
}

impl RawSpawn {
    pub(crate) fn construct(self) -> Box<dyn ErasedRawInstance> {
        match self {
            Self::Restartable(factory) => factory(),
            Self::OneShot(instance) => instance,
        }
    }
}

pub(crate) enum RawSource {
    Restartable(RawFactory),
    OneShot(Option<Box<dyn ErasedRawInstance>>),
    Spent,
}

pub(crate) struct RawRunContext {
    pub(crate) id: ChildId,
    pub(crate) incarnation: Incarnation,
    pub(crate) member: Arc<crate::driver::MemberCell>,
    pub(crate) scope: ScopeRef,
    pub(crate) shutdown: Latch,
    pub(crate) abort: Latch,
    pub(crate) ready: Latch,
    pub(crate) local_stop: Latch,
    pub(crate) mailbox_shutdown: MailboxShutdown,
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Waker},
        thread,
        time::Duration,
    };

    use super::{
        EventQueue, OffloadResource, PanicPayload, PanicSlot, QueuedEvent, RawResources,
        SharedOffloadFuture, SharedOffloadState, resume_preferred_panic,
    };
    use crate::{driver::Signal, runtime::Latch};

    fn marker(value: usize) -> QueuedEvent<usize> {
        QueuedEvent::Deliver {
            cancellation: Latch::default(),
            make_message: Box::new(move || value),
        }
    }

    fn value(event: QueuedEvent<usize>) -> usize {
        match event {
            QueuedEvent::Deliver { make_message, .. } => make_message(),
        }
    }

    fn panic_message(payload: &PanicPayload) -> Option<&str> {
        payload
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
    }

    #[test]
    fn primary_panic_precedence_discards_a_secondary_cleanup_panic() {
        let payload = catch_unwind(AssertUnwindSafe(|| {
            resume_preferred_panic(
                Some(Box::new("primary actor panic")),
                Some(Box::new("secondary cleanup panic")),
            );
        }))
        .expect_err("the primary panic is resumed");
        assert_eq!(panic_message(&payload), Some("primary actor panic"));
    }

    struct BlockingPollDrop {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }

    impl Future for BlockingPollDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.entered.wait();
            self.release.wait();
            Poll::Pending
        }
    }

    impl Drop for BlockingPollDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.panic_on_drop {
                panic!("unit offload destructor panic");
            }
        }
    }

    #[test]
    fn concurrent_pushes_and_timer_watermark_share_one_linearization_point() {
        let queue = Arc::new(EventQueue::default());
        let first_entered = Arc::new(Barrier::new(2));
        let release_first = Arc::new(Barrier::new(2));
        let first = {
            let queue = Arc::clone(&queue);
            let first_entered = Arc::clone(&first_entered);
            let release_first = Arc::clone(&release_first);
            thread::spawn(move || {
                queue.insert_with(marker(1), || {
                    first_entered.wait();
                    release_first.wait();
                });
            })
        };
        first_entered.wait();

        assert!(
            queue.queue.try_lock().is_err(),
            "FIFO insertion and timer watermarking share one lock"
        );

        let race = Arc::new(Barrier::new(3));
        let second = {
            let queue = Arc::clone(&queue);
            let race = Arc::clone(&race);
            thread::spawn(move || {
                race.wait();
                queue.push(marker(2));
            })
        };
        let watermark = {
            let queue = Arc::clone(&queue);
            let race = Arc::clone(&race);
            thread::spawn(move || {
                race.wait();
                queue.watermark()
            })
        };
        race.wait();
        release_first.wait();
        first.join().expect("first producer completes");
        second.join().expect("second producer completes");
        let mut watermark = watermark.join().expect("watermark reader completes");

        assert!(matches!(watermark, 1 | 2));
        assert_eq!(
            value(queue.pop_through(&mut watermark).expect("first is covered")),
            1
        );
        if watermark == 1 {
            assert_eq!(
                value(
                    queue
                        .pop_through(&mut watermark)
                        .expect("second is covered")
                ),
                2
            );
        } else {
            assert!(queue.pop_through(&mut watermark).is_none());
            assert_eq!(value(queue.pop().expect("second follows watermark")), 2);
        }
        assert!(queue.pop().is_none());
    }

    #[test]
    fn timer_watermark_drains_exactly_the_preexisting_fifo_prefix() {
        let queue = EventQueue::default();
        queue.push(marker(1));
        queue.push(marker(2));
        let mut watermark = queue.watermark();
        queue.push(marker(3));

        assert_eq!(
            value(queue.pop_through(&mut watermark).expect("first is covered")),
            1
        );
        assert_eq!(
            value(
                queue
                    .pop_through(&mut watermark)
                    .expect("second is covered")
            ),
            2
        );
        assert!(queue.pop_through(&mut watermark).is_none());
        assert_eq!(
            value(queue.pop().expect("post-watermark event remains queued")),
            3
        );
    }

    #[crate::runtime::test]
    async fn event_visibility_precedes_signal_without_losing_the_wakeup() {
        let queue = Arc::new(EventQueue::default());
        let mut watcher = queue.signal.watcher();
        let inserted = Arc::new(Barrier::new(2));
        let release_signal = Arc::new(Barrier::new(2));
        let producer = {
            let queue = Arc::clone(&queue);
            let inserted = Arc::clone(&inserted);
            let release_signal = Arc::clone(&release_signal);
            thread::spawn(move || {
                queue.push_with_hooks(
                    marker(7),
                    || {},
                    || {
                        inserted.wait();
                        release_signal.wait();
                    },
                );
            })
        };
        inserted.wait();

        let mut watermark = queue.watermark();
        assert_eq!(watermark, 1);
        assert_eq!(
            value(
                queue
                    .pop_through(&mut watermark)
                    .expect("inserted event is visible before its signal")
            ),
            7
        );
        release_signal.wait();
        producer.join().expect("producer completes");
        assert!(matches!(
            crate::runtime::timeout(Duration::from_secs(1), watcher.changed()).await,
            crate::runtime::Timeout::Completed(())
        ));
    }

    #[test]
    fn polling_cancellation_drops_outside_the_mutex_and_is_idempotent() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let drops = Arc::new(AtomicUsize::new(0));
        let panic = Arc::new(PanicSlot::default());
        let finished = Latch::default();
        let state = SharedOffloadState::new(
            Box::pin(BlockingPollDrop {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
                drops: Arc::clone(&drops),
                panic_on_drop: true,
            }),
            Arc::clone(&panic),
            Signal::default(),
            finished.clone(),
        );
        let poller = {
            let state = Arc::clone(&state);
            thread::spawn(move || {
                let mut future = SharedOffloadFuture(state);
                let waker = Waker::noop();
                let mut context = Context::from_waker(waker);
                assert!(Pin::new(&mut future).poll(&mut context).is_ready());
            })
        };
        entered.wait();

        state.cancel();
        assert!(
            finished.is_fired(),
            "cancellation always signals completion"
        );
        state.cancel();
        release.wait();
        poller.join().expect("the destructor panic stays contained");

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(state.state.lock().is_ok(), "offload mutex is not poisoned");
        let payload = panic.take().expect("destructor panic is retained");
        assert_eq!(
            panic_message(&payload),
            Some("unit offload destructor panic")
        );
    }

    #[test]
    fn raw_resources_drop_cancels_every_offload_after_one_destructor_panics() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut resources = RawResources::<()>::default();
        let mut states = Vec::new();
        let mut finished = Vec::new();
        for panic_on_drop in [true, false] {
            let completion = Latch::default();
            let state = SharedOffloadState::new(
                Box::pin(BlockingPollDrop {
                    entered: Arc::new(Barrier::new(1)),
                    release: Arc::new(Barrier::new(1)),
                    drops: Arc::clone(&drops),
                    panic_on_drop,
                }),
                Arc::clone(&resources.panic),
                resources.events.signal.clone(),
                completion.clone(),
            );
            resources.offloads.push(OffloadResource {
                cancellation: Latch::default(),
                finished: completion.clone(),
                state: Some(Arc::clone(&state)),
                task: None,
            });
            states.push(state);
            finished.push(completion);
        }

        let payload = catch_unwind(AssertUnwindSafe(|| drop(resources)))
            .expect_err("the first destructor panic is surfaced");
        assert_eq!(
            panic_message(&payload),
            Some("unit offload destructor panic")
        );
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(finished.iter().all(Latch::is_fired));
        assert!(states.iter().all(|state| state.state.lock().is_ok()));
        for state in states {
            state.cancel();
        }
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "repeat cancellation is inert"
        );
    }
}

#[cfg(test)]
mod timer_store_tests {
    use std::{
        collections::HashSet,
        time::{Duration, Instant},
    };

    use super::{TimerMessage, TimerStore};

    #[derive(Eq, PartialEq)]
    struct CollidingKey(u8);

    impl std::hash::Hash for CollidingKey {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            0_u8.hash(state);
        }
    }

    fn once(entry: super::TimerEntry<&'static str>) -> &'static str {
        let TimerMessage::Once(Some(message)) = entry.message else {
            panic!("expected a live one-shot timer")
        };
        message
    }

    #[test]
    fn heterogeneous_keys_keep_exact_identity_and_deadline_order() {
        let start = Instant::now();
        let mut timers = TimerStore::default();
        timers.replace(
            7_u8,
            Some(start + Duration::from_secs(3)),
            1,
            TimerMessage::Once(Some("old-u8")),
            None,
        );
        timers.replace(
            7_u16,
            Some(start + Duration::from_secs(1)),
            2,
            TimerMessage::Once(Some("u16")),
            None,
        );
        timers.replace(
            7_u8,
            Some(start + Duration::from_secs(2)),
            3,
            TimerMessage::Once(Some("new-u8")),
            None,
        );

        assert_eq!(timers.next_deadline(), Some(start + Duration::from_secs(1)));
        assert_eq!(
            timers.take_due(start + Duration::from_secs(3)),
            [2, 3],
            "different key types coexist and replacement takes a fresh order"
        );
        assert_eq!(
            once(timers.remove_arming(2).expect("u16 timer remains")),
            "u16"
        );
        assert_eq!(
            once(timers.remove_arming(3).expect("replacement remains")),
            "new-u8"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn hash_collision_uses_exact_erased_key_equality() {
        let mut timers = TimerStore::default();
        timers.replace(
            CollidingKey(1),
            None,
            1,
            TimerMessage::Once(Some("first")),
            None,
        );
        timers.replace(
            CollidingKey(2),
            None,
            2,
            TimerMessage::Once(Some("second")),
            None,
        );
        timers.replace(
            CollidingKey(1),
            None,
            3,
            TimerMessage::Once(Some("replacement")),
            None,
        );

        assert_eq!(
            once(
                timers
                    .remove(&CollidingKey(2))
                    .expect("colliding peer remains registered")
            ),
            "second"
        );
        assert_eq!(
            once(
                timers
                    .remove(&CollidingKey(1))
                    .expect("replacement remains registered")
            ),
            "replacement"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn keyed_timer_churn_has_one_lookup_probe_per_removal() {
        const TIMERS: usize = 16_384;

        let start = Instant::now();
        let mut timers = TimerStore::default();
        let mut hashes = HashSet::with_capacity(TIMERS);
        let mut keys = Vec::with_capacity(TIMERS);
        let mut candidate = 0_usize;
        while keys.len() < TIMERS {
            if hashes.insert(timers.hash_key(&candidate)) {
                keys.push(candidate);
            }
            candidate = candidate
                .checked_add(1)
                .expect("test key space must contain enough distinct hashes");
        }
        for (index, key) in keys.iter().copied().enumerate() {
            timers.replace(
                key,
                Some(start + Duration::from_secs((TIMERS - index) as u64)),
                index as u64,
                TimerMessage::Once(Some(())),
                None,
            );
        }
        for key in keys.into_iter().rev() {
            assert!(timers.remove(&key).is_some());
        }

        assert!(timers.is_empty());
        assert!(timers.deadlines.is_empty());
        assert_eq!(
            timers.lookup_probes, TIMERS,
            "distinct hashes need one exact-key check each, not a vector scan"
        );
    }
}
