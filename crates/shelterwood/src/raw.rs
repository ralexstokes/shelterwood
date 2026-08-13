//! Minimal loop-owning raw actors and their incarnation context.

use std::{
    any::{Any, TypeId},
    collections::{BTreeSet, HashMap, VecDeque, hash_map::RandomState},
    fmt,
    future::Future,
    hash::{BuildHasher, Hash},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskPollContext, Poll},
    time::{Duration, Instant},
};

use crate::{
    ActorRef, ChildId, DeadlineBudget, ExitResult, Incarnation, Mailbox, MailboxShutdown,
    PolicyError, Readiness, ReadinessDeadline, RestartPolicy, Retention, Shutdown,
    cancellation::{CancellationToken, ParentCancellationToken},
    cells::MemberCell,
    definition::DefinitionSource,
    identity::PoisonedCounter,
    mailbox::{
        AcceptedSequence, MailboxCell, MailboxControl, MailboxReceiver, actor_ref_from_parts,
    },
    policy::{ChildMode, CommonOptions},
    runtime::{
        self, ActorWork, CompletionGatedLatch, Latch, PanicAccumulator, PanicPayload, Signal,
        SignalWatcher, UnwindPanics, catch_panic, discard_panic, keep_first_panic,
        resume_preferred_panic, resume_preferred_panic_outside_unwind,
    },
    scope::ScopeRef,
};

type DeferredMessage<M> = Box<dyn FnOnce() -> M + Send + 'static>;
type OffloadFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type SharedWork = Arc<SharedOffloadState>;

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

/// Future panic boundary that owns and destroys its inner future.
///
/// Once the inner future returns `Ready`, it is destroyed before the output is
/// released. If that destruction panics, the already-produced output is
/// discarded and the destructor panic becomes this future's error.
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
        let polled = catch_panic(|| {
            this.future
                .as_mut()
                .expect("a completed panic boundary was polled again")
                .as_mut()
                .poll(context)
        });
        match polled {
            Ok(Poll::Ready(value)) => {
                let future = this.future.take();
                match catch_panic(|| drop(future)) {
                    Ok(()) => Poll::Ready(Ok(value)),
                    Err(payload) => Poll::Ready(Err(payload)),
                }
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                let future = this.future.take();
                discard_panic(catch_panic(|| drop(future)).err());
                Poll::Ready(Err(payload))
            }
        }
    }
}

impl<F> Drop for CatchUnwindFuture<F> {
    fn drop(&mut self) {
        let future = self.future.take();
        let mut panics = PanicAccumulator::default();
        panics.run(|| drop(future));
    }
}

struct QueuedEvent<M> {
    cancellation: Latch,
    make_message: DeferredMessage<M>,
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

/// The one cleanup route for values owned by a raw incarnation.
///
/// Collection drains and offload futures route user payloads through this
/// funnel. Collections keep their resident elements raw and dispose each one
/// explicitly when draining, avoiding a cloned disposal handle per element.
/// A destructor panic is retained as cleanup evidence and wakes an idle actor;
/// it never unwinds through another user destructor.
#[derive(Clone)]
struct RawDisposal {
    panic: Arc<PanicSlot>,
    signal: Signal,
}

/// Test-only: mints an orphan disposal whose panic slot and signal nothing
/// observes. Production wiring threads one shared disposal per incarnation
/// through the container constructors.
#[cfg(test)]
impl Default for RawDisposal {
    fn default() -> Self {
        Self {
            panic: Arc::new(PanicSlot::default()),
            signal: Signal::default(),
        }
    }
}

impl RawDisposal {
    fn record(&self, payload: PanicPayload) {
        self.panic.record(payload);
        if let Err(payload) = catch_panic(|| self.signal.pulse()) {
            self.panic.record(payload);
        }
    }

    fn dispose<T>(&self, value: T) {
        if let Err(payload) = catch_panic(|| drop(value)) {
            self.record(payload);
        }
    }
}

#[must_use = "contained user ownership must be consumed or disposed"]
struct Contained<T> {
    value: Option<T>,
    disposal: RawDisposal,
}

impl<T> Contained<T> {
    fn new(value: T, disposal: RawDisposal) -> Self {
        Self {
            value: Some(value),
            disposal,
        }
    }

    fn get(&self) -> &T {
        self.value
            .as_ref()
            .expect("contained ownership is consumed once")
    }

    fn into_inner(mut self) -> T {
        self.value
            .take()
            .expect("contained ownership is consumed once")
    }
}

impl<T> Drop for Contained<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            self.disposal.dispose(value);
        }
    }
}

/// Incarnation-internal completion storage for offload continuations.
///
/// The queue is deliberately unbounded, but sustained traffic cannot grow it
/// without bound. Every entry is produced by work this incarnation itself
/// started — exactly one completion per offload, no external sender can
/// reach it — and `next_ready` snapshots queued completions into bounded
/// arbitration turns. A turn admits at most one mailbox delivery before its
/// captured completion prefix, with continuation fairness interleaved, so the
/// population stays bounded by the actor's own in-flight offload count plus
/// the completions arriving within one such window. Bounded-mailbox policy
/// governs external input only (SPEC §5.5: offload completions do not consume
/// mailbox capacity), and imposing a bound here would either block the offload
/// task or drop a completion the total-continuation contract promises to
/// deliver. Freezing at stop clears the queue, and dropped `RawResources`
/// clear it on every exit path.
struct EventQueue<M> {
    // Arbitration batches snapshot the number of currently queued events.
    // Insertion and the snapshot share this lock, so FIFO order itself is the
    // sequence and there is no integer counter whose saturation could blur a
    // boundary.
    queue: Mutex<VecDeque<QueuedEvent<M>>>,
    signal: Signal,
    disposal: RawDisposal,
}

#[cfg(test)]
impl<M> Default for EventQueue<M> {
    fn default() -> Self {
        Self::new(RawDisposal::default())
    }
}

impl<M> EventQueue<M> {
    fn new(disposal: RawDisposal) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            signal: disposal.signal.clone(),
            disposal,
        }
    }

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

    #[cfg(test)]
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
        let mut queue = {
            let mut queue = self.queue.lock().expect("actor event queue mutex poisoned");
            std::mem::take(&mut *queue)
        };
        while let Some(event) = queue.pop_front() {
            self.disposal.dispose(event);
        }
    }
}

impl<M> Drop for EventQueue<M> {
    fn drop(&mut self) {
        let queue = self
            .queue
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(event) = queue.pop_front() {
            self.disposal.dispose(event);
        }
    }
}

/// Incarnation-owned storage for `continue_with` messages.
///
/// Elements are stored raw — the queue owns one disposal handle instead of one
/// per element — so every drain must route its payloads through that funnel.
/// `Drop` re-drains unconditionally: a `freeze` that never ran, or that failed
/// partway through an earlier cleanup step, must not leave queued user
/// messages to be destroyed outside the disposal boundary.
struct ContinuationQueue<M> {
    queue: VecDeque<M>,
    disposal: RawDisposal,
}

impl<M> ContinuationQueue<M> {
    fn new(disposal: RawDisposal) -> Self {
        Self {
            queue: VecDeque::new(),
            disposal,
        }
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn push_back(&mut self, message: M) {
        self.queue.push_back(message);
    }

    fn pop_front(&mut self) -> Option<M> {
        self.queue.pop_front()
    }

    fn clear(&mut self) {
        while let Some(message) = self.queue.pop_front() {
            self.disposal.dispose(message);
        }
    }
}

impl<M> Drop for ContinuationQueue<M> {
    fn drop(&mut self) {
        self.clear();
    }
}

enum TimerMessage<M> {
    Once(M),
    Interval(M, fn(&M) -> M),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ArmingOrder(u64);

impl ArmingOrder {
    const MAX: Self = Self(u64::MAX);
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct KeyHash(u64);

struct TimerEntry<M> {
    key: Box<dyn Any + Send>,
    /// `None` when the requested delay overflows the clock: a deadline that
    /// never arrives, mirroring the offload path — never "due now".
    deadline: Option<Instant>,
    arming_order: ArmingOrder,
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
    keyed: HashMap<KeyHash, Vec<TimerEntry<M>>>,
    armings: HashMap<ArmingOrder, KeyHash>,
    deadlines: BTreeSet<(Instant, ArmingOrder)>,
    disposal: RawDisposal,
    #[cfg(test)]
    lookup_probes: usize,
}

#[cfg(test)]
impl<M> Default for TimerStore<M> {
    fn default() -> Self {
        Self::new(RawDisposal::default())
    }
}

impl<M> TimerStore<M> {
    fn new(disposal: RawDisposal) -> Self {
        Self {
            key_hasher: RandomState::new(),
            keyed: HashMap::new(),
            armings: HashMap::new(),
            deadlines: BTreeSet::new(),
            disposal,
            #[cfg(test)]
            lookup_probes: 0,
        }
    }
    fn hash_key<K: Hash + 'static>(&self, key: &K) -> KeyHash {
        KeyHash(self.key_hasher.hash_one((TypeId::of::<K>(), key)))
    }

    fn is_empty(&self) -> bool {
        self.armings.is_empty()
    }

    fn clear(&mut self) {
        let keyed = std::mem::take(&mut self.keyed);
        self.armings.clear();
        self.deadlines.clear();
        for entry in keyed.into_values().flatten() {
            self.dispose_entry(entry);
        }
    }

    fn replace<K>(
        &mut self,
        key: K,
        deadline: Option<Instant>,
        arming_order: ArmingOrder,
        message: TimerMessage<M>,
        period: Option<Duration>,
    ) where
        K: Hash + Eq + Send + 'static,
    {
        // Hash and equality are user code. Keep incoming ownership behind the
        // disposal boundary until those callbacks finish so a callback panic
        // cannot unwind through a hostile key or message destructor.
        let key = Contained::new(key, self.disposal.clone());
        let message = Contained::new(message, self.disposal.clone());
        self.remove(key.get());
        let hash = self.hash_key(key.get());
        self.keyed.entry(hash).or_default().push(TimerEntry {
            key: Box::new(key.into_inner()),
            deadline,
            arming_order,
            message: message.into_inner(),
            period,
        });
        let previous = self.armings.insert(arming_order, hash);
        debug_assert!(previous.is_none());
        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, arming_order));
        }
    }

    fn take<K>(&mut self, key: &K) -> Option<TimerEntry<M>>
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
                entry.key.downcast_ref::<K>() == Some(key)
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

    fn remove<K>(&mut self, key: &K) -> bool
    where
        K: Hash + Eq + 'static,
    {
        let Some(entry) = self.take(key) else {
            return false;
        };
        self.dispose_entry(entry);
        true
    }

    fn clear_and_dispose<K>(&mut self, key: K, message: M)
    where
        K: Hash + Eq + Send + 'static,
    {
        // A zero-period interval still invokes user Hash/Eq while it owns the
        // rejected inputs. Keep both values contained through that lookup.
        let key = Contained::new(key, self.disposal.clone());
        let message = Contained::new(message, self.disposal.clone());
        self.remove(key.get());
        drop(key);
        drop(message);
    }

    fn dispose_entry(&self, entry: TimerEntry<M>) {
        let TimerEntry { key, message, .. } = entry;
        self.disposal.dispose(key);
        self.disposal.dispose(message);
    }

    fn remove_arming(&mut self, arming_order: ArmingOrder) -> Option<TimerEntry<M>> {
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

    fn entry_mut(&mut self, arming_order: ArmingOrder) -> Option<&mut TimerEntry<M>> {
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

    fn take_due(&mut self, now: Instant) -> VecDeque<ArmingOrder> {
        let due = self
            .deadlines
            .range(..=(now, ArmingOrder::MAX))
            .copied()
            .collect::<Vec<_>>();
        for deadline in &due {
            self.deadlines.remove(deadline);
        }
        due.into_iter().map(|(_, arming)| arming).collect()
    }

    fn arm_deadline(&mut self, arming_order: ArmingOrder, deadline: Option<Instant>) {
        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, arming_order));
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }
}

impl<M> Drop for TimerStore<M> {
    fn drop(&mut self) {
        self.clear();
    }
}

struct ReadyBatch {
    armings: VecDeque<ArmingOrder>,
    // Only fired batches constrain continuations to a captured prefix.
    // Steady-state continuations stay live so one queued by an external
    // handler retains `continue_with`'s next-message priority.
    continuations_remaining: usize,
    mailbox_through: AcceptedSequence,
    // Steady state takes one mailbox delivery before its captured offload
    // prefix. A timer promotion removes that budget and drains the entire
    // pre-fire mailbox prefix before delivering the timer armings.
    mailbox_remaining: Option<usize>,
    mailbox_complete: bool,
    offloads_remaining: usize,
    offloads_complete: bool,
}

impl ReadyBatch {
    fn steady(mailbox_through: AcceptedSequence, offloads_remaining: usize) -> Self {
        Self {
            armings: VecDeque::new(),
            continuations_remaining: 0,
            mailbox_through,
            mailbox_remaining: Some(1),
            mailbox_complete: false,
            offloads_remaining,
            offloads_complete: false,
        }
    }

    fn promote_to_fired(
        &mut self,
        armings: VecDeque<ArmingOrder>,
        continuations_remaining: usize,
        mailbox_through: AcceptedSequence,
        offloads_remaining: usize,
    ) {
        debug_assert!(!armings.is_empty());
        debug_assert!(self.mailbox_remaining.is_some());
        self.armings = armings;
        self.continuations_remaining = continuations_remaining;
        self.mailbox_through = mailbox_through;
        self.mailbox_remaining = None;
        self.mailbox_complete = false;
        self.offloads_remaining = offloads_remaining;
        self.offloads_complete = false;
    }

    fn mailbox_budget_exhausted(&self) -> bool {
        self.mailbox_remaining == Some(0)
    }

    fn is_fired(&self) -> bool {
        self.mailbox_remaining.is_none()
    }

    fn continuation_is_eligible(&self) -> bool {
        !self.is_fired() || self.continuations_remaining > 0
    }

    fn record_continuation_delivery(&mut self) {
        if self.is_fired() {
            debug_assert!(self.continuations_remaining > 0);
            self.continuations_remaining -= 1;
        }
    }

    fn record_mailbox_delivery(&mut self) {
        if let Some(remaining) = &mut self.mailbox_remaining {
            debug_assert!(*remaining > 0);
            *remaining -= 1;
            if *remaining == 0 {
                self.mailbox_complete = true;
            }
        }
    }
}

struct OffloadFutureState {
    future: Option<OffloadFuture>,
    polling: bool,
    cancelled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OffloadPoll {
    Pending,
    Finished,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OffloadScope {
    Unscoped,
    Scoped,
}

struct SharedOffloadState {
    // Polling takes the future out of this mutex. Cancellation either takes
    // an idle future or marks an in-progress poll so that the poller disposes
    // it, always after releasing the lock.
    state: Mutex<OffloadFutureState>,
    disposal: RawDisposal,
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
            disposal: RawDisposal { panic, signal },
            finished,
        })
    }

    fn take_for_poll(&self) -> Option<OffloadFuture> {
        let mut state = self.state.lock().expect("offload future mutex poisoned");
        if state.cancelled {
            return None;
        }
        debug_assert!(!state.polling, "offload work must have one poller");
        let future = state.future.take();
        state.polling = future.is_some();
        future
    }

    fn finish_poll(&self, future: OffloadFuture, outcome: OffloadPoll) -> Option<OffloadFuture> {
        let mut state = self.state.lock().expect("offload future mutex poisoned");
        state.polling = false;
        if outcome == OffloadPoll::Pending && !state.cancelled {
            debug_assert!(state.future.is_none());
            state.future = Some(future);
            None
        } else {
            Some(future)
        }
    }

    fn record(&self, payload: PanicPayload) {
        // Dropping a losing or cancelled operation can panic after its body
        // has stopped running, so every retained panic must wake the actor's
        // control plane independently of ordinary event delivery.
        self.disposal.record(payload);
    }

    fn dispose(&self, future: Option<OffloadFuture>) {
        if let Some(future) = future {
            self.disposal.dispose(future);
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
        let polled = catch_panic(|| future.as_mut().poll(context));
        match polled {
            Ok(Poll::Pending) => {
                let dispose = self.0.finish_poll(future, OffloadPoll::Pending);
                if dispose.is_some() {
                    self.0.dispose(dispose);
                    self.0.finished.fire();
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            Ok(Poll::Ready(())) => {
                let dispose = self.0.finish_poll(future, OffloadPoll::Finished);
                self.0.dispose(dispose);
                self.0.finished.fire();
                Poll::Ready(())
            }
            Err(payload) => {
                let dispose = self.0.finish_poll(future, OffloadPoll::Finished);
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
            // These are complementary: `state.cancel()` synchronously
            // disposes idle work (capturing destructor panic) or marks an
            // in-progress poll to dispose on return, while abort independently
            // requests cancellation of the runtime task driving that poll.
            // Neither substitutes for the other.
            task.abort();
        }
        self.finished.fire();
    }
}

struct RawResources<M> {
    accepting: bool,
    continuations: ContinuationQueue<M>,
    // Set after returning a continuation and cleared after an external
    // mailbox/offload/timer item. `next_ready` uses it to prohibit two local
    // continuations from leading while an external source remains eligible.
    continuation_needs_external: bool,
    timers: TimerStore<M>,
    timer_orders: PoisonedCounter,
    ready_batch: Option<ReadyBatch>,
    events: Arc<EventQueue<M>>,
    panic: Arc<PanicSlot>,
    disposal: RawDisposal,
    event_watcher: SignalWatcher,
    offloads: Vec<OffloadResource>,
}

impl<M> Default for RawResources<M> {
    fn default() -> Self {
        let signal = Signal::default();
        let panic = Arc::new(PanicSlot::default());
        let disposal = RawDisposal {
            panic: Arc::clone(&panic),
            signal,
        };
        let events = Arc::new(EventQueue::new(disposal.clone()));
        let event_watcher = events.signal.watcher();
        Self {
            accepting: true,
            continuations: ContinuationQueue::new(disposal.clone()),
            continuation_needs_external: false,
            timers: TimerStore::new(disposal.clone()),
            timer_orders: PoisonedCounter::new(),
            ready_batch: None,
            events,
            panic,
            disposal,
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
        self.ready_batch = None;
        self.events.clear();
        dropped_continuations
    }

    /// Drops ledger entries for offloads that already finished, keeping a
    /// long-lived incarnation's ledger O(in-flight) rather than
    /// O(offloads-ever-issued). Invoked when a new offload starts, before each
    /// ready-selection turn, and when the loop goes idle. The selection point
    /// is what bounds retention for an actor whose mailbox never empties.
    ///
    /// The scan is therefore O(in-flight offloads) per delivered input, by
    /// design: the ledger is already bounded by the caller's own in-flight
    /// count, so a per-turn walk of it is proportional to work the actor
    /// itself has outstanding.
    fn reclaim_finished(&mut self) {
        self.offloads.retain(|offload| !offload.finished.is_fired());
    }

    fn resume_pending_panic(&self) {
        // Reached from the actor's own receive path, never from cleanup. The
        // take is destructive, so containment here would drop the retained
        // offload diagnostic and let the loop keep running.
        resume_preferred_panic_outside_unwind(UnwindPanics {
            primary: self.panic.take(),
            cleanup: None,
        });
    }

    async fn join_offloads(&mut self) {
        for offload in &mut self.offloads {
            if let Some(task) = offload.task.take() {
                match task.join().await {
                    runtime::JoinOutcome::Ok { value: () } | runtime::JoinOutcome::Cancelled => {}
                    runtime::JoinOutcome::Panic { message } => {
                        let message = message.unwrap_or_else(|| {
                            "library-owned offload task panicked without a string payload"
                                .to_owned()
                        });
                        tracing::error!(%message, "library-owned offload task panicked");
                        self.panic.record(Box::new(message));
                    }
                }
            }
        }
        self.events.clear();
        self.offloads.clear();
    }
}

impl<M> Drop for RawResources<M> {
    fn drop(&mut self) {
        let freeze_panic = catch_panic(|| {
            let _ = self.freeze();
        })
        .err();
        let mut panics = PanicAccumulator::default();
        // `freeze` can transfer a destructor panic into the shared slot. Take
        // that retained application diagnostic after cleanup and preserve it
        // ahead of a direct framework-cleanup panic.
        panics.record(self.panic.take());
        panics.record(freeze_panic);
    }
}

/// Minimal actor contract for application-owned receive loops.
pub trait RawActor: Send + 'static {
    /// Message accepted by this actor.
    type Msg: Send + 'static;

    /// Declares when this actor type becomes ready.
    ///
    /// This is definition metadata: the framework reads it before constructing
    /// an incarnation, so it cannot depend on per-incarnation actor state.
    fn readiness() -> Readiness {
        Readiness::Immediate
    }

    /// Runs one incarnation using the membership-owned mailbox binding.
    ///
    /// The framework calls this method at most once on an incarnation's root
    /// raw-actor value and never re-enters it on that value. Shutdown may
    /// destroy a constructed root before its run begins; a restart that reaches
    /// construction obtains a fresh root value.
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
    shutdown: ParentCancellationToken,
    abort: CancellationToken,
    ready: CompletionGatedLatch,
    local_stop: Latch,
    readiness: Readiness,
    mailbox_shutdown: MailboxShutdown,
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
            shutdown: ParentCancellationToken::from_latch(run.shutdown),
            abort: CancellationToken::from_latch(run.abort),
            ready: run.ready,
            local_stop: run.local_stop,
            readiness,
            mailbox_shutdown: run.mailbox_shutdown,
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
        self.shutdown.token()
    }

    /// Returns the escalation token.
    #[must_use]
    pub fn abort_token(&self) -> CancellationToken {
        self.abort.clone()
    }

    /// Requests shutdown of the supervising scope without waiting.
    ///
    /// Do not await that scope's shutdown from this actor: the scope cannot
    /// finish until this actor's `run` future returns.
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
    /// and timers are discarded, and [`recv`](Self::recv) returns `None`.
    /// A raw loop honoring [`MailboxShutdown::Drain`] must then consume the
    /// frozen prefix with [`try_recv`](Self::try_recv); `recv` never drains a
    /// frozen mailbox. Idempotent. This is the primitive the blanket handler
    /// loop's `Context::stop` is built on (§1 principle 5); the child's
    /// configured §10 ladder bounds the stop.
    pub fn stop(&mut self) {
        self.receiver.freeze();
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
        self.replace_timer(key, TimerMessage::Once(message), after, None);
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
            self.resources.timers.clear_and_dispose(key, message);
            return Ok(());
        }
        self.replace_timer(
            key,
            TimerMessage::Interval(message, Clone::clone),
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
        self.resources.timers.remove(key)
    }

    /// Starts incarnation-owned async work with one total deadline budget.
    ///
    /// Completions re-enter the loop through incarnation-internal storage
    /// that does not consume mailbox capacity (SPEC §5.5). That storage is
    /// unbounded but cannot accumulate a backlog: it holds at most one entry
    /// per offload the actor itself started, and each bounded arbitration turn
    /// admits at most one mailbox delivery before its captured completion
    /// prefix. Its population therefore stays proportional to the caller's
    /// in-flight count even under sustained mailbox traffic. Bookkeeping for
    /// finished offloads is reclaimed when a new offload starts, on every
    /// input-selection turn, and when the loop goes idle.
    /// A zero budget never polls `work`; its continuation is queued with
    /// [`DeadlineElapsed`] through the ordinary completion path.
    pub fn offload<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: impl Into<DeadlineBudget>,
    ) -> Result<(), Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> M + Send + 'static,
    {
        self.start_offload(work, continuation, deadline.into(), OffloadScope::Unscoped)
            .map(|_| ())
    }

    /// Starts guarded incarnation-owned async work with one deadline budget.
    ///
    /// Completion storage follows [`offload`](Self::offload): unbounded, but
    /// one entry per offload the actor itself started and drained in bounded
    /// arbitration turns alongside mailbox input so no backlog accumulates.
    /// Like `offload`, a zero budget never polls `work` and queues the
    /// continuation with [`DeadlineElapsed`].
    pub fn offload_scoped<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: impl Into<DeadlineBudget>,
    ) -> Result<Guard, Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> M + Send + 'static,
    {
        self.start_offload(work, continuation, deadline.into(), OffloadScope::Scoped)
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
        let work = runtime::spawn_blocking_work(move || operation(token));
        Blocking {
            future: Box::pin(work),
            cancellation,
            completed: false,
        }
    }

    /// Receives the next accepted message, biased toward shutdown.
    ///
    /// While the incarnation is running, a retained offload-work panic resumes
    /// from this receive path before another event is delivered. A panic in an
    /// offload's continuation closure — the `FnOnce` that builds the message
    /// from the offload result, not a [`continue_with`](Self::continue_with)
    /// continuation, which is a plain stored message and cannot panic here —
    /// surfaces directly from this receive call.
    pub async fn recv(&mut self) -> Option<M> {
        loop {
            if self.local_stop.is_fired() {
                self.freeze_and_report();
                // `stop()` originates on this task, but the configured
                // shutdown ladder is owned by the driver. The driver's helper
                // only observes the local-stop latch and forwards
                // `ChildEvent::SelfStop`; it is the driver's stop ladder that
                // fires the shared shutdown token. Wait for that token before
                // ending the raw loop; removing this await would let a local
                // stop bypass that cross-task handshake.
                self.shutdown.cancelled().await;
                return None;
            }
            if self.shutdown.is_cancelled() {
                self.freeze_and_report();
                return None;
            }
            if let Some(message) = self.next_ready() {
                return Some(message);
            }
            self.wait_for_event().await;
        }
    }

    /// Receives one ready event without awaiting or consulting shutdown.
    ///
    /// Outside shutdown drain, this resumes a retained offload-work panic
    /// before returning another event; a panic in an offload's continuation
    /// closure (the message-building `FnOnce`, not a
    /// [`continue_with`](Self::continue_with) continuation, which is a plain
    /// stored message) surfaces directly from this call. During drain it reads
    /// the frozen accepted mailbox prefix directly.
    pub fn try_recv(&mut self) -> Option<M> {
        if self.is_stopping() {
            self.freeze_and_report();
            self.receiver.try_recv()
        } else {
            self.next_ready()
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
        let arming_order = ArmingOrder(
            self.resources
                .timer_orders
                .mint()
                .expect("timer arming-order space exhausted"),
        );
        let now = runtime::now();
        let deadline = crate::deadline::Deadline::after(now, after).instant();
        self.resources
            .timers
            .replace(key, deadline, arming_order, message, period);
    }

    fn start_offload<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: DeadlineBudget,
        scope: OffloadScope,
    ) -> Result<Option<Guard>, Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> M + Send + 'static,
    {
        if self.is_stopping() || !self.resources.accepting {
            return Err(Rejected::new((work, continuation)));
        }
        let work = Contained::new(work, self.resources.disposal.clone());
        let continuation = Contained::new(continuation, self.resources.disposal.clone());
        // Completed offloads no longer need their resources.
        self.resources.reclaim_finished();

        let cancellation = Latch::default();
        let finished = Latch::default();
        let guard = (scope == OffloadScope::Scoped).then(|| Guard {
            cancellation: cancellation.clone(),
            finished: finished.clone(),
            armed: true,
        });
        let events = Arc::clone(&self.resources.events);
        let disposal = self.resources.disposal.clone();
        // Zero selects no attempt (SPEC Appendix B): the work future is never
        // polled, and the continuation still travels the ordinary completion
        // path so its venue and ordering are unchanged.
        if deadline.is_zero() {
            drop(work);
            events.push(QueuedEvent {
                cancellation: cancellation.clone(),
                make_message: Box::new(move || continuation.into_inner()(Err(DeadlineElapsed))),
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
        let started_at = runtime::now();
        let expires_at = crate::deadline::Deadline::after_budget(started_at, deadline).instant();
        let event_cancellation = cancellation.clone();
        let operation = async move {
            let completion = async move {
                let work = CatchUnwindFuture::new(work.into_inner());
                if let Some(expires_at) = expires_at {
                    match runtime::select_two(work, runtime::sleep_until(expires_at)).await {
                        runtime::Either::Left(result) => result.map(Ok),
                        runtime::Either::Right(()) => Ok(Err(DeadlineElapsed)),
                    }
                } else {
                    work.await.map(Ok)
                }
            };
            match runtime::select_two(token.cancelled(), completion).await {
                runtime::Either::Left(()) => {}
                runtime::Either::Right(Ok(result)) => {
                    events.push(QueuedEvent {
                        cancellation: event_cancellation,
                        make_message: Box::new(move || continuation.into_inner()(result)),
                    });
                }
                runtime::Either::Right(Err(payload)) => {
                    disposal.record(payload);
                }
            }
        };
        let state = SharedOffloadState::new(
            Box::pin(operation),
            Arc::clone(&self.resources.panic),
            self.resources.events.signal.clone(),
            finished.clone(),
        );
        let task = runtime::spawn_actor_work(SharedOffloadFuture(Arc::clone(&state)));
        self.resources.offloads.push(OffloadResource {
            cancellation,
            finished,
            state: Some(state),
            task: Some(task),
        });
        Ok(guard)
    }

    /// Selects one live-incarnation input without awaiting.
    ///
    /// Every selection runs through one bounded arbitration batch. Steady
    /// state captures at most one mailbox delivery together with the queued
    /// offload prefix, while continuations remain live across handler calls.
    /// If a timer is due, that same batch is promoted by widening the mailbox
    /// cutoff to everything accepted at the fire observation and capturing
    /// the continuation and offload prefixes at that point.
    ///
    /// Stage priority is: at most one fairness continuation, mailbox prefix,
    /// offload prefix, remaining snapshotted continuations, then timer
    /// armings. Each external delivery permits one continuation to lead the
    /// next call, so continuations can interleave with the mailbox and offload
    /// stages without repeatedly cutting ahead of them. Once those external
    /// prefixes are exhausted, the captured continuation remainder drains
    /// before timers; arrivals after a fired batch's cutoffs cannot jump its
    /// timers. The one-mailbox steady-state bound prevents an always-readable
    /// mailbox from starving completions, while the completion cutoff prevents
    /// a self-feeding offload chain from starving the mailbox. One steady-batch
    /// consequence: a mailbox message arriving mid-drain waits behind the
    /// batch's captured completion prefix (bounded by the in-flight offload
    /// count) — a cross-source ordering the spec leaves unspecified (SPEC §5.2:
    /// no global linearization point across source cutoffs), so tests must not
    /// pin any particular interleaving.
    ///
    /// Frozen mailbox input is deliberately absent. Once stopping begins,
    /// [`try_recv`](Self::try_recv) bypasses this selector and drains the
    /// accepted prefix directly according to the caller's shutdown policy.
    fn next_ready(&mut self) -> Option<M> {
        // A permanently busy actor never reaches `wait_for_event`; reclaim at
        // its other guaranteed re-entry point so completed task handles do not
        // accumulate for the lifetime of the incarnation. This is the point
        // that bounds ledger retention.
        self.resources.reclaim_finished();
        loop {
            self.resources.resume_pending_panic();
            self.begin_ready_batch();
            let mut batch = self
                .resources
                .ready_batch
                .take()
                .expect("ready selection always owns an arbitration batch");
            if !self.resources.continuation_needs_external
                && batch.continuation_is_eligible()
                && let Some(message) = self.resources.continuations.pop_front()
            {
                batch.record_continuation_delivery();
                self.resources.continuation_needs_external = true;
                self.resources.ready_batch = Some(batch);
                return Some(message);
            }

            if !batch.mailbox_complete {
                let message = self.receiver.try_recv_live_through(batch.mailbox_through);
                if let Some(message) = message {
                    batch.record_mailbox_delivery();
                    self.resources.continuation_needs_external = false;
                    self.resources.ready_batch = Some(batch);
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
                    if let Some(message) = self.materialize_event(event) {
                        self.resources.continuation_needs_external = false;
                        self.resources.ready_batch = Some(batch);
                        return Some(message);
                    }
                }
                batch.offloads_complete = true;
            }

            // A steady batch may have exhausted its captured external turn
            // while a continuation handler made later external work ready.
            // Start a fresh bounded turn before allowing another continuation
            // so that work receives §5.2's mandatory fairness opportunity.
            // Fired batches deliberately retain their immutable cutoffs:
            // post-fire arrivals must not jump the already-fired timers.
            if !batch.is_fired()
                && self.resources.continuation_needs_external
                && (batch.mailbox_budget_exhausted()
                    || self.receiver.accepted_sequence() > batch.mailbox_through
                    || self.resources.events.watermark() > 0)
            {
                self.resources.ready_batch = None;
                continue;
            }

            if batch.continuation_is_eligible()
                && let Some(message) = self.resources.continuations.pop_front()
            {
                batch.record_continuation_delivery();
                self.resources.continuation_needs_external = true;
                self.resources.ready_batch = Some(batch);
                return Some(message);
            }
            batch.continuations_remaining = 0;

            while let Some(arming) = batch.armings.pop_front() {
                if let Some(message) = self.deliver_timer(arming) {
                    self.resources.continuation_needs_external = false;
                    self.resources.ready_batch = Some(batch);
                    return Some(message);
                }
            }

            let mailbox_may_remain = batch.mailbox_budget_exhausted();
            let mailbox_cutoff = batch.mailbox_through;
            self.resources.ready_batch = None;
            let timer_is_due = self
                .next_timer_deadline()
                .is_some_and(|deadline| deadline <= runtime::now());
            if mailbox_may_remain
                || self.receiver.accepted_sequence() > mailbox_cutoff
                || self.resources.events.watermark() > 0
                || !self.resources.continuations.is_empty()
                || timer_is_due
            {
                continue;
            }
            return None;
        }
    }

    fn materialize_event(&self, event: QueuedEvent<M>) -> Option<M> {
        if event.cancellation.is_fired() {
            self.resources.disposal.dispose(event);
            return None;
        }
        Some((event.make_message)())
    }

    fn begin_ready_batch(&mut self) {
        if self.resources.ready_batch.is_none() {
            self.resources.ready_batch = Some(ReadyBatch::steady(
                self.receiver.accepted_sequence(),
                self.resources.events.watermark(),
            ));
        }
        if self
            .resources
            .ready_batch
            .as_ref()
            .is_some_and(ReadyBatch::is_fired)
            || self.resources.timers.is_empty()
        {
            return;
        }

        let now = runtime::now();
        let armings = self.resources.timers.take_due(now);
        if armings.is_empty() {
            return;
        }
        self.resources
            .ready_batch
            .as_mut()
            .expect("steady batch was initialized")
            .promote_to_fired(
                armings,
                self.resources.continuations.len(),
                self.receiver.accepted_sequence(),
                self.resources.events.watermark(),
            );
    }

    fn deliver_timer(&mut self, arming: ArmingOrder) -> Option<M> {
        let entry = self.resources.timers.entry_mut(arming)?;
        if let Some(period) = entry.period {
            let deadline = crate::deadline::Deadline::after(runtime::now(), period).instant();
            entry.deadline = deadline;
            let TimerMessage::Interval(message, clone_message) = &entry.message else {
                unreachable!("an interval timer must own a message factory")
            };
            let message = clone_message(message);
            self.resources.timers.arm_deadline(arming, deadline);
            return Some(message);
        }

        let entry = self
            .resources
            .timers
            .remove_arming(arming)
            .expect("a due one-shot timer remains registered");
        let TimerEntry { key, message, .. } = entry;
        self.resources.disposal.dispose(key);
        let TimerMessage::Once(message) = message else {
            unreachable!("a non-interval timer must own a one-shot message")
        };
        Some(message)
    }

    fn next_timer_deadline(&self) -> Option<Instant> {
        self.resources.timers.next_deadline()
    }

    async fn wait_for_event(&mut self) {
        // Retention is already bounded by the reclaim in `next_ready`, which
        // ran on this task with no await in between. This is a same-instant
        // backstop for the one case that reclaim could not have seen: an
        // offload finishing on another thread between the two calls.
        self.resources.reclaim_finished();
        let sleep = self.next_timer_deadline().map_or_else(
            || Box::pin(std::future::pending()) as runtime::BoxedSleep,
            runtime::sleep_until,
        );
        let shutdown = self.shutdown.clone();
        let local_stop = self.local_stop.clone();
        let mailbox = &mut self.receiver;
        let event_watcher = &mut self.resources.event_watcher;
        let delivery = async move {
            let _ = runtime::select_two(
                mailbox.changed(),
                runtime::select_two(event_watcher.changed(), sleep),
            )
            .await;
        };
        let _ = runtime::select_two(
            shutdown.cancelled(),
            runtime::select_two(local_stop.fired(), delivery),
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
    factory: Box<dyn Fn() -> R + Send + Sync + 'static>,
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
            factory: Box::new(factory),
            options: CommonOptions::default(),
        }
    }

    common_options_setters!(
        restart,
        shutdown,
        mailbox,
        mailbox_shutdown,
        raw_readiness,
        structural_readiness_deadline,
        retention,
    );

    pub(crate) fn erase(self, mailbox: Arc<MailboxCell<R::Msg>>) -> RawConstruction {
        let factory = self.factory;
        let readiness = self.options.readiness.unwrap_or_else(R::readiness);
        RawConstruction {
            source: DefinitionSource::Restartable(Arc::new(move || {
                let actor = factory();
                Box::new(RawInstance {
                    actor,
                    mailbox: Arc::clone(&mailbox),
                })
            })),
            options: self.options,
            readiness,
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

    common_options_setters!(
        shutdown,
        mailbox,
        mailbox_shutdown,
        raw_readiness,
        structural_readiness_deadline,
        retention,
    );

    pub(crate) fn erase(self, mailbox: Arc<MailboxCell<R::Msg>>) -> RawConstruction {
        let readiness = self.options.readiness.unwrap_or_else(R::readiness);
        RawConstruction {
            source: DefinitionSource::OneShot(Box::new(RawInstance {
                actor: self.actor,
                mailbox,
            })),
            options: self.options,
            readiness,
        }
    }
}

type RawFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
type RawFactory = Arc<dyn Fn() -> Box<dyn ErasedRawInstance> + Send + Sync + 'static>;

trait ErasedRawInstance: Send {
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
        let mut cleanup = PanicAccumulator::default();
        cleanup.run(|| self.drop_raw());
        cleanup.run(|| self.drop_actor());
        resume_preferred_panic(UnwindPanics {
            primary: primary_panic,
            cleanup: cleanup.take(),
        });
    }
}

impl<R: RawActor> ErasedRawInstance for RawInstance<R> {
    fn run(self: Box<Self>, context: RawRunContext, readiness: Readiness) -> RawFuture {
        Box::pin(async move {
            let Self { actor, mailbox } = *self;
            let incarnation = context.incarnation;
            let myself = actor_ref_from_parts(Arc::clone(&context.member), Arc::clone(&mailbox));
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
            let mailbox_freeze_panic = catch_panic(|| mailbox.freeze(incarnation)).err();
            let resource_freeze_panic = catch_panic(|| owner.raw().freeze_resources()).err();
            let mut cleanup_panic = owner.raw().take_resource_panic();
            keep_first_panic(&mut cleanup_panic, mailbox_freeze_panic);
            keep_first_panic(&mut cleanup_panic, resource_freeze_panic);

            let joined = CatchUnwindFuture::new(owner.raw().join_resources()).await;
            keep_first_panic(&mut cleanup_panic, joined.err());
            let pending = owner.raw().take_resource_panic();
            keep_first_panic(&mut cleanup_panic, pending);
            let raw_drop = catch_panic(|| owner.drop_raw()).err();
            keep_first_panic(&mut cleanup_panic, raw_drop);

            let actor_drop = catch_panic(|| owner.drop_actor()).err();
            keep_first_panic(&mut cleanup_panic, actor_drop);
            // Once actor execution has panicked, teardown is secondary: never
            // replace the actor's original diagnostic. This is the incarnation
            // body's normal return path, so the resume must be unconditional:
            // containing the primary payload here would strand `result` at
            // `None` and report the actor's panic as the framework expect
            // below.
            resume_preferred_panic_outside_unwind(UnwindPanics {
                primary: owner.take_primary_panic(),
                cleanup: cleanup_panic,
            });
            result.expect("an incarnation without a primary panic returns a result")
        })
    }
}

pub(crate) struct RawConstruction {
    source: DefinitionSource<RawFactory, Box<dyn ErasedRawInstance>>,
    options: CommonOptions,
    readiness: Readiness,
}

impl RawConstruction {
    pub(crate) fn options(&self) -> &CommonOptions {
        &self.options
    }

    pub(crate) fn readiness(&self) -> Readiness {
        self.readiness
    }

    pub(crate) fn mode(&self) -> ChildMode {
        if self.source.is_one_shot() {
            ChildMode::OneShot
        } else {
            ChildMode::Restartable
        }
    }

    pub(crate) fn one_shot(&self) -> bool {
        self.source.is_one_shot()
    }

    pub(crate) fn take_spawn(&mut self) -> RawSpawn {
        if let Some(factory) = self.source.restartable() {
            RawSpawn(RawSpawnKind::Restartable(Arc::clone(factory)))
        } else {
            RawSpawn(RawSpawnKind::OneShot(self.source.take_one_shot().expect(
                "one-shot raw actor construction invoked more than once",
            )))
        }
    }

    #[cfg(test)]
    pub(crate) fn for_policy_test(options: CommonOptions, readiness: Readiness) -> Self {
        Self {
            source: DefinitionSource::Restartable(Arc::new(|| {
                unreachable!("policy resolution never constructs the actor")
            })),
            options,
            readiness,
        }
    }
}

pub(crate) struct RawSpawn(RawSpawnKind);

enum RawSpawnKind {
    Restartable(RawFactory),
    OneShot(Box<dyn ErasedRawInstance>),
}

impl RawSpawn {
    pub(crate) async fn run(self, context: RawRunContext, readiness: Readiness) -> ExitResult {
        let instance = match self.0 {
            RawSpawnKind::Restartable(factory) => factory(),
            RawSpawnKind::OneShot(instance) => instance,
        };
        instance.run(context, readiness).await
    }
}

pub(crate) struct RawRunContext {
    pub(crate) id: ChildId,
    pub(crate) incarnation: Incarnation,
    pub(crate) member: Arc<MemberCell>,
    pub(crate) scope: ScopeRef,
    pub(crate) shutdown: Latch,
    pub(crate) abort: Latch,
    pub(crate) ready: CompletionGatedLatch,
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
        ArmingOrder, EventQueue, OffloadPoll, OffloadResource, PanicSlot, QueuedEvent, RawContext,
        RawResources, RawRunContext, SharedOffloadFuture, SharedOffloadState, TimerMessage,
    };
    use crate::{
        ChildId, MailboxShutdown, Readiness,
        cells::{MemberCell, ScopeCell},
        identity::ScopeIdentity,
        mailbox::{ActorRef, MailboxCell, MailboxControl, actor_ref_from_parts},
        policy::{ResolvedDefaults, ScopeFlavor},
        runtime::{
            CompletionGatedLatch, Latch, PanicPayload, Signal, UnwindPanics,
            resume_preferred_panic, resume_preferred_panic_outside_unwind,
        },
        scope::ScopeRef,
    };

    /// Builds a live raw incarnation context whose mailbox is configured and
    /// bound, so `next_ready` can take the busy path without a driver.
    fn bound_raw_context() -> (RawContext<u8>, ActorRef<u8>) {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("raw-actor");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(id.clone());
        member.attach_mailbox(mailbox.clone());
        MailboxControl::configure(&*mailbox, ResolvedDefaults::default().mailbox);
        let incarnation = member
            .take_incarnation_counter()
            .mint()
            .expect("incarnation available");
        MailboxControl::bind(&*mailbox, incarnation);

        let mut scope_identity = ScopeIdentity::new();
        let scope_id = ChildId::from("scope");
        let scope_member = MemberCell::new(
            scope_id.clone(),
            scope_identity
                .mint_membership(&scope_id)
                .expect("membership available"),
        );
        let scope = ScopeCell::new(scope_member, ScopeFlavor::Ordered, ScopeIdentity::new());

        let myself = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
        let context = RawContext::new(
            RawRunContext {
                id,
                incarnation,
                member,
                scope: ScopeRef { cell: scope },
                shutdown: Latch::default(),
                abort: Latch::default(),
                ready: CompletionGatedLatch::default(),
                local_stop: Latch::default(),
                mailbox_shutdown: MailboxShutdown::Drain,
            },
            myself.clone(),
            mailbox,
            Readiness::Immediate,
        );
        (context, myself)
    }

    fn marker(value: usize) -> QueuedEvent<usize> {
        QueuedEvent {
            cancellation: Latch::default(),
            make_message: Box::new(move || value),
        }
    }

    fn value(event: QueuedEvent<usize>) -> usize {
        (event.make_message)()
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
            resume_preferred_panic(UnwindPanics {
                primary: Some(Box::new("primary actor panic")),
                cleanup: Some(Box::new("secondary cleanup panic")),
            });
        }))
        .expect_err("the primary panic is resumed");
        assert_eq!(panic_message(&payload), Some("primary actor panic"));
    }

    #[test]
    fn the_incarnation_return_path_resumes_a_primary_panic_it_solely_owns() {
        let payload = catch_unwind(AssertUnwindSafe(|| {
            resume_preferred_panic_outside_unwind(UnwindPanics {
                primary: Some(Box::new("primary actor panic")),
                cleanup: None,
            });
        }))
        .expect_err("a sole-owned primary panic is never contained");
        assert_eq!(panic_message(&payload), Some("primary actor panic"));

        let payload = catch_unwind(AssertUnwindSafe(|| {
            resume_preferred_panic_outside_unwind(UnwindPanics {
                primary: None,
                cleanup: Some(Box::new("cleanup panic")),
            });
        }))
        .expect_err("cleanup stands in when there is no primary panic");
        assert_eq!(panic_message(&payload), Some("cleanup panic"));

        resume_preferred_panic_outside_unwind(UnwindPanics {
            primary: None,
            cleanup: None,
        });
    }

    struct BlockingPollDrop {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }

    struct PanickingDrop(Arc<AtomicUsize>);

    impl Drop for PanickingDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("contained raw payload destructor panic");
        }
    }

    struct CountedDrop {
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }

    impl Drop for CountedDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.panic_on_drop {
                panic!("queued continuation destructor panic");
            }
        }
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
    fn absent_offload_future_does_not_claim_a_poller() {
        let state = SharedOffloadState::new(
            Box::pin(std::future::pending()),
            Arc::new(PanicSlot::default()),
            Signal::default(),
            Latch::default(),
        );
        let future = state.take_for_poll().expect("fixture future is present");
        let dispose = state.finish_poll(future, OffloadPoll::Finished);
        state.dispose(dispose);

        assert!(state.take_for_poll().is_none());
        assert!(
            !state
                .state
                .lock()
                .expect("offload future mutex starts healthy")
                .polling,
            "a contract-violating re-poll with no future cannot leave a poller claimed"
        );
    }

    #[test]
    fn finished_offload_ledger_entries_are_reclaimed() {
        let mut resources = RawResources::<()>::default();
        for finished in [true, false, true] {
            let completion = Latch::default();
            if finished {
                completion.fire();
            }
            resources.offloads.push(OffloadResource {
                cancellation: Latch::default(),
                finished: completion,
                state: None,
                task: None,
            });
        }

        resources.reclaim_finished();

        assert_eq!(
            resources.offloads.len(),
            1,
            "reclamation retains exactly the unfinished offloads"
        );
        assert!(!resources.offloads[0].finished.is_fired());
    }

    /// `freeze` is the drain that normally empties the continuation queue, but
    /// it is not a guaranteed one: `Drop for RawResources` skips it once the
    /// incarnation is already frozen, and runs it under `catch_panic` so an
    /// earlier cleanup step failing can cut it short. Either way the queue's
    /// own destructor must still route its payloads through the disposal
    /// funnel instead of letting them unwind out of incarnation cleanup.
    #[test]
    fn a_skipped_freeze_still_contains_queued_continuation_destructors() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut resources = RawResources::<CountedDrop>::default();
        let panic = Arc::clone(&resources.panic);
        resources.accepting = false;
        for panic_on_drop in [true, false] {
            resources.continuations.push_back(CountedDrop {
                drops: Arc::clone(&drops),
                panic_on_drop,
            });
        }

        catch_unwind(AssertUnwindSafe(|| drop(resources)))
            .expect("a hostile continuation destructor never escapes incarnation cleanup");

        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "one hostile destructor cannot skip the rest of the queue"
        );
        assert_eq!(
            panic.take().as_ref().and_then(panic_message),
            Some("queued continuation destructor panic"),
            "the contained destructor panic is retained as cleanup evidence"
        );
    }

    /// The ledger's retention bound rests on the reclaim point inside
    /// `next_ready`, not on the one in `wait_for_event`: an actor whose
    /// mailbox never empties returns from every receive without going idle,
    /// so the idle reclaim never runs and finished task handles would
    /// otherwise accumulate for the lifetime of the incarnation.
    #[test]
    fn a_busy_receive_turn_reclaims_finished_offload_ledger_entries() {
        let (mut context, actor) = bound_raw_context();
        actor
            .try_send(1)
            .expect("a bound mailbox accepts the message");
        actor
            .try_send(2)
            .expect("a bound mailbox keeps the actor busy");
        for finished in [true, false, true] {
            let completion = Latch::default();
            if finished {
                completion.fire();
            }
            context.resources.offloads.push(OffloadResource {
                cancellation: Latch::default(),
                finished: completion,
                state: None,
                task: None,
            });
        }

        assert_eq!(
            context.try_recv(),
            Some(1),
            "a readable mailbox returns from ready selection without going idle"
        );

        assert_eq!(
            context.resources.offloads.len(),
            1,
            "the ready-selection turn reclaimed both finished ledger entries"
        );
        assert!(!context.resources.offloads[0].finished.is_fired());
    }

    #[test]
    fn resident_raw_collections_do_not_clone_disposal_per_element() {
        let mut resources = RawResources::<()>::default();
        let baseline = Arc::strong_count(&resources.panic);

        resources.continuations.push_back(());
        resources.events.push(QueuedEvent {
            cancellation: Latch::default(),
            make_message: Box::new(|| ()),
        });
        resources
            .timers
            .replace(7_u8, None, ArmingOrder(1), TimerMessage::Once(()), None);

        assert_eq!(
            Arc::strong_count(&resources.panic),
            baseline,
            "continuations, events, and timers store raw elements without disposal clones"
        );
    }

    #[crate::runtime::test]
    async fn joining_offloads_retains_a_framework_task_panic() {
        let mut resources = RawResources::<()>::default();
        resources.offloads.push(OffloadResource {
            cancellation: Latch::default(),
            finished: Latch::default(),
            state: None,
            task: Some(crate::runtime::spawn_actor_work(async {
                panic!("unit framework offload panic");
            })),
        });

        resources.join_offloads().await;

        let payload = resources
            .panic
            .take()
            .expect("the framework panic is retained for incarnation teardown");
        assert_eq!(
            panic_message(&payload),
            Some("unit framework offload panic")
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

    #[test]
    fn freeze_drains_each_raw_collection_with_independent_containment() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut resources = RawResources::<PanickingDrop>::default();
        for _ in 0..2 {
            resources
                .continuations
                .push_back(PanickingDrop(Arc::clone(&drops)));
        }
        let event_payload = PanickingDrop(Arc::clone(&drops));
        resources.events.push(QueuedEvent {
            cancellation: Latch::default(),
            make_message: Box::new(move || {
                drop(event_payload);
                unreachable!("a disposed event is never materialized")
            }),
        });
        resources.timers.replace(
            1_u8,
            None,
            ArmingOrder(1),
            TimerMessage::Once(PanickingDrop(Arc::clone(&drops))),
            None,
        );

        assert_eq!(resources.freeze(), 2);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            4,
            "one hostile destructor cannot skip later collection elements or drains"
        );
        let payload = resources
            .panic
            .take()
            .expect("the first cleanup panic is retained");
        assert_eq!(
            panic_message(&payload),
            Some("contained raw payload destructor panic")
        );
    }
}

#[cfg(test)]
mod timer_store_tests {
    use std::{
        collections::HashSet,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use super::{ArmingOrder, TimerMessage, TimerStore};

    fn order(value: u64) -> ArmingOrder {
        ArmingOrder(value)
    }

    #[derive(Eq, PartialEq)]
    struct CollidingKey(u8);

    impl std::hash::Hash for CollidingKey {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            0_u8.hash(state);
        }
    }

    struct PanickingHashKey(Arc<AtomicUsize>);

    impl PartialEq for PanickingHashKey {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Eq for PanickingHashKey {}

    impl std::hash::Hash for PanickingHashKey {
        fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
            panic!("timer key hash panic");
        }
    }

    impl Drop for PanickingHashKey {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("timer key destructor panic");
        }
    }

    struct PanickingTimerMessage(Arc<AtomicUsize>);

    impl Drop for PanickingTimerMessage {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("timer message destructor panic");
        }
    }

    fn once(entry: super::TimerEntry<&'static str>) -> &'static str {
        let TimerMessage::Once(message) = entry.message else {
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
            order(1),
            TimerMessage::Once("old-u8"),
            None,
        );
        timers.replace(
            7_u16,
            Some(start + Duration::from_secs(1)),
            order(2),
            TimerMessage::Once("u16"),
            None,
        );
        timers.replace(
            7_u8,
            Some(start + Duration::from_secs(2)),
            order(3),
            TimerMessage::Once("new-u8"),
            None,
        );

        assert_eq!(timers.next_deadline(), Some(start + Duration::from_secs(1)));
        assert_eq!(
            timers.take_due(start + Duration::from_secs(3)),
            [order(2), order(3)],
            "different key types coexist and replacement takes a fresh order"
        );
        assert_eq!(
            once(timers.remove_arming(order(2)).expect("u16 timer remains")),
            "u16"
        );
        assert_eq!(
            once(timers.remove_arming(order(3)).expect("replacement remains")),
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
            order(1),
            TimerMessage::Once("first"),
            None,
        );
        timers.replace(
            CollidingKey(2),
            None,
            order(2),
            TimerMessage::Once("second"),
            None,
        );
        timers.replace(
            CollidingKey(1),
            None,
            order(3),
            TimerMessage::Once("replacement"),
            None,
        );

        assert_eq!(
            once(
                timers
                    .take(&CollidingKey(2))
                    .expect("colliding peer remains registered")
            ),
            "second"
        );
        assert_eq!(
            once(
                timers
                    .take(&CollidingKey(1))
                    .expect("replacement remains registered")
            ),
            "replacement"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn timer_input_cleanup_stays_contained_when_hash_panics() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut timers = TimerStore::default();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            timers.replace(
                PanickingHashKey(Arc::clone(&drops)),
                None,
                order(1),
                TimerMessage::Once(PanickingTimerMessage(Arc::clone(&drops))),
                None,
            );
        }))
        .expect_err("the user key hash panic escapes the timer operation");

        assert_eq!(
            panic.downcast_ref::<&'static str>().copied(),
            Some("timer key hash panic"),
            "the callback panic remains primary"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "both hostile incoming destructors run behind independent boundaries"
        );
        let cleanup = timers
            .disposal
            .panic
            .take()
            .expect("the first destructor panic is retained as cleanup evidence");
        assert!(
            matches!(
                cleanup.downcast_ref::<&'static str>().copied(),
                Some("timer message destructor panic" | "timer key destructor panic")
            ),
            "a hostile incoming destructor is recorded"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn zero_period_timer_cleanup_stays_contained_when_hash_panics() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut timers = TimerStore::default();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            timers.clear_and_dispose(
                PanickingHashKey(Arc::clone(&drops)),
                PanickingTimerMessage(Arc::clone(&drops)),
            );
        }))
        .expect_err("the user key hash panic escapes the clear operation");

        assert_eq!(
            panic.downcast_ref::<&'static str>().copied(),
            Some("timer key hash panic"),
            "the callback panic remains primary"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "both zero-period inputs are destroyed behind containment"
        );
        assert!(
            timers.disposal.panic.take().is_some(),
            "a hostile input destructor is retained as cleanup evidence"
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
                order(index as u64),
                TimerMessage::Once(()),
                None,
            );
        }
        for key in keys.into_iter().rev() {
            assert!(timers.remove(&key));
        }

        assert!(timers.is_empty());
        assert!(timers.deadlines.is_empty());
        assert_eq!(
            timers.lookup_probes, TIMERS,
            "distinct hashes need one exact-key check each, not a vector scan"
        );
    }
}
