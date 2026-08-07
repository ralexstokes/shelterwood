//! Minimal loop-owning raw actors and their incarnation context.

use std::{
    any::Any,
    collections::VecDeque,
    fmt,
    future::Future,
    hash::Hash,
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
type SharedWork = Arc<Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>>;

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
        let mut pending = self.payload.lock().expect("offload panic mutex poisoned");
        if pending.is_none() {
            *pending = Some(payload);
        }
    }

    fn take(&self) -> Option<PanicPayload> {
        self.payload
            .lock()
            .expect("offload panic mutex poisoned")
            .take()
    }
}

struct SequencedEvent<M> {
    sequence: u64,
    event: QueuedEvent<M>,
}

struct EventQueue<M> {
    queue: Mutex<VecDeque<SequencedEvent<M>>>,
    next_sequence: Mutex<u64>,
    signal: Signal,
}

impl<M> Default for EventQueue<M> {
    fn default() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            next_sequence: Mutex::new(0),
            signal: Signal::default(),
        }
    }
}

impl<M> EventQueue<M> {
    fn push(&self, event: QueuedEvent<M>) {
        let sequence = {
            let mut next = self
                .next_sequence
                .lock()
                .expect("event sequence mutex poisoned");
            *next = next.saturating_add(1);
            *next
        };
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .push_back(SequencedEvent { sequence, event });
        self.signal.pulse();
    }

    fn watermark(&self) -> u64 {
        *self
            .next_sequence
            .lock()
            .expect("event sequence mutex poisoned")
    }

    fn pop(&self) -> Option<QueuedEvent<M>> {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .pop_front()
            .map(|item| item.event)
    }

    fn pop_through(&self, sequence: u64) -> Option<QueuedEvent<M>> {
        let mut queue = self.queue.lock().expect("actor event queue mutex poisoned");
        if queue.front().is_some_and(|item| item.sequence <= sequence) {
            queue.pop_front().map(|item| item.event)
        } else {
            None
        }
    }

    fn clear(&self) {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .clear();
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

struct FiredTimerBatch {
    armings: VecDeque<u64>,
    continuations_remaining: usize,
    mailbox_through: u64,
    mailbox_complete: bool,
    offloads_through: u64,
    offloads_complete: bool,
}

struct SharedOffloadFuture(SharedWork);

impl Future for SharedOffloadFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut TaskPollContext<'_>) -> Poll<Self::Output> {
        let mut state = self.0.lock().expect("offload future mutex poisoned");
        let Some(future) = state.as_mut() else {
            return Poll::Ready(());
        };
        if future.as_mut().poll(context).is_ready() {
            state.take();
            Poll::Ready(())
        } else {
            Poll::Pending
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
            let future = state.lock().expect("offload future mutex poisoned").take();
            drop(future);
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
    timers: Vec<TimerEntry<M>>,
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
            timers: Vec::new(),
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
        self.continuations.clear();
        self.timers.clear();
        self.fired_batch = None;
        for offload in &mut self.offloads {
            offload.cancel();
        }
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
        self.resume_pending_panic();
    }
}

impl<M> Drop for RawResources<M> {
    fn drop(&mut self) {
        let _ = self.freeze();
        if !std::thread::panicking() {
            self.resume_pending_panic();
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
        let Some(index) = self
            .resources
            .timers
            .iter()
            .position(|entry| entry.key.as_any().downcast_ref::<K>() == Some(key))
        else {
            return false;
        };
        self.resources.timers.swap_remove(index);
        true
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
        if let Some(index) = self
            .resources
            .timers
            .iter()
            .position(|entry| entry.key.as_any().downcast_ref::<K>() == Some(&key))
        {
            self.resources.timers.swap_remove(index);
        }
        self.resources.next_timer_order = self.resources.next_timer_order.saturating_add(1);
        let now = crate::driver::now();
        let deadline = now.checked_add(after);
        self.resources.timers.push(TimerEntry {
            key: Box::new(StoredTimerKey(key)),
            deadline,
            arming_order: self.resources.next_timer_order,
            message,
            period,
        });
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
        let expires_at = started_at.checked_add(deadline);
        let event_cancellation = cancellation.clone();
        let event_finished = finished.clone();
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
            event_finished.fire();
        };
        let state: SharedWork = Arc::new(Mutex::new(Some(Box::pin(operation))));
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
                    while let Some(event) =
                        self.resources.events.pop_through(batch.offloads_through)
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
        let mut elapsed: Vec<_> = self
            .resources
            .timers
            .iter()
            .filter(|timer| timer.deadline.is_some_and(|deadline| deadline <= now))
            .map(|timer| (timer.deadline, timer.arming_order))
            .collect();
        if elapsed.is_empty() {
            return;
        }
        elapsed.sort_unstable();
        self.resources.fired_batch = Some(FiredTimerBatch {
            armings: elapsed.into_iter().map(|(_, arming)| arming).collect(),
            continuations_remaining: self.resources.continuations.len(),
            mailbox_through: self.receiver.accepted_sequence(),
            mailbox_complete: false,
            offloads_through: self.resources.events.watermark(),
            offloads_complete: false,
        });
    }

    fn deliver_timer(&mut self, arming: u64) -> Option<M> {
        let index = self
            .resources
            .timers
            .iter()
            .position(|timer| timer.arming_order == arming)?;
        let TimerEntry {
            deadline,
            message,
            period,
            ..
        } = &mut self.resources.timers[index];
        match message {
            TimerMessage::Once(message) => {
                let message = message.take();
                self.resources.timers.swap_remove(index);
                message
            }
            TimerMessage::Interval(make_message) => {
                let period = period.expect("an interval timer must retain its period");
                let now = crate::driver::now();
                *deadline = now.checked_add(period);
                Some(make_message())
            }
        }
    }

    fn next_timer_deadline(&self) -> Option<Instant> {
        self.resources
            .timers
            .iter()
            .filter_map(|timer| timer.deadline)
            .min()
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
        self.resources.resume_pending_panic();
    }

    pub(crate) async fn join_resources(&mut self) {
        self.resources.join_offloads().await;
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
    fn run(self: Box<Self>, context: RawRunContext) -> RawFuture;
}

struct RawInstance<R: RawActor> {
    actor: R,
    mailbox: Arc<MailboxCell<R::Msg>>,
}

struct RawIncarnationOwner<R: RawActor> {
    raw: Option<RawContext<R::Msg>>,
    actor: Option<R>,
}

impl<R: RawActor> RawIncarnationOwner<R> {
    fn new(raw: RawContext<R::Msg>, actor: R) -> Self {
        Self {
            raw: Some(raw),
            actor: Some(actor),
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

    fn drop_actor_catching(&mut self) {
        let actor = self.actor.take();
        let _ = catch_unwind(AssertUnwindSafe(|| drop(actor)));
    }
}

impl<R: RawActor> Drop for RawIncarnationOwner<R> {
    fn drop(&mut self) {
        // A hard abort destroys the incarnation future instead of polling its
        // teardown epilogue. Preserve §5.5's resource-before-actor order, but
        // put a boundary around each destructor so two panics cannot abort the
        // process. The resource panic is primary: it may be an owned offload
        // panic that completed before cancellation was requested.
        let raw_panic = catch_unwind(AssertUnwindSafe(|| self.drop_raw())).err();
        let actor_panic = catch_unwind(AssertUnwindSafe(|| self.drop_actor())).err();
        if !std::thread::panicking()
            && let Some(payload) = raw_panic.or(actor_panic)
        {
            resume_unwind(payload);
        }
    }
}

impl<R: RawActor> ErasedRawInstance for RawInstance<R> {
    fn readiness(&self) -> Readiness {
        self.actor.readiness()
    }

    fn run(self: Box<Self>, context: RawRunContext) -> RawFuture {
        Box::pin(async move {
            let Self { actor, mailbox } = *self;
            let incarnation = context.incarnation;
            let myself = ActorRef::new(Arc::clone(&context.member), Arc::clone(&mailbox));
            let readiness = context.readiness_override.unwrap_or(actor.readiness());
            let raw = RawContext::new(context, myself, Arc::clone(&mailbox), readiness);
            let mut owner = RawIncarnationOwner::new(raw, actor);
            let outcome = {
                let (actor, raw) = owner.parts();
                CatchUnwindFuture::new(actor.run(raw)).await
            };
            mailbox.freeze(incarnation);
            owner.raw().freeze_resources();
            owner.raw().join_resources().await;
            owner.drop_raw();
            match outcome {
                Ok(result) => {
                    owner.drop_actor();
                    result
                }
                Err(payload) => {
                    owner.drop_actor_catching();
                    resume_unwind(payload)
                }
            }
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
    pub(crate) readiness_override: Option<Readiness>,
    pub(crate) mailbox_shutdown: MailboxShutdown,
}
