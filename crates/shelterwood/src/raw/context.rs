//! Per-incarnation raw actor context and owned resources.

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    hash::Hash,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    ActorRef, ChildId, DeadlineBudget, Incarnation, MailboxShutdown, Readiness,
    cancellation::{CancellationToken, ParentCancellationToken},
    identity::PoisonedCounter,
    mailbox::{AcceptedSequence, MailboxCell, MailboxReceiver},
    runtime::{
        self, CompletionGatedLatch, Latch, PanicAccumulator, PanicPayload, Signal, SignalWatcher,
        UnwindPanics, catch_panic, resume_preferred_panic_outside_unwind,
    },
    scope::ScopeRef,
};

use super::{
    definition::RawRunContext,
    disposal::{CatchUnwindFuture, Contained, PanicSlot, RawDisposal},
    offload::{
        Blocking, DeadlineElapsed, Guard, OffloadResource, SharedOffloadFuture, SharedOffloadState,
    },
};

#[cfg(test)]
use super::offload::OffloadPoll;

mod timers;

use timers::{ArmingOrder, IntervalRearm, TimerEntry, TimerMessage, TimerStore};

type DeferredMessage<M> = Box<dyn FnOnce() -> M + Send + 'static>;
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

struct QueuedEvent<M> {
    cancellation: Latch,
    make_message: DeferredMessage<M>,
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
/// governs external input only (SPEC §6.5: offload completions do not consume
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
            disposal,
        }
    }

    fn push(&self, event: QueuedEvent<M>) {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .push_back(event);
        self.disposal.signal.pulse();
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
        self.disposal.signal.pulse();
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

struct ReadyBatch {
    phase: ReadyBatchPhase,
    mailbox_through: AcceptedSequence,
    offloads_remaining: usize,
}

enum ReadyBatchPhase {
    // Steady state takes one mailbox delivery before its captured offload
    // prefix. Steady-state continuations stay live so one queued by an
    // external handler retains `continue_with`'s next-message priority.
    Steady {
        mailbox_budget: usize,
    },
    // Fired batches drain the entire pre-fire mailbox prefix, constrain
    // continuations to their captured prefix, and retain every due arming
    // until its delivery commits.
    Fired {
        armings: VecDeque<ArmingOrder>,
        continuations_remaining: usize,
    },
}

impl ReadyBatch {
    fn steady(mailbox_through: AcceptedSequence, offloads_remaining: usize) -> Self {
        Self {
            phase: ReadyBatchPhase::Steady { mailbox_budget: 1 },
            mailbox_through,
            offloads_remaining,
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
        debug_assert!(matches!(self.phase, ReadyBatchPhase::Steady { .. }));
        self.phase = ReadyBatchPhase::Fired {
            armings,
            continuations_remaining,
        };
        self.mailbox_through = mailbox_through;
        self.offloads_remaining = offloads_remaining;
    }

    fn mailbox_budget_exhausted(&self) -> bool {
        matches!(self.phase, ReadyBatchPhase::Steady { mailbox_budget: 0 })
    }

    fn mailbox_is_eligible(&self) -> bool {
        match self.phase {
            ReadyBatchPhase::Steady { mailbox_budget } => mailbox_budget > 0,
            ReadyBatchPhase::Fired { .. } => true,
        }
    }

    fn is_fired(&self) -> bool {
        matches!(self.phase, ReadyBatchPhase::Fired { .. })
    }

    fn continuation_is_eligible(&self) -> bool {
        match self.phase {
            ReadyBatchPhase::Steady { .. } => true,
            ReadyBatchPhase::Fired {
                continuations_remaining,
                ..
            } => continuations_remaining > 0,
        }
    }

    fn record_continuation_delivery(&mut self) {
        if let ReadyBatchPhase::Fired {
            continuations_remaining,
            ..
        } = &mut self.phase
        {
            debug_assert!(*continuations_remaining > 0);
            *continuations_remaining -= 1;
        }
    }

    fn record_mailbox_delivery(&mut self) {
        if let ReadyBatchPhase::Steady { mailbox_budget } = &mut self.phase {
            debug_assert!(*mailbox_budget > 0);
            *mailbox_budget -= 1;
        }
    }

    fn next_arming(&self) -> Option<ArmingOrder> {
        let ReadyBatchPhase::Fired { armings, .. } = &self.phase else {
            return None;
        };
        armings.front().copied()
    }

    fn commit_arming(&mut self, arming: ArmingOrder) {
        let ReadyBatchPhase::Fired { armings, .. } = &mut self.phase else {
            debug_assert!(false, "only a fired batch delivers timer armings");
            return;
        };
        let removed = armings.pop_front();
        debug_assert_eq!(removed, Some(arming));
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
    disposal: RawDisposal,
    event_watcher: SignalWatcher,
    offloads: Vec<OffloadResource>,
}

impl<M> Default for RawResources<M> {
    fn default() -> Self {
        let signal = Signal::default();
        let panic = Arc::new(PanicSlot::default());
        let disposal = RawDisposal { panic, signal };
        let event_watcher = disposal.signal.watcher();
        let events = Arc::new(EventQueue::new(disposal.clone()));
        Self {
            accepting: true,
            continuations: ContinuationQueue::new(disposal.clone()),
            continuation_needs_external: false,
            timers: TimerStore::new(disposal.clone()),
            timer_orders: PoisonedCounter::new(),
            ready_batch: None,
            events,
            disposal,
            event_watcher,
            offloads: Vec::new(),
        }
    }
}

impl<M> RawResources<M> {
    fn freeze(&mut self) {
        if !self.accepting {
            return;
        }
        self.accepting = false;
        // Treat the whole freeze as one cleanup transaction. Cancellation
        // contains each latch wake, future disposal and task abort
        // independently, while this outer accumulator keeps one failure from
        // skipping later offloads or any of the collection drains.
        let mut panics = PanicAccumulator::default();
        // An already-retained offload failure happened before this freeze and
        // therefore precedes every synchronous cleanup failure below.
        panics.record(self.disposal.panic.take());
        for offload in &mut self.offloads {
            panics.record(offload.cancel(&self.disposal.panic));
            panics.record(self.disposal.panic.take());
        }
        panics.run(|| self.continuations.clear());
        panics.record(self.disposal.panic.take());
        panics.run(|| self.timers.clear());
        panics.record(self.disposal.panic.take());
        panics.run(|| self.ready_batch = None);
        panics.record(self.disposal.panic.take());
        panics.run(|| self.events.clear());
        panics.record(self.disposal.panic.take());
        if let Some(payload) = panics.take() {
            // The owned raw-incarnation epilogue drains this slot after the
            // freeze and before joining, so publication is delayed until all
            // synchronous cleanup has completed without losing the diagnostic.
            self.disposal.panic.restore_first(payload);
        }
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
            primary: self.disposal.panic.take(),
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
                        self.disposal.panic.record(Box::new(message));
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
        let freeze_panic = catch_panic(|| self.freeze()).err();
        let mut panics = PanicAccumulator::default();
        // `freeze` can transfer a destructor panic into the shared slot. Take
        // that retained application diagnostic after cleanup and preserve it
        // ahead of a direct framework-cleanup panic.
        panics.record(self.disposal.panic.take());
        panics.record(freeze_panic);
    }
}

/// Per-incarnation capabilities supplied to a [`RawActor`](crate::RawActor).
pub struct RawContext<M> {
    id: ChildId,
    incarnation: Incarnation,
    myself: ActorRef<M>,
    scope: ScopeRef,
    shutdown: ParentCancellationToken,
    abort: CancellationToken,
    ready: CompletionGatedLatch,
    local_stop: Latch,
    deferred_init_stop: bool,
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
    pub(super) fn new(
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
            abort: ParentCancellationToken::from_latch(run.abort).token(),
            ready: run.ready,
            local_stop: run.local_stop,
            deferred_init_stop: false,
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
    ///
    /// Available for the whole raw incarnation, teardown included. The
    /// high-level [`crate::StopContext`] withholds its equivalent, so
    /// callback-actor authors capture [`crate::Context::myself`] while live
    /// instead.
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
    /// gate is driven by (§7) — decorators and the blanket handler loop
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
    /// already-accepted prefix (§5.4's close point) — queued continuations
    /// and timers are discarded, and [`recv`](Self::recv) returns `None`.
    /// A raw loop honoring [`MailboxShutdown::Drain`] must then consume the
    /// frozen prefix with [`try_recv`](Self::try_recv); `recv` never drains a
    /// frozen mailbox. Idempotent. This is the primitive the blanket handler
    /// loop's `Context::stop` is built on (§1 principle 5); the child's
    /// configured §11 ladder bounds the stop.
    ///
    /// A cleanup failure raised while freezing — a hostile waker woken by
    /// offload cancellation, or a released destructor — is retained as this
    /// incarnation's exit rather than raised here, so this call returns
    /// normally and the caller's loop keeps running. The payload surfaces
    /// from the next [`recv`](Self::recv)/[`try_recv`](Self::try_recv) or
    /// from the epilogue.
    pub fn stop(&mut self) {
        self.receiver.freeze();
        self.freeze_resources();
        self.local_stop.fire();
    }

    /// Freezes a callback actor at an `AfterInit` initializer's stop request,
    /// but holds supervisor publication until the initializer returns. The
    /// blanket handler then fires automatic readiness and this stop in one
    /// fixed order; projected decorator contexts share the same pending bit.
    pub(crate) fn defer_stop_until_after_init(&mut self) {
        if self.deferred_init_stop {
            return;
        }
        // Record the request before cleanup so an unwind from cleanup still
        // publishes it through the initializer context's Drop fallback.
        self.deferred_init_stop = true;
        self.receiver.freeze();
        self.freeze_resources();
    }

    /// Closes the callback initializer boundary. Consuming the pending bit
    /// lets a successful effective `AfterInit` initializer use the ordinary
    /// `mark_ready` path before its own stop is published. Parent shutdown
    /// keeps the existing no-readiness rule.
    pub(crate) fn finish_callback_init(&mut self, successful: bool) {
        let deferred_stop = std::mem::take(&mut self.deferred_init_stop);
        if successful && self.readiness == Readiness::AfterInit {
            self.mark_ready();
        }
        if deferred_stop {
            self.local_stop.fire();
        }
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.deferred_init_stop || self.local_stop.is_fired() || self.shutdown.is_cancelled()
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
    /// that does not consume mailbox capacity (SPEC §6.5). That storage is
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
        self.start_offload(work, continuation, deadline.into())
            .map(Guard::detach)
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
        self.start_offload(work, continuation, deadline.into())
    }

    /// Starts blocking work with cancellation tied to shutdown and future drop.
    ///
    /// Cancellation is cooperative. If this future is dropped or its actor is
    /// hard-aborted, the OS thread detaches and can outlive the incarnation.
    /// A blocking-pool rejection during runtime teardown uses a detached
    /// Shelterwood thread; an operation that never runs — cancelled with the
    /// runtime, or with no thread left to start it — makes the returned
    /// future panic with a runtime-teardown cancellation diagnostic when
    /// awaited.
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
    /// Any incarnation-owned disposal panic retained by the time this path
    /// runs resumes here before another event is delivered or a stop is
    /// reported: an offload-work panic, and — because the stop branches
    /// freeze first — a destructor panic from the queued continuations,
    /// armed timers, queued offload completions and offload futures that the
    /// freeze releases, a waker panic from the `Guard::finished()` waiters
    /// and cancellation latches that cancelling those offloads wakes, and a
    /// panic raised by aborting an offload task. Retention is the guarantee,
    /// not a join: a payload recorded after this check is still the
    /// incarnation's exit, but is classified by the epilogue and cannot
    /// suppress `on_stop`.
    ///
    /// A panic in an offload's continuation closure — the `FnOnce` that
    /// builds the message from the offload result, not a
    /// [`continue_with`](Self::continue_with) continuation, which is a plain
    /// stored message whose construction cannot panic here — surfaces
    /// directly from this receive call.
    ///
    /// A panic escaping this call leaves the fired selection cut installed,
    /// so the next receive retries the same timer arming rather than
    /// discarding the remaining batch. A raw loop that catches such a panic
    /// and receives again therefore repeats an interval whose user `Clone`
    /// panics deterministically; clear that key with
    /// [`clear_timer`](Self::clear_timer) — or [`stop`](Self::stop) — before
    /// resuming the loop.
    pub async fn recv(&mut self) -> Option<M> {
        loop {
            if self.local_stop.is_fired() {
                self.receiver.freeze();
                self.freeze_resources();
                // `stop()` originates on this task, but the configured
                // shutdown ladder is owned by the driver. The driver's helper
                // only observes the local-stop latch and forwards
                // `ChildEvent::SelfStop`; it is the driver's stop ladder that
                // fires the shared shutdown token. Wait for that token before
                // ending the raw loop; removing this await would let a local
                // stop bypass that cross-task handshake.
                self.shutdown.cancelled().await;
                self.resources.resume_pending_panic();
                return None;
            }
            if self.shutdown.is_cancelled() {
                // Freeze locally as part of observing shutdown. The driver
                // also freezes before cancellation, but correctness of this
                // receive boundary does not depend on that remote ordering.
                self.receiver.freeze();
                self.freeze_resources();
                self.resources.resume_pending_panic();
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
    /// Outside shutdown drain, this resumes a retained disposal panic before
    /// returning another event; a panic in an offload's continuation closure
    /// (the message-building `FnOnce`, not a
    /// [`continue_with`](Self::continue_with) continuation, which is a plain
    /// stored message) surfaces directly from this call. During drain it
    /// freezes first, then resumes any incarnation-owned disposal panic
    /// retained by that point — an offload-work panic, a destructor panic
    /// from the continuations, timers, queued completions and offload futures
    /// the freeze releases, a waker panic from the `Guard::finished()`
    /// waiters and cancellation latches that cancelling those offloads wakes,
    /// or a panic raised by aborting an offload task — before reading the
    /// frozen accepted mailbox
    /// prefix. Retention is the guarantee, not a join: a payload recorded
    /// after the check is still the incarnation's exit, but is classified by
    /// the epilogue and cannot suppress `on_stop`.
    ///
    /// A panic escaping this call leaves the fired selection cut installed,
    /// so the next receive retries the same timer arming rather than
    /// discarding the remaining batch. A raw loop that catches such a panic
    /// and receives again therefore repeats an interval whose user `Clone`
    /// panics deterministically; clear that key with
    /// [`clear_timer`](Self::clear_timer) — or [`stop`](Self::stop) — before
    /// resuming the loop.
    pub fn try_recv(&mut self) -> Option<M> {
        if self.is_stopping() {
            // Establish the receive boundary locally just as `recv` does. Do
            // not rely on the driver's mailbox-freeze ordering relative to
            // the shutdown latch this call observes.
            self.receiver.freeze();
            self.freeze_resources();
            self.resources.resume_pending_panic();
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
    ) -> Result<Guard, Rejected<(F, C)>>
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
        let guard = Guard {
            cancellation: cancellation.clone(),
            finished: finished.clone(),
            armed: true,
        };
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
            self.resources.disposal.clone(),
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

    fn pop_continuation(&mut self, batch: &mut ReadyBatch, is_lead_slot: bool) -> Option<M> {
        if (!is_lead_slot || !self.resources.continuation_needs_external)
            && batch.continuation_is_eligible()
            && let Some(message) = self.resources.continuations.pop_front()
        {
            batch.record_continuation_delivery();
            self.resources.continuation_needs_external = true;
            return Some(message);
        }
        None
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
    /// count) — a cross-source ordering the spec leaves unspecified (SPEC §6.1:
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
            if let Some(message) = self.pop_continuation(&mut batch, true) {
                self.resources.ready_batch = Some(batch);
                return Some(message);
            }

            if batch.mailbox_is_eligible() {
                let message = self.receiver.try_recv_live_through(batch.mailbox_through);
                if let Some(message) = message {
                    batch.record_mailbox_delivery();
                    self.resources.continuation_needs_external = false;
                    self.resources.ready_batch = Some(batch);
                    return Some(message);
                }
            }

            if batch.offloads_remaining > 0 {
                while let Some(event) = self
                    .resources
                    .events
                    .pop_through(&mut batch.offloads_remaining)
                {
                    let (restored, message) = self
                        .with_ready_batch_installed(batch, |this| this.materialize_event(event));
                    batch = restored;
                    if let Some(message) = message {
                        self.resources.continuation_needs_external = false;
                        self.resources.ready_batch = Some(batch);
                        return Some(message);
                    }
                }
            }

            // A steady batch may have exhausted its captured external turn
            // while a continuation handler made later external work ready.
            // Start a fresh bounded turn before allowing another continuation
            // so that work receives §6.1's mandatory fairness opportunity.
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

            if let Some(message) = self.pop_continuation(&mut batch, false) {
                self.resources.ready_batch = Some(batch);
                return Some(message);
            }

            while let Some(arming) = batch.next_arming() {
                let (restored, message) =
                    self.with_ready_batch_installed(batch, |this| this.deliver_timer(arming));
                batch = restored;
                batch.commit_arming(arming);
                if let Some(message) = message {
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

    /// Runs a callback-capable selection step while the authoritative batch
    /// remains installed. If user code unwinds, the next caught receive sees
    /// the same cutoffs and any not-yet-committed timer armings.
    fn with_ready_batch_installed<R>(
        &mut self,
        batch: ReadyBatch,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> (ReadyBatch, R) {
        debug_assert!(self.resources.ready_batch.is_none());
        self.resources.ready_batch = Some(batch);
        let result = operation(self);
        let batch = self
            .resources
            .ready_batch
            .take()
            .expect("callback-capable selection keeps its ready batch installed");
        (batch, result)
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
        match self.resources.timers.rearm_interval(arming, runtime::now()) {
            IntervalRearm::Missing => return None,
            IntervalRearm::OneShot => {}
            IntervalRearm::Interval(message) => return Some(message),
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

    /// Freezes incarnation resources on the exit path, discarding §6.2's
    /// queued continuations.
    pub(crate) fn freeze_resources(&mut self) {
        self.resources.freeze();
    }

    pub(crate) async fn join_resources(&mut self) {
        self.resources.join_offloads().await;
    }

    pub(super) fn take_resource_panic(&self) -> Option<PanicPayload> {
        self.resources.disposal.panic.take()
    }
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
        task::{Context, Poll, Wake, Waker},
        thread,
        time::Duration,
    };

    use super::{
        ArmingOrder, EventQueue, OffloadPoll, OffloadResource, PanicSlot, QueuedEvent, RawContext,
        RawDisposal, RawResources, RawRunContext, SharedOffloadFuture, SharedOffloadState,
        TimerMessage,
    };
    use crate::{
        ChildId, MailboxShutdown, Readiness,
        cells::{MemberCell, ScopeCell},
        identity::ScopeIdentity,
        mailbox::{
            ActorRef, MailboxCell, MailboxControl, MailboxEffectQueue, actor_ref_from_parts,
        },
        policy::{ResolvedDefaults, ScopeFlavor},
        runtime::{
            CompletionGatedLatch, Latch, PanicPayload, Signal, UnwindPanics,
            resume_preferred_panic, resume_preferred_panic_outside_unwind,
        },
        scope::ScopeRef,
    };

    /// Builds a live raw incarnation context whose mailbox is configured and
    /// bound, so `next_ready` can take the busy path without a driver. The
    /// returned latch is the context's own shutdown token.
    fn bound_raw_context_for<M: Send + 'static>() -> (RawContext<M>, ActorRef<M>, Latch) {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("raw-actor");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(id.clone(), crate::runtime::mailbox_runtime());
        member.attach_mailbox(mailbox.clone());
        let mut effects = MailboxEffectQueue::default();
        let token = MailboxControl::configure(
            &*mailbox,
            ResolvedDefaults::default().mailbox(),
            &mut effects,
        );
        let incarnation = member
            .take_incarnation_counter()
            .mint()
            .expect("incarnation available");
        MailboxControl::bind(&*mailbox, token, incarnation, &mut effects);

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
        let shutdown = Latch::default();
        let context = RawContext::new(
            RawRunContext {
                id,
                incarnation,
                member,
                scope: ScopeRef { cell: scope },
                shutdown: shutdown.clone(),
                abort: Latch::default(),
                ready: CompletionGatedLatch::default(),
                local_stop: Latch::default(),
                mailbox_shutdown: MailboxShutdown::Drain,
            },
            myself.clone(),
            mailbox,
            Readiness::Immediate,
        );
        (context, myself, shutdown)
    }

    fn bound_raw_context() -> (RawContext<u8>, ActorRef<u8>) {
        let (context, actor, _shutdown) = bound_raw_context_for();
        (context, actor)
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

    struct PanickingWake(&'static str);

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic!("{}", self.0);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            panic!("{}", self.0);
        }
    }

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

    #[derive(Debug)]
    struct PanicOnceClone {
        clones: Arc<AtomicUsize>,
        value: u8,
    }

    impl Clone for PanicOnceClone {
        fn clone(&self) -> Self {
            if self.clones.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("interval message clone panic");
            }
            Self {
                clones: Arc::clone(&self.clones),
                value: self.value,
            }
        }
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
        let mut watcher = queue.disposal.signal.watcher();
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
            RawDisposal {
                panic: Arc::clone(&panic),
                signal: Signal::default(),
            },
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
            RawDisposal::default(),
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
        let panic = Arc::clone(&resources.disposal.panic);
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

    #[crate::runtime::test]
    async fn recv_freezes_its_mailbox_when_it_observes_shutdown() {
        let (mut context, actor, shutdown) = bound_raw_context_for::<u8>();
        actor
            .try_send(1)
            .expect("the live mailbox accepts before shutdown");
        shutdown.fire();

        assert_eq!(context.recv().await, None);
        assert_eq!(
            actor
                .try_send(2)
                .expect_err("recv's local freeze closes the acceptance boundary")
                .kind,
            crate::SendErrorKind::NotRunning
        );
    }

    #[test]
    fn try_recv_freezes_its_mailbox_when_it_observes_shutdown() {
        let (mut context, actor, shutdown) = bound_raw_context_for::<u8>();
        actor
            .try_send(1)
            .expect("the live mailbox accepts before shutdown");
        shutdown.fire();

        assert_eq!(
            context.try_recv(),
            Some(1),
            "drain mode still reads the prefix accepted before the local freeze"
        );
        assert_eq!(
            actor
                .try_send(2)
                .expect_err("try_recv's local freeze closes the acceptance boundary")
                .kind,
            crate::SendErrorKind::NotRunning
        );
    }

    #[test]
    fn cancelled_queued_event_is_disposed_without_materializing_its_message() {
        let (mut context, _actor, _shutdown) = bound_raw_context_for::<u8>();
        let cancellation = Latch::default();
        cancellation.fire();
        let drops = Arc::new(AtomicUsize::new(0));
        let invoked = Arc::new(AtomicUsize::new(0));
        let event_payload = PanickingDrop(Arc::clone(&drops));
        let invoked_by_event = Arc::clone(&invoked);
        context.resources.events.push(QueuedEvent {
            cancellation,
            make_message: Box::new(move || {
                invoked_by_event.fetch_add(1, Ordering::SeqCst);
                drop(event_payload);
                7
            }),
        });

        assert_eq!(
            context.try_recv(),
            None,
            "a completion cancelled after queueing does not deliver a message"
        );
        assert_eq!(
            invoked.load(Ordering::SeqCst),
            0,
            "the cancelled completion never runs its message builder"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the queued completion is disposed at materialization time"
        );
        let payload = context
            .resources
            .disposal
            .panic
            .take()
            .expect("the hostile event destructor is retained by raw disposal");
        assert_eq!(
            panic_message(&payload),
            Some("contained raw payload destructor panic")
        );
    }

    #[test]
    fn a_caught_interval_clone_panic_preserves_the_fired_batch_for_retry() {
        let clones = Arc::new(AtomicUsize::new(0));
        let mut context = bound_raw_context_for::<PanicOnceClone>().0;
        let arming = ArmingOrder(1);
        let now = crate::runtime::now();
        context.resources.timers.replace(
            "interval",
            Some(now),
            arming,
            TimerMessage::Interval(
                PanicOnceClone {
                    clones: Arc::clone(&clones),
                    value: 7,
                },
                Clone::clone,
            ),
            Some(Duration::from_secs(1)),
        );

        let panic = catch_unwind(AssertUnwindSafe(|| context.try_recv()))
            .expect_err("the first interval clone panic escapes the receive call");
        assert_eq!(panic_message(&panic), Some("interval message clone panic"));
        assert!(
            context
                .resources
                .ready_batch
                .as_ref()
                .is_some_and(|batch| batch.next_arming() == Some(arming)),
            "the caught panic leaves the due arming in its installed batch"
        );

        let message = context
            .try_recv()
            .expect("the next receive retries the same interval firing");
        assert_eq!(message.value, 7);
        assert_eq!(clones.load(Ordering::SeqCst), 2);
        assert!(
            context.resources.timers.next_deadline().is_some(),
            "a successful retry rearms the interval"
        );
    }

    #[test]
    fn a_caught_offload_continuation_panic_preserves_later_fired_work() {
        let mut context = bound_raw_context_for::<u8>().0;
        let arming = ArmingOrder(1);
        let now = crate::runtime::now();
        context
            .resources
            .timers
            .replace("timer", Some(now), arming, TimerMessage::Once(7), None);
        context.resources.events.push(QueuedEvent {
            cancellation: Latch::default(),
            make_message: Box::new(|| panic!("offload continuation panic")),
        });

        let panic = catch_unwind(AssertUnwindSafe(|| context.try_recv()))
            .expect_err("the offload continuation panic escapes the receive call");
        assert_eq!(panic_message(&panic), Some("offload continuation panic"));
        assert!(
            context
                .resources
                .ready_batch
                .as_ref()
                .is_some_and(|batch| batch.next_arming() == Some(arming)),
            "the caught continuation panic leaves later timer work in the batch"
        );
        assert_eq!(
            context.try_recv(),
            Some(7),
            "the next receive completes the fired batch instead of losing it"
        );
    }

    #[test]
    fn clearing_an_elapsed_undelivered_timer_skips_its_captured_arming() {
        let mut context = bound_raw_context_for::<u8>().0;
        let now = crate::runtime::now();
        context.resources.timers.replace(
            "first",
            Some(now),
            ArmingOrder(1),
            TimerMessage::Once(1),
            None,
        );
        context.resources.timers.replace(
            "second",
            Some(now),
            ArmingOrder(2),
            TimerMessage::Once(2),
            None,
        );

        assert_eq!(context.try_recv(), Some(1));
        assert!(
            context.clear_timer(&"second"),
            "an elapsed timer remains clearable before delivery"
        );
        assert_eq!(
            context.try_recv(),
            None,
            "the fired batch skips an arming removed after its cut was captured"
        );
    }

    #[test]
    fn resident_raw_collections_do_not_clone_disposal_per_element() {
        let mut resources = RawResources::<()>::default();
        let baseline = Arc::strong_count(&resources.disposal.panic);

        resources.continuations.push_back(());
        resources.events.push(QueuedEvent {
            cancellation: Latch::default(),
            make_message: Box::new(|| ()),
        });
        resources
            .timers
            .replace(7_u8, None, ArmingOrder(1), TimerMessage::Once(()), None);

        assert_eq!(
            Arc::strong_count(&resources.disposal.panic),
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
            .disposal
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
                resources.disposal.clone(),
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
    fn freeze_preserves_an_offload_wake_panic_ahead_of_later_collection_disposal() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut resources = RawResources::<PanickingDrop>::default();
        resources
            .continuations
            .push_back(PanickingDrop(Arc::clone(&drops)));

        let finished = Latch::default();
        let mut waiter = Box::pin(finished.fired());
        let hostile = Waker::from(Arc::new(PanickingWake("first finished wake panic")));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        resources.offloads.push(OffloadResource {
            cancellation: Latch::default(),
            finished: finished.clone(),
            state: None,
            task: None,
        });

        resources.freeze();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let payload = resources
            .disposal
            .panic
            .take()
            .expect("the first cleanup failure is retained");
        assert_eq!(panic_message(&payload), Some("first finished wake panic"));
    }

    #[test]
    fn freeze_preserves_future_disposal_ahead_of_its_later_finished_wake() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut resources = RawResources::<()>::default();
        let finished = Latch::default();
        let mut waiter = Box::pin(finished.fired());
        let hostile = Waker::from(Arc::new(PanickingWake("later finished wake panic")));
        assert!(
            waiter
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        let state = SharedOffloadState::new(
            Box::pin(BlockingPollDrop {
                entered: Arc::new(Barrier::new(1)),
                release: Arc::new(Barrier::new(1)),
                drops: Arc::clone(&drops),
                panic_on_drop: true,
            }),
            resources.disposal.clone(),
            finished.clone(),
        );
        resources.offloads.push(OffloadResource {
            cancellation: Latch::default(),
            finished: finished.clone(),
            state: Some(state),
            task: None,
        });

        resources.freeze();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let payload = resources
            .disposal
            .panic
            .take()
            .expect("the first cleanup failure is retained");
        assert_eq!(
            panic_message(&payload),
            Some("unit offload destructor panic")
        );
    }

    #[test]
    fn repeated_freeze_preserves_the_first_failure_without_repeating_disposal() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut resources = RawResources::<PanickingDrop>::default();
        resources
            .continuations
            .push_back(PanickingDrop(Arc::clone(&drops)));

        resources.freeze();
        resources.freeze();
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the freeze transition is one-shot"
        );

        let payload = catch_unwind(AssertUnwindSafe(|| drop(resources)))
            .expect_err("drop resumes the failure retained by the first freeze");
        assert_eq!(
            panic_message(&payload),
            Some("contained raw payload destructor panic")
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "drop does not re-run already completed disposal"
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

        resources.freeze();
        assert_eq!(
            drops.load(Ordering::SeqCst),
            4,
            "one hostile destructor cannot skip later collection elements or drains"
        );
        let payload = resources
            .disposal
            .panic
            .take()
            .expect("the first cleanup panic is retained");
        assert_eq!(
            panic_message(&payload),
            Some("contained raw payload destructor panic")
        );
    }

    /// A callback actor whose initialization always fails, so `Handler::run`
    /// takes the error path that returns without installing an actor.
    struct RefusingInit;

    impl crate::Actor for RefusingInit {
        type Msg = u8;
        type Args = ();

        async fn init(
            _args: Self::Args,
            _context: &mut crate::Context<'_, Self>,
        ) -> Result<Self, crate::ExitError> {
            Err(crate::ExitError::message("initialization refused"))
        }

        async fn handle(
            &mut self,
            _message: Self::Msg,
            _context: &mut crate::Context<'_, Self>,
        ) -> crate::ExitResult {
            Ok(())
        }
    }

    // The handler lives in `crate::actor`, but its one-run contract is only
    // observable against a live raw incarnation, which this module's fixture
    // owns. A failed initialization spends the handler exactly as a
    // successful one does.
    #[crate::runtime::test]
    #[should_panic(expected = "handler actor initialization invoked more than once")]
    async fn handler_initialization_runs_at_most_once() {
        let (mut context, _myself) = bound_raw_context();
        let mut handler = crate::actor::Handler::<RefusingInit>::new(());
        crate::RawActor::run(&mut handler, &mut context)
            .await
            .expect_err("initialization failure exits the incarnation");
        let _ = crate::RawActor::run(&mut handler, &mut context).await;
    }
}
