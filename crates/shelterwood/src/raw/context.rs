//! Per-incarnation raw actor context and owned resources.

mod resources;
mod select;
#[cfg(test)]
mod tests;
mod timers;

use std::{fmt, future::Future, hash::Hash, sync::Arc, time::Duration};

use crate::{
    ActorRef, ChildId, DeadlineBudget, Incarnation, MailboxShutdown, Readiness,
    cells::{CancellationToken, ParentCancellationToken},
    mailbox::{MailboxCell, MailboxReceiver},
    runtime::{self, CompletionGatedLatch, Latch},
    scope::ScopeRef,
};

use super::{
    definition::RawRunContext,
    disposal::{CatchUnwindFuture, Contained, PanicSlot},
    offload::{
        Blocking, DeadlineElapsed, Guard, OffloadResource, SharedOffloadFuture, SharedOffloadState,
    },
};

#[cfg(test)]
use super::offload::OffloadPoll;

use resources::{QueuedEvent, RawResources};
use timers::TimerMessage;

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
    /// readiness gate is driven by — decorators and the blanket handler loop
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
    /// already-accepted prefix — queued continuations
    /// and timers are discarded, and [`recv`](Self::recv) returns `None`.
    /// A raw loop honoring [`MailboxShutdown::Drain`] must then consume the
    /// frozen prefix with [`try_recv`](Self::try_recv); `recv` never drains a
    /// frozen mailbox. Idempotent. This is the primitive the blanket handler
    /// loop's `Context::stop` is built on; the child's configured shutdown
    /// escalation (grace, then abort) bounds the stop.
    ///
    /// A cleanup failure raised while freezing — a hostile waker woken by
    /// offload cancellation, or a released destructor — is retained as this
    /// incarnation's exit rather than raised here, so this call returns
    /// normally and the caller's loop keeps running. The payload surfaces
    /// from the next [`recv`](Self::recv)/[`try_recv`](Self::try_recv) or
    /// from the epilogue.
    pub fn stop(&mut self) {
        self.freeze_intake();
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
        self.freeze_intake();
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

    /// Whether a stop has begun: a local stop (published or deferred until
    /// after init), cooperative shutdown, or escalation. B.2's stopping rule
    /// applies to every context uniformly, so this matches
    /// `TaskContext::is_stopping` in counting the abort token too.
    pub(crate) fn is_stopping(&self) -> bool {
        self.deferred_init_stop
            || self.local_stop.is_fired()
            || self.shutdown.is_cancelled()
            || self.abort.is_cancelled()
    }

    /// Queues an actor-local continuation ahead of external input.
    pub fn continue_with(&mut self, message: M) -> Result<(), Rejected<M>> {
        if self.rejects_new_work() {
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
        if self.rejects_new_work() {
            return Err(Rejected::new((key, message)));
        }
        self.replace_timer(key, TimerMessage::Once(message), after);
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
        if self.rejects_new_work() {
            return Err(Rejected::new((key, message)));
        }
        if period.is_zero() {
            self.resources.timers.clear_and_dispose(key, message);
            return Ok(());
        }
        self.replace_timer(
            key,
            TimerMessage::Interval {
                message,
                clone: Clone::clone,
                period,
            },
            period,
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
    /// that does not consume mailbox capacity. That storage is
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
    /// Unlike `continue_with`, timers, and offloads, this operation has no
    /// stopping gate: it deliberately keeps working from stop paths, because
    /// it is the one resource operation teardown code may still need. The
    /// returned [`Blocking`] is caller-owned rather than tracked in the
    /// incarnation resource ledger, so intake freeze and teardown never wait
    /// on it or depend on its completion.
    /// Awaiting the returned [`Blocking`] resumes a panic raised inside the
    /// closure at the await point (distinct from the runtime-teardown
    /// cancellation panic below).
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
    /// reported: an offload-work panic, a waker panic from the
    /// `Guard::finished()` waiters that *ordinary* offload completion wakes,
    /// and — because the stop branches freeze first — a destructor panic from
    /// the queued continuations, armed timers, queued offload completions and
    /// offload futures that the freeze releases, a waker panic from the
    /// cancellation latches and completion notifications that cancelling
    /// those offloads wakes, and a panic raised by aborting an offload task.
    /// The completion-waiter class is not teardown-only: a third party
    /// awaiting `Guard::finished()` with a panicking waker fails a live
    /// incarnation here, and a failed incarnation skips `on_stop`. Retention is the guarantee, not a join: a payload recorded
    /// after this check is still the incarnation's exit, but is classified by
    /// the epilogue and cannot suppress `on_stop`. The one exception is that
    /// same completion wake: because retention has to be established before
    /// the ledger may forget the work, a completion waker that *blocks*
    /// instead of panicking pins its entry, and incarnation teardown then
    /// joins it — an accepted trade against retiring work whose wake is still
    /// in flight.
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
                self.freeze_intake();
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
                self.freeze_intake();
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
    /// stored message) surfaces directly from this call. The outside-drain
    /// class is not only offload work: a waker panic from the
    /// `Guard::finished()` waiters that ordinary offload completion wakes is
    /// retained the same way, so a third party awaiting `Guard::finished()`
    /// with a panicking waker fails a live incarnation here and skips its
    /// `on_stop`. During drain it freezes first, then resumes
    /// any incarnation-owned disposal panic retained by that point — those
    /// two, plus a destructor panic from the continuations, timers, queued
    /// completions and offload futures the freeze releases, a waker panic
    /// from the cancellation latches and completion notifications that
    /// cancelling those offloads wakes, or a panic raised by aborting an
    /// offload task — before reading the frozen accepted mailbox
    /// prefix. Retention is the guarantee, not a join: a payload recorded
    /// after the check is still the incarnation's exit, but is classified by
    /// the epilogue and cannot suppress `on_stop`. A completion waker that
    /// blocks rather than panicking is the accepted exception: it pins its
    /// ledger entry until it returns, so incarnation teardown joins that
    /// completion.
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
            self.freeze_intake();
            self.resources.resume_pending_panic();
            self.receiver.try_recv()
        } else {
            self.next_ready()
        }
    }

    fn replace_timer<K>(&mut self, key: K, message: TimerMessage<M>, after: Duration)
    where
        K: Hash + Eq + Send + 'static,
    {
        let now = runtime::now();
        let deadline = crate::deadline::Deadline::after(now, after).instant();
        self.resources.timers.replace(key, deadline, message);
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
        if self.rejects_new_work() {
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
            // The one uncontained completion fire in this file, and safe only
            // because `guard` has not been handed back yet: no caller can hold
            // this latch, so its waiter registry is empty and this wake runs
            // nothing. Nothing structural enforces that — keep the fire ahead
            // of the `return Ok(guard)` below.
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
                    // The deadline stays outside the work future so a
                    // completing offload retires the timer through
                    // `timeout_at`'s synchronous poll-path boundary instead
                    // of paying the drop-glue disposal venue on every
                    // deadlined completion.
                    match runtime::timeout_at(expires_at, work).await {
                        runtime::Timeout::Completed(result) => result.map(Ok),
                        runtime::Timeout::Elapsed => Ok(Err(DeadlineElapsed)),
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

    fn freeze_intake(&mut self) {
        self.receiver.freeze();
        self.freeze_resources();
    }

    fn rejects_new_work(&self) -> bool {
        self.is_stopping() || !self.resources.accepting
    }

    /// Freezes incarnation resources on the exit path, discarding §6.2's
    /// queued continuations.
    pub(crate) fn freeze_resources(&mut self) {
        self.resources.freeze();
    }

    pub(crate) async fn join_resources(&mut self) {
        self.resources.join_offloads().await;
    }

    /// The incarnation's cleanup-panic slot, shared with every resource.
    pub(super) fn panic_slot(&self) -> Arc<PanicSlot> {
        Arc::clone(&self.resources.disposal.panic)
    }
}
