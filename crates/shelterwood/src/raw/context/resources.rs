//! Incarnation-owned resource ledger: queued continuations, offload
//! completions, timers and offload tasks, and their disposal on freeze.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use crate::runtime::{self, Latch, Signal, SignalWatcher, UnwindPanics, resume_preferred_panic};

use super::{
    super::{
        disposal::{PanicSlot, RawDisposal},
        offload::OffloadResource,
    },
    select::ReadyBatch,
    timers::TimerStore,
};

type DeferredMessage<M> = Box<dyn FnOnce() -> M + Send + 'static>;
pub(super) struct QueuedEvent<M> {
    pub(super) cancellation: Latch,
    pub(super) make_message: DeferredMessage<M>,
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
pub(super) struct EventQueue<M> {
    // Arbitration batches snapshot the number of currently queued events.
    // Insertion and the snapshot share this lock, so FIFO order itself is the
    // sequence and there is no integer counter whose saturation could blur a
    // boundary.
    pub(super) queue: Mutex<DisposingQueue<QueuedEvent<M>>>,
    pub(super) signal: Signal,
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
            signal: disposal.signal.clone(),
            queue: Mutex::new(DisposingQueue::new(disposal)),
        }
    }

    pub(super) fn push(&self, event: QueuedEvent<M>) {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .push_back(event);
        self.signal.pulse();
    }

    #[cfg(test)]
    pub(super) fn insert_with(&self, event: QueuedEvent<M>, before_insert: impl FnOnce()) {
        let mut queue = self.queue.lock().expect("actor event queue mutex poisoned");
        before_insert();
        queue.push_back(event);
    }

    #[cfg(test)]
    pub(super) fn push_with_hooks(
        &self,
        event: QueuedEvent<M>,
        before_insert: impl FnOnce(),
        after_insert: impl FnOnce(),
    ) {
        self.insert_with(event, before_insert);
        after_insert();
        self.signal.pulse();
    }

    pub(super) fn watermark(&self) -> usize {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .len()
    }

    #[cfg(test)]
    pub(super) fn pop(&self) -> Option<QueuedEvent<M>> {
        self.queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .pop_front()
    }

    pub(super) fn pop_through(&self, remaining: &mut usize) -> Option<QueuedEvent<M>> {
        if *remaining == 0 {
            None
        } else {
            let event = self
                .queue
                .lock()
                .expect("actor event queue mutex poisoned")
                .pop_front();
            // The guard is a temporary, so this is raised with the queue mutex
            // already released.
            assert!(event.is_some(), "a timer watermark covers queued events");
            *remaining -= 1;
            event
        }
    }

    fn clear(&self) {
        // The guard is a temporary: the drained events are disposed after
        // the queue mutex is released.
        let drained = self
            .queue
            .lock()
            .expect("actor event queue mutex poisoned")
            .take();
        drop(drained);
    }
}

/// Incarnation-owned FIFO of user payloads: `continue_with` messages, and
/// behind [`EventQueue`]'s lock, queued offload completions.
///
/// Elements are stored raw — the queue owns one disposal handle instead of one
/// per element — so every drain routes its payloads through that funnel.
/// `Drop` drains unconditionally: a `freeze` that never ran, or that failed
/// partway through an earlier cleanup step, must not leave queued user
/// payloads to be destroyed outside the disposal boundary.
pub(super) struct DisposingQueue<T> {
    queue: VecDeque<T>,
    disposal: RawDisposal,
}

impl<T> DisposingQueue<T> {
    fn new(disposal: RawDisposal) -> Self {
        Self {
            queue: VecDeque::new(),
            disposal,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(super) fn push_back(&mut self, value: T) {
        self.queue.push_back(value);
    }

    fn pop_front(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    /// Moves every element into a new queue that disposes them on drop.
    fn take(&mut self) -> Self {
        Self {
            queue: std::mem::take(&mut self.queue),
            disposal: self.disposal.clone(),
        }
    }

    fn clear(&mut self) {
        while let Some(value) = self.queue.pop_front() {
            self.disposal.dispose(value);
        }
    }
}

impl<T> Drop for DisposingQueue<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

pub(super) struct RawResources<M> {
    pub(super) accepting: bool,
    pub(super) continuations: DisposingQueue<M>,
    // Set after returning a continuation and cleared after an external
    // mailbox/offload/timer item. `next_ready` uses it to prohibit two local
    // continuations from leading while an external source remains eligible.
    pub(super) last_delivery_was_continuation: bool,
    pub(super) timers: TimerStore<M>,
    pub(super) ready_batch: Option<ReadyBatch>,
    pub(super) events: Arc<EventQueue<M>>,
    pub(super) disposal: RawDisposal,
    pub(super) event_watcher: SignalWatcher,
    pub(super) offloads: Vec<OffloadResource>,
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
            continuations: DisposingQueue::new(disposal.clone()),
            last_delivery_was_continuation: false,
            timers: TimerStore::new(disposal.clone()),
            ready_batch: None,
            events,
            disposal,
            event_watcher,
            offloads: Vec::new(),
        }
    }
}

impl<M> RawResources<M> {
    pub(super) fn freeze(&mut self) {
        if !self.accepting {
            return;
        }
        self.accepting = false;
        // Each step records its own failure in the first-wins slot, so one
        // failure cannot skip a later step and whatever was retained before
        // the freeze keeps precedence over what the freeze releases.
        let slot = &self.disposal.panic;
        for offload in &self.offloads {
            offload.cancel(slot);
        }
        slot.run(|| self.continuations.clear());
        slot.run(|| self.timers.clear());
        slot.run(|| self.ready_batch = None);
        slot.run(|| self.events.clear());
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
    pub(super) fn reclaim_finished(&mut self) {
        self.offloads.retain(|offload| {
            if let Some(state) = &offload.state {
                // Deliberately not the latch's fired bit: that is set before
                // completion waiters are woken, so it would let this retire
                // the entry — and with it the task handle teardown must join
                // — while a caller's `Guard::finished()` waker is still
                // running.
                !state.finished_published()
            } else {
                // Only the zero-deadline short-circuit builds a resource with
                // no shared state. It fires its latch while the `Guard` is
                // still on the way back to the caller, so the waiter registry
                // is provably empty: there is no wake for the fired bit to
                // outrun, and no task handle to lose.
                !offload.finished.is_fired()
            }
        });
    }

    pub(super) fn batch(&self) -> &ReadyBatch {
        self.ready_batch
            .as_ref()
            .expect("ready selection always owns an arbitration batch")
    }

    pub(super) fn batch_mut(&mut self) -> &mut ReadyBatch {
        self.ready_batch
            .as_mut()
            .expect("ready selection always owns an arbitration batch")
    }

    pub(super) fn pop_continuation(&mut self, is_lead_slot: bool) -> Option<M> {
        let batch = self
            .ready_batch
            .as_mut()
            .expect("ready selection always owns an arbitration batch");
        if (!is_lead_slot || !self.last_delivery_was_continuation)
            && batch.continuation_is_eligible()
            && let Some(message) = self.continuations.pop_front()
        {
            batch.record_continuation_delivery();
            self.last_delivery_was_continuation = true;
            return Some(message);
        }
        None
    }

    /// Pops the next offload completion inside the batch's captured prefix.
    pub(super) fn pop_batched_event(&mut self) -> Option<QueuedEvent<M>> {
        let batch = self
            .ready_batch
            .as_mut()
            .expect("ready selection always owns an arbitration batch");
        self.events.pop_through(&mut batch.offloads_remaining)
    }

    pub(super) fn resume_pending_panic(&self) {
        // Reached from the actor's own receive path, never from cleanup. The
        // take is destructive, so containment here would drop the retained
        // offload diagnostic and let the loop keep running.
        resume_preferred_panic(UnwindPanics {
            primary: self.disposal.panic.take(),
            cleanup: None,
        });
    }

    /// Awaits every offload task the ledger still holds.
    ///
    /// Reclamation retires an entry only once its completion wake has
    /// returned, so an offload whose caller-owned `Guard::finished()` waker
    /// blocks is necessarily still here and is joined. That is the accepted
    /// cost of never retiring work whose wake is in flight: caller code can
    /// delay incarnation teardown, and therefore exit publication, for as
    /// long as it blocks in that waker.
    pub(super) async fn join_offloads(&mut self) {
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
        // Evidence stays in the slot for the incarnation owner to report.
        let slot = Arc::clone(&self.disposal.panic);
        slot.run(|| self.freeze());
    }
}
