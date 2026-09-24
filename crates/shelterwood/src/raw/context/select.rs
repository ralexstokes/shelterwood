//! Ready selection: the bounded arbitration batch and the receive-side
//! selector that drains continuations, mailbox, offload completions and timers.

use std::{collections::VecDeque, time::Instant};

use crate::{
    mailbox::AcceptedSequence,
    runtime::{self, catch_panic},
};

use super::{RawContext, resources::QueuedEvent, timers::ArmingOrder};

pub(super) struct ReadyBatch {
    phase: ReadyBatchPhase,
    mailbox_through: AcceptedSequence,
    pub(super) offloads_remaining: usize,
}

enum ReadyBatchPhase {
    // Steady state takes one mailbox delivery before its captured offload
    // prefix. Steady-state continuations stay live so one queued by an
    // external handler retains `continue_with`'s next-message priority.
    Steady {
        mailbox_delivered: bool,
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
            phase: ReadyBatchPhase::Steady {
                mailbox_delivered: false,
            },
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
        assert!(!armings.is_empty());
        assert!(matches!(self.phase, ReadyBatchPhase::Steady { .. }));
        self.phase = ReadyBatchPhase::Fired {
            armings,
            continuations_remaining,
        };
        self.mailbox_through = mailbox_through;
        self.offloads_remaining = offloads_remaining;
    }

    fn mailbox_budget_exhausted(&self) -> bool {
        matches!(
            self.phase,
            ReadyBatchPhase::Steady {
                mailbox_delivered: true
            }
        )
    }

    fn mailbox_is_eligible(&self) -> bool {
        match self.phase {
            ReadyBatchPhase::Steady { mailbox_delivered } => !mailbox_delivered,
            ReadyBatchPhase::Fired { .. } => true,
        }
    }

    fn is_fired(&self) -> bool {
        matches!(self.phase, ReadyBatchPhase::Fired { .. })
    }

    pub(super) fn continuation_is_eligible(&self) -> bool {
        match self.phase {
            ReadyBatchPhase::Steady { .. } => true,
            ReadyBatchPhase::Fired {
                continuations_remaining,
                ..
            } => continuations_remaining > 0,
        }
    }

    pub(super) fn record_continuation_delivery(&mut self) {
        if let ReadyBatchPhase::Fired {
            continuations_remaining,
            ..
        } = &mut self.phase
        {
            assert!(*continuations_remaining > 0);
            *continuations_remaining -= 1;
        }
    }

    fn record_mailbox_delivery(&mut self) {
        if let ReadyBatchPhase::Steady { mailbox_delivered } = &mut self.phase {
            assert!(!*mailbox_delivered);
            *mailbox_delivered = true;
        }
    }

    pub(super) fn next_arming(&self) -> Option<ArmingOrder> {
        let ReadyBatchPhase::Fired { armings, .. } = &self.phase else {
            return None;
        };
        armings.front().copied()
    }

    fn commit_arming(&mut self, arming: ArmingOrder) {
        let ReadyBatchPhase::Fired { armings, .. } = &mut self.phase else {
            panic!("only a fired batch delivers timer armings");
        };
        let removed = armings.pop_front();
        assert_eq!(removed, Some(arming));
    }
}

impl<M: Send + 'static> RawContext<M> {
    // The external-work half of §6.1's fairness rule, shared by the
    // steady-batch refresh and the empty-selection retry: accepted mailbox
    // traffic past the batch cutoff, an exhausted mailbox budget, or a
    // pending offload event all count as external work being ready.
    fn external_work_is_ready(
        &self,
        mailbox_budget_exhausted: bool,
        mailbox_cutoff: AcceptedSequence,
    ) -> bool {
        mailbox_budget_exhausted
            || self.receiver.accepted_sequence() > mailbox_cutoff
            || self.resources.events.watermark() > 0
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
    pub(super) fn next_ready(&mut self) -> Option<M> {
        let message = self.select_ready();
        // Selection itself runs incarnation-owned disposal — a cancelled
        // completion, a fired one-shot timer's key — after this call's last
        // loop-top check. SPEC §6.2 fails the incarnation on a disposal panic
        // retained when a receive boundary is reached, even if selection found
        // no message, so resume it before returning or waiting again. Any selected
        // message goes through the funnel first so its destructor never runs
        // on the unwind.
        if let Some(panic) = self.resources.disposal.panic.take() {
            self.resources.disposal.dispose(message);
            runtime::resume_panic(panic);
        }
        message
    }

    /// The batch stays installed in `resources` for the whole selection, so
    /// when user code unwinds out of a step, the next caught receive sees the
    /// same cutoffs and any not-yet-committed timer armings.
    fn select_ready(&mut self) -> Option<M> {
        // A permanently busy actor never reaches `wait_for_event`; reclaim at
        // its other guaranteed re-entry point so completed task handles do not
        // accumulate for the lifetime of the incarnation. This is the point
        // that bounds ledger retention.
        self.resources.reclaim_finished();
        loop {
            self.resources.resume_pending_panic();
            self.begin_ready_batch();
            if let Some(message) = self.resources.pop_continuation(true) {
                return Some(message);
            }

            let batch = self.resources.batch();
            if batch.mailbox_is_eligible() {
                let cutoff = batch.mailbox_through;
                let result = catch_panic(|| self.receiver.try_recv_live_through(cutoff));
                // Receive runs caller code only in its post-consumption
                // effects: a sender wake can panic after the selected message
                // has left the queue and been sent to disposal. Spend that
                // mailbox turn before resuming the panic, so a caught wake
                // cannot bypass offload fairness. Failures before consumption
                // are internal invariant/poison paths, not recoverable user
                // callbacks.
                if !matches!(result, Ok(None)) {
                    self.resources.batch_mut().record_mailbox_delivery();
                    self.resources.last_delivery_was_continuation = false;
                }
                if let Some(message) = result.unwrap_or_else(|panic| runtime::resume_panic(panic)) {
                    return Some(message);
                }
            }

            while let Some(event) = self.resources.pop_batched_event() {
                if let Some(message) = self.materialize_event(event) {
                    self.resources.last_delivery_was_continuation = false;
                    return Some(message);
                }
            }

            // A steady batch may have exhausted its captured external turn
            // while a continuation handler made later external work ready.
            // Start a fresh bounded turn before allowing another continuation
            // so that work receives §6.1's mandatory fairness opportunity.
            // Fired batches deliberately retain their immutable cutoffs:
            // post-fire arrivals must not jump the already-fired timers.
            let batch = self.resources.batch();
            if !batch.is_fired()
                && self.resources.last_delivery_was_continuation
                && self
                    .external_work_is_ready(batch.mailbox_budget_exhausted(), batch.mailbox_through)
            {
                self.resources.ready_batch = None;
                continue;
            }

            if let Some(message) = self.resources.pop_continuation(false) {
                return Some(message);
            }

            while let Some(arming) = self.resources.batch().next_arming() {
                let message = self.deliver_timer(arming);
                self.resources.batch_mut().commit_arming(arming);
                if let Some(message) = message {
                    self.resources.last_delivery_was_continuation = false;
                    return Some(message);
                }
            }

            let batch = self.resources.batch();
            let mailbox_may_remain = batch.mailbox_budget_exhausted();
            let mailbox_cutoff = batch.mailbox_through;
            self.resources.ready_batch = None;
            let timer_is_due = self
                .next_timer_deadline()
                .is_some_and(|deadline| deadline <= runtime::now());
            if self.external_work_is_ready(mailbox_may_remain, mailbox_cutoff)
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
        self.resources.timers.deliver_due(arming, runtime::now())
    }

    fn next_timer_deadline(&self) -> Option<Instant> {
        self.resources.timers.next_deadline()
    }

    pub(super) async fn wait_for_event(&mut self) {
        // Retention is already bounded by the reclaim in `next_ready`, which
        // ran on this task with no await in between. This is a same-instant
        // backstop for the one case that reclaim could not have seen: an
        // offload finishing on another thread between the two calls.
        self.resources.reclaim_finished();
        let deadline = self.next_timer_deadline();
        let shutdown = self.shutdown.clone();
        let mailbox = &mut self.receiver;
        let event_watcher = &mut self.resources.event_watcher;
        let delivery = async move {
            let _ = runtime::select_two(mailbox.changed(), event_watcher.changed()).await;
        };
        // `local_stop` can only be fired by this actor task, and `recv`
        // checks it before parking here. No other task can make it a wakeup
        // source while this future is pending.
        let event = runtime::select_two(shutdown.cancelled(), delivery);
        // The timer stays outside the whole event selection so every event
        // winner -- especially the warm mailbox path -- retires its caller
        // waker through timeout_at's synchronous poll-path boundary. Burying
        // the sleep in a nested select would run its drop-glue disposal venue
        // every time another arm won.
        if let Some(deadline) = deadline {
            let _ = runtime::timeout_at(deadline, event).await;
        } else {
            let _ = event.await;
        }
    }
}
