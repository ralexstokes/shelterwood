//! Runtime-independent supervision decisions.

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
    time::{Duration, Instant},
};

use crate::{
    Exit, Intensity, RestartPolicy, Shutdown, deadline::Deadline, policy::tidy_abort_beat,
};

/// Deterministic priority when one driver wake exposes several events.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ArbitrationClass {
    ScopeShutdown,
    MembershipRemoval,
    ChildExit,
    ReadinessSignal,
    ReadinessDeadline,
    BackoffDue,
    StopDeadline,
    Admission,
}

pub(crate) fn arbitrate<T>(events: &mut [(ArbitrationClass, T)]) {
    events.sort_by_key(|(class, _)| *class);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StopAction {
    Cancel,
    Escalate,
    HardAbort { after_grace: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopPhase {
    Idle,
    Cooperative,
    Escalated,
    Finished,
}

/// The single per-child shutdown escalation state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StopLadder {
    policy: Shutdown,
    phase: StopPhase,
    deadline: Option<Instant>,
    after_grace: bool,
}

impl StopLadder {
    pub(crate) fn new(policy: Shutdown) -> Self {
        Self {
            policy,
            phase: StopPhase::Idle,
            deadline: None,
            after_grace: false,
        }
    }

    pub(crate) fn deadline(self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) fn advance(&mut self, now: Instant) -> Option<StopAction> {
        match self.phase {
            StopPhase::Idle => {
                self.phase = StopPhase::Cooperative;
                match self.policy {
                    Shutdown::Graceful { grace } => {
                        self.deadline = Deadline::after(now, grace).instant();
                    }
                    Shutdown::Abort => {
                        self.deadline = Some(now);
                    }
                }
                Some(StopAction::Cancel)
            }
            StopPhase::Cooperative if self.deadline.is_some_and(|deadline| now >= deadline) => {
                let grace = match self.policy {
                    Shutdown::Graceful { grace } => {
                        self.after_grace = true;
                        grace
                    }
                    Shutdown::Abort => Duration::ZERO,
                };
                self.phase = StopPhase::Escalated;
                self.deadline = Deadline::after(now, tidy_abort_beat(grace)).instant();
                Some(StopAction::Escalate)
            }
            StopPhase::Escalated if self.deadline.is_some_and(|deadline| now >= deadline) => {
                self.phase = StopPhase::Finished;
                self.deadline = None;
                Some(StopAction::HardAbort {
                    after_grace: self.after_grace,
                })
            }
            StopPhase::Cooperative | StopPhase::Escalated | StopPhase::Finished => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeMode {
    Running,
    Draining,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MembershipMode {
    Active,
    Removing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExitDispatch {
    Terminal,
    ScheduleRestart,
}

pub(crate) fn dispatch_exit(
    exit: &Exit,
    restart: RestartPolicy,
    scope: ScopeMode,
    membership: MembershipMode,
) -> ExitDispatch {
    if scope == ScopeMode::Draining || membership == MembershipMode::Removing {
        return ExitDispatch::Terminal;
    }
    if restart.should_restart(exit.is_failure()) {
        ExitDispatch::ScheduleRestart
    } else {
        ExitDispatch::Terminal
    }
}

#[derive(Debug, Default)]
pub(crate) struct IntensityState {
    charges: VecDeque<Instant>,
    total_restarts: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IntensityCharge {
    pub(crate) in_window: u64,
    pub(crate) total_restarts: u64,
    pub(crate) tripped: bool,
}

impl IntensityState {
    pub(crate) fn charge(&mut self, policy: Intensity, now: Instant) -> IntensityCharge {
        while self.charges.front().is_some_and(|charge| {
            now.checked_duration_since(*charge)
                .is_some_and(|age| age > policy.within)
        }) {
            self.charges.pop_front();
        }
        self.charges.push_back(now);
        self.total_restarts = self.total_restarts.saturating_add(1);
        let in_window = u64::try_from(self.charges.len()).unwrap_or(u64::MAX);
        IntensityCharge {
            in_window,
            total_restarts: self.total_restarts,
            tripped: in_window > policy.max_restarts,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RestartState {
    attempt: u64,
    cumulative: u64,
}

impl RestartState {
    pub(crate) fn new() -> Self {
        Self {
            attempt: 0,
            cumulative: 0,
        }
    }

    pub(crate) fn schedule(&mut self) -> (u64, u64) {
        self.attempt = self.attempt.saturating_add(1);
        self.cumulative = self.cumulative.saturating_add(1);
        (self.attempt, self.cumulative)
    }

    pub(crate) fn settled(&mut self) {
        self.attempt = 0;
    }
}

/// Complete restart verdict consumed verbatim by the scope driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RestartDecision {
    pub(crate) attempt: u64,
    pub(crate) restart_count: u64,
    pub(crate) delay: Duration,
    pub(crate) restart_at: Option<Instant>,
    pub(crate) charge: IntensityCharge,
}

pub(crate) fn schedule_restart(
    restarts: &mut RestartState,
    intensity: &mut IntensityState,
    intensity_policy: Intensity,
    restart_policy: RestartPolicy,
    now: Instant,
    jitter_sample: f64,
) -> RestartDecision {
    let (attempt, restart_count) = restarts.schedule();
    let delay = restart_policy.backoff().next_delay(attempt, jitter_sample);
    let restart_at = Deadline::after(now, delay).instant();
    let charge = intensity.charge(intensity_policy, now);
    RestartDecision {
        attempt,
        restart_count,
        delay,
        restart_at,
        charge,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadinessGate {
    Immediate,
    Waiting { deadline: Option<Instant> },
    Ready,
    Disarmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadinessEvent {
    Signal,
    Deadline(Instant),
    Shutdown,
    Exit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadinessEffect {
    BecameReady,
    TimedOut { deadline: Instant },
    Disarmed,
}

impl ReadinessGate {
    pub(crate) fn step(&mut self, event: ReadinessEvent) -> Option<ReadinessEffect> {
        match (*self, event) {
            (Self::Immediate, _) | (Self::Ready, _) | (Self::Disarmed, _) => None,
            (Self::Waiting { .. }, ReadinessEvent::Signal) => {
                *self = Self::Ready;
                Some(ReadinessEffect::BecameReady)
            }
            (
                Self::Waiting {
                    deadline: Some(deadline),
                },
                ReadinessEvent::Deadline(now),
            ) if now >= deadline => {
                *self = Self::Disarmed;
                Some(ReadinessEffect::TimedOut { deadline })
            }
            (Self::Waiting { .. }, ReadinessEvent::Shutdown | ReadinessEvent::Exit) => {
                *self = Self::Disarmed;
                Some(ReadinessEffect::Disarmed)
            }
            (Self::Waiting { .. }, ReadinessEvent::Deadline(_)) => None,
        }
    }
}

impl PartialEq for DeadlineEntry {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.order == other.order
    }
}

impl Eq for DeadlineEntry {}

impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.order.cmp(&self.order))
    }
}

/// The engine's single deadline priority queue.
#[derive(Debug)]
pub(crate) struct DeadlineQueue<K> {
    next_order: u64,
    entries: BinaryHeap<DeadlineEntry>,
    slots: Vec<DeadlineSlot<K>>,
    free: Vec<usize>,
    len: usize,
}

/// A generation-checked registration for one armed deadline.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DeadlineHandle {
    index: usize,
    generation: u64,
}

#[derive(Debug)]
struct DeadlineSlot<K> {
    generation: u64,
    key: Option<K>,
}

#[derive(Debug)]
struct DeadlineEntry {
    at: Instant,
    order: u64,
    handle: DeadlineHandle,
}

impl<K> Default for DeadlineQueue<K> {
    fn default() -> Self {
        Self {
            next_order: 0,
            entries: BinaryHeap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            len: 0,
        }
    }
}

impl<K> DeadlineQueue<K> {
    pub(crate) fn push(&mut self, at: Instant, key: K) -> DeadlineHandle {
        let order = self.next_arming_order();
        let handle = if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index];
            debug_assert!(slot.key.is_none());
            slot.key = Some(key);
            DeadlineHandle {
                index,
                generation: slot.generation,
            }
        } else {
            let index = self.slots.len();
            self.slots.push(DeadlineSlot {
                generation: 0,
                key: Some(key),
            });
            DeadlineHandle {
                index,
                generation: 0,
            }
        };
        self.len += 1;
        self.entries.push(DeadlineEntry { at, order, handle });
        handle
    }

    pub(crate) fn cancel(&mut self, handle: DeadlineHandle) -> bool {
        let removed = self.take(handle).is_some();
        if removed {
            self.compact_if_sparse();
        }
        removed
    }

    pub(crate) fn next(&mut self) -> Option<Instant> {
        self.prune_stale_head();
        self.entries.peek().map(|entry| entry.at)
    }

    pub(crate) fn pop_due(&mut self, now: Instant) -> Option<K> {
        self.prune_stale_head();
        if self.entries.peek().is_some_and(|entry| entry.at <= now) {
            let entry = self.entries.pop().expect("the due entry was just observed");
            self.take(entry.handle)
        } else {
            None
        }
    }

    fn next_arming_order(&mut self) -> u64 {
        if self.next_order == u64::MAX {
            // Preserve the relative arming order of every live registration,
            // while dropping tombstones, before the sequence space wraps.
            // Handles address the separate generational arena and remain
            // valid across this rebuild.
            let slots = &self.slots;
            let mut entries = std::mem::take(&mut self.entries).into_vec();
            entries.retain(|entry| Self::is_active_in(slots, entry.handle));
            entries.sort_unstable_by_key(|entry| entry.order);
            for (order, entry) in entries.iter_mut().enumerate() {
                entry.order = u64::try_from(order)
                    .expect("live deadline count cannot exceed the arming sequence space");
            }
            self.next_order = u64::try_from(entries.len())
                .expect("live deadline count cannot exceed the arming sequence space");
            self.entries = BinaryHeap::from(entries);
        }
        let order = self.next_order;
        self.next_order += 1;
        order
    }

    fn take(&mut self, handle: DeadlineHandle) -> Option<K> {
        let slot = self.slots.get_mut(handle.index)?;
        if slot.generation != handle.generation {
            return None;
        }
        let key = slot.key.take()?;
        self.len -= 1;
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            self.free.push(handle.index);
        }
        Some(key)
    }

    fn is_active(&self, handle: DeadlineHandle) -> bool {
        Self::is_active_in(&self.slots, handle)
    }

    fn is_active_in(slots: &[DeadlineSlot<K>], handle: DeadlineHandle) -> bool {
        slots
            .get(handle.index)
            .is_some_and(|slot| slot.generation == handle.generation && slot.key.is_some())
    }

    fn prune_stale_head(&mut self) {
        while self
            .entries
            .peek()
            .is_some_and(|entry| !self.is_active(entry.handle))
        {
            self.entries.pop();
        }
    }

    fn compact_if_sparse(&mut self) {
        if self.entries.len() > self.len.saturating_mul(2) {
            let slots = &self.slots;
            self.entries
                .retain(|entry| Self::is_active_in(slots, entry.handle));
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn storage_len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn registration_slots_len(&self) -> usize {
        self.slots.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use crate::{
        Exit, ExitKind, Intensity, RestartCondition, RestartPolicy, Shutdown, policy::Backoff,
    };

    use super::{
        ArbitrationClass, DeadlineHandle, DeadlineQueue, ExitDispatch, IntensityState,
        MembershipMode, ReadinessEffect, ReadinessEvent, ReadinessGate, RestartState, ScopeMode,
        StopAction, StopLadder, arbitrate, dispatch_exit, schedule_restart,
    };

    #[test]
    fn arbitration_order_is_explicit_and_stable() {
        use ArbitrationClass::{
            Admission, BackoffDue, ChildExit, MembershipRemoval, ReadinessDeadline,
            ReadinessSignal, ScopeShutdown, StopDeadline,
        };
        let mut events = [
            (StopDeadline, 6),
            (ChildExit, 2),
            (ReadinessDeadline, 4),
            (ScopeShutdown, 0),
            (Admission, 7),
            (BackoffDue, 5),
            (MembershipRemoval, 1),
            (ReadinessSignal, 3),
        ];
        arbitrate(&mut events);
        assert_eq!(events.map(|(_, value)| value), [0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn same_batch_intensity_exit_suppresses_expedited_restart_work() {
        enum Event {
            IntensityTrippingExit,
            ExpeditedRestart,
        }

        let mut events = [
            (ArbitrationClass::BackoffDue, Event::ExpeditedRestart),
            (ArbitrationClass::ChildExit, Event::IntensityTrippingExit),
        ];
        arbitrate(&mut events);

        let mut draining = false;
        let mut factory_starts = 0;
        for (_, event) in events {
            match event {
                Event::IntensityTrippingExit => draining = true,
                Event::ExpeditedRestart if !draining => factory_starts += 1,
                Event::ExpeditedRestart => {}
            }
        }

        assert_eq!(factory_starts, 0);
    }

    #[test]
    fn ladder_uses_cancel_escalate_and_hard_abort_for_every_policy() {
        let start = Instant::now();
        let grace = Duration::from_millis(100);
        let mut graceful = StopLadder::new(Shutdown::Graceful { grace });
        assert_eq!(graceful.advance(start), Some(StopAction::Cancel));
        assert_eq!(graceful.advance(start + grace / 2), None);
        assert_eq!(graceful.advance(start + grace), Some(StopAction::Escalate));
        assert_eq!(
            graceful.advance(graceful.deadline().expect("tidy deadline")),
            Some(StopAction::HardAbort { after_grace: true })
        );

        let mut abort = StopLadder::new(Shutdown::Abort);
        assert_eq!(abort.advance(start), Some(StopAction::Cancel));
        assert_eq!(abort.advance(start), Some(StopAction::Escalate));
        assert_eq!(
            abort.advance(abort.deadline().expect("tidy deadline")),
            Some(StopAction::HardAbort { after_grace: false })
        );
    }

    #[test]
    fn overflowing_grace_never_becomes_immediately_due() {
        let start = Instant::now();
        let mut ladder = StopLadder::new(Shutdown::Graceful {
            grace: Duration::MAX,
        });

        assert_eq!(ladder.advance(start), Some(StopAction::Cancel));
        assert_eq!(ladder.deadline(), None);
        assert_eq!(ladder.advance(start), None);
    }

    #[test]
    fn funnel_dispatch_depends_on_mode_and_membership_state() {
        let failure = Exit::new(ExitKind::Failed(crate::ExitError::message("boom")), false);
        let restart = RestartPolicy::new(RestartCondition::OnFailure, Backoff::Immediate);
        assert_eq!(
            dispatch_exit(
                &failure,
                restart,
                ScopeMode::Running,
                MembershipMode::Active
            ),
            ExitDispatch::ScheduleRestart
        );
        assert_eq!(
            dispatch_exit(
                &failure,
                restart,
                ScopeMode::Draining,
                MembershipMode::Active
            ),
            ExitDispatch::Terminal
        );
        assert_eq!(
            dispatch_exit(
                &failure,
                restart,
                ScopeMode::Running,
                MembershipMode::Removing
            ),
            ExitDispatch::Terminal
        );
    }

    #[test]
    fn intensity_window_is_strict_and_tripping_charge_is_counted() {
        let start = Instant::now();
        let policy = Intensity::new(1, Duration::from_secs(10)).expect("valid intensity");
        let mut state = IntensityState::default();
        assert!(!state.charge(policy, start).tripped);
        let trip = state.charge(policy, start + Duration::from_secs(10));
        assert!(trip.tripped);
        assert_eq!(trip.in_window, 2);
        assert_eq!(trip.total_restarts, 2);

        let aged = state.charge(policy, start + Duration::from_secs(21));
        assert!(!aged.tripped);
        assert_eq!(aged.in_window, 1);
        assert_eq!(aged.total_restarts, 3);
    }

    #[test]
    fn restart_decision_owns_the_backoff_and_intensity_verdict() {
        let now = Instant::now();
        let intensity_policy = Intensity::new(0, Duration::from_secs(10)).expect("valid intensity");
        let restart_policy = RestartPolicy::new(RestartCondition::OnFailure, Backoff::Immediate);
        let mut restarts = RestartState::new();
        let mut intensity = IntensityState::default();
        let decision = schedule_restart(
            &mut restarts,
            &mut intensity,
            intensity_policy,
            restart_policy,
            now,
            0.5,
        );

        assert_eq!(decision.attempt, 1);
        assert_eq!(decision.restart_count, 1);
        assert_eq!(decision.delay, Duration::ZERO);
        assert_eq!(decision.restart_at, Some(now));
        assert_eq!(decision.charge.total_restarts, 1);
        assert!(decision.charge.tripped);
    }

    #[test]
    fn overflowing_restart_delay_has_no_immediate_spawn_deadline() {
        let now = Instant::now();
        let intensity_policy = Intensity::new(5, Duration::from_secs(10)).expect("valid intensity");
        let restart_policy = RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(Duration::MAX, crate::Jitter::None).expect("valid backoff"),
        );
        let mut restarts = RestartState::new();
        let mut intensity = IntensityState::default();
        let decision = schedule_restart(
            &mut restarts,
            &mut intensity,
            intensity_policy,
            restart_policy,
            now,
            0.5,
        );

        assert_eq!(decision.restart_at, None);
    }

    #[test]
    fn readiness_signal_wins_at_its_deadline_and_shutdown_disarms() {
        let deadline = Instant::now();
        let mut ready = ReadinessGate::Waiting {
            deadline: Some(deadline),
        };
        assert_eq!(
            ready.step(ReadinessEvent::Signal),
            Some(ReadinessEffect::BecameReady)
        );
        assert_eq!(ready.step(ReadinessEvent::Deadline(deadline)), None);

        let mut shutdown = ReadinessGate::Waiting {
            deadline: Some(deadline),
        };
        assert_eq!(
            shutdown.step(ReadinessEvent::Shutdown),
            Some(ReadinessEffect::Disarmed)
        );
        assert_eq!(shutdown.step(ReadinessEvent::Deadline(deadline)), None);
    }

    #[test]
    fn one_priority_queue_orders_equal_deadlines_by_arming_order() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        deadlines.push(now, "first");
        deadlines.push(now, "second");
        assert_eq!(deadlines.next(), Some(now));
        assert_eq!(deadlines.pop_due(now), Some("first"));
        assert_eq!(deadlines.pop_due(now), Some("second"));
    }

    #[test]
    fn cancelled_deadlines_release_keys_and_bound_heap_storage() {
        let far_future = Instant::now() + Duration::from_secs(60 * 60);
        let mut deadlines = DeadlineQueue::default();
        let persistent = deadlines.push(far_future, "persistent");

        for _ in 0..10_000 {
            let cancelled = deadlines.push(far_future, "cancelled");
            assert!(deadlines.cancel(cancelled));
            assert_eq!(deadlines.len(), 1);
            assert!(
                deadlines.storage_len() <= 2,
                "heap tombstones must stay proportional to live deadlines"
            );
            assert_eq!(
                deadlines.registration_slots_len(),
                2,
                "registration slots must be reused"
            );
        }

        assert!(deadlines.cancel(persistent));
        assert_eq!(deadlines.len(), 0);
        assert_eq!(deadlines.storage_len(), 0);
    }

    #[test]
    fn cancelled_deadline_drops_payload_and_stale_handle_misses_reused_slot() {
        struct DropProbe(Arc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let mut deadlines = DeadlineQueue::default();
        let stale = deadlines.push(Instant::now(), DropProbe(Arc::clone(&drops)));
        assert!(deadlines.cancel(stale));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "cancellation drops promptly"
        );

        let current = deadlines.push(Instant::now(), DropProbe(Arc::clone(&drops)));
        assert_eq!(stale.index, current.index, "the vacant slot is reused");
        assert_ne!(stale.generation, current.generation);
        assert!(
            !deadlines.cancel(stale),
            "a stale handle misses the replacement"
        );
        assert!(deadlines.cancel(current));
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn deadline_generation_exhaustion_retires_the_slot() {
        let mut deadlines = DeadlineQueue::default();
        let original = deadlines.push(Instant::now(), "retired");
        deadlines.slots[original.index].generation = u64::MAX;
        let exhausted = DeadlineHandle {
            index: original.index,
            generation: u64::MAX,
        };

        assert!(deadlines.cancel(exhausted));
        let current = deadlines.push(Instant::now(), "current");
        assert_ne!(exhausted.index, current.index);
        assert!(!deadlines.cancel(exhausted));
        assert_eq!(deadlines.pop_due(Instant::now()), Some("current"));
    }

    #[test]
    fn arming_order_exhaustion_rebases_without_changing_equal_deadline_order() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        deadlines.push(now, "first");
        deadlines.push(now, "second");
        deadlines.next_order = u64::MAX;
        deadlines.push(now, "third");

        assert_eq!(deadlines.pop_due(now), Some("first"));
        assert_eq!(deadlines.pop_due(now), Some("second"));
        assert_eq!(deadlines.pop_due(now), Some("third"));
    }

    #[test]
    fn cancelling_the_earliest_deadline_recomputes_the_next_wake() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        let earliest = deadlines.push(now + Duration::from_secs(1), "cancelled");
        deadlines.push(now + Duration::from_secs(2), "live");

        assert_eq!(deadlines.next(), Some(now + Duration::from_secs(1)));
        assert!(deadlines.cancel(earliest));
        assert_eq!(deadlines.next(), Some(now + Duration::from_secs(2)));
        assert_eq!(
            deadlines.pop_due(now + Duration::from_secs(2)),
            Some("live")
        );
    }
}
