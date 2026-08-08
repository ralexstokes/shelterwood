//! Runtime-independent supervision decisions.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, VecDeque},
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
    AbortFramework { after_grace: bool },
    HardAbort { after_grace: bool },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopPhase {
    Idle,
    Cooperative,
    Escalated,
    AbortingFramework,
    Finished,
}

/// The single per-child shutdown escalation state machine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StopLadder {
    policy: Shutdown,
    phase: StopPhase,
    deadline: Option<Instant>,
    after_grace: bool,
    force_requested: bool,
    framework_driver: bool,
    framework_abort_acked: bool,
}

impl StopLadder {
    pub(crate) fn new(policy: Shutdown) -> Self {
        Self::with_framework_driver(policy, false)
    }

    pub(crate) fn for_framework_driver(policy: Shutdown) -> Self {
        Self::with_framework_driver(policy, true)
    }

    fn with_framework_driver(policy: Shutdown, framework_driver: bool) -> Self {
        Self {
            policy,
            phase: StopPhase::Idle,
            deadline: None,
            after_grace: false,
            force_requested: false,
            framework_driver,
            framework_abort_acked: false,
        }
    }

    pub(crate) fn deadline(self) -> Option<Instant> {
        self.deadline
    }

    /// Expedites this ladder without replacing or rewinding it.
    pub(crate) fn force(&mut self, now: Instant) {
        if self.phase == StopPhase::Finished {
            return;
        }
        if self.phase == StopPhase::Cooperative {
            if !self.force_requested
                && matches!(self.policy, Shutdown::Graceful { .. })
                && self.deadline.is_some_and(|deadline| now >= deadline)
            {
                self.after_grace = true;
            }
            self.deadline = Some(now);
        }
        self.force_requested = true;
    }

    pub(crate) fn acknowledge_framework_abort(&mut self) {
        if self.phase == StopPhase::AbortingFramework {
            self.framework_abort_acked = true;
        }
    }

    pub(crate) fn advance(&mut self, now: Instant) -> Option<StopAction> {
        match self.phase {
            StopPhase::Idle => {
                self.phase = StopPhase::Cooperative;
                if self.force_requested {
                    self.deadline = Some(now);
                } else {
                    match self.policy {
                        Shutdown::Graceful { grace } => {
                            self.deadline = Deadline::after(now, grace).instant();
                        }
                        Shutdown::Abort => {
                            self.deadline = Some(now);
                        }
                    }
                }
                Some(StopAction::Cancel)
            }
            StopPhase::Cooperative if self.deadline.is_some_and(|deadline| now >= deadline) => {
                let grace = match self.policy {
                    Shutdown::Graceful { grace } => {
                        if !self.force_requested {
                            self.after_grace = true;
                        }
                        grace
                    }
                    Shutdown::Abort => Duration::ZERO,
                };
                self.phase = StopPhase::Escalated;
                // A forced ladder is the `Abort` policy's zero-grace point on
                // this same ladder (§10), so it takes the zero-grace tidy beat
                // rather than one scaled to the grace force just skipped. The
                // `after_grace` provenance above is unaffected: whether grace
                // actually expired is a separate fact from how long the beat
                // between escalation and hard abort runs.
                let beat = if self.force_requested {
                    Duration::ZERO
                } else {
                    grace
                };
                self.deadline = Deadline::after(now, tidy_abort_beat(beat)).instant();
                Some(StopAction::Escalate)
            }
            StopPhase::Escalated if self.deadline.is_some_and(|deadline| now >= deadline) => {
                if self.framework_driver {
                    self.phase = StopPhase::AbortingFramework;
                    self.deadline = Deadline::after(now, tidy_abort_beat(Duration::ZERO)).instant();
                    Some(StopAction::AbortFramework {
                        after_grace: self.after_grace,
                    })
                } else {
                    self.phase = StopPhase::Finished;
                    self.deadline = None;
                    Some(StopAction::HardAbort {
                        after_grace: self.after_grace,
                    })
                }
            }
            StopPhase::AbortingFramework
                if self.deadline.is_some_and(|deadline| now >= deadline) =>
            {
                self.phase = StopPhase::Finished;
                self.deadline = None;
                (!self.framework_abort_acked).then_some(StopAction::HardAbort {
                    after_grace: self.after_grace,
                })
            }
            StopPhase::Cooperative
            | StopPhase::Escalated
            | StopPhase::AbortingFramework
            | StopPhase::Finished => None,
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
        self.at == other.at && self.handle == other.handle
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
            .then_with(|| other.handle.cmp(&self.handle))
    }
}

/// The engine's single deadline priority queue.
#[derive(Debug)]
pub(crate) struct DeadlineQueue<K> {
    // Keys are both registration identity and equal-deadline arming order.
    // They are never reused, so a stale handle can only miss.
    next_key: u64,
    entries: BinaryHeap<DeadlineEntry>,
    registrations: BTreeMap<DeadlineHandle, K>,
}

/// A never-reused registration for one armed deadline.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DeadlineHandle(u64);

#[derive(Debug)]
struct DeadlineEntry {
    at: Instant,
    handle: DeadlineHandle,
}

impl<K> Default for DeadlineQueue<K> {
    fn default() -> Self {
        Self {
            next_key: 0,
            entries: BinaryHeap::new(),
            registrations: BTreeMap::new(),
        }
    }
}

impl<K> DeadlineQueue<K> {
    pub(crate) fn push(&mut self, at: Instant, key: K) -> DeadlineHandle {
        let handle = self.next_handle();
        let replaced = self.registrations.insert(handle, key);
        debug_assert!(
            replaced.is_none(),
            "monotonic deadline keys are never reused"
        );
        self.entries.push(DeadlineEntry { at, handle });
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

    fn next_handle(&mut self) -> DeadlineHandle {
        let Some(next) = self.next_key.checked_add(1) else {
            panic!("deadline key space exhausted");
        };
        // Reserve the maximum value as poison. Once reached, the counter stays
        // poisoned and every later push fails instead of wrapping or reusing.
        if next == u64::MAX {
            self.next_key = u64::MAX;
            panic!("deadline key space exhausted");
        }
        self.next_key = next;
        DeadlineHandle(next)
    }

    fn take(&mut self, handle: DeadlineHandle) -> Option<K> {
        self.registrations.remove(&handle)
    }

    fn is_active(&self, handle: DeadlineHandle) -> bool {
        self.registrations.contains_key(&handle)
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
        if self.entries.len() > self.registrations.len().saturating_mul(2) {
            let registrations = &self.registrations;
            self.entries
                .retain(|entry| registrations.contains_key(&entry.handle));
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.registrations.len()
    }

    #[cfg(test)]
    pub(crate) fn storage_len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn registration_storage_len(&self) -> usize {
        self.registrations.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
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
        StopAction, StopLadder, arbitrate, dispatch_exit, schedule_restart, tidy_abort_beat,
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
    fn repeated_force_expedites_without_rewinding_the_ladder() {
        let start = Instant::now();
        let grace = Duration::from_secs(30);
        let mut ladder = StopLadder::new(Shutdown::Graceful { grace });

        assert_eq!(ladder.advance(start), Some(StopAction::Cancel));
        ladder.force(start);
        ladder.force(start);
        assert_eq!(ladder.advance(start), Some(StopAction::Escalate));

        let tidy = ladder
            .deadline()
            .expect("forced ladder keeps its tidy beat");
        assert_eq!(
            tidy,
            start + tidy_abort_beat(Duration::ZERO),
            "a forced ladder takes the zero-grace tidy beat, not one scaled \
             to the grace it skipped"
        );

        let mut unforced = StopLadder::new(Shutdown::Graceful { grace });
        assert_eq!(unforced.advance(start), Some(StopAction::Cancel));
        assert_eq!(unforced.advance(start + grace), Some(StopAction::Escalate));
        assert_eq!(
            unforced.deadline(),
            Some(start + grace + tidy_abort_beat(grace)),
            "an unforced ladder still scales its tidy beat to its grace"
        );

        ladder.force(start);
        assert_eq!(
            ladder.advance(start),
            None,
            "force does not skip the tidy beat"
        );
        assert_eq!(
            ladder.advance(tidy),
            Some(StopAction::HardAbort { after_grace: false })
        );
        ladder.force(tidy);
        assert_eq!(
            ladder.advance(tidy),
            None,
            "a finished ladder stays finished"
        );
    }

    #[test]
    fn framework_abort_and_ack_are_owned_by_the_same_stop_ladder() {
        let start = Instant::now();
        let mut ladder = StopLadder::for_framework_driver(Shutdown::Abort);

        assert_eq!(ladder.advance(start), Some(StopAction::Cancel));
        assert_eq!(ladder.advance(start), Some(StopAction::Escalate));
        let tidy = ladder
            .deadline()
            .expect("abort policy keeps the first tidy beat");
        assert_eq!(
            ladder.advance(tidy),
            Some(StopAction::AbortFramework { after_grace: false })
        );
        ladder.acknowledge_framework_abort();
        let framework_tidy = ladder
            .deadline()
            .expect("framework acknowledgment has a bounded tidy beat");
        assert_eq!(ladder.advance(framework_tidy), None);
        assert_eq!(ladder.deadline(), None);

        let mut unacked = StopLadder::for_framework_driver(Shutdown::Abort);
        assert_eq!(unacked.advance(start), Some(StopAction::Cancel));
        assert_eq!(unacked.advance(start), Some(StopAction::Escalate));
        let tidy = unacked.deadline().expect("abort tidy beat");
        assert_eq!(
            unacked.advance(tidy),
            Some(StopAction::AbortFramework { after_grace: false })
        );
        let framework_tidy = unacked.deadline().expect("framework tidy beat");
        assert_eq!(
            unacked.advance(framework_tidy),
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
    fn one_priority_queue_orders_deadlines_then_equal_deadline_fifo() {
        let now = Instant::now();
        let later = now + Duration::from_secs(1);
        let mut deadlines = DeadlineQueue::default();
        deadlines.push(later, "later-first");
        deadlines.push(now, "now-first");
        deadlines.push(later, "later-second");
        deadlines.push(now, "now-second");

        assert_eq!(deadlines.next(), Some(now));
        assert_eq!(deadlines.pop_due(now), Some("now-first"));
        assert_eq!(deadlines.pop_due(now), Some("now-second"));
        assert_eq!(deadlines.pop_due(now), None);
        assert_eq!(deadlines.next(), Some(later));
        assert_eq!(deadlines.pop_due(later), Some("later-first"));
        assert_eq!(deadlines.pop_due(later), Some("later-second"));
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
                deadlines.registration_storage_len(),
                deadlines.len(),
                "the registration map stores only live payloads"
            );
        }

        assert!(deadlines.cancel(persistent));
        assert_eq!(deadlines.len(), 0);
        assert_eq!(deadlines.storage_len(), 0);
    }

    #[test]
    fn cancellation_and_queue_drop_own_payloads_while_stale_handles_stay_absent() {
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
        assert!(
            current > stale,
            "deadline keys are monotonic and never reused"
        );
        assert!(!deadlines.cancel(stale), "a stale handle remains absent");
        assert_eq!(
            deadlines.len(),
            1,
            "stale cancellation preserves the live key"
        );
        drop(deadlines);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn deadline_key_exhaustion_never_mints_poison_or_reuses_a_key() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue {
            next_key: u64::MAX - 2,
            ..DeadlineQueue::default()
        };
        let last = deadlines.push(now, "last usable");
        assert_eq!(last, DeadlineHandle(u64::MAX - 1));
        assert!(deadlines.cancel(last));

        let first_exhausted = catch_unwind(AssertUnwindSafe(|| deadlines.push(now, "poison")));
        assert!(first_exhausted.is_err(), "the poison key is never minted");
        assert_eq!(deadlines.next_key, u64::MAX);
        assert!(
            !deadlines
                .registrations
                .contains_key(&DeadlineHandle(u64::MAX))
        );

        let still_exhausted = catch_unwind(AssertUnwindSafe(|| deadlines.push(now, "wrapped")));
        assert!(
            still_exhausted.is_err(),
            "the exhausted domain stays poisoned"
        );
        assert!(deadlines.registrations.is_empty());
        assert!(deadlines.entries.is_empty());
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
