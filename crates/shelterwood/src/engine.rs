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

#[derive(Debug)]
struct DeadlineEntry<K> {
    at: Instant,
    order: u64,
    key: K,
}

impl<K> PartialEq for DeadlineEntry<K> {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.order == other.order
    }
}

impl<K> Eq for DeadlineEntry<K> {}

impl<K> PartialOrd for DeadlineEntry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for DeadlineEntry<K> {
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
    entries: BinaryHeap<DeadlineEntry<K>>,
}

impl<K> Default for DeadlineQueue<K> {
    fn default() -> Self {
        Self {
            next_order: 0,
            entries: BinaryHeap::new(),
        }
    }
}

impl<K> DeadlineQueue<K> {
    pub(crate) fn push(&mut self, at: Instant, key: K) {
        let order = self.next_order;
        self.next_order = self.next_order.saturating_add(1);
        self.entries.push(DeadlineEntry { at, order, key });
    }

    pub(crate) fn next(&self) -> Option<Instant> {
        self.entries.peek().map(|entry| entry.at)
    }

    pub(crate) fn pop_due(&mut self, now: Instant) -> Option<K> {
        if self.entries.peek().is_some_and(|entry| entry.at <= now) {
            self.entries.pop().map(|entry| entry.key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::{
        Exit, ExitKind, Intensity, RestartCondition, RestartPolicy, Shutdown, policy::Backoff,
    };

    use super::{
        ArbitrationClass, DeadlineQueue, ExitDispatch, IntensityState, MembershipMode,
        ReadinessEffect, ReadinessEvent, ReadinessGate, RestartState, ScopeMode, StopAction,
        StopLadder, arbitrate, dispatch_exit, schedule_restart,
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
}
