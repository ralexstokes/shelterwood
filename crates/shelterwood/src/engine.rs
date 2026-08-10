//! Runtime-independent supervision decisions.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, VecDeque},
    time::{Duration, Instant},
};

use crate::{
    Exit, GracePhase, Intensity, IntensityTrip, JitterSample, Readiness, RestartAttempt,
    RestartCount, RestartPolicy, Shutdown, TotalRestarts,
    deadline::Deadline,
    exit::StopReason,
    identity::PoisonedCounter,
    policy::{ScopeFlavor, tidy_abort_beat},
};

/// Current state of a scope membership or incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScopeState {
    /// Membership exists but no incarnation has spawned.
    Unstarted,
    /// The current incarnation is starting its initial children.
    Starting,
    /// Aggregate readiness completed.
    Running,
    /// Root startup failed while the started prefix remains supervised.
    StartupFailed,
    /// The current incarnation is tearing down.
    Draining,
    /// One incarnation stopped.
    Stopped {
        /// Structured stop reason.
        reason: StopReason,
    },
}

impl ScopeState {
    /// Returns whether this is a membership-terminal state.
    ///
    /// A nested scope can transiently publish `Stopped` before its parent
    /// restarts the same membership, so callers that need membership
    /// terminality should prefer stream closure or `wait_stopped()`.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped { .. })
    }
}

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
    AbortFramework { phase: GracePhase },
    HardAbort { phase: GracePhase },
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
    grace_phase: GracePhase,
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
            grace_phase: GracePhase::WithinGrace,
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
                self.grace_phase = GracePhase::AfterGrace;
            }
            self.deadline = Some(self.deadline.map_or(now, |deadline| deadline.min(now)));
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
                            self.grace_phase = GracePhase::AfterGrace;
                        }
                        grace
                    }
                    Shutdown::Abort => Duration::ZERO,
                };
                self.phase = StopPhase::Escalated;
                // A forced ladder is the `Abort` policy's zero-grace point on
                // this same ladder (§10), so it takes the zero-grace tidy beat
                // rather than one scaled to the grace force just skipped. The
                // Grace-phase provenance above is unaffected: whether grace
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
                        phase: self.grace_phase,
                    })
                } else {
                    self.phase = StopPhase::Finished;
                    self.deadline = None;
                    Some(StopAction::HardAbort {
                        phase: self.grace_phase,
                    })
                }
            }
            StopPhase::AbortingFramework
                if self.deadline.is_some_and(|deadline| now >= deadline) =>
            {
                self.phase = StopPhase::Finished;
                self.deadline = None;
                (!self.framework_abort_acked).then_some(StopAction::HardAbort {
                    phase: self.grace_phase,
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

/// Whether a child membership is active or undergoing planned removal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MembershipStatus {
    /// The membership remains resident normally.
    Active,
    /// A planned removal has begun.
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
    membership: MembershipStatus,
) -> ExitDispatch {
    if scope == ScopeMode::Draining || membership == MembershipStatus::Removing {
        return ExitDispatch::Terminal;
    }
    if restart.should_restart(exit) {
        ExitDispatch::ScheduleRestart
    } else {
        ExitDispatch::Terminal
    }
}

#[derive(Debug)]
pub(crate) struct IntensityState {
    charges: VecDeque<Instant>,
    total_restarts: TotalRestarts,
}

impl Default for IntensityState {
    fn default() -> Self {
        Self {
            charges: VecDeque::new(),
            total_restarts: TotalRestarts::ZERO,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IntensityCharge {
    pub(crate) in_window: u64,
    pub(crate) total_restarts: TotalRestarts,
    pub(crate) tripped: bool,
}

impl IntensityTrip {
    pub(crate) fn new(policy: Intensity, charge: IntensityCharge) -> Self {
        debug_assert_eq!(charge.tripped, charge.in_window > policy.max_restarts);
        Self {
            max_restarts: policy.max_restarts,
            observed_restarts: charge.in_window,
            within: policy.within,
        }
    }
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
        self.total_restarts = self.total_restarts.bump();
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
    attempt: RestartAttempt,
    cumulative: RestartCount,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IncarnationRun {
    pub(crate) started_at: Instant,
    pub(crate) stopped_at: Instant,
}

impl RestartState {
    pub(crate) fn new() -> Self {
        Self {
            attempt: RestartAttempt::ZERO,
            cumulative: RestartCount::ZERO,
        }
    }

    pub(crate) fn schedule(&mut self) -> (RestartAttempt, RestartCount) {
        self.attempt = self.attempt.bump();
        self.cumulative = self.cumulative.bump();
        (self.attempt, self.cumulative)
    }

    pub(crate) fn settled(&mut self) {
        self.attempt = RestartAttempt::ZERO;
    }

    /// Resets the consecutive-attempt counter after one stable incarnation.
    ///
    /// Saturating elapsed time deliberately treats a clock regression as a
    /// zero-length run, so it cannot accidentally forgive restart pressure.
    pub(crate) fn settle_if_stable(&mut self, run: IncarnationRun, stable_for: Duration) -> bool {
        if run.stopped_at.saturating_duration_since(run.started_at) < stable_for {
            return false;
        }
        self.settled();
        true
    }
}

/// Complete restart verdict consumed verbatim by the scope driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RestartDecision {
    pub(crate) attempt: RestartAttempt,
    pub(crate) restart_count: RestartCount,
    pub(crate) delay: Duration,
    /// Absolute backoff deadline, or `None` when the exact point cannot be
    /// represented and armed by the runtime clock.
    pub(crate) restart_at: Option<Instant>,
    pub(crate) charge: IntensityCharge,
}

pub(crate) fn schedule_restart(
    restarts: &mut RestartState,
    intensity: &mut IntensityState,
    intensity_policy: Intensity,
    restart_policy: RestartPolicy,
    now: Instant,
    jitter_sample: JitterSample,
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
enum ReadinessState {
    Unconfigured,
    Immediate,
    Waiting { deadline: Option<Instant> },
    Ready,
    Disarmed,
}

/// The authoritative per-incarnation readiness state machine.
///
/// The shell only applies returned effects (publish ready, arm/cancel a
/// deadline, or begin timeout shutdown); it never assigns readiness state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadinessGate {
    state: ReadinessState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadinessEvent {
    Configure {
        readiness: Readiness,
        deadline: Option<Instant>,
    },
    Signal,
    Deadline {
        now: Instant,
        signal_seen: bool,
    },
    Shutdown,
    Exit {
        signal_seen: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadinessEffect {
    BecameReady,
    ArmDeadline { deadline: Instant },
    TimedOut { deadline: Instant },
    Disarmed,
}

impl ReadinessGate {
    pub(crate) fn new() -> Self {
        Self {
            state: ReadinessState::Unconfigured,
        }
    }

    /// Whether a retained signal watcher is needed for this incarnation.
    ///
    /// The `Unconfigured` case is unreachable from the driver, which
    /// configures the gate at spawn now that readiness is definition-level;
    /// it remains for the state machine's own completeness and unit tests.
    pub(crate) fn needs_signal_watch(self) -> bool {
        matches!(
            self.state,
            ReadinessState::Unconfigured | ReadinessState::Waiting { .. }
        )
    }

    pub(crate) fn step(&mut self, event: ReadinessEvent) -> Option<ReadinessEffect> {
        match (self.state, event) {
            (
                ReadinessState::Unconfigured,
                ReadinessEvent::Configure {
                    readiness: Readiness::Immediate,
                    ..
                },
            ) => {
                self.state = ReadinessState::Immediate;
                Some(ReadinessEffect::BecameReady)
            }
            (
                ReadinessState::Unconfigured,
                ReadinessEvent::Configure {
                    readiness: Readiness::Manual | Readiness::AfterInit,
                    deadline,
                },
            ) => {
                self.state = ReadinessState::Waiting { deadline };
                deadline.map(|deadline| ReadinessEffect::ArmDeadline { deadline })
            }
            (ReadinessState::Waiting { .. }, ReadinessEvent::Signal)
            | (
                ReadinessState::Waiting { .. },
                ReadinessEvent::Deadline {
                    signal_seen: true, ..
                },
            )
            | (ReadinessState::Waiting { .. }, ReadinessEvent::Exit { signal_seen: true }) => {
                self.state = ReadinessState::Ready;
                Some(ReadinessEffect::BecameReady)
            }
            (
                ReadinessState::Waiting {
                    deadline: Some(deadline),
                },
                ReadinessEvent::Deadline {
                    now,
                    signal_seen: false,
                },
            ) if now >= deadline => {
                self.state = ReadinessState::Disarmed;
                Some(ReadinessEffect::TimedOut { deadline })
            }
            // The `Unconfigured` half of this arm is unreachable from the
            // driver, which configures the gate at spawn now that readiness
            // is definition-level; it remains for the state machine's own
            // completeness and unit tests.
            (
                ReadinessState::Unconfigured,
                ReadinessEvent::Shutdown | ReadinessEvent::Exit { .. },
            )
            | (
                ReadinessState::Waiting { .. },
                ReadinessEvent::Shutdown | ReadinessEvent::Exit { signal_seen: false },
            ) => {
                self.state = ReadinessState::Disarmed;
                Some(ReadinessEffect::Disarmed)
            }
            (
                ReadinessState::Waiting { .. },
                ReadinessEvent::Deadline {
                    signal_seen: false, ..
                },
            )
            | (ReadinessState::Immediate | ReadinessState::Ready | ReadinessState::Disarmed, _)
            | (
                ReadinessState::Unconfigured,
                ReadinessEvent::Signal | ReadinessEvent::Deadline { .. },
            )
            | (ReadinessState::Waiting { .. }, ReadinessEvent::Configure { .. }) => None,
        }
    }
}

/// One scope incarnation's ownership token: minted for the driver that runs
/// the incarnation, and also addressed forward by a shutdown request that
/// targets the next incarnation before any driver has begun it.
///
/// Epochs are minted per scope in strictly increasing order starting at
/// [`Epoch::FIRST`], and `u64::MAX` is never minted ([`Epoch::successor`]
/// reserves it to poison [`ScopeEpochs`] exhaustion). Plain ordering is
/// therefore total over every minted epoch, so `Epoch` derives `Ord` where
/// identity generations instead guard their in-band poison with `supersedes`.
/// Unlike an incarnation identity, an epoch carries no scope tag, so ordering
/// is meaningful only between epochs of one scope; every comparison site
/// draws both operands from that scope's own control plane.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Epoch(u64);

impl Epoch {
    /// The first epoch a scope can mint.
    const FIRST: Self = Self(1);

    /// The epoch minted after `previous`, or the first with no predecessor.
    fn after(previous: Option<Self>) -> Option<Self> {
        previous.map_or(Some(Self::FIRST), Self::successor)
    }

    /// The next mintable epoch. `None` reserves `u64::MAX` as the poison so
    /// no observable epoch can alias permanent exhaustion.
    fn successor(self) -> Option<Self> {
        self.0
            .checked_add(1)
            .filter(|next| *next != u64::MAX)
            .map(Self)
    }
}

/// The epoch a scope shutdown request lands on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestTarget {
    pub(crate) epoch: Epoch,
    /// The target incarnation has not begun: the request addresses the next
    /// epoch a future driver will mint.
    pub(crate) pending_incarnation: bool,
}

/// Cross-incarnation liveness encoded once as an epoch state, rather than an
/// epoch pair plus an independently mutable `live` bit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeEpochs {
    Idle {
        last_stopped: Option<Epoch>,
    },
    Live {
        current: Epoch,
        last_stopped: Option<Epoch>,
    },
    Exhausted {
        last_stopped: Option<Epoch>,
    },
}

impl Default for ScopeEpochs {
    fn default() -> Self {
        Self::Idle { last_stopped: None }
    }
}

impl ScopeEpochs {
    pub(crate) fn begin(&mut self) -> Option<Epoch> {
        let last_stopped = match *self {
            Self::Idle { last_stopped } => last_stopped,
            // One scope cell cannot own two simultaneous drivers. Rejecting
            // a second begin also prevents it from invalidating the live
            // driver's epoch while trying to advance the counter.
            Self::Live { .. } | Self::Exhausted { .. } => return None,
        };
        let Some(current) = Epoch::after(last_stopped) else {
            *self = Self::Exhausted { last_stopped };
            return None;
        };
        *self = Self::Live {
            current,
            last_stopped,
        };
        Some(current)
    }

    pub(crate) fn live_epoch(self) -> Option<Epoch> {
        match self {
            Self::Idle { .. } | Self::Exhausted { .. } => None,
            Self::Live { current, .. } => Some(current),
        }
    }

    pub(crate) fn request_target(self) -> Option<RequestTarget> {
        match self {
            Self::Live { current, .. } => Some(RequestTarget {
                epoch: current,
                pending_incarnation: false,
            }),
            Self::Idle { last_stopped } => Epoch::after(last_stopped).map(|epoch| RequestTarget {
                epoch,
                pending_incarnation: true,
            }),
            Self::Exhausted { .. } => None,
        }
    }

    pub(crate) fn finish(&mut self, epoch: Epoch) -> bool {
        match *self {
            Self::Live {
                current,
                last_stopped,
            } if current == epoch => {
                *self = Self::Idle {
                    last_stopped: last_stopped.max(Some(epoch)),
                };
                true
            }
            Self::Idle { .. } | Self::Live { .. } | Self::Exhausted { .. } => false,
        }
    }

    pub(crate) fn is_current(self, epoch: Epoch) -> bool {
        self.live_epoch() == Some(epoch)
    }

    pub(crate) fn request_is_pending(self, epoch: Epoch) -> bool {
        matches!(self, Self::Idle { last_stopped } if Some(epoch) > last_stopped)
    }

    pub(crate) fn finished(self, epoch: Epoch) -> bool {
        match self {
            Self::Idle { last_stopped }
            | Self::Live { last_stopped, .. }
            | Self::Exhausted { last_stopped } => {
                last_stopped.is_some_and(|last_stopped| last_stopped >= epoch)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupPhase {
    Pending,
    Complete,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DrainReasonPrecedence {
    Finished,
    IntensityTripped,
    StartupFailed,
    ShutdownRequested,
}

impl DrainReasonPrecedence {
    fn of(reason: &StopReason) -> Self {
        match reason {
            StopReason::Finished => Self::Finished,
            StopReason::IntensityTripped(_) => Self::IntensityTripped,
            StopReason::StartupFailed(_) => Self::StartupFailed,
            StopReason::ShutdownRequested => Self::ShutdownRequested,
            StopReason::NeverStarted => {
                unreachable!("NeverStarted is not a live-incarnation drain reason")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScopeDrain {
    reason: StopReason,
    startup: StartupPhase,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DrainEffect {
    pub(crate) startup_pending: bool,
    /// Always [`ScopeState::Draining`] by construction; carried so the driver
    /// publishes exactly the state the machine returned.
    pub(crate) state: ScopeState,
}

/// The child state relevant to deciding whether a scope can finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChildCompletionState {
    pub(crate) has_children: bool,
    pub(crate) all_terminal: bool,
}

/// Authoritative lifecycle and finish policy for one scope incarnation.
///
/// `ScopeRecord` is only this machine's observation projection; epoch-tagged
/// requests use [`ScopeEpochs`] and do not encode this phase a second time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScopeLifecycle {
    state: ScopeLifecycleState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ScopeLifecycleState {
    Starting,
    Running,
    StartupFailed,
    Draining(ScopeDrain),
}

impl ScopeLifecycle {
    pub(crate) fn starting() -> Self {
        Self {
            state: ScopeLifecycleState::Starting,
        }
    }

    pub(crate) fn state(&self) -> ScopeState {
        match &self.state {
            ScopeLifecycleState::Starting => ScopeState::Starting,
            ScopeLifecycleState::Running => ScopeState::Running,
            ScopeLifecycleState::StartupFailed => ScopeState::StartupFailed,
            ScopeLifecycleState::Draining(_) => ScopeState::Draining,
        }
    }

    #[cfg(test)]
    pub(crate) fn running() -> Self {
        Self {
            state: ScopeLifecycleState::Running,
        }
    }

    pub(crate) fn is_starting(&self) -> bool {
        matches!(self.state, ScopeLifecycleState::Starting)
    }

    pub(crate) fn startup_complete(&self) -> bool {
        matches!(
            &self.state,
            ScopeLifecycleState::Running
                | ScopeLifecycleState::Draining(ScopeDrain {
                    startup: StartupPhase::Complete,
                    ..
                })
        )
    }

    pub(crate) fn startup_failed(&self) -> bool {
        matches!(
            &self.state,
            ScopeLifecycleState::StartupFailed
                | ScopeLifecycleState::Draining(ScopeDrain {
                    startup: StartupPhase::Failed,
                    ..
                })
        )
    }

    pub(crate) fn is_draining(&self) -> bool {
        matches!(self.state, ScopeLifecycleState::Draining(_))
    }

    pub(crate) fn draining_reason(&self) -> Option<&StopReason> {
        match &self.state {
            ScopeLifecycleState::Draining(drain) => Some(&drain.reason),
            ScopeLifecycleState::Starting
            | ScopeLifecycleState::Running
            | ScopeLifecycleState::StartupFailed => None,
        }
    }

    pub(crate) fn complete_startup(&mut self) -> Option<ScopeState> {
        if !matches!(self.state, ScopeLifecycleState::Starting) {
            return None;
        }
        self.state = ScopeLifecycleState::Running;
        Some(self.state())
    }

    /// Records only the first startup failure, so simultaneous failing
    /// initial children cannot publish the transition twice.
    pub(crate) fn fail_startup(&mut self) -> Option<ScopeState> {
        if !matches!(self.state, ScopeLifecycleState::Starting) {
            return None;
        }
        self.state = ScopeLifecycleState::StartupFailed;
        Some(self.state())
    }

    /// Begins draining or monotonically upgrades an in-progress drain.
    ///
    /// The returned effect exists only for the initial transition: upgrades
    /// change the eventual verdict without repeating teardown side effects.
    pub(crate) fn begin_drain(&mut self, reason: StopReason) -> Option<DrainEffect> {
        let incoming_precedence = DrainReasonPrecedence::of(&reason);
        let startup = match &mut self.state {
            ScopeLifecycleState::Starting => StartupPhase::Pending,
            ScopeLifecycleState::Running => StartupPhase::Complete,
            ScopeLifecycleState::StartupFailed => StartupPhase::Failed,
            ScopeLifecycleState::Draining(drain) => {
                if incoming_precedence > DrainReasonPrecedence::of(&drain.reason) {
                    drain.reason = reason;
                }
                return None;
            }
        };
        let startup_pending = startup == StartupPhase::Pending;
        self.state = ScopeLifecycleState::Draining(ScopeDrain { reason, startup });
        Some(DrainEffect {
            startup_pending,
            state: self.state(),
        })
    }

    pub(crate) fn finish_if_ready(
        &self,
        flavor: ScopeFlavor,
        children: ChildCompletionState,
    ) -> Option<StopReason> {
        if let Some(reason) = self.draining_reason() {
            return children.all_terminal.then(|| reason.clone());
        }
        (!self.startup_failed()
            && flavor == ScopeFlavor::Ordered
            && children.has_children
            && children.all_terminal)
            .then_some(StopReason::Finished)
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
    registration_ids: PoisonedCounter,
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
            registration_ids: PoisonedCounter::new(),
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
        let next = self
            .registration_ids
            .mint()
            .expect("deadline key space exhausted");
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
        Cancellation, Exit, ExitKind, GracePhase, Intensity, JitterSample, RestartAttempt,
        RestartCondition, RestartCount, RestartPolicy, Shutdown, TotalRestarts,
        identity::PoisonedCounter,
        policy::{Backoff, ScopeFlavor},
    };

    use super::{
        ArbitrationClass, ChildCompletionState, DeadlineHandle, DeadlineQueue, Epoch, ExitDispatch,
        IncarnationRun, IntensityState, MembershipStatus, ReadinessEffect, ReadinessEvent,
        ReadinessGate, RequestTarget, RestartState, ScopeEpochs, ScopeLifecycle, ScopeMode,
        ScopeState, StopAction, StopLadder, arbitrate, dispatch_exit, schedule_restart,
        tidy_abort_beat,
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
            Some(StopAction::HardAbort {
                phase: GracePhase::AfterGrace
            })
        );

        let mut abort = StopLadder::new(Shutdown::Abort);
        assert_eq!(abort.advance(start), Some(StopAction::Cancel));
        assert_eq!(abort.advance(start), Some(StopAction::Escalate));
        assert_eq!(
            abort.advance(abort.deadline().expect("tidy deadline")),
            Some(StopAction::HardAbort {
                phase: GracePhase::WithinGrace
            })
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
            Some(StopAction::HardAbort {
                phase: GracePhase::WithinGrace
            })
        );
        ladder.force(tidy);
        assert_eq!(
            ladder.advance(tidy),
            None,
            "a finished ladder stays finished"
        );
    }

    #[test]
    fn force_preserves_an_already_due_deadline() {
        let start = Instant::now();
        let grace = Duration::from_secs(30);
        let mut ladder = StopLadder::new(Shutdown::Graceful { grace });

        assert_eq!(ladder.advance(start), Some(StopAction::Cancel));
        let due = ladder.deadline().expect("grace deadline");
        ladder.force(due + Duration::from_secs(1));

        assert_eq!(
            ladder.deadline(),
            Some(due),
            "forcing after expiry cannot move the deadline later"
        );
        assert_eq!(
            ladder.advance(due),
            Some(StopAction::Escalate),
            "the original due instant remains actionable"
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
            Some(StopAction::AbortFramework {
                phase: GracePhase::WithinGrace
            })
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
            Some(StopAction::AbortFramework {
                phase: GracePhase::WithinGrace
            })
        );
        let framework_tidy = unacked.deadline().expect("framework tidy beat");
        assert_eq!(
            unacked.advance(framework_tidy),
            Some(StopAction::HardAbort {
                phase: GracePhase::WithinGrace
            })
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
        let failure = Exit::new(
            ExitKind::Failed(crate::ExitError::message("boom")),
            Cancellation::NotObserved,
        );
        let restart = RestartPolicy::new(RestartCondition::OnFailure, Backoff::Immediate);
        assert_eq!(
            dispatch_exit(
                &failure,
                restart,
                ScopeMode::Running,
                MembershipStatus::Active
            ),
            ExitDispatch::ScheduleRestart
        );
        assert_eq!(
            dispatch_exit(
                &failure,
                restart,
                ScopeMode::Draining,
                MembershipStatus::Active
            ),
            ExitDispatch::Terminal
        );
        assert_eq!(
            dispatch_exit(
                &failure,
                restart,
                ScopeMode::Running,
                MembershipStatus::Removing
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
        assert_eq!(trip.total_restarts, TotalRestarts::ZERO.bump().bump());
        let trip_payload = crate::IntensityTrip::new(policy, trip);
        assert_eq!(trip_payload.max_restarts, policy.max_restarts);
        assert_eq!(trip_payload.observed_restarts, trip.in_window);
        assert_eq!(trip_payload.within, policy.within);

        let aged = state.charge(policy, start + Duration::from_secs(21));
        assert!(!aged.tripped);
        assert_eq!(aged.in_window, 1);
        assert_eq!(
            aged.total_restarts,
            TotalRestarts::ZERO.bump().bump().bump()
        );
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
            JitterSample::new(0.5),
        );

        assert_eq!(decision.attempt, RestartAttempt::ZERO.bump());
        assert_eq!(decision.restart_count, RestartCount::ZERO.bump());
        assert_eq!(decision.delay, Duration::ZERO);
        assert_eq!(decision.restart_at, Some(now));
        assert_eq!(decision.charge.total_restarts, TotalRestarts::ZERO.bump());
        assert!(decision.charge.tripped);
    }

    #[test]
    fn overflowing_restart_delay_has_no_substitute_deadline() {
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
            JitterSample::new(0.5),
        );

        assert_eq!(decision.delay, Duration::MAX);
        assert_eq!(decision.restart_at, None);
    }

    #[test]
    fn stable_run_settles_restart_attempt_at_the_exact_boundary() {
        let start = Instant::now();
        let stable_for = Duration::from_secs(10);
        let mut restarts = RestartState::new();
        let attempt_one = RestartAttempt::ZERO.bump();
        let attempt_two = attempt_one.bump();
        let count_one = RestartCount::ZERO.bump();
        let count_two = count_one.bump();
        let count_three = count_two.bump();
        assert_eq!(restarts.schedule(), (attempt_one, count_one));
        assert!(!restarts.settle_if_stable(
            IncarnationRun {
                started_at: start,
                stopped_at: start + stable_for - Duration::from_nanos(1),
            },
            stable_for,
        ));
        assert_eq!(restarts.schedule(), (attempt_two, count_two));
        assert!(restarts.settle_if_stable(
            IncarnationRun {
                started_at: start,
                stopped_at: start + stable_for,
            },
            stable_for,
        ));
        assert_eq!(restarts.schedule(), (attempt_one, count_three));

        assert!(
            !restarts.settle_if_stable(
                IncarnationRun {
                    started_at: start,
                    stopped_at: start - Duration::from_nanos(1),
                },
                stable_for,
            ),
            "a regressed clock cannot forgive restart pressure"
        );
    }

    #[test]
    fn readiness_configuration_and_signal_deadline_race_are_engine_owned() {
        let deadline = Instant::now();
        let mut ready = ReadinessGate::new();
        assert_eq!(
            ready.step(ReadinessEvent::Configure {
                readiness: crate::Readiness::Manual,
                deadline: Some(deadline),
            }),
            Some(ReadinessEffect::ArmDeadline { deadline })
        );
        assert_eq!(
            ready.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: true,
            }),
            Some(ReadinessEffect::BecameReady)
        );
        assert_eq!(
            ready.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: false,
            }),
            None
        );

        let mut exited = ReadinessGate::new();
        assert_eq!(
            exited.step(ReadinessEvent::Configure {
                readiness: crate::Readiness::Manual,
                deadline: None,
            }),
            None
        );
        assert_eq!(
            exited.step(ReadinessEvent::Exit { signal_seen: true }),
            Some(ReadinessEffect::BecameReady)
        );

        let mut timed_out = ReadinessGate::new();
        assert_eq!(
            timed_out.step(ReadinessEvent::Configure {
                readiness: crate::Readiness::AfterInit,
                deadline: Some(deadline),
            }),
            Some(ReadinessEffect::ArmDeadline { deadline })
        );
        assert_eq!(
            timed_out.step(ReadinessEvent::Deadline {
                now: deadline,
                signal_seen: false,
            }),
            Some(ReadinessEffect::TimedOut { deadline })
        );

        let mut shutdown = ReadinessGate::new();
        assert_eq!(
            shutdown.step(ReadinessEvent::Configure {
                readiness: crate::Readiness::Manual,
                deadline: None,
            }),
            None
        );
        assert_eq!(
            shutdown.step(ReadinessEvent::Shutdown),
            Some(ReadinessEffect::Disarmed)
        );
    }

    #[test]
    fn scope_lifecycle_owns_first_failure_drain_status_and_finish_policy() {
        let mut lifecycle = ScopeLifecycle::starting();
        assert_eq!(lifecycle.fail_startup(), Some(ScopeState::StartupFailed));
        assert_eq!(
            lifecycle.fail_startup(),
            None,
            "simultaneous initial failures publish one transition"
        );
        assert_eq!(
            lifecycle.finish_if_ready(
                ScopeFlavor::Ordered,
                ChildCompletionState {
                    has_children: true,
                    all_terminal: true,
                },
            ),
            None
        );
        let drain = lifecycle
            .begin_drain(crate::exit::StopReason::ShutdownRequested)
            .expect("a failed startup can begin draining");
        assert!(!drain.startup_pending);
        assert_eq!(drain.state, ScopeState::Draining);
        assert_eq!(
            lifecycle.finish_if_ready(
                ScopeFlavor::Dynamic,
                ChildCompletionState {
                    has_children: true,
                    all_terminal: true,
                },
            ),
            Some(crate::exit::StopReason::ShutdownRequested)
        );

        let mut running = ScopeLifecycle::starting();
        assert_eq!(running.complete_startup(), Some(ScopeState::Running));
        assert_eq!(
            running.finish_if_ready(
                ScopeFlavor::Ordered,
                ChildCompletionState {
                    has_children: false,
                    all_terminal: true,
                },
            ),
            None
        );
        assert_eq!(
            running.finish_if_ready(
                ScopeFlavor::Ordered,
                ChildCompletionState {
                    has_children: true,
                    all_terminal: true,
                },
            ),
            Some(crate::exit::StopReason::Finished)
        );

        let mut starting = ScopeLifecycle::starting();
        let drain = starting
            .begin_drain(crate::exit::StopReason::ShutdownRequested)
            .expect("starting can begin draining");
        assert!(drain.startup_pending);
        assert_eq!(drain.state, ScopeState::Draining);
    }

    #[test]
    fn scope_lifecycle_upgrades_drain_reasons_monotonically() {
        let trip = crate::IntensityTrip {
            max_restarts: 0,
            observed_restarts: 1,
            within: Duration::from_secs(10),
        };
        let startup_failure = crate::StartupFailure {
            cause: crate::StartupFailureCause::IdentityExhausted {
                id: crate::ChildId::from("worker"),
            },
        };
        let mut lifecycle = ScopeLifecycle::running();

        assert!(lifecycle.begin_drain(crate::StopReason::Finished).is_some());
        assert_eq!(
            lifecycle.draining_reason(),
            Some(&crate::StopReason::Finished)
        );

        assert!(
            lifecycle
                .begin_drain(crate::StopReason::IntensityTripped(trip.clone()))
                .is_none(),
            "an upgrade does not repeat the enter-drain effect"
        );
        assert_eq!(
            lifecycle.draining_reason(),
            Some(&crate::StopReason::IntensityTripped(trip.clone()))
        );

        assert!(
            lifecycle
                .begin_drain(crate::StopReason::StartupFailed(startup_failure.clone()))
                .is_none()
        );
        assert_eq!(
            lifecycle.draining_reason(),
            Some(&crate::StopReason::StartupFailed(startup_failure.clone()))
        );

        assert!(
            lifecycle
                .begin_drain(crate::StopReason::ShutdownRequested)
                .is_none()
        );
        assert_eq!(
            lifecycle.draining_reason(),
            Some(&crate::StopReason::ShutdownRequested)
        );

        let lower_reasons = [
            crate::StopReason::Finished,
            crate::StopReason::IntensityTripped(trip),
            crate::StopReason::StartupFailed(startup_failure),
        ];
        for reason in lower_reasons {
            assert!(lifecycle.begin_drain(reason).is_none());
            assert_eq!(
                lifecycle.draining_reason(),
                Some(&crate::StopReason::ShutdownRequested),
                "a lower-precedence reason cannot replace shutdown"
            );
        }
    }

    #[test]
    fn scope_epoch_exhaustion_is_poisoned_without_minting_or_reuse() {
        let mut epochs = ScopeEpochs::default();
        assert_eq!(
            epochs.request_target(),
            Some(RequestTarget {
                epoch: Epoch(1),
                pending_incarnation: true,
            })
        );
        let first = epochs.begin().expect("first epoch is available");
        assert_eq!(epochs.live_epoch(), Some(first));
        assert_eq!(
            epochs.request_target(),
            Some(RequestTarget {
                epoch: first,
                pending_incarnation: false,
            })
        );
        assert_eq!(epochs.begin(), None, "a live epoch cannot be replaced");
        let unminted = first.successor().expect("a successor epoch is available");
        assert!(!epochs.finish(unminted));
        assert_eq!(epochs.live_epoch(), Some(first));
        assert!(epochs.finish(first));
        assert!(!epochs.finish(first), "a stopped epoch cannot finish twice");
        assert_eq!(epochs.live_epoch(), None);
        assert!(epochs.finished(first));
        assert!(epochs.request_is_pending(unminted));

        let mut exhausted = ScopeEpochs::Idle {
            last_stopped: Some(Epoch(u64::MAX - 2)),
        };
        let last = exhausted.begin().expect("last non-poison epoch");
        assert_eq!(last, Epoch(u64::MAX - 1));
        assert!(exhausted.finish(last));
        assert_eq!(
            exhausted.request_target(),
            None,
            "an idle request cannot address the reserved poison epoch"
        );
        assert_eq!(exhausted.begin(), None, "MAX is reserved as poison");
        assert_eq!(exhausted.live_epoch(), None);
        assert_eq!(exhausted.request_target(), None);
        assert_eq!(exhausted.begin(), None, "poisoning is permanent");
        assert!(!exhausted.finish(Epoch(u64::MAX)));
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
            assert_eq!(
                deadlines.len(),
                1,
                "the registration map keeps only live payloads"
            );
            assert!(
                deadlines.storage_len() <= 2,
                "heap tombstones must stay proportional to live deadlines"
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
            registration_ids: PoisonedCounter::near_exhaustion(),
            ..DeadlineQueue::default()
        };
        let last = deadlines.push(now, "last usable");
        assert_eq!(last, DeadlineHandle(u64::MAX - 1));
        assert!(deadlines.cancel(last));

        let first_exhausted = catch_unwind(AssertUnwindSafe(|| deadlines.push(now, "poison")));
        assert!(first_exhausted.is_err(), "the poison key is never minted");
        assert!(deadlines.registration_ids.is_poisoned());
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
