//! Runtime-independent supervision decisions.

mod deadline_queue;
mod epochs;
mod readiness;

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

pub use self::{
    deadline_queue::{DeadlineHandle, DeadlineQueue},
    epochs::{Epoch, RequestTarget, ScopeEpochs},
    readiness::{ReadinessEffect, ReadinessEvent, ReadinessGate},
};
use crate::{
    deadline::Deadline,
    exit::{Exit, ExitKind, GracePhase, IntensityTrip, StopReason, stop_reason_precedence},
    policy::{
        Intensity, JitterSample, RestartAttempt, RestartCount, RestartPolicy, ScopeFlavor,
        Shutdown, TotalRestarts, tidy_abort_beat,
    },
};

/// Current state of a scope membership or incarnation.
///
/// Exhaustive on purpose: the driver and the observation surface both decide
/// by matching every state, and pre-release there is no downstream user for
/// `#[non_exhaustive]` to protect. See [`crate::exit::StopReason`].
#[derive(Clone, Debug, Eq, PartialEq)]
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
pub enum ArbitrationClass {
    ScopeShutdown,
    MembershipRemoval,
    ChildExit,
    ReadinessSignal,
    ReadinessDeadline,
    BackoffDue,
    StopDeadline,
    Admission,
}

pub fn arbitrate<T>(events: &mut [(ArbitrationClass, T)]) {
    // Priority is deterministic across classes, and input order is FIFO
    // within one class. Callers build the slice in observation order, so a
    // stable sort is part of the driver contract rather than an incidental
    // implementation choice.
    events.sort_by_key(|(class, _)| *class);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopAction {
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
pub struct StopLadder {
    policy: Shutdown,
    phase: StopPhase,
    deadline: Option<Instant>,
    grace_phase: GracePhase,
    force_requested: bool,
    framework_driver: bool,
    framework_abort_acked: bool,
}

impl StopLadder {
    pub fn new(policy: Shutdown) -> Self {
        Self::with_framework_driver(policy, false)
    }

    pub fn for_framework_driver(policy: Shutdown) -> Self {
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

    pub fn deadline(self) -> Option<Instant> {
        self.deadline
    }

    /// Expedites this ladder without replacing or rewinding it.
    pub fn force(&mut self, now: Instant) {
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

    pub fn acknowledge_framework_abort(&mut self) {
        if self.phase == StopPhase::AbortingFramework {
            self.framework_abort_acked = true;
        }
    }

    pub fn advance(&mut self, now: Instant) -> Option<StopAction> {
        match self.phase {
            StopPhase::Idle => {
                self.phase = StopPhase::Cooperative;
                if self.force_requested {
                    self.deadline = Some(now);
                } else {
                    match self.policy {
                        Shutdown::Graceful { grace } => {
                            self.deadline = Deadline::after(now, grace.get()).instant();
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
                        grace.get()
                    }
                    Shutdown::Abort => Duration::ZERO,
                };
                self.phase = StopPhase::Escalated;
                // A forced ladder is the `Abort` policy's zero-grace point on
                // this same ladder (§11), so it takes the zero-grace tidy beat
                // rather than one scaled to the grace force just skipped. The
                // Grace-phase provenance above is unaffected: whether grace
                // actually expired is a separate fact from how long the beat
                // between escalation and hard abort runs.
                let beat = if self.force_requested {
                    Duration::ZERO
                } else {
                    grace
                };
                // Unlike an unrepresentable cooperative grace, an
                // unrepresentable tidy beat has no force rescue: `force`
                // only expedites `Cooperative`, and this ladder is already
                // `Escalated`. The same applies to the framework tidy beat
                // below once the ladder reaches `AbortingFramework`. This can
                // park either phase only when `now` is within the beat plus
                // `Deadline::ARMING_HEADROOM` of `Instant`'s ceiling — the
                // beat is capped at ten milliseconds, so just over a second;
                // accept that theoretical clock-boundary asymmetry rather
                // than substituting an earlier public deadline.
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

/// Whether a child membership is active or undergoing planned removal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MembershipStatus {
    /// The membership remains resident normally.
    Active,
    /// A planned removal has begun.
    Removing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExitDispatch {
    Terminal,
    ScheduleRestart,
}

pub fn dispatch_exit(
    exit: &Exit,
    restart: RestartPolicy,
    scope_draining: bool,
    membership: MembershipStatus,
) -> ExitDispatch {
    // SPEC §8: `NeverStarted` is a membership fact, not an incarnation
    // verdict — it "is never input to restart or intensity accounting".
    // Every supported producer terminalizes the membership directly and never
    // reaches dispatch, so arriving here is a framework bug. Fail before
    // charging a restart for an incarnation that never ran or publishing the
    // inconsistent terminal projection that SPEC §B.4 excludes: a `Removed`
    // edge pairs `last_incarnation: None` with never having started.
    assert!(
        !matches!(exit.kind(), ExitKind::NeverStarted),
        "NeverStarted is a membership outcome outside incarnation dispatch"
    );
    if scope_draining || membership == MembershipStatus::Removing {
        return ExitDispatch::Terminal;
    }
    if restart.should_restart(exit) {
        ExitDispatch::ScheduleRestart
    } else {
        ExitDispatch::Terminal
    }
}

#[derive(Debug)]
pub struct IntensityState {
    // This is deliberately one entry per restart still inside the policy
    // window. `Intensity::new(u64::MAX, Duration::MAX)` can therefore retain
    // charges without bound; that degenerate configuration is accepted as
    // operator self-harm rather than adding a second, lossy accounting mode.
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
struct IntensityCharge {
    policy: Intensity,
    in_window: u64,
    total_restarts: TotalRestarts,
    tripped: bool,
}

impl IntensityTrip {
    fn new(charge: IntensityCharge) -> Self {
        let policy = charge.policy;
        Self {
            max_restarts: policy.max_restarts(),
            observed_restarts: charge.in_window,
            within: policy.within(),
        }
    }
}

impl IntensityState {
    fn charge(&mut self, policy: Intensity, now: Instant) -> IntensityCharge {
        while self.charges.front().is_some_and(|charge| {
            now.checked_duration_since(*charge)
                .is_some_and(|age| age > policy.within())
        }) {
            self.charges.pop_front();
        }
        self.charges.push_back(now);
        self.total_restarts = self.total_restarts.bump();
        let in_window = u64::try_from(self.charges.len()).unwrap_or(u64::MAX);
        IntensityCharge {
            policy,
            in_window,
            total_restarts: self.total_restarts,
            tripped: in_window > policy.max_restarts(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestartState {
    attempt: RestartAttempt,
    cumulative: RestartCount,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IncarnationRun {
    pub started_at: Instant,
    pub stopped_at: Instant,
}

impl RestartState {
    pub fn new() -> Self {
        Self {
            attempt: RestartAttempt::ZERO,
            cumulative: RestartCount::ZERO,
        }
    }

    pub fn schedule(&mut self) -> (RestartAttempt, RestartCount) {
        self.attempt = self.attempt.bump();
        self.cumulative = self.cumulative.bump();
        (self.attempt, self.cumulative)
    }

    fn settled(&mut self) {
        self.attempt = RestartAttempt::ZERO;
    }

    /// Resets the consecutive-attempt counter after one stable incarnation.
    ///
    /// Saturating elapsed time deliberately treats a clock regression as a
    /// zero-length run, so it cannot accidentally forgive restart pressure.
    pub fn settle_if_stable(&mut self, run: IncarnationRun, stable_for: Duration) -> bool {
        if run.stopped_at.saturating_duration_since(run.started_at) < stable_for {
            return false;
        }
        self.settled();
        true
    }
}

impl Default for RestartState {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete restart verdict consumed verbatim by the cross-crate scope driver.
///
/// Its public visibility is required by [`schedule_restart`]'s sibling-crate
/// return edge; the supported façade neither names nor exports this type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestartDecision {
    attempt: RestartAttempt,
    restart_count: RestartCount,
    delay: Duration,
    /// Absolute backoff deadline, or `None` when the exact point cannot be
    /// represented and armed by the runtime clock.
    restart_at: Option<Instant>,
    charge: IntensityCharge,
}

impl RestartDecision {
    pub fn attempt(&self) -> RestartAttempt {
        self.attempt
    }

    pub fn restart_count(&self) -> RestartCount {
        self.restart_count
    }

    pub fn delay(&self) -> Duration {
        self.delay
    }

    pub fn restart_at(&self) -> Option<Instant> {
        self.restart_at
    }

    pub fn total_restarts(&self) -> TotalRestarts {
        self.charge.total_restarts
    }

    pub fn intensity_trip(&self) -> Option<IntensityTrip> {
        self.charge.tripped.then(|| IntensityTrip::new(self.charge))
    }
}

pub fn schedule_restart(
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
enum StartupPhase {
    Pending,
    Complete,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScopeDrain {
    reason: StopReason,
    startup: StartupPhase,
}

/// The child state relevant to deciding whether a scope can finish.
///
/// The two fields are both booleans and answer different questions, so a
/// positional pair would transpose silently at the one call site
/// (`SupervisorState::settle`) and quietly change the finish predicate.
/// Naming them is what makes that transposition a compile error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChildCompletionState {
    /// Whether the scope holds any child registration at all.
    pub(crate) has_children: bool,
    /// Whether every child registration has reached the joined terminal.
    pub(crate) all_terminal: bool,
}

/// Authoritative lifecycle and finish policy for one scope incarnation.
///
/// `ScopeRecord` is only this machine's observation projection; epoch-tagged
/// requests use [`ScopeEpochs`] and do not encode this phase a second time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopeLifecycle {
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
    pub fn starting() -> Self {
        Self {
            state: ScopeLifecycleState::Starting,
        }
    }

    pub fn state(&self) -> ScopeState {
        match &self.state {
            ScopeLifecycleState::Starting => ScopeState::Starting,
            ScopeLifecycleState::Running => ScopeState::Running,
            ScopeLifecycleState::StartupFailed => ScopeState::StartupFailed,
            ScopeLifecycleState::Draining(_) => ScopeState::Draining,
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn running() -> Self {
        Self {
            state: ScopeLifecycleState::Running,
        }
    }

    /// A hashable projection of the whole lifecycle, for the reducer's
    /// reachable-state walk.
    ///
    /// Exhaustive on purpose: a new lifecycle state or drain field has to
    /// choose its projection here or fail to compile, because the walk treats
    /// equal fingerprints as the same state and would otherwise stop exploring
    /// successors it has never seen. Drain reasons project through their
    /// precedence, which is injective over the walk's alphabet of unit reasons
    /// and is also the only part of a reason the reducer branches on.
    #[cfg(test)]
    pub(crate) fn fingerprint(&self) -> (u8, u8) {
        match &self.state {
            ScopeLifecycleState::Starting => (0, 0),
            ScopeLifecycleState::Running => (1, 0),
            ScopeLifecycleState::StartupFailed => (2, 0),
            ScopeLifecycleState::Draining(ScopeDrain { reason, startup }) => (
                3 + match startup {
                    StartupPhase::Pending => 0,
                    StartupPhase::Complete => 1,
                    StartupPhase::Failed => 2,
                },
                stop_reason_precedence(reason) as u8,
            ),
        }
    }

    pub fn is_starting(&self) -> bool {
        matches!(self.state, ScopeLifecycleState::Starting)
    }

    pub fn startup_complete(&self) -> bool {
        matches!(
            &self.state,
            ScopeLifecycleState::Running
                | ScopeLifecycleState::Draining(ScopeDrain {
                    startup: StartupPhase::Complete,
                    ..
                })
        )
    }

    pub fn startup_failed(&self) -> bool {
        matches!(
            &self.state,
            ScopeLifecycleState::StartupFailed
                | ScopeLifecycleState::Draining(ScopeDrain {
                    startup: StartupPhase::Failed,
                    ..
                })
        )
    }

    pub fn is_draining(&self) -> bool {
        matches!(self.state, ScopeLifecycleState::Draining(_))
    }

    pub fn draining_reason(&self) -> Option<&StopReason> {
        match &self.state {
            ScopeLifecycleState::Draining(drain) => Some(&drain.reason),
            ScopeLifecycleState::Starting
            | ScopeLifecycleState::Running
            | ScopeLifecycleState::StartupFailed => None,
        }
    }

    pub fn complete_startup(&mut self) -> Option<ScopeState> {
        if !matches!(self.state, ScopeLifecycleState::Starting) {
            return None;
        }
        self.state = ScopeLifecycleState::Running;
        Some(self.state())
    }

    /// Records only the first startup failure, so simultaneous failing
    /// initial children cannot publish the transition twice.
    pub fn fail_startup(&mut self) -> Option<ScopeState> {
        if !matches!(self.state, ScopeLifecycleState::Starting) {
            return None;
        }
        self.state = ScopeLifecycleState::StartupFailed;
        Some(self.state())
    }

    /// Begins draining or monotonically upgrades an in-progress drain.
    ///
    /// Upgrades join through `StopPrecedence`, the same lattice the stopped
    /// publisher uses, so a drain verdict and a published verdict can never
    /// resolve competing reasons in opposite directions. The returned effect
    /// exists only for the initial transition: upgrades change the eventual
    /// verdict without repeating teardown side effects.
    pub fn begin_drain(&mut self, reason: StopReason) -> Option<(bool, ScopeState)> {
        assert!(
            !matches!(reason, StopReason::NeverStarted),
            "NeverStarted is not a live-incarnation drain reason"
        );
        let incoming_precedence = stop_reason_precedence(&reason);
        let startup = match &mut self.state {
            ScopeLifecycleState::Starting => StartupPhase::Pending,
            ScopeLifecycleState::Running => StartupPhase::Complete,
            ScopeLifecycleState::StartupFailed => StartupPhase::Failed,
            ScopeLifecycleState::Draining(drain) => {
                if incoming_precedence > stop_reason_precedence(&drain.reason) {
                    drain.reason = reason;
                }
                return None;
            }
        };
        let startup_pending = startup == StartupPhase::Pending;
        self.state = ScopeLifecycleState::Draining(ScopeDrain { reason, startup });
        Some((startup_pending, self.state()))
    }

    pub(crate) fn finish_if_ready(
        &self,
        flavor: ScopeFlavor,
        children: ChildCompletionState,
    ) -> Option<StopReason> {
        if let Some(reason) = self.draining_reason() {
            return children.all_terminal.then(|| reason.clone());
        }
        (self.startup_complete()
            && flavor == ScopeFlavor::Ordered
            && children.has_children
            && children.all_terminal)
            .then_some(StopReason::Finished)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::{
        exit::{
            Cancellation, Exit, ExitError, ExitKind, GracePhase, IntensityTrip, StartupFailure,
            StartupFailureCause, StopReason,
        },
        identity::ChildId,
        policy::{
            Backoff, Intensity, Jitter, JitterSample, RestartAttempt, RestartCondition,
            RestartCount, RestartPolicy, ScopeFlavor, Shutdown, TotalRestarts,
        },
    };

    use super::{
        ArbitrationClass, ChildCompletionState, ExitDispatch, IncarnationRun, IntensityState,
        MembershipStatus, RestartState, ScopeLifecycle, ScopeState, StopAction, StopLadder,
        arbitrate, dispatch_exit, schedule_restart, tidy_abort_beat,
    };

    #[test]
    fn arbitration_order_is_explicit_and_stable() {
        use ArbitrationClass::{
            Admission, BackoffDue, ChildExit, MembershipRemoval, ReadinessDeadline,
            ReadinessSignal, ScopeShutdown, StopDeadline,
        };
        let mut events = [
            (StopDeadline, 8),
            (ChildExit, 2),
            (ReadinessDeadline, 6),
            (ChildExit, 3),
            (ScopeShutdown, 0),
            (Admission, 9),
            (BackoffDue, 7),
            (MembershipRemoval, 1),
            (ReadinessSignal, 4),
            (ReadinessSignal, 5),
        ];
        arbitrate(&mut events);
        assert_eq!(
            events.map(|(_, value)| value),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            "same-class events retain their observation order"
        );

        // Keep this well above any small-sort threshold: the compact fixture
        // above documents every class, but an unstable insertion sort can
        // preserve a couple of duplicate pairs by accident. Every class is
        // duplicated many times here so the fixture's discriminating power
        // against a `sort_unstable_by_key` mutant does not rest on one
        // toolchain's threshold or pivot choice.
        const CLASSES: [ArbitrationClass; 8] = [
            ScopeShutdown,
            MembershipRemoval,
            ChildExit,
            ReadinessSignal,
            ReadinessDeadline,
            BackoffDue,
            StopDeadline,
            Admission,
        ];
        const PRESSURE: usize = 256;
        let mut same_class_pressure: [(ArbitrationClass, usize); PRESSURE] =
            std::array::from_fn(|index| (CLASSES[index % CLASSES.len()], index));
        arbitrate(&mut same_class_pressure);
        assert_eq!(
            same_class_pressure.map(|(_, value)| value).as_slice(),
            (0..CLASSES.len())
                .flat_map(|rank| (rank..PRESSURE).step_by(CLASSES.len()))
                .collect::<Vec<_>>(),
            "larger same-class batches retain FIFO rather than unstable-sort order"
        );
    }

    #[test]
    fn ladder_uses_cancel_escalate_and_hard_abort_for_every_policy() {
        let start = Instant::now();
        let grace = Duration::from_millis(100);
        let mut graceful =
            StopLadder::new(Shutdown::graceful(grace).expect("test grace is non-zero"));
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
        let mut ladder =
            StopLadder::new(Shutdown::graceful(grace).expect("test grace is non-zero"));

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

        let mut unforced =
            StopLadder::new(Shutdown::graceful(grace).expect("test grace is non-zero"));
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
    fn force_in_idle_arms_immediate_escalation_on_the_first_advance() {
        let forced_at = Instant::now();
        let mut ladder = StopLadder::new(
            Shutdown::graceful(Duration::from_secs(30)).expect("test grace is non-zero"),
        );

        ladder.force(forced_at);
        assert_eq!(ladder.deadline(), None, "an idle ladder is not armed yet");
        assert_eq!(ladder.advance(forced_at), Some(StopAction::Cancel));
        assert_eq!(ladder.deadline(), Some(forced_at));
        assert_eq!(
            ladder.advance(forced_at),
            Some(StopAction::Escalate),
            "force before the first advance skips the cooperative grace"
        );
    }

    #[test]
    fn force_preserves_an_already_due_deadline() {
        let start = Instant::now();
        let grace = Duration::from_secs(30);
        let mut ladder =
            StopLadder::new(Shutdown::graceful(grace).expect("test grace is non-zero"));

        assert_eq!(ladder.advance(start), Some(StopAction::Cancel));
        let due = ladder.deadline().expect("grace deadline");
        ladder.force(due + Duration::from_secs(1));

        assert_eq!(
            ladder.deadline(),
            Some(due),
            "forcing after expiry cannot move the deadline later"
        );
        assert_eq!(
            ladder.advance(due + Duration::from_secs(1)),
            Some(StopAction::Escalate),
            "the already-due ladder remains actionable at the force instant"
        );
        assert_eq!(
            ladder.advance(ladder.deadline().expect("tidy deadline")),
            Some(StopAction::HardAbort {
                phase: GracePhase::AfterGrace
            }),
            "force arriving after grace expiry preserves after-grace provenance"
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
    fn framework_abort_acknowledgement_is_ignored_before_the_abort_phase() {
        let start = Instant::now();
        let mut ladder = StopLadder::for_framework_driver(Shutdown::Abort);

        ladder.acknowledge_framework_abort();
        assert_eq!(ladder.advance(start), Some(StopAction::Cancel));
        ladder.acknowledge_framework_abort();
        assert_eq!(ladder.advance(start), Some(StopAction::Escalate));
        ladder.acknowledge_framework_abort();
        let tidy = ladder.deadline().expect("abort policy keeps a tidy beat");
        assert_eq!(
            ladder.advance(tidy),
            Some(StopAction::AbortFramework {
                phase: GracePhase::WithinGrace
            })
        );
        let framework_tidy = ladder.deadline().expect("framework abort has a tidy beat");
        assert_eq!(
            ladder.advance(framework_tidy),
            Some(StopAction::HardAbort {
                phase: GracePhase::WithinGrace
            }),
            "acknowledgements before AbortingFramework must not suppress the hard-abort fallback"
        );
    }

    #[test]
    fn overflowing_grace_stays_pending_until_force_rescues_the_ladder() {
        let start = Instant::now();
        let mut ladder = StopLadder::new(
            Shutdown::graceful(Duration::MAX).expect("maximum duration is non-zero"),
        );

        assert_eq!(ladder.advance(start), Some(StopAction::Cancel));
        assert_eq!(ladder.deadline(), None);
        assert_eq!(ladder.advance(start), None);

        let forced_at = start + Duration::from_secs(1);
        ladder.force(forced_at);
        assert_eq!(ladder.deadline(), Some(forced_at));
        assert_eq!(ladder.advance(forced_at), Some(StopAction::Escalate));
        assert_eq!(
            ladder.advance(ladder.deadline().expect("forced tidy deadline")),
            Some(StopAction::HardAbort {
                phase: GracePhase::WithinGrace
            })
        );
    }

    #[test]
    fn funnel_dispatch_covers_every_policy_exit_and_suppression_combination() {
        let cases = [
            (ExitKind::Completed, false),
            (ExitKind::Failed(ExitError::message("boom")), true),
            (
                ExitKind::Panicked {
                    message: Some("boom".to_owned()),
                },
                true,
            ),
            (
                ExitKind::ReadinessTimedOut {
                    deadline: Instant::now(),
                },
                true,
            ),
            (
                ExitKind::Aborted {
                    phase: GracePhase::AfterGrace,
                },
                true,
            ),
        ];
        let policies = [
            (RestartCondition::Never, false, false),
            (RestartCondition::OnFailure, false, true),
            (RestartCondition::Always, true, true),
        ];

        for cancellation in [Cancellation::NotObserved, Cancellation::Observed] {
            for (kind, failure) in &cases {
                // `NeverStarted` is a membership fact outside dispatch's
                // incarnation-exit domain.
                let exit = match kind {
                    ExitKind::Completed => Exit::completed(cancellation),
                    ExitKind::Failed(error) => Exit::failed(error.clone(), cancellation),
                    ExitKind::Panicked { message } => Exit::panicked(message.clone(), cancellation),
                    ExitKind::ReadinessTimedOut { deadline } => {
                        Exit::readiness_timed_out(*deadline, cancellation)
                    }
                    ExitKind::Aborted { phase } => Exit::aborted(*phase, cancellation),
                    ExitKind::NeverStarted => unreachable!("the cases exclude membership exits"),
                };
                assert_eq!(exit.is_failure(), *failure);
                for (condition, restart_completed, restart_failure) in policies {
                    let policy = RestartPolicy::new(condition, Backoff::Immediate);
                    let expected = if *failure {
                        restart_failure
                    } else {
                        restart_completed
                    };
                    assert_eq!(
                        dispatch_exit(&exit, policy, false, MembershipStatus::Active),
                        if expected {
                            ExitDispatch::ScheduleRestart
                        } else {
                            ExitDispatch::Terminal
                        },
                        "condition={condition:?}, kind={kind:?}, cancellation={cancellation:?}"
                    );
                    assert_eq!(
                        dispatch_exit(&exit, policy, true, MembershipStatus::Active),
                        ExitDispatch::Terminal,
                        "draining suppresses every restart"
                    );
                    assert_eq!(
                        dispatch_exit(&exit, policy, false, MembershipStatus::Removing),
                        ExitDispatch::Terminal,
                        "planned removal suppresses every restart"
                    );
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "NeverStarted is a membership outcome outside incarnation dispatch")]
    fn funnel_dispatch_rejects_the_membership_level_never_started_outcome() {
        let exit = Exit::never_started();
        let _ = dispatch_exit(
            &exit,
            RestartPolicy::new(RestartCondition::Always, Backoff::Immediate),
            false,
            MembershipStatus::Active,
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
        let trip_payload = IntensityTrip::new(trip);
        assert_eq!(trip_payload.max_restarts, policy.max_restarts());
        assert_eq!(trip_payload.observed_restarts, trip.in_window);
        assert_eq!(trip_payload.within, policy.within());

        let aged = state.charge(policy, start + Duration::from_secs(21));
        assert!(!aged.tripped);
        assert_eq!(aged.in_window, 1);
        assert_eq!(
            aged.total_restarts,
            TotalRestarts::ZERO.bump().bump().bump()
        );
    }

    /// Pins the observable behaviour of a charge dated before one already in
    /// the window; it cannot discriminate `charge`'s `checked_duration_since`
    /// from the saturating `duration_since`. That guard is defence in depth:
    /// the saturating form yields `Duration::ZERO` for a regressed clock, and
    /// `Intensity::new` rejects a zero `within`, so `ZERO > within` is
    /// already false for every constructible policy. Replacing the checked
    /// call with the saturating one leaves this assertion — and the rest of
    /// the suite — green.
    #[test]
    fn intensity_clock_regression_retains_future_charges() {
        let start = Instant::now();
        let policy = Intensity::new(5, Duration::from_secs(10)).expect("valid intensity");
        let mut state = IntensityState::default();

        assert_eq!(
            state
                .charge(policy, start + Duration::from_secs(5))
                .in_window,
            1
        );
        let regressed = state.charge(policy, start);
        assert_eq!(
            regressed.in_window, 2,
            "a charge from the apparent future stays in the window"
        );
        assert_eq!(regressed.total_restarts, TotalRestarts::ZERO.bump().bump());
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
        assert_eq!(
            decision.intensity_trip(),
            Some(IntensityTrip::new(decision.charge))
        );
    }

    #[test]
    fn overflowing_restart_delay_has_no_substitute_deadline() {
        let now = Instant::now();
        let intensity_policy = Intensity::new(5, Duration::from_secs(10)).expect("valid intensity");
        let restart_policy = RestartPolicy::new(
            RestartCondition::OnFailure,
            Backoff::fixed(Duration::MAX, Jitter::None).expect("valid backoff"),
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
    fn scope_lifecycle_owns_first_failure_drain_status_and_finish_policy() {
        let starting = ScopeLifecycle::starting();
        assert_eq!(
            starting.finish_if_ready(
                ScopeFlavor::Ordered,
                ChildCompletionState {
                    has_children: true,
                    all_terminal: true,
                },
            ),
            None,
            "natural completion waits for aggregate startup to complete"
        );

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
        let (startup_pending, state) = lifecycle
            .begin_drain(StopReason::ShutdownRequested)
            .expect("a failed startup can begin draining");
        assert!(!startup_pending);
        assert_eq!(state, ScopeState::Draining);
        assert_eq!(
            lifecycle.finish_if_ready(
                ScopeFlavor::Dynamic,
                ChildCompletionState {
                    has_children: true,
                    all_terminal: true,
                },
            ),
            Some(StopReason::ShutdownRequested)
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
            Some(StopReason::Finished)
        );

        let mut starting = ScopeLifecycle::starting();
        let (startup_pending, state) = starting
            .begin_drain(StopReason::ShutdownRequested)
            .expect("starting can begin draining");
        assert!(startup_pending);
        assert_eq!(state, ScopeState::Draining);
    }

    #[test]
    fn scope_lifecycle_upgrades_drain_reasons_monotonically() {
        let trip = IntensityTrip {
            max_restarts: 0,
            observed_restarts: 1,
            within: Duration::from_secs(10),
        };
        let startup_failure = StartupFailure {
            cause: StartupFailureCause::Lowering {
                undefined: vec![ChildId::from("worker")],
            },
        };
        let mut lifecycle = ScopeLifecycle::running();

        assert!(lifecycle.begin_drain(StopReason::Finished).is_some());
        assert_eq!(lifecycle.draining_reason(), Some(&StopReason::Finished));

        assert!(
            lifecycle
                .begin_drain(StopReason::IntensityTripped(trip.clone()))
                .is_none(),
            "an upgrade does not repeat the enter-drain effect"
        );
        assert_eq!(
            lifecycle.draining_reason(),
            Some(&StopReason::IntensityTripped(trip.clone()))
        );

        assert!(
            lifecycle
                .begin_drain(StopReason::StartupFailed(startup_failure.clone()))
                .is_none()
        );
        assert_eq!(
            lifecycle.draining_reason(),
            Some(&StopReason::StartupFailed(startup_failure.clone()))
        );

        assert!(
            lifecycle
                .begin_drain(StopReason::ShutdownRequested)
                .is_none()
        );
        assert_eq!(
            lifecycle.draining_reason(),
            Some(&StopReason::ShutdownRequested)
        );

        let lower_reasons = [
            StopReason::Finished,
            StopReason::IntensityTripped(trip),
            StopReason::StartupFailed(startup_failure),
        ];
        for reason in lower_reasons {
            assert!(lifecycle.begin_drain(reason).is_none());
            assert_eq!(
                lifecycle.draining_reason(),
                Some(&StopReason::ShutdownRequested),
                "a lower-precedence reason cannot replace shutdown"
            );
        }
    }
}
