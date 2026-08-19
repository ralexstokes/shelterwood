//! Structured child exits and pure exit classification.

use std::{
    error::Error,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::identity::{ChildId, Membership};

/// The terminal reason for a scope incarnation or root system.
///
/// Deliberately exhaustive while the crate is pre-release: matching it across
/// the crate boundary is how the façade proves it handles every state, and
/// there is no downstream user for `#[non_exhaustive]` to protect yet. The
/// attribute is a release-tagging decision, not an implementation one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StopReason {
    /// A non-empty ordered workload completed naturally.
    Finished,
    /// Shutdown was explicitly requested.
    ShutdownRequested,
    /// The scope exceeded its restart budget.
    IntensityTripped(IntensityTrip),
    /// A nested scope could not complete startup.
    StartupFailed(StartupFailure),
    /// The membership terminalized without an incarnation.
    NeverStarted,
}

pub fn stop_reason_into_nested_result(reason: StopReason) -> ExitResult {
    match reason {
        StopReason::Finished | StopReason::ShutdownRequested => Ok(()),
        StopReason::IntensityTripped(trip) => Err(structured_intensity_trip_error(trip)),
        StopReason::StartupFailed(failure) => Err(structured_startup_failure_error(failure)),
        StopReason::NeverStarted => Err(ExitError::message("nested scope never started")),
    }
}

pub fn stop_reason_root_exit(reason: &StopReason) -> Exit {
    match reason {
        StopReason::Finished => Exit::completed(Cancellation::NotObserved),
        StopReason::ShutdownRequested => Exit::completed(Cancellation::Observed),
        StopReason::IntensityTripped(trip) => Exit::failed(
            structured_intensity_trip_error(trip.clone()),
            Cancellation::NotObserved,
        ),
        StopReason::StartupFailed(failure) => Exit::failed(
            structured_startup_failure_error(failure.clone()),
            Cancellation::NotObserved,
        ),
        StopReason::NeverStarted => Exit::never_started(),
    }
}

pub fn stop_reason_precedence(reason: &StopReason) -> u8 {
    (match reason {
        StopReason::Finished => StopPrecedence::Finished,
        StopReason::IntensityTripped(_) => StopPrecedence::IntensityTripped,
        StopReason::StartupFailed(_) => StopPrecedence::StartupFailed,
        StopReason::ShutdownRequested => StopPrecedence::ShutdownRequested,
        StopReason::NeverStarted => StopPrecedence::NeverStarted,
    }) as u8
}

/// Total precedence order over stop reasons: the single lattice that resolves
/// every competing stop verdict for one incarnation.
///
/// Two owners can each reach a stop verdict for the same incarnation — a
/// driver's drain and a later teardown fallback, say — so the resolution rule
/// must be a property of the reasons themselves rather than of arrival order.
/// Both consumers join through this order: `ScopeLifecycle::begin_drain`
/// upgrades an in-progress drain, and `ScopeCell`'s stopped publisher upgrades
/// an already-published `Stopped` projection. Strictly-greater wins in both,
/// so equal verdicts are idempotent repeats.
///
/// The order is severity-ascending. `Finished` is the weakest claim: a drain
/// that began on natural completion says nothing about how the teardown
/// itself ended. `ShutdownRequested` outranks the structured failures because
/// a requested stop supersedes whatever the incarnation would otherwise have
/// reported. `NeverStarted` is the top element because it is not a live
/// incarnation's verdict at all but the membership-terminal twin of §7's
/// `Exit::never_started()` (SPEC B.6): whenever a membership terminalizes
/// without ever spawning, the scope-state projection must agree with the
/// membership exit, in either arrival order.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum StopPrecedence {
    Finished,
    IntensityTripped,
    StartupFailed,
    ShutdownRequested,
    NeverStarted,
}

/// Failure of the root startup barrier.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StartupError {
    /// A child or nested lowering failed terminally during startup.
    #[error("tree startup failed")]
    StartupFailed(#[source] StartupFailure),
    /// Restart intensity tripped during startup.
    #[error("restart intensity tripped during startup")]
    IntensityTripped(#[source] IntensityTrip),
    /// Shutdown began before startup completed.
    #[error("shutdown was requested during startup")]
    ShutdownRequested,
}

/// The exact result returned by restartable tasks and actor callbacks.
pub type ExitResult = Result<(), ExitError>;

/// A cloneable, type-erased application error.
///
/// This type deliberately does not implement [`std::error::Error`], allowing
/// the blanket `From<E>` conversion without overlap.
#[derive(Clone)]
pub struct ExitError(Arc<ExitErrorInner>);

enum ExitErrorInner {
    Application(Box<dyn Error + Send + Sync + 'static>),
    IntensityTrip(StructuredIntensityTrip),
    StartupFailure(StructuredStartupFailure),
}

impl ExitError {
    /// Creates an application error from a displayable message.
    #[must_use]
    pub fn message(message: impl Into<String>) -> Self {
        Self(Arc::new(ExitErrorInner::Application(Box::new(
            MessageError(message.into()),
        ))))
    }

    /// Views the erased source error.
    #[must_use]
    pub fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        match self.0.as_ref() {
            ExitErrorInner::Application(error) => error.as_ref(),
            ExitErrorInner::IntensityTrip(error) => error,
            ExitErrorInner::StartupFailure(error) => error,
        }
    }

    /// Returns the framework-authenticated intensity-trip payload, if any.
    #[must_use]
    pub fn intensity_trip(&self) -> Option<&IntensityTrip> {
        match self.0.as_ref() {
            ExitErrorInner::IntensityTrip(value) => Some(&value.0),
            ExitErrorInner::Application(_) | ExitErrorInner::StartupFailure(_) => None,
        }
    }

    /// Returns the framework-authenticated startup-failure payload, if any.
    #[must_use]
    pub fn startup_failure(&self) -> Option<&StartupFailure> {
        match self.0.as_ref() {
            ExitErrorInner::StartupFailure(value) => Some(&value.0),
            ExitErrorInner::Application(_) | ExitErrorInner::IntensityTrip(_) => None,
        }
    }
}

/// Mints the framework-authenticated intensity-trip payload.
///
/// This and its startup-failure twin are the entire authentication boundary
/// for [`ExitError::intensity_trip`] and [`ExitError::startup_failure`]: the
/// wrapped inner variants are unreachable any other way, so a value that
/// arrived through the blanket user conversion can never answer `Some`. The
/// split published this crate, which means the boundary is now a convention
/// rather than a privacy rule — hidden from documentation so it does not read
/// as supported API.
#[doc(hidden)]
fn structured_intensity_trip_error(value: IntensityTrip) -> ExitError {
    ExitError(Arc::new(ExitErrorInner::IntensityTrip(
        StructuredIntensityTrip(value),
    )))
}

/// Mints the framework-authenticated startup-failure payload; see
/// [`structured_intensity_trip_error`] for the boundary this participates in.
#[doc(hidden)]
pub fn structured_startup_failure_error(value: StartupFailure) -> ExitError {
    ExitError(Arc::new(ExitErrorInner::StartupFailure(
        StructuredStartupFailure(value),
    )))
}

/// Wraps any error as an application-classified [`ExitError`].
///
/// The classification is unconditional: converting an [`IntensityTrip`] or a
/// [`StartupFailure`] through this impl yields an unauthenticated application
/// error for which [`ExitError::intensity_trip`] and
/// [`ExitError::startup_failure`] return `None`. The structured,
/// framework-authenticated variants cannot be produced through the supported
/// `shelterwood` façade.
impl<E> From<E> for ExitError
where
    E: Error + Send + Sync + 'static,
{
    fn from(value: E) -> Self {
        Self(Arc::new(ExitErrorInner::Application(Box::new(value))))
    }
}

impl fmt::Debug for ExitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ExitError")
            .field(&format_args!("{}", self))
            .finish()
    }
}

impl fmt::Display for ExitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_error(), formatter)
    }
}

#[derive(Debug)]
struct MessageError(String);

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for MessageError {}

/// A scope restart-budget failure.
///
/// This type implements [`std::error::Error`], so `ExitError::from(trip)` or
/// `?` compiles via the blanket application-error conversion — but that path
/// produces an ordinary, *unauthenticated* application error for which
/// [`ExitError::intensity_trip`] returns `None`. Only the framework's
/// cross-crate implementation seam mints the structured variant; observe trips
/// through [`ExitError::intensity_trip`] rather than round-tripping the payload
/// through a user conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntensityTrip {
    /// Configured maximum restarts within the rolling window.
    pub max_restarts: u64,
    /// Number of charges including the charge that tripped the scope.
    pub observed_restarts: u64,
    /// Configured rolling-window width.
    pub within: Duration,
}

impl fmt::Display for IntensityTrip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "restart intensity exceeded: {} restarts exceeds {} within {:?}",
            self.observed_restarts, self.max_restarts, self.within
        )
    }
}

impl Error for IntensityTrip {}

#[derive(Debug)]
struct StructuredIntensityTrip(IntensityTrip);

impl fmt::Display for StructuredIntensityTrip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl Error for StructuredIntensityTrip {}

/// A structured nested-scope startup failure.
///
/// This type implements [`std::error::Error`], so `ExitError::from(failure)`
/// or `?` compiles via the blanket application-error conversion — but that
/// path produces an ordinary, *unauthenticated* application error for which
/// [`ExitError::startup_failure`] returns `None`. Only the framework's
/// cross-crate implementation seam mints the structured variant; observe
/// startup failures through [`ExitError::startup_failure`] rather than
/// round-tripping the payload through a user conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupFailure {
    /// The startup failure's exact cause.
    pub cause: StartupFailureCause,
}

impl fmt::Display for StartupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            StartupFailureCause::Child { id, exit, .. } => {
                write!(formatter, "child `{id}` failed during startup: ")?;
                match exit.kind() {
                    ExitKind::Completed => formatter.write_str("completed before readiness"),
                    ExitKind::Failed(error) => error.fmt(formatter),
                    ExitKind::Panicked {
                        message: Some(message),
                    } => write!(formatter, "panicked: {message}"),
                    ExitKind::Panicked { message: None } => formatter.write_str("panicked"),
                    ExitKind::ReadinessTimedOut { deadline } => {
                        write!(formatter, "readiness deadline expired at {deadline:?}")
                    }
                    ExitKind::Aborted {
                        phase: GracePhase::WithinGrace,
                    } => formatter.write_str("aborted within shutdown grace"),
                    ExitKind::Aborted {
                        phase: GracePhase::AfterGrace,
                    } => formatter.write_str("aborted after shutdown grace"),
                    ExitKind::NeverStarted => formatter.write_str("never started"),
                }
            }
            StartupFailureCause::Lowering { undefined } => {
                write!(formatter, "subtree has {} undefined slots", undefined.len())
            }
            StartupFailureCause::IdentityExhausted { id } => {
                write!(
                    formatter,
                    "membership identity space is exhausted for child `{id}`"
                )
            }
        }
    }
}

impl Error for StartupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            StartupFailureCause::Child { exit, .. } => match exit.kind() {
                ExitKind::Failed(error) => Some(error.as_error()),
                ExitKind::Completed
                | ExitKind::Panicked { .. }
                | ExitKind::ReadinessTimedOut { .. }
                | ExitKind::Aborted { .. }
                | ExitKind::NeverStarted => None,
            },
            StartupFailureCause::Lowering { .. }
            | StartupFailureCause::IdentityExhausted { .. } => None,
        }
    }
}

/// The cause of a nested-scope startup failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartupFailureCause {
    /// A child terminalized before releasing its readiness gate.
    Child {
        /// The triggering child id.
        id: ChildId,
        /// The triggering membership.
        membership: Membership,
        /// The triggering terminal exit.
        exit: Exit,
    },
    /// A produced subtree contained undefined reservations.
    Lowering {
        /// Undefined child ids relative to the subtree root.
        undefined: Vec<ChildId>,
    },
    /// The stable scope could mint no identity for a declared child.
    IdentityExhausted {
        /// Child whose stable membership could not be minted.
        id: ChildId,
    },
}

#[derive(Debug)]
struct StructuredStartupFailure(StartupFailure);

impl fmt::Display for StructuredStartupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl Error for StructuredStartupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

/// Whether an incarnation observed supervisor cancellation before exiting.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Cancellation {
    /// The incarnation completed without observing supervisor cancellation.
    NotObserved,
    /// The incarnation's shutdown token fired before its outcome was recorded.
    Observed,
}

/// Whether a forced abort happened before or after cooperative grace expired.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GracePhase {
    /// The future was aborted before the grace deadline expired.
    WithinGrace,
    /// The future was aborted after the grace deadline expired.
    AfterGrace,
}

/// The structured result of one child incarnation or never-started
/// membership.
///
/// # Equality
///
/// `Exit` equality is structural for every variant except
/// [`ExitKind::Failed`], whose [`ExitError`] payload compares by
/// **shared provenance** (the same erased error allocation), not by error
/// content: an exit and its clones — including the copies the framework
/// publishes through snapshots and lifecycle events — compare equal,
/// while two independently constructed errors with identical messages do
/// not. Compare failure content through [`ExitError::as_error`] instead.
#[derive(Clone, Debug)]
pub struct Exit {
    kind: ExitKind,
    cancellation: Cancellation,
}

impl Exit {
    const fn from_kind(kind: ExitKind, cancellation: Cancellation) -> Self {
        Self { kind, cancellation }
    }

    /// Constructs a successfully completed incarnation exit.
    #[must_use]
    pub const fn completed(cancellation: Cancellation) -> Self {
        Self::from_kind(ExitKind::Completed, cancellation)
    }

    /// Constructs an application-failure exit.
    #[must_use]
    pub const fn failed(error: ExitError, cancellation: Cancellation) -> Self {
        Self::from_kind(ExitKind::Failed(error), cancellation)
    }

    /// Constructs an exit for a panic in user code or destruction.
    #[must_use]
    pub const fn panicked(message: Option<String>, cancellation: Cancellation) -> Self {
        Self::from_kind(ExitKind::Panicked { message }, cancellation)
    }

    /// Constructs an exit for an expired readiness deadline.
    #[must_use]
    pub const fn readiness_timed_out(deadline: Instant, cancellation: Cancellation) -> Self {
        Self::from_kind(ExitKind::ReadinessTimedOut { deadline }, cancellation)
    }

    /// Constructs an exit for an incarnation future destroyed by the framework.
    #[must_use]
    pub const fn aborted(phase: GracePhase, cancellation: Cancellation) -> Self {
        Self::from_kind(ExitKind::Aborted { phase }, cancellation)
    }

    /// Returns the exit kind.
    #[must_use]
    pub const fn kind(&self) -> &ExitKind {
        &self.kind
    }

    /// Returns the incarnation's supervisor-cancellation observation.
    #[must_use]
    pub const fn cancellation(&self) -> Cancellation {
        self.cancellation
    }

    /// Reports whether this exit participates in failure restart policy.
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        !matches!(self.kind, ExitKind::Completed)
    }

    /// Constructs the membership-level never-started exit.
    ///
    /// A membership with no incarnation cannot have observed an incarnation's
    /// cancellation token, so this constructor fixes cancellation to
    /// [`Cancellation::NotObserved`].
    #[must_use]
    pub const fn never_started() -> Self {
        Self::from_kind(ExitKind::NeverStarted, Cancellation::NotObserved)
    }
}

/// Equality per the type-level contract: structural on every variant,
/// except that [`ExitKind::Failed`] payloads compare by shared provenance
/// ([`Arc::ptr_eq`] on the erased error), because a type-erased error has
/// no content equality to consult.
impl PartialEq for Exit {
    fn eq(&self, other: &Self) -> bool {
        self.cancellation == other.cancellation && exit_kind_eq(&self.kind, &other.kind)
    }
}

impl Eq for Exit {}

fn exit_kind_eq(left: &ExitKind, right: &ExitKind) -> bool {
    match (left, right) {
        (ExitKind::Completed, ExitKind::Completed)
        | (ExitKind::NeverStarted, ExitKind::NeverStarted) => true,
        (ExitKind::Failed(left), ExitKind::Failed(right)) => Arc::ptr_eq(&left.0, &right.0),
        (ExitKind::Panicked { message: left }, ExitKind::Panicked { message: right }) => {
            left == right
        }
        (
            ExitKind::ReadinessTimedOut { deadline: left },
            ExitKind::ReadinessTimedOut { deadline: right },
        ) => left == right,
        (ExitKind::Aborted { phase: left }, ExitKind::Aborted { phase: right }) => left == right,
        _ => false,
    }
}

/// The primary classification of an [`Exit`].
#[derive(Clone, Debug)]
pub enum ExitKind {
    /// The incarnation returned successfully.
    Completed,
    /// User code returned an error.
    ///
    /// Under [`Exit`]'s `PartialEq`, this payload compares by shared
    /// provenance, not by error content — see [`Exit`]'s equality docs.
    Failed(ExitError),
    /// User code or destruction panicked.
    Panicked {
        /// A string panic payload when one was available.
        message: Option<String>,
    },
    /// The configured readiness deadline expired.
    ReadinessTimedOut {
        /// Absolute deadline that expired.
        deadline: Instant,
    },
    /// The incarnation future was destroyed before producing an outcome.
    Aborted {
        /// Cooperative-grace phase in which the abort happened.
        phase: GracePhase,
    },
    /// The membership terminalized without spawning an incarnation.
    NeverStarted,
}

/// Runtime-neutral result of joining one supervised operation.
pub enum JoinOutcome<T> {
    Ok { value: T },
    Panic { message: Option<String> },
    Cancelled,
}

/// An exit kind recorded before the incarnation's runtime join completes.
///
/// Keeping this as a distinct type preserves the report protocol's provenance
/// distinction without maintaining a second encoding of exit classification.
#[derive(Clone, Debug)]
pub struct RecordedOutcome(ExitKind);

impl RecordedOutcome {
    pub fn returned(result: ExitResult) -> Self {
        Self(match result {
            Ok(()) => ExitKind::Completed,
            Err(error) => ExitKind::Failed(error),
        })
    }

    pub fn readiness_timed_out(deadline: Instant) -> Self {
        Self(ExitKind::ReadinessTimedOut { deadline })
    }

    pub fn aborted(phase: GracePhase) -> Self {
        Self(ExitKind::Aborted { phase })
    }

    fn into_kind(self) -> ExitKind {
        self.0
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn kind(&self) -> &ExitKind {
        &self.0
    }
}

/// Reconciles task-produced evidence with a later supervisor-forced outcome.
///
/// The same diagnostic precedence used for the final join applies here, and
/// equal-ranked evidence keeps the task-produced value because it was
/// recorded first. This prevents a forced abort from erasing a structured
/// application failure while still allowing a readiness timeout to supersede
/// ordinary completion; the final join uses the same rule when it contributes
/// panic evidence.
pub fn reconcile_recorded_outcomes(
    recorded: Option<RecordedOutcome>,
    forced: Option<RecordedOutcome>,
) -> Option<RecordedOutcome> {
    match (recorded, forced) {
        (Some(recorded), Some(forced)) => Some(RecordedOutcome(prefer_earlier(
            recorded.into_kind(),
            forced.into_kind(),
        ))),
        (Some(recorded), None) => Some(recorded),
        (None, Some(forced)) => Some(forced),
        (None, None) => None,
    }
}

/// Purely folds the provisional run outcome and post-destruction join verdict.
///
/// Precedence follows the amount of diagnostic evidence retained by each
/// outcome: a panic is strongest, followed by a readiness timeout, an
/// application failure, an abort, and successful completion. In particular,
/// cancellation reported while joining only proves that destruction ended the
/// task; it must not erase an application error recorded before destruction.
/// Equal-ranked outcomes keep the earlier recorded evidence, including its
/// panic payload or abort provenance. A cancelled join uses the supervisor's
/// hard-abort phase when one was recorded and otherwise fails closed to
/// [`GracePhase::WithinGrace`].
pub fn classify_exit(
    recorded: Option<RecordedOutcome>,
    join: JoinOutcome<()>,
    hard_abort_phase: Option<GracePhase>,
    cancellation: Cancellation,
) -> Exit {
    let recorded_kind = recorded.map(RecordedOutcome::into_kind);
    let join_kind = match join {
        JoinOutcome::Ok { .. } => None,
        JoinOutcome::Panic { message } => Some(ExitKind::Panicked { message }),
        JoinOutcome::Cancelled => Some(ExitKind::Aborted {
            phase: hard_abort_phase.unwrap_or(GracePhase::WithinGrace),
        }),
    };

    let kind = match (recorded_kind, join_kind) {
        (Some(recorded), Some(join)) => prefer_earlier(recorded, join),
        (Some(recorded), None) => recorded,
        (None, Some(join)) => join,
        (None, None) => ExitKind::Aborted {
            phase: GracePhase::WithinGrace,
        },
    };
    Exit::from_kind(kind, cancellation)
}

/// Adds a destructor panic to an already-classified exit without erasing an
/// earlier panic, which has equal diagnostic precedence and happened first.
pub fn classify_disposal_panic(exit: Exit, message: Option<String>) -> Exit {
    let Exit { kind, cancellation } = exit;
    let kind = prefer_earlier(kind, ExitKind::Panicked { message });
    Exit::from_kind(kind, cancellation)
}

/// Diagnostic precedence shared by provisional and final exit evidence.
///
/// Variant order is weakest to strongest so derived ordering selects the
/// outcome that preserves the most useful evidence.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OutcomeRank {
    NeverStarted,
    Completed,
    Aborted,
    Failed,
    ReadinessTimedOut,
    Panicked,
}

fn prefer_earlier(earlier: ExitKind, later: ExitKind) -> ExitKind {
    if rank(&earlier) >= rank(&later) {
        earlier
    } else {
        later
    }
}

fn rank(kind: &ExitKind) -> OutcomeRank {
    match kind {
        ExitKind::Panicked { .. } => OutcomeRank::Panicked,
        ExitKind::ReadinessTimedOut { .. } => OutcomeRank::ReadinessTimedOut,
        ExitKind::Failed(_) => OutcomeRank::Failed,
        ExitKind::Aborted { .. } => OutcomeRank::Aborted,
        ExitKind::Completed => OutcomeRank::Completed,
        ExitKind::NeverStarted => OutcomeRank::NeverStarted,
    }
}

/// A child path and membership that exceeded a scope shutdown deadline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownStraggler {
    /// Child-id path relative to the scope whose deadline expired.
    pub path: Vec<ChildId>,
    /// Exact membership that remained live.
    pub membership: Membership,
}

/// Structured descendants remaining at a scope shutdown deadline.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("scope shutdown exceeded its deadline with {0} stragglers", .stragglers.len())]
pub struct ShutdownTimeout {
    /// Descendants that required forced abort.
    pub stragglers: Vec<ShutdownStraggler>,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::identity::ScopeIdentity;

    use super::{
        Cancellation, ChildId, Exit, ExitError, ExitKind, GracePhase, IntensityTrip, JoinOutcome,
        RecordedOutcome, StartupError, StartupFailure, StartupFailureCause, StopReason,
        classify_disposal_panic, classify_exit, exit_kind_eq, prefer_earlier,
        reconcile_recorded_outcomes, stop_reason_into_nested_result, stop_reason_precedence,
        stop_reason_root_exit, structured_intensity_trip_error, structured_startup_failure_error,
    };

    fn exit(kind: ExitKind, cancellation: Cancellation) -> Exit {
        match kind {
            ExitKind::Completed => Exit::completed(cancellation),
            ExitKind::Failed(error) => Exit::failed(error, cancellation),
            ExitKind::Panicked { message } => Exit::panicked(message, cancellation),
            ExitKind::ReadinessTimedOut { deadline } => {
                Exit::readiness_timed_out(deadline, cancellation)
            }
            ExitKind::Aborted { phase } => Exit::aborted(phase, cancellation),
            ExitKind::NeverStarted => {
                assert_eq!(cancellation, Cancellation::NotObserved);
                Exit::never_started()
            }
        }
    }

    #[test]
    fn child_startup_failure_display_summarizes_every_exit_kind() {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let membership = identity
            .mint_membership(&id)
            .expect("membership available")
            .membership();
        let deadline = Instant::now() + Duration::from_secs(1);
        let cases = [
            (
                ExitKind::Completed,
                "child `worker` failed during startup: completed before readiness".to_owned(),
            ),
            (
                ExitKind::Failed(ExitError::message("application failure")),
                "child `worker` failed during startup: application failure".to_owned(),
            ),
            (
                ExitKind::Panicked {
                    message: Some("panic payload".to_owned()),
                },
                "child `worker` failed during startup: panicked: panic payload".to_owned(),
            ),
            (
                ExitKind::Panicked { message: None },
                "child `worker` failed during startup: panicked".to_owned(),
            ),
            (
                ExitKind::ReadinessTimedOut { deadline },
                format!(
                    "child `worker` failed during startup: readiness deadline expired at {deadline:?}"
                ),
            ),
            (
                ExitKind::Aborted {
                    phase: GracePhase::WithinGrace,
                },
                "child `worker` failed during startup: aborted within shutdown grace".to_owned(),
            ),
            (
                ExitKind::Aborted {
                    phase: GracePhase::AfterGrace,
                },
                "child `worker` failed during startup: aborted after shutdown grace".to_owned(),
            ),
            (
                ExitKind::NeverStarted,
                "child `worker` failed during startup: never started".to_owned(),
            ),
        ];

        for (kind, expected) in cases {
            let failure = StartupFailure {
                cause: StartupFailureCause::Child {
                    id: id.clone(),
                    membership,
                    exit: exit(kind, Cancellation::NotObserved),
                },
            };

            assert_eq!(failure.to_string(), expected);
        }
    }

    #[test]
    fn recorded_outcomes_are_canonical_exit_kinds() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let failure = ExitError::message("failed");
        let cases = [
            (
                "successful return",
                RecordedOutcome::returned(Ok(())),
                ExitKind::Completed,
            ),
            (
                "failed return",
                RecordedOutcome::returned(Err(failure.clone())),
                ExitKind::Failed(failure),
            ),
            (
                "readiness timeout",
                RecordedOutcome::readiness_timed_out(deadline),
                ExitKind::ReadinessTimedOut { deadline },
            ),
            (
                "forced abort",
                RecordedOutcome::aborted(GracePhase::AfterGrace),
                ExitKind::Aborted {
                    phase: GracePhase::AfterGrace,
                },
            ),
        ];

        for (case, recorded, expected) in cases {
            assert!(
                exit_kind_eq(recorded.kind(), &expected),
                "{case}: recorded {:?}, expected {expected:?}",
                recorded.kind()
            );
        }
    }

    #[test]
    fn precedence_is_total_and_keeps_earlier_evidence_on_ties() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let failure = ExitError::message("failed");
        let ordered = [
            ("never started", ExitKind::NeverStarted),
            ("completed", ExitKind::Completed),
            (
                "aborted",
                ExitKind::Aborted {
                    phase: GracePhase::WithinGrace,
                },
            ),
            ("failed", ExitKind::Failed(failure)),
            (
                "readiness timed out",
                ExitKind::ReadinessTimedOut { deadline },
            ),
            (
                "panicked",
                ExitKind::Panicked {
                    message: Some("primary".to_owned()),
                },
            ),
        ];

        for (earlier_index, (earlier_name, earlier)) in ordered.iter().enumerate() {
            for (later_index, (later_name, later)) in ordered.iter().enumerate() {
                let selected = prefer_earlier(earlier.clone(), later.clone());
                let expected = if earlier_index >= later_index {
                    earlier
                } else {
                    later
                };
                assert!(
                    exit_kind_eq(&selected, expected),
                    "earlier {earlier_name}, later {later_name}: selected {selected:?}, expected {expected:?}"
                );
            }
        }

        let earlier = ExitKind::Panicked {
            message: Some("earlier".to_owned()),
        };
        let later = ExitKind::Panicked {
            message: Some("later".to_owned()),
        };
        assert!(exit_kind_eq(
            &prefer_earlier(earlier.clone(), later),
            &earlier
        ));
    }

    #[test]
    fn classification_precedence_is_table_driven() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let failure = ExitError::message("failed");
        let cases = [
            (
                "recorded completion",
                Some(RecordedOutcome::returned(Ok(()))),
                JoinOutcome::Ok { value: () },
                None,
                Cancellation::NotObserved,
                Exit::completed(Cancellation::NotObserved),
            ),
            (
                "cancellation overrides recorded completion",
                Some(RecordedOutcome::returned(Ok(()))),
                JoinOutcome::Cancelled,
                Some(GracePhase::AfterGrace),
                Cancellation::Observed,
                exit(
                    ExitKind::Aborted {
                        phase: GracePhase::AfterGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "recorded failure",
                Some(RecordedOutcome::returned(Err(failure.clone()))),
                JoinOutcome::Ok { value: () },
                None,
                Cancellation::NotObserved,
                Exit::failed(failure.clone(), Cancellation::NotObserved),
            ),
            (
                "late cancellation preserves recorded failure",
                Some(RecordedOutcome::returned(Err(failure.clone()))),
                JoinOutcome::Cancelled,
                Some(GracePhase::AfterGrace),
                Cancellation::Observed,
                Exit::failed(failure.clone(), Cancellation::Observed),
            ),
            (
                "join panic overrides recorded failure",
                Some(RecordedOutcome::returned(Err(failure.clone()))),
                JoinOutcome::Panic {
                    message: Some("drop panic".to_owned()),
                },
                None,
                Cancellation::NotObserved,
                exit(
                    ExitKind::Panicked {
                        message: Some("drop panic".to_owned()),
                    },
                    Cancellation::NotObserved,
                ),
            ),
            (
                "readiness timeout overrides cancellation",
                Some(RecordedOutcome::readiness_timed_out(deadline)),
                JoinOutcome::Cancelled,
                Some(GracePhase::AfterGrace),
                Cancellation::Observed,
                exit(
                    ExitKind::ReadinessTimedOut { deadline },
                    Cancellation::Observed,
                ),
            ),
            (
                "join panic overrides recorded completion",
                Some(RecordedOutcome::returned(Ok(()))),
                JoinOutcome::Panic {
                    message: Some("drop panic".to_owned()),
                },
                None,
                Cancellation::NotObserved,
                exit(
                    ExitKind::Panicked {
                        message: Some("drop panic".to_owned()),
                    },
                    Cancellation::NotObserved,
                ),
            ),
            (
                "earlier recorded panic wins a tie",
                Some(RecordedOutcome(ExitKind::Panicked {
                    message: Some("callback panic".to_owned()),
                })),
                JoinOutcome::Panic {
                    message: Some("drop panic".to_owned()),
                },
                None,
                Cancellation::Observed,
                exit(
                    ExitKind::Panicked {
                        message: Some("callback panic".to_owned()),
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "earlier recorded abort wins a tie",
                Some(RecordedOutcome::aborted(GracePhase::WithinGrace)),
                JoinOutcome::Cancelled,
                Some(GracePhase::AfterGrace),
                Cancellation::Observed,
                exit(
                    ExitKind::Aborted {
                        phase: GracePhase::WithinGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "cancelled join without a hard abort fails closed to within grace",
                None,
                JoinOutcome::Cancelled,
                None,
                Cancellation::Observed,
                exit(
                    ExitKind::Aborted {
                        phase: GracePhase::WithinGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "hard abort phase is ignored when the join completed normally",
                Some(RecordedOutcome::returned(Ok(()))),
                JoinOutcome::Ok { value: () },
                Some(GracePhase::AfterGrace),
                Cancellation::Observed,
                Exit::completed(Cancellation::Observed),
            ),
            (
                "join cancellation supplies a missing outcome",
                None,
                JoinOutcome::Cancelled,
                Some(GracePhase::AfterGrace),
                Cancellation::Observed,
                exit(
                    ExitKind::Aborted {
                        phase: GracePhase::AfterGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "missing outcome fails closed",
                None,
                JoinOutcome::Ok { value: () },
                None,
                Cancellation::NotObserved,
                exit(
                    ExitKind::Aborted {
                        phase: GracePhase::WithinGrace,
                    },
                    Cancellation::NotObserved,
                ),
            ),
        ];

        for (case, recorded, join, hard_abort_phase, cancellation, expected) in cases {
            assert_eq!(
                classify_exit(recorded, join, hard_abort_phase, cancellation),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn disposal_panic_uses_exit_precedence_and_preserves_cancellation() {
        let classified = classify_disposal_panic(
            Exit::completed(Cancellation::Observed),
            Some("destructor".to_owned()),
        );
        assert_eq!(
            classified,
            exit(
                ExitKind::Panicked {
                    message: Some("destructor".to_owned())
                },
                Cancellation::Observed
            )
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        for weaker in [
            ExitKind::Failed(ExitError::message("failed")),
            ExitKind::ReadinessTimedOut { deadline },
        ] {
            assert_eq!(
                classify_disposal_panic(
                    exit(weaker, Cancellation::NotObserved),
                    Some("destructor".to_owned()),
                ),
                exit(
                    ExitKind::Panicked {
                        message: Some("destructor".to_owned())
                    },
                    Cancellation::NotObserved
                )
            );
        }

        let earlier = exit(
            ExitKind::Panicked {
                message: Some("task".to_owned()),
            },
            Cancellation::NotObserved,
        );
        assert_eq!(
            classify_disposal_panic(earlier.clone(), Some("destructor".to_owned())),
            earlier
        );
    }

    #[test]
    fn stop_reasons_own_nested_and_root_exit_projection() {
        assert!(stop_reason_into_nested_result(StopReason::Finished).is_ok());
        assert!(stop_reason_into_nested_result(StopReason::ShutdownRequested).is_ok());
        assert_eq!(
            stop_reason_root_exit(&StopReason::ShutdownRequested),
            Exit::completed(Cancellation::Observed)
        );
        assert_eq!(
            stop_reason_root_exit(&StopReason::NeverStarted),
            Exit::never_started()
        );

        let trip = IntensityTrip {
            max_restarts: 2,
            observed_restarts: 3,
            within: Duration::from_secs(10),
        };
        let nested = stop_reason_into_nested_result(StopReason::IntensityTripped(trip.clone()))
            .expect_err("an intensity trip fails a nested scope");
        assert_eq!(nested.intensity_trip(), Some(&trip));
        let root = stop_reason_root_exit(&StopReason::IntensityTripped(trip.clone()));
        let ExitKind::Failed(error) = root.kind() else {
            panic!("an intensity trip fails the root")
        };
        assert_eq!(error.intensity_trip(), Some(&trip));

        let failure = StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: ChildId::from("nested"),
            },
        };
        let nested = stop_reason_into_nested_result(StopReason::StartupFailed(failure.clone()))
            .expect_err("startup failure fails a nested scope");
        assert_eq!(nested.startup_failure(), Some(&failure));
        let root = stop_reason_root_exit(&StopReason::StartupFailed(failure.clone()));
        let ExitKind::Failed(error) = root.kind() else {
            panic!("startup failure fails the root")
        };
        assert_eq!(error.startup_failure(), Some(&failure));

        let never_started = stop_reason_into_nested_result(StopReason::NeverStarted)
            .expect_err("a never-started nested scope fails closed");
        assert_eq!(
            never_started.as_error().to_string(),
            "nested scope never started"
        );
    }

    #[test]
    fn stop_reason_precedence_is_an_explicit_exhaustive_table() {
        let trip = IntensityTrip {
            max_restarts: 1,
            observed_restarts: 2,
            within: Duration::from_secs(10),
        };
        let startup = StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: ChildId::from("nested"),
            },
        };
        let cases = [
            (StopReason::Finished, super::StopPrecedence::Finished),
            (
                StopReason::IntensityTripped(trip),
                super::StopPrecedence::IntensityTripped,
            ),
            (
                StopReason::StartupFailed(startup),
                super::StopPrecedence::StartupFailed,
            ),
            (
                StopReason::ShutdownRequested,
                super::StopPrecedence::ShutdownRequested,
            ),
            (
                StopReason::NeverStarted,
                super::StopPrecedence::NeverStarted,
            ),
        ];

        for (reason, expected) in cases {
            assert_eq!(stop_reason_precedence(&reason), expected as u8);
        }
    }

    #[test]
    fn failed_exit_equality_is_shared_provenance_not_content() {
        let error = ExitError::message("boom");
        let exit = Exit::failed(error.clone(), Cancellation::NotObserved);

        // Clones of one error — including framework-published copies —
        // compare equal.
        assert_eq!(exit, exit.clone());
        assert_eq!(exit, Exit::failed(error.clone(), Cancellation::NotObserved));

        // Independently created errors with identical content do not.
        assert_ne!(
            exit,
            Exit::failed(ExitError::message("boom"), Cancellation::NotObserved)
        );

        // Cancellation remains structural even with shared provenance.
        assert_ne!(exit, Exit::failed(error, Cancellation::Observed));
    }

    #[test]
    fn forced_outcomes_do_not_erase_stronger_recorded_evidence() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let failure = structured_startup_failure_error(StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: ChildId::from("nested"),
            },
        });
        let cases = [
            (
                "application failure survives forced abort",
                Some(RecordedOutcome::returned(Err(failure.clone()))),
                Some(RecordedOutcome::aborted(GracePhase::AfterGrace)),
                Exit::failed(failure, Cancellation::Observed),
            ),
            (
                "forced readiness timeout overrides completion",
                Some(RecordedOutcome::returned(Ok(()))),
                Some(RecordedOutcome::readiness_timed_out(deadline)),
                exit(
                    ExitKind::ReadinessTimedOut { deadline },
                    Cancellation::Observed,
                ),
            ),
            (
                "earlier abort evidence wins a tie",
                Some(RecordedOutcome::aborted(GracePhase::WithinGrace)),
                Some(RecordedOutcome::aborted(GracePhase::AfterGrace)),
                exit(
                    ExitKind::Aborted {
                        phase: GracePhase::WithinGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
        ];

        for (case, recorded, forced, expected) in cases {
            let reconciled = reconcile_recorded_outcomes(recorded, forced);
            assert_eq!(
                classify_exit(
                    reconciled,
                    JoinOutcome::Cancelled,
                    Some(GracePhase::AfterGrace),
                    Cancellation::Observed
                ),
                expected,
                "{case}"
            );
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("restart intensity exceeded")]
    struct ForgedTrip;

    #[test]
    fn application_errors_cannot_forge_structured_provenance() {
        let error = ExitError::from(ForgedTrip);
        assert!(error.intensity_trip().is_none());
        assert!(error.startup_failure().is_none());

        let error = ExitError::from(IntensityTrip {
            max_restarts: 2,
            observed_restarts: 3,
            within: Duration::from_secs(10),
        });
        assert!(error.intensity_trip().is_none());

        let error = ExitError::from(StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: ChildId::from("nested"),
            },
        });
        assert!(error.startup_failure().is_none());
    }

    #[test]
    fn framework_errors_expose_structured_provenance_and_erased_views() {
        let trip = IntensityTrip {
            max_restarts: 2,
            observed_restarts: 3,
            within: Duration::from_secs(10),
        };
        let error = structured_intensity_trip_error(trip.clone());
        assert_eq!(error.intensity_trip(), Some(&trip));
        assert_eq!(
            error.as_error().to_string(),
            "restart intensity exceeded: 3 restarts exceeds 2 within 10s"
        );

        let failure = StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: ChildId::from("nested"),
            },
        };
        let error = structured_startup_failure_error(failure.clone());
        assert_eq!(error.startup_failure(), Some(&failure));
        assert_eq!(
            error.as_error().to_string(),
            "membership identity space is exhausted for child `nested`"
        );
    }

    #[test]
    fn startup_errors_chain_their_structured_detail() {
        let error = StartupError::StartupFailed(StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: ChildId::from("nested"),
            },
        });
        assert_eq!(error.to_string(), "tree startup failed");
        assert_eq!(
            std::error::Error::source(&error)
                .expect("startup failure is the source")
                .to_string(),
            "membership identity space is exhausted for child `nested`"
        );

        let error = StartupError::IntensityTripped(IntensityTrip {
            max_restarts: 2,
            observed_restarts: 3,
            within: Duration::from_secs(10),
        });
        assert_eq!(
            error.to_string(),
            "restart intensity tripped during startup"
        );
        assert_eq!(
            std::error::Error::source(&error)
                .expect("intensity trip is the source")
                .to_string(),
            "restart intensity exceeded: 3 restarts exceeds 2 within 10s"
        );
    }
}
