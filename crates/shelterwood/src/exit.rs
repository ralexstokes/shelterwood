//! Structured child exits and pure exit classification.

use std::{
    error::Error,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::identity::{ChildId, Membership};

/// The terminal reason for a scope incarnation or root system.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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

/// Failure of the root startup barrier.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
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

    pub(crate) fn from_intensity_trip(value: IntensityTrip) -> Self {
        Self(Arc::new(ExitErrorInner::IntensityTrip(
            StructuredIntensityTrip(value),
        )))
    }

    pub(crate) fn from_startup_failure(value: StartupFailure) -> Self {
        Self(Arc::new(ExitErrorInner::StartupFailure(
            StructuredStartupFailure(value),
        )))
    }
}

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
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StartupFailure {
    /// The startup failure's exact cause.
    pub cause: StartupFailureCause,
}

impl fmt::Display for StartupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            StartupFailureCause::Child { id, .. } => {
                write!(formatter, "child `{id}` failed during startup")
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
            StartupFailureCause::InvalidPolicy(invalid) => invalid.fmt(formatter),
        }
    }
}

impl Error for StartupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            StartupFailureCause::InvalidPolicy(invalid) => Some(invalid),
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
#[non_exhaustive]
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
        /// Undefined child-id paths relative to the subtree root.
        undefined: Vec<Vec<ChildId>>,
    },
    /// The stable scope could mint no identity for a declared child.
    IdentityExhausted {
        /// Child whose stable membership could not be minted.
        id: ChildId,
    },
    /// A produced subtree contained an invalid public policy literal.
    InvalidPolicy(crate::policy::InvalidPolicy),
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
    /// Creates an exit from its kind and cancellation observation.
    #[must_use]
    pub const fn new(kind: ExitKind, cancellation: Cancellation) -> Self {
        Self { kind, cancellation }
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
    #[must_use]
    pub const fn never_started() -> Self {
        Self::new(ExitKind::NeverStarted, Cancellation::NotObserved)
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
#[non_exhaustive]
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

#[derive(Clone, Debug)]
#[allow(dead_code)] // Callback-boundary panic recording joins in M3.
pub(crate) enum RecordedOutcome {
    Returned(ExitResult),
    Panicked { message: Option<String> },
    ReadinessTimedOut { deadline: Instant },
    Aborted { phase: GracePhase },
}

#[derive(Clone, Debug)]
pub(crate) enum JoinVerdict {
    Completed,
    Panicked { message: Option<String> },
    Cancelled { phase: GracePhase },
}

/// Reconciles task-produced evidence with a later supervisor-forced outcome.
///
/// The same diagnostic precedence used for the final join applies here, and
/// equal-ranked evidence keeps the task-produced value because it was
/// recorded first. This prevents a forced abort from erasing a structured
/// application failure while still allowing readiness timeout or panic to
/// supersede ordinary completion.
pub(crate) fn reconcile_recorded_outcomes(
    recorded: Option<RecordedOutcome>,
    forced: Option<RecordedOutcome>,
) -> Option<RecordedOutcome> {
    match (recorded, forced) {
        (Some(recorded), Some(forced)) if recorded_rank(&recorded) >= recorded_rank(&forced) => {
            Some(recorded)
        }
        (_, Some(forced)) => Some(forced),
        (Some(recorded), None) => Some(recorded),
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
/// panic payload or abort provenance.
pub(crate) fn classify_exit(
    recorded: Option<RecordedOutcome>,
    join: JoinVerdict,
    cancellation: Cancellation,
) -> Exit {
    let recorded_kind = recorded.map(recorded_kind);
    let join_kind = match join {
        JoinVerdict::Completed => None,
        JoinVerdict::Panicked { message } => Some(ExitKind::Panicked { message }),
        JoinVerdict::Cancelled { phase } => Some(ExitKind::Aborted { phase }),
    };

    let kind = match (recorded_kind, join_kind) {
        (Some(recorded), Some(join)) if rank(&recorded) >= rank(&join) => recorded,
        (_, Some(join)) => join,
        (Some(recorded), None) => recorded,
        (None, None) => ExitKind::Aborted {
            phase: GracePhase::WithinGrace,
        },
    };
    Exit::new(kind, cancellation)
}

fn recorded_kind(recorded: RecordedOutcome) -> ExitKind {
    match recorded {
        RecordedOutcome::Returned(Ok(())) => ExitKind::Completed,
        RecordedOutcome::Returned(Err(error)) => ExitKind::Failed(error),
        RecordedOutcome::Panicked { message } => ExitKind::Panicked { message },
        RecordedOutcome::ReadinessTimedOut { deadline } => ExitKind::ReadinessTimedOut { deadline },
        RecordedOutcome::Aborted { phase } => ExitKind::Aborted { phase },
    }
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

fn recorded_rank(recorded: &RecordedOutcome) -> OutcomeRank {
    match recorded {
        RecordedOutcome::Panicked { .. } => OutcomeRank::Panicked,
        RecordedOutcome::ReadinessTimedOut { .. } => OutcomeRank::ReadinessTimedOut,
        RecordedOutcome::Returned(Err(_)) => OutcomeRank::Failed,
        RecordedOutcome::Aborted { .. } => OutcomeRank::Aborted,
        RecordedOutcome::Returned(Ok(())) => OutcomeRank::Completed,
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

    use super::{
        Cancellation, ChildId, Exit, ExitError, ExitKind, GracePhase, IntensityTrip, JoinVerdict,
        RecordedOutcome, StartupError, StartupFailure, StartupFailureCause, classify_exit,
        reconcile_recorded_outcomes,
    };

    #[test]
    fn classification_precedence_is_table_driven() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let failure = ExitError::message("failed");
        let cases = [
            (
                "recorded completion",
                Some(RecordedOutcome::Returned(Ok(()))),
                JoinVerdict::Completed,
                Cancellation::NotObserved,
                Exit::new(ExitKind::Completed, Cancellation::NotObserved),
            ),
            (
                "cancellation overrides recorded completion",
                Some(RecordedOutcome::Returned(Ok(()))),
                JoinVerdict::Cancelled {
                    phase: GracePhase::AfterGrace,
                },
                Cancellation::Observed,
                Exit::new(
                    ExitKind::Aborted {
                        phase: GracePhase::AfterGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "recorded failure",
                Some(RecordedOutcome::Returned(Err(failure.clone()))),
                JoinVerdict::Completed,
                Cancellation::NotObserved,
                Exit::new(ExitKind::Failed(failure.clone()), Cancellation::NotObserved),
            ),
            (
                "late cancellation preserves recorded failure",
                Some(RecordedOutcome::Returned(Err(failure.clone()))),
                JoinVerdict::Cancelled {
                    phase: GracePhase::AfterGrace,
                },
                Cancellation::Observed,
                Exit::new(ExitKind::Failed(failure.clone()), Cancellation::Observed),
            ),
            (
                "join panic overrides recorded failure",
                Some(RecordedOutcome::Returned(Err(failure.clone()))),
                JoinVerdict::Panicked {
                    message: Some("drop panic".to_owned()),
                },
                Cancellation::NotObserved,
                Exit::new(
                    ExitKind::Panicked {
                        message: Some("drop panic".to_owned()),
                    },
                    Cancellation::NotObserved,
                ),
            ),
            (
                "readiness timeout overrides cancellation",
                Some(RecordedOutcome::ReadinessTimedOut { deadline }),
                JoinVerdict::Cancelled {
                    phase: GracePhase::AfterGrace,
                },
                Cancellation::Observed,
                Exit::new(
                    ExitKind::ReadinessTimedOut { deadline },
                    Cancellation::Observed,
                ),
            ),
            (
                "join panic overrides recorded completion",
                Some(RecordedOutcome::Returned(Ok(()))),
                JoinVerdict::Panicked {
                    message: Some("drop panic".to_owned()),
                },
                Cancellation::NotObserved,
                Exit::new(
                    ExitKind::Panicked {
                        message: Some("drop panic".to_owned()),
                    },
                    Cancellation::NotObserved,
                ),
            ),
            (
                "earlier recorded panic wins a tie",
                Some(RecordedOutcome::Panicked {
                    message: Some("callback panic".to_owned()),
                }),
                JoinVerdict::Panicked {
                    message: Some("drop panic".to_owned()),
                },
                Cancellation::Observed,
                Exit::new(
                    ExitKind::Panicked {
                        message: Some("callback panic".to_owned()),
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "earlier recorded abort wins a tie",
                Some(RecordedOutcome::Aborted {
                    phase: GracePhase::WithinGrace,
                }),
                JoinVerdict::Cancelled {
                    phase: GracePhase::AfterGrace,
                },
                Cancellation::Observed,
                Exit::new(
                    ExitKind::Aborted {
                        phase: GracePhase::WithinGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "join cancellation supplies a missing outcome",
                None,
                JoinVerdict::Cancelled {
                    phase: GracePhase::AfterGrace,
                },
                Cancellation::Observed,
                Exit::new(
                    ExitKind::Aborted {
                        phase: GracePhase::AfterGrace,
                    },
                    Cancellation::Observed,
                ),
            ),
            (
                "missing outcome fails closed",
                None,
                JoinVerdict::Completed,
                Cancellation::NotObserved,
                Exit::new(
                    ExitKind::Aborted {
                        phase: GracePhase::WithinGrace,
                    },
                    Cancellation::NotObserved,
                ),
            ),
        ];

        for (case, recorded, join, cancellation, expected) in cases {
            assert_eq!(
                classify_exit(recorded, join, cancellation),
                expected,
                "{case}"
            );
        }
    }

    #[test]
    fn failed_exit_equality_is_shared_provenance_not_content() {
        let error = ExitError::message("boom");
        let exit = Exit::new(ExitKind::Failed(error.clone()), Cancellation::NotObserved);

        // Clones of one error — including framework-published copies —
        // compare equal.
        assert_eq!(exit, exit.clone());
        assert_eq!(
            exit,
            Exit::new(ExitKind::Failed(error.clone()), Cancellation::NotObserved)
        );

        // Independently created errors with identical content do not.
        assert_ne!(
            exit,
            Exit::new(
                ExitKind::Failed(ExitError::message("boom")),
                Cancellation::NotObserved
            )
        );

        // Cancellation remains structural even with shared provenance.
        assert_ne!(
            exit,
            Exit::new(ExitKind::Failed(error), Cancellation::Observed)
        );
    }

    #[test]
    fn forced_outcomes_do_not_erase_stronger_recorded_evidence() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let failure = ExitError::from_startup_failure(StartupFailure {
            cause: StartupFailureCause::IdentityExhausted {
                id: ChildId::from("nested"),
            },
        });
        let cases = [
            (
                "application failure survives forced abort",
                Some(RecordedOutcome::Returned(Err(failure.clone()))),
                Some(RecordedOutcome::Aborted {
                    phase: GracePhase::AfterGrace,
                }),
                Exit::new(ExitKind::Failed(failure), Cancellation::Observed),
            ),
            (
                "forced readiness timeout overrides completion",
                Some(RecordedOutcome::Returned(Ok(()))),
                Some(RecordedOutcome::ReadinessTimedOut { deadline }),
                Exit::new(
                    ExitKind::ReadinessTimedOut { deadline },
                    Cancellation::Observed,
                ),
            ),
            (
                "earlier abort evidence wins a tie",
                Some(RecordedOutcome::Aborted {
                    phase: GracePhase::WithinGrace,
                }),
                Some(RecordedOutcome::Aborted {
                    phase: GracePhase::AfterGrace,
                }),
                Exit::new(
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
                    JoinVerdict::Cancelled {
                        phase: GracePhase::AfterGrace
                    },
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
        let error = ExitError::from_intensity_trip(trip.clone());
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
        let error = ExitError::from_startup_failure(failure.clone());
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
