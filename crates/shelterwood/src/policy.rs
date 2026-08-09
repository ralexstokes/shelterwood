//! Supervision policy and validated configuration data.

use std::{fmt, num::NonZeroUsize, time::Duration};

use crate::{Exit, identity::ChildId};

/// Whether a scope has fixed ordered membership or runtime-dynamic membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeFlavor {
    Ordered,
    Dynamic,
}

/// A one-origin backoff attempt that resets after a stable incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RestartAttempt(u64);

impl RestartAttempt {
    /// The state before a restart has been scheduled.
    pub const ZERO: Self = Self(0);

    /// Returns the next attempt, saturating at the numeric limit.
    #[must_use]
    pub const fn bump(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the numeric attempt value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Cumulative scheduled-restart charges for one child membership.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RestartCount(u64);

impl RestartCount {
    /// A membership with no scheduled restarts.
    pub const ZERO: Self = Self(0);

    /// Returns the next count, saturating at the numeric limit.
    #[must_use]
    pub const fn bump(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the numeric restart count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Cumulative restart charges across one scope incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TotalRestarts(u64);

impl TotalRestarts {
    /// A scope incarnation with no restart charges.
    pub const ZERO: Self = Self(0);

    /// Returns the next total, saturating at the numeric limit.
    #[must_use]
    pub const fn bump(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the numeric restart total.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Whether a child construction can be recreated after its first incarnation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChildMode {
    Restartable,
    OneShot,
}

/// The default bounded FIFO mailbox capacity.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 64;
/// The default child shutdown grace.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// The default gated-readiness deadline.
pub const DEFAULT_READINESS_DEADLINE: Duration = Duration::from_secs(30);

/// The condition under which an exited child is restarted.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RestartCondition {
    /// Restart after every exit, including successful completion.
    Always,
    /// Restart only after a failure exit.
    OnFailure,
    /// Never restart.
    Never,
}

/// Whether equal jitter is applied to a backoff delay.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Jitter {
    /// Use the derived delay exactly.
    None,
    /// Select uniformly from the upper half of the derived delay.
    Equal,
}

/// A jitter sample clamped to the half-open interval `[0, 1)`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct JitterSample(f64);

impl JitterSample {
    /// Clamps a sample into `[0, 1)`; non-finite values become zero.
    #[must_use]
    pub fn new(value: f64) -> Self {
        let value = if value.is_finite() {
            value.clamp(0.0, 1.0 - f64::EPSILON)
        } else {
            0.0
        };
        Self(value)
    }

    /// Normalizes an integer ratio and clamps it into `[0, 1)`.
    #[must_use]
    pub fn from_u64_ratio(numerator: u64, denominator: u64) -> Self {
        Self::new(numerator as f64 / denominator as f64)
    }
}

/// A validated exponential-backoff multiplier.
#[derive(Clone, Copy)]
pub struct BackoffFactor(f64);

impl BackoffFactor {
    /// Validates and constructs a factor.
    pub fn new(value: f64) -> Result<Self, PolicyError> {
        if value.is_finite() && value >= 1.0 {
            Ok(Self(value))
        } else {
            Err(PolicyError::InvalidBackoffFactor)
        }
    }

    fn get(self) -> f64 {
        self.0
    }

    fn validate(self) -> Result<(), PolicyError> {
        if self.0.is_finite() && self.0 >= 1.0 {
            Ok(())
        } else {
            Err(PolicyError::InvalidBackoffFactor)
        }
    }
}

impl fmt::Debug for BackoffFactor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl PartialEq for BackoffFactor {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for BackoffFactor {}

impl std::hash::Hash for BackoffFactor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// Delay policy for a scheduled restart.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Backoff {
    /// Restart without a delay.
    Immediate,
    /// Use a fixed non-zero delay.
    Fixed {
        /// The fixed delay.
        delay: Duration,
        /// Optional equal jitter.
        jitter: Jitter,
    },
    /// Increase the delay exponentially, clamped to `max`.
    Exponential {
        /// The first delay.
        base: Duration,
        /// The per-attempt multiplier.
        factor: BackoffFactor,
        /// The maximum derived delay.
        max: Duration,
        /// Optional equal jitter.
        jitter: Jitter,
    },
}

impl Backoff {
    /// Constructs a validated fixed backoff.
    pub fn fixed(delay: Duration, jitter: Jitter) -> Result<Self, PolicyError> {
        if delay.is_zero() {
            Err(PolicyError::ZeroDuration)
        } else {
            Ok(Self::Fixed { delay, jitter })
        }
    }

    /// Constructs a validated exponential backoff.
    pub fn exponential(
        base: Duration,
        factor: BackoffFactor,
        max: Duration,
        jitter: Jitter,
    ) -> Result<Self, PolicyError> {
        if base.is_zero() || max.is_zero() {
            return Err(PolicyError::ZeroDuration);
        }
        if max < base {
            return Err(PolicyError::BackoffMaximumBeforeBase);
        }
        Ok(Self::Exponential {
            base,
            factor,
            max,
            jitter,
        })
    }

    /// Derives the delay for a one-origin restart attempt.
    ///
    /// Randomness is deliberately supplied as external, range-checked data.
    #[must_use]
    pub fn next_delay(self, attempt: RestartAttempt, jitter_sample: JitterSample) -> Duration {
        let attempt = attempt.get().max(1);
        let (delay, jitter, maximum) = match self {
            Self::Immediate => return Duration::ZERO,
            Self::Fixed { delay, jitter } => (delay, jitter, delay),
            Self::Exponential {
                base,
                factor,
                max,
                jitter,
            } => {
                let delay = if attempt == 1 || factor.get() == 1.0 {
                    // The multiplier is exactly one, so the product is the
                    // base itself. Skipping the float round-trip keeps the
                    // delay exact above 2^53 nanoseconds.
                    base
                } else {
                    let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
                    let nanos = duration_nanos(base) * factor.get().powi(exponent);
                    if nanos < duration_nanos(max) {
                        duration_from_nanos(nanos)
                    } else {
                        // At or above the cap, saturate to the exact
                        // configured maximum rather than to a float
                        // round-trip of it.
                        max
                    }
                };
                (delay.min(max), jitter, max)
            }
        };
        let delay = match jitter {
            Jitter::None => delay,
            Jitter::Equal => {
                let sample = jitter_sample.0;
                if sample == 0.0 {
                    // The lower jitter edge is exactly half the delay,
                    // rounded to the nearest nanosecond without a float
                    // round-trip.
                    half_duration(delay)
                } else {
                    duration_from_nanos(duration_nanos(delay) * (0.5 + sample * 0.5))
                }
            }
        };
        // Floating-point duration conversion can round an extreme value a
        // few nanoseconds above the configured cap. Reapply the policy bound
        // with exact Duration ordering after every conversion and jitter.
        delay.min(maximum)
    }

    fn validate(self) -> Result<(), PolicyError> {
        match self {
            Self::Immediate => Ok(()),
            Self::Fixed { delay, .. } if delay.is_zero() => Err(PolicyError::ZeroDuration),
            Self::Fixed { .. } => Ok(()),
            Self::Exponential {
                base, factor, max, ..
            } => {
                if base.is_zero() || max.is_zero() {
                    return Err(PolicyError::ZeroDuration);
                }
                factor.validate()?;
                if max < base {
                    return Err(PolicyError::BackoffMaximumBeforeBase);
                }
                Ok(())
            }
        }
    }
}

fn duration_nanos(duration: Duration) -> f64 {
    duration.as_nanos() as f64
}

/// Halves a delay exactly, rounding half a nanosecond up — the same
/// direction the float path rounds an exact `.5` remainder.
fn half_duration(duration: Duration) -> Duration {
    let nanos = duration.as_nanos().div_ceil(2);
    let seconds =
        u64::try_from(nanos / 1_000_000_000).expect("a halved delay fits the duration range");
    let subsecond =
        u32::try_from(nanos % 1_000_000_000).expect("nanosecond remainder is below one billion");
    Duration::new(seconds, subsecond)
}

fn duration_from_nanos(nanos: f64) -> Duration {
    let rounded = nanos.round();
    if !rounded.is_finite() || rounded >= Duration::MAX.as_nanos() as f64 {
        Duration::MAX
    } else {
        let nanos = rounded.max(0.0) as u128;
        let seconds = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
        let subsecond = u32::try_from(nanos % 1_000_000_000)
            .expect("nanosecond remainder is below one billion");
        Duration::new(seconds, subsecond)
    }
}

/// Restart condition and delay policy for one child.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RestartPolicy {
    condition: RestartCondition,
    backoff: Backoff,
}

impl RestartPolicy {
    /// Constructs a restart policy.
    #[must_use]
    pub const fn new(condition: RestartCondition, backoff: Backoff) -> Self {
        Self { condition, backoff }
    }

    /// Returns the configured condition.
    #[must_use]
    pub const fn condition(self) -> RestartCondition {
        self.condition
    }

    /// Returns the configured delay policy.
    #[must_use]
    pub const fn backoff(self) -> Backoff {
        self.backoff
    }

    /// Reports whether this policy can never restart.
    #[must_use]
    pub const fn is_never(self) -> bool {
        matches!(self.condition, RestartCondition::Never)
    }

    fn validate(self) -> Result<(), PolicyError> {
        self.backoff.validate()
    }

    pub(crate) fn should_restart(self, exit: &Exit) -> bool {
        match self.condition {
            RestartCondition::Always => true,
            RestartCondition::OnFailure => exit.is_failure(),
            RestartCondition::Never => false,
        }
    }
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::new(RestartCondition::OnFailure, Backoff::Immediate)
    }
}

/// Per-child shutdown behavior.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Shutdown {
    /// Request cooperative shutdown for up to the supplied grace.
    Graceful {
        /// Cooperative grace.
        grace: Duration,
    },
    /// Escalate immediately after cancellation.
    Abort,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::Graceful {
            grace: DEFAULT_SHUTDOWN_GRACE,
        }
    }
}

/// A child's readiness mode.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Readiness {
    /// Ready as soon as the incarnation starts.
    Immediate,
    /// Ready after handler initialization completes.
    AfterInit,
    /// Ready only after an explicit context signal.
    Manual,
}

/// Resolution state for a gated-readiness deadline.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum ReadinessDeadline {
    /// Resolve from the nearest scope default.
    #[default]
    Inherit,
    /// Apply a non-zero bound.
    Bounded(Duration),
    /// Wait without a deadline.
    Unbounded,
}

impl ReadinessDeadline {
    /// Constructs a validated bounded deadline.
    pub fn bounded(duration: Duration) -> Result<Self, PolicyError> {
        if duration.is_zero() {
            Err(PolicyError::ZeroDuration)
        } else {
            Ok(Self::Bounded(duration))
        }
    }

    fn validate_declared(self) -> Result<(), PolicyError> {
        match self {
            Self::Bounded(duration) if duration.is_zero() => Err(PolicyError::ZeroDuration),
            Self::Inherit | Self::Bounded(_) | Self::Unbounded => Ok(()),
        }
    }

    fn validate_resolved(self) -> Result<(), PolicyError> {
        match self {
            Self::Inherit => Err(PolicyError::UnresolvedReadinessDeadline),
            value => value.validate_declared(),
        }
    }
}

/// The scope-wide restart budget.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Intensity {
    /// Maximum restart charges allowed inside the rolling window.
    pub max_restarts: u64,
    /// The rolling-window duration.
    pub within: Duration,
}

impl Intensity {
    /// Constructs a validated intensity budget.
    pub fn new(max_restarts: u64, within: Duration) -> Result<Self, PolicyError> {
        if within.is_zero() {
            Err(PolicyError::ZeroDuration)
        } else {
            Ok(Self {
                max_restarts,
                within,
            })
        }
    }

    pub(crate) fn validate(self) -> Result<(), PolicyError> {
        if self.within.is_zero() {
            Err(PolicyError::ZeroDuration)
        } else {
            Ok(())
        }
    }
}

impl Default for Intensity {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            within: Duration::from_secs(30),
        }
    }
}

/// Core mailbox declarations carried as L1 policy data.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Mailbox {
    /// Bounded FIFO; `None` defers capacity by matching mailbox kind.
    Queue(Option<NonZeroUsize>),
    /// A single latest-value slot.
    Latest,
}

impl Mailbox {
    /// Constructs a FIFO mailbox with an explicit capacity.
    pub fn queue(capacity: usize) -> Result<Self, PolicyError> {
        NonZeroUsize::new(capacity)
            .map(|capacity| Self::Queue(Some(capacity)))
            .ok_or(PolicyError::ZeroCapacity)
    }

    /// Constructs a FIFO mailbox whose capacity is inherited by kind.
    #[must_use]
    pub const fn queue_inherit() -> Self {
        Self::Queue(None)
    }

    /// Constructs a conflating latest-value mailbox.
    #[must_use]
    pub const fn latest() -> Self {
        Self::Latest
    }
}

impl Default for Mailbox {
    fn default() -> Self {
        Self::Queue(NonZeroUsize::new(DEFAULT_MAILBOX_CAPACITY))
    }
}

/// Fate of the frozen accepted mailbox prefix during shutdown.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum MailboxShutdown {
    /// Deliver the frozen accepted prefix before stopping.
    #[default]
    Drain,
    /// Drop the frozen accepted prefix.
    Discard,
}

/// Whether a terminal child remains resident as a tombstone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Retention {
    /// Retain the terminal membership until explicit removal or scope exit.
    Retain,
    /// Prune the membership immediately after terminalization.
    Remove,
}

/// Ordered-scope fate-sharing strategy.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum Strategy {
    /// Restart only the exiting child.
    #[default]
    OneForOne,
}

/// Scope defaults inherited by child declarations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScopeDefaults {
    /// Default child restart policy.
    pub child_restart: Option<RestartPolicy>,
    /// Default child shutdown policy.
    pub child_shutdown: Option<Shutdown>,
    /// Default actor mailbox kind and capacity.
    pub mailbox: Option<Mailbox>,
    /// Default mailbox shutdown behavior.
    pub mailbox_shutdown: Option<MailboxShutdown>,
    /// Default readiness deadline.
    pub readiness_deadline: Option<ReadinessDeadline>,
}

/// How a subtree edge treats inherited scope defaults.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum DefaultsInheritance {
    /// Continue resolving unset fields through the parent.
    #[default]
    Inherit,
    /// Resolve unset fields directly from library defaults.
    Reset,
}

/// A policy-construction error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum PolicyError {
    /// A required duration was zero.
    #[error("duration must be non-zero")]
    ZeroDuration,
    /// A mailbox capacity was zero.
    #[error("mailbox capacity must be non-zero")]
    ZeroCapacity,
    /// An exponential factor was non-finite or below one.
    #[error("backoff factor must be finite and at least 1.0")]
    InvalidBackoffFactor,
    /// An exponential maximum preceded its base delay.
    #[error("backoff maximum must not be shorter than its base")]
    BackoffMaximumBeforeBase,
    /// An inherited readiness deadline remained unresolved at execution.
    #[error("readiness deadline must be resolved before execution")]
    UnresolvedReadinessDeadline,
    /// A readiness mode is not meaningful for this child kind.
    #[error("readiness mode is not supported by this child kind")]
    UnsupportedReadiness,
}

/// Policy field rejected at a trusted lowering or admission boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PolicyField {
    /// A scope restart-intensity window.
    Intensity,
    /// A default or child restart backoff.
    RestartBackoff,
    /// A default or child readiness deadline.
    ReadinessDeadline,
}

impl fmt::Display for PolicyField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Intensity => formatter.write_str("restart intensity"),
            Self::RestartBackoff => formatter.write_str("restart backoff"),
            Self::ReadinessDeadline => formatter.write_str("readiness deadline"),
        }
    }
}

/// Structured evidence for a policy value rejected during lowering.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid {field} at child path {path:?}: {error}")]
#[non_exhaustive]
pub struct InvalidPolicy {
    /// Child-id path relative to the scope being lowered; empty for scope policy.
    pub path: Vec<ChildId>,
    /// Policy field whose public representation bypassed validation.
    pub field: PolicyField,
    /// Exact validation failure.
    pub error: PolicyError,
}

impl InvalidPolicy {
    pub(crate) fn new(field: PolicyField, error: PolicyError) -> Self {
        Self {
            path: Vec::new(),
            field,
            error,
        }
    }

    pub(crate) fn prepend(mut self, id: &ChildId) -> Self {
        self.path.insert(0, id.clone());
        self
    }
}

pub(crate) fn tidy_abort_beat(grace: Duration) -> Duration {
    (grace / 10).clamp(Duration::from_millis(1), Duration::from_millis(10))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CommonOptions {
    pub(crate) restart: Option<RestartPolicy>,
    pub(crate) shutdown: Option<Shutdown>,
    pub(crate) mailbox: Option<Mailbox>,
    pub(crate) mailbox_shutdown: Option<MailboxShutdown>,
    pub(crate) readiness: Option<Readiness>,
    pub(crate) readiness_deadline: ReadinessDeadline,
    pub(crate) retention: Option<Retention>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedDefaults {
    pub(crate) child_restart: RestartPolicy,
    pub(crate) child_shutdown: Shutdown,
    pub(crate) mailbox: Mailbox,
    pub(crate) mailbox_shutdown: MailboxShutdown,
    pub(crate) readiness_deadline: ReadinessDeadline,
}

impl Default for ResolvedDefaults {
    fn default() -> Self {
        Self {
            child_restart: RestartPolicy::default(),
            child_shutdown: Shutdown::default(),
            mailbox: Mailbox::default(),
            mailbox_shutdown: MailboxShutdown::default(),
            readiness_deadline: ReadinessDeadline::Bounded(DEFAULT_READINESS_DEADLINE),
        }
    }
}

impl ResolvedDefaults {
    pub(crate) fn overlay(&self, values: &ScopeDefaults) -> Result<Self, InvalidPolicy> {
        self.validate()?;
        if let Some(restart) = values.child_restart {
            restart
                .validate()
                .map_err(|error| InvalidPolicy::new(PolicyField::RestartBackoff, error))?;
        }
        if let Some(deadline) = values.readiness_deadline {
            deadline
                .validate_declared()
                .map_err(|error| InvalidPolicy::new(PolicyField::ReadinessDeadline, error))?;
        }
        let resolved = Self {
            child_restart: values.child_restart.unwrap_or(self.child_restart),
            child_shutdown: values.child_shutdown.unwrap_or(self.child_shutdown),
            mailbox: resolve_default_mailbox(values.mailbox, self.mailbox),
            mailbox_shutdown: values.mailbox_shutdown.unwrap_or(self.mailbox_shutdown),
            readiness_deadline: match values.readiness_deadline.unwrap_or(self.readiness_deadline) {
                ReadinessDeadline::Inherit => self.readiness_deadline,
                value => value,
            },
        };
        resolved.validate()?;
        Ok(resolved)
    }

    pub(crate) fn validate(&self) -> Result<(), InvalidPolicy> {
        self.child_restart
            .validate()
            .map_err(|error| InvalidPolicy::new(PolicyField::RestartBackoff, error))?;
        self.readiness_deadline
            .validate_resolved()
            .map_err(|error| InvalidPolicy::new(PolicyField::ReadinessDeadline, error))
    }
}

fn resolve_default_mailbox(value: Option<Mailbox>, inherited: Mailbox) -> Mailbox {
    match value {
        None => inherited,
        Some(Mailbox::Queue(None)) => match inherited {
            Mailbox::Queue(Some(capacity)) => Mailbox::Queue(Some(capacity)),
            Mailbox::Queue(None) | Mailbox::Latest => Mailbox::default(),
        },
        Some(value) => value,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedCommonOptions {
    pub(crate) restart: RestartPolicy,
    pub(crate) shutdown: Shutdown,
    pub(crate) mailbox: Mailbox,
    pub(crate) mailbox_shutdown: MailboxShutdown,
    pub(crate) readiness: Readiness,
    pub(crate) readiness_override: Option<Readiness>,
    pub(crate) readiness_deadline: ReadinessDeadline,
    pub(crate) retention: Retention,
}

pub(crate) fn resolve_common(
    options: &CommonOptions,
    defaults: &ResolvedDefaults,
    mode: ChildMode,
    default_readiness: Readiness,
) -> Result<ResolvedCommonOptions, InvalidPolicy> {
    defaults.validate()?;
    if let Some(restart) = options.restart {
        restart
            .validate()
            .map_err(|error| InvalidPolicy::new(PolicyField::RestartBackoff, error))?;
    }
    options
        .readiness_deadline
        .validate_declared()
        .map_err(|error| InvalidPolicy::new(PolicyField::ReadinessDeadline, error))?;
    let readiness_deadline = match options.readiness_deadline {
        ReadinessDeadline::Inherit => defaults.readiness_deadline,
        value => value,
    };
    let resolved = ResolvedCommonOptions {
        restart: if mode == ChildMode::OneShot {
            RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
        } else {
            options.restart.unwrap_or(defaults.child_restart)
        },
        shutdown: options.shutdown.unwrap_or(defaults.child_shutdown),
        mailbox: resolve_default_mailbox(options.mailbox, defaults.mailbox),
        mailbox_shutdown: options
            .mailbox_shutdown
            .unwrap_or(defaults.mailbox_shutdown),
        readiness: options.readiness.unwrap_or(default_readiness),
        readiness_override: options.readiness,
        readiness_deadline,
        retention: options.retention.unwrap_or(if mode == ChildMode::OneShot {
            Retention::Remove
        } else {
            Retention::Retain
        }),
    };
    resolved
        .restart
        .validate()
        .map_err(|error| InvalidPolicy::new(PolicyField::RestartBackoff, error))?;
    resolved
        .readiness_deadline
        .validate_resolved()
        .map_err(|error| InvalidPolicy::new(PolicyField::ReadinessDeadline, error))?;
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use super::{
        Backoff, BackoffFactor, Intensity, InvalidPolicy, Jitter, JitterSample, Mailbox,
        PolicyError, PolicyField, ReadinessDeadline, ResolvedDefaults, RestartAttempt,
        RestartCondition, RestartCount, RestartPolicy, ScopeDefaults, Shutdown, TotalRestarts,
        tidy_abort_beat,
    };

    #[test]
    fn restart_counters_saturate_at_the_numeric_limit() {
        let attempt = RestartAttempt(u64::MAX);
        let count = RestartCount(u64::MAX);
        let total = TotalRestarts(u64::MAX);

        assert_eq!(attempt.bump(), attempt);
        assert_eq!(count.bump(), count);
        assert_eq!(total.bump(), total);
    }

    #[test]
    fn shipped_defaults_match_the_normative_policy() {
        assert_eq!(Mailbox::default(), Mailbox::Queue(NonZeroUsize::new(64)));
        assert_eq!(
            Shutdown::default(),
            Shutdown::Graceful {
                grace: Duration::from_secs(5),
            }
        );
        assert_eq!(
            ResolvedDefaults::default().readiness_deadline,
            ReadinessDeadline::Bounded(Duration::from_secs(30))
        );
        assert_eq!(
            Intensity::default(),
            Intensity {
                max_restarts: 5,
                within: Duration::from_secs(30),
            }
        );

        assert_eq!(tidy_abort_beat(Duration::ZERO), Duration::from_millis(1));
        assert_eq!(
            tidy_abort_beat(Duration::from_millis(50)),
            Duration::from_millis(5)
        );
        assert_eq!(
            tidy_abort_beat(Duration::from_millis(100)),
            Duration::from_millis(10)
        );
        assert_eq!(
            tidy_abort_beat(Duration::from_secs(5)),
            Duration::from_millis(10)
        );
    }

    #[test]
    fn backoff_progression_and_jitter_are_pure_math() {
        let backoff = Backoff::exponential(
            Duration::from_millis(10),
            BackoffFactor::new(2.0).expect("valid factor"),
            Duration::from_millis(35),
            Jitter::None,
        )
        .expect("valid backoff");
        assert_eq!(
            backoff.next_delay(RestartAttempt(1), JitterSample::new(0.9)),
            Duration::from_millis(10)
        );
        assert_eq!(
            backoff.next_delay(RestartAttempt(2), JitterSample::new(0.1)),
            Duration::from_millis(20)
        );
        assert_eq!(
            backoff.next_delay(RestartAttempt(3), JitterSample::new(0.5)),
            Duration::from_millis(35)
        );
        assert_eq!(
            backoff.next_delay(RestartAttempt(99), JitterSample::new(0.5)),
            Duration::from_millis(35)
        );

        let jittered =
            Backoff::fixed(Duration::from_millis(10), Jitter::Equal).expect("valid backoff");
        assert_eq!(
            jittered.next_delay(RestartAttempt(1), JitterSample::new(0.0)),
            Duration::from_millis(5)
        );
        assert_eq!(
            jittered.next_delay(RestartAttempt(1), JitterSample::new(0.5)),
            Duration::from_micros(7_500)
        );
    }

    #[test]
    fn jitter_samples_own_the_half_open_unit_interval() {
        assert_eq!(JitterSample::new(-1.0).0, 0.0);
        assert_eq!(JitterSample::new(f64::NAN).0, 0.0);
        assert_eq!(JitterSample::new(f64::INFINITY).0, 0.0);
        assert_eq!(JitterSample::new(1.0).0, 1.0 - f64::EPSILON);
        assert_eq!(JitterSample::from_u64_ratio(0, u64::MAX).0, 0.0);
        assert_eq!(
            JitterSample::from_u64_ratio(u64::MAX, u64::MAX).0,
            1.0 - f64::EPSILON
        );
    }

    #[test]
    fn extreme_backoffs_never_round_above_their_exact_maximum() {
        let maximum = Duration::MAX - Duration::from_nanos(1);
        for jitter in [Jitter::None, Jitter::Equal] {
            let backoff = Backoff::exponential(
                Duration::from_nanos(1),
                BackoffFactor::new(f64::MAX).expect("finite factor"),
                maximum,
                jitter,
            )
            .expect("valid extreme backoff");
            for sample in [
                f64::NEG_INFINITY,
                -1.0,
                0.0,
                1.0 - f64::EPSILON,
                1.0,
                f64::INFINITY,
                f64::NAN,
            ] {
                assert!(
                    backoff.next_delay(RestartAttempt(u64::MAX), JitterSample::new(sample))
                        <= maximum
                );
            }
        }

        let fixed = Backoff::fixed(maximum, Jitter::Equal).expect("valid fixed backoff");
        assert!(fixed.next_delay(RestartAttempt(u64::MAX), JitterSample::new(1.0)) <= maximum);
    }

    #[test]
    fn invalid_policy_data_is_rejected_eagerly() {
        assert_eq!(
            Backoff::fixed(Duration::ZERO, Jitter::None),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(Mailbox::queue(0), Err(PolicyError::ZeroCapacity));
        assert_eq!(
            ReadinessDeadline::bounded(Duration::ZERO),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(
            Intensity::new(1, Duration::ZERO),
            Err(PolicyError::ZeroDuration)
        );
        let factor = BackoffFactor::new(2.0).expect("valid factor");
        assert_eq!(
            Backoff::exponential(Duration::ZERO, factor, Duration::from_secs(1), Jitter::None),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(
            Backoff::exponential(Duration::from_secs(1), factor, Duration::ZERO, Jitter::None),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(
            Backoff::exponential(
                Duration::from_secs(2),
                factor,
                Duration::from_secs(1),
                Jitter::None,
            ),
            Err(PolicyError::BackoffMaximumBeforeBase)
        );
        let just_below_one = f64::from_bits(1.0_f64.to_bits() - 1);
        for invalid in [
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            just_below_one,
            0.5,
            0.0,
            -1.0,
        ] {
            assert_eq!(
                BackoffFactor::new(invalid),
                Err(PolicyError::InvalidBackoffFactor),
                "factor {invalid} must be rejected"
            );
        }
        assert!(BackoffFactor::new(1.0).is_ok());
        assert!(BackoffFactor::new(f64::MAX).is_ok());
        assert!(Mailbox::queue(1).is_ok());
        assert!(ReadinessDeadline::bounded(Duration::from_nanos(1)).is_ok());
        assert!(Intensity::new(0, Duration::from_nanos(1)).is_ok());
        assert!(Backoff::fixed(Duration::from_nanos(1), Jitter::Equal).is_ok());
        assert!(
            Backoff::exponential(
                Duration::from_nanos(1),
                BackoffFactor::new(1.0).expect("valid factor"),
                Duration::from_nanos(1),
                Jitter::Equal,
            )
            .is_ok(),
            "an exponential maximum equal to its base is valid"
        );
    }

    #[test]
    fn unit_multiplier_backoff_delays_are_exact_beyond_float_precision() {
        // 2^60 + 1 whole nanoseconds cannot survive an f64 round-trip.
        let base = Duration::from_nanos((1 << 60) + 1);
        let doubling = Backoff::exponential(
            base,
            BackoffFactor::new(2.0).expect("valid factor"),
            Duration::MAX,
            Jitter::None,
        )
        .expect("valid backoff");
        assert_eq!(
            doubling.next_delay(RestartAttempt(1), JitterSample::new(0.9)),
            base
        );

        let flat = Backoff::exponential(
            base,
            BackoffFactor::new(1.0).expect("valid factor"),
            Duration::MAX,
            Jitter::None,
        )
        .expect("valid backoff");
        assert_eq!(
            flat.next_delay(RestartAttempt(1), JitterSample::new(0.0)),
            base
        );
        assert_eq!(
            flat.next_delay(RestartAttempt(u64::MAX), JitterSample::new(0.0)),
            base
        );

        let fixed = Backoff::fixed(base, Jitter::None).expect("valid backoff");
        assert_eq!(
            fixed.next_delay(RestartAttempt(u64::MAX), JitterSample::new(0.99)),
            base
        );

        let huge_fixed = Backoff::fixed(Duration::MAX, Jitter::None).expect("valid backoff");
        assert_eq!(
            huge_fixed.next_delay(RestartAttempt(u64::MAX), JitterSample::new(0.5)),
            Duration::MAX
        );
    }

    #[test]
    fn saturated_backoff_delays_land_exactly_on_the_configured_maximum() {
        // One nanosecond below `Duration::MAX` does not survive an f64
        // round-trip, so an exact saturation is observable.
        let max = Duration::MAX - Duration::from_nanos(1);
        let explosive = Backoff::exponential(
            Duration::from_nanos(1),
            BackoffFactor::new(f64::MAX).expect("finite factor"),
            max,
            Jitter::None,
        )
        .expect("valid backoff");
        assert_eq!(
            explosive.next_delay(RestartAttempt(2), JitterSample::new(0.0)),
            max
        );
        assert_eq!(
            explosive.next_delay(RestartAttempt(u64::MAX), JitterSample::new(0.0)),
            max
        );

        let doubling = Backoff::exponential(
            Duration::from_secs(1),
            BackoffFactor::new(2.0).expect("valid factor"),
            max,
            Jitter::None,
        )
        .expect("valid backoff");
        assert_eq!(
            doubling.next_delay(RestartAttempt(200), JitterSample::new(0.0)),
            max
        );
    }

    #[test]
    fn large_exponent_backoff_is_monotone_and_saturates_at_the_maximum() {
        // Pins the §9.2 amendment (SPEC §4.6): in the large-exponent regime
        // the derived delay is monotone nondecreasing in the attempt and
        // plateaus exactly at the configured maximum, including across the
        // `i32::MAX` exponent clamp and out to `RestartAttempt(u64::MAX)`.
        let max = Duration::from_secs(3_600);
        let backoff = Backoff::exponential(
            Duration::from_millis(1),
            BackoffFactor::new(1.5).expect("valid factor"),
            max,
            Jitter::None,
        )
        .expect("valid backoff");
        let clamp = u64::try_from(i32::MAX).expect("i32::MAX fits in u64");
        let attempts = [
            1,
            2,
            16,
            64,
            256,
            clamp - 1,
            clamp,
            clamp + 1,
            clamp + 2,
            u64::MAX - 1,
            u64::MAX,
        ];
        let mut previous = Duration::ZERO;
        for attempt in attempts {
            let delay = backoff.next_delay(RestartAttempt(attempt), JitterSample::new(0.0));
            assert!(
                delay >= previous,
                "attempt {attempt}: delay {delay:?} regressed below {previous:?}"
            );
            assert!(
                delay <= max,
                "attempt {attempt}: delay {delay:?} exceeds the maximum"
            );
            previous = delay;
        }
        for attempt in [clamp - 1, clamp, clamp + 1, u64::MAX - 1, u64::MAX] {
            assert_eq!(
                backoff.next_delay(RestartAttempt(attempt), JitterSample::new(0.0)),
                max,
                "attempt {attempt}: the astronomical-attempt plateau is the exact maximum"
            );
        }
    }

    #[test]
    fn zero_sample_equal_jitter_is_the_exact_half_delay() {
        // Half of an odd nanosecond count rounds up by exactly half a
        // nanosecond; the float path would land one nanosecond short.
        let odd = Duration::from_nanos((1 << 60) + 1);
        let fixed = Backoff::fixed(odd, Jitter::Equal).expect("valid backoff");
        let exact_half = Duration::from_nanos((1 << 59) + 1);
        assert_eq!(
            fixed.next_delay(RestartAttempt(1), JitterSample::new(0.0)),
            exact_half
        );
        // Non-finite and negative samples clamp to the same exact edge.
        for sample in [f64::NAN, f64::NEG_INFINITY, -3.0] {
            assert_eq!(
                fixed.next_delay(RestartAttempt(1), JitterSample::new(sample)),
                exact_half
            );
        }

        let even =
            Backoff::fixed(Duration::from_nanos(1 << 60), Jitter::Equal).expect("valid backoff");
        assert_eq!(
            even.next_delay(RestartAttempt(1), JitterSample::new(0.0)),
            Duration::from_nanos(1 << 59)
        );
    }

    #[test]
    fn restart_policy_is_never_matches_only_the_never_condition() {
        assert!(RestartPolicy::new(RestartCondition::Never, Backoff::Immediate).is_never());
        assert!(!RestartPolicy::new(RestartCondition::OnFailure, Backoff::Immediate).is_never());
        assert!(!RestartPolicy::new(RestartCondition::Always, Backoff::Immediate).is_never());
    }

    #[test]
    fn defaults_overlay_as_one_value_and_mailbox_capacity_resolves_by_kind() {
        let library = ResolvedDefaults::default();
        let latest = library
            .overlay(&ScopeDefaults {
                mailbox: Some(Mailbox::latest()),
                ..ScopeDefaults::default()
            })
            .expect("valid defaults");
        assert_eq!(latest.mailbox, Mailbox::Latest);

        let deferred_queue = latest
            .overlay(&ScopeDefaults {
                mailbox: Some(Mailbox::queue_inherit()),
                ..ScopeDefaults::default()
            })
            .expect("valid defaults");
        assert_eq!(deferred_queue.mailbox, Mailbox::default());
        assert_eq!(deferred_queue.child_restart, latest.child_restart);
        assert_eq!(deferred_queue.child_shutdown, latest.child_shutdown);
    }

    #[test]
    fn public_literal_representations_are_revalidated() {
        assert_eq!(
            Backoff::Fixed {
                delay: Duration::ZERO,
                jitter: Jitter::None,
            }
            .validate(),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(
            Backoff::Exponential {
                base: Duration::ZERO,
                factor: BackoffFactor(2.0),
                max: Duration::from_secs(1),
                jitter: Jitter::None,
            }
            .validate(),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(
            Backoff::Exponential {
                base: Duration::from_secs(1),
                factor: BackoffFactor(f64::NAN),
                max: Duration::from_secs(2),
                jitter: Jitter::None,
            }
            .validate(),
            Err(PolicyError::InvalidBackoffFactor)
        );
        assert_eq!(
            Backoff::Exponential {
                base: Duration::from_secs(2),
                factor: BackoffFactor(2.0),
                max: Duration::from_secs(1),
                jitter: Jitter::None,
            }
            .validate(),
            Err(PolicyError::BackoffMaximumBeforeBase)
        );
        assert_eq!(
            ReadinessDeadline::Bounded(Duration::ZERO).validate_declared(),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(
            Intensity {
                max_restarts: 0,
                within: Duration::ZERO,
            }
            .validate(),
            Err(PolicyError::ZeroDuration)
        );
    }

    #[test]
    fn inherited_and_resolved_policy_values_are_revalidated() {
        let invalid_restart = ResolvedDefaults {
            child_restart: RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Fixed {
                    delay: Duration::ZERO,
                    jitter: Jitter::None,
                },
            ),
            ..ResolvedDefaults::default()
        };
        assert_eq!(
            invalid_restart.overlay(&ScopeDefaults {
                child_restart: Some(RestartPolicy::default()),
                ..ScopeDefaults::default()
            }),
            Err(InvalidPolicy::new(
                PolicyField::RestartBackoff,
                PolicyError::ZeroDuration
            ))
        );

        let unresolved = ResolvedDefaults {
            readiness_deadline: ReadinessDeadline::Inherit,
            ..ResolvedDefaults::default()
        };
        assert_eq!(
            unresolved.overlay(&ScopeDefaults::default()),
            Err(InvalidPolicy::new(
                PolicyField::ReadinessDeadline,
                PolicyError::UnresolvedReadinessDeadline
            ))
        );
    }

    #[test]
    fn minimum_valid_literal_edges_are_accepted() {
        assert_eq!(
            Backoff::Fixed {
                delay: Duration::from_nanos(1),
                jitter: Jitter::None,
            }
            .validate(),
            Ok(())
        );
        assert_eq!(
            Backoff::Exponential {
                base: Duration::from_nanos(1),
                factor: BackoffFactor(1.0),
                max: Duration::from_nanos(1),
                jitter: Jitter::Equal,
            }
            .validate(),
            Ok(())
        );
        assert_eq!(
            ReadinessDeadline::Bounded(Duration::from_nanos(1)).validate_resolved(),
            Ok(())
        );
        assert_eq!(
            Intensity {
                max_restarts: 0,
                within: Duration::from_nanos(1),
            }
            .validate(),
            Ok(())
        );
    }
}
