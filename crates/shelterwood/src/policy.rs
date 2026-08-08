//! Supervision policy and validated configuration data.

use std::{fmt, num::NonZeroUsize, time::Duration};

/// Whether a scope has fixed ordered membership or runtime-dynamic membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeFlavor {
    Ordered,
    Dynamic,
}

/// The default bounded FIFO mailbox capacity.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 64;
/// The default child shutdown grace.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// The default gated-readiness deadline.
pub const DEFAULT_READINESS_DEADLINE: Duration = Duration::from_secs(30);

/// Whether a scope has fixed ordered membership or runtime-dynamic membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeFlavor {
    Ordered,
    Dynamic,
}

/// A child identifier within one scope.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChildId(String);

impl ChildId {
    pub(crate) fn validate(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        if value.is_empty() {
            Err(IdError::Empty)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChildId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<&str> for ChildId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for ChildId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdError {
    Empty,
}

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
    /// `jitter_sample` is clamped into `[0, 1)` so callers cannot violate
    /// the equal-jitter range. Randomness is deliberately external data.
    #[must_use]
    pub fn next_delay(self, attempt: u64, jitter_sample: f64) -> Duration {
        let attempt = attempt.max(1);
        let (delay, jitter, maximum) = match self {
            Self::Immediate => return Duration::ZERO,
            Self::Fixed { delay, jitter } => (delay, jitter, delay),
            Self::Exponential {
                base,
                factor,
                max,
                jitter,
            } => {
                let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
                let nanos = duration_nanos(base) * factor.get().powi(exponent);
                let clamped = nanos.min(duration_nanos(max));
                (duration_from_nanos(clamped), jitter, max)
            }
        };
        let delay = match jitter {
            Jitter::None => delay,
            Jitter::Equal => {
                let sample = if jitter_sample.is_finite() {
                    jitter_sample.clamp(0.0, 1.0 - f64::EPSILON)
                } else {
                    0.0
                };
                duration_from_nanos(duration_nanos(delay) * (0.5 + sample * 0.5))
            }
        };
        // Floating-point duration conversion can round an extreme value a
        // few nanoseconds above the configured cap. Reapply the policy bound
        // with exact Duration ordering after every conversion and jitter.
        delay.min(maximum)
    }
}

fn duration_nanos(duration: Duration) -> f64 {
    duration.as_nanos() as f64
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

    pub(crate) fn should_restart(self, failure: bool) -> bool {
        match self.condition {
            RestartCondition::Always => true,
            RestartCondition::OnFailure => failure,
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
    /// A readiness mode is not meaningful for this child kind.
    #[error("readiness mode is not supported by this child kind")]
    UnsupportedReadiness,
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
    pub(crate) fn overlay(&self, values: &ScopeDefaults) -> Self {
        Self {
            child_restart: values.child_restart.unwrap_or(self.child_restart),
            child_shutdown: values.child_shutdown.unwrap_or(self.child_shutdown),
            mailbox: resolve_default_mailbox(values.mailbox, self.mailbox),
            mailbox_shutdown: values.mailbox_shutdown.unwrap_or(self.mailbox_shutdown),
            readiness_deadline: match values.readiness_deadline.unwrap_or(self.readiness_deadline) {
                ReadinessDeadline::Inherit => self.readiness_deadline,
                value => value,
            },
        }
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
    one_shot: bool,
    default_readiness: Readiness,
) -> ResolvedCommonOptions {
    let readiness_deadline = match options.readiness_deadline {
        ReadinessDeadline::Inherit => defaults.readiness_deadline,
        value => value,
    };
    ResolvedCommonOptions {
        restart: if one_shot {
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
        retention: options.retention.unwrap_or(if one_shot {
            Retention::Remove
        } else {
            Retention::Retain
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        Backoff, BackoffFactor, Intensity, Jitter, Mailbox, PolicyError, ReadinessDeadline,
        ResolvedDefaults, ScopeDefaults,
    };

    #[test]
    fn backoff_progression_and_jitter_are_pure_math() {
        let backoff = Backoff::exponential(
            Duration::from_millis(10),
            BackoffFactor::new(2.0).expect("valid factor"),
            Duration::from_millis(35),
            Jitter::None,
        )
        .expect("valid backoff");
        assert_eq!(backoff.next_delay(1, 0.9), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(2, 0.1), Duration::from_millis(20));
        assert_eq!(backoff.next_delay(3, 0.5), Duration::from_millis(35));
        assert_eq!(backoff.next_delay(99, 0.5), Duration::from_millis(35));

        let jittered =
            Backoff::fixed(Duration::from_millis(10), Jitter::Equal).expect("valid backoff");
        assert_eq!(jittered.next_delay(1, 0.0), Duration::from_millis(5));
        assert_eq!(jittered.next_delay(1, 0.5), Duration::from_micros(7_500));
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
                assert!(backoff.next_delay(u64::MAX, sample) <= maximum);
            }
        }

        let fixed = Backoff::fixed(maximum, Jitter::Equal).expect("valid fixed backoff");
        assert!(fixed.next_delay(u64::MAX, 1.0) <= maximum);
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
        assert_eq!(
            Backoff::exponential(
                Duration::from_secs(2),
                BackoffFactor::new(2.0).expect("valid factor"),
                Duration::from_secs(1),
                Jitter::None,
            ),
            Err(PolicyError::BackoffMaximumBeforeBase)
        );
        assert_eq!(
            BackoffFactor::new(f64::NAN),
            Err(PolicyError::InvalidBackoffFactor)
        );
        assert_eq!(
            BackoffFactor::new(0.5),
            Err(PolicyError::InvalidBackoffFactor)
        );
    }

    #[test]
    fn defaults_overlay_as_one_value_and_mailbox_capacity_resolves_by_kind() {
        let library = ResolvedDefaults::default();
        let latest = library.overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::latest()),
            ..ScopeDefaults::default()
        });
        assert_eq!(latest.mailbox, Mailbox::Latest);

        let deferred_queue = latest.overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::queue_inherit()),
            ..ScopeDefaults::default()
        });
        assert_eq!(deferred_queue.mailbox, Mailbox::default());
        assert_eq!(deferred_queue.child_restart, latest.child_restart);
        assert_eq!(deferred_queue.child_shutdown, latest.child_shutdown);
    }
}
