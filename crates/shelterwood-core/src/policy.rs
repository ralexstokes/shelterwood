//! Supervision policy and validated configuration data.

use std::{fmt, num::NonZeroUsize, time::Duration};

use crate::Exit;

/// Whether a scope has fixed ordered membership or runtime-dynamic membership.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScopeFlavor {
    /// Fixed, readiness-ordered membership.
    Ordered,
    /// Runtime-dynamic membership.
    Dynamic,
}

/// A one-origin backoff attempt that resets after a stable incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RestartAttempt(u64);

impl RestartAttempt {
    /// The state before a restart has been scheduled.
    pub const ZERO: Self = Self(0);

    /// Returns the next attempt, saturating at the numeric limit.
    ///
    /// This is bounded policy arithmetic, not an identity: equal saturated
    /// values cannot alias a resource or route an event.
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
    ///
    /// This public diagnostic total deliberately stays monotone at exhaustion;
    /// it is never used as identity or a storage key.
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
    ///
    /// This public diagnostic total deliberately stays monotone at exhaustion;
    /// it is never used as identity or a storage key.
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
pub enum ChildMode {
    Restartable,
    OneShot,
}

/// The default bounded FIFO mailbox capacity.
const DEFAULT_MAILBOX_CAPACITY: usize = 64;
/// The default child shutdown grace.
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// The default gated-readiness deadline.
const DEFAULT_READINESS_DEADLINE: Duration = Duration::from_secs(30);

/// A duration statically known to be non-zero.
///
/// Policy variants use this sealed value when zero would otherwise create a
/// second semantic branch: [`Shutdown::Graceful`] (whose zero grace would
/// duplicate `Abort` with different recorded provenance) and
/// [`ReadinessDeadline::Bounded`]. Construct it with [`NonZeroDuration::new`];
/// the private representation prevents zero-valued literals:
///
/// ```compile_fail,E0423
/// use std::time::Duration;
/// use shelterwood_core::policy::NonZeroDuration;
///
/// let _invalid = NonZeroDuration(Duration::ZERO);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NonZeroDuration(Duration);

impl NonZeroDuration {
    /// Validates and constructs a non-zero duration.
    pub fn new(duration: Duration) -> Result<Self, PolicyError> {
        if duration.is_zero() {
            Err(PolicyError::ZeroDuration)
        } else {
            Ok(Self(duration))
        }
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
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
        let factor = Self(value);
        factor.validate()?;
        Ok(factor)
    }

    /// Returns the validated multiplier.
    #[must_use]
    pub const fn get(self) -> f64 {
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
///
/// A fixed-backoff payload cannot be forged as a literal. `E0451` records the
/// privacy failure this proof depends on, and the payload types are kept
/// nameable by a companion test, so a rename cannot make the fence pass
/// vacuously:
///
/// ```compile_fail,E0451
/// use std::time::Duration;
/// use shelterwood_core::policy::{Backoff, FixedBackoff, Jitter};
///
/// let _ = Backoff::Fixed(FixedBackoff {
///     delay: Duration::ZERO,
///     jitter: Jitter::None,
/// });
/// ```
///
/// An exponential-backoff payload is sealed independently:
///
/// ```compile_fail,E0451
/// use std::time::Duration;
/// use shelterwood_core::policy::{Backoff, BackoffFactor, ExponentialBackoff, Jitter};
///
/// let _ = Backoff::Exponential(ExponentialBackoff {
///     base: Duration::ZERO,
///     factor: BackoffFactor::new(2.0).unwrap(),
///     max: Duration::from_secs(1),
///     jitter: Jitter::None,
/// });
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Backoff {
    /// Restart without a delay.
    Immediate,
    /// Use a fixed non-zero delay.
    Fixed(FixedBackoff),
    /// Increase the delay exponentially, clamped to `max`.
    Exponential(ExponentialBackoff),
}

/// Validated payload of a fixed [`Backoff`].
///
/// Values are created by [`Backoff::fixed`], which guarantees a non-zero
/// delay. The private representation prevents invalid fixed-backoff literals.
///
/// The literal fence on [`Backoff`] is satisfied by any one surviving private
/// field, so the invariant-bearing field is fenced on its own: reading
/// `delay` off a constructor-validated payload must not compile. `E0616`
/// records the privacy failure this proof depends on:
///
/// ```compile_fail,E0616
/// use std::time::Duration;
/// use shelterwood_core::policy::{Backoff, Jitter};
///
/// let Backoff::Fixed(fixed) = Backoff::fixed(Duration::from_secs(1), Jitter::None).unwrap()
/// else { unreachable!() };
/// let _ = fixed.delay;
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FixedBackoff {
    delay: Duration,
    jitter: Jitter,
}

impl FixedBackoff {
    /// Returns the fixed non-zero delay.
    #[must_use]
    pub const fn delay(self) -> Duration {
        self.delay
    }

    /// Returns the configured jitter mode.
    #[must_use]
    pub const fn jitter(self) -> Jitter {
        self.jitter
    }
}

/// Validated payload of an exponential [`Backoff`].
///
/// Values are created by [`Backoff::exponential`], which guarantees non-zero
/// delays, a valid factor, and a maximum no shorter than the base.
///
/// The literal fence on [`Backoff`] is satisfied by any one surviving private
/// field, so each invariant-bearing field is fenced on its own: reading
/// `base`, `factor`, or `max` off a constructor-validated payload must not
/// compile. `E0616` records the privacy failure these proofs depend on:
///
/// ```compile_fail,E0616
/// use std::time::Duration;
/// use shelterwood_core::policy::{Backoff, BackoffFactor, Jitter};
///
/// let Backoff::Exponential(exponential) = Backoff::exponential(
///     Duration::from_secs(1),
///     BackoffFactor::new(2.0).unwrap(),
///     Duration::from_secs(2),
///     Jitter::None,
/// )
/// .unwrap() else { unreachable!() };
/// let _ = exponential.base;
/// ```
///
/// ```compile_fail,E0616
/// use std::time::Duration;
/// use shelterwood_core::policy::{Backoff, BackoffFactor, Jitter};
///
/// let Backoff::Exponential(exponential) = Backoff::exponential(
///     Duration::from_secs(1),
///     BackoffFactor::new(2.0).unwrap(),
///     Duration::from_secs(2),
///     Jitter::None,
/// )
/// .unwrap() else { unreachable!() };
/// let _ = exponential.factor;
/// ```
///
/// ```compile_fail,E0616
/// use std::time::Duration;
/// use shelterwood_core::policy::{Backoff, BackoffFactor, Jitter};
///
/// let Backoff::Exponential(exponential) = Backoff::exponential(
///     Duration::from_secs(1),
///     BackoffFactor::new(2.0).unwrap(),
///     Duration::from_secs(2),
///     Jitter::None,
/// )
/// .unwrap() else { unreachable!() };
/// let _ = exponential.max;
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExponentialBackoff {
    base: Duration,
    factor: BackoffFactor,
    max: Duration,
    jitter: Jitter,
}

impl ExponentialBackoff {
    /// Returns the non-zero initial delay.
    #[must_use]
    pub const fn base(self) -> Duration {
        self.base
    }

    /// Returns the validated per-attempt multiplier.
    #[must_use]
    pub const fn factor(self) -> BackoffFactor {
        self.factor
    }

    /// Returns the non-zero maximum delay.
    #[must_use]
    pub const fn max(self) -> Duration {
        self.max
    }

    /// Returns the configured jitter mode.
    #[must_use]
    pub const fn jitter(self) -> Jitter {
        self.jitter
    }
}

impl Backoff {
    /// Constructs a validated fixed backoff.
    pub fn fixed(delay: Duration, jitter: Jitter) -> Result<Self, PolicyError> {
        if delay.is_zero() {
            return Err(PolicyError::ZeroDuration);
        }
        Ok(Self::Fixed(FixedBackoff { delay, jitter }))
    }

    /// Constructs a validated exponential backoff.
    ///
    /// Only [`PolicyError::ZeroDuration`] and
    /// [`PolicyError::BackoffMaximumBeforeBase`] are reachable here: the
    /// multiplier carries its own invariant, so
    /// [`PolicyError::InvalidBackoffFactor`] is spent at
    /// [`BackoffFactor::new`] before this call can be written.
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
        Ok(Self::Exponential(ExponentialBackoff {
            base,
            factor,
            max,
            jitter,
        }))
    }

    /// Derives the delay for a one-origin restart attempt.
    ///
    /// Randomness is deliberately supplied as external, range-checked data.
    #[must_use]
    pub fn next_delay(self, attempt: RestartAttempt, jitter_sample: JitterSample) -> Duration {
        let attempt = attempt.get().max(1);
        let (delay, jitter, maximum) = match self {
            Self::Immediate => return Duration::ZERO,
            Self::Fixed(fixed) => (fixed.delay, fixed.jitter, fixed.delay),
            Self::Exponential(exponential) => {
                let ExponentialBackoff {
                    base,
                    factor,
                    max,
                    jitter,
                } = exponential;
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
                        .max(half_duration(delay))
                }
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
        let seconds = u64::try_from(nanos / 1_000_000_000)
            .expect("a bounded nanosecond count fits the duration range");
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
        /// Validated non-zero cooperative grace.
        grace: NonZeroDuration,
    },
    /// Escalate immediately after cancellation.
    Abort,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::Graceful {
            grace: NonZeroDuration(DEFAULT_SHUTDOWN_GRACE),
        }
    }
}

impl Shutdown {
    /// Constructs a graceful policy with a validated non-zero grace.
    pub fn graceful(grace: Duration) -> Result<Self, PolicyError> {
        Ok(Self::Graceful {
            grace: NonZeroDuration::new(grace)?,
        })
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
    Bounded(NonZeroDuration),
    /// Wait without a deadline.
    Unbounded,
}

impl ReadinessDeadline {
    /// Constructs a validated bounded deadline.
    pub fn bounded(duration: Duration) -> Result<Self, PolicyError> {
        Ok(Self::Bounded(NonZeroDuration::new(duration)?))
    }
}

/// The scope-wide restart budget.
///
/// Values are created by [`Intensity::new`], and cannot be forged with an
/// invalid zero-length window. `E0451` records the privacy failure this proof
/// depends on:
///
/// ```compile_fail,E0451
/// use std::time::Duration;
/// use shelterwood_core::policy::Intensity;
///
/// let _ = Intensity {
///     max_restarts: 1,
///     within: Duration::ZERO,
/// };
/// ```
///
/// That literal fence is satisfied by either private field, so the
/// invariant-bearing field is fenced on its own: reading `within` off a
/// constructor-validated budget must not compile. `E0616` records the privacy
/// failure this proof depends on:
///
/// ```compile_fail,E0616
/// use std::time::Duration;
/// use shelterwood_core::policy::Intensity;
///
/// let intensity = Intensity::new(1, Duration::from_secs(1)).unwrap();
/// let _ = intensity.within;
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Intensity {
    // Maximum restart charges allowed inside the rolling window.
    max_restarts: u64,
    // The rolling-window duration.
    within: Duration,
}

impl Intensity {
    /// Constructs a validated intensity budget.
    pub fn new(max_restarts: u64, within: Duration) -> Result<Self, PolicyError> {
        if within.is_zero() {
            return Err(PolicyError::ZeroDuration);
        }
        Ok(Self {
            max_restarts,
            within,
        })
    }

    /// Returns the maximum restart charges allowed inside the window.
    #[must_use]
    pub const fn max_restarts(self) -> u64 {
        self.max_restarts
    }

    /// Returns the non-zero rolling-window duration.
    #[must_use]
    pub const fn within(self) -> Duration {
        self.within
    }
}

impl Default for Intensity {
    fn default() -> Self {
        // The library default is minted through the validating constructor, so
        // no in-crate literal can install a window the public API rejects.
        Self::new(5, Duration::from_secs(30)).expect("the library default window is non-zero")
    }
}

/// Core mailbox declarations carried as L1 policy data.
///
/// Non-exhaustive deliberately: Part II adds `latest_by_key`.
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
        Self::Queue(Some(
            NonZeroUsize::new(DEFAULT_MAILBOX_CAPACITY)
                .expect("the library mailbox capacity is non-zero"),
        ))
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
///
/// Non-exhaustive deliberately: Part II adds group strategies.
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
    /// Default readiness deadline; [`ReadinessDeadline::Inherit`] is the
    /// explicit unset state.
    pub readiness_deadline: ReadinessDeadline,
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
///
/// Non-exhaustive deliberately: future validated policy payloads add
/// construction failures without changing existing variants.
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

pub fn tidy_abort_beat(grace: Duration) -> Duration {
    (grace / 10).clamp(Duration::from_millis(1), Duration::from_millis(10))
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommonOptions {
    pub restart: Option<RestartPolicy>,
    pub shutdown: Option<Shutdown>,
    pub mailbox: Option<Mailbox>,
    pub mailbox_shutdown: Option<MailboxShutdown>,
    pub readiness: Option<Readiness>,
    pub readiness_deadline: ReadinessDeadline,
    pub retention: Option<Retention>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedDefaults {
    pub child_restart: RestartPolicy,
    pub child_shutdown: Shutdown,
    mailbox_kind: ResolvedMailboxKind,
    /// Nearest resolved queue capacity, retained across defaults of other kinds.
    queue_capacity: NonZeroUsize,
    pub mailbox_shutdown: MailboxShutdown,
    readiness_deadline: ResolvedReadinessDeadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolvedMailboxKind {
    Queue,
    Latest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedMailbox {
    Queue(NonZeroUsize),
    Latest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolvedReadinessDeadline {
    Bounded(NonZeroDuration),
    Unbounded,
}

impl ResolvedReadinessDeadline {
    fn resolve(value: ReadinessDeadline, inherited: Self) -> Self {
        match value {
            ReadinessDeadline::Inherit => inherited,
            ReadinessDeadline::Bounded(duration) => Self::Bounded(duration),
            ReadinessDeadline::Unbounded => Self::Unbounded,
        }
    }

    fn duration(self) -> Option<Duration> {
        match self {
            Self::Bounded(duration) => Some(duration.get()),
            Self::Unbounded => None,
        }
    }
}

impl Default for ResolvedDefaults {
    fn default() -> Self {
        let queue_capacity = NonZeroUsize::new(DEFAULT_MAILBOX_CAPACITY)
            .expect("the library mailbox capacity is non-zero");
        // The library deadline is minted through the validating constructor, so
        // no in-crate literal can install a payload the public API rejects.
        let ReadinessDeadline::Bounded(readiness_deadline) =
            ReadinessDeadline::bounded(DEFAULT_READINESS_DEADLINE)
                .expect("the library readiness deadline is non-zero")
        else {
            unreachable!("the bounded constructor returns the bounded variant")
        };
        Self {
            child_restart: RestartPolicy::default(),
            child_shutdown: Shutdown::default(),
            mailbox_kind: ResolvedMailboxKind::Queue,
            queue_capacity,
            mailbox_shutdown: MailboxShutdown::default(),
            readiness_deadline: ResolvedReadinessDeadline::Bounded(readiness_deadline),
        }
    }
}

impl ResolvedDefaults {
    pub fn mailbox(&self) -> ResolvedMailbox {
        match self.mailbox_kind {
            ResolvedMailboxKind::Queue => ResolvedMailbox::Queue(self.queue_capacity),
            ResolvedMailboxKind::Latest => ResolvedMailbox::Latest,
        }
    }

    /// Resolves a child's mailbox override against these defaults.
    ///
    /// Kept on `ResolvedDefaults` for the same reason `overlay` resolves the
    /// kind and the capacity together: an inherited `Queue` and the capacity
    /// it inherits come from one value, so the contradictory pairing of
    /// `Queue(a)` with a capacity `b != a` has no way to be written.
    pub fn resolve_child_mailbox(&self, value: Option<Mailbox>) -> ResolvedMailbox {
        match value {
            None => self.mailbox(),
            Some(Mailbox::Queue(None)) => ResolvedMailbox::Queue(self.queue_capacity),
            Some(Mailbox::Queue(Some(capacity))) => ResolvedMailbox::Queue(capacity),
            Some(Mailbox::Latest) => ResolvedMailbox::Latest,
        }
    }

    pub fn overlay(&self, values: &ScopeDefaults) -> Self {
        let (mailbox_kind, queue_capacity) = match values.mailbox {
            None => (self.mailbox_kind, self.queue_capacity),
            Some(Mailbox::Queue(None)) => (ResolvedMailboxKind::Queue, self.queue_capacity),
            Some(Mailbox::Queue(Some(capacity))) => (ResolvedMailboxKind::Queue, capacity),
            Some(Mailbox::Latest) => (ResolvedMailboxKind::Latest, self.queue_capacity),
        };
        Self {
            child_restart: resolve(values.child_restart, self.child_restart),
            child_shutdown: resolve(values.child_shutdown, self.child_shutdown),
            mailbox_kind,
            queue_capacity,
            mailbox_shutdown: resolve(values.mailbox_shutdown, self.mailbox_shutdown),
            readiness_deadline: ResolvedReadinessDeadline::resolve(
                values.readiness_deadline,
                self.readiness_deadline,
            ),
        }
    }
}

fn resolve<T: Copy>(value: Option<T>, inherited: T) -> T {
    value.unwrap_or(inherited)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedCommonOptions {
    pub restart: RestartPolicy,
    pub shutdown: Shutdown,
    pub mailbox: ResolvedMailbox,
    pub mailbox_shutdown: MailboxShutdown,
    /// Effective definition readiness. Every kind arrives here already
    /// resolved: raw actors fold their type-level default into the definition
    /// at erasure, so no per-incarnation fallback remains.
    pub readiness: Readiness,
    readiness_deadline: ResolvedReadinessDeadline,
    pub retention: Retention,
}

impl ResolvedCommonOptions {
    pub fn readiness_deadline(&self) -> Option<Duration> {
        self.readiness_deadline.duration()
    }
}

pub fn resolve_common(
    options: &CommonOptions,
    defaults: &ResolvedDefaults,
    mode: ChildMode,
    default_readiness: Readiness,
) -> ResolvedCommonOptions {
    ResolvedCommonOptions {
        restart: if mode == ChildMode::OneShot {
            RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
        } else {
            resolve(options.restart, defaults.child_restart)
        },
        shutdown: resolve(options.shutdown, defaults.child_shutdown),
        mailbox: defaults.resolve_child_mailbox(options.mailbox),
        mailbox_shutdown: resolve(options.mailbox_shutdown, defaults.mailbox_shutdown),
        readiness: resolve(options.readiness, default_readiness),
        readiness_deadline: ResolvedReadinessDeadline::resolve(
            options.readiness_deadline,
            defaults.readiness_deadline,
        ),
        retention: resolve(
            options.retention,
            if mode == ChildMode::OneShot {
                Retention::Remove
            } else {
                Retention::Retain
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use super::{
        Backoff, BackoffFactor, ChildMode, CommonOptions, Intensity, Jitter, JitterSample, Mailbox,
        MailboxShutdown, NonZeroDuration, PolicyError, Readiness, ReadinessDeadline,
        ResolvedDefaults, ResolvedMailbox, RestartAttempt, RestartCondition, RestartCount,
        RestartPolicy, Retention, ScopeDefaults, Shutdown, TotalRestarts, resolve_common,
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
                grace: NonZeroDuration::new(Duration::from_secs(5))
                    .expect("the default grace is non-zero"),
            }
        );
        assert_eq!(
            ResolvedDefaults::default().readiness_deadline.duration(),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            Intensity::default(),
            Intensity::new(5, Duration::from_secs(30)).expect("the default window is non-zero")
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
    fn graceful_shutdown_rejects_the_zero_duration_branch() {
        assert_eq!(
            Shutdown::graceful(Duration::ZERO),
            Err(PolicyError::ZeroDuration)
        );
        assert_eq!(
            Shutdown::graceful(Duration::from_nanos(1)),
            Ok(Shutdown::Graceful {
                grace: NonZeroDuration::new(Duration::from_nanos(1))
                    .expect("one nanosecond is non-zero"),
            })
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
        assert_eq!(JitterSample::from_u64_ratio(1, 0).0, 0.0);
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
        // Pins SPEC §10.2's large-exponent contract: in the large-exponent regime
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
    fn smallest_positive_sample_equal_jitter_respects_the_exact_half_delay() {
        let odd = Duration::from_nanos((1 << 60) + 1);
        let fixed = Backoff::fixed(odd, Jitter::Equal).expect("valid backoff");
        let smallest_positive = JitterSample::new(f64::from_bits(1));
        assert_ne!(smallest_positive, JitterSample::new(0.0));
        assert_eq!(
            fixed.next_delay(RestartAttempt(1), smallest_positive),
            Duration::from_nanos((1 << 59) + 1)
        );
    }

    #[test]
    fn restart_policy_is_never_matches_only_the_never_condition() {
        assert!(RestartPolicy::new(RestartCondition::Never, Backoff::Immediate).is_never());
        assert!(!RestartPolicy::new(RestartCondition::OnFailure, Backoff::Immediate).is_never());
        assert!(!RestartPolicy::new(RestartCondition::Always, Backoff::Immediate).is_never());
    }

    #[test]
    fn mailbox_capacity_walks_outward_by_kind_across_defaults_overlays() {
        let library = ResolvedDefaults::default();
        let library_latest = library.overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::latest()),
            ..ScopeDefaults::default()
        });
        let library_queue = library_latest.overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::queue_inherit()),
            ..ScopeDefaults::default()
        });
        assert_eq!(
            library_queue.mailbox(),
            ResolvedMailbox::Queue(NonZeroUsize::new(64).expect("capacity is non-zero"))
        );

        let outer_queue = library.overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::queue(10).expect("non-zero capacity")),
            ..ScopeDefaults::default()
        });
        let latest = outer_queue.overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::latest()),
            ..ScopeDefaults::default()
        });
        assert_eq!(latest.mailbox(), ResolvedMailbox::Latest);

        let deferred_queue = latest.overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::queue_inherit()),
            ..ScopeDefaults::default()
        });
        assert_eq!(
            deferred_queue.mailbox(),
            ResolvedMailbox::Queue(NonZeroUsize::new(10).expect("capacity is non-zero"))
        );
        assert_eq!(deferred_queue.child_restart, latest.child_restart);
        assert_eq!(deferred_queue.child_shutdown, latest.child_shutdown);

        let reset_queue = ResolvedDefaults::default().overlay(&ScopeDefaults {
            mailbox: Some(Mailbox::queue_inherit()),
            ..ScopeDefaults::default()
        });
        assert_eq!(
            reset_queue.mailbox(),
            ResolvedMailbox::Queue(NonZeroUsize::new(64).expect("capacity is non-zero"))
        );
    }

    #[test]
    fn child_mailbox_overrides_cover_every_kind_and_capacity_source() {
        let queue_capacity = NonZeroUsize::new(10).expect("capacity is non-zero");
        let explicit_capacity = NonZeroUsize::new(3).expect("capacity is non-zero");
        let defaults = ResolvedDefaults::default()
            .overlay(&ScopeDefaults {
                mailbox: Some(Mailbox::Queue(Some(queue_capacity))),
                ..ScopeDefaults::default()
            })
            .overlay(&ScopeDefaults {
                mailbox: Some(Mailbox::Latest),
                ..ScopeDefaults::default()
            });

        assert_eq!(
            defaults.resolve_child_mailbox(None),
            ResolvedMailbox::Latest
        );
        assert_eq!(
            defaults.resolve_child_mailbox(Some(Mailbox::Queue(None))),
            ResolvedMailbox::Queue(queue_capacity),
            "queue inheritance retains the last queue capacity across a Latest default"
        );
        assert_eq!(
            defaults.resolve_child_mailbox(Some(Mailbox::Queue(Some(explicit_capacity)))),
            ResolvedMailbox::Queue(explicit_capacity)
        );
        assert_eq!(
            defaults.resolve_child_mailbox(Some(Mailbox::Latest)),
            ResolvedMailbox::Latest
        );
    }

    #[test]
    fn one_shot_resolution_forces_never_restart_and_defaults_to_removal() {
        let explicit_restart = RestartPolicy::new(RestartCondition::Always, Backoff::Immediate);
        let defaults = ResolvedDefaults::default();
        let resolved = resolve_common(
            &CommonOptions {
                restart: Some(explicit_restart),
                ..CommonOptions::default()
            },
            &defaults,
            ChildMode::OneShot,
            Readiness::Immediate,
        );
        assert_eq!(
            resolved.restart,
            RestartPolicy::new(RestartCondition::Never, Backoff::Immediate),
            "one-shot mode overrides even an explicit restart policy"
        );
        assert_eq!(resolved.retention, Retention::Remove);

        let retained = resolve_common(
            &CommonOptions {
                restart: Some(explicit_restart),
                retention: Some(Retention::Retain),
                ..CommonOptions::default()
            },
            &defaults,
            ChildMode::OneShot,
            Readiness::Immediate,
        );
        assert_eq!(
            retained.restart,
            RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
        );
        assert_eq!(retained.retention, Retention::Retain);
    }

    #[test]
    fn default_overlay_and_child_resolution_share_inheritance_rules() {
        let inherited = ResolvedDefaults::default().overlay(&ScopeDefaults {
            child_restart: Some(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Immediate,
            )),
            child_shutdown: Some(Shutdown::Abort),
            mailbox: Some(Mailbox::Latest),
            mailbox_shutdown: Some(MailboxShutdown::Discard),
            readiness_deadline: ReadinessDeadline::Unbounded,
        });

        assert_eq!(inherited.overlay(&ScopeDefaults::default()), inherited);
        assert_eq!(
            inherited.overlay(&ScopeDefaults {
                readiness_deadline: ReadinessDeadline::Inherit,
                ..ScopeDefaults::default()
            }),
            inherited
        );

        let child = resolve_common(
            &CommonOptions::default(),
            &inherited,
            ChildMode::Restartable,
            Readiness::Manual,
        );
        assert_eq!(child.restart, inherited.child_restart);
        assert_eq!(child.shutdown, inherited.child_shutdown);
        assert_eq!(child.mailbox, inherited.mailbox());
        assert_eq!(child.mailbox_shutdown, inherited.mailbox_shutdown);
        assert_eq!(child.readiness, Readiness::Manual);
        assert_eq!(child.readiness_deadline, inherited.readiness_deadline);
        assert_eq!(child.retention, Retention::Retain);

        let overridden = resolve_common(
            &CommonOptions {
                restart: Some(RestartPolicy::default()),
                shutdown: Some(Shutdown::default()),
                mailbox_shutdown: Some(MailboxShutdown::Drain),
                readiness: Some(Readiness::Immediate),
                readiness_deadline: ReadinessDeadline::bounded(Duration::from_nanos(1))
                    .expect("non-zero deadline"),
                retention: Some(Retention::Remove),
                ..CommonOptions::default()
            },
            &inherited,
            ChildMode::Restartable,
            Readiness::Manual,
        );
        assert_eq!(overridden.restart, RestartPolicy::default());
        assert_eq!(overridden.shutdown, Shutdown::default());
        assert_eq!(overridden.mailbox, inherited.mailbox());
        assert_eq!(overridden.mailbox_shutdown, MailboxShutdown::Drain);
        assert_eq!(overridden.readiness, Readiness::Immediate);
        assert_eq!(
            overridden.readiness_deadline.duration(),
            Some(Duration::from_nanos(1))
        );
        assert_eq!(overridden.retention, Retention::Remove);
    }
}
