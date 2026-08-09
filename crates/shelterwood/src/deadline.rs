//! Overflow-safe internal deadline semantics.

use std::time::{Duration, Instant};

/// An absolute deadline, or one too distant for the clock to represent.
///
/// Overflow consistently means that the deadline never arrives. In
/// particular, it must never be substituted with the budget's start time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Deadline(Option<Instant>);

impl Deadline {
    /// Captures a duration budget relative to `started_at`.
    pub(crate) fn after(started_at: Instant, duration: Duration) -> Self {
        Self(started_at.checked_add(duration))
    }

    /// Returns the representable absolute deadline, if there is one.
    pub(crate) fn instant(self) -> Option<Instant> {
        self.0
    }

    /// Captures a duration budget as an absolute instant, saturating to a
    /// far-future point when the exact deadline overflows the clock.
    ///
    /// This is the far-future clamp: callers that must surface a present
    /// absolute point — a `Restarting` snapshot's `restart_at` — use this
    /// instead of [`Deadline::after`]'s never-arrives `None`.
    pub(crate) fn saturating_after(started_at: Instant, duration: Duration) -> Instant {
        let mut budget = duration;
        loop {
            if let Some(instant) = started_at.checked_add(budget) {
                return instant;
            }
            // Halving converges, and the first representable budget after
            // an overflowing one is at least half the clock's remaining
            // range — still far future.
            budget /= 2;
        }
    }

    /// Reports whether a representable deadline has elapsed.
    pub(crate) fn is_due(self, now: Instant) -> bool {
        self.0.is_some_and(|deadline| now >= deadline)
    }

    /// Reports whether a representable deadline is strictly in the past.
    ///
    /// Exact-boundary arbitration can still let already-ready work win, but
    /// work that has not started must not begin after its budget has elapsed.
    pub(crate) fn is_overdue(self, now: Instant) -> bool {
        self.0.is_some_and(|deadline| now > deadline)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::Deadline;

    #[test]
    fn overflow_is_never_due() {
        let now = Instant::now();
        let deadline = Deadline::after(now, Duration::MAX);

        assert_eq!(deadline.instant(), None);
        assert!(!deadline.is_due(now));
        assert!(!deadline.is_overdue(now));
    }

    #[test]
    fn saturating_after_matches_exact_addition_when_representable() {
        let now = Instant::now();
        assert_eq!(
            Deadline::saturating_after(now, Duration::from_secs(1)),
            now + Duration::from_secs(1)
        );
    }

    #[test]
    fn saturating_after_clamps_overflow_to_a_far_future_instant() {
        let now = Instant::now();
        let century = Duration::from_secs(60 * 60 * 24 * 365 * 100);
        assert!(Deadline::saturating_after(now, Duration::MAX) > now + century);
    }

    #[test]
    fn representable_deadline_distinguishes_due_from_overdue() {
        let now = Instant::now();
        let deadline = Deadline::after(now, Duration::from_secs(1));
        let at = now + Duration::from_secs(1);

        assert_eq!(deadline.instant(), Some(at));
        assert!(deadline.is_due(at));
        assert!(!deadline.is_overdue(at));
        assert!(deadline.is_overdue(at + Duration::from_nanos(1)));
    }
}
