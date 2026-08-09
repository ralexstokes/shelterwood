//! Overflow-safe internal deadline semantics.

use std::time::{Duration, Instant};

/// An absolute deadline, or one too distant for the clock to represent.
///
/// Overflow consistently means that the deadline never arrives. In
/// particular, it must never be substituted with the budget's start time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Deadline(Option<Instant>);

impl Deadline {
    /// Captures an absolute point only when the runtime can arm it exactly.
    ///
    /// A representable instant within [`Self::ARMING_HEADROOM`] of the clock
    /// limit still cannot be handed to the timer safely. Such a point has the
    /// same never-arrives semantics as an overflowing relative budget; it is
    /// never replaced with an earlier, merely armable instant.
    pub(crate) fn at(instant: Instant) -> Self {
        Self(instant.checked_add(Self::ARMING_HEADROOM).map(|_| instant))
    }

    /// Captures a duration budget relative to `started_at`.
    ///
    /// A deadline the timer could not arm — one within
    /// [`Self::ARMING_HEADROOM`] of the clock limit — is treated exactly
    /// like one the clock cannot represent: it never arrives. Filtering
    /// here keeps every stored deadline identical to the instant that
    /// would be armed for it, so due-ness checks and timer wakes can
    /// never disagree at the clock boundary.
    pub(crate) fn after(started_at: Instant, duration: Duration) -> Self {
        started_at
            .checked_add(duration)
            .map_or(Self(None), Self::at)
    }

    /// Returns the representable absolute deadline, if there is one.
    pub(crate) fn instant(self) -> Option<Instant> {
        self.0
    }

    /// Headroom an absolute deadline must leave below the clock limit.
    ///
    /// Arming a deadline is itself clock arithmetic — tokio rounds a
    /// sleep deadline up to the next whole millisecond by adding to it
    /// before tick conversion, and a paused test clock advances its base
    /// past the armed tick by another whole millisecond — so a deadline
    /// flush against `Instant`'s limit would panic at arming or advance
    /// time. One second comfortably covers both additions without
    /// changing the requested deadline.
    pub(crate) const ARMING_HEADROOM: Duration = Duration::from_secs(1);

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

    fn latest_representable(started_at: Instant) -> Instant {
        let mut low = Duration::ZERO;
        let mut high = Duration::MAX;
        assert!(started_at.checked_add(high).is_none());
        while high - low > Duration::from_nanos(1) {
            let mid = low + (high - low) / 2;
            if started_at.checked_add(mid).is_some() {
                low = mid;
            } else {
                high = mid;
            }
        }
        started_at + low
    }

    #[test]
    fn overflow_is_never_due() {
        let now = Instant::now();
        let deadline = Deadline::after(now, Duration::MAX);

        assert_eq!(deadline.instant(), None);
        assert!(!deadline.is_due(now));
        assert!(!deadline.is_overdue(now));
    }

    #[test]
    fn ordinary_budget_preserves_the_exact_deadline() {
        let now = Instant::now();
        assert_eq!(
            Deadline::after(now, Duration::from_secs(1)).instant(),
            Some(now + Duration::from_secs(1))
        );
    }

    #[test]
    fn unarmable_absolute_point_is_not_substituted() {
        let now = Instant::now();
        let unarmable = latest_representable(now);

        assert_ne!(unarmable, now, "the edge point is not the budget start");
        assert_eq!(Deadline::at(unarmable).instant(), None);
    }

    #[test]
    fn unarmable_but_representable_budgets_never_arrive() {
        let now = Instant::now();
        let unarmable = latest_representable(now) - now;
        let armable = unarmable - Deadline::ARMING_HEADROOM;

        assert_eq!(
            Deadline::after(now, armable).instant(),
            Some(now + armable),
            "a budget that leaves the arming headroom is a real deadline"
        );
        let flush = Deadline::after(now, unarmable);
        assert_eq!(
            flush.instant(),
            None,
            "a budget the timer could not arm never arrives"
        );
        assert!(!flush.is_due(now + armable));
        assert!(!flush.is_overdue(now + armable));
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
