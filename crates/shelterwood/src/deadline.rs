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
    ///
    /// A deadline the timer could not arm — one within
    /// [`Self::ARMING_HEADROOM`] of the clock limit — is treated exactly
    /// like one the clock cannot represent: it never arrives. Filtering
    /// here keeps every stored deadline identical to the instant that
    /// would be armed for it, so due-ness checks and timer wakes can
    /// never disagree at the clock boundary.
    pub(crate) fn after(started_at: Instant, duration: Duration) -> Self {
        Self(
            started_at
                .checked_add(duration)
                .filter(|deadline| deadline.checked_add(Self::ARMING_HEADROOM).is_some()),
        )
    }

    /// Returns the representable absolute deadline, if there is one.
    pub(crate) fn instant(self) -> Option<Instant> {
        self.0
    }

    /// Headroom the far-future clamp keeps below the clock limit.
    ///
    /// Arming a deadline is itself clock arithmetic — tokio rounds a
    /// sleep deadline up to the next whole millisecond by adding to it
    /// before tick conversion, and a paused test clock advances its base
    /// past the armed tick by another whole millisecond — so a deadline
    /// flush against `Instant`'s limit would panic at arming or advance
    /// time. One second comfortably covers both additions without
    /// measurably loosening the clamp.
    pub(crate) const ARMING_HEADROOM: Duration = Duration::from_secs(1);

    /// Captures a duration budget as an absolute instant, saturating to
    /// the largest armable deadline when the exact one overflows the
    /// clock (or its [`Self::ARMING_HEADROOM`]).
    ///
    /// This is the far-future clamp: callers that must surface a present
    /// absolute point — a `Restarting` snapshot's `restart_at` — use this
    /// instead of [`Deadline::after`]'s never-arrives `None`.
    pub(crate) fn saturating_after(started_at: Instant, duration: Duration) -> Instant {
        let armable = |budget: Duration| {
            budget
                .checked_add(Self::ARMING_HEADROOM)
                .and_then(|padded| started_at.checked_add(padded))
                .is_some()
        };
        if armable(duration) {
            return started_at + duration;
        }
        // The exact deadline overflows: saturate to the largest budget
        // that still fits — the closest armable approximation — so a
        // narrow clock never arms the deadline earlier than it must.
        // Binary search on the nanosecond-granular budget: `low` always
        // fits, `high` never does, and the gap halves every step,
        // bounding the loop by the bit width of `Duration`.
        let mut low = Duration::ZERO;
        let mut high = duration;
        while high - low > Duration::from_nanos(1) {
            let mid = low + (high - low) / 2;
            if armable(mid) {
                low = mid;
            } else {
                high = mid;
            }
        }
        started_at + low
    }

    /// Clamps an absolute deadline to the largest armable instant.
    ///
    /// A representable deadline within [`Self::ARMING_HEADROOM`] of the
    /// clock limit would still panic inside the timer's millisecond
    /// round-up, so the arming boundary pulls it back just far enough to
    /// fit. Deadlines that already leave the headroom pass through
    /// unchanged — including everything [`Deadline::after`] produces,
    /// which withholds unarmable deadlines at computation time; this
    /// clamp is the last line of defense for instants that reach the
    /// runtime boundary by any other route.
    pub(crate) fn clamp_armable(deadline: Instant) -> Instant {
        if deadline.checked_add(Self::ARMING_HEADROOM).is_some() {
            return deadline;
        }
        let now = Instant::now();
        Self::saturating_after(now, deadline.saturating_duration_since(now))
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
    fn saturating_after_clamps_overflow_to_the_largest_armable_instant() {
        let now = Instant::now();
        let clamped = Deadline::saturating_after(now, Duration::MAX);
        let century = Duration::from_secs(60 * 60 * 24 * 365 * 100);
        assert!(clamped > now + century);
        assert!(
            clamped.checked_add(Deadline::ARMING_HEADROOM).is_some(),
            "the clamp leaves the timer's arming headroom below the clock limit"
        );
        assert!(
            clamped
                .checked_add(Deadline::ARMING_HEADROOM + Duration::from_nanos(1))
                .is_none(),
            "the clamp saturates to the clock limit less the arming headroom, \
             not to a halved budget"
        );
    }

    #[test]
    fn unarmable_but_representable_budgets_never_arrive() {
        let now = Instant::now();
        // The largest armable budget: its deadline sits exactly one
        // headroom below the clock limit, so one more headroom on top of
        // it lands flush against the limit — representable, but a panic
        // to arm.
        let armable = Deadline::saturating_after(now, Duration::MAX) - now;
        let unarmable = armable + Deadline::ARMING_HEADROOM;

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
    fn clamp_armable_passes_ordinary_deadlines_through_unchanged() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(1);
        assert_eq!(Deadline::clamp_armable(deadline), deadline);
    }

    #[test]
    fn clamp_armable_pulls_a_near_limit_deadline_below_the_headroom() {
        let now = Instant::now();
        // The far-future clamp saturates to the clock limit less the
        // headroom, so adding the headroom back lands flush against the
        // limit: representable, but a panic to arm.
        let flush = Deadline::saturating_after(now, Duration::MAX) + Deadline::ARMING_HEADROOM;
        let clamped = Deadline::clamp_armable(flush);
        assert!(clamped < flush, "the clamp never arms later than asked");
        assert!(
            clamped.checked_add(Deadline::ARMING_HEADROOM).is_some(),
            "the clamp leaves the timer's arming headroom below the clock limit"
        );
        let century = Duration::from_secs(60 * 60 * 24 * 365 * 100);
        assert!(clamped > now + century, "the clamp stays far future");
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
