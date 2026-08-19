use std::time::{Duration, Instant};

/// Returns the latest instant in the platform clock's representable range.
pub fn latest_representable(started_at: Instant) -> Instant {
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
