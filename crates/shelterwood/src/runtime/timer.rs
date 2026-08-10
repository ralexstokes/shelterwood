use std::{future::Future, pin::Pin, time::Duration};

use tokio::time;

use crate::deadline::Deadline;

/// Advances a paused test clock, keeping timer control in this module.
#[cfg(test)]
pub(crate) async fn advance(duration: Duration) {
    time::advance(duration).await;
}

pub(crate) fn now() -> std::time::Instant {
    time::Instant::now().into_std()
}

// Keep each timer registration comfortably inside tokio's millisecond tick
// range. Tokio caps instants beyond its private `MAX_SAFE_MILLIS_DURATION`
// (`u64::MAX - 2` milliseconds in tokio 1.53),
// which would otherwise make a valid but very distant std Instant fire early.
// Rechecking the original absolute point after bounded slices preserves exact
// never-early semantics without coupling this crate to that private constant.
const MAX_TIMER_SLICE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

fn next_timer_deadline(
    current: std::time::Instant,
    requested: std::time::Instant,
) -> Option<std::time::Instant> {
    if requested <= current {
        return None;
    }
    let slice = requested.duration_since(current).min(MAX_TIMER_SLICE);
    current.checked_add(slice)
}

pub(crate) type BoxedSleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub(crate) fn deadline(duration: Duration) -> Deadline {
    Deadline::after(now(), duration)
}

pub(crate) fn sleep_deadline(deadline: Deadline) -> BoxedSleep {
    Box::pin(async move {
        match deadline.instant() {
            Some(deadline) => sleep_until_std(deadline).await,
            None => std::future::pending().await,
        }
    })
}

pub(crate) fn sleep_until(deadline: std::time::Instant) -> BoxedSleep {
    Box::pin(sleep_until_std(deadline))
}
pub(crate) async fn sleep_until_std(deadline: std::time::Instant) {
    // Every absolute-deadline arming crosses the runtime boundary here:
    // tokio rounds the deadline up to the next whole millisecond with a
    // panicking add before tick conversion, so a deadline flush against
    // the clock limit would panic at arming time rather than when it was
    // computed. Absolute instants that arrive by another route obey the same
    // never-substitute rule as relative budgets: if this exact point cannot
    // be armed, it never arrives.
    loop {
        let current = now();
        let Some(next) = next_timer_deadline(current, deadline) else {
            return;
        };
        match Deadline::at(next).instant() {
            Some(next) => time::sleep_until(time::Instant::from_std(next)).await,
            None => std::future::pending().await,
        }
    }
}

pub(crate) enum Timeout<T> {
    Completed(T),
    Elapsed,
}

pub(crate) async fn timeout<F>(duration: Duration, future: F) -> Timeout<F::Output>
where
    F: Future,
{
    // tokio's timeout only falls back to its internal far future when the
    // deadline addition overflows outright; a representable deadline flush
    // against the clock limit would still panic at arming. Route the
    // budget through Deadline so an unarmable timeout never elapses,
    // matching sleep_deadline's overflow semantics.
    let Some(deadline) = deadline(duration).instant() else {
        return Timeout::Completed(future.await);
    };
    // Deadline's zero-budget carve-out keeps an exact zero budget
    // representable even when its clock value is too close to Instant's
    // ceiling for the timer to arm safely. Handing that instant to tokio's
    // timeout would still arm it — the tick conversion rounds the deadline
    // up with a panicking add — so apply the carve-out's own due-check here
    // instead of arming: the budget is already due, and exact-boundary
    // arbitration gives an immediately ready operation its one poll before
    // the elapse. Every other zero budget stays on tokio's timeout below,
    // keeping the normal regime's semantics untouched.
    if duration.is_zero() && Deadline::at(deadline).instant().is_none() {
        tokio::pin!(future);
        return std::future::poll_fn(|context| {
            std::task::Poll::Ready(match future.as_mut().poll(context) {
                std::task::Poll::Ready(value) => Timeout::Completed(value),
                std::task::Poll::Pending => Timeout::Elapsed,
            })
        })
        .await;
    }
    if duration <= MAX_TIMER_SLICE {
        return match time::timeout(duration, future).await {
            Ok(value) => Timeout::Completed(value),
            Err(_) => Timeout::Elapsed,
        };
    }
    let sleep = sleep_until_std(deadline);
    tokio::pin!(future);
    tokio::pin!(sleep);
    tokio::select! {
        // Match tokio::time::timeout's boundary rule: the operation receives
        // the first poll when it and a zero-duration timer are both ready.
        biased;
        value = &mut future => Timeout::Completed(value),
        () = &mut sleep => Timeout::Elapsed,
    }
}
#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        task::{Context, Waker},
        time::Duration,
    };

    use super::{MAX_TIMER_SLICE, next_timer_deadline, timeout};

    fn latest_representable(started_at: std::time::Instant) -> std::time::Instant {
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

    #[tokio::test(start_paused = true)]
    async fn unarmable_absolute_deadline_stays_pending_without_substitution() {
        let now = std::time::Instant::now();
        let flush = latest_representable(now);
        let mut sleep = std::pin::pin!(super::sleep_until_std(flush));
        let mut context = Context::from_waker(Waker::noop());
        // The timer registers on first poll: passing this instant to tokio
        // would panic during its millisecond round-up rather than parking.
        assert!(sleep.as_mut().poll(&mut context).is_pending());
    }

    #[test]
    fn deadline_beyond_tokios_tick_range_is_armed_in_a_bounded_slice() {
        // Tokio 1.53 reserves the top three u64 millisecond ticks. The exact
        // value is test evidence only: production uses a small stable slice
        // rather than depending on tokio's private sentinel.
        let beyond_tokio_ticks = Duration::from_millis(u64::MAX - 2);
        let current = std::time::Instant::now();
        let Some(requested) = current.checked_add(beyond_tokio_ticks) else {
            // Some platforms have a narrower Instant domain than Tokio's
            // u64 millisecond tick range, so this boundary cannot be tested.
            return;
        };

        assert_eq!(
            next_timer_deadline(current, requested),
            current.checked_add(MAX_TIMER_SLICE)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_beyond_tokios_tick_range_does_not_fire_at_the_first_slice() {
        let current = super::now();
        let Some(requested) = current.checked_add(Duration::from_millis(u64::MAX - 2)) else {
            // See `deadline_beyond_tokios_tick_range_is_armed_in_a_bounded_slice`.
            return;
        };
        let mut sleep = std::pin::pin!(super::sleep_until_std(requested));
        let mut context = Context::from_waker(Waker::noop());

        assert!(sleep.as_mut().poll(&mut context).is_pending());
        tokio::time::advance(MAX_TIMER_SLICE).await;
        assert!(
            sleep.as_mut().poll(&mut context).is_pending(),
            "finishing an internal slice must not finish the requested sleep"
        );
    }

    #[test]
    fn already_due_clock_limit_needs_no_timer_arm() {
        let edge = latest_representable(std::time::Instant::now());

        assert_eq!(next_timer_deadline(edge, edge), None);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_with_an_unarmable_budget_never_elapses() {
        let now = super::now();
        let flush = latest_representable(now);
        // The paused clock is frozen, so the budget reconstructs the flush
        // deadline exactly and the unarmable-budget guard must engage.
        let budget = flush - now;
        let mut timeout = std::pin::pin!(timeout(budget, std::future::pending::<()>()));
        let mut context = Context::from_waker(Waker::noop());
        // Without the guard, tokio's timeout armed this representable
        // deadline and panicked inside the millisecond round-up.
        assert!(timeout.as_mut().poll(&mut context).is_pending());
    }
}
