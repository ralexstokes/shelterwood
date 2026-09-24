use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
    time::Instant,
};

use crate::identity::MonotonicCounter;

impl PartialEq for DeadlineEntry {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.handle == other.handle
    }
}

impl Eq for DeadlineEntry {}

impl PartialOrd for DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeadlineEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.handle.cmp(&self.handle))
    }
}

/// The engine's single deadline priority queue.
#[derive(Debug)]
pub struct DeadlineQueue<K> {
    // Keys are both registration identity and equal-deadline arming order.
    // They are never reused, so a stale handle can only miss.
    registration_ids: MonotonicCounter,
    entries: BinaryHeap<DeadlineEntry>,
    registrations: BTreeMap<DeadlineHandle, K>,
}

/// A never-reused registration for one armed deadline.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DeadlineHandle(u64);

#[derive(Debug)]
struct DeadlineEntry {
    at: Instant,
    handle: DeadlineHandle,
}

impl<K> Default for DeadlineQueue<K> {
    fn default() -> Self {
        Self {
            registration_ids: MonotonicCounter::new(),
            entries: BinaryHeap::new(),
            registrations: BTreeMap::new(),
        }
    }
}

impl<K> DeadlineQueue<K> {
    pub fn push(&mut self, at: Instant, key: K) -> DeadlineHandle {
        let handle = self.next_handle();
        let _ = self.registrations.insert(handle, key);
        self.entries.push(DeadlineEntry { at, handle });
        handle
    }

    pub fn cancel(&mut self, handle: DeadlineHandle) -> bool {
        let removed = self.take(handle).is_some();
        if removed {
            self.compact_if_sparse();
        }
        removed
    }

    pub fn next_deadline(&mut self) -> Option<Instant> {
        self.prune_stale_head();
        self.entries.peek().map(|entry| entry.at)
    }

    pub fn pop_due(&mut self, now: Instant) -> Option<K> {
        self.prune_stale_head();
        if self.entries.peek().is_some_and(|entry| entry.at <= now) {
            let entry = self.entries.pop().expect("the due entry was just observed");
            self.take(entry.handle)
        } else {
            None
        }
    }

    fn next_handle(&mut self) -> DeadlineHandle {
        DeadlineHandle(self.registration_ids.mint())
    }

    fn take(&mut self, handle: DeadlineHandle) -> Option<K> {
        self.registrations.remove(&handle)
    }

    fn is_active(&self, handle: DeadlineHandle) -> bool {
        self.registrations.contains_key(&handle)
    }

    fn prune_stale_head(&mut self) {
        while self
            .entries
            .peek()
            .is_some_and(|entry| !self.is_active(entry.handle))
        {
            self.entries.pop();
        }
    }

    fn compact_if_sparse(&mut self) {
        if self.entries.len() > self.registrations.len().saturating_mul(2) {
            let registrations = &self.registrations;
            self.entries
                .retain(|entry| registrations.contains_key(&entry.handle));
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn len(&self) -> usize {
        self.registrations.len()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn storage_len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use super::DeadlineQueue;

    #[test]
    fn one_priority_queue_orders_deadlines_then_equal_deadline_fifo() {
        let now = Instant::now();
        let later = now + Duration::from_secs(1);
        let mut deadlines = DeadlineQueue::default();
        deadlines.push(later, "later-first");
        deadlines.push(now, "now-first");
        deadlines.push(later, "later-second");
        deadlines.push(now, "now-second");

        assert_eq!(deadlines.next_deadline(), Some(now));
        assert_eq!(deadlines.pop_due(now), Some("now-first"));
        assert_eq!(deadlines.pop_due(now), Some("now-second"));
        assert_eq!(deadlines.pop_due(now), None);
        assert_eq!(deadlines.next_deadline(), Some(later));
        assert_eq!(deadlines.pop_due(later), Some("later-first"));
        assert_eq!(deadlines.pop_due(later), Some("later-second"));
    }

    #[test]
    fn cancelled_deadlines_release_keys_and_bound_heap_storage() {
        let far_future = Instant::now() + Duration::from_secs(60 * 60);
        let mut deadlines = DeadlineQueue::default();
        let persistent = deadlines.push(far_future, "persistent");

        for _ in 0..10_000 {
            let cancelled = deadlines.push(far_future, "cancelled");
            assert!(deadlines.cancel(cancelled));
            assert_eq!(
                deadlines.len(),
                1,
                "the registration map keeps only live payloads"
            );
            assert!(
                deadlines.storage_len() <= 2,
                "heap tombstones must stay proportional to live deadlines"
            );
        }

        assert!(deadlines.cancel(persistent));
        assert_eq!(deadlines.len(), 0);
        assert_eq!(deadlines.storage_len(), 0);
    }

    #[test]
    fn cancellation_and_queue_drop_own_payloads_while_stale_handles_stay_absent() {
        struct DropProbe(Arc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let mut deadlines = DeadlineQueue::default();
        let stale = deadlines.push(Instant::now(), DropProbe(Arc::clone(&drops)));
        assert!(deadlines.cancel(stale));
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "cancellation drops promptly"
        );

        let current = deadlines.push(Instant::now(), DropProbe(Arc::clone(&drops)));
        assert!(
            current > stale,
            "deadline keys are monotonic and never reused"
        );
        assert!(!deadlines.cancel(stale), "a stale handle remains absent");
        assert_eq!(
            deadlines.len(),
            1,
            "stale cancellation preserves the live key"
        );
        drop(deadlines);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn cancelling_the_earliest_deadline_recomputes_the_next_wake() {
        let now = Instant::now();
        let mut deadlines = DeadlineQueue::default();
        let earliest = deadlines.push(now + Duration::from_secs(1), "cancelled");
        deadlines.push(now + Duration::from_secs(2), "live");

        assert_eq!(
            deadlines.next_deadline(),
            Some(now + Duration::from_secs(1))
        );
        assert!(deadlines.cancel(earliest));
        assert_eq!(
            deadlines.next_deadline(),
            Some(now + Duration::from_secs(2))
        );
        assert_eq!(
            deadlines.pop_due(now + Duration::from_secs(2)),
            Some("live")
        );
    }
}
