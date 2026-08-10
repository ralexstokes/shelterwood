//! Non-driver storage primitives used by the mutable runtime shell.

use std::{
    collections::BTreeMap,
    ops::{Bound, Index, IndexMut},
};

use crate::identity::PoisonedCounter;

/// An exactly-once synchronous completion.
///
/// The orderly path consumes the payload with [`Self::complete`]. If that
/// path is destroyed before doing so, dropping the obligation executes the
/// fail-closed fallback instead. Fallbacks must never await or join.
#[must_use = "dropping an obligation executes its fallback completion"]
pub(super) struct Obligation<T> {
    payload: Option<T>,
    fallback: fn(T),
}

impl<T> Obligation<T> {
    pub(super) fn new(payload: T, fallback: fn(T)) -> Self {
        Self {
            payload: Some(payload),
            fallback,
        }
    }

    pub(super) fn payload_mut(&mut self) -> &mut T {
        self.payload
            .as_mut()
            .expect("a completed obligation has no payload")
    }

    pub(super) fn complete(&mut self, completion: impl FnOnce(T)) {
        if let Some(payload) = self.payload.take() {
            completion(payload);
        }
    }

    pub(super) fn discharge(&mut self) {
        if let Some(payload) = self.payload.take() {
            (self.fallback)(payload);
        }
    }
}

impl<T> Drop for Obligation<T> {
    fn drop(&mut self) {
        self.discharge();
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ChildKey(pub(super) u64);

pub(super) struct ChildArena<T> {
    // Keys are insertion-order ids and are never reused. A late event can
    // therefore miss; it can never address a subsequently inserted child.
    children: BTreeMap<ChildKey, T>,
    // `u64::MAX` is poison and is never minted. Once exhausted, every later
    // insertion fails closed instead of wrapping into the live key domain.
    keys: PoisonedCounter,
}

impl<T> Default for ChildArena<T> {
    fn default() -> Self {
        Self {
            children: BTreeMap::new(),
            keys: PoisonedCounter::new(),
        }
    }
}

impl<T> ChildArena<T> {
    pub(super) fn insert(&mut self, child: T) -> Result<ChildKey, Box<T>> {
        let Some(next) = self.keys.mint() else {
            return Err(Box::new(child));
        };
        let key = ChildKey(next);
        let replaced = self.children.insert(key, child);
        debug_assert!(replaced.is_none(), "monotonic child keys are never reused");
        Ok(key)
    }

    pub(super) fn get(&self, key: ChildKey) -> Option<&T> {
        self.children.get(&key)
    }

    pub(super) fn get_mut(&mut self, key: ChildKey) -> Option<&mut T> {
        self.children.get_mut(&key)
    }

    pub(super) fn remove(&mut self, key: ChildKey) -> Option<T> {
        self.children.remove(&key)
    }

    pub(super) fn keys(&self) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children.keys().copied()
    }

    pub(super) fn keys_after(
        &self,
        key: ChildKey,
    ) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children
            .range((Bound::Excluded(key), Bound::Unbounded))
            .map(|(key, _)| *key)
    }

    pub(super) fn previous_key(&self, key: ChildKey) -> Option<ChildKey> {
        self.children.range(..key).next_back().map(|(key, _)| *key)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (ChildKey, &T)> {
        self.children.iter().map(|(key, child)| (*key, child))
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.children.values()
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.children.values_mut()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.children.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.children.clear();
    }

    #[cfg(test)]
    pub(super) fn storage_len(&self) -> usize {
        self.children.len()
    }
}

impl<T> Index<ChildKey> for ChildArena<T> {
    type Output = T;

    fn index(&self, key: ChildKey) -> &Self::Output {
        self.get(key).expect("live child key")
    }
}

impl<T> IndexMut<ChildKey> for ChildArena<T> {
    fn index_mut(&mut self, key: ChildKey) -> &mut Self::Output {
        self.get_mut(key).expect("live child key")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::identity::PoisonedCounter;

    use super::{ChildArena, ChildKey, Obligation};

    fn count_fallback(fallbacks: Arc<AtomicUsize>) {
        fallbacks.fetch_add(1, Ordering::SeqCst);
    }

    fn panic_after_counting(fallbacks: Arc<AtomicUsize>) {
        fallbacks.fetch_add(1, Ordering::SeqCst);
        panic!("injected fallback panic");
    }

    #[test]
    fn obligation_completes_or_falls_back_exactly_once() {
        let fallbacks = Arc::new(AtomicUsize::new(0));
        let mut completed = false;
        let mut orderly = Obligation::new(Arc::clone(&fallbacks), count_fallback);
        orderly.complete(|_| completed = true);
        drop(orderly);
        assert!(completed);
        assert_eq!(fallbacks.load(Ordering::SeqCst), 0);

        drop(Obligation::new(Arc::clone(&fallbacks), count_fallback));
        assert_eq!(fallbacks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn panicking_fallback_consumes_the_payload_before_unwinding() {
        let fallbacks = Arc::new(AtomicUsize::new(0));
        let mut obligation = Obligation::new(Arc::clone(&fallbacks), panic_after_counting);
        assert!(
            catch_unwind(AssertUnwindSafe(|| obligation.discharge())).is_err(),
            "the injected fallback panic reaches the caller"
        );
        drop(obligation);
        assert_eq!(
            fallbacks.load(Ordering::SeqCst),
            1,
            "drop cannot repeat a fallback that panicked"
        );
    }

    #[test]
    fn removed_child_keys_are_never_reused() {
        let mut arena = ChildArena::default();
        let stale = arena.insert("worker").expect("key mints");
        let child = arena.remove(stale).expect("live key removes its child");
        let current = arena.insert(child).expect("key mints");

        assert!(current > stale, "keys advance monotonically across removal");
        assert!(arena.get(stale).is_none());
        assert!(arena.remove(stale).is_none());
        assert!(arena.get(current).is_some());
    }

    #[test]
    fn child_key_exhaustion_poison_is_never_minted() {
        let mut arena = ChildArena {
            children: BTreeMap::new(),
            keys: PoisonedCounter::near_exhaustion(),
        };
        let last = arena.insert("worker").expect("the last usable key mints");
        assert_eq!(last, ChildKey(u64::MAX - 1));
        let child = arena.remove(last).expect("the last usable key is live");

        let child = *arena
            .insert(child)
            .expect_err("the poison key is never minted");
        assert!(arena.keys.is_poisoned());
        assert!(arena.get(ChildKey(u64::MAX)).is_none());
        assert!(
            arena.insert(child).is_err(),
            "the exhausted domain stays poisoned"
        );
    }
}
