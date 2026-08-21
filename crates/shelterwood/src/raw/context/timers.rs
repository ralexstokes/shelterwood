//! Keyed timer storage for a raw incarnation.
//!
//! The store owns every armed timer's user key and message, mints the arming
//! orders that index them, and is the only place either value is destroyed —
//! so both always retire through [`RawDisposal`] rather than on the actor
//! task.

use std::{
    any::{Any, TypeId},
    collections::{BTreeSet, HashMap, VecDeque, hash_map::RandomState},
    hash::{BuildHasher, Hash},
    time::{Duration, Instant},
};

use crate::{
    identity::PoisonedCounter,
    raw::disposal::{Contained, RawDisposal},
};

pub(super) enum TimerMessage<M> {
    Once(M),
    Interval(M, fn(&M) -> M),
}

/// A timer's index identity, mintable only by the store that installs it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ArmingOrder(u64);

impl ArmingOrder {
    const MAX: Self = Self(u64::MAX);
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct KeyHash(u64);

struct TimerEntry<M> {
    key: Box<dyn Any + Send>,
    /// `None` when the requested delay overflows the clock: a deadline that
    /// never arrives, mirroring the offload path — never "due now".
    deadline: Option<Instant>,
    arming_order: ArmingOrder,
    message: TimerMessage<M>,
    period: Option<Duration>,
}

pub(super) enum IntervalRearm<M> {
    Missing,
    OneShot,
    Interval(M),
}

#[derive(Clone, Copy)]
struct TimerLocation {
    hash: KeyHash,
    index: usize,
}

/// Type-aware keyed timer lookup paired with an independently ordered
/// deadline index.
///
/// Type identity participates in the key hash, so equal values of different
/// key types remain distinct. A hash bucket still verifies erased equality to
/// preserve `Eq` semantics in the unlikely event of a collision.
pub(super) struct TimerStore<M> {
    key_hasher: RandomState,
    arming_orders: PoisonedCounter,
    keyed: HashMap<KeyHash, Vec<TimerEntry<M>>>,
    armings: HashMap<ArmingOrder, KeyHash>,
    deadlines: BTreeSet<(Instant, ArmingOrder)>,
    disposal: RawDisposal,
    #[cfg(test)]
    lookup_probes: usize,
}

#[cfg(test)]
impl<M> Default for TimerStore<M> {
    fn default() -> Self {
        Self::new(RawDisposal::default())
    }
}

impl<M> TimerStore<M> {
    pub(super) fn new(disposal: RawDisposal) -> Self {
        Self {
            key_hasher: RandomState::new(),
            arming_orders: PoisonedCounter::new(),
            keyed: HashMap::new(),
            armings: HashMap::new(),
            deadlines: BTreeSet::new(),
            disposal,
            #[cfg(test)]
            lookup_probes: 0,
        }
    }

    fn hash_key<K: Hash + 'static>(&self, key: &K) -> KeyHash {
        KeyHash(self.key_hasher.hash_one((TypeId::of::<K>(), key)))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.armings.is_empty()
    }

    pub(super) fn clear(&mut self) {
        let keyed = std::mem::take(&mut self.keyed);
        self.armings.clear();
        self.deadlines.clear();
        for entry in keyed.into_values().flatten() {
            self.dispose_entry(entry);
        }
    }

    /// Installs a timer under a freshly minted arming order and returns it.
    ///
    /// Minting is the store's own so an arming order cannot be fabricated,
    /// reused, or installed out of band by the parent context.
    pub(super) fn replace<K>(
        &mut self,
        key: K,
        deadline: Option<Instant>,
        message: TimerMessage<M>,
        period: Option<Duration>,
    ) -> ArmingOrder
    where
        K: Hash + Eq + Send + 'static,
    {
        // Every verdict below is framework-owned. Contain incoming ownership
        // before even minting the arming id so exhaustion cannot unwind a
        // hostile key or message destructor on the framework panic's stack.
        // Hash and equality are user code and need the same boundary.
        let key = Contained::new(key, self.disposal.clone());
        let message = Contained::new(message, self.disposal.clone());
        let Some(arming_order) = self.arming_orders.mint().map(ArmingOrder) else {
            // Dispose both inputs before establishing the diagnostic. Their
            // destructor panics are retained by RawDisposal and cannot replace
            // the arming-space invariant.
            drop(message);
            drop(key);
            panic!("timer arming-order space exhausted");
        };
        let hash = self.hash_key(key.get());
        self.remove_hashed(hash, key.get());
        self.keyed.entry(hash).or_default().push(TimerEntry {
            key: Box::new(key.into_inner()),
            deadline,
            arming_order,
            message: message.into_inner(),
            period,
        });
        let previous = self.armings.insert(arming_order, hash);
        assert!(previous.is_none());
        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, arming_order));
        }
        arming_order
    }

    fn take<K>(&mut self, key: &K) -> Option<TimerEntry<M>>
    where
        K: Hash + Eq + 'static,
    {
        let hash = self.hash_key(key);
        self.take_hashed(hash, key)
    }

    fn take_hashed<K>(&mut self, hash: KeyHash, key: &K) -> Option<TimerEntry<M>>
    where
        K: Eq + 'static,
    {
        #[cfg(test)]
        let mut probes = 0;
        let location = self.locate(hash, |entry| {
            #[cfg(test)]
            {
                probes += 1;
            }
            entry.key.downcast_ref::<K>() == Some(key)
        })?;
        #[cfg(test)]
        {
            self.lookup_probes = self.lookup_probes.saturating_add(probes);
        }
        Some(self.unlink(location))
    }

    pub(super) fn remove<K>(&mut self, key: &K) -> bool
    where
        K: Hash + Eq + 'static,
    {
        let Some(entry) = self.take(key) else {
            return false;
        };
        self.dispose_entry(entry);
        true
    }

    fn remove_hashed<K>(&mut self, hash: KeyHash, key: &K) -> bool
    where
        K: Eq + 'static,
    {
        let Some(entry) = self.take_hashed(hash, key) else {
            return false;
        };
        self.dispose_entry(entry);
        true
    }

    pub(super) fn clear_and_dispose<K>(&mut self, key: K, message: M)
    where
        K: Hash + Eq + Send + 'static,
    {
        // A zero-period interval still invokes user Hash/Eq while it owns the
        // rejected inputs. Keep both values contained through that lookup.
        let key = Contained::new(key, self.disposal.clone());
        let message = Contained::new(message, self.disposal.clone());
        self.remove(key.get());
        drop(key);
        drop(message);
    }

    fn dispose_entry(&self, entry: TimerEntry<M>) {
        let TimerEntry { key, message, .. } = entry;
        self.disposal.dispose(key);
        self.disposal.dispose(message);
    }

    fn locate(
        &self,
        hash: KeyHash,
        mut predicate: impl FnMut(&TimerEntry<M>) -> bool,
    ) -> Option<TimerLocation> {
        let index = self.keyed.get(&hash)?.iter().position(&mut predicate)?;
        Some(TimerLocation { hash, index })
    }

    fn unlink(&mut self, location: TimerLocation) -> TimerEntry<M> {
        let (entry, empty) = {
            let bucket = self
                .keyed
                .get_mut(&location.hash)
                .expect("a timer location must reference a key bucket");
            let entry = bucket.swap_remove(location.index);
            (entry, bucket.is_empty())
        };
        // `replace` writes the key bucket and the arming index in one window
        // that cannot panic between them — the user `Hash`/`Eq` callbacks run
        // strictly before it — and an entry never migrates buckets except
        // through this method. Their agreement is structural. Do not diagnose
        // a future regression here: `entry` already owns the user's key and
        // message, so an assertion would destroy both on a framework-panic
        // stack. The cleanup below is total for either recorded hash.
        if empty {
            self.keyed.remove(&location.hash);
        }
        self.armings.remove(&entry.arming_order);
        if let Some(deadline) = entry.deadline {
            self.deadlines.remove(&(deadline, entry.arming_order));
        }
        entry
    }

    /// Resolves an arming index to its key-bucket location.
    ///
    /// `None` means the arming order is unknown, which is an ordinary miss.
    /// A known arming order whose bucket or entry is absent is index
    /// corruption; the two shapes panic distinctly so a failure names which
    /// half of the pair went missing.
    fn locate_arming(&self, arming_order: ArmingOrder) -> Option<TimerLocation> {
        let hash = *self.armings.get(&arming_order)?;
        let index = self
            .keyed
            .get(&hash)
            .expect("an arming index must reference a key bucket")
            .iter()
            .position(|entry| entry.arming_order == arming_order)
            .expect("an arming index must reference a timer");
        Some(TimerLocation { hash, index })
    }

    /// Unlinks a one-shot timer and yields its message for delivery.
    ///
    /// The key is retired through disposal here rather than handed out, so no
    /// caller can destroy a timer's user key on the actor task.
    pub(super) fn take_due_once(&mut self, arming_order: ArmingOrder) -> Option<M> {
        let location = self.locate_arming(arming_order)?;
        let TimerEntry { key, message, .. } = self.unlink(location);
        self.disposal.dispose(key);
        let TimerMessage::Once(message) = message else {
            unreachable!("a non-interval timer must own a one-shot message")
        };
        Some(message)
    }

    fn entry_mut(&mut self, arming_order: ArmingOrder) -> Option<&mut TimerEntry<M>> {
        let location = self.locate_arming(arming_order)?;
        Some(
            self.keyed
                .get_mut(&location.hash)
                .expect("a timer location must reference a key bucket")
                .get_mut(location.index)
                .expect("a timer location must reference a timer"),
        )
    }

    pub(super) fn take_due(&mut self, now: Instant) -> VecDeque<ArmingOrder> {
        let due = self
            .deadlines
            .range(..=(now, ArmingOrder::MAX))
            .copied()
            .collect::<Vec<_>>();
        for deadline in &due {
            self.deadlines.remove(deadline);
        }
        due.into_iter().map(|(_, arming)| arming).collect()
    }

    fn arm_deadline(&mut self, arming_order: ArmingOrder, deadline: Option<Instant>) {
        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, arming_order));
        }
    }

    pub(super) fn rearm_interval(
        &mut self,
        arming_order: ArmingOrder,
        now: Instant,
    ) -> IntervalRearm<M> {
        let (message, deadline) = {
            let Some(entry) = self.entry_mut(arming_order) else {
                return IntervalRearm::Missing;
            };
            let Some(period) = entry.period else {
                return IntervalRearm::OneShot;
            };
            let deadline = crate::deadline::Deadline::after(now, period).instant();
            let TimerMessage::Interval(message, clone_message) = &entry.message else {
                unreachable!("an interval timer must own a message factory")
            };
            // Cloning is user code. Keep the entry's prior deadline intact
            // until it succeeds so the fired batch can retry this arming if
            // the panic escapes `recv` and the raw actor catches it.
            let message = clone_message(message);
            entry.deadline = deadline;
            (message, deadline)
        };
        self.arm_deadline(arming_order, deadline);
        IntervalRearm::Interval(message)
    }

    pub(super) fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }
}

impl<M> Drop for TimerStore<M> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use super::{IntervalRearm, PoisonedCounter, TimerMessage, TimerStore};

    #[derive(Eq, PartialEq)]
    struct CollidingKey(u8);

    impl std::hash::Hash for CollidingKey {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            0_u8.hash(state);
        }
    }

    struct CountingHashKey {
        value: u8,
        hashes: Arc<AtomicUsize>,
    }

    impl PartialEq for CountingHashKey {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }

    impl Eq for CountingHashKey {}

    impl std::hash::Hash for CountingHashKey {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.hashes.fetch_add(1, Ordering::SeqCst);
            std::hash::Hash::hash(&self.value, state);
        }
    }

    struct PanickingEqKey {
        value: u8,
        panic_on_eq: Arc<AtomicBool>,
    }

    impl PartialEq for PanickingEqKey {
        fn eq(&self, other: &Self) -> bool {
            assert!(
                !self.panic_on_eq.load(Ordering::SeqCst),
                "timer key equality panic"
            );
            self.value == other.value
        }
    }

    impl Eq for PanickingEqKey {}

    impl std::hash::Hash for PanickingEqKey {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.value.hash(state);
        }
    }

    struct PanickingHashKey(Arc<AtomicUsize>);

    impl PartialEq for PanickingHashKey {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Eq for PanickingHashKey {}

    impl std::hash::Hash for PanickingHashKey {
        fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
            panic!("timer key hash panic");
        }
    }

    impl Drop for PanickingHashKey {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("timer key destructor panic");
        }
    }

    struct PanickingDropKey(Arc<AtomicUsize>);

    impl PartialEq for PanickingDropKey {
        fn eq(&self, _other: &Self) -> bool {
            true
        }
    }

    impl Eq for PanickingDropKey {}

    impl std::hash::Hash for PanickingDropKey {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            0_u8.hash(state);
        }
    }

    impl Drop for PanickingDropKey {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("timer key destructor panic");
        }
    }

    struct PanickingTimerMessage(Arc<AtomicUsize>);

    impl Drop for PanickingTimerMessage {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("timer message destructor panic");
        }
    }

    fn once(entry: super::TimerEntry<&'static str>) -> &'static str {
        let TimerMessage::Once(message) = entry.message else {
            panic!("expected a live one-shot timer")
        };
        message
    }

    #[test]
    fn timer_replacement_hashes_each_incoming_key_once() {
        let hashes = Arc::new(AtomicUsize::new(0));
        let mut timers = TimerStore::default();

        for message in ["first", "second"] {
            timers.replace(
                CountingHashKey {
                    value: 7,
                    hashes: Arc::clone(&hashes),
                },
                None,
                TimerMessage::Once(message),
                None,
            );
        }

        assert_eq!(hashes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn heterogeneous_keys_keep_exact_identity_and_deadline_order() {
        let start = Instant::now();
        let mut timers = TimerStore::default();
        timers.replace(
            7_u8,
            Some(start + Duration::from_secs(3)),
            TimerMessage::Once("old-u8"),
            None,
        );
        let u16_arming = timers.replace(
            7_u16,
            Some(start + Duration::from_secs(1)),
            TimerMessage::Once("u16"),
            None,
        );
        let replacement = timers.replace(
            7_u8,
            Some(start + Duration::from_secs(2)),
            TimerMessage::Once("new-u8"),
            None,
        );

        assert_eq!(timers.next_deadline(), Some(start + Duration::from_secs(1)));
        assert_eq!(
            timers.take_due(start + Duration::from_secs(3)),
            [u16_arming, replacement],
            "different key types coexist and replacement takes a fresh order"
        );
        assert_eq!(
            timers.take_due_once(u16_arming).expect("u16 timer remains"),
            "u16"
        );
        assert_eq!(
            timers
                .take_due_once(replacement)
                .expect("replacement remains"),
            "new-u8"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn interval_rearm_overflow_makes_the_live_entry_dormant() {
        let now = Instant::now();
        let mut timers = TimerStore::default();
        // Construct the delivery-time edge directly: the interval already
        // fired at a representable deadline, but its next period does not fit
        // in the clock domain.
        let arming = timers.replace(
            "interval",
            Some(now),
            TimerMessage::Interval("tick", Clone::clone),
            Some(Duration::MAX),
        );
        assert_eq!(timers.take_due(now), [arming]);

        assert!(matches!(
            timers.rearm_interval(arming, now),
            IntervalRearm::Interval("tick")
        ));
        assert_eq!(
            timers
                .entry_mut(arming)
                .expect("the dormant interval remains clearable")
                .deadline,
            None
        );
        assert_eq!(
            timers.next_deadline(),
            None,
            "overflow never substitutes an immediate delivery"
        );
        assert!(
            timers.take(&"interval").is_some(),
            "overflow dormancy does not erase the keyed interval"
        );
    }

    #[test]
    fn hash_collision_uses_exact_erased_key_equality() {
        let mut timers = TimerStore::default();
        timers.replace(CollidingKey(1), None, TimerMessage::Once("first"), None);
        timers.replace(CollidingKey(2), None, TimerMessage::Once("second"), None);
        timers.replace(
            CollidingKey(1),
            None,
            TimerMessage::Once("replacement"),
            None,
        );

        assert_eq!(
            once(
                timers
                    .take(&CollidingKey(2))
                    .expect("colliding peer remains registered")
            ),
            "second"
        );
        assert_eq!(
            once(
                timers
                    .take(&CollidingKey(1))
                    .expect("replacement remains registered")
            ),
            "replacement"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn equality_panic_leaves_timer_indexes_and_probe_count_coherent() {
        let panic_on_eq = Arc::new(AtomicBool::new(false));
        let start = Instant::now();
        let deadline = start + Duration::from_secs(1);
        let mut timers = TimerStore::default();
        let arming = timers.replace(
            PanickingEqKey {
                value: 7,
                panic_on_eq: Arc::clone(&panic_on_eq),
            },
            Some(deadline),
            TimerMessage::Once("message"),
            None,
        );
        let query = PanickingEqKey {
            value: 7,
            panic_on_eq: Arc::clone(&panic_on_eq),
        };

        panic_on_eq.store(true, Ordering::SeqCst);
        let panic = catch_unwind(AssertUnwindSafe(|| timers.take(&query)))
            .err()
            .expect("user equality panic escapes the timer lookup");
        assert_eq!(
            panic.downcast_ref::<&'static str>().copied(),
            Some("timer key equality panic")
        );
        assert_eq!(timers.lookup_probes, 0, "a panicked scan is not committed");
        assert_eq!(timers.armings.get(&arming), Some(&timers.hash_key(&query)));
        assert_eq!(timers.next_deadline(), Some(deadline));

        panic_on_eq.store(false, Ordering::SeqCst);
        assert_eq!(
            once(timers.take(&query).expect("the timer remains linked")),
            "message"
        );
        assert_eq!(timers.lookup_probes, 1);
        assert!(timers.is_empty());
        assert!(timers.deadlines.is_empty());
    }

    #[test]
    fn timer_input_cleanup_stays_contained_when_hash_panics() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut timers = TimerStore::default();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            timers.replace(
                PanickingHashKey(Arc::clone(&drops)),
                None,
                TimerMessage::Once(PanickingTimerMessage(Arc::clone(&drops))),
                None,
            );
        }))
        .expect_err("the user key hash panic escapes the timer operation");

        assert_eq!(
            panic.downcast_ref::<&'static str>().copied(),
            Some("timer key hash panic"),
            "the callback panic remains primary"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "both hostile incoming destructors run behind independent boundaries"
        );
        let cleanup = timers
            .disposal
            .panic
            .take()
            .expect("the first destructor panic is retained as cleanup evidence");
        assert!(
            matches!(
                cleanup.downcast_ref::<&'static str>().copied(),
                Some("timer message destructor panic" | "timer key destructor panic")
            ),
            "a hostile incoming destructor is recorded"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn arming_exhaustion_disposes_inputs_before_the_framework_panic() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut timers = TimerStore::default();
        timers.arming_orders = PoisonedCounter::near_exhaustion();
        assert!(
            timers.arming_orders.mint().is_some(),
            "the fixture consumes the final arming id"
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            timers.replace(
                PanickingDropKey(Arc::clone(&drops)),
                None,
                TimerMessage::Once(PanickingTimerMessage(Arc::clone(&drops))),
                None,
            );
        }))
        .expect_err("the exhausted arming domain remains a framework panic");

        assert_eq!(
            panic.downcast_ref::<&'static str>().copied(),
            Some("timer arming-order space exhausted"),
            "hostile input destructors cannot replace the invariant"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "both inputs are disposed before the framework panic begins"
        );
        assert!(
            timers.disposal.panic.take().is_some(),
            "the first hostile destructor panic remains cleanup evidence"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn zero_period_timer_cleanup_stays_contained_when_hash_panics() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut timers = TimerStore::default();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            timers.clear_and_dispose(
                PanickingHashKey(Arc::clone(&drops)),
                PanickingTimerMessage(Arc::clone(&drops)),
            );
        }))
        .expect_err("the user key hash panic escapes the clear operation");

        assert_eq!(
            panic.downcast_ref::<&'static str>().copied(),
            Some("timer key hash panic"),
            "the callback panic remains primary"
        );
        assert_eq!(
            drops.load(Ordering::SeqCst),
            2,
            "both zero-period inputs are destroyed behind containment"
        );
        assert!(
            timers.disposal.panic.take().is_some(),
            "a hostile input destructor is retained as cleanup evidence"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn one_shot_delivery_retires_its_key_through_disposal() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut timers = TimerStore::default();
        let arming = timers.replace(
            PanickingDropKey(Arc::clone(&drops)),
            None,
            TimerMessage::Once("message"),
            None,
        );

        // Delivery hands back only the message. A hostile key destructor is
        // therefore contained here instead of unwinding into the caller on
        // the actor task.
        assert_eq!(
            timers
                .take_due_once(arming)
                .expect("the timer is registered"),
            "message"
        );

        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the timer key is destroyed"
        );
        let cleanup = timers
            .disposal
            .panic
            .take()
            .expect("the key destructor panic is retained as cleanup evidence");
        assert_eq!(
            cleanup.downcast_ref::<&'static str>().copied(),
            Some("timer key destructor panic")
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn unlink_is_total_when_the_arming_hash_disagrees() {
        let mut timers = TimerStore::default();
        let arming = timers.replace(7_u8, None, TimerMessage::Once("message"), None);
        let recorded = timers.armings[&arming];
        timers
            .armings
            .insert(arming, super::KeyHash(recorded.0.wrapping_add(1)));

        assert_eq!(
            once(
                timers
                    .take(&7_u8)
                    .expect("the keyed entry remains removable")
            ),
            "message"
        );
        assert!(timers.is_empty());
    }

    #[test]
    fn keyed_timer_churn_has_one_lookup_probe_per_removal() {
        const TIMERS: usize = 16_384;

        let start = Instant::now();
        let mut timers = TimerStore::default();
        let mut hashes = HashSet::with_capacity(TIMERS);
        let mut keys = Vec::with_capacity(TIMERS);
        let mut candidate = 0_usize;
        while keys.len() < TIMERS {
            if hashes.insert(timers.hash_key(&candidate)) {
                keys.push(candidate);
            }
            candidate = candidate
                .checked_add(1)
                .expect("test key space must contain enough distinct hashes");
        }
        for (index, key) in keys.iter().copied().enumerate() {
            timers.replace(
                key,
                Some(start + Duration::from_secs((TIMERS - index) as u64)),
                TimerMessage::Once(()),
                None,
            );
        }
        for key in keys.into_iter().rev() {
            assert!(timers.remove(&key));
        }

        assert!(timers.is_empty());
        assert!(timers.deadlines.is_empty());
        assert_eq!(
            timers.lookup_probes, TIMERS,
            "distinct hashes need one exact-key check each, not a vector scan"
        );
    }
}
