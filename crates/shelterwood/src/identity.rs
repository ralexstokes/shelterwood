//! Child, membership, and incarnation identity.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[cfg(test)]
use std::cell::Cell;

static NEXT_LINEAGE: AtomicPoisonedCounter = AtomicPoisonedCounter::new();

#[cfg(test)]
thread_local! {
    static CURRENT_THREAD_SCOPE_CREATIONS: Cell<u64> = const { Cell::new(0) };
}

/// A child identifier within one scope.
// Shared text keeps error evidence allocation-free: every rejected send and
// call clones the id, so a clone must be a refcount bump, not a heap copy.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ChildId(Arc<str>);

impl ChildId {
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
        Self(Arc::from(value))
    }
}

impl From<String> for ChildId {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}

/// A child's identity within one supervising scope.
///
/// Membership identity survives incarnation restarts. It does not survive a
/// remove-and-re-add operation, even when the child id is reused.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Membership(Fence);

impl Membership {
    /// Returns `true` when `self` replaced `other` under the same child id and
    /// stable owning scope.
    ///
    /// Tokens for different child ids or owning scopes are incomparable and
    /// return `false`. Terminalization evicts the retained id lineage, so a
    /// later remove-and-re-add is deliberately incomparable in both directions.
    #[must_use]
    pub fn supersedes(self, other: Self) -> bool {
        self.0.supersedes(other.0)
    }
}

/// The identity of one run of a membership.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Incarnation {
    membership: Membership,
    generation: Generation,
}

impl Incarnation {
    /// Returns the membership this incarnation belongs to.
    #[must_use]
    pub fn membership(self) -> Membership {
        self.membership
    }

    /// Returns `true` when `self` is a newer incarnation of `other`.
    ///
    /// Incarnations from different memberships are incomparable and return
    /// `false`.
    #[must_use]
    pub fn supersedes(self, other: Self) -> bool {
        self.membership == other.membership && self.generation.supersedes(other.generation)
    }
}

/// An ordered generation within one identity lineage.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Generation(u64);

impl Generation {
    const POISON: Self = Self(u64::MAX);

    fn new(value: u64) -> Option<Self> {
        (value != Self::POISON.0).then_some(Self(value))
    }

    fn get(self) -> u64 {
        self.0
    }

    fn supersedes(self, other: Self) -> bool {
        self != Self::POISON && other != Self::POISON && self.0 > other.0
    }

    #[cfg(test)]
    fn fixture(value: u64) -> Self {
        Self(value)
    }
}

/// An unordered identity domain shared by every generation in one fence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Lineage(u64);

/// A complete membership fence: its lineage and ordered generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Fence {
    lineage: Lineage,
    generation: Generation,
}

impl Fence {
    fn supersedes(self, other: Self) -> bool {
        self.lineage == other.lineage && self.generation.supersedes(other.generation)
    }
}

/// A fail-closed source of fencing generations.
///
/// `u64::MAX` is poison and is never returned. Once the last usable value has
/// been minted, no successor can be minted.
#[derive(Clone, Debug)]
pub(crate) struct PoisonedCounter {
    current: u64,
}

impl PoisonedCounter {
    pub(crate) const fn new() -> Self {
        Self { current: 0 }
    }

    fn from_current(current: u64) -> Self {
        debug_assert_ne!(current, u64::MAX);
        Self { current }
    }

    /// Returns the next mintable value under the shared poison rule.
    ///
    /// State-owning callers install `u64::MAX` when this returns `None`; state
    /// machines that represent poison out of band use the same decision and
    /// retain their explicit exhausted variant.
    pub(crate) fn minted_after(current: u64) -> Option<u64> {
        let next = current.saturating_add(1);
        (next != u64::MAX).then_some(next)
    }

    #[cfg(test)]
    pub(crate) const fn near_exhaustion() -> Self {
        Self {
            current: u64::MAX - 2,
        }
    }

    pub(crate) fn mint(&mut self) -> Option<u64> {
        // Saturation is the poison transition itself: MAX is never returned,
        // and retaining it makes every later mint fail closed.
        let minted = Self::minted_after(self.current);
        self.current = minted.unwrap_or(u64::MAX);
        minted
    }

    pub(crate) fn current(&self) -> u64 {
        self.current
    }

    #[cfg(test)]
    pub(crate) fn is_poisoned(&self) -> bool {
        self.current == u64::MAX
    }
}

/// A thread-safe [`PoisonedCounter`] with the same fail-closed domain.
#[derive(Debug)]
pub(crate) struct AtomicPoisonedCounter(AtomicU64);

impl AtomicPoisonedCounter {
    pub(crate) const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    #[cfg(test)]
    pub(crate) const fn near_exhaustion() -> Self {
        Self(AtomicU64::new(u64::MAX - 2))
    }

    pub(crate) fn mint(&self, success: Ordering, failure: Ordering) -> Option<u64> {
        let previous = self
            .0
            .try_update(success, failure, |current| {
                // Saturation atomically installs the permanent poison value;
                // MAX is interpreted below and never returned to a caller.
                Some(PoisonedCounter::minted_after(current).unwrap_or(u64::MAX))
            })
            .expect("a poisoned counter update never rejects");
        PoisonedCounter::minted_after(previous)
    }

    pub(crate) fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    #[cfg(test)]
    pub(crate) fn set(&self, value: u64, ordering: Ordering) {
        self.0.store(value, ordering);
    }
}

#[derive(Debug)]
struct FenceCounter {
    lineage: Lineage,
    generations: PoisonedCounter,
}

impl FenceCounter {
    fn new(lineage: Lineage) -> Self {
        Self {
            lineage,
            generations: PoisonedCounter::new(),
        }
    }

    fn from_fence(fence: Fence) -> Self {
        Self {
            lineage: fence.lineage,
            generations: PoisonedCounter::from_current(fence.generation.get()),
        }
    }

    fn sequence() -> Self {
        Self::new(Lineage(0))
    }

    #[cfg(test)]
    fn near_exhaustion(lineage: u64) -> Self {
        Self {
            lineage: Lineage(lineage),
            generations: PoisonedCounter::near_exhaustion(),
        }
    }

    fn mint(&mut self) -> Option<Fence> {
        let generation = Generation::new(self.generations.mint()?)?;
        Some(Fence {
            lineage: self.lineage,
            generation,
        })
    }

    fn issued(&self) -> Fence {
        let generation = if self.generations.current == u64::MAX {
            u64::MAX - 1
        } else {
            self.generations.current
        };
        Fence {
            lineage: self.lineage,
            generation: Generation::new(generation)
                .expect("a live identity cannot expose its poison generation"),
        }
    }
}

/// A membership and the generation counter that can mint only its incarnations.
#[derive(Debug)]
pub(crate) struct IncarnationCounter {
    membership: Membership,
    counter: FenceCounter,
}

/// An inseparable identity grant: the counter for an incarnation lineage is
/// allocated exactly once as its first membership method returns.
#[derive(Debug)]
pub(crate) struct MintedMembership {
    membership: Membership,
    incarnation_counter: IncarnationCounter,
}

impl MintedMembership {
    #[cfg(test)]
    pub(crate) fn membership(&self) -> Membership {
        self.membership
    }

    pub(crate) fn into_pair(self) -> (Membership, IncarnationCounter) {
        (self.membership, self.incarnation_counter)
    }
}

impl IncarnationCounter {
    pub(crate) fn mint(&mut self) -> Option<Incarnation> {
        self.counter.mint().map(|fence| Incarnation {
            membership: self.membership,
            generation: fence.generation,
        })
    }

    #[cfg(test)]
    pub(crate) fn fixture(membership: Membership) -> Self {
        Self {
            membership,
            counter: FenceCounter::sequence(),
        }
    }

    #[cfg(test)]
    pub(crate) fn near_exhaustion(membership: Membership) -> Self {
        Self {
            membership,
            counter: FenceCounter::near_exhaustion(0),
        }
    }
}

/// The identity domain owned by one scope membership.
#[derive(Debug)]
pub(crate) struct ScopeIdentity {
    memberships: HashMap<ChildId, FenceCounter>,
}

impl ScopeIdentity {
    pub(crate) fn new() -> Self {
        #[cfg(test)]
        CURRENT_THREAD_SCOPE_CREATIONS.with(|creations| {
            creations.set(creations.get().saturating_add(1));
        });
        Self {
            memberships: HashMap::new(),
        }
    }

    fn fresh_counter() -> Option<FenceCounter> {
        let lineage = NEXT_LINEAGE.mint(Ordering::Relaxed, Ordering::Relaxed)?;
        Some(FenceCounter::new(Lineage(lineage)))
    }

    #[cfg(test)]
    pub(crate) fn current_thread_creations() -> u64 {
        CURRENT_THREAD_SCOPE_CREATIONS.with(Cell::get)
    }

    #[cfg(test)]
    pub(crate) fn near_exhaustion(id: ChildId, lineage: u64) -> Self {
        Self {
            memberships: HashMap::from([(id, FenceCounter::near_exhaustion(lineage))]),
        }
    }

    pub(crate) fn mint_membership(&mut self, id: &ChildId) -> Option<MintedMembership> {
        let membership = match self.memberships.entry(id.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().mint().map(Membership),
            Entry::Vacant(entry) => {
                let mut counter = Self::fresh_counter()?;
                let membership = counter.mint().map(Membership)?;
                entry.insert(counter);
                Some(membership)
            }
        }?;
        Some(MintedMembership {
            membership,
            incarnation_counter: IncarnationCounter {
                membership,
                counter: FenceCounter::sequence(),
            },
        })
    }

    /// Reconciles a declaration-time membership with this stable scope.
    ///
    /// The first declaration of an untracked id donates its already-minted
    /// lineage so pre-spawn handles retain their identity. If the scope still
    /// tracks that lineage, a later provisional declaration mints its ordered
    /// successor. Terminalization evicts the lineage, so an ordinary later
    /// remove-and-re-add or post-restart rebuild donates a fresh, incomparable
    /// identity instead.
    pub(crate) fn adopt_or_mint_membership(
        &mut self,
        id: &ChildId,
        provisional: Membership,
    ) -> Option<Option<MintedMembership>> {
        match self.memberships.entry(id.clone()) {
            Entry::Occupied(mut entry) => {
                let membership = entry.get_mut().mint().map(Membership)?;
                Some(Some(MintedMembership {
                    membership,
                    incarnation_counter: IncarnationCounter {
                        membership,
                        counter: FenceCounter::sequence(),
                    },
                }))
            }
            Entry::Vacant(entry) => {
                debug_assert_ne!(provisional.0.generation, Generation::POISON);
                entry.insert(FenceCounter::from_fence(provisional.0));
                Some(None)
            }
        }
    }

    pub(crate) fn evict(&mut self, id: &ChildId, membership: Membership) {
        let Entry::Occupied(entry) = self.memberships.entry(id.clone()) else {
            return;
        };
        if entry.get().issued() == membership.0 {
            entry.remove();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::ChildId;

    use super::{
        AtomicPoisonedCounter, FenceCounter, Generation, IncarnationCounter, PoisonedCounter,
        ScopeIdentity,
    };

    #[test]
    fn shared_counter_primitive_poison_is_never_minted() {
        let mut local = PoisonedCounter::near_exhaustion();
        assert_eq!(local.mint(), Some(u64::MAX - 1));
        assert_eq!(local.mint(), None);
        assert_eq!(local.mint(), None);
        assert!(local.is_poisoned());

        let atomic = AtomicPoisonedCounter(std::sync::atomic::AtomicU64::new(u64::MAX - 2));
        assert_eq!(
            atomic.mint(Ordering::Relaxed, Ordering::Relaxed),
            Some(u64::MAX - 1)
        );
        assert_eq!(atomic.mint(Ordering::Relaxed, Ordering::Relaxed), None);
        assert_eq!(atomic.mint(Ordering::Relaxed, Ordering::Relaxed), None);
        assert_eq!(atomic.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn eviction_makes_readds_incomparable_and_stale_eviction_is_harmless() {
        let id = ChildId::from("worker");
        let mut scope = ScopeIdentity::new();
        let first = scope
            .mint_membership(&id)
            .expect("first identity available")
            .membership();
        scope.evict(&id, first);
        let second = scope
            .mint_membership(&id)
            .expect("re-added identity available")
            .membership();
        assert!(!first.supersedes(second));
        assert!(!second.supersedes(first));

        scope.evict(&id, first);
        let third = scope
            .mint_membership(&id)
            .expect("stale eviction preserved the current domain")
            .membership();
        assert!(third.supersedes(second));
    }

    #[test]
    fn cross_scope_tokens_fail_closed() {
        let mut left = ScopeIdentity::new();
        let mut right = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let left_member = left
            .mint_membership(&id)
            .expect("membership available")
            .membership();
        let right_member = right
            .mint_membership(&id)
            .expect("membership available")
            .membership();

        assert_ne!(left_member, right_member);
        assert!(!left_member.supersedes(right_member));
        assert!(!right_member.supersedes(left_member));
    }

    #[test]
    fn membership_and_incarnation_order_is_scoped_by_owner_and_id() {
        let mut scope = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let first_grant = scope.mint_membership(&id).expect("membership available");
        let first = first_grant.membership();
        assert!(!first.supersedes(first));
        let second_grant = scope.mint_membership(&id).expect("membership available");
        let second = second_grant.membership();
        assert!(!second.supersedes(second));
        assert!(second.supersedes(first));
        assert!(!first.supersedes(second));

        let other = scope
            .mint_membership(&ChildId::from("other"))
            .expect("other membership available")
            .membership();
        assert!(!other.supersedes(first));
        assert!(!first.supersedes(other));

        let (_, mut generations) = first_grant.into_pair();
        let a = generations.mint().expect("incarnation available");
        let b = generations.mint().expect("incarnation available");
        assert!(!a.supersedes(a));
        assert!(!b.supersedes(b));
        assert!(b.supersedes(a));
        assert_eq!(a.membership(), first);
        assert!(
            !b.supersedes(
                second_grant
                    .into_pair()
                    .1
                    .mint()
                    .expect("incarnation available")
            )
        );
    }

    #[test]
    fn adopted_membership_orders_a_direct_mint_and_later_rebuild() {
        let id = ChildId::from("worker");
        let mut declaration = ScopeIdentity::new();
        let provisional = declaration
            .mint_membership(&id)
            .expect("provisional membership available")
            .membership();

        let mut stable = ScopeIdentity::new();
        assert!(matches!(
            stable.adopt_or_mint_membership(&id, provisional),
            Some(None)
        ));
        let direct = stable
            .mint_membership(&id)
            .expect("a direct mint follows the adopted generation")
            .membership();

        let mut rebuilt_declaration = ScopeIdentity::new();
        let rebuilt = rebuilt_declaration
            .mint_membership(&id)
            .expect("rebuilt provisional membership available")
            .membership();
        let reconciled = stable
            .adopt_or_mint_membership(&id, rebuilt)
            .expect("stable identity is not exhausted")
            .expect("an occupied stable identity mints a successor")
            .membership();

        assert!(direct.supersedes(provisional));
        assert!(reconciled.supersedes(direct));
        assert!(!direct.supersedes(reconciled));
        assert!(!rebuilt.supersedes(reconciled));
        assert!(!reconciled.supersedes(rebuilt));
    }

    #[test]
    fn stable_scope_adopts_the_first_declaration_then_orders_a_retained_lineage() {
        let id = ChildId::from("worker");
        let other_id = ChildId::from("other");
        let mut first_builder = ScopeIdentity::new();
        let first = first_builder
            .mint_membership(&id)
            .expect("first provisional membership available")
            .membership();
        let other = first_builder
            .mint_membership(&other_id)
            .expect("other provisional membership available")
            .membership();
        let mut rebuilt_builder = ScopeIdentity::new();
        let rebuilt = rebuilt_builder
            .mint_membership(&id)
            .expect("rebuilt provisional membership available")
            .membership();

        let mut stable = ScopeIdentity::new();
        assert!(matches!(
            stable.adopt_or_mint_membership(&id, first),
            Some(None)
        ));
        assert!(matches!(
            stable.adopt_or_mint_membership(&other_id, other),
            Some(None)
        ));
        let successor = stable
            .adopt_or_mint_membership(&id, rebuilt)
            .expect("stable identity is not exhausted")
            .expect("stable identity mints a rebuilt successor")
            .membership();

        assert!(successor.supersedes(first));
        assert!(!first.supersedes(successor));
        assert!(!successor.supersedes(rebuilt));
        assert!(!rebuilt.supersedes(successor));
        assert!(!successor.supersedes(other));
        assert!(!other.supersedes(successor));
    }

    #[test]
    fn exhaustion_never_mints_the_poison_value_or_a_duplicate() {
        const TEST_LINEAGE: u64 = u64::MAX;
        let id = ChildId::from("worker");
        // A fixture lineage this test does not otherwise allocate, so it
        // cannot collide with an unrelated identity created below.
        let mut scope = ScopeIdentity::near_exhaustion(id.clone(), TEST_LINEAGE);
        let last = scope
            .mint_membership(&id)
            .expect("last usable membership is minted")
            .membership();
        assert!(scope.mint_membership(&id).is_none());
        assert!(scope.mint_membership(&id).is_none());

        let other = MembershipFixture::at(TEST_LINEAGE, u64::MAX);
        assert_ne!(last, other);
        assert!(!last.supersedes(other));
        assert!(!other.supersedes(last));

        let unrelated = scope
            .mint_membership(&ChildId::from("other"))
            .expect("one exhausted id does not poison an unrelated domain")
            .membership();
        assert!(!unrelated.supersedes(last));
        assert!(!last.supersedes(unrelated));
    }

    #[test]
    fn exhausted_stable_domain_rejects_a_rebuilt_declaration() {
        let id = ChildId::from("worker");
        let mut stable = ScopeIdentity::near_exhaustion(id.clone(), 7);
        stable
            .mint_membership(&id)
            .expect("last usable stable membership is minted");
        let mut builder = ScopeIdentity::new();
        let provisional = builder
            .mint_membership(&id)
            .expect("provisional membership available")
            .membership();

        assert!(stable.adopt_or_mint_membership(&id, provisional).is_none());
        assert!(stable.adopt_or_mint_membership(&id, provisional).is_none());
        assert!(
            stable.mint_membership(&ChildId::from("other")).is_some(),
            "exhaustion remains local to one child id"
        );
    }

    #[test]
    fn incarnation_exhaustion_also_mints_nothing() {
        let mut scope = ScopeIdentity::new();
        let membership = scope
            .mint_membership(&ChildId::from("worker"))
            .expect("membership available")
            .membership();
        let mut incarnations = IncarnationCounter::near_exhaustion(membership);
        let last = incarnations
            .mint()
            .expect("last usable incarnation is minted");
        assert!(incarnations.mint().is_none());
        assert!(incarnations.mint().is_none());
        assert_eq!(last.membership(), membership);
    }

    #[test]
    fn generation_ordering_is_typed_and_poison_fails_closed() {
        let first = Generation::fixture(1);
        let second = Generation::fixture(2);

        assert!(second.supersedes(first));
        assert!(!first.supersedes(second));
        assert!(!Generation::POISON.supersedes(second));
        assert!(!second.supersedes(Generation::POISON));

        let mut sequence = FenceCounter::sequence();
        assert_eq!(sequence.mint().map(|fence| fence.generation.get()), Some(1));
    }

    struct MembershipFixture;

    impl MembershipFixture {
        fn at(lineage: u64, generation: u64) -> super::Membership {
            super::Membership(super::Fence {
                lineage: super::Lineage(lineage),
                generation: super::Generation::fixture(generation),
            })
        }
    }
}
