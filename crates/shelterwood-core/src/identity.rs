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

#[cfg(any(test, feature = "test-util"))]
use std::cell::Cell;

static NEXT_LINEAGE: AtomicMonotonicCounter = AtomicMonotonicCounter::new();

#[cfg(any(test, feature = "test-util"))]
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

/// Lets a borrowed id — typically a handle's `id()` — go wherever an owned
/// one is expected. Ids are shared text, so this is a refcount bump.
impl From<&ChildId> for ChildId {
    fn from(value: &ChildId) -> Self {
        value.clone()
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
    fn get(self) -> u64 {
        self.0
    }

    fn supersedes(self, other: Self) -> bool {
        self.0 > other.0
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

/// A source of fencing generations: strictly increasing, one step per mint.
///
/// `u64` counters that advance by one per operation do not exhaust in
/// practice (one mint per nanosecond reaches the limit in 584 years), so no
/// caller handles exhaustion. The limit is decided here, once: a mint past
/// `u64::MAX` aborts the process rather than wrapping, because a wrapped
/// value would let a fence accept a stale token (SPEC §3.1). An abort does not
/// unwind, so it cannot poison a framework lock or run user code under one.
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct MonotonicCounter {
    current: u64,
}

impl MonotonicCounter {
    pub const fn new() -> Self {
        Self { current: 0 }
    }

    fn from_current(current: u64) -> Self {
        Self { current }
    }

    /// Returns the value minted after `current`, for state machines that
    /// keep their counter inline rather than in a `MonotonicCounter`.
    pub fn successor(current: u64) -> u64 {
        current.checked_add(1).unwrap_or_else(|| exhausted())
    }

    pub fn mint(&mut self) -> u64 {
        self.current = Self::successor(self.current);
        self.current
    }

    pub fn current(&self) -> u64 {
        self.current
    }
}

impl Default for MonotonicCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// A thread-safe [`MonotonicCounter`] with the same abort-at-the-limit rule.
#[derive(Debug)]
#[doc(hidden)]
pub struct AtomicMonotonicCounter(AtomicU64);

impl AtomicMonotonicCounter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn mint(&self, success: Ordering, failure: Ordering) -> u64 {
        // A compare-and-swap, not `fetch_add`: `fetch_add` would store the
        // wrapped value before this thread could abort, and a concurrent mint
        // could return it.
        let previous = self
            .0
            .try_update(success, failure, |current| current.checked_add(1))
            .unwrap_or_else(|_| exhausted());
        previous + 1
    }

    pub fn load(&self, ordering: Ordering) -> u64 {
        self.0.load(ordering)
    }

    #[cfg(test)]
    fn set(&self, value: u64, ordering: Ordering) {
        self.0.store(value, ordering);
    }
}

impl Default for AtomicMonotonicCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// The one overflow branch for every identity counter (SPEC §3.1).
#[cold]
#[inline(never)]
fn exhausted() -> ! {
    std::process::abort()
}

#[derive(Debug)]
struct FenceCounter {
    lineage: Lineage,
    generations: MonotonicCounter,
}

impl FenceCounter {
    fn new(lineage: Lineage) -> Self {
        Self {
            lineage,
            generations: MonotonicCounter::new(),
        }
    }

    fn from_fence(fence: Fence) -> Self {
        Self {
            lineage: fence.lineage,
            generations: MonotonicCounter::from_current(fence.generation.get()),
        }
    }

    fn mint(&mut self) -> Fence {
        Fence {
            lineage: self.lineage,
            generation: Generation(self.generations.mint()),
        }
    }

    fn issued(&self) -> Fence {
        Fence {
            lineage: self.lineage,
            generation: Generation(self.generations.current()),
        }
    }
}

/// A membership and the generation counter that can mint only its incarnations.
#[derive(Debug)]
#[doc(hidden)]
pub struct IncarnationCounter {
    membership: Membership,
    generations: MonotonicCounter,
}

/// Linear authority to reconcile one declaration-time membership.
///
/// The token carries the [`ChildId`] its lineage was minted for, and
/// [`ScopeIdentity::adopt_or_mint_membership`] reads the id from it rather
/// than from a second argument: donating a lineage to a *different* id is
/// therefore unconstructible, not merely unused. Deliberately not `Clone` —
/// one minted lineage may seed at most one stable id, so a copyable token
/// would restore the cross-domain donation the binding removes. The
/// remaining cross-*scope* half (two provisionals for one id, adopted into
/// two stable scopes) cannot be closed by construction and rides on the
/// framework-only ruling that keeps this whole minting family
/// `#[doc(hidden)]`.
#[derive(Debug)]
#[doc(hidden)]
pub struct ProvisionalMembership {
    id: ChildId,
    membership: Membership,
}

/// An inseparable identity grant: the counter for an incarnation lineage is
/// allocated exactly once as its first membership method returns.
#[derive(Debug)]
#[doc(hidden)]
pub struct MintedMembership {
    provisional: ProvisionalMembership,
    incarnation_counter: IncarnationCounter,
}

/// Result of reconciling a provisional membership with a stable scope.
///
/// Dropping a `Minted` outcome strands the slot on its provisional lineage
/// while the stable scope has already issued the successor, so the
/// reconciliation is never optional to consume.
#[derive(Debug)]
#[must_use]
#[doc(hidden)]
pub enum MembershipReconciliation {
    /// The stable scope adopted the provisional lineage unchanged.
    Adopted,
    /// The stable scope already tracked the id and minted its successor.
    Minted(MintedMembership),
}

impl MintedMembership {
    fn new(id: ChildId, membership: Membership) -> Self {
        Self {
            provisional: ProvisionalMembership { id, membership },
            incarnation_counter: IncarnationCounter {
                membership,
                generations: MonotonicCounter::new(),
            },
        }
    }

    /// Returns the child id this lineage was minted for.
    ///
    /// Keeping the id inside the grant is what lets a member cell derive its
    /// own id from its identity instead of accepting the two separately.
    pub fn id(&self) -> &ChildId {
        &self.provisional.id
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn membership(&self) -> Membership {
        self.provisional.membership
    }

    pub fn into_pair(self) -> (Membership, IncarnationCounter) {
        (self.provisional.membership, self.incarnation_counter)
    }

    pub fn into_provisional_parts(self) -> (Membership, ProvisionalMembership, IncarnationCounter) {
        (
            self.provisional.membership,
            self.provisional,
            self.incarnation_counter,
        )
    }
}

impl IncarnationCounter {
    pub fn mint(&mut self) -> Incarnation {
        Incarnation {
            membership: self.membership,
            generation: Generation(self.generations.mint()),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn fixture(membership: Membership) -> Self {
        Self {
            membership,
            generations: MonotonicCounter::new(),
        }
    }
}

/// The identity domain owned by one scope membership.
#[derive(Debug)]
#[doc(hidden)]
pub struct ScopeIdentity {
    memberships: HashMap<ChildId, FenceCounter>,
}

impl ScopeIdentity {
    pub fn new() -> Self {
        #[cfg(any(test, feature = "test-util"))]
        CURRENT_THREAD_SCOPE_CREATIONS.with(|creations| {
            creations.set(creations.get().saturating_add(1));
        });
        Self {
            memberships: HashMap::new(),
        }
    }

    fn fresh_counter() -> FenceCounter {
        FenceCounter::new(Lineage(
            NEXT_LINEAGE.mint(Ordering::Relaxed, Ordering::Relaxed),
        ))
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn current_thread_creations() -> u64 {
        CURRENT_THREAD_SCOPE_CREATIONS.with(Cell::get)
    }

    pub fn mint_membership(&mut self, id: &ChildId) -> MintedMembership {
        let fence = match self.memberships.entry(id.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().mint(),
            Entry::Vacant(entry) => entry.insert(Self::fresh_counter()).mint(),
        };
        MintedMembership::new(id.clone(), Membership(fence))
    }

    /// Reconciles a declaration-time membership with this stable scope.
    ///
    /// The first declaration of an untracked id donates its already-minted
    /// lineage so pre-spawn handles retain their identity. If the scope still
    /// tracks that lineage, a later provisional declaration mints its ordered
    /// successor. Terminalization evicts the lineage, so an ordinary later
    /// remove-and-re-add or post-restart rebuild donates a fresh, incomparable
    /// identity instead.
    ///
    /// The reconciled id comes from the [`ProvisionalMembership`] itself, so
    /// the lineage can only ever be donated to the id it was minted for.
    pub fn adopt_or_mint_membership(
        &mut self,
        provisional: ProvisionalMembership,
    ) -> MembershipReconciliation {
        let ProvisionalMembership { id, membership } = provisional;
        match self.memberships.entry(id) {
            Entry::Occupied(mut entry) => {
                let membership = Membership(entry.get_mut().mint());
                let id = entry.key().clone();
                MembershipReconciliation::Minted(MintedMembership::new(id, membership))
            }
            Entry::Vacant(entry) => {
                entry.insert(FenceCounter::from_fence(membership.0));
                MembershipReconciliation::Adopted
            }
        }
    }

    pub fn evict(&mut self, id: &ChildId, membership: Membership) {
        let Entry::Occupied(entry) = self.memberships.entry(id.clone()) else {
            return;
        };
        if entry.get().issued() == membership.0 {
            entry.remove();
        }
    }
}

impl Default for ScopeIdentity {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::identity::ChildId;

    use super::{
        AtomicMonotonicCounter, Generation, IncarnationCounter, MembershipReconciliation,
        MonotonicCounter, ScopeIdentity,
    };

    #[test]
    fn counters_mint_a_strictly_increasing_sequence_up_to_the_last_value() {
        let mut local = MonotonicCounter::new();
        assert_eq!((local.mint(), local.mint(), local.mint()), (1, 2, 3));
        assert_eq!(local.current(), 3);

        let atomic = AtomicMonotonicCounter::new();
        let first = atomic.mint(Ordering::Relaxed, Ordering::Relaxed);
        assert_eq!(atomic.mint(Ordering::Relaxed, Ordering::Relaxed), first + 1);

        // The limit itself is minted; only a mint past it aborts.
        assert_eq!(MonotonicCounter::successor(u64::MAX - 1), u64::MAX);
        atomic.set(u64::MAX - 1, Ordering::Relaxed);
        assert_eq!(atomic.mint(Ordering::Relaxed, Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn eviction_makes_readds_incomparable_and_stale_eviction_is_harmless() {
        let id = ChildId::from("worker");
        let mut scope = ScopeIdentity::new();
        let first = scope.mint_membership(&id).membership();
        scope.evict(&id, first);
        let second = scope.mint_membership(&id).membership();
        assert!(!first.supersedes(second));
        assert!(!second.supersedes(first));

        scope.evict(&id, first);
        let third = scope.mint_membership(&id).membership();
        assert!(third.supersedes(second));
    }

    #[test]
    fn cross_scope_tokens_fail_closed() {
        let mut left = ScopeIdentity::new();
        let mut right = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let left_member = left.mint_membership(&id).membership();
        let right_member = right.mint_membership(&id).membership();

        assert_ne!(left_member, right_member);
        assert!(!left_member.supersedes(right_member));
        assert!(!right_member.supersedes(left_member));
    }

    #[test]
    fn membership_and_incarnation_order_is_scoped_by_owner_and_id() {
        let mut scope = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let first_grant = scope.mint_membership(&id);
        let first = first_grant.membership();
        assert!(!first.supersedes(first));
        let second_grant = scope.mint_membership(&id);
        let second = second_grant.membership();
        assert!(!second.supersedes(second));
        assert!(second.supersedes(first));
        assert!(!first.supersedes(second));

        let other = scope.mint_membership(&ChildId::from("other")).membership();
        assert!(!other.supersedes(first));
        assert!(!first.supersedes(other));

        let (_, mut generations) = first_grant.into_pair();
        let a = generations.mint();
        let b = generations.mint();
        assert!(!a.supersedes(a));
        assert!(!b.supersedes(b));
        assert!(b.supersedes(a));
        assert_eq!(a.membership(), first);
        assert!(!b.supersedes(second_grant.into_pair().1.mint()));
    }

    #[test]
    fn adopted_membership_orders_a_direct_mint_and_later_rebuild() {
        let id = ChildId::from("worker");
        let mut declaration = ScopeIdentity::new();
        let (provisional_membership, provisional, _) =
            declaration.mint_membership(&id).into_provisional_parts();

        let mut stable = ScopeIdentity::new();
        assert!(matches!(
            stable.adopt_or_mint_membership(provisional),
            MembershipReconciliation::Adopted
        ));
        let direct = stable.mint_membership(&id).membership();

        let mut rebuilt_declaration = ScopeIdentity::new();
        let (rebuilt_membership, rebuilt, _) = rebuilt_declaration
            .mint_membership(&id)
            .into_provisional_parts();
        let MembershipReconciliation::Minted(reconciled) = stable.adopt_or_mint_membership(rebuilt)
        else {
            panic!("an occupied stable identity mints a successor")
        };
        let reconciled = reconciled.membership();

        assert!(direct.supersedes(provisional_membership));
        assert!(reconciled.supersedes(direct));
        assert!(!direct.supersedes(reconciled));
        assert!(!rebuilt_membership.supersedes(reconciled));
        assert!(!reconciled.supersedes(rebuilt_membership));
    }

    #[test]
    fn evicting_an_adopted_lineage_releases_the_id_and_stale_eviction_keeps_it() {
        let id = ChildId::from("worker");
        let mut stable = ScopeIdentity::new();
        let declare = || {
            ScopeIdentity::new()
                .mint_membership(&id)
                .into_provisional_parts()
        };

        // Evicting exactly the adopted membership releases the id: the next
        // declaration donates its own, incomparable lineage instead of
        // minting a successor of the evicted one.
        let (adopted, provisional, _) = declare();
        assert!(matches!(
            stable.adopt_or_mint_membership(provisional),
            MembershipReconciliation::Adopted
        ));
        stable.evict(&id, adopted);
        let (readopted, provisional, _) = declare();
        assert!(matches!(
            stable.adopt_or_mint_membership(provisional),
            MembershipReconciliation::Adopted
        ));
        assert!(!readopted.supersedes(adopted));
        assert!(!adopted.supersedes(readopted));

        // Once a direct mint has succeeded the adopted membership, evicting
        // with the adopted token is stale and leaves the lineage tracked.
        let direct = stable.mint_membership(&id).membership();
        stable.evict(&id, readopted);
        let (_, provisional, _) = declare();
        let MembershipReconciliation::Minted(successor) =
            stable.adopt_or_mint_membership(provisional)
        else {
            panic!("a stale eviction keeps the adopted lineage tracked")
        };
        assert!(successor.membership().supersedes(direct));
    }

    #[test]
    fn generation_ordering_is_typed() {
        let first = Generation::fixture(1);
        let second = Generation::fixture(2);

        assert!(second.supersedes(first));
        assert!(!first.supersedes(second));

        let membership = MembershipFixture::at(1, 1);
        let mut sequence = IncarnationCounter::fixture(membership);
        assert_eq!(sequence.mint().generation.get(), 1);
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
