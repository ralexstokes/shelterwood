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

static NEXT_LINEAGE: AtomicU64 = AtomicU64::new(0);

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
    pub(crate) fn validate(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        if value.is_empty() {
            Err(IdError::Empty)
        } else {
            Ok(Self(Arc::from(value)))
        }
    }

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IdError {
    Empty,
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
    /// return `false`.
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
    const ZERO: Self = Self(0);
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
#[derive(Debug)]
struct FenceCounter {
    lineage: Lineage,
    current: Generation,
}

impl FenceCounter {
    fn new(lineage: Lineage) -> Self {
        Self {
            lineage,
            current: Generation::ZERO,
        }
    }

    fn from_fence(fence: Fence) -> Self {
        Self {
            lineage: fence.lineage,
            current: fence.generation,
        }
    }

    fn sequence() -> Self {
        Self::new(Lineage(0))
    }

    #[cfg(test)]
    fn near_exhaustion(lineage: u64) -> Self {
        Self {
            lineage: Lineage(lineage),
            current: Generation(u64::MAX - 2),
        }
    }

    fn mint(&mut self) -> Option<Fence> {
        let next = self.current.get().checked_add(1)?;
        let Some(generation) = Generation::new(next) else {
            self.current = Generation::POISON;
            return None;
        };
        self.current = generation;
        Some(Fence {
            lineage: self.lineage,
            generation,
        })
    }
}

/// A membership and the generation counter that can mint only its incarnations.
#[derive(Debug)]
pub(crate) struct IncarnationCounter {
    membership: Membership,
    counter: FenceCounter,
}

impl IncarnationCounter {
    pub(crate) fn mint(&mut self) -> Option<Incarnation> {
        self.counter.mint().map(|fence| Incarnation {
            membership: self.membership,
            generation: fence.generation,
        })
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
        let lineage = NEXT_LINEAGE
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                let next = current.checked_add(1)?;
                (next != u64::MAX).then_some(next)
            })
            .ok()?
            + 1;
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

    pub(crate) fn mint_membership(&mut self, id: &ChildId) -> Option<Membership> {
        match self.memberships.entry(id.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().mint().map(Membership),
            Entry::Vacant(entry) => {
                let mut counter = Self::fresh_counter()?;
                let membership = counter.mint().map(Membership)?;
                entry.insert(counter);
                Some(membership)
            }
        }
    }

    /// Reconciles a declaration-time membership with this stable scope.
    ///
    /// The first declaration of an id donates its already-minted lineage so
    /// pre-spawn handles retain their identity. Later declarations of that id
    /// (including declarations produced after a scope restart) mint from the
    /// retained counter and therefore supersede their predecessors.
    pub(crate) fn adopt_or_mint_membership(
        &mut self,
        id: &ChildId,
        provisional: Membership,
    ) -> Option<Membership> {
        match self.memberships.entry(id.clone()) {
            Entry::Occupied(mut entry) => entry.get_mut().mint().map(Membership),
            Entry::Vacant(entry) => {
                debug_assert_ne!(provisional.0.generation, Generation::POISON);
                entry.insert(FenceCounter::from_fence(provisional.0));
                Some(provisional)
            }
        }
    }

    pub(crate) fn incarnation_counter(&self, membership: Membership) -> IncarnationCounter {
        // Membership already supplies the complete incarnation lineage. The
        // per-membership counter therefore needs only an ordered generation.
        IncarnationCounter {
            membership,
            counter: FenceCounter::sequence(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ChildId;

    use super::{FenceCounter, Generation, IncarnationCounter, ScopeIdentity};

    #[test]
    fn cross_scope_tokens_fail_closed() {
        let mut left = ScopeIdentity::new();
        let mut right = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let left_member = left.mint_membership(&id).expect("membership available");
        let right_member = right.mint_membership(&id).expect("membership available");

        assert_ne!(left_member, right_member);
        assert!(!left_member.supersedes(right_member));
        assert!(!right_member.supersedes(left_member));
    }

    #[test]
    fn membership_and_incarnation_order_is_scoped_by_owner_and_id() {
        let mut scope = ScopeIdentity::new();
        let id = ChildId::from("worker");
        let first = scope.mint_membership(&id).expect("membership available");
        let second = scope.mint_membership(&id).expect("membership available");
        assert!(second.supersedes(first));
        assert!(!first.supersedes(second));

        let other = scope
            .mint_membership(&ChildId::from("other"))
            .expect("other membership available");
        assert!(!other.supersedes(first));
        assert!(!first.supersedes(other));

        let mut generations = scope.incarnation_counter(first);
        let a = generations.mint().expect("incarnation available");
        let b = generations.mint().expect("incarnation available");
        assert!(b.supersedes(a));
        assert_eq!(a.membership(), first);
        assert!(
            !b.supersedes(
                scope
                    .incarnation_counter(second)
                    .mint()
                    .expect("incarnation available")
            )
        );
    }

    #[test]
    fn stable_scope_adopts_the_first_declaration_then_orders_rebuilds() {
        let id = ChildId::from("worker");
        let other_id = ChildId::from("other");
        let mut first_builder = ScopeIdentity::new();
        let first = first_builder
            .mint_membership(&id)
            .expect("first provisional membership available");
        let other = first_builder
            .mint_membership(&other_id)
            .expect("other provisional membership available");
        let mut rebuilt_builder = ScopeIdentity::new();
        let rebuilt = rebuilt_builder
            .mint_membership(&id)
            .expect("rebuilt provisional membership available");

        let mut stable = ScopeIdentity::new();
        assert_eq!(
            stable.adopt_or_mint_membership(&id, first),
            Some(first),
            "first lowering preserves pre-spawn identity"
        );
        assert_eq!(
            stable.adopt_or_mint_membership(&other_id, other),
            Some(other),
            "each id adopts its own provisional lineage"
        );
        let successor = stable
            .adopt_or_mint_membership(&id, rebuilt)
            .expect("stable identity mints a rebuilt successor");

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
            .expect("last usable membership is minted");
        assert!(scope.mint_membership(&id).is_none());
        assert!(scope.mint_membership(&id).is_none());

        let other = MembershipFixture::at(TEST_LINEAGE, u64::MAX);
        assert_ne!(last, other);
        assert!(!last.supersedes(other));
        assert!(!other.supersedes(last));

        let unrelated = scope
            .mint_membership(&ChildId::from("other"))
            .expect("one exhausted id does not poison an unrelated domain");
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
            .expect("provisional membership available");

        assert_eq!(stable.adopt_or_mint_membership(&id, provisional), None);
        assert_eq!(stable.adopt_or_mint_membership(&id, provisional), None);
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
            .expect("membership available");
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
