//! Membership and incarnation identity.

use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::ChildId;

static NEXT_LINEAGE: AtomicU64 = AtomicU64::new(0);

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
    generation: Fence,
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

/// The one ordered fencing value used for membership and incarnation checks.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct Fence {
    lineage: u64,
    generation: u64,
}

impl Fence {
    fn supersedes(self, other: Self) -> bool {
        self.lineage == other.lineage
            && self.generation != u64::MAX
            && other.generation != u64::MAX
            && self.generation > other.generation
    }
}

/// A fail-closed source of fencing generations.
///
/// `u64::MAX` is poison and is never returned. Once the last usable value has
/// been minted, no successor can be minted.
#[derive(Debug)]
pub(crate) struct FenceCounter {
    lineage: u64,
    current: u64,
}

impl FenceCounter {
    pub(crate) fn new(lineage: u64) -> Self {
        Self {
            lineage,
            current: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn near_exhaustion(lineage: u64) -> Self {
        Self {
            lineage,
            current: u64::MAX - 2,
        }
    }

    fn mint(&mut self) -> Option<Fence> {
        let next = self.current.checked_add(1)?;
        if next == u64::MAX {
            self.current = u64::MAX;
            return None;
        }
        self.current = next;
        Some(Fence {
            lineage: self.lineage,
            generation: next,
        })
    }

    /// Mints a generation for a non-identity monotone sequence.
    ///
    /// Observation uses the same saturating, poison-never-minted primitive as
    /// membership and incarnation fencing.
    pub(crate) fn mint_sequence(&mut self) -> Option<u64> {
        self.mint().map(|fence| fence.generation)
    }
}

/// The identity domain owned by one scope membership.
#[derive(Debug)]
pub(crate) struct ScopeIdentity {
    memberships: HashMap<ChildId, FenceCounter>,
}

impl ScopeIdentity {
    pub(crate) fn new() -> Option<Self> {
        Some(Self {
            memberships: HashMap::new(),
        })
    }

    fn fresh_counter() -> Option<FenceCounter> {
        let lineage = NEXT_LINEAGE
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                let next = current.checked_add(1)?;
                (next != u64::MAX).then_some(next)
            })
            .ok()?
            + 1;
        Some(FenceCounter::new(lineage))
    }

    #[cfg(test)]
    pub(crate) fn with_counter(id: ChildId, memberships: FenceCounter) -> Self {
        Self {
            memberships: HashMap::from([(id, memberships)]),
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
                debug_assert_ne!(provisional.0.generation, u64::MAX);
                entry.insert(FenceCounter {
                    lineage: provisional.0.lineage,
                    current: provisional.0.generation,
                });
                Some(provisional)
            }
        }
    }

    pub(crate) fn incarnation_counter(&self, membership: Membership) -> FenceCounter {
        // The membership's complete fence is compressed into a unique local
        // lineage. The membership scope lineage remains part of the mixing, so
        // matching child ids in different scopes cannot compare equal.
        //
        // A nested builder mints its public handles before it is attached to
        // the parent scope. Deriving from the membership itself keeps those
        // handles valid after lowering under the parent's runtime cell.
        let lineage = membership.0.lineage.rotate_left(17) ^ membership.0.generation;
        FenceCounter::new(lineage)
    }

    pub(crate) fn mint_incarnation(
        membership: Membership,
        counter: &mut FenceCounter,
    ) -> Option<Incarnation> {
        counter.mint().map(|generation| Incarnation {
            membership,
            generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ChildId;

    use super::{FenceCounter, ScopeIdentity};

    #[test]
    fn cross_scope_tokens_fail_closed() {
        let mut left = ScopeIdentity::new().expect("scope identity available");
        let mut right = ScopeIdentity::new().expect("scope identity available");
        let id = ChildId::from("worker");
        let left_member = left.mint_membership(&id).expect("membership available");
        let right_member = right.mint_membership(&id).expect("membership available");

        assert_ne!(left_member, right_member);
        assert!(!left_member.supersedes(right_member));
        assert!(!right_member.supersedes(left_member));
    }

    #[test]
    fn membership_and_incarnation_order_is_scoped() {
        let mut scope = ScopeIdentity::new().expect("scope identity available");
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
        let a = ScopeIdentity::mint_incarnation(first, &mut generations)
            .expect("incarnation available");
        let b = ScopeIdentity::mint_incarnation(first, &mut generations)
            .expect("incarnation available");
        assert!(b.supersedes(a));
        assert_eq!(a.membership(), first);
        assert!(
            !b.supersedes(
                ScopeIdentity::mint_incarnation(second, &mut scope.incarnation_counter(second))
                    .expect("incarnation available")
            )
        );
    }

    #[test]
    fn exhaustion_never_mints_the_poison_value_or_a_duplicate() {
        let id = ChildId::from("worker");
        let counter = FenceCounter::near_exhaustion(7);
        let mut scope = ScopeIdentity::with_counter(id.clone(), counter);
        let last = scope
            .mint_membership(&id)
            .expect("last usable membership is minted");
        assert!(scope.mint_membership(&id).is_none());
        assert!(scope.mint_membership(&id).is_none());

        let other = MembershipFixture::at(7, u64::MAX);
        assert_ne!(last, other);
        assert!(!last.supersedes(other));
        assert!(!other.supersedes(last));
    }

    #[test]
    fn incarnation_exhaustion_also_mints_nothing() {
        let mut scope = ScopeIdentity::new().expect("scope identity available");
        let membership = scope
            .mint_membership(&ChildId::from("worker"))
            .expect("membership available");
        let mut incarnations = FenceCounter::near_exhaustion(91);
        let last = ScopeIdentity::mint_incarnation(membership, &mut incarnations)
            .expect("last usable incarnation is minted");
        assert!(ScopeIdentity::mint_incarnation(membership, &mut incarnations).is_none());
        assert!(ScopeIdentity::mint_incarnation(membership, &mut incarnations).is_none());
        assert_eq!(last.membership(), membership);
    }

    struct MembershipFixture;

    impl MembershipFixture {
        fn at(lineage: u64, generation: u64) -> super::Membership {
            super::Membership(super::Fence {
                lineage,
                generation,
            })
        }
    }
}
