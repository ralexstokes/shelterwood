//! What an out-of-crate consumer can reach in the identity module.
//!
//! `#[doc(hidden)]` on the minting-authority family is a *documentation*
//! marker, not a visibility one, so nothing here can pin it — core has no
//! public-surface audit lane (recorded on #436 as a deferred probe, not part
//! of this fix). What this file does pin is the half that is mechanical: the
//! three supported value types stay reachable and keyed, and the
//! reconciliation token stays linear so a lineage cannot be donated twice.

use std::hash::Hash;

use shelterwood_core::{
    ChildId, Incarnation, Membership, MembershipReconciliation, ProvisionalMembership,
    ScopeIdentity,
};

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Check<T: ?Sized>(std::marker::PhantomData<T>);
            trait AmbiguousIfImpl<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for Check<T> {}
            impl<T: ?Sized + $trait> AmbiguousIfImpl<u8> for Check<T> {}
            let _ = <Check<$type> as AmbiguousIfImpl<_>>::check;
        };
    };
}

// Copying the token would let one minted lineage seed two stable ids, which
// is the cross-domain donation the id binding exists to remove.
assert_not_impl!(ProvisionalMembership: Clone);

fn assert_copy_identity<T: Copy + Eq + Hash + Send + Sync>() {}

#[test]
fn supported_identity_values_remain_plain_public_types() {
    assert_copy_identity::<Membership>();
    assert_copy_identity::<Incarnation>();
    let id = ChildId::from("worker");
    assert_eq!(id.as_str(), "worker");
}

#[test]
fn provisional_membership_selects_its_minting_child_id() {
    let worker_id = ChildId::from("worker");
    let other_id = ChildId::from("other");
    let mut declaration = ScopeIdentity::new();
    let (declared, provisional, _) = declaration
        .mint_membership(&worker_id)
        .expect("declaration membership available")
        .into_provisional_parts();

    let mut stable = ScopeIdentity::new();
    assert!(matches!(
        stable.adopt_or_mint_membership(provisional),
        MembershipReconciliation::Adopted
    ));

    let worker_successor = stable
        .mint_membership(&worker_id)
        .expect("adopted worker lineage remains mintable")
        .into_pair()
        .0;
    let other = stable
        .mint_membership(&other_id)
        .expect("unrelated child identity remains mintable")
        .into_pair()
        .0;

    assert!(worker_successor.supersedes(declared));
    assert!(!other.supersedes(declared));
    assert!(!declared.supersedes(other));
}
