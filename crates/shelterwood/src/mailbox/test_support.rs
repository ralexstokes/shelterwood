use shelterwood_core::{ChildId, Incarnation, IncarnationCounter, Membership, ScopeIdentity};

pub(crate) fn mint_actor_membership() -> (Membership, IncarnationCounter) {
    ScopeIdentity::new()
        .mint_membership(&ChildId::from("actor"))
        .expect("membership available")
        .into_pair()
}

pub(crate) fn mint_actor_incarnation() -> Incarnation {
    let (_, mut incarnations) = mint_actor_membership();
    incarnations.mint().expect("incarnation available")
}
