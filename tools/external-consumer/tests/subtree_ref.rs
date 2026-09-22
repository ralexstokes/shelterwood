use std::{collections::HashSet, fmt::Debug, hash::Hash};

use shelterwood::{
    DynamicScopeRef, DynamicTree, ScopeRef, StaticReserveError, Subtree, SubtreeOnceDef, Tree,
};

/// A subtree's handle type is nameable through the public trait, and its
/// bounds let generic code clone, compare, hash and print it.
fn add_and_collect<T: Subtree>(
    tree: &mut Tree,
    id: &str,
    subtree: T,
) -> Result<HashSet<<T as Subtree>::Ref>, StaticReserveError> {
    let handle: <T as Subtree>::Ref = tree.add_subtree_once(id, SubtreeOnceDef::new(subtree))?;
    assert_handle_bounds(&handle);
    Ok(HashSet::from([handle.clone(), handle]))
}

fn assert_handle_bounds<R: Clone + Debug + Eq + Hash + Send + Sync + 'static>(_: &R) {}

#[test]
fn subtree_handle_types_are_nameable_by_external_generic_code() {
    let mut tree = Tree::new();
    let ordered: HashSet<ScopeRef> =
        add_and_collect(&mut tree, "ordered", Tree::new()).expect("valid ordered subtree");
    let dynamic: HashSet<DynamicScopeRef> =
        add_and_collect(&mut tree, "dynamic", DynamicTree::new()).expect("valid dynamic subtree");
    assert_eq!((ordered.len(), dynamic.len()), (1, 1));
}
