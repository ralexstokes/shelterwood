use std::{error::Error, fmt::Debug};

use shelterwood::{
    ChildSnapshot, ChildState, LifecycleEvent, LifecycleEventKind, LifecycleEvents, LifecycleItem,
    LifecycleTryRecvError, MembershipStatus, ScopeKind, ScopeSnapshot, SnapshotClosed,
    SnapshotReceiver, WaitError,
};

fn assert_debug<T: Debug>() {}
fn assert_error<T: Error>() {}
fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn m4_public_observation_types_obey_the_trait_matrix() {
    assert_debug::<ChildSnapshot>();
    assert_debug::<ChildState>();
    assert_debug::<LifecycleEvent>();
    assert_debug::<LifecycleEventKind>();
    assert_debug::<LifecycleItem>();
    assert_debug::<MembershipStatus>();
    assert_debug::<ScopeKind>();
    assert_debug::<ScopeSnapshot>();
    assert_error::<LifecycleTryRecvError>();
    assert_error::<SnapshotClosed>();
    assert_error::<WaitError>();
    assert_send_sync::<LifecycleEvents>();
    assert_send_sync::<SnapshotReceiver>();
}
