//! Public-path pins that `Admission` and `Removal` never park the caller's
//! raw waker in Tokio's one-shot: reverting either to the raw receive makes
//! its fixture fail at hostile caller-waker destruction. On the fixed path
//! response publication consumes the installed clone through the proxy wake,
//! so the hostile drop vtable never fires here — contained ready-path
//! destruction and the detached cancellation venue are pinned by
//! `shelterwood-runtime`'s `sync` unit tests instead.

mod common;

use std::{
    future::Future,
    task::{Context, Poll},
    time::Duration,
};

use common::{assert_eventually, hostile_waker, waiting::task as waiting_task};
use shelterwood::{ChildState, DynamicTree, RemoveOutcome};

/// Admission does not enqueue its driver request until first poll. A
/// current-thread runtime therefore makes the pending registration
/// deterministic: no driver task can publish the response during that poll.
/// The later Running projection is downstream of response publication.
#[tokio::test]
async fn successful_admission_never_parks_its_caller_waker_and_returns_the_exact_handle() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let slot = scope.reserve_task("proxied-admission").expect("id is free");
    let expected = slot.task_ref();
    let mut admission = Box::pin(slot.define(waiting_task()));
    let hostile = hostile_waker("injected admission/removal caller-waker drop panic");

    assert!(matches!(
        admission.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Pending
    ));
    assert_eventually!(
        || scope
            .as_scope()
            .child("proxied-admission")
            .is_some_and(|child| matches!(child.state, ChildState::Running)),
        "the driver did not start the admitted task"
    )
    .await;

    let Poll::Ready(Ok(admitted)) = admission.as_mut().poll(&mut Context::from_waker(&hostile))
    else {
        panic!("the published admission response is ready")
    };
    assert_eq!(admitted, expected, "admission returns its reserved handle");

    assert_eq!(scope.remove_task(&admitted).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root stops");
}

/// Exact-handle removal is latched synchronously, so its first manual poll is
/// pending before the current-thread driver can run. Once the Removed edge is
/// visible, response publication follows in the same driver turn. The stale
/// exact handle must still resolve AlreadyAbsent with the hostile caller
/// waker kept out of the one-shot.
#[tokio::test]
async fn exact_removal_never_parks_its_caller_waker_and_preserves_its_outcomes() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let task = scope
        .add_task("proxied-removal", waiting_task())
        .await
        .expect("task is admitted");
    let mut removal = Box::pin(scope.remove_task(&task));
    let hostile = hostile_waker("injected admission/removal caller-waker drop panic");

    assert!(matches!(
        removal.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Pending
    ));
    assert_eventually!(
        || scope.as_scope().child("proxied-removal").is_none(),
        "the driver did not publish the Removed edge"
    )
    .await;
    tokio::task::yield_now().await;

    assert!(matches!(
        removal.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Ready(RemoveOutcome::Removed)
    ));
    let replacement = scope
        .add_task("proxied-removal", waiting_task())
        .await
        .expect("the removed id is reusable");
    assert_eq!(
        scope.remove_task(&task).await,
        RemoveOutcome::AlreadyAbsent,
        "a stale exact handle cannot remove a successor"
    );
    assert_eq!(
        scope.remove_task(&replacement).await,
        RemoveOutcome::Removed,
        "the exact successor handle remains authoritative"
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root stops");
}
