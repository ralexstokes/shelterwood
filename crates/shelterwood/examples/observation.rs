//! The subscribe-then-snapshot catch-up protocol: subscribe to lifecycle
//! events first, read `snapshot()` as ground truth, then apply later events
//! as deltas using the watermark rule.

use std::time::Duration;

use shelterwood::{
    ChildState, DynamicTree, LifecycleEventKind, LifecycleItem, RemoveOutcome, TaskDef,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = DynamicTree::new().spawn()?;
    system.wait_started().await?;
    let scope = system.scope();

    // ANCHOR: observation
    // ANCHOR: subscribe_then_snapshot
    // Subscribe first: subscriptions begin "now" and never replay history.
    let mut events = scope.as_scope().subscribe_lifecycle();
    // Read the snapshot after subscribing and treat it as ground truth.
    let ground_truth = scope.as_scope().snapshot();
    assert!(ground_truth.child("worker").is_none());
    // ANCHOR_END: subscribe_then_snapshot

    let worker = scope
        .add_task(
            "worker",
            TaskDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }),
        )
        .await?;

    // ANCHOR: watermark
    let added = loop {
        let item = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await?
            .expect("the stream stays open while the scope handle lives");
        let LifecycleItem::Event(event) = item else {
            // On `Lagged`, restart the protocol from a fresh snapshot.
            unreachable!("this small fixture cannot overflow its buffer");
        };
        // The watermark rule: an event with `seq <= watermark` is already
        // reflected by the ground-truth snapshot and must be discarded.
        let watermark = if event.scope == scope.as_scope().membership() {
            Some(ground_truth.lifecycle_seq)
        } else {
            ground_truth.watermark(event.scope)
        };
        if watermark.is_some_and(|watermark| event.seq <= watermark) {
            continue;
        }
        if let LifecycleEventKind::Added { membership, .. } = event.kind
            && membership == worker.membership()
        {
            break event;
        }
    };
    assert!(
        added.seq > ground_truth.lifecycle_seq,
        "the admission edge postdates the ground-truth snapshot"
    );
    // ANCHOR_END: watermark

    // ANCHOR: wait_for_child
    // Predicates accept every state at or beyond the desired edge — watches
    // conflate intermediate committed states — under a finite budget.
    let child = scope
        .as_scope()
        .wait_for_child(
            "worker",
            |child| matches!(child.state, ChildState::Running) || child.state.is_terminal(),
            Duration::from_secs(10),
        )
        .await?;
    assert_eq!(child.membership, worker.membership());
    // ANCHOR_END: wait_for_child
    // ANCHOR_END: observation

    assert_eq!(scope.remove_task(&worker).await, RemoveOutcome::Removed);
    system.shutdown(Duration::from_secs(5)).await?;
    println!("observed the admission through catch-up and a bounded wait");
    Ok(())
}
