use shelterwood::{LifecycleEvent, LifecycleEvents, LifecycleItem};

pub(crate) async fn next_item(events: &mut LifecycleEvents) -> LifecycleItem {
    tokio::time::timeout(super::POLL_TIMEOUT, events.recv())
        .await
        .expect("lifecycle receive is bounded")
        .expect("lifecycle stream remains open")
}

pub(crate) async fn next_event(events: &mut LifecycleEvents) -> LifecycleEvent {
    match next_item(events).await {
        LifecycleItem::Event(event) => event,
        LifecycleItem::Lagged { dropped } => {
            panic!("unexpected lifecycle lag marker dropping {dropped}")
        }
    }
}
