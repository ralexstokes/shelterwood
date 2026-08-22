use shelterwood::{
    Exit, ExitKind, LifecycleEvent, LifecycleEventKind, LifecycleEvents, LifecycleItem,
};

pub(crate) async fn next_item(events: &mut LifecycleEvents) -> LifecycleItem {
    tokio::time::timeout(super::POLL_TIMEOUT, events.recv())
        .await
        .expect("lifecycle receive is bounded")
        .expect("lifecycle stream remains open")
}

/// The next lifecycle event, treating a lag marker as a fixture defect.
///
/// Every caller subscribes to a fixture whose whole event volume is bounded by
/// its own construction — a handful of children, a fixed number of restarts —
/// and reads far more often than the 128-event per-subscriber capacity. Lag is
/// therefore a function of what the fixture publishes, not of scheduling, so a
/// marker means the fixture outgrew its subscription rather than that the
/// machine was busy. Tolerating it would let dropped events silently decide a
/// `saw_x` flag or hide the very edge under test, so it panics here instead.
pub(crate) async fn next_event(events: &mut LifecycleEvents) -> LifecycleEvent {
    match next_item(events).await {
        LifecycleItem::Event(event) => event,
        LifecycleItem::Lagged { dropped } => {
            panic!("unexpected lifecycle lag marker dropping {dropped}")
        }
    }
}

/// Returns the next exit published for `id`, rejecting fixture lag.
///
/// It stops at the *first* matching exit, so it judges one incarnation only.
/// A fixture that restarts the child and means the final exit wants
/// [`last_panic_message`]'s drain-to-closure shape instead.
pub(crate) async fn next_exit_of(events: &mut LifecycleEvents, id: &str) -> Exit {
    loop {
        if let LifecycleEventKind::Exited {
            id: exited, exit, ..
        } = next_event(events).await.kind
            && exited.as_str() == id
        {
            return exit;
        }
    }
}

/// The message of the last `Panicked` exit published for `id`, after draining
/// the lifecycle stream to closure.
///
/// Draining to closure rather than stopping at the first match is what the
/// exit-scanning tests rely on: it proves the stream ends, and it keeps the
/// *final* panic exit, so a child that produced more than one incarnation
/// cannot pass by matching an earlier one. `None` means no such exit was
/// published, or the last one carried no message.
pub(crate) async fn last_panic_message(events: &mut LifecycleEvents, id: &str) -> Option<String> {
    let drain = async {
        let mut message = None;
        while let Some(item) = events.recv().await {
            let LifecycleItem::Event(event) = item else {
                panic!("unexpected lifecycle lag marker while draining exits for {id}")
            };
            if let LifecycleEventKind::Exited {
                id: exited, exit, ..
            } = event.kind
                && exited.as_str() == id
                && let ExitKind::Panicked { message: panicked } = exit.kind()
            {
                message = panicked.clone();
            }
        }
        message
    };
    tokio::time::timeout(super::POLL_TIMEOUT, drain)
        .await
        .expect("the lifecycle stream closes once the fixture has stopped")
}
