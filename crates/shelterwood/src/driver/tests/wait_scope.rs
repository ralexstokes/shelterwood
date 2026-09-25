use std::{
    task::{Context, Waker},
    time::Duration,
};

use super::super::incarnation::{ScopeWait, ScopeWake, wait_scope};

#[crate::runtime::test]
async fn scope_wait_prefers_signal_when_both_control_futures_are_ready() {
    let (_sender, mut receiver) = crate::runtime::unbounded_mpsc::<()>();

    let wake = wait_scope(
        ScopeWait {
            signal: std::future::ready(()),
            parent_shutdown: std::future::ready(()),
        },
        &mut receiver,
        None,
        None,
    )
    .await;

    assert!(matches!(wake, ScopeWake::Signal));
}

#[crate::runtime::test]
async fn scope_wait_prefers_a_primary_event_over_a_control_backlog() {
    let (sender, mut receiver) = crate::runtime::unbounded_mpsc();
    let (control_sender, mut control_receiver) = crate::runtime::unbounded_mpsc();
    for value in 0..128 {
        assert!(control_sender.send(value).is_ok());
    }
    assert!(sender.send(999).is_ok());

    let wake = wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::pending(),
        },
        &mut receiver,
        Some(&mut control_receiver),
        None,
    )
    .await;

    assert!(matches!(wake, ScopeWake::Message(Some(999))));
    assert_eq!(control_receiver.try_recv(), Some(0));
}

#[crate::runtime::test]
async fn scope_wait_reports_parent_shutdown_directly() {
    let (_sender, mut receiver) = crate::runtime::unbounded_mpsc::<()>();
    let wake = wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::ready(()),
        },
        &mut receiver,
        None,
        None,
    )
    .await;

    assert!(matches!(wake, ScopeWake::ParentShutdown));
}

/// Each arm alone only pins its own `ScopeWake` mapping; the precedence is a
/// property of ties. Walk the chain
/// `signal > parent_shutdown > message > control > deadline` with every
/// weaker arm simultaneously ready, so reordering any nested selection
/// cannot pass.
#[crate::runtime::test]
async fn scope_wait_resolves_simultaneous_arms_in_declaration_order() {
    let elapsed = crate::runtime::now();
    let (sender, mut receiver) = crate::runtime::unbounded_mpsc();
    let (control_sender, mut control_receiver) = crate::runtime::unbounded_mpsc();
    assert!(sender.send(1_u8).is_ok());
    assert!(control_sender.send(2_u8).is_ok());

    let wake = wait_scope(
        ScopeWait {
            signal: std::future::ready(()),
            parent_shutdown: std::future::ready(()),
        },
        &mut receiver,
        Some(&mut control_receiver),
        Some(elapsed),
    )
    .await;
    assert!(matches!(wake, ScopeWake::Signal));

    let wake = wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::ready(()),
        },
        &mut receiver,
        Some(&mut control_receiver),
        Some(elapsed),
    )
    .await;
    assert!(matches!(wake, ScopeWake::ParentShutdown));

    let wake = wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::pending(),
        },
        &mut receiver,
        Some(&mut control_receiver),
        Some(elapsed),
    )
    .await;
    assert!(matches!(wake, ScopeWake::Message(Some(1))));

    let wake = wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::pending(),
        },
        &mut receiver,
        Some(&mut control_receiver),
        Some(elapsed),
    )
    .await;
    assert!(matches!(wake, ScopeWake::ControlMessage(Some(2))));

    let wake = wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::pending(),
        },
        &mut receiver,
        Some(&mut control_receiver),
        Some(elapsed),
    )
    .await;
    assert!(matches!(wake, ScopeWake::Deadline));
}

#[crate::runtime::test(start_paused = true)]
async fn scope_wait_reports_deadline_and_keeps_an_absent_deadline_pending() {
    let (_sender, mut receiver) = crate::runtime::unbounded_mpsc::<()>();
    let deadline = crate::runtime::now() + Duration::from_secs(10);
    let wake = wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::pending(),
        },
        &mut receiver,
        None,
        Some(deadline),
    )
    .await;
    assert!(matches!(wake, ScopeWake::Deadline));

    let (sender, mut receiver) = crate::runtime::unbounded_mpsc();
    let mut waiting = Box::pin(wait_scope(
        ScopeWait {
            signal: std::future::pending(),
            parent_shutdown: std::future::pending(),
        },
        &mut receiver,
        None,
        None,
    ));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(sender.send(7_u8).is_ok());
    assert!(matches!(waiting.await, ScopeWake::Message(Some(7))));
}
