use std::{fmt, sync::mpsc, time::Duration};

use super::support::*;

struct PanickingDrop(mpsc::SyncSender<()>);

impl fmt::Debug for PanickingDrop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PanickingDrop")
    }
}

impl fmt::Display for PanickingDrop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("panicking drop")
    }
}

impl std::error::Error for PanickingDrop {}

impl Drop for PanickingDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
        panic!("recorded outcome payload destructor");
    }
}

/// Drives the drain directly rather than `run_scope`'s dispatch loop: the fix
/// is a type change on `ChildEvent::Exited`, so what needs pinning is the
/// carrier's behaviour inside an unwind, not the driver's suffix guard.
#[test]
fn pending_drain_suffix_contains_failed_outcome_destruction_during_unwind() {
    let child = ChildKey::fixture(1);
    let id = ChildId::from("worker");
    let mut identity = ScopeIdentity::new();
    let minted = identity
        .mint_membership(&id)
        .expect("membership is available");
    let (_, mut incarnations) = minted.into_pair();
    let incarnation = incarnations.mint().expect("incarnation is available");
    let (dropped, observed) = mpsc::sync_channel(1);
    let exited = Pending::Child(ChildEvent::Exited {
        child,
        incarnation,
        recorded: Some(RetainedRecordedOutcome::new(RecordedOutcome::returned(
            Err(ExitError::from(PanickingDrop(dropped))),
        ))),
        join: crate::runtime::JoinOutcome::Ok { value: () },
        cancellation: Cancellation::NotObserved,
        readiness_signal_seen: false,
    });

    let unwind = catch_unwind(AssertUnwindSafe(|| {
        let mut pending = vec![Pending::Shutdown.classified(), exited.classified()];
        let mut drain = pending.drain(..);
        let _ = drain.next().expect("the first event starts dispatch");
        panic!("driver dispatch panic");
    }));
    let payload = unwind.expect_err("the primary dispatch panic is caught");
    assert_eq!(
        payload.downcast_ref::<&'static str>().copied(),
        Some("driver dispatch panic"),
        "the primary dispatch panic survives"
    );
    observed
        .recv_timeout(Duration::from_secs(10))
        .expect("the drained recorded outcome is disposed");
}
