use std::{fmt, process::Command, sync::mpsc, time::Duration};

use super::support::*;

const CHILD_ENV: &str = "SHELTERWOOD_RETAINED_OUTCOME_DRAIN_CHILD";

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

fn exercise_pending_drain_suffix() {
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
    assert!(unwind.is_err(), "the primary dispatch panic is caught");
    observed
        .recv_timeout(Duration::from_secs(10))
        .expect("the drained recorded outcome is disposed");
}

#[test]
fn pending_drain_suffix_contains_failed_outcome_destruction_during_unwind() {
    if std::env::var_os(CHILD_ENV).is_some() {
        exercise_pending_drain_suffix();
        return;
    }

    let output = Command::new(std::env::current_exe().expect("unit-test executable"))
        .arg("--exact")
        .arg("driver::tests::retained_outcomes::pending_drain_suffix_contains_failed_outcome_destruction_during_unwind")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .output()
        .expect("retained-outcome subprocess starts");
    assert!(
        output.status.success(),
        "a failed pending-suffix outcome must not double-panic during driver unwind\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
