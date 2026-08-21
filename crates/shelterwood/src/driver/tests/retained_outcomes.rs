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

/// The other fold in `handle_exit`: a task that recorded an application
/// failure and then panicked on its way out of the runtime. `Panicked`
/// outranks `Failed`, so the join verdict is published and the recorded
/// application error is the loser — a user value the framework destroys with
/// no other observer, which is why its destruction thread is what pins the
/// retention.
#[crate::runtime::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_join_panic_disposes_the_recorded_application_failure_off_the_driver() {
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new(|_| future::pending::<crate::ExitResult>()).restart(RestartPolicy::new(
            RestartCondition::Never,
            Backoff::Immediate,
        )),
    )
    .expect("valid task");
    let fixture = OrderedScopeFixture::new(tree);
    let root = Arc::clone(&fixture.root);
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state_and_startup(ScopeState::Running, Ok(()));
    let key = fixture.children.keys().next().expect("one child plan");
    let (mut scope, mut event_receiver) = fixture.with_lifecycle(ScopeLifecycle::running()).build();

    scope.spawn_child(key);
    let active = scope.children[key]
        .active
        .as_ref()
        .expect("worker is active");
    let incarnation = active.incarnation;
    active.abort_handle.abort();
    let _joined = recv_child_exit(
        &mut event_receiver,
        DRIVER_PROGRESS_WAIT,
        "the aborted fixture task to join",
    )
    .await;

    let (recorded, observed) = thread_reporting_error();
    // Sample after the last await: the fold below runs inline on whichever
    // worker this multi-thread test task currently occupies.
    let driver_thread = std::thread::current().id();
    scope.handle_exit(
        key,
        incarnation,
        Some(RetainedRecordedOutcome::new(RecordedOutcome::returned(
            Err(recorded),
        ))),
        crate::runtime::JoinOutcome::Panic {
            message: Some("join panic".to_owned()),
        },
        Cancellation::NotObserved,
        false,
    );

    assert_ne!(
        observed
            .recv_timeout(DRIVER_PROGRESS_WAIT)
            .expect("the losing recorded failure is disposed"),
        driver_thread,
        "the losing application error must not be destroyed on the driver"
    );
}
