use super::support::*;

#[test]
fn owned_report_token_consumes_or_falls_back_once() {
    let shutdown = Latch::default();
    let readiness = CompletionGatedLatch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, readiness.clone());
    token.record(RecordedOutcome::returned(Ok(())));
    shutdown.fire();
    readiness.fire();
    let report = claim.receive();
    assert!(matches!(
        report.outcome,
        Some(outcome) if matches!(outcome.kind(), ExitKind::Completed)
    ));
    assert_eq!(report.cancellation, Cancellation::NotObserved);
    assert!(!report.readiness_signal_seen);

    let shutdown = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    shutdown.fire();
    drop(token);
    let report = claim.receive();
    assert!(report.outcome.is_none());
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[test]
fn owned_report_token_records_prior_cancellation() {
    let shutdown = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    shutdown.fire();
    token.record(RecordedOutcome::returned(Ok(())));
    let report = claim.receive();
    assert!(matches!(
        report.outcome,
        Some(outcome) if matches!(outcome.kind(), ExitKind::Completed)
    ));
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[test]
fn owned_report_token_records_prior_local_stop() {
    let shutdown = Latch::default();
    let local_stop = Latch::default();
    let (token, claim) = report_slot(
        shutdown,
        Some(local_stop.clone()),
        CompletionGatedLatch::default(),
    );
    local_stop.fire();
    token.record(RecordedOutcome::returned(Ok(())));
    assert_eq!(claim.receive().cancellation, Cancellation::Observed);
}

#[test]
fn report_cell_falls_back_while_its_owner_thread_unwinds() {
    let shutdown = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    let worker = std::thread::spawn(move || {
        let _token = token;
        shutdown.fire();
        panic!("inject child-task panic");
    });

    assert!(worker.join().is_err());
    let report = claim.receive();
    assert!(report.outcome.is_none());
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[crate::runtime::test]
async fn cancelled_task_report_cell_is_ready_after_join() {
    let shutdown = Latch::default();
    let entered = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    let task_entered = entered.clone();
    let task = crate::runtime::spawn(async move {
        let _token = token;
        task_entered.fire();
        future::pending::<()>().await;
    });
    entered.fired().await;

    shutdown.fire();
    task.abort_handle().abort();
    assert!(matches!(
        crate::runtime::join(task).await,
        crate::runtime::JoinOutcome::Cancelled
    ));

    let report = claim.receive();
    assert!(report.outcome.is_none());
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[test]
fn owned_report_token_records_readiness_at_completion() {
    let readiness = CompletionGatedLatch::default();
    let (token, claim) = report_slot(Latch::default(), None, readiness.clone());
    readiness.fire();
    token.record(RecordedOutcome::returned(Ok(())));
    assert!(!readiness.fire(), "completion closes retained capabilities");
    assert!(claim.receive().readiness_signal_seen);
}
