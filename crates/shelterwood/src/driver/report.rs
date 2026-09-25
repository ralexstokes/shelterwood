use super::*;

pub(super) struct RecordedReport {
    pub(super) outcome: Option<Retained<RecordedOutcome>>,
    pub(super) cancellation: Cancellation,
    pub(super) readiness_signal_seen: bool,
}

struct ReportCompletion {
    report: Arc<OnceLock<RecordedReport>>,
    shutdown: Latch,
    local_stop: Option<Latch>,
    readiness: CompletionGatedLatch,
}

pub(super) struct ReportToken {
    completion: Obligation<ReportCompletion>,
}

pub(super) struct ReportClaim(Arc<OnceLock<RecordedReport>>);

/// Couples the child task's outcome report to its join verdict without an
/// asynchronous handoff race.
///
/// `ReportToken` is owned by the child task and its fail-closed `Drop` fills a
/// shared cell synchronously. The runtime resolves `runtime::join` only after
/// the spawned future has been destroyed (a tokio `JoinHandle` guarantee, not
/// a language one — any replacement executor behind `runtime` must preserve
/// it), so the exit joiner may consume the cell immediately: its claim is the
/// sole surviving owner and the cell is initialized on every return, panic,
/// and cancellation edge. The shutdown/local-stop and readiness latches are
/// sampled by that same initialization, making the report and its
/// completion-boundary evidence one ordered observation.
pub(super) fn report_slot(
    shutdown: Latch,
    local_stop: Option<Latch>,
    readiness: CompletionGatedLatch,
) -> (ReportToken, ReportClaim) {
    let report = Arc::new(OnceLock::new());
    (
        ReportToken {
            completion: Obligation::new(
                ReportCompletion {
                    report: Arc::clone(&report),
                    shutdown,
                    local_stop,
                    readiness,
                },
                |completion| completion.fill(None),
            ),
        },
        ReportClaim(report),
    )
}

impl ReportCompletion {
    fn fill(self, outcome: Option<RecordedOutcome>) {
        // Retain here rather than at the send site. From `set` until the exit
        // joiner's `receive`, the joiner's claim is the cell's sole owner; a
        // joiner dropped un-polled at runtime teardown would otherwise run the
        // application error's destructor inline on the teardown thread.
        let outcome = outcome.map(Retained::new);
        let cancellation =
            if self.shutdown.is_fired() || self.local_stop.as_ref().is_some_and(Latch::is_fired) {
                Cancellation::Observed
            } else {
                Cancellation::NotObserved
            };
        let readiness_signal_seen = self.readiness.complete();
        let report = RecordedReport {
            outcome,
            cancellation,
            readiness_signal_seen,
        };
        // Ownership supplies exactly one `ReportCompletion`; ignoring the
        // impossible occupied-cell result keeps the Drop fallback infallible.
        let _ = self.report.set(report);
    }
}

impl ReportToken {
    pub(super) fn record(mut self, outcome: RecordedOutcome) {
        self.completion
            .complete(|completion| completion.fill(Some(outcome)));
    }
}

impl ReportClaim {
    pub(super) fn receive(self) -> RecordedReport {
        Arc::try_unwrap(self.0)
            .unwrap_or_else(|_| {
                panic!("owned report token must be destroyed before its task joins")
            })
            .into_inner()
            .expect("owned report token must record or fall back before its task joins")
    }
}
