use std::{fmt, sync::Arc};

use crate::runtime;
use shelterwood_core::{
    Exit,
    engine::ScopeState,
    exit::{
        Cancellation, ExitKind, ExitResult, GracePhase, JoinOutcome, RecordedOutcome, StartupError,
        StartupFailure, StartupFailureCause, StopReason, classify_disposal_panic, classify_exit,
        reconcile_recorded_outcomes,
    },
};

/// An exit copy retained by framework state.
///
/// Failed exits own a type-erased user error. Retiring such a copy always
/// transfers it to isolated disposal, regardless of its current strong count:
/// a count probe would race every other owner and could still leave one
/// framework thread running the last user destructor inline.
#[derive(Clone)]
pub(crate) struct RetainedExit(Option<Exit>);

impl RetainedExit {
    pub(crate) fn new(exit: Exit) -> Self {
        Self(Some(exit))
    }

    pub(crate) fn as_exit(&self) -> &Exit {
        self.0.as_ref().expect("retained exit was already taken")
    }

    pub(crate) fn into_exit(mut self) -> Exit {
        self.0.take().expect("retained exit was already taken")
    }

    pub(crate) fn retain_scope_state(exits: &mut Vec<Self>, state: &ScopeState) {
        if let ScopeState::Stopped { reason } = state {
            Self::retain_stop_reason(exits, reason);
        }
    }

    pub(crate) fn retain_startup_result(exits: &mut Vec<Self>, startup: &Result<(), StartupError>) {
        if let Err(StartupError::StartupFailed(failure)) = startup {
            Self::retain_startup_failure(exits, failure);
        }
    }

    pub(crate) fn retain_stop_reason(exits: &mut Vec<Self>, reason: &StopReason) {
        if let StopReason::StartupFailed(failure) = reason {
            Self::retain_startup_failure(exits, failure);
        }
    }

    fn retain_startup_failure(exits: &mut Vec<Self>, failure: &StartupFailure) {
        if let StartupFailureCause::Child { exit, .. } = &failure.cause {
            Self::retain_exit(exits, exit);
        }
    }

    pub(crate) fn retain_exit(exits: &mut Vec<Self>, exit: &Exit) {
        if !exits.iter().any(|retained| retained.as_exit() == exit) {
            exits.push(Self::new(exit.clone()));
        }
    }

    pub(crate) fn retain_owned(exits: &mut Vec<Self>, exit: Self) {
        if exits.iter().any(|retained| retained == &exit) {
            // An existing retained copy keeps the raw clone alive while it is
            // released. Avoid submitting a duplicate disposal job.
            drop(exit.into_exit());
        } else {
            exits.push(exit);
        }
    }

    /// Installs a freshly computed guard set into a shared record slot.
    ///
    /// Records that hand out clones keep their guards behind one `Arc` so a
    /// read costs refcount traffic rather than one disposal job per exit. An
    /// unchanged guard set is therefore kept in place: the probe copies are
    /// released as refcount traffic, because an equal retained copy — for
    /// `ExitKind::Failed`, equality is `Arc::ptr_eq` — proves the payload
    /// stays owned.
    pub(super) fn install(guards: &mut Arc<Vec<Self>>, incoming: Vec<Self>) {
        if incoming.len() == guards.len()
            && incoming
                .iter()
                .all(|incoming| guards.iter().any(|current| current == incoming))
        {
            for exit in incoming {
                drop(exit.into_exit());
            }
            return;
        }
        *guards = Arc::new(incoming);
    }
}

impl From<Exit> for RetainedExit {
    fn from(exit: Exit) -> Self {
        Self::new(exit)
    }
}

impl fmt::Debug for RetainedExit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_exit().fmt(formatter)
    }
}

impl PartialEq for RetainedExit {
    fn eq(&self, other: &Self) -> bool {
        self.as_exit() == other.as_exit()
    }
}

impl PartialEq<Exit> for RetainedExit {
    fn eq(&self, other: &Exit) -> bool {
        self.as_exit() == other
    }
}

impl Eq for RetainedExit {}

impl Drop for RetainedExit {
    fn drop(&mut self) {
        let Some(exit) = self.0.take() else {
            return;
        };
        if matches!(exit.kind(), ExitKind::Failed(_)) {
            runtime::dispose_critical(exit);
        }
    }
}

/// A provisional outcome retained by framework event and driver state.
///
/// The failed variant owns a type-erased application error just like a failed
/// [`Exit`]. If framework control flow retires the carrier without selecting
/// that outcome, its user value is transferred to critical disposal.
pub(crate) struct RetainedRecordedOutcome(Option<RecordedOutcome>);

impl RetainedRecordedOutcome {
    pub(crate) fn new(outcome: RecordedOutcome) -> Self {
        Self(Some(outcome))
    }

    pub(crate) fn as_outcome(&self) -> &RecordedOutcome {
        self.0
            .as_ref()
            .expect("retained recorded outcome was already taken")
    }

    pub(crate) fn into_outcome(mut self) -> RecordedOutcome {
        self.0
            .take()
            .expect("retained recorded outcome was already taken")
    }
}

impl fmt::Debug for RetainedRecordedOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_outcome().fmt(formatter)
    }
}

impl Drop for RetainedRecordedOutcome {
    fn drop(&mut self) {
        let Some(outcome) = self.0.take() else {
            return;
        };
        if outcome.is_failed() {
            runtime::dispose_critical(outcome);
        }
    }
}

/// A completed incarnation result retained across a fallible teardown epilogue.
///
/// The failed variant owns a type-erased application error that no framework
/// copy has reached yet: the first retention point downstream is
/// [`RetainedRecordedOutcome`], one hop after the incarnation future returns.
/// The raw epilogue runs several contained teardown steps while holding the
/// completed result and then resumes any surviving panic, so the carrier is
/// dropped during that unwind — and destroying a user error there is a second
/// panic inside the first one's cleanup, which aborts the process.
/// [`Self::into_result`] on the normal return path preserves ordinary
/// downstream ownership.
pub(crate) struct RetainedExitResult(Option<ExitResult>);

impl RetainedExitResult {
    pub(crate) fn new(result: ExitResult) -> Self {
        Self(Some(result))
    }

    pub(crate) fn into_result(mut self) -> ExitResult {
        self.0
            .take()
            .expect("retained exit result was already taken")
    }
}

impl Drop for RetainedExitResult {
    fn drop(&mut self) {
        let Some(result) = self.0.take() else {
            return;
        };
        if let Err(error) = result {
            runtime::dispose_critical(error);
        }
    }
}

/// Reconciles a recorded outcome against a forced one, retaining the loser.
///
/// [`reconcile_recorded_outcomes`] hands both outcomes back so its caller can
/// choose the losing outcome's destruction venue. Framework callers have no
/// reason to choose anything but isolated disposal, so this is the shape they
/// use: the raw fold is reached only from core's own unit tests.
pub(crate) fn reconcile_recorded_outcomes_retaining(
    recorded: Option<RetainedRecordedOutcome>,
    forced: Option<RecordedOutcome>,
) -> Option<RecordedOutcome> {
    let (selected, discarded) =
        reconcile_recorded_outcomes(recorded.map(RetainedRecordedOutcome::into_outcome), forced);
    drop(discarded.map(RetainedRecordedOutcome::new));
    selected
}

/// Classifies a child exit, retaining the losing evidence.
///
/// The counterpart of [`reconcile_recorded_outcomes_retaining`] for
/// [`classify_exit`]. Returning only the selected exit is what keeps
/// `let (exit, _) = classify_exit(..)` — which silently destroys a losing
/// application error on the calling framework thread — out of driver code.
pub(crate) fn classify_exit_retaining(
    recorded: Option<RecordedOutcome>,
    join: JoinOutcome<()>,
    hard_abort_phase: Option<GracePhase>,
    cancellation: Cancellation,
) -> Exit {
    let (exit, discarded) = classify_exit(recorded, join, hard_abort_phase, cancellation);
    drop(discarded.map(RetainedExit::new));
    exit
}

/// Folds a destructor panic into a retained exit, retaining the loser.
///
/// The carrier stays a [`RetainedExit`] across the fold: the losing half is
/// the application error whenever one was recorded, because
/// `Failed` ranks below `Panicked`.
pub(crate) fn classify_disposal_panic_retaining(
    exit: RetainedExit,
    message: Option<String>,
) -> RetainedExit {
    let (selected, discarded) = classify_disposal_panic(exit.into_exit(), message);
    drop(RetainedExit::new(discarded));
    RetainedExit::new(selected)
}

/// A stop reason retained by driver state or a runtime completion.
///
/// Structured startup reasons recursively contain the triggering child's
/// `Exit`. Keeping the public reason before its guards gives the same
/// raw-projection-first retirement order as `RetainedScopeSnapshot`.
#[derive(Clone, Debug)]
pub(crate) struct RetainedStopReason {
    reason: Option<StopReason>,
    retained_exits: Vec<RetainedExit>,
}

impl RetainedStopReason {
    pub(crate) fn new(reason: StopReason) -> Self {
        let mut retained_exits = Vec::new();
        RetainedExit::retain_stop_reason(&mut retained_exits, &reason);
        Self {
            reason: Some(reason),
            retained_exits,
        }
    }

    pub(crate) fn as_reason(&self) -> &StopReason {
        self.reason
            .as_ref()
            .expect("retained stop reason was already taken")
    }

    pub(crate) fn into_public(mut self) -> StopReason {
        let reason = self
            .reason
            .take()
            .expect("retained stop reason was already taken");
        for exit in std::mem::take(&mut self.retained_exits) {
            drop(exit.into_exit());
        }
        reason
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use shelterwood_core::{Cancellation, ChildId, ExitError, identity::ScopeIdentity};

    use super::*;
    use crate::cells::{
        MemberCell,
        test_support::{TEST_WAIT, ThreadProbe},
    };

    #[test]
    fn retained_exit_install_keeps_an_equal_shared_guard_set_in_place() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let exit = Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        );
        let mut guards = Arc::new(vec![RetainedExit::new(exit.clone())]);
        let original = Arc::clone(&guards);

        RetainedExit::install(&mut guards, vec![RetainedExit::new(exit.clone())]);

        assert!(
            Arc::ptr_eq(&guards, &original),
            "an unchanged guard set keeps its shared allocation"
        );
        assert_eq!(
            observed.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "the failure payload still has owners here, so no retirement can reach it"
        );
        drop(exit);
        drop(original);
        drop(guards);
        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("the final guard isolates its failed payload"),
            retiring_thread
        );
    }

    #[test]
    fn retained_exit_install_replaces_and_isolates_a_changed_last_owned_set() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let mut guards = Arc::new(vec![RetainedExit::new(Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        ))]);
        let original = Arc::downgrade(&guards);

        RetainedExit::install(
            &mut guards,
            vec![RetainedExit::new(Exit::completed(
                Cancellation::NotObserved,
            ))],
        );

        assert!(
            original.upgrade().is_none(),
            "a changed guard set replaces the shared allocation"
        );
        assert!(matches!(guards[0].as_exit().kind(), ExitKind::Completed));
        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("the displaced last-owned failure is disposed"),
            retiring_thread,
            "replacement must isolate the displaced failed payload"
        );
    }

    #[test]
    fn retained_failed_exit_disposes_off_the_retiring_thread() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retained = RetainedExit::new(Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        ));

        drop(retained);

        let disposal_thread = observed
            .recv_timeout(Duration::from_secs(10))
            .expect("isolated exit disposal completes");
        assert_ne!(
            disposal_thread, retiring_thread,
            "a retained failed exit must not run its user destructor inline"
        );
    }

    #[test]
    fn retained_exit_conversion_preserves_the_callers_drop_thread() {
        let caller = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retained = RetainedExit::new(Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        ));

        drop(retained.into_exit());

        assert_eq!(
            observed
                .recv_timeout(Duration::from_secs(10))
                .expect("converted exit destruction completes"),
            caller,
            "a converted public exit keeps ordinary caller-owned drop timing"
        );
    }

    #[test]
    fn retained_failed_recorded_outcome_disposes_off_the_retiring_thread() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retained = RetainedRecordedOutcome::new(RecordedOutcome::returned(Err(
            ExitError::from(ThreadProbe(dropped)),
        )));

        drop(retained);

        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("recorded failure disposal completes"),
            retiring_thread
        );
    }

    #[test]
    fn retained_failed_exit_result_disposes_off_the_retiring_thread() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retained = RetainedExitResult::new(Err(ExitError::from(ThreadProbe(dropped))));

        drop(retained);

        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("retained result disposal completes"),
            retiring_thread,
            "a teardown unwind must not run the application error's destructor inline"
        );
    }

    #[test]
    fn taken_exit_result_preserves_the_callers_drop_thread() {
        let caller = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retained = RetainedExitResult::new(Err(ExitError::from(ThreadProbe(dropped))));

        drop(retained.into_result());

        assert_eq!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("taken result destruction completes"),
            caller,
            "the normal return path keeps ordinary downstream drop timing"
        );
    }

    #[test]
    fn selected_recorded_outcome_preserves_the_callers_drop_thread() {
        let caller = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retained = RetainedRecordedOutcome::new(RecordedOutcome::returned(Err(
            ExitError::from(ThreadProbe(dropped)),
        )));

        drop(retained.into_outcome());

        assert_eq!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("selected recorded outcome destruction completes"),
            caller
        );
    }

    #[test]
    fn retained_stop_reason_isolates_its_nested_exit() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let id = ChildId::from("worker");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(
            id.clone(),
            identity
                .mint_membership(&id)
                .expect("membership is available"),
        );
        let retained = RetainedStopReason::new(StopReason::StartupFailed(StartupFailure {
            cause: StartupFailureCause::Child {
                id,
                membership: member.membership(),
                exit: Exit::failed(
                    ExitError::from(ThreadProbe(dropped)),
                    Cancellation::NotObserved,
                ),
            },
        }));

        drop(retained);

        assert_ne!(
            observed
                .recv_timeout(Duration::from_secs(10))
                .expect("nested exit disposal completes"),
            retiring_thread,
            "a retained driver completion must isolate its nested exit"
        );
    }
}
