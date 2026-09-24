use std::{fmt, sync::Arc};

use crate::{cells::ObservationTxn, runtime};
use shelterwood_core::{
    Exit,
    engine::ScopeState,
    exit::{
        Cancellation, ExitKind, ExitResult, GracePhase, JoinOutcome, RecordedOutcome, StartupError,
        StartupFailure, StartupFailureCause, StopReason, classify_exit,
        reconcile_recorded_outcomes,
    },
};

/// A value that may own a type-erased user error.
///
/// A failed [`Exit`], a failed provisional [`RecordedOutcome`] and a failed
/// incarnation [`ExitResult`] each own the application error an actor
/// returned. [`Retained`] routes the destruction of such a value to critical
/// disposal whenever framework state retires it.
pub(crate) trait CarriesUserError: Send + 'static {
    fn carries_user_error(&self) -> bool;
}

impl CarriesUserError for Exit {
    fn carries_user_error(&self) -> bool {
        matches!(self.kind(), ExitKind::Failed(_))
    }
}

impl CarriesUserError for RecordedOutcome {
    fn carries_user_error(&self) -> bool {
        self.is_failed()
    }
}

impl CarriesUserError for ExitResult {
    fn carries_user_error(&self) -> bool {
        self.is_err()
    }
}

/// A user-error-bearing value retained by framework state.
///
/// Retiring a carrier that owns a user error always transfers it to critical
/// disposal, regardless of its current strong count: a count probe would race
/// every other owner and could still leave one framework thread running the
/// last user destructor inline. Taking the value back out is the only way to
/// give it ordinary drop timing, and each value type names its own exit:
/// `Retained<Exit>::into_user_owned` is deliberately narrower than the
/// others.
pub(crate) struct Retained<T: CarriesUserError>(Option<T>);

impl<T: CarriesUserError> Retained<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(Some(value))
    }

    pub(crate) fn get(&self) -> &T {
        self.0
            .as_ref()
            .expect("a retained value is taken only by value")
    }

    fn take(mut self) -> T {
        self.0
            .take()
            .expect("a retained value is taken only by value")
    }
}

impl<T: CarriesUserError> Drop for Retained<T> {
    fn drop(&mut self) {
        if let Some(value) = self.0.take()
            && value.carries_user_error()
        {
            runtime::dispose_critical(value);
        }
    }
}

impl<T: CarriesUserError + Clone> Clone for Retained<T> {
    fn clone(&self) -> Self {
        Self::new(self.get().clone())
    }
}

impl<T: CarriesUserError + fmt::Debug> fmt::Debug for Retained<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

impl<T: CarriesUserError + PartialEq> PartialEq for Retained<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<T: CarriesUserError + PartialEq> PartialEq<T> for Retained<T> {
    fn eq(&self, other: &T) -> bool {
        self.get() == other
    }
}

impl<T: CarriesUserError + Eq> Eq for Retained<T> {}

impl<T: CarriesUserError> From<T> for Retained<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl Retained<RecordedOutcome> {
    /// Hands back the outcome a fold selected.
    pub(crate) fn into_outcome(self) -> RecordedOutcome {
        self.take()
    }
}

impl Retained<ExitResult> {
    /// Hands back the result on the raw epilogue's normal return path.
    pub(crate) fn into_result(self) -> ExitResult {
        self.take()
    }
}

impl Retained<Exit> {
    /// Hands the raw exit to a public/user-owned value.
    ///
    /// Framework-internal copies must instead go through
    /// [`ObservationTxn::surrender`], which makes their pre-commit co-owner
    /// proof structural and releases them only after the observation gate.
    /// `pub(in crate::cells)` is what keeps that structural: no driver-layer
    /// caller can reach the raw exit at all, so a framework carrier crossing
    /// a driver seam has to stay a carrier.
    pub(super) fn into_user_owned(self) -> Exit {
        self.take()
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
        if !exits.iter().any(|retained| retained == exit) {
            exits.push(Self::new(exit.clone()));
        }
    }

    pub(crate) fn retain_owned(exits: &mut Vec<Self>, exit: Self, surrendered: &mut Vec<Self>) {
        if exits.iter().any(|retained| retained == &exit) {
            // An existing retained copy keeps the raw clone alive while it is
            // released. Hand the duplicate to the surrounding observation
            // transaction rather than submitting a duplicate disposal job.
            surrendered.push(exit);
        } else {
            exits.push(exit);
        }
    }

    /// Installs a freshly computed guard set into a shared record slot.
    ///
    /// Records that hand out clones keep their guards behind one `Arc` so a
    /// read costs refcount traffic rather than one disposal job per exit. An
    /// unchanged guard set is therefore kept in place: the probe copies are
    /// surrendered, because an equal retained copy — for
    /// `ExitKind::Failed`, equality is `Arc::ptr_eq` — proves the payload
    /// stays owned. A changed set displaces the old allocation, which may be
    /// the last owner of a user error the record no longer holds, so its
    /// retirement leaves with the transaction's post-unlock effects.
    pub(super) fn install(
        guards: &mut Arc<Vec<Self>>,
        incoming: Vec<Self>,
        txn: &mut ObservationTxn<'_>,
    ) {
        if incoming.len() == guards.len()
            && incoming
                .iter()
                .all(|incoming| guards.iter().any(|current| current == incoming))
        {
            txn.surrender(incoming);
            return;
        }
        let displaced = std::mem::replace(guards, Arc::new(incoming));
        txn.defer(move || drop(displaced));
    }
}

impl ObservationTxn<'_> {
    /// Surrenders framework-internal retained copies as raw refcount traffic.
    ///
    /// Accepting the transaction token makes the co-owner proof structural:
    /// every caller queues the surrender before commit. Surrenders flush first
    /// after unlock, while record owners still exist and before an ordinary
    /// deferred effect can hand a queued co-owner to concurrent disposal.
    pub(crate) fn surrender(&mut self, exits: impl IntoIterator<Item = Retained<Exit>>) {
        let exits: Vec<_> = exits.into_iter().collect();
        if exits.is_empty() {
            return;
        }
        self.defer_surrender(move || {
            for exit in exits {
                drop(exit.take());
            }
        });
    }
}

/// Reconciles a recorded outcome against a forced one, retaining the loser.
///
/// [`reconcile_recorded_outcomes`] hands both outcomes back so its caller can
/// choose the losing outcome's destruction venue. Framework callers have no
/// reason to choose anything but isolated disposal, so this is the shape they
/// use: the raw fold is reached only from core's own unit tests.
pub(crate) fn reconcile_recorded_outcomes_retaining(
    recorded: Option<Retained<RecordedOutcome>>,
    forced: Option<RecordedOutcome>,
) -> Option<RecordedOutcome> {
    let (selected, discarded) =
        reconcile_recorded_outcomes(recorded.map(Retained::into_outcome), forced);
    drop(discarded.map(Retained::new));
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
    drop(discarded.map(Retained::new));
    exit
}

/// A stop reason retained by driver state or a runtime completion.
///
/// Structured startup reasons recursively contain the triggering child's
/// `Exit`. Keeping the public reason before its guards gives the same
/// raw-projection-first retirement order as `RetainedScopeSnapshot`.
#[derive(Clone, Debug)]
pub(crate) struct RetainedStopReason {
    reason: Option<StopReason>,
    retained_exits: Vec<Retained<Exit>>,
}

impl RetainedStopReason {
    pub(crate) fn new(reason: StopReason) -> Self {
        let mut retained_exits = Vec::new();
        Retained::retain_stop_reason(&mut retained_exits, &reason);
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
            drop(exit.into_user_owned());
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
        let mut guards = Arc::new(vec![Retained::new(exit.clone())]);
        let original = Arc::clone(&guards);

        let mut txn = ObservationTxn::detached();
        Retained::install(&mut guards, vec![Retained::new(exit.clone())], &mut txn);
        drop(txn);

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
    fn retained_exit_install_retires_a_changed_last_owned_set_after_commit() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let mut guards = Arc::new(vec![Retained::new(Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        ))]);
        let original = Arc::downgrade(&guards);

        let mut txn = ObservationTxn::detached();
        Retained::install(
            &mut guards,
            vec![Retained::new(Exit::completed(Cancellation::NotObserved))],
            &mut txn,
        );
        assert!(matches!(guards[0].get().kind(), ExitKind::Completed));
        // P3 #14: the displaced set may own the last copy of a user error the
        // record no longer holds, so it leaves with the transaction's
        // post-unlock effects rather than inside the critical section.
        assert!(
            original.upgrade().is_some(),
            "the displaced guard set survives until the transaction commits"
        );
        drop(txn);

        assert!(
            original.upgrade().is_none(),
            "a changed guard set replaces the shared allocation"
        );
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
        let retained = Retained::new(Exit::failed(
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
    fn retained_exit_user_handoff_preserves_the_callers_drop_thread() {
        let caller = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let retained = Retained::new(Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        ));

        drop(retained.into_user_owned());

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
        let retained = Retained::new(RecordedOutcome::returned(Err(ExitError::from(
            ThreadProbe(dropped),
        ))));

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
        let retained = Retained::new(Err(ExitError::from(ThreadProbe(dropped))));

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
        let retained = Retained::new(Err(ExitError::from(ThreadProbe(dropped))));

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
        let retained = Retained::new(RecordedOutcome::returned(Err(ExitError::from(
            ThreadProbe(dropped),
        ))));

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
        let member = MemberCell::new(identity.mint_membership(&id));
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
