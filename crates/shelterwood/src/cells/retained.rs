use std::{fmt, sync::Arc};

use crate::{cells::ObservationTxn, runtime};
use shelterwood_core::{
    engine::ScopeState,
    exit::{
        Cancellation, Exit, ExitKind, ExitResult, GracePhase, JoinOutcome, RecordedOutcome,
        StartupError, StartupFailure, StartupFailureCause, StopReason, classify_exit,
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
/// give it ordinary drop timing, and each value type names its own exit. A
/// raw [`Exit`] leaves only through [`Guarded::into_user_owned`], so no
/// driver-layer caller can extract one and a carrier crossing a driver seam
/// stays a carrier.
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
    /// Private to this module: its one caller is [`Guarded::into_user_owned`].
    /// Framework-internal copies must instead go through
    /// [`ObservationTxn::surrender`], which makes their pre-commit co-owner
    /// proof structural and releases them only after the observation gate.
    fn into_user_owned(self) -> Exit {
        self.take()
    }
}

/// A framework value whose user errors a guard set can enumerate.
///
/// `retain_guards` adds a retained copy of every [`Exit`] the value owns,
/// deduplicated by exit identity, so a [`Guarded`] value's guards are always
/// a function of the value itself.
pub(crate) trait RetainGuards {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>);
}

impl RetainGuards for Exit {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        if !guards.iter().any(|retained| retained == self) {
            guards.push(Retained::new(self.clone()));
        }
    }
}

impl<T: RetainGuards> RetainGuards for Option<T> {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        if let Some(value) = self {
            value.retain_guards(guards);
        }
    }
}

impl<T: RetainGuards> RetainGuards for Arc<T> {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        T::retain_guards(self, guards);
    }
}

impl RetainGuards for StartupFailure {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        if let StartupFailureCause::Child { exit, .. } = &self.cause {
            exit.retain_guards(guards);
        }
    }
}

impl RetainGuards for StopReason {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        if let Self::StartupFailed(failure) = self {
            failure.retain_guards(guards);
        }
    }
}

impl RetainGuards for ScopeState {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        if let Self::Stopped { reason } = self {
            reason.retain_guards(guards);
        }
    }
}

impl RetainGuards for Result<(), StartupError> {
    fn retain_guards(&self, guards: &mut Vec<Retained<Exit>>) {
        if let Err(StartupError::StartupFailed(failure)) = self {
            failure.retain_guards(guards);
        }
    }
}

/// A raw framework value plus retained copies of every exit it owns.
///
/// Field order is the carrier's whole argument: drop glue releases the raw
/// value while the guards still prove that none of its user errors can be
/// destroyed inline, and only then do the guards transfer each failed payload
/// to critical disposal. Clones share one guard allocation, so reading a
/// guarded record is refcount traffic rather than one disposal job per exit.
#[derive(Clone)]
pub(crate) struct Guarded<T> {
    value: T,
    guards: Arc<Vec<Retained<Exit>>>,
}

impl<T: RetainGuards> Guarded<T> {
    pub(crate) fn new(value: T) -> Self {
        let mut guards = Vec::new();
        value.retain_guards(&mut guards);
        Self {
            value,
            guards: Arc::new(guards),
        }
    }

    /// Mutates the value in place and re-derives its guards.
    ///
    /// The mutation runs while the previous guard set still covers every raw
    /// value it overwrites, so those inline drops are refcount work. An
    /// unchanged guard set stays in place and the fresh probe copies are
    /// surrendered, because an equal retained copy — for `ExitKind::Failed`,
    /// equality is `Arc::ptr_eq` — proves the payload stays owned. A changed
    /// set displaces the old allocation, which may be the last owner of a user
    /// error the value no longer holds, so it retires with the transaction's
    /// post-unlock effects.
    pub(super) fn update<R>(
        &mut self,
        txn: &mut ObservationTxn<'_>,
        update: impl FnOnce(&mut T) -> R,
    ) -> R {
        let result = update(&mut self.value);
        let mut incoming = Vec::new();
        self.value.retain_guards(&mut incoming);
        if incoming.len() == self.guards.len()
            && incoming
                .iter()
                .all(|incoming| self.guards.iter().any(|current| current == incoming))
        {
            txn.surrender(incoming);
        } else {
            let displaced = std::mem::replace(&mut self.guards, Arc::new(incoming));
            txn.defer(move || drop(displaced));
        }
        result
    }
}

impl<T> Guarded<T> {
    pub(crate) fn get(&self) -> &T {
        &self.value
    }

    /// Hands the raw value to an owner that keeps it past `txn`'s commit.
    ///
    /// Returning the value out of the gate closure, or storing it in state
    /// the gate protects, keeps a raw co-owner of every guarded exit alive
    /// while the transaction surrenders the guards.
    pub(super) fn release(self, txn: &mut ObservationTxn<'_>) -> T {
        let Self { value, guards } = self;
        txn.surrender(Arc::unwrap_or_clone(guards));
        value
    }

    /// Hands the raw value to a user-owned destination outside any lock.
    ///
    /// The value owns a raw clone corresponding to every guard, so retiring
    /// the guards inline is provably refcount-only. Unwrapping the allocation
    /// first matters: the caller is often its last owner, and letting the
    /// `Arc` drop the guards would route a live user error through critical
    /// disposal for nothing.
    pub(super) fn into_user_owned(self) -> T {
        let Self { value, guards } = self;
        for guard in Arc::unwrap_or_clone(guards) {
            drop(guard.into_user_owned());
        }
        value
    }

    #[cfg(test)]
    pub(crate) fn guard_count(&self) -> usize {
        self.guards.len()
    }

    #[cfg(test)]
    pub(crate) fn shares_guards_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.guards, &other.guards)
    }
}

impl<T> std::ops::Deref for Guarded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: fmt::Debug> fmt::Debug for Guarded<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(formatter)
    }
}

// The guards are a function of the value, so the value decides equality.
impl<T: PartialEq> PartialEq for Guarded<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for Guarded<T> {}

impl<T: RetainGuards> From<T> for Guarded<T> {
    fn from(value: T) -> Self {
        Self::new(value)
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

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use shelterwood_core::{
        exit::{Cancellation, ExitError},
        identity::{ChildId, ScopeIdentity},
    };

    use super::*;
    use crate::cells::{
        MemberCell,
        test_support::{TEST_WAIT, ThreadProbe},
    };

    #[test]
    fn guarded_update_keeps_an_equal_shared_guard_set_in_place() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let exit = Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        );
        let mut guarded = Guarded::new(Some(exit.clone()));
        let original = guarded.clone();

        let mut txn = ObservationTxn::detached();
        guarded.update(&mut txn, |value| *value = Some(exit.clone()));
        drop(txn);

        assert!(
            guarded.shares_guards_with(&original),
            "an unchanged guard set keeps its shared allocation"
        );
        assert_eq!(
            observed.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "the failure payload still has owners here, so no retirement can reach it"
        );
        drop(exit);
        drop(original);
        drop(guarded);
        assert_ne!(
            observed
                .recv_timeout(TEST_WAIT)
                .expect("the final guard isolates its failed payload"),
            retiring_thread
        );
    }

    #[test]
    fn guarded_update_retires_a_changed_last_owned_set_after_commit() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let mut guarded = Guarded::new(Some(Exit::failed(
            ExitError::from(ThreadProbe(dropped)),
            Cancellation::NotObserved,
        )));
        let original = Arc::downgrade(&guarded.guards);

        let mut txn = ObservationTxn::detached();
        guarded.update(&mut txn, |value| {
            *value = Some(Exit::completed(Cancellation::NotObserved));
        });
        assert!(matches!(
            guarded.guards[0].get().kind(),
            ExitKind::Completed
        ));
        // P3 #14: the displaced set may own the last copy of a user error the
        // value no longer holds, so it leaves with the transaction's
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
    fn guarded_stop_reason_isolates_its_nested_exit() {
        let retiring_thread = std::thread::current().id();
        let (dropped, observed) = mpsc::sync_channel(1);
        let id = ChildId::from("worker");
        let mut identity = ScopeIdentity::new();
        let member = MemberCell::new(identity.mint_membership(&id));
        let retained = Guarded::new(StopReason::StartupFailed(StartupFailure {
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
