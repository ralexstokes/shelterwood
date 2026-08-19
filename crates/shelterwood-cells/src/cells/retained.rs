use std::{fmt, sync::Arc};

use shelterwood_core::{
    Exit,
    engine::ScopeState,
    exit::{ExitKind, StartupError, StartupFailure, StartupFailureCause, StopReason},
};
use shelterwood_runtime as runtime;

/// An exit copy retained by framework state.
///
/// Failed exits own a type-erased user error. Retiring such a copy always
/// transfers it to isolated disposal, regardless of its current strong count:
/// a count probe would race every other owner and could still leave one
/// framework thread running the last user destructor inline.
#[derive(Clone)]
pub struct RetainedExit(Option<Exit>);

impl RetainedExit {
    pub fn new(exit: Exit) -> Self {
        Self(Some(exit))
    }

    pub fn as_exit(&self) -> &Exit {
        self.0.as_ref().expect("retained exit was already taken")
    }

    pub fn into_exit(mut self) -> Exit {
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

    pub fn retain_stop_reason(exits: &mut Vec<Self>, reason: &StopReason) {
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

/// A stop reason retained by driver state or a runtime completion.
///
/// Structured startup reasons recursively contain the triggering child's
/// `Exit`. Keeping the public reason before its guards gives the same
/// raw-projection-first retirement order as `RetainedScopeSnapshot`.
#[derive(Clone, Debug)]
pub struct RetainedStopReason {
    reason: Option<StopReason>,
    retained_exits: Vec<RetainedExit>,
}

impl RetainedStopReason {
    pub fn new(reason: StopReason) -> Self {
        let mut retained_exits = Vec::new();
        RetainedExit::retain_stop_reason(&mut retained_exits, &reason);
        Self {
            reason: Some(reason),
            retained_exits,
        }
    }

    pub fn as_reason(&self) -> &StopReason {
        self.reason
            .as_ref()
            .expect("retained stop reason was already taken")
    }

    pub fn into_public(mut self) -> StopReason {
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
