use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    cells::{RemoveOutcome, ReserveError},
    driver::DynamicReservation,
    runtime::{DisposingReceiver, Latch},
};
use shelterwood_core::panic::{UnwindPanics, catch_panic, resume_preferred_panic};

use crate::driver::{LATCHED_REMOVAL_OUTCOME, LOST_ADMISSION_RESPONSE_ERROR};

/// Resolves a driver response that may have been lost.
///
/// The driver's owned completion publishes on every path, including its own
/// drop fallback, so `None` means that obligation regressed. SPEC B.8: debug
/// builds expose the regression by panicking; release builds fail closed to
/// `fallback`.
fn fail_closed<T>(response: Option<T>, fallback: T, debug_panics: bool, what: &str) -> T {
    match response {
        Some(response) => response,
        None if debug_panics => panic!("{what} response obligation must complete"),
        None => fallback,
    }
}

/// An admission future.
///
/// *Fused* admissions — the `add_*` methods on [`DynamicScopeRef`](crate::DynamicScopeRef),
/// which reserve and define in one call — abort on drop. *Split* admissions —
/// `define` on a slot from a `reserve_*` method — detach once their first
/// poll starts admission. Reservation and that first poll require an ambient
/// Tokio runtime. A first poll outside one returns [`ReserveError::NoRuntime`]
/// and releases the reservation.
///
/// Admission starts at the first poll, so dropping the future before then
/// never admits the child, fused or split. The reservation is released, the
/// reserved member ends as [`ExitKind::NeverStarted`](crate::ExitKind::NeverStarted), and
/// its id becomes reusable — the same outcome as a first poll outside a
/// runtime. For a split definition that is the one drop edge that does not
/// detach: the handles taken from the slot beforehand stay valid but name a
/// child that never ran.
///
/// Polled to completion, the future yields the admitted handles or a
/// [`ReserveError`]: the driver's admission obligation publishes an outcome
/// on every path, including its own drop fallback. Should that obligation
/// ever be destroyed without publishing — a framework invariant failure, not
/// a condition callers can provoke — debug builds panic and release builds
/// fail closed, resolving [`ReserveError::NotAdmitting`] with a terminal
/// cause.
///
/// After it has produced its result, further polls return `Pending`; they
/// neither panic nor produce a second result.
#[must_use]
pub struct Admission<H> {
    state: AdmissionState<H>,
}

type AdmissionWait = DisposingReceiver<Result<(), ReserveError>>;

struct PendingAdmission<H> {
    reservation: DynamicReservation,
    handles: H,
    fused_cancel: Option<Latch>,
}

impl<H> PendingAdmission<H> {
    fn start(&self) -> Result<AdmissionWait, ReserveError> {
        let response = self
            .reservation
            .start_admission(self.fused_cancel.clone())?;
        Ok(DisposingReceiver::new(response))
    }

    fn annul(&self) {
        let signal_panic = self.fused_cancel.as_ref().and_then(|cancel| {
            catch_panic(|| {
                crate::driver::signal_fused_cancel(
                    &self.reservation.scope,
                    self.reservation.control.as_ref(),
                    &self.reservation.slot,
                    cancel,
                );
            })
            .err()
        });
        let cleanup_panic = catch_panic(|| self.reservation.cancel()).err();
        resume_preferred_panic(UnwindPanics {
            primary: signal_panic,
            cleanup: cleanup_panic,
        });
    }
}

enum AdmissionState<H> {
    Immediate(ReserveError),
    Unpolled(PendingAdmission<H>),
    InFlight {
        pending: PendingAdmission<H>,
        wait: AdmissionWait,
    },
    Done,
}

impl<H> AdmissionState<H> {
    fn name(&self) -> &'static str {
        match self {
            Self::Immediate(_) => "Immediate",
            Self::Unpolled(_) => "Unpolled",
            Self::InFlight { .. } => "InFlight",
            Self::Done => "Done",
        }
    }
}

impl<H> fmt::Debug for Admission<H> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Admission")
            .field("state", &self.state.name())
            .finish_non_exhaustive()
    }
}

impl<H> Admission<H> {
    pub(super) fn error(error: ReserveError) -> Self {
        Self {
            state: AdmissionState::Immediate(error),
        }
    }

    pub(super) fn new(
        reservation: DynamicReservation,
        handles: H,
        fused_cancel: Option<Latch>,
    ) -> Self {
        Self {
            state: AdmissionState::Unpolled(PendingAdmission {
                reservation,
                handles,
                fused_cancel,
            }),
        }
    }
}

// `Admission` never pins its contents: `poll` opens with `get_mut` and every
// await point goes through an owned wait. The unconditional impl keeps that
// structural fact from surfacing as a semantically empty `H: Unpin` bound on
// the public `Future` impl.
impl<H> Unpin for Admission<H> {}

impl<H> Future for Admission<H> {
    type Output = Result<H, ReserveError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        loop {
            match &mut this.state {
                AdmissionState::Immediate(error) => {
                    let error = std::mem::replace(error, ReserveError::NoRuntime);
                    this.state = AdmissionState::Done;
                    return Poll::Ready(Err(error));
                }
                AdmissionState::Unpolled(pending) => {
                    // Keep the annul-owning `Unpolled` state installed until
                    // this fallible operation returns. If it unwinds, Drop
                    // must still find `pending` and cancel the reservation.
                    let wait = match pending.start() {
                        Ok(wait) => wait,
                        Err(error) => {
                            pending.reservation.cancel();
                            this.state = AdmissionState::Done;
                            return Poll::Ready(Err(error));
                        }
                    };
                    let previous = std::mem::replace(&mut this.state, AdmissionState::Done);
                    let AdmissionState::Unpolled(pending) = previous else {
                        unreachable!("the matched admission state was replaced in place")
                    };
                    this.state = AdmissionState::InFlight { pending, wait };
                }
                AdmissionState::InFlight { wait, .. } => match wait.poll_receive(context) {
                    Poll::Ready(result) => {
                        let result = fail_closed(
                            result,
                            Err(LOST_ADMISSION_RESPONSE_ERROR),
                            cfg!(debug_assertions),
                            "admission",
                        );
                        let previous = std::mem::replace(&mut this.state, AdmissionState::Done);
                        let AdmissionState::InFlight { pending, .. } = previous else {
                            unreachable!("the matched admission state was replaced in place")
                        };
                        return Poll::Ready(result.map(|()| pending.handles));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                AdmissionState::Done => return Poll::Pending,
            }
        }
    }
}

impl<H> Drop for Admission<H> {
    fn drop(&mut self) {
        match &self.state {
            AdmissionState::Unpolled(pending) => {
                // A fused admission annuls its reservation on every drop edge,
                // polled or not. Firing the latch before cancelling keeps the
                // scope's control-plane wake and the cancellation evidence in
                // the same order the in-flight path uses.
                pending.annul();
            }
            AdmissionState::InFlight { pending, .. } => {
                if pending.fused_cancel.is_some() {
                    pending.annul();
                }
            }
            AdmissionState::Immediate(_) | AdmissionState::Done => {}
        }
    }
}
/// Observation future for a synchronously latched dynamic removal.
///
/// The driver publishes the latched outcome on every destruction path. Should
/// that obligation ever be destroyed without publishing — a framework
/// invariant failure — debug builds panic and release builds resolve
/// [`RemoveOutcome::Removed`]: the removal latched at the call, and its route
/// becoming terminal satisfies the removal goal.
/// After it has produced its outcome, further polls return `Pending`, as on
/// [`Admission`].
#[must_use]
pub struct Removal {
    inner: DisposingReceiver<RemoveOutcome>,
    done: bool,
}

impl fmt::Debug for Removal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Removal").finish_non_exhaustive()
    }
}

impl Removal {
    pub(super) fn new(response: crate::driver::RemovalResponse) -> Self {
        Self {
            inner: DisposingReceiver::new(response),
            done: false,
        }
    }
}

impl Future for Removal {
    type Output = RemoveOutcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.done {
            return Poll::Pending;
        }
        let response = std::task::ready!(self.inner.poll_receive(context));
        self.done = true;
        Poll::Ready(fail_closed(
            response,
            LATCHED_REMOVAL_OUTCOME,
            cfg!(debug_assertions),
            "removal",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Wake, Waker},
    };

    use crate::{ExitKind, TaskDef, test_support::SHUTDOWN_BUDGET};

    use super::{Admission, Removal};
    use crate::{TaskRef, runtime::Latch, tree::DynamicTree};

    struct DropAdmissionAndPanic {
        admission: Mutex<Option<Admission<TaskRef>>>,
    }

    impl Wake for DropAdmissionAndPanic {
        fn wake(self: Arc<Self>) {
            drop(
                self.admission
                    .lock()
                    .expect("admission mutex poisoned")
                    .take(),
            );
            panic!("hostile observation waker");
        }

        fn wake_by_ref(self: &Arc<Self>) {
            drop(
                self.admission
                    .lock()
                    .expect("admission mutex poisoned")
                    .take(),
            );
            panic!("hostile observation waker");
        }
    }

    #[test]
    fn lost_admission_response_policy_fails_closed() {
        assert!(matches!(
            super::LOST_ADMISSION_RESPONSE_ERROR,
            crate::ReserveError::NotAdmitting(crate::NotAdmittingCause::Terminal)
        ));
    }

    #[test]
    fn lost_removal_response_policy_fails_closed() {
        assert_eq!(
            super::LATCHED_REMOVAL_OUTCOME,
            crate::RemoveOutcome::Removed,
            "a lost removal response must preserve the removal goal"
        );
    }

    #[test]
    fn lost_response_panics_in_debug_and_fails_closed_in_release() {
        let debug = catch_unwind(|| super::fail_closed(None, 0, true, "test"));
        assert!(debug.is_err(), "debug builds expose the lost response");
        assert_eq!(super::fail_closed(None, 7, false, "test"), 7);
        assert_eq!(super::fail_closed(Some(3), 7, true, "test"), 3);
    }

    #[test]
    fn closed_removal_response_follows_the_build_profile() {
        let (sender, response) = crate::runtime::oneshot();
        drop(sender);
        let mut removal = Removal::new(response);
        let mut context = Context::from_waker(Waker::noop());
        let observed = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut removal).poll(&mut context)
        }));

        if cfg!(debug_assertions) {
            assert!(observed.is_err(), "debug builds expose the lost response");
        } else {
            assert!(matches!(
                observed,
                Ok(Poll::Ready(crate::RemoveOutcome::Removed))
            ));
        }
    }

    #[test]
    fn completed_admission_and_removal_stay_pending_when_polled_again() {
        let mut context = Context::from_waker(Waker::noop());

        let mut admission = Admission::<()>::error(crate::ReserveError::EmptyId);
        assert!(matches!(
            Pin::new(&mut admission).poll(&mut context),
            Poll::Ready(Err(crate::ReserveError::EmptyId))
        ));
        assert!(Pin::new(&mut admission).poll(&mut context).is_pending());

        let (sender, response) = crate::runtime::oneshot();
        assert!(sender.send(crate::RemoveOutcome::AlreadyAbsent).is_ok());
        let mut removal = Removal::new(response);
        assert_eq!(
            Pin::new(&mut removal).poll(&mut context),
            Poll::Ready(crate::RemoveOutcome::AlreadyAbsent)
        );
        assert!(
            Pin::new(&mut removal).poll(&mut context).is_pending(),
            "a completed removal neither panics nor reports a second outcome"
        );
    }

    #[crate::runtime::test]
    async fn queued_fused_drop_before_exit_dispatch_suppresses_restart_accounting() {
        crate::driver::exercise_queued_fused_drop_before_exit_dispatch(|reservation| {
            Admission::new(reservation, (), Some(Latch::default()))
        })
        .await;
    }

    #[crate::runtime::test]
    async fn unpolled_fused_drop_releases_reservations_despite_a_reentrant_panicking_waker() {
        let system = DynamicTree::new().spawn().expect("runtime is available");
        system.wait_started().await.expect("dynamic root starts");
        let scope = system.scope();

        let first_slot = scope.reserve_task("first").expect("first id is free");
        let first = first_slot.task_ref();
        let first_admission = first_slot.define(TaskDef::new(|_| std::future::pending()));
        let second_slot = scope.reserve_task("second").expect("second id is free");
        let second = second_slot.task_ref();
        let second_admission = second_slot.define(TaskDef::new(|_| std::future::pending()));

        let mut first_wait = Box::pin(first.wait());
        let waker = Waker::from(Arc::new(DropAdmissionAndPanic {
            admission: Mutex::new(Some(second_admission)),
        }));
        assert!(
            first_wait
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        catch_unwind(AssertUnwindSafe(|| drop(first_admission)))
            .expect_err("the hostile membership waker still surfaces");
        assert!(matches!(
            first_wait
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        assert!(matches!(second.wait().await.kind(), ExitKind::NeverStarted));

        drop(
            scope
                .reserve_task("first")
                .expect("first reservation was released"),
        );
        drop(
            scope
                .reserve_task("second")
                .expect("reentrant second reservation was released"),
        );
        system
            .shutdown(SHUTDOWN_BUDGET)
            .await
            .expect("cancelled reservations leave no stragglers");
    }
}
