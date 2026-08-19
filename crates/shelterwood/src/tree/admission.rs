use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{
    admission::{RemoveOutcome, ReserveError},
    driver::{DynamicReservation, LATCHED_REMOVAL_OUTCOME, LOST_ADMISSION_RESPONSE_ERROR},
    runtime::Latch,
};

use super::slots::AdmissionOwnership;

/// An admission future.
///
/// Fused additions abort on drop; split definitions detach after their first
/// poll starts admission. Reservation and that first poll require an ambient
/// Tokio runtime. A first poll outside one returns [`ReserveError::NoRuntime`]
/// and releases the reservation. If the driver's internal completion route is
/// lost, release builds fail closed with [`ReserveError::NotAdmitting`] and a
/// [`crate::NotAdmittingCause::Terminal`] cause. Debug builds additionally
/// assert that the completion obligation regressed.
/// Like a fused future, it remains pending if polled again after completion.
#[must_use]
pub struct Admission<H> {
    state: AdmissionState<H>,
}

type AdmissionWait = Pin<Box<dyn Future<Output = Result<(), ReserveError>> + Send + 'static>>;

struct PendingAdmission<H> {
    reservation: DynamicReservation,
    handles: H,
    fused_cancel: Option<Latch>,
}

impl<H> PendingAdmission<H> {
    fn start(&self) -> Result<AdmissionWait, ReserveError> {
        let response = crate::driver::start_admission(
            Arc::clone(&self.reservation.control),
            Arc::clone(&self.reservation.slot),
            self.fused_cancel.clone(),
        )?;
        Ok(Box::pin(async move {
            response.receive().await.unwrap_or_else(|| {
                // The driver's admission `Obligation` publishes an outcome on
                // every path, including its drop fallback. Treat a missing
                // response as the scope having gone terminal so a caller is
                // never stranded, but fail loudly in debug builds: silence
                // here would mask an obligation regression.
                debug_assert!(false, "admission response obligation must complete");
                Err(LOST_ADMISSION_RESPONSE_ERROR)
            })
        }))
    }

    fn cancel_reservation(&self) {
        crate::driver::cancel_dynamic_reservation(
            &self.reservation.scope,
            self.reservation.control.as_ref(),
            &self.reservation.slot,
        );
    }

    fn annul(&self) {
        let signal_panic = self.fused_cancel.as_ref().and_then(|cancel| {
            crate::runtime::catch_panic(|| {
                crate::driver::signal_fused_cancel(
                    &self.reservation.scope,
                    self.reservation.control.as_ref(),
                    &self.reservation.slot,
                    cancel,
                );
            })
            .err()
        });
        let cleanup_panic = crate::runtime::catch_panic(|| self.cancel_reservation()).err();
        crate::runtime::resume_preferred_panic(crate::runtime::UnwindPanics {
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
        ownership: AdmissionOwnership,
    ) -> Self {
        Self {
            state: AdmissionState::Unpolled(PendingAdmission {
                reservation,
                handles,
                fused_cancel: match ownership {
                    AdmissionOwnership::Split => None,
                    AdmissionOwnership::Fused => Some(Latch::default()),
                },
            }),
        }
    }
}

impl<H: Unpin> Future for Admission<H> {
    type Output = Result<H, ReserveError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        loop {
            match &mut this.state {
                AdmissionState::Immediate(error) => {
                    let error = error.clone();
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
                            pending.cancel_reservation();
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
                AdmissionState::InFlight { wait, .. } => match wait.as_mut().poll(context) {
                    Poll::Ready(result) => {
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
/// If the driver's internal completion route is lost after the request is
/// latched, release builds fail closed with [`RemoveOutcome::Removed`]; debug
/// builds additionally assert that the completion obligation regressed.
#[must_use]
pub struct Removal {
    inner: Pin<Box<dyn Future<Output = RemoveOutcome> + Send + 'static>>,
}

impl fmt::Debug for Removal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Removal").finish_non_exhaustive()
    }
}

impl Removal {
    pub(super) fn new(response: crate::driver::RemovalResponse) -> Self {
        Self {
            inner: Box::pin(async move {
                response.receive().await.unwrap_or_else(|| {
                    // The driver's removal `Obligation` publishes `Removed`
                    // on every destruction path. A missing response therefore
                    // means the terminal route vanished after removal latched:
                    // preserve the removal goal, but flag the invariant break
                    // in debug builds just as admission does above.
                    debug_assert!(false, "removal response obligation must complete");
                    LATCHED_REMOVAL_OUTCOME
                })
            }),
        }
    }
}

impl Future for Removal {
    type Output = RemoveOutcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
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
        time::Duration,
    };

    use crate::{ExitKind, TaskDef};

    use super::{Admission, Removal};
    use crate::{
        TaskRef,
        tree::{DynamicTree, slots::AdmissionOwnership},
    };

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
        // Profile-independent: the release fallback below is unreachable in
        // the debug builds CI runs, so the policy itself is pinned here rather
        // than only through the value a release build happens to observe.
        assert_eq!(
            super::LATCHED_REMOVAL_OUTCOME,
            crate::RemoveOutcome::Removed,
            "a lost removal response must preserve the removal goal"
        );
    }

    #[test]
    fn closed_removal_response_fails_closed_after_debug_diagnostic() {
        let (sender, response) = crate::runtime::oneshot();
        drop(sender);
        let mut removal = Removal::new(response);
        let mut context = Context::from_waker(Waker::noop());
        let observed = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut removal).poll(&mut context)
        }));

        #[cfg(debug_assertions)]
        assert!(
            observed.is_err(),
            "debug builds expose the broken removal response obligation"
        );
        #[cfg(not(debug_assertions))]
        assert_eq!(
            observed.expect("release fallback does not panic"),
            std::task::Poll::Ready(super::LATCHED_REMOVAL_OUTCOME)
        );
    }

    #[crate::runtime::test]
    async fn queued_fused_drop_before_exit_dispatch_suppresses_restart_accounting() {
        crate::driver::exercise_queued_fused_drop_before_exit_dispatch(|reservation| {
            Admission::new(reservation, (), AdmissionOwnership::Fused)
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
            .shutdown(Duration::from_secs(1))
            .await
            .expect("cancelled reservations leave no stragglers");
    }
}
