//! Offloaded blocking work and its shared per-incarnation resource.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context as TaskPollContext, Poll},
};

use crate::runtime::{ActorWork, Latch, PanicAccumulator, PanicPayload, catch_panic};

use super::disposal::{PanicSlot, RawDisposal};

type OffloadFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type SharedWork = Arc<SharedOffloadState>;

const FINISHED_PENDING: u8 = 0;
const FINISHED_PUBLISHING: u8 = 1;
const FINISHED_PUBLISHED: u8 = 2;

/// Marker returned to an offload continuation when its one deadline expires.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the offload deadline elapsed")]
pub struct DeadlineElapsed;

/// An owned cancel-on-drop lease for a scoped offload.
#[must_use = "dropping the guard cancels its offload; call detach to keep only incarnation ownership"]
pub struct Guard {
    pub(super) cancellation: Latch,
    pub(super) finished: Latch,
    pub(super) armed: bool,
}

impl fmt::Debug for Guard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Guard")
            .field("detached", &!self.armed)
            .finish()
    }
}

impl Guard {
    /// Reports whether cancellation has been requested for this lease.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_fired()
    }

    /// Reports whether the work completed or incarnation teardown requested cancellation.
    ///
    /// A teardown notification is not a join: under hard abort the task may
    /// still be unwinding when this becomes true.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished.is_fired()
    }

    /// Waits for work completion or an incarnation-teardown cancellation request.
    ///
    /// This notification does not join work that is being hard-aborted.
    pub async fn finished(&self) {
        self.finished.fired().await;
    }

    /// Cancels the guarded offload immediately and consumes the guard.
    pub fn cancel(mut self) {
        self.cancellation.fire();
        self.armed = false;
    }

    /// Releases this lease's cancel-on-drop behavior.
    pub fn detach(mut self) {
        self.armed = false;
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.fire();
        }
    }
}

/// A blocking operation whose thread cooperatively observes actor cancellation.
///
/// Cancellation cannot forcibly stop a blocking thread. Dropping this future
/// or hard-aborting its actor detaches the thread after requesting cooperative
/// cancellation; Shelterwood does not join it, and any later value or panic is
/// discarded through detached disposal. A submission rejected during runtime
/// teardown moves to a detached Shelterwood thread; an operation that never
/// runs — cancelled with the runtime, or with no thread left to start it —
/// makes awaiting the future panic with a runtime-teardown cancellation
/// diagnostic. The operation must therefore be safe to outlive its actor.
#[must_use = "dropping this future requests cooperative cancellation and detaches the thread"]
pub struct Blocking<T> {
    pub(super) future: Pin<Box<dyn Future<Output = T> + Send + 'static>>,
    pub(super) cancellation: Latch,
    pub(super) completed: bool,
}

impl<T> fmt::Debug for Blocking<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Blocking")
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}

impl<T> Future for Blocking<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskPollContext<'_>) -> Poll<Self::Output> {
        match self.future.as_mut().poll(context) {
            Poll::Ready(value) => {
                self.completed = true;
                Poll::Ready(value)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for Blocking<T> {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellation.fire();
        }
    }
}

pub(super) struct OffloadFutureState {
    future: Option<OffloadFuture>,
    pub(super) polling: bool,
    cancelled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OffloadPoll {
    Pending,
    Finished,
}

pub(super) struct SharedOffloadState {
    // Polling takes the future out of this mutex. Cancellation either takes
    // an idle future or marks an in-progress poll so that the poller disposes
    // it, always after releasing the lock.
    pub(super) state: Mutex<OffloadFutureState>,
    disposal: RawDisposal,
    finished: Latch,
    finished_publication: AtomicU8,
}

impl SharedOffloadState {
    pub(super) fn new(future: OffloadFuture, disposal: RawDisposal, finished: Latch) -> SharedWork {
        Arc::new(Self {
            state: Mutex::new(OffloadFutureState {
                future: Some(future),
                polling: false,
                cancelled: false,
            }),
            disposal,
            finished,
            finished_publication: AtomicU8::new(FINISHED_PENDING),
        })
    }

    pub(super) fn take_for_poll(&self) -> Option<OffloadFuture> {
        let mut state = self.state.lock().expect("offload future mutex poisoned");
        if state.cancelled {
            return None;
        }
        debug_assert!(!state.polling, "offload work must have one poller");
        let future = state.future.take();
        state.polling = future.is_some();
        future
    }

    pub(super) fn finish_poll(
        &self,
        future: OffloadFuture,
        outcome: OffloadPoll,
    ) -> Option<OffloadFuture> {
        let mut state = self.state.lock().expect("offload future mutex poisoned");
        state.polling = false;
        if outcome == OffloadPoll::Pending && !state.cancelled {
            debug_assert!(state.future.is_none());
            state.future = Some(future);
            None
        } else {
            Some(future)
        }
    }

    fn record(&self, payload: PanicPayload) {
        // Dropping a losing or cancelled operation can panic after its body
        // has stopped running, so every retained panic must wake the actor's
        // control plane independently of ordinary event delivery.
        self.disposal.record(payload);
    }

    fn fire_finished(&self) {
        // The latch exposes its fired bit before invoking caller wakers. Claim
        // that notification once so a concurrent cancellation cannot call a
        // no-op second `fire` and mark publication complete while the winning
        // caller is still inside a hostile wake.
        if self
            .finished_publication
            .compare_exchange(
                FINISHED_PENDING,
                FINISHED_PUBLISHING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        if let Err(payload) = catch_panic(|| self.finished.fire()) {
            // Completion waiters are caller-owned. Keep their wake panics in
            // the incarnation slot instead of letting them escape through the
            // framework-owned offload task's join result.
            self.record(payload);
        }
        // Release-publish only after a hostile wake has either returned or
        // had its panic installed in the incarnation slot. Ledger reclamation
        // uses this edge instead of the latch's earlier fired bit.
        self.finished_publication
            .store(FINISHED_PUBLISHED, Ordering::Release);
    }

    pub(super) fn finished_published(&self) -> bool {
        self.finished_publication.load(Ordering::Acquire) == FINISHED_PUBLISHED
    }

    pub(super) fn dispose(&self, future: Option<OffloadFuture>) {
        if let Some(future) = future {
            self.disposal.dispose(future);
        }
    }

    pub(super) fn cancel(&self) {
        let future = {
            let mut state = self.state.lock().expect("offload future mutex poisoned");
            state.cancelled = true;
            if state.polling {
                None
            } else {
                state.future.take()
            }
        };
        self.dispose(future);
        self.fire_finished();
    }
}

pub(super) struct SharedOffloadFuture(pub(super) SharedWork);

impl Future for SharedOffloadFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut TaskPollContext<'_>) -> Poll<Self::Output> {
        let Some(mut future) = self.0.take_for_poll() else {
            return Poll::Ready(());
        };
        let polled = catch_panic(|| future.as_mut().poll(context));
        match polled {
            Ok(Poll::Pending) => {
                let dispose = self.0.finish_poll(future, OffloadPoll::Pending);
                if dispose.is_some() {
                    self.0.dispose(dispose);
                    self.0.fire_finished();
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            Ok(Poll::Ready(())) => {
                let dispose = self.0.finish_poll(future, OffloadPoll::Finished);
                self.0.dispose(dispose);
                self.0.fire_finished();
                Poll::Ready(())
            }
            Err(payload) => {
                let dispose = self.0.finish_poll(future, OffloadPoll::Finished);
                self.0.record(payload);
                self.0.dispose(dispose);
                self.0.fire_finished();
                Poll::Ready(())
            }
        }
    }
}

pub(super) struct OffloadResource {
    pub(super) cancellation: Latch,
    pub(super) finished: Latch,
    pub(super) state: Option<SharedWork>,
    pub(super) task: Option<ActorWork>,
}

impl OffloadResource {
    pub(super) fn cancel(&mut self, retained: &PanicSlot) -> Option<PanicPayload> {
        let mut panics = PanicAccumulator::default();
        panics.run(|| {
            self.cancellation.fire();
        });
        panics.record(retained.take());
        if let Some(state) = &self.state {
            // `state.cancel` disposes the future before firing `finished`.
            // Pull that contained destructor failure into the accumulator
            // before recording a later wake panic escaping the call.
            let state_panic = catch_panic(|| state.cancel()).err();
            panics.record(retained.take());
            panics.record(state_panic);
        }
        if let Some(task) = &self.task {
            // These are complementary: `state.cancel()` synchronously
            // disposes idle work (capturing destructor panic) or marks an
            // in-progress poll to dispose on return, while abort independently
            // requests cancellation of the runtime task driving that poll.
            // Neither substitutes for the other.
            panics.run(|| task.abort());
            panics.record(retained.take());
        }
        panics.run(|| {
            self.finished.fire();
        });
        panics.record(retained.take());
        panics.take()
    }
}
