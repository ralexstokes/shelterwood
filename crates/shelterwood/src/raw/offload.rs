//! Offloaded blocking work and its shared per-incarnation resource.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context as TaskPollContext, Poll},
};

use crate::runtime::{ActorWork, Latch, PanicAccumulator, PanicPayload, catch_panic};

use super::disposal::{PanicSlot, RawDisposal};

type OffloadFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type SharedWork = Arc<SharedOffloadState>;

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
    ///
    /// The waker driving this await is caller-owned, but the incarnation is
    /// what runs it, so it belongs to SPEC §6.2's incarnation-owned disposal
    /// funnel on the *ordinary* completion path and not only at teardown. A
    /// waker that panics while being woken has its payload retained, which
    /// fails the incarnation at its next receive boundary and suppresses
    /// `on_stop` — a third party awaiting this can therefore kill the actor.
    /// A waker that blocks instead holds the work in the incarnation's
    /// resource ledger until it returns, so incarnation teardown joins that
    /// completion rather than sailing past it. Await this from a task whose
    /// waker does neither.
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
/// Awaiting it returns the closure's value; a panic raised inside the
/// closure is resumed at the await point.
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
    finished_published: AtomicBool,
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
            finished_published: AtomicBool::new(false),
        })
    }

    pub(super) fn take_for_poll(&self) -> Option<OffloadFuture> {
        let mut state = self.state.lock().expect("offload future mutex poisoned");
        if state.cancelled || state.polling {
            return None;
        }
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
            if state.future.is_none() {
                state.future = Some(future);
                None
            } else {
                // A duplicate poll completion cannot overwrite and destroy
                // the already-retained user future under this mutex. Return
                // the duplicate to the caller's disposal path instead.
                Some(future)
            }
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

    /// Fires the completion notification once, and publishes it only after
    /// this thread's wake has finished.
    ///
    /// [`Latch::fire`] sets its fired bit inside `fire_silently`, before it
    /// wakes anything, so that bit cannot be the ledger's retirement
    /// predicate: reclamation would be free to retire the entry — dropping the
    /// task handle teardown still has to join — while a caller's completion
    /// waker is mid-wake, after which a payload recorded by that wake lands in
    /// a slot nobody reads again. Splitting the transition from the wake and
    /// claiming the *latch's own* transition, rather than a second claim
    /// beside it, keeps the wake-running thread and the publishing thread the
    /// same thread by construction: two claims could disagree, letting a
    /// concurrent canceller win the latch and run the wake while the publisher
    /// marked completion around it.
    fn fire_finished(&self) {
        if !self.finished.fire_silently() {
            return;
        }
        if let Err(payload) = catch_panic(|| self.finished.notify()) {
            // Completion waiters are caller-owned. Keep their wake panics in
            // the incarnation slot instead of letting them escape through the
            // framework-owned offload task's join result.
            self.record(payload);
        }
        // Release-publish only after the wake above returned or had its panic
        // installed in the incarnation slot. Ledger reclamation consumes this
        // edge with the matching `Acquire` in `finished_published`; weakening
        // either side would let a reclaiming actor observe retirement without
        // observing the recorded payload. No test pins that pairing — only
        // this comment does.
        self.finished_published.store(true, Ordering::Release);
    }

    /// Reports whether completion has been notified *and* its wake has
    /// returned. See [`Self::fire_finished`] for why this, not
    /// [`Latch::is_fired`], is what may retire ledger state.
    pub(super) fn finished_published(&self) -> bool {
        self.finished_published.load(Ordering::Acquire)
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
            // `state.cancel` disposes the future and then contains its own
            // completion wake, so neither a destructor panic nor a caller
            // waker panic escapes the call: both arrive through the retained
            // slot, where `PanicSlot`'s first-wins policy discards the loser.
            // `state_panic` can therefore only be a poisoned-mutex `expect`.
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
        // Unconditional on purpose. Once any contained fire has claimed the
        // latch this is a no-op, which is what keeps a caller's completion
        // wake off the actor thread while the offload task is running it. It
        // is not redundant, though: a `state.cancel` that panicked before
        // reaching its own fire — only a poisoned mutex can do that — would
        // otherwise leave `Guard::finished()` waiters unwoken forever.
        panics.run(|| {
            self.finished.fire();
        });
        panics.record(retained.take());
        panics.take()
    }
}
