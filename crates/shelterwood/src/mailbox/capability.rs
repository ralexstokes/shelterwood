//! Reply-channel receive state over the adapter's typed one-shot.
//!
//! The mailbox reaches the runtime through the façade's `crate::runtime`
//! module like every other layer; this module only adds the reply seam's own
//! caller-waker venue on top of the adapter's receiver.

use std::task::{Context, Poll};

use shelterwood_core::{
    ProxiedPoll,
    waker::{WakerAction, WakerEffects},
};

use crate::runtime::{OneShotClose, OneShotReceiver, PanicAccumulator, dispose_detached};

/// The receive edges a [`DisposingReceiver`] drives.
///
/// Production uses the adapter's typed [`OneShotReceiver`]. The parameter
/// exists so this crate's tests can script the ready, close, and arbitration
/// edges that `ProxiedPoll` retires on; it is resolved statically and is not a
/// runtime seam.
pub(crate) trait OneShotReceive<T> {
    fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>>;
    fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T>;
    fn close(&mut self);
    fn close_and_take(&mut self) -> Option<T>;
}

impl<T> OneShotReceive<T> for OneShotReceiver<T> {
    fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        Self::poll_receive(self, context)
    }

    fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T> {
        Self::close_and_poll_receive(self, context)
    }

    fn close(&mut self) {
        Self::close(self);
    }

    fn close_and_take(&mut self) -> Option<T> {
        Self::close_and_take(self)
    }
}

/// Receive state that keeps an unclaimed user value out of holder drop glue.
///
/// This differs from the adapter's `DisposingReceiver` in venue, not only in
/// surface: cancellation here retires the caller waker inline (see the `Drop`
/// implementation), while the adapter's wrapper hands it to the disposal lane.
/// The disposal function is captured at construction, where `T: Send +
/// 'static` holds, so the unbounded public holders need not repeat that bound.
pub(crate) struct DisposingReceiver<T, R: OneShotReceive<T> = OneShotReceiver<T>> {
    inner: Option<R>,
    dispose: fn(T),
    reply_poll: ProxiedPoll,
}

impl<T: Send + 'static, R: OneShotReceive<T>> DisposingReceiver<T, R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner: Some(inner),
            dispose: dispose_detached::<T>,
            reply_poll: ProxiedPoll::new(),
        }
    }
}

impl<T, R: OneShotReceive<T>> DisposingReceiver<T, R> {
    fn inner_mut(&mut self) -> &mut R {
        self.inner
            .as_mut()
            .expect("a live disposing receiver retains its channel")
    }

    pub(crate) fn close(&mut self) {
        self.inner_mut().close();
        // Contained rather than re-raised, for the same reason as the delivery
        // seams: `close`'s callers can own a live user value while they call
        // it. `CallOperation::fail_send` holds the recovered `SendError<M>`
        // message in its frame, so letting a hostile caller-waker destructor
        // unwind out of here would put that panic in flight while the message
        // is still to be destroyed -- the double-panic abort this proxy exists
        // to remove.
        let mut panics = PanicAccumulator::default();
        self.retire_reply_waker(&mut panics);
        crate::runtime::discard_panic(panics.take());
    }

    /// Takes the caller waker out of the proxy and queues its destructor into
    /// an effects sink, so it runs with no proxy mutex held.
    ///
    /// The delivery seams then *discard* whatever that destructor raises. Two
    /// costs ride on that, both accepted by #398 ruling 3:
    ///
    /// * The panic is swallowed with no diagnostic. A delivered user value has
    ///   to be returned by value, so there is no point after the handoff at
    ///   which a retained payload could be resumed -- resuming before it would
    ///   destroy the value the caller is owed. `shelterwood-core`'s `panic`
    ///   module holds that containment is false on a normal return path; this
    ///   is the deliberate exception to that guidance, not an oversight.
    /// * A caller-waker destructor that *blocks* stalls the delivering task
    ///   synchronously. The same ruling weighed a per-delivery disposal-lane
    ///   submission against it: delivery is the hot path of every successful
    ///   `call` and `recv`, and a lane submission there is real cost on every
    ///   reply, where a contained drop of a benign waker is nearly free.
    fn retire_reply_waker(&mut self, panics: &mut PanicAccumulator) {
        let mut effects = WakerEffects::default();
        self.reply_poll
            .retire(WakerAction::DropInline, &mut effects);
        effects.flush(panics);
    }
}

impl<T: Send + 'static, R: OneShotReceive<T>> DisposingReceiver<T, R> {
    pub(crate) fn poll_receive(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        // The adapter's receiver documents the pinned Tokio 1.53.1 delivery
        // seam: `Receiver::poll` obtains the result before its last
        // `Inner::drop` calls `rx_task.drop_task`, while that result can own
        // the delivered value. Probing with a framework waker and leaving only
        // the proxy registered across a pending return keeps Tokio from ever
        // destroying a caller waker there.
        // Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
        // A ready result may own a user value; `ProxiedPoll::poll` retires
        // the caller registration synchronously and contains any hostile
        // destructor panic before returning it. See `retire_reply_waker` for
        // the two costs that ride on the discard.
        self.reply_poll.poll(
            self.inner
                .as_mut()
                .expect("a live disposing receiver retains its channel"),
            context,
            R::poll_receive,
            Poll::is_pending,
        )
    }

    pub(crate) fn close_and_poll_receive(&mut self, context: &mut Context<'_>) -> OneShotClose<T> {
        // Timeout arbitration can return a concurrently delivered user value,
        // so its ready edge uses the same synchronous contained retirement as
        // the ordinary delivery path, and accepts the same two costs.
        self.reply_poll.poll(
            self.inner
                .as_mut()
                .expect("a live disposing receiver retains its channel"),
            context,
            R::close_and_poll_receive,
            |result| matches!(result, OneShotClose::Pending),
        )
    }
}

impl<T, R: OneShotReceive<T>> Drop for DisposingReceiver<T, R> {
    fn drop(&mut self) {
        let mut inner = self
            .inner
            .take()
            .expect("a live disposing receiver retains its channel");
        let mut value = None;
        let mut panics = PanicAccumulator::default();
        // Cancellation never required the timer path's blocking-disposal
        // venue, which is a claim about venue only: closing a one-shot runs on
        // the receiver's own thread with no shared driver or wheel mutex held,
        // so a slow caller-waker destructor there stalls this future alone
        // rather than every timer registration in the process. It is not that
        // close touches no wakers -- in the pinned 1.53.1, `Inner::close`
        // wakes a set tx task and calls `rx_task.drop_task()` when the channel
        // is not yet complete, so pre-proxy this path did destroy a caller
        // waker inline. The old ruling that the venue argument justified
        // leaving the one-shot registration unproxied is superseded by #398:
        // reply polling registers a proxy uniformly because delivery, not
        // cancellation, is the abort-class seam, so the waker Tokio drops here
        // is now only ever a framework proxy clone. Cancellation inherits that
        // containment without retaining a special raw-waker path of its own.
        //
        // Listed for re-audit beside the Tokio pin in the workspace `Cargo.toml`.
        // Recovery runs first so an unclaimed value reaches isolated disposal
        // before the receiver -- and therefore before the waker clone it
        // registered -- is retired; a hostile waker destructor can neither
        // divert nor destroy it. If recovery itself unwinds, the value stays
        // in the channel and `inner`'s own drop glue destroys it inline
        // rather than through the isolated lane: accepted, because reaching
        // it requires a destructor that has already panicked, and the
        // alternative is retrying a step that just failed.
        panics.run(|| value = inner.close_and_take());
        // `dispose_detached` contains its own submission failures and cannot
        // unwind; the boundary is the cheapest safe fallback, kept so this
        // drop glue does not rest on that proof.
        panics.run(|| {
            if let Some(value) = value {
                (self.dispose)(value);
            }
        });
        panics.run(|| drop(inner));
        self.retire_reply_waker(&mut panics);
    }
}
