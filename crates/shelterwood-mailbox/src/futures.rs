use std::{
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use shelterwood_core::DeadlineBudget;

use crate::{
    ActorIdentity, ChildId, Incarnation, MailboxRuntime, Membership, capability::DisposingReceiver,
};

use super::{
    CallError, CallErrorKind, Replied, Reply, ReplyError, SendError, SendErrorKind,
    cell::{
        MailboxCell, OperationPoll, SendOperation, Submission, Withdrawal, WithdrawalDisposition,
        WithdrawalOutcome,
    },
    deadline::{DeadlineOperation, DeadlinePhase, Deadlined},
    reply::{ReplyOperation, ReplyPoll, poll_reply},
};

/// Future returned by [`ReplyReceiver::recv`](crate::ReplyReceiver::recv).
#[must_use]
pub struct ReplyReceive<T> {
    pub(super) deadlined: Deadlined<ReplyOperation<T>>,
}

impl<T> fmt::Debug for ReplyReceive<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyReceive")
            .field("started", &self.deadlined.started)
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> Future for ReplyReceive<T> {
    type Output = Result<T, ReplyError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.deadlined).poll(context)
    }
}
/// A cheap membership-addressed actor handle.
pub struct ActorRef<M> {
    member: Arc<dyn ActorIdentity>,
    mailbox: Arc<MailboxCell<M>>,
}

impl<M> ActorRef<M> {
    /// Returns the actor's child id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.member.id()
    }

    /// Returns the actor membership identity.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.member.membership()
    }

    pub(super) fn runtime(&self) -> Arc<dyn MailboxRuntime> {
        self.mailbox.runtime()
    }
}

/// Builds the façade's actor handle from its cross-crate identity and mailbox
/// capabilities without exposing a new inherent constructor on `ActorRef`.
///
/// This sibling-crate seam is not a supported constructor. The identity and
/// mailbox must belong to the same member; violating that invariant produces
/// a handle whose equality and error identity disagree with its route.
// `doc(hidden)` trades enforcement for rustdoc tidiness: the rustdoc-JSON
// reachability walk (`shelterwood-api-reachability`) skips hidden items, so it
// no longer sees this signature. It names only local types today, so nothing
// leaks; the sole remaining enforcement that the façade cannot re-export it is
// `tools/check-external-consumer.sh`'s `installable-seams` probe.
#[doc(hidden)]
pub fn actor_ref_from_parts<I, M>(member: Arc<I>, mailbox: Arc<MailboxCell<M>>) -> ActorRef<M>
where
    I: ActorIdentity + 'static,
{
    ActorRef { member, mailbox }
}

impl<M: Send + 'static> ActorRef<M> {
    /// Sends with backpressure and transparently waits through rebind windows.
    ///
    /// On a live latest-value mailbox, acceptance can replace the previous
    /// message. The displaced message is dropped inline on the task polling
    /// this send, after the new message's acceptance is visible; a panicking
    /// displaced-message destructor therefore resumes on that task.
    pub fn send(&self, message: M) -> SendFuture<M> {
        SendFuture::new(Arc::clone(&self.mailbox), message)
    }

    /// Attempts immediate acceptance without parking.
    ///
    /// On a live latest-value mailbox, acceptance can replace the previous
    /// message. The displaced message is dropped inline on the calling task,
    /// after the new message's acceptance is visible; a panicking
    /// displaced-message destructor therefore resumes on that task.
    pub fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        self.mailbox.try_send(message)
    }

    /// Sends within one acceptance budget, recovering an unaccepted message.
    ///
    /// Live latest-value displacement follows [`send`](Self::send): the
    /// displaced message is dropped inline on the task polling this send,
    /// after acceptance of the replacement is visible.
    /// A zero budget makes no acceptance attempt and returns the unaccepted
    /// message with [`SendErrorKind::TimedOut`].
    pub fn send_timeout(&self, message: M, deadline: impl Into<DeadlineBudget>) -> SendTimeout<M> {
        let runtime = self.mailbox.runtime();
        SendTimeout {
            deadlined: Deadlined::no_attempt(
                TimedSend {
                    send: self.send(message),
                },
                deadline,
                runtime,
            ),
        }
    }

    /// Creates a reply capability using this actor handle's installed runtime.
    #[must_use]
    pub fn reply_channel<T: Send + 'static>(&self) -> (Reply<T>, crate::ReplyReceiver<T>) {
        Reply::channel(self.runtime())
    }

    /// Sends a request built around one reply capability and awaits its reply.
    ///
    /// One deadline covers message construction, binding, mailbox acceptance,
    /// and response, starting when the returned future is first polled. The
    /// returned [`Replied`] identifies the accepting incarnation; [`CallError`]
    /// distinguishes a guaranteed-unaccepted timeout from an accepted request
    /// with an unknown outcome. See [`CallError`] for the required retry
    /// discipline.
    /// A zero budget constructs or submits no message and reports
    /// [`CallErrorKind::AcceptanceTimedOut`].
    ///
    /// On a latest-value mailbox, a newer accepted message can replace this
    /// request. Dropping the replaced request's [`Reply`] reports
    /// [`CallErrorKind::ReplyDropped`]. Awaiting a call to `myself()` from an
    /// actor handler deadlocks because the blocked handler is also the only code
    /// that can produce the reply; use an actor-local continuation or an
    /// incarnation-owned offload instead.
    ///
    /// A timeout before acceptance has no caller to hand the request back to,
    /// so it destroys both the recovered request and the waker it registered
    /// through isolated disposal, as cancelling a parked [`send`](Self::send)
    /// does. A panicking waker destructor is contained there rather than
    /// resuming on the awaiting task.
    pub fn call<T: Send + 'static>(
        &self,
        make_msg: impl FnOnce(Reply<T>) -> M + Send + 'static,
        deadline: impl Into<DeadlineBudget>,
    ) -> CallFuture<M, T> {
        let runtime = self.mailbox.runtime();
        CallFuture {
            deadlined: Deadlined::no_attempt(
                CallOperation {
                    actor: self.clone(),
                    make_msg: Some(Box::new(make_msg)),
                    send: None,
                    reply: None,
                    accepted: None,
                },
                deadline,
                runtime,
            ),
        }
    }
}

impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            member: Arc::clone(&self.member),
            mailbox: Arc::clone(&self.mailbox),
        }
    }
}

impl<M> fmt::Debug for ActorRef<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRef")
            .field("membership", &self.membership())
            .finish()
    }
}

// Handle identity is the slot cell, not the membership token: declaration
// lowering can rebase a provisional token behind live pre-spawn handles, and
// a token-value hash would strand entries keyed before that rebase.
impl<M> PartialEq for ActorRef<M> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.member, &other.member)
    }
}

impl<M> Eq for ActorRef<M> {}

impl<M> Hash for ActorRef<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.member) as *const ()).hash(state);
    }
}

/// Cancellation-safe future returned by [`ActorRef::send`].
#[must_use]
pub struct SendFuture<M: Send + 'static> {
    mailbox: Arc<MailboxCell<M>>,
    state: SendFutureState<M>,
}

enum SendFutureState<M> {
    Immediate(Option<M>),
    Parked(Arc<SendOperation<M>>),
    // Acceptance is remembered so a completed send stays idempotently
    // observable, matching the retained-operation behaviour this state
    // machine replaced.
    Sent(Incarnation),
    Done,
}

// No field is structurally pinned. In particular, the immediate message may
// move when first poll hands it to the mailbox.
impl<M: Send + 'static> Unpin for SendFuture<M> {}

impl<M: Send + 'static> SendFuture<M> {
    fn new(mailbox: Arc<MailboxCell<M>>, message: M) -> Self {
        Self {
            mailbox,
            state: SendFutureState::Immediate(Some(message)),
        }
    }

    /// Withdraws this send into a post-unlock effect set.
    fn withdraw(&mut self, disposition: WithdrawalDisposition) -> Withdrawal<M> {
        match std::mem::replace(&mut self.state, SendFutureState::Done) {
            SendFutureState::Immediate(mut message) => {
                Withdrawal::without_effects(WithdrawalOutcome::Withdrawn {
                    message: message
                        .take()
                        .expect("an unsubmitted send retains its message"),
                    observed: self.mailbox.current_observation(),
                })
            }
            SendFutureState::Parked(operation) => self.mailbox.withdraw(&operation, disposition),
            SendFutureState::Sent(incarnation) => {
                Withdrawal::without_effects(WithdrawalOutcome::Accepted(incarnation))
            }
            SendFutureState::Done => panic!("a completed send was withdrawn"),
        }
    }
}

impl<M: Send + 'static> fmt::Debug for SendFuture<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendFuture")
            .field(
                "submitted",
                &!matches!(self.state, SendFutureState::Immediate(_)),
            )
            .field(
                "done",
                &matches!(self.state, SendFutureState::Sent(_) | SendFutureState::Done),
            )
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> Future for SendFuture<M> {
    type Output = Result<Incarnation, SendError<M>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if let SendFutureState::Immediate(message) = &mut this.state {
            let message = message
                .take()
                .expect("an unsubmitted send retains its message");
            match this.mailbox.submit(message) {
                Submission::Accepted(incarnation) => {
                    this.state = SendFutureState::Sent(incarnation);
                    return Poll::Ready(Ok(incarnation));
                }
                Submission::Parked(operation) => {
                    this.state = SendFutureState::Parked(operation);
                }
                Submission::Terminated {
                    message,
                    final_incarnation,
                } => {
                    this.state = SendFutureState::Done;
                    return Poll::Ready(Err(SendError {
                        actor_id: this.mailbox.actor_id.clone(),
                        incarnation_observed: final_incarnation,
                        message,
                        kind: SendErrorKind::Terminated,
                    }));
                }
            }
        }

        if let SendFutureState::Sent(incarnation) = this.state {
            return Poll::Ready(Ok(incarnation));
        }
        let SendFutureState::Parked(operation) = &this.state else {
            panic!("a completed send future was polled")
        };
        // A `RawWaker` vtable is caller code, so no branch here may run one
        // under the operation lock, and a completing outcome must never depend
        // on one at all. The loop inspects the outcome locked, then -- only
        // when parking actually needs a different waker -- releases the lock to
        // clone and comes back to install it. `will_wake` is a pointer
        // comparison rather than a vtable call, so the repeat-poll fast path
        // touches no caller code whatsoever.
        //
        // Reacquiring re-reads the outcome, so an acceptance or termination
        // that lands in the unlocked window is reported instead of parked. The
        // outcome transition and the waker take stay atomic under the operation
        // lock in `SendOperation::accept`/`terminate`, so installing after that
        // re-read cannot lose a wakeup: any completion ordered after it takes
        // the waker this poll registered.
        let mut cloned_waker = None;
        loop {
            match operation.poll(cloned_waker.take(), context.waker()) {
                OperationPoll::Accepted(incarnation) => {
                    this.state = SendFutureState::Sent(incarnation);
                    return Poll::Ready(Ok(incarnation));
                }
                OperationPoll::Terminated {
                    message,
                    final_incarnation,
                } => {
                    let error = SendError {
                        actor_id: this.mailbox.actor_id.clone(),
                        incarnation_observed: final_incarnation,
                        message,
                        kind: SendErrorKind::Terminated,
                    };
                    this.state = SendFutureState::Done;
                    return Poll::Ready(Err(error));
                }
                OperationPoll::Pending => return Poll::Pending,
                OperationPoll::NeedsWakerClone => {
                    cloned_waker = Some(context.waker().clone());
                }
            }
        }
    }
}

impl<M: Send + 'static> Drop for SendFuture<M> {
    fn drop(&mut self) {
        // Cancellation recovers the unaccepted message with no caller left to
        // hand it to. Destroying it inline would run a possibly blocking or
        // panicking user destructor in this drop glue, so route the payload
        // through isolated disposal.
        match std::mem::replace(&mut self.state, SendFutureState::Done) {
            SendFutureState::Immediate(mut message) => {
                if let Some(message) = message.take() {
                    self.mailbox.dispose(message);
                }
            }
            SendFutureState::Parked(operation) => {
                let mut withdrawal = self
                    .mailbox
                    .withdraw(&operation, WithdrawalDisposition::Isolated);
                match withdrawal.take_outcome() {
                    WithdrawalOutcome::Withdrawn { message, .. }
                    | WithdrawalOutcome::Terminated { message, .. } => {
                        self.mailbox.dispose(message);
                    }
                    WithdrawalOutcome::Accepted(_) => {}
                }
                // A RawWaker vtable is caller code and there is no caller left
                // to surface a panic to. Destroying it inline would run a
                // possibly blocking or panicking user destructor in this drop
                // glue -- during an unwind that is a double panic and an abort
                // -- so route it through isolated disposal like the message.
                // Sequencing it after the message also keeps a hostile waker
                // destructor from diverting the message from that route.
                withdrawal.finish();
            }
            SendFutureState::Sent(_) | SendFutureState::Done => {}
        }
    }
}

/// Cancellation-safe future returned by [`ActorRef::send_timeout`].
#[must_use]
pub struct SendTimeout<M: Send + 'static> {
    deadlined: Deadlined<TimedSend<M>>,
}

struct TimedSend<M: Send + 'static> {
    send: SendFuture<M>,
}

impl<M: Send + 'static> fmt::Debug for SendTimeout<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendTimeout")
            .field("started", &self.deadlined.started)
            .finish_non_exhaustive()
    }
}

/// Withdraws an unaccepted send and classifies its outcome under the
/// caller's chosen waker disposition.
///
/// The outcome is taken before `finish()` releases the registered waker, so
/// under either disposition the recovered message already belongs to
/// `result` and a hostile waker destructor can neither divert nor destroy
/// it.
fn withdraw_send_with<M: Send + 'static>(
    send: &mut SendFuture<M>,
    disposition: WithdrawalDisposition,
) -> Result<Incarnation, SendError<M>> {
    let actor_id = send.mailbox.actor_id.clone();
    let mut withdrawal = send.withdraw(disposition);
    let result = match withdrawal.take_outcome() {
        WithdrawalOutcome::Withdrawn { message, observed } => Err(SendError {
            actor_id,
            incarnation_observed: observed,
            message,
            kind: SendErrorKind::TimedOut,
        }),
        WithdrawalOutcome::Accepted(incarnation) => Ok(incarnation),
        WithdrawalOutcome::Terminated { message, observed } => Err(SendError {
            actor_id,
            incarnation_observed: observed,
            message,
            kind: SendErrorKind::Terminated,
        }),
    };
    withdrawal.finish();
    result
}

fn withdraw_send<M: Send + 'static>(send: &mut SendFuture<M>) -> Result<Incarnation, SendError<M>> {
    // Unlike the cancellation path this is a normal return on the caller's own
    // task, and the recovered message goes back to that same caller. No core
    // lock is held, so the caller's waker destructor stays inline and its
    // panic stays visible.
    withdraw_send_with(send, WithdrawalDisposition::Inline)
}

impl<M: Send + 'static> DeadlineOperation for TimedSend<M> {
    type Output = Result<Incarnation, SendError<M>>;

    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        _budget: crate::deadline::Deadline,
        phase: DeadlinePhase,
    ) -> Poll<Self::Output> {
        if let Poll::Ready(result) = Pin::new(&mut self.send).poll(context) {
            return Poll::Ready(result);
        }
        if phase == DeadlinePhase::InitialAttempt {
            Poll::Pending
        } else {
            Poll::Ready(withdraw_send(&mut self.send))
        }
    }

    fn short_circuit(&mut self) -> Self::Output {
        // The send was never polled, so it was never submitted: withdrawal
        // recovers the message and reports the mailbox's current binding as
        // the newest incarnation observed during the attempt.
        withdraw_send(&mut self.send)
    }
}

impl<M: Send + 'static> Future for SendTimeout<M> {
    type Output = Result<Incarnation, SendError<M>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.deadlined).poll(context)
    }
}

/// Cancellation-safe future returned by [`ActorRef::call`].
#[must_use]
pub struct CallFuture<M: Send + 'static, T: Send + 'static> {
    deadlined: Deadlined<CallOperation<M, T>>,
}

type MessageConstructor<M, T> = Box<dyn FnOnce(Reply<T>) -> M + Send + 'static>;

struct CallOperation<M: Send + 'static, T: Send + 'static> {
    actor: ActorRef<M>,
    make_msg: Option<MessageConstructor<M, T>>,
    send: Option<SendFuture<M>>,
    reply: Option<DisposingReceiver<T>>,
    accepted: Option<Incarnation>,
}

impl<M: Send + 'static, T: Send + 'static> Drop for CallOperation<M, T> {
    fn drop(&mut self) {
        if let Some(make_msg) = self.make_msg.take() {
            // An unstarted or short-circuited call discards its constructor
            // without ever building a message. Destroying the captures inline
            // would run possibly blocking or panicking user destructors in
            // this drop glue, so route them through isolated disposal.
            self.actor.mailbox.dispose(make_msg);
        }
    }
}

impl<M: Send + 'static, T: Send + 'static> fmt::Debug for CallFuture<M, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallFuture")
            .field("started", &self.deadlined.started)
            .field("accepted", &self.deadlined.operation.accepted)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static, T: Send + 'static> CallOperation<M, T> {
    fn poll_reply(
        &mut self,
        context: &mut Context<'_>,
        incarnation: Incarnation,
        phase: DeadlinePhase,
    ) -> Poll<Result<Replied<T>, CallError>> {
        let reply = self
            .reply
            .as_mut()
            .expect("accepted call retains reply state");
        match poll_reply(reply, context, phase) {
            Poll::Ready(ReplyPoll::Value(value)) => Poll::Ready(Ok(Replied { value, incarnation })),
            Poll::Ready(ReplyPoll::SenderClosed) => Poll::Ready(Err(CallError {
                actor_id: self.actor.id().clone(),
                incarnation_observed: Some(incarnation),
                kind: CallErrorKind::ReplyDropped,
            })),
            Poll::Ready(ReplyPoll::TimedOut) => Poll::Ready(Err(CallError {
                actor_id: self.actor.id().clone(),
                incarnation_observed: Some(incarnation),
                kind: CallErrorKind::ResponseTimedOut,
            })),
            Poll::Pending => Poll::Pending,
        }
    }

    fn close_reply(&mut self) {
        if let Some(reply) = &mut self.reply {
            reply.close();
        }
    }
}

impl<M: Send + 'static, T: Send + 'static> DeadlineOperation for CallOperation<M, T> {
    type Output = Result<Replied<T>, CallError>;

    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        budget: crate::deadline::Deadline,
        phase: DeadlinePhase,
    ) -> Poll<Self::Output> {
        if self.make_msg.is_some() {
            // Capture the one overall budget before invoking user code. A
            // slow message constructor consumes acceptance/response time. The
            // shared scaffold captured `budget` before this callback runs.
            let (reply, receiver) = self.actor.reply_channel();
            let message =
                self.make_msg
                    .take()
                    .expect("unstarted call retains its message constructor")(reply);
            // The normal polling order lets an acceptance already available
            // at the exact deadline win. Construction is different: no send
            // existed before it completed, so do not start one after the
            // captured budget is strictly in the past.
            if budget.is_overdue(self.actor.mailbox.now()) {
                // Construction completed, but timeout cleanup owns the
                // unsubmitted message. Keep its potentially blocking or
                // panicking destructor off the caller task.
                self.actor.mailbox.dispose(message);
                return Poll::Ready(self.short_circuit());
            }
            self.reply = Some(receiver.receiver);
            self.send = Some(self.actor.send(message));
        }

        if self.accepted.is_none() {
            let send = self
                .send
                .as_mut()
                .expect("sending call retains send future");
            match Pin::new(send).poll(context) {
                Poll::Ready(Ok(incarnation)) => {
                    self.accepted = Some(incarnation);
                    self.send = None;
                }
                Poll::Ready(Err(error)) => {
                    self.close_reply();
                    // The call surface has no way to hand the recovered
                    // message back; route the discard through isolated
                    // disposal.
                    self.actor.mailbox.dispose(error.message);
                    return Poll::Ready(Err(CallError {
                        actor_id: error.actor_id,
                        incarnation_observed: error.incarnation_observed,
                        kind: CallErrorKind::Terminated,
                    }));
                }
                Poll::Pending => {}
            }
        }

        // Acceptance released the send future, so the reply is the only
        // remaining source of an outcome. An elapsed reply poll may still be
        // pending when a completion transition won without publishing its
        // payload yet; that transition wakes the registered waker, so waiting
        // is the answer rather than reaching for a send that no longer exists.
        if let Some(accepted) = self.accepted {
            return self.poll_reply(context, accepted, phase);
        }

        if phase == DeadlinePhase::InitialAttempt {
            return Poll::Pending;
        }

        // A call cannot return a recovered message to its caller. Isolate the
        // registered waker before taking that message so a hostile waker
        // destructor cannot unwind through and destroy the message inline.
        let result = withdraw_send_with(
            self.send
                .as_mut()
                .expect("an unaccepted call retains its send future"),
            WithdrawalDisposition::Isolated,
        );
        self.send = None;
        match result {
            Ok(incarnation) => {
                self.accepted = Some(incarnation);
                self.poll_reply(context, incarnation, DeadlinePhase::TimeoutArbitration)
            }
            Err(error) => {
                self.close_reply();
                // The call surface has no way to hand the recovered message
                // back; route the discard through isolated disposal.
                self.actor.mailbox.dispose(error.message);
                Poll::Ready(Err(CallError {
                    actor_id: error.actor_id,
                    incarnation_observed: error.incarnation_observed,
                    kind: match error.kind {
                        SendErrorKind::TimedOut => CallErrorKind::AcceptanceTimedOut,
                        SendErrorKind::Terminated => CallErrorKind::Terminated,
                        SendErrorKind::NotRunning | SendErrorKind::Full => {
                            unreachable!("withdrawal returns only timed-out or terminal errors")
                        }
                    },
                }))
            }
        }
    }

    fn short_circuit(&mut self) -> Self::Output {
        // No message was submitted to the mailbox (any constructed message
        // was already routed to isolated disposal), so there is nothing to
        // withdraw and no accepting incarnation to report.
        Err(CallError {
            actor_id: self.actor.id().clone(),
            incarnation_observed: self.actor.mailbox.current_observation(),
            kind: CallErrorKind::AcceptanceTimedOut,
        })
    }
}

impl<M: Send + 'static, T: Send + 'static> Future for CallFuture<M, T> {
    type Output = Result<Replied<T>, CallError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.deadlined).poll(context)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        mem::ManuallyDrop,
        sync::{
            Arc, Mutex, Weak,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker},
        thread::ThreadId,
        time::Duration,
    };

    use crate::{ChildId, MailboxControl, identity::ScopeIdentity};

    use super::{
        super::cell::tests::{actor, actor_for},
        ActorRef, DeadlineOperation, DeadlinePhase, MailboxCell, SendFuture, SendFutureState,
        SendOperation,
    };

    #[derive(Default)]
    struct WakerVtableCalls {
        clones: AtomicUsize,
        drops: AtomicUsize,
    }

    struct OperationLockProbe {
        operation: Weak<SendOperation<u8>>,
        calls: Arc<WakerVtableCalls>,
    }

    impl OperationLockProbe {
        fn assert_operation_unlocked(&self, callback: &str) {
            let operation = self
                .operation
                .upgrade()
                .expect("the parked send retains its operation");
            let _guard = operation
                .state
                .try_lock()
                .unwrap_or_else(|_| panic!("waker {callback} ran under the operation lock"));
        }
    }

    unsafe fn clone_operation_lock_probe(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable was produced by
        // `Arc::into_raw` for an `OperationLockProbe`. `ManuallyDrop` preserves
        // the reference represented by `data`; the returned raw waker owns the
        // newly cloned reference.
        let probe = ManuallyDrop::new(unsafe { Arc::<OperationLockProbe>::from_raw(data.cast()) });
        probe.assert_operation_unlocked("clone");
        probe.calls.clones.fetch_add(1, Ordering::SeqCst);
        let cloned = Arc::clone(&probe);
        RawWaker::new(Arc::into_raw(cloned).cast(), &OPERATION_LOCK_PROBE_VTABLE)
    }

    unsafe fn wake_operation_lock_probe(data: *const ()) {
        // SAFETY: `wake` consumes the raw-waker reference represented by data.
        drop(unsafe { Arc::<OperationLockProbe>::from_raw(data.cast()) });
    }

    unsafe fn wake_operation_lock_probe_by_ref(_data: *const ()) {}

    unsafe fn drop_operation_lock_probe(data: *const ()) {
        // SAFETY: `drop` consumes the raw-waker reference represented by data.
        let probe = unsafe { Arc::<OperationLockProbe>::from_raw(data.cast()) };
        probe.assert_operation_unlocked("drop");
        probe.calls.drops.fetch_add(1, Ordering::SeqCst);
    }

    static OPERATION_LOCK_PROBE_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_operation_lock_probe,
        wake_operation_lock_probe,
        wake_operation_lock_probe_by_ref,
        drop_operation_lock_probe,
    );

    fn operation_lock_probe_waker(
        operation: &Arc<SendOperation<u8>>,
        calls: Arc<WakerVtableCalls>,
    ) -> Waker {
        let probe = Arc::new(OperationLockProbe {
            operation: Arc::downgrade(operation),
            calls,
        });
        let raw = RawWaker::new(Arc::into_raw(probe).cast(), &OPERATION_LOCK_PROBE_VTABLE);
        // SAFETY: `raw` owns one `OperationLockProbe` reference and its vtable
        // maintains that reference count for every clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    #[test]
    fn an_accepted_send_reports_acceptance_on_every_poll() {
        let (mailbox, actor) = actor();
        MailboxControl::configure(
            &*mailbox,
            crate::policy::ResolvedDefaults::default().mailbox,
        );
        let mut identity = ScopeIdentity::new();
        let (_, mut incarnations) = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available")
            .into_pair();
        let incarnation = incarnations.mint().expect("incarnation available");
        MailboxControl::bind(&*mailbox, incarnation);

        let mut send = Box::pin(actor.send(1));
        let mut context = Context::from_waker(Waker::noop());
        let Poll::Ready(Ok(first)) = send.as_mut().poll(&mut context) else {
            panic!("a bound, non-full mailbox accepts immediately")
        };
        let Poll::Ready(Ok(second)) = send.as_mut().poll(&mut context) else {
            panic!("a completed send stays observable")
        };
        assert_eq!(first, second);
        assert_eq!(first, incarnation);
    }

    #[test]
    fn replacing_a_send_waker_runs_its_vtable_outside_the_operation_lock() {
        let (_, actor) = actor();
        let mut send = Box::pin(actor.send(1));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let operation = match &send.as_ref().get_ref().state {
            SendFutureState::Parked(operation) => operation.clone(),
            SendFutureState::Immediate(_) | SendFutureState::Sent(_) | SendFutureState::Done => {
                panic!("an unbound mailbox parks its send")
            }
        };
        let calls = Arc::new(WakerVtableCalls::default());
        let hostile = operation_lock_probe_waker(&operation, Arc::clone(&calls));

        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        drop(hostile);
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );

        assert_eq!(calls.clones.load(Ordering::SeqCst), 1);
        assert_eq!(calls.drops.load(Ordering::SeqCst), 2);
        drop(
            operation
                .state
                .lock()
                .expect("hostile waker callbacks cannot poison the operation lock"),
        );
    }

    struct CountedDrop(Arc<AtomicUsize>);

    impl Drop for CountedDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn dropping_a_never_polled_send_disposes_the_message_exactly_once() {
        let (mailbox, actor): (Arc<MailboxCell<CountedDrop>>, ActorRef<CountedDrop>) = actor_for();
        let drops = Arc::new(AtomicUsize::new(0));

        drop(actor.send(CountedDrop(Arc::clone(&drops))));

        // The never-submitted message routes through detached isolated
        // disposal on another thread, so acknowledge it with a bounded wait.
        for _ in 0..1_000 {
            if drops.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let state = mailbox.state.lock().expect("mailbox mutex poisoned");
        assert!(state.queue.is_empty());
        assert!(state.waiters().is_some_and(|waiters| waiters.is_empty()));
    }

    /// Records the thread a destructor ran on, so a test can tell an inline
    /// destructor apart from one that reached isolated disposal.
    type DisposalThread = Arc<Mutex<Option<ThreadId>>>;

    fn disposal_thread() -> DisposalThread {
        Arc::new(Mutex::new(None))
    }

    fn await_disposal(recorder: &DisposalThread) -> ThreadId {
        for _ in 0..1_000 {
            if let Some(thread) = *recorder.lock().expect("disposal recorder mutex") {
                return thread;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("isolated disposal never destroyed the value")
    }

    /// A caller waker whose destructor records itself and then panics.
    struct HostileWakerDrop(DisposalThread);

    impl Wake for HostileWakerDrop {
        fn wake(self: Arc<Self>) {
            panic!("the cancellation regressions only destroy their waker")
        }
    }

    impl Drop for HostileWakerDrop {
        fn drop(&mut self) {
            *self.0.lock().expect("disposal recorder mutex") = Some(std::thread::current().id());
            panic!("injected waker drop panic");
        }
    }

    struct ThreadRecordingDrop(DisposalThread);

    impl Drop for ThreadRecordingDrop {
        fn drop(&mut self) {
            *self.0.lock().expect("disposal recorder mutex") = Some(std::thread::current().id());
        }
    }

    struct CallMessage {
        _reply: crate::Reply<u8>,
        _payload: ThreadRecordingDrop,
    }

    struct EveryWakerDropPanics(DisposalThread);

    unsafe fn clone_panicking_drop_waker(data: *const ()) -> RawWaker {
        // SAFETY: every pointer using this vtable came from an Arc of the
        // matching type. ManuallyDrop preserves the reference represented by
        // `data`; the returned raw waker owns only the new clone.
        let probe =
            ManuallyDrop::new(unsafe { Arc::<EveryWakerDropPanics>::from_raw(data.cast()) });
        RawWaker::new(
            Arc::into_raw(Arc::clone(&probe)).cast(),
            &PANICKING_DROP_WAKER_VTABLE,
        )
    }

    unsafe fn wake_panicking_drop_waker(data: *const ()) {
        // SAFETY: wake consumes the reference represented by this raw waker.
        drop(unsafe { Arc::<EveryWakerDropPanics>::from_raw(data.cast()) });
    }

    unsafe fn wake_by_ref_panicking_drop_waker(_data: *const ()) {}

    unsafe fn drop_panicking_drop_waker(data: *const ()) {
        // SAFETY: drop consumes the reference represented by this raw waker.
        let probe = unsafe { Arc::<EveryWakerDropPanics>::from_raw(data.cast()) };
        *probe.0.lock().expect("waker disposal recorder mutex") = Some(std::thread::current().id());
        panic!("injected call waker drop panic");
    }

    static PANICKING_DROP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_panicking_drop_waker,
        wake_panicking_drop_waker,
        wake_by_ref_panicking_drop_waker,
        drop_panicking_drop_waker,
    );

    fn panicking_drop_waker(recorder: DisposalThread) -> Waker {
        let raw = RawWaker::new(
            Arc::into_raw(Arc::new(EveryWakerDropPanics(recorder))).cast(),
            &PANICKING_DROP_WAKER_VTABLE,
        );
        // SAFETY: `raw` owns one Arc reference and its vtable maintains that
        // ownership across clone, wake, and drop.
        unsafe { Waker::from_raw(raw) }
    }

    /// Parks `send` behind a hostile waker and releases the caller's own
    /// reference, leaving the registered clone as the last owner.
    fn park_behind_hostile_waker<M: Send + 'static>(
        send: &mut std::pin::Pin<Box<SendFuture<M>>>,
        recorder: &DisposalThread,
    ) {
        let hostile = Waker::from(Arc::new(HostileWakerDrop(Arc::clone(recorder))));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );
        drop(hostile);
    }

    #[test]
    fn cancelling_a_send_contains_a_hostile_waker_destructor() {
        let (_, actor) = actor();
        let mut send = Box::pin(actor.send(1u8));
        let waker_thread = disposal_thread();
        park_behind_hostile_waker(&mut send, &waker_thread);

        // Cancellation is drop glue with no caller left to receive a panic, so
        // the destructor must reach isolated disposal rather than run here.
        drop(send);

        assert_ne!(
            await_disposal(&waker_thread),
            std::thread::current().id(),
            "a cancelled send disposes its waker off the cancelling thread"
        );
    }

    #[test]
    fn a_hostile_waker_destructor_cannot_double_panic_through_cancellation() {
        let (_, actor) = actor();
        let mut send = Box::pin(actor.send(1u8));
        let waker_thread = disposal_thread();
        park_behind_hostile_waker(&mut send, &waker_thread);

        // Before containment this aborted the process: a panicking waker
        // destructor reached from drop glue during an unwind is a double panic.
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _send = send;
            panic!("primary unwind");
        }))
        .expect_err("the primary panic survives cancellation");
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("primary unwind")
        );
        assert_ne!(await_disposal(&waker_thread), std::thread::current().id());
    }

    #[test]
    fn a_hostile_waker_destructor_cannot_divert_a_cancelled_message() {
        let (_mailbox, actor): (
            Arc<MailboxCell<ThreadRecordingDrop>>,
            ActorRef<ThreadRecordingDrop>,
        ) = actor_for();

        let message_thread = disposal_thread();
        let waker_thread = disposal_thread();
        let mut send = Box::pin(actor.send(ThreadRecordingDrop(Arc::clone(&message_thread))));
        park_behind_hostile_waker(&mut send, &waker_thread);

        drop(send);

        let here = std::thread::current().id();
        assert_ne!(
            await_disposal(&message_thread),
            here,
            "a hostile waker destructor cannot divert the message from isolated disposal"
        );
        assert_ne!(await_disposal(&waker_thread), here);
    }

    #[test]
    fn call_acceptance_timeout_isolates_message_before_hostile_waker_drop() {
        let (_mailbox, actor): (Arc<MailboxCell<CallMessage>>, ActorRef<CallMessage>) = actor_for();
        let message_thread = disposal_thread();
        let waker_thread = disposal_thread();
        let mut call = Box::pin(actor.call(
            {
                let message_thread = Arc::clone(&message_thread);
                move |reply| CallMessage {
                    _reply: reply,
                    _payload: ThreadRecordingDrop(message_thread),
                }
            },
            Duration::from_secs(1),
        ));
        let hostile = panicking_drop_waker(Arc::clone(&waker_thread));
        let mut context = Context::from_waker(&hostile);
        let deadline = crate::deadline::Deadline::after(
            crate::capability::tests::runtime().now(),
            Duration::from_secs(1),
        );

        assert!(
            call.deadlined
                .operation
                .poll_deadlined(&mut context, deadline, DeadlinePhase::InitialAttempt)
                .is_pending()
        );
        let polling_thread = std::thread::current().id();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            call.deadlined.operation.poll_deadlined(
                &mut context,
                deadline,
                DeadlinePhase::TimeoutArbitration,
            )
        }));
        // The context's raw waker is deliberately leaked: its separately
        // registered clone is the object whose disposal boundary this test
        // observes, and dropping this caller-owned instance would invoke the
        // same intentionally hostile vtable on the test thread.
        std::mem::forget(hostile);

        let outcome = result.expect("isolated withdrawal contains the hostile waker destructor");
        assert!(matches!(
            outcome,
            Poll::Ready(Err(crate::CallError {
                kind: crate::CallErrorKind::AcceptanceTimedOut,
                ..
            }))
        ));
        assert_ne!(
            await_disposal(&waker_thread),
            polling_thread,
            "the pre-acceptance withdrawal disposes its registered waker off the polling task"
        );
        assert_ne!(
            await_disposal(&message_thread),
            polling_thread,
            "the recovered call message reaches isolated disposal before any waker panic can unwind"
        );
    }

    #[test]
    fn a_completed_send_reports_its_outcome_without_touching_the_caller_vtable() {
        let (mailbox, actor) = actor();
        let mut send = Box::pin(actor.send(1));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let operation = match &send.as_ref().get_ref().state {
            SendFutureState::Parked(operation) => operation.clone(),
            SendFutureState::Immediate(_) | SendFutureState::Sent(_) | SendFutureState::Done => {
                panic!("an unbound mailbox parks its send")
            }
        };
        let teardown =
            MailboxControl::prepare_termination(&*mailbox).expect("the mailbox terminates once");
        let _ = teardown.finish();

        let calls = Arc::new(WakerVtableCalls::default());
        let hostile = operation_lock_probe_waker(&operation, Arc::clone(&calls));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_ready()
        );
        assert_eq!(
            calls.clones.load(Ordering::SeqCst),
            0,
            "a completing poll never speculatively clones the caller waker"
        );
        drop(hostile);
        assert_eq!(calls.drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn repolling_a_parked_send_with_the_same_waker_touches_no_vtable() {
        let (_, actor) = actor();
        let mut send = Box::pin(actor.send(1));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let operation = match &send.as_ref().get_ref().state {
            SendFutureState::Parked(operation) => operation.clone(),
            SendFutureState::Immediate(_) | SendFutureState::Sent(_) | SendFutureState::Done => {
                panic!("an unbound mailbox parks its send")
            }
        };
        let calls = Arc::new(WakerVtableCalls::default());
        let hostile = operation_lock_probe_waker(&operation, Arc::clone(&calls));

        for _ in 0..4 {
            assert!(
                send.as_mut()
                    .poll(&mut Context::from_waker(&hostile))
                    .is_pending()
            );
        }

        assert_eq!(
            calls.clones.load(Ordering::SeqCst),
            1,
            "will_wake registers once and skips the vtable on every repoll"
        );
        assert_eq!(calls.drops.load(Ordering::SeqCst), 0);
    }
}
