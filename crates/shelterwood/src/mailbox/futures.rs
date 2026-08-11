use std::{
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    ChildId, Incarnation, Membership,
    cells::MemberCell,
    runtime::{DisposingReceiver, dispose_detached},
};

use super::{
    CallError, CallErrorKind, Replied, Reply, ReplyError, SendError, SendErrorKind,
    cell::{MailboxCell, OperationOutcome, SendOperation, Submission, Withdrawal},
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
    member: Arc<MemberCell>,
    mailbox: Arc<MailboxCell<M>>,
}

impl<M> ActorRef<M> {
    pub(crate) fn new(member: Arc<MemberCell>, mailbox: Arc<MailboxCell<M>>) -> Self {
        Self { member, mailbox }
    }

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
    pub fn send_timeout(&self, message: M, deadline: Duration) -> SendTimeout<M> {
        SendTimeout {
            deadlined: Deadlined::new(
                TimedSend {
                    send: self.send(message),
                },
                deadline,
            ),
        }
    }

    /// Sends a request built around one reply capability and awaits its reply.
    ///
    /// One deadline covers message construction, binding, mailbox acceptance,
    /// and response, starting when the returned future is first polled. The
    /// returned [`Replied`] identifies the accepting incarnation; [`CallError`]
    /// distinguishes a guaranteed-unaccepted timeout from an accepted request
    /// with an unknown outcome. See [`CallError`] for the required retry
    /// discipline.
    ///
    /// On a latest-value mailbox, a newer accepted message can replace this
    /// request. Dropping the replaced request's [`Reply`] reports
    /// [`CallErrorKind::ReplyDropped`]. Awaiting a call to `myself()` from an
    /// actor handler deadlocks because the blocked handler is also the only code
    /// that can produce the reply; use an actor-local continuation or an
    /// incarnation-owned offload instead.
    pub fn call<T: Send + 'static>(
        &self,
        make_msg: impl FnOnce(Reply<T>) -> M + Send + 'static,
        deadline: Duration,
    ) -> CallFuture<M, T> {
        CallFuture {
            deadlined: Deadlined::new(
                CallOperation {
                    actor: self.clone(),
                    make_msg: Some(Box::new(make_msg)),
                    send: None,
                    reply: None,
                    accepted: None,
                    dispose_constructor: dispose_detached::<MessageConstructor<M, T>>,
                },
                deadline,
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
        Arc::as_ptr(&self.member).hash(state);
    }
}

/// Cancellation-safe future returned by [`ActorRef::send`].
#[must_use]
pub struct SendFuture<M> {
    mailbox: Arc<MailboxCell<M>>,
    state: SendFutureState<M>,
    // Captured where `M: Send + 'static` holds so the unbounded `Drop` impl
    // can route a withdrawn message through isolated disposal.
    dispose: fn(M),
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
impl<M> Unpin for SendFuture<M> {}

impl<M: Send + 'static> SendFuture<M> {
    fn new(mailbox: Arc<MailboxCell<M>>, message: M) -> Self {
        Self {
            mailbox,
            state: SendFutureState::Immediate(Some(message)),
            dispose: dispose_detached::<M>,
        }
    }

    /// Withdraws this send, also releasing any waker it had registered.
    ///
    /// The waker travels back to the caller unchanged: only the caller knows
    /// whether a `RawWaker` destructor may run on its thread.
    fn withdraw(&mut self) -> (Withdrawal<M>, Option<Waker>) {
        match std::mem::replace(&mut self.state, SendFutureState::Done) {
            SendFutureState::Immediate(mut message) => (
                Withdrawal::Withdrawn {
                    message: message
                        .take()
                        .expect("an unsubmitted send retains its message"),
                    observed: self.mailbox.current_observation(),
                },
                None,
            ),
            SendFutureState::Parked(operation) => self.mailbox.withdraw(&operation),
            SendFutureState::Sent(incarnation) => (Withdrawal::Accepted(incarnation), None),
            SendFutureState::Done => panic!("a completed send was withdrawn"),
        }
    }
}

impl<M> fmt::Debug for SendFuture<M> {
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
        let mut cloned_waker: Option<Waker> = None;
        loop {
            let mut operation_state = operation
                .state
                .lock()
                .expect("send operation mutex poisoned");
            match &mut operation_state.outcome {
                OperationOutcome::Accepted(incarnation) => {
                    let incarnation = *incarnation;
                    drop(operation_state);
                    this.state = SendFutureState::Sent(incarnation);
                    return Poll::Ready(Ok(incarnation));
                }
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => {
                    let error = SendError {
                        actor_id: this.mailbox.actor_id.clone(),
                        incarnation_observed: *final_incarnation,
                        message: message
                            .take()
                            .expect("a terminal operation retains its message until observed"),
                        kind: SendErrorKind::Terminated,
                    };
                    drop(operation_state);
                    this.state = SendFutureState::Done;
                    return Poll::Ready(Err(error));
                }
                OperationOutcome::Waiting { .. } => {
                    if let Some(next_waker) = cloned_waker.take() {
                        let stale_waker = operation_state.waker.replace(next_waker);
                        drop(operation_state);
                        drop(stale_waker);
                        return Poll::Pending;
                    }
                    if operation_state
                        .waker
                        .as_ref()
                        .is_some_and(|registered| registered.will_wake(context.waker()))
                    {
                        return Poll::Pending;
                    }
                    drop(operation_state);
                    cloned_waker = Some(context.waker().clone());
                }
                OperationOutcome::Withdrawn => panic!("a withdrawn send future was polled"),
            }
        }
    }
}

impl<M> Drop for SendFuture<M> {
    fn drop(&mut self) {
        // Cancellation recovers the unaccepted message with no caller left to
        // hand it to. Destroying it inline would run a possibly blocking or
        // panicking user destructor in this drop glue, so route the payload
        // through isolated disposal.
        match std::mem::replace(&mut self.state, SendFutureState::Done) {
            SendFutureState::Immediate(mut message) => {
                if let Some(message) = message.take() {
                    (self.dispose)(message);
                }
            }
            SendFutureState::Parked(operation) => {
                let (withdrawal, waker) = self.mailbox.withdraw(&operation);
                match withdrawal {
                    Withdrawal::Withdrawn { message, .. }
                    | Withdrawal::Terminated { message, .. } => {
                        (self.dispose)(message);
                    }
                    Withdrawal::Accepted(_) => {}
                }
                // A RawWaker vtable is caller code and there is no caller left
                // to surface a panic to. Destroying it inline would run a
                // possibly blocking or panicking user destructor in this drop
                // glue -- during an unwind that is a double panic and an abort
                // -- so route it through isolated disposal like the message.
                // Sequencing it after the message also keeps a hostile waker
                // destructor from diverting the message from that route.
                if let Some(waker) = waker {
                    dispose_detached(waker);
                }
            }
            SendFutureState::Sent(_) | SendFutureState::Done => {}
        }
    }
}

/// Cancellation-safe future returned by [`ActorRef::send_timeout`].
#[must_use]
pub struct SendTimeout<M> {
    deadlined: Deadlined<TimedSend<M>>,
}

struct TimedSend<M> {
    send: SendFuture<M>,
}

impl<M> fmt::Debug for SendTimeout<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendTimeout")
            .field("started", &self.deadlined.started)
            .finish_non_exhaustive()
    }
}

fn withdraw_send<M: Send + 'static>(send: &mut SendFuture<M>) -> Result<Incarnation, SendError<M>> {
    let actor_id = send.mailbox.actor_id.clone();
    let (withdrawal, waker) = send.withdraw();
    let result = match withdrawal {
        Withdrawal::Withdrawn { message, observed } => Err(SendError {
            actor_id,
            incarnation_observed: observed,
            message,
            kind: SendErrorKind::TimedOut,
        }),
        Withdrawal::Accepted(incarnation) => Ok(incarnation),
        Withdrawal::Terminated { message, observed } => Err(SendError {
            actor_id,
            incarnation_observed: observed,
            message,
            kind: SendErrorKind::Terminated,
        }),
    };
    // Unlike the cancellation path this is a normal return on the caller's own
    // task, so the caller's waker destructor stays inline and its panic stays
    // visible; no core lock is held and the recovered message already belongs
    // to `result`.
    drop(waker);
    result
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
pub struct CallFuture<M, T> {
    deadlined: Deadlined<CallOperation<M, T>>,
}

type MessageConstructor<M, T> = Box<dyn FnOnce(Reply<T>) -> M + Send + 'static>;

struct CallOperation<M, T> {
    actor: ActorRef<M>,
    make_msg: Option<MessageConstructor<M, T>>,
    send: Option<SendFuture<M>>,
    reply: Option<DisposingReceiver<T>>,
    accepted: Option<Incarnation>,
    // Captured where `M: Send + 'static` holds so the unbounded `Drop` impl
    // can route an unused constructor and its captures through isolated
    // disposal.
    dispose_constructor: fn(MessageConstructor<M, T>),
}

impl<M, T> Drop for CallOperation<M, T> {
    fn drop(&mut self) {
        if let Some(make_msg) = self.make_msg.take() {
            // An unstarted or short-circuited call discards its constructor
            // without ever building a message. Destroying the captures inline
            // would run possibly blocking or panicking user destructors in
            // this drop glue, so route them through isolated disposal.
            (self.dispose_constructor)(make_msg);
        }
    }
}

impl<M, T> fmt::Debug for CallFuture<M, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallFuture")
            .field("started", &self.deadlined.started)
            .field("accepted", &self.deadlined.operation.accepted)
            .finish_non_exhaustive()
    }
}

impl<M, T> CallOperation<M, T> {
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
            if phase == DeadlinePhase::TimeoutArbitration {
                return Poll::Ready(self.short_circuit());
            }
            // Capture the one overall budget before invoking user code. A
            // slow message constructor consumes acceptance/response time. The
            // shared scaffold captured `budget` before this callback runs.
            let (reply, receiver) = Reply::channel();
            let message =
                self.make_msg
                    .take()
                    .expect("unstarted call retains its message constructor")(reply);
            // The normal polling order lets an acceptance already available
            // at the exact deadline win. Construction is different: no send
            // existed before it completed, so do not start one after the
            // captured budget is strictly in the past.
            if budget.is_overdue(crate::runtime::now()) {
                // Construction completed, but timeout cleanup owns the
                // unsubmitted message. Keep its potentially blocking or
                // panicking destructor off the caller task.
                dispose_detached(message);
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
                    dispose_detached(error.message);
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

        let result = withdraw_send(
            self.send
                .as_mut()
                .expect("an unaccepted call retains its send future"),
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
                dispose_detached(error.message);
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

    use crate::{
        ChildId,
        cells::{MailboxControl, MemberCell},
        identity::ScopeIdentity,
    };

    use super::{
        super::cell::tests::actor, ActorRef, MailboxCell, SendFuture, SendFutureState,
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
            SendFutureState::Parked(operation) => Arc::clone(operation),
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
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("actor");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox: Arc<MailboxCell<CountedDrop>> = MailboxCell::new(member.id().clone());
        member.attach_mailbox(mailbox.clone());
        let actor = ActorRef::new(member, Arc::clone(&mailbox));
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
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("actor");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox: Arc<MailboxCell<ThreadRecordingDrop>> = MailboxCell::new(member.id().clone());
        member.attach_mailbox(mailbox.clone());
        let actor = ActorRef::new(member, Arc::clone(&mailbox));

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
    fn a_completed_send_reports_its_outcome_without_touching_the_caller_vtable() {
        let (mailbox, actor) = actor();
        let mut send = Box::pin(actor.send(1));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let operation = match &send.as_ref().get_ref().state {
            SendFutureState::Parked(operation) => Arc::clone(operation),
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
            SendFutureState::Parked(operation) => Arc::clone(operation),
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
