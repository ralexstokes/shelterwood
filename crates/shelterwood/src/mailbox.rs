//! Membership-owned actor mailboxes and request/reply capabilities.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    ChildId, Incarnation, Mailbox, Membership,
    cells::{MailboxControl, MailboxDisposal, MailboxTermination, MemberCell},
    runtime::{
        DisposingReceiver, OneShotClose, OneShotSender, PanicAccumulator, PanicPayload, Signal,
        SignalWatcher, dispose_detached, resume_panic,
    },
};

/// The kind of a failed actor send.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum SendErrorKind {
    /// The membership has no accepting incarnation right now.
    NotRunning,
    /// A bounded FIFO mailbox has no free capacity.
    Full,
    /// The membership is terminal.
    Terminated,
    /// A timed send was withdrawn before acceptance.
    TimedOut,
}

/// A failed send with its recoverable message and identity evidence.
pub struct SendError<M> {
    /// Target actor id.
    pub actor_id: ChildId,
    /// Incarnation required by the error-kind identity table.
    pub incarnation_observed: Option<Incarnation>,
    /// Message proven not to have been accepted.
    pub message: M,
    /// Failure category.
    pub kind: SendErrorKind,
}

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendError")
            .field("actor_id", &self.actor_id)
            .field("incarnation_observed", &self.incarnation_observed)
            .field("message", &"<recoverable message>")
            .field("kind", &self.kind)
            .finish()
    }
}

impl<M> fmt::Display for SendError<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "send to {} failed: {}", self.actor_id, self.kind)
    }
}

impl<M> std::error::Error for SendError<M> {}

impl fmt::Display for SendErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotRunning => "actor is not accepting messages",
            Self::Full => "mailbox is full",
            Self::Terminated => "actor membership is terminal",
            Self::TimedOut => "acceptance deadline elapsed",
        })
    }
}

/// The kind of a failed request/reply call.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum CallErrorKind {
    /// The membership terminalized before acceptance.
    Terminated,
    /// The request was withdrawn before acceptance.
    AcceptanceTimedOut,
    /// The request was accepted but its response missed the deadline.
    ResponseTimedOut,
    /// The accepted request's reply capability was dropped unanswered.
    ReplyDropped,
}

/// A failed request/reply call with retry-discipline identity evidence.
///
/// [`CallErrorKind::AcceptanceTimedOut`] proves that the request was not
/// accepted and is safe to retry. [`CallErrorKind::ResponseTimedOut`] means
/// that it was accepted with an unknown outcome and must be reconciled rather
/// than blindly retried. A [`CallErrorKind::ReplyDropped`] retry is safe only
/// for an idempotent operation, under one overall deadline, after snapshots or
/// lifecycle events show an incarnation that
/// [`supersedes`](Incarnation::supersedes) `incarnation_observed`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallError {
    /// Target actor id.
    pub actor_id: ChildId,
    /// Accepting or observed incarnation, as determined by [`CallErrorKind`].
    pub incarnation_observed: Option<Incarnation>,
    /// Failure category.
    pub kind: CallErrorKind,
}

impl fmt::Display for CallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "call to {} failed: {}", self.actor_id, self.kind)
    }
}

impl std::error::Error for CallError {}

impl fmt::Display for CallErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Terminated => "actor membership terminalized before acceptance",
            Self::AcceptanceTimedOut => "request acceptance deadline elapsed",
            Self::ResponseTimedOut => "response deadline elapsed after acceptance",
            Self::ReplyDropped => "reply capability was dropped unanswered",
        })
    }
}

/// A successful call result and the incarnation that accepted its request.
#[derive(Clone, Eq, PartialEq)]
pub struct Replied<T> {
    /// Reply value.
    pub value: T,
    /// Incarnation that accepted the request.
    pub incarnation: Incarnation,
}

impl<T> fmt::Debug for Replied<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Replied")
            .field("value", &"<reply value>")
            .field("incarnation", &self.incarnation)
            .finish()
    }
}

/// Failure of a standalone reply receiver.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ReplyError {
    /// The reply capability was dropped unanswered.
    Dropped,
    /// The response deadline elapsed.
    Timeout,
}

impl fmt::Display for ReplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Dropped => "reply capability was dropped unanswered",
            Self::Timeout => "reply deadline elapsed",
        })
    }
}

impl std::error::Error for ReplyError {}

trait DeadlineOperation {
    type Output;

    /// Polls the operation before or after the shared deadline transition.
    ///
    /// This is a second operation poll after the timer becomes ready so
    /// completion or acceptance at the exact boundary wins before
    /// operation-specific timeout cleanup runs. It may remain pending only
    /// when an atomic completion transition won but has not published its
    /// payload yet; that transition must wake the registered operation waker.
    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        budget: crate::deadline::Deadline,
        elapsed: bool,
    ) -> Poll<Self::Output>;

    /// Resolves a zero-width budget without ever attempting the operation.
    ///
    /// A zero budget is a short-circuit, not a raced deadline: nothing is
    /// submitted and no completion is observed, so this reports the
    /// operation's timeout outcome and performs only its own cleanup.
    fn short_circuit(&mut self) -> Self::Output;
}

/// First-poll deadline capture shared by every public mailbox deadline future.
struct Deadlined<F> {
    operation: F,
    duration: Duration,
    budget: Option<crate::deadline::Deadline>,
    timer: Option<crate::runtime::BoxedSleep>,
    started: bool,
    elapsed: bool,
    done: bool,
}

impl<F> Deadlined<F> {
    fn new(operation: F, duration: Duration) -> Self {
        Self {
            operation,
            duration,
            budget: None,
            timer: None,
            started: false,
            elapsed: false,
            done: false,
        }
    }
}

impl<F: DeadlineOperation + Unpin> Future for Deadlined<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if !this.started {
            this.started = true;
            let budget = crate::runtime::deadline(this.duration);
            this.budget = Some(budget);
            if !this.duration.is_zero() {
                this.timer = Some(crate::runtime::sleep_deadline(budget));
            }
        }
        // A zero budget short-circuits: the operation is never attempted, so
        // nothing is submitted and no completion is observed. The expiry
        // boundary below governs only non-zero budgets, and the two rules
        // therefore never compete.
        if this.duration.is_zero() {
            this.done = true;
            return Poll::Ready(this.operation.short_circuit());
        }
        let budget = this
            .budget
            .expect("a started deadline future retains its captured budget");
        if let Poll::Ready(result) = this.operation.poll_deadlined(context, budget, false) {
            this.done = true;
            return Poll::Ready(result);
        }
        if !this.elapsed {
            if this
                .timer
                .as_mut()
                .expect("an unelapsed deadline future retains its timer")
                .as_mut()
                .poll(context)
                .is_pending()
            {
                return Poll::Pending;
            }
            // The timer is a one-shot future: polling it again after it
            // resolves panics. Latch the transition and release it, so an
            // elapsed poll that stays pending re-polls only the operation.
            this.elapsed = true;
            this.timer = None;
        }
        let result = this.operation.poll_deadlined(context, budget, true);
        this.done = result.is_ready();
        result
    }
}

/// A consuming, infallible reply capability.
///
/// Dropping an unanswered capability is completion: its receiver observes
/// [`ReplyError::Dropped`]. Dropping or timing out the receiver instead closes
/// the channel, so a late [`Reply::send`] safely discards its value through
/// isolated disposal.
pub struct Reply<T> {
    sender: Option<OneShotSender<T>>,
    answered: bool,
}

impl<T> fmt::Debug for Reply<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reply")
            .field("answered", &self.answered)
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> Reply<T> {
    /// Creates a reply capability and its single owned receiver.
    #[must_use]
    pub fn channel() -> (Self, ReplyReceiver<T>) {
        let (sender, receiver) = crate::runtime::oneshot();
        (
            Self {
                sender: Some(sender),
                answered: false,
            },
            ReplyReceiver {
                receiver: Some(DisposingReceiver::new(receiver)),
            },
        )
    }

    /// Consumes the capability and delivers or discards the reply.
    pub fn send(mut self, value: T) {
        self.answered = true;
        let sender = self
            .sender
            .take()
            .expect("an unanswered reply retains its sender");
        // A cancelled receiver rejects the value. Destroying it inline would
        // run a possibly blocking or panicking user destructor on the replying
        // actor; route the discard through isolated disposal instead.
        if let Err(unclaimed) = sender.send(value) {
            dispose_detached(unclaimed);
        }
    }
}

/// The owned, non-cloneable receive half of [`Reply::channel`].
pub struct ReplyReceiver<T> {
    receiver: Option<DisposingReceiver<T>>,
}

impl<T> fmt::Debug for ReplyReceiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyReceiver")
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> ReplyReceiver<T> {
    /// Consumes the receiver and waits within one response-only budget.
    pub fn recv(mut self, deadline: Duration) -> ReplyReceive<T> {
        ReplyReceive {
            deadlined: Deadlined::new(
                ReplyOperation {
                    receiver: self.receiver.take().expect("unused reply receiver is live"),
                },
                deadline,
            ),
        }
    }
}

struct ReplyOperation<T> {
    receiver: DisposingReceiver<T>,
}

impl<T> DeadlineOperation for ReplyOperation<T> {
    type Output = Result<T, ReplyError>;

    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        _budget: crate::deadline::Deadline,
        elapsed: bool,
    ) -> Poll<Self::Output> {
        match self.receiver.inner.poll_receive(context) {
            Poll::Ready(Some(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(None) => Poll::Ready(Err(ReplyError::Dropped)),
            Poll::Pending if elapsed => match self.receiver.inner.close_and_poll_receive(context) {
                OneShotClose::Value(value) => Poll::Ready(Ok(value)),
                OneShotClose::SenderClosed => Poll::Ready(Err(ReplyError::Dropped)),
                OneShotClose::Empty => Poll::Ready(Err(ReplyError::Timeout)),
                OneShotClose::Pending => Poll::Pending,
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn short_circuit(&mut self) -> Self::Output {
        self.receiver.inner.close();
        Err(ReplyError::Timeout)
    }
}

/// Future returned by [`ReplyReceiver::recv`].
#[must_use]
pub struct ReplyReceive<T> {
    deadlined: Deadlined<ReplyOperation<T>>,
}

impl<T> fmt::Debug for ReplyReceive<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyReceive")
            .field("started", &self.deadlined.started)
            .field("done", &self.deadlined.done)
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> Future for ReplyReceive<T> {
    type Output = Result<T, ReplyError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.deadlined).poll(context)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindingStatus {
    Unbound,
    Bound(Incarnation),
    Frozen(Incarnation),
    Terminal(Option<Incarnation>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MailboxKind {
    Queue(NonZeroUsize),
    Latest,
}

struct Envelope<M> {
    message: M,
    accepted_sequence: u64,
}

enum OperationOutcome<M> {
    Waiting {
        message: Option<M>,
        newest_observed: Option<Incarnation>,
    },
    Accepted(Incarnation),
    Terminated {
        message: Option<M>,
        final_incarnation: Option<Incarnation>,
    },
    Withdrawn,
}

struct OperationState<M> {
    outcome: OperationOutcome<M>,
    waker: Option<Waker>,
    registration: Option<WaiterId>,
}

struct SendOperation<M> {
    state: Mutex<OperationState<M>>,
}

// Lock order is mailbox state, then send-operation state. Code that starts
// from an operation lock must release it before entering the mailbox. Paths
// that take only the operation lock (polling, detached teardown) never reach
// back into mailbox state. This keeps acceptance, withdrawal, and waiter
// registration on one acyclic ordering.

impl<M> SendOperation<M> {
    fn new(message: M) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OperationState {
                outcome: OperationOutcome::Waiting {
                    message: Some(message),
                    newest_observed: None,
                },
                waker: None,
                registration: None,
            }),
        })
    }

    fn register(&self, registration: WaiterId) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        debug_assert!(state.registration.is_none());
        debug_assert!(matches!(state.outcome, OperationOutcome::Waiting { .. }));
        state.registration = Some(registration);
    }

    fn clear_registration(&self, registration: WaiterId) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        if state.registration == Some(registration) {
            state.registration = None;
        } else {
            // A cancellation can win after terminal teardown detaches the
            // queue but before it discharges this entry.
            debug_assert!(state.registration.is_none());
        }
    }

    fn observe(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("send operation mutex poisoned");
        if let OperationOutcome::Waiting {
            newest_observed, ..
        } = &mut state.outcome
        {
            *newest_observed = Some(incarnation);
        }
    }

    fn accept(&self, incarnation: Incarnation) -> Option<(M, Option<Waker>)> {
        let (message, wake) = {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return None;
            };
            let message = message.take()?;
            state.outcome = OperationOutcome::Accepted(incarnation);
            (message, state.waker.take())
        };
        Some((message, wake))
    }

    fn terminate(&self, final_incarnation: Option<Incarnation>) {
        let wake = {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return;
            };
            let message = message.take();
            state.outcome = OperationOutcome::Terminated {
                message,
                final_incarnation,
            };
            state.waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

struct MailboxState<M> {
    kind: Option<MailboxKind>,
    status: BindingStatus,
    last_bound: Option<Incarnation>,
    queue: VecDeque<Envelope<M>>,
    latest: Option<Envelope<M>>,
    waiters: WaiterQueue<M>,
    accepted: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct WaiterId(u64);

/// FIFO registrations with direct removal by a send operation.
///
/// Monotonic keys are insertion order, so the first map entry is the oldest
/// waiter. Keys are never reused, making stale cancellation ids harmless.
/// `u64::MAX` is a poison key and is never minted; exhaustion remains poisoned
/// instead of wrapping back into the live id domain.
struct WaiterQueue<M> {
    entries: BTreeMap<WaiterId, Arc<SendOperation<M>>>,
    next_id: u64,
    #[cfg(test)]
    direct_removals: usize,
}

impl<M> Default for WaiterQueue<M> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            next_id: 0,
            #[cfg(test)]
            direct_removals: 0,
        }
    }
}

impl<M> WaiterQueue<M> {
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn push_back(&mut self, operation: Arc<SendOperation<M>>) -> WaiterId {
        let Some(next) = self.next_id.checked_add(1) else {
            panic!("mailbox waiter identity space exhausted");
        };
        if next == u64::MAX {
            self.next_id = u64::MAX;
            panic!("mailbox waiter identity space exhausted");
        }
        self.next_id = next;
        let id = WaiterId(next);
        let replaced = self.entries.insert(id, operation);
        debug_assert!(replaced.is_none());
        id
    }

    fn park(&mut self, operation: &Arc<SendOperation<M>>) {
        let registration = self.push_back(Arc::clone(operation));
        operation.register(registration);
    }

    fn observe_all(&self, incarnation: Incarnation) {
        for operation in self.entries.values() {
            operation.observe(incarnation);
        }
    }

    fn pop_front(&mut self) -> Option<(WaiterId, Arc<SendOperation<M>>)> {
        let entry = self.entries.pop_first();
        #[cfg(test)]
        if entry.is_some() {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        entry
    }

    fn remove(&mut self, id: WaiterId) -> Option<Arc<SendOperation<M>>> {
        let operation = self.entries.remove(&id)?;
        #[cfg(test)]
        {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        Some(operation)
    }
}

struct Promotion<M: Send + 'static> {
    displaced: Vec<Envelope<M>>,
    wakers: Vec<Waker>,
}

impl<M: Send + 'static> Default for Promotion<M> {
    fn default() -> Self {
        Self {
            displaced: Vec::new(),
            wakers: Vec::new(),
        }
    }
}

impl<M: Send + 'static> Promotion<M> {
    // Drop is the completion path on every ownership edge. `finish` only
    // names the intentional consume point; its Drop still wakes all accepted
    // senders and isolates every displaced-message destructor.
    fn finish(self) {}

    fn finish_isolated(mut self) {
        let displaced = std::mem::take(&mut self.displaced);
        if !displaced.is_empty() {
            crate::runtime::dispose_detached(MailboxPayload {
                queue: Some(displaced.into()),
                latest: None,
                retired: Vec::new(),
            });
        }
        self.finish();
    }
}

impl<M: Send + 'static> Drop for Promotion<M> {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        for waker in self.wakers.drain(..) {
            panics.run(|| waker.wake());
        }
        for displaced in self.displaced.drain(..) {
            panics.run(|| drop(displaced));
        }
    }
}

struct Termination<M> {
    waiters: WaiterQueue<M>,
    final_incarnation: Option<Incarnation>,
}

impl<M> Termination<M> {
    fn finish(&mut self, retired: &mut Vec<Arc<SendOperation<M>>>) -> Option<PanicPayload> {
        let mut panics = PanicAccumulator::default();
        let final_incarnation = self.final_incarnation;
        while let Some((registration, waiter)) = self.waiters.pop_front() {
            waiter.clear_registration(registration);
            panics.run(|| {
                waiter.terminate(final_incarnation);
            });
            // A withdrawn sender may leave this as the final operation owner,
            // so retain it for the same isolated path as unread messages.
            retired.push(waiter);
        }
        panics.take()
    }
}

struct MailboxPayload<M> {
    queue: Option<VecDeque<Envelope<M>>>,
    latest: Option<Envelope<M>>,
    retired: Vec<Arc<SendOperation<M>>>,
}

impl<M> Drop for MailboxPayload<M> {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        if let Some(mut queue) = self.queue.take() {
            while let Some(envelope) = queue.pop_front() {
                panics.run(|| drop(envelope));
            }
        }
        if let Some(latest) = self.latest.take() {
            panics.run(|| drop(latest));
        }
        for waiter in self.retired.drain(..) {
            panics.run(|| drop(waiter));
        }
    }
}

struct MailboxTeardown<M: Send + 'static> {
    changed: Option<Signal>,
    payload: crate::runtime::Isolated<MailboxPayload<M>>,
    termination: Option<Termination<M>>,
}

impl<M: Send + 'static> MailboxTeardown<M> {
    fn finish_framework(&mut self) -> Option<PanicPayload> {
        let mut panics = PanicAccumulator::default();
        if let Some(changed) = self.changed.take() {
            panics.run(|| changed.pulse());
        }
        if let Some(mut termination) = self.termination.take() {
            panics.record(termination.finish(&mut self.payload.get_mut().retired));
        }
        panics.take()
    }
}

impl<M: Send + 'static> MailboxTermination for MailboxTeardown<M> {
    fn finish(mut self: Box<Self>) -> Option<MailboxDisposal> {
        let panic = self.finish_framework();
        let payload = self
            .payload
            .take()
            .map(|payload| Box::new(payload) as MailboxDisposal);
        if let Some(panic) = panic {
            if let Some(payload) = payload {
                crate::runtime::dispose_detached(payload);
            }
            resume_panic(panic);
        }
        payload
    }
}

impl<M: Send + 'static> Drop for MailboxTeardown<M> {
    fn drop(&mut self) {
        let mut panics = PanicAccumulator::default();
        panics.record(self.finish_framework());
        if let Some(payload) = self.payload.take() {
            crate::runtime::dispose_detached(payload);
        }
    }
}

pub(crate) struct MailboxCell<M> {
    actor_id: ChildId,
    state: Mutex<MailboxState<M>>,
    changed: Signal,
}

impl<M> fmt::Debug for MailboxCell<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MailboxCell")
            .field("actor_id", &self.actor_id)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> MailboxCell<M> {
    pub(crate) fn new(actor_id: ChildId) -> Arc<Self> {
        Arc::new(Self {
            actor_id,
            state: Mutex::new(MailboxState {
                kind: None,
                status: BindingStatus::Unbound,
                last_bound: None,
                queue: VecDeque::new(),
                latest: None,
                waiters: WaiterQueue::default(),
                accepted: 0,
            }),
            changed: Signal::default(),
        })
    }

    fn submit(&self, operation: &Arc<SendOperation<M>>) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.status {
            BindingStatus::Terminal(final_incarnation) => {
                drop(state);
                operation.terminate(final_incarnation);
            }
            BindingStatus::Bound(incarnation) => {
                operation.observe(incarnation);
                let can_accept = match state.kind {
                    Some(MailboxKind::Queue(capacity)) => {
                        state.waiters.is_empty() && state.queue.len() < capacity.get()
                    }
                    Some(MailboxKind::Latest) => true,
                    None => false,
                };
                if can_accept {
                    let (message, wake) = operation
                        .accept(incarnation)
                        .expect("a newly submitted operation still owns its message");
                    state.accepted = state.accepted.saturating_add(1);
                    let accepted_sequence = state.accepted;
                    let displaced = match state.kind {
                        Some(MailboxKind::Queue(_)) => {
                            state.queue.push_back(Envelope {
                                message,
                                accepted_sequence,
                            });
                            None
                        }
                        Some(MailboxKind::Latest) => state.latest.replace(Envelope {
                            message,
                            accepted_sequence,
                        }),
                        None => unreachable!(),
                    };
                    drop(state);
                    self.changed.pulse();
                    if let Some(waker) = wake {
                        waker.wake();
                    }
                    drop(displaced);
                } else {
                    state.waiters.park(operation);
                }
            }
            BindingStatus::Frozen(incarnation) => {
                operation.observe(incarnation);
                state.waiters.park(operation);
            }
            BindingStatus::Unbound => state.waiters.park(operation),
        }
    }

    fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.status {
            BindingStatus::Terminal(final_incarnation) => Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: final_incarnation,
                message,
                kind: SendErrorKind::Terminated,
            }),
            BindingStatus::Unbound => Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: None,
                message,
                kind: SendErrorKind::NotRunning,
            }),
            BindingStatus::Frozen(incarnation) => Err(SendError {
                actor_id: self.actor_id.clone(),
                incarnation_observed: Some(incarnation),
                message,
                kind: SendErrorKind::NotRunning,
            }),
            BindingStatus::Bound(incarnation) => match state.kind {
                Some(MailboxKind::Queue(capacity))
                    if !state.waiters.is_empty() || state.queue.len() >= capacity.get() =>
                {
                    Err(SendError {
                        actor_id: self.actor_id.clone(),
                        incarnation_observed: Some(incarnation),
                        message,
                        kind: SendErrorKind::Full,
                    })
                }
                Some(MailboxKind::Queue(_)) => {
                    state.accepted = state.accepted.saturating_add(1);
                    let accepted_sequence = state.accepted;
                    state.queue.push_back(Envelope {
                        message,
                        accepted_sequence,
                    });
                    drop(state);
                    self.changed.pulse();
                    Ok(incarnation)
                }
                Some(MailboxKind::Latest) => {
                    state.accepted = state.accepted.saturating_add(1);
                    let accepted_sequence = state.accepted;
                    let displaced = state.latest.replace(Envelope {
                        message,
                        accepted_sequence,
                    });
                    drop(state);
                    self.changed.pulse();
                    drop(displaced);
                    Ok(incarnation)
                }
                None => Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: None,
                    message,
                    kind: SendErrorKind::NotRunning,
                }),
            },
        }
    }

    fn receive(
        &self,
        incarnation: Incarnation,
        allow_frozen: bool,
        accepted_through: Option<u64>,
    ) -> Option<M> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        let eligible = match state.status {
            BindingStatus::Bound(current) => current == incarnation,
            BindingStatus::Frozen(current) => allow_frozen && current == incarnation,
            BindingStatus::Unbound | BindingStatus::Terminal(_) => false,
        };
        if !eligible {
            return None;
        }
        let message = match state.kind {
            Some(MailboxKind::Queue(_)) => {
                let eligible = state.queue.front().is_some_and(|item| {
                    accepted_through.is_none_or(|limit| item.accepted_sequence <= limit)
                });
                eligible
                    .then(|| state.queue.pop_front())
                    .flatten()
                    .map(|item| item.message)
            }
            Some(MailboxKind::Latest) => {
                let eligible = state.latest.as_ref().is_some_and(|item| {
                    accepted_through.is_none_or(|limit| item.accepted_sequence <= limit)
                });
                eligible
                    .then(|| state.latest.take())
                    .flatten()
                    .map(|item| item.message)
            }
            None => None,
        };
        let promotion = if message.is_some() && matches!(state.status, BindingStatus::Bound(_)) {
            promote_waiters(&mut state)
        } else {
            Promotion::default()
        };
        drop(state);
        if message.is_some() {
            self.changed.pulse();
        }
        promotion.finish();
        message
    }

    fn current_observation(&self) -> Option<Incarnation> {
        match self.state.lock().expect("mailbox mutex poisoned").status {
            BindingStatus::Bound(incarnation) | BindingStatus::Frozen(incarnation) => {
                Some(incarnation)
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => None,
        }
    }

    fn watcher(&self) -> SignalWatcher {
        self.changed.watcher()
    }

    fn accepted_sequence(&self) -> u64 {
        self.state.lock().expect("mailbox mutex poisoned").accepted
    }
}

impl<M> MailboxCell<M> {
    fn withdraw(&self, operation: &Arc<SendOperation<M>>) -> Withdrawal<M> {
        let mut mailbox = self.state.lock().expect("mailbox mutex poisoned");
        let current_observation = match mailbox.status {
            BindingStatus::Bound(incarnation) | BindingStatus::Frozen(incarnation) => {
                Some(incarnation)
            }
            BindingStatus::Unbound | BindingStatus::Terminal(_) => None,
        };
        let (result, registration) = {
            let mut state = operation
                .state
                .lock()
                .expect("send operation mutex poisoned");
            match &mut state.outcome {
                OperationOutcome::Waiting {
                    message,
                    newest_observed,
                } => {
                    let message = message
                        .take()
                        .expect("a waiting operation must retain its message");
                    // The mailbox lock makes this one evidence snapshot: a
                    // binding either precedes withdrawal and contributes its
                    // incarnation, or follows the completed withdrawal. This
                    // also covers an operation first submitted by an elapsed
                    // (including zero-duration) deadline poll.
                    let observed = (*newest_observed).or(current_observation);
                    state.outcome = OperationOutcome::Withdrawn;
                    state.waker = None;
                    (
                        Withdrawal::Withdrawn { message, observed },
                        state.registration.take(),
                    )
                }
                OperationOutcome::Accepted(incarnation) => (
                    Withdrawal::Accepted(*incarnation),
                    state.registration.take(),
                ),
                OperationOutcome::Terminated {
                    message,
                    final_incarnation,
                } => (
                    Withdrawal::Terminated {
                        message: message
                            .take()
                            .expect("a terminal operation must retain its message"),
                        observed: *final_incarnation,
                    },
                    state.registration.take(),
                ),
                OperationOutcome::Withdrawn => {
                    panic!("a send operation was withdrawn more than once")
                }
            }
        };
        if let Some(registration) = registration {
            if let Some(removed) = mailbox.waiters.remove(registration) {
                debug_assert!(Arc::ptr_eq(&removed, operation));
            } else {
                debug_assert!(matches!(mailbox.status, BindingStatus::Terminal(_)));
            }
        }
        result
    }
}

impl<M: Send + 'static> MailboxControl for MailboxCell<M> {
    fn configure(&self, mailbox: Mailbox) {
        let kind = match mailbox {
            Mailbox::Queue(Some(capacity)) => MailboxKind::Queue(capacity),
            Mailbox::Queue(None) => MailboxKind::Queue(
                NonZeroUsize::new(crate::policy::DEFAULT_MAILBOX_CAPACITY)
                    .expect("library mailbox capacity is non-zero"),
            ),
            Mailbox::Latest => MailboxKind::Latest,
        };
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.kind {
            Some(existing) => debug_assert_eq!(existing, kind),
            None => state.kind = Some(kind),
        }
    }

    fn bind(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if matches!(state.status, BindingStatus::Terminal(_)) {
            return;
        }
        debug_assert!(
            state.kind.is_some(),
            "mailbox must be configured before its first bind"
        );
        debug_assert!(
            matches!(state.status, BindingStatus::Unbound),
            "mailbox must close the prior incarnation before rebinding"
        );
        state.status = BindingStatus::Bound(incarnation);
        state.last_bound = Some(incarnation);
        // Binding is an observation edge for every operation that remained
        // parked through it, including FIFO overflow that cannot be promoted
        // into the current capacity. Withdrawal takes the mailbox lock before
        // the operation lock, so a concurrent timeout sees either the prior
        // evidence or this incarnation consistently with which edge won.
        state.waiters.observe_all(incarnation);
        let promotion = promote_waiters(&mut state);
        drop(state);
        self.changed.pulse();
        promotion.finish_isolated();
    }

    fn freeze(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if state.status == BindingStatus::Bound(incarnation) {
            state.status = BindingStatus::Frozen(incarnation);
            drop(state);
            self.changed.pulse();
        }
    }

    fn close(&self, incarnation: Incarnation) -> Option<MailboxDisposal> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if !matches!(
            state.status,
            BindingStatus::Bound(current) | BindingStatus::Frozen(current)
                if current == incarnation
        ) {
            return None;
        }
        state.status = BindingStatus::Unbound;
        let queue = std::mem::take(&mut state.queue);
        let latest = state.latest.take();
        drop(state);
        self.changed.pulse();
        Some(Box::new(MailboxPayload {
            queue: Some(queue),
            latest,
            retired: Vec::new(),
        }) as MailboxDisposal)
    }

    fn prepare_termination(&self) -> Option<Box<dyn MailboxTermination>> {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if matches!(state.status, BindingStatus::Terminal(_)) {
            return None;
        }
        let final_incarnation = state.last_bound;
        state.status = BindingStatus::Terminal(final_incarnation);
        let queue = std::mem::take(&mut state.queue);
        let latest = state.latest.take();
        let waiters = std::mem::take(&mut state.waiters);
        let termination = Termination {
            waiters,
            final_incarnation,
        };
        drop(state);
        Some(Box::new(MailboxTeardown {
            changed: Some(self.changed.clone()),
            payload: crate::runtime::Isolated::new(MailboxPayload {
                queue: Some(queue),
                latest,
                retired: Vec::new(),
            }),
            termination: Some(termination),
        }))
    }

    #[cfg(debug_assertions)]
    fn bind_order_valid(&self) -> bool {
        let state = self.state.lock().expect("mailbox mutex poisoned");
        state.kind.is_some()
            && matches!(
                state.status,
                BindingStatus::Unbound | BindingStatus::Terminal(_)
            )
    }
}

fn promote_waiters<M: Send + 'static>(state: &mut MailboxState<M>) -> Promotion<M> {
    let mut promotion = Promotion::default();
    let Some(kind) = state.kind else {
        return promotion;
    };
    let BindingStatus::Bound(incarnation) = state.status else {
        return promotion;
    };
    let available = match kind {
        MailboxKind::Queue(capacity) => capacity.get().saturating_sub(state.queue.len()),
        MailboxKind::Latest => usize::MAX,
    };
    let mut accepted = 0usize;
    while accepted < available {
        let Some((registration, operation)) = state.waiters.pop_front() else {
            break;
        };
        operation.clear_registration(registration);
        operation.observe(incarnation);
        let Some((message, wake)) = operation.accept(incarnation) else {
            continue;
        };
        if let Some(waker) = wake {
            promotion.wakers.push(waker);
        }
        state.accepted = state.accepted.saturating_add(1);
        let accepted_sequence = state.accepted;
        match kind {
            MailboxKind::Queue(_) => state.queue.push_back(Envelope {
                message,
                accepted_sequence,
            }),
            MailboxKind::Latest => {
                if let Some(displaced) = state.latest.replace(Envelope {
                    message,
                    accepted_sequence,
                }) {
                    promotion.displaced.push(displaced);
                }
            }
        }
        accepted += 1;
    }
    promotion
}

enum Withdrawal<M> {
    Withdrawn {
        message: M,
        observed: Option<Incarnation>,
    },
    Accepted(Incarnation),
    Terminated {
        message: M,
        observed: Option<Incarnation>,
    },
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
    pub fn send(&self, message: M) -> SendFuture<M> {
        SendFuture::new(self.clone(), message)
    }

    /// Attempts immediate acceptance without parking.
    pub fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        self.mailbox.try_send(message)
    }

    /// Sends within one acceptance budget, recovering an unaccepted message.
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

// Handle identity is the slot cell, not the membership token: lowering a
// rebuilt nested declaration rebases the token behind live pre-spawn handles,
// and a token-value hash would strand entries keyed before the rebase.
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
    actor: ActorRef<M>,
    operation: Arc<SendOperation<M>>,
    // Captured where `M: Send + 'static` holds so the unbounded `Drop` impl
    // can route a withdrawn message through isolated disposal.
    dispose: fn(M),
    submitted: bool,
    done: bool,
}

impl<M: Send + 'static> SendFuture<M> {
    fn new(actor: ActorRef<M>, message: M) -> Self {
        Self {
            actor,
            operation: SendOperation::new(message),
            dispose: dispose_detached::<M>,
            submitted: false,
            done: false,
        }
    }

    fn withdraw(&mut self) -> Withdrawal<M> {
        let result = self.actor.mailbox.withdraw(&self.operation);
        self.done = true;
        result
    }
}

impl<M> fmt::Debug for SendFuture<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendFuture")
            .field("submitted", &self.submitted)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> Future for SendFuture<M> {
    type Output = Result<Incarnation, SendError<M>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.submitted {
            self.submitted = true;
            self.actor.mailbox.submit(&self.operation);
        }
        let mut state = self
            .operation
            .state
            .lock()
            .expect("send operation mutex poisoned");
        match &mut state.outcome {
            OperationOutcome::Accepted(incarnation) => {
                let incarnation = *incarnation;
                drop(state);
                self.done = true;
                Poll::Ready(Ok(incarnation))
            }
            OperationOutcome::Terminated {
                message,
                final_incarnation,
            } => {
                let error = SendError {
                    actor_id: self.actor.id().clone(),
                    incarnation_observed: *final_incarnation,
                    message: message
                        .take()
                        .expect("a terminal operation retains its message until observed"),
                    kind: SendErrorKind::Terminated,
                };
                drop(state);
                self.done = true;
                Poll::Ready(Err(error))
            }
            OperationOutcome::Waiting { .. } => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
            OperationOutcome::Withdrawn => panic!("a withdrawn send future was polled"),
        }
    }
}

impl<M> Drop for SendFuture<M> {
    fn drop(&mut self) {
        if !self.done {
            // Cancellation recovers the unaccepted message with no caller
            // left to hand it to. Destroying it inline would run a possibly
            // blocking or panicking user destructor in this drop glue, so
            // route the payload through isolated disposal.
            match self.actor.mailbox.withdraw(&self.operation) {
                Withdrawal::Withdrawn { message, .. } | Withdrawal::Terminated { message, .. } => {
                    (self.dispose)(message);
                }
                Withdrawal::Accepted(_) => {}
            }
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
            .field("done", &self.deadlined.done)
            .finish_non_exhaustive()
    }
}

fn withdraw_send<M: Send + 'static>(send: &mut SendFuture<M>) -> Result<Incarnation, SendError<M>> {
    let actor_id = send.actor.id().clone();
    match send.withdraw() {
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
    }
}

impl<M: Send + 'static> DeadlineOperation for TimedSend<M> {
    type Output = Result<Incarnation, SendError<M>>;

    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        _budget: crate::deadline::Deadline,
        elapsed: bool,
    ) -> Poll<Self::Output> {
        if let Poll::Ready(result) = Pin::new(&mut self.send).poll(context) {
            return Poll::Ready(result);
        }
        if !elapsed {
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
            .field("done", &self.deadlined.done)
            .finish_non_exhaustive()
    }
}

impl<M, T> CallOperation<M, T> {
    fn poll_reply(
        &mut self,
        context: &mut Context<'_>,
        incarnation: Incarnation,
        deadline_elapsed: bool,
    ) -> Poll<Result<Replied<T>, CallError>> {
        let reply = self
            .reply
            .as_mut()
            .expect("accepted call retains reply state");
        match reply.inner.poll_receive(context) {
            Poll::Ready(Some(value)) => Poll::Ready(Ok(Replied { value, incarnation })),
            Poll::Ready(None) => Poll::Ready(Err(CallError {
                actor_id: self.actor.id().clone(),
                incarnation_observed: Some(incarnation),
                kind: CallErrorKind::ReplyDropped,
            })),
            Poll::Pending if deadline_elapsed => {
                match reply.inner.close_and_poll_receive(context) {
                    OneShotClose::Value(value) => Poll::Ready(Ok(Replied { value, incarnation })),
                    OneShotClose::SenderClosed => Poll::Ready(Err(CallError {
                        actor_id: self.actor.id().clone(),
                        incarnation_observed: Some(incarnation),
                        kind: CallErrorKind::ReplyDropped,
                    })),
                    OneShotClose::Empty => Poll::Ready(Err(CallError {
                        actor_id: self.actor.id().clone(),
                        incarnation_observed: Some(incarnation),
                        kind: CallErrorKind::ResponseTimedOut,
                    })),
                    OneShotClose::Pending => Poll::Pending,
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn close_reply(&mut self) {
        if let Some(reply) = &mut self.reply {
            reply.inner.close();
        }
    }
}

impl<M: Send + 'static, T: Send + 'static> DeadlineOperation for CallOperation<M, T> {
    type Output = Result<Replied<T>, CallError>;

    fn poll_deadlined(
        &mut self,
        context: &mut Context<'_>,
        budget: crate::deadline::Deadline,
        elapsed: bool,
    ) -> Poll<Self::Output> {
        if self.make_msg.is_some() {
            if elapsed {
                return Poll::Ready(self.short_circuit());
            }
            // Capture the one overall budget before invoking user code. A
            // slow message constructor consumes acceptance/response time. The
            // shared scaffold captured `budget` before this callback runs.
            let (reply, mut receiver) = Reply::channel();
            let message =
                self.make_msg
                    .take()
                    .expect("unstarted call retains its message constructor")(reply);
            // The normal polling order lets an acceptance already available
            // at the exact deadline win. Construction is different: no send
            // existed before it completed, so do not start one after the
            // captured budget is strictly in the past.
            if budget.is_overdue(crate::runtime::now()) {
                return Poll::Ready(self.short_circuit());
            }
            self.reply = receiver.receiver.take();
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
            return self.poll_reply(context, accepted, elapsed);
        }

        if !elapsed {
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
                self.poll_reply(context, incarnation, true)
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
        // No message was ever constructed, so there is nothing to withdraw
        // and no accepting incarnation to report.
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

pub(crate) struct MailboxReceiver<M> {
    mailbox: Arc<MailboxCell<M>>,
    incarnation: Incarnation,
    watcher: SignalWatcher,
}

impl<M: Send + 'static> MailboxReceiver<M> {
    pub(crate) fn new(mailbox: Arc<MailboxCell<M>>, incarnation: Incarnation) -> Self {
        let watcher = mailbox.watcher();
        Self {
            mailbox,
            incarnation,
            watcher,
        }
    }

    pub(crate) fn try_recv(&self) -> Option<M> {
        self.mailbox.receive(self.incarnation, true, None)
    }

    pub(crate) fn try_recv_live(&self) -> Option<M> {
        self.mailbox.receive(self.incarnation, false, None)
    }

    pub(crate) fn try_recv_live_through(&self, accepted_sequence: u64) -> Option<M> {
        self.mailbox
            .receive(self.incarnation, false, Some(accepted_sequence))
    }

    pub(crate) fn accepted_sequence(&self) -> u64 {
        self.mailbox.accepted_sequence()
    }

    pub(crate) async fn changed(&mut self) {
        self.watcher.changed().await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    use crate::{ChildId, Mailbox, SendErrorKind, cells::MemberCell, identity::ScopeIdentity};

    use super::{ActorRef, MailboxCell, MailboxControl};

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("injected waker panic");
        }
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn actor() -> (Arc<MailboxCell<u8>>, ActorRef<u8>) {
        let mut identity = ScopeIdentity::new();
        let id = ChildId::from("actor");
        let member = MemberCell::new(
            id.clone(),
            identity.mint_membership(&id).expect("membership available"),
        );
        let mailbox = MailboxCell::new(member.id().clone());
        member.attach_mailbox(mailbox.clone());
        (Arc::clone(&mailbox), ActorRef::new(member, mailbox))
    }

    fn park_with(future: &mut std::pin::Pin<Box<super::SendFuture<u8>>>, waker: &Waker) {
        let mut context = Context::from_waker(waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "mailbox must be configured before its first bind")]
    fn binding_before_configuration_trips_the_driver_contract() {
        let (mailbox, _) = actor();
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available");
        let mut incarnations = identity.incarnation_counter(membership);
        let incarnation = ScopeIdentity::mint_incarnation(membership, &mut incarnations)
            .expect("incarnation available");

        MailboxControl::bind(&*mailbox, incarnation);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "mailbox must close the prior incarnation before rebinding")]
    fn rebinding_before_close_trips_the_driver_contract() {
        let (mailbox, _) = actor();
        MailboxControl::configure(&*mailbox, Mailbox::default());
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&ChildId::from("actor"))
            .expect("membership available");
        let mut incarnations = identity.incarnation_counter(membership);
        let first = ScopeIdentity::mint_incarnation(membership, &mut incarnations)
            .expect("first incarnation available");
        let second = ScopeIdentity::mint_incarnation(membership, &mut incarnations)
            .expect("second incarnation available");

        MailboxControl::bind(&*mailbox, first);
        MailboxControl::bind(&*mailbox, second);
    }

    #[test]
    fn cancelling_many_parked_sends_unlinks_one_registration_each() {
        const SENDS: usize = 16_384;

        let (mailbox, actor) = actor();
        let mut sends = Vec::with_capacity(SENDS);
        for _ in 0..SENDS {
            let mut send = Box::pin(actor.send(1));
            park_with(&mut send, Waker::noop());
            sends.push(Some(send));
        }
        assert_eq!(
            mailbox
                .state
                .lock()
                .expect("mailbox mutex poisoned")
                .waiters
                .len(),
            SENDS
        );

        // Exercise one interior, the head, and the tail explicitly, then a
        // deterministic odd/even permutation. The counter measures queue
        // operations rather than elapsed time or scheduler behavior.
        for index in [SENDS / 2, 0, SENDS - 1] {
            drop(sends[index].take().expect("selected send remains live"));
        }
        for index in (1..SENDS - 1).step_by(2) {
            if let Some(send) = sends[index].take() {
                drop(send);
            }
        }
        for send in sends.into_iter().flatten() {
            drop(send);
        }

        let state = mailbox.state.lock().expect("mailbox mutex poisoned");
        assert!(state.waiters.is_empty());
        assert_eq!(
            state.waiters.direct_removals, SENDS,
            "mass cancellation must do one direct unlink per parked send"
        );
    }

    #[test]
    fn promotion_wakes_every_sender_when_multiple_wakers_panic() {
        let (mailbox, actor) = actor();
        let mut first = Box::pin(actor.send(1));
        let mut second = Box::pin(actor.send(2));
        let mut third = Box::pin(actor.send(3));
        let first_panicking = Waker::from(Arc::new(PanicWake));
        let second_panicking = Waker::from(Arc::new(PanicWake));
        let wakes = Arc::new(AtomicUsize::new(0));
        let counting = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        park_with(&mut first, &first_panicking);
        park_with(&mut second, &second_panicking);
        park_with(&mut third, &counting);
        MailboxControl::configure(&*mailbox, Mailbox::default());
        let mut generations = {
            let mut identity = ScopeIdentity::new();
            let membership = identity
                .mint_membership(&ChildId::from("actor"))
                .expect("membership available");
            (membership, identity.incarnation_counter(membership))
        };
        let incarnation = ScopeIdentity::mint_incarnation(generations.0, &mut generations.1)
            .expect("incarnation available");

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                MailboxControl::bind(&*mailbox, incarnation);
            }))
            .is_err()
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            first.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
        assert!(matches!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
        assert!(matches!(
            third.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
    }

    #[test]
    fn termination_discharge_reaches_every_sender_when_multiple_wakers_panic() {
        let (mailbox, actor) = actor();
        let mut first = Box::pin(actor.send(1));
        let mut second = Box::pin(actor.send(2));
        let mut third = Box::pin(actor.send(3));
        let first_panicking = Waker::from(Arc::new(PanicWake));
        let second_panicking = Waker::from(Arc::new(PanicWake));
        let wakes = Arc::new(AtomicUsize::new(0));
        let counting = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
        park_with(&mut first, &first_panicking);
        park_with(&mut second, &second_panicking);
        park_with(&mut third, &counting);

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                drop(MailboxControl::prepare_termination(&*mailbox));
            }))
            .is_err()
        );
        assert_eq!(wakes.load(Ordering::SeqCst), 1);
        for future in [&mut first, &mut second, &mut third] {
            let Poll::Ready(Err(error)) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("every parked send must be discharged");
            };
            assert_eq!(error.kind, SendErrorKind::Terminated);
        }
    }

    #[test]
    fn cancellation_can_race_detached_terminal_teardown() {
        let (mailbox, actor) = actor();
        let mut send = Box::pin(actor.send(1));
        park_with(&mut send, Waker::noop());

        let teardown = MailboxControl::prepare_termination(&*mailbox)
            .expect("live mailbox prepares terminal teardown");
        // Teardown is deliberately retained: cancellation sees terminal state
        // after the waiter queue was detached but before it was discharged.
        drop(send);
        drop(teardown);

        assert!(
            mailbox
                .state
                .lock()
                .expect("mailbox mutex remains healthy")
                .waiters
                .is_empty()
        );
    }

    #[test]
    fn stale_waiter_id_cannot_unlink_a_later_registration() {
        let mut waiters = super::WaiterQueue::default();
        let first = super::SendOperation::new(1_u8);
        let first_id = waiters.push_back(Arc::clone(&first));
        first.register(first_id);
        let removed = waiters.remove(first_id).expect("first waiter is live");
        removed.clear_registration(first_id);

        let second = super::SendOperation::new(2_u8);
        let second_id = waiters.push_back(Arc::clone(&second));
        second.register(second_id);
        assert_ne!(first_id, second_id, "waiter identities are never reused");
        assert!(
            waiters.remove(first_id).is_none(),
            "a stale cancellation cannot unlink a later waiter"
        );
        let removed = waiters
            .remove(second_id)
            .expect("second waiter remains live");
        assert!(Arc::ptr_eq(&removed, &second));
        removed.clear_registration(second_id);
        assert!(waiters.is_empty());
    }

    #[test]
    fn waiter_queue_preserves_fifo_across_removal() {
        let mut waiters = super::WaiterQueue::default();
        let first = super::SendOperation::new(1_u8);
        let second = super::SendOperation::new(2_u8);
        let third = super::SendOperation::new(3_u8);
        let first_id = waiters.push_back(Arc::clone(&first));
        let second_id = waiters.push_back(Arc::clone(&second));
        let third_id = waiters.push_back(Arc::clone(&third));

        assert!(Arc::ptr_eq(
            &waiters.remove(second_id).expect("middle waiter is live"),
            &second
        ));
        let (popped_first, operation) = waiters.pop_front().expect("head remains live");
        assert_eq!(popped_first, first_id);
        assert!(Arc::ptr_eq(&operation, &first));
        let (popped_third, operation) = waiters.pop_front().expect("tail remains live");
        assert_eq!(popped_third, third_id);
        assert!(Arc::ptr_eq(&operation, &third));
        assert!(waiters.is_empty());
    }

    #[test]
    fn waiter_identity_exhaustion_poison_is_never_minted() {
        let mut waiters = super::WaiterQueue {
            next_id: u64::MAX - 2,
            ..super::WaiterQueue::default()
        };
        let last = waiters.push_back(super::SendOperation::new(1_u8));
        assert_eq!(last, super::WaiterId(u64::MAX - 1));

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                waiters.push_back(super::SendOperation::new(2_u8));
            }))
            .is_err(),
            "the poison key is never minted"
        );
        assert_eq!(waiters.next_id, u64::MAX);
        assert!(!waiters.entries.contains_key(&super::WaiterId(u64::MAX)));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                waiters.push_back(super::SendOperation::new(3_u8));
            }))
            .is_err(),
            "the exhausted domain stays poisoned"
        );
    }

    /// Stays pending on its first elapsed poll, modelling an atomic
    /// completion transition that won without publishing its payload yet.
    #[derive(Default)]
    struct PendingOnFirstExpiry {
        elapsed_polls: usize,
    }

    impl super::DeadlineOperation for PendingOnFirstExpiry {
        type Output = usize;

        fn poll_deadlined(
            &mut self,
            _context: &mut Context<'_>,
            _budget: crate::deadline::Deadline,
            elapsed: bool,
        ) -> Poll<usize> {
            if !elapsed {
                return Poll::Pending;
            }
            self.elapsed_polls += 1;
            if self.elapsed_polls < 2 {
                Poll::Pending
            } else {
                Poll::Ready(self.elapsed_polls)
            }
        }

        fn short_circuit(&mut self) -> usize {
            0
        }
    }

    #[crate::runtime::test(start_paused = true)]
    async fn an_expired_deadline_future_never_repolls_its_resolved_timer() {
        let width = std::time::Duration::from_secs(1);
        let mut future = Box::pin(super::Deadlined::new(
            PendingOnFirstExpiry::default(),
            width,
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(future.as_mut().poll(&mut context).is_pending());
        crate::runtime::advance(width * 2).await;
        // The timer resolves here and the operation stays pending, so the
        // scaffold must latch the expiry rather than poll the timer again.
        assert!(future.as_mut().poll(&mut context).is_pending());

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(2)));
    }

    #[test]
    fn a_zero_budget_short_circuits_without_polling_the_operation() {
        let mut future = Box::pin(super::Deadlined::new(
            PendingOnFirstExpiry::default(),
            std::time::Duration::ZERO,
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(0)));
        assert_eq!(
            future.operation.elapsed_polls, 0,
            "a zero budget never attempts the operation"
        );
    }
}
