//! Membership-owned actor mailboxes and request/reply capabilities.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    ChildId, Incarnation, Mailbox, Membership,
    driver::{MemberCell, Signal, SignalWatcher},
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

enum ReplyOutcome<T> {
    Pending,
    Value(T),
    Dropped,
}

struct ReplyState<T> {
    outcome: ReplyOutcome<T>,
    receiver_alive: bool,
    waker: Option<Waker>,
}

struct ReplyShared<T> {
    state: Mutex<ReplyState<T>>,
}

impl<T> ReplyShared<T> {
    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<T, ReplyError>> {
        let mut state = self.state.lock().expect("reply mutex poisoned");
        match std::mem::replace(&mut state.outcome, ReplyOutcome::Pending) {
            ReplyOutcome::Value(value) => {
                state.receiver_alive = false;
                Poll::Ready(Ok(value))
            }
            ReplyOutcome::Dropped => {
                state.receiver_alive = false;
                Poll::Ready(Err(ReplyError::Dropped))
            }
            ReplyOutcome::Pending => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }

    fn close_receiver(&self) {
        let mut state = self.state.lock().expect("reply mutex poisoned");
        state.receiver_alive = false;
        state.waker = None;
        if matches!(state.outcome, ReplyOutcome::Value(_)) {
            state.outcome = ReplyOutcome::Pending;
        }
    }
}

/// A consuming, infallible reply capability.
pub struct Reply<T> {
    shared: Arc<ReplyShared<T>>,
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
        let shared = Arc::new(ReplyShared {
            state: Mutex::new(ReplyState {
                outcome: ReplyOutcome::Pending,
                receiver_alive: true,
                waker: None,
            }),
        });
        (
            Self {
                shared: Arc::clone(&shared),
                answered: false,
            },
            ReplyReceiver {
                shared: Some(shared),
            },
        )
    }

    /// Consumes the capability and delivers or discards the reply.
    pub fn send(mut self, value: T) {
        self.answered = true;
        let wake = {
            let mut state = self.shared.state.lock().expect("reply mutex poisoned");
            if state.receiver_alive && matches!(state.outcome, ReplyOutcome::Pending) {
                state.outcome = ReplyOutcome::Value(value);
                state.waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        let wake = {
            let mut state = self.shared.state.lock().expect("reply mutex poisoned");
            if matches!(state.outcome, ReplyOutcome::Pending) {
                state.outcome = ReplyOutcome::Dropped;
                state.waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }
}

/// The owned, non-cloneable receive half of [`Reply::channel`].
pub struct ReplyReceiver<T> {
    shared: Option<Arc<ReplyShared<T>>>,
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
            shared: self.shared.take(),
            deadline,
            timer: None,
            started: false,
            done: false,
        }
    }
}

impl<T> Drop for ReplyReceiver<T> {
    fn drop(&mut self) {
        if let Some(shared) = &self.shared {
            shared.close_receiver();
        }
    }
}

/// Future returned by [`ReplyReceiver::recv`].
#[must_use]
pub struct ReplyReceive<T> {
    shared: Option<Arc<ReplyShared<T>>>,
    deadline: Duration,
    timer: Option<crate::driver::DriverSleep>,
    started: bool,
    done: bool,
}

impl<T> fmt::Debug for ReplyReceive<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyReceive")
            .field("started", &self.started)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> Future for ReplyReceive<T> {
    type Output = Result<T, ReplyError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            self.started = true;
            let budget = crate::driver::deadline(self.deadline);
            if self.deadline.is_zero() {
                self.done = true;
                if let Some(shared) = &self.shared {
                    shared.close_receiver();
                }
                return Poll::Ready(Err(ReplyError::Timeout));
            }
            self.timer = Some(crate::driver::sleep_deadline(budget));
        }
        let shared = Arc::clone(
            self.shared
                .as_ref()
                .expect("pending reply receive must retain its shared state"),
        );
        if let Poll::Ready(result) = shared.poll(context) {
            self.done = true;
            return Poll::Ready(result);
        }
        if self
            .timer
            .as_mut()
            .expect("started reply receive must have a timer")
            .as_mut()
            .poll(context)
            .is_ready()
        {
            shared.close_receiver();
            self.done = true;
            return Poll::Ready(Err(ReplyError::Timeout));
        }
        Poll::Pending
    }
}

impl<T> Drop for ReplyReceive<T> {
    fn drop(&mut self) {
        if !self.done
            && let Some(shared) = &self.shared
        {
            shared.close_receiver();
        }
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
    delivered: u64,
    conflated: u64,
    sends_rejected: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WaiterId(u64);

struct WaiterEntry<M> {
    operation: Arc<SendOperation<M>>,
    previous: Option<WaiterId>,
    next: Option<WaiterId>,
}

/// FIFO registrations with constant-time removal by a send operation.
///
/// The mailbox mutex serializes every mutation, so a compact intrusive list
/// can use monotonic local ids without another synchronization layer. The ids
/// are never reused, which also prevents a stale cancellation from unlinking
/// a later operation.
struct WaiterQueue<M> {
    entries: HashMap<WaiterId, WaiterEntry<M>>,
    head: Option<WaiterId>,
    tail: Option<WaiterId>,
    next_id: u64,
    #[cfg(test)]
    direct_removals: usize,
}

impl<M> Default for WaiterQueue<M> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            head: None,
            tail: None,
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
        let id = WaiterId(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("mailbox waiter identity space exhausted");
        let previous = self.tail;
        let replaced = self.entries.insert(
            id,
            WaiterEntry {
                operation,
                previous,
                next: None,
            },
        );
        debug_assert!(replaced.is_none());
        if let Some(previous) = previous {
            self.entries
                .get_mut(&previous)
                .expect("tail registration must be live")
                .next = Some(id);
        } else {
            self.head = Some(id);
        }
        self.tail = Some(id);
        id
    }

    fn park(&mut self, operation: &Arc<SendOperation<M>>) {
        let registration = self.push_back(Arc::clone(operation));
        operation.register(registration);
    }

    fn observe_all(&self, incarnation: Incarnation) {
        for entry in self.entries.values() {
            entry.operation.observe(incarnation);
        }
    }

    fn pop_front(&mut self) -> Option<(WaiterId, Arc<SendOperation<M>>)> {
        let id = self.head?;
        self.remove(id).map(|operation| (id, operation))
    }

    fn remove(&mut self, id: WaiterId) -> Option<Arc<SendOperation<M>>> {
        let entry = self.entries.remove(&id)?;
        #[cfg(test)]
        {
            self.direct_removals = self.direct_removals.saturating_add(1);
        }
        if let Some(previous) = entry.previous {
            self.entries
                .get_mut(&previous)
                .expect("previous registration must be live")
                .next = entry.next;
        } else {
            self.head = entry.next;
        }
        if let Some(next) = entry.next {
            self.entries
                .get_mut(&next)
                .expect("next registration must be live")
                .previous = entry.previous;
        } else {
            self.tail = entry.previous;
        }
        Some(entry.operation)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MailboxStats {
    pub(crate) accepted: u64,
    pub(crate) delivered: u64,
    pub(crate) conflated: u64,
    pub(crate) sends_rejected: u64,
    pub(crate) depth: usize,
    pub(crate) capacity: usize,
}

struct Promotion<M> {
    displaced: Vec<Envelope<M>>,
    wakers: Vec<Waker>,
}

impl<M> Default for Promotion<M> {
    fn default() -> Self {
        Self {
            displaced: Vec::new(),
            wakers: Vec::new(),
        }
    }
}

impl<M> Promotion<M> {
    fn finish(self) {}
}

impl<M> Drop for Promotion<M> {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        let mut first_panic = None;
        for waker in self.wakers.drain(..) {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waker.wake()))
                && first_panic.is_none()
            {
                first_panic = Some(payload);
            }
        }
        if !already_panicking && let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }
}

struct Termination<M> {
    waiters: WaiterQueue<M>,
    final_incarnation: Option<Incarnation>,
}

impl<M> Termination<M> {
    fn finish(
        &mut self,
        retired: &mut Vec<Arc<SendOperation<M>>>,
    ) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        let mut first_panic = None;
        let final_incarnation = self.final_incarnation;
        while let Some((registration, waiter)) = self.waiters.pop_front() {
            waiter.clear_registration(registration);
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                waiter.terminate(final_incarnation);
            })) && first_panic.is_none()
            {
                first_panic = Some(payload);
            }
            // A withdrawn sender may leave this as the final operation owner,
            // so retain it for the same isolated path as unread messages.
            retired.push(waiter);
        }
        first_panic
    }
}

pub(crate) trait MailboxDisposal: Send {}

struct MailboxPayload<M> {
    queue: Option<VecDeque<Envelope<M>>>,
    latest: Option<Envelope<M>>,
    retired: Vec<Arc<SendOperation<M>>>,
}

impl<M> Drop for MailboxPayload<M> {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        let mut first_panic = None;
        if let Some(mut queue) = self.queue.take() {
            while let Some(envelope) = queue.pop_front() {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(envelope)))
                    && first_panic.is_none()
                {
                    first_panic = Some(payload);
                }
            }
        }
        if let Some(latest) = self.latest.take()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(latest)))
            && first_panic.is_none()
        {
            first_panic = Some(payload);
        }
        for waiter in self.retired.drain(..) {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(waiter)))
                && first_panic.is_none()
            {
                first_panic = Some(payload);
            }
        }
        if !already_panicking && let Some(payload) = first_panic {
            resume_unwind(payload);
        }
    }
}

impl<M: Send> MailboxDisposal for MailboxPayload<M> {}

pub(crate) trait MailboxTermination: Send {
    fn finish(self: Box<Self>) -> Option<Box<dyn MailboxDisposal>>;
}

struct MailboxTeardown<M: Send + 'static> {
    changed: Option<Signal>,
    payload: crate::runtime::Isolated<MailboxPayload<M>>,
    termination: Option<Termination<M>>,
}

impl<M: Send + 'static> MailboxTeardown<M> {
    fn finish_framework(&mut self) -> Option<Box<dyn std::any::Any + Send + 'static>> {
        let mut first_panic = None;
        if let Some(changed) = self.changed.take()
            && let Err(payload) = catch_unwind(AssertUnwindSafe(|| changed.pulse()))
        {
            first_panic = Some(payload);
        }
        if let Some(mut termination) = self.termination.take() {
            let panic = termination.finish(&mut self.payload.get_mut().retired);
            if first_panic.is_none() {
                first_panic = panic;
            }
        }
        first_panic
    }
}

impl<M: Send + 'static> MailboxTermination for MailboxTeardown<M> {
    fn finish(mut self: Box<Self>) -> Option<Box<dyn MailboxDisposal>> {
        let panic = self.finish_framework();
        let payload = self
            .payload
            .take()
            .map(|payload| Box::new(payload) as Box<dyn MailboxDisposal>);
        if let Some(panic) = panic {
            if let Some(payload) = payload {
                crate::runtime::dispose_detached(payload);
            }
            resume_unwind(panic);
        }
        payload
    }
}

impl<M: Send + 'static> Drop for MailboxTeardown<M> {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        let panic = self.finish_framework();
        if let Some(payload) = self.payload.take() {
            crate::runtime::dispose_detached(payload);
        }
        if !already_panicking && let Some(panic) = panic {
            resume_unwind(panic);
        }
    }
}

/// Type-erased control used by the supervision driver.
pub(crate) trait MailboxControl: fmt::Debug + Send + Sync {
    fn configure(&self, mailbox: Mailbox);
    fn bind(&self, incarnation: Incarnation);
    fn freeze(&self, incarnation: Incarnation);
    fn close(&self, incarnation: Incarnation) -> Option<Box<dyn MailboxDisposal>>;
    fn prepare_termination(&self) -> Option<Box<dyn MailboxTermination>>;
    fn stats(&self) -> MailboxStats;
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
                delivered: 0,
                conflated: 0,
                sends_rejected: 0,
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
                        Some(MailboxKind::Latest) => {
                            let displaced = state.latest.replace(Envelope {
                                message,
                                accepted_sequence,
                            });
                            state.conflated = state
                                .conflated
                                .saturating_add(u64::from(displaced.is_some()));
                            displaced
                        }
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
            BindingStatus::Terminal(final_incarnation) => {
                state.sends_rejected = state.sends_rejected.saturating_add(1);
                Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: final_incarnation,
                    message,
                    kind: SendErrorKind::Terminated,
                })
            }
            BindingStatus::Unbound => {
                state.sends_rejected = state.sends_rejected.saturating_add(1);
                Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: None,
                    message,
                    kind: SendErrorKind::NotRunning,
                })
            }
            BindingStatus::Frozen(incarnation) => {
                state.sends_rejected = state.sends_rejected.saturating_add(1);
                Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: Some(incarnation),
                    message,
                    kind: SendErrorKind::NotRunning,
                })
            }
            BindingStatus::Bound(incarnation) => match state.kind {
                Some(MailboxKind::Queue(capacity))
                    if !state.waiters.is_empty() || state.queue.len() >= capacity.get() =>
                {
                    state.sends_rejected = state.sends_rejected.saturating_add(1);
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
                    state.conflated = state
                        .conflated
                        .saturating_add(u64::from(displaced.is_some()));
                    drop(state);
                    self.changed.pulse();
                    drop(displaced);
                    Ok(incarnation)
                }
                None => {
                    state.sends_rejected = state.sends_rejected.saturating_add(1);
                    Err(SendError {
                        actor_id: self.actor_id.clone(),
                        incarnation_observed: None,
                        message,
                        kind: SendErrorKind::NotRunning,
                    })
                }
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
        state.delivered = state.delivered.saturating_add(u64::from(message.is_some()));
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

    pub(crate) fn stats(&self) -> MailboxStats {
        let state = self.state.lock().expect("mailbox mutex poisoned");
        let (depth, capacity) = match state.kind {
            Some(MailboxKind::Queue(capacity)) => (state.queue.len(), capacity.get()),
            Some(MailboxKind::Latest) => (usize::from(state.latest.is_some()), 1),
            None => (0, 0),
        };
        MailboxStats {
            accepted: state.accepted,
            delivered: state.delivered,
            conflated: state.conflated,
            sends_rejected: state.sends_rejected,
            depth,
            capacity,
        }
    }
}

impl<M> MailboxCell<M> {
    fn withdraw(&self, operation: &Arc<SendOperation<M>>) -> Withdrawal<M> {
        let mut mailbox = self.state.lock().expect("mailbox mutex poisoned");
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
                    let observed = *newest_observed;
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
        promotion.finish();
    }

    fn freeze(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if state.status == BindingStatus::Bound(incarnation) {
            state.status = BindingStatus::Frozen(incarnation);
            drop(state);
            self.changed.pulse();
        }
    }

    fn close(&self, incarnation: Incarnation) -> Option<Box<dyn MailboxDisposal>> {
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
        }))
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

    fn stats(&self) -> MailboxStats {
        self.stats()
    }
}

fn promote_waiters<M>(state: &mut MailboxState<M>) -> Promotion<M> {
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
                    state.conflated = state.conflated.saturating_add(1);
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
            send: Some(self.send(message)),
            deadline,
            timer: None,
            started: false,
            done: false,
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
            actor: self.clone(),
            make_msg: Some(Box::new(make_msg)),
            deadline,
            timer: None,
            send: None,
            reply: None,
            accepted: None,
            started: false,
            done: false,
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
    submitted: bool,
    done: bool,
}

impl<M: Send + 'static> SendFuture<M> {
    fn new(actor: ActorRef<M>, message: M) -> Self {
        Self {
            actor,
            operation: SendOperation::new(message),
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
            let _ = self.actor.mailbox.withdraw(&self.operation);
        }
    }
}

/// Cancellation-safe future returned by [`ActorRef::send_timeout`].
#[must_use]
pub struct SendTimeout<M> {
    send: Option<SendFuture<M>>,
    deadline: Duration,
    timer: Option<crate::driver::DriverSleep>,
    started: bool,
    done: bool,
}

impl<M> fmt::Debug for SendTimeout<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendTimeout")
            .field("started", &self.started)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> Future for SendTimeout<M> {
    type Output = Result<Incarnation, SendError<M>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            self.started = true;
            let budget = crate::driver::deadline(self.deadline);
            if self.deadline.is_zero() {
                let actor_id = self
                    .send
                    .as_ref()
                    .expect("pending timed send retains send")
                    .actor
                    .id()
                    .clone();
                let current = self
                    .send
                    .as_ref()
                    .expect("pending timed send retains send")
                    .actor
                    .mailbox
                    .current_observation();
                let withdrawal = self
                    .send
                    .as_mut()
                    .expect("pending timed send retains send")
                    .withdraw();
                let Withdrawal::Withdrawn { message, observed } = withdrawal else {
                    unreachable!("an unpolled send cannot already be accepted")
                };
                self.done = true;
                return Poll::Ready(Err(SendError {
                    actor_id,
                    incarnation_observed: observed.or(current),
                    message,
                    kind: SendErrorKind::TimedOut,
                }));
            }
            self.timer = Some(crate::driver::sleep_deadline(budget));
        }
        let send = self.send.as_mut().expect("pending timed send retains send");
        if let Poll::Ready(result) = Pin::new(send).poll(context) {
            self.done = true;
            return Poll::Ready(result);
        }
        if self
            .timer
            .as_mut()
            .expect("started timed send has a timer")
            .as_mut()
            .poll(context)
            .is_ready()
        {
            let send = self.send.as_mut().expect("pending timed send retains send");
            let actor_id = send.actor.id().clone();
            match send.withdraw() {
                Withdrawal::Withdrawn { message, observed } => {
                    self.done = true;
                    Poll::Ready(Err(SendError {
                        actor_id,
                        incarnation_observed: observed,
                        message,
                        kind: SendErrorKind::TimedOut,
                    }))
                }
                Withdrawal::Accepted(incarnation) => {
                    self.done = true;
                    Poll::Ready(Ok(incarnation))
                }
                Withdrawal::Terminated { message, observed } => {
                    self.done = true;
                    Poll::Ready(Err(SendError {
                        actor_id,
                        incarnation_observed: observed,
                        message,
                        kind: SendErrorKind::Terminated,
                    }))
                }
            }
        } else {
            Poll::Pending
        }
    }
}

/// Cancellation-safe future returned by [`ActorRef::call`].
#[must_use]
pub struct CallFuture<M, T> {
    actor: ActorRef<M>,
    make_msg: Option<Box<dyn FnOnce(Reply<T>) -> M + Send + 'static>>,
    deadline: Duration,
    timer: Option<crate::driver::DriverSleep>,
    send: Option<SendFuture<M>>,
    reply: Option<Arc<ReplyShared<T>>>,
    accepted: Option<Incarnation>,
    started: bool,
    done: bool,
}

impl<M, T> fmt::Debug for CallFuture<M, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallFuture")
            .field("started", &self.started)
            .field("accepted", &self.accepted)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<M, T> CallFuture<M, T> {
    fn poll_reply(
        &self,
        context: &mut Context<'_>,
        incarnation: Incarnation,
        deadline_elapsed: bool,
    ) -> Poll<Result<Replied<T>, CallError>> {
        let reply = self
            .reply
            .as_ref()
            .expect("accepted call retains reply state");
        match reply.poll(context) {
            Poll::Ready(Ok(value)) => Poll::Ready(Ok(Replied { value, incarnation })),
            Poll::Ready(Err(ReplyError::Dropped)) => Poll::Ready(Err(CallError {
                actor_id: self.actor.id().clone(),
                incarnation_observed: Some(incarnation),
                kind: CallErrorKind::ReplyDropped,
            })),
            Poll::Ready(Err(ReplyError::Timeout)) => unreachable!(),
            Poll::Pending if deadline_elapsed => {
                reply.close_receiver();
                Poll::Ready(Err(CallError {
                    actor_id: self.actor.id().clone(),
                    incarnation_observed: Some(incarnation),
                    kind: CallErrorKind::ResponseTimedOut,
                }))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<M: Send + 'static, T: Send + 'static> Future for CallFuture<M, T> {
    type Output = Result<Replied<T>, CallError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            self.started = true;
            // Capture the one overall budget before invoking user code. A
            // slow message constructor consumes acceptance/response time.
            let budget = crate::driver::deadline(self.deadline);
            if self.deadline.is_zero() {
                self.done = true;
                return Poll::Ready(Err(CallError {
                    actor_id: self.actor.id().clone(),
                    incarnation_observed: self.actor.mailbox.current_observation(),
                    kind: CallErrorKind::AcceptanceTimedOut,
                }));
            }
            let (reply, mut receiver) = Reply::channel();
            let message =
                self.make_msg
                    .take()
                    .expect("unstarted call retains its message constructor")(reply);
            // The normal polling order lets an acceptance already available
            // at the exact deadline win. Construction is different: no send
            // existed before it completed, so do not start one after the
            // captured budget is strictly in the past.
            if budget.is_overdue(crate::driver::now()) {
                self.done = true;
                return Poll::Ready(Err(CallError {
                    actor_id: self.actor.id().clone(),
                    incarnation_observed: self.actor.mailbox.current_observation(),
                    kind: CallErrorKind::AcceptanceTimedOut,
                }));
            }
            self.reply = receiver.shared.take();
            self.send = Some(self.actor.send(message));
            self.timer = Some(crate::driver::sleep_deadline(budget));
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
                    if let Some(reply) = &self.reply {
                        reply.close_receiver();
                    }
                    self.done = true;
                    return Poll::Ready(Err(CallError {
                        actor_id: error.actor_id,
                        incarnation_observed: error.incarnation_observed,
                        kind: CallErrorKind::Terminated,
                    }));
                }
                Poll::Pending => {}
            }
        }

        if let Some(accepted) = self.accepted
            && let Poll::Ready(result) = self.poll_reply(context, accepted, false)
        {
            self.done = true;
            return Poll::Ready(result);
        }

        if self
            .timer
            .as_mut()
            .expect("started call has a timer")
            .as_mut()
            .poll(context)
            .is_pending()
        {
            return Poll::Pending;
        }

        if let Some(accepted) = self.accepted {
            let result = self.poll_reply(context, accepted, true);
            self.done = true;
            result
        } else {
            let actor_id = self.actor.id().clone();
            let send = self
                .send
                .as_mut()
                .expect("unaccepted call retains send future");
            let result = match send.withdraw() {
                Withdrawal::Withdrawn { observed, .. } => CallError {
                    actor_id,
                    incarnation_observed: observed,
                    kind: CallErrorKind::AcceptanceTimedOut,
                },
                Withdrawal::Accepted(incarnation) => {
                    let result = self.poll_reply(context, incarnation, true);
                    self.done = true;
                    return result;
                }
                Withdrawal::Terminated { observed, .. } => CallError {
                    actor_id,
                    incarnation_observed: observed,
                    kind: CallErrorKind::Terminated,
                },
            };
            if let Some(reply) = &self.reply {
                reply.close_receiver();
            }
            self.done = true;
            Poll::Ready(Err(result))
        }
    }
}

impl<M, T> Drop for CallFuture<M, T> {
    fn drop(&mut self) {
        if !self.done
            && let Some(reply) = &self.reply
        {
            reply.close_receiver();
        }
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
        self.mailbox.stats().accepted
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

    use crate::{ChildId, Mailbox, SendErrorKind, driver::MemberCell, identity::ScopeIdentity};

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
        let mut identity = ScopeIdentity::new().expect("scope identity available");
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
            let mut identity = ScopeIdentity::new().expect("scope identity available");
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
}
