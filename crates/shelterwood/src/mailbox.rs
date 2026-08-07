//! Membership-owned actor mailboxes and request/reply capabilities.

// `SendError<M>` intentionally owns `M`: guaranteed-unaccepted sends must let
// callers recover arbitrarily sized messages without another allocation.
#![allow(clippy::result_large_err)]

use std::{
    any::Any,
    collections::{VecDeque, hash_map::DefaultHasher},
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    Backoff, ChildId, Incarnation, Mailbox, Membership, PolicyError,
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
    /// An incarnation-pinned ref no longer names the accepting incarnation.
    Superseded {
        /// Incarnation the ref was pinned to.
        pinned: Incarnation,
        /// Newest currently bound incarnation, when one exists.
        newest_observed: Option<Incarnation>,
    },
}

/// Payload carried by a failed ingress operation.
///
/// Ordinary actor refs always return [`SendPayload::Recovered`]. A
/// contramapped ref has already consumed its input while applying the mapping
/// closure and therefore returns [`SendPayload::Projected`].
#[derive(Eq, PartialEq)]
pub enum SendPayload<M> {
    /// The value was not accepted and is recoverable by the caller.
    Recovered(M),
    /// The value was consumed by an eager ingress projection.
    Projected,
}

impl<M> fmt::Debug for SendPayload<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recovered(_) => formatter.write_str("Recovered(<payload>)"),
            Self::Projected => formatter.write_str("Projected"),
        }
    }
}

/// A failed send with its payload disposition and identity evidence.
pub struct SendError<M> {
    /// Target actor id.
    pub actor_id: ChildId,
    /// Incarnation required by the error-kind identity table.
    pub incarnation_observed: Option<Incarnation>,
    /// Disposition of the value submitted at this ingress layer.
    pub payload: SendPayload<M>,
    /// Failure category.
    pub kind: SendErrorKind,
}

impl<M> SendError<M> {
    /// Recovers an unaccepted value from an unmapped ingress layer.
    #[must_use]
    pub fn into_message(self) -> Option<M> {
        match self.payload {
            SendPayload::Recovered(message) => Some(message),
            SendPayload::Projected => None,
        }
    }
}

impl<M> fmt::Debug for SendError<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendError")
            .field("actor_id", &self.actor_id)
            .field("incarnation_observed", &self.incarnation_observed)
            .field("payload", &self.payload)
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
            Self::Superseded { .. } => "pinned actor incarnation was superseded",
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
    /// An incarnation-pinned ref no longer names the accepting incarnation.
    Superseded {
        /// Incarnation the ref was pinned to.
        pinned: Incarnation,
        /// Newest currently bound incarnation, when one exists.
        newest_observed: Option<Incarnation>,
    },
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
            Self::Superseded { .. } => "pinned actor incarnation was superseded",
        })
    }
}

fn call_error_kind_from_send(
    kind: SendErrorKind,
    pinned: Option<Incarnation>,
    observed: Option<Incarnation>,
) -> CallErrorKind {
    match kind {
        SendErrorKind::Superseded {
            pinned,
            newest_observed,
        } => CallErrorKind::Superseded {
            pinned,
            newest_observed,
        },
        SendErrorKind::NotRunning if pinned.is_some() => CallErrorKind::Superseded {
            pinned: pinned.expect("checked pinned call"),
            newest_observed: observed,
        },
        SendErrorKind::NotRunning
        | SendErrorKind::Full
        | SendErrorKind::Terminated
        | SendErrorKind::TimedOut => CallErrorKind::Terminated,
    }
}

/// Failure while awaiting a strictly newer accepting incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum NextIncarnationError {
    /// The actor membership terminalized before a newer incarnation ran.
    Terminated {
        /// Final incarnation, or `None` when the membership never ran.
        last: Option<Incarnation>,
    },
    /// The deadline elapsed before a newer accepting incarnation ran.
    TimedOut,
}

impl fmt::Display for NextIncarnationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Terminated { .. } => "actor membership terminalized",
            Self::TimedOut => "next-incarnation deadline elapsed",
        })
    }
}

impl std::error::Error for NextIncarnationError {}

/// Validated retry data for [`ActorRef::call_idempotent`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RetryPolicy {
    per_attempt: Duration,
    backoff: Backoff,
}

impl RetryPolicy {
    /// Constructs a policy with a non-zero per-attempt slice.
    pub fn new(per_attempt: Duration, backoff: Backoff) -> Result<Self, PolicyError> {
        if per_attempt.is_zero() {
            Err(PolicyError::ZeroDuration)
        } else {
            Ok(Self {
                per_attempt,
                backoff,
            })
        }
    }

    /// Returns the maximum budget for one call attempt.
    #[must_use]
    pub const fn per_attempt(self) -> Duration {
        self.per_attempt
    }

    /// Returns the delay policy applied after retryable failures.
    #[must_use]
    pub const fn backoff(self) -> Backoff {
        self.backoff
    }
}

/// Why one idempotent-call attempt ended.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum AttemptEnd {
    /// The request was withdrawn before acceptance.
    AcceptanceTimedOut,
    /// The accepting incarnation dropped the reply capability.
    ReplyDropped,
    /// The request was accepted but its response slice expired.
    ResponseTimedOut,
    /// The target membership terminalized before acceptance.
    Terminated,
}

/// Identity evidence for one completed idempotent-call attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Attempt {
    /// Accepting or newest-observed incarnation for this attempt.
    pub incarnation: Option<Incarnation>,
    /// Terminal observation for this attempt.
    pub ended: AttemptEnd,
}

/// Terminal category for an idempotent call.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum IdempotentCallErrorKind {
    /// The overall logical-operation budget elapsed.
    BudgetExhausted,
    /// An accepted request's response slice elapsed; reconcile, never resend.
    ResponseTimedOut,
    /// The actor membership terminalized.
    Terminated,
}

/// Terminal idempotent-call failure with complete attempt history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdempotentCallError {
    /// Attempts that actually began, in call order.
    pub attempts: Vec<Attempt>,
    /// Terminal category.
    pub kind: IdempotentCallErrorKind,
}

impl fmt::Display for IdempotentCallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "idempotent call failed after {} attempt(s): {}",
            self.attempts.len(),
            self.kind
        )
    }
}

impl std::error::Error for IdempotentCallError {}

impl fmt::Display for IdempotentCallErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BudgetExhausted => "overall deadline elapsed",
            Self::ResponseTimedOut => "response deadline elapsed after acceptance",
            Self::Terminated => "actor membership terminalized",
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
            if self.deadline.is_zero() {
                self.done = true;
                if let Some(shared) = &self.shared {
                    shared.close_receiver();
                }
                return Poll::Ready(Err(ReplyError::Timeout));
            }
            self.timer = Some(crate::driver::sleep(self.deadline));
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
    LatestByKey(NonZeroUsize),
}

struct Envelope<M> {
    message: M,
    accepted_sequence: u64,
}

trait ErasedMailboxKey: Send {
    fn as_any(&self) -> &dyn Any;
}

struct StoredMailboxKey<K>(K);

impl<K: Send + 'static> ErasedMailboxKey for StoredMailboxKey<K> {
    fn as_any(&self) -> &dyn Any {
        &self.0
    }
}

pub(crate) struct MailboxKey {
    hash: u64,
    value: Box<dyn ErasedMailboxKey>,
}

pub(crate) trait MailboxKeyFn<M>: Send + Sync {
    fn extract(&self, message: &M) -> MailboxKey;
    fn equals(&self, left: &MailboxKey, right: &MailboxKey) -> bool;
}

struct TypedMailboxKeyFn<F, K> {
    key_fn: F,
    marker: std::marker::PhantomData<fn() -> K>,
}

impl<M, F, K> MailboxKeyFn<M> for TypedMailboxKeyFn<F, K>
where
    F: Fn(&M) -> K + Send + Sync,
    K: Eq + Hash + Send + 'static,
{
    fn extract(&self, message: &M) -> MailboxKey {
        let key = (self.key_fn)(message);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        MailboxKey {
            hash: hasher.finish(),
            value: Box::new(StoredMailboxKey(key)),
        }
    }

    fn equals(&self, left: &MailboxKey, right: &MailboxKey) -> bool {
        left.hash == right.hash
            && left.value.as_any().downcast_ref::<K>() == right.value.as_any().downcast_ref::<K>()
    }
}

pub(crate) type MailboxKeyExtractor<M> = Arc<dyn MailboxKeyFn<M> + 'static>;

pub(crate) type MessageSizeObserver<M> = Arc<dyn Fn(&M) -> usize + Send + Sync + 'static>;

pub(crate) fn message_size_observer<M, F>(measure: F) -> MessageSizeObserver<M>
where
    M: Send + 'static,
    F: Fn(&M) -> usize + Send + Sync + 'static,
{
    Arc::new(measure)
}

pub(crate) fn mailbox_key_extractor<M, K, F>(key_fn: F) -> MailboxKeyExtractor<M>
where
    M: Send + 'static,
    K: Eq + Hash + Send + 'static,
    F: Fn(&M) -> K + Send + Sync + 'static,
{
    Arc::new(TypedMailboxKeyFn {
        key_fn,
        marker: std::marker::PhantomData,
    })
}

struct KeyedEnvelope<M> {
    envelope: Envelope<M>,
    key: MailboxKey,
}

enum OperationOutcome<M> {
    Waiting {
        message: Option<M>,
        newest_observed: Option<Incarnation>,
    },
    Accepted(Incarnation),
    Failed {
        payload: Option<SendPayload<M>>,
        observed: Option<Incarnation>,
        kind: SendErrorKind,
    },
    Withdrawn,
}

struct OperationState<M> {
    outcome: OperationOutcome<M>,
    waker: Option<Waker>,
}

struct SendOperation<M> {
    pinned: Option<Incarnation>,
    key: Mutex<Option<MailboxKey>>,
    key_preparation_requested: AtomicBool,
    measured_size: Mutex<Option<u64>>,
    size_preparation_requested: AtomicBool,
    state: Mutex<OperationState<M>>,
}

struct AcceptedMessage<M> {
    message: M,
    key: Option<MailboxKey>,
    measured_size: Option<u64>,
    waker: Option<Waker>,
}

impl<M> SendOperation<M> {
    fn new(message: M, pinned: Option<Incarnation>, key: Option<MailboxKey>) -> Arc<Self> {
        Arc::new(Self {
            pinned,
            key: Mutex::new(key),
            key_preparation_requested: AtomicBool::new(false),
            measured_size: Mutex::new(None),
            size_preparation_requested: AtomicBool::new(false),
            state: Mutex::new(OperationState {
                outcome: OperationOutcome::Waiting {
                    message: Some(message),
                    newest_observed: None,
                },
                waker: None,
            }),
        })
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

    fn is_waiting(&self) -> bool {
        matches!(
            &self
                .state
                .lock()
                .expect("send operation mutex poisoned")
                .outcome,
            OperationOutcome::Waiting { .. }
        )
    }

    fn accept(&self, incarnation: Incarnation) -> Option<AcceptedMessage<M>> {
        let (message, key, measured_size, wake) = {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return None;
            };
            let message = message.take()?;
            let key = self.take_key();
            let measured_size = self.measured_size();
            state.outcome = OperationOutcome::Accepted(incarnation);
            (message, key, measured_size, state.waker.take())
        };
        Some(AcceptedMessage {
            message,
            key,
            measured_size,
            waker: wake,
        })
    }

    fn fail(&self, observed: Option<Incarnation>, kind: SendErrorKind) -> Option<Waker> {
        {
            let mut state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting { message, .. } = &mut state.outcome else {
                return None;
            };
            let payload = message.take().map(SendPayload::Recovered);
            state.outcome = OperationOutcome::Failed {
                payload,
                observed,
                kind,
            };
            state.waker.take()
        }
    }

    fn terminate(&self, final_incarnation: Option<Incarnation>) -> Option<Waker> {
        self.fail(final_incarnation, SendErrorKind::Terminated)
    }

    fn pinned(&self) -> Option<Incarnation> {
        self.pinned
    }

    fn take_key(&self) -> Option<MailboxKey> {
        self.key
            .lock()
            .expect("send operation key mutex poisoned")
            .take()
    }

    fn has_key(&self) -> bool {
        self.key
            .lock()
            .expect("send operation key mutex poisoned")
            .is_some()
    }

    fn request_key_preparation(&self) -> Option<Waker> {
        self.key_preparation_requested
            .store(true, Ordering::Release);
        self.state
            .lock()
            .expect("send operation mutex poisoned")
            .waker
            .take()
    }

    fn prepare_key_if_missing(&self, extractor: &dyn MailboxKeyFn<M>) -> bool {
        if self
            .key
            .lock()
            .expect("send operation key mutex poisoned")
            .is_some()
        {
            return false;
        }
        let key = {
            let state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting {
                message: Some(message),
                ..
            } = &state.outcome
            else {
                return false;
            };
            let extracted = catch_unwind(AssertUnwindSafe(|| extractor.extract(message)));
            drop(state);
            match extracted {
                Ok(key) => key,
                Err(payload) => resume_unwind(payload),
            }
        };
        let mut stored = self.key.lock().expect("send operation key mutex poisoned");
        if stored.is_none() {
            *stored = Some(key);
            self.key_preparation_requested
                .store(false, Ordering::Release);
            true
        } else {
            false
        }
    }

    fn measured_size(&self) -> Option<u64> {
        *self
            .measured_size
            .lock()
            .expect("send operation size mutex poisoned")
    }

    fn has_measured_size(&self) -> bool {
        self.measured_size().is_some()
    }

    fn request_size_preparation(&self) -> Option<Waker> {
        self.size_preparation_requested
            .store(true, Ordering::Release);
        self.state
            .lock()
            .expect("send operation mutex poisoned")
            .waker
            .take()
    }

    fn prepare_size_if_missing(&self, observer: &MessageSizeObserver<M>) -> bool {
        if self.has_measured_size() {
            return false;
        }
        let measured = {
            let state = self.state.lock().expect("send operation mutex poisoned");
            let OperationOutcome::Waiting {
                message: Some(message),
                ..
            } = &state.outcome
            else {
                return false;
            };
            let measured = catch_unwind(AssertUnwindSafe(|| observer(message)));
            drop(state);
            match measured {
                Ok(bytes) => u64::try_from(bytes).unwrap_or(u64::MAX),
                Err(payload) => resume_unwind(payload),
            }
        };
        let mut stored = self
            .measured_size
            .lock()
            .expect("send operation size mutex poisoned");
        if stored.is_none() {
            *stored = Some(measured);
            self.size_preparation_requested
                .store(false, Ordering::Release);
            true
        } else {
            false
        }
    }
}

struct MailboxState<M> {
    kind: Option<MailboxKind>,
    status: BindingStatus,
    last_bound: Option<Incarnation>,
    queue: VecDeque<Envelope<M>>,
    latest: Option<Envelope<M>>,
    keyed: VecDeque<KeyedEnvelope<M>>,
    waiters: VecDeque<Arc<SendOperation<M>>>,
    accepted: u64,
    received: u64,
    conflated: u64,
    evicted: u64,
    message_bytes_accepted: u64,
    sends_rejected: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MailboxStats {
    pub(crate) accepted: u64,
    pub(crate) received: u64,
    pub(crate) conflated: u64,
    pub(crate) evicted: u64,
    pub(crate) message_bytes_accepted: Option<u64>,
    pub(crate) sends_rejected: u64,
    pub(crate) outstanding_offloads: u64,
    pub(crate) observed_incarnation: Option<Incarnation>,
    pub(crate) depth: u64,
    pub(crate) capacity: u64,
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
    fn finish(self) {
        for waker in self.wakers {
            waker.wake();
        }
        drop(self.displaced);
    }
}

/// Type-erased control used by the supervision driver.
pub(crate) trait MailboxControl: fmt::Debug + Send + Sync {
    fn configure(&self, mailbox: Mailbox, keyed_capacity: Option<NonZeroUsize>);
    fn bind(&self, incarnation: Incarnation);
    fn freeze(&self, incarnation: Incarnation);
    fn close(&self, incarnation: Incarnation);
    fn terminate(&self);
    fn stats(&self) -> MailboxStats;
    fn record_rejection(&self);
}

pub(crate) struct MailboxCell<M> {
    actor_id: ChildId,
    member: Weak<MemberCell>,
    state: Mutex<MailboxState<M>>,
    key_extractor: Mutex<Option<MailboxKeyExtractor<M>>>,
    size_observer: Mutex<Option<MessageSizeObserver<M>>>,
    outstanding_offloads: AtomicU64,
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
    pub(crate) fn new(member: &Arc<MemberCell>) -> Arc<Self> {
        Arc::new(Self {
            actor_id: member.id().clone(),
            member: Arc::downgrade(member),
            state: Mutex::new(MailboxState {
                kind: None,
                status: BindingStatus::Unbound,
                last_bound: None,
                queue: VecDeque::new(),
                latest: None,
                keyed: VecDeque::new(),
                waiters: VecDeque::new(),
                accepted: 0,
                received: 0,
                conflated: 0,
                evicted: 0,
                message_bytes_accepted: 0,
                sends_rejected: 0,
            }),
            key_extractor: Mutex::new(None),
            size_observer: Mutex::new(None),
            outstanding_offloads: AtomicU64::new(0),
            changed: Signal::default(),
        })
    }

    pub(crate) fn install_key_extractor(&self, extractor: MailboxKeyExtractor<M>) {
        let previous = self
            .key_extractor
            .lock()
            .expect("mailbox key-extractor mutex poisoned")
            .replace(extractor);
        assert!(
            previous.is_none(),
            "a mailbox can own only one key extractor"
        );
        let waiters = self
            .state
            .lock()
            .expect("mailbox mutex poisoned")
            .waiters
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for waiter in waiters {
            if let Some(waker) = waiter.request_key_preparation() {
                waker.wake();
            }
        }
    }

    pub(crate) fn install_size_observer(&self, observer: MessageSizeObserver<M>) {
        let previous = self
            .size_observer
            .lock()
            .expect("mailbox size-observer mutex poisoned")
            .replace(observer);
        assert!(
            previous.is_none(),
            "a mailbox can own only one message-size observer"
        );
        let waiters = self
            .state
            .lock()
            .expect("mailbox mutex poisoned")
            .waiters
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for waiter in waiters {
            if let Some(waker) = waiter.request_size_preparation() {
                waker.wake();
            }
        }
    }

    fn prepare_key(&self, message: &M) -> Option<MailboxKey> {
        let extractor = self
            .key_extractor
            .lock()
            .expect("mailbox key-extractor mutex poisoned")
            .clone();
        extractor.map(|extractor| extractor.extract(message))
    }

    fn prepare_operation_key(&self, operation: &SendOperation<M>) -> bool {
        if let Some(extractor) = self.cloned_key_extractor() {
            operation.prepare_key_if_missing(&*extractor)
        } else {
            false
        }
    }

    fn cloned_size_observer(&self) -> Option<MessageSizeObserver<M>> {
        self.size_observer
            .lock()
            .expect("mailbox size-observer mutex poisoned")
            .clone()
    }

    fn prepare_operation_size(&self, operation: &SendOperation<M>) -> bool {
        self.cloned_size_observer()
            .is_some_and(|observer| operation.prepare_size_if_missing(&observer))
    }

    fn measure_message(&self, message: &M) -> Option<u64> {
        self.cloned_size_observer().map(|observer| {
            let measured = catch_unwind(AssertUnwindSafe(|| observer(message)));
            match measured {
                Ok(bytes) => u64::try_from(bytes).unwrap_or(u64::MAX),
                Err(payload) => resume_unwind(payload),
            }
        })
    }

    fn promote_bound_waiters(&self) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        let extractor = self.cloned_key_extractor();
        let size_observer = self.cloned_size_observer();
        let promotion = promote_waiters(&mut state, extractor.as_deref(), size_observer.as_ref());
        drop(state);
        self.changed.pulse();
        promotion.finish();
        self.publish_stats();
    }

    fn keys_equal(&self, left: &MailboxKey, right: &MailboxKey) -> bool {
        self.key_extractor
            .lock()
            .expect("mailbox key-extractor mutex poisoned")
            .as_ref()
            .expect("a keyed mailbox has a key extractor")
            .equals(left, right)
    }

    fn cloned_key_extractor(&self) -> Option<MailboxKeyExtractor<M>> {
        self.key_extractor
            .lock()
            .expect("mailbox key-extractor mutex poisoned")
            .clone()
    }

    fn insert_keyed(
        &self,
        state: &mut MailboxState<M>,
        capacity: NonZeroUsize,
        envelope: Envelope<M>,
        key: MailboxKey,
    ) -> Option<Envelope<M>> {
        if let Some(index) = state
            .keyed
            .iter()
            .position(|entry| self.keys_equal(&entry.key, &key))
        {
            state.conflated = state.conflated.saturating_add(1);
            return Some(std::mem::replace(
                &mut state.keyed[index].envelope,
                envelope,
            ));
        }
        let displaced = if state.keyed.len() == capacity.get() {
            state.evicted = state.evicted.saturating_add(1);
            state.keyed.pop_front().map(|entry| entry.envelope)
        } else {
            None
        };
        state.keyed.push_back(KeyedEnvelope { envelope, key });
        displaced
    }

    fn submit(&self, operation: &Arc<SendOperation<M>>) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.status {
            BindingStatus::Terminal(final_incarnation) => {
                state.sends_rejected = state
                    .sends_rejected
                    .saturating_add(u64::from(operation.is_waiting()));
                drop(state);
                if let Some(waker) = operation.terminate(final_incarnation) {
                    waker.wake();
                }
                self.publish_stats();
            }
            BindingStatus::Bound(incarnation) => {
                if operation
                    .pinned()
                    .is_some_and(|pinned| pinned != incarnation)
                {
                    let pinned = operation.pinned().expect("checked pinned incarnation");
                    state.sends_rejected = state
                        .sends_rejected
                        .saturating_add(u64::from(operation.is_waiting()));
                    drop(state);
                    if let Some(waker) = operation.fail(
                        Some(incarnation),
                        SendErrorKind::Superseded {
                            pinned,
                            newest_observed: Some(incarnation),
                        },
                    ) {
                        waker.wake();
                    }
                    self.publish_stats();
                    return;
                }
                operation.observe(incarnation);
                let can_accept = match state.kind {
                    Some(MailboxKind::Queue(capacity)) => {
                        state.waiters.is_empty() && state.queue.len() < capacity.get()
                    }
                    Some(MailboxKind::Latest) => true,
                    Some(MailboxKind::LatestByKey(_)) => true,
                    None => false,
                };
                if can_accept {
                    if self.cloned_size_observer().is_some() && !operation.has_measured_size() {
                        let wake = operation.request_size_preparation();
                        state.waiters.push_back(Arc::clone(operation));
                        drop(state);
                        if let Some(waker) = wake {
                            waker.wake();
                        }
                        return;
                    }
                    let AcceptedMessage {
                        message,
                        key,
                        measured_size,
                        waker: wake,
                    } = operation
                        .accept(incarnation)
                        .expect("a newly submitted operation still owns its message");
                    state.accepted = state.accepted.saturating_add(1);
                    state.message_bytes_accepted = state
                        .message_bytes_accepted
                        .saturating_add(measured_size.unwrap_or(0));
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
                        Some(MailboxKind::LatestByKey(capacity)) => self.insert_keyed(
                            &mut state,
                            capacity,
                            Envelope {
                                message,
                                accepted_sequence,
                            },
                            key.expect("a keyed send has a prepared key"),
                        ),
                        None => unreachable!(),
                    };
                    drop(state);
                    self.changed.pulse();
                    self.publish_stats();
                    if let Some(waker) = wake {
                        waker.wake();
                    }
                    drop(displaced);
                } else {
                    state.waiters.push_back(Arc::clone(operation));
                }
            }
            BindingStatus::Frozen(incarnation) => {
                operation.observe(incarnation);
                if let Some(pinned) = operation.pinned() {
                    let kind = if pinned == incarnation {
                        SendErrorKind::NotRunning
                    } else {
                        SendErrorKind::Superseded {
                            pinned,
                            newest_observed: Some(incarnation),
                        }
                    };
                    state.sends_rejected = state
                        .sends_rejected
                        .saturating_add(u64::from(operation.is_waiting()));
                    drop(state);
                    if let Some(waker) = operation.fail(Some(incarnation), kind) {
                        waker.wake();
                    }
                    self.publish_stats();
                } else {
                    state.waiters.push_back(Arc::clone(operation));
                }
            }
            BindingStatus::Unbound => {
                if let Some(pinned) = operation.pinned() {
                    state.sends_rejected = state
                        .sends_rejected
                        .saturating_add(u64::from(operation.is_waiting()));
                    drop(state);
                    if let Some(waker) = operation.fail(
                        None,
                        SendErrorKind::Superseded {
                            pinned,
                            newest_observed: None,
                        },
                    ) {
                        waker.wake();
                    }
                    self.publish_stats();
                } else {
                    state.waiters.push_back(Arc::clone(operation));
                }
            }
        }
    }

    fn try_send(
        &self,
        message: M,
        pinned: Option<Incarnation>,
    ) -> Result<Incarnation, SendError<M>> {
        let mut key = None;
        let mut measured_size = None;
        let mut state = loop {
            let state = self.state.lock().expect("mailbox mutex poisoned");
            let can_accept = matches!(state.status, BindingStatus::Bound(incarnation)
                if pinned.is_none_or(|pinned| pinned == incarnation))
                && match state.kind {
                    Some(MailboxKind::Queue(capacity)) => {
                        state.waiters.is_empty() && state.queue.len() < capacity.get()
                    }
                    Some(MailboxKind::Latest | MailboxKind::LatestByKey(_)) => true,
                    None => false,
                };
            let needs_key = can_accept
                && key.is_none()
                && matches!(state.kind, Some(MailboxKind::LatestByKey(_)));
            let needs_size =
                can_accept && measured_size.is_none() && self.cloned_size_observer().is_some();
            if needs_key || needs_size {
                drop(state);
                if needs_key {
                    key = self.prepare_key(&message);
                }
                if needs_size {
                    measured_size = self.measure_message(&message);
                }
                continue;
            }
            break state;
        };
        match state.status {
            BindingStatus::Terminal(final_incarnation) => {
                state.sends_rejected = state.sends_rejected.saturating_add(1);
                drop(state);
                self.publish_stats();
                Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: final_incarnation,
                    payload: SendPayload::Recovered(message),
                    kind: SendErrorKind::Terminated,
                })
            }
            BindingStatus::Unbound => {
                state.sends_rejected = state.sends_rejected.saturating_add(1);
                drop(state);
                self.publish_stats();
                Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: None,
                    payload: SendPayload::Recovered(message),
                    kind: pinned.map_or(SendErrorKind::NotRunning, |pinned| {
                        SendErrorKind::Superseded {
                            pinned,
                            newest_observed: None,
                        }
                    }),
                })
            }
            BindingStatus::Frozen(incarnation) => {
                state.sends_rejected = state.sends_rejected.saturating_add(1);
                drop(state);
                self.publish_stats();
                Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: Some(incarnation),
                    payload: SendPayload::Recovered(message),
                    kind: match pinned {
                        Some(pinned) if pinned != incarnation => SendErrorKind::Superseded {
                            pinned,
                            newest_observed: Some(incarnation),
                        },
                        _ => SendErrorKind::NotRunning,
                    },
                })
            }
            BindingStatus::Bound(incarnation)
                if pinned.is_some_and(|pinned| pinned != incarnation) =>
            {
                state.sends_rejected = state.sends_rejected.saturating_add(1);
                let pinned = pinned.expect("checked pinned incarnation");
                drop(state);
                self.publish_stats();
                Err(SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: Some(incarnation),
                    payload: SendPayload::Recovered(message),
                    kind: SendErrorKind::Superseded {
                        pinned,
                        newest_observed: Some(incarnation),
                    },
                })
            }
            BindingStatus::Bound(incarnation) => match state.kind {
                Some(MailboxKind::Queue(capacity))
                    if !state.waiters.is_empty() || state.queue.len() >= capacity.get() =>
                {
                    state.sends_rejected = state.sends_rejected.saturating_add(1);
                    drop(state);
                    self.publish_stats();
                    Err(SendError {
                        actor_id: self.actor_id.clone(),
                        incarnation_observed: Some(incarnation),
                        payload: SendPayload::Recovered(message),
                        kind: SendErrorKind::Full,
                    })
                }
                Some(MailboxKind::Queue(_)) => {
                    state.accepted = state.accepted.saturating_add(1);
                    state.message_bytes_accepted = state
                        .message_bytes_accepted
                        .saturating_add(measured_size.unwrap_or(0));
                    let accepted_sequence = state.accepted;
                    state.queue.push_back(Envelope {
                        message,
                        accepted_sequence,
                    });
                    drop(state);
                    self.changed.pulse();
                    self.publish_stats();
                    Ok(incarnation)
                }
                Some(MailboxKind::Latest) => {
                    state.accepted = state.accepted.saturating_add(1);
                    state.message_bytes_accepted = state
                        .message_bytes_accepted
                        .saturating_add(measured_size.unwrap_or(0));
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
                    self.publish_stats();
                    drop(displaced);
                    Ok(incarnation)
                }
                Some(MailboxKind::LatestByKey(capacity)) => {
                    state.accepted = state.accepted.saturating_add(1);
                    state.message_bytes_accepted = state
                        .message_bytes_accepted
                        .saturating_add(measured_size.unwrap_or(0));
                    let accepted_sequence = state.accepted;
                    let displaced = self.insert_keyed(
                        &mut state,
                        capacity,
                        Envelope {
                            message,
                            accepted_sequence,
                        },
                        key.expect("a keyed send has a prepared key"),
                    );
                    drop(state);
                    self.changed.pulse();
                    self.publish_stats();
                    drop(displaced);
                    Ok(incarnation)
                }
                None => {
                    state.sends_rejected = state.sends_rejected.saturating_add(1);
                    drop(state);
                    self.publish_stats();
                    Err(SendError {
                        actor_id: self.actor_id.clone(),
                        incarnation_observed: None,
                        payload: SendPayload::Recovered(message),
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
            Some(MailboxKind::LatestByKey(_)) => {
                let eligible = accepted_through.map_or(Some(0), |limit| {
                    state
                        .keyed
                        .iter()
                        .position(|item| item.envelope.accepted_sequence <= limit)
                });
                eligible
                    .and_then(|index| state.keyed.remove(index))
                    .map(|item| item.envelope.message)
            }
            None => None,
        };
        let promotion = if message.is_some() && matches!(state.status, BindingStatus::Bound(_)) {
            let extractor = self.cloned_key_extractor();
            let size_observer = self.cloned_size_observer();
            promote_waiters(&mut state, extractor.as_deref(), size_observer.as_ref())
        } else {
            Promotion::default()
        };
        state.received = state.received.saturating_add(u64::from(message.is_some()));
        drop(state);
        if message.is_some() {
            self.changed.pulse();
        }
        promotion.finish();
        if message.is_some() {
            self.publish_stats();
        }
        message
    }

    fn binding_observation(&self) -> BindingObservation {
        match self.state.lock().expect("mailbox mutex poisoned").status {
            BindingStatus::Bound(incarnation) => BindingObservation::Accepting(incarnation),
            BindingStatus::Unbound | BindingStatus::Frozen(_) => BindingObservation::NotAccepting,
            BindingStatus::Terminal(last) => BindingObservation::Terminal(last),
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
            Some(MailboxKind::LatestByKey(capacity)) => (state.keyed.len(), capacity.get()),
            None => (0, 0),
        };
        MailboxStats {
            accepted: state.accepted,
            received: state.received,
            conflated: state.conflated,
            evicted: state.evicted,
            message_bytes_accepted: self
                .size_observer
                .lock()
                .expect("mailbox size-observer mutex poisoned")
                .as_ref()
                .map(|_| state.message_bytes_accepted),
            sends_rejected: state.sends_rejected,
            outstanding_offloads: self.outstanding_offloads.load(Ordering::Acquire),
            observed_incarnation: match state.status {
                BindingStatus::Bound(incarnation) | BindingStatus::Frozen(incarnation) => {
                    Some(incarnation)
                }
                BindingStatus::Unbound | BindingStatus::Terminal(_) => None,
            },
            depth: u64::try_from(depth).unwrap_or(u64::MAX),
            capacity: u64::try_from(capacity).unwrap_or(u64::MAX),
        }
    }

    pub(crate) fn record_local_delivery(&self) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        state.received = state.received.saturating_add(1);
        drop(state);
        self.publish_stats();
    }

    fn record_rejection(&self) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        state.sends_rejected = state.sends_rejected.saturating_add(1);
        drop(state);
        self.publish_stats();
    }

    pub(crate) fn begin_offload(self: &Arc<Self>) -> OutstandingOffload<M> {
        let _ =
            self.outstanding_offloads
                .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_add(1))
                });
        self.publish_stats();
        OutstandingOffload {
            mailbox: Arc::downgrade(self),
        }
    }

    fn finish_offload(&self) {
        let _ =
            self.outstanding_offloads
                .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(1))
                });
        self.publish_stats();
    }

    fn publish_stats(&self) {
        let stats = self.stats();
        tracing::trace!(
            actor.id = %self.actor_id,
            actor.membership = ?self.member.upgrade().map(|member| member.membership()),
            messages_accepted = stats.accepted,
            messages_received = stats.received,
            messages_conflated = stats.conflated,
            messages_evicted = stats.evicted,
            sends_rejected = stats.sends_rejected,
            outstanding_offloads = stats.outstanding_offloads,
            mailbox_depth = stats.depth,
            mailbox_capacity = stats.capacity,
            "actor statistics changed"
        );
        self.emit_metrics(stats);
    }

    #[cfg(not(feature = "metrics"))]
    fn emit_metrics(&self, _stats: MailboxStats) {}

    #[cfg(feature = "metrics")]
    fn emit_metrics(&self, stats: MailboxStats) {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "shelterwood.actor.messages_accepted",
                metrics::Unit::Count,
                "Messages accepted by an actor membership"
            );
            metrics::describe_counter!(
                "shelterwood.actor.messages_received",
                metrics::Unit::Count,
                "Messages received by an actor loop"
            );
            metrics::describe_counter!(
                "shelterwood.actor.messages_conflated",
                metrics::Unit::Count,
                "Messages replaced by conflation"
            );
            metrics::describe_counter!(
                "shelterwood.actor.messages_evicted",
                metrics::Unit::Count,
                "Messages evicted by keyed conflation"
            );
            metrics::describe_counter!(
                "shelterwood.actor.message_bytes_accepted",
                metrics::Unit::Bytes,
                "Message bytes accepted by an actor membership"
            );
            metrics::describe_counter!(
                "shelterwood.actor.sends_rejected",
                metrics::Unit::Count,
                "Actor ingress operations rejected before acceptance"
            );
            metrics::describe_gauge!(
                "shelterwood.actor.outstanding_offloads",
                metrics::Unit::Count,
                "Current outstanding actor offloads"
            );
            metrics::describe_gauge!(
                "shelterwood.actor.mailbox_depth",
                metrics::Unit::Count,
                "Current actor mailbox depth"
            );
            metrics::describe_gauge!(
                "shelterwood.actor.mailbox_capacity",
                metrics::Unit::Count,
                "Resolved actor mailbox capacity"
            );
        });

        let (scope_path, membership) = self.member.upgrade().map_or_else(
            || (String::from("/"), String::from("unbound")),
            |member| {
                (
                    member.scope_path_label(),
                    format!("{:?}", member.membership()),
                )
            },
        );
        let labels = [
            ("scope.path", scope_path),
            ("actor.id", self.actor_id.as_str().to_owned()),
            ("actor.membership", membership),
        ];
        metrics::counter!("shelterwood.actor.messages_accepted", &labels).absolute(stats.accepted);
        metrics::counter!("shelterwood.actor.messages_received", &labels).absolute(stats.received);
        metrics::counter!("shelterwood.actor.messages_conflated", &labels)
            .absolute(stats.conflated);
        metrics::counter!("shelterwood.actor.messages_evicted", &labels).absolute(stats.evicted);
        if let Some(bytes) = stats.message_bytes_accepted {
            metrics::counter!("shelterwood.actor.message_bytes_accepted", &labels).absolute(bytes);
        }
        metrics::counter!("shelterwood.actor.sends_rejected", &labels)
            .absolute(stats.sends_rejected);
        metrics::gauge!("shelterwood.actor.outstanding_offloads", &labels)
            .set(stats.outstanding_offloads as f64);
        metrics::gauge!("shelterwood.actor.mailbox_depth", &labels).set(stats.depth as f64);
        metrics::gauge!("shelterwood.actor.mailbox_capacity", &labels).set(stats.capacity as f64);
    }
}

pub(crate) struct OutstandingOffload<M: Send + 'static> {
    mailbox: Weak<MailboxCell<M>>,
}

impl<M: Send + 'static> Drop for OutstandingOffload<M> {
    fn drop(&mut self) {
        if let Some(mailbox) = self.mailbox.upgrade() {
            mailbox.finish_offload();
        }
    }
}

impl<M> MailboxCell<M> {
    fn withdraw(&self, operation: &Arc<SendOperation<M>>) -> Withdrawal<M> {
        let mut mailbox = self.state.lock().expect("mailbox mutex poisoned");
        let result = {
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
                    Withdrawal::Withdrawn {
                        payload: SendPayload::Recovered(message),
                        observed,
                    }
                }
                OperationOutcome::Accepted(incarnation) => Withdrawal::Accepted(*incarnation),
                OperationOutcome::Failed {
                    payload,
                    observed,
                    kind,
                } => Withdrawal::Failed {
                    payload: payload
                        .take()
                        .expect("a failed operation must retain its payload"),
                    observed: *observed,
                    kind: *kind,
                },
                OperationOutcome::Withdrawn => {
                    panic!("a send operation was withdrawn more than once")
                }
            }
        };
        mailbox
            .waiters
            .retain(|candidate| !Arc::ptr_eq(candidate, operation));
        result
    }
}

impl<M: Send + 'static> MailboxControl for MailboxCell<M> {
    fn configure(&self, mailbox: Mailbox, keyed_capacity: Option<NonZeroUsize>) {
        let kind = keyed_capacity.map_or_else(
            || match mailbox {
                Mailbox::Queue(Some(capacity)) => MailboxKind::Queue(capacity),
                Mailbox::Queue(None) => MailboxKind::Queue(
                    NonZeroUsize::new(crate::policy::DEFAULT_MAILBOX_CAPACITY)
                        .expect("library mailbox capacity is non-zero"),
                ),
                Mailbox::Latest => MailboxKind::Latest,
            },
            MailboxKind::LatestByKey,
        );
        if matches!(kind, MailboxKind::LatestByKey(_)) {
            assert!(
                self.key_extractor
                    .lock()
                    .expect("mailbox key-extractor mutex poisoned")
                    .is_some(),
                "a keyed mailbox requires a typed key extractor"
            );
        }
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        match state.kind {
            Some(existing) => debug_assert_eq!(existing, kind),
            None => state.kind = Some(kind),
        }
        drop(state);
        self.publish_stats();
    }

    fn bind(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if matches!(state.status, BindingStatus::Terminal(_)) {
            return;
        }
        state.status = BindingStatus::Bound(incarnation);
        state.last_bound = Some(incarnation);
        let extractor = self.cloned_key_extractor();
        let size_observer = self.cloned_size_observer();
        let promotion = promote_waiters(&mut state, extractor.as_deref(), size_observer.as_ref());
        drop(state);
        self.changed.pulse();
        promotion.finish();
        self.publish_stats();
    }

    fn freeze(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if state.status == BindingStatus::Bound(incarnation) {
            state.status = BindingStatus::Frozen(incarnation);
            let mut retained = VecDeque::with_capacity(state.waiters.len());
            let mut wakers = Vec::new();
            while let Some(waiter) = state.waiters.pop_front() {
                if waiter.pinned() == Some(incarnation) {
                    state.sends_rejected = state
                        .sends_rejected
                        .saturating_add(u64::from(waiter.is_waiting()));
                    if let Some(waker) = waiter.fail(Some(incarnation), SendErrorKind::NotRunning) {
                        wakers.push(waker);
                    }
                } else {
                    retained.push_back(waiter);
                }
            }
            state.waiters = retained;
            drop(state);
            self.changed.pulse();
            for waker in wakers {
                waker.wake();
            }
            self.publish_stats();
        }
    }

    fn close(&self, incarnation: Incarnation) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if !matches!(
            state.status,
            BindingStatus::Bound(current) | BindingStatus::Frozen(current)
                if current == incarnation
        ) {
            return;
        }
        state.status = BindingStatus::Unbound;
        let queue = std::mem::take(&mut state.queue);
        let latest = state.latest.take();
        let keyed = std::mem::take(&mut state.keyed);
        let mut retained = VecDeque::with_capacity(state.waiters.len());
        let mut wakers = Vec::new();
        while let Some(waiter) = state.waiters.pop_front() {
            if let Some(pinned) = waiter.pinned() {
                state.sends_rejected = state
                    .sends_rejected
                    .saturating_add(u64::from(waiter.is_waiting()));
                if let Some(waker) = waiter.fail(
                    None,
                    SendErrorKind::Superseded {
                        pinned,
                        newest_observed: None,
                    },
                ) {
                    wakers.push(waker);
                }
            } else {
                retained.push_back(waiter);
            }
        }
        state.waiters = retained;
        drop(state);
        self.changed.pulse();
        for waker in wakers {
            waker.wake();
        }
        drop(queue);
        drop(latest);
        drop(keyed);
        self.publish_stats();
    }

    fn terminate(&self) {
        let mut state = self.state.lock().expect("mailbox mutex poisoned");
        if matches!(state.status, BindingStatus::Terminal(_)) {
            return;
        }
        let final_incarnation = state.last_bound;
        state.status = BindingStatus::Terminal(final_incarnation);
        let queue = std::mem::take(&mut state.queue);
        let latest = state.latest.take();
        let keyed = std::mem::take(&mut state.keyed);
        let waiters = std::mem::take(&mut state.waiters);
        state.sends_rejected = state
            .sends_rejected
            .saturating_add(waiters.iter().filter(|waiter| waiter.is_waiting()).count() as u64);
        drop(state);
        let mut wakers = Vec::new();
        for waiter in waiters {
            if let Some(waker) = waiter.terminate(final_incarnation) {
                wakers.push(waker);
            }
        }
        self.changed.pulse();
        for waker in wakers {
            waker.wake();
        }
        drop(queue);
        drop(latest);
        drop(keyed);
        self.publish_stats();
    }

    fn stats(&self) -> MailboxStats {
        self.stats()
    }

    fn record_rejection(&self) {
        self.record_rejection();
    }
}

fn promote_waiters<M>(
    state: &mut MailboxState<M>,
    key_extractor: Option<&dyn MailboxKeyFn<M>>,
    size_observer: Option<&MessageSizeObserver<M>>,
) -> Promotion<M> {
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
        MailboxKind::LatestByKey(_) => usize::MAX,
    };
    let mut accepted = 0usize;
    let candidates = state.waiters.len();
    let mut examined = 0usize;
    while accepted < available && examined < candidates {
        let Some(operation) = state.waiters.pop_front() else {
            break;
        };
        examined += 1;
        if operation
            .pinned()
            .is_some_and(|pinned| pinned != incarnation)
        {
            let pinned = operation.pinned().expect("checked pinned incarnation");
            state.sends_rejected = state
                .sends_rejected
                .saturating_add(u64::from(operation.is_waiting()));
            if let Some(waker) = operation.fail(
                Some(incarnation),
                SendErrorKind::Superseded {
                    pinned,
                    newest_observed: Some(incarnation),
                },
            ) {
                promotion.wakers.push(waker);
            }
            continue;
        }
        operation.observe(incarnation);
        if matches!(kind, MailboxKind::LatestByKey(_)) && !operation.has_key() {
            if let Some(waker) = operation.request_key_preparation() {
                promotion.wakers.push(waker);
            }
            state.waiters.push_back(operation);
            continue;
        }
        if size_observer.is_some() && !operation.has_measured_size() {
            if let Some(waker) = operation.request_size_preparation() {
                promotion.wakers.push(waker);
            }
            state.waiters.push_back(operation);
            continue;
        }
        let Some(AcceptedMessage {
            message,
            key,
            measured_size,
            waker: wake,
        }) = operation.accept(incarnation)
        else {
            continue;
        };
        if let Some(waker) = wake {
            promotion.wakers.push(waker);
        }
        state.accepted = state.accepted.saturating_add(1);
        state.message_bytes_accepted = state
            .message_bytes_accepted
            .saturating_add(measured_size.unwrap_or(0));
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
            MailboxKind::LatestByKey(capacity) => {
                if let Some(displaced) = state.keyed.iter().position(|entry| {
                    let key = key.as_ref().expect("a keyed send has a prepared key");
                    key_extractor
                        .expect("a keyed mailbox has a key extractor")
                        .equals(&entry.key, key)
                }) {
                    state.conflated = state.conflated.saturating_add(1);
                    promotion.displaced.push(std::mem::replace(
                        &mut state.keyed[displaced].envelope,
                        Envelope {
                            message,
                            accepted_sequence,
                        },
                    ));
                } else {
                    if state.keyed.len() == capacity.get()
                        && let Some(displaced) = state.keyed.pop_front()
                    {
                        state.evicted = state.evicted.saturating_add(1);
                        promotion.displaced.push(displaced.envelope);
                    }
                    state.keyed.push_back(KeyedEnvelope {
                        envelope: Envelope {
                            message,
                            accepted_sequence,
                        },
                        key: key.expect("a keyed send has a prepared key"),
                    });
                }
            }
        }
        accepted += 1;
    }
    promotion
}

enum Withdrawal<M> {
    Withdrawn {
        payload: SendPayload<M>,
        observed: Option<Incarnation>,
    },
    Accepted(Incarnation),
    Failed {
        payload: SendPayload<M>,
        observed: Option<Incarnation>,
        kind: SendErrorKind,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BindingObservation {
    Accepting(Incarnation),
    NotAccepting,
    Terminal(Option<Incarnation>),
}

trait SendDriver<M>: Send + Unpin {
    fn poll_send(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<Incarnation, SendError<M>>>;
    fn withdraw(&mut self) -> Withdrawal<M>;
}

trait ActorIngress<M>: Send + Sync {
    fn start_send(&self, message: M, pinned: Option<Incarnation>) -> SendFuture<M>;
    fn try_send(
        &self,
        message: M,
        pinned: Option<Incarnation>,
    ) -> Result<Incarnation, SendError<M>>;
    fn binding_observation(&self) -> BindingObservation;
    fn watcher(&self) -> SignalWatcher;
}

struct MappedIngress<N, M> {
    outer: ActorRef<M>,
    wrap: Arc<dyn Fn(N) -> M + Send + Sync + 'static>,
}

impl<N: Send + 'static, M: Send + 'static> ActorIngress<N> for MappedIngress<N, M> {
    fn start_send(&self, message: N, pinned: Option<Incarnation>) -> SendFuture<N> {
        let message = (self.wrap)(message);
        SendFuture::from_driver(ProjectedSend::<N, M> {
            outer: self.outer.start_send(message, pinned),
            marker: std::marker::PhantomData,
        })
    }

    fn try_send(
        &self,
        message: N,
        pinned: Option<Incarnation>,
    ) -> Result<Incarnation, SendError<N>> {
        let message = (self.wrap)(message);
        self.outer
            .try_send_with_pin(message, pinned)
            .map_err(project_send_error)
    }

    fn binding_observation(&self) -> BindingObservation {
        self.outer.binding_observation()
    }

    fn watcher(&self) -> SignalWatcher {
        self.outer.binding_watcher()
    }
}

fn project_send_error<N, M>(error: SendError<M>) -> SendError<N> {
    SendError {
        actor_id: error.actor_id,
        incarnation_observed: error.incarnation_observed,
        payload: SendPayload::Projected,
        kind: error.kind,
    }
}

/// A cheap membership-addressed actor handle.
pub struct ActorRef<M> {
    member: Arc<MemberCell>,
    ingress: Ingress<M>,
}

enum Ingress<M> {
    Direct(Arc<MailboxCell<M>>),
    Mapped(Arc<dyn ActorIngress<M>>),
}

impl<M> Clone for Ingress<M> {
    fn clone(&self) -> Self {
        match self {
            Self::Direct(mailbox) => Self::Direct(Arc::clone(mailbox)),
            Self::Mapped(ingress) => Self::Mapped(Arc::clone(ingress)),
        }
    }
}

impl<M> ActorRef<M> {
    pub(crate) fn member_cell(&self) -> Arc<MemberCell> {
        Arc::clone(&self.member)
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

    /// Samples membership-cumulative actor counters and current gauges.
    #[must_use]
    pub fn stats(&self) -> crate::ActorStatsSnapshot {
        self.member
            .actor_stats()
            .expect("an actor reference always owns mailbox metadata")
    }

    fn record_rejection(&self) {
        self.member
            .mailbox()
            .expect("an actor reference always owns mailbox metadata")
            .record_rejection();
    }
}

impl<M: Send + 'static> ActorRef<M> {
    pub(crate) fn new(member: Arc<MemberCell>, mailbox: Arc<MailboxCell<M>>) -> Self {
        Self {
            member,
            ingress: Ingress::Direct(mailbox),
        }
    }

    fn start_send(&self, message: M, pinned: Option<Incarnation>) -> SendFuture<M> {
        match &self.ingress {
            Ingress::Direct(mailbox) => SendFuture::from_direct(DirectSend {
                operation: {
                    let key = mailbox.prepare_key(&message);
                    SendOperation::new(message, pinned, key)
                },
                actor_id: self.id().clone(),
                mailbox: Arc::clone(mailbox),
                submitted: false,
                done: false,
            }),
            Ingress::Mapped(ingress) => ingress.start_send(message, pinned),
        }
    }

    fn try_send_with_pin(
        &self,
        message: M,
        pinned: Option<Incarnation>,
    ) -> Result<Incarnation, SendError<M>> {
        match &self.ingress {
            Ingress::Direct(mailbox) => mailbox.try_send(message, pinned),
            Ingress::Mapped(ingress) => ingress.try_send(message, pinned),
        }
    }

    fn binding_observation(&self) -> BindingObservation {
        match &self.ingress {
            Ingress::Direct(mailbox) => mailbox.binding_observation(),
            Ingress::Mapped(ingress) => ingress.binding_observation(),
        }
    }

    fn binding_watcher(&self) -> SignalWatcher {
        match &self.ingress {
            Ingress::Direct(mailbox) => mailbox.watcher(),
            Ingress::Mapped(ingress) => ingress.watcher(),
        }
    }

    /// Sends with backpressure and transparently waits through rebind windows.
    pub fn send(&self, message: M) -> SendFuture<M> {
        self.start_send(message, None)
    }

    /// Attempts immediate acceptance without parking.
    pub fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        self.try_send_with_pin(message, None)
    }

    /// Sends within one acceptance budget, recovering an unaccepted message.
    pub fn send_timeout(&self, message: M, deadline: Duration) -> SendTimeout<M> {
        SendTimeout {
            actor: self.clone(),
            send: Some(self.send(message)),
            deadline,
            timer: None,
            started: false,
            done: false,
        }
    }

    /// Sends a request built around one reply capability and awaits its reply.
    ///
    /// One deadline covers binding, mailbox acceptance, and response. The
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
            pinned: None,
            make_msg: Some(Box::new(make_msg)),
            prepared_message: None,
            prepared_reply: None,
            deadline,
            timer: None,
            send: None,
            reply: None,
            accepted: None,
            started: false,
            done: false,
        }
    }

    /// Waits for an acceptance-open incarnation strictly newer than `after`.
    pub fn next_incarnation(&self, after: Incarnation, deadline: Duration) -> NextIncarnation {
        let actor = self.clone();
        let mut watcher = actor.binding_watcher();
        NextIncarnation {
            inner: Box::pin(async move {
                if deadline.is_zero() {
                    return Err(NextIncarnationError::TimedOut);
                }
                let wait = async {
                    loop {
                        match actor.binding_observation() {
                            BindingObservation::Accepting(current) if current.supersedes(after) => {
                                return Ok(current);
                            }
                            BindingObservation::Terminal(last) => {
                                return Err(NextIncarnationError::Terminated { last });
                            }
                            BindingObservation::Accepting(_) | BindingObservation::NotAccepting => {
                                watcher.changed().await
                            }
                        }
                    }
                };
                match crate::driver::select(wait, crate::driver::sleep(deadline)).await {
                    crate::driver::Selected::First(result) => result,
                    crate::driver::Selected::Second(()) => Err(NextIncarnationError::TimedOut),
                }
            }),
        }
    }

    /// Repeats an explicitly idempotent call under one overall deadline.
    ///
    /// Only guaranteed-unaccepted attempts and reply loss followed by a
    /// strictly newer accepting incarnation retry. A response timeout or
    /// terminal membership returns immediately with attempt history.
    pub fn call_idempotent<T: Send + 'static>(
        &self,
        make_msg: impl Fn(Reply<T>) -> M + Send + 'static,
        policy: RetryPolicy,
        overall_deadline: Duration,
    ) -> IdempotentCallFuture<T> {
        IdempotentCallFuture {
            inner: Box::pin(run_idempotent_call(
                self.clone(),
                make_msg,
                policy,
                overall_deadline,
            )),
        }
    }

    /// Creates a ref that accepts an input type mapped into this actor's
    /// message type on the sender's ingress path.
    pub fn contramap<N: Send + 'static>(
        &self,
        wrap: impl Fn(N) -> M + Send + Sync + 'static,
    ) -> ActorRef<N> {
        let ingress: Arc<dyn ActorIngress<N>> = Arc::new(MappedIngress {
            outer: self.clone(),
            wrap: Arc::new(wrap),
        });
        ActorRef {
            member: Arc::clone(&self.member),
            ingress: Ingress::Mapped(ingress),
        }
    }

    /// Pins this membership-addressed ref to one exact incarnation.
    #[must_use]
    pub fn pinned(&self, incarnation: Incarnation) -> PinnedRef<M> {
        PinnedRef {
            actor: self.clone(),
            incarnation,
        }
    }
}

impl<M> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            member: Arc::clone(&self.member),
            ingress: self.ingress.clone(),
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

impl<M> PartialEq for ActorRef<M> {
    fn eq(&self, other: &Self) -> bool {
        self.membership() == other.membership()
    }
}

impl<M> Eq for ActorRef<M> {}

impl<M> Hash for ActorRef<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.membership().hash(state);
    }
}

/// A cheap actor handle constrained to one exact incarnation.
pub struct PinnedRef<M> {
    actor: ActorRef<M>,
    incarnation: Incarnation,
}

impl<M: Send + 'static> PinnedRef<M> {
    /// Returns the actor's child id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.actor.id()
    }

    /// Returns the actor membership identity.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.actor.membership()
    }

    /// Returns the incarnation this ref is constrained to.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Recovers the membership-addressed actor ref.
    #[must_use]
    pub fn unpinned(&self) -> ActorRef<M> {
        self.actor.clone()
    }

    /// Sends with backpressure only while this incarnation remains current.
    pub fn send(&self, message: M) -> SendFuture<M> {
        self.actor.start_send(message, Some(self.incarnation))
    }

    /// Attempts immediate acceptance by this exact incarnation.
    pub fn try_send(&self, message: M) -> Result<Incarnation, SendError<M>> {
        self.actor
            .try_send_with_pin(message, Some(self.incarnation))
    }

    /// Sends within one acceptance budget without riding through a rebind.
    pub fn send_timeout(&self, message: M, deadline: Duration) -> SendTimeout<M> {
        SendTimeout {
            actor: self.actor.clone(),
            send: Some(self.send(message)),
            deadline,
            timer: None,
            started: false,
            done: false,
        }
    }

    /// Calls this exact incarnation within one acceptance-and-response budget.
    pub fn call<T: Send + 'static>(
        &self,
        make_msg: impl FnOnce(Reply<T>) -> M + Send + 'static,
        deadline: Duration,
    ) -> CallFuture<M, T> {
        CallFuture {
            actor: self.actor.clone(),
            pinned: Some(self.incarnation),
            make_msg: Some(Box::new(make_msg)),
            prepared_message: None,
            prepared_reply: None,
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

impl<M> Clone for PinnedRef<M> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            incarnation: self.incarnation,
        }
    }
}

impl<M> fmt::Debug for PinnedRef<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedRef")
            .field("membership", &self.actor.membership())
            .field("incarnation", &self.incarnation)
            .finish()
    }
}

/// Future returned by [`ActorRef::next_incarnation`].
#[must_use]
pub struct NextIncarnation {
    inner:
        Pin<Box<dyn Future<Output = Result<Incarnation, NextIncarnationError>> + Send + 'static>>,
}

impl fmt::Debug for NextIncarnation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NextIncarnation")
            .finish_non_exhaustive()
    }
}

impl Future for NextIncarnation {
    type Output = Result<Incarnation, NextIncarnationError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
    }
}

/// Future returned by [`ActorRef::call_idempotent`].
#[must_use]
pub struct IdempotentCallFuture<T> {
    inner: Pin<Box<dyn Future<Output = Result<Replied<T>, IdempotentCallError>> + Send + 'static>>,
}

impl<T> fmt::Debug for IdempotentCallFuture<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdempotentCallFuture")
            .finish_non_exhaustive()
    }
}

impl<T> Future for IdempotentCallFuture<T> {
    type Output = Result<Replied<T>, IdempotentCallError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
    }
}

async fn run_idempotent_call<M, T, F>(
    actor: ActorRef<M>,
    make_msg: F,
    policy: RetryPolicy,
    overall_deadline: Duration,
) -> Result<Replied<T>, IdempotentCallError>
where
    M: Send + 'static,
    T: Send + 'static,
    F: Fn(Reply<T>) -> M + Send + 'static,
{
    let started = crate::driver::now();
    let mut attempts = Vec::new();
    let mut retry_number = 0u64;

    loop {
        let remaining = overall_remaining(started, overall_deadline);
        if remaining.is_zero() {
            return Err(IdempotentCallError {
                attempts,
                kind: IdempotentCallErrorKind::BudgetExhausted,
            });
        }
        let (reply, mut receiver) = Reply::channel();
        let message = make_msg(reply);
        let remaining = overall_remaining(started, overall_deadline);
        if remaining.is_zero() {
            return Err(IdempotentCallError {
                attempts,
                kind: IdempotentCallErrorKind::BudgetExhausted,
            });
        }
        let slice = policy.per_attempt.min(remaining);
        let reply = receiver
            .shared
            .take()
            .expect("a fresh reply receiver owns its shared state");
        let result = CallFuture::prepared(actor.clone(), message, reply, slice).await;
        match result {
            Ok(replied) => return Ok(replied),
            Err(error) => match error.kind {
                CallErrorKind::AcceptanceTimedOut => {
                    attempts.push(Attempt {
                        incarnation: error.incarnation_observed,
                        ended: AttemptEnd::AcceptanceTimedOut,
                    });
                }
                CallErrorKind::ReplyDropped => {
                    let accepting = error
                        .incarnation_observed
                        .expect("reply loss carries the accepting incarnation");
                    attempts.push(Attempt {
                        incarnation: Some(accepting),
                        ended: AttemptEnd::ReplyDropped,
                    });
                    let remaining = overall_remaining(started, overall_deadline);
                    if remaining.is_zero() {
                        return Err(IdempotentCallError {
                            attempts,
                            kind: IdempotentCallErrorKind::BudgetExhausted,
                        });
                    }
                    match actor.next_incarnation(accepting, remaining).await {
                        Ok(_) => {}
                        Err(NextIncarnationError::TimedOut) => {
                            return Err(IdempotentCallError {
                                attempts,
                                kind: IdempotentCallErrorKind::BudgetExhausted,
                            });
                        }
                        Err(NextIncarnationError::Terminated { .. }) => {
                            return Err(IdempotentCallError {
                                attempts,
                                kind: IdempotentCallErrorKind::Terminated,
                            });
                        }
                    }
                }
                CallErrorKind::ResponseTimedOut => {
                    attempts.push(Attempt {
                        incarnation: error.incarnation_observed,
                        ended: AttemptEnd::ResponseTimedOut,
                    });
                    return Err(IdempotentCallError {
                        attempts,
                        kind: IdempotentCallErrorKind::ResponseTimedOut,
                    });
                }
                CallErrorKind::Terminated => {
                    attempts.push(Attempt {
                        incarnation: error.incarnation_observed,
                        ended: AttemptEnd::Terminated,
                    });
                    return Err(IdempotentCallError {
                        attempts,
                        kind: IdempotentCallErrorKind::Terminated,
                    });
                }
                CallErrorKind::Superseded { .. } => {
                    unreachable!("membership-addressed idempotent calls are never pinned")
                }
            },
        }

        retry_number = retry_number.saturating_add(1);
        let delay = policy
            .backoff
            .next_delay(retry_number, crate::driver::jitter_sample());
        let remaining = overall_remaining(started, overall_deadline);
        if remaining.is_zero() {
            return Err(IdempotentCallError {
                attempts,
                kind: IdempotentCallErrorKind::BudgetExhausted,
            });
        }
        if !delay.is_zero() {
            crate::driver::sleep(delay.min(remaining)).await;
            if delay >= remaining {
                return Err(IdempotentCallError {
                    attempts,
                    kind: IdempotentCallErrorKind::BudgetExhausted,
                });
            }
        }
    }
}

fn overall_remaining(started: std::time::Instant, overall: Duration) -> Duration {
    overall.saturating_sub(crate::driver::now().saturating_duration_since(started))
}

/// Cancellation-safe future returned by [`ActorRef::send`].
#[must_use]
pub struct SendFuture<M> {
    inner: SendFutureInner<M>,
    done: bool,
}

enum SendFutureInner<M> {
    Direct(DirectSend<M>),
    Mapped(Pin<Box<dyn SendDriver<M>>>),
}

impl<M> SendFuture<M> {
    fn from_direct(driver: DirectSend<M>) -> Self {
        Self {
            inner: SendFutureInner::Direct(driver),
            done: false,
        }
    }

    fn from_driver(driver: impl SendDriver<M> + 'static) -> Self {
        Self {
            inner: SendFutureInner::Mapped(Box::pin(driver)),
            done: false,
        }
    }

    fn withdraw(&mut self) -> Withdrawal<M> {
        self.done = true;
        match &mut self.inner {
            SendFutureInner::Direct(driver) => driver.withdraw_inner(),
            SendFutureInner::Mapped(driver) => driver.as_mut().get_mut().withdraw(),
        }
    }
}

struct DirectSend<M> {
    actor_id: ChildId,
    mailbox: Arc<MailboxCell<M>>,
    operation: Arc<SendOperation<M>>,
    submitted: bool,
    done: bool,
}

impl<M> DirectSend<M> {
    fn withdraw_inner(&mut self) -> Withdrawal<M> {
        let result = self.mailbox.withdraw(&self.operation);
        self.done = true;
        result
    }
}

impl<M> fmt::Debug for SendFuture<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendFuture")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> Future for SendFuture<M> {
    type Output = Result<Incarnation, SendError<M>>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = match &mut self.inner {
            SendFutureInner::Direct(driver) => Pin::new(driver).poll_send(context),
            SendFutureInner::Mapped(driver) => driver.as_mut().poll_send(context),
        };
        if result.is_ready() {
            self.done = true;
        }
        result
    }
}

impl<M: Send + 'static> SendDriver<M> for DirectSend<M> {
    fn poll_send(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<Incarnation, SendError<M>>> {
        let prepared_key = self.mailbox.prepare_operation_key(&self.operation);
        let prepared_size = self
            .operation
            .size_preparation_requested
            .load(Ordering::Acquire)
            && self.mailbox.prepare_operation_size(&self.operation);
        if !self.submitted {
            self.submitted = true;
            self.mailbox.submit(&self.operation);
        } else if prepared_key || prepared_size {
            self.mailbox.promote_bound_waiters();
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
            OperationOutcome::Failed {
                payload,
                observed,
                kind,
            } => {
                let error = SendError {
                    actor_id: self.actor_id.clone(),
                    incarnation_observed: *observed,
                    payload: payload
                        .take()
                        .expect("a failed operation retains its payload until observed"),
                    kind: *kind,
                };
                drop(state);
                self.done = true;
                Poll::Ready(Err(error))
            }
            OperationOutcome::Waiting { .. } => {
                state.waker = Some(context.waker().clone());
                let wake_for_key = self
                    .operation
                    .key_preparation_requested
                    .load(Ordering::Acquire)
                    && !self.operation.has_key();
                let wake_for_size = self
                    .operation
                    .size_preparation_requested
                    .load(Ordering::Acquire)
                    && !self.operation.has_measured_size();
                drop(state);
                if wake_for_key || wake_for_size {
                    context.waker().wake_by_ref();
                }
                Poll::Pending
            }
            OperationOutcome::Withdrawn => panic!("a withdrawn send future was polled"),
        }
    }

    fn withdraw(&mut self) -> Withdrawal<M> {
        self.withdraw_inner()
    }
}

impl<M> Drop for SendFuture<M> {
    fn drop(&mut self) {
        if !self.done {
            let _ = match &mut self.inner {
                SendFutureInner::Direct(driver) => driver.withdraw_inner(),
                SendFutureInner::Mapped(driver) => driver.as_mut().get_mut().withdraw(),
            };
        }
    }
}

struct ProjectedSend<N, M> {
    outer: SendFuture<M>,
    marker: std::marker::PhantomData<fn(N)>,
}

impl<N: Send + 'static, M: Send + 'static> SendDriver<N> for ProjectedSend<N, M> {
    fn poll_send(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<Incarnation, SendError<N>>> {
        match Pin::new(&mut self.outer).poll(context) {
            Poll::Ready(result) => Poll::Ready(result.map_err(project_send_error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn withdraw(&mut self) -> Withdrawal<N> {
        match self.outer.withdraw() {
            Withdrawal::Withdrawn { observed, .. } => Withdrawal::Withdrawn {
                payload: SendPayload::Projected,
                observed,
            },
            Withdrawal::Accepted(incarnation) => Withdrawal::Accepted(incarnation),
            Withdrawal::Failed { observed, kind, .. } => Withdrawal::Failed {
                payload: SendPayload::Projected,
                observed,
                kind,
            },
        }
    }
}

/// Cancellation-safe future returned by [`ActorRef::send_timeout`].
#[must_use]
pub struct SendTimeout<M> {
    actor: ActorRef<M>,
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
            if self.deadline.is_zero() {
                let actor_id = self.actor.id().clone();
                let current = match self.actor.binding_observation() {
                    BindingObservation::Accepting(incarnation) => Some(incarnation),
                    BindingObservation::NotAccepting | BindingObservation::Terminal(_) => None,
                };
                let withdrawal = self
                    .send
                    .as_mut()
                    .expect("pending timed send retains send")
                    .withdraw();
                let Withdrawal::Withdrawn { payload, observed } = withdrawal else {
                    unreachable!("an unpolled send cannot already be accepted")
                };
                self.actor.record_rejection();
                self.done = true;
                return Poll::Ready(Err(SendError {
                    actor_id,
                    incarnation_observed: observed.or(current),
                    payload,
                    kind: SendErrorKind::TimedOut,
                }));
            }
            self.timer = Some(crate::driver::sleep(self.deadline));
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
            let actor_id = self.actor.id().clone();
            let send = self.send.as_mut().expect("pending timed send retains send");
            match send.withdraw() {
                Withdrawal::Withdrawn { payload, observed } => {
                    self.actor.record_rejection();
                    self.done = true;
                    Poll::Ready(Err(SendError {
                        actor_id,
                        incarnation_observed: observed,
                        payload,
                        kind: SendErrorKind::TimedOut,
                    }))
                }
                Withdrawal::Accepted(incarnation) => {
                    self.done = true;
                    Poll::Ready(Ok(incarnation))
                }
                Withdrawal::Failed {
                    payload,
                    observed,
                    kind,
                } => {
                    self.done = true;
                    Poll::Ready(Err(SendError {
                        actor_id,
                        incarnation_observed: observed,
                        payload,
                        kind,
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
    pinned: Option<Incarnation>,
    make_msg: Option<Box<dyn FnOnce(Reply<T>) -> M + Send + 'static>>,
    prepared_message: Option<M>,
    prepared_reply: Option<Arc<ReplyShared<T>>>,
    deadline: Duration,
    timer: Option<crate::driver::DriverSleep>,
    send: Option<SendFuture<M>>,
    reply: Option<Arc<ReplyShared<T>>>,
    accepted: Option<Incarnation>,
    started: bool,
    done: bool,
}

impl<M, T> Unpin for CallFuture<M, T> {}

impl<M: Send + 'static, T: Send + 'static> CallFuture<M, T> {
    fn prepared(
        actor: ActorRef<M>,
        message: M,
        reply: Arc<ReplyShared<T>>,
        deadline: Duration,
    ) -> Self {
        Self {
            actor,
            pinned: None,
            make_msg: None,
            prepared_message: Some(message),
            prepared_reply: Some(reply),
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

impl<M: Send + 'static, T: Send + 'static> Future for CallFuture<M, T> {
    type Output = Result<Replied<T>, CallError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.started {
            self.started = true;
            if self.deadline.is_zero() {
                self.actor.record_rejection();
                self.done = true;
                return Poll::Ready(Err(CallError {
                    actor_id: self.actor.id().clone(),
                    incarnation_observed: match self.actor.binding_observation() {
                        BindingObservation::Accepting(incarnation) => Some(incarnation),
                        BindingObservation::NotAccepting | BindingObservation::Terminal(_) => None,
                    },
                    kind: CallErrorKind::AcceptanceTimedOut,
                }));
            }
            let message = if let Some(message) = self.prepared_message.take() {
                self.reply = self.prepared_reply.take();
                message
            } else {
                let (reply, mut receiver) = Reply::channel();
                let message = self
                    .make_msg
                    .take()
                    .expect("unstarted call retains its message constructor")(
                    reply
                );
                self.reply = receiver.shared.take();
                message
            };
            self.send = Some(self.actor.start_send(message, self.pinned));
            self.timer = Some(crate::driver::sleep(self.deadline));
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
                        kind: call_error_kind_from_send(
                            error.kind,
                            self.pinned,
                            error.incarnation_observed,
                        ),
                    }));
                }
                Poll::Pending => {}
            }
        }

        if let Some(accepted) = self.accepted {
            let reply = self
                .reply
                .as_ref()
                .expect("accepted call retains reply state");
            match reply.poll(context) {
                Poll::Ready(Ok(value)) => {
                    self.done = true;
                    return Poll::Ready(Ok(Replied {
                        value,
                        incarnation: accepted,
                    }));
                }
                Poll::Ready(Err(ReplyError::Dropped)) => {
                    self.done = true;
                    return Poll::Ready(Err(CallError {
                        actor_id: self.actor.id().clone(),
                        incarnation_observed: Some(accepted),
                        kind: CallErrorKind::ReplyDropped,
                    }));
                }
                Poll::Ready(Err(ReplyError::Timeout)) => unreachable!(),
                Poll::Pending => {}
            }
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
            let reply = self
                .reply
                .as_ref()
                .expect("accepted call retains reply state");
            match reply.poll(context) {
                Poll::Ready(Ok(value)) => {
                    self.done = true;
                    Poll::Ready(Ok(Replied {
                        value,
                        incarnation: accepted,
                    }))
                }
                Poll::Ready(Err(ReplyError::Dropped)) => {
                    self.done = true;
                    Poll::Ready(Err(CallError {
                        actor_id: self.actor.id().clone(),
                        incarnation_observed: Some(accepted),
                        kind: CallErrorKind::ReplyDropped,
                    }))
                }
                Poll::Ready(Err(ReplyError::Timeout)) => unreachable!(),
                Poll::Pending => {
                    reply.close_receiver();
                    self.done = true;
                    Poll::Ready(Err(CallError {
                        actor_id: self.actor.id().clone(),
                        incarnation_observed: Some(accepted),
                        kind: CallErrorKind::ResponseTimedOut,
                    }))
                }
            }
        } else {
            let actor_id = self.actor.id().clone();
            let send = self
                .send
                .as_mut()
                .expect("unaccepted call retains send future");
            let result = match send.withdraw() {
                Withdrawal::Withdrawn { observed, .. } => {
                    self.actor.record_rejection();
                    CallError {
                        actor_id,
                        incarnation_observed: observed,
                        kind: CallErrorKind::AcceptanceTimedOut,
                    }
                }
                Withdrawal::Accepted(incarnation) => {
                    if let Some(reply) = &self.reply {
                        match reply.poll(context) {
                            Poll::Ready(Ok(value)) => {
                                self.done = true;
                                return Poll::Ready(Ok(Replied { value, incarnation }));
                            }
                            Poll::Ready(Err(ReplyError::Dropped)) => {
                                self.done = true;
                                return Poll::Ready(Err(CallError {
                                    actor_id,
                                    incarnation_observed: Some(incarnation),
                                    kind: CallErrorKind::ReplyDropped,
                                }));
                            }
                            Poll::Ready(Err(ReplyError::Timeout)) => unreachable!(),
                            Poll::Pending => reply.close_receiver(),
                        }
                    }
                    CallError {
                        actor_id,
                        incarnation_observed: Some(incarnation),
                        kind: CallErrorKind::ResponseTimedOut,
                    }
                }
                Withdrawal::Failed { observed, kind, .. } => CallError {
                    actor_id,
                    incarnation_observed: observed,
                    kind: call_error_kind_from_send(kind, self.pinned, observed),
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
    use std::{num::NonZeroUsize, sync::Arc};

    use super::{MailboxCell, MailboxControl, mailbox_key_extractor};
    use crate::{ChildId, Mailbox, driver::MemberCell, identity::ScopeIdentity};

    #[test]
    fn keyed_eviction_has_a_dedicated_internal_counter() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let membership = identity.mint_membership().expect("membership available");
        let mut incarnations = identity.incarnation_counter(membership);
        let incarnation = ScopeIdentity::mint_incarnation(membership, &mut incarnations)
            .expect("incarnation available");
        let member = MemberCell::new(ChildId::from("keyed-counter"), membership);
        let mailbox = MailboxCell::new(&member);
        mailbox.install_key_extractor(mailbox_key_extractor(|message: &(usize, usize)| message.0));
        mailbox.configure(Mailbox::latest(), NonZeroUsize::new(2));
        mailbox.bind(incarnation);

        mailbox.try_send((1, 1), None).expect("a accepted");
        mailbox.try_send((2, 2), None).expect("b accepted");
        mailbox.try_send((1, 3), None).expect("a replaced");
        mailbox.try_send((3, 4), None).expect("c evicts a");

        let stats = mailbox.stats();
        assert_eq!(stats.accepted, 4);
        assert_eq!(stats.conflated, 1);
        assert_eq!(stats.evicted, 1);
        assert_eq!(stats.depth, 2);
        let _keep_type_inference_honest: Arc<MailboxCell<(usize, usize)>> = mailbox;
    }
}
