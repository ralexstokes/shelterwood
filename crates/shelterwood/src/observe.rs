//! Restart-stable scope snapshots and lifecycle streams.

use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use crate::{
    ChildId, Exit, Incarnation, Intensity, Membership, RestartPolicy, Retention, StopReason,
    Strategy,
    driver::{Signal, SignalWatcher},
};

/// Number of lifecycle events retained independently for each subscriber.
pub const LIFECYCLE_EVENT_CAPACITY: usize = 128;

/// Membership-cumulative actor counters and current gauges.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorStats {
    /// Messages dequeued by the actor loop, including offload, timer, and monitor deliveries.
    pub messages_received: u64,
    /// Messages accepted through actor ingress.
    pub messages_accepted: u64,
    /// Accepted messages that replaced a pending same-slot or same-key message.
    pub messages_conflated: u64,
    /// Pending keyed messages evicted by acceptance of a different key.
    pub messages_evicted: u64,
    /// Bytes accepted through ingress, or `None` when no size observer is installed.
    pub message_bytes_accepted: Option<u64>,
    /// Immediate ingress operations rejected before acceptance.
    pub sends_rejected: u64,
    /// Incarnation-owned offloads that have not finished yet.
    pub outstanding_offloads: u64,
    /// Current number of messages pending in the mailbox.
    pub mailbox_depth: u64,
    /// Resolved mailbox capacity.
    pub mailbox_capacity: u64,
}

/// An on-demand actor statistics sample fenced by membership and incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActorStatsSnapshot {
    /// Actor membership whose cumulative counters are sampled.
    pub membership: Membership,
    /// Incarnation bound to the mailbox at sampling time, if any.
    pub observed_incarnation: Option<Incarnation>,
    /// Membership-cumulative counters and current gauges.
    pub stats: ActorStats,
}

/// Typed actor flavor retained by recursive statistics metadata.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ActorKind {
    /// Callback-oriented [`crate::Actor`].
    Actor,
    /// Loop-owning [`crate::RawActor`].
    RawActor,
}

/// One actor row returned by [`crate::ScopeRef::stats_recursive`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecursiveActorStats {
    /// Path from the queried scope to the actor's containing scope.
    pub path: Vec<ChildId>,
    /// Actor id within its containing scope.
    pub id: ChildId,
    /// Typed actor flavor from its declaration.
    pub kind: ActorKind,
    /// Actor membership identity.
    pub membership: Membership,
    /// Incarnation bound to the mailbox at sampling time, if any.
    pub observed_incarnation: Option<Incarnation>,
    /// Membership-cumulative counters and current gauges.
    pub stats: ActorStats,
}

/// One item read from a lifecycle subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LifecycleItem {
    /// One ordered lifecycle edge.
    Event(LifecycleEvent),
    /// Older events were dropped from this subscriber's private queue.
    Lagged {
        /// Exact number of events dropped in this overflow episode.
        dropped: u64,
    },
}

/// One lifecycle edge emitted by a scope membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleEvent {
    /// Child-id path from the subscribed scope to the emitting scope.
    pub scope_path: Vec<ChildId>,
    /// Membership identity of the emitting scope.
    pub scope: Membership,
    /// Emitting scope's membership-owned sequence number.
    pub seq: u64,
    /// State transition carried by this event.
    pub kind: LifecycleEventKind,
}

/// Core lifecycle event inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LifecycleEventKind {
    /// A child membership was admitted.
    Added {
        /// Child label in the emitting scope.
        id: ChildId,
        /// New child membership identity.
        membership: Membership,
    },
    /// A child incarnation was spawned.
    Started {
        /// Child label in the emitting scope.
        id: ChildId,
        /// Child membership identity.
        membership: Membership,
        /// Newly running incarnation.
        incarnation: Incarnation,
    },
    /// A child incarnation passed its readiness gate.
    Ready {
        /// Child label in the emitting scope.
        id: ChildId,
        /// Child membership identity.
        membership: Membership,
        /// Ready incarnation.
        incarnation: Incarnation,
    },
    /// A child incarnation exited.
    Exited {
        /// Child label in the emitting scope.
        id: ChildId,
        /// Child membership identity.
        membership: Membership,
        /// Exited incarnation.
        incarnation: Incarnation,
        /// Classified exit.
        exit: Exit,
    },
    /// A restart charge was scheduled.
    RestartScheduled {
        /// Child label in the emitting scope.
        id: ChildId,
        /// Child membership identity.
        membership: Membership,
        /// Resettable backoff attempt number.
        attempt: u64,
        /// Sampled restart delay.
        delay: Duration,
    },
    /// A child membership became terminal.
    Removed {
        /// Child label in the emitting scope.
        id: ChildId,
        /// Terminal membership identity.
        membership: Membership,
        /// Last incarnation, or `None` when the membership never ran.
        last_incarnation: Option<Incarnation>,
    },
    /// The emitting scope changed its own incarnation state.
    ScopeState {
        /// New scope state.
        state: ScopeState,
    },
}

/// Whether a child membership is active or undergoing planned removal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MembershipStatus {
    /// The membership remains resident normally.
    Active,
    /// A planned removal has begun.
    Removing,
}

/// Current state of one child membership.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChildState {
    /// Membership admitted; its first spawn has not begun.
    Admitted,
    /// An incarnation is starting or waiting for readiness.
    Starting,
    /// An incarnation is ready and running.
    Running,
    /// An incarnation is being stopped.
    Stopping,
    /// No incarnation is live while restart backoff runs.
    Restarting,
    /// The membership is terminal after running or being admitted.
    Stopped {
        /// Terminal exit.
        exit: Exit,
    },
    /// The membership failed before its initial readiness edge.
    StartupAborted {
        /// Terminal startup exit.
        exit: Exit,
    },
}

impl ChildState {
    /// Returns whether this state is terminal.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped { .. } | Self::StartupAborted { .. })
    }
}

/// Current state of a scope membership or incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScopeState {
    /// Membership exists but no incarnation has spawned.
    Unstarted,
    /// The current incarnation is starting its initial children.
    Starting,
    /// Aggregate readiness completed.
    Running,
    /// Root startup failed while the started prefix remains supervised.
    StartupFailed,
    /// The current incarnation is tearing down.
    Draining,
    /// One incarnation stopped.
    Stopped {
        /// Structured stop reason.
        reason: StopReason,
    },
}

impl ScopeState {
    /// Returns whether this is a membership-terminal state.
    ///
    /// A nested scope can transiently publish `Stopped` before its parent
    /// restarts the same membership, so callers that need membership
    /// terminality should prefer stream closure or `wait_stopped()`.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped { .. })
    }
}

/// Static flavor of a scope snapshot.
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScopeKind {
    /// Fixed, readiness-ordered membership.
    Ordered,
    /// Runtime-dynamic membership.
    Dynamic,
}

/// Recursive current-state projection for one child membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildSnapshot {
    /// Child label in its containing scope.
    pub id: ChildId,
    /// Child membership identity.
    pub membership: Membership,
    /// Live incarnation, if one exists.
    pub incarnation: Option<Incarnation>,
    /// Current membership state.
    pub state: ChildState,
    /// Newest prior exit, if an incarnation has exited.
    pub last_exit: Option<Exit>,
    /// Planned-removal status.
    pub membership_status: MembershipStatus,
    /// Cumulative scheduled-restart charges for this membership.
    pub restart_count: u64,
    /// Resolved restart policy.
    pub restart_policy: RestartPolicy,
    /// Resolved terminal-retention policy.
    pub retention: Retention,
    /// Absolute backoff deadline, present exactly while restarting.
    pub restart_at: Option<Instant>,
    /// Recursive state of a scope child when its incarnation is live or terminal.
    pub nested: Option<Arc<ScopeSnapshot>>,
    /// Lifecycle watermark of a scope child, including restart windows.
    pub scope_seq: Option<u64>,
}

/// Arc-shareable recursive current-state projection for one scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopeSnapshot {
    /// Current scope state.
    pub state: ScopeState,
    /// Ordered or dynamic scope flavor.
    pub kind: ScopeKind,
    /// Fate-sharing strategy for ordered scopes; `None` for dynamic scopes.
    pub strategy: Option<Strategy>,
    /// Scope-wide restart budget.
    pub intensity: Intensity,
    /// Cumulative restart charges across children in this incarnation.
    pub total_restarts: u64,
    /// This scope membership's lifecycle watermark.
    pub lifecycle_seq: u64,
    /// Children in declaration or admission order.
    pub children: Arc<[ChildSnapshot]>,
}

impl ScopeSnapshot {
    /// Finds a direct child by its current resident label.
    #[must_use]
    pub fn child(&self, id: impl AsRef<str>) -> Option<&ChildSnapshot> {
        let id = id.as_ref();
        self.children.iter().find(|child| child.id.as_str() == id)
    }

    /// Traverses a child-id path through recursive scope snapshots.
    #[must_use]
    pub fn descendant<I, S>(&self, path: I) -> Option<&ChildSnapshot>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut found: Option<&ChildSnapshot> = None;
        for id in path {
            // A nested snapshot is required only to *advance* past a scope
            // child — a path may end at any child kind, including a scope in
            // a restart window whose nested snapshot is momentarily absent.
            let scope = match found {
                None => self,
                Some(previous) => previous.nested.as_deref()?,
            };
            found = Some(scope.child(id)?);
        }
        found
    }

    /// Looks up the lifecycle watermark for a descendant emitting scope.
    ///
    /// For the scope represented by this snapshot itself, compare the event's
    /// membership with the corresponding scope handle and use [`Self::lifecycle_seq`].
    #[must_use]
    pub fn watermark(&self, membership: Membership) -> Option<u64> {
        self.children.iter().find_map(|child| {
            if child.membership == membership {
                child.scope_seq
            } else {
                child
                    .nested
                    .as_deref()
                    .and_then(|nested| nested.watermark(membership))
            }
        })
    }
}

/// Error returned after a snapshot watch has closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("scope snapshot watch is closed")]
pub struct SnapshotClosed;

#[derive(Debug)]
struct SnapshotSubscriber {
    state: Mutex<SnapshotSubscriberState>,
    signal: Signal,
}

#[derive(Debug)]
struct SnapshotSubscriberState {
    latest: Arc<ScopeSnapshot>,
    version: u64,
    closed: bool,
}

/// Conflating receiver for recursive scope snapshots.
pub struct SnapshotReceiver {
    subscriber: Arc<SnapshotSubscriber>,
    watcher: SignalWatcher,
    seen: u64,
}

impl SnapshotReceiver {
    /// Borrows the newest snapshot without marking it observed.
    #[must_use]
    pub fn borrow_latest(&self) -> Arc<ScopeSnapshot> {
        Arc::clone(
            &self
                .subscriber
                .state
                .lock()
                .expect("snapshot subscriber mutex poisoned")
                .latest,
        )
    }

    /// Waits for and returns a newer snapshot.
    pub async fn changed(&mut self) -> Result<Arc<ScopeSnapshot>, SnapshotClosed> {
        loop {
            {
                let state = self
                    .subscriber
                    .state
                    .lock()
                    .expect("snapshot subscriber mutex poisoned");
                if state.version != self.seen {
                    self.seen = state.version;
                    return Ok(Arc::clone(&state.latest));
                }
                if state.closed {
                    return Err(SnapshotClosed);
                }
            }
            self.watcher.changed().await;
        }
    }
}

impl Clone for SnapshotReceiver {
    fn clone(&self) -> Self {
        Self {
            subscriber: Arc::clone(&self.subscriber),
            watcher: self.subscriber.signal.watcher(),
            seen: self.seen,
        }
    }
}

impl fmt::Debug for SnapshotReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotReceiver")
            .field("seen", &self.seen)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub(crate) struct SnapshotHub {
    subscribers: Mutex<Vec<Weak<SnapshotSubscriber>>>,
}

impl SnapshotHub {
    pub(crate) fn subscribe(&self, initial: Arc<ScopeSnapshot>) -> SnapshotReceiver {
        let subscriber = Arc::new(SnapshotSubscriber {
            state: Mutex::new(SnapshotSubscriberState {
                latest: initial,
                version: 0,
                closed: false,
            }),
            signal: Signal::default(),
        });
        self.subscribers
            .lock()
            .expect("snapshot hub mutex poisoned")
            .push(Arc::downgrade(&subscriber));
        SnapshotReceiver {
            watcher: subscriber.signal.watcher(),
            subscriber,
            seen: 0,
        }
    }

    pub(crate) fn publish(&self, snapshot: impl FnOnce() -> Arc<ScopeSnapshot>) {
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("snapshot hub mutex poisoned");
        subscribers.retain(|subscriber| subscriber.strong_count() > 0);
        if subscribers.is_empty() {
            return;
        }
        let snapshot = snapshot();
        subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            let mut state = subscriber
                .state
                .lock()
                .expect("snapshot subscriber mutex poisoned");
            state.latest = Arc::clone(&snapshot);
            state.version = state.version.saturating_add(1);
            drop(state);
            subscriber.signal.pulse();
            true
        });
    }

    pub(crate) fn close(&self) {
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("snapshot hub mutex poisoned");
        subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            subscriber
                .state
                .lock()
                .expect("snapshot subscriber mutex poisoned")
                .closed = true;
            subscriber.signal.pulse();
            true
        });
    }
}

#[derive(Debug)]
struct LifecycleQueue {
    state: Mutex<LifecycleQueueState>,
    signal: Signal,
}

#[derive(Debug, Default)]
struct LifecycleQueueState {
    events: VecDeque<LifecycleEvent>,
    lagged: Option<u64>,
    closed: bool,
}

/// Error from a non-blocking lifecycle receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum LifecycleTryRecvError {
    /// No item is ready yet.
    #[error("lifecycle subscription is empty")]
    Empty,
    /// The scope membership is terminal and the queue is drained.
    #[error("lifecycle subscription is closed")]
    Closed,
}

/// One membership-owned lifecycle subscription.
pub struct LifecycleEvents {
    queue: Arc<LifecycleQueue>,
    watcher: SignalWatcher,
}

impl LifecycleEvents {
    /// Receives the next event or lag marker, returning `None` after closure.
    pub async fn recv(&mut self) -> Option<LifecycleItem> {
        loop {
            match self.try_recv() {
                Ok(item) => return Some(item),
                Err(LifecycleTryRecvError::Closed) => return None,
                Err(LifecycleTryRecvError::Empty) => self.watcher.changed().await,
            }
        }
    }

    /// Attempts to receive without waiting.
    pub fn try_recv(&mut self) -> Result<LifecycleItem, LifecycleTryRecvError> {
        let mut state = self
            .queue
            .state
            .lock()
            .expect("lifecycle queue mutex poisoned");
        if let Some(dropped) = state.lagged.take() {
            return Ok(LifecycleItem::Lagged { dropped });
        }
        if let Some(event) = state.events.pop_front() {
            return Ok(LifecycleItem::Event(event));
        }
        if state.closed {
            Err(LifecycleTryRecvError::Closed)
        } else {
            Err(LifecycleTryRecvError::Empty)
        }
    }
}

impl fmt::Debug for LifecycleEvents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .queue
            .state
            .lock()
            .expect("lifecycle queue mutex poisoned");
        formatter
            .debug_struct("LifecycleEvents")
            .field("queued", &state.events.len())
            .field("lagged", &state.lagged)
            .field("closed", &state.closed)
            .finish()
    }
}

/// Self-recovering child-observation item.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChildObservation {
    /// Authoritative replacement state, initially or after subscriber loss.
    Reset {
        /// Fresh recursive scope snapshot.
        snapshot: Arc<ScopeSnapshot>,
        /// Lifecycle events lost since the prior authoritative state.
        dropped: u64,
    },
    /// One lifecycle cause paired with consistent-or-newer authoritative state.
    Changed {
        /// Fresh recursive scope snapshot.
        snapshot: Arc<ScopeSnapshot>,
        /// Ordered lifecycle edge that caused this observation.
        cause: LifecycleEvent,
    },
}

/// Library-owned child observer that hides raw lifecycle lag markers.
pub struct ChildObserver {
    scope: crate::ScopeRef,
    events: LifecycleEvents,
    initial: Option<Arc<ScopeSnapshot>>,
}

impl ChildObserver {
    pub(crate) fn new(
        scope: crate::ScopeRef,
        events: LifecycleEvents,
        initial: Arc<ScopeSnapshot>,
    ) -> Self {
        Self {
            scope,
            events,
            initial: Some(initial),
        }
    }

    /// Returns the next reduced observation, or `None` after scope terminality.
    pub async fn next(&mut self) -> Option<ChildObservation> {
        if let Some(snapshot) = self.initial.take() {
            return Some(ChildObservation::Reset {
                snapshot,
                dropped: 0,
            });
        }
        match self.events.recv().await? {
            LifecycleItem::Event(cause) => Some(ChildObservation::Changed {
                snapshot: self.scope.snapshot(),
                cause,
            }),
            LifecycleItem::Lagged { dropped } => Some(ChildObservation::Reset {
                snapshot: self.scope.snapshot(),
                dropped,
            }),
        }
    }
}

impl fmt::Debug for ChildObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildObserver")
            .field("scope", &self.scope)
            .field("initial_pending", &self.initial.is_some())
            .field("events", &self.events)
            .finish()
    }
}

/// One deduplicated cumulative restart-counter sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestartCount {
    /// Authoritative cumulative restart total.
    pub total: u64,
    /// Saturating increase since the prior emitted total.
    pub delta: u64,
    /// Whether this sample followed an initial or lag-recovery reset.
    pub resynced: bool,
}

/// Library-owned restart counter derived from authoritative scope snapshots.
pub struct RestartCounter {
    observer: ChildObserver,
    prior: Option<u64>,
}

impl RestartCounter {
    pub(crate) fn new(observer: ChildObserver) -> Self {
        Self {
            observer,
            prior: None,
        }
    }

    /// Returns the next changed or resynchronized restart total.
    pub async fn next(&mut self) -> Option<RestartCount> {
        loop {
            let observation = self.observer.next().await?;
            let (total, resynced) = match observation {
                ChildObservation::Reset { snapshot, .. } => (snapshot.total_restarts, true),
                ChildObservation::Changed { snapshot, .. } => (snapshot.total_restarts, false),
            };
            let prior = self.prior.unwrap_or(0);
            if !resynced && self.prior == Some(total) {
                continue;
            }
            self.prior = Some(total);
            return Some(RestartCount {
                total,
                delta: total.saturating_sub(prior),
                resynced,
            });
        }
    }
}

impl fmt::Debug for RestartCounter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestartCounter")
            .field("prior", &self.prior)
            .field("observer", &self.observer)
            .finish()
    }
}

#[derive(Debug, Default)]
pub(crate) struct LifecycleHub {
    subscribers: Mutex<Vec<Weak<LifecycleQueue>>>,
}

impl LifecycleHub {
    pub(crate) fn subscribe(&self) -> LifecycleEvents {
        let queue = Arc::new(LifecycleQueue {
            state: Mutex::new(LifecycleQueueState::default()),
            signal: Signal::default(),
        });
        self.subscribers
            .lock()
            .expect("lifecycle hub mutex poisoned")
            .push(Arc::downgrade(&queue));
        LifecycleEvents {
            watcher: queue.signal.watcher(),
            queue,
        }
    }

    pub(crate) fn publish(&self, event: LifecycleEvent) {
        tracing::trace!(
            scope = ?event.scope,
            seq = event.seq,
            path = ?event.scope_path,
            kind = ?event.kind,
            "scope lifecycle event"
        );
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("lifecycle hub mutex poisoned");
        subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            let mut state = subscriber
                .state
                .lock()
                .expect("lifecycle queue mutex poisoned");
            if state.events.len() == LIFECYCLE_EVENT_CAPACITY {
                let dropped = state.events.pop_front();
                debug_assert!(dropped.is_some());
                state.lagged = Some(state.lagged.unwrap_or(0).saturating_add(1));
            }
            state.events.push_back(event.clone());
            drop(state);
            subscriber.signal.pulse();
            true
        });
    }

    pub(crate) fn publish_lagged(&self, dropped: u64) {
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("lifecycle hub mutex poisoned");
        subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            let mut state = subscriber
                .state
                .lock()
                .expect("lifecycle queue mutex poisoned");
            state.lagged = Some(state.lagged.unwrap_or(0).saturating_add(dropped));
            drop(state);
            subscriber.signal.pulse();
            true
        });
    }

    pub(crate) fn close(&self) {
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("lifecycle hub mutex poisoned");
        subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            subscriber
                .state
                .lock()
                .expect("lifecycle queue mutex poisoned")
                .closed = true;
            subscriber.signal.pulse();
            true
        });
    }
}

/// Failure from [`crate::ScopeRef::wait_for_child`].
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum WaitError {
    /// The trailing deadline elapsed before a matching child appeared.
    #[error("child wait timed out")]
    TimedOut,
    /// The observed scope membership terminalized before a match.
    #[error("scope terminated before a child matched")]
    ScopeTerminated {
        /// Final scope state.
        state: ScopeState,
    },
}

#[cfg(test)]
mod tests {
    use super::SnapshotHub;

    #[test]
    fn snapshot_projection_is_skipped_without_subscribers() {
        let hub = SnapshotHub::default();
        hub.publish(|| panic!("projection must be lazy when no receiver exists"));
    }
}
