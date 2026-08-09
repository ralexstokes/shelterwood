//! Restart-stable scope snapshots and lifecycle streams.

use std::{
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{
    ChildId, Exit, Incarnation, Intensity, Membership, RestartPolicy, Retention, Strategy,
    exit::StopReason, runtime,
};

/// Number of lifecycle events retained independently for each subscriber.
pub const LIFECYCLE_EVENT_CAPACITY: usize = 128;

// Tokio rounds broadcast capacity up to a power of two. `try_recv` compares
// receiver length with this requested capacity and therefore requires the
// requested and effective capacities to remain equal.
const _: () = assert!(LIFECYCLE_EVENT_CAPACITY.is_power_of_two());

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
    /// Absolute backoff deadline, present exactly while the membership is
    /// restarting; a delay too distant for the clock to represent is
    /// clamped to a far-future instant.
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

/// Conflating receiver for recursive scope snapshots.
#[derive(Clone)]
pub struct SnapshotReceiver {
    inner: runtime::WatchReceiver<SnapshotHubState>,
    seen_generation: u64,
}

impl SnapshotReceiver {
    /// Borrows the newest snapshot without marking it observed.
    #[must_use]
    pub fn borrow_latest(&self) -> Arc<ScopeSnapshot> {
        self.inner.borrow_cloned().snapshot
    }

    /// Waits for and returns a newer snapshot.
    pub async fn changed(&mut self) -> Result<Arc<ScopeSnapshot>, SnapshotClosed> {
        loop {
            let state = self.inner.borrow_cloned();
            if state.generation != self.seen_generation {
                let state = self.inner.borrow_and_update_cloned();
                self.seen_generation = state.generation;
                return Ok(state.snapshot);
            }
            if state.closed {
                return Err(SnapshotClosed);
            }
            if !self.inner.changed_or_closed().await {
                return Err(SnapshotClosed);
            }
        }
    }
}

impl fmt::Debug for SnapshotReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotReceiver")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
pub(crate) struct SnapshotHub {
    sender: OnceLock<runtime::WatchSender<SnapshotHubState>>,
    closed: AtomicBool,
}

#[derive(Clone, Debug)]
struct SnapshotHubState {
    snapshot: Arc<ScopeSnapshot>,
    generation: u64,
    closed: bool,
}

impl SnapshotHub {
    pub(crate) fn subscribe(&self, initial: Arc<ScopeSnapshot>) -> SnapshotReceiver {
        if self.closed.load(Ordering::Acquire) {
            let (sender, inner) = runtime::watch(SnapshotHubState {
                snapshot: initial,
                generation: 0,
                closed: true,
            });
            drop(sender);
            return SnapshotReceiver {
                inner,
                seen_generation: 0,
            };
        }
        let sender = self.sender.get_or_init(|| {
            runtime::watch(SnapshotHubState {
                snapshot: Arc::clone(&initial),
                generation: 0,
                closed: false,
            })
            .0
        });
        // `close` may win between the first atomic check and lazy sender
        // initialization. Reconcile the channel state after initialization so
        // that first subscriber still observes closure; once installed,
        // `close` itself performs this publication and wakeup.
        if self.closed.load(Ordering::Acquire) {
            sender.send_modify(|state| state.closed = true);
        } else if sender.receiver_count() == 0 {
            sender.send_modify(|state| {
                state.snapshot = initial;
                state.generation = state.generation.saturating_add(1);
            });
        }
        let inner = sender.watcher();
        let seen_generation = inner.borrow_cloned().generation;
        SnapshotReceiver {
            inner,
            seen_generation,
        }
    }

    pub(crate) fn publish(&self, snapshot: impl FnOnce() -> Arc<ScopeSnapshot>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let Some(sender) = self.sender.get() else {
            return;
        };
        if sender.receiver_count() > 0 {
            sender.send_modify(|state| {
                state.snapshot = snapshot();
                state.generation = state.generation.saturating_add(1);
            });
        }
    }

    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(sender) = self.sender.get() {
            sender.send_modify(|state| state.closed = true);
        }
    }
}

impl fmt::Debug for SnapshotHub {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotHub")
            .field(
                "receivers",
                &self
                    .sender
                    .get()
                    .map_or(0, |sender| sender.receiver_count()),
            )
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish()
    }
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
    events: runtime::BroadcastReceiver<LifecycleEvent>,
    signal: runtime::WatchReceiver<LifecycleSignal>,
    seen_explicit_lag: u64,
}

impl LifecycleEvents {
    /// Receives the next event or lag marker, returning `None` after closure.
    pub async fn recv(&mut self) -> Option<LifecycleItem> {
        loop {
            match self.try_recv() {
                Ok(item) => return Some(item),
                Err(LifecycleTryRecvError::Closed) => return None,
                Err(LifecycleTryRecvError::Empty) => {}
            }
            let _ = self.signal.changed_or_closed().await;
        }
    }

    /// Attempts to receive without waiting.
    pub fn try_recv(&mut self) -> Result<LifecycleItem, LifecycleTryRecvError> {
        // The marker leads the overflow episode deliberately. A consumer
        // snapshots here, then discards retained events at or below that
        // watermark before applying the newer suffix.
        let signal = self.signal.borrow_cloned();
        let current_explicit_lag = signal.explicit_lag;
        if current_explicit_lag != self.seen_explicit_lag {
            let mut dropped = current_explicit_lag.saturating_sub(self.seen_explicit_lag);
            self.seen_explicit_lag = current_explicit_lag;
            // Do not pull a retained event forward across this marker. It
            // must remain in the bounded ring so a later overflow can still
            // evict the oldest unread event. Tokio guarantees the next read
            // reports lag when `len` exceeds the effective capacity; 128 is
            // already a power of two, so the effective capacity is exact.
            if self.events.len() > LIFECYCLE_EVENT_CAPACITY {
                match self.events.try_receive() {
                    runtime::BroadcastReceive::Lagged(overflow) => {
                        dropped = dropped.saturating_add(overflow);
                    }
                    runtime::BroadcastReceive::Item(_)
                    | runtime::BroadcastReceive::Empty
                    | runtime::BroadcastReceive::Closed => {
                        unreachable!("a lagging broadcast receiver reports its dropped prefix")
                    }
                }
            }
            return Ok(LifecycleItem::Lagged { dropped });
        }
        match self.events.try_receive() {
            runtime::BroadcastReceive::Item(event) => Ok(LifecycleItem::Event(event)),
            runtime::BroadcastReceive::Lagged(dropped) => Ok(LifecycleItem::Lagged { dropped }),
            runtime::BroadcastReceive::Empty if signal.closed => Err(LifecycleTryRecvError::Closed),
            runtime::BroadcastReceive::Empty => Err(LifecycleTryRecvError::Empty),
            runtime::BroadcastReceive::Closed => Err(LifecycleTryRecvError::Closed),
        }
    }
}

impl fmt::Debug for LifecycleEvents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleEvents")
            .field("queued", &self.events.len())
            .finish_non_exhaustive()
    }
}

pub(crate) struct LifecycleHub {
    channels: LifecycleChannels,
    closed: AtomicBool,
}

struct LifecycleChannels {
    events: runtime::BroadcastSender<LifecycleEvent>,
    signal: runtime::WatchSender<LifecycleSignal>,
}

#[derive(Clone, Copy, Debug, Default)]
struct LifecycleSignal {
    explicit_lag: u64,
    closed: bool,
}

impl Default for LifecycleHub {
    fn default() -> Self {
        let (events, _) = runtime::broadcast(LIFECYCLE_EVENT_CAPACITY);
        let (signal, _) = runtime::watch(LifecycleSignal::default());
        Self {
            channels: LifecycleChannels { events, signal },
            closed: AtomicBool::new(false),
        }
    }
}

impl LifecycleHub {
    pub(crate) fn subscribe(&self) -> LifecycleEvents {
        let signal = self.channels.signal.watcher();
        let seen_explicit_lag = signal.borrow_cloned().explicit_lag;
        LifecycleEvents {
            events: self.channels.events.subscribe(),
            signal,
            seen_explicit_lag,
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
        if !self.closed.load(Ordering::Acquire) {
            let _ = self.channels.events.send(event);
            // The watch version is also the no-loss activity notification for
            // async receivers. Its value changes only for explicit lag.
            self.channels.signal.pulse();
        }
    }

    pub(crate) fn publish_lagged(&self, dropped: u64) {
        if !self.closed.load(Ordering::Acquire) {
            self.channels.signal.send_modify(|signal| {
                signal.explicit_lag = signal.explicit_lag.saturating_add(dropped);
            });
        }
    }

    pub(crate) fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.channels
                .signal
                .send_modify(|signal| signal.closed = true);
        }
    }
}

impl fmt::Debug for LifecycleHub {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleHub")
            .field("receivers", &self.channels.events.receiver_count())
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish()
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
    use crate::{
        ScopeState,
        identity::ScopeIdentity,
        observe::{LifecycleEvent, LifecycleEventKind, LifecycleItem},
    };

    use super::{LIFECYCLE_EVENT_CAPACITY, LifecycleHub, SnapshotHub};

    #[test]
    fn snapshot_projection_is_skipped_without_subscribers() {
        let hub = SnapshotHub::default();
        hub.publish(|| panic!("projection must be lazy when no receiver exists"));
    }

    #[test]
    fn a_retained_event_after_explicit_lag_remains_subject_to_later_overflow() {
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&crate::ChildId::from("scope"))
            .expect("membership available");
        let event = |seq| LifecycleEvent {
            scope_path: Vec::new(),
            scope: membership,
            seq,
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Running,
            },
        };
        let hub = LifecycleHub::default();
        let mut events = hub.subscribe();

        hub.publish_lagged(1);
        hub.publish(event(1));
        assert_eq!(events.try_recv(), Ok(LifecycleItem::Lagged { dropped: 1 }));

        for seq in 2..=(LIFECYCLE_EVENT_CAPACITY as u64 + 2) {
            hub.publish(event(seq));
        }
        assert_eq!(
            events.try_recv(),
            Ok(LifecycleItem::Lagged { dropped: 2 }),
            "the prior retained event and the next oldest event are both evicted"
        );
        let LifecycleItem::Event(first_retained) =
            events.try_recv().expect("the retained suffix follows lag")
        else {
            panic!("expected the retained suffix");
        };
        assert_eq!(first_retained.seq, 3);
    }
}
