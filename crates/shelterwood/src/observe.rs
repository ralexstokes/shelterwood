//! Restart-stable scope snapshots and lifecycle streams.

use std::{
    fmt,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use crate::{
    ChildId, Exit, Incarnation, Intensity, Membership, RestartAttempt, RestartCount, RestartPolicy,
    Retention, Strategy, TotalRestarts,
    engine::{MembershipStatus, ScopeState},
    runtime,
};

/// Number of lifecycle events retained independently for each subscriber.
pub const LIFECYCLE_EVENT_CAPACITY: usize = 128;

// Tokio rounds broadcast capacity up to a power of two. `try_recv` compares
// receiver length with this requested capacity and therefore requires the
// requested and effective capacities to remain equal.
const _: () = assert!(LIFECYCLE_EVENT_CAPACITY.is_power_of_two());

/// A sequence in one scope membership's lifecycle event domain.
///
/// Values increase monotonically and remain continuous across restarts of the
/// same membership. A replacement membership starts a distinct sequence
/// domain, identified by its [`Membership`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LifecycleSeq(u64);

impl LifecycleSeq {
    /// Permanent watermark used after the lifecycle sequence space is
    /// exhausted. This value is never assigned to an event.
    pub const EXHAUSTED: Self = Self(u64::MAX);

    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the underlying numeric sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
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
    pub seq: LifecycleSeq,
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
        attempt: RestartAttempt,
        /// Sampled restart delay.
        delay: Duration,
    },
    /// A child membership was pruned from the scope's resident child set.
    Removed {
        /// Child label in the emitting scope.
        id: ChildId,
        /// Pruned membership identity.
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
    pub restart_count: RestartCount,
    /// Resolved restart policy.
    pub restart_policy: RestartPolicy,
    /// Resolved terminal-retention policy.
    pub retention: Retention,
    /// Absolute backoff deadline while restarting, or `None` when the exact
    /// point is too distant for the runtime clock to represent and arm.
    pub restart_at: Option<Instant>,
    /// Recursive state of a scope child when its incarnation is live or terminal.
    pub nested: Option<Arc<ScopeSnapshot>>,
    /// Lifecycle watermark of a scope child, including restart windows.
    pub scope_seq: Option<LifecycleSeq>,
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
    pub total_restarts: TotalRestarts,
    /// This scope membership's lifecycle watermark.
    pub lifecycle_seq: LifecycleSeq,
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
    pub fn watermark(&self, membership: Membership) -> Option<LifecycleSeq> {
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

    /// Borrows the newest snapshot and terminal flag from one retained state.
    #[must_use]
    pub(crate) fn borrow_latest_and_closed(&self) -> (Arc<ScopeSnapshot>, bool) {
        let state = self.inner.borrow_cloned();
        (state.snapshot, state.closed)
    }

    /// Waits for and returns a newer snapshot.
    pub async fn changed(&mut self) -> Result<Arc<ScopeSnapshot>, SnapshotClosed> {
        loop {
            let state = self.inner.borrow_cloned();
            if state.generation.current() != self.seen_generation {
                let state = self.inner.borrow_and_update_cloned();
                self.seen_generation = state.generation.current();
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
}

#[derive(Clone, Debug)]
struct SnapshotHubState {
    snapshot: Arc<ScopeSnapshot>,
    generation: crate::identity::PoisonedCounter,
    closed: bool,
}

impl SnapshotHub {
    pub(crate) fn subscribe(
        &self,
        initial: Arc<ScopeSnapshot>,
        txn: &mut crate::cells::ObservationTxn<'_>,
    ) -> SnapshotReceiver {
        let sender = self.sender.get_or_init(|| {
            runtime::watch(SnapshotHubState {
                snapshot: Arc::clone(&initial),
                generation: crate::identity::PoisonedCounter::new(),
                closed: false,
            })
            .0
        });
        if sender.receiver_count() == 0 {
            // Publication is skipped while receiverless, so the first
            // subscriber after a quiet stretch installs current state itself.
            // A closed hub already holds the authoritative terminal
            // projection, installed by `close`; leave it, and skip the pulse
            // that would then wake nobody about nothing.
            let mut refreshed = false;
            sender.modify_silently(|state| {
                if state.closed {
                    return;
                }
                state.snapshot = initial;
                state
                    .generation
                    .mint()
                    .expect("snapshot generation space exhausted");
                refreshed = true;
            });
            if refreshed {
                txn.pulse(sender);
            }
        }
        let inner = sender.watcher();
        let seen_generation = inner.borrow_cloned().generation.current();
        SnapshotReceiver {
            inner,
            seen_generation,
        }
    }

    /// Publishes while the containing scope's observation gate is held.
    ///
    /// The gate serializes publication, subscription, and closure, so the
    /// retained watch state is the only hub-local source of terminality.
    pub(crate) fn publish(
        &self,
        txn: &mut crate::cells::ObservationTxn<'_>,
        snapshot: impl FnOnce() -> Arc<ScopeSnapshot>,
    ) {
        let Some(sender) = self.sender.get() else {
            return;
        };
        if sender.receiver_count() == 0 {
            return;
        }
        let mut modified = false;
        sender.modify_silently(|state| {
            if state.closed {
                return;
            }
            state.snapshot = snapshot();
            state
                .generation
                .mint()
                .expect("snapshot generation space exhausted");
            modified = true;
        });
        if modified {
            txn.pulse(sender);
        }
    }

    /// Closes while the containing scope's observation gate is held.
    ///
    /// The retained watch state is the hub's only record of closure, so an
    /// unsubscribed hub is materialized here rather than left empty: a later
    /// subscriber must be able to learn that the scope is terminal, and there
    /// is no longer a separate closed flag for it to read.
    pub(crate) fn close(
        &self,
        txn: &mut crate::cells::ObservationTxn<'_>,
        final_snapshot: impl FnOnce() -> Arc<ScopeSnapshot>,
    ) {
        let mut final_snapshot = Some(final_snapshot);
        let mut initialized = false;
        let sender = self.sender.get_or_init(|| {
            initialized = true;
            runtime::watch(SnapshotHubState {
                snapshot: final_snapshot
                    .take()
                    .expect("final snapshot is built exactly once")(),
                generation: crate::identity::PoisonedCounter::new(),
                closed: true,
            })
            .0
        });
        if initialized {
            return;
        }
        let mut modified = false;
        sender.modify_silently(|state| {
            if !state.closed {
                // Install the caller's authoritative terminal projection
                // unconditionally, whether or not receivers are attached.
                // Publication is skipped entirely while receiverless, and one
                // close site (`finish_incarnation_with_terminal`'s stale-epoch
                // branch) deliberately closes without publishing a `Stopped`
                // projection first, so the retained value is not otherwise
                // reliably terminal -- and it is what every later subscriber
                // reads, since a closed hub declines to install their
                // recomputed snapshot.
                //
                // The install is silent: no generation mint, so live receivers
                // see exactly the closure they saw before rather than an extra
                // conflated event. A closed hub delivers no further changes,
                // so this only corrects what `borrow_latest` reports.
                state.snapshot = final_snapshot
                    .take()
                    .expect("final snapshot is built exactly once")(
                );
                state.closed = true;
                modified = true;
            }
        });
        if modified {
            txn.pulse(sender);
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.sender
            .get()
            .is_some_and(|sender| sender.read_with(|state| state.closed))
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
            .field("closed", &self.is_closed())
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
                let overflow = self.events.try_receive();
                debug_assert!(
                    matches!(&overflow, runtime::BroadcastReceive::Lagged(_)),
                    "a lagging broadcast receiver should report its dropped prefix"
                );
                if let runtime::BroadcastReceive::Lagged(overflow) = overflow {
                    dropped = dropped.saturating_add(overflow);
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
}

struct LifecycleChannels {
    events: runtime::BroadcastSender<LifecycleEvent>,
    signal: runtime::WatchSender<LifecycleSignal>,
}

#[derive(Clone, Copy, Debug, Default)]
struct LifecycleSignal {
    // A diagnostic-only loss total. Saturation deliberately preserves the
    // strongest possible "at least this many" report; it never routes an
    // event or identifies storage.
    explicit_lag: u64,
    closed: bool,
}

impl Default for LifecycleHub {
    fn default() -> Self {
        let (events, _) = runtime::broadcast(LIFECYCLE_EVENT_CAPACITY);
        let (signal, _) = runtime::watch(LifecycleSignal::default());
        Self {
            channels: LifecycleChannels { events, signal },
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

    pub(crate) fn is_closed(&self) -> bool {
        self.channels.signal.read_with(|signal| signal.closed)
    }

    /// Publishes while the containing scope's observation gate is held.
    pub(crate) fn publish(
        &self,
        txn: &mut crate::cells::ObservationTxn<'_>,
        event: LifecycleEvent,
    ) {
        tracing::trace!(
            scope = ?event.scope,
            seq = event.seq.get(),
            path = ?event.scope_path,
            kind = ?event.kind,
            "scope lifecycle event"
        );
        let mut published = false;
        self.channels.signal.modify_silently(|signal| {
            if signal.closed {
                return;
            }
            let _ = self.channels.events.send(event);
            published = true;
        });
        if published {
            txn.pulse(&self.channels.signal);
        }
    }

    /// Publishes an explicit lag marker under the observation gate.
    pub(crate) fn publish_lagged(&self, txn: &mut crate::cells::ObservationTxn<'_>, dropped: u64) {
        let mut published = false;
        self.channels.signal.modify_silently(|signal| {
            if signal.closed {
                return;
            }
            signal.explicit_lag = signal.explicit_lag.saturating_add(dropped);
            published = true;
        });
        if published {
            txn.pulse(&self.channels.signal);
        }
    }

    /// Closes while the containing scope's observation gate is held.
    pub(crate) fn close(&self, txn: &mut crate::cells::ObservationTxn<'_>) {
        let mut modified = false;
        self.channels.signal.modify_silently(|signal| {
            if !signal.closed {
                signal.closed = true;
                modified = true;
            }
        });
        if modified {
            txn.pulse(&self.channels.signal);
        }
    }
}

impl fmt::Debug for LifecycleHub {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleHub")
            .field("receivers", &self.channels.events.receiver_count())
            .field(
                "closed",
                &self.channels.signal.read_with(|signal| signal.closed),
            )
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
    use std::sync::Arc;

    use crate::{
        Intensity, ScopeState,
        identity::ScopeIdentity,
        observe::{
            LifecycleEvent, LifecycleEventKind, LifecycleItem, LifecycleTryRecvError, ScopeKind,
            ScopeSnapshot,
        },
    };

    use super::{LIFECYCLE_EVENT_CAPACITY, LifecycleHub, LifecycleSeq, SnapshotHub};

    fn snapshot(state: ScopeState) -> Arc<ScopeSnapshot> {
        Arc::new(ScopeSnapshot {
            state,
            kind: ScopeKind::Dynamic,
            strategy: None,
            intensity: Intensity::default(),
            total_restarts: crate::TotalRestarts::ZERO,
            lifecycle_seq: LifecycleSeq::new(0),
            children: Arc::from([]),
        })
    }

    #[test]
    fn snapshot_projection_is_skipped_without_subscribers() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        hub.publish(&mut txn, || {
            panic!("projection must be lazy when no receiver exists")
        });
    }

    #[crate::runtime::test]
    async fn snapshot_publication_before_close_is_drained() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let mut snapshots = hub.subscribe(snapshot(ScopeState::Unstarted), &mut txn);
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.publish(&mut txn, || snapshot(ScopeState::Running));
        drop(txn);

        assert_eq!(
            snapshots
                .changed()
                .await
                .expect("publication precedes close")
                .state,
            ScopeState::Running
        );

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.publish(&mut txn, || {
            snapshot(ScopeState::Stopped {
                reason: crate::StopReason::Finished,
            })
        });
        hub.close(&mut txn, || {
            snapshot(ScopeState::Stopped {
                reason: crate::StopReason::Finished,
            })
        });
        drop(txn);

        assert!(matches!(
            snapshots
                .changed()
                .await
                .expect("final publication precedes close")
                .state,
            ScopeState::Stopped {
                reason: crate::StopReason::Finished
            }
        ));
        assert!(snapshots.changed().await.is_err());
        let mut txn = crate::cells::ObservationTxn::detached();
        hub.publish(&mut txn, || {
            panic!("publication after close must remain lazy")
        });
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        let mut after_close = hub.subscribe(
            snapshot(ScopeState::Stopped {
                reason: crate::StopReason::Finished,
            }),
            &mut txn,
        );
        drop(txn);
        assert!(matches!(
            after_close.borrow_latest().state,
            ScopeState::Stopped {
                reason: crate::StopReason::Finished
            }
        ));
        assert!(after_close.changed().await.is_err());
    }

    #[crate::runtime::test]
    async fn receiverless_snapshot_close_installs_the_terminal_state() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        hub.close(&mut txn, || {
            snapshot(ScopeState::Stopped {
                reason: crate::StopReason::NeverStarted,
            })
        });
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        let mut receiver = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);
        assert!(matches!(
            receiver.borrow_latest().state,
            ScopeState::Stopped {
                reason: crate::StopReason::NeverStarted
            }
        ));
        assert!(receiver.changed().await.is_err());
    }

    #[crate::runtime::test]
    async fn initialized_receiverless_snapshot_close_refreshes_the_terminal_state() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);
        drop(receiver);

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.close(&mut txn, || {
            snapshot(ScopeState::Stopped {
                reason: crate::StopReason::Finished,
            })
        });
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        let mut receiver = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);
        assert!(matches!(
            receiver.borrow_latest().state,
            ScopeState::Stopped {
                reason: crate::StopReason::Finished
            }
        ));
        assert!(receiver.changed().await.is_err());
    }

    #[crate::runtime::test]
    async fn snapshot_close_installs_the_terminal_state_even_with_live_receivers() {
        // A close site may terminalize without publishing a `Stopped`
        // projection first; the retained value is what every later subscriber
        // reads, so closure must install the authoritative one regardless of
        // who is currently attached.
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let mut live = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.close(&mut txn, || {
            snapshot(ScopeState::Stopped {
                reason: crate::StopReason::Finished,
            })
        });
        drop(txn);

        assert!(live.changed().await.is_err());
        drop(live);

        let mut txn = crate::cells::ObservationTxn::detached();
        let mut later = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);
        assert!(matches!(
            later.borrow_latest().state,
            ScopeState::Stopped {
                reason: crate::StopReason::Finished
            }
        ));
        assert!(later.changed().await.is_err());
    }

    #[crate::runtime::test]
    async fn lifecycle_close_wakes_parked_receivers() {
        let hub = Arc::new(LifecycleHub::default());
        let mut events = hub.subscribe();
        let waiter = crate::runtime::spawn(async move { events.recv().await });
        crate::runtime::yield_now().await;

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.close(&mut txn);
        drop(txn);

        assert_eq!(crate::runtime::join_resuming(waiter).await, None);
    }

    #[test]
    fn lifecycle_publication_after_close_appends_nothing() {
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&crate::ChildId::from("scope"))
            .expect("membership available")
            .membership();
        let hub = LifecycleHub::default();
        let mut events = hub.subscribe();
        let mut txn = crate::cells::ObservationTxn::detached();
        hub.close(&mut txn);

        hub.publish(
            &mut txn,
            LifecycleEvent {
                scope_path: Vec::new(),
                scope: membership,
                seq: LifecycleSeq::new(1),
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::Running,
                },
            },
        );
        drop(txn);

        assert!(matches!(
            events.try_recv(),
            Err(LifecycleTryRecvError::Closed)
        ));
    }

    #[test]
    fn a_retained_event_after_explicit_lag_remains_subject_to_later_overflow() {
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&crate::ChildId::from("scope"))
            .expect("membership available")
            .membership();
        let event = |seq| LifecycleEvent {
            scope_path: Vec::new(),
            scope: membership,
            seq: LifecycleSeq::new(seq),
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Running,
            },
        };
        let hub = LifecycleHub::default();
        let mut events = hub.subscribe();

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.publish_lagged(&mut txn, 1);
        hub.publish(&mut txn, event(1));
        drop(txn);
        assert_eq!(events.try_recv(), Ok(LifecycleItem::Lagged { dropped: 1 }));

        let mut txn = crate::cells::ObservationTxn::detached();
        for seq in 2..=(LIFECYCLE_EVENT_CAPACITY as u64 + 2) {
            hub.publish(&mut txn, event(seq));
        }
        drop(txn);
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
        assert_eq!(first_retained.seq.get(), 3);
    }

    #[test]
    fn lifecycle_close_drains_the_queued_prefix_and_rejects_late_publication() {
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&crate::ChildId::from("scope"))
            .expect("membership available")
            .membership();
        let event = |seq| LifecycleEvent {
            scope_path: Vec::new(),
            scope: membership,
            seq: LifecycleSeq::new(seq),
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Running,
            },
        };
        let hub = LifecycleHub::default();
        let mut events = hub.subscribe();

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.publish_lagged(&mut txn, 2);
        hub.publish(&mut txn, event(1));
        hub.close(&mut txn);
        hub.publish_lagged(&mut txn, 3);
        hub.publish(&mut txn, event(2));
        drop(txn);

        assert_eq!(events.try_recv(), Ok(LifecycleItem::Lagged { dropped: 2 }));
        assert_eq!(events.try_recv(), Ok(LifecycleItem::Event(event(1))));
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
        assert_eq!(
            hub.subscribe().try_recv(),
            Err(LifecycleTryRecvError::Closed),
            "post-close subscribers start at the drained terminal edge"
        );
    }
}
