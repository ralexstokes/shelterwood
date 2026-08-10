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
    ChildId, Exit, Incarnation, Intensity, Membership, RestartAttempt, RestartCount, RestartPolicy,
    Retention, Strategy, TotalRestarts, exit::StopReason, runtime,
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

    #[cfg(test)]
    pub(crate) fn publish(&self, snapshot: impl FnOnce() -> Arc<ScopeSnapshot>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let Some(sender) = self.sender.get() else {
            return;
        };
        if sender.receiver_count() > 0 {
            sender.send_if_modified(|state| {
                // `close` and publication serialize on the retained watch
                // state. The atomic is the fast path; this check prevents a
                // publisher that passed it just before closure from mutating
                // the terminal value afterward.
                if state.closed {
                    return false;
                }
                state.snapshot = snapshot();
                state.generation = state.generation.saturating_add(1);
                true
            });
        }
    }

    pub(crate) fn publish_deferred(
        &self,
        wakes: &mut crate::cells::ObservationWakes,
        snapshot: impl FnOnce() -> Arc<ScopeSnapshot>,
    ) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
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
            state.generation = state.generation.saturating_add(1);
            modified = true;
        });
        if modified {
            wakes.pulse(sender);
        }
    }

    #[cfg(test)]
    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(sender) = self.sender.get() {
            sender.send_modify(|state| state.closed = true);
        }
    }

    pub(crate) fn close_deferred(&self, wakes: &mut crate::cells::ObservationWakes) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(sender) = self.sender.get() {
            sender.modify_silently(|state| state.closed = true);
            wakes.pulse(sender);
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

    #[cfg(test)]
    pub(crate) fn publish(&self, event: LifecycleEvent) {
        tracing::trace!(
            scope = ?event.scope,
            seq = event.seq.get(),
            path = ?event.scope_path,
            kind = ?event.kind,
            "scope lifecycle event"
        );
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.publish_past_fast_path(event);
    }

    pub(crate) fn publish_deferred(
        &self,
        wakes: &mut crate::cells::ObservationWakes,
        event: LifecycleEvent,
    ) {
        tracing::trace!(
            scope = ?event.scope,
            seq = event.seq.get(),
            path = ?event.scope_path,
            kind = ?event.kind,
            "scope lifecycle event"
        );
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let mut published = false;
        self.channels.signal.modify_silently(|signal| {
            if signal.closed {
                return;
            }
            let _ = self.channels.events.send(event);
            published = true;
        });
        if published {
            wakes.pulse(&self.channels.signal);
        }
    }

    /// The enqueue half of [`publish`](Self::publish), after the atomic
    /// fast-path check. Split out so tests can pin the close race: calling
    /// this on a closed hub is exactly a publisher that read `closed ==
    /// false` and then lost the linearization race to `close`.
    #[cfg(test)]
    fn publish_past_fast_path(&self, event: LifecycleEvent) {
        self.channels.signal.send_if_modified(|signal| {
            // Enqueue and activity notification share the same linearization
            // point as closure. A publisher that passed the atomic fast-path
            // check cannot append after receivers have observed `closed`.
            if signal.closed {
                return false;
            }
            let _ = self.channels.events.send(event);
            // The watch version is also the no-loss activity notification for
            // async receivers. Its value changes only for explicit lag.
            true
        });
    }

    #[cfg(test)]
    pub(crate) fn publish_lagged(&self, dropped: u64) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.channels.signal.send_if_modified(|signal| {
            if signal.closed {
                return false;
            }
            signal.explicit_lag = signal.explicit_lag.saturating_add(dropped);
            true
        });
    }

    pub(crate) fn publish_lagged_deferred(
        &self,
        wakes: &mut crate::cells::ObservationWakes,
        dropped: u64,
    ) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let mut published = false;
        self.channels.signal.modify_silently(|signal| {
            if signal.closed {
                return;
            }
            signal.explicit_lag = signal.explicit_lag.saturating_add(dropped);
            published = true;
        });
        if published {
            wakes.pulse(&self.channels.signal);
        }
    }

    #[cfg(test)]
    pub(crate) fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.channels
                .signal
                .send_modify(|signal| signal.closed = true);
        }
    }

    pub(crate) fn close_deferred(&self, wakes: &mut crate::cells::ObservationWakes) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.channels
            .signal
            .modify_silently(|signal| signal.closed = true);
        wakes.pulse(&self.channels.signal);
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
    use std::{
        sync::{Arc, atomic::Ordering},
        thread,
    };

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
        hub.publish(|| panic!("projection must be lazy when no receiver exists"));
    }

    #[crate::runtime::test]
    async fn snapshot_publication_that_linearizes_before_close_is_drained() {
        let hub = Arc::new(SnapshotHub::default());
        let mut snapshots = hub.subscribe(snapshot(ScopeState::Unstarted));
        let (entered, wait_for_release) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();

        let publisher = {
            let hub = Arc::clone(&hub);
            thread::spawn(move || {
                hub.publish(|| {
                    entered.send(()).expect("test remains active");
                    released.recv().expect("publisher is released");
                    snapshot(ScopeState::Running)
                });
            })
        };
        wait_for_release
            .recv()
            .expect("publisher reaches the serialized channel update");
        let closer = {
            let hub = Arc::clone(&hub);
            thread::spawn(move || hub.close())
        };
        while !hub.closed.load(Ordering::Acquire) {
            thread::yield_now();
        }
        release.send(()).expect("publisher is still waiting");
        publisher.join().expect("publisher does not panic");
        closer.join().expect("closer does not panic");

        assert_eq!(
            snapshots
                .changed()
                .await
                .expect("publication precedes close")
                .state,
            ScopeState::Running
        );
        assert!(snapshots.changed().await.is_err());
        hub.publish(|| panic!("publication after close must remain lazy"));

        let mut after_close = hub.subscribe(snapshot(ScopeState::Stopped {
            reason: crate::StopReason::Finished,
        }));
        assert!(matches!(
            after_close.borrow_latest().state,
            ScopeState::Stopped {
                reason: crate::StopReason::Finished
            }
        ));
        assert!(after_close.changed().await.is_err());
    }

    #[crate::runtime::test]
    async fn lifecycle_close_wakes_parked_receivers() {
        let hub = Arc::new(LifecycleHub::default());
        let mut events = hub.subscribe();
        let waiter = crate::runtime::spawn(async move { events.recv().await });
        crate::runtime::yield_now().await;

        hub.close();

        assert_eq!(crate::runtime::join_resuming(waiter).await, None);
    }

    #[test]
    fn lifecycle_publication_that_lost_the_close_race_appends_nothing() {
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&crate::ChildId::from("scope"))
            .expect("membership available");
        let hub = LifecycleHub::default();
        let mut events = hub.subscribe();
        hub.close();

        // A publisher that read `closed == false` and then lost the
        // linearization race to `close` must append nothing.
        hub.publish_past_fast_path(LifecycleEvent {
            scope_path: Vec::new(),
            scope: membership,
            seq: LifecycleSeq::new(1),
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Running,
            },
        });

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
            .expect("membership available");
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
        assert_eq!(first_retained.seq.get(), 3);
    }

    #[test]
    fn lifecycle_close_drains_the_queued_prefix_and_rejects_late_publication() {
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&crate::ChildId::from("scope"))
            .expect("membership available");
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

        hub.publish_lagged(2);
        hub.publish(event(1));
        hub.close();
        hub.publish_lagged(3);
        hub.publish(event(2));

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
