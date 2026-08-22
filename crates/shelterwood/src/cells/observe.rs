//! Restart-stable scope snapshots and lifecycle streams.

use std::{
    fmt,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use crate::runtime;
use shelterwood_core::{
    ChildId, Exit, Incarnation, Intensity, Membership, RestartAttempt, RestartCount, RestartPolicy,
    Retention, Strategy, TotalRestarts,
    engine::{MembershipStatus, ScopeState},
    identity::PoisonedCounter,
    policy::ScopeFlavor,
};

use crate::cells::RetainedExit;

/// Number of lifecycle events retained independently for each subscriber.
#[doc(hidden)]
pub(crate) const LIFECYCLE_EVENT_CAPACITY: usize = 128;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetainedLifecycleEvent {
    // Keep the public event before its guards. Ring eviction therefore drops
    // every raw exit projection while a retained copy still protects its user
    // error, and only the guards' later drop submits destruction to isolated
    // disposal. This is the same field-order argument as
    // `RetainedScopeSnapshot` and `RetainedStopReason`.
    event: LifecycleEvent,
    guards: Arc<Vec<RetainedExit>>,
}

impl RetainedLifecycleEvent {
    /// Retention guards protecting every user error one lifecycle edge
    /// carries.
    ///
    /// Split out of [`Self::new`] so a producer can mint the guards *before*
    /// the fallible framework bookkeeping that decides whether the edge is
    /// ever assembled. The single exhaustive `match` stays here either way.
    pub(crate) fn retain_guards(kind: &LifecycleEventKind) -> Vec<RetainedExit> {
        // Exhaustive with no wildcard arm on purpose: `LifecycleEventKind` is
        // non-exhaustive for downstream crates but not here, so a new variant
        // fails to compile until its retention is declared. A variant that
        // carries an `Exit` and slipped through with no guard would let ring
        // eviction drop a user error under the observation gate.
        let mut guards = Vec::new();
        match kind {
            LifecycleEventKind::Exited { exit, .. } => {
                RetainedExit::retain_exit(&mut guards, exit);
            }
            LifecycleEventKind::ScopeState { state } => {
                RetainedExit::retain_scope_state(&mut guards, state);
            }
            LifecycleEventKind::Added { .. }
            | LifecycleEventKind::Started { .. }
            | LifecycleEventKind::Ready { .. }
            | LifecycleEventKind::RestartScheduled { .. }
            | LifecycleEventKind::Removed { .. } => {}
        }
        guards
    }

    fn new(event: LifecycleEvent) -> Self {
        let guards = Self::retain_guards(&event.kind);
        Self {
            event,
            guards: Arc::new(guards),
        }
    }

    /// Assembles a leaf edge from a kind whose guards were already minted by
    /// [`Self::retain_guards`].
    pub(crate) fn from_parts(
        scope: Membership,
        seq: LifecycleSeq,
        kind: LifecycleEventKind,
        guards: Vec<RetainedExit>,
    ) -> Self {
        Self {
            event: LifecycleEvent {
                scope_path: Vec::new(),
                scope,
                seq,
                kind,
            },
            guards: Arc::new(guards),
        }
    }

    /// Extends the scope path towards the subscribed ancestor.
    pub(crate) fn prepend_scope(&mut self, id: ChildId) {
        self.event.scope_path.insert(0, id);
    }

    fn into_public(self) -> LifecycleEvent {
        let Self { event, guards } = self;
        // The public event owns a raw clone corresponding to every guard, so
        // retiring these copies inline is provably refcount-only. Unwrapping
        // the guard allocation first matters: a broadcast receive is often the
        // last owner, and letting the arc drop the guards would route a live
        // user error through isolated disposal for nothing.
        let guards = match Arc::try_unwrap(guards) {
            Ok(guards) => guards,
            Err(shared) => shared.as_ref().clone(),
        };
        for guard in guards {
            drop(guard.into_exit());
        }
        event
    }
}

impl From<LifecycleEvent> for RetainedLifecycleEvent {
    fn from(event: LifecycleEvent) -> Self {
        Self::new(event)
    }
}

/// Core lifecycle event inventory.
///
/// Non-exhaustive deliberately: Part II observation extensions add event kinds.
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
    pub kind: ScopeFlavor,
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

/// A public projection plus retained copies of every exit it contains.
///
/// Field order is intentional: the public projection's `Arc` is released
/// while the retained exits still prove that no user error can be destroyed
/// inline. Each retained exit then transfers its failed payload to isolated
/// disposal. A public `Arc` handed to a caller may outlive these guards, in
/// which case that caller keeps ordinary last-drop semantics.
#[derive(Clone, Debug)]
pub(crate) struct RetainedScopeSnapshot {
    snapshot: Arc<ScopeSnapshot>,
    exits: Arc<Vec<RetainedExit>>,
}

impl RetainedScopeSnapshot {
    pub(crate) fn new(snapshot: Arc<ScopeSnapshot>, exits: Vec<RetainedExit>) -> Self {
        Self {
            snapshot,
            exits: Arc::new(exits),
        }
    }

    fn public(&self) -> Arc<ScopeSnapshot> {
        Arc::clone(&self.snapshot)
    }

    pub(crate) fn into_public(self) -> Arc<ScopeSnapshot> {
        let (snapshot, exits) = self.into_parts();
        // The public projection owns a raw clone corresponding to every
        // guard, so releasing these copies inline is provably refcount-only.
        // The caller's eventual last drop keeps ordinary user semantics.
        for exit in exits {
            drop(exit.into_exit());
        }
        snapshot
    }

    pub(crate) fn into_parts(self) -> (Arc<ScopeSnapshot>, Vec<RetainedExit>) {
        let exits = match Arc::try_unwrap(self.exits) {
            Ok(exits) => exits,
            Err(exits) => exits.as_ref().clone(),
        };
        (self.snapshot, exits)
    }
}

/// Error returned after a snapshot watch has closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("scope snapshot watch is closed")]
pub struct SnapshotClosed;

/// Conflating receiver for recursive scope snapshots.
///
/// Every retained value is a complete observation-gate transaction cut.
/// Repeated publications for one scope within a transaction are coalesced, so
/// an ungated borrow sees either the preceding committed cut or the final one,
/// never an intermediate value from a compound transition.
#[derive(Clone)]
pub struct SnapshotReceiver {
    inner: runtime::WatchReceiver<SnapshotHubState>,
    seen_generation: u64,
}

impl SnapshotReceiver {
    /// Borrows the newest committed transaction cut without marking it
    /// observed.
    #[must_use]
    pub fn borrow_latest(&self) -> Arc<ScopeSnapshot> {
        self.inner.borrow_cloned().snapshot.public()
    }

    /// Borrows the newest snapshot and terminal flag from one retained state.
    // This method bridges the lower cell crate to the downstream façade's
    // `ScopeRef` implementation. It remains callable on the façade-public
    // receiver, but its signature is entirely supported façade data and grants
    // no construction or implementation capability. Hiding it from generated
    // docs is the explicit boundary ruling; the rustdoc-JSON walk deliberately
    // does not grow a second hidden-item graph for this benign bridge.
    #[must_use]
    #[doc(hidden)]
    pub fn borrow_latest_and_closed(&self) -> (Arc<ScopeSnapshot>, bool) {
        let state = self.inner.borrow_cloned();
        (state.snapshot.public(), state.closed)
    }

    /// The hub's current generation.
    ///
    /// Delivery conflates generation edges, so [`Self::changed`] resolves once
    /// whether a transaction minted one edge or ten. A test that pins the
    /// publication economy — one edge per hub per transaction — has to read
    /// the counter directly.
    #[cfg(test)]
    #[must_use]
    pub fn current_generation(&self) -> u64 {
        self.inner.borrow_cloned().generation.current()
    }

    /// Waits for and returns a newer snapshot.
    pub async fn changed(&mut self) -> Result<Arc<ScopeSnapshot>, SnapshotClosed> {
        loop {
            let state = self.inner.borrow_and_update_cloned();
            if state.generation.current() != self.seen_generation {
                self.seen_generation = state.generation.current();
                return Ok(state.snapshot.public());
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
    snapshot: RetainedScopeSnapshot,
    generation: PoisonedCounter,
    closed: bool,
}

/// Deferred construction of one hub's transaction-final projection.
///
/// A publication stages a producer rather than a value. A compound transition
/// touching M children publishes M times per hub, and every intermediate cut
/// is superseded by the next one, so materializing them would build M full
/// recursive projections and retain all but one of them until commit — M²/2
/// `ChildSnapshot`s per hub for a batch admission whose width the user
/// declares. Coalescing replaces the producer instead, so a superseded cut is
/// never built at all and the survivor runs exactly once, inside `commit`,
/// while the observation gate still holds the resident tree still.
///
/// The producer is `FnMut` rather than `FnOnce` so that running it does not
/// also destroy it: it captures an `Arc<ScopeCell>` whose release belongs
/// after the gate is unlocked, like every other user-bearing drop. The
/// transaction hands the whole spent publication to its effect list after
/// each attempted install, including one that trips an invariant.
pub(crate) struct SnapshotProjection(Box<dyn FnMut() -> Option<RetainedScopeSnapshot>>);

impl SnapshotProjection {
    fn new(build: impl FnOnce() -> RetainedScopeSnapshot + 'static) -> Self {
        let mut build = Some(build);
        Self(Box::new(move || build.take().map(|build| build())))
    }

    fn build(&mut self) -> RetainedScopeSnapshot {
        (self.0)().expect("a staged projection is built exactly once")
    }
}

pub(crate) struct SnapshotPublication {
    sender: runtime::WatchSender<SnapshotHubState>,
    projection: SnapshotProjection,
    published: bool,
    closed: bool,
}

impl SnapshotPublication {
    fn published(
        sender: runtime::WatchSender<SnapshotHubState>,
        projection: SnapshotProjection,
    ) -> Self {
        Self {
            sender,
            projection,
            published: true,
            closed: false,
        }
    }

    fn closed(
        sender: runtime::WatchSender<SnapshotHubState>,
        projection: SnapshotProjection,
    ) -> Self {
        Self {
            sender,
            projection,
            published: false,
            closed: true,
        }
    }

    pub(crate) fn same_hub(&self, other: &Self) -> bool {
        self.sender.same_channel(&other.sender)
    }

    pub(crate) fn closes(&self, hub: &SnapshotHub) -> bool {
        self.closed
            && hub
                .sender
                .get()
                .is_some_and(|sender| self.sender.same_channel(sender))
    }

    pub(crate) fn coalesce(&mut self, newer: Self) -> SnapshotProjection {
        // `ObservationTxn::stage_snapshot` selects this publication with
        // `same_hub`, so hub identity is structural. Closure is total: once
        // staged, it wins over every later publication in the transaction.
        if self.closed {
            return newer.projection;
        }
        self.published |= newer.published;
        self.closed = newer.closed;
        std::mem::replace(&mut self.projection, newer.projection)
    }

    /// Builds and installs this hub's final cut while the gate is still held.
    ///
    /// Every user-bearing value leaves through `effects`: the spent producer
    /// with its capture of the publishing scope, the projection this one
    /// displaces, and the generation-exhaustion panic. The producer runs
    /// before the watch's own lock is taken, so building a cut — which walks
    /// the resident tree — never nests that tree's locks inside the watch.
    pub(crate) fn install(&mut self, effects: &mut Vec<Box<dyn FnOnce()>>) {
        // A hub closed by an earlier transaction keeps the authoritative
        // terminal projection `close` installed; this cut is never built.
        let mut snapshot =
            (!self.sender.read_with(|state| state.closed)).then(|| self.projection.build());
        let mut retired = None;
        let mut modified = false;
        let mut generation_exhausted = false;
        if snapshot.is_some() {
            self.sender.modify_silently(|state| {
                if state.closed {
                    return;
                }
                retired = Some(std::mem::replace(
                    &mut state.snapshot,
                    snapshot
                        .take()
                        .expect("a staged snapshot is installed exactly once"),
                ));
                if self.published && state.generation.mint().is_none() {
                    generation_exhausted = true;
                }
                state.closed = self.closed;
                modified = true;
            });
        }
        if let Some(uninstalled) = snapshot.take() {
            // A prior committed close remains authoritative. The cut was
            // already built, so retire its user-bearing snapshot after both
            // the watch mutex and observation gate are released.
            effects.push(Box::new(move || drop(uninstalled)));
        }
        if let Some(retired) = retired {
            effects.push(Box::new(move || drop(retired)));
        }
        if generation_exhausted {
            effects.push(Box::new(|| panic!("snapshot generation space exhausted")));
        }
        if modified {
            let sender = self.sender.clone();
            effects.push(Box::new(move || sender.pulse()));
        }
    }
}

impl SnapshotHub {
    /// Subscribes while the containing scope's observation gate is held.
    ///
    /// No wake is owed for the receiverless refresh below: the gate is what
    /// makes it safe, and the only receiver that could observe the refresh is
    /// the one minted here — which captures the refreshed generation as
    /// already seen. The displaced projection, the caller's own projection
    /// wherever it goes uninstalled, and a generation-exhaustion panic are
    /// still deferred through the transaction so retirement and panic
    /// resumption happen after that gate is unlocked.
    ///
    /// Requiring the transaction also makes "hub initialization and the
    /// receiverless refresh are serialized against publication" a static
    /// property of every caller rather than a convention.
    pub(crate) fn subscribe(
        &self,
        initial: RetainedScopeSnapshot,
        txn: &mut crate::cells::ObservationTxn<'_>,
    ) -> SnapshotReceiver {
        let mut initialized = false;
        let sender = self.sender.get_or_init(|| {
            initialized = true;
            runtime::watch(SnapshotHubState {
                snapshot: initial.clone(),
                generation: PoisonedCounter::new(),
                closed: false,
            })
            .0
        });
        // Initialization consumed a clone, and a closed hub declines the
        // install below. Whatever the caller's projection is not moved into is
        // still a full retained cut, so it retires through the transaction
        // rather than under the gate — and, on the closed branch, under the
        // watch's own lock as well.
        let mut uninstalled = Some(initial);
        if !initialized && sender.receiver_count() == 0 {
            // Publication is skipped while receiverless, so the first
            // subscriber after a quiet stretch installs current state itself.
            // A closed hub already holds the authoritative terminal
            // projection installed by `close`; leave it unchanged.
            let mut retired = None;
            let mut generation_exhausted = false;
            sender.modify_silently(|state| {
                if state.closed {
                    return;
                }
                retired = Some(std::mem::replace(
                    &mut state.snapshot,
                    uninstalled
                        .take()
                        .expect("the caller's projection is installed at most once"),
                ));
                generation_exhausted = state.generation.mint().is_none();
            });
            if let Some(retired) = retired {
                txn.defer(move || drop(retired));
            }
            if generation_exhausted {
                txn.defer(|| panic!("snapshot generation space exhausted"));
            }
        }
        if let Some(uninstalled) = uninstalled {
            txn.defer(move || drop(uninstalled));
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
        snapshot: impl FnOnce() -> RetainedScopeSnapshot + 'static,
    ) {
        let Some(sender) = self.sender.get() else {
            return;
        };
        if sender.receiver_count() == 0 {
            return;
        }
        if txn.snapshot_hub_will_close(self) || sender.read_with(|state| state.closed) {
            return;
        }
        txn.stage_snapshot(SnapshotPublication::published(
            sender.clone(),
            SnapshotProjection::new(snapshot),
        ));
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
        final_snapshot: impl FnOnce() -> RetainedScopeSnapshot + 'static,
    ) {
        let mut final_snapshot = Some(final_snapshot);
        let mut initialized = false;
        let sender = self.sender.get_or_init(|| {
            initialized = true;
            runtime::watch(SnapshotHubState {
                snapshot: final_snapshot
                    .take()
                    .expect("final snapshot is built exactly once")(),
                generation: PoisonedCounter::new(),
                closed: true,
            })
            .0
        });
        if initialized {
            return;
        }
        if txn.snapshot_hub_will_close(self) || sender.read_with(|state| state.closed) {
            return;
        }
        // Install the caller's authoritative terminal projection
        // unconditionally, whether or not receivers are attached.
        // Publication is skipped entirely while receiverless, and one close
        // site (`finish_incarnation_with_terminal`'s stale-epoch branch)
        // deliberately closes without publishing a `Stopped` projection
        // first, so the retained value is not otherwise reliably terminal.
        // If this transaction already staged a publication, coalescing keeps
        // that publication's generation edge but replaces its projection with
        // this final one.
        txn.stage_snapshot(SnapshotPublication::closed(
            sender.clone(),
            SnapshotProjection::new(move || {
                final_snapshot
                    .take()
                    .expect("final snapshot is built exactly once")()
            }),
        ));
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
    events: runtime::BroadcastReceiver<RetainedLifecycleEvent>,
    signal: runtime::WatchReceiver<LifecycleSignal>,
    seen_explicit_lag: u64,
    // Total fallback for a broadcast implementation that returns an item
    // despite reporting a length beyond its effective capacity. The explicit
    // marker remains first and the retained event is not destroyed on an
    // invariant-panic stack.
    pending: Option<RetainedLifecycleEvent>,
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
            if self.pending.is_none() && self.events.len() > LIFECYCLE_EVENT_CAPACITY {
                match self.events.try_receive() {
                    runtime::BroadcastReceive::Lagged(overflow) => {
                        dropped = dropped.saturating_add(overflow);
                    }
                    runtime::BroadcastReceive::Item(event) => {
                        self.pending = Some(event);
                    }
                    runtime::BroadcastReceive::Empty | runtime::BroadcastReceive::Closed => {}
                }
            }
            return Ok(LifecycleItem::Lagged { dropped });
        }
        if let Some(event) = self.pending.take() {
            return Ok(LifecycleItem::Event(event.into_public()));
        }
        match self.events.try_receive() {
            runtime::BroadcastReceive::Item(event) => Ok(LifecycleItem::Event(event.into_public())),
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
            .field(
                "queued",
                &(self.events.len() + usize::from(self.pending.is_some())),
            )
            .finish_non_exhaustive()
    }
}

pub(crate) struct LifecycleHub {
    events: runtime::BroadcastSender<RetainedLifecycleEvent>,
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
        Self { events, signal }
    }
}

impl LifecycleHub {
    /// Subscribes while the containing scope's observation gate is held.
    ///
    /// Requiring the transaction makes the signal snapshot and broadcast
    /// subscription atomic with publication by construction.
    pub(crate) fn subscribe(&self, _txn: &mut crate::cells::ObservationTxn<'_>) -> LifecycleEvents {
        let signal = self.signal.watcher();
        let seen_explicit_lag = signal.borrow_cloned().explicit_lag;
        LifecycleEvents {
            events: self.events.subscribe(),
            signal,
            seen_explicit_lag,
            pending: None,
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.signal.read_with(|signal| signal.closed)
    }

    /// Publishes while the containing scope's observation gate is held.
    pub(crate) fn publish(
        &self,
        txn: &mut crate::cells::ObservationTxn<'_>,
        event: impl Into<RetainedLifecycleEvent>,
    ) {
        let event = event.into();
        let mut published = false;
        let mut undelivered = None;
        self.signal.modify_silently(|signal| {
            if signal.closed {
                undelivered = Some(event);
                return;
            }
            // Keep insertion under the gate. Deferring `send` would let two
            // transactions flush out of `seq` order, invert a child's `Added`
            // edge against events from inside it, and expose closure before
            // the final event from the closing transaction. Tokio's send does
            // not invoke a user callback here: Shelterwood never awaits the
            // broadcast receiver, so its waiter list is empty. A full ring can
            // evict a retained event in this call, whose last guard may submit
            // critical disposal under both locks. That submission is
            // refcount/framework work only and is the accepted trade because
            // drop glue inside the send has no transaction sink to reach.
            undelivered = self.events.send(event).err();
            published = true;
        });
        if let Some(undelivered) = undelivered {
            txn.defer(move || drop(undelivered));
        }
        if published {
            txn.pulse(&self.signal);
        }
    }

    /// Publishes an explicit lag marker under the observation gate.
    pub(crate) fn publish_lagged(&self, txn: &mut crate::cells::ObservationTxn<'_>, dropped: u64) {
        let mut published = false;
        self.signal.modify_silently(|signal| {
            if signal.closed {
                return;
            }
            signal.explicit_lag = signal.explicit_lag.saturating_add(dropped);
            published = true;
        });
        if published {
            txn.pulse(&self.signal);
        }
    }

    /// Closes while the containing scope's observation gate is held.
    pub(crate) fn close(&self, txn: &mut crate::cells::ObservationTxn<'_>) {
        let mut modified = false;
        self.signal.modify_silently(|signal| {
            if !signal.closed {
                signal.closed = true;
                modified = true;
            }
        });
        if modified {
            txn.pulse(&self.signal);
        }
    }
}

impl fmt::Debug for LifecycleHub {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleHub")
            .field("receivers", &self.events.receiver_count())
            .field("closed", &self.signal.read_with(|signal| signal.closed))
            .finish()
    }
}

/// Failure returned while a façade scope handle waits for a child snapshot.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
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
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::runtime;
    use shelterwood_core::{
        ChildId, Intensity, TotalRestarts,
        identity::{PoisonedCounter, ScopeIdentity},
        policy::ScopeFlavor,
    };

    use crate::observe::{
        LifecycleEvent, LifecycleEventKind, LifecycleItem, LifecycleTryRecvError, ScopeSnapshot,
    };
    use shelterwood_core::{ScopeState, StopReason};

    use super::{
        LIFECYCLE_EVENT_CAPACITY, LifecycleHub, LifecycleSeq, RetainedScopeSnapshot, SnapshotHub,
    };

    fn snapshot(state: ScopeState) -> RetainedScopeSnapshot {
        RetainedScopeSnapshot::new(
            Arc::new(ScopeSnapshot {
                state,
                kind: ScopeFlavor::Dynamic,
                strategy: None,
                intensity: Intensity::default(),
                total_restarts: TotalRestarts::ZERO,
                lifecycle_seq: LifecycleSeq::new(0),
                children: Arc::from([]),
            }),
            Vec::new(),
        )
    }

    #[test]
    fn snapshot_projection_is_skipped_without_subscribers() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        hub.publish(&mut txn, || {
            panic!("projection must be lazy when no receiver exists")
        });
    }

    #[test]
    fn initial_snapshot_subscription_does_not_mint_a_generation() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Unstarted), &mut txn);
        drop(txn);

        let generation = hub
            .sender
            .get()
            .expect("subscription initializes the hub")
            .read_with(|state| state.generation.current());
        assert_eq!(generation, 0);
        drop(receiver);
    }

    #[test]
    fn receiverless_generation_exhaustion_is_deferred_until_commit() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Unstarted), &mut txn);
        drop(txn);
        drop(receiver);

        let sender = hub.sender.get().expect("subscription initializes the hub");
        sender.modify_silently(|state| {
            state.generation = PoisonedCounter::near_exhaustion();
        });

        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Starting), &mut txn);
        drop(txn);
        drop(receiver);

        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        assert_eq!(receiver.borrow_latest().state, ScopeState::Running);
        drop(receiver);
        assert!(
            catch_unwind(AssertUnwindSafe(|| drop(txn))).is_err(),
            "generation exhaustion must resume only when the transaction commits"
        );
    }

    #[test]
    fn staged_snapshot_is_installed_during_unwind() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Unstarted), &mut txn);
        drop(txn);

        // The mid-transaction reading is published outward rather than
        // asserted in place: an assert inside the closure would panic into
        // the unwind that `catch_unwind` swallows, leaving the staging half
        // of this test inert.
        let mut mid_txn = None;
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let mut txn = crate::cells::ObservationTxn::detached();
                hub.publish(&mut txn, || snapshot(ScopeState::Running));
                mid_txn = Some(receiver.borrow_latest().state.clone());
                panic!("inject transaction unwind");
            }))
            .is_err()
        );
        assert_eq!(
            mid_txn,
            Some(ScopeState::Unstarted),
            "an ungated borrow taken mid-transaction reads the preceding cut"
        );
        assert_eq!(receiver.borrow_latest().state, ScopeState::Running);
    }

    #[test]
    fn panicking_snapshot_install_does_not_strand_later_cuts_or_effects() {
        let failing = SnapshotHub::default();
        let succeeding = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let _failing_receiver = failing.subscribe(snapshot(ScopeState::Unstarted), &mut txn);
        let succeeding_receiver = succeeding.subscribe(snapshot(ScopeState::Unstarted), &mut txn);
        drop(txn);

        let effects = Arc::new(AtomicUsize::new(0));
        let payload = catch_unwind(AssertUnwindSafe({
            let effects = Arc::clone(&effects);
            move || {
                let mut txn = crate::cells::ObservationTxn::detached();
                failing.publish(&mut txn, || panic!("injected snapshot installation panic"));
                succeeding.publish(&mut txn, || snapshot(ScopeState::Running));
                txn.defer(move || {
                    effects.fetch_add(1, Ordering::SeqCst);
                });
            }
        }))
        .expect_err("the first installation panic resumes after commit drains");

        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"injected snapshot installation panic")
        );
        assert_eq!(
            succeeding_receiver.borrow_latest().state,
            ScopeState::Running
        );
        assert_eq!(
            effects.load(Ordering::SeqCst),
            1,
            "installation failure cannot suppress the queued effect suffix"
        );
    }

    #[test]
    fn repeated_publication_builds_and_mints_once_per_transaction() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Unstarted), &mut txn);
        drop(txn);

        let builds = Arc::new(AtomicUsize::new(0));
        let mut txn = crate::cells::ObservationTxn::detached();
        for state in [
            ScopeState::Starting,
            ScopeState::Running,
            ScopeState::Draining,
        ] {
            let builds = Arc::clone(&builds);
            hub.publish(&mut txn, move || {
                builds.fetch_add(1, Ordering::Relaxed);
                snapshot(state)
            });
        }
        drop(txn);

        assert_eq!(
            builds.load(Ordering::Relaxed),
            1,
            "coalescing leaves one producer per hub, and only it is built"
        );
        assert_eq!(receiver.borrow_latest().state, ScopeState::Draining);
        let generation = hub
            .sender
            .get()
            .expect("subscription initializes the hub")
            .read_with(|state| state.generation.current());
        assert_eq!(generation, 1, "one transaction mints one generation edge");
    }

    #[test]
    fn publication_after_staged_close_is_not_built() {
        let hub = SnapshotHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let receiver = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.close(&mut txn, || {
            snapshot(ScopeState::Stopped {
                reason: StopReason::Finished,
            })
        });
        assert!(
            !receiver.borrow_latest_and_closed().1,
            "the close is still staged, so the publication below is declined \
             by the transaction rather than by an installed terminal state"
        );
        hub.publish(&mut txn, || {
            panic!("publication after a staged close must remain lazy")
        });
        drop(txn);

        assert!(matches!(
            receiver.borrow_latest_and_closed(),
            (
                snapshot,
                true
            ) if matches!(
                snapshot.state,
                ScopeState::Stopped {
                    reason: StopReason::Finished
                }
            )
        ));
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
                reason: StopReason::Finished,
            })
        });
        hub.close(&mut txn, || {
            snapshot(ScopeState::Stopped {
                reason: StopReason::Finished,
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
                reason: StopReason::Finished
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
                reason: StopReason::Finished,
            }),
            &mut txn,
        );
        drop(txn);
        assert!(matches!(
            after_close.borrow_latest().state,
            ScopeState::Stopped {
                reason: StopReason::Finished
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
                reason: StopReason::NeverStarted,
            })
        });
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        let mut receiver = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);
        assert!(matches!(
            receiver.borrow_latest().state,
            ScopeState::Stopped {
                reason: StopReason::NeverStarted
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
                reason: StopReason::Finished,
            })
        });
        drop(txn);

        let mut txn = crate::cells::ObservationTxn::detached();
        let mut receiver = hub.subscribe(snapshot(ScopeState::Running), &mut txn);
        drop(txn);
        assert!(matches!(
            receiver.borrow_latest().state,
            ScopeState::Stopped {
                reason: StopReason::Finished
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
                reason: StopReason::Finished,
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
                reason: StopReason::Finished
            }
        ));
        assert!(later.changed().await.is_err());
    }

    #[crate::runtime::test]
    async fn lifecycle_close_wakes_parked_receivers() {
        let hub = Arc::new(LifecycleHub::default());
        let mut txn = crate::cells::ObservationTxn::detached();
        let mut events = hub.subscribe(&mut txn);
        drop(txn);
        let waiter = runtime::spawn(async move { events.recv().await });
        runtime::yield_now().await;

        let mut txn = crate::cells::ObservationTxn::detached();
        hub.close(&mut txn);
        drop(txn);

        assert_eq!(runtime::join_resuming(waiter).await, None);
    }

    #[test]
    fn lifecycle_publication_after_close_appends_nothing() {
        let mut identity = ScopeIdentity::new();
        let membership = identity
            .mint_membership(&ChildId::from("scope"))
            .expect("membership available")
            .membership();
        let hub = LifecycleHub::default();
        let mut txn = crate::cells::ObservationTxn::detached();
        let mut events = hub.subscribe(&mut txn);
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
            .mint_membership(&ChildId::from("scope"))
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
        let mut txn = crate::cells::ObservationTxn::detached();
        let mut events = hub.subscribe(&mut txn);
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
            .mint_membership(&ChildId::from("scope"))
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
        let mut txn = crate::cells::ObservationTxn::detached();
        let mut events = hub.subscribe(&mut txn);
        hub.publish_lagged(&mut txn, 2);
        hub.publish(&mut txn, event(1));
        hub.close(&mut txn);
        hub.publish_lagged(&mut txn, 3);
        hub.publish(&mut txn, event(2));
        drop(txn);

        assert_eq!(events.try_recv(), Ok(LifecycleItem::Lagged { dropped: 2 }));
        assert_eq!(events.try_recv(), Ok(LifecycleItem::Event(event(1))));
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
        let mut txn = crate::cells::ObservationTxn::detached();
        let mut after_close = hub.subscribe(&mut txn);
        drop(txn);
        assert_eq!(
            after_close.try_recv(),
            Err(LifecycleTryRecvError::Closed),
            "post-close subscribers start at the drained terminal edge"
        );
    }
}
