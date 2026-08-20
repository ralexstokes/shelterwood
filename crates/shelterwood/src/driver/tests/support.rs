pub(super) use std::{
    collections::BTreeMap,
    future::{self, Future},
    ops::{Index, IndexMut},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Barrier, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

pub(super) use crate::{
    ActorRef, Backoff, Cancellation, ChildId, ChildState, DynamicTree, Exit, ExitError, ExitKind,
    GracePhase, Incarnation, Intensity, LifecycleEventKind, LifecycleItem, LifecycleTryRecvError,
    Mailbox, MembershipStatus, RawOnceDef, Readiness, ReadinessDeadline, RemoveOutcome,
    ReserveError, RestartCondition, RestartCount, RestartPolicy, Retention, ScopeRef, ScopeState,
    SendErrorKind, StartupError, StartupFailure, StartupFailureCause, StopReason, SubtreeDef,
    SubtreeOnceDef, TaskDef, TaskOnceDef, Tree,
    cells::LIFECYCLE_EVENT_CAPACITY,
    engine::{Epoch, ScopeLifecycle, StopLadder, arbitrate},
    exit::RecordedOutcome,
    identity::{IncarnationCounter, ScopeIdentity},
    mailbox::{MailboxCell, actor_ref_from_parts},
    plan::{BuilderCore, ChildConstruction, SlotCell, resolve_fixture_options},
    policy::ResolvedDefaults,
    runtime::{CompletionGatedLatch, DedicatedRuntime, Latch},
};
pub(super) use shelterwood_core::supervisor::{ChildKey, Event as SupervisorEvent};

pub(super) struct ObservedExit {
    pub(super) child: ChildKey,
    incarnation: Incarnation,
    recorded: Option<RetainedRecordedOutcome>,
    join: crate::runtime::JoinOutcome<()>,
    cancellation: Cancellation,
    readiness_signal_seen: bool,
}

impl ObservedExit {
    pub(super) fn dispatch(self, scope: &mut ScopeRuntime) {
        scope.handle_exit(
            self.child,
            self.incarnation,
            self.recorded,
            self.join,
            self.cancellation,
            self.readiness_signal_seen,
        );
    }
}

pub(super) async fn recv_driver_event(
    receiver: &mut crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    timeout: Duration,
    expectation: &str,
) -> DriverEvent {
    match crate::runtime::timeout(timeout, receiver.recv()).await {
        crate::runtime::Timeout::Completed(Some(event)) => event,
        crate::runtime::Timeout::Completed(None) => {
            panic!("event lane closed while waiting for {expectation}")
        }
        crate::runtime::Timeout::Elapsed => panic!("timed out waiting for {expectation}"),
    }
}

pub(super) async fn recv_child_event(
    receiver: &mut crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    timeout: Duration,
    expectation: &str,
) -> ChildEvent {
    let DriverEvent::Child(event) = recv_driver_event(receiver, timeout, expectation).await else {
        panic!("expected {expectation}")
    };
    event
}

pub(super) async fn recv_child_exit(
    receiver: &mut crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    timeout: Duration,
    expectation: &str,
) -> ObservedExit {
    match recv_child_event(receiver, timeout, expectation).await {
        ChildEvent::Exited {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        } => ObservedExit {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        },
        _ => panic!("expected {expectation}"),
    }
}

pub(super) async fn recv_removal(
    receiver: &mut crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    timeout: Duration,
    expectation: &str,
) -> RemovalRequest {
    let DriverEvent::Removal(removal) = recv_driver_event(receiver, timeout, expectation).await
    else {
        panic!("expected {expectation}")
    };
    removal
}

pub(super) async fn recv_construction_disposed(
    receiver: &mut crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    timeout: Duration,
    expectation: &str,
) -> (ChildKey, Option<crate::runtime::DisposalPanic>) {
    let ChildEvent::ConstructionDisposed { child, panic } =
        recv_child_event(receiver, timeout, expectation).await
    else {
        panic!("expected {expectation}")
    };
    (child, panic)
}

pub(super) use super::super::{
    AdmissionRequest, AncestorCommandLatches, ChildEvent, ChildRuntime, ChildTerminality,
    DriverEvent, DynamicControl, DynamicEntry, DynamicReservation, GateCapture, MemberCell,
    MemberStage, MemberTransition, NestedScopeLatches, Pending, RemovalRequest, RemovalResponses,
    ResidentProjection, RetainedRecordedOutcome, RuntimeStorage, ScopeCell, ScopeControlEvent,
    ScopeEpochGuard, ScopeFlavor, ScopeRole, ScopeRuntime, ScopeRuntimeTestWiring,
    StartupDisposition, cancel_dynamic_reservation, child::dispatch_child_construction_for_test,
    discharge_child_terminality, events::collect_driver_events, monitor_root_driver, report_slot,
    nested_scope_start, reserve_dynamic, resident_projection, restart_shutdown_work,
    run_nested_factory, run_nested_tree, run_scope, run_scope_incarnation, storage::Obligation,
};

pub(super) async fn begin_admission(
    reservation: &DynamicReservation,
    receiver: &mut crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    fused_cancel: Option<Latch>,
) -> (
    crate::runtime::OneShotReceiver<Result<(), ReserveError>>,
    AdmissionRequest,
) {
    let response = super::super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        fused_cancel,
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = receiver.recv().await else {
        panic!("admission enqueueing submits the request")
    };
    (response, request)
}

pub(super) enum DynamicFixtureState {
    Resident,
    Removing,
}

pub(super) fn insert_dynamic_fixture(
    scope: &mut ScopeRuntime,
    control: &Arc<DynamicControl>,
    id: impl Into<ChildId>,
    construction: ChildConstruction,
    prepare: impl FnOnce(&mut ChildRuntime),
    state: DynamicFixtureState,
) -> (DynamicReservation, ChildKey) {
    let root = Arc::clone(&scope.root);
    let reservation = reserve_dynamic(&root, id.into(), None)
        .expect("running dynamic scope reserves the fixture child");
    reservation.slot.define(construction);
    let (definition, resolved) = reservation
        .slot
        .resolve_and_take_defined(&scope.defaults)
        .expect("the fixture slot is defined");
    let plan =
        crate::plan::ChildPlan::with_options(Arc::clone(&reservation.slot), definition, resolved);
    let mut child = ChildRuntime::from_plan(plan, &root);
    prepare(&mut child);
    let key = root.with_observation_gate(|txn| {
        let key = scope
            .insert_child(child, false)
            .unwrap_or_else(|_| panic!("the fixture child-key domain is available"));
        {
            let mut dynamic = control.state.lock().expect("dynamic-state mutex poisoned");
            let entry = dynamic
                .entries
                .get_mut(reservation.slot.member.id())
                .expect("the fixture reservation remains registered");
            entry.promote(key, None, txn);
            if matches!(state, DynamicFixtureState::Removing) {
                entry
                    .mark_removing(txn)
                    .expect("the promoted fixture resident transitions to Removing");
            }
        }
        root.admit_child_locked(resident_projection(&reservation.slot), txn);
        key
    });
    (reservation, key)
}

pub(super) struct ChildArena<T> {
    children: BTreeMap<ChildKey, T>,
    next: u64,
}

impl<T> Default for ChildArena<T> {
    fn default() -> Self {
        Self {
            children: BTreeMap::new(),
            next: 0,
        }
    }
}

impl<T> ChildArena<T> {
    pub(super) fn insert(&mut self, child: T) -> ChildKey {
        self.next += 1;
        let key = ChildKey::fixture(self.next);
        self.children.insert(key, child);
        key
    }

    pub(super) fn keys(&self) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children.keys().copied()
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.children.values_mut()
    }

    fn into_iter(self) -> impl Iterator<Item = (ChildKey, T)> {
        self.children.into_iter()
    }
}

impl<T> Index<ChildKey> for ChildArena<T> {
    type Output = T;

    fn index(&self, key: ChildKey) -> &Self::Output {
        &self.children[&key]
    }
}

impl<T> IndexMut<ChildKey> for ChildArena<T> {
    fn index_mut(&mut self, key: ChildKey) -> &mut Self::Output {
        self.children.get_mut(&key).expect("fixture child key")
    }
}

pub(super) struct ScopeRuntimeBuilder {
    root: Arc<ScopeCell>,
    epoch: ScopeEpochGuard,
    events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    defaults: ResolvedDefaults,
    intensity_policy: Intensity,
    children: ChildArena<ChildRuntime>,
    lifecycle: ScopeLifecycle,
    next_ordered_start: Option<Option<ChildKey>>,
    dynamic: Option<Arc<DynamicControl>>,
    hard_forced: bool,
    transferred_plan: Option<crate::plan::ScopePlan>,
}

impl ScopeRuntimeBuilder {
    pub(super) fn new(
        root: Arc<ScopeCell>,
        epoch: ScopeEpochGuard,
        events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    ) -> Self {
        Self {
            root,
            epoch,
            events,
            defaults: ResolvedDefaults::default(),
            intensity_policy: Intensity::default(),
            children: ChildArena::default(),
            lifecycle: ScopeLifecycle::starting(),
            next_ordered_start: None,
            dynamic: None,
            hard_forced: false,
            transferred_plan: None,
        }
    }

    pub(super) fn with_defaults(mut self, defaults: ResolvedDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    pub(super) fn with_intensity_policy(mut self, intensity_policy: Intensity) -> Self {
        self.intensity_policy = intensity_policy;
        self
    }

    pub(super) fn with_children(mut self, children: ChildArena<ChildRuntime>) -> Self {
        self.children = children;
        self
    }

    pub(super) fn with_lifecycle(mut self, lifecycle: ScopeLifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    pub(super) fn with_next_ordered_start(mut self, next: Option<ChildKey>) -> Self {
        self.next_ordered_start = Some(next);
        self
    }

    pub(super) fn with_dynamic(mut self, dynamic: Option<Arc<DynamicControl>>) -> Self {
        self.dynamic = dynamic;
        self
    }

    pub(super) fn with_hard_forced(mut self, hard_forced: bool) -> Self {
        self.hard_forced = hard_forced;
        self
    }

    pub(super) fn with_transferred_plan(mut self, plan: crate::plan::ScopePlan) -> Self {
        self.transferred_plan = Some(plan);
        self
    }

    pub(super) fn build(self) -> ScopeRuntime {
        let scope = ScopeRuntime::for_test(
            ScopeRuntimeTestWiring {
                root: self.root,
                defaults: self.defaults,
                intensity_policy: self.intensity_policy,
                children: self.children.into_iter().collect(),
                lifecycle: self.lifecycle,
                next_ordered_start: self.next_ordered_start,
                events: self.events,
                dynamic: self.dynamic,
                hard_forced: self.hard_forced,
            },
            self.epoch,
        );
        if let Some(plan) = self.transferred_plan {
            plan.finish_transfer();
        }
        scope.publish_initial_children();
        scope
    }
}

pub(super) struct OrderedScopeFixture {
    pub(super) root: Arc<ScopeCell>,
    pub(super) children: ChildArena<ChildRuntime>,
    plan: crate::plan::ScopePlan,
    epoch: ScopeEpochGuard,
    events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    event_receiver: crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    lifecycle: ScopeLifecycle,
    next_ordered_start: Option<Option<ChildKey>>,
    hard_forced: bool,
}

impl OrderedScopeFixture {
    pub(super) fn new(tree: Tree) -> Self {
        let mut plan = tree.lower_for_test();
        assert_eq!(plan.root.flavor, ScopeFlavor::Ordered);
        let root = Arc::clone(&plan.root);
        let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
        let (events, event_receiver) = crate::runtime::unbounded_mpsc();
        let mut children = ChildArena::default();
        plan.children.reverse();
        while let Some(child) = plan.children.pop() {
            children.insert(ChildRuntime::from_plan(child, &root));
        }
        Self {
            root,
            children,
            plan,
            epoch,
            events,
            event_receiver,
            lifecycle: ScopeLifecycle::starting(),
            next_ordered_start: None,
            hard_forced: false,
        }
    }

    /// The raw epoch the built scope transfers, for tests that assert epoch
    /// retirement after the scope itself is gone.
    pub(super) fn epoch(&self) -> Epoch {
        self.epoch.epoch()
    }

    pub(super) fn with_lifecycle(mut self, lifecycle: ScopeLifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    pub(super) fn with_next_ordered_start(mut self, next: Option<ChildKey>) -> Self {
        self.next_ordered_start = Some(next);
        self
    }

    pub(super) fn with_hard_forced(mut self, hard_forced: bool) -> Self {
        self.hard_forced = hard_forced;
        self
    }

    pub(super) fn build(
        self,
    ) -> (
        ScopeRuntime,
        crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    ) {
        let mut builder = ScopeRuntimeBuilder::new(Arc::clone(&self.root), self.epoch, self.events)
            .with_defaults(self.plan.defaults.clone())
            .with_intensity_policy(self.plan.intensity_policy())
            .with_children(self.children)
            .with_lifecycle(self.lifecycle)
            .with_hard_forced(self.hard_forced)
            .with_transferred_plan(self.plan);
        if let Some(next) = self.next_ordered_start {
            builder = builder.with_next_ordered_start(next);
        }
        let scope = builder.build();
        (scope, self.event_receiver)
    }
}

/// Bounds every gate-capture probe wait. The probe sender lives inside
/// the scope cell for the whole test, so the channel can never
/// disconnect: a regression that keeps a thread from reaching its gate
/// must time out with a diagnostic rather than hang the test on `recv`.
pub(super) const CAPTURE_PROBE_WAIT: Duration = Duration::from_secs(10);

/// Bounds every real-clock wait on driver progress in the async driver
/// tests, on the same reasoning and at the same scale as
/// [`CAPTURE_PROBE_WAIT`]. Each such wait covers a step an idle machine
/// reaches immediately, so the bound is a diagnostic rather than a timing
/// property: sizing it near the expected latency makes scheduler starvation
/// on a loaded machine indistinguishable from the regression it is meant to
/// catch.
pub(super) const DRIVER_PROGRESS_WAIT: Duration = Duration::from_secs(10);

pub(super) fn isolated_scope(id: &'static str, flavor: ScopeFlavor) -> Arc<ScopeCell> {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from(id);
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    ScopeCell::new(member, flavor, ScopeIdentity::new())
}

pub(super) fn running_dynamic_fixture() -> (
    ScopeRuntime,
    crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    Arc<DynamicControl>,
) {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, event_receiver) = crate::runtime::unbounded_mpsc();
    let (dynamic_events, dynamic_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(dynamic_events);
    let scope = ScopeRuntimeBuilder::new(root, epoch, events)
        .with_lifecycle(ScopeLifecycle::running())
        .with_dynamic(Some(control.clone()))
        .build();
    (scope, event_receiver, dynamic_event_receiver, control)
}

#[derive(Debug, Default)]
pub(super) struct FactoryGate {
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl FactoryGate {
    pub(super) fn block(&self) {
        self.entered.store(true, Ordering::Release);
        let mut released = self
            .released
            .lock()
            .expect("factory gate mutex remains healthy");
        self.changed.notify_all();
        while !*released {
            released = self
                .changed
                .wait(released)
                .expect("factory gate mutex remains healthy while blocked");
        }
    }

    pub(super) async fn wait_entered(&self) {
        while !self.entered.load(Ordering::Acquire) {
            crate::runtime::yield_now().await;
        }
    }

    pub(super) fn release(&self) {
        let mut released = self
            .released
            .lock()
            .expect("factory gate mutex remains healthy");
        *released = true;
        self.changed.notify_all();
    }
}

pub(super) fn pending_tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task("pending", TaskDef::new(|_| future::pending()))
        .expect("pending child is valid");
    tree
}

pub(super) fn finished_tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task(
        "finished",
        TaskDef::new(|_| async { Ok::<_, ExitError>(()) }),
    )
    .expect("finished child is valid");
    tree
}

/// A user error payload that records whether its destructor ran on the
/// retiring thread while the observation gate was held.
///
/// The lock rule's probe for `Exit`: an `ExitKind::Failed` carries a
/// type-erased application error, so wherever the cell layer destroys an exit
/// it is running caller code. The retiring thread identity plus
/// `ObservationGate::is_held` answers from inside that destructor without the
/// reentrant acquisition that would deadlock.
pub(super) struct GateProbeError {
    gate: crate::cells::ObservationGate,
    retiring_thread: std::thread::ThreadId,
    held_at_drop: Arc<Mutex<Option<bool>>>,
}

/// Builds a failed exit whose payload reports where it was destroyed.
pub(super) fn gate_probe_exit(scope: &Arc<ScopeCell>) -> (Exit, Arc<Mutex<Option<bool>>>) {
    let held_at_drop = Arc::new(Mutex::new(None));
    let exit = Exit::failed(
        ExitError::from(GateProbeError {
            gate: scope.observation_gate(),
            retiring_thread: std::thread::current().id(),
            held_at_drop: Arc::clone(&held_at_drop),
        }),
        Cancellation::NotObserved,
    );
    (exit, held_at_drop)
}

/// A dynamic root with one admitted, started child, plus the child's
/// incarnation counter — the shape a restart schedule needs.
pub(super) fn restarting_member_fixture() -> (Arc<ScopeCell>, Arc<MemberCell>, IncarnationCounter) {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.mint_membership(&child_id)
            .expect("child membership available"),
    );
    resolve_fixture_options(&member);
    let mut incarnations = member.take_incarnation_counter();
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let first = incarnations.mint().expect("incarnation available");
    member.transition(MemberTransition::Starting { incarnation: first });
    (root, member, incarnations)
}

pub(super) fn gate_probe_verdict(held_at_drop: &Arc<Mutex<Option<bool>>>) -> Option<bool> {
    *held_at_drop.lock().expect("gate probe mutex poisoned")
}

pub(super) fn wait_for_gate_probe(held_at_drop: &Arc<Mutex<Option<bool>>>) -> bool {
    let deadline = std::time::Instant::now() + CAPTURE_PROBE_WAIT;
    loop {
        if let Some(verdict) = gate_probe_verdict(held_at_drop) {
            return verdict;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for retained exit disposal"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

impl std::fmt::Debug for GateProbeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GateProbeError")
    }
}

impl std::fmt::Display for GateProbeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("gate probe")
    }
}

impl std::error::Error for GateProbeError {}

impl Drop for GateProbeError {
    fn drop(&mut self) {
        let ran_inline_under_gate =
            std::thread::current().id() == self.retiring_thread && self.gate.is_held();
        *self.held_at_drop.lock().expect("gate probe mutex poisoned") = Some(ran_inline_under_gate);
    }
}

/// A user error payload that reports the thread its destructor ran on.
///
/// The venue probe for a *losing* application error: the framework selects a
/// different verdict, so the loser is never published and its destruction
/// thread is the only observable it leaves behind.
pub(super) struct ThreadReportingError(std::sync::mpsc::SyncSender<std::thread::ThreadId>);

/// Builds an application error paired with the receiver of its drop thread.
pub(super) fn thread_reporting_error()
-> (ExitError, std::sync::mpsc::Receiver<std::thread::ThreadId>) {
    let (dropped, observed) = std::sync::mpsc::sync_channel(1);
    (ExitError::from(ThreadReportingError(dropped)), observed)
}

impl std::fmt::Debug for ThreadReportingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ThreadReportingError")
    }
}

impl std::fmt::Display for ThreadReportingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("thread reporting error")
    }
}

impl std::error::Error for ThreadReportingError {}

impl Drop for ThreadReportingError {
    fn drop(&mut self) {
        let _ = self.0.send(std::thread::current().id());
    }
}

pub(super) struct SnapshotReentryWake {
    scope: ScopeRef,
    observed: std::sync::mpsc::Sender<ScopeState>,
}

impl Wake for SnapshotReentryWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.observed.send(self.scope.snapshot().state.clone());
    }
}

pub(super) fn snapshot_reentry_waker(
    scope: &Arc<ScopeCell>,
) -> (Waker, std::sync::mpsc::Receiver<ScopeState>) {
    let (observed, receiver) = std::sync::mpsc::channel();
    (
        Waker::from(Arc::new(SnapshotReentryWake {
            scope: ScopeRef {
                cell: Arc::clone(scope),
            },
            observed,
        })),
        receiver,
    )
}

pub(super) struct PendingRaw;

impl crate::RawActor for PendingRaw {
    type Msg = u8;

    fn readiness() -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, _: &mut crate::RawContext<Self::Msg>) -> crate::ExitResult {
        future::pending().await
    }
}

pub(super) struct PanicWake(pub(super) &'static str);

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(self.0);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(self.0);
    }
}

pub(super) struct PanicDropProbe(pub(super) Arc<AtomicUsize>);

impl Drop for PanicDropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct CountedPanicWake(pub(super) Arc<AtomicUsize>);

impl Wake for CountedPanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(PanicDropProbe(Arc::clone(&self.0)));
    }

    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(PanicDropProbe(Arc::clone(&self.0)));
    }
}

pub(super) struct CountWake(pub(super) Arc<AtomicUsize>);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct ObserveWakeCount {
    pub(super) wakes: Arc<AtomicUsize>,
    pub(super) observed: Mutex<Option<usize>>,
}

impl Wake for ObserveWakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        *self.observed.lock().expect("observation mutex poisoned") =
            Some(self.wakes.load(Ordering::SeqCst));
    }
}

pub(super) struct ExitOnSignalRaw {
    pub(super) exit: Latch,
}

impl crate::RawActor for ExitOnSignalRaw {
    type Msg = u8;

    async fn run(&mut self, _: &mut crate::RawContext<Self::Msg>) -> crate::ExitResult {
        self.exit.fired().await;
        Ok(())
    }
}
