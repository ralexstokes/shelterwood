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
    SendErrorKind, StartupError, StartupFailureCause, StopReason, SubtreeDef, SubtreeOnceDef,
    TaskDef, Tree,
    engine::{
        ChildKey, Effect as SupervisorEffect, Epoch, Event as SupervisorEvent, ScopeLifecycle,
        StopLadder, SupervisorState, arbitrate, step as supervisor_step,
    },
    exit::RecordedOutcome,
    identity::{IncarnationCounter, ScopeIdentity},
    mailbox::{MailboxCell, actor_ref_from_parts},
    plan::{BuilderCore, ChildConstruction, SlotCell, resolve_fixture_options},
    policy::ResolvedDefaults,
    runtime::{CompletionGatedLatch, Latch},
};

pub(super) struct ObservedExit {
    pub(super) child: ChildKey,
    incarnation: Incarnation,
    recorded: Option<RecordedOutcome>,
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

pub(super) async fn recv_child_exit(
    receiver: &mut crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    timeout: Duration,
    expectation: &str,
) -> ObservedExit {
    match crate::runtime::timeout(timeout, receiver.recv()).await {
        crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        }))) => ObservedExit {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        },
        crate::runtime::Timeout::Completed(_) => panic!("expected {expectation}"),
        crate::runtime::Timeout::Elapsed => panic!("timed out waiting for {expectation}"),
    }
}

pub(super) use super::super::{
    AdmissionRequest, AncestorCommandLatches, ChildEvent, ChildResources, ChildRuntime,
    ChildTerminality, DriverEvent, DynamicControl, DynamicEntry, DynamicReservation, GateCapture,
    MemberCell, MemberStage, MemberTransition, NestedScopeLatches, Pending, RemovalRequest,
    RemovalResponses, ResidentProjection, RuntimeStorage, ScopeCell, ScopeControlEvent,
    ScopeEpochGuard, ScopeFlavor, ScopeRole, ScopeRuntime, StartupDisposition,
    cancel_dynamic_reservation, discharge_child_terminality, report_slot, reserve_dynamic,
    resident_projection, restart_shutdown_work, run_nested_factory, run_nested_tree,
    run_scope_incarnation, storage::Obligation,
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
    pub(super) fn insert(&mut self, child: T) -> Result<ChildKey, Box<T>> {
        self.next += 1;
        let key = ChildKey::fixture(self.next);
        self.children.insert(key, child);
        Ok(key)
    }

    pub(super) fn keys(&self) -> impl DoubleEndedIterator<Item = ChildKey> + '_ {
        self.children.keys().copied()
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.children.values_mut()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (ChildKey, &T)> {
        self.children.iter().map(|(key, child)| (*key, child))
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
    epoch: Epoch,
    events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    disposal_events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    defaults: ResolvedDefaults,
    intensity_policy: Intensity,
    children: ChildArena<ChildRuntime>,
    lifecycle: ScopeLifecycle,
    next_ordered_start: Option<Option<ChildKey>>,
    dynamic: Option<Arc<DynamicControl>>,
    hard_forced: bool,
}

impl ScopeRuntimeBuilder {
    pub(super) fn new(
        root: Arc<ScopeCell>,
        epoch: Epoch,
        events: crate::runtime::UnboundedMpscSender<DriverEvent>,
        disposal_events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    ) -> Self {
        Self {
            root,
            epoch,
            events,
            disposal_events,
            defaults: ResolvedDefaults::default(),
            intensity_policy: Intensity::default(),
            children: ChildArena::default(),
            lifecycle: ScopeLifecycle::starting(),
            next_ordered_start: None,
            dynamic: None,
            hard_forced: false,
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

    pub(super) fn build(self) -> ScopeRuntime {
        let mut supervisor = SupervisorState::new(self.root.flavor, self.lifecycle);
        let mut supervisor_effects = Vec::new();
        let mut children = ChildResources::default();
        for (expected, child) in self.children.into_iter() {
            supervisor_step(
                &mut supervisor,
                SupervisorEvent::Admit {
                    membership: child.slot.member.membership(),
                    initial: true,
                    start_immediately: false,
                },
                &mut supervisor_effects,
            );
            let Some(SupervisorEffect::Admitted { child: actual }) = supervisor_effects.pop()
            else {
                panic!("fixture admission produces one key")
            };
            assert_eq!(actual, expected);
            children.insert(actual, child);
        }
        if let Some(next) = self.next_ordered_start {
            supervisor.set_next_ordered_start_for_test(next);
        }
        supervisor.set_hard_forced_for_test(self.hard_forced);
        ScopeRuntime {
            root: self.root,
            defaults: self.defaults,
            intensity_policy: self.intensity_policy,
            intensity: super::super::IntensityState::default(),
            restart_shutdown_retries: Vec::new(),
            children,
            supervisor,
            supervisor_effects,
            events: self.events,
            disposal_events: self.disposal_events,
            deadlines: super::super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::new(),
            role: ScopeRole::Root,
            dynamic: self.dynamic,
            pending_startup_removals: Vec::new(),
            epoch: self.epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            completion: None,
            finished: None,
        }
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
    crate::runtime::UnboundedMpscReceiver<DriverEvent>,
    Arc<DynamicControl>,
) {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = root
        .begin_incarnation(ScopeState::Starting)
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, event_receiver) = crate::runtime::unbounded_mpsc();
    let (dynamic_events, dynamic_event_receiver) = crate::runtime::unbounded_mpsc();
    let (disposal_events, disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(dynamic_events);
    root.set_dynamic_route(Some(control.clone()));
    root.set_admitted_children(Vec::new());
    let scope = ScopeRuntimeBuilder::new(root, epoch, events, disposal_events)
        .with_lifecycle(ScopeLifecycle::running())
        .with_dynamic(Some(control.clone()))
        .build();
    (
        scope,
        event_receiver,
        dynamic_event_receiver,
        disposal_event_receiver,
        control,
    )
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
