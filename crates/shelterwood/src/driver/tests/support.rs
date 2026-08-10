pub(super) use std::{
    future::{self, Future},
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
    GracePhase, Intensity, LifecycleEventKind, LifecycleItem, LifecycleTryRecvError, Mailbox,
    MembershipStatus, RawOnceDef, Readiness, ReadinessDeadline, RemoveOutcome, ReserveError,
    RestartCondition, RestartCount, RestartPolicy, Retention, ScopeRef, ScopeState, SendErrorKind,
    StartupError, StartupFailureCause, StopReason, SubtreeDef, SubtreeOnceDef, TaskDef, Tree,
    engine::{Epoch, ScopeLifecycle, StopLadder, arbitrate},
    exit::RecordedOutcome,
    identity::{IncarnationCounter, ScopeIdentity},
    mailbox::MailboxCell,
    plan::{ChildConstruction, SlotCell},
    policy::ResolvedDefaults,
    runtime::{CompletionGatedLatch, Latch},
};

pub(super) use super::super::{
    AncestorCommandLatches, ChildArena, ChildEvent, ChildKey, ChildRuntime, ChildTerminality,
    DriverEvent, DynamicControl, DynamicEntry, GateCapture, MemberCell, MemberStage,
    MemberTransition, NestedScopeLatches, Pending, RemovalRequest, RemovalResponses,
    ResidentProjection, RuntimeStorage, ScopeCell, ScopeEpochGuard, ScopeFlavor, ScopeRole,
    ScopeRuntime, StartupDisposition, cancel_dynamic_reservation, discharge_child_terminality,
    report_slot, reserve_dynamic, resident_projection, restart_shutdown_work, run_nested_factory,
    run_nested_tree, run_scope_incarnation, storage::Obligation,
};

pub(super) struct ScopeRuntimeBuilder {
    root: Arc<ScopeCell>,
    epoch: Epoch,
    events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    disposal_events: crate::runtime::UnboundedMpscSender<DriverEvent>,
    defaults: ResolvedDefaults,
    intensity_policy: Intensity,
    children: ChildArena<ChildRuntime>,
    lifecycle: ScopeLifecycle,
    next_ordered_start: Option<ChildKey>,
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
        self.next_ordered_start = next;
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
        ScopeRuntime {
            root: self.root,
            defaults: self.defaults,
            intensity_policy: self.intensity_policy,
            intensity: super::super::IntensityState::default(),
            children: self.children,
            events: self.events,
            disposal_events: self.disposal_events,
            deadlines: super::super::DeadlineQueue::default(),
            jitter: crate::runtime::JitterRng::from_system_entropy(),
            lifecycle: self.lifecycle,
            next_ordered_start: self.next_ordered_start,
            role: ScopeRole::Root,
            dynamic: self.dynamic,
            epoch: self.epoch,
            ancestor_shutdown_seen: false,
            ancestor_abort_seen: false,
            hard_forced: self.hard_forced,
            ordered_stop_progressing: false,
            ordered_stop_cursor: None,
            ordered_stop_waiting: None,
            ordered_stop_inspections: 0,
            completion: None,
        }
    }
}

/// Bounds every gate-capture probe wait. The probe sender lives inside
/// the scope cell for the whole test, so the channel can never
/// disconnect: a regression that keeps a thread from reaching its gate
/// must time out with a diagnostic rather than hang the test on `recv`.
pub(super) const CAPTURE_PROBE_WAIT: Duration = Duration::from_secs(10);

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
