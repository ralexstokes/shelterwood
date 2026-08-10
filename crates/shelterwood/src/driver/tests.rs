use std::{
    future::{self, Future},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Barrier, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use crate::{
    ActorRef, Backoff, Cancellation, ChildId, ChildState, DynamicTree, Exit, ExitError, ExitKind,
    GracePhase, Intensity, LifecycleEventKind, LifecycleItem, LifecycleTryRecvError, Mailbox,
    RawOnceDef, Readiness, ReadinessDeadline, RemoveOutcome, ReserveError, RestartCondition,
    RestartPolicy, Retention, ScopeRef, ScopeState, SendErrorKind, StartupError,
    StartupFailureCause, StopReason, SubtreeDef, SubtreeOnceDef, TaskDef, Tree,
    engine::{Epoch, ScopeLifecycle, StopLadder, arbitrate},
    exit::{JoinVerdict, RecordedOutcome},
    identity::{IncarnationCounter, ScopeIdentity},
    mailbox::MailboxCell,
    plan::{ChildConstruction, SlotCell},
    policy::ResolvedDefaults,
    runtime::{CompletionGatedLatch, Latch},
};

use super::{
    AncestorCommandLatches, ChildArena, ChildEvent, ChildKey, ChildRuntime, ChildTerminality,
    DriverEvent, DynamicControl, DynamicEntry, GateCapture, MemberCell, MemberStage,
    NestedScopeLatches, Pending, RemovalRequest, RemovalResponses, ResidentProjection,
    RuntimeStorage, ScopeCell, ScopeEpochGuard, ScopeFlavor, ScopeRole, ScopeRuntime,
    StartupDisposition, cancel_dynamic_reservation, discharge_child_terminality, report_slot,
    reserve_dynamic, resident_projection, restart_shutdown_work, run_nested_factory,
    run_nested_tree, run_scope_incarnation, storage::Obligation,
};

/// Bounds every gate-capture probe wait. The probe sender lives inside
/// the scope cell for the whole test, so the channel can never
/// disconnect: a regression that keeps a thread from reaching its gate
/// must time out with a diagnostic rather than hang the test on `recv`.
const CAPTURE_PROBE_WAIT: Duration = Duration::from_secs(10);

fn isolated_scope(id: &'static str, flavor: ScopeFlavor) -> Arc<ScopeCell> {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from(id);
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    ScopeCell::new(member, flavor, ScopeIdentity::new())
}

#[derive(Debug, Default)]
struct FactoryGate {
    entered: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl FactoryGate {
    fn block(&self) {
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

    async fn wait_entered(&self) {
        while !self.entered.load(Ordering::Acquire) {
            crate::runtime::yield_now().await;
        }
    }

    fn release(&self) {
        let mut released = self
            .released
            .lock()
            .expect("factory gate mutex remains healthy");
        *released = true;
        self.changed.notify_all();
    }
}

fn pending_tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task("pending", TaskDef::new(|_| future::pending()))
        .expect("pending child is valid");
    tree
}

fn finished_tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task(
        "finished",
        TaskDef::new(|_| async { Ok::<_, ExitError>(()) }),
    )
    .expect("finished child is valid");
    tree
}

struct SnapshotReentryWake {
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

fn snapshot_reentry_waker(
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

struct PendingRaw;

impl crate::RawActor for PendingRaw {
    type Msg = u8;

    fn readiness(&self) -> Readiness {
        Readiness::Manual
    }

    async fn run(&mut self, _: &mut crate::RawContext<Self::Msg>) -> crate::ExitResult {
        future::pending().await
    }
}

struct PanicWake(&'static str);

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(self.0);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(self.0);
    }
}

struct PanicDropProbe(Arc<AtomicUsize>);

impl Drop for PanicDropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct CountedPanicWake(Arc<AtomicUsize>);

impl Wake for CountedPanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(PanicDropProbe(Arc::clone(&self.0)));
    }

    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(PanicDropProbe(Arc::clone(&self.0)));
    }
}

struct CountWake(Arc<AtomicUsize>);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ObserveWakeCount {
    wakes: Arc<AtomicUsize>,
    observed: Mutex<Option<usize>>,
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

struct ExitOnSignalRaw {
    exit: Latch,
}

impl crate::RawActor for ExitOnSignalRaw {
    type Msg = u8;

    async fn run(&mut self, _: &mut crate::RawContext<Self::Msg>) -> crate::ExitResult {
        self.exit.fired().await;
        Ok(())
    }
}

#[test]
fn independent_systems_do_not_share_an_observation_critical_section() {
    let first = isolated_scope("first", ScopeFlavor::Ordered);
    let second = isolated_scope("second", ScopeFlavor::Ordered);
    let first_gate = first.observation_gate();
    let second_gate = second.observation_gate();
    assert!(!first_gate.same_gate(&second_gate));

    let held = first_gate.lock();
    let (completed, receiver) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        second.set_state(ScopeState::Starting);
        completed.send(()).expect("test receiver remains available");
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(held);
    worker.join().expect("independent transition succeeds");
    assert_eq!(
        result,
        Ok(()),
        "holding one system's gate must not stall another system"
    );
}

#[test]
fn snapshot_subscription_waker_can_reenter_snapshot() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut snapshots = handle.subscribe_snapshots();
    let (waker, observed) = snapshot_reentry_waker(&scope);
    let mut changed = Box::pin(snapshots.changed());
    assert!(matches!(
        changed.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    let publisher = std::thread::spawn(move || scope.set_state(ScopeState::Starting));
    assert_eq!(
        observed.recv_timeout(Duration::from_secs(2)),
        Ok(ScopeState::Starting),
        "the watch waker must run only after snapshot can reacquire the gate"
    );
    publisher.join().expect("snapshot publication completes");
    assert!(matches!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn lifecycle_subscription_waker_can_reenter_snapshot() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut events = handle.subscribe_lifecycle();
    let (waker, observed) = snapshot_reentry_waker(&scope);
    let mut next = Box::pin(events.recv());
    assert!(matches!(
        next.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));

    let publisher = std::thread::spawn(move || scope.set_state(ScopeState::Starting));
    assert_eq!(
        observed.recv_timeout(Duration::from_secs(2)),
        Ok(ScopeState::Starting),
        "the lifecycle waker must run only after snapshot can reacquire the gate"
    );
    publisher.join().expect("lifecycle publication completes");
    assert!(matches!(
        next.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(_))
    ));
}

#[test]
fn scope_wait_waker_can_reenter_snapshot_at_terminality() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut stopped = Box::pin(handle.wait_stopped());
    let (stopped_waker, stopped_observed) = snapshot_reentry_waker(&scope);
    assert!(matches!(
        stopped
            .as_mut()
            .poll(&mut Context::from_waker(&stopped_waker)),
        Poll::Pending
    ));

    let terminalizer = std::thread::spawn(move || scope.terminalize_never_started());
    assert!(matches!(
        stopped_observed.recv_timeout(Duration::from_secs(2)),
        Ok(ScopeState::Stopped { .. })
    ));
    terminalizer.join().expect("terminal publication completes");
    assert!(matches!(
        stopped
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(_)
    ));
}

#[test]
fn observation_gate_poison_does_not_wedge_later_observation() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let gate = scope.observation_gate();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _held = gate.lock();
            panic!("inject observation failure");
        }))
        .is_err()
    );

    scope.set_state(ScopeState::Starting);
    assert_eq!(scope.record().state, ScopeState::Starting);
}

#[test]
fn stale_scope_driver_cannot_stop_a_newer_live_incarnation_projection() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let first = scope
        .begin_incarnation()
        .expect("first scope epoch is available");
    scope.finish_incarnation(first, StopReason::Finished);
    let second = scope
        .begin_incarnation()
        .expect("second scope epoch is available");
    scope.set_state(ScopeState::Running);

    scope.finish_incarnation(first, StopReason::Finished);
    assert_eq!(scope.record().state, ScopeState::Running);
    assert!(!scope.incarnation_finished(second));

    scope.finish_incarnation(second, StopReason::Finished);
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::Finished,
        }
    );
    assert!(scope.incarnation_finished(second));
}

#[crate::runtime::test]
async fn terminal_scope_waits_for_its_live_incarnation_to_stop() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);

    let epoch = nested
        .begin_incarnation()
        .expect("nested scope epoch is available");
    let mut incarnations = ScopeIdentity::new().incarnation_counter(nested.member.membership());
    let incarnation = incarnations.mint().expect("child incarnation is available");
    nested.member.update(|record| {
        record.stage = MemberStage::Running;
        record.incarnation = Some(incarnation);
        record.last_incarnation = Some(incarnation);
    });
    nested.set_state(ScopeState::Running);

    assert!(parent.terminalize_child(
        &nested.member,
        Exit::new(
            ExitKind::Aborted {
                phase: GracePhase::WithinGrace,
            },
            Cancellation::Observed,
        ),
        Some(incarnation),
        StartupDisposition::NotAborted,
    ));

    let mut waiter = Box::pin(nested.wait_stopped());
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    assert!(
        first_poll.is_pending(),
        "membership terminality does not imply that its live scope incarnation stopped"
    );

    nested.finish_incarnation(epoch, StopReason::ShutdownRequested);
    assert_eq!(waiter.await, StopReason::ShutdownRequested);
}

#[crate::runtime::test]
async fn terminality_fallback_preserves_restart_window_scope_reason() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);

    let mut incarnations = ScopeIdentity::new().incarnation_counter(nested.member.membership());
    let last_incarnation = incarnations.mint().expect("child incarnation is available");
    nested.member.update(|record| {
        record.stage = MemberStage::Restarting;
        record.incarnation = None;
        record.last_incarnation = Some(last_incarnation);
        record.last_exit = Some(Exit::new(ExitKind::Completed, Cancellation::NotObserved));
    });
    nested.set_state(ScopeState::Stopped {
        reason: StopReason::Finished,
    });
    let mut snapshots = nested.subscribe_snapshots();

    let mut terminality = Obligation::new(
        ChildTerminality {
            root: Arc::clone(&parent),
            slot,
        },
        discharge_child_terminality,
    );
    terminality.discharge();

    assert_eq!(nested.wait_stopped().await, StopReason::Finished);
    let MemberStage::Terminal(exit) = nested.member.record().stage else {
        panic!("the fallback must terminalize the nested membership");
    };
    assert!(matches!(
        exit.kind(),
        ExitKind::Aborted {
            phase: GracePhase::WithinGrace
        }
    ));
    assert_eq!(exit.cancellation(), Cancellation::Observed);
    assert_eq!(
        snapshots.borrow_latest().state,
        ScopeState::Stopped {
            reason: StopReason::Finished
        }
    );
    assert!(
        snapshots.changed().await.is_err(),
        "the fallback closes observation after retaining the final stopped snapshot"
    );
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_initial_scope_factory_owns_its_stop_epilogue() {
    let gate = Arc::new(FactoryGate::default());
    let mut tree = Tree::new();
    let nested = tree
        .add_subtree(
            "nested",
            SubtreeDef::factory({
                let gate = Arc::clone(&gate);
                move || {
                    gate.block();
                    pending_tree()
                }
            }),
        )
        .expect("nested scope is valid");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&root).expect("parent epoch is available");
    let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
    let abort = driver.abort_handle();

    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), gate.wait_entered()).await,
        crate::runtime::Timeout::Completed(())
    ));
    let factory_state = nested.snapshot().state.clone();

    abort.abort();
    let parent_join = crate::runtime::join(driver).await;
    let mut waiter = Box::pin(nested.wait_stopped());
    let before_release =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    gate.release();
    assert_eq!(factory_state, ScopeState::Starting);
    assert!(matches!(
        parent_join,
        crate::runtime::JoinOutcome::Cancelled
    ));
    assert!(
        before_release.is_pending(),
        "an executing initial factory still owns the final scope epilogue"
    );
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), waiter).await,
        crate::runtime::Timeout::Completed(StopReason::ShutdownRequested)
    ));
}

#[crate::runtime::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_restart_scope_factory_supersedes_the_stale_stopped_projection() {
    let gate = Arc::new(FactoryGate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut tree = Tree::new();
    let nested = tree
        .add_subtree(
            "nested",
            SubtreeDef::factory({
                let gate = Arc::clone(&gate);
                let calls = Arc::clone(&calls);
                move || {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        finished_tree()
                    } else {
                        gate.block();
                        pending_tree()
                    }
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Immediate,
            )),
        )
        .expect("restartable nested scope is valid");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&root).expect("parent epoch is available");
    let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
    let abort = driver.abort_handle();

    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), gate.wait_entered()).await,
        crate::runtime::Timeout::Completed(())
    ));
    let factory_calls = calls.load(Ordering::SeqCst);
    let factory_state = nested.snapshot().state.clone();

    abort.abort();
    let parent_join = crate::runtime::join(driver).await;
    let mut waiter = Box::pin(nested.wait_stopped());
    let before_release =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    gate.release();
    assert_eq!(factory_calls, 2);
    assert_eq!(
        factory_state,
        ScopeState::Starting,
        "the second epoch supersedes the first incarnation's Stopped projection before its factory runs"
    );
    assert!(matches!(
        parent_join,
        crate::runtime::JoinOutcome::Cancelled
    ));
    assert!(
        before_release.is_pending(),
        "an executing restart factory still owns the final scope epilogue"
    );
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(2), waiter).await,
        crate::runtime::Timeout::Completed(StopReason::ShutdownRequested)
    ));
}

#[crate::runtime::test]
async fn panicking_nested_factory_releases_its_pre_driver_epoch() {
    let scope = isolated_scope("nested", ScopeFlavor::Ordered);
    let driver_scope = Arc::clone(&scope);
    let driver = crate::runtime::spawn(async move {
        let factory = Arc::new(|| -> crate::plan::BuilderCore {
            panic!("injected nested factory panic");
        });
        run_nested_factory(
            factory,
            driver_scope,
            crate::policy::ResolvedDefaults::default(),
            NestedScopeLatches {
                parent_ready: CompletionGatedLatch::default(),
                ancestor: AncestorCommandLatches {
                    shutdown: Latch::default(),
                    abort: Latch::default(),
                    abort_ack: Latch::default(),
                },
            },
        )
        .await
    });

    assert!(matches!(
        crate::runtime::join(driver).await,
        crate::runtime::JoinOutcome::Panic { .. }
    ));
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested,
        }
    );
    let successor =
        ScopeEpochGuard::begin(&scope).expect("factory unwind retires the reserved scope epoch");
    successor.finish(StopReason::NeverStarted);
}

#[test]
fn pre_driver_epoch_guard_releases_on_cancellation_and_unwind() {
    let cancelled = isolated_scope("cancelled", ScopeFlavor::Ordered);
    let guard = ScopeEpochGuard::begin(&cancelled).expect("first epoch is available");
    let mut setup = Box::pin(async move {
        let _guard = guard;
        future::pending::<()>().await;
    });
    assert!(
        setup
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(setup);
    let successor =
        ScopeEpochGuard::begin(&cancelled).expect("cancelling pre-driver setup retires its epoch");
    successor.finish(StopReason::NeverStarted);

    let unwound = isolated_scope("unwound", ScopeFlavor::Ordered);
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = ScopeEpochGuard::begin(&unwound).expect("unwind epoch is available");
            panic!("injected pre-driver unwind");
        }))
        .is_err()
    );
    let successor =
        ScopeEpochGuard::begin(&unwound).expect("unwinding pre-driver setup retires its epoch");
    successor.finish(StopReason::NeverStarted);
}

#[test]
fn a_declined_epoch_still_publishes_its_owned_terminal_exit() {
    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let epoch = scope.begin_incarnation().expect("scope epoch is available");
    // The orderly finisher retires the epoch without a terminal exit, so
    // a second owner still holds the only membership verdict.
    scope.finish_incarnation(epoch, StopReason::Finished);
    assert!(!matches!(
        scope.member.record().stage,
        MemberStage::Terminal(_)
    ));

    scope.finish_root_incarnation(
        epoch,
        StopReason::ShutdownRequested,
        Exit::new(
            ExitKind::Aborted {
                phase: GracePhase::WithinGrace,
            },
            Cancellation::Observed,
        ),
    );
    assert!(matches!(
        scope.member.record().stage,
        MemberStage::Terminal(_)
    ));
    assert_eq!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::Finished,
        },
        "a declined epoch must not rewrite the retired stop reason"
    );
}

#[test]
fn admitted_subtrees_share_their_parent_observation_gate() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));

    root.set_admitted_children(vec![resident_projection(&slot)]);

    assert!(
        root.observation_gate()
            .same_gate(&nested.observation_gate())
    );
}

#[test]
fn admitted_subtree_rehomes_existing_descendants_to_one_gate() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let leaf = isolated_scope("leaf", ScopeFlavor::Ordered);
    let leaf_slot = SlotCell::new(Arc::clone(&leaf.member), Some(Arc::clone(&leaf)));
    nested.set_admitted_children(vec![resident_projection(&leaf_slot)]);
    assert!(
        nested
            .observation_gate()
            .same_gate(&leaf.observation_gate())
    );

    let nested_slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    root.set_admitted_children(vec![resident_projection(&nested_slot)]);

    let root_gate = root.observation_gate();
    assert!(root_gate.same_gate(&nested.observation_gate()));
    assert!(root_gate.same_gate(&leaf.observation_gate()));
}

#[test]
fn receiverless_config_state_is_atomic_under_concurrent_snapshots() {
    const UPDATES: usize = 2_000;

    let scope = isolated_scope("scope", ScopeFlavor::Ordered);
    let first = Intensity::new(1, Duration::from_secs(1)).expect("valid first intensity");
    let second = Intensity::new(2, Duration::from_secs(2)).expect("valid second intensity");
    scope.set_observation_config(Default::default(), first);

    let start = Arc::new(Barrier::new(2));
    let writer_scope = Arc::clone(&scope);
    let writer_start = Arc::clone(&start);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for update in 0..UPDATES {
            let intensity = if update % 2 == 0 { second } else { first };
            writer_scope.set_observation_config(Default::default(), intensity);
        }
    });

    start.wait();
    for _ in 0..UPDATES {
        let intensity = scope.snapshot().intensity;
        assert!(
            intensity == first || intensity == second,
            "a snapshot observes one complete configuration update"
        );
    }
    writer.join().expect("config writer completes");
}

#[test]
fn plain_resident_state_is_released_before_recursive_removed_publication() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let first = isolated_scope("first", ScopeFlavor::Dynamic);
    let second = isolated_scope("second", ScopeFlavor::Dynamic);
    let first_slot = SlotCell::new(Arc::clone(&first.member), Some(first));
    let second_slot = SlotCell::new(Arc::clone(&second.member), Some(second));
    let mut events = root.subscribe_lifecycle();
    let snapshots = root.subscribe_snapshots();

    root.set_admitted_children(vec![
        resident_projection(&first_slot),
        resident_projection(&second_slot),
    ]);
    root.clear_residents();

    assert!(root.resident_projections().is_empty());
    assert!(snapshots.borrow_latest().children.is_empty());
    let mut added = 0;
    let mut removed = 0;
    while let Ok(LifecycleItem::Event(event)) = events.try_recv() {
        match event.kind {
            LifecycleEventKind::Added { .. } => added += 1,
            LifecycleEventKind::Removed { .. } => removed += 1,
            _ => {}
        }
    }
    assert_eq!((added, removed), (2, 2));
}

#[test]
fn plain_parent_state_preserves_nested_snapshot_propagation() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let mut incarnations = IncarnationCounter::near_exhaustion(nested.member.membership());
    nested.member.update(|record| {
        record.incarnation = incarnations.mint();
        record.stage = MemberStage::Starting;
    });
    let nested_slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    root.set_admitted_children(vec![resident_projection(&nested_slot)]);
    let snapshots = root.subscribe_snapshots();
    let intensity = Intensity::new(7, Duration::from_secs(11)).expect("valid intensity");

    nested.set_observation_config(Default::default(), intensity);

    assert_eq!(
        snapshots
            .borrow_latest()
            .child("nested")
            .and_then(|child| child.nested.as_deref())
            .map(|snapshot| snapshot.intensity),
        Some(intensity)
    );
}

#[test]
fn pre_admission_observer_retries_after_gate_handoff() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let captures = nested.probe_gate_captures();
    let prior_gate = nested.observation_gate();
    let held = prior_gate.lock();
    let observer = Arc::clone(&nested);
    let worker = std::thread::spawn(move || observer.set_state(ScopeState::Starting));

    // The capture report proves the observer committed to the
    // pre-admission gate, which the held guard keeps it from acquiring.
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("the observer reports its capture within the bound"),
        GateCapture::Observation
    );

    // Model the instant at which adoption owns the old gate and publishes
    // the replacement. The waiting observer must acquire the old gate,
    // detect this handoff, and retry on the root gate.
    nested.replace_observation_gate(root.observation_gate());
    drop(held);
    worker.join().expect("observer follows the gate handoff");

    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("the observer reports its retry within the bound"),
        GateCapture::Observation,
        "the handoff forces one retry capture on the root gate"
    );
    assert_eq!(nested.record().state, ScopeState::Starting);
    assert!(
        root.observation_gate()
            .same_gate(&nested.observation_gate())
    );
}

#[crate::runtime::test]
async fn force_uses_the_stop_funnel_for_every_ordered_child() {
    let mut tree = Tree::new();
    let first = tree
        .add_raw("first", crate::RawDef::factory(|| PendingRaw))
        .expect("valid first actor");
    let second = tree
        .add_raw("second", crate::RawDef::factory(|| PendingRaw))
        .expect("valid second actor");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
    }
    let keys = children.keys().collect::<Vec<_>>();
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: super::IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::running(),
        next_ordered_start: None,
        role: ScopeRole::Root,
        dynamic: None,
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };
    plan.armed = false;
    drop(plan);

    for key in &keys {
        scope.spawn_child(*key);
    }
    let incarnations = keys
        .iter()
        .map(|key| {
            scope.children[*key]
                .active
                .as_ref()
                .expect("child is active")
                .incarnation
        })
        .collect::<Vec<_>>();

    scope.force_all();

    for key in &keys {
        let active = scope.children[*key]
            .active
            .as_ref()
            .expect("forced child remains active through the tidy beat");
        assert!(active.shutdown.is_fired(), "force sends cancellation");
        assert!(active.abort.is_fired(), "force immediately escalates");
    }
    assert_eq!(
        first.try_send(1).expect_err("first mailbox freezes").kind,
        SendErrorKind::NotRunning
    );
    assert_eq!(
        second.try_send(2).expect_err("second mailbox freezes").kind,
        SendErrorKind::NotRunning
    );

    // Model readiness messages that shared the driver's wake with force.
    // The force boundary disarmed both gates before either can publish a
    // late Running transition.
    for (key, incarnation) in keys.iter().zip(incarnations) {
        scope.handle_ready(*key, incarnation);
        assert!(matches!(
            scope.children[*key].slot.member.record().stage,
            MemberStage::Stopping
        ));
    }

    let deadlines = keys
        .iter()
        .map(|key| {
            scope.children[*key]
                .active
                .as_ref()
                .and_then(|active| active.ladder)
                .and_then(StopLadder::deadline)
                .expect("each ladder retains its tidy deadline")
        })
        .collect::<Vec<_>>();
    scope.force_all();
    for (key, deadline) in keys.iter().zip(deadlines) {
        assert_eq!(
            scope.children[*key]
                .active
                .as_ref()
                .and_then(|active| active.ladder)
                .and_then(StopLadder::deadline),
            Some(deadline),
            "repeated force cannot rewind or skip the ladder"
        );
    }
}

#[test]
fn forced_ordered_drain_advances_an_inactive_suffix_iteratively() {
    const CHILDREN: usize = 1_024;

    let mut tree = Tree::new();
    for index in 0..CHILDREN {
        tree.add_task(
            format!("inactive-{index}"),
            TaskDef::new(|_| future::pending()),
        )
        .expect("unique child declaration");
    }
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the fixture fits in the child-key domain"));
    }
    // Model restart-window children: no incarnation and no retained
    // construction remains, so forced terminalization completes inline.
    // This used to re-enter `stop_next_ordered` once per child.
    for child in children.values_mut() {
        drop(child.construction.take());
    }
    let mut scope = ScopeRuntime {
        root,
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: super::IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::running(),
        next_ordered_start: None,
        role: ScopeRole::Root,
        dynamic: None,
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: true,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };
    plan.armed = false;
    drop(plan);

    scope.begin_drain(StopReason::ShutdownRequested);

    assert!(scope.children.values().all(ChildRuntime::is_terminal));
    assert!(!scope.ordered_stop_progressing);
    assert_eq!(scope.ordered_stop_waiting, None);
    assert_eq!(
        scope.ordered_stop_inspections, CHILDREN,
        "the reverse cursor inspects each ordered child exactly once"
    );
}

#[crate::runtime::test]
async fn same_batch_intensity_exit_suppresses_real_expedited_factory() {
    let factories = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tree = Tree::new();
    tree.intensity(Intensity::new(0, Duration::from_secs(10)).expect("valid intensity"));
    tree.add_subtree(
        "nested",
        SubtreeDef::factory({
            let factories = Arc::clone(&factories);
            move || {
                factories.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Tree::new()
            }
        }),
    )
    .expect("valid subtree");
    tree.add_task("trip", TaskDef::new(|_| future::pending()))
        .expect("valid task");

    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let mut children = ChildArena::default();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        children
            .insert(ChildRuntime::from_plan(child, &root))
            .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    }
    let nested = children
        .keys()
        .find(|key| children[*key].slot.member.id().as_str() == "nested")
        .expect("nested child key");
    let trip = children
        .keys()
        .find(|key| children[*key].slot.member.id().as_str() == "trip")
        .expect("tripping child key");
    let next_ordered_start = children.keys().next();
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: super::IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::running(),
        next_ordered_start,
        role: ScopeRole::Root,
        dynamic: None,
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };
    plan.armed = false;
    drop(plan);

    root.transition_child(
        &scope.children[nested].slot.member,
        |record| {
            record.incarnation = None;
            record.stage = MemberStage::Restarting;
        },
        None,
    );
    let _ = scope.children[nested]
        .slot
        .scope
        .as_ref()
        .expect("nested scope cell")
        .request_shutdown();
    assert_eq!(scope.pending_restart_shutdowns(), vec![nested]);

    scope.spawn_child(trip);
    let incarnation = scope.children[trip]
        .active
        .as_ref()
        .expect("tripping child is active")
        .incarnation;
    scope.children[trip]
        .active
        .as_ref()
        .expect("tripping child is active")
        .abort_handle
        .abort();
    let exit = DriverEvent::Child(ChildEvent::Exited {
        child: trip,
        incarnation,
        recorded: Some(RecordedOutcome::returned(Err(ExitError::message(
            "trip intensity",
        )))),
        join: JoinVerdict::Completed,
        cancellation: Cancellation::NotObserved,
        readiness_signal_seen: false,
    });
    let mut pending = [
        restart_shutdown_work(nested),
        Pending::Driver(exit).classified(),
    ];
    arbitrate(&mut pending);
    for (_, event) in pending {
        match event {
            Pending::RestartShutdown(child) => scope.expedite_restart_shutdown(child),
            Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            })) => scope.handle_exit(
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            ),
            _ => unreachable!("the fixture queues only exit and restart work"),
        }
    }

    crate::runtime::yield_now().await;
    crate::runtime::yield_now().await;

    assert!(scope.lifecycle.is_draining());
    assert_eq!(
        factories.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the production guard must suppress the expedited factory after intensity drain"
    );
}

/// A `mark_ready(); stop()` child reports its local stop and exit on
/// helper tasks, so one driver wake can collect both while the fired
/// readiness latch's Ready event is still undrained — and arbitration
/// orders the stop ahead of the readiness signal. `handle_self_stop`
/// must consult the fired latch before `begin_stop_child`'s Shutdown
/// step disarms the gate, or the clean post-ready exit is misread as a
/// pre-ready stop and spuriously aborts startup.
#[crate::runtime::test]
async fn same_batch_self_stop_preserves_fired_readiness_for_startup() {
    let mut tree = Tree::new();
    tree.add_task(
        "ready-then-stop",
        TaskDef::new(|_| future::pending::<crate::ExitResult>())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid")
            .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid task");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: super::IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::starting(),
        next_ordered_start: Some(key),
        role: ScopeRole::Root,
        dynamic: None,
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };
    plan.armed = false;
    drop(plan);

    scope.spawn_child(key);
    let active = scope.children[key]
        .active
        .as_ref()
        .expect("spawned child is active");
    let incarnation = active.incarnation;
    // The application task fired its readiness latch before stopping;
    // the driver has not yet drained the corresponding Ready event.
    assert!(active.ready_signal.fire());
    active.abort_handle.abort();

    let mut pending = [
        Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
            child: key,
            incarnation,
            recorded: Some(RecordedOutcome::returned(Ok(()))),
            join: JoinVerdict::Completed,
            cancellation: Cancellation::NotObserved,
            readiness_signal_seen: true,
        }))
        .classified(),
        Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop {
            child: key,
            incarnation,
        }))
        .classified(),
    ];
    arbitrate(&mut pending);
    assert!(
        matches!(
            pending[0].1,
            Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop { .. }))
        ),
        "the regression premise: arbitration orders the stop ahead of the exit"
    );
    for (_, event) in pending {
        match event {
            Pending::Driver(DriverEvent::Child(ChildEvent::SelfStop { child, incarnation })) => {
                scope.handle_self_stop(child, incarnation)
            }
            Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            })) => scope.handle_exit(
                child,
                incarnation,
                recorded,
                join,
                cancellation,
                readiness_signal_seen,
            ),
            _ => unreachable!("the fixture queues only the stop and the exit"),
        }
    }

    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
        disposal_event_receiver
            .recv()
            .await
            .expect("disposal reports completion")
    else {
        panic!("only construction disposal was armed")
    };
    scope.handle_construction_disposed(child, panic);

    assert!(
        scope.lifecycle.startup_complete(),
        "the ready-before-stop child completes startup"
    );
    assert!(
        matches!(root.record().startup, Some(Ok(()))),
        "a fired readiness latch must survive a same-batch local stop: {:?}",
        root.record().startup
    );
    assert_eq!(root.record().state, ScopeState::Running);
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::Completed)
    ));
    assert!(
        !scope.children[key].slot.member.record().startup_aborted,
        "a post-ready clean self-stop is not a startup abort"
    );
}

#[test]
fn gate_handoff_waits_for_an_in_flight_observation_edge() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Dynamic);
    let captures = nested.probe_gate_captures();
    let (entered, entered_receiver) = std::sync::mpsc::sync_channel(0);
    let (release, release_receiver) = std::sync::mpsc::sync_channel(0);
    let observer = Arc::clone(&nested);
    let observation = std::thread::spawn(move || {
        observer.with_observation_gate(|_| {
            entered.send(()).expect("test receiver remains available");
            release_receiver
                .recv()
                .expect("test sender releases the observation edge");
        });
    });
    entered_receiver
        .recv()
        .expect("observer enters the pre-admission edge");
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("the observation edge reports its capture within the bound"),
        GateCapture::Observation
    );

    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    let adopting_root = Arc::clone(&root);
    let (adopted, adopted_receiver) = std::sync::mpsc::sync_channel(0);
    let adoption = std::thread::spawn(move || {
        adopting_root.set_admitted_children(vec![resident_projection(&slot)]);
        adopted.send(()).expect("test receiver remains available");
    });

    // The adoption capture proves handoff committed to the prior gate and
    // is blocked behind the complete observation edge rather than
    // replacing it concurrently, so adoption cannot yet have completed.
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("adoption reports its capture within the bound"),
        GateCapture::Adoption
    );
    assert!(matches!(
        adopted_receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    release
        .send(())
        .expect("active observation remains available");
    observation.join().expect("observation edge completes");
    adopted_receiver
        .recv()
        .expect("adoption reports completion after the edge");
    adoption.join().expect("gate handoff completes");
    assert!(
        root.observation_gate()
            .same_gate(&nested.observation_gate())
    );
}

#[crate::runtime::test(start_paused = true)]
async fn dynamic_high_cycle_add_remove_keeps_only_live_runtime_storage() {
    const CYCLES: usize = 1_000;

    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let cell = Arc::clone(&scope.as_scope().cell);

    for cycle in 0..CYCLES {
        let task = scope
            .add_task(
                "worker",
                TaskDef::new(|_| future::pending())
                    .readiness(Readiness::Manual)
                    .expect("manual readiness is valid")
                    .readiness_deadline(
                        ReadinessDeadline::bounded(Duration::from_secs(60 * 60))
                            .expect("non-zero readiness deadline"),
                    )
                    .shutdown(crate::Shutdown::Abort),
            )
            .await
            .expect("task admission")
            .into_handles();
        assert_eq!(
            cell.runtime_storage(),
            RuntimeStorage {
                children: 1,
                child_slots: 1,
                deadlines: 1,
                deadline_slots: 1,
            },
            "cycle {cycle} stores only the live child"
        );

        assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
        assert_eq!(
            cell.runtime_storage(),
            RuntimeStorage {
                children: 0,
                child_slots: 0,
                deadlines: 0,
                deadline_slots: 0,
            },
            "cycle {cycle} must release removed-child storage"
        );

        let automatic = scope
            .add_task(
                "worker",
                TaskDef::new(|_| async { Ok(()) }).retention(Retention::Remove),
            )
            .await
            .expect("auto-removing task admission")
            .into_handles();
        automatic.wait().await;
        assert_eq!(
            cell.runtime_storage(),
            RuntimeStorage {
                children: 0,
                child_slots: 0,
                deadlines: 0,
                deadline_slots: 0,
            },
            "cycle {cycle} must release Retention::Remove storage"
        );
    }

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("empty dynamic scope shuts down");
}

#[test]
fn owned_report_token_consumes_or_falls_back_once() {
    let shutdown = Latch::default();
    let readiness = CompletionGatedLatch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, readiness.clone());
    token.record(RecordedOutcome::returned(Ok(())));
    shutdown.fire();
    readiness.fire();
    let report = claim.receive();
    assert!(matches!(
        report.outcome,
        Some(outcome) if matches!(outcome.kind(), ExitKind::Completed)
    ));
    assert_eq!(report.cancellation, Cancellation::NotObserved);
    assert!(!report.readiness_signal_seen);

    let shutdown = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    shutdown.fire();
    drop(token);
    let report = claim.receive();
    assert!(report.outcome.is_none());
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[test]
fn owned_report_token_records_prior_cancellation() {
    let shutdown = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    shutdown.fire();
    token.record(RecordedOutcome::returned(Ok(())));
    let report = claim.receive();
    assert!(matches!(
        report.outcome,
        Some(outcome) if matches!(outcome.kind(), ExitKind::Completed)
    ));
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[test]
fn owned_report_token_records_prior_local_stop() {
    let shutdown = Latch::default();
    let local_stop = Latch::default();
    let (token, claim) = report_slot(
        shutdown,
        Some(local_stop.clone()),
        CompletionGatedLatch::default(),
    );
    local_stop.fire();
    token.record(RecordedOutcome::returned(Ok(())));
    assert_eq!(claim.receive().cancellation, Cancellation::Observed);
}

#[test]
fn report_cell_falls_back_while_its_owner_thread_unwinds() {
    let shutdown = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    let worker = std::thread::spawn(move || {
        let _token = token;
        shutdown.fire();
        panic!("inject child-task panic");
    });

    assert!(worker.join().is_err());
    let report = claim.receive();
    assert!(report.outcome.is_none());
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[crate::runtime::test]
async fn cancelled_task_report_cell_is_ready_after_join() {
    let shutdown = Latch::default();
    let entered = Latch::default();
    let (token, claim) = report_slot(shutdown.clone(), None, CompletionGatedLatch::default());
    let task_entered = entered.clone();
    let task = crate::runtime::spawn(async move {
        let _token = token;
        task_entered.fire();
        future::pending::<()>().await;
    });
    entered.fired().await;

    shutdown.fire();
    task.abort_handle().abort();
    assert!(matches!(
        crate::runtime::join(task).await,
        crate::runtime::JoinOutcome::Cancelled
    ));

    let report = claim.receive();
    assert!(report.outcome.is_none());
    assert_eq!(report.cancellation, Cancellation::Observed);
}

#[test]
fn owned_report_token_records_readiness_at_completion() {
    let readiness = CompletionGatedLatch::default();
    let (token, claim) = report_slot(Latch::default(), None, readiness.clone());
    readiness.fire();
    token.record(RecordedOutcome::returned(Ok(())));
    assert!(!readiness.fire(), "completion closes retained capabilities");
    assert!(claim.receive().readiness_signal_seen);
}

#[test]
fn handle_identity_is_stable_across_membership_rebase() {
    fn hashed(value: &impl std::hash::Hash) -> u64 {
        use std::hash::Hasher;
        let mut hasher = std::hash::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox: Arc<MailboxCell<u8>> = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), mailbox);
    let peer = actor.clone();
    let task = crate::TaskRef::new(Arc::clone(&member));
    let declared = actor.membership();
    let actor_hash = hashed(&actor);
    let task_hash = hashed(&task);

    member.rebase_membership(
        identity
            .mint_membership(&id)
            .expect("successor membership available"),
    );

    assert!(actor.membership().supersedes(declared));
    assert_eq!(actor, peer);
    assert_eq!(hashed(&actor), actor_hash);
    assert_eq!(hashed(&task), task_hash);
}

#[crate::runtime::test]
async fn attaching_after_terminality_closes_the_mailbox() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    let mut parked = Box::pin(actor.send(1));
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(parked.as_mut().poll(context))).await;
    assert!(first_poll.is_pending());

    member.terminalize(Exit::never_started());
    member.attach_mailbox(mailbox);

    let parked = match crate::runtime::timeout(Duration::from_secs(1), parked).await {
        crate::runtime::Timeout::Completed(result) => {
            result.expect_err("parked send is terminated")
        }
        crate::runtime::Timeout::Elapsed => panic!("parked send must not remain pending"),
    };
    assert_eq!(parked.kind, SendErrorKind::Terminated);
    let immediate = actor.try_send(2).expect_err("terminal send is rejected");
    assert_eq!(immediate.kind, SendErrorKind::Terminated);
}

#[crate::runtime::test]
async fn task_aborted_scope_driver_resolves_startup() {
    let mut tree = Tree::new();
    tree.add_task(
        "not-ready",
        TaskDef::new(|_| future::pending())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid"),
    )
    .expect("valid task");
    let plan = tree.lower_for_test();
    let scope = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&scope).expect("test scope epoch is available");
    let driver = crate::runtime::spawn(run_scope_incarnation(
        plan,
        ScopeRole::Nested(NestedScopeLatches {
            parent_ready: CompletionGatedLatch::default(),
            ancestor: AncestorCommandLatches {
                shutdown: Latch::default(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        }),
        epoch,
    ));
    let abort = driver.abort_handle();
    let reached_startup = crate::runtime::timeout(Duration::from_secs(1), async {
        while !matches!(scope.record().state, ScopeState::Starting) {
            crate::runtime::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        reached_startup,
        crate::runtime::Timeout::Completed(())
    ));
    let waiter_scope = Arc::clone(&scope);
    let mut waiter = Box::pin(waiter_scope.wait_started());
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    assert!(first_poll.is_pending());

    abort.abort();
    assert!(matches!(
        crate::runtime::join(driver).await,
        crate::runtime::JoinOutcome::Cancelled
    ));
    let result = crate::runtime::timeout(Duration::from_secs(1), waiter).await;
    assert!(matches!(
        result,
        crate::runtime::Timeout::Completed(Err(crate::StartupError::ShutdownRequested))
    ));
    assert!(matches!(scope.record().state, ScopeState::Stopped { .. }));
    assert!(scope.record().startup.is_some());
}

#[crate::runtime::test]
async fn dropped_unpolled_scope_plan_terminalizes_its_root() {
    let plan = Tree::new().lower_for_test();
    let root = Arc::clone(&plan.root);

    drop(plan);

    assert_eq!(
        root.wait_started().await,
        Err(crate::StartupError::ShutdownRequested)
    );
    assert_eq!(root.wait_stopped().await, StopReason::NeverStarted);
}

#[crate::runtime::test]
async fn dropped_unpolled_scope_plan_terminalizes_nested_declarations() {
    let mut inner = Tree::new();
    let leaf = inner
        .add_task("leaf", TaskDef::new(|_| future::pending()))
        .expect("valid nested task");
    let mut outer = Tree::new();
    let nested = outer
        .add_subtree_once("nested", SubtreeOnceDef::new(inner))
        .expect("valid nested scope");
    let mut snapshots = nested.subscribe_snapshots();
    let mut events = nested.subscribe_lifecycle();
    let plan = outer.lower_for_test();

    drop(plan);

    assert_eq!(nested.wait_stopped().await, StopReason::NeverStarted);
    snapshots
        .changed()
        .await
        .expect("the final nested snapshot is delivered before closure");
    assert!(matches!(
        snapshots.borrow_latest().state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    ));
    assert!(snapshots.changed().await.is_err());
    assert!(matches!(leaf.wait().await.kind(), ExitKind::NeverStarted));
    let mut saw_stopped = false;
    while let Some(item) = events.recv().await {
        saw_stopped |= matches!(
            item,
            LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::Stopped {
                        reason: StopReason::NeverStarted
                    }
                },
                ..
            })
        );
    }
    assert!(
        saw_stopped,
        "nested observation closes after its final event"
    );
}

#[crate::runtime::test]
async fn scope_plan_conversion_panic_terminalizes_every_child() {
    let mut tree = Tree::new();
    for id in ["first", "second"] {
        tree.add_task(id, TaskDef::new(|_| future::pending()))
            .expect("valid task");
    }
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let mut events = root.subscribe_lifecycle();
    let children: Vec<_> = plan
        .children
        .iter()
        .map(|child| Arc::clone(&child.slot.member))
        .collect();
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _identity = root
                .child_identity
                .lock()
                .expect("scope identity mutex starts healthy");
            panic!("inject child conversion failure");
        }))
        .is_err()
    );

    let mut driver = Box::pin(run_scope_incarnation(
        plan,
        ScopeRole::Nested(NestedScopeLatches {
            parent_ready: CompletionGatedLatch::default(),
            ancestor: AncestorCommandLatches {
                shutdown: Latch::default(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        }),
        epoch,
    ));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let mut context = Context::from_waker(Waker::noop());
            let _ = driver.as_mut().poll(&mut context);
        }))
        .is_err()
    );
    drop(driver);

    for child in children {
        assert!(
            matches!(child.record().stage, MemberStage::Terminal(_)),
            "every transferred or pending child must terminalize"
        );
    }
    assert_eq!(
        root.wait_started().await,
        Err(crate::StartupError::ShutdownRequested)
    );
    assert_eq!(root.wait_stopped().await, StopReason::NeverStarted);
    assert!(
        root.snapshot().children.is_empty(),
        "the fallback must release every admitted residency"
    );
    let mut removed = 0;
    while let Some(item) = events.recv().await {
        if matches!(
            item,
            LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::Removed { .. },
                ..
            })
        ) {
            removed += 1;
        }
    }
    assert_eq!(
        removed, 0,
        "conversion must finish before publication begins"
    );
}

struct ReserveOnLifecycleWake {
    scope: Arc<ScopeCell>,
    result: Mutex<Option<Result<(), ReserveError>>>,
    observed: Latch,
}

impl ReserveOnLifecycleWake {
    fn observe(&self) {
        let mut result = self.result.lock().expect("observation mutex poisoned");
        if result.is_none() {
            *result = Some(
                reserve_dynamic(&self.scope, ChildId::from("reentrant"), None).map(|reservation| {
                    cancel_dynamic_reservation(reservation.control.as_ref(), &reservation.slot);
                }),
            );
            self.observed.fire();
        }
    }
}

impl Wake for ReserveOnLifecycleWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

#[crate::runtime::test]
async fn initial_added_wake_observes_the_keyed_dynamic_route() {
    let mut tree = DynamicTree::new();
    tree.add_task("initial", TaskDef::new(|_| future::pending()))
        .expect("valid task");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let mut events = root.subscribe_lifecycle();
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    assert!(events.recv().await.is_some(), "Starting is observed first");

    let probe = Arc::new(ReserveOnLifecycleWake {
        scope: Arc::clone(&root),
        result: Mutex::new(None),
        observed: Latch::default(),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut added = Box::pin(events.recv());
    assert!(
        added
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
    let abort = driver.abort_handle();
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(1), probe.observed.fired()).await,
        crate::runtime::Timeout::Completed(())
    ));
    drop(added);
    assert!(matches!(
        probe
            .result
            .lock()
            .expect("observation mutex poisoned")
            .as_ref(),
        Some(Ok(()))
    ));
    abort.abort();
    assert!(matches!(
        crate::runtime::join(driver).await,
        crate::runtime::JoinOutcome::Cancelled
    ));
}

struct TrySendOnWake {
    actor: ActorRef<u8>,
    observed: Mutex<Option<SendErrorKind>>,
}

impl TrySendOnWake {
    fn observe(&self) {
        let error = self
            .actor
            .try_send(1)
            .expect_err("a terminality-derived wake observes a closed mailbox");
        *self.observed.lock().expect("observation mutex poisoned") = Some(error.kind);
    }
}

impl Wake for TrySendOnWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

struct ObserveMemberOnMailboxWake {
    member: Arc<MemberCell>,
    competing_exit: Exit,
    observed: Mutex<Option<(MemberStage, MemberStage)>>,
}

impl ObserveMemberOnMailboxWake {
    fn observe(&self) {
        let before = self.member.record().stage;
        self.member.terminalize(self.competing_exit.clone());
        let after = self.member.record().stage;
        *self.observed.lock().expect("observation mutex poisoned") = Some((before, after));
    }
}

impl Wake for ObserveMemberOnMailboxWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

struct ObserveScopeOnStartupWake {
    scope: Arc<ScopeCell>,
    epoch: Option<Epoch>,
    observed: Mutex<Option<(MemberStage, Option<bool>)>>,
}

impl ObserveScopeOnStartupWake {
    fn observe(&self) {
        let member = self.scope.member.record().stage;
        let incarnation_finished = self
            .epoch
            .map(|epoch| self.scope.incarnation_finished(epoch));
        *self.observed.lock().expect("observation mutex poisoned") =
            Some((member, incarnation_finished));
    }
}

#[derive(Clone, Copy, Debug)]
enum TerminalStopPath {
    LiveEpoch,
    NoLiveEpoch,
    NeverStarted,
}

#[crate::runtime::test]
async fn terminal_stop_paths_share_one_complete_observation_transition() {
    for (path, reason) in [
        (TerminalStopPath::LiveEpoch, StopReason::ShutdownRequested),
        (TerminalStopPath::NoLiveEpoch, StopReason::ShutdownRequested),
        (TerminalStopPath::NeverStarted, StopReason::NeverStarted),
    ] {
        let scope = isolated_scope("root", ScopeFlavor::Ordered);
        let epoch = matches!(path, TerminalStopPath::LiveEpoch).then(|| {
            scope
                .begin_incarnation()
                .expect("test scope epoch is available")
        });
        let handle = ScopeRef {
            cell: Arc::clone(&scope),
        };
        let mut snapshots = handle.subscribe_snapshots();
        let mut events = handle.subscribe_lifecycle();

        match path {
            TerminalStopPath::LiveEpoch => scope.finish_root_incarnation(
                epoch.expect("live path owns an epoch"),
                reason.clone(),
                Exit::never_started(),
            ),
            TerminalStopPath::NoLiveEpoch => {
                scope.finish_live_root_incarnation(reason.clone(), Exit::never_started())
            }
            TerminalStopPath::NeverStarted => scope.terminalize_never_started(),
        }

        let expected_state = ScopeState::Stopped {
            reason: reason.clone(),
        };
        assert!(matches!(
            scope.member.record().stage,
            MemberStage::Terminal(_)
        ));
        assert_eq!(scope.record().state, expected_state);
        assert_eq!(
            epoch.map(|epoch| scope.incarnation_finished(epoch)),
            epoch.map(|_| true)
        );
        assert_eq!(snapshots.borrow_latest().state, expected_state);
        assert!(matches!(
            events.try_recv(),
            Ok(LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState { state },
                ..
            })) if state == expected_state
        ));
        assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));

        snapshots
            .changed()
            .await
            .expect("the final snapshot precedes observation closure");
        assert!(snapshots.changed().await.is_err());
    }
}

impl Wake for ObserveScopeOnStartupWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

#[test]
fn stopped_publication_keeps_mailbox_panic_primary_and_finishes_observation() {
    let scope = isolated_scope("root", ScopeFlavor::Ordered);
    let epoch = scope
        .begin_incarnation()
        .expect("test scope epoch is available");
    let handle = ScopeRef {
        cell: Arc::clone(&scope),
    };
    let mut events = handle.subscribe_lifecycle();
    let mailbox = MailboxCell::new(scope.member.id().clone());
    let actor: ActorRef<u8> = ActorRef::new(Arc::clone(&scope.member), Arc::clone(&mailbox));
    scope.member.attach_mailbox(mailbox);

    let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &mailbox_payload_dropped,
    ))));
    let mut parked_send = Box::pin(actor.send(1));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let terminal_payload_dropped = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &terminal_payload_dropped,
    ))));
    let mut terminal = Box::pin(scope.member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    let payload = catch_unwind(AssertUnwindSafe(|| {
        scope.finish_root_incarnation(epoch, StopReason::ShutdownRequested, Exit::never_started());
    }))
    .expect_err("the primary mailbox panic still surfaces");
    assert_eq!(
        mailbox_payload_dropped.load(Ordering::SeqCst),
        0,
        "the mailbox panic payload remains owned by the caller"
    );
    assert!(
        terminal_payload_dropped.load(Ordering::SeqCst) > 0,
        "later member-pulse panics are contained as cleanup"
    );
    assert!(matches!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        }
    ));
    assert!(scope.incarnation_finished(epoch));
    assert!(matches!(
        events.try_recv(),
        Ok(LifecycleItem::Event(crate::LifecycleEvent {
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped {
                    reason: StopReason::ShutdownRequested
                }
            },
            ..
        }))
    ));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
    assert!(matches!(
        scope.member.record().stage,
        MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    drop(payload);
    assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn panicking_mailbox_waker_cannot_skip_the_terminal_pulse() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mut parked_send = Box::pin(actor.send(1));
    let panicking_waker = Waker::from(Arc::new(PanicWake("injected mailbox waker panic")));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&panicking_waker))
            .is_pending()
    );

    let wakes = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    let mut terminal = Box::pin(member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    catch_unwind(AssertUnwindSafe(|| {
        member.terminalize(Exit::never_started());
    }))
    .expect_err("the hostile mailbox waker still surfaces its panic");
    assert_eq!(
        wakes.load(Ordering::SeqCst),
        1,
        "membership terminality is pulsed before the mailbox panic resumes"
    );
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[test]
fn mailbox_teardown_panic_precedes_a_terminal_pulse_panic() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mut parked_send = Box::pin(actor.send(1));
    let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &mailbox_payload_dropped,
    ))));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let mut terminal = Box::pin(member.wait_terminal());
    let pulse_payload_dropped = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &pulse_payload_dropped,
    ))));
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    let payload = catch_unwind(AssertUnwindSafe(|| {
        member.terminalize(Exit::never_started());
    }))
    .expect_err("the primary mailbox panic still surfaces");
    assert_eq!(
        mailbox_payload_dropped.load(Ordering::SeqCst),
        0,
        "the primary mailbox payload is retained for the caller"
    );
    assert_eq!(
        pulse_payload_dropped.load(Ordering::SeqCst),
        1,
        "the membership-pulse panic is cleanup and is contained"
    );
    drop(payload);
    assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[crate::runtime::test]
async fn mailbox_waker_panic_is_contained_without_wedging_system_completion() {
    let exit = Latch::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "worker",
            RawOnceDef::new(ExitOnSignalRaw { exit: exit.clone() })
                .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let plan = tree.lower_for_test();
    let member = Arc::clone(&plan.children[0].slot.member);
    let root = Arc::clone(&plan.root);
    let mut events = root.subscribe_lifecycle();
    let mut system = super::spawn_system(plan);
    root.wait_started().await.expect("actor starts");
    actor
        .try_send(1)
        .expect("the first message fills the queue");

    let mut parked_send = Box::pin(actor.send(2));
    let panicking_waker = Waker::from(Arc::new(PanicWake("injected mailbox waker panic")));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&panicking_waker))
            .is_pending()
    );

    let waiter_started = Latch::default();
    let terminal_waiter = crate::runtime::spawn({
        let member = Arc::clone(&member);
        let waiter_started = waiter_started.clone();
        async move {
            waiter_started.fire();
            member.wait_terminal().await
        }
    });
    waiter_started.fired().await;
    exit.fire();

    let member_exit = match crate::runtime::timeout(
        Duration::from_secs(2),
        crate::runtime::join(terminal_waiter),
    )
    .await
    {
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Ok { value }) => value,
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Panic { message }) => {
            panic!("the terminal waiter panicked: {message:?}")
        }
        crate::runtime::Timeout::Completed(crate::runtime::JoinOutcome::Cancelled) => {
            panic!("the terminal waiter was cancelled")
        }
        crate::runtime::Timeout::Elapsed => {
            panic!("the terminal waiter was not pulsed after the mailbox panic")
        }
    };
    assert!(matches!(member_exit.kind(), ExitKind::Completed));

    let reason = match crate::runtime::timeout(Duration::from_secs(2), system.wait()).await {
        crate::runtime::Timeout::Completed(reason) => reason,
        crate::runtime::Timeout::Elapsed => {
            panic!("the system monitor did not contain the driver unwind")
        }
    };
    assert_eq!(reason, StopReason::ShutdownRequested);
    let root_exit = root.member.wait_terminal().await;
    assert!(matches!(
        root_exit.kind(),
        ExitKind::Panicked { message }
            if message.as_deref() == Some("injected mailbox waker panic")
    ));
    let mut terminal_trace = Vec::new();
    while let Some(item) = events.recv().await {
        let LifecycleItem::Event(event) = item else {
            panic!("the single-child terminal trace cannot lag");
        };
        match event.kind {
            LifecycleEventKind::Exited { .. } => terminal_trace.push("exited"),
            LifecycleEventKind::Removed { .. } => terminal_trace.push("removed"),
            _ => {}
        }
    }
    assert_eq!(
        terminal_trace,
        ["exited", "removed"],
        "mailbox panic resumes only after the terminal event, pruning edge, and stream closure"
    );
}

#[test]
fn terminality_signal_follows_mailbox_termination() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let probe = Arc::new(TrySendOnWake {
        actor,
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut context = Context::from_waker(&waker);
    let mut watcher = member.record_watcher();
    let mut changed = Box::pin(watcher.changed());
    assert!(changed.as_mut().poll(&mut context).is_pending());

    member.terminalize(Exit::never_started());

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some(SendErrorKind::Terminated)
    );
    assert!(changed.as_mut().poll(&mut context).is_ready());
}

#[test]
fn supervised_terminality_pulse_follows_mailbox_termination() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mailbox_wakes = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountWake(Arc::clone(&mailbox_wakes))));
    let mut parked_send = Box::pin(actor.send(1));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let probe = Arc::new(ObserveWakeCount {
        wakes: Arc::clone(&mailbox_wakes),
        observed: Mutex::new(None),
    });
    let terminal_waker = Waker::from(Arc::clone(&probe));
    let mut terminal = Box::pin(member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    assert!(root.terminalize_child(
        &member,
        Exit::never_started(),
        None,
        StartupDisposition::Aborted,
    ));

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some(1),
        "the supervised terminal pulse must follow parked-sender discharge"
    );
    assert!(member.record().startup_aborted);
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[test]
fn supervised_mailbox_teardown_panic_precedes_terminal_pulse_panic() {
    let root = isolated_scope("root", ScopeFlavor::Ordered);
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    root.admit_child(ResidentProjection::new(Arc::clone(&member), None));
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);

    let mailbox_payload_dropped = Arc::new(AtomicUsize::new(0));
    let mailbox_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &mailbox_payload_dropped,
    ))));
    let mut parked_send = Box::pin(actor.send(1));
    assert!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(&mailbox_waker))
            .is_pending()
    );

    let terminal_payload_dropped = Arc::new(AtomicUsize::new(0));
    let terminal_waker = Waker::from(Arc::new(CountedPanicWake(Arc::clone(
        &terminal_payload_dropped,
    ))));
    let mut terminal = Box::pin(member.wait_terminal());
    assert!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_waker))
            .is_pending()
    );

    let payload = catch_unwind(AssertUnwindSafe(|| {
        root.terminalize_child(
            &member,
            Exit::never_started(),
            None,
            StartupDisposition::Aborted,
        );
    }))
    .expect_err("the primary mailbox panic still surfaces");
    assert_eq!(
        mailbox_payload_dropped.load(Ordering::SeqCst),
        0,
        "the primary mailbox payload is retained for the caller"
    );
    assert_eq!(
        terminal_payload_dropped.load(Ordering::SeqCst),
        1,
        "the later terminal-pulse panic is contained as cleanup"
    );
    drop(payload);
    assert_eq!(mailbox_payload_dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        terminal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        parked_send
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(error)) if error.kind == SendErrorKind::Terminated
    ));
}

#[test]
fn mailbox_wake_observes_terminal_record_and_reentrant_terminality_is_idempotent() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    member.attach_mailbox(mailbox);
    let first_exit = Exit::never_started();
    let probe = Arc::new(ObserveMemberOnMailboxWake {
        member: Arc::clone(&member),
        competing_exit: Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut parked = Box::pin(actor.send(1));
    assert!(
        parked
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    member.terminalize(first_exit.clone());

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some((
            MemberStage::Terminal(first_exit.clone()),
            MemberStage::Terminal(first_exit.clone())
        ))
    );
    assert!(matches!(
        member.record().stage,
        MemberStage::Terminal(exit) if exit == first_exit
    ));
}

#[test]
fn attach_during_terminal_publication_finishes_record_before_mailbox_wake() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone());
    let actor = ActorRef::new(Arc::clone(&member), Arc::clone(&mailbox));
    let first_exit = Exit::never_started();
    member.stage_terminal_before_mailbox(first_exit.clone());
    let probe = Arc::new(ObserveMemberOnMailboxWake {
        member: Arc::clone(&member),
        competing_exit: Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut parked = Box::pin(actor.send(1));
    assert!(
        parked
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    member.attach_mailbox(mailbox);

    assert_eq!(
        *probe.observed.lock().expect("observation mutex poisoned"),
        Some((
            MemberStage::Terminal(first_exit.clone()),
            MemberStage::Terminal(first_exit.clone())
        ))
    );
    assert!(matches!(
        member.record().stage,
        MemberStage::Terminal(exit) if exit == first_exit
    ));
}

#[test]
fn concurrent_terminalizers_return_after_one_consistent_record_is_visible() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let start = Arc::new(Barrier::new(3));
    let workers = [
        Exit::never_started(),
        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
    ]
    .into_iter()
    .map(|exit| {
        let member = Arc::clone(&member);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            member.terminalize(exit);
            assert!(matches!(member.record().stage, MemberStage::Terminal(_)));
        })
    })
    .collect::<Vec<_>>();
    start.wait();
    for worker in workers {
        worker.join().expect("terminalizer thread succeeds");
    }

    let record = member.record();
    let MemberStage::Terminal(exit) = record.stage else {
        panic!("one terminal record must be visible");
    };
    assert_eq!(record.last_exit, Some(exit));
}

#[test]
fn terminal_startup_wake_follows_member_and_incarnation_publication() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("root");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
    let epoch = scope
        .begin_incarnation()
        .expect("test scope epoch is available");
    let probe = Arc::new(ObserveScopeOnStartupWake {
        scope: Arc::clone(&scope),
        epoch: Some(epoch),
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut watcher = scope.record_watcher();
    let mut changed = Box::pin(watcher.changed());
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    scope.finish_root_incarnation(epoch, StopReason::ShutdownRequested, Exit::never_started());

    let observed = probe
        .observed
        .lock()
        .expect("observation mutex poisoned")
        .clone()
        .expect("terminal startup wakes the scope watcher");
    assert!(matches!(observed.0, MemberStage::Terminal(_)));
    assert_eq!(observed.1, Some(true));
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
}

#[test]
fn no_live_root_startup_wake_follows_member_publication() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("root");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
    let probe = Arc::new(ObserveScopeOnStartupWake {
        scope: Arc::clone(&scope),
        epoch: None,
        observed: Mutex::new(None),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut watcher = scope.record_watcher();
    let mut changed = Box::pin(watcher.changed());
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    scope.finish_live_root_incarnation(StopReason::ShutdownRequested, Exit::never_started());

    let observed = probe
        .observed
        .lock()
        .expect("observation mutex poisoned")
        .clone()
        .expect("terminal startup wakes the scope watcher");
    assert!(matches!(observed.0, MemberStage::Terminal(_)));
    assert_eq!(observed.1, None);
    assert!(
        changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
}

#[test]
fn dynamic_close_holds_removal_completion_through_observation_cleanup() {
    let mut identity = ScopeIdentity::new();
    let root_id = ChildId::from("root");
    let root_member = MemberCell::new(
        root_id.clone(),
        identity
            .mint_membership(&root_id)
            .expect("root membership available"),
    );
    let root = ScopeCell::new(root_member, ScopeFlavor::Dynamic, ScopeIdentity::new());
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&child_id)
            .expect("child membership available"),
    );
    let slot = SlotCell::new(Arc::clone(&member), None);
    root.set_admitted_children(vec![resident_projection(&slot)]);
    let (events, _receiver) = crate::runtime::bounded_mpsc(1);
    let control = DynamicControl::new(events);
    let (sender, mut response) = crate::runtime::oneshot();
    let mut responses = RemovalResponses::default();
    responses.0.push(sender);
    control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .insert(
            ChildId::from("worker"),
            DynamicEntry::removing(slot, ChildKey(1), responses),
        );

    let entries = control.close();
    assert!(
        response.try_receive().is_none(),
        "closing admission must not complete removal before teardown"
    );
    member.terminalize(Exit::never_started());
    assert!(root.prune_child(&member));
    assert!(
        response.try_receive().is_none(),
        "terminality and Removed precede removal completion"
    );
    drop(entries);
    assert_eq!(response.try_receive(), Some(RemoveOutcome::Removed));
}

#[crate::runtime::test]
async fn retained_unadmitted_slot_does_not_retain_the_request_forwarder() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let slot = scope
        .reserve_task("retained")
        .expect("unadmitted reservation is retained");
    let control = scope
        .as_scope()
        .cell
        .dynamic_route()
        .expect("the live dynamic scope exposes its control");
    let (forwarder_close, forwarder_ended) = control.request_forwarder_probe();

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("driver teardown completes");

    assert!(forwarder_close.is_fired());
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(1), forwarder_ended.fired()).await,
        crate::runtime::Timeout::Completed(())
    ));
    drop(slot);
    drop(control);
}

#[test]
fn reserve_dynamic_rejects_an_empty_id_at_the_driver_boundary() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);

    assert!(matches!(
        super::reserve_dynamic(&root, ChildId::from(""), None),
        Err(crate::ReserveError::EmptyId)
    ));
}

#[test]
fn dynamic_removal_releases_state_before_waiting_for_the_observation_gate() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&child_id)
            .expect("child membership available"),
    );
    let slot = SlotCell::new(Arc::clone(&member), None);
    root.set_admitted_children(vec![resident_projection(&slot)]);
    let (events, _receiver) = crate::runtime::bounded_mpsc(1);
    let control = DynamicControl::new(events);
    let key = ChildKey(1);
    control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .insert(child_id.clone(), DynamicEntry::resident(slot, key, None));
    root.set_dynamic_route(Some(control.clone()));

    let captures = root.probe_gate_captures();
    let gate = root.observation_gate();
    let held_gate = gate.lock();
    let removal_root = Arc::clone(&root);
    let removal_id = child_id.clone();
    let worker =
        std::thread::spawn(move || super::remove_dynamic(&removal_root, &removal_id, None));

    // The capture report proves removal committed to the held observation
    // gate for its removing transition. Dynamic state cannot change again
    // until that gate is released, so a single acquisition attempt decides
    // whether removal reached the gate while still holding the state.
    assert_eq!(
        captures
            .recv_timeout(CAPTURE_PROBE_WAIT)
            .expect("removal reports its gate capture within the bound"),
        GateCapture::Observation
    );
    let state = control
        .state
        .try_lock()
        .expect("a removal waiting on observation must release dynamic state");
    let entry = state
        .entries
        .get(&child_id)
        .expect("the removal keeps its resident registration");
    assert!(entry.is_removing());
    assert!(entry.matches_key(key));
    drop(state);

    let route = root
        .dynamic_route()
        .expect("the fixture exposes its dynamic route");
    assert!(matches!(
        route.reserve(&root, child_id.clone(), None),
        Err(crate::ReserveError::RemovalInProgress(id)) if id == child_id
    ));

    drop(held_gate);
    let response = worker.join().expect("removal transition completes");
    drop(response);
}

#[crate::runtime::test]
async fn saturated_removal_from_a_foreign_thread_reaches_the_driver() {
    let mut identity = ScopeIdentity::new();
    let first_id = ChildId::from("first");
    let second_id = ChildId::from("second");
    let first = identity
        .mint_membership(&first_id)
        .expect("first membership available");
    let second = identity
        .mint_membership(&second_id)
        .expect("second membership available");
    let member = MemberCell::new(second_id.clone(), second);
    let slot = SlotCell::new(Arc::clone(&member), None);
    let second_key = ChildKey(2);
    let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
    assert!(
        events
            .try_send(DriverEvent::Removal(RemovalRequest {
                membership: first,
                key: ChildKey(1),
            }))
            .is_ok(),
        "the fixture saturates the bounded driver lane"
    );
    let control = DynamicControl::new(events);
    control
        .state
        .lock()
        .expect("dynamic-state mutex poisoned")
        .entries
        .insert(
            second_id,
            DynamicEntry::resident(Arc::clone(&slot), second_key, Some(Latch::default())),
        );
    let foreign_control = Arc::clone(&control);
    let foreign_slot = Arc::clone(&slot);
    std::thread::spawn(move || {
        assert!(
            !crate::runtime::is_available(),
            "Tokio context is not inherited by a foreign thread"
        );
        super::signal_fused_cancel(foreign_control.as_ref(), &foreign_slot, &Latch::default());
    })
    .join()
    .expect("foreign-thread removal signaling succeeds");

    let Some(DriverEvent::Removal(observed_first)) = event_receiver.recv().await else {
        panic!("the saturated event remains first");
    };
    assert_eq!(observed_first.membership, first);
    let second_event =
        match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
            crate::runtime::Timeout::Completed(event) => {
                event.expect("the control forwarder remains open")
            }
            crate::runtime::Timeout::Elapsed => {
                panic!("the off-runtime removal edge must not be lost")
            }
        };
    let DriverEvent::Removal(observed_second) = second_event else {
        panic!("the forwarded event is the requested removal");
    };
    assert_eq!(observed_second.membership, second);
    assert_eq!(observed_second.key, second_key);
}

#[crate::runtime::test]
async fn admission_conversion_panic_does_not_poison_dynamic_cleanup() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
    let (disposal_events, _disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events.clone());
    root.set_dynamic_route(Some(control.clone()));
    root.set_admitted_children(Vec::new());
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: ResolvedDefaults::default(),
        intensity_policy: Intensity::default(),
        intensity: super::IntensityState::default(),
        children: ChildArena::default(),
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::running(),
        next_ordered_start: None,
        role: ScopeRole::Root,
        dynamic: Some(control.clone()),
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };

    let reservation = super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    let response = super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        None,
    )
    .expect("admission starts inside the runtime");
    let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
        panic!("the admission forwarder submits the request")
    };

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _identity = root
                .child_identity
                .lock()
                .expect("scope identity mutex starts healthy");
            panic!("inject admission conversion failure");
        }))
        .is_err()
    );
    assert!(
        catch_unwind(AssertUnwindSafe(|| scope.handle_admission(request))).is_err(),
        "the poisoned child identity injects the conversion panic"
    );
    assert!(matches!(
        response.receive().await,
        Some(Err(ReserveError::NotAdmitting(
            crate::NotAdmittingCause::Terminal
        )))
    ));
    assert!(matches!(member.record().stage, MemberStage::Terminal(_)));

    let cleanup = catch_unwind(AssertUnwindSafe(|| drop(scope)));
    assert!(
        cleanup.is_ok(),
        "conversion failure must not poison dynamic cleanup"
    );
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex remains healthy")
            .entries
            .is_empty(),
        "cleanup discharges the stranded reservation"
    );
}

pub(crate) async fn exercise_saturated_fused_drop_before_exit<A>(
    make_admission: impl FnOnce(super::DynamicReservation) -> A,
) where
    A: Future,
{
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events.clone());
    root.set_dynamic_route(Some(control.clone()));
    root.set_admitted_children(Vec::new());
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: ResolvedDefaults::default(),
        intensity_policy: Intensity::default(),
        intensity: super::IntensityState::default(),
        children: ChildArena::default(),
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::running(),
        next_ordered_start: None,
        role: ScopeRole::Root,
        dynamic: Some(control.clone()),
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };

    let release_failure = Latch::default();
    let starts = Arc::new(AtomicUsize::new(0));
    let reservation = super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new({
            let release_failure = release_failure.clone();
            let starts = Arc::clone(&starts);
            move |_| {
                let release_failure = release_failure.clone();
                let invocation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if invocation == 1 {
                        release_failure.fired().await;
                        Err(ExitError::message("first incarnation failed"))
                    } else {
                        future::pending().await
                    }
                }
            }
        })));
    let membership = member.membership();
    let mut admission = Box::pin(make_admission(reservation));
    assert!(
        admission
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "first poll submits the fused admission"
    );
    let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
        panic!("the admission forwarder submits the request")
    };
    scope.handle_admission(request);

    for _ in 0..64 {
        if starts.load(Ordering::SeqCst) == 1 {
            break;
        }
        crate::runtime::yield_now().await;
    }
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    release_failure.fire();
    let exit = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
        crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        }))) => (
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        ),
        crate::runtime::Timeout::Completed(_) => panic!("the first incarnation reports exit"),
        crate::runtime::Timeout::Elapsed => panic!("the first incarnation exit must arrive"),
    };
    let key = exit.0;
    assert!(
        scope
            .events
            .try_send(DriverEvent::Removal(RemovalRequest {
                membership: root.member.membership(),
                key: ChildKey(u64::MAX - 1),
            }))
            .is_ok(),
        "the fixture saturates the bounded driver lane"
    );
    drop(admission);
    assert!(
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .get(member.id())
            .is_some_and(|entry| entry.is_removing() && entry.matches_key(key)),
        "fused drop marks the indexed membership removing before its queued edge advances"
    );

    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(scope.children[key].restart_deadline.is_none());
    assert_eq!(
        root.snapshot().total_restarts,
        crate::TotalRestarts::ZERO,
        "cancellation that linearized before exit incurs no restart charge"
    );
    for _ in 0..16 {
        crate::runtime::yield_now().await;
    }
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "exit dispatch consults fused cancellation before restart construction"
    );

    assert!(matches!(
        event_receiver.recv().await,
        Some(DriverEvent::Removal(queued))
            if queued.membership == root.member.membership()
    ));
    let forwarded =
        match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
            crate::runtime::Timeout::Completed(Some(event)) => event,
            crate::runtime::Timeout::Completed(None) => panic!("the driver lane remains open"),
            crate::runtime::Timeout::Elapsed => panic!("the fused removal edge is forwarded"),
        };
    let DriverEvent::Removal(removal) = forwarded else {
        panic!("the forwarded event is the fused removal")
    };
    assert_eq!(removal.membership, membership);
    assert_eq!(removal.key, key);
    scope.handle_removal(removal);
    let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
        crate::runtime::unbounded_mpsc_recv(&mut disposal_event_receiver).await
    else {
        panic!("removal joins retained construction disposal")
    };
    scope.handle_construction_disposed(child, panic);
    assert!(scope.children.get(key).is_none());

    control.request_forwarder_close.fire();
    control.request_forwarder_ended.fired().await;
}

/// Exercises the `DeadlineKind::Restart` suppression gate on its own:
/// the restart deadline is scheduled first (no stop source latched at
/// exit time), then the fused cancellation lands before the deadline's
/// batch runs. The gate must clear the stale backoff edge without
/// invoking user construction.
#[crate::runtime::test]
async fn restart_deadline_gate_suppresses_a_fused_cancel_landing_after_scheduling() {
    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.member
        .update(|record| record.stage = MemberStage::Running);
    root.set_state(ScopeState::Running);
    root.set_startup(Ok(()));

    let (events, mut event_receiver) = crate::runtime::bounded_mpsc(64);
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let control = DynamicControl::new(events.clone());
    root.set_dynamic_route(Some(control.clone()));
    root.set_admitted_children(Vec::new());
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: ResolvedDefaults::default(),
        intensity_policy: Intensity::default(),
        intensity: super::IntensityState::default(),
        children: ChildArena::default(),
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::running(),
        next_ordered_start: None,
        role: ScopeRole::Root,
        dynamic: Some(control.clone()),
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };

    let starts = Arc::new(AtomicUsize::new(0));
    let reservation = super::reserve_dynamic(&root, ChildId::from("worker"), None)
        .expect("running dynamic scope reserves the child");
    let member = Arc::clone(&reservation.slot.member);
    reservation
        .slot
        .define(ChildConstruction::Task(TaskDef::new({
            let starts = Arc::clone(&starts);
            move |_| {
                let invocation = starts.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if invocation == 1 {
                        Err(ExitError::message("first incarnation failed"))
                    } else {
                        future::pending().await
                    }
                }
            }
        })));
    let membership = member.membership();
    let fused_cancel = Latch::default();
    let mut response = super::start_admission(
        Arc::clone(&reservation.control),
        Arc::clone(&reservation.slot),
        Some(fused_cancel.clone()),
    )
    .expect("the running scope accepts the admission");
    let Some(DriverEvent::Admission(request)) = event_receiver.recv().await else {
        panic!("the admission forwarder submits the request")
    };
    scope.handle_admission(request);
    assert!(matches!(response.try_receive(), Some(Ok(()))));

    let exit = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
        crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        }))) => (
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        ),
        crate::runtime::Timeout::Completed(_) => panic!("the first incarnation reports exit"),
        crate::runtime::Timeout::Elapsed => panic!("the first incarnation exit must arrive"),
    };
    let key = exit.0;
    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(
        scope.children[key].restart_deadline.is_some(),
        "a live fused admission does not suppress the restart at exit dispatch"
    );
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Restarting
    ));

    // The fused admission handle drops only now: the cancellation latch
    // fires after the backoff was scheduled, and its Removal edge queues
    // behind the already-due restart deadline.
    super::signal_fused_cancel(control.as_ref(), &reservation.slot, &fused_cancel);
    assert!(fused_cancel.is_fired());

    let deadline = scope
        .deadlines
        .pop_due(crate::runtime::now() + Duration::from_secs(60 * 60))
        .expect("the immediate-backoff restart deadline is registered");
    assert!(matches!(deadline, super::DeadlineKind::Restart { .. }));
    scope.handle_deadline(deadline);

    assert!(
        scope.children[key].restart_deadline.is_none(),
        "the gate clears the stale backoff edge"
    );
    assert!(scope.children[key].active.is_none());
    for _ in 0..16 {
        crate::runtime::yield_now().await;
    }
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "the restart deadline arm rechecks level-triggered stop sources"
    );

    let forwarded =
        match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
            crate::runtime::Timeout::Completed(Some(event)) => event,
            crate::runtime::Timeout::Completed(None) => panic!("the driver lane remains open"),
            crate::runtime::Timeout::Elapsed => panic!("the fused removal edge is forwarded"),
        };
    let DriverEvent::Removal(removal) = forwarded else {
        panic!("the forwarded event is the fused removal");
    };
    assert_eq!(removal.membership, membership);
    assert_eq!(removal.key, key);
    scope.handle_removal(removal);
    let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
        crate::runtime::unbounded_mpsc_recv(&mut disposal_event_receiver).await
    else {
        panic!("removal joins retained construction disposal")
    };
    scope.handle_construction_disposed(child, panic);
    assert!(scope.children.get(key).is_none());

    control.request_forwarder_close.fire();
    control.request_forwarder_ended.fired().await;
}

#[crate::runtime::test]
async fn saturated_admissions_share_one_forwarder_task_and_all_resolve() {
    const ADMISSIONS: usize = 64;

    let root = isolated_scope("root", ScopeFlavor::Dynamic);
    let (events, event_receiver) = crate::runtime::bounded_mpsc(1);
    let control = DynamicControl::new(events);
    let child_id = ChildId::from("worker");
    let member = MemberCell::new(
        child_id.clone(),
        root.child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&child_id)
            .expect("child membership available"),
    );
    let slot = SlotCell::new(Arc::clone(&member), None);

    let baseline = crate::runtime::alive_task_count();
    let mut responses = Vec::with_capacity(ADMISSIONS);
    for _ in 0..ADMISSIONS {
        responses.push(
            super::start_admission(control.clone(), Arc::clone(&slot), None)
                .expect("runtime is available"),
        );
    }

    // Let the forwarder run until it parks on the saturated driver
    // channel; pending admissions must not each hold a live sender task.
    for _ in 0..64 {
        crate::runtime::yield_now().await;
    }
    assert!(
        crate::runtime::alive_task_count() <= baseline + 1,
        "saturated admissions share one forwarder task instead of spawning one each"
    );

    // Ending the driver channel answers every queued admission through
    // its response obligation.
    drop(event_receiver);
    for response in responses {
        assert!(matches!(
            response.receive().await,
            Some(Err(crate::ReserveError::NotAdmitting(
                crate::NotAdmittingCause::Terminal
            )))
        ));
    }

    let mut alive = crate::runtime::alive_task_count();
    for _ in 0..1_024 {
        if alive == baseline {
            break;
        }
        crate::runtime::yield_now().await;
        alive = crate::runtime::alive_task_count();
    }
    assert_eq!(alive, baseline, "the forwarder exits once its queue drains");
}

#[crate::runtime::test]
async fn system_shutdown_joins_root_driver_teardown() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    let root = system.scope();
    system.wait_started().await.expect("dynamic root starts");
    let control = root
        .as_scope()
        .cell
        .dynamic_route()
        .expect("running dynamic root has a control");
    let weak = Arc::downgrade(&control);
    drop(control);

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("empty dynamic root shuts down");

    assert!(
        weak.upgrade().is_none(),
        "shutdown returns only after root driver teardown drops dynamic state"
    );
}

#[test]
fn incarnation_mint_exhaustion_has_no_terminal_side_effects() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let membership = identity.mint_membership(&id).expect("membership available");
    let member = MemberCell::new(id, membership);
    let previous = Exit::new(
        ExitKind::Failed(ExitError::message("last completed incarnation")),
        Cancellation::NotObserved,
    );
    member.update(|record| {
        record.stage = MemberStage::Restarting;
        record.last_exit = Some(previous.clone());
    });
    let mut counter = IncarnationCounter::near_exhaustion(membership);

    assert!(counter.mint().is_some());
    assert!(counter.mint().is_none());
    assert!(matches!(member.record().stage, MemberStage::Restarting));
    assert_eq!(member.record().last_exit, Some(previous));
    assert!(counter.mint().is_none());
}

#[crate::runtime::test]
async fn incarnation_exhaustion_uses_post_disposal_retention_routing() {
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new(|_| future::pending::<crate::ExitResult>()).retention(Retention::Remove),
    )
    .expect("valid task");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, mut event_receiver) = crate::runtime::bounded_mpsc(1);
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: super::IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::running(),
        next_ordered_start: Some(key),
        role: ScopeRole::Root,
        dynamic: None,
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };
    plan.armed = false;
    drop(plan);

    scope.children[key].incarnations =
        IncarnationCounter::near_exhaustion(scope.children[key].slot.member.membership());
    let first = scope.children[key]
        .incarnations
        .mint()
        .expect("the last usable incarnation mints");
    let previous = Exit::new(
        ExitKind::Failed(ExitError::message("last completed incarnation")),
        Cancellation::NotObserved,
    );
    root.transition_child(
        &scope.children[key].slot.member,
        |record| {
            record.incarnation = None;
            record.last_incarnation = Some(first);
            record.last_exit = Some(previous.clone());
            record.stage = MemberStage::Restarting;
        },
        None,
    );

    assert!(
        scope
            .events
            .try_send(DriverEvent::Child(ChildEvent::Ready {
                child: key,
                incarnation: first,
            }))
            .is_ok(),
        "the bounded driver queue is deliberately saturated"
    );

    scope.spawn_child(key);
    assert!(scope.children[key].is_disposing());
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Restarting
    ));
    assert_eq!(root.snapshot().children.len(), 1);

    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
        disposal_event_receiver
            .recv()
            .await
            .expect("disposal reports completion")
    else {
        panic!("only construction disposal was armed")
    };
    assert!(matches!(
        event_receiver.try_recv(),
        Ok(DriverEvent::Child(ChildEvent::Ready { .. }))
    ));
    scope.handle_construction_disposed(child, panic);

    assert!(!scope.children[key].is_disposing());
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(ref exit) if exit == &previous
    ));
    assert!(
        root.snapshot().children.is_empty(),
        "retention pruning follows joined disposal"
    );
}

/// Exhaustion terminalizes a membership that never spawned. B.6 makes
/// that the plain `Stopped { NeverStarted }` verdict; §6's
/// `StartupAborted` stays reserved for a membership that ran and failed
/// before its initial readiness edge. The pre-readiness position still
/// has to route the scope's startup failure.
#[crate::runtime::test]
async fn first_spawn_exhaustion_stops_without_reporting_a_startup_abort() {
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new(|_| future::pending::<crate::ExitResult>()),
    )
    .expect("valid task");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, _event_receiver) = crate::runtime::bounded_mpsc(64);
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: super::IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::starting(),
        next_ordered_start: Some(key),
        role: ScopeRole::Root,
        dynamic: None,
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };
    plan.armed = false;
    drop(plan);

    // Burn the counter's last usable generation without touching the
    // member record: the child is still an unspawned initial member, so
    // its very first `spawn_child` exhausts before any incarnation runs.
    scope.children[key].incarnations =
        IncarnationCounter::near_exhaustion(scope.children[key].slot.member.membership());
    assert!(scope.children[key].incarnations.mint().is_some());
    assert!(scope.children[key].initial);
    assert!(!scope.children[key].initial_ready);
    assert_eq!(
        scope.children[key].slot.member.record().last_incarnation,
        None
    );

    scope.spawn_child(key);
    assert!(scope.children[key].is_disposing());

    let DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic }) =
        disposal_event_receiver
            .recv()
            .await
            .expect("disposal reports completion")
    else {
        panic!("only construction disposal was armed")
    };
    scope.handle_construction_disposed(child, panic);

    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(ref exit) if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    let snapshot = root.snapshot();
    let published = snapshot
        .child("worker")
        .expect("a retained exhausted child stays resident");
    assert!(
        matches!(
            published.state,
            ChildState::Stopped { ref exit } if matches!(exit.kind(), ExitKind::NeverStarted)
        ),
        "an unspawned exhausted membership is Stopped, not StartupAborted: {:?}",
        published.state
    );
    assert!(
        !scope.children[key].slot.member.record().startup_aborted,
        "§6's startup-abort flag belongs to a membership that ran"
    );
    assert!(
        matches!(
            root.record().startup,
            Some(Err(StartupError::StartupFailed(_)))
        ),
        "the pre-readiness position still routes the scope's startup failure"
    );
}

/// Pins main's linearization for a scope stop latched cross-batch: a
/// restartable initial child failing pre-ready still dispatches
/// `ScheduleRestart`, and the latched stop's own follow-up event owns
/// the startup verdict (`ShutdownRequested`). Exit dispatch must not
/// consult latched-but-unprocessed scope-stop sources for its membership
/// classification, or the failure would be rerouted into
/// `StartupFailed` while restart suppression claims the stop was first.
#[crate::runtime::test]
async fn latched_shutdown_keeps_the_startup_verdict_for_its_follow_up_event() {
    let mut tree = Tree::new();
    tree.add_task(
        "worker",
        TaskDef::new(|_| async { Err(ExitError::message("failed before readiness")) })
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid"),
    )
    .expect("valid task");
    let mut plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let epoch = root
        .begin_incarnation()
        .expect("test scope epoch is available");
    root.set_admitted_children(
        plan.children
            .iter()
            .map(|child| resident_projection(&child.slot))
            .collect(),
    );
    let (events, mut event_receiver) = crate::runtime::bounded_mpsc(64);
    let (disposal_events, mut disposal_event_receiver) = crate::runtime::unbounded_mpsc();
    let child = ChildRuntime::from_plan(plan.children.pop().expect("one child plan"), &root);
    let mut children = ChildArena::default();
    let key = children
        .insert(child)
        .unwrap_or_else(|_| panic!("the test fixture fits in the child-key domain"));
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        defaults: plan.defaults.clone(),
        intensity_policy: plan.config.intensity,
        intensity: super::IntensityState::default(),
        children,
        events,
        disposal_events,
        deadlines: super::DeadlineQueue::default(),
        jitter: crate::runtime::JitterRng::from_system_entropy(),
        lifecycle: ScopeLifecycle::starting(),
        next_ordered_start: Some(key),
        role: ScopeRole::Root,
        dynamic: None,
        epoch,
        ancestor_shutdown_seen: false,
        ancestor_abort_seen: false,
        hard_forced: false,
        ordered_stop_progressing: false,
        ordered_stop_cursor: None,
        ordered_stop_waiting: None,
        ordered_stop_inspections: 0,
        completion: None,
    };
    plan.armed = false;
    drop(plan);

    assert!(scope.children[key].initial);
    scope.spawn_child(key);
    assert!(!scope.children[key].initial_ready);
    let exit = match crate::runtime::timeout(Duration::from_secs(2), event_receiver.recv()).await {
        crate::runtime::Timeout::Completed(Some(DriverEvent::Child(ChildEvent::Exited {
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        }))) => (
            child,
            incarnation,
            recorded,
            join,
            cancellation,
            readiness_signal_seen,
        ),
        crate::runtime::Timeout::Completed(_) => panic!("the pre-ready failure reports exit"),
        crate::runtime::Timeout::Elapsed => panic!("the pre-ready failure exit must arrive"),
    };

    // The stop request latches after this batch was collected: it is
    // visible to `has_stop_request`, but its `Pending::Shutdown` follow-up
    // event belongs to the next batch.
    assert!(root.request_shutdown().is_some());
    assert!(root.has_stop_request(scope.epoch));

    scope.handle_exit(exit.0, exit.1, exit.2, exit.3, exit.4, exit.5);
    assert!(
        scope.children[key].restart_deadline.is_some(),
        "a latched scope stop does not reclassify exit dispatch"
    );
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Restarting
    ));
    assert!(
        root.record().startup.is_none(),
        "the pre-ready failure must not claim the startup verdict: {:?}",
        root.record().startup
    );

    // The latched stop's guaranteed follow-up event runs in the next
    // batch and owns the verdict, exactly as an unlatched scope would.
    assert!(root.take_shutdown_request(scope.epoch));
    scope.begin_drain(StopReason::ShutdownRequested);
    assert!(
        matches!(
            root.record().startup,
            Some(Err(StartupError::ShutdownRequested))
        ),
        "the latched stop owns the startup verdict: {:?}",
        root.record().startup
    );
    assert!(scope.children[key].restart_deadline.is_none());

    let Some(DriverEvent::Child(ChildEvent::ConstructionDisposed { child, panic })) =
        crate::runtime::unbounded_mpsc_recv(&mut disposal_event_receiver).await
    else {
        panic!("only construction disposal was armed")
    };
    scope.handle_construction_disposed(child, panic);
    assert!(matches!(
        scope.children[key].slot.member.record().stage,
        MemberStage::Terminal(_)
    ));
    assert!(
        !scope.children[key].slot.member.record().startup_aborted,
        "shutdown-first linearization publishes no startup abort"
    );
    assert!(matches!(
        root.record().startup,
        Some(Err(StartupError::ShutdownRequested))
    ));
}

#[crate::runtime::test]
async fn nested_membership_exhaustion_is_structured_and_fail_closed() {
    let nested_id = ChildId::from("nested");
    let mut parent_identity = ScopeIdentity::new();
    let nested_membership = parent_identity
        .mint_membership(&nested_id)
        .expect("nested membership available");
    let nested_member = MemberCell::new(nested_id, nested_membership);

    let worker_id = ChildId::from("worker");
    let mut child_identity = ScopeIdentity::near_exhaustion(worker_id.clone(), 7);
    child_identity
        .mint_membership(&worker_id)
        .expect("last usable membership is minted before the rebuild");
    let scope = ScopeCell::new(nested_member, ScopeFlavor::Ordered, child_identity);

    let mut tree = Tree::new();
    let worker = tree
        .add_task(
            worker_id.clone(),
            TaskDef::new(|_| future::pending::<crate::ExitResult>()),
        )
        .expect("provisional declaration succeeds");
    let ready = CompletionGatedLatch::default();
    let error = run_nested_tree(
        tree.into_core_for_test(),
        Arc::clone(&scope),
        crate::policy::ResolvedDefaults::default(),
        NestedScopeLatches {
            parent_ready: ready.clone(),
            ancestor: AncestorCommandLatches {
                shutdown: Latch::default(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        },
    )
    .await
    .expect_err("the stable child-id domain is exhausted");

    let failure = error
        .startup_failure()
        .expect("framework provenance is retained");
    assert!(matches!(
        failure.cause,
        StartupFailureCause::IdentityExhausted { ref id } if id == &worker_id
    ));
    assert!(matches!(
        scope.record().startup,
        Some(Err(StartupError::StartupFailed(ref failure)))
            if matches!(failure.cause, StartupFailureCause::IdentityExhausted { ref id } if id == &worker_id)
    ));
    assert!(matches!(
        scope.record().state,
        ScopeState::Stopped {
            reason: StopReason::StartupFailed(_)
        }
    ));
    assert!(!ready.is_fired());
    assert!(matches!(worker.wait().await.kind(), ExitKind::NeverStarted));
}

#[crate::runtime::test]
async fn scope_incarnation_exhaustion_closes_nested_observation() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("nested");
    let membership = identity.mint_membership(&id).expect("membership available");
    let member = MemberCell::new(id, membership);
    let scope = ScopeCell::new(
        Arc::clone(&member),
        ScopeFlavor::Ordered,
        ScopeIdentity::new(),
    );
    let slot = SlotCell::new(Arc::clone(&member), Some(Arc::clone(&scope)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);
    let mut snapshots = scope.subscribe_snapshots();
    let mut events = scope.subscribe_lifecycle();
    let mut counter = IncarnationCounter::near_exhaustion(membership);
    let first = counter.mint().expect("last incarnation mints");
    member.update(|record| {
        record.stage = MemberStage::Restarting;
        record.last_incarnation = Some(first);
        record.last_exit = Some(Exit::new(ExitKind::Completed, Cancellation::NotObserved));
    });
    scope.set_state(ScopeState::Stopped {
        reason: StopReason::Finished,
    });

    assert!(counter.mint().is_none());
    assert!(matches!(member.record().stage, MemberStage::Restarting));
    assert!(matches!(
        events.try_recv(),
        Ok(LifecycleItem::Event(crate::LifecycleEvent {
            kind: LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped {
                    reason: StopReason::Finished
                }
            },
            ..
        }))
    ));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));
    assert!(parent.terminalize_child(
        &member,
        Exit::new(ExitKind::Completed, Cancellation::NotObserved),
        None,
        StartupDisposition::NotAborted,
    ));
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Closed));
    snapshots
        .changed()
        .await
        .expect("the prior incarnation's final snapshot remains observable");
    assert!(matches!(
        snapshots.borrow_latest().state,
        ScopeState::Stopped {
            reason: StopReason::Finished
        }
    ));
    assert!(snapshots.changed().await.is_err());
}

#[crate::runtime::test]
async fn never_started_nested_terminal_publishes_one_final_parent_snapshot() {
    let parent = isolated_scope("parent", ScopeFlavor::Ordered);
    let nested = isolated_scope("nested", ScopeFlavor::Ordered);
    let slot = SlotCell::new(Arc::clone(&nested.member), Some(Arc::clone(&nested)));
    parent.set_admitted_children(vec![resident_projection(&slot)]);
    let mut snapshots = parent.subscribe_snapshots();

    assert!(parent.terminalize_child(
        &nested.member,
        Exit::never_started(),
        None,
        StartupDisposition::NotAborted,
    ));
    snapshots
        .changed()
        .await
        .expect("no-incarnation terminal publishes the parent projection");

    assert!(matches!(
        snapshots
            .borrow_latest()
            .child("nested")
            .expect("retained nested child remains resident")
            .state,
        ChildState::Stopped { ref exit } if matches!(exit.kind(), ExitKind::NeverStarted)
    ));
    assert!(matches!(
        nested.record().state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    ));
}

#[test]
fn lifecycle_sequence_exhaustion_poison_is_never_minted_and_becomes_lag() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("scope");
    let membership = identity.mint_membership(&id).expect("membership available");
    let member = MemberCell::new(id, membership);
    let scope = ScopeCell::new(member, ScopeFlavor::Ordered, ScopeIdentity::new());
    scope.set_lifecycle_sequence(u64::MAX - 2);
    let mut events = scope.subscribe_lifecycle();

    scope.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Starting,
    });
    scope.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Running,
    });
    scope.emit(LifecycleEventKind::ScopeState {
        state: ScopeState::Draining,
    });

    assert_eq!(events.try_recv(), Ok(LifecycleItem::Lagged { dropped: 2 }));
    let LifecycleItem::Event(event) = events.try_recv().expect("last mintable event remains")
    else {
        panic!("expected the final mintable event");
    };
    assert_eq!(event.seq.get(), u64::MAX - 1);
    assert_eq!(
        scope.snapshot().lifecycle_seq,
        crate::LifecycleSeq::EXHAUSTED
    );
}
