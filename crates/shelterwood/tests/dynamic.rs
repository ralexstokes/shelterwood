use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, ReleaseGate, advance_time, assert_quiet,
    policy::never,
    poll_once, poll_until,
    waiting::{signalled_waiting_task, task as waiting_task, tree as waiting_tree},
};
use shelterwood::{
    Actor, ActorOnceDef, Backoff, ChildState, Context as ActorContext, DynamicScopeRef,
    DynamicTree, ExitError, ExitKind, ExitResult, NotAdmittingCause, RawActor, RawContext, RawDef,
    RawOnceDef, Readiness, RemoveOutcome, ReserveError, RestartCondition, RestartPolicy, Retention,
    ScopeRef, ScopeState, SendErrorKind, Shutdown, StopReason, SubtreeDef, SubtreeOnceDef, System,
    TaskDef, TaskOnceDef, Tree,
};

/// Waits until a fused drop has finished releasing its id.
///
/// Dropping a polled fused `Admission` only fires the fused-cancel latch.
/// The driver then marks the membership `removing` and runs the stop ladder,
/// so a flag stored from inside the child's own shutdown handler is raised
/// *within* the removal window, while the dynamic entry is still held.
/// Reusing the id off that flag alone therefore races the only producer of
/// `ReserveError::RemovalInProgress`: a surviving entry whose member is
/// already `removing`.
///
/// Removal withdraws residency and publishes `Removed` while the old entry
/// still claims the id, then releases the entry in the same observation
/// transaction. An absent resident is therefore a sound signal that the id
/// is free again.
async fn wait_for_id_release(scope: &DynamicScopeRef, id: &str) {
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            scope.child(id).is_none()
        })
        .await,
        "removal of {id} did not release its id"
    );
}

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct DropSignal(ReleaseGate);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct GatedDynamicActor;

impl Actor for GatedDynamicActor {
    type Msg = ();
    type Args = ReleaseGate;

    async fn init(gate: Self::Args, _: &mut ActorContext<'_, Self>) -> Result<Self, ExitError> {
        gate.wait().await;
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut ActorContext<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct EvidenceActor;

impl Actor for EvidenceActor {
    type Msg = ();
    type Args = ();

    async fn init((): (), _: &mut ActorContext<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut ActorContext<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct WaitingRaw;

impl RawActor for WaitingRaw {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

struct SignalledWaitingRaw {
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl RawActor for SignalledWaitingRaw {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.started.store(true, Ordering::SeqCst);
        context.shutdown_token().cancelled().await;
        self.cancelled.store(true, Ordering::SeqCst);
        Ok(())
    }
}

fn signalled_waiting_tree(started: Arc<AtomicBool>, cancelled: Arc<AtomicBool>) -> Tree {
    let mut tree = Tree::new();
    let (_, completion) = tree
        .add_task_once(
            "worker",
            TaskOnceDef::new(move |context| async move {
                started.store(true, Ordering::SeqCst);
                context.shutdown_token().cancelled().await;
                cancelled.store(true, Ordering::SeqCst);
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid signalled task");
    drop(completion);
    tree
}

#[tokio::test]
async fn scope_dynamic_conversion_and_exact_dynamic_scope_removal_are_publicly_usable() {
    let ordered = Tree::new().spawn().expect("runtime is available");
    let ordered_scope: ScopeRef = ordered.scope();
    assert!(ordered_scope.dynamic().is_none());
    ordered
        .shutdown(Duration::from_secs(1))
        .await
        .expect("ordered root stops");

    let dynamic = DynamicTree::new().spawn().expect("runtime is available");
    dynamic.wait_started().await.expect("dynamic root starts");
    let dynamic_scope = dynamic.scope();
    let erased: &ScopeRef = dynamic_scope.as_scope();
    let recovered = erased.dynamic().expect("dynamic capability is recoverable");
    assert_eq!(recovered.as_scope(), erased);

    let nested = dynamic_scope
        .add_subtree_once("nested-dynamic", SubtreeOnceDef::new(DynamicTree::new()))
        .await
        .expect("dynamic subtree is admitted");
    assert_eq!(
        dynamic_scope.remove_dynamic_scope(&nested).await,
        RemoveOutcome::Removed
    );
    dynamic
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root stops");
}

#[tokio::test]
async fn restartable_dynamic_surfaces_are_parallel_across_all_three_child_kinds() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let task = scope
        .add_task("task", waiting_task())
        .await
        .expect("restartable task is admitted");
    let raw = scope
        .add_raw("raw", RawDef::factory(|| WaitingRaw))
        .await
        .expect("restartable raw actor is admitted");
    let subtree = scope
        .add_subtree("subtree", SubtreeDef::factory(waiting_tree))
        .await
        .expect("restartable subtree is admitted");

    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    assert_eq!(scope.remove_actor(&raw).await, RemoveOutcome::Removed);
    assert_eq!(scope.remove_scope(&subtree).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn consuming_dynamic_surfaces_are_parallel_across_all_three_child_kinds() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let (task, completion) = scope
        .add_task_once(
            "task-once",
            TaskOnceDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok::<_, ExitError>(())
            }),
        )
        .await
        .expect("one-shot task is admitted");
    drop(completion);
    let raw = scope
        .add_raw_once("raw-once", RawOnceDef::new(WaitingRaw))
        .await
        .expect("one-shot raw actor is admitted");
    let subtree = scope
        .add_subtree_once("subtree-once", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("one-shot subtree is admitted");

    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    assert_eq!(scope.remove_actor(&raw).await, RemoveOutcome::Removed);
    assert_eq!(scope.remove_scope(&subtree).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn dynamic_actor_add_resolves_at_admission_without_awaiting_init() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();
    let gate = ReleaseGate::default();
    let actor = tokio::time::timeout(
        Duration::from_secs(1),
        scope.add_actor_once(
            "gated",
            ActorOnceDef::<GatedDynamicActor>::new(gate)
                .readiness(Readiness::Manual)
                .shutdown(Shutdown::Abort),
        ),
    )
    .await
    .expect("admission does not wait for init")
    .expect("actor admitted");
    actor.send(()).await.expect("admitted mailbox is usable");
    assert_eq!(scope.remove_actor(&actor).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test]
async fn exact_handles_reject_cross_scope_and_same_id_successors() {
    let left = DynamicTree::new().spawn().expect("runtime is available");
    let right = DynamicTree::new().spawn().expect("runtime is available");
    left.wait_started().await.expect("left starts");
    right.wait_started().await.expect("right starts");
    let left_scope = left.scope();
    let right_scope = right.scope();
    let left_task = left_scope
        .add_task("same", waiting_task())
        .await
        .expect("left admission");
    let right_task = right_scope
        .add_task("same", waiting_task())
        .await
        .expect("right admission");

    assert_eq!(
        right_scope.remove_task(&left_task).await,
        RemoveOutcome::AlreadyAbsent
    );
    assert_eq!(
        left_scope.remove_task(&left_task).await,
        RemoveOutcome::Removed
    );
    let replacement = left_scope
        .add_task("same", waiting_task())
        .await
        .expect("replacement admission");
    assert!(!replacement.membership().supersedes(left_task.membership()));
    assert!(!left_task.membership().supersedes(replacement.membership()));
    assert_eq!(
        left_scope.remove_task(&left_task).await,
        RemoveOutcome::AlreadyAbsent
    );
    assert_eq!(
        left_scope.remove_task(&replacement).await,
        RemoveOutcome::Removed
    );
    assert_eq!(
        right_scope.remove_task(&right_task).await,
        RemoveOutcome::Removed
    );
    left.shutdown(Duration::from_secs(1))
        .await
        .expect("left stops");
    right
        .shutdown(Duration::from_secs(1))
        .await
        .expect("right stops");
}

#[tokio::test]
async fn nested_declared_membership_is_incomparable_with_its_runtime_replacement() {
    let mut nested = DynamicTree::new();
    let declared = nested
        .add_task("worker", waiting_task())
        .expect("valid declared task");
    let different_id = nested
        .add_task("other", waiting_task())
        .expect("valid unrelated task");
    let declared_membership = declared.membership();

    let mut unrelated = DynamicTree::new();
    let unrelated_worker = unrelated
        .add_task("worker", waiting_task())
        .expect("valid task in unrelated scope");

    let mut root = Tree::new();
    let nested_scope = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid nested scope");
    root.add_subtree_once("unrelated", SubtreeOnceDef::new(unrelated))
        .expect("valid unrelated scope");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");

    assert_eq!(
        declared.membership(),
        declared_membership,
        "first lowering preserves the declared handle's identity"
    );
    assert_eq!(
        nested_scope.remove_task(&declared).await,
        RemoveOutcome::Removed
    );
    let replacement = nested_scope
        .add_task("worker", waiting_task())
        .await
        .expect("runtime replacement is admitted");

    assert!(!replacement.membership().supersedes(declared_membership));
    assert!(!declared_membership.supersedes(replacement.membership()));
    assert!(
        !replacement
            .membership()
            .supersedes(different_id.membership())
    );
    assert!(
        !different_id
            .membership()
            .supersedes(replacement.membership())
    );
    assert!(
        !replacement
            .membership()
            .supersedes(unrelated_worker.membership())
    );
    assert!(
        !unrelated_worker
            .membership()
            .supersedes(replacement.membership())
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test]
async fn nested_actor_replacement_keeps_mailbox_evidence_in_each_exact_membership() {
    let mut nested = DynamicTree::new();
    let declared = nested
        .add_actor("worker", shelterwood::ActorDef::<EvidenceActor>::cloned(()))
        .expect("valid declared actor");
    let mut root = Tree::new();
    let nested_scope = root
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid nested scope");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");

    let declared_incarnation = declared.try_send(()).expect("declared actor accepts");
    assert_eq!(declared_incarnation.membership(), declared.membership());
    assert_eq!(
        nested_scope.remove_actor(&declared).await,
        RemoveOutcome::Removed
    );
    let terminal = declared
        .try_send(())
        .expect_err("the declared handle remains pinned to its removed membership");
    assert_eq!(terminal.kind, SendErrorKind::Terminated);
    assert_eq!(terminal.incarnation_observed, Some(declared_incarnation));

    let replacement = nested_scope
        .add_actor("worker", shelterwood::ActorDef::<EvidenceActor>::cloned(()))
        .await
        .expect("runtime replacement is admitted");
    let replacement_incarnation = replacement.try_send(()).expect("replacement actor accepts");
    assert!(!replacement.membership().supersedes(declared.membership()));
    assert!(!declared.membership().supersedes(replacement.membership()));
    assert_eq!(
        replacement_incarnation.membership(),
        replacement.membership()
    );
    assert!(
        !replacement_incarnation.supersedes(declared_incarnation),
        "incarnation retry order never crosses membership replacement"
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test]
async fn exact_scope_removal_does_not_touch_a_same_id_successor() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let root = system.scope();
    let first = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("first subtree admitted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), root.remove_scope(&first))
            .await
            .expect("first subtree removal completes"),
        RemoveOutcome::Removed
    );
    let second = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("second subtree admitted");
    assert!(!second.membership().supersedes(first.membership()));
    assert!(!first.membership().supersedes(second.membership()));
    assert_eq!(
        root.remove_scope(&first).await,
        RemoveOutcome::AlreadyAbsent
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), root.remove_scope(&second))
            .await
            .expect("second subtree removal completes"),
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn tombstones_occupy_ids_until_explicit_removal() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let task = scope
        .add_task(
            "tombstone",
            TaskDef::new(|_| async { Ok(()) })
                .restart(never())
                .retention(Retention::Retain),
        )
        .await
        .expect("task admitted");
    task.wait().await;
    assert!(matches!(
        scope.add_task("tombstone", waiting_task()).await,
        Err(ReserveError::DuplicateId(ref id)) if id.as_str() == "tombstone"
    ));
    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    let replacement = scope
        .add_task("tombstone", waiting_task())
        .await
        .expect("removal frees id");
    assert_eq!(
        scope.remove_task(&replacement).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn removal_is_synchronous_detached_and_shared() {
    let gate = ReleaseGate::default();
    let cancelled = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let task = scope
        .add_task(
            "worker",
            TaskDef::new({
                let gate = gate.clone();
                let cancelled = Arc::clone(&cancelled);
                move |context| {
                    let gate = gate.clone();
                    let cancelled = Arc::clone(&cancelled);
                    async move {
                        context.shutdown_token().cancelled().await;
                        cancelled.store(true, Ordering::SeqCst);
                        gate.wait().await;
                        Ok(())
                    }
                }
            }),
        )
        .await
        .expect("task admitted");

    let first = scope.remove_task(&task);
    let second = scope.remove_task(&task);
    assert!(matches!(
        scope.reserve_task("worker"),
        Err(ReserveError::RemovalInProgress(ref id)) if id.as_str() == "worker"
    ));
    drop(first);
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    gate.release();
    assert_eq!(second.await, RemoveOutcome::Removed);
    let replacement = scope
        .add_task("worker", waiting_task())
        .await
        .expect("id is free after detached removal");
    assert_eq!(
        scope.remove_task(&replacement).await,
        RemoveOutcome::Removed
    );
    let shared = scope
        .add_task("shared", waiting_task())
        .await
        .expect("shared-removal task admitted");
    let (left, right) = tokio::join!(scope.remove_task(&shared), scope.remove_task(&shared));
    assert_eq!(left, RemoveOutcome::Removed);
    assert_eq!(right, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn reserved_cell_removal_wins_a_queued_split_definition() {
    let factory_dropped = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let slot = scope
        .reserve_task("reserved")
        .expect("reservation succeeds");
    let task = slot.task_ref();
    let removed = scope.remove("reserved");
    let admission = slot.define(TaskDef::new({
        let probe = DropProbe(Arc::clone(&factory_dropped));
        move |context| {
            let _ = &probe;
            async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    }));
    assert_eq!(removed.await, RemoveOutcome::Removed);
    assert!(matches!(
        admission.await,
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::ReservationEnded
        ))
    ));
    assert!(matches!(
        task.wait().await.kind(),
        shelterwood::ExitKind::NeverStarted
    ));
    assert!(factory_dropped.load(Ordering::SeqCst));

    let survivor = scope
        .add_task("survivor", waiting_task())
        .await
        .expect("scope remains admitting");
    assert_eq!(scope.remove_task(&survivor).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[test]
fn admission_runtime_guards_leave_reservation_ids_reusable() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime");
    let (system, scope, task, mut admission) = runtime.block_on(async {
        let system = DynamicTree::new().spawn().expect("runtime is available");
        system.wait_started().await.expect("root starts");
        let scope = system.scope();
        let slot = scope
            .reserve_task("poll-outside")
            .expect("reservation starts inside the runtime");
        let task = slot.task_ref();
        let admission = Box::pin(slot.define(waiting_task()));
        (system, scope, task, admission)
    });

    assert!(matches!(scope.reserve_task(""), Err(ReserveError::EmptyId)));
    assert!(matches!(
        scope.reserve_task("poll-outside"),
        Err(ReserveError::NoRuntime)
    ));
    assert!(matches!(
        scope.reserve_task("reserve-outside"),
        Err(ReserveError::NoRuntime)
    ));
    let mut immediate = Box::pin(scope.add_task("add-outside", waiting_task()));
    assert!(matches!(
        poll_once(immediate.as_mut()),
        std::task::Poll::Ready(Err(ReserveError::NoRuntime))
    ));
    assert!(matches!(
        poll_once(admission.as_mut()),
        std::task::Poll::Ready(Err(ReserveError::NoRuntime))
    ));
    assert!(poll_once(admission.as_mut()).is_pending());

    runtime.block_on(async {
        assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
        for id in ["reserve-outside", "add-outside", "poll-outside"] {
            let task = scope
                .add_task(id, waiting_task())
                .await
                .unwrap_or_else(|error| panic!("id `{id}` remains reusable: {error:?}"));
            assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
        }
        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("root stops");
    });
    assert!(matches!(
        scope.reserve_task("stopped-outside"),
        Err(ReserveError::NoRuntime)
    ));
}

#[tokio::test]
async fn select_and_timeout_preserve_fused_and_split_admission_ownership() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let mut fused = Box::pin(scope.add_task("fused-select", waiting_task()));
    assert!(poll_once(fused.as_mut()).is_pending());
    tokio::select! {
        biased;
        () = std::future::ready(()) => {}
        result = fused => panic!("ready branch must win over admission: {result:?}"),
    }
    let reused = scope
        .add_task("fused-select", waiting_task())
        .await
        .expect("select dropping a fused admission frees its id");
    assert_eq!(scope.remove_task(&reused).await, RemoveOutcome::Removed);

    let split_started = Arc::new(AtomicBool::new(false));
    let split_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope
        .reserve_task("split-timeout")
        .expect("split reservation");
    let task = slot.task_ref();
    let mut split = Box::pin(slot.define(signalled_waiting_task(
        Arc::clone(&split_started),
        Arc::clone(&split_cancelled),
    )));
    assert!(poll_once(split.as_mut()).is_pending());
    let timed_admission = async move {
        let _split = split;
        std::future::pending::<()>().await;
    };
    assert!(
        tokio::time::timeout(Duration::ZERO, timed_admission)
            .await
            .is_err()
    );
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            split_started.load(Ordering::SeqCst)
        })
        .await,
        "timing out a polled split admission detaches the running child"
    );
    assert!(matches!(
        scope.reserve_task("split-timeout"),
        Err(ReserveError::DuplicateId(_))
    ));
    assert_quiet(Duration::from_millis(20), || {
        split_cancelled.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    assert!(split_cancelled.load(Ordering::SeqCst));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn fused_drop_withdraws_or_removes_while_split_drop_detaches() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    drop(scope.add_task("fused-before-poll", waiting_task()));
    let reused = scope
        .add_task("fused-before-poll", waiting_task())
        .await
        .expect("never-polled fused add withdraws");
    assert_eq!(scope.remove_task(&reused).await, RemoveOutcome::Removed);

    let fused_started = Arc::new(AtomicBool::new(false));
    let fused_cancelled = Arc::new(AtomicBool::new(false));
    let mut fused = Box::pin(scope.add_task(
        "fused-after-admission",
        signalled_waiting_task(Arc::clone(&fused_started), Arc::clone(&fused_cancelled)),
    ));
    assert!(poll_once(fused.as_mut()).is_pending());
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            fused_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(fused);
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            fused_cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    wait_for_id_release(&scope, "fused-after-admission").await;
    let reused = scope
        .add_task("fused-after-admission", waiting_task())
        .await
        .expect("post-admission fused drop removes");
    assert_eq!(scope.remove_task(&reused).await, RemoveOutcome::Removed);

    let split_started = Arc::new(AtomicBool::new(false));
    let split_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope.reserve_task("split").expect("split reservation");
    let split_task = slot.task_ref();
    let mut split = Box::pin(slot.define(signalled_waiting_task(
        Arc::clone(&split_started),
        Arc::clone(&split_cancelled),
    )));
    assert!(poll_once(split.as_mut()).is_pending());
    drop(split);
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            split_started.load(Ordering::SeqCst)
        })
        .await
    );
    assert_quiet(Duration::from_millis(20), || {
        split_cancelled.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(scope.remove_task(&split_task).await, RemoveOutcome::Removed);

    let split_after_started = Arc::new(AtomicBool::new(false));
    let split_after_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope
        .reserve_task("split-after-admission")
        .expect("split reservation");
    let split_after_task = slot.task_ref();
    let mut split_after = Box::pin(slot.define(signalled_waiting_task(
        Arc::clone(&split_after_started),
        Arc::clone(&split_after_cancelled),
    )));
    assert!(poll_once(split_after.as_mut()).is_pending());
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            split_after_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(split_after);
    assert_quiet(Duration::from_millis(20), || {
        split_after_cancelled.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(
        scope.remove_task(&split_after_task).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn actor_and_subtree_slots_preserve_fused_and_split_drop_ownership() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let actor_started = Arc::new(AtomicBool::new(false));
    let actor_cancelled = Arc::new(AtomicBool::new(false));
    let mut fused_actor = Box::pin(scope.add_raw(
        "fused-actor",
        RawDef::factory({
            let actor_started = Arc::clone(&actor_started);
            let actor_cancelled = Arc::clone(&actor_cancelled);
            move || SignalledWaitingRaw {
                started: Arc::clone(&actor_started),
                cancelled: Arc::clone(&actor_cancelled),
            }
        }),
    ));
    assert!(poll_once(fused_actor.as_mut()).is_pending());
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            actor_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(fused_actor);
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            actor_cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    wait_for_id_release(&scope, "fused-actor").await;
    let actor = scope
        .add_raw("fused-actor", RawDef::factory(|| WaitingRaw))
        .await
        .expect("fused actor drop frees its id");
    assert_eq!(scope.remove_actor(&actor).await, RemoveOutcome::Removed);

    let subtree_started = Arc::new(AtomicBool::new(false));
    let subtree_cancelled = Arc::new(AtomicBool::new(false));
    let mut fused_subtree = Box::pin(scope.add_subtree_once(
        "fused-subtree",
        SubtreeOnceDef::new(signalled_waiting_tree(
            Arc::clone(&subtree_started),
            Arc::clone(&subtree_cancelled),
        )),
    ));
    assert!(poll_once(fused_subtree.as_mut()).is_pending());
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            subtree_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(fused_subtree);
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            subtree_cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    wait_for_id_release(&scope, "fused-subtree").await;
    let subtree = scope
        .add_subtree_once("fused-subtree", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("fused subtree drop frees its id");
    assert_eq!(scope.remove_scope(&subtree).await, RemoveOutcome::Removed);

    let actor_started = Arc::new(AtomicBool::new(false));
    let actor_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope
        .reserve_actor("split-actor")
        .expect("actor reservation succeeds");
    let actor = slot.actor_ref();
    let mut split_actor = Box::pin(slot.define_once_raw(RawOnceDef::new(SignalledWaitingRaw {
        started: Arc::clone(&actor_started),
        cancelled: Arc::clone(&actor_cancelled),
    })));
    assert!(poll_once(split_actor.as_mut()).is_pending());
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            actor_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(split_actor);
    assert_quiet(Duration::from_millis(20), || {
        actor_cancelled.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(scope.remove_actor(&actor).await, RemoveOutcome::Removed);
    assert!(actor_cancelled.load(Ordering::SeqCst));

    let subtree_started = Arc::new(AtomicBool::new(false));
    let subtree_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope
        .reserve_subtree::<Tree>("split-subtree")
        .expect("subtree reservation succeeds");
    let subtree = slot.scope_ref();
    let mut split_subtree = Box::pin(slot.define_once(SubtreeOnceDef::new(
        signalled_waiting_tree(Arc::clone(&subtree_started), Arc::clone(&subtree_cancelled)),
    )));
    assert!(poll_once(split_subtree.as_mut()).is_pending());
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            subtree_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(split_subtree);
    assert_quiet(Duration::from_millis(20), || {
        subtree_cancelled.load(Ordering::SeqCst)
    })
    .await;
    assert_eq!(scope.remove_scope(&subtree).await, RemoveOutcome::Removed);
    assert!(subtree_cancelled.load(Ordering::SeqCst));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn removing_a_member_releases_its_factory_before_scope_shutdown() {
    let started = Arc::new(AtomicBool::new(false));
    let factory_dropped = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let task = scope
        .add_task(
            "worker",
            TaskDef::new({
                let started = Arc::clone(&started);
                let probe = DropProbe(Arc::clone(&factory_dropped));
                move |context| {
                    let _ = &probe;
                    started.store(true, Ordering::SeqCst);
                    async move {
                        context.shutdown_token().cancelled().await;
                        Ok(())
                    }
                }
            }),
        )
        .await
        .expect("task admitted");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            started.load(Ordering::SeqCst)
        })
        .await
    );

    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    assert!(factory_dropped.load(Ordering::SeqCst));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn pre_spawn_shutdown_waits_for_teardown_to_exist() {
    let gate = ReleaseGate::default();
    let mut nested = Tree::new();
    nested
        .add_task(
            "worker",
            TaskDef::new({
                let gate = gate.clone();
                move |context| {
                    let gate = gate.clone();
                    async move {
                        context.shutdown_token().cancelled().await;
                        gate.wait().await;
                        Ok(())
                    }
                }
            }),
        )
        .expect("valid task");
    let mut root = Tree::new();
    let slot = root.reserve_subtree::<Tree>("nested").expect("reservation");
    let scope = slot.scope_ref();
    let mut waiter = Box::pin(scope.shutdown_and_wait(Duration::from_millis(10)));
    assert!(
        poll_once(waiter.as_mut()).is_pending(),
        "the pre-spawn shutdown request registers without completing"
    );
    assert_quiet(Duration::from_millis(25), || {
        poll_once(waiter.as_mut()).is_ready()
    })
    .await;
    let _nested_handle = slot.define_once(SubtreeOnceDef::new(nested));
    let system = root.spawn().expect("runtime is available");
    let timeout = waiter.await.expect_err("live teardown exceeds its bound");
    assert_eq!(timeout.stragglers.len(), 1);
    assert_eq!(timeout.stragglers[0].path[0].as_str(), "worker");
    gate.release();
    assert_eq!(scope.wait_stopped().await, StopReason::ShutdownRequested);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn pre_spawn_shutdown_resolves_if_the_tree_is_dropped_unspawned() {
    let mut root = Tree::new();
    let slot = root.reserve_subtree::<Tree>("nested").expect("reservation");
    let scope = slot.scope_ref();
    let wait_scope = scope.clone();
    let waiter =
        tokio::spawn(async move { wait_scope.shutdown_and_wait(Duration::from_millis(1)).await });
    drop(slot);
    drop(root);
    waiter
        .await
        .expect("waiter joins")
        .expect("already stopped");
    assert_eq!(scope.wait_stopped().await, StopReason::NeverStarted);
}

#[tokio::test]
async fn pre_spawn_dynamic_handles_reject_reservations_without_a_live_incarnation() {
    let mut root = Tree::new();
    let slot = root
        .reserve_subtree::<DynamicTree>("dynamic")
        .expect("reservation");
    let scope = slot.scope_ref();
    assert!(matches!(
        scope.reserve_task("too-early"),
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::NoLiveIncarnation
        ))
    ));
    drop(slot);
    drop(root);
    assert_eq!(scope.wait_stopped().await, StopReason::NeverStarted);
}

#[tokio::test]
async fn dropping_undefined_dynamic_slots_terminalizes_cells_and_frees_ids() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let actor_slot = scope
        .reserve_actor::<()>("actor")
        .expect("actor reservation");
    let abandoned_actor = actor_slot.actor_ref();
    drop(actor_slot);
    assert_eq!(
        abandoned_actor
            .send(())
            .await
            .expect_err("abandoned actor is terminal")
            .kind,
        SendErrorKind::Terminated
    );

    let task_slot = scope.reserve_task("worker").expect("task reservation");
    let abandoned_task = task_slot.task_ref();
    drop(task_slot);
    assert!(matches!(
        abandoned_task.wait().await.kind(),
        ExitKind::NeverStarted
    ));

    let subtree_slot = scope
        .reserve_subtree::<Tree>("nested")
        .expect("subtree reservation");
    let abandoned_scope = subtree_slot.scope_ref();
    drop(subtree_slot);
    assert_eq!(
        abandoned_scope.wait_stopped().await,
        StopReason::NeverStarted
    );

    let worker = scope
        .add_task("worker", waiting_task())
        .await
        .expect("task id was released");
    assert_eq!(scope.remove_task(&worker).await, RemoveOutcome::Removed);
    let actor = scope
        .add_raw("actor", RawDef::factory(|| WaitingRaw))
        .await
        .expect("actor id was released");
    assert_eq!(scope.remove_actor(&actor).await, RemoveOutcome::Removed);
    let nested = scope
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("subtree id was released");
    assert_eq!(scope.remove_scope(&nested).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn dynamic_scope_rejects_reservations_between_incarnations() {
    let factories = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(AtomicBool::new(false));
    let width = Duration::from_secs(10);
    let subtree = SubtreeDef::factory({
        let factories = Arc::clone(&factories);
        let first_started = Arc::clone(&first_started);
        move || {
            let generation = factories.fetch_add(1, Ordering::SeqCst) + 1;
            let mut tree = DynamicTree::new();
            if generation == 1 {
                tree.add_task(
                    "fails-before-ready",
                    TaskDef::new({
                        let first_started = Arc::clone(&first_started);
                        move |_| {
                            first_started.store(true, Ordering::SeqCst);
                            async { Err(ExitError::message("first incarnation fails startup")) }
                        }
                    })
                    .restart(never())
                    .readiness(Readiness::Manual)
                    .expect("manual readiness"),
                )
                .expect("valid task");
            } else {
                tree.add_task("worker", waiting_task()).expect("valid task");
            }
            tree
        }
    })
    .restart(RestartPolicy::new(
        RestartCondition::OnFailure,
        Backoff::fixed(width, shelterwood::Jitter::None).expect("non-zero backoff"),
    ));
    let mut root = Tree::new();
    let scope = root.add_subtree("dynamic", subtree).expect("valid subtree");
    let system = root.spawn().expect("runtime is available");

    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            first_started.load(Ordering::SeqCst)
        })
        .await
    );
    system
        .scope()
        .wait_for_child(
            "dynamic",
            |child| matches!(child.state, ChildState::Restarting),
            Duration::MAX,
        )
        .await
        .expect("the failed incarnation enters its explicit restart window");
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    assert!(matches!(
        scope.reserve_task("too-early"),
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::NoLiveIncarnation
        ))
    ));

    advance_time(width).await;
    system
        .wait_started()
        .await
        .expect("replacement incarnation starts");
    assert_eq!(factories.load(Ordering::SeqCst), 2);
    let runtime_task = scope
        .add_task("runtime", waiting_task())
        .await
        .expect("the replacement incarnation admits");
    assert_eq!(
        scope.remove_task(&runtime_task).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

fn pending_restart_subtree(
    width: Duration,
    factories: Arc<AtomicUsize>,
    starts: Arc<AtomicUsize>,
) -> SubtreeDef<Tree> {
    SubtreeDef::factory({
        move || {
            let generation = factories.fetch_add(1, Ordering::SeqCst) + 1;
            let mut tree = Tree::new();
            if generation == 1 {
                let (_task, _completion) = tree
                    .add_task_once(
                        "worker",
                        TaskOnceDef::new({
                            let starts = Arc::clone(&starts);
                            move |_| async move {
                                starts.fetch_add(1, Ordering::SeqCst);
                                Ok::<_, ExitError>(())
                            }
                        }),
                    )
                    .expect("valid task");
            } else {
                tree.add_task(
                    "worker",
                    TaskDef::new({
                        let starts = Arc::clone(&starts);
                        move |context| {
                            let starts = Arc::clone(&starts);
                            async move {
                                starts.fetch_add(1, Ordering::SeqCst);
                                context.shutdown_token().cancelled().await;
                                Ok(())
                            }
                        }
                    }),
                )
                .expect("valid task");
            }
            tree
        }
    })
    .restart(RestartPolicy::new(
        RestartCondition::Always,
        Backoff::fixed(width, shelterwood::Jitter::None).expect("non-zero backoff"),
    ))
}

async fn await_first_restart_window(root: &ScopeRef, starts: &Arc<AtomicUsize>) {
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) == 1
        })
        .await
    );
    root.wait_for_child(
        "nested",
        |child| matches!(child.state, ChildState::Restarting),
        Duration::MAX,
    )
    .await
    .expect("the first incarnation enters its explicit restart window");
}

async fn pending_restart_fixture(
    width: Duration,
) -> (
    System<shelterwood::DynamicScopeRef>,
    ScopeRef,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let factories = Arc::new(AtomicUsize::new(0));
    let starts = Arc::new(AtomicUsize::new(0));
    let subtree = pending_restart_subtree(width, Arc::clone(&factories), Arc::clone(&starts));
    let mut root = DynamicTree::new();
    let nested = root.add_subtree("nested", subtree).expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("first incarnation starts");
    let root_scope = system.scope();
    await_first_restart_window(root_scope.as_scope(), &starts).await;

    (system, nested, factories, starts)
}

/// The ordered-parent twin of [`pending_restart_fixture`]. Restart suppression,
/// ordered startup progression and reverse teardown all differ from the dynamic
/// flavor, so the pending-incarnation stop needs its own coverage here.
async fn ordered_pending_restart_fixture(
    width: Duration,
) -> (
    System<ScopeRef>,
    ScopeRef,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let factories = Arc::new(AtomicUsize::new(0));
    let starts = Arc::new(AtomicUsize::new(0));
    let subtree = pending_restart_subtree(width, Arc::clone(&factories), Arc::clone(&starts));
    let mut root = Tree::new();
    let nested = root.add_subtree("nested", subtree).expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("first incarnation starts");
    let root_scope = system.scope();
    await_first_restart_window(&root_scope, &starts).await;

    (system, nested, factories, starts)
}

async fn assert_pending_restart_shutdown_is_expedited<R: Clone>(
    width: Duration,
    system: System<R>,
    nested: ScopeRef,
    factories: Arc<AtomicUsize>,
    starts: Arc<AtomicUsize>,
) {
    tokio::time::timeout(
        Duration::from_secs(1),
        nested.shutdown_and_wait(Duration::from_secs(1)),
    )
    .await
    .expect("shutdown does not wait for the pending restart deadline")
    .expect("the pending incarnation stops cooperatively");
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    assert_eq!(
        factories.load(Ordering::SeqCst),
        2,
        "shutdown must start exactly the pending incarnation without waiting for backoff"
    );
    if width == Duration::MAX {
        // An unrepresentable deadline has no substitute and never arrives,
        // so there is no later incarnation to reach.
        // Only the quiet window applies.
        advance_time(Duration::from_secs(1)).await;
        assert_eq!(
            starts.load(Ordering::SeqCst),
            2,
            "an unrepresentable deadline must not resurrect the stopped incarnation"
        );
        system.shutdown(Duration::ZERO).await.expect("root stops");
        return;
    }

    // The expedited incarnation exited cooperatively, so `Always` schedules an
    // ordinary backoff restart. Cross the whole window: a storm assertion that
    // never reaches a *later* incarnation only re-measures the quiet interior
    // of the window it already observed.
    advance_time(width + Duration::from_secs(1)).await;
    assert!(
        poll_until(Duration::from_secs(5), Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) >= 3
        })
        .await,
        "a consumed pending request must not suppress the ordinary backoff restart"
    );
    assert_eq!(
        starts.load(Ordering::SeqCst),
        3,
        "exactly one incarnation follows the expedited stop"
    );
    assert_eq!(
        factories.load(Ordering::SeqCst),
        3,
        "the backoff restart re-lowers the subtree exactly once"
    );
    advance_time(width * 3).await;
    assert_quiet(Duration::from_secs(1), || {
        starts.load(Ordering::SeqCst) != 3
    })
    .await;
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn pending_restart_shutdown_expedites_finite_and_unrepresentable_backoff() {
    for width in [Duration::from_secs(60 * 60), Duration::MAX] {
        let (system, nested, factories, starts) = pending_restart_fixture(width).await;
        assert_pending_restart_shutdown_is_expedited(width, system, nested, factories, starts)
            .await;
    }
}

#[tokio::test(start_paused = true)]
async fn ordered_parent_pending_restart_shutdown_is_expedited() {
    let width = Duration::from_secs(60 * 60);
    let (system, nested, factories, starts) = ordered_pending_restart_fixture(width).await;
    assert_pending_restart_shutdown_is_expedited(width, system, nested, factories, starts).await;
}

#[tokio::test(start_paused = true)]
async fn same_batch_removal_suppresses_pending_restart_shutdown() {
    let (system, nested, factories, starts) =
        pending_restart_fixture(Duration::from_secs(60 * 60)).await;

    // Both level-triggered commands are latched before yielding to the
    // driver. Removal owns restart suppression even though the nested scope's
    // pending shutdown would otherwise expedite its next incarnation.
    let removal = system.scope().remove_scope(&nested);
    nested.request_shutdown();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), removal)
            .await
            .expect("removal does not wait for restart backoff"),
        RemoveOutcome::Removed
    );
    assert_eq!(factories.load(Ordering::SeqCst), 1);
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    system.shutdown(Duration::ZERO).await.expect("root stops");
}

#[tokio::test]
async fn draining_scopes_reject_admission_and_treat_removal_as_absent() {
    let gate = ReleaseGate::default();
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut tree = DynamicTree::new();
    tree.add_task(
        "worker",
        TaskDef::new({
            let gate = gate.clone();
            let cancelled = Arc::clone(&cancelled);
            move |context| {
                let gate = gate.clone();
                let cancelled = Arc::clone(&cancelled);
                async move {
                    context.shutdown_token().cancelled().await;
                    cancelled.store(true, Ordering::SeqCst);
                    gate.wait().await;
                    Ok(())
                }
            }
        })
        .shutdown(Shutdown::Graceful {
            grace: Duration::from_secs(1),
        }),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let shutdown = tokio::spawn(system.shutdown(Duration::from_secs(2)));
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    assert!(matches!(
        scope.add_task("late", waiting_task()).await,
        Err(ReserveError::NotAdmitting(NotAdmittingCause::Draining))
    ));
    assert_eq!(scope.remove("worker").await, RemoveOutcome::AlreadyAbsent);
    gate.release();
    shutdown
        .await
        .expect("shutdown joins")
        .expect("clean shutdown");
}

#[tokio::test]
async fn removal_of_a_polled_split_definition_keeps_the_scope_admitting() {
    let factory_dropped = Arc::new(AtomicBool::new(false));
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let slot = scope.reserve_task("worker").expect("reservation succeeds");
    let task = slot.task_ref();
    let mut admission = Box::pin(slot.define(TaskDef::new({
        let probe = DropProbe(Arc::clone(&factory_dropped));
        move |context| {
            let _ = &probe;
            async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }
        }
    })));
    // The first poll queues the admission with the driver; the removal then
    // lands before the driver dequeues it — §13.12's mandated race, which
    // must resolve `ReservationEnded` while the scope keeps admitting.
    assert!(poll_once(admission.as_mut()).is_pending());
    assert_eq!(scope.remove("worker").await, RemoveOutcome::Removed);
    assert!(matches!(
        admission.await,
        Err(ReserveError::NotAdmitting(
            NotAdmittingCause::ReservationEnded
        ))
    ));
    assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    assert!(factory_dropped.load(Ordering::SeqCst));

    let survivor = scope
        .add_task("worker", waiting_task())
        .await
        .expect("the scope keeps admitting and the id is free");
    assert_eq!(scope.remove_task(&survivor).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn ancestor_hard_abort_disposes_a_queued_admission_and_midflight_removal() {
    let worker_started = ReleaseGate::default();
    let worker_cancelled = ReleaseGate::default();
    let queued_disposed = ReleaseGate::default();
    let mut nested = DynamicTree::new();
    let worker = nested
        .add_task(
            "worker",
            TaskDef::new({
                let worker_started = worker_started.clone();
                let worker_cancelled = worker_cancelled.clone();
                move |context| {
                    let worker_started = worker_started.clone();
                    let worker_cancelled = worker_cancelled.clone();
                    async move {
                        worker_started.release();
                        context.shutdown_token().cancelled().await;
                        worker_cancelled.release();
                        std::future::pending::<ExitResult>().await
                    }
                }
            })
            .shutdown(Shutdown::Graceful {
                grace: Duration::from_secs(60),
            }),
        )
        .expect("valid worker");
    let mut root = Tree::new();
    let nested = root
        .add_subtree_once(
            "nested",
            SubtreeOnceDef::new(nested).shutdown(Shutdown::Abort),
        )
        .expect("valid dynamic subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("nested scope starts");
    worker_started.wait().await;

    let mut removal = Box::pin(nested.remove_task(&worker));
    worker_cancelled.wait().await;
    assert!(
        poll_once(removal.as_mut()).is_pending(),
        "the worker keeps exact removal in flight"
    );

    let slot = nested.reserve_task("queued").expect("reservation succeeds");
    let queued = slot.task_ref();
    let mut admission = Box::pin(
        slot.define(
            TaskDef::new({
                let drop_signal = DropSignal(queued_disposed.clone());
                move |_| {
                    let _ = &drop_signal;
                    std::future::pending::<ExitResult>()
                }
            })
            .shutdown(Shutdown::Abort),
        ),
    );
    assert!(
        poll_once(admission.as_mut()).is_pending(),
        "first poll owns a queued admission request before yielding"
    );

    let mut shutdown = Box::pin(system.shutdown(Duration::from_secs(1)));
    assert!(
        poll_once(shutdown.as_mut()).is_pending(),
        "ancestor shutdown is armed before queued work gets a scheduler turn"
    );

    match admission.await {
        Ok(task) => {
            assert_eq!(task.membership(), queued.membership());
            drop(task);
        }
        Err(ReserveError::NotAdmitting(_)) => {}
        Err(error) => panic!("unexpected queued-admission result: {error:?}"),
    }
    shutdown
        .await
        .expect("the aborting subtree recursively joins its descendants");
    queued_disposed.wait().await;
    assert_eq!(removal.await, RemoveOutcome::Removed);
    assert!(matches!(
        worker.wait().await.kind(),
        ExitKind::Aborted { .. }
    ));
    assert!(matches!(
        queued.wait().await.kind(),
        ExitKind::Aborted { .. } | ExitKind::NeverStarted
    ));
}

#[tokio::test]
async fn admissions_return_kind_specific_handles_directly() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let actor_slot = scope
        .reserve_actor::<()>("actor")
        .expect("actor is reserved");
    let reserved_actor = actor_slot.actor_ref();
    let actor = actor_slot
        .define(shelterwood::ActorDef::<EvidenceActor>::cloned(()))
        .await
        .expect("actor is admitted");
    assert_eq!(actor.membership(), reserved_actor.membership());

    let task_slot = scope.reserve_task("task").expect("task is reserved");
    let reserved_task = task_slot.task_ref();
    let task = task_slot
        .define(waiting_task())
        .await
        .expect("task is admitted");
    assert_eq!(task.membership(), reserved_task.membership());

    let one_shot_slot = scope
        .reserve_task("one-shot")
        .expect("one-shot task is reserved");
    let reserved_one_shot = one_shot_slot.task_ref();
    let (one_shot, completion) = one_shot_slot
        .define_once(TaskOnceDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok::<_, ExitError>(())
        }))
        .await
        .expect("one-shot task is admitted");
    assert_eq!(one_shot.membership(), reserved_one_shot.membership());
    drop(completion);

    let subtree_slot = scope
        .reserve_subtree::<Tree>("subtree")
        .expect("subtree is reserved");
    let reserved_subtree = subtree_slot.scope_ref();
    let subtree = subtree_slot
        .define_once(SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("subtree is admitted");
    assert_eq!(subtree.membership(), reserved_subtree.membership());

    assert_eq!(scope.remove_actor(&actor).await, RemoveOutcome::Removed);
    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    assert_eq!(scope.remove_task(&one_shot).await, RemoveOutcome::Removed);
    assert_eq!(scope.remove_scope(&subtree).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

/// A root whose initial child fails before its readiness edge publishes
/// `StartupFailed` and keeps the started prefix supervised (§6); reservation
/// and admission on that scope both surface the dedicated cause.
#[tokio::test]
async fn startup_failed_roots_reject_reservation_and_admission_with_startup_failed() {
    let mut tree = DynamicTree::new();
    tree.add_task(
        "failing-readiness",
        TaskDef::new(|_| async { Err(ExitError::message("fails before ready")) })
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .restart(never()),
    )
    .expect("valid failing task");
    tree.add_task("survivor", waiting_task())
        .expect("valid surviving task");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    system
        .wait_started()
        .await
        .expect_err("pre-ready terminal exit aborts startup");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            matches!(scope.snapshot().state, ScopeState::StartupFailed)
        })
        .await,
        "the root publishes StartupFailed while the started prefix remains supervised"
    );

    assert!(matches!(
        scope.reserve_task("late"),
        Err(ReserveError::NotAdmitting(NotAdmittingCause::StartupFailed))
    ));
    assert!(matches!(
        scope.add_task("late", waiting_task()).await,
        Err(ReserveError::NotAdmitting(NotAdmittingCause::StartupFailed))
    ));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed startup rolls back");
}

/// After the owner completes shutdown the scope membership is terminal;
/// reservation and admission both surface `Terminal` rather than a
/// live-incarnation cause.
#[tokio::test]
async fn terminal_scopes_reject_reservation_and_admission_with_terminal() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
    assert_eq!(scope.wait_stopped().await, StopReason::ShutdownRequested);

    assert!(matches!(
        scope.reserve_task("late"),
        Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal))
    ));
    assert!(matches!(
        scope.add_task("late", waiting_task()).await,
        Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal))
    ));
}
