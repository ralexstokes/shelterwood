use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    ReleaseGate, advance_time, assert_quiet,
    policy::never,
    poll_once, poll_until,
    waiting::{task as waiting_task, tree as waiting_tree},
};
use shelterwood::{
    Actor, ActorOnceDef, Backoff, ChildState, Context as ActorContext, DynamicTree, ExitError,
    ExitKind, ExitResult, NotAdmittingCause, RawActor, RawContext, RawDef, Readiness,
    RemoveOutcome, ReserveError, RestartCondition, RestartPolicy, Retention, SendErrorKind,
    Shutdown, StopReason, SubtreeDef, SubtreeOnceDef, TaskDef, TaskOnceDef, Tree,
};

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
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

#[tokio::test]
async fn restartable_dynamic_surfaces_are_parallel_across_all_three_child_kinds() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let task_receipt = scope
        .add_task("task", waiting_task())
        .await
        .expect("restartable task is admitted");
    let task = task_receipt.into_handles();
    let raw_receipt = scope
        .add_raw("raw", RawDef::factory(|| WaitingRaw))
        .await
        .expect("restartable raw actor is admitted");
    let raw = raw_receipt.into_handles();
    let subtree_receipt = scope
        .add_subtree("subtree", SubtreeDef::factory(waiting_tree))
        .await
        .expect("restartable subtree is admitted");
    let subtree = subtree_receipt.into_handles();

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
    let receipt = tokio::time::timeout(
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
    let actor = receipt.into_handles();
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
        .expect("left admission")
        .into_handles();
    let right_task = right_scope
        .add_task("same", waiting_task())
        .await
        .expect("right admission")
        .into_handles();

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
        .expect("replacement admission")
        .into_handles();
    assert!(replacement.membership().supersedes(left_task.membership()));
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
async fn nested_declared_membership_is_superseded_by_its_runtime_replacement() {
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
        .expect("runtime replacement is admitted")
        .into_handles();

    assert!(replacement.membership().supersedes(declared_membership));
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
        .expect("runtime replacement is admitted")
        .into_handles();
    let replacement_incarnation = replacement.try_send(()).expect("replacement actor accepts");
    assert!(replacement.membership().supersedes(declared.membership()));
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
        .expect("first subtree admitted")
        .into_handles();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), root.remove_scope(&first))
            .await
            .expect("first subtree removal completes"),
        RemoveOutcome::Removed
    );
    let second = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("second subtree admitted")
        .into_handles();
    assert!(second.membership().supersedes(first.membership()));
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
        .expect("task admitted")
        .into_handles();
    task.wait().await;
    assert!(matches!(
        scope.add_task("tombstone", waiting_task()).await,
        Err(ReserveError::DuplicateId(ref id)) if id.as_str() == "tombstone"
    ));
    assert_eq!(scope.remove_task(&task).await, RemoveOutcome::Removed);
    let replacement = scope
        .add_task("tombstone", waiting_task())
        .await
        .expect("removal frees id")
        .into_handles();
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
        .expect("task admitted")
        .into_handles();

    let first = scope.remove_task(&task);
    let second = scope.remove_task(&task);
    assert!(matches!(
        scope.reserve_task("worker"),
        Err(ReserveError::RemovalInProgress(ref id)) if id.as_str() == "worker"
    ));
    drop(first);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    gate.release();
    assert_eq!(second.await, RemoveOutcome::Removed);
    let replacement = scope
        .add_task("worker", waiting_task())
        .await
        .expect("id is free after detached removal")
        .into_handles();
    assert_eq!(
        scope.remove_task(&replacement).await,
        RemoveOutcome::Removed
    );
    let shared = scope
        .add_task("shared", waiting_task())
        .await
        .expect("shared-removal task admitted")
        .into_handles();
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
        .expect("scope remains admitting")
        .into_handles();
    assert_eq!(scope.remove_task(&survivor).await, RemoveOutcome::Removed);
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
        .expect("never-polled fused add withdraws")
        .into_handles();
    assert_eq!(scope.remove_task(&reused).await, RemoveOutcome::Removed);

    let fused_started = Arc::new(AtomicBool::new(false));
    let fused_cancelled = Arc::new(AtomicBool::new(false));
    let mut fused = Box::pin(scope.add_task(
        "fused-after-admission",
        TaskDef::new({
            let fused_started = Arc::clone(&fused_started);
            let fused_cancelled = Arc::clone(&fused_cancelled);
            move |context| {
                let fused_started = Arc::clone(&fused_started);
                let fused_cancelled = Arc::clone(&fused_cancelled);
                async move {
                    fused_started.store(true, Ordering::SeqCst);
                    context.shutdown_token().cancelled().await;
                    fused_cancelled.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        }),
    ));
    assert!(poll_once(fused.as_mut()).is_pending());
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            fused_started.load(Ordering::SeqCst)
        })
        .await
    );
    drop(fused);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            fused_cancelled.load(Ordering::SeqCst)
        })
        .await
    );
    let reused = scope
        .add_task("fused-after-admission", waiting_task())
        .await
        .expect("post-admission fused drop removes")
        .into_handles();
    assert_eq!(scope.remove_task(&reused).await, RemoveOutcome::Removed);

    let split_started = Arc::new(AtomicBool::new(false));
    let split_cancelled = Arc::new(AtomicBool::new(false));
    let slot = scope.reserve_task("split").expect("split reservation");
    let split_task = slot.task_ref();
    let mut split = Box::pin(slot.define(TaskDef::new({
        let split_started = Arc::clone(&split_started);
        let split_cancelled = Arc::clone(&split_cancelled);
        move |context| {
            let split_started = Arc::clone(&split_started);
            let split_cancelled = Arc::clone(&split_cancelled);
            async move {
                split_started.store(true, Ordering::SeqCst);
                context.shutdown_token().cancelled().await;
                split_cancelled.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    })));
    assert!(poll_once(split.as_mut()).is_pending());
    drop(split);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
    let mut split_after = Box::pin(slot.define(TaskDef::new({
        let split_after_started = Arc::clone(&split_after_started);
        let split_after_cancelled = Arc::clone(&split_after_cancelled);
        move |context| {
            let split_after_started = Arc::clone(&split_after_started);
            let split_after_cancelled = Arc::clone(&split_after_cancelled);
            async move {
                split_after_started.store(true, Ordering::SeqCst);
                context.shutdown_token().cancelled().await;
                split_after_cancelled.store(true, Ordering::SeqCst);
                Ok(())
            }
        }
    })));
    assert!(poll_once(split_after.as_mut()).is_pending());
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
        .expect("task admitted")
        .into_handles();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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

#[tokio::test]
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
    let scope_for_wait = scope.clone();
    let waiter = tokio::spawn(async move {
        scope_for_wait
            .shutdown_and_wait(Duration::from_millis(10))
            .await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !waiter.is_finished(),
        "timeout must not arm before an incarnation starts"
    );
    let _nested_handle = slot.define_once(SubtreeOnceDef::new(nested));
    let system = root.spawn().expect("runtime is available");
    let timeout = waiter
        .await
        .expect("waiter joins")
        .expect_err("live teardown exceeds its bound");
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
        .expect("task id was released")
        .into_handles();
    assert_eq!(scope.remove_task(&worker).await, RemoveOutcome::Removed);
    let nested = scope
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("subtree id was released")
        .into_handles();
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
        .expect("the replacement incarnation admits")
        .into_handles();
    assert_eq!(
        scope.remove_task(&runtime_task).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn pending_restart_window_stop_targets_the_next_incarnation_once() {
    let factories = Arc::new(AtomicUsize::new(0));
    let starts = Arc::new(AtomicUsize::new(0));
    let width = Duration::from_secs(10);
    let subtree = SubtreeDef::factory({
        let factories = Arc::clone(&factories);
        let starts = Arc::clone(&starts);
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
    ));
    let mut root = Tree::new();
    let nested = root.add_subtree("nested", subtree).expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("first incarnation starts");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            starts.load(Ordering::SeqCst) == 1
        })
        .await
    );
    system
        .scope()
        .wait_for_child(
            "nested",
            |child| matches!(child.state, ChildState::Restarting),
            Duration::MAX,
        )
        .await
        .expect("the first incarnation enters its explicit restart window");

    let nested_wait = nested.clone();
    let stop_started = ReleaseGate::default();
    let stop = tokio::spawn({
        let stop_started = stop_started.clone();
        async move {
            stop_started.release();
            nested_wait.shutdown_and_wait(Duration::from_secs(1)).await
        }
    });
    stop_started.wait().await;
    assert_quiet(Duration::from_secs(1), || stop.is_finished()).await;
    advance_time(width).await;
    stop.await
        .expect("stop waiter joins")
        .expect("next incarnation cooperates");
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    advance_time(width / 2).await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "a consumed per-incarnation latch must not cause a stop/restart storm"
    );
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
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
        .expect("the scope keeps admitting and the id is free")
        .into_handles();
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
    let mut admission =
        Box::pin(slot.define(
            TaskDef::new(|_| std::future::pending::<ExitResult>()).shutdown(Shutdown::Abort),
        ));
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
        Ok(receipt) => {
            assert_eq!(receipt.membership(), queued.membership());
            drop(receipt);
        }
        Err(ReserveError::NotAdmitting(_)) => {}
        Err(error) => panic!("unexpected queued-admission result: {error:?}"),
    }
    shutdown
        .await
        .expect("the aborting subtree recursively joins its descendants");
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
