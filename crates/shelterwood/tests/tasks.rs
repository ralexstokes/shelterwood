use std::time::Duration;

use crate::common::LiveFlag;
use shelterwood::{
    BuildError, DynamicTree, ExitKind, Readiness, RemoveOutcome, StopReason, TaskDef, TaskOnceDef,
    Tree,
};

#[test]
fn spawn_without_runtime_is_a_build_error() {
    assert!(matches!(Tree::new().spawn(), Err(BuildError::NoRuntime)));
}

#[test]
fn declaration_errors_are_eager_and_root_lowering_is_the_only_other_build_error() {
    let mut tree = Tree::new();
    assert!(matches!(
        tree.reserve_task(""),
        Err(shelterwood::ReserveError::EmptyId)
    ));
    let slot = tree.reserve_task("duplicate").expect("first id is free");
    assert!(matches!(
        tree.reserve_task("duplicate"),
        Err(shelterwood::ReserveError::DuplicateId(ref id)) if id.as_str() == "duplicate"
    ));
    let task = slot.task_ref();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");
    runtime.block_on(async move {
        assert!(matches!(
            tree.spawn(),
            Err(BuildError::UnfilledReservations { ref paths })
                if paths.len() == 1 && paths[0][0].as_str() == "duplicate"
        ));
        assert!(matches!(task.wait().await.kind(), ExitKind::NeverStarted));
    });
}

#[tokio::test]
async fn one_shot_completion_reports_never_started_for_an_unspawned_membership() {
    let mut tree = Tree::new();
    let (_task, completion) = tree
        .add_task_once(
            "never",
            TaskOnceDef::new(|_| async { Ok::<_, shelterwood::ExitError>(1) }),
        )
        .expect("valid task");
    drop(tree);
    assert!(matches!(
        completion.wait().await.expect_err("task never ran").kind(),
        ExitKind::NeverStarted
    ));
}

#[tokio::test]
async fn dynamic_add_resolves_at_admission_and_removal_is_exact() {
    let tree = DynamicTree::new();
    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("empty dynamic root starts");
    let scope = system.scope();

    let definition = TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    })
    .readiness(Readiness::Manual)
    .expect("manual task readiness is valid");
    let receipt = scope
        .add_task("worker", definition)
        .await
        .expect("admission succeeds before readiness");
    let first = receipt.into_handles();
    let removal = scope.remove_task(&first);
    drop(removal);
    let first_exit = tokio::time::timeout(Duration::from_secs(1), first.wait())
        .await
        .expect("first removal completes");
    assert!(matches!(first_exit.kind(), ExitKind::Completed));

    let second = scope
        .add_task_once(
            "worker",
            TaskOnceDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok::<_, shelterwood::ExitError>(7_u8)
            }),
        )
        .await
        .expect("same id is free after detached removal")
        .into_handles()
        .0;
    assert_eq!(
        scope.remove_task(&first).await,
        RemoveOutcome::AlreadyAbsent
    );
    assert_eq!(scope.remove_task(&second).await, RemoveOutcome::Removed);
    assert_eq!(system.shutdown(Duration::from_secs(1)).await, Ok(()));
}

#[tokio::test]
async fn one_shot_completion_finishes_an_ordered_root() {
    let mut tree = Tree::new();
    let (task, completion) = tree
        .add_task_once("work", TaskOnceDef::new(|_| async { Ok(42_u64) }))
        .expect("valid declaration");
    let system = tree.spawn().expect("runtime is available");

    system.wait_started().await.expect("tree starts");
    assert_eq!(completion.wait().await.expect("task completed"), 42);
    assert!(matches!(task.wait().await.kind(), ExitKind::Completed));
    assert_eq!(system.wait().await, StopReason::Finished);
}

#[tokio::test]
async fn dropping_the_owner_requests_cooperative_shutdown() {
    let mut tree = Tree::new();
    let (live, guard) = LiveFlag::guarded();
    let (task, completion) = tree
        .add_task_once(
            "worker",
            TaskOnceDef::new(move |context| async move {
                let _guard = guard;
                context.shutdown_token().cancelled().await;
                Ok::<_, shelterwood::ExitError>(())
            }),
        )
        .expect("valid declaration");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    drop(system);
    let exit = task.wait().await;
    assert!(exit.cancelled());
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(completion.wait().await, Ok(()));
    tokio::time::timeout(Duration::from_secs(1), async {
        while live.is_live() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task future dropped");
}
