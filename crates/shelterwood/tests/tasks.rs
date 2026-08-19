mod common;

use std::time::Duration;

use crate::common::{LiveFlag, assert_eventually};
use shelterwood::{
    BuildError, Cancellation, DynamicTree, Exit, ExitError, ExitKind, GracePhase, PolicyError,
    Readiness, ReadinessDeadline, RemoveOutcome, ReserveError, Shutdown, StopReason, TaskDef,
    TaskOnceDef, Tree,
};

#[test]
fn task_families_reject_after_init_readiness_eagerly() {
    let restartable = TaskDef::new(|_| async { Ok(()) })
        .readiness(Readiness::AfterInit)
        .expect_err("restartable tasks have no init phase");
    assert_eq!(restartable, PolicyError::UnsupportedReadiness);

    let one_shot = TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) })
        .readiness(Readiness::AfterInit)
        .expect_err("one-shot tasks have no init phase");
    assert_eq!(one_shot, PolicyError::UnsupportedReadiness);
}

#[test]
fn public_exit_constructor_preserves_evidence_and_classifies_failures() {
    let completed = Exit::new(ExitKind::Completed, Cancellation::Observed);
    assert_eq!(completed.cancellation(), Cancellation::Observed);
    assert!(matches!(completed.kind(), ExitKind::Completed));
    assert!(!completed.is_failure());

    for kind in [
        ExitKind::Failed(ExitError::message("failed")),
        ExitKind::Panicked {
            message: Some("panicked".to_owned()),
        },
        ExitKind::ReadinessTimedOut {
            deadline: std::time::Instant::now(),
        },
        ExitKind::Aborted {
            phase: GracePhase::AfterGrace,
        },
        ExitKind::NeverStarted,
    ] {
        let exit = Exit::new(kind, Cancellation::NotObserved);
        assert_eq!(exit.cancellation(), Cancellation::NotObserved);
        assert!(exit.is_failure(), "non-completed exit: {:?}", exit.kind());
    }
}

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
    drop(slot);
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
async fn dynamic_reservation_validates_ids_at_the_driver_boundary() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("dynamic root starts");
    let scope = system.scope();

    assert!(matches!(scope.reserve_task(""), Err(ReserveError::EmptyId)));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("dynamic root stops");

    assert!(matches!(scope.reserve_task(""), Err(ReserveError::EmptyId)));
    assert!(matches!(
        scope.reserve_task("worker"),
        Err(ReserveError::NotAdmitting(_))
    ));
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
    let first = scope
        .add_task("worker", definition)
        .await
        .expect("admission succeeds before readiness");
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
async fn large_ordered_immediate_startup_is_iterative() {
    const CHILDREN: usize = 4_096;

    let mut tree = Tree::new();
    for index in 0..CHILDREN {
        let (_task, _completion) = tree
            .add_task_once(
                format!("immediate-{index}"),
                TaskOnceDef::new(|_| async { Ok::<(), ExitError>(()) })
                    .readiness(Readiness::Immediate)
                    .expect("immediate task readiness is valid"),
            )
            .expect("unique child declaration");
    }

    let system = tree.spawn().expect("runtime is available");
    system
        .wait_started()
        .await
        .expect("all immediate children start without recursive re-entry");
    assert_eq!(system.wait().await, StopReason::Finished);
}

#[tokio::test]
async fn one_shot_completion_reports_failure_and_panic_verdicts() {
    let mut failed_tree = Tree::new();
    let (_task, failed) = failed_tree
        .add_task_once(
            "failed",
            TaskOnceDef::<u8>::new(|_| async { Err(ExitError::message("failed")) }),
        )
        .expect("valid declaration");
    let failed_system = failed_tree.spawn().expect("runtime is available");
    assert!(matches!(
        failed
            .wait()
            .await
            .expect_err("failure has no value")
            .kind(),
        ExitKind::Failed(_)
    ));
    assert_eq!(failed_system.wait().await, StopReason::Finished);

    let mut panicked_tree = Tree::new();
    let (_task, panicked) = panicked_tree
        .add_task_once(
            "panicked",
            TaskOnceDef::<u8>::new(|_| async { panic!("one-shot panic") }),
        )
        .expect("valid declaration");
    let panicked_system = panicked_tree.spawn().expect("runtime is available");
    assert!(matches!(
        panicked
            .wait()
            .await
            .expect_err("panic has no value")
            .kind(),
        ExitKind::Panicked { .. }
    ));
    assert_eq!(panicked_system.wait().await, StopReason::Finished);
}

#[tokio::test(start_paused = true)]
async fn one_shot_completion_reports_abort_verdict() {
    let mut tree = Tree::new();
    let (_task, completion) = tree
        .add_task_once(
            "aborted",
            TaskOnceDef::<u8>::new(|_| std::future::pending()).shutdown(Shutdown::Abort),
        )
        .expect("valid declaration");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("task starts");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("forced shutdown joins the task");

    assert!(matches!(
        completion
            .wait()
            .await
            .expect_err("aborted task has no value")
            .kind(),
        ExitKind::Aborted { .. }
    ));
}

#[tokio::test(start_paused = true)]
async fn one_shot_value_cannot_override_readiness_timeout_verdict() {
    let readiness_width = Duration::from_secs(10);
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let mut tree = Tree::new();
    let (_task, completion) = tree
        .add_task_once(
            "late-value",
            TaskOnceDef::new(move |context| async move {
                entered.send(()).expect("test still waits for task entry");
                context.shutdown_token().cancelled().await;
                Ok::<_, ExitError>(42_u8)
            })
            .shutdown(Shutdown::Abort)
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(
                ReadinessDeadline::bounded(readiness_width).expect("non-zero deadline"),
            ),
        )
        .expect("valid declaration");
    let system = tree.spawn().expect("runtime is available");

    entered_rx.await.expect("task body entered");
    tokio::time::advance(readiness_width).await;
    assert!(matches!(
        completion
            .wait()
            .await
            .expect_err("the readiness verdict wins over the returned value")
            .kind(),
        ExitKind::ReadinessTimedOut { .. }
    ));
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::ZERO)
        .await
        .expect("terminal task leaves no straggler");
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
    assert_eq!(exit.cancellation(), Cancellation::Observed);
    assert!(matches!(exit.kind(), ExitKind::Completed));
    assert_eq!(completion.wait().await, Ok(()));
    assert_eventually!(|| !live.is_live(), "task future dropped").await;
}

#[tokio::test]
async fn cancelling_system_wait_keeps_drop_shutdown_armed() {
    let mut tree = Tree::new();
    let task = tree
        .add_task(
            "worker",
            TaskDef::new(|context| async move {
                context.shutdown_token().cancelled().await;
                Ok(())
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let scope = system.scope();
    let mut waiting = Box::pin(system.wait());

    tokio::select! {
        biased;
        _ = &mut waiting => panic!("live system must not stop naturally"),
        () = std::future::ready(()) => {}
    }
    drop(waiting);

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), scope.wait_stopped())
            .await
            .expect("dropping the cancelled wait requests shutdown"),
        StopReason::ShutdownRequested
    );
    assert_eq!(task.wait().await.cancellation(), Cancellation::Observed);
}
