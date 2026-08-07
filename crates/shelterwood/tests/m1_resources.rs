use std::time::Duration;

use shelterwood::{
    Backoff, ExitError, Readiness, ReadinessDeadline, RestartCondition, RestartPolicy,
    SubtreeOnceDef, TaskDef, TaskOnceDef, Tree,
};
use shelterwood_test_support::ConsumeCount;

fn never() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_normal_exit() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    let (_task, completion) = tree
        .add_task_once(
            "task",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    completion.wait().await.expect("task completes");
    system.wait().await;
    count.assert_once();
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_panic() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    let (task, _completion) = tree
        .add_task_once(
            "task",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                panic!("construction body panic");
                #[allow(unreachable_code)]
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    task.wait().await;
    system.wait().await;
    count.assert_once();
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_startup_failure() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    let (_task, _completion) = tree
        .add_task_once(
            "task",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Err::<(), _>(ExitError::message("failed before ready"))
            })
            .readiness(Readiness::Manual)
            .expect("manual readiness"),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_task_resource_drops_once_on_shutdown_before_start() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut tree = Tree::new();
    tree.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .restart(never())
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid gate");
    let (_task, _completion) = tree
        .add_task_once(
            "never-spawned",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown completes");
    count.assert_once();
}

fn one_shot_subtree_with_guard(count: &ConsumeCount, mode: &'static str) -> Tree {
    let guard = count.guard();
    let mut tree = Tree::new();
    let definition = match mode {
        "normal" => TaskOnceDef::new(move |_| async move {
            let _guard = guard;
            Ok::<_, ExitError>(())
        }),
        "panic" => TaskOnceDef::new(move |_| async move {
            let _guard = guard;
            panic!("subtree construction panic");
            #[allow(unreachable_code)]
            Ok::<_, ExitError>(())
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness"),
        _ => unreachable!("known fixture mode"),
    };
    let (_task, _completion) = tree
        .add_task_once("resource", definition)
        .expect("valid task");
    tree
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_normal_exit() {
    let count = ConsumeCount::default();
    let nested = one_shot_subtree_with_guard(&count, "normal");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait().await;
    count.assert_once();
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_panic() {
    let count = ConsumeCount::default();
    let nested = one_shot_subtree_with_guard(&count, "panic");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_lowering_failure() {
    let count = ConsumeCount::default();
    let guard = count.guard();
    let mut nested = Tree::new();
    let _undefined = nested.reserve_task("undefined").expect("reservation");
    let (_task, _completion) = nested
        .add_task_once(
            "resource",
            TaskOnceDef::new(move |_| async move {
                let _guard = guard;
                Ok::<_, ExitError>(())
            }),
        )
        .expect("valid task");
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    assert!(system.wait_started().await.is_err());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
    count.assert_once();
}

#[tokio::test]
async fn one_shot_subtree_resource_drops_once_on_shutdown_before_start() {
    let count = ConsumeCount::default();
    let nested = one_shot_subtree_with_guard(&count, "normal");
    let mut root = Tree::new();
    root.add_task(
        "gate",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .readiness(Readiness::Manual)
        .expect("manual readiness")
        .readiness_deadline(ReadinessDeadline::Unbounded),
    )
    .expect("valid gate");
    root.add_subtree_once("never-spawned", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown completes");
    count.assert_once();
}
