use shelterwood::{ExitError, StopReason, TaskOnceDef, Tree};

#[test]
fn default_feature_consumer_runs_a_supervised_task_to_completion() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("consumer runtime builds");
    runtime.block_on(async {
        let mut tree = Tree::new();
        let (_task, completion) = tree
            .add_task_once(
                "consumer-task",
                TaskOnceDef::new(|_| async { Ok::<_, ExitError>(42_u8) }),
            )
            .expect("the default façade accepts a task");
        let system = tree.spawn().expect("the consumer runtime is active");
        system.wait_started().await.expect("the task starts");
        assert_eq!(completion.wait().await, Ok(42));
        assert_eq!(system.wait().await, StopReason::Finished);
    });
}
