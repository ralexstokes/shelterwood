use shelterwood::prelude::{errors::StopReason, *};

/// The prelude is reachable from outside the workspace and carries enough
/// to declare, spawn, and join a system without naming a crate-root path.
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

/// Ring 0 names every handle an ordinary program stores or passes: the
/// one-shot task claim, the reply half of `reply_channel`, and the tokens
/// task and raw contexts hand out. Compiling this signature under the glob
/// alone is the check.
#[allow(dead_code)]
fn ring_zero_names_stored_handles(
    _claim: OneShotTaskRef<u8>,
    _reply: ReplyReceiver<u8>,
    _token: CancellationToken,
) {
}

/// Each ring-1 bundle is complete for its own area. A file that globs only
/// `prelude::raw` can still name the token its `run_blocking` closures
/// receive, even though ring 0 exports it as well.
mod raw_bundle_alone {
    use shelterwood::prelude::raw::*;

    #[allow(dead_code)]
    fn names_the_blocking_token(_token: CancellationToken) {}
}
