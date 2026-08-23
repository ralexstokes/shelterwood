//! Regression coverage for the root terminality fence at its real venue.
//!
//! `ScopeRuntime::drop` deliberately leaves root membership terminality to the
//! join monitor. Runtime teardown drops *both* tasks, so the monitor is
//! dropped mid-join and neither of its arms runs; its fence guard is the only
//! thing left to publish. Without it a `ScopeRef` that outlives the runtime
//! parks on `wait_stopped()` forever and the observation streams never close.
//!
//! The test deliberately never calls `System::wait`: that would exercise
//! `SystemRun`'s join-first self-heal instead of the guard.

use std::{sync::mpsc, time::Duration};

use shelterwood::{ExitError, StopReason, System, TaskOnceDef, Tree};

/// Bounds the observer so a regression fails the run instead of hanging CI.
const OBSERVER_TIMEOUT: Duration = Duration::from_secs(10);

fn gated_system() -> (tokio::sync::oneshot::Sender<()>, System) {
    let (hold, gated) = tokio::sync::oneshot::channel::<()>();
    let mut tree = Tree::new();
    let (_task, _completion) = tree
        .add_task_once(
            "work",
            TaskOnceDef::new(move |_| async move {
                let _ = gated.await;
                Ok::<(), ExitError>(())
            }),
        )
        .expect("valid declaration");
    (hold, tree.spawn().expect("runtime is available"))
}

#[test]
fn runtime_teardown_terminalizes_the_root_for_an_outliving_observer() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a dedicated runtime is available");

    // The oneshot sender stays on the test thread, so the root's only child
    // is still parked when the runtime goes away.
    let (_hold, system) = runtime.block_on(async {
        let (hold, system) = gated_system();
        system.wait_started().await.expect("tree starts");
        (hold, system)
    });
    let scope = system.scope();

    // Runtime drop returns only once every task future has been dropped, so
    // the monitor's fence has already run by the time this returns.
    drop(runtime);

    let (reasons, observed) = mpsc::channel();
    let observer = std::thread::spawn(move || {
        let waiter = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("the observer runtime is available");
        let _ = reasons.send(waiter.block_on(scope.wait_stopped()));
    });

    assert_eq!(
        observed.recv_timeout(OBSERVER_TIMEOUT),
        Ok(StopReason::ShutdownRequested),
        "a retained scope handle resolves after the runtime that ran the system"
    );
    observer.join().expect("the observer thread does not panic");
    drop(system);
}
