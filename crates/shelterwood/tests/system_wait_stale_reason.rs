//! Regression coverage for the root terminality fence.
//!
//! Adapted from the `verify-wait` repro at commit 3169ba6. The original
//! reliably exposed a completed-drain window in which a hostile terminal
//! observer killed the root driver after `Finished` was published but before
//! the join monitor upgraded the final verdict. Root membership terminality is
//! now owned by that monitor, so every `wait_stopped` observer remains behind
//! the last possible classification point.

use std::{
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use shelterwood::{ExitError, ScopeRef, ScopeState, StopReason, System, TaskOnceDef, Tree};

/// Panics during the root's terminal wake flush after giving well-behaved
/// waiters on another worker time to run.
struct TerminalPanicWake {
    scope: ScopeRef,
    seen: std::sync::Mutex<Vec<ScopeState>>,
}

impl Wake for TerminalPanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let state = self.scope.snapshot().state.clone();
        self.seen
            .lock()
            .expect("probe mutex poisoned")
            .push(state.clone());
        if matches!(state, ScopeState::Stopped { .. }) {
            std::thread::sleep(Duration::from_millis(250));
            panic!("hostile terminal observer wake");
        }
    }
}

fn gated_finishing_system() -> (tokio::sync::oneshot::Sender<()>, System) {
    let (release, gated) = tokio::sync::oneshot::channel::<()>();
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
    (release, tree.spawn().expect("runtime is available"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_drain_hostile_terminal_wake_preserves_the_final_reason() {
    let (release, system) = gated_finishing_system();
    system.wait_started().await.expect("tree starts");
    let scope = system.scope();

    let waiter = tokio::spawn(async move { system.wait().await });
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    let mut observers = Vec::new();
    for _ in 0..4 {
        let observer = scope.clone();
        observers.push(tokio::spawn(async move { observer.wait_stopped().await }));
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    let probe = Arc::new(TerminalPanicWake {
        scope: scope.clone(),
        seen: std::sync::Mutex::new(Vec::new()),
    });
    let hostile = Waker::from(Arc::clone(&probe));
    let mut parked = Box::pin(scope.wait_stopped());
    assert_eq!(
        parked.as_mut().poll(&mut Context::from_waker(&hostile)),
        Poll::Pending,
        "the hostile observer parks on the live root"
    );

    release.send(()).expect("the gated task is still running");
    let mut awaited = vec![waiter.await.expect("the waiting task does not panic")];
    for observer in observers {
        awaited.push(observer.await.expect("an observer task does not panic"));
    }

    let recorded = scope.snapshot().state.clone();
    assert_eq!(
        recorded,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        },
        "the join monitor publishes the final classification after the driver wake panic"
    );
    assert_eq!(
        probe.seen.lock().expect("probe mutex poisoned").as_slice(),
        [ScopeState::Stopped {
            reason: StopReason::Finished
        }],
        "the hostile observer kills the driver from its completed-drain publication"
    );
    for reason in awaited {
        assert_eq!(
            ScopeState::Stopped { reason },
            recorded,
            "every awaited reason agrees with the final record"
        );
    }
}
