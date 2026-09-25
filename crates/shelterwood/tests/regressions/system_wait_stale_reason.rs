//! Regression coverage for the root terminality fence.
//!
//! Adapted from the `verify-wait` repro at commit 3169ba6. The original
//! reliably exposed a completed-drain window in which a hostile terminal
//! observer killed the root driver after `Finished` was published but before
//! the join monitor upgraded the final verdict. Root membership terminality is
//! now owned by that monitor, so every `wait_stopped` observer remains behind
//! the last possible classification point.

use std::{
    future::Future,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
};

use shelterwood::{
    ExitError, ScopeRef, ScopeState, StopReason, System, TaskOnceDef, TaskRef, Tree,
};

/// Panics from the completed-drain wake flush and records the state it saw.
struct TerminalPanicWake {
    scope: ScopeRef,
    seen: Mutex<Vec<ScopeState>>,
}

impl Wake for TerminalPanicWake {
    fn wake(self: Arc<Self>) {
        let state = self.scope.snapshot().state.clone();
        self.seen
            .lock()
            .expect("probe mutex poisoned")
            .push(state.clone());
        if matches!(state, ScopeState::Stopped { .. }) {
            panic!("hostile terminal observer wake");
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Arc::clone(self).wake();
    }
}

/// Holds the root driver in the child's terminal wake flush so the test can
/// install the hostile root observer after every earlier root signal and
/// before the completed-drain publication.
#[derive(Default)]
struct TerminalWakeGate {
    reached: tokio::sync::Notify,
    released: Mutex<bool>,
    release: Condvar,
}

impl TerminalWakeGate {
    fn open(&self) {
        *self.released.lock().expect("gate mutex poisoned") = true;
        self.release.notify_all();
    }

    fn pause(&self) {
        self.reached.notify_one();
        let mut released = self.released.lock().expect("gate mutex poisoned");
        while !*released {
            released = self
                .release
                .wait(released)
                .expect("gate mutex stays healthy while waiting");
        }
    }
}

impl Wake for TerminalWakeGate {
    fn wake(self: Arc<Self>) {
        self.pause();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.pause();
    }
}

/// Releases a blocked driver if a test assertion unwinds before the explicit
/// open, so runtime teardown can always join its worker threads.
struct TerminalWakeGateGuard(Arc<TerminalWakeGate>);

impl Drop for TerminalWakeGateGuard {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// Spawns a future and reports only after its first poll proves it is parked.
fn spawn_parked<F>(
    future: F,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::task::JoinHandle<F::Output>,
)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (parked, registered) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut future = Box::pin(future);
        let mut parked = Some(parked);
        std::future::poll_fn(move |context| {
            let result = future.as_mut().poll(context);
            if result.is_pending()
                && let Some(parked) = parked.take()
            {
                parked
                    .send(())
                    .expect("the test retains the parking handshake");
            }
            result
        })
        .await
    });
    (registered, task)
}

fn gated_finishing_system() -> (tokio::sync::oneshot::Sender<()>, System, TaskRef) {
    let (release, gated) = tokio::sync::oneshot::channel::<()>();
    let mut tree = Tree::new();
    let (task, _completion) = tree
        .add_task_once(
            "work",
            TaskOnceDef::new(move |_| async move {
                let _ = gated.await;
                Ok::<(), ExitError>(())
            }),
        )
        .expect("valid declaration");
    (release, tree.spawn().expect("runtime is available"), task)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_drain_hostile_terminal_wake_preserves_the_final_reason() {
    let (release, system, task) = gated_finishing_system();
    system.wait_started().await.expect("tree starts");
    let scope = system.scope();

    let (waiter_parked, waiter) = spawn_parked(async move { system.wait().await });
    waiter_parked
        .await
        .expect("System::wait parks before the root finishes");

    let mut observers = Vec::new();
    for _ in 0..4 {
        let observer = scope.clone();
        let (observer_parked, observer) =
            spawn_parked(async move { observer.wait_stopped().await });
        observer_parked
            .await
            .expect("the stop observer parks before the root finishes");
        observers.push(observer);
    }

    let terminal_gate = Arc::new(TerminalWakeGate::default());
    let terminal_gate_guard = TerminalWakeGateGuard(Arc::clone(&terminal_gate));
    let terminal_gate_waker = Waker::from(Arc::clone(&terminal_gate));
    let mut task_stopped = Box::pin(task.wait());
    assert_eq!(
        task_stopped
            .as_mut()
            .poll(&mut Context::from_waker(&terminal_gate_waker)),
        Poll::Pending,
        "the child-terminal gate parks before the task finishes"
    );

    release.send(()).expect("the gated task is still running");
    terminal_gate.reached.notified().await;

    let probe = Arc::new(TerminalPanicWake {
        scope: scope.clone(),
        seen: Mutex::new(Vec::new()),
    });
    let hostile = Waker::from(Arc::clone(&probe));
    let mut root_stopped = Box::pin(scope.wait_stopped());
    assert_eq!(
        root_stopped
            .as_mut()
            .poll(&mut Context::from_waker(&hostile)),
        Poll::Pending,
        "the hostile root observer parks before the completed-drain publication"
    );
    terminal_gate.open();

    let mut awaited = vec![waiter.await.expect("the waiting task does not panic")];
    for observer in observers {
        awaited.push(observer.await.expect("an observer task does not panic"));
    }

    let recorded = scope.snapshot().state.clone();
    let seen = probe.seen.lock().expect("probe mutex poisoned").clone();
    assert_eq!(
        recorded,
        ScopeState::Stopped {
            reason: StopReason::ShutdownRequested
        },
        "the join monitor publishes the final classification after the driver wake panic; \
         awaited={awaited:?}; hostile wake states={seen:?}"
    );
    assert_eq!(
        seen.last(),
        Some(&ScopeState::Stopped {
            reason: StopReason::Finished
        }),
        "the hostile observer kills the driver from its completed-drain publication; \
         all hostile wake states={seen:?}"
    );
    for reason in awaited {
        assert_eq!(
            ScopeState::Stopped { reason },
            recorded,
            "every awaited reason agrees with the final record"
        );
    }
    drop(terminal_gate_guard);
}
