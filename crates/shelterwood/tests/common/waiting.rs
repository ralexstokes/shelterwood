use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use shelterwood::{TaskDef, Tree};

pub(crate) fn task() -> TaskDef {
    TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    })
}

pub(crate) fn signalled_waiting_task(
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
) -> TaskDef {
    TaskDef::new(move |context| {
        let started = Arc::clone(&started);
        let cancelled = Arc::clone(&cancelled);
        async move {
            started.store(true, Ordering::SeqCst);
            context.shutdown_token().cancelled().await;
            cancelled.store(true, Ordering::SeqCst);
            Ok(())
        }
    })
}

/// [`signalled_waiting_task`] plus a liveness probe.
///
/// The task races its shutdown token against `liveness_gate` with a biased
/// select, so releasing the gate proves the incarnation is still being polled
/// rather than merely un-cancelled. Shutdown still wins a genuine tie, and the
/// task returns to waiting for it after the probe, so the started/cancelled
/// observables are exactly those of the unprobed helper.
pub(crate) fn liveness_probed_waiting_task(
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    liveness_gate: super::ReleaseGate,
    liveness_seen: Arc<AtomicBool>,
) -> TaskDef {
    TaskDef::new(move |context| {
        let started = Arc::clone(&started);
        let cancelled = Arc::clone(&cancelled);
        let liveness_gate = liveness_gate.clone();
        let liveness_seen = Arc::clone(&liveness_seen);
        async move {
            started.store(true, Ordering::SeqCst);
            let shutdown = context.shutdown_token();
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    cancelled.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                () = liveness_gate.wait() => {
                    liveness_seen.store(true, Ordering::SeqCst);
                }
            }
            context.shutdown_token().cancelled().await;
            cancelled.store(true, Ordering::SeqCst);
            Ok(())
        }
    })
}

pub(crate) fn tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task("worker", task()).expect("valid task");
    tree
}
