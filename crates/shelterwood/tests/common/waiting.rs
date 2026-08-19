use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use shelterwood::{
    CancellationToken, ExitResult, Readiness, TaskContext, TaskDef, TaskOnceDef, Tree,
};

pub(crate) async fn liveness_probe(shutdown: CancellationToken, gate: super::ReleaseGate) -> bool {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => false,
        () = gate.wait() => true,
    }
}

pub(crate) fn gate_released_manual_ready_task(
    gate: super::ReleaseGate,
    on_start: impl Fn() + Clone + Send + Sync + 'static,
) -> TaskDef {
    TaskDef::new(move |context| {
        let gate = gate.clone();
        let on_start = on_start.clone();
        async move {
            on_start();
            gate.wait().await;
            context.mark_ready();
            context.shutdown_token().cancelled().await;
            Ok(())
        }
    })
    .readiness(Readiness::Manual)
    .expect("manual readiness is supported for tasks")
}

pub(crate) fn task() -> TaskDef {
    TaskDef::new(|context| async move {
        context.shutdown_token().cancelled().await;
        Ok(())
    })
}

/// The body every signalled waiting fixture shares.
///
/// `liveness`, when present, races the shutdown token against a release gate
/// before the task parks, so a released gate proves the incarnation is still
/// being polled rather than merely un-cancelled.
async fn signalled_waiting_body(
    context: TaskContext,
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    liveness: Option<(super::ReleaseGate, Arc<AtomicBool>)>,
) -> ExitResult {
    started.store(true, Ordering::SeqCst);
    if let Some((gate, seen)) = liveness {
        if !liveness_probe(context.shutdown_token(), gate).await {
            cancelled.store(true, Ordering::SeqCst);
            return Ok(());
        }
        seen.store(true, Ordering::SeqCst);
    }
    context.shutdown_token().cancelled().await;
    cancelled.store(true, Ordering::SeqCst);
    Ok(())
}

pub(crate) fn signalled_waiting_task(
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
) -> TaskDef {
    TaskDef::new(move |context| {
        signalled_waiting_body(context, Arc::clone(&started), Arc::clone(&cancelled), None)
    })
}

/// [`signalled_waiting_task`] as a consuming one-shot definition.
///
/// One-shot is the child *kind*, not a detail: it has no restart edge and its
/// completion claim is the caller's, which is what fixtures proving handle
/// ownership depend on.
pub(crate) fn signalled_waiting_once_task(
    started: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
    liveness: Option<(super::ReleaseGate, Arc<AtomicBool>)>,
) -> TaskOnceDef<()> {
    TaskOnceDef::new(move |context| signalled_waiting_body(context, started, cancelled, liveness))
}

pub(crate) fn start_signalled_waiting_task(started: Arc<AtomicBool>) -> TaskDef {
    signalled_waiting_task(started, Arc::new(AtomicBool::new(false)))
}

pub(crate) fn cancellation_signalled_waiting_task(cancelled: Arc<AtomicBool>) -> TaskDef {
    signalled_waiting_task(Arc::new(AtomicBool::new(false)), cancelled)
}

/// A waiting task that signals `constructed` from its *factory*, not from the
/// body.
///
/// [`signalled_waiting_task`] stores at the first poll of the task future,
/// which is a later and scheduler-dependent moment: a caller that only knows
/// the child was spawned cannot assert the flag without polling for it. The
/// factory instead runs synchronously while the driver starts the child, so a
/// caller that has observed startup advance past this child may assert the
/// flag outright.
pub(crate) fn construction_signalled_waiting_task(constructed: Arc<AtomicBool>) -> TaskDef {
    TaskDef::new(move |context| {
        constructed.store(true, Ordering::SeqCst);
        async move {
            context.shutdown_token().cancelled().await;
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
        signalled_waiting_body(
            context,
            Arc::clone(&started),
            Arc::clone(&cancelled),
            Some((liveness_gate.clone(), Arc::clone(&liveness_seen))),
        )
    })
}

pub(crate) fn tree() -> Tree {
    let mut tree = Tree::new();
    tree.add_task("worker", task()).expect("valid task");
    tree
}
