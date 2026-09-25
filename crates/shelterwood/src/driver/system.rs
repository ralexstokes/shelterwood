//! The root driver: spawning it, joining it, and the monitor that publishes
//! root terminality.

use super::*;

pub(crate) struct SystemRun {
    pub(crate) root: Arc<ScopeCell>,
    pub(super) driver: Option<runtime::JoinHandle<Guarded<StopReason>>>,
    // Taking the handle starts a cancellation-sensitive join. Keep its
    // completion separate so dropping a cancelled consuming API still requests
    // shutdown even though the in-flight future already moved the handle out.
    pub(super) driver_joined: bool,
}

impl SystemRun {
    pub(crate) async fn shutdown(
        &mut self,
        timeout: DeadlineBudget,
    ) -> Result<(), ShutdownTimeout> {
        let result = shutdown_scope(Arc::clone(&self.root), timeout).await;
        self.join_driver().await;
        result
    }

    pub(crate) async fn wait(&mut self) -> StopReason {
        self.join_driver().await;
        self.root.wait_stopped().await
    }

    async fn join_driver(&mut self) {
        let Some(driver) = self.driver.take() else {
            return;
        };
        // This is the sole production join polled directly by a public API
        // caller: `System::wait`, `System::shutdown`, and startup rollback all
        // converge here. The other joins are framework-task venues -- the
        // root monitor below, child monitors, and raw offload reclamation --
        // and therefore keep the ordinary runtime join path.
        if let Err(exit) =
            classify_retained_root_driver_join(runtime::join_user_polled(driver).await)
        {
            finish_monitored_root(&self.root, StopReason::ShutdownRequested, exit);
        }
        self.driver_joined = true;
    }
}

pub(super) fn classify_retained_root_driver_join(
    outcome: runtime::JoinOutcome<Guarded<StopReason>>,
) -> Result<Guarded<StopReason>, Exit> {
    let (join, cancellation) = match outcome {
        runtime::JoinOutcome::Ok { value } => return Ok(value),
        runtime::JoinOutcome::Panic { message } => (
            runtime::JoinOutcome::Panic { message },
            Cancellation::NotObserved,
        ),
        runtime::JoinOutcome::Cancelled => {
            (runtime::JoinOutcome::Cancelled, Cancellation::Observed)
        }
    };
    Err(classify_exit_retaining(None, join, None, cancellation))
}

impl Drop for SystemRun {
    fn drop(&mut self) {
        if self.driver_joined {
            return;
        }
        // After a clean shutdown the root epochs are `Idle`, so this writes a
        // real `ScopeRequest` — targeting the pending next incarnation — into
        // dead control state and pulses the member record.
        // That stays harmless only while watchers tolerate spurious wakes and
        // the driver we already joined was the sole consumer of scope
        // requests; nothing may come to treat a post-shutdown request as
        // meaningful. The poison-tolerant entry point keeps this drop from
        // panicking — and aborting — on an already-unwinding thread.
        let _ = self.root.request_shutdown_ignoring_poison();
    }
}

pub(crate) fn spawn_system(plan: ScopePlan) -> SystemRun {
    let root = Arc::clone(&plan.root);
    let driver = runtime::spawn(async move { run_scope(plan, ScopeRole::Root).await });
    let lifecycle = monitor_root_driver(Arc::clone(&root), driver);
    SystemRun {
        root,
        driver: Some(lifecycle),
        driver_joined: false,
    }
}

pub(super) fn monitor_root_driver(
    monitor_root: Arc<ScopeCell>,
    driver: runtime::JoinHandle<Guarded<StopReason>>,
) -> runtime::JoinHandle<Guarded<StopReason>> {
    // Constructed before the future so it is an upvar of the `async move`
    // block rather than a local of its body: an `async` block owns its
    // captures from creation, so the fence retires even for a monitor task
    // that is dropped without ever being polled.
    let mut fence = MonitorFence::armed(monitor_root);
    runtime::spawn(async move {
        match classify_retained_root_driver_join(runtime::join(driver).await) {
            Ok(reason) => {
                let public_reason = reason.get().clone();
                let exit = stop_reason_root_exit(&public_reason);
                fence.publish(public_reason, exit);
                reason
            }
            Err(exit) => {
                fence.publish(StopReason::ShutdownRequested, exit);
                Guarded::new(StopReason::ShutdownRequested)
            }
        }
    })
}

/// The monitor body's root-terminality duty, held across the driver join.
///
/// Invariant: **the monitor future never completes or is dropped without root
/// membership terminality having been published.** Since `ScopeRuntime::drop`
/// deliberately leaves root terminality to this monitor, an unpublished fence
/// would strand every retained `wait_stopped()` observer forever and leave the
/// observation streams open — the guard is what makes that unrepresentable.
///
/// The only way to reach `drop` still armed is cancellation of the monitor
/// task itself: dropping the monitor's `JoinHandle` merely detaches, and no
/// framework path aborts it. Runtime teardown is the reachable case, where
/// every spawned task future is dropped, polled or not. Cancellation is also why the fallback
/// verdict is not speculative: the driver's own `JoinHandle` lives inside the
/// dropped join future, so the driver's real outcome becomes unobservable to
/// everyone at exactly this moment, and the one join anyone can still observe
/// — the monitor handle — resolves `Cancelled`. Publishing that outcome's
/// classification is therefore a statement about the join that did settle, and
/// it is bit-for-bit the verdict `SystemRun::join_driver`'s self-heal computes
/// for the same cancellation. Should both run, member terminalization is
/// first-writer-wins and the record lattice treats an equal verdict as an
/// idempotent repeat, so neither can outrank or race the other.
///
/// Teardown drops the two tasks in an unspecified order, so the fence can
/// precede the driver's own epilogue and close observation ahead of its final
/// `Removed` edges. That truncation is confined to a runtime that is going
/// away underneath its subscribers, and it is strictly the better half of the
/// trade against an observer that never resolves at all.
///
/// Publication goes through [`finish_monitored_root`], which contains any
/// hostile terminal-wake panic: this `drop` runs in task drop glue on a
/// teardown thread that may already be unwinding, where a second panic would
/// abort the process. The cancellation exit owns no user error, so the guard
/// adds no user-value destruction and no lock site of its own.
struct MonitorFence {
    root: Arc<ScopeCell>,
    armed: bool,
}

impl MonitorFence {
    fn armed(root: Arc<ScopeCell>) -> Self {
        Self { root, armed: true }
    }

    fn publish(&mut self, reason: StopReason, exit: Exit) {
        finish_monitored_root(&self.root, reason, exit);
        self.armed = false;
    }
}

impl Drop for MonitorFence {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // A cancelled join never classifies as a completed reason, so the
        // `Ok` half is unreachable; matching rather than unwrapping keeps a
        // panic out of drop glue regardless.
        if let Err(exit) = classify_retained_root_driver_join(runtime::JoinOutcome::Cancelled) {
            finish_monitored_root(&self.root, StopReason::ShutdownRequested, exit);
        }
    }
}

/// Publishes the join monitor's final root verdict without letting a hostile
/// terminal observer kill the monitor itself.
///
/// The driver is already joined, so exit classification cannot change after
/// this boundary. A panic from the terminal wake flush is consequently only a
/// diagnostic; discard it rather than replacing a classified completion with
/// a second monitor failure that could strand the root's finality fence.
fn finish_monitored_root(root: &ScopeCell, reason: StopReason, exit: Exit) {
    runtime::discard_panic(
        runtime::catch_panic(|| root.finish_live_root_incarnation(reason, exit)).err(),
    );
}
