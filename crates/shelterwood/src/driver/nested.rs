use super::*;

pub(super) struct AncestorCommandLatches {
    pub(super) framework_shutdown: Latch,
    pub(super) abort: Latch,
    pub(super) abort_ack: Latch,
}

pub(super) struct NestedScopeLatches {
    pub(super) parent_ready: CompletionGatedLatch,
    pub(super) child_shutdown: Latch,
    pub(super) ancestor: AncestorCommandLatches,
}

pub(super) enum ScopeRole {
    Root,
    Nested(NestedScopeLatches),
}

impl ScopeRole {
    pub(super) fn is_root(&self) -> bool {
        matches!(self, Self::Root)
    }

    pub(super) fn parent_ready(&self) -> Option<&CompletionGatedLatch> {
        match self {
            Self::Root => None,
            Self::Nested(latches) => Some(&latches.parent_ready),
        }
    }

    pub(super) fn ancestor(&self) -> Option<&AncestorCommandLatches> {
        match self {
            Self::Root => None,
            Self::Nested(latches) => Some(&latches.ancestor),
        }
    }
}

pub(super) type NestedScopeStart = Obligation<Arc<ScopeCell>>;

pub(super) fn nested_scope_start(scope: &Arc<ScopeCell>) -> NestedScopeStart {
    Obligation::new(Arc::clone(scope), close_never_started_scope_body)
}

fn close_never_started_scope_body(scope: Arc<ScopeCell>) {
    // Bare like every sibling fallback: `Obligation::drop` is this
    // obligation's only discharge path and contains the call itself.
    scope.close_never_started_body();
}

pub(super) async fn run_nested_tree(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
    mut start: NestedScopeStart,
) -> crate::ExitResult {
    let epoch = begin_nested_incarnation(&scope)?;
    start.complete(drop);
    run_nested_tree_with_epoch(tree, scope, inherited, latches, epoch).await
}

/// Begins the nested epoch before invoking its synchronous restartable
/// factory. Tokio cancellation cannot interrupt one in-progress poll, so a
/// factory that overlaps parent-driver destruction must already own the
/// epilogue that makes `wait_stopped` final.
pub(super) async fn run_nested_factory(
    factory: ScopeFactory,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
    mut start: NestedScopeStart,
) -> crate::ExitResult {
    let epoch = begin_nested_incarnation(&scope)?;
    start.complete(drop);
    let tree = factory();
    run_nested_tree_with_epoch(tree, scope, inherited, latches, epoch).await
}

fn begin_nested_incarnation(scope: &Arc<ScopeCell>) -> Result<ScopeEpochGuard, crate::ExitError> {
    // The previous incarnation's guard finishes its epoch before the parent
    // can begin the next one, so the epoch plane is idle here unless the
    // framework broke that protocol. Like the root, report that no
    // incarnation began.
    ScopeEpochGuard::begin(scope).ok_or_else(|| {
        debug_assert!(false, "a nested scope begins from an idle epoch plane");
        crate::ExitError::message("nested scope never started")
    })
}

pub(super) async fn run_nested_tree_with_epoch(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    latches: NestedScopeLatches,
    mut epoch: ScopeEpochGuard,
) -> crate::ExitResult {
    let plan = match tree.lower(inherited, Some(Arc::clone(&scope))) {
        Ok(plan) => plan,
        Err(LowerError { paths, disposal }) => {
            let cause = StartupFailureCause::Lowering { undefined: paths };
            // Lowering never created a nested driver to own teardown. Keep
            // its isolated definitions attached to this incarnation until
            // they finish; hard-aborting the incarnation still detaches the
            // cancellation-safe disposal jobs.
            disposal.fired().await;
            let failure = StartupFailure { cause };
            // A lowering failure occurs before the driver loop exists, but it
            // still belongs to a live incarnation. Resolve every stop source
            // through the same monotone verdict lattice as the loop path.
            // Peek rather than consume: `finish_incarnation` clears both
            // epoch-tagged request latches after publishing the verdict.
            // The ancestor *abort* latch needs no separate arm: it is the
            // framework-abort edge, fired only by `StopAction::AbortFramework`,
            // and the stop ladder unconditionally passes through
            // `StopAction::Cancel` — which fires this same framework shutdown
            // latch — before it can reach that phase.
            let self_shutdown = scope.has_stop_request(epoch.epoch());
            if self_shutdown || latches.ancestor.framework_shutdown.is_fired() {
                // Mirror the loop's `Pending::Shutdown` arm: firing the
                // child-facing shutdown latch is what makes this scope's exit read
                // `Cancellation::Observed` at its parent, as a requested stop
                // must (§12). An ancestor-driven stop already fired that latch
                // independently; do not couple its framework observer back to
                // user-installable cancellation waiters.
                if self_shutdown {
                    latches.child_shutdown.fire();
                }
                epoch.lifecycle.begin_drain(StopReason::ShutdownRequested);
            }
            // Both drain effects are deliberately discarded: this path
            // publishes no `Draining` edge because nothing was ever started to
            // drain, matching the pre-lattice behaviour of the `StartupFailed`
            // verdict it generalizes.
            epoch.lifecycle.begin_drain({
                let reason = StopReason::StartupFailed(failure);
                reason.retain_guards(&mut epoch.retained_exits);
                reason
            });
            let reason = epoch
                .lifecycle
                .draining_reason()
                .cloned()
                .expect("pre-loop verdicts enter the drain lattice");
            let startup = match &reason {
                StopReason::ShutdownRequested => Err(StartupError::ShutdownRequested),
                StopReason::StartupFailed(failure) => {
                    Err(StartupError::StartupFailed(failure.clone()))
                }
                StopReason::Finished
                | StopReason::IntensityTripped(_)
                | StopReason::NeverStarted => {
                    unreachable!("lowering resolves only failure or shutdown verdicts")
                }
            };
            scope.set_startup(startup);
            epoch.finish(reason.clone());
            return stop_reason_into_nested_result(reason);
        }
    };
    stop_reason_into_nested_result(
        run_scope_incarnation(plan, ScopeRole::Nested(latches), epoch).await,
    )
}
