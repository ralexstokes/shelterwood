use std::{fmt, marker::PhantomData, sync::Arc};

use crate::{
    DeadlineBudget, DefaultsInheritance, ReadinessDeadline, RestartPolicy, Retention, Shutdown,
    ShutdownTimeout,
    definition::DefinitionSource,
    exit::{StartupError, StopReason},
    plan::{BuilderCore, ScopeConstruction},
    policy::{CommonOptions, ScopeFlavor},
    scope::{DynamicScopeRef, ScopeRef},
};

use super::{DynamicTree, Tree};

/// Startup failure paired with any rollback timeout report.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("tree startup failed and was rolled back")]
pub struct StartOrShutdownError {
    /// Original startup error.
    pub startup: StartupError,
    /// Stragglers forced down after the rollback bound, if any.
    pub rollback_timeout: Option<ShutdownTimeout>,
}

/// A restartable subtree definition.
pub struct SubtreeDef<T: Subtree> {
    factory: Arc<dyn Fn() -> BuilderCore + Send + Sync + 'static>,
    options: CommonOptions,
    defaults: DefaultsInheritance,
    subtree: PhantomData<fn() -> T>,
}

impl<T: Subtree> fmt::Debug for SubtreeDef<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubtreeDef")
            .field("options", &self.options)
            .field("defaults", &self.defaults)
            .finish_non_exhaustive()
    }
}

impl<T: Subtree> SubtreeDef<T> {
    /// Creates a restartable subtree from a repeatable declaration source.
    pub fn factory(factory: impl Fn() -> T + Send + Sync + 'static) -> Self {
        Self {
            factory: Arc::new(move || <T as sealed::Sealed>::into_core(factory())),
            options: CommonOptions::default(),
            defaults: DefaultsInheritance::Inherit,
            subtree: PhantomData,
        }
    }

    common_options_setters!(restart, shutdown, structural_readiness_deadline, retention,);

    /// Selects inheritance or reset for unset nested defaults.
    #[must_use]
    pub fn defaults(mut self, defaults: DefaultsInheritance) -> Self {
        self.defaults = defaults;
        self
    }

    pub(super) fn erase(self) -> ScopeConstruction {
        let factory = self.factory;
        ScopeConstruction {
            source: DefinitionSource::Restartable(factory),
            options: self.options,
            defaults: self.defaults,
        }
    }
}

/// A consuming one-shot subtree definition.
pub struct SubtreeOnceDef<T: Subtree> {
    tree: T,
    options: CommonOptions,
    defaults: DefaultsInheritance,
}

impl<T: Subtree> fmt::Debug for SubtreeOnceDef<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubtreeOnceDef")
            .field("tree", &self.tree)
            .field("options", &self.options)
            .field("defaults", &self.defaults)
            .finish()
    }
}

impl<T: Subtree> SubtreeOnceDef<T> {
    /// Creates a one-shot subtree from one owned declaration.
    #[must_use]
    pub fn new(tree: T) -> Self {
        Self {
            tree,
            options: CommonOptions::default(),
            defaults: DefaultsInheritance::Inherit,
        }
    }

    common_options_setters!(shutdown, structural_readiness_deadline, retention,);

    /// Selects inheritance or reset for unset nested defaults.
    #[must_use]
    pub fn defaults(mut self, defaults: DefaultsInheritance) -> Self {
        self.defaults = defaults;
        self
    }

    pub(super) fn erase(self) -> ScopeConstruction {
        ScopeConstruction {
            source: DefinitionSource::OneShot(Box::new(<T as sealed::Sealed>::into_core(
                self.tree,
            ))),
            options: self.options,
            defaults: self.defaults,
        }
    }
}

/// Private dispatch that seals subtree flavor conversion inside the façade.
#[allow(private_interfaces)]
pub(super) mod sealed {
    use super::{BuilderCore, DynamicScopeRef, ScopeFlavor, ScopeRef};

    pub trait Sealed {
        type Ref;
        const FLAVOR: ScopeFlavor;
        fn into_core(self) -> BuilderCore;
        fn make_ref(scope: ScopeRef) -> Self::Ref;
    }

    impl Sealed for super::Tree {
        type Ref = ScopeRef;
        const FLAVOR: ScopeFlavor = ScopeFlavor::Ordered;

        fn into_core(self) -> BuilderCore {
            self.core
        }

        fn make_ref(scope: ScopeRef) -> Self::Ref {
            scope
        }
    }

    impl Sealed for super::DynamicTree {
        type Ref = DynamicScopeRef;
        const FLAVOR: ScopeFlavor = ScopeFlavor::Dynamic;

        fn into_core(self) -> BuilderCore {
            self.core
        }

        fn make_ref(scope: ScopeRef) -> Self::Ref {
            DynamicScopeRef(scope)
        }
    }
}

/// Sealed dispatch from a tree flavor to its capability-preserving handle.
pub trait Subtree: sealed::Sealed + fmt::Debug + Send + 'static {}

impl Subtree for Tree {}

impl Subtree for DynamicTree {}

/// The sole owning handle for a running root system.
///
/// [`Tree::spawn`] and [`DynamicTree::spawn`] return exactly one `System`;
/// it is not cloneable, and non-owning handles come from [`System::scope`].
/// The owner sequences the root lifecycle:
///
/// - [`System::wait_started`] reports the startup outcome but deliberately
///   leaves the successfully started prefix running and supervised, so the
///   caller decides what a partial start means.
/// - [`System::start_or_shutdown`] is the rollback form: on startup failure
///   it consumes the owner and drives full shutdown of the started prefix,
///   preserving the original cause beside any rollback timeout report.
/// - [`System::shutdown`] consumes the owner, bounds the cooperative
///   teardown phase with its timeout, and joins the root driver before
///   returning.
/// - [`System::wait`] consumes the owner and resolves at natural or
///   externally requested terminal state.
///
/// Dropping a `System` requests graceful shutdown, but only an awaited
/// [`System::shutdown`] joins the root driver and returns straggler
/// evidence ([`ShutdownTimeout`]); an embedding host should normally await
/// it before tearing down the async runtime. The escalation ladder, what a
/// completed shutdown does and does not guarantee, and runtime-lifetime
/// obligations are documented in [`crate::guides::shutdown_and_resources`].
#[must_use = "dropping the sole system owner requests graceful shutdown"]
pub struct System<R = ScopeRef> {
    pub(super) root: R,
    pub(super) run: crate::driver::SystemRun,
}

impl<R: fmt::Debug> fmt::Debug for System<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("System")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl<R: Clone> System<R> {
    /// Returns a non-owning root scope handle preserving dynamic capability.
    #[must_use]
    pub fn scope(&self) -> R {
        self.root.clone()
    }
}

impl<R> System<R> {
    /// Waits until the declared tree is ready or startup terminally fails.
    ///
    /// Resolves `Ok` once every declared child is ready — gated one child at
    /// a time in an ordered [`Tree`], concurrently and counting initial
    /// members only in a [`DynamicTree`]. A startup failure is reported here
    /// but deliberately leaves the successfully started prefix running and
    /// supervised: the caller chooses between a host-specific response and
    /// rollback. Use [`Self::start_or_shutdown`] when a failed startup
    /// should tear the prefix down.
    ///
    /// # Errors
    ///
    /// Returns the [`StartupError`] recorded by the root startup barrier: a
    /// terminal child failure during startup, a restart-intensity trip, or a
    /// shutdown requested before startup completed.
    pub async fn wait_started(&self) -> Result<(), StartupError> {
        self.run.root.wait_started().await
    }

    /// Rolls a startup failure back through full shutdown.
    ///
    /// Unlike [`Self::wait_started`], this consumes the owner so a failed
    /// startup cannot leave its successfully started prefix running. The error
    /// preserves both the original startup cause and any rollback timeout.
    /// A zero rollback budget requests cooperative cancellation and then
    /// enters the ordinary escalation tail without a cooperative wait.
    ///
    /// # Errors
    ///
    /// Returns [`StartOrShutdownError`] carrying the original
    /// [`StartupError`] beside any [`ShutdownTimeout`] the rollback
    /// produced; rollback never masks the startup cause.
    pub async fn start_or_shutdown(
        mut self,
        timeout: impl Into<DeadlineBudget>,
    ) -> Result<Self, StartOrShutdownError> {
        let timeout: DeadlineBudget = timeout.into();
        match self.wait_started().await {
            Ok(()) => Ok(self),
            Err(startup) => {
                let rollback_timeout = self.run.shutdown(timeout).await.err();
                Err(StartOrShutdownError {
                    startup,
                    rollback_timeout,
                })
            }
        }
    }

    /// Requests shutdown, escalates at `timeout`, joins the root driver, and
    /// consumes the owner.
    ///
    /// `timeout` bounds cooperative teardown, not necessarily this method's
    /// wall-clock return. A nested framework driver normally joins its
    /// children, but its hard-abort fallback can only request their abort from
    /// its synchronous drop epilogue; deeper task destruction may therefore
    /// finish after return. Blocking threads created by `run_blocking` detach
    /// past hard abort independently of that fallback.
    /// A zero budget still requests cooperative cancellation, then skips its
    /// wait and enters the ordinary escalation tail immediately.
    /// [`crate::guides::shutdown_and_resources`] documents the full
    /// escalation ladder and the runtime-lifetime obligations around this
    /// call.
    ///
    /// # Errors
    ///
    /// Returns [`ShutdownTimeout`] when cooperative teardown exceeded
    /// `timeout` and stragglers were escalated; the root driver is joined
    /// before the report returns.
    pub async fn shutdown(
        mut self,
        timeout: impl Into<DeadlineBudget>,
    ) -> Result<(), ShutdownTimeout> {
        self.run.shutdown(timeout.into()).await
    }

    /// Waits for natural or externally requested terminal state.
    pub async fn wait(mut self) -> StopReason {
        self.run.wait().await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::pin,
        sync::Arc,
        task::{Context, Poll, Waker},
    };

    use super::{DynamicTree, StopReason, Tree, sealed::Sealed};
    use crate::identity::ScopeIdentity;

    #[test]
    fn subtree_conversion_moves_without_minting_a_phantom_scope() {
        let tree = Tree::new();
        let after_tree = ScopeIdentity::current_thread_creations();
        let core = <Tree as Sealed>::into_core(tree);
        assert_eq!(ScopeIdentity::current_thread_creations(), after_tree);
        drop(core);

        let tree = DynamicTree::new();
        let after_tree = ScopeIdentity::current_thread_creations();
        let core = <DynamicTree as Sealed>::into_core(tree);
        assert_eq!(ScopeIdentity::current_thread_creations(), after_tree);
        drop(core);
    }

    /// `SystemRun::drop` requests root shutdown so a dropped owner tears the
    /// tree down. Once the driver has been joined that request has no
    /// consumer, and writing it anyway would publish a live `ScopeRequest`
    /// against the pending next incarnation and pulse the root member
    /// record. The early return makes the drop inert instead; this pins it
    /// through the pulse, which fires only when the request is published.
    #[crate::runtime::test]
    async fn joined_system_run_drop_publishes_no_root_scope_request() {
        let mut system = Tree::new().spawn().expect("runtime is available");
        let root = Arc::clone(&system.run.root);
        system.scope().request_shutdown();
        assert_eq!(system.run.wait().await, StopReason::ShutdownRequested);

        // Subscribed after the join, so the only pulse it can observe is one
        // the drop below publishes.
        let mut watcher = root.signal().watcher();
        drop(system);

        let mut changed = pin!(watcher.changed());
        assert!(
            matches!(
                changed
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ),
            "a joined driver's SystemRun drop must not publish a root scope request"
        );
    }
}
