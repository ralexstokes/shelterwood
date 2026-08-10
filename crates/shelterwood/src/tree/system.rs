use std::{fmt, marker::PhantomData, sync::Arc, time::Duration};

use crate::{
    DefaultsInheritance, ReadinessDeadline, RestartPolicy, Retention, Shutdown, ShutdownTimeout,
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
#[must_use = "dropping the sole system owner requests graceful shutdown"]
/// The sole owning handle for a running root system.
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

    /// Waits until the declared tree is ready or startup terminally fails.
    pub async fn wait_started(&self) -> Result<(), StartupError> {
        self.run.root.wait_started().await
    }

    /// Rolls a startup failure back through full shutdown.
    ///
    /// Unlike [`Self::wait_started`], this consumes the owner so a failed
    /// startup cannot leave its successfully started prefix running. The error
    /// preserves both the original startup cause and any rollback timeout.
    pub async fn start_or_shutdown(
        mut self,
        timeout: Duration,
    ) -> Result<Self, StartOrShutdownError> {
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

    /// Requests shutdown, escalates at `timeout`, joins, and consumes owner.
    ///
    /// `timeout` bounds cooperative teardown, not necessarily this method's
    /// wall-clock return: after escalation every actor future is still joined.
    /// Blocking threads created by `run_blocking` detach past hard abort.
    pub async fn shutdown(mut self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        self.run.shutdown(timeout).await
    }

    /// Waits for natural or externally requested terminal state.
    pub async fn wait(mut self) -> StopReason {
        self.run.wait().await
    }
}

#[cfg(test)]
mod tests {
    use super::{DynamicTree, Tree, sealed::Sealed};
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
}
