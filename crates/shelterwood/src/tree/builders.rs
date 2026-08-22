use std::{fmt, sync::Arc};

use crate::{
    ActorDef, ActorOnceDef, ActorRef, ChildId, Intensity, ScopeDefaults,
    cells::ReserveError,
    plan::{BuilderCore, LowerError},
    policy::{ResolvedDefaults, ScopeFlavor},
    raw::{RawDef, RawOnceDef},
    runtime,
    scope::{DynamicScopeRef, ScopeRef},
    task::{OneShotTaskRef, TaskDef, TaskOnceDef, TaskRef},
};

use super::{
    ActorSlot, Subtree, SubtreeDef, SubtreeOnceDef, SubtreeSlot, System, TaskSlot,
    slots::{ActorKind, Definition, SubtreeKind, TaskKind, reserve_static},
};

/// A root lowering error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BuildError {
    /// No ambient supported async runtime exists.
    #[error("no ambient Tokio runtime is available")]
    NoRuntime,
    /// One or more reserved slots were left undefined.
    #[error("tree contains undefined reserved slots")]
    UnfilledReservations {
        /// Child ids of every undefined reservation.
        paths: Vec<ChildId>,
    },
}
/// Routes a definition rejected before admission through isolated disposal.
///
/// Builder and dynamic-add entry points wrap the definition before evaluating
/// the caller's `Into<ChildId>` hook or raw readiness metadata. A failed
/// reservation therefore arrives here already isolated, and dropping the
/// wrapper cannot run a possibly blocking or panicking user destructor on the
/// caller instead of producing an ordinary [`ReserveError`]. This guarantee
/// covers the supplied definition only; Rust's ordinary unwind rules still
/// govern every other caller-owned local.
pub(super) fn dispose_rejected<D: Send + 'static>(
    definition: runtime::Isolated<D>,
    error: ReserveError,
) -> ReserveError {
    drop(definition);
    error
}

fn add_definition<D: Definition>(
    core: &mut BuilderCore,
    id: impl Into<ChildId>,
    definition: D,
) -> Result<D::Handles, ReserveError> {
    let mut definition = runtime::Isolated::new(definition);
    match reserve_static::<D::Kind>(core, id) {
        Ok(slot) => Ok(slot.define(definition.take().expect("isolated definition is available"))),
        Err(error) => Err(dispose_rejected(definition, error)),
    }
}

macro_rules! impl_common_builder_surface {
    ($($builder:ty => $member_note:literal),+ $(,)?) => {
        $(
            impl $builder {
                impl_common_builder_surface!(@methods $member_note);
            }
        )+
    };
    (@methods $member_note:literal) => {
        /// Sets the scope restart-intensity budget.
        pub fn intensity(&mut self, intensity: Intensity) -> &mut Self {
            self.core.set_intensity(intensity);
            self
        }

        /// Sets child policy defaults for this scope.
        pub fn defaults(&mut self, defaults: ScopeDefaults) -> &mut Self {
            self.core.set_defaults(defaults);
            self
        }

        /// Reserves an actor membership and returns its pre-spawn handle slot.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn reserve_actor<M: Send + 'static>(
            &mut self,
            id: impl Into<ChildId>,
        ) -> Result<ActorSlot<M>, ReserveError> {
            reserve_static::<ActorKind<M>>(&mut self.core, id).map(|core| ActorSlot { core })
        }

        /// Adds a restartable callback-oriented actor.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_actor<A: crate::Actor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: ActorDef<A>,
        ) -> Result<ActorRef<A::Msg>, ReserveError> {
            add_definition(&mut self.core, id, definition)
        }

        /// Adds a consuming one-shot callback-oriented actor.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_actor_once<A: crate::Actor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: ActorOnceDef<A>,
        ) -> Result<ActorRef<A::Msg>, ReserveError> {
            add_definition(&mut self.core, id, definition)
        }

        /// Adds a restartable raw actor.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_raw<R: crate::RawActor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: RawDef<R>,
        ) -> Result<ActorRef<R::Msg>, ReserveError> {
            add_definition(&mut self.core, id, definition)
        }

        /// Adds a consuming one-shot raw actor.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_raw_once<R: crate::RawActor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: RawOnceDef<R>,
        ) -> Result<ActorRef<R::Msg>, ReserveError> {
            add_definition(&mut self.core, id, definition)
        }

        /// Reserves a task membership and returns its pre-spawn handle slot.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn reserve_task(&mut self, id: impl Into<ChildId>) -> Result<TaskSlot, ReserveError> {
            reserve_static::<TaskKind>(&mut self.core, id).map(|core| TaskSlot { core })
        }

        /// Adds a restartable task.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_task(
            &mut self,
            id: impl Into<ChildId>,
            definition: TaskDef,
        ) -> Result<TaskRef, ReserveError> {
            add_definition(&mut self.core, id, definition)
        }

        /// Adds a consuming one-shot task and its typed completion claim.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_task_once<T: Send + 'static>(
            &mut self,
            id: impl Into<ChildId>,
            definition: TaskOnceDef<T>,
        ) -> Result<(TaskRef, OneShotTaskRef<T>), ReserveError> {
            add_definition(&mut self.core, id, definition)
        }

        /// Reserves a typed subtree membership.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn reserve_subtree<T: Subtree>(
            &mut self,
            id: impl Into<ChildId>,
        ) -> Result<SubtreeSlot<T>, ReserveError> {
            reserve_static::<SubtreeKind<T>>(&mut self.core, id).map(|core| SubtreeSlot { core })
        }

        /// Adds a restartable subtree.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_subtree<T: Subtree>(
            &mut self,
            id: impl Into<ChildId>,
            definition: SubtreeDef<T>,
        ) -> Result<T::Ref, ReserveError> {
            add_definition(&mut self.core, id, definition)
        }

        /// Adds a consuming one-shot subtree.
        #[doc = $member_note]
        ///
        /// # Errors
        ///
        /// Fails with [`ReserveError::EmptyId`] or
        /// [`ReserveError::DuplicateId`] when the id is unusable, or with
        /// [`ReserveError::IdentityExhausted`]; no other variant is
        /// reachable from a pre-spawn builder.
        pub fn add_subtree_once<T: Subtree>(
            &mut self,
            id: impl Into<ChildId>,
            definition: SubtreeOnceDef<T>,
        ) -> Result<T::Ref, ReserveError> {
            add_definition(&mut self.core, id, definition)
        }
    };
}

/// A fixed-membership, readiness-ordered tree declaration.
///
/// Declaration order is the contract: children start in the order they were
/// added, each child's readiness gates the start of the next, and shutdown
/// stops them in reverse declaration order, one fully joined child at a
/// time. Membership is fixed at [`Tree::spawn`] — an ordered scope neither
/// admits nor removes children at runtime; declare a [`DynamicTree`] where
/// that is needed.
///
/// Every `add_*` method returns the child's membership-addressed handle
/// before anything runs. The `reserve_*` methods ([`Tree::reserve_actor`],
/// [`Tree::reserve_task`], [`Tree::reserve_subtree`]) split reservation from
/// definition so cyclically wired children can hold each other's handles
/// before either is defined; every reservation must be defined before
/// [`Tree::spawn`]. Nested composition goes through [`SubtreeDef`] and
/// [`SubtreeOnceDef`], and [`Tree::spawn`] returns the owning [`System`].
///
/// # Examples
///
/// A task and an actor under one ordered tree. The task is declared first,
/// so it starts first, its readiness gates the actor's start, and it stops
/// last:
///
/// ```rust
/// # use std::time::Duration;
/// # use shelterwood::{Actor, ActorDef, Context, ExitError, ExitResult, TaskDef, Tree};
/// # struct Greeter;
/// # impl Actor for Greeter {
/// #     type Msg = String;
/// #     type Args = ();
/// #     async fn init(_args: (), _context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
/// #         Ok(Self)
/// #     }
/// #     async fn handle(&mut self, name: String, _context: &mut Context<'_, Self>) -> ExitResult {
/// #         println!("hello, {name}");
/// #         Ok(())
/// #     }
/// # }
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut tree = Tree::new();
///
/// tree.add_task(
///     "ticker",
///     TaskDef::new(|context| async move {
///         context.shutdown_token().cancelled().await;
///         Ok(())
///     }),
/// )?;
/// let greeter = tree.add_actor("greeter", ActorDef::<Greeter>::cloned(()))?;
///
/// let system = tree.spawn()?;
/// system.wait_started().await?;
///
/// greeter.send("world".to_string()).await?;
///
/// system.shutdown(Duration::from_secs(5)).await?;
/// # Ok(())
/// # }
/// ```
pub struct Tree {
    pub(super) core: BuilderCore,
}

impl fmt::Debug for Tree {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Tree")
            .field("children", &self.core.slots.len())
            .field("config", &self.core.config_debug())
            .finish_non_exhaustive()
    }
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}

impl Tree {
    /// Creates an empty ordered tree declaration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            core: BuilderCore::new(ScopeFlavor::Ordered),
        }
    }

    /// Lowers and starts this tree synchronously.
    ///
    /// The returned [`System`] is the sole owner of the running root.
    /// Startup proceeds asynchronously from here; await
    /// [`System::wait_started`] for the outcome, or use
    /// [`System::start_or_shutdown`] to roll a failed startup back.
    ///
    /// # Errors
    ///
    /// Returns [`BuildError::NoRuntime`] outside an ambient supported async
    /// runtime, and [`BuildError::UnfilledReservations`] when any reserved
    /// slot was left undefined.
    pub fn spawn(self) -> Result<System<ScopeRef>, BuildError> {
        spawn_builder(self.core, |scope| scope)
    }
}

/// A runtime-dynamic tree declaration.
///
/// A dynamic scope starts its declared children concurrently — no child's
/// readiness gates a sibling's start — and shutdown runs every member's
/// stop ladder together rather than in declaration order. Membership stays
/// open after [`DynamicTree::spawn`]: the [`DynamicScopeRef`] returned by
/// [`System::scope`] admits new members through its own `add_*` and
/// `reserve_*` methods and removes existing ones through
/// [`DynamicScopeRef::remove`] and its typed variants.
///
/// The builder surface here declares the scope's *initial* membership.
/// [`System::wait_started`] aggregates readiness over those initial members
/// only; children admitted later through the scope handle never join the
/// aggregate. The same reservation/definition split as on [`Tree`] is
/// available through [`DynamicTree::reserve_actor`],
/// [`DynamicTree::reserve_task`], and [`DynamicTree::reserve_subtree`].
pub struct DynamicTree {
    pub(super) core: BuilderCore,
}

impl fmt::Debug for DynamicTree {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamicTree")
            .field("children", &self.core.slots.len())
            .field("config", &self.core.config_debug())
            .finish_non_exhaustive()
    }
}

impl Default for DynamicTree {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicTree {
    /// Creates an empty dynamic tree declaration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            core: BuilderCore::new(ScopeFlavor::Dynamic),
        }
    }

    /// Lowers and starts this tree synchronously.
    ///
    /// The returned [`System`] is the sole owner of the running root, and
    /// [`System::scope`] hands out the [`DynamicScopeRef`] used for runtime
    /// admission and removal.
    ///
    /// # Errors
    ///
    /// Returns [`BuildError::NoRuntime`] outside an ambient supported async
    /// runtime, and [`BuildError::UnfilledReservations`] when any reserved
    /// slot was left undefined.
    pub fn spawn(self) -> Result<System<DynamicScopeRef>, BuildError> {
        spawn_builder(self.core, DynamicScopeRef)
    }
}

impl_common_builder_surface!(
    Tree => "",
    DynamicTree => "\nThe membership minted here belongs to this scope's *initial* set: \
        aggregate readiness counts initial members only, and children added at runtime \
        through [`DynamicScopeRef`] never join the aggregate.",
);

fn spawn_builder<R>(
    core: BuilderCore,
    make_ref: impl FnOnce(ScopeRef) -> R,
) -> Result<System<R>, BuildError> {
    if !runtime::is_available() {
        return Err(BuildError::NoRuntime);
    }
    let root = Arc::clone(&core.root);
    let plan = core
        .lower(ResolvedDefaults::default(), None)
        .map_err(|error| match error {
            LowerError::Undefined { paths, .. } => BuildError::UnfilledReservations { paths },
            LowerError::IdentityExhausted { .. } => {
                unreachable!("root lowering does not mint memberships")
            }
        })?;
    let scope_ref = ScopeRef { cell: root };
    let run = crate::driver::spawn_system(plan);
    Ok(System {
        root: make_ref(scope_ref),
        run,
    })
}

#[cfg(test)]
impl Tree {
    pub(crate) fn lower_for_test(self) -> crate::plan::ScopePlan {
        self.core
            .lower(ResolvedDefaults::default(), None)
            .expect("test tree must be fully defined")
    }

    pub(crate) fn into_core_for_test(self) -> BuilderCore {
        self.core
    }
}

#[cfg(test)]
impl DynamicTree {
    pub(crate) fn lower_for_test(self) -> crate::plan::ScopePlan {
        self.core
            .lower(ResolvedDefaults::default(), None)
            .expect("test tree must be fully defined")
    }
}
