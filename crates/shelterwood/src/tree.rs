//! Tree declarations, scope handles, and the owning system façade.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    hash::{Hash, Hasher},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    ActorDef, ActorOnceDef, ActorRef, ChildId, DefaultsInheritance, Exit, Intensity, IntensityTrip,
    LifecycleEvents, Membership, ReadinessDeadline, RestartPolicy, Retention, ScopeDefaults,
    ScopeSnapshot, Shutdown, ShutdownTimeout, SnapshotReceiver, StartupFailure, Strategy,
    WaitError,
    driver::{DynamicReservation, MemberCell, ScopeCell},
    identity::ScopeIdentity,
    mailbox::MailboxCell,
    policy::{CommonOptions, IdError, ResolvedCommonOptions, ResolvedDefaults, resolve_common},
    raw::{RawConstruction, RawDef, RawOnceDef},
    runtime::{self, Latch},
    task::{OnceTask, OneShotTaskRef, TaskDef, TaskOnceDef, TaskRef},
};

/// Whether a scope has fixed ordered membership or runtime-dynamic membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeFlavor {
    Ordered,
    Dynamic,
}

/// The terminal reason for a scope incarnation or root system.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StopReason {
    /// A non-empty ordered workload completed naturally.
    Finished,
    /// Shutdown was explicitly requested.
    ShutdownRequested,
    /// The scope exceeded its restart budget.
    IntensityTripped(IntensityTrip),
    /// A nested scope could not complete startup.
    StartupFailed(StartupFailure),
    /// The membership terminalized without an incarnation.
    NeverStarted,
}

/// Failure of the root startup barrier.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum StartupError {
    /// A child or nested lowering failed terminally during startup.
    #[error("tree startup failed")]
    StartupFailed(StartupFailure),
    /// Restart intensity tripped during startup.
    #[error("restart intensity tripped during startup")]
    IntensityTripped(IntensityTrip),
    /// Shutdown began before startup completed.
    #[error("shutdown was requested during startup")]
    ShutdownRequested,
}

/// Startup failure paired with any rollback timeout report.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("tree startup failed and was rolled back")]
pub struct StartOrShutdownError {
    /// Original startup error.
    pub startup: StartupError,
    /// Stragglers forced down after the rollback bound, if any.
    pub rollback_timeout: Option<ShutdownTimeout>,
}

/// A declaration-time reservation error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ReserveError {
    /// The child id was empty.
    #[error("child id must not be empty")]
    EmptyId,
    /// A resident membership already occupies the id.
    #[error("child id `{0}` is already resident")]
    DuplicateId(ChildId),
    /// A same-id membership is currently being removed.
    #[error("child id `{0}` is being removed")]
    RemovalInProgress(ChildId),
    /// The target dynamic scope is not admitting.
    #[error("scope is not admitting: {0:?}")]
    NotAdmitting(NotAdmittingCause),
    /// The scope can mint no further membership identities.
    #[error("membership identity space is exhausted")]
    IdentityExhausted,
}

/// Exact reason an admission operation could not proceed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NotAdmittingCause {
    /// The scope membership is terminal.
    Terminal,
    /// The live scope incarnation is draining.
    Draining,
    /// The dynamic root is parked after startup failure.
    StartupFailed,
    /// No scope incarnation is currently live.
    NoLiveIncarnation,
    /// This operation's reservation ended before admission.
    ReservationEnded,
}

/// A root lowering error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// No ambient supported async runtime exists.
    #[error("no ambient Tokio runtime is available")]
    NoRuntime,
    /// One or more reserved slots were left undefined.
    #[error("tree contains undefined reserved slots")]
    UnfilledReservations {
        /// Child-id paths of every undefined reservation.
        paths: Vec<Vec<ChildId>>,
    },
}

#[derive(Debug)]
pub(crate) enum LowerError {
    Undefined {
        paths: Vec<Vec<ChildId>>,
        disposal: crate::runtime::Latch,
    },
    IdentityExhausted {
        id: ChildId,
        disposal: crate::runtime::Latch,
    },
}

/// Outcome of an idempotent dynamic removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveOutcome {
    /// A reservation or resident membership was removed.
    Removed,
    /// No matching membership remained to remove.
    AlreadyAbsent,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ScopeConfig {
    pub(crate) strategy: Strategy,
    pub(crate) intensity: Intensity,
    pub(crate) defaults: ScopeDefaults,
}

pub(crate) enum ChildConstruction {
    Raw(RawConstruction),
    Task(TaskDef),
    TaskOnce(OnceTask),
    Scope(Box<ScopeConstruction>),
}

enum DefinitionState {
    Undefined,
    Defined(crate::runtime::Isolated<ChildConstruction>),
    Lowered,
}

pub(crate) struct SlotCell {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) scope: Option<Arc<ScopeCell>>,
    definition: Mutex<DefinitionState>,
}

impl SlotCell {
    pub(crate) fn new(member: Arc<MemberCell>, scope: Option<Arc<ScopeCell>>) -> Arc<Self> {
        Arc::new(Self {
            member,
            scope,
            definition: Mutex::new(DefinitionState::Undefined),
        })
    }

    pub(crate) fn define(&self, definition: ChildConstruction) {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        match *state {
            DefinitionState::Undefined => {
                *state = DefinitionState::Defined(crate::runtime::Isolated::new(definition));
            }
            DefinitionState::Defined(_) | DefinitionState::Lowered => {
                panic!("a child slot was defined more than once")
            }
        }
    }

    pub(crate) fn is_undefined(&self) -> bool {
        matches!(
            *self.definition.lock().expect("definition mutex poisoned"),
            DefinitionState::Undefined
        )
    }

    pub(crate) fn take_definition(&self) -> Option<crate::runtime::Isolated<ChildConstruction>> {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        match std::mem::replace(&mut *state, DefinitionState::Lowered) {
            DefinitionState::Defined(definition) => Some(definition),
            DefinitionState::Undefined => {
                *state = DefinitionState::Undefined;
                None
            }
            DefinitionState::Lowered => panic!("a tree was lowered more than once"),
        }
    }

    pub(crate) fn take_defined(&self) -> Option<crate::runtime::Isolated<ChildConstruction>> {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        match std::mem::replace(&mut *state, DefinitionState::Lowered) {
            DefinitionState::Defined(definition) => Some(definition),
            DefinitionState::Undefined => {
                *state = DefinitionState::Undefined;
                None
            }
            DefinitionState::Lowered => None,
        }
    }
}

pub(crate) struct BuilderCore {
    root: Arc<ScopeCell>,
    flavor: ScopeFlavor,
    config: ScopeConfig,
    slots: Vec<Arc<SlotCell>>,
    ids: HashMap<ChildId, Arc<SlotCell>>,
    armed: bool,
}

impl BuilderCore {
    fn begin_failed_disposal(&self) -> crate::runtime::Latch {
        let definitions = self
            .slots
            .iter()
            .filter_map(|slot| slot.take_defined())
            .filter_map(|mut definition| definition.take())
            .collect();
        crate::runtime::dispose_all(definitions)
    }

    fn new(flavor: ScopeFlavor) -> Self {
        let root_id = ChildId::from("$root");
        let mut root_identity =
            ScopeIdentity::new().expect("global scope identity space exhausted");
        let membership = root_identity
            .mint_membership(&root_id)
            .expect("fresh scope identity must mint its root membership");
        let member = MemberCell::new(root_id, membership);
        let child_identity = ScopeIdentity::new().expect("global scope identity space exhausted");
        let root = ScopeCell::new(member, flavor, child_identity);
        Self {
            root,
            flavor,
            config: ScopeConfig::default(),
            slots: Vec::new(),
            ids: HashMap::new(),
            armed: true,
        }
    }

    fn reserve(
        &mut self,
        id: impl Into<ChildId>,
        scope: Option<ScopeFlavor>,
    ) -> Result<Arc<SlotCell>, ReserveError> {
        let id = checked_id(id)?;
        if self.ids.contains_key(&id) {
            return Err(ReserveError::DuplicateId(id));
        }
        let membership = self
            .root
            .child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&id)
            .ok_or(ReserveError::IdentityExhausted)?;
        let member = MemberCell::new(id.clone(), membership);
        let scope = scope.map(|flavor| {
            let identity = ScopeIdentity::new().expect("global scope identity space exhausted");
            ScopeCell::new(Arc::clone(&member), flavor, identity)
        });
        let slot = SlotCell::new(member, scope);
        self.ids.insert(id, Arc::clone(&slot));
        self.slots.push(Arc::clone(&slot));
        Ok(slot)
    }

    pub(crate) fn lower(
        mut self,
        inherited: ResolvedDefaults,
        root_override: Option<Arc<ScopeCell>>,
    ) -> Result<ScopePlan, LowerError> {
        let root = root_override.unwrap_or_else(|| Arc::clone(&self.root));
        let undefined: Vec<_> = self
            .slots
            .iter()
            .filter(|slot| slot.is_undefined())
            .map(|slot| vec![slot.member.id().clone()])
            .collect();
        if !undefined.is_empty() {
            let disposal = self.begin_failed_disposal();
            return Err(LowerError::Undefined {
                paths: undefined,
                disposal,
            });
        }
        if !Arc::ptr_eq(&root, &self.root) {
            let mut identity = root
                .child_identity
                .lock()
                .expect("scope identity mutex poisoned");
            for slot in &self.slots {
                let Some(membership) =
                    identity.adopt_or_mint_membership(slot.member.id(), slot.member.membership())
                else {
                    let id = slot.member.id().clone();
                    drop(identity);
                    let disposal = self.begin_failed_disposal();
                    return Err(LowerError::IdentityExhausted { id, disposal });
                };
                if membership != slot.member.membership() {
                    slot.member.rebase_membership(membership);
                }
            }
        }
        root.set_config(self.config.clone());
        let defaults = inherited.overlay(&self.config.defaults);
        let mut children = Vec::with_capacity(self.slots.len());
        for slot in &self.slots {
            let definition = slot
                .take_definition()
                .expect("validated slot must have a definition");
            let (options, one_shot) = match definition.get() {
                ChildConstruction::Raw(definition) => (&definition.options, definition.one_shot()),
                ChildConstruction::Task(definition) => (&definition.options, false),
                ChildConstruction::TaskOnce(definition) => (&definition.options, true),
                ChildConstruction::Scope(definition) => {
                    (&definition.options, definition.one_shot())
                }
            };
            let resolved =
                resolve_common(options, &defaults, one_shot, crate::Readiness::Immediate);
            slot.member.set_options(resolved.clone());
            children.push(ChildPlan {
                slot: Arc::clone(slot),
                construction: definition,
                options: resolved,
            });
        }
        self.armed = false;
        Ok(ScopePlan {
            root,
            flavor: self.flavor,
            config: self.config.clone(),
            defaults,
            children,
            armed: true,
        })
    }

    fn terminalize(&self) {
        for slot in &self.slots {
            slot.member.terminalize(Exit::never_started());
            if let Some(scope) = &slot.scope {
                scope.terminalize_never_started();
            }
        }
        self.root.terminalize_never_started();
    }
}

impl Drop for BuilderCore {
    fn drop(&mut self) {
        if self.armed {
            self.terminalize();
        }
    }
}

pub(crate) struct ScopePlan {
    pub(crate) root: Arc<ScopeCell>,
    pub(crate) flavor: ScopeFlavor,
    pub(crate) config: ScopeConfig,
    pub(crate) defaults: ResolvedDefaults,
    pub(crate) children: Vec<ChildPlan>,
    pub(crate) armed: bool,
}

impl Drop for ScopePlan {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for child in &self.children {
            child.slot.member.terminalize(Exit::never_started());
            if let Some(scope) = &child.slot.scope {
                scope.terminalize_never_started();
            }
        }
        // Lowering can publish the planned children before ScopeRuntime takes
        // ownership. If construction then unwinds, the plan fallback also
        // owns those residencies and their matching Removed edges.
        self.root.clear_residents();
        self.root.terminalize_never_started();
    }
}

pub(crate) struct ChildPlan {
    pub(crate) slot: Arc<SlotCell>,
    pub(crate) construction: crate::runtime::Isolated<ChildConstruction>,
    pub(crate) options: ResolvedCommonOptions,
}

fn checked_id(id: impl Into<ChildId>) -> Result<ChildId, ReserveError> {
    let id = id.into();
    ChildId::validate(id.as_str().to_owned()).map_err(|error| match error {
        IdError::Empty => ReserveError::EmptyId,
    })
}

fn attach_actor_slot<M: Send + 'static>(slot: Arc<SlotCell>) -> ActorSlot<M> {
    let mailbox = MailboxCell::new(slot.member.id().clone());
    slot.member.attach_mailbox(mailbox.clone());
    ActorSlot { slot, mailbox }
}

/// A fixed-membership, readiness-ordered tree declaration.
pub struct Tree {
    core: BuilderCore,
}

impl fmt::Debug for Tree {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Tree")
            .field("children", &self.core.slots.len())
            .field("config", &self.core.config)
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

    /// Sets the ordered scope's fate-sharing strategy.
    pub fn strategy(&mut self, strategy: Strategy) -> &mut Self {
        self.core.config.strategy = strategy;
        self
    }

    /// Sets the scope restart-intensity budget.
    pub fn intensity(&mut self, intensity: Intensity) -> &mut Self {
        self.core.config.intensity = intensity;
        self
    }

    /// Sets child policy defaults for this scope.
    pub fn defaults(&mut self, defaults: ScopeDefaults) -> &mut Self {
        self.core.config.defaults = defaults;
        self
    }

    /// Reserves an actor membership and returns its pre-spawn handle slot.
    pub fn reserve_actor<M: Send + 'static>(
        &mut self,
        id: impl Into<ChildId>,
    ) -> Result<ActorSlot<M>, ReserveError> {
        self.core.reserve(id, None).map(attach_actor_slot)
    }

    /// Adds a restartable callback-oriented actor.
    pub fn add_actor<A: crate::Actor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: ActorDef<A>,
    ) -> Result<ActorRef<A::Msg>, ReserveError> {
        self.reserve_actor(id).map(|slot| slot.define(definition))
    }

    /// Adds a consuming one-shot callback-oriented actor.
    pub fn add_actor_once<A: crate::Actor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: ActorOnceDef<A>,
    ) -> Result<ActorRef<A::Msg>, ReserveError> {
        self.reserve_actor(id)
            .map(|slot| slot.define_once(definition))
    }

    /// Adds a restartable raw actor.
    pub fn add_raw<R: crate::RawActor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: RawDef<R>,
    ) -> Result<ActorRef<R::Msg>, ReserveError> {
        self.reserve_actor(id)
            .map(|slot| slot.define_raw(definition))
    }

    /// Adds a consuming one-shot raw actor.
    pub fn add_raw_once<R: crate::RawActor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: RawOnceDef<R>,
    ) -> Result<ActorRef<R::Msg>, ReserveError> {
        self.reserve_actor(id)
            .map(|slot| slot.define_once_raw(definition))
    }

    /// Reserves a task membership and returns its pre-spawn handle slot.
    pub fn reserve_task(&mut self, id: impl Into<ChildId>) -> Result<TaskSlot, ReserveError> {
        self.core.reserve(id, None).map(|slot| TaskSlot { slot })
    }

    /// Adds a restartable task.
    pub fn add_task(
        &mut self,
        id: impl Into<ChildId>,
        definition: TaskDef,
    ) -> Result<TaskRef, ReserveError> {
        self.reserve_task(id).map(|slot| slot.define(definition))
    }

    /// Adds a consuming one-shot task and its typed completion claim.
    pub fn add_task_once<T: Send + 'static>(
        &mut self,
        id: impl Into<ChildId>,
        definition: TaskOnceDef<T>,
    ) -> Result<(TaskRef, OneShotTaskRef<T>), ReserveError> {
        self.reserve_task(id)
            .map(|slot| slot.define_once(definition))
    }

    /// Reserves a typed subtree membership.
    pub fn reserve_subtree<T: Subtree>(
        &mut self,
        id: impl Into<ChildId>,
    ) -> Result<SubtreeSlot<T>, ReserveError> {
        self.core
            .reserve(id, Some(<T as sealed::Sealed>::FLAVOR))
            .map(|slot| SubtreeSlot {
                slot,
                marker: PhantomData,
            })
    }

    /// Adds a restartable subtree.
    pub fn add_subtree<T: Subtree>(
        &mut self,
        id: impl Into<ChildId>,
        definition: SubtreeDef<T>,
    ) -> Result<T::Ref, ReserveError> {
        self.reserve_subtree::<T>(id)
            .map(|slot| slot.define(definition))
    }

    /// Adds a consuming one-shot subtree.
    pub fn add_subtree_once<T: Subtree>(
        &mut self,
        id: impl Into<ChildId>,
        definition: SubtreeOnceDef<T>,
    ) -> Result<T::Ref, ReserveError> {
        self.reserve_subtree::<T>(id)
            .map(|slot| slot.define_once(definition))
    }

    /// Lowers and starts this tree synchronously.
    pub fn spawn(self) -> Result<System<ScopeRef>, BuildError> {
        spawn_builder(self.core, |scope| scope)
    }
}

/// A runtime-dynamic tree declaration.
pub struct DynamicTree {
    core: BuilderCore,
}

impl fmt::Debug for DynamicTree {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamicTree")
            .field("children", &self.core.slots.len())
            .field("config", &self.core.config)
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

    /// Sets the scope restart-intensity budget.
    pub fn intensity(&mut self, intensity: Intensity) -> &mut Self {
        self.core.config.intensity = intensity;
        self
    }

    /// Sets child policy defaults for this scope.
    pub fn defaults(&mut self, defaults: ScopeDefaults) -> &mut Self {
        self.core.config.defaults = defaults;
        self
    }

    /// Reserves an initial actor membership.
    pub fn reserve_actor<M: Send + 'static>(
        &mut self,
        id: impl Into<ChildId>,
    ) -> Result<ActorSlot<M>, ReserveError> {
        self.core.reserve(id, None).map(attach_actor_slot)
    }

    /// Adds an initial restartable callback-oriented actor.
    pub fn add_actor<A: crate::Actor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: ActorDef<A>,
    ) -> Result<ActorRef<A::Msg>, ReserveError> {
        self.reserve_actor(id).map(|slot| slot.define(definition))
    }

    /// Adds an initial consuming one-shot callback-oriented actor.
    pub fn add_actor_once<A: crate::Actor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: ActorOnceDef<A>,
    ) -> Result<ActorRef<A::Msg>, ReserveError> {
        self.reserve_actor(id)
            .map(|slot| slot.define_once(definition))
    }

    /// Adds an initial restartable raw actor.
    pub fn add_raw<R: crate::RawActor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: RawDef<R>,
    ) -> Result<ActorRef<R::Msg>, ReserveError> {
        self.reserve_actor(id)
            .map(|slot| slot.define_raw(definition))
    }

    /// Adds an initial consuming one-shot raw actor.
    pub fn add_raw_once<R: crate::RawActor>(
        &mut self,
        id: impl Into<ChildId>,
        definition: RawOnceDef<R>,
    ) -> Result<ActorRef<R::Msg>, ReserveError> {
        self.reserve_actor(id)
            .map(|slot| slot.define_once_raw(definition))
    }

    /// Reserves an initial task membership.
    pub fn reserve_task(&mut self, id: impl Into<ChildId>) -> Result<TaskSlot, ReserveError> {
        self.core.reserve(id, None).map(|slot| TaskSlot { slot })
    }

    /// Adds an initial restartable task.
    pub fn add_task(
        &mut self,
        id: impl Into<ChildId>,
        definition: TaskDef,
    ) -> Result<TaskRef, ReserveError> {
        self.reserve_task(id).map(|slot| slot.define(definition))
    }

    /// Adds an initial one-shot task.
    pub fn add_task_once<T: Send + 'static>(
        &mut self,
        id: impl Into<ChildId>,
        definition: TaskOnceDef<T>,
    ) -> Result<(TaskRef, OneShotTaskRef<T>), ReserveError> {
        self.reserve_task(id)
            .map(|slot| slot.define_once(definition))
    }

    /// Reserves an initial typed subtree membership.
    pub fn reserve_subtree<T: Subtree>(
        &mut self,
        id: impl Into<ChildId>,
    ) -> Result<SubtreeSlot<T>, ReserveError> {
        self.core
            .reserve(id, Some(<T as sealed::Sealed>::FLAVOR))
            .map(|slot| SubtreeSlot {
                slot,
                marker: PhantomData,
            })
    }

    /// Adds an initial restartable subtree.
    pub fn add_subtree<T: Subtree>(
        &mut self,
        id: impl Into<ChildId>,
        definition: SubtreeDef<T>,
    ) -> Result<T::Ref, ReserveError> {
        self.reserve_subtree::<T>(id)
            .map(|slot| slot.define(definition))
    }

    /// Adds an initial one-shot subtree.
    pub fn add_subtree_once<T: Subtree>(
        &mut self,
        id: impl Into<ChildId>,
        definition: SubtreeOnceDef<T>,
    ) -> Result<T::Ref, ReserveError> {
        self.reserve_subtree::<T>(id)
            .map(|slot| slot.define_once(definition))
    }

    /// Lowers and starts this tree synchronously.
    pub fn spawn(self) -> Result<System<DynamicScopeRef>, BuildError> {
        spawn_builder(self.core, DynamicScopeRef)
    }
}

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
        armed: true,
    })
}

#[cfg(test)]
pub(crate) fn lower_tree_for_test(tree: Tree) -> ScopePlan {
    tree.core
        .lower(ResolvedDefaults::default(), None)
        .expect("test tree must be fully defined")
}

#[cfg(test)]
pub(crate) fn into_core_for_test(tree: Tree) -> BuilderCore {
    tree.core
}

/// An owned pre-spawn actor slot with a stable mailbox binding.
pub struct ActorSlot<M> {
    slot: Arc<SlotCell>,
    mailbox: Arc<MailboxCell<M>>,
}

impl<M> fmt::Debug for ActorSlot<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorSlot")
            .field("id", self.slot.member.id())
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> ActorSlot<M> {
    /// Returns the membership-addressed handle before definition or spawn.
    #[must_use]
    pub fn actor_ref(&self) -> ActorRef<M> {
        ActorRef::new(Arc::clone(&self.slot.member), Arc::clone(&self.mailbox))
    }

    /// Defines a restartable callback-oriented actor and consumes the slot.
    #[must_use]
    pub fn define<A>(self, definition: ActorDef<A>) -> ActorRef<M>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_raw(definition.into_raw())
    }

    /// Defines a consuming one-shot callback-oriented actor and consumes the slot.
    #[must_use]
    pub fn define_once<A>(self, definition: ActorOnceDef<A>) -> ActorRef<M>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_once_raw(definition.into_raw())
    }

    /// Defines a restartable raw actor and consumes the slot.
    #[must_use]
    pub fn define_raw<R>(self, definition: RawDef<R>) -> ActorRef<M>
    where
        R: crate::RawActor<Msg = M>,
    {
        let actor = self.actor_ref();
        self.slot
            .define(ChildConstruction::Raw(definition.erase(self.mailbox)));
        actor
    }

    /// Defines a consuming one-shot raw actor and consumes the slot.
    #[must_use]
    pub fn define_once_raw<R>(self, definition: RawOnceDef<R>) -> ActorRef<M>
    where
        R: crate::RawActor<Msg = M>,
    {
        let actor = self.actor_ref();
        self.slot
            .define(ChildConstruction::Raw(definition.erase(self.mailbox)));
        actor
    }
}

/// An owned pre-spawn task slot.
pub struct TaskSlot {
    slot: Arc<SlotCell>,
}

impl fmt::Debug for TaskSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskSlot")
            .field("id", self.slot.member.id())
            .finish_non_exhaustive()
    }
}

impl TaskSlot {
    /// Returns the membership handle before definition or spawn.
    #[must_use]
    pub fn task_ref(&self) -> TaskRef {
        TaskRef::new(Arc::clone(&self.slot.member))
    }

    /// Defines a restartable task and consumes the slot.
    #[must_use]
    pub fn define(self, definition: TaskDef) -> TaskRef {
        let task = self.task_ref();
        self.slot.define(ChildConstruction::Task(definition));
        task
    }

    /// Defines a one-shot task and consumes the slot.
    pub fn define_once<T: Send + 'static>(
        self,
        definition: TaskOnceDef<T>,
    ) -> (TaskRef, OneShotTaskRef<T>) {
        let task = self.task_ref();
        let (completion, receiver) = runtime::oneshot();
        let claim = OneShotTaskRef::new(receiver, task.clone());
        self.slot
            .define(ChildConstruction::TaskOnce(definition.erase(completion)));
        (task, claim)
    }
}

/// A successful dynamic admission and its exact membership identity.
#[derive(Debug)]
pub struct AdmissionReceipt<H> {
    membership: Membership,
    handles: H,
}

impl<H> AdmissionReceipt<H> {
    /// Returns the admitted membership identity.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.membership
    }

    /// Borrows the kind-specific handles.
    #[must_use]
    pub fn handles(&self) -> &H {
        &self.handles
    }

    /// Consumes the receipt and returns its kind-specific handles.
    #[must_use]
    pub fn into_handles(self) -> H {
        self.handles
    }
}

/// An admission future. Fused additions abort on drop; split definitions
/// detach after their first poll.
#[must_use]
pub struct Admission<H> {
    reservation: Option<DynamicReservation>,
    receipt: Option<AdmissionReceipt<H>>,
    fused_cancel: Option<Latch>,
    inner: Option<AdmissionWait>,
    immediate: Option<ReserveError>,
    polled: bool,
    done: bool,
}

type AdmissionWait = Pin<Box<dyn Future<Output = Result<(), ReserveError>> + Send + 'static>>;

impl<H> fmt::Debug for Admission<H> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Admission")
            .field("polled", &self.polled)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl<H> Admission<H> {
    fn error(error: ReserveError) -> Self {
        Self {
            reservation: None,
            receipt: None,
            fused_cancel: None,
            inner: None,
            immediate: Some(error),
            polled: false,
            done: false,
        }
    }

    fn new(reservation: DynamicReservation, handles: H, fused: bool) -> Self {
        let membership = reservation.slot.member.membership();
        Self {
            reservation: Some(reservation),
            receipt: Some(AdmissionReceipt {
                membership,
                handles,
            }),
            fused_cancel: fused.then(Latch::default),
            inner: None,
            immediate: None,
            polled: false,
            done: false,
        }
    }
}

impl<H: Unpin> Future for Admission<H> {
    type Output = Result<AdmissionReceipt<H>, ReserveError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(error) = self.immediate.take() {
            self.done = true;
            return Poll::Ready(Err(error));
        }
        if !self.polled {
            self.polled = true;
            let reservation = self
                .reservation
                .as_ref()
                .expect("pending admission must carry its reservation");
            let response = crate::driver::start_admission(
                Arc::clone(&reservation.control),
                Arc::clone(&reservation.slot),
                self.fused_cancel.clone(),
            );
            self.inner = Some(Box::pin(async move {
                response
                    .receive()
                    .await
                    .expect("admission response obligation must complete")
            }));
        }
        let poll = self
            .inner
            .as_mut()
            .expect("polled admission must carry its response future")
            .as_mut()
            .poll(context);
        match poll {
            Poll::Ready(Ok(())) => {
                self.done = true;
                Poll::Ready(Ok(self
                    .receipt
                    .take()
                    .expect("successful admission must carry a receipt")))
            }
            Poll::Ready(Err(error)) => {
                self.done = true;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<H> Drop for Admission<H> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let Some(reservation) = &self.reservation else {
            return;
        };
        if let Some(cancel) = &self.fused_cancel {
            crate::driver::signal_fused_cancel(&reservation.control, cancel);
            crate::driver::cancel_dynamic_reservation(&reservation.control, &reservation.slot);
        } else if !self.polled {
            crate::driver::cancel_dynamic_reservation(&reservation.control, &reservation.slot);
        }
    }
}

/// A split dynamic actor reservation with a stable mailbox binding.
pub struct DynamicActorSlot<M> {
    reservation: Option<DynamicReservation>,
    mailbox: Arc<MailboxCell<M>>,
}

impl<M> fmt::Debug for DynamicActorSlot<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamicActorSlot")
            .field(
                "id",
                self.reservation
                    .as_ref()
                    .expect("dynamic actor slot reservation was already consumed")
                    .slot
                    .member
                    .id(),
            )
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> DynamicActorSlot<M> {
    fn reservation(&self) -> &DynamicReservation {
        self.reservation
            .as_ref()
            .expect("dynamic actor slot reservation was already consumed")
    }

    fn take_reservation(&mut self) -> DynamicReservation {
        self.reservation
            .take()
            .expect("dynamic actor slot reservation was already consumed")
    }

    /// Returns the exact actor handle before admission.
    #[must_use]
    pub fn actor_ref(&self) -> ActorRef<M> {
        ActorRef::new(
            Arc::clone(&self.reservation().slot.member),
            Arc::clone(&self.mailbox),
        )
    }

    /// Defines a restartable callback-oriented actor; dropping after first poll detaches.
    pub fn define<A>(self, definition: ActorDef<A>) -> Admission<ActorRef<M>>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_raw(definition.into_raw())
    }

    fn define_fused<A>(self, definition: ActorDef<A>) -> Admission<ActorRef<M>>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_raw_fused(definition.into_raw())
    }

    /// Defines a one-shot callback-oriented actor; dropping after first poll detaches.
    pub fn define_once<A>(self, definition: ActorOnceDef<A>) -> Admission<ActorRef<M>>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_once_raw(definition.into_raw())
    }

    fn define_once_fused<A>(self, definition: ActorOnceDef<A>) -> Admission<ActorRef<M>>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_once_raw_fused(definition.into_raw())
    }

    /// Defines a restartable raw actor; dropping after first poll detaches.
    pub fn define_raw<R>(mut self, definition: RawDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.define_raw_with(definition, false)
    }

    fn define_raw_fused<R>(mut self, definition: RawDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.define_raw_with(definition, true)
    }

    fn define_raw_with<R>(&mut self, definition: RawDef<R>, fused: bool) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        let actor = self.actor_ref();
        let reservation = self.take_reservation();
        reservation.slot.define(ChildConstruction::Raw(
            definition.erase(Arc::clone(&self.mailbox)),
        ));
        Admission::new(reservation, actor, fused)
    }

    /// Defines a one-shot raw actor; dropping after first poll detaches.
    pub fn define_once_raw<R>(mut self, definition: RawOnceDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.define_once_raw_with(definition, false)
    }

    fn define_once_raw_fused<R>(mut self, definition: RawOnceDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.define_once_raw_with(definition, true)
    }

    fn define_once_raw_with<R>(
        &mut self,
        definition: RawOnceDef<R>,
        fused: bool,
    ) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        let actor = self.actor_ref();
        let reservation = self.take_reservation();
        reservation.slot.define(ChildConstruction::Raw(
            definition.erase(Arc::clone(&self.mailbox)),
        ));
        Admission::new(reservation, actor, fused)
    }
}

impl<M> Drop for DynamicActorSlot<M> {
    fn drop(&mut self) {
        if let Some(reservation) = &self.reservation {
            crate::driver::cancel_dynamic_reservation(&reservation.control, &reservation.slot);
        }
    }
}

/// A split dynamic task reservation.
pub struct DynamicTaskSlot {
    reservation: Option<DynamicReservation>,
}

impl fmt::Debug for DynamicTaskSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamicTaskSlot")
            .field("id", self.reservation().slot.member.id())
            .finish_non_exhaustive()
    }
}

impl DynamicTaskSlot {
    fn reservation(&self) -> &DynamicReservation {
        self.reservation
            .as_ref()
            .expect("dynamic task slot reservation was already consumed")
    }

    fn take_reservation(&mut self) -> DynamicReservation {
        self.reservation
            .take()
            .expect("dynamic task slot reservation was already consumed")
    }

    /// Returns the exact task handle before admission.
    #[must_use]
    pub fn task_ref(&self) -> TaskRef {
        TaskRef::new(Arc::clone(&self.reservation().slot.member))
    }

    /// Defines a restartable task; dropping after first poll detaches admission.
    pub fn define(mut self, definition: TaskDef) -> Admission<TaskRef> {
        let task = self.task_ref();
        let reservation = self.take_reservation();
        reservation.slot.define(ChildConstruction::Task(definition));
        Admission::new(reservation, task, false)
    }

    fn define_fused(mut self, definition: TaskDef) -> Admission<TaskRef> {
        let task = self.task_ref();
        let reservation = self.take_reservation();
        reservation.slot.define(ChildConstruction::Task(definition));
        Admission::new(reservation, task, true)
    }

    /// Defines a one-shot task; dropping after first poll detaches admission.
    pub fn define_once<T: Send + 'static>(
        mut self,
        definition: TaskOnceDef<T>,
    ) -> Admission<(TaskRef, OneShotTaskRef<T>)> {
        let task = self.task_ref();
        let (completion, receiver) = runtime::oneshot();
        let reservation = self.take_reservation();
        let claim = OneShotTaskRef::new(receiver, task.clone());
        reservation
            .slot
            .define(ChildConstruction::TaskOnce(definition.erase(completion)));
        Admission::new(reservation, (task, claim), false)
    }

    fn define_once_fused<T: Send + 'static>(
        mut self,
        definition: TaskOnceDef<T>,
    ) -> Admission<(TaskRef, OneShotTaskRef<T>)> {
        let task = self.task_ref();
        let (completion, receiver) = runtime::oneshot();
        let reservation = self.take_reservation();
        let claim = OneShotTaskRef::new(receiver, task.clone());
        reservation
            .slot
            .define(ChildConstruction::TaskOnce(definition.erase(completion)));
        Admission::new(reservation, (task, claim), true)
    }
}

impl Drop for DynamicTaskSlot {
    fn drop(&mut self) {
        if let Some(reservation) = &self.reservation {
            crate::driver::cancel_dynamic_reservation(&reservation.control, &reservation.slot);
        }
    }
}

/// A split dynamic subtree reservation.
pub struct DynamicSubtreeSlot<T: Subtree> {
    reservation: Option<DynamicReservation>,
    marker: PhantomData<fn() -> T>,
}

impl<T: Subtree> fmt::Debug for DynamicSubtreeSlot<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamicSubtreeSlot")
            .field("id", self.reservation().slot.member.id())
            .finish_non_exhaustive()
    }
}

impl<T: Subtree> DynamicSubtreeSlot<T> {
    fn reservation(&self) -> &DynamicReservation {
        self.reservation
            .as_ref()
            .expect("dynamic subtree slot reservation was already consumed")
    }

    fn take_reservation(&mut self) -> DynamicReservation {
        self.reservation
            .take()
            .expect("dynamic subtree slot reservation was already consumed")
    }

    /// Returns the typed exact scope handle before admission.
    #[must_use]
    pub fn scope_ref(&self) -> T::Ref {
        <T as sealed::Sealed>::make_ref(ScopeRef {
            cell: Arc::clone(
                self.reservation()
                    .slot
                    .scope
                    .as_ref()
                    .expect("subtree reservation needs a scope cell"),
            ),
        })
    }

    /// Defines a restartable subtree; dropping after first poll detaches.
    pub fn define(mut self, definition: SubtreeDef<T>) -> Admission<T::Ref> {
        let scope = self.scope_ref();
        let reservation = self.take_reservation();
        reservation
            .slot
            .define(ChildConstruction::Scope(Box::new(definition.erase())));
        Admission::new(reservation, scope, false)
    }

    fn define_fused(mut self, definition: SubtreeDef<T>) -> Admission<T::Ref> {
        let scope = self.scope_ref();
        let reservation = self.take_reservation();
        reservation
            .slot
            .define(ChildConstruction::Scope(Box::new(definition.erase())));
        Admission::new(reservation, scope, true)
    }

    /// Defines a one-shot subtree; dropping after first poll detaches.
    pub fn define_once(mut self, definition: SubtreeOnceDef<T>) -> Admission<T::Ref> {
        let scope = self.scope_ref();
        let reservation = self.take_reservation();
        reservation
            .slot
            .define(ChildConstruction::Scope(Box::new(definition.erase())));
        Admission::new(reservation, scope, false)
    }

    fn define_once_fused(mut self, definition: SubtreeOnceDef<T>) -> Admission<T::Ref> {
        let scope = self.scope_ref();
        let reservation = self.take_reservation();
        reservation
            .slot
            .define(ChildConstruction::Scope(Box::new(definition.erase())));
        Admission::new(reservation, scope, true)
    }
}

impl<T: Subtree> Drop for DynamicSubtreeSlot<T> {
    fn drop(&mut self) {
        if let Some(reservation) = &self.reservation {
            crate::driver::cancel_dynamic_reservation(&reservation.control, &reservation.slot);
        }
    }
}

/// Observation future for a synchronously latched dynamic removal.
#[must_use]
pub struct Removal {
    inner: Pin<Box<dyn Future<Output = RemoveOutcome> + Send + 'static>>,
}

impl fmt::Debug for Removal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Removal").finish_non_exhaustive()
    }
}

impl Removal {
    fn new(response: crate::driver::RemovalResponse) -> Self {
        Self {
            inner: Box::pin(async move {
                response
                    .receive()
                    .await
                    .expect("removal response obligation must complete")
            }),
        }
    }
}

impl Future for Removal {
    type Output = RemoveOutcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(context)
    }
}

/// A restartable subtree definition.
pub struct SubtreeDef<T: Subtree> {
    factory: Box<dyn Fn() -> T + Send + Sync + 'static>,
    options: CommonOptions,
    defaults: DefaultsInheritance,
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
            factory: Box::new(factory),
            options: CommonOptions::default(),
            defaults: DefaultsInheritance::Inherit,
        }
    }

    /// Overrides the restart policy.
    #[must_use]
    pub fn restart(mut self, restart: RestartPolicy) -> Self {
        self.options.restart = Some(restart);
        self
    }

    /// Overrides the shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.options.shutdown = Some(shutdown);
        self
    }

    /// Overrides the structural readiness deadline.
    #[must_use]
    pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
        self.options.readiness_deadline = deadline;
        self
    }

    /// Overrides terminal-membership retention.
    #[must_use]
    pub fn retention(mut self, retention: Retention) -> Self {
        self.options.retention = Some(retention);
        self
    }

    /// Selects inheritance or reset for unset nested defaults.
    #[must_use]
    pub fn defaults(mut self, defaults: DefaultsInheritance) -> Self {
        self.defaults = defaults;
        self
    }

    fn erase(self) -> ScopeConstruction {
        let factory = self.factory;
        ScopeConstruction {
            source: ScopeSource::Restartable(Arc::new(move || {
                <T as sealed::Sealed>::into_core(factory())
            })),
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

    /// Overrides the shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.options.shutdown = Some(shutdown);
        self
    }

    /// Overrides the structural readiness deadline.
    #[must_use]
    pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
        self.options.readiness_deadline = deadline;
        self
    }

    /// Overrides terminal-membership retention.
    #[must_use]
    pub fn retention(mut self, retention: Retention) -> Self {
        self.options.retention = Some(retention);
        self
    }

    /// Selects inheritance or reset for unset nested defaults.
    #[must_use]
    pub fn defaults(mut self, defaults: DefaultsInheritance) -> Self {
        self.defaults = defaults;
        self
    }

    fn erase(self) -> ScopeConstruction {
        ScopeConstruction {
            source: ScopeSource::OneShot(Box::new(<T as sealed::Sealed>::into_core(self.tree))),
            options: self.options,
            defaults: self.defaults,
        }
    }
}

pub(crate) struct ScopeConstruction {
    pub(crate) source: ScopeSource,
    pub(crate) options: CommonOptions,
    pub(crate) defaults: DefaultsInheritance,
}

pub(crate) type ScopeFactory = Arc<dyn Fn() -> BuilderCore + Send + Sync + 'static>;

impl ScopeConstruction {
    pub(crate) fn one_shot(&self) -> bool {
        matches!(self.source, ScopeSource::OneShot(_) | ScopeSource::Spent)
    }
}

pub(crate) enum ScopeSource {
    Restartable(ScopeFactory),
    OneShot(Box<BuilderCore>),
    Spent,
}

/// A typed pre-spawn subtree slot.
pub struct SubtreeSlot<T: Subtree> {
    slot: Arc<SlotCell>,
    marker: PhantomData<fn() -> T>,
}

impl<T: Subtree> fmt::Debug for SubtreeSlot<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubtreeSlot")
            .field("id", self.slot.member.id())
            .finish_non_exhaustive()
    }
}

impl<T: Subtree> SubtreeSlot<T> {
    /// Returns the typed scope handle before definition or spawn.
    #[must_use]
    pub fn scope_ref(&self) -> T::Ref {
        <T as sealed::Sealed>::make_ref(ScopeRef {
            cell: Arc::clone(
                self.slot
                    .scope
                    .as_ref()
                    .expect("subtree slot must carry a scope cell"),
            ),
        })
    }

    /// Defines a restartable subtree and consumes the slot.
    #[must_use]
    pub fn define(self, definition: SubtreeDef<T>) -> T::Ref {
        let scope = self.scope_ref();
        self.slot
            .define(ChildConstruction::Scope(Box::new(definition.erase())));
        scope
    }

    /// Defines a one-shot subtree and consumes the slot.
    #[must_use]
    pub fn define_once(self, definition: SubtreeOnceDef<T>) -> T::Ref {
        let scope = self.scope_ref();
        self.slot
            .define(ChildConstruction::Scope(Box::new(definition.erase())));
        scope
    }
}

#[allow(private_interfaces)]
mod sealed {
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

/// A cheap, membership-addressed ordered scope handle.
#[derive(Clone)]
pub struct ScopeRef {
    pub(crate) cell: Arc<ScopeCell>,
}

impl ScopeRef {
    /// Returns this scope's child id within its parent.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.cell.member.id()
    }

    /// Returns the scope membership identity.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.cell.member.membership()
    }

    /// Computes an authoritative recursive snapshot on demand.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.cell.snapshot()
    }

    /// Subscribes to conflated recursive snapshots.
    #[must_use]
    pub fn subscribe_snapshots(&self) -> SnapshotReceiver {
        self.cell.subscribe_snapshots()
    }

    /// Subscribes to this scope's lifecycle and all forwarded descendants.
    #[must_use]
    pub fn subscribe_lifecycle(&self) -> LifecycleEvents {
        self.cell.subscribe_lifecycle()
    }

    /// Looks up a direct child in an authoritative current snapshot.
    #[must_use]
    pub fn child(&self, id: impl AsRef<str>) -> Option<crate::ChildSnapshot> {
        self.snapshot().child(id).cloned()
    }

    /// Traverses a child-id path in an authoritative current snapshot.
    #[must_use]
    pub fn descendant<I, S>(&self, path: I) -> Option<crate::ChildSnapshot>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.snapshot().descendant(path).cloned()
    }

    /// Waits for a named child snapshot satisfying an at-or-past predicate.
    ///
    /// Snapshot watches conflate intermediate states, so `pred` should accept
    /// every state at or beyond the desired edge and must remain cheap and
    /// non-blocking.
    pub async fn wait_for_child<P>(
        &self,
        id: impl Into<ChildId>,
        mut pred: P,
        deadline: Duration,
    ) -> Result<crate::ChildSnapshot, WaitError>
    where
        P: FnMut(&crate::ChildSnapshot) -> bool + Send,
    {
        let id = id.into();
        let expires = crate::deadline::Deadline::after(crate::runtime::now(), deadline);
        let mut snapshots = self.subscribe_snapshots();

        loop {
            let snapshot = snapshots.borrow_latest();
            if let Some(child) = snapshot.child(id.as_str())
                && pred(child)
            {
                return Ok(child.clone());
            }
            // Inspect the current snapshot before rejecting even an already
            // elapsed (including zero-duration) budget.
            if expires.is_due(crate::runtime::now()) {
                return Err(WaitError::TimedOut);
            }
            if matches!(
                self.cell.member.record().stage,
                crate::driver::MemberStage::Terminal(_)
            ) {
                return Err(WaitError::ScopeTerminated {
                    state: snapshot.state.clone(),
                });
            }
            match crate::runtime::select_two(snapshots.changed(), async {
                match expires.instant() {
                    Some(expires) => crate::runtime::sleep_until_std(expires).await,
                    None => std::future::pending().await,
                }
            })
            .await
            {
                crate::runtime::Either::Left(Ok(_)) => {}
                crate::runtime::Either::Left(Err(_)) => {
                    let snapshot = snapshots.borrow_latest();
                    if let Some(child) = snapshot.child(id.as_str())
                        && pred(child)
                    {
                        return Ok(child.clone());
                    }
                    return Err(WaitError::ScopeTerminated {
                        state: snapshot.state.clone(),
                    });
                }
                crate::runtime::Either::Right(()) => {
                    let snapshot = snapshots.borrow_latest();
                    if let Some(child) = snapshot.child(id.as_str())
                        && pred(child)
                    {
                        return Ok(child.clone());
                    }
                    return Err(WaitError::TimedOut);
                }
            }
        }
    }

    /// Requests shutdown without waiting.
    pub fn request_shutdown(&self) {
        self.cell.request_shutdown();
    }

    /// Waits for terminal membership state.
    pub async fn wait_stopped(&self) -> StopReason {
        self.cell.wait_stopped().await
    }

    /// Requests shutdown and waits for this scope to stop.
    pub async fn shutdown_and_wait(&self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        crate::driver::shutdown_scope(Arc::clone(&self.cell), timeout).await
    }

    /// Dynamically queries whether this scope has dynamic capabilities.
    #[must_use]
    pub fn dynamic(&self) -> Option<DynamicScopeRef> {
        (self.cell.flavor == ScopeFlavor::Dynamic).then(|| DynamicScopeRef(self.clone()))
    }
}

impl fmt::Debug for ScopeRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopeRef")
            .field("membership", &self.membership())
            .finish()
    }
}

// Handle identity is the slot cell, not the membership token: lowering a
// rebuilt nested declaration rebases the token behind live pre-spawn handles,
// and a token-value hash would strand entries keyed before the rebase.
impl PartialEq for ScopeRef {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cell, &other.cell)
    }
}

impl Eq for ScopeRef {}

impl Hash for ScopeRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.cell).hash(state);
    }
}

/// A cheap scope handle carrying dynamic-membership capability.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DynamicScopeRef(ScopeRef);

impl DynamicScopeRef {
    /// Returns the underlying observation/control scope handle.
    #[must_use]
    pub fn as_scope(&self) -> &ScopeRef {
        &self.0
    }

    /// Returns this scope's child id within its parent.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.0.id()
    }

    /// Returns the scope membership identity.
    #[must_use]
    pub fn membership(&self) -> Membership {
        self.0.membership()
    }

    /// Computes an authoritative recursive snapshot on demand.
    #[must_use]
    pub fn snapshot(&self) -> Arc<ScopeSnapshot> {
        self.0.snapshot()
    }

    /// Subscribes to conflated recursive snapshots.
    #[must_use]
    pub fn subscribe_snapshots(&self) -> SnapshotReceiver {
        self.0.subscribe_snapshots()
    }

    /// Subscribes to this scope's lifecycle and all forwarded descendants.
    #[must_use]
    pub fn subscribe_lifecycle(&self) -> LifecycleEvents {
        self.0.subscribe_lifecycle()
    }

    /// Looks up a direct child in an authoritative current snapshot.
    #[must_use]
    pub fn child(&self, id: impl AsRef<str>) -> Option<crate::ChildSnapshot> {
        self.0.child(id)
    }

    /// Traverses a child-id path in an authoritative current snapshot.
    #[must_use]
    pub fn descendant<I, S>(&self, path: I) -> Option<crate::ChildSnapshot>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.0.descendant(path)
    }

    /// Waits for a named child snapshot satisfying an at-or-past predicate.
    pub async fn wait_for_child<P>(
        &self,
        id: impl Into<ChildId>,
        pred: P,
        deadline: Duration,
    ) -> Result<crate::ChildSnapshot, WaitError>
    where
        P: FnMut(&crate::ChildSnapshot) -> bool + Send,
    {
        self.0.wait_for_child(id, pred, deadline).await
    }

    /// Requests shutdown without waiting.
    pub fn request_shutdown(&self) {
        self.0.request_shutdown();
    }

    /// Waits for terminal membership state.
    pub async fn wait_stopped(&self) -> StopReason {
        self.0.wait_stopped().await
    }

    /// Requests shutdown and waits for this scope to stop.
    pub async fn shutdown_and_wait(&self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        self.0.shutdown_and_wait(timeout).await
    }

    /// Reserves an actor id synchronously and exposes its exact handle.
    pub fn reserve_actor<M: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicActorSlot<M>, ReserveError> {
        let id = checked_id(id)?;
        crate::driver::reserve_dynamic(&self.0.cell, id, None).map(|reservation| {
            let mailbox = MailboxCell::new(reservation.slot.member.id().clone());
            reservation.slot.member.attach_mailbox(mailbox.clone());
            DynamicActorSlot {
                reservation: Some(reservation),
                mailbox,
            }
        })
    }

    /// Adds a restartable callback-oriented actor, resolving at admission.
    pub fn add_actor<A: crate::Actor>(
        &self,
        id: impl Into<ChildId>,
        definition: ActorDef<A>,
    ) -> Admission<ActorRef<A::Msg>> {
        match self.reserve_actor(id) {
            Ok(slot) => slot.define_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Adds a consuming one-shot callback-oriented actor, resolving at admission.
    pub fn add_actor_once<A: crate::Actor>(
        &self,
        id: impl Into<ChildId>,
        definition: ActorOnceDef<A>,
    ) -> Admission<ActorRef<A::Msg>> {
        match self.reserve_actor(id) {
            Ok(slot) => slot.define_once_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Adds a restartable raw actor, resolving at admission.
    pub fn add_raw<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        match self.reserve_actor(id) {
            Ok(slot) => slot.define_raw_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Adds a consuming one-shot raw actor, resolving at admission.
    pub fn add_raw_once<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawOnceDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        match self.reserve_actor(id) {
            Ok(slot) => slot.define_once_raw_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Reserves a task id synchronously and exposes its exact handle.
    pub fn reserve_task(&self, id: impl Into<ChildId>) -> Result<DynamicTaskSlot, ReserveError> {
        let id = checked_id(id)?;
        crate::driver::reserve_dynamic(&self.0.cell, id, None).map(|reservation| DynamicTaskSlot {
            reservation: Some(reservation),
        })
    }

    /// Adds a restartable task, resolving at admission rather than startup.
    pub fn add_task(&self, id: impl Into<ChildId>, definition: TaskDef) -> Admission<TaskRef> {
        match self.reserve_task(id) {
            Ok(slot) => slot.define_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Adds a consuming one-shot task, resolving at admission.
    pub fn add_task_once<T: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
        definition: TaskOnceDef<T>,
    ) -> Admission<(TaskRef, OneShotTaskRef<T>)> {
        match self.reserve_task(id) {
            Ok(slot) => slot.define_once_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Reserves a typed subtree id synchronously.
    pub fn reserve_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicSubtreeSlot<T>, ReserveError> {
        let id = checked_id(id)?;
        crate::driver::reserve_dynamic(&self.0.cell, id, Some(<T as sealed::Sealed>::FLAVOR)).map(
            |reservation| DynamicSubtreeSlot {
                reservation: Some(reservation),
                marker: PhantomData,
            },
        )
    }

    /// Adds a restartable subtree, resolving at admission.
    pub fn add_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeDef<T>,
    ) -> Admission<T::Ref> {
        match self.reserve_subtree::<T>(id) {
            Ok(slot) => slot.define_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Adds a consuming one-shot subtree, resolving at admission.
    pub fn add_subtree_once<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeOnceDef<T>,
    ) -> Admission<T::Ref> {
        match self.reserve_subtree::<T>(id) {
            Ok(slot) => slot.define_once_fused(definition),
            Err(error) => Admission::error(error),
        }
    }

    /// Latches id-based removal synchronously; the returned future only
    /// observes completion.
    pub fn remove(&self, id: impl Into<ChildId>) -> Removal {
        let id = id.into();
        Removal::new(crate::driver::remove_dynamic(&self.0.cell, &id, None))
    }

    /// Removes exactly the held task membership, never a same-id successor.
    pub fn remove_task(&self, task: &TaskRef) -> Removal {
        Removal::new(crate::driver::remove_dynamic(
            &self.0.cell,
            task.id(),
            Some(task.membership()),
        ))
    }

    /// Removes exactly the held actor membership, never a same-id successor.
    pub fn remove_actor<M>(&self, actor: &ActorRef<M>) -> Removal {
        Removal::new(crate::driver::remove_dynamic(
            &self.0.cell,
            actor.id(),
            Some(actor.membership()),
        ))
    }

    /// Removes exactly the held ordered-scope membership.
    pub fn remove_scope(&self, scope: &ScopeRef) -> Removal {
        Removal::new(crate::driver::remove_dynamic(
            &self.0.cell,
            scope.id(),
            Some(scope.membership()),
        ))
    }

    /// Removes exactly the held dynamic-scope membership.
    pub fn remove_dynamic_scope(&self, scope: &DynamicScopeRef) -> Removal {
        self.remove_scope(scope.as_scope())
    }
}

#[must_use = "dropping the sole system owner requests graceful shutdown"]
/// The sole owning handle for a running root system.
pub struct System<R = ScopeRef> {
    root: R,
    run: crate::driver::SystemRun,
    armed: bool,
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
                self.armed = false;
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
        let result = self.run.shutdown(timeout).await;
        self.armed = false;
        result
    }

    /// Waits for natural or externally requested terminal state.
    pub async fn wait(mut self) -> StopReason {
        let reason = self.run.wait().await;
        self.armed = false;
        reason
    }
}

impl<R> Drop for System<R> {
    fn drop(&mut self) {
        if self.armed {
            self.run.request_shutdown();
        }
    }
}
