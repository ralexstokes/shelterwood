use crate::{
    ActorDef, ActorOnceDef, ActorRef, ChildId, Membership,
    cells::ReserveError,
    raw::{RawDef, RawOnceDef},
    runtime::{self, Latch},
    scope::{DynamicScopeRef, ScopeRef},
    task::{OneShotTaskRef, TaskDef, TaskOnceDef, TaskRef},
};

use super::{
    Admission, DynamicActorSlot, DynamicSubtreeSlot, DynamicTaskSlot, Removal, Subtree, SubtreeDef,
    SubtreeOnceDef,
    builders::dispose_rejected,
    slots::{ActorKind, Definition, SubtreeKind, TaskKind, reserve_dynamic},
};

impl DynamicScopeRef {
    fn add_definition<D: Definition>(
        &self,
        id: impl Into<ChildId>,
        definition: D,
    ) -> Admission<D::Handles> {
        let mut definition = runtime::Isolated::new(definition);
        match reserve_dynamic::<D::Kind>(self, id, Some(Latch::default())) {
            Ok(slot) => slot.define(definition.take().expect("isolated definition is available")),
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Reserves an actor id synchronously and exposes its exact handle.
    ///
    /// # Errors
    ///
    /// Fails with [`ReserveError::EmptyId`] for an empty id,
    /// [`ReserveError::NoRuntime`] outside an ambient Tokio runtime,
    /// [`ReserveError::NotAdmitting`] when the scope is terminal, draining,
    /// parked after a startup failure, or has no live incarnation,
    /// and [`ReserveError::RemovalInProgress`] or [`ReserveError::DuplicateId`]
    /// when a same-id member is being removed or is resident.
    pub fn reserve_actor<M: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicActorSlot<M>, ReserveError> {
        reserve_dynamic::<ActorKind<M>>(self, id, None).map(|core| DynamicActorSlot { core })
    }

    /// Adds a restartable callback-oriented actor, resolving at admission.
    pub fn add_actor<A: crate::Actor>(
        &self,
        id: impl Into<ChildId>,
        definition: ActorDef<A>,
    ) -> Admission<ActorRef<A::Msg>> {
        self.add_definition(id, definition)
    }

    /// Adds a consuming one-shot callback-oriented actor, resolving at admission.
    pub fn add_actor_once<A: crate::Actor>(
        &self,
        id: impl Into<ChildId>,
        definition: ActorOnceDef<A>,
    ) -> Admission<ActorRef<A::Msg>> {
        self.add_definition(id, definition)
    }

    /// Adds a restartable raw actor, resolving at admission.
    pub fn add_raw<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        self.add_definition(id, definition)
    }

    /// Adds a consuming one-shot raw actor, resolving at admission.
    pub fn add_raw_once<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawOnceDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        self.add_definition(id, definition)
    }

    /// Reserves a task id synchronously and exposes its exact handle.
    ///
    /// # Errors
    ///
    /// Fails with [`ReserveError::EmptyId`] for an empty id,
    /// [`ReserveError::NoRuntime`] outside an ambient Tokio runtime,
    /// [`ReserveError::NotAdmitting`] when the scope is terminal, draining,
    /// parked after a startup failure, or has no live incarnation,
    /// and [`ReserveError::RemovalInProgress`] or [`ReserveError::DuplicateId`]
    /// when a same-id member is being removed or is resident.
    pub fn reserve_task(&self, id: impl Into<ChildId>) -> Result<DynamicTaskSlot, ReserveError> {
        reserve_dynamic::<TaskKind>(self, id, None).map(|core| DynamicTaskSlot { core })
    }

    /// Adds a restartable task, resolving at admission rather than startup.
    pub fn add_task(&self, id: impl Into<ChildId>, definition: TaskDef) -> Admission<TaskRef> {
        self.add_definition(id, definition)
    }

    /// Adds a consuming one-shot task, resolving at admission.
    pub fn add_task_once<T: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
        definition: TaskOnceDef<T>,
    ) -> Admission<(TaskRef, OneShotTaskRef<T>)> {
        self.add_definition(id, definition)
    }

    /// Reserves a typed subtree id synchronously.
    ///
    /// # Errors
    ///
    /// Fails with [`ReserveError::EmptyId`] for an empty id,
    /// [`ReserveError::NoRuntime`] outside an ambient Tokio runtime,
    /// [`ReserveError::NotAdmitting`] when the scope is terminal, draining,
    /// parked after a startup failure, or has no live incarnation,
    /// and [`ReserveError::RemovalInProgress`] or [`ReserveError::DuplicateId`]
    /// when a same-id member is being removed or is resident.
    pub fn reserve_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicSubtreeSlot<T>, ReserveError> {
        reserve_dynamic::<SubtreeKind<T>>(self, id, None).map(|core| DynamicSubtreeSlot { core })
    }

    /// Adds a restartable subtree, resolving at admission.
    pub fn add_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeDef<T>,
    ) -> Admission<T::Ref> {
        self.add_definition(id, definition)
    }

    /// Adds a consuming one-shot subtree, resolving at admission.
    pub fn add_subtree_once<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeOnceDef<T>,
    ) -> Admission<T::Ref> {
        self.add_definition(id, definition)
    }

    /// Latches id-based removal synchronously; the returned future only
    /// observes completion.
    pub fn remove(&self, id: impl Into<ChildId>) -> Removal {
        let id = id.into();
        Removal::new(crate::driver::remove_dynamic(&self.0.cell, &id, None))
    }

    /// Removes exactly the membership `handle` names, never a same-id
    /// successor.
    ///
    /// Accepts any child handle — [`TaskRef`], [`ActorRef`], [`ScopeRef`],
    /// or [`DynamicScopeRef`] — through the sealed [`MemberHandle`] trait.
    /// Like [`remove`](Self::remove), the removal latches synchronously and
    /// the returned future only observes completion; a handle whose
    /// membership is no longer resident resolves
    /// [`RemoveOutcome::AlreadyAbsent`](crate::RemoveOutcome::AlreadyAbsent).
    pub fn remove_exact(&self, handle: &impl MemberHandle) -> Removal {
        let (id, membership) = handle.exact();
        Removal::new(crate::driver::remove_dynamic(
            &self.0.cell,
            id,
            Some(membership),
        ))
    }
}

/// A handle that names exactly one child membership, accepted by
/// [`DynamicScopeRef::remove_exact`].
///
/// Implemented by [`TaskRef`], [`ActorRef`], [`ScopeRef`], and
/// [`DynamicScopeRef`]. The trait is sealed: its implementations are the
/// framework's own handles, so a membership can only be named by a handle
/// the framework minted for it.
///
/// ```compile_fail,E0277
/// use shelterwood::MemberHandle;
///
/// struct Forged;
///
/// impl MemberHandle for Forged {}
/// ```
///
/// A `compile_fail` fence passes for any compilation error, so the trait
/// is named the same way here, as a bound instead of an impl — which
/// isolates the fence above to the sealed supertrait:
///
/// ```
/// use shelterwood::{DynamicScopeRef, MemberHandle, Removal};
///
/// fn remove_held(scope: &DynamicScopeRef, handle: &impl MemberHandle) -> Removal {
///     scope.remove_exact(handle)
/// }
/// ```
pub trait MemberHandle: sealed::Sealed {}

mod sealed {
    use crate::{ChildId, Membership};

    pub trait Sealed {
        /// The id and membership this handle names.
        fn exact(&self) -> (&ChildId, Membership);
    }
}

impl MemberHandle for TaskRef {}

impl sealed::Sealed for TaskRef {
    fn exact(&self) -> (&ChildId, Membership) {
        (self.id(), self.membership())
    }
}

impl<M> MemberHandle for ActorRef<M> {}

impl<M> sealed::Sealed for ActorRef<M> {
    fn exact(&self) -> (&ChildId, Membership) {
        (self.id(), self.membership())
    }
}

impl MemberHandle for ScopeRef {}

impl sealed::Sealed for ScopeRef {
    fn exact(&self) -> (&ChildId, Membership) {
        (self.id(), self.membership())
    }
}

impl MemberHandle for DynamicScopeRef {}

impl sealed::Sealed for DynamicScopeRef {
    fn exact(&self) -> (&ChildId, Membership) {
        sealed::Sealed::exact(self.as_scope())
    }
}
