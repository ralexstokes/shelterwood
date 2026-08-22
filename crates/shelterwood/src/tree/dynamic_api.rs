use crate::{
    ActorDef, ActorOnceDef, ActorRef, ChildId,
    cells::ReserveError,
    raw::{RawDef, RawOnceDef},
    runtime,
    scope::{DynamicScopeRef, ScopeRef},
    task::{OneShotTaskRef, TaskDef, TaskOnceDef, TaskRef},
};

use super::{
    Admission, DynamicActorSlot, DynamicSubtreeSlot, DynamicTaskSlot, Removal, Subtree, SubtreeDef,
    SubtreeOnceDef,
    builders::dispose_rejected,
    slots::{ActorKind, AdmissionOwnership, Definition, SubtreeKind, TaskKind, reserve_dynamic},
};

impl DynamicScopeRef {
    fn add_definition<D: Definition>(
        &self,
        id: impl Into<ChildId>,
        definition: D,
    ) -> Admission<D::Handles> {
        let mut definition = runtime::Isolated::new(definition);
        match reserve_dynamic::<D::Kind>(self, id, AdmissionOwnership::Fused) {
            Ok(slot) => slot.define(definition.take().expect("isolated definition is available")),
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Reserves an actor id synchronously and exposes its exact handle.
    ///
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_actor<M: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicActorSlot<M>, ReserveError> {
        reserve_dynamic::<ActorKind<M>>(self, id, AdmissionOwnership::Split)
            .map(|core| DynamicActorSlot { core })
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
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_task(&self, id: impl Into<ChildId>) -> Result<DynamicTaskSlot, ReserveError> {
        reserve_dynamic::<TaskKind>(self, id, AdmissionOwnership::Split)
            .map(|core| DynamicTaskSlot { core })
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
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicSubtreeSlot<T>, ReserveError> {
        reserve_dynamic::<SubtreeKind<T>>(self, id, AdmissionOwnership::Split)
            .map(|core| DynamicSubtreeSlot { core })
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
