use std::sync::Arc;

use crate::{
    ActorDef, ActorOnceDef, ActorRef, ChildId,
    admission::ReserveError,
    mailbox::MailboxCell,
    raw::{RawDef, RawOnceDef},
    scope::{DynamicScopeRef, ScopeRef},
    task::{OneShotTaskRef, TaskDef, TaskOnceDef, TaskRef},
};

use super::{
    Admission, DynamicActorSlot, DynamicSubtreeSlot, DynamicTaskSlot, Removal, Subtree, SubtreeDef,
    SubtreeOnceDef,
    builders::dispose_rejected,
    slots::{
        ActorSlotCore, AdmissionOwnership, DynamicSlotEndpoint, SubtreeSlotCore, TaskSlotCore,
    },
    system::sealed,
};

impl ScopeRef {
    /// Requests shutdown and waits for this scope's incarnation to finish its
    /// scope epilogue.
    ///
    /// `timeout` bounds cooperative teardown, not this call's return: it arms
    /// when the targeted incarnation enters its drain and, once expired,
    /// escalates and reports stragglers while the wait continues to
    /// completion. Membership terminality published by an ancestor's teardown
    /// does not resolve the call while that incarnation is still running its
    /// epilogue.
    ///
    /// A scheduled scope driver joins its children before completing. If an
    /// ancestor hard-aborts a framework driver at the tidy-abort backstop, its
    /// synchronous drop epilogue can only *request* abort for active children,
    /// so cancellation and user-future destruction below that boundary may
    /// finish after this call returns.
    /// A zero budget still requests cooperative cancellation, then skips its
    /// wait and enters the ordinary escalation tail immediately.
    pub async fn shutdown_and_wait(
        &self,
        timeout: crate::DeadlineBudget,
    ) -> Result<(), crate::ShutdownTimeout> {
        crate::driver::shutdown_scope(Arc::clone(&self.cell), timeout).await
    }
}

impl DynamicScopeRef {
    fn define_or_reject<D, S, H>(
        definition: D,
        slot: Result<S, ReserveError>,
        define: impl FnOnce(S, D) -> Admission<H>,
    ) -> Admission<H>
    where
        D: Send + 'static,
    {
        match slot {
            Ok(slot) => define(slot, definition),
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    fn reserve_actor_with<M: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
        ownership: AdmissionOwnership,
    ) -> Result<DynamicActorSlot<M>, ReserveError> {
        crate::driver::reserve_dynamic(&self.0.cell, id.into(), None).map(|reservation| {
            let mailbox = MailboxCell::new(reservation.slot.member.id().clone());
            reservation.slot.member.attach_mailbox(mailbox.clone());
            DynamicActorSlot {
                core: ActorSlotCore::new(DynamicSlotEndpoint::new(reservation, ownership), mailbox),
            }
        })
    }

    fn reserve_task_with(
        &self,
        id: impl Into<ChildId>,
        ownership: AdmissionOwnership,
    ) -> Result<DynamicTaskSlot, ReserveError> {
        crate::driver::reserve_dynamic(&self.0.cell, id.into(), None).map(|reservation| {
            DynamicTaskSlot {
                core: TaskSlotCore::new(DynamicSlotEndpoint::new(reservation, ownership)),
            }
        })
    }

    fn reserve_subtree_with<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        ownership: AdmissionOwnership,
    ) -> Result<DynamicSubtreeSlot<T>, ReserveError> {
        crate::driver::reserve_dynamic(&self.0.cell, id.into(), Some(<T as sealed::Sealed>::FLAVOR))
            .map(|reservation| DynamicSubtreeSlot {
                core: SubtreeSlotCore::new(DynamicSlotEndpoint::new(reservation, ownership)),
            })
    }

    /// Reserves an actor id synchronously and exposes its exact handle.
    ///
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_actor<M: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicActorSlot<M>, ReserveError> {
        self.reserve_actor_with(id, AdmissionOwnership::Split)
    }

    /// Adds a restartable callback-oriented actor, resolving at admission.
    pub fn add_actor<A: crate::Actor>(
        &self,
        id: impl Into<ChildId>,
        definition: ActorDef<A>,
    ) -> Admission<ActorRef<A::Msg>> {
        Self::define_or_reject(
            definition,
            self.reserve_actor_with(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define_raw(definition.into_raw()),
        )
    }

    /// Adds a consuming one-shot callback-oriented actor, resolving at admission.
    pub fn add_actor_once<A: crate::Actor>(
        &self,
        id: impl Into<ChildId>,
        definition: ActorOnceDef<A>,
    ) -> Admission<ActorRef<A::Msg>> {
        Self::define_or_reject(
            definition,
            self.reserve_actor_with(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define_once_raw(definition.into_raw()),
        )
    }

    /// Adds a restartable raw actor, resolving at admission.
    pub fn add_raw<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        Self::define_or_reject(
            definition,
            self.reserve_actor_with(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define_raw(definition),
        )
    }

    /// Adds a consuming one-shot raw actor, resolving at admission.
    pub fn add_raw_once<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawOnceDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        Self::define_or_reject(
            definition,
            self.reserve_actor_with(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define_once_raw(definition),
        )
    }

    /// Reserves a task id synchronously and exposes its exact handle.
    ///
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_task(&self, id: impl Into<ChildId>) -> Result<DynamicTaskSlot, ReserveError> {
        self.reserve_task_with(id, AdmissionOwnership::Split)
    }

    /// Adds a restartable task, resolving at admission rather than startup.
    pub fn add_task(&self, id: impl Into<ChildId>, definition: TaskDef) -> Admission<TaskRef> {
        Self::define_or_reject(
            definition,
            self.reserve_task_with(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define(definition),
        )
    }

    /// Adds a consuming one-shot task, resolving at admission.
    pub fn add_task_once<T: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
        definition: TaskOnceDef<T>,
    ) -> Admission<(TaskRef, OneShotTaskRef<T>)> {
        Self::define_or_reject(
            definition,
            self.reserve_task_with(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define_once(definition),
        )
    }

    /// Reserves a typed subtree id synchronously.
    ///
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicSubtreeSlot<T>, ReserveError> {
        self.reserve_subtree_with(id, AdmissionOwnership::Split)
    }

    /// Adds a restartable subtree, resolving at admission.
    pub fn add_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeDef<T>,
    ) -> Admission<T::Ref> {
        Self::define_or_reject(
            definition,
            self.reserve_subtree_with::<T>(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define(definition),
        )
    }

    /// Adds a consuming one-shot subtree, resolving at admission.
    pub fn add_subtree_once<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeOnceDef<T>,
    ) -> Admission<T::Ref> {
        Self::define_or_reject(
            definition,
            self.reserve_subtree_with::<T>(id, AdmissionOwnership::Fused),
            |slot, definition| slot.core.define_once(definition),
        )
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
