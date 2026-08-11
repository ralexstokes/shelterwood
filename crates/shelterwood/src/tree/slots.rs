use std::{fmt, marker::PhantomData, sync::Arc};

use crate::{
    ActorDef, ActorOnceDef, ActorRef,
    driver::DynamicReservation,
    mailbox::{MailboxCell, actor_ref_from_parts},
    plan::{ChildConstruction, SlotCell},
    raw::{RawDef, RawOnceDef},
    runtime,
    scope::ScopeRef,
    task::{OneShotTaskRef, TaskDef, TaskOnceDef, TaskRef},
};

use super::{Admission, Subtree, SubtreeDef, SubtreeOnceDef, system::sealed};

#[derive(Clone, Copy)]
pub(super) enum AdmissionOwnership {
    Split,
    Fused,
}

pub(super) trait SlotEndpoint: Sized {
    type Output<H>;

    fn slot(&self) -> &SlotCell;

    fn define<H>(self, handles: H, construction: ChildConstruction) -> Self::Output<H>;
}

pub(super) struct StaticSlotEndpoint(pub(super) Arc<SlotCell>);

impl SlotEndpoint for StaticSlotEndpoint {
    type Output<H> = H;

    fn slot(&self) -> &SlotCell {
        &self.0
    }

    fn define<H>(self, handles: H, construction: ChildConstruction) -> H {
        self.0.define(construction);
        handles
    }
}

pub(super) struct DynamicSlotEndpoint {
    reservation: Option<DynamicReservation>,
    ownership: AdmissionOwnership,
}

impl DynamicSlotEndpoint {
    pub(super) fn new(reservation: DynamicReservation, ownership: AdmissionOwnership) -> Self {
        Self {
            reservation: Some(reservation),
            ownership,
        }
    }

    fn reservation(&self) -> &DynamicReservation {
        self.reservation
            .as_ref()
            .expect("dynamic slot reservation was already consumed")
    }
}

impl SlotEndpoint for DynamicSlotEndpoint {
    type Output<H> = Admission<H>;

    fn slot(&self) -> &SlotCell {
        &self.reservation().slot
    }

    fn define<H>(mut self, handles: H, construction: ChildConstruction) -> Admission<H> {
        let reservation = self
            .reservation
            .take()
            .expect("dynamic slot reservation was already consumed");
        reservation.slot.define(construction);
        Admission::new(reservation, handles, self.ownership)
    }
}

impl Drop for DynamicSlotEndpoint {
    fn drop(&mut self) {
        if let Some(reservation) = &self.reservation {
            crate::driver::cancel_dynamic_reservation(
                &reservation.scope,
                reservation.control.as_ref(),
                &reservation.slot,
            );
        }
    }
}

pub(super) struct ActorSlotCore<E, M> {
    pub(super) endpoint: E,
    mailbox: Arc<MailboxCell<M>>,
}

macro_rules! impl_actor_core_definition {
    ($method:ident, $definition:ident) => {
        pub(super) fn $method<R>(self, definition: $definition<R>) -> E::Output<ActorRef<M>>
        where
            R: crate::RawActor<Msg = M>,
        {
            let actor = self.actor_ref();
            let Self { endpoint, mailbox } = self;
            endpoint.define(actor, ChildConstruction::Raw(definition.erase(mailbox)))
        }
    };
}

impl<E: SlotEndpoint, M: Send + 'static> ActorSlotCore<E, M> {
    pub(super) fn new(endpoint: E, mailbox: Arc<MailboxCell<M>>) -> Self {
        Self { endpoint, mailbox }
    }

    fn actor_ref(&self) -> ActorRef<M> {
        actor_ref_from_parts(
            Arc::clone(&self.endpoint.slot().member),
            Arc::clone(&self.mailbox),
        )
    }

    impl_actor_core_definition!(define_raw, RawDef);
    impl_actor_core_definition!(define_once_raw, RawOnceDef);
}

pub(super) struct TaskSlotCore<E> {
    pub(super) endpoint: E,
}

impl<E: SlotEndpoint> TaskSlotCore<E> {
    pub(super) fn new(endpoint: E) -> Self {
        Self { endpoint }
    }

    fn task_ref(&self) -> TaskRef {
        TaskRef::new(Arc::clone(&self.endpoint.slot().member))
    }

    pub(super) fn define(self, definition: TaskDef) -> E::Output<TaskRef> {
        let task = self.task_ref();
        self.endpoint
            .define(task, ChildConstruction::Task(definition))
    }

    pub(super) fn define_once<T: Send + 'static>(
        self,
        definition: TaskOnceDef<T>,
    ) -> E::Output<(TaskRef, OneShotTaskRef<T>)> {
        let task = self.task_ref();
        let (completion, receiver) = runtime::oneshot();
        let claim = OneShotTaskRef::new(receiver, task.clone());
        self.endpoint.define(
            (task, claim),
            ChildConstruction::TaskOnce(definition.erase(completion)),
        )
    }
}

pub(super) struct SubtreeSlotCore<E, T: Subtree> {
    pub(super) endpoint: E,
    marker: PhantomData<fn() -> T>,
}

macro_rules! impl_subtree_core_definition {
    ($method:ident, $definition:ident) => {
        pub(super) fn $method(self, definition: $definition<T>) -> E::Output<T::Ref> {
            let scope = self.scope_ref();
            self.endpoint.define(
                scope,
                ChildConstruction::Scope(Box::new(definition.erase())),
            )
        }
    };
}

impl<E: SlotEndpoint, T: Subtree> SubtreeSlotCore<E, T> {
    pub(super) fn new(endpoint: E) -> Self {
        Self {
            endpoint,
            marker: PhantomData,
        }
    }

    fn scope_ref(&self) -> T::Ref {
        <T as sealed::Sealed>::make_ref(ScopeRef {
            cell: Arc::clone(
                self.endpoint
                    .slot()
                    .scope
                    .as_ref()
                    .expect("subtree slot must carry a scope cell"),
            ),
        })
    }

    impl_subtree_core_definition!(define, SubtreeDef);
    impl_subtree_core_definition!(define_once, SubtreeOnceDef);
}

macro_rules! impl_slot_debug {
    ($slot:ident $(<$generic:ident $(: $bound:path)?>)?, $label:literal) => {
        impl$(<$generic $(: $bound)?>)? fmt::Debug for $slot$(<$generic>)? {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct($label)
                    .field("id", self.core.endpoint.slot().member.id())
                    .finish_non_exhaustive()
            }
        }
    };
}
/// An owned pre-spawn actor slot with a stable mailbox binding.
pub struct ActorSlot<M> {
    pub(super) core: ActorSlotCore<StaticSlotEndpoint, M>,
}

impl_slot_debug!(ActorSlot<M>, "ActorSlot");

impl<M: Send + 'static> ActorSlot<M> {
    /// Returns the membership-addressed handle before definition or spawn.
    #[must_use]
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.core.actor_ref()
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
        self.core.define_raw(definition)
    }

    /// Defines a consuming one-shot raw actor and consumes the slot.
    #[must_use]
    pub fn define_once_raw<R>(self, definition: RawOnceDef<R>) -> ActorRef<M>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.core.define_once_raw(definition)
    }
}

/// An owned pre-spawn task slot.
pub struct TaskSlot {
    pub(super) core: TaskSlotCore<StaticSlotEndpoint>,
}

impl_slot_debug!(TaskSlot, "TaskSlot");

impl TaskSlot {
    /// Returns the membership handle before definition or spawn.
    #[must_use]
    pub fn task_ref(&self) -> TaskRef {
        self.core.task_ref()
    }

    /// Defines a restartable task and consumes the slot.
    #[must_use]
    pub fn define(self, definition: TaskDef) -> TaskRef {
        self.core.define(definition)
    }

    /// Defines a one-shot task and consumes the slot.
    pub fn define_once<T: Send + 'static>(
        self,
        definition: TaskOnceDef<T>,
    ) -> (TaskRef, OneShotTaskRef<T>) {
        self.core.define_once(definition)
    }
}
/// A split dynamic actor reservation with a stable mailbox binding.
pub struct DynamicActorSlot<M> {
    pub(super) core: ActorSlotCore<DynamicSlotEndpoint, M>,
}

impl_slot_debug!(DynamicActorSlot<M>, "DynamicActorSlot");

impl<M: Send + 'static> DynamicActorSlot<M> {
    /// Returns the exact actor handle before admission.
    #[must_use]
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.core.actor_ref()
    }

    /// Defines a restartable callback-oriented actor; dropping after first poll detaches.
    pub fn define<A>(self, definition: ActorDef<A>) -> Admission<ActorRef<M>>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_raw(definition.into_raw())
    }

    /// Defines a one-shot callback-oriented actor; dropping after first poll detaches.
    pub fn define_once<A>(self, definition: ActorOnceDef<A>) -> Admission<ActorRef<M>>
    where
        A: crate::Actor<Msg = M>,
    {
        self.define_once_raw(definition.into_raw())
    }

    /// Defines a restartable raw actor; dropping after first poll detaches.
    pub fn define_raw<R>(self, definition: RawDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.core.define_raw(definition)
    }

    /// Defines a one-shot raw actor; dropping after first poll detaches.
    pub fn define_once_raw<R>(self, definition: RawOnceDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.core.define_once_raw(definition)
    }
}

/// A split dynamic task reservation.
pub struct DynamicTaskSlot {
    pub(super) core: TaskSlotCore<DynamicSlotEndpoint>,
}

impl_slot_debug!(DynamicTaskSlot, "DynamicTaskSlot");

impl DynamicTaskSlot {
    /// Returns the exact task handle before admission.
    #[must_use]
    pub fn task_ref(&self) -> TaskRef {
        self.core.task_ref()
    }

    /// Defines a restartable task; dropping after first poll detaches admission.
    pub fn define(self, definition: TaskDef) -> Admission<TaskRef> {
        self.core.define(definition)
    }

    /// Defines a one-shot task; dropping after first poll detaches admission.
    pub fn define_once<T: Send + 'static>(
        self,
        definition: TaskOnceDef<T>,
    ) -> Admission<(TaskRef, OneShotTaskRef<T>)> {
        self.core.define_once(definition)
    }
}

/// A split dynamic subtree reservation.
pub struct DynamicSubtreeSlot<T: Subtree> {
    pub(super) core: SubtreeSlotCore<DynamicSlotEndpoint, T>,
}

impl_slot_debug!(DynamicSubtreeSlot<T: Subtree>, "DynamicSubtreeSlot");

impl<T: Subtree> DynamicSubtreeSlot<T> {
    /// Returns the typed exact scope handle before admission.
    #[must_use]
    pub fn scope_ref(&self) -> T::Ref {
        self.core.scope_ref()
    }

    /// Defines a restartable subtree; dropping after first poll detaches.
    pub fn define(self, definition: SubtreeDef<T>) -> Admission<T::Ref> {
        self.core.define(definition)
    }

    /// Defines a one-shot subtree; dropping after first poll detaches.
    pub fn define_once(self, definition: SubtreeOnceDef<T>) -> Admission<T::Ref> {
        self.core.define_once(definition)
    }
}
/// A typed pre-spawn subtree slot.
pub struct SubtreeSlot<T: Subtree> {
    pub(super) core: SubtreeSlotCore<StaticSlotEndpoint, T>,
}

impl_slot_debug!(SubtreeSlot<T: Subtree>, "SubtreeSlot");

impl<T: Subtree> SubtreeSlot<T> {
    /// Returns the typed scope handle before definition or spawn.
    #[must_use]
    pub fn scope_ref(&self) -> T::Ref {
        self.core.scope_ref()
    }

    /// Defines a restartable subtree and consumes the slot.
    #[must_use]
    pub fn define(self, definition: SubtreeDef<T>) -> T::Ref {
        self.core.define(definition)
    }

    /// Defines a one-shot subtree and consumes the slot.
    #[must_use]
    pub fn define_once(self, definition: SubtreeOnceDef<T>) -> T::Ref {
        self.core.define_once(definition)
    }
}
