use std::{fmt, marker::PhantomData, sync::Arc};

use crate::{
    ActorDef, ActorOnceDef, ActorRef, ChildId,
    admission::ReserveError,
    driver::DynamicReservation,
    mailbox::{MailboxCell, actor_ref_from_parts},
    plan::{BuilderCore, ChildConstruction, SlotCell},
    policy::ScopeFlavor,
    raw::{RawDef, RawOnceDef},
    runtime,
    scope::{DynamicScopeRef, ScopeRef},
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

/// Private declaration-kind dispatch shared by reservation and definition.
///
/// The public API stays nominal: callers never name this trait or its marker
/// types. It exists so adding a definition mode supplies only its semantic
/// lowering instead of copying the reserve/attach/admit choreography.
pub(super) trait SlotKind: Sized {
    const SCOPE_FLAVOR: Option<ScopeFlavor>;

    fn attach(slot: &SlotCell) -> Self;
}

pub(super) struct ActorKind<M> {
    mailbox: Arc<MailboxCell<M>>,
}

impl<M: Send + 'static> SlotKind for ActorKind<M> {
    const SCOPE_FLAVOR: Option<ScopeFlavor> = None;

    fn attach(slot: &SlotCell) -> Self {
        let mailbox = MailboxCell::new(slot.member.id().clone(), crate::runtime::mailbox_runtime());
        slot.member.attach_mailbox(mailbox.clone());
        Self { mailbox }
    }
}

pub(super) struct TaskKind;

impl SlotKind for TaskKind {
    const SCOPE_FLAVOR: Option<ScopeFlavor> = None;

    fn attach(_slot: &SlotCell) -> Self {
        Self
    }
}

pub(super) struct SubtreeKind<T: Subtree>(PhantomData<fn() -> T>);

impl<T: Subtree> SlotKind for SubtreeKind<T> {
    const SCOPE_FLAVOR: Option<ScopeFlavor> = Some(<T as sealed::Sealed>::FLAVOR);

    fn attach(_slot: &SlotCell) -> Self {
        Self(PhantomData)
    }
}

pub(super) struct SlotCore<E, K> {
    pub(super) endpoint: E,
    kind: K,
}

impl<E: SlotEndpoint, K: SlotKind> SlotCore<E, K> {
    fn new(endpoint: E) -> Self {
        let kind = K::attach(endpoint.slot());
        Self { endpoint, kind }
    }

    pub(super) fn define<D>(self, definition: D) -> E::Output<D::Handles>
    where
        D: Definition<Kind = K>,
    {
        definition.define(self)
    }
}

pub(super) fn reserve_static<K: SlotKind>(
    core: &mut BuilderCore,
    id: impl Into<ChildId>,
) -> Result<SlotCore<StaticSlotEndpoint, K>, ReserveError> {
    core.reserve(id, K::SCOPE_FLAVOR)
        .map(|slot| SlotCore::new(StaticSlotEndpoint(slot)))
}

pub(super) fn reserve_dynamic<K: SlotKind>(
    scope: &DynamicScopeRef,
    id: impl Into<ChildId>,
    ownership: AdmissionOwnership,
) -> Result<SlotCore<DynamicSlotEndpoint, K>, ReserveError> {
    crate::driver::reserve_dynamic(&scope.0.cell, id.into(), K::SCOPE_FLAVOR)
        .map(|reservation| SlotCore::new(DynamicSlotEndpoint::new(reservation, ownership)))
}

/// Sealed semantic lowering for each nominal public definition type.
///
/// `Kind` chooses the reservation shape; `Handles` pins the return type. The
/// eight public add methods remain concrete wrappers, so this trait cannot
/// enlarge the accepted public surface or weaken inference.
pub(super) trait Definition: Send + 'static + Sized {
    type Kind: SlotKind;
    type Handles;

    fn define<E: SlotEndpoint>(self, slot: SlotCore<E, Self::Kind>) -> E::Output<Self::Handles>;
}

impl<E: SlotEndpoint, M: Send + 'static> SlotCore<E, ActorKind<M>> {
    fn actor_ref(&self) -> ActorRef<M> {
        actor_ref_from_parts(
            Arc::clone(&self.endpoint.slot().member),
            Arc::clone(&self.kind.mailbox),
        )
    }
}

macro_rules! impl_raw_definition {
    ($definition:ident) => {
        impl<R: crate::RawActor> Definition for $definition<R> {
            type Kind = ActorKind<R::Msg>;
            type Handles = ActorRef<R::Msg>;

            fn define<E: SlotEndpoint>(
                self,
                slot: SlotCore<E, Self::Kind>,
            ) -> E::Output<Self::Handles> {
                let actor = slot.actor_ref();
                let SlotCore {
                    endpoint,
                    kind: ActorKind { mailbox },
                } = slot;
                endpoint.define(
                    actor,
                    ChildConstruction::Raw($definition::erase(
                        runtime::Isolated::new(self),
                        mailbox,
                    )),
                )
            }
        }
    };
}

impl_raw_definition!(RawDef);
impl_raw_definition!(RawOnceDef);

impl<A: crate::Actor> Definition for ActorDef<A> {
    type Kind = ActorKind<A::Msg>;
    type Handles = ActorRef<A::Msg>;

    fn define<E: SlotEndpoint>(self, slot: SlotCore<E, Self::Kind>) -> E::Output<Self::Handles> {
        slot.define(self.into_raw())
    }
}

impl<A: crate::Actor> Definition for ActorOnceDef<A> {
    type Kind = ActorKind<A::Msg>;
    type Handles = ActorRef<A::Msg>;

    fn define<E: SlotEndpoint>(self, slot: SlotCore<E, Self::Kind>) -> E::Output<Self::Handles> {
        slot.define(self.into_raw())
    }
}

impl<E: SlotEndpoint> SlotCore<E, TaskKind> {
    fn task_ref(&self) -> TaskRef {
        TaskRef::new(Arc::clone(&self.endpoint.slot().member))
    }
}

impl Definition for TaskDef {
    type Kind = TaskKind;
    type Handles = TaskRef;

    fn define<E: SlotEndpoint>(self, slot: SlotCore<E, Self::Kind>) -> E::Output<Self::Handles> {
        let task = slot.task_ref();
        slot.endpoint
            .define(task, ChildConstruction::Task(self.erase()))
    }
}

impl<T: Send + 'static> Definition for TaskOnceDef<T> {
    type Kind = TaskKind;
    type Handles = (TaskRef, OneShotTaskRef<T>);

    fn define<E: SlotEndpoint>(self, slot: SlotCore<E, Self::Kind>) -> E::Output<Self::Handles> {
        let task = slot.task_ref();
        let (completion, receiver) = runtime::oneshot();
        let claim = OneShotTaskRef::new(receiver, task.clone());
        slot.endpoint.define(
            (task, claim),
            ChildConstruction::Task(self.erase(completion)),
        )
    }
}

impl<E: SlotEndpoint, T: Subtree> SlotCore<E, SubtreeKind<T>> {
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
}

macro_rules! impl_subtree_definition {
    ($definition:ident) => {
        impl<T: Subtree> Definition for $definition<T> {
            type Kind = SubtreeKind<T>;
            type Handles = T::Ref;

            fn define<E: SlotEndpoint>(
                self,
                slot: SlotCore<E, Self::Kind>,
            ) -> E::Output<Self::Handles> {
                let scope = slot.scope_ref();
                slot.endpoint
                    .define(scope, ChildConstruction::Scope(Box::new(self.erase())))
            }
        }
    };
}

impl_subtree_definition!(SubtreeDef);
impl_subtree_definition!(SubtreeOnceDef);

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
    pub(super) core: SlotCore<StaticSlotEndpoint, ActorKind<M>>,
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
        self.core.define(definition)
    }

    /// Defines a consuming one-shot callback-oriented actor and consumes the slot.
    #[must_use]
    pub fn define_once<A>(self, definition: ActorOnceDef<A>) -> ActorRef<M>
    where
        A: crate::Actor<Msg = M>,
    {
        self.core.define(definition)
    }

    /// Defines a restartable raw actor and consumes the slot.
    #[must_use]
    pub fn define_raw<R>(self, definition: RawDef<R>) -> ActorRef<M>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.core.define(definition)
    }

    /// Defines a consuming one-shot raw actor and consumes the slot.
    #[must_use]
    pub fn define_once_raw<R>(self, definition: RawOnceDef<R>) -> ActorRef<M>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.core.define(definition)
    }
}

/// An owned pre-spawn task slot.
pub struct TaskSlot {
    pub(super) core: SlotCore<StaticSlotEndpoint, TaskKind>,
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
    #[must_use = "the returned task and completion handles must be retained"]
    pub fn define_once<T: Send + 'static>(
        self,
        definition: TaskOnceDef<T>,
    ) -> (TaskRef, OneShotTaskRef<T>) {
        self.core.define(definition)
    }
}
/// A split dynamic actor reservation with a stable mailbox binding.
pub struct DynamicActorSlot<M> {
    pub(super) core: SlotCore<DynamicSlotEndpoint, ActorKind<M>>,
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
        self.core.define(definition)
    }

    /// Defines a one-shot callback-oriented actor; dropping after first poll detaches.
    pub fn define_once<A>(self, definition: ActorOnceDef<A>) -> Admission<ActorRef<M>>
    where
        A: crate::Actor<Msg = M>,
    {
        self.core.define(definition)
    }

    /// Defines a restartable raw actor; dropping after first poll detaches.
    pub fn define_raw<R>(self, definition: RawDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.core.define(definition)
    }

    /// Defines a one-shot raw actor; dropping after first poll detaches.
    pub fn define_once_raw<R>(self, definition: RawOnceDef<R>) -> Admission<ActorRef<M>>
    where
        R: crate::RawActor<Msg = M>,
    {
        self.core.define(definition)
    }
}

/// A split dynamic task reservation.
pub struct DynamicTaskSlot {
    pub(super) core: SlotCore<DynamicSlotEndpoint, TaskKind>,
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
        self.core.define(definition)
    }
}

/// A split dynamic subtree reservation.
pub struct DynamicSubtreeSlot<T: Subtree> {
    pub(super) core: SlotCore<DynamicSlotEndpoint, SubtreeKind<T>>,
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
        self.core.define(definition)
    }
}
/// A typed pre-spawn subtree slot.
pub struct SubtreeSlot<T: Subtree> {
    pub(super) core: SlotCore<StaticSlotEndpoint, SubtreeKind<T>>,
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
        self.core.define(definition)
    }
}
