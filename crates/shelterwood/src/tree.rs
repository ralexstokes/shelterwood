//! Tree declarations, dynamic construction handles, and the owning system façade.

use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    ActorDef, ActorOnceDef, ActorRef, ChildId, DefaultsInheritance, Intensity, ReadinessDeadline,
    RestartPolicy, Retention, ScopeDefaults, Shutdown, ShutdownTimeout, Strategy,
    admission::{RemoveOutcome, ReserveError},
    definition::DefinitionSource,
    driver::DynamicReservation,
    exit::{StartupError, StopReason},
    mailbox::MailboxCell,
    plan::{BuilderCore, ChildConstruction, LowerError, ScopeConstruction, SlotCell},
    policy::{CommonOptions, InvalidPolicy, ResolvedDefaults, ScopeFlavor},
    raw::{RawDef, RawOnceDef},
    runtime::{self, Latch},
    scope::{DynamicScopeRef, ScopeRef},
    task::{OneShotTaskRef, TaskDef, TaskOnceDef, TaskRef},
};

/// Startup failure paired with any rollback timeout report.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("tree startup failed and was rolled back")]
pub struct StartOrShutdownError {
    /// Original startup error.
    pub startup: StartupError,
    /// Stragglers forced down after the rollback bound, if any.
    pub rollback_timeout: Option<ShutdownTimeout>,
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
    /// A public policy representation contained an invalid literal value.
    #[error(transparent)]
    InvalidPolicy(InvalidPolicy),
}

#[derive(Clone, Copy)]
enum AdmissionOwnership {
    Split,
    Fused,
}

trait SlotEndpoint: Sized {
    type Output<H>;

    fn slot(&self) -> &SlotCell;

    fn define<H>(self, handles: H, construction: ChildConstruction) -> Self::Output<H>;
}

struct StaticSlotEndpoint(Arc<SlotCell>);

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

struct DynamicSlotEndpoint {
    reservation: Option<DynamicReservation>,
    ownership: AdmissionOwnership,
}

impl DynamicSlotEndpoint {
    fn split(reservation: DynamicReservation) -> Self {
        Self {
            reservation: Some(reservation),
            ownership: AdmissionOwnership::Split,
        }
    }

    fn fuse(&mut self) {
        self.ownership = AdmissionOwnership::Fused;
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
                reservation.control.as_ref(),
                &reservation.slot,
            );
        }
    }
}

struct ActorSlotCore<E, M> {
    endpoint: E,
    mailbox: Arc<MailboxCell<M>>,
}

macro_rules! impl_actor_core_definition {
    ($method:ident, $definition:ident) => {
        fn $method<R>(self, definition: $definition<R>) -> E::Output<ActorRef<M>>
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
    fn new(endpoint: E, mailbox: Arc<MailboxCell<M>>) -> Self {
        Self { endpoint, mailbox }
    }

    fn actor_ref(&self) -> ActorRef<M> {
        ActorRef::new(
            Arc::clone(&self.endpoint.slot().member),
            Arc::clone(&self.mailbox),
        )
    }

    impl_actor_core_definition!(define_raw, RawDef);
    impl_actor_core_definition!(define_once_raw, RawOnceDef);
}

struct TaskSlotCore<E> {
    endpoint: E,
}

impl<E: SlotEndpoint> TaskSlotCore<E> {
    fn new(endpoint: E) -> Self {
        Self { endpoint }
    }

    fn task_ref(&self) -> TaskRef {
        TaskRef::new(Arc::clone(&self.endpoint.slot().member))
    }

    fn define(self, definition: TaskDef) -> E::Output<TaskRef> {
        let task = self.task_ref();
        self.endpoint
            .define(task, ChildConstruction::Task(definition))
    }

    fn define_once<T: Send + 'static>(
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

struct SubtreeSlotCore<E, T: Subtree> {
    endpoint: E,
    marker: PhantomData<fn() -> T>,
}

macro_rules! impl_subtree_core_definition {
    ($method:ident, $definition:ident) => {
        fn $method(self, definition: $definition<T>) -> E::Output<T::Ref> {
            let scope = self.scope_ref();
            self.endpoint.define(
                scope,
                ChildConstruction::Scope(Box::new(definition.erase())),
            )
        }
    };
}

impl<E: SlotEndpoint, T: Subtree> SubtreeSlotCore<E, T> {
    fn new(endpoint: E) -> Self {
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

/// Routes a definition rejected before admission through isolated disposal.
///
/// A failed reservation returns before framework state owns the supplied
/// definition, so dropping it inline would run a possibly blocking or
/// panicking user destructor on the caller instead of producing an ordinary
/// [`ReserveError`].
fn dispose_rejected<D: Send + 'static>(definition: D, error: ReserveError) -> ReserveError {
    runtime::dispose_detached(definition);
    error
}

fn attach_actor_slot<M: Send + 'static>(slot: Arc<SlotCell>) -> ActorSlot<M> {
    let mailbox = MailboxCell::new(slot.member.id().clone());
    slot.member.attach_mailbox(mailbox.clone());
    ActorSlot {
        core: ActorSlotCore::new(StaticSlotEndpoint(slot), mailbox),
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
            self.core.config.intensity = intensity;
            self
        }

        /// Sets child policy defaults for this scope.
        pub fn defaults(&mut self, defaults: ScopeDefaults) -> &mut Self {
            self.core.config.defaults = defaults;
            self
        }

        /// Reserves an actor membership and returns its pre-spawn handle slot.
        #[doc = $member_note]
        pub fn reserve_actor<M: Send + 'static>(
            &mut self,
            id: impl Into<ChildId>,
        ) -> Result<ActorSlot<M>, ReserveError> {
            self.core.reserve(id, None).map(attach_actor_slot)
        }

        /// Adds a restartable callback-oriented actor.
        #[doc = $member_note]
        pub fn add_actor<A: crate::Actor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: ActorDef<A>,
        ) -> Result<ActorRef<A::Msg>, ReserveError> {
            match self.reserve_actor(id) {
                Ok(slot) => Ok(slot.define(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }

        /// Adds a consuming one-shot callback-oriented actor.
        #[doc = $member_note]
        pub fn add_actor_once<A: crate::Actor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: ActorOnceDef<A>,
        ) -> Result<ActorRef<A::Msg>, ReserveError> {
            match self.reserve_actor(id) {
                Ok(slot) => Ok(slot.define_once(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }

        /// Adds a restartable raw actor.
        #[doc = $member_note]
        pub fn add_raw<R: crate::RawActor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: RawDef<R>,
        ) -> Result<ActorRef<R::Msg>, ReserveError> {
            match self.reserve_actor(id) {
                Ok(slot) => Ok(slot.define_raw(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }

        /// Adds a consuming one-shot raw actor.
        #[doc = $member_note]
        pub fn add_raw_once<R: crate::RawActor>(
            &mut self,
            id: impl Into<ChildId>,
            definition: RawOnceDef<R>,
        ) -> Result<ActorRef<R::Msg>, ReserveError> {
            match self.reserve_actor(id) {
                Ok(slot) => Ok(slot.define_once_raw(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }

        /// Reserves a task membership and returns its pre-spawn handle slot.
        #[doc = $member_note]
        pub fn reserve_task(&mut self, id: impl Into<ChildId>) -> Result<TaskSlot, ReserveError> {
            self.core.reserve(id, None).map(|slot| TaskSlot {
                core: TaskSlotCore::new(StaticSlotEndpoint(slot)),
            })
        }

        /// Adds a restartable task.
        #[doc = $member_note]
        pub fn add_task(
            &mut self,
            id: impl Into<ChildId>,
            definition: TaskDef,
        ) -> Result<TaskRef, ReserveError> {
            match self.reserve_task(id) {
                Ok(slot) => Ok(slot.define(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }

        /// Adds a consuming one-shot task and its typed completion claim.
        #[doc = $member_note]
        pub fn add_task_once<T: Send + 'static>(
            &mut self,
            id: impl Into<ChildId>,
            definition: TaskOnceDef<T>,
        ) -> Result<(TaskRef, OneShotTaskRef<T>), ReserveError> {
            match self.reserve_task(id) {
                Ok(slot) => Ok(slot.define_once(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }

        /// Reserves a typed subtree membership.
        #[doc = $member_note]
        pub fn reserve_subtree<T: Subtree>(
            &mut self,
            id: impl Into<ChildId>,
        ) -> Result<SubtreeSlot<T>, ReserveError> {
            self.core
                .reserve(id, Some(<T as sealed::Sealed>::FLAVOR))
                .map(|slot| SubtreeSlot {
                    core: SubtreeSlotCore::new(StaticSlotEndpoint(slot)),
                })
        }

        /// Adds a restartable subtree.
        #[doc = $member_note]
        pub fn add_subtree<T: Subtree>(
            &mut self,
            id: impl Into<ChildId>,
            definition: SubtreeDef<T>,
        ) -> Result<T::Ref, ReserveError> {
            match self.reserve_subtree::<T>(id) {
                Ok(slot) => Ok(slot.define(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }

        /// Adds a consuming one-shot subtree.
        #[doc = $member_note]
        pub fn add_subtree_once<T: Subtree>(
            &mut self,
            id: impl Into<ChildId>,
            definition: SubtreeOnceDef<T>,
        ) -> Result<T::Ref, ReserveError> {
            match self.reserve_subtree::<T>(id) {
                Ok(slot) => Ok(slot.define_once(definition)),
                Err(error) => Err(dispose_rejected(definition, error)),
            }
        }
    };
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

    /// Lowers and starts this tree synchronously.
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
            LowerError::InvalidPolicy { invalid, .. } => BuildError::InvalidPolicy(invalid),
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

/// An owned pre-spawn actor slot with a stable mailbox binding.
pub struct ActorSlot<M> {
    core: ActorSlotCore<StaticSlotEndpoint, M>,
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
    core: TaskSlotCore<StaticSlotEndpoint>,
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

/// An admission future.
///
/// Fused additions abort on drop; split definitions detach after their first
/// poll starts admission. Reservation and that first poll require an ambient
/// Tokio runtime. A first poll outside one returns [`ReserveError::NoRuntime`]
/// and releases the reservation. If the driver's internal completion route is
/// lost, release builds fail closed with [`ReserveError::NotAdmitting`] and a
/// [`crate::NotAdmittingCause::Terminal`] cause. Debug builds additionally
/// assert that the completion obligation regressed.
/// Like a fused future, it remains pending if polled again after completion.
#[must_use]
pub struct Admission<H> {
    state: AdmissionState<H>,
}

type AdmissionWait = Pin<Box<dyn Future<Output = Result<(), ReserveError>> + Send + 'static>>;

struct PendingAdmission<H> {
    reservation: DynamicReservation,
    handles: H,
    fused_cancel: Option<Latch>,
}

impl<H> PendingAdmission<H> {
    fn start(&self) -> Result<AdmissionWait, ReserveError> {
        let response = crate::driver::start_admission(
            Arc::clone(&self.reservation.control),
            Arc::clone(&self.reservation.slot),
            self.fused_cancel.clone(),
        )?;
        Ok(Box::pin(async move {
            response.receive().await.unwrap_or_else(|| {
                // The driver's admission `Obligation` publishes an outcome on
                // every path, including its drop fallback. Treat a missing
                // response as the scope having gone terminal so a caller is
                // never stranded, but fail loudly in debug builds: silence
                // here would mask an obligation regression.
                debug_assert!(false, "admission response obligation must complete");
                Err(ReserveError::NotAdmitting(
                    crate::NotAdmittingCause::Terminal,
                ))
            })
        }))
    }

    fn cancel_reservation(&self) {
        crate::driver::cancel_dynamic_reservation(
            self.reservation.control.as_ref(),
            &self.reservation.slot,
        );
    }
}

enum AdmissionState<H> {
    Immediate(ReserveError),
    Unpolled(PendingAdmission<H>),
    InFlight {
        pending: PendingAdmission<H>,
        wait: AdmissionWait,
    },
    Done,
}

impl<H> AdmissionState<H> {
    fn name(&self) -> &'static str {
        match self {
            Self::Immediate(_) => "Immediate",
            Self::Unpolled(_) => "Unpolled",
            Self::InFlight { .. } => "InFlight",
            Self::Done => "Done",
        }
    }
}

impl<H> fmt::Debug for Admission<H> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Admission")
            .field("state", &self.state.name())
            .finish_non_exhaustive()
    }
}

impl<H> Admission<H> {
    fn error(error: ReserveError) -> Self {
        Self {
            state: AdmissionState::Immediate(error),
        }
    }

    fn new(reservation: DynamicReservation, handles: H, ownership: AdmissionOwnership) -> Self {
        Self {
            state: AdmissionState::Unpolled(PendingAdmission {
                reservation,
                handles,
                fused_cancel: match ownership {
                    AdmissionOwnership::Split => None,
                    AdmissionOwnership::Fused => Some(Latch::default()),
                },
            }),
        }
    }
}

impl<H: Unpin> Future for Admission<H> {
    type Output = Result<H, ReserveError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        loop {
            match &mut this.state {
                AdmissionState::Immediate(error) => {
                    let error = error.clone();
                    this.state = AdmissionState::Done;
                    return Poll::Ready(Err(error));
                }
                AdmissionState::Unpolled(pending) => {
                    let wait = match pending.start() {
                        Ok(wait) => wait,
                        Err(error) => {
                            pending.cancel_reservation();
                            this.state = AdmissionState::Done;
                            return Poll::Ready(Err(error));
                        }
                    };
                    let previous = std::mem::replace(&mut this.state, AdmissionState::Done);
                    let AdmissionState::Unpolled(pending) = previous else {
                        unreachable!("the matched admission state was replaced in place")
                    };
                    this.state = AdmissionState::InFlight { pending, wait };
                }
                AdmissionState::InFlight { wait, .. } => match wait.as_mut().poll(context) {
                    Poll::Ready(result) => {
                        let previous = std::mem::replace(&mut this.state, AdmissionState::Done);
                        let AdmissionState::InFlight { pending, .. } = previous else {
                            unreachable!("the matched admission state was replaced in place")
                        };
                        return Poll::Ready(result.map(|()| pending.handles));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                AdmissionState::Done => return Poll::Pending,
            }
        }
    }
}

impl<H> Drop for Admission<H> {
    fn drop(&mut self) {
        match &self.state {
            AdmissionState::Unpolled(pending) => {
                // A fused admission annuls its reservation on every drop edge,
                // polled or not. Firing the latch before cancelling keeps the
                // scope's control-plane wake and the cancellation evidence in
                // the same order the in-flight path uses.
                let signal_panic = pending.fused_cancel.as_ref().and_then(|cancel| {
                    crate::runtime::catch_panic(|| {
                        crate::driver::signal_fused_cancel(
                            pending.reservation.control.as_ref(),
                            &pending.reservation.slot,
                            cancel,
                        );
                    })
                    .err()
                });
                let cleanup_panic =
                    crate::runtime::catch_panic(|| pending.cancel_reservation()).err();
                crate::runtime::resume_preferred_panic(crate::runtime::UnwindPanics {
                    primary: signal_panic,
                    cleanup: cleanup_panic,
                });
            }
            AdmissionState::InFlight { pending, .. } => {
                if let Some(cancel) = &pending.fused_cancel {
                    let signal_panic = crate::runtime::catch_panic(|| {
                        crate::driver::signal_fused_cancel(
                            pending.reservation.control.as_ref(),
                            &pending.reservation.slot,
                            cancel,
                        );
                    })
                    .err();
                    let cleanup_panic =
                        crate::runtime::catch_panic(|| pending.cancel_reservation()).err();
                    crate::runtime::resume_preferred_panic(crate::runtime::UnwindPanics {
                        primary: signal_panic,
                        cleanup: cleanup_panic,
                    });
                }
            }
            AdmissionState::Immediate(_) | AdmissionState::Done => {}
        }
    }
}

/// A split dynamic actor reservation with a stable mailbox binding.
pub struct DynamicActorSlot<M> {
    core: ActorSlotCore<DynamicSlotEndpoint, M>,
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
    core: TaskSlotCore<DynamicSlotEndpoint>,
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
    core: SubtreeSlotCore<DynamicSlotEndpoint, T>,
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

/// Observation future for a synchronously latched dynamic removal.
///
/// If the driver's internal completion route is lost after the request is
/// latched, release builds fail closed with [`RemoveOutcome::Removed`]; debug
/// builds additionally assert that the completion obligation regressed.
#[must_use]
pub struct Removal {
    inner: Pin<Box<dyn Future<Output = RemoveOutcome> + Send + 'static>>,
}

impl fmt::Debug for Removal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Removal").finish_non_exhaustive()
    }
}

fn lost_removal_response_outcome() -> RemoveOutcome {
    RemoveOutcome::Removed
}

impl Removal {
    fn new(response: crate::driver::RemovalResponse) -> Self {
        Self {
            inner: Box::pin(async move {
                response.receive().await.unwrap_or_else(|| {
                    // The driver's removal `Obligation` publishes `Removed`
                    // on every destruction path. A missing response therefore
                    // means the terminal route vanished after removal latched:
                    // preserve the removal goal, but flag the invariant break
                    // in debug builds just as admission does above.
                    debug_assert!(false, "removal response obligation must complete");
                    lost_removal_response_outcome()
                })
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

    fn erase(self) -> ScopeConstruction {
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

    fn erase(self) -> ScopeConstruction {
        ScopeConstruction {
            source: DefinitionSource::OneShot(Box::new(<T as sealed::Sealed>::into_core(
                self.tree,
            ))),
            options: self.options,
            defaults: self.defaults,
        }
    }
}

/// A typed pre-spawn subtree slot.
pub struct SubtreeSlot<T: Subtree> {
    core: SubtreeSlotCore<StaticSlotEndpoint, T>,
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

impl ScopeRef {
    /// Requests shutdown and waits for this scope to stop.
    pub async fn shutdown_and_wait(&self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        crate::driver::shutdown_scope(Arc::clone(&self.cell), timeout).await
    }
}

impl DynamicScopeRef {
    /// Requests shutdown and waits for this scope to stop.
    pub async fn shutdown_and_wait(&self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        self.0.shutdown_and_wait(timeout).await
    }

    /// Reserves an actor id synchronously and exposes its exact handle.
    ///
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_actor<M: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicActorSlot<M>, ReserveError> {
        crate::driver::reserve_dynamic(&self.0.cell, id.into(), None).map(|reservation| {
            let mailbox = MailboxCell::new(reservation.slot.member.id().clone());
            reservation.slot.member.attach_mailbox(mailbox.clone());
            DynamicActorSlot {
                core: ActorSlotCore::new(DynamicSlotEndpoint::split(reservation), mailbox),
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
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define_raw(definition.into_raw())
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Adds a consuming one-shot callback-oriented actor, resolving at admission.
    pub fn add_actor_once<A: crate::Actor>(
        &self,
        id: impl Into<ChildId>,
        definition: ActorOnceDef<A>,
    ) -> Admission<ActorRef<A::Msg>> {
        match self.reserve_actor(id) {
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define_once_raw(definition.into_raw())
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Adds a restartable raw actor, resolving at admission.
    pub fn add_raw<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        match self.reserve_actor(id) {
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define_raw(definition)
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Adds a consuming one-shot raw actor, resolving at admission.
    pub fn add_raw_once<R: crate::RawActor>(
        &self,
        id: impl Into<ChildId>,
        definition: RawOnceDef<R>,
    ) -> Admission<ActorRef<R::Msg>> {
        match self.reserve_actor(id) {
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define_once_raw(definition)
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Reserves a task id synchronously and exposes its exact handle.
    ///
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_task(&self, id: impl Into<ChildId>) -> Result<DynamicTaskSlot, ReserveError> {
        crate::driver::reserve_dynamic(&self.0.cell, id.into(), None).map(|reservation| {
            DynamicTaskSlot {
                core: TaskSlotCore::new(DynamicSlotEndpoint::split(reservation)),
            }
        })
    }

    /// Adds a restartable task, resolving at admission rather than startup.
    pub fn add_task(&self, id: impl Into<ChildId>, definition: TaskDef) -> Admission<TaskRef> {
        match self.reserve_task(id) {
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define(definition)
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Adds a consuming one-shot task, resolving at admission.
    pub fn add_task_once<T: Send + 'static>(
        &self,
        id: impl Into<ChildId>,
        definition: TaskOnceDef<T>,
    ) -> Admission<(TaskRef, OneShotTaskRef<T>)> {
        match self.reserve_task(id) {
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define_once(definition)
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Reserves a typed subtree id synchronously.
    ///
    /// Returns [`ReserveError::NoRuntime`] outside an ambient Tokio runtime.
    pub fn reserve_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
    ) -> Result<DynamicSubtreeSlot<T>, ReserveError> {
        crate::driver::reserve_dynamic(&self.0.cell, id.into(), Some(<T as sealed::Sealed>::FLAVOR))
            .map(|reservation| DynamicSubtreeSlot {
                core: SubtreeSlotCore::new(DynamicSlotEndpoint::split(reservation)),
            })
    }

    /// Adds a restartable subtree, resolving at admission.
    pub fn add_subtree<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeDef<T>,
    ) -> Admission<T::Ref> {
        match self.reserve_subtree::<T>(id) {
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define(definition)
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
        }
    }

    /// Adds a consuming one-shot subtree, resolving at admission.
    pub fn add_subtree_once<T: Subtree>(
        &self,
        id: impl Into<ChildId>,
        definition: SubtreeOnceDef<T>,
    ) -> Admission<T::Ref> {
        match self.reserve_subtree::<T>(id) {
            Ok(mut slot) => {
                slot.core.endpoint.fuse();
                slot.core.define_once(definition)
            }
            Err(error) => Admission::error(dispose_rejected(definition, error)),
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
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Wake, Waker},
        time::Duration,
    };

    use super::{
        Admission, AdmissionOwnership, DynamicTree, Removal, TaskRef, Tree, sealed::Sealed,
    };
    use crate::{ExitKind, TaskDef, identity::ScopeIdentity};

    struct DropAdmissionAndPanic {
        admission: Mutex<Option<Admission<TaskRef>>>,
    }

    impl Wake for DropAdmissionAndPanic {
        fn wake(self: Arc<Self>) {
            drop(
                self.admission
                    .lock()
                    .expect("admission mutex poisoned")
                    .take(),
            );
            panic!("hostile observation waker");
        }

        fn wake_by_ref(self: &Arc<Self>) {
            drop(
                self.admission
                    .lock()
                    .expect("admission mutex poisoned")
                    .take(),
            );
            panic!("hostile observation waker");
        }
    }

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

    #[test]
    fn lost_removal_response_policy_fails_closed() {
        assert_eq!(
            super::lost_removal_response_outcome(),
            crate::RemoveOutcome::Removed
        );
    }

    #[test]
    fn closed_removal_response_fails_closed_after_debug_diagnostic() {
        let (sender, response) = crate::runtime::oneshot();
        drop(sender);
        let mut removal = Removal::new(response);
        let mut context = Context::from_waker(Waker::noop());
        let observed = catch_unwind(AssertUnwindSafe(|| {
            Pin::new(&mut removal).poll(&mut context)
        }));

        #[cfg(debug_assertions)]
        assert!(
            observed.is_err(),
            "debug builds expose the broken removal response obligation"
        );
        #[cfg(not(debug_assertions))]
        assert_eq!(
            observed.expect("release fallback does not panic"),
            std::task::Poll::Ready(crate::RemoveOutcome::Removed)
        );
    }

    #[crate::runtime::test]
    async fn saturated_fused_drop_before_exit_suppresses_restart_accounting() {
        crate::driver::exercise_saturated_fused_drop_before_exit(|reservation| {
            Admission::new(reservation, (), AdmissionOwnership::Fused)
        })
        .await;
    }

    #[crate::runtime::test]
    async fn unpolled_fused_drop_releases_reservations_despite_a_reentrant_panicking_waker() {
        let system = DynamicTree::new().spawn().expect("runtime is available");
        system.wait_started().await.expect("dynamic root starts");
        let scope = system.scope();

        let first_slot = scope.reserve_task("first").expect("first id is free");
        let first = first_slot.task_ref();
        let first_admission = first_slot.define(TaskDef::new(|_| std::future::pending()));
        let second_slot = scope.reserve_task("second").expect("second id is free");
        let second = second_slot.task_ref();
        let second_admission = second_slot.define(TaskDef::new(|_| std::future::pending()));

        let mut first_wait = Box::pin(first.wait());
        let waker = Waker::from(Arc::new(DropAdmissionAndPanic {
            admission: Mutex::new(Some(second_admission)),
        }));
        assert!(
            first_wait
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );

        catch_unwind(AssertUnwindSafe(|| drop(first_admission)))
            .expect_err("the hostile membership waker still surfaces");
        assert!(matches!(
            first_wait
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(exit) if matches!(exit.kind(), ExitKind::NeverStarted)
        ));
        assert!(matches!(second.wait().await.kind(), ExitKind::NeverStarted));

        drop(
            scope
                .reserve_task("first")
                .expect("first reservation was released"),
        );
        drop(
            scope
                .reserve_task("second")
                .expect("reentrant second reservation was released"),
        );
        system
            .shutdown(Duration::from_secs(1))
            .await
            .expect("cancelled reservations leave no stragglers");
    }
}
