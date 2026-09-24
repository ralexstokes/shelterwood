//! The [`RawActor`] trait, its definitions, and their erasure into a
//! spawnable construction.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use crate::{
    ChildId, ExitResult, Incarnation, Mailbox, MailboxShutdown, PolicyError, Readiness,
    ReadinessDeadline, RestartPolicy, Retention, Shutdown,
    cells::{MemberCell, RetainedExitResult},
    definition::DefinitionSource,
    mailbox::{MailboxCell, MailboxControl, MailboxEffectQueue, actor_ref_from_parts},
    policy::CommonOptions,
    runtime::{
        CompletionGatedLatch, Isolated, Latch, PanicAccumulator, PanicPayload, UnwindPanics,
        resume_preferred_panic,
    },
    scope::ScopeRef,
};

use super::{
    context::RawContext,
    disposal::{CatchUnwindFuture, PanicSlot},
};

/// Minimal actor contract for application-owned receive loops.
pub trait RawActor: Send + 'static {
    /// Message accepted by this actor.
    type Msg: Send + 'static;

    /// Declares when this actor type becomes ready.
    ///
    /// This is definition metadata: the framework reads it before constructing
    /// an incarnation, so it cannot depend on per-incarnation actor state.
    fn readiness() -> Readiness {
        Readiness::Immediate
    }

    /// Runs one incarnation using the membership-owned mailbox binding.
    ///
    /// The framework calls this method at most once on an incarnation's root
    /// raw-actor value and never re-enters it on that value. Shutdown may
    /// destroy a constructed root before its run begins; a restart that reaches
    /// construction obtains a fresh root value.
    ///
    /// [`RawContext::recv`] freezes external intake and returns `None` when
    /// shutdown begins. A raw loop must then honor
    /// [`RawContext::mailbox_shutdown`]: for
    /// [`MailboxShutdown::Drain`], repeatedly call [`RawContext::try_recv`] to
    /// handle the frozen accepted prefix; for [`MailboxShutdown::Discard`],
    /// return without draining. The high-level [`crate::Actor`] loop implements
    /// this policy automatically.
    fn run(
        &mut self,
        context: &mut RawContext<Self::Msg>,
    ) -> impl Future<Output = ExitResult> + Send;
}

/// Restartable raw-actor definition.
pub struct RawDef<R: RawActor> {
    factory: Box<dyn Fn() -> R + Send + Sync + 'static>,
    pub(crate) options: CommonOptions,
}

impl<R: RawActor> fmt::Debug for RawDef<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawDef")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<R: RawActor> RawDef<R> {
    /// Creates a restartable definition from a repeatable actor factory.
    pub fn factory(factory: impl Fn() -> R + Send + Sync + 'static) -> Self {
        Self {
            factory: Box::new(factory),
            options: CommonOptions::default(),
        }
    }

    common_options_setters!(
        restart,
        shutdown,
        mailbox,
        mailbox_shutdown,
        raw_readiness,
        structural_readiness_deadline,
        retention,
    );

    pub(crate) fn erase(
        mut definition: Isolated<Self>,
        mailbox: Arc<MailboxCell<R::Msg>>,
    ) -> RawConstruction {
        let readiness = definition
            .get()
            .options
            .readiness
            .unwrap_or_else(R::readiness);
        let Self { factory, options } = definition
            .take()
            .expect("isolated raw definition is available");
        RawConstruction {
            source: DefinitionSource::Restartable(Arc::new(move || {
                let actor = factory();
                Box::new(RawInstance {
                    actor,
                    mailbox: Arc::clone(&mailbox),
                })
            })),
            options,
            readiness,
        }
    }
}

/// Consuming one-shot raw-actor definition.
pub struct RawOnceDef<R: RawActor> {
    actor: R,
    pub(crate) options: CommonOptions,
}

impl<R: RawActor> fmt::Debug for RawOnceDef<R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawOnceDef")
            .field("actor", &"<owned raw actor>")
            .field("options", &self.options)
            .finish()
    }
}

impl<R: RawActor> RawOnceDef<R> {
    /// Creates a one-shot definition from an owned actor value.
    pub fn new(actor: R) -> Self {
        Self {
            actor,
            options: CommonOptions::default(),
        }
    }

    common_options_setters!(
        shutdown,
        mailbox,
        mailbox_shutdown,
        raw_readiness,
        structural_readiness_deadline,
        retention,
    );

    pub(crate) fn erase(
        mut definition: Isolated<Self>,
        mailbox: Arc<MailboxCell<R::Msg>>,
    ) -> RawConstruction {
        let readiness = definition
            .get()
            .options
            .readiness
            .unwrap_or_else(R::readiness);
        let Self { actor, options } = definition
            .take()
            .expect("isolated raw definition is available");
        RawConstruction {
            source: DefinitionSource::OneShot(Box::new(RawInstance { actor, mailbox })),
            options,
            readiness,
        }
    }
}

type RawFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
type RawFactory = Arc<dyn Fn() -> Box<dyn ErasedRawInstance> + Send + Sync + 'static>;

pub(crate) trait ErasedRawInstance: Send {
    fn run(self: Box<Self>, context: RawRunContext, readiness: Readiness) -> RawFuture;
}

struct RawInstance<R: RawActor> {
    actor: R,
    mailbox: Arc<MailboxCell<R::Msg>>,
}

/// Owns everything a raw incarnation's teardown must report on.
///
/// Teardown is freeze, join, then this owner's `Drop`: the raw context
/// (resources before actor state, §6.5), then the actor, then the preferred
/// panic resumes. The normal epilogue and a hard abort that destroys the
/// incarnation future mid-join therefore finish through the same code, and
/// the evidence never leaves a `Drop`-bearing owner (§8: a panic is never
/// masked).
struct RawIncarnationOwner<R: RawActor> {
    raw: Option<RawContext<R::Msg>>,
    actor: Option<R>,
    /// The actor's own panic from `run`, which outranks all cleanup evidence.
    primary_panic: Option<PanicPayload>,
    /// The incarnation's first-wins cleanup slot, shared with its resources.
    cleanup: Arc<PanicSlot>,
}

impl<R: RawActor> RawIncarnationOwner<R> {
    fn new(raw: RawContext<R::Msg>, actor: R) -> Self {
        Self {
            cleanup: raw.panic_slot(),
            raw: Some(raw),
            actor: Some(actor),
            primary_panic: None,
        }
    }

    fn parts(&mut self) -> (&mut R, &mut RawContext<R::Msg>) {
        let actor = self.actor.as_mut().expect("raw actor owner is armed");
        let raw = self.raw.as_mut().expect("raw context owner is armed");
        (actor, raw)
    }

    fn raw(&mut self) -> &mut RawContext<R::Msg> {
        self.raw.as_mut().expect("raw context owner is armed")
    }

    /// Closes mailbox intake and freezes incarnation resources.
    fn freeze(&mut self, mailbox: &MailboxCell<R::Msg>, incarnation: Incarnation) {
        // Wake receivers before synchronous resource destruction: a resource
        // destructor can wait for that wake. Keep its lower-priority panic in
        // a Drop-bearing accumulator until resource freezing has finished.
        let mut mailbox_panics = PanicAccumulator::default();
        mailbox_panics.run(|| {
            let mut effects = MailboxEffectQueue::default();
            mailbox.freeze(incarnation, &mut effects);
        });
        let raw = self.raw.as_mut().expect("raw context owner is armed");
        self.cleanup.run(|| raw.freeze_resources());
        if let Some(payload) = mailbox_panics.take() {
            self.cleanup.record(payload);
        }
    }

    async fn join(&mut self) {
        if let Err(payload) = CatchUnwindFuture::new(self.raw().join_resources()).await {
            self.cleanup.record(payload);
        }
    }
}

impl<R: RawActor> Drop for RawIncarnationOwner<R> {
    fn drop(&mut self) {
        self.cleanup.run(|| drop(self.raw.take()));
        self.cleanup.run(|| drop(self.actor.take()));
        resume_preferred_panic(UnwindPanics {
            primary: self.primary_panic.take(),
            cleanup: self.cleanup.take(),
        });
    }
}

impl<R: RawActor> ErasedRawInstance for RawInstance<R> {
    fn run(self: Box<Self>, context: RawRunContext, readiness: Readiness) -> RawFuture {
        Box::pin(async move {
            let Self { actor, mailbox } = *self;
            let incarnation = context.incarnation;
            let myself = actor_ref_from_parts(Arc::clone(&context.member), Arc::clone(&mailbox));
            let raw = RawContext::new(context, myself, Arc::clone(&mailbox), readiness);
            let mut owner = RawIncarnationOwner::new(raw, actor);
            let outcome = {
                let (actor, raw) = owner.parts();
                CatchUnwindFuture::new(actor.run(raw)).await
            };
            let result = match outcome {
                Ok(result) => Some(RetainedExitResult::new(result)),
                Err(payload) => {
                    owner.primary_panic = Some(payload);
                    None
                }
            };
            owner.freeze(&mailbox, incarnation);
            owner.join().await;
            // Resumes the actor's panic, else the first cleanup panic.
            drop(owner);
            result
                .expect("an incarnation without a primary panic returns a result")
                .into_result()
        })
    }
}

pub(crate) struct RawConstruction {
    pub(crate) source: DefinitionSource<RawFactory, Box<dyn ErasedRawInstance>>,
    options: CommonOptions,
    readiness: Readiness,
}

impl RawConstruction {
    pub(crate) fn options(&self) -> &CommonOptions {
        &self.options
    }

    pub(crate) fn readiness(&self) -> Readiness {
        self.readiness
    }

    pub(crate) fn one_shot(&self) -> bool {
        self.source.is_one_shot()
    }

    pub(crate) fn take_spawn(&mut self) -> RawSpawn {
        if let Some(factory) = self.source.restartable() {
            RawSpawn(RawSpawnKind::Restartable(Arc::clone(factory)))
        } else {
            RawSpawn(RawSpawnKind::OneShot(self.source.take_one_shot().expect(
                "one-shot raw actor construction invoked more than once",
            )))
        }
    }

    #[cfg(test)]
    pub(crate) fn for_policy_test(options: CommonOptions, readiness: Readiness) -> Self {
        Self {
            source: DefinitionSource::Restartable(Arc::new(|| {
                unreachable!("policy resolution never constructs the actor")
            })),
            options,
            readiness,
        }
    }
}

pub(crate) struct RawSpawn(RawSpawnKind);

enum RawSpawnKind {
    Restartable(RawFactory),
    OneShot(Box<dyn ErasedRawInstance>),
}

impl RawSpawn {
    pub(crate) async fn run(self, context: RawRunContext, readiness: Readiness) -> ExitResult {
        let instance = match self.0 {
            RawSpawnKind::Restartable(factory) => factory(),
            RawSpawnKind::OneShot(instance) => instance,
        };
        instance.run(context, readiness).await
    }
}

pub(crate) struct RawRunContext {
    pub(crate) id: ChildId,
    pub(crate) incarnation: Incarnation,
    pub(crate) member: Arc<MemberCell>,
    pub(crate) scope: ScopeRef,
    pub(crate) shutdown: Latch,
    pub(crate) abort: Latch,
    pub(crate) ready: CompletionGatedLatch,
    pub(crate) local_stop: Latch,
    pub(crate) mailbox_shutdown: MailboxShutdown,
}
