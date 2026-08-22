//! The [`RawActor`] trait, its definitions, and their erasure into a
//! spawnable construction.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use crate::{
    ChildId, ExitResult, Incarnation, Mailbox, MailboxShutdown, PolicyError, Readiness,
    ReadinessDeadline, RestartPolicy, Retention, Shutdown,
    cells::MemberCell,
    definition::DefinitionSource,
    mailbox::{MailboxCell, MailboxControl, MailboxEffectQueue, actor_ref_from_parts},
    policy::CommonOptions,
    runtime::{
        self, CompletionGatedLatch, Isolated, Latch, PanicAccumulator, PanicPayload, UnwindPanics,
        catch_panic, keep_first_panic, resume_preferred_panic,
        resume_preferred_panic_outside_unwind,
    },
    scope::ScopeRef,
};

use super::{context::RawContext, disposal::CatchUnwindFuture};

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

/// A completed raw outcome retained across the fallible teardown epilogue.
///
/// A failed result owns a type-erased application error. If teardown resumes a
/// panic, the incarnation future drops this carrier during that unwind, so the
/// error must not be destroyed on the same stack. The normal return path takes
/// the result back and preserves ordinary downstream ownership.
struct RetainedExitResult(Option<ExitResult>);

impl RetainedExitResult {
    fn new(result: ExitResult) -> Self {
        Self(Some(result))
    }

    fn into_result(mut self) -> ExitResult {
        self.0
            .take()
            .expect("retained exit result was already taken")
    }
}

impl Drop for RetainedExitResult {
    fn drop(&mut self) {
        let Some(result) = self.0.take() else {
            return;
        };
        if let Err(error) = result {
            runtime::dispose_critical(error);
        }
    }
}

pub(crate) trait ErasedRawInstance: Send {
    fn run(self: Box<Self>, context: RawRunContext, readiness: Readiness) -> RawFuture;
}

struct RawInstance<R: RawActor> {
    actor: R,
    mailbox: Arc<MailboxCell<R::Msg>>,
}

struct RawIncarnationOwner<R: RawActor> {
    raw: Option<RawContext<R::Msg>>,
    actor: Option<R>,
    primary_panic: Option<PanicPayload>,
}

impl<R: RawActor> RawIncarnationOwner<R> {
    fn new(raw: RawContext<R::Msg>, actor: R) -> Self {
        Self {
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

    fn drop_raw(&mut self) {
        drop(self.raw.take());
    }

    fn drop_actor(&mut self) {
        drop(self.actor.take());
    }

    fn record_primary_panic(&mut self, payload: PanicPayload) {
        // The actor's first panic is authoritative. A second opaque payload
        // may itself have hostile drop glue, so route the loser through the
        // same contained precedence helper as teardown rather than asserting
        // and destroying it during the assertion's unwind.
        keep_first_panic(&mut self.primary_panic, Some(payload));
    }

    fn take_primary_panic(&mut self) -> Option<PanicPayload> {
        self.primary_panic.take()
    }
}

impl<R: RawActor> Drop for RawIncarnationOwner<R> {
    fn drop(&mut self) {
        // A hard abort destroys the incarnation future instead of polling its
        // teardown epilogue. Preserve §6.5's resource-before-actor order, but
        // put a boundary around each destructor so two panics cannot abort the
        // process. The resource panic is primary: it may be an owned offload
        // panic that completed before cancellation was requested.
        let primary_panic = self.take_primary_panic();
        let mut cleanup = PanicAccumulator::default();
        cleanup.run(|| self.drop_raw());
        cleanup.run(|| self.drop_actor());
        resume_preferred_panic(UnwindPanics {
            primary: primary_panic,
            cleanup: cleanup.take(),
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
                    // Keep the actor's diagnostic in the owned epilogue so a
                    // hard abort during async teardown cannot replace it with
                    // a later cancellation or destructor panic.
                    owner.record_primary_panic(payload);
                    None
                }
            };
            let mailbox_freeze_panic = catch_panic(|| {
                let mut effects = MailboxEffectQueue::default();
                mailbox.freeze(incarnation, &mut effects);
            })
            .err();
            let resource_freeze_panic = catch_panic(|| owner.raw().freeze_resources()).err();
            let mut cleanup_panic = owner.raw().take_resource_panic();
            keep_first_panic(&mut cleanup_panic, mailbox_freeze_panic);
            keep_first_panic(&mut cleanup_panic, resource_freeze_panic);

            let joined = CatchUnwindFuture::new(owner.raw().join_resources()).await;
            keep_first_panic(&mut cleanup_panic, joined.err());
            let pending = owner.raw().take_resource_panic();
            keep_first_panic(&mut cleanup_panic, pending);
            let raw_drop = catch_panic(|| owner.drop_raw()).err();
            keep_first_panic(&mut cleanup_panic, raw_drop);

            let actor_drop = catch_panic(|| owner.drop_actor()).err();
            keep_first_panic(&mut cleanup_panic, actor_drop);
            // Once actor execution has panicked, teardown is secondary: never
            // replace the actor's original diagnostic. This is the incarnation
            // body's normal return path, so the resume must be unconditional:
            // containing the primary payload here would strand `result` at
            // `None` and report the actor's panic as the framework expect
            // below.
            resume_preferred_panic_outside_unwind(UnwindPanics {
                primary: owner.take_primary_panic(),
                cleanup: cleanup_panic,
            });
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
