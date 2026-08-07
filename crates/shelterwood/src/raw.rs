//! Minimal loop-owning raw actors and their incarnation context.

use std::{
    any::Any,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskPollContext, Poll},
};

use crate::{
    ActorRef, CancellationToken, ChildId, ExitResult, Incarnation, Mailbox, MailboxShutdown,
    PolicyError, Readiness, ReadinessDeadline, RestartPolicy, Retention, ScopeRef, Shutdown,
    driver::Latch,
    mailbox::{MailboxCell, MailboxControl, MailboxReceiver},
    policy::CommonOptions,
};

type PanicPayload = Box<dyn Any + Send + 'static>;

/// Polls a future behind a panic boundary so a panic can be recorded before
/// any surrounding state is destroyed (§7's containment boundary).
pub(crate) struct CatchUnwindFuture<F> {
    future: Option<Pin<Box<F>>>,
}

impl<F> CatchUnwindFuture<F> {
    pub(crate) fn new(future: F) -> Self {
        Self {
            future: Some(Box::pin(future)),
        }
    }
}

impl<F: Future> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output, PanicPayload>;

    fn poll(self: Pin<&mut Self>, context: &mut TaskPollContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polled = catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("a completed panic boundary was polled again")
                .as_mut()
                .poll(context)
        }));
        match polled {
            Ok(Poll::Ready(value)) => {
                let future = this.future.take();
                match catch_unwind(AssertUnwindSafe(|| drop(future))) {
                    Ok(()) => Poll::Ready(Ok(value)),
                    Err(payload) => Poll::Ready(Err(payload)),
                }
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                let future = this.future.take();
                let _ = catch_unwind(AssertUnwindSafe(|| drop(future)));
                Poll::Ready(Err(payload))
            }
        }
    }
}

/// Minimal actor contract for application-owned receive loops.
pub trait RawActor: Send + 'static {
    /// Message accepted by this actor.
    type Msg: Send + 'static;

    /// Declares when this actor becomes ready. Read before `run` is polled.
    fn readiness(&self) -> Readiness {
        Readiness::Immediate
    }

    /// Runs one incarnation using the membership-owned mailbox binding.
    fn run(
        &mut self,
        context: &mut RawContext<Self::Msg>,
    ) -> impl Future<Output = ExitResult> + Send;
}

/// Per-incarnation capabilities supplied to a [`RawActor`].
pub struct RawContext<M> {
    id: ChildId,
    incarnation: Incarnation,
    myself: ActorRef<M>,
    scope: ScopeRef,
    shutdown: CancellationToken,
    abort: CancellationToken,
    ready: Latch,
    mailbox_shutdown: MailboxShutdown,
    receiver: MailboxReceiver<M>,
}

impl<M> fmt::Debug for RawContext<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawContext")
            .field("id", &self.id)
            .field("incarnation", &self.incarnation)
            .finish_non_exhaustive()
    }
}

impl<M: Send + 'static> RawContext<M> {
    fn new(run: RawRunContext, myself: ActorRef<M>, mailbox: Arc<MailboxCell<M>>) -> Self {
        Self {
            id: run.id,
            incarnation: run.incarnation,
            myself,
            scope: run.scope,
            shutdown: CancellationToken::from_latch(run.shutdown),
            abort: CancellationToken::from_latch(run.abort),
            ready: run.ready,
            mailbox_shutdown: run.mailbox_shutdown,
            receiver: MailboxReceiver::new(mailbox, run.incarnation),
        }
    }

    /// Returns this actor's child id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        &self.id
    }

    /// Returns this actor's current incarnation.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.incarnation
    }

    /// Returns a membership-addressed handle to this actor.
    #[must_use]
    pub fn myself(&self) -> ActorRef<M> {
        self.myself.clone()
    }

    /// Returns the actor's supervising scope.
    #[must_use]
    pub fn scope(&self) -> ScopeRef {
        self.scope.clone()
    }

    /// Returns the cooperative shutdown token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Returns the escalation token.
    #[must_use]
    pub fn abort_token(&self) -> CancellationToken {
        self.abort.clone()
    }

    /// Requests shutdown of the supervising scope without waiting.
    pub fn request_scope_shutdown(&self) {
        self.scope.request_shutdown();
    }

    /// Returns the resolved frozen-prefix shutdown policy.
    #[must_use]
    pub fn mailbox_shutdown(&self) -> MailboxShutdown {
        self.mailbox_shutdown
    }

    /// Releases this incarnation's readiness gate.
    pub fn mark_ready(&self) {
        self.ready.fire();
    }

    /// Receives the next accepted message, biased toward shutdown.
    pub async fn recv(&mut self) -> Option<M> {
        loop {
            if self.shutdown.is_cancelled() {
                return None;
            }
            if let Some(message) = self.receiver.try_recv_live() {
                return Some(message);
            }
            let shutdown = self.shutdown.clone();
            match crate::driver::select(shutdown.cancelled(), self.receiver.changed()).await {
                crate::driver::Selected::First(()) => return None,
                crate::driver::Selected::Second(()) => {}
            }
        }
    }

    /// Drains one already-accepted message without consulting shutdown.
    pub fn try_recv(&mut self) -> Option<M> {
        self.receiver.try_recv()
    }
}

/// Restartable raw-actor definition.
pub struct RawDef<R: RawActor> {
    factory: Arc<Mutex<Box<dyn Fn() -> R + Send + 'static>>>,
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
    pub fn factory(factory: impl Fn() -> R + Send + 'static) -> Self {
        Self {
            factory: Arc::new(Mutex::new(Box::new(factory))),
            options: CommonOptions::default(),
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

    /// Overrides the actor mailbox kind and capacity.
    #[must_use]
    pub fn mailbox(mut self, mailbox: Mailbox) -> Self {
        self.options.mailbox = Some(mailbox);
        self
    }

    /// Overrides frozen-prefix drain versus discard behavior.
    #[must_use]
    pub fn mailbox_shutdown(mut self, shutdown: MailboxShutdown) -> Self {
        self.options.mailbox_shutdown = Some(shutdown);
        self
    }

    /// Overrides the actor's declared readiness mode.
    pub fn readiness(mut self, readiness: Readiness) -> Result<Self, PolicyError> {
        if readiness == Readiness::AfterInit {
            return Err(PolicyError::UnsupportedReadiness);
        }
        self.options.readiness = Some(readiness);
        Ok(self)
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

    pub(crate) fn erase(self, mailbox: Arc<MailboxCell<R::Msg>>) -> RawConstruction {
        let factory = self.factory;
        RawConstruction {
            source: RawSource::Restartable(Arc::new(Mutex::new(Box::new(move || {
                let actor = (factory
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner))(
                );
                Box::new(RawInstance {
                    actor,
                    mailbox: Arc::clone(&mailbox),
                })
            })))),
            options: self.options,
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

    /// Overrides the shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.options.shutdown = Some(shutdown);
        self
    }

    /// Overrides the actor mailbox kind and capacity.
    #[must_use]
    pub fn mailbox(mut self, mailbox: Mailbox) -> Self {
        self.options.mailbox = Some(mailbox);
        self
    }

    /// Overrides frozen-prefix drain versus discard behavior.
    #[must_use]
    pub fn mailbox_shutdown(mut self, shutdown: MailboxShutdown) -> Self {
        self.options.mailbox_shutdown = Some(shutdown);
        self
    }

    /// Overrides the actor's declared readiness mode.
    pub fn readiness(mut self, readiness: Readiness) -> Result<Self, PolicyError> {
        if readiness == Readiness::AfterInit {
            return Err(PolicyError::UnsupportedReadiness);
        }
        self.options.readiness = Some(readiness);
        Ok(self)
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

    pub(crate) fn erase(self, mailbox: Arc<MailboxCell<R::Msg>>) -> RawConstruction {
        RawConstruction {
            source: RawSource::OneShot(Some(Box::new(RawInstance {
                actor: self.actor,
                mailbox,
            }))),
            options: self.options,
        }
    }
}

pub(crate) type RawFuture = Pin<Box<dyn Future<Output = ExitResult> + Send + 'static>>;
type RawFactory = Arc<Mutex<Box<dyn Fn() -> Box<dyn ErasedRawInstance> + Send + 'static>>>;

pub(crate) trait ErasedRawInstance: Send {
    fn readiness(&self) -> Readiness;
    fn run(self: Box<Self>, context: RawRunContext) -> RawFuture;
}

struct RawInstance<R: RawActor> {
    actor: R,
    mailbox: Arc<MailboxCell<R::Msg>>,
}

impl<R: RawActor> ErasedRawInstance for RawInstance<R> {
    fn readiness(&self) -> Readiness {
        self.actor.readiness()
    }

    fn run(self: Box<Self>, context: RawRunContext) -> RawFuture {
        Box::pin(async move {
            let Self { mut actor, mailbox } = *self;
            let incarnation = context.incarnation;
            let myself = ActorRef::new(Arc::clone(&context.member), Arc::clone(&mailbox));
            let mut raw = RawContext::new(context, myself, Arc::clone(&mailbox));
            // The panic boundary sits before the actor value is destroyed
            // (§7): a `run` panic must never unwind through the actor's own
            // destructor, where a second panic would abort the process.
            let outcome = CatchUnwindFuture::new(actor.run(&mut raw)).await;
            mailbox.freeze(incarnation);
            drop(raw);
            match outcome {
                Ok(result) => {
                    drop(actor);
                    result
                }
                Err(payload) => {
                    let _ = catch_unwind(AssertUnwindSafe(|| drop(actor)));
                    resume_unwind(payload)
                }
            }
        })
    }
}

pub(crate) struct RawConstruction {
    pub(crate) source: RawSource,
    pub(crate) options: CommonOptions,
}

impl RawConstruction {
    pub(crate) fn one_shot(&self) -> bool {
        matches!(self.source, RawSource::OneShot(_) | RawSource::Spent)
    }

    pub(crate) fn take_spawn(&mut self) -> RawSpawn {
        match &mut self.source {
            RawSource::Restartable(factory) => RawSpawn::Restartable(Arc::clone(factory)),
            RawSource::OneShot(instance) => {
                let instance = instance
                    .take()
                    .expect("one-shot raw actor construction invoked more than once");
                self.source = RawSource::Spent;
                RawSpawn::OneShot(instance)
            }
            RawSource::Spent => panic!("one-shot raw actor construction invoked more than once"),
        }
    }
}

pub(crate) enum RawSpawn {
    Restartable(RawFactory),
    OneShot(Box<dyn ErasedRawInstance>),
}

impl RawSpawn {
    pub(crate) fn construct(self) -> Box<dyn ErasedRawInstance> {
        match self {
            Self::Restartable(factory) => (factory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner))(
            ),
            Self::OneShot(instance) => instance,
        }
    }
}

pub(crate) enum RawSource {
    Restartable(RawFactory),
    OneShot(Option<Box<dyn ErasedRawInstance>>),
    Spent,
}

pub(crate) struct RawRunContext {
    pub(crate) id: ChildId,
    pub(crate) incarnation: Incarnation,
    pub(crate) member: Arc<crate::driver::MemberCell>,
    pub(crate) scope: ScopeRef,
    pub(crate) shutdown: Latch,
    pub(crate) abort: Latch,
    pub(crate) ready: Latch,
    pub(crate) mailbox_shutdown: MailboxShutdown,
}
