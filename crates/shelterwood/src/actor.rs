//! Callback-oriented actors layered entirely on the public raw-actor surface.

use std::{
    fmt,
    future::Future,
    hash::Hash,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    ActorRef, Blocking, CancellationToken, ChildId, DeadlineElapsed, ExitError, ExitResult, Guard,
    Incarnation, Mailbox, MailboxShutdown, RawActor, RawContext, RawDef, RawOnceDef, Readiness,
    ReadinessDeadline, Rejected, RestartPolicy, Retention, ScopeRef, Shutdown,
    policy::CommonOptions, raw::CatchUnwindFuture,
};

/// Callback-oriented actor contract.
pub trait Actor: Sized + Send + 'static {
    /// Message accepted by the actor.
    type Msg: Send + 'static;

    /// Fresh per-incarnation input consumed by [`Actor::init`].
    type Args: Send + 'static;

    /// Constructs one actor incarnation.
    fn init(
        args: Self::Args,
        context: &mut Context<'_, Self>,
    ) -> impl Future<Output = Result<Self, ExitError>> + Send;

    /// Handles one mailbox, continuation, timer, or offload delivery.
    fn handle(
        &mut self,
        message: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> impl Future<Output = ExitResult> + Send;

    /// Performs best-effort cooperative teardown.
    fn on_stop(&mut self, _context: &mut StopContext<'_, Self>) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Callback context used by both live and frozen-prefix handler deliveries.
pub struct Context<'a, A: Actor> {
    raw: &'a mut RawContext<A::Msg>,
    draining: bool,
    actor: PhantomData<fn() -> A>,
}

impl<A: Actor> fmt::Debug for Context<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Context")
            .field("id", self.raw.id())
            .field("incarnation", &self.raw.incarnation())
            .field("draining", &self.draining)
            .finish_non_exhaustive()
    }
}

impl<'a, A: Actor> Context<'a, A> {
    fn new(raw: &'a mut RawContext<A::Msg>, draining: bool) -> Self {
        Self {
            raw,
            draining,
            actor: PhantomData,
        }
    }

    /// Returns this actor's child id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.raw.id()
    }

    /// Returns this actor's current incarnation.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.raw.incarnation()
    }

    /// Returns a membership-addressed handle to this actor.
    #[must_use]
    pub fn myself(&self) -> ActorRef<A::Msg> {
        self.raw.myself()
    }

    /// Returns this actor's supervising scope.
    #[must_use]
    pub fn scope(&self) -> ScopeRef {
        self.raw.scope()
    }

    /// Returns the cooperative shutdown token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.raw.shutdown_token()
    }

    /// Returns the escalation token.
    #[must_use]
    pub fn abort_token(&self) -> CancellationToken {
        self.raw.abort_token()
    }

    /// Requests shutdown of the supervising scope without waiting.
    pub fn request_scope_shutdown(&self) {
        self.raw.request_scope_shutdown();
    }

    /// Releases this incarnation's readiness gate while live.
    pub fn mark_ready(&self) {
        if !self.draining {
            self.raw.mark_ready();
        }
    }

    /// Requests a clean local stop after the current callback.
    pub fn stop(&mut self) {
        if !self.draining {
            self.raw.stop();
        }
    }

    /// Reports whether this callback is draining the frozen mailbox prefix.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.draining
    }

    /// Queues an actor-local continuation ahead of external input.
    pub fn continue_with(&mut self, message: A::Msg) -> Result<(), Rejected<A::Msg>> {
        if self.draining {
            Err(Rejected::new(message))
        } else {
            self.raw.continue_with(message)
        }
    }

    /// Arms or replaces a one-shot keyed timer.
    pub fn set_timeout<K>(
        &mut self,
        key: K,
        message: A::Msg,
        after: Duration,
    ) -> Result<(), Rejected<(K, A::Msg)>>
    where
        K: Hash + Eq + Send + 'static,
    {
        if self.draining {
            Err(Rejected::new((key, message)))
        } else {
            self.raw.set_timeout(key, message, after)
        }
    }

    /// Arms or replaces a keyed interval; a zero period clears the key.
    pub fn set_interval<K>(
        &mut self,
        key: K,
        message: A::Msg,
        period: Duration,
    ) -> Result<(), Rejected<(K, A::Msg)>>
    where
        K: Hash + Eq + Send + 'static,
        A::Msg: Clone,
    {
        if self.draining {
            Err(Rejected::new((key, message)))
        } else {
            self.raw.set_interval(key, message, period)
        }
    }

    /// Retracts a keyed timer or rejects the operation while draining.
    pub fn clear_timer<K>(&mut self, key: &K) -> Result<bool, Rejected<()>>
    where
        K: Hash + Eq + Send + 'static,
    {
        if self.draining {
            Err(Rejected::new(()))
        } else {
            Ok(self.raw.clear_timer(key))
        }
    }

    /// Starts incarnation-owned async work with one total deadline budget.
    pub fn offload<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: Duration,
    ) -> Result<(), Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> A::Msg + Send + 'static,
    {
        if self.draining {
            Err(Rejected::new((work, continuation)))
        } else {
            self.raw.offload(work, continuation, deadline)
        }
    }

    /// Starts guarded incarnation-owned async work with one deadline budget.
    pub fn offload_scoped<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: Duration,
    ) -> Result<Guard, Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> A::Msg + Send + 'static,
    {
        if self.draining {
            Err(Rejected::new((work, continuation)))
        } else {
            self.raw.offload_scoped(work, continuation, deadline)
        }
    }

    /// Starts blocking work tied to actor shutdown and returned-future drop.
    ///
    /// Cancellation is cooperative; a hard-aborted operation's OS thread
    /// detaches and may outlive this actor incarnation.
    pub fn run_blocking<F, T>(&self, operation: F) -> Blocking<T>
    where
        F: FnOnce(CancellationToken) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.raw.run_blocking(operation)
    }

    /// Re-enters a same-message actor with this exact context and stage.
    pub fn for_actor<B: Actor<Msg = A::Msg>>(&mut self) -> Context<'_, B> {
        Context::new(self.raw, self.draining)
    }
}

/// Narrowed context supplied only to [`Actor::on_stop`].
pub struct StopContext<'a, A: Actor> {
    raw: &'a mut RawContext<A::Msg>,
    actor: PhantomData<fn() -> A>,
}

impl<A: Actor> fmt::Debug for StopContext<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StopContext")
            .field("id", self.raw.id())
            .field("incarnation", &self.raw.incarnation())
            .finish_non_exhaustive()
    }
}

impl<'a, A: Actor> StopContext<'a, A> {
    fn new(raw: &'a mut RawContext<A::Msg>) -> Self {
        Self {
            raw,
            actor: PhantomData,
        }
    }

    /// Returns this actor's child id.
    #[must_use]
    pub fn id(&self) -> &ChildId {
        self.raw.id()
    }

    /// Returns this actor's current incarnation.
    #[must_use]
    pub fn incarnation(&self) -> Incarnation {
        self.raw.incarnation()
    }

    /// Returns a membership-addressed handle to this actor.
    #[must_use]
    pub fn myself(&self) -> ActorRef<A::Msg> {
        self.raw.myself()
    }

    /// Returns this actor's supervising scope.
    #[must_use]
    pub fn scope(&self) -> ScopeRef {
        self.raw.scope()
    }

    /// Returns the cooperative shutdown token.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.raw.shutdown_token()
    }

    /// Returns the escalation token.
    #[must_use]
    pub fn abort_token(&self) -> CancellationToken {
        self.raw.abort_token()
    }

    /// Requests shutdown of the supervising scope without waiting.
    pub fn request_scope_shutdown(&self) {
        self.raw.request_scope_shutdown();
    }

    /// Starts blocking work tied to actor shutdown and returned-future drop.
    ///
    /// Cancellation is cooperative; a hard-aborted operation's OS thread
    /// detaches and may outlive this actor incarnation.
    pub fn run_blocking<F, T>(&self, operation: F) -> Blocking<T>
    where
        F: FnOnce(CancellationToken) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.raw.run_blocking(operation)
    }

    /// Re-enters a same-message actor with the same narrowed stop context.
    pub fn for_actor<B: Actor<Msg = A::Msg>>(&mut self) -> StopContext<'_, B> {
        StopContext::new(self.raw)
    }
}

/// Raw-actor wrapper that owns the `Uninit(args) -> Running(actor)` transition.
///
/// This is the composition point for raw decorators around callback-oriented
/// actors. Its declared readiness is read before [`RawActor::run`] is polled.
pub struct Handler<A: Actor> {
    args: Option<A::Args>,
    readiness: Readiness,
}

impl<A: Actor> Handler<A> {
    /// Wraps owned args using the callback-actor default `AfterInit` readiness.
    pub fn new(args: A::Args) -> Self {
        Self::with_readiness(args, Readiness::AfterInit)
    }

    fn with_readiness(args: A::Args, readiness: Readiness) -> Self {
        Self {
            args: Some(args),
            readiness,
        }
    }
}

impl<A: Actor> RawActor for Handler<A> {
    type Msg = A::Msg;

    fn readiness(&self) -> Readiness {
        self.readiness
    }

    async fn run(&mut self, raw: &mut RawContext<Self::Msg>) -> ExitResult {
        let args = self
            .args
            .take()
            .expect("handler actor initialization invoked more than once");
        let mut actor = {
            let mut context = Context::<A>::new(raw, false);
            A::init(args, &mut context).await?
        };
        if raw.readiness() == Readiness::AfterInit {
            raw.mark_ready();
        }

        loop {
            let received = CatchUnwindFuture::new(raw.recv()).await;
            let message = match received {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(payload) => resume_after_teardown(raw, actor, payload).await,
            };
            let handled = {
                let mut context = Context::<A>::new(raw, false);
                CatchUnwindFuture::new(actor.handle(message, &mut context)).await
            };
            match handled {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return fail_after_teardown(raw, error).await,
                Err(payload) => resume_after_teardown(raw, actor, payload).await,
            }
        }

        match raw.mailbox_shutdown() {
            MailboxShutdown::Drain => {
                while let Some(message) = raw.try_recv() {
                    let handled = {
                        let mut context = Context::<A>::new(raw, true);
                        CatchUnwindFuture::new(actor.handle(message, &mut context)).await
                    };
                    match handled {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => return fail_after_teardown(raw, error).await,
                        Err(payload) => resume_after_teardown(raw, actor, payload).await,
                    }
                }
            }
            MailboxShutdown::Discard => {
                while let Some(message) = raw.try_recv() {
                    drop(message);
                }
            }
        }

        let mut context = StopContext::<A>::new(raw);
        if let Err(payload) = CatchUnwindFuture::new(actor.on_stop(&mut context)).await {
            resume_after_teardown(raw, actor, payload).await;
        }
        Ok(())
    }
}

/// Propagates a handler error after §5.5's orderly teardown order: incarnation-owned
/// work is frozen, cancelled, and joined before actor state — which the
/// caller still owns — is dropped at its frame's exit.
async fn fail_after_teardown<M: Send + 'static>(
    raw: &mut RawContext<M>,
    error: ExitError,
) -> ExitResult {
    raw.freeze_resources();
    raw.join_resources().await;
    Err(error)
}

/// Resumes a caught callback panic after §5.5's orderly teardown order: offloads and
/// other incarnation-owned work are destroyed (frozen, cancelled, joined)
/// before actor state, and the destructor runs outside the callback's unwind
/// per §7's containment boundary.
async fn resume_after_teardown<A, M: Send + 'static>(
    raw: &mut RawContext<M>,
    actor: A,
    payload: Box<dyn std::any::Any + Send + 'static>,
) -> ! {
    raw.freeze_resources();
    raw.join_resources().await;
    let _ = catch_unwind(AssertUnwindSafe(|| drop(actor)));
    resume_unwind(payload)
}

type ArgsFactory<A> = Arc<Mutex<Box<dyn Fn() -> <A as Actor>::Args + Send + 'static>>>;

/// Restartable handler-actor definition.
pub struct ActorDef<A: Actor> {
    factory: ArgsFactory<A>,
    pub(crate) options: CommonOptions,
}

impl<A: Actor> fmt::Debug for ActorDef<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorDef")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<A: Actor> ActorDef<A> {
    /// Creates a restartable definition by cloning args inside each incarnation.
    pub fn cloned(args: A::Args) -> Self
    where
        A::Args: Clone,
    {
        Self::factory(move || args.clone())
    }

    /// Creates a restartable definition from a fresh per-incarnation args source.
    pub fn factory(factory: impl Fn() -> A::Args + Send + 'static) -> Self {
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
    #[must_use]
    pub fn readiness(mut self, readiness: Readiness) -> Self {
        self.options.readiness = Some(readiness);
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

    pub(crate) fn into_raw(self) -> RawDef<Handler<A>> {
        let factory = self.factory;
        let readiness = self.options.readiness.unwrap_or(Readiness::AfterInit);
        let mut raw = RawDef::factory(move || {
            let args = (factory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner))();
            Handler::with_readiness(args, readiness)
        });
        raw.options = self.options;
        raw
    }
}

/// Consuming one-shot handler-actor definition.
///
/// “One-shot” means exactly one incarnation because the owned arguments cannot
/// be minted again; it does not mean one handler iteration.
pub struct ActorOnceDef<A: Actor> {
    args: A::Args,
    pub(crate) options: CommonOptions,
}

impl<A: Actor> fmt::Debug for ActorOnceDef<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorOnceDef")
            .field("args", &"<owned actor args>")
            .field("options", &self.options)
            .finish()
    }
}

impl<A: Actor> ActorOnceDef<A> {
    /// Creates a one-shot definition from owned per-incarnation args.
    pub fn new(args: A::Args) -> Self {
        Self {
            args,
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
    #[must_use]
    pub fn readiness(mut self, readiness: Readiness) -> Self {
        self.options.readiness = Some(readiness);
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

    pub(crate) fn into_raw(self) -> RawOnceDef<Handler<A>> {
        let readiness = self.options.readiness.unwrap_or(Readiness::AfterInit);
        let mut raw = RawOnceDef::new(Handler::with_readiness(self.args, readiness));
        raw.options = self.options;
        raw
    }
}
