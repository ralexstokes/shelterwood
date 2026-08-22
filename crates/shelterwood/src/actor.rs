//! Callback-oriented actors composed through the public [`Handler`] raw-actor
//! wrapper, which encapsulates the callback loop's error-path freeze-and-join
//! discipline.

use std::{fmt, future::Future, hash::Hash, marker::PhantomData, time::Duration};

use crate::{
    ActorRef, Blocking, ChildId, DeadlineBudget, DeadlineElapsed, ExitError, ExitResult, Guard,
    Incarnation, Mailbox, MailboxShutdown, RawActor, RawContext, RawDef, RawOnceDef, Readiness,
    ReadinessDeadline, Rejected, RestartPolicy, Retention, ScopeRef, Shutdown,
    cells::CancellationToken, policy::CommonOptions,
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
    ///
    /// Runs under the child's shutdown grace with a narrowed [`StopContext`].
    /// Not called when `init` failed, when a handler returned `Err` or
    /// panicked, or on hard abort. [`StopContext`] has no self-handle;
    /// capture [`Context::myself`] during the live phase if teardown needs
    /// the handle itself. Identity alone needs no capture — the stop
    /// context still exposes [`StopContext::incarnation`].
    fn on_stop(&mut self, _context: &mut StopContext<'_, Self>) -> impl Future<Output = ()> + Send {
        async {}
    }
}

macro_rules! context_common_forwarders {
    () => {
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
        ///
        /// Do not await that scope's shutdown from this actor: the scope cannot
        /// finish until the current actor callback returns.
        pub fn request_scope_shutdown(&self) {
            self.raw.request_scope_shutdown();
        }

        /// Starts blocking work tied to actor shutdown and returned-future drop.
        ///
        /// Available from [`StopContext`] too — deliberately, unlike
        /// continuations, timers, and offloads. Awaiting the returned
        /// [`Blocking`] resumes a panic raised inside the closure at the
        /// await point.
        ///
        /// Cancellation is cooperative; a hard-aborted operation's OS thread
        /// detaches and may outlive this actor incarnation.
        /// A blocking-pool rejection during runtime teardown uses a detached
        /// Shelterwood thread; an operation that never runs — cancelled with
        /// the runtime, or with no thread left to start it — makes the
        /// returned future panic with a runtime-teardown cancellation
        /// diagnostic when awaited.
        pub fn run_blocking<F, T>(&self, operation: F) -> Blocking<T>
        where
            F: FnOnce(CancellationToken) -> T + Send + 'static,
            T: Send + 'static,
        {
            self.raw.run_blocking(operation)
        }
    };
}

macro_rules! actor_context_forwarders {
    ($actor:ident) => {
        context_common_forwarders!();

        /// Returns a membership-addressed handle to this actor.
        ///
        /// Absent from [`StopContext`]. Capture it during [`Actor::init`] or
        /// [`Actor::handle`] if [`Actor::on_stop`] needs the handle itself;
        /// for identity alone the stop context keeps `incarnation()`.
        ///
        /// Never `call` and await this handle from inside a handler — the
        /// reply can be produced only by the actor loop that handler is
        /// blocking. [`crate::guides::retry_and_ordering`] catalogs the
        /// supported shapes.
        #[must_use]
        pub fn myself(&self) -> ActorRef<$actor::Msg> {
            self.raw.myself()
        }
    };
}

/// Which mailbox delivery stage owns the current callback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeliveryStage {
    Initializing,
    Live,
    DrainingFrozenPrefix,
}

/// Callback context used by both live and frozen-prefix handler deliveries.
///
/// One `Context` is lent to [`Actor::init`] and to each [`Actor::handle`]
/// call; it is the whole capability surface a callback has. The capabilities
/// group as:
///
/// - **Identity** — [`id`](Context::id),
///   [`incarnation`](Context::incarnation), and [`scope`](Context::scope)
///   name this child, this run of it, and its supervising scope.
/// - **Self-handle** — [`myself`](Context::myself) returns the
///   membership-addressed [`ActorRef`] other tasks use to reach this actor.
/// - **Protocol steps** — [`continue_with`](Context::continue_with) queues an
///   actor-local message ahead of external input, the shape for splitting
///   one request into steps without a handler awaiting itself.
/// - **Timers** — [`set_timeout`](Context::set_timeout) and
///   [`set_interval`](Context::set_interval) arm keyed deliveries;
///   [`clear_timer`](Context::clear_timer) retracts one.
/// - **Offloads** — [`offload`](Context::offload) and
///   [`offload_scoped`](Context::offload_scoped) start incarnation-owned
///   async work whose completion re-enters [`Actor::handle`] as a message.
/// - **Blocking work** — [`run_blocking`](Context::run_blocking) moves a
///   closure to a blocking thread with shutdown-tied cancellation.
/// - **Readiness** — [`mark_ready`](Context::mark_ready) releases the
///   incarnation's readiness gate, the completing move under an effective
///   [`Readiness::Manual`].
/// - **Stop and shutdown** — [`stop`](Context::stop) requests a clean local
///   stop, [`is_draining`](Context::is_draining) reports the delivery
///   regime, [`shutdown_token`](Context::shutdown_token) and
///   [`abort_token`](Context::abort_token) expose the cooperative and
///   escalation cancellation tokens, and
///   [`request_scope_shutdown`](Context::request_scope_shutdown) asks the
///   supervising scope to shut down.
/// - **Decoration** — [`for_actor`](Context::for_actor) re-projects the
///   context for a same-message wrapped actor.
///
/// # Delivery regimes
///
/// A context serves two regimes. **Live** deliveries — `init` and every
/// `handle` call before a stop — have the full surface. Once the incarnation
/// is **stopping** — after [`stop`](Context::stop), once cooperative
/// shutdown is requested, or while draining the frozen mailbox prefix under
/// [`MailboxShutdown::Drain`] — every operation that would queue new work
/// for this incarnation ([`continue_with`](Context::continue_with),
/// [`set_timeout`](Context::set_timeout),
/// [`set_interval`](Context::set_interval), [`offload`](Context::offload),
/// and [`offload_scoped`](Context::offload_scoped)) returns [`Rejected`]
/// carrying its inputs back, and during frozen-prefix drain
/// [`clear_timer`](Context::clear_timer) is rejected as well.
/// [`run_blocking`](Context::run_blocking) is deliberately not gated because
/// teardown code may still need it, and [`mark_ready`](Context::mark_ready)
/// and [`stop`](Context::stop) degrade to documented no-ops while draining
/// rather than rejecting.
///
/// # Never call and await `myself()`
///
/// Awaiting `context.myself().call(...)` from a handler deadlocks: the reply
/// can be produced only by the actor loop the handler itself is blocking.
/// [`crate::guides::retry_and_ordering`] catalogs the supported shapes —
/// continuations, offloads, and split reply channels.
///
/// # Examples
///
/// A handler splitting one request into protocol steps with
/// [`continue_with`](Context::continue_with) instead of awaiting itself:
///
/// ```
/// use shelterwood::{Actor, Context, ExitError, ExitResult, Reply};
/// # use std::time::Duration;
/// # use shelterwood::{ActorDef, Tree};
///
/// struct Pipeline {
///     staged: Option<u64>,
/// }
///
/// enum Msg {
///     Begin(u64),
///     Finish,
///     Result(Reply<Option<u64>>),
/// }
///
/// impl Actor for Pipeline {
///     type Msg = Msg;
///     type Args = ();
///
///     async fn init(_args: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
///         Ok(Self { staged: None })
///     }
///
///     async fn handle(&mut self, message: Msg, context: &mut Context<'_, Self>) -> ExitResult {
///         match message {
///             Msg::Begin(n) => {
///                 self.staged = Some(n * 2);
///                 // Queue the next protocol step; it is delivered ahead of
///                 // external input instead of this handler awaiting itself.
///                 if context.continue_with(Msg::Finish).is_err() {
///                     // Stopping incarnation: the step will not run.
///                     self.staged = None;
///                 }
///             }
///             Msg::Finish => {
///                 if let Some(n) = self.staged.as_mut() {
///                     *n += 1;
///                 }
///             }
///             Msg::Result(reply) => reply.send(self.staged),
///         }
///         Ok(())
///     }
/// }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut tree = Tree::new();
/// # let pipeline = tree.add_actor("pipeline", ActorDef::<Pipeline>::cloned(()))?;
/// # let system = tree.spawn()?;
/// # system.wait_started().await?;
/// pipeline.send(Msg::Begin(20)).await?;
/// let replied = pipeline.call(Msg::Result, Duration::from_secs(1)).await?;
/// assert_eq!(replied.value, Some(41));
/// # system.shutdown(Duration::from_secs(5)).await?;
/// # Ok(())
/// # }
/// ```
pub struct Context<'a, A: Actor> {
    raw: &'a mut RawContext<A::Msg>,
    stage: DeliveryStage,
    owns_init_boundary: bool,
    actor: PhantomData<fn() -> A>,
}

impl<A: Actor> fmt::Debug for Context<'_, A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Context")
            .field("id", self.raw.id())
            .field("incarnation", &self.raw.incarnation())
            .field("stage", &self.stage)
            .finish_non_exhaustive()
    }
}

impl<'a, A: Actor> Context<'a, A> {
    fn new_live(raw: &'a mut RawContext<A::Msg>) -> Self {
        Self {
            raw,
            stage: DeliveryStage::Live,
            owns_init_boundary: false,
            actor: PhantomData,
        }
    }

    fn new_draining(raw: &'a mut RawContext<A::Msg>) -> Self {
        Self {
            raw,
            stage: DeliveryStage::DrainingFrozenPrefix,
            owns_init_boundary: false,
            actor: PhantomData,
        }
    }

    fn new_initializing(raw: &'a mut RawContext<A::Msg>) -> Self {
        Self {
            raw,
            stage: DeliveryStage::Initializing,
            owns_init_boundary: true,
            actor: PhantomData,
        }
    }

    fn initialization_returned(&mut self) {
        assert_eq!(self.stage, DeliveryStage::Initializing);
        self.owns_init_boundary = false;
    }

    actor_context_forwarders!(A);

    /// Releases this incarnation's readiness gate while live.
    ///
    /// During frozen-prefix drain the incarnation is already stopping, so
    /// readiness can no longer change the startup outcome and this call is a
    /// deliberate no-op.
    pub fn mark_ready(&self) {
        if self.stage != DeliveryStage::DrainingFrozenPrefix {
            self.raw.mark_ready();
        }
    }

    /// Requests a clean local stop.
    ///
    /// External intake and incarnation-owned continuations, timers, and
    /// offloads freeze at this call. The current callback may finish, but any
    /// later attempt from it to queue such work is rejected. Terminal
    /// publication follows callback completion.
    ///
    /// During a successful initializer with effective [`Readiness::AfterInit`],
    /// the automatic readiness edge is published before this request becomes
    /// observable to the supervisor. This preserves `AfterInit`'s promise
    /// that returning `Ok` establishes readiness. An explicit
    /// [`Readiness::Manual`] override keeps ordinary pre-ready stop semantics.
    ///
    /// During frozen-prefix drain the incarnation is already stopping, so
    /// this call deliberately does not inject a second local-stop edge and
    /// returns silently.
    pub fn stop(&mut self) {
        match self.stage {
            DeliveryStage::Initializing if self.raw.readiness() == Readiness::AfterInit => {
                self.raw.defer_stop_until_after_init();
            }
            DeliveryStage::Initializing | DeliveryStage::Live => self.raw.stop(),
            DeliveryStage::DrainingFrozenPrefix => {}
        }
    }

    /// Reports whether this callback is draining the frozen mailbox prefix.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.stage == DeliveryStage::DrainingFrozenPrefix
    }

    /// Queues an actor-local continuation ahead of external input.
    ///
    /// Returns [`Rejected`] carrying the message back once this incarnation
    /// is stopping: after [`Context::stop`], once cooperative shutdown is
    /// requested, or during frozen-prefix drain.
    pub fn continue_with(&mut self, message: A::Msg) -> Result<(), Rejected<A::Msg>> {
        if self.stage == DeliveryStage::DrainingFrozenPrefix {
            Err(Rejected::new(message))
        } else {
            self.raw.continue_with(message)
        }
    }

    /// Arms or replaces a one-shot keyed timer.
    ///
    /// Returns [`Rejected`] carrying the key and message back once this
    /// incarnation is stopping: after [`Context::stop`], once cooperative
    /// shutdown is requested, or during frozen-prefix drain.
    pub fn set_timeout<K>(
        &mut self,
        key: K,
        message: A::Msg,
        after: Duration,
    ) -> Result<(), Rejected<(K, A::Msg)>>
    where
        K: Hash + Eq + Send + 'static,
    {
        if self.stage == DeliveryStage::DrainingFrozenPrefix {
            Err(Rejected::new((key, message)))
        } else {
            self.raw.set_timeout(key, message, after)
        }
    }

    /// Arms or replaces a keyed interval; a zero period clears the key.
    ///
    /// Returns [`Rejected`] carrying the key and message back once this
    /// incarnation is stopping: after [`Context::stop`], once cooperative
    /// shutdown is requested, or during frozen-prefix drain.
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
        if self.stage == DeliveryStage::DrainingFrozenPrefix {
            Err(Rejected::new((key, message)))
        } else {
            self.raw.set_interval(key, message, period)
        }
    }

    /// Retracts a keyed timer or rejects the operation while draining.
    ///
    /// `Ok(true)` reports that a timer was armed for the key, including one
    /// that had elapsed but was not yet delivered. [`Rejected`] is returned
    /// only during frozen-prefix drain; unlike the queueing operations, a
    /// retraction stays available to a live handler after [`Context::stop`].
    pub fn clear_timer<K>(&mut self, key: &K) -> Result<bool, Rejected<()>>
    where
        K: Hash + Eq + Send + 'static,
    {
        if self.stage == DeliveryStage::DrainingFrozenPrefix {
            Err(Rejected::new(()))
        } else {
            Ok(self.raw.clear_timer(key))
        }
    }

    /// Starts incarnation-owned async work with one total deadline budget.
    /// A zero budget never polls `work` and re-enters with
    /// [`DeadlineElapsed`].
    ///
    /// Returns [`Rejected`] handing the work and continuation back once this
    /// incarnation is stopping: after [`Context::stop`], once cooperative
    /// shutdown is requested, or during frozen-prefix drain.
    pub fn offload<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: impl Into<DeadlineBudget>,
    ) -> Result<(), Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> A::Msg + Send + 'static,
    {
        if self.stage == DeliveryStage::DrainingFrozenPrefix {
            Err(Rejected::new((work, continuation)))
        } else {
            self.raw.offload(work, continuation, deadline)
        }
    }

    /// Starts guarded incarnation-owned async work with one deadline budget.
    /// A zero budget never polls `work` and re-enters with
    /// [`DeadlineElapsed`].
    ///
    /// Returns [`Rejected`] handing the work and continuation back once this
    /// incarnation is stopping: after [`Context::stop`], once cooperative
    /// shutdown is requested, or during frozen-prefix drain.
    pub fn offload_scoped<F, T, C>(
        &mut self,
        work: F,
        continuation: C,
        deadline: impl Into<DeadlineBudget>,
    ) -> Result<Guard, Rejected<(F, C)>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
        C: FnOnce(Result<T, DeadlineElapsed>) -> A::Msg + Send + 'static,
    {
        if self.stage == DeliveryStage::DrainingFrozenPrefix {
            Err(Rejected::new((work, continuation)))
        } else {
            self.raw.offload_scoped(work, continuation, deadline)
        }
    }

    /// Re-enters a same-message actor with this exact context and stage.
    pub fn for_actor<B: Actor<Msg = A::Msg>>(&mut self) -> Context<'_, B> {
        let stage = self.stage;
        Context {
            raw: &mut *self.raw,
            stage,
            // The outermost handler context owns the initializer-return
            // boundary. A decorator's projected context must not publish a
            // deferred self-stop before the outer initializer also returns.
            owns_init_boundary: false,
            actor: PhantomData,
        }
    }
}

impl<A: Actor> Drop for Context<'_, A> {
    fn drop(&mut self) {
        if self.owns_init_boundary {
            // Cancellation and panic do not cross the successful-init
            // ordering point. Preserve the requested stop for exit
            // classification without manufacturing readiness.
            self.raw.finish_callback_init(false);
        }
    }
}

/// Narrowed context supplied only to [`Actor::on_stop`].
///
/// Work that queues delivery to this incarnation is unrepresentable: no
/// continuations, timers, offloads, or [`Context::myself`]. [`ActorRef`] is
/// a send handle; posting from `on_stop` is futile, so there is no
/// self-handle here. The absence is structural — `myself` is forwarded only
/// onto [`Context`] — and this fence is what keeps it so:
///
/// ```compile_fail,E0599
/// use shelterwood::{Actor, Context, ExitError, ExitResult, StopContext};
///
/// struct Teardown;
///
/// impl Actor for Teardown {
///     type Msg = ();
///     type Args = ();
///
///     async fn init((): Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
///         Ok(Self)
///     }
///
///     async fn handle(&mut self, (): Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
///         Ok(())
///     }
///
///     async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
///         let _self_handle = context.myself();
///     }
/// }
/// ```
///
/// A `compile_fail` fence passes for *any* compilation error, so the same
/// actor compiles here with the one supported teardown identity in place of
/// that call — which is what isolates the fence above to the missing method:
///
/// ```
/// use shelterwood::{Actor, Context, ExitError, ExitResult, Membership, StopContext};
///
/// struct Teardown;
///
/// impl Actor for Teardown {
///     type Msg = ();
///     type Args = ();
///
///     async fn init((): Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
///         Ok(Self)
///     }
///
///     async fn handle(&mut self, (): Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
///         Ok(())
///     }
///
///     async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
///         // Process-wide unique, so it keys a registry no `ActorRef` needs
///         // to have been captured for.
///         let _key: Membership = context.incarnation().membership();
///     }
/// }
/// ```
///
/// Capture [`Context::myself`] during [`Actor::init`] or [`Actor::handle`]
/// only when teardown needs the handle itself.
///
/// What remains is the common surface: identity ([`StopContext::id`],
/// [`StopContext::incarnation`], [`StopContext::scope`]), the
/// [`shutdown`](StopContext::shutdown_token) and
/// [`abort`](StopContext::abort_token) tokens,
/// [`StopContext::request_scope_shutdown`], and
/// [`StopContext::run_blocking`] — the one resource operation teardown code
/// may still need.
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

    context_common_forwarders!();

    /// Re-enters a same-message actor with the same narrowed stop context.
    pub fn for_actor<B: Actor<Msg = A::Msg>>(&mut self) -> StopContext<'_, B> {
        StopContext {
            raw: &mut *self.raw,
            actor: PhantomData,
        }
    }
}

/// Raw-actor wrapper that owns the `Uninit(args) -> Running(actor)` transition.
///
/// This is the composition point for raw decorators around callback-oriented
/// actors. It also encapsulates the callback loop's error-path resource
/// teardown, so decorators need no framework-internal teardown operations;
/// the post-error context capabilities a resuming decorator may rely on are
/// documented on this type's [`RawActor::run`] implementation below.
/// Its declared readiness is read before [`RawActor::run`] is polled.
///
/// Like every [`RawActor`], one `Handler` value owns exactly one call to
/// [`RawActor::run`]. The call consumes its `Uninit` state even when
/// initialization returns an error or its future is cancelled; a second call
/// is a contract violation and panics.
pub struct Handler<A: Actor> {
    state: HandlerState<A>,
}

enum HandlerState<A: Actor> {
    Uninit(A::Args),
    Running(A),
    // The one-run sentinel. Initialization moves the arguments out and leaves
    // `Spent` behind, so every path that returns without constructing an
    // actor is already observably spent and a second run has exactly one
    // panic site to reach.
    Spent,
}

impl<A: Actor> Handler<A> {
    /// Wraps owned args using the callback-actor default `AfterInit` readiness.
    pub fn new(args: A::Args) -> Self {
        Self {
            state: HandlerState::Uninit(args),
        }
    }
}

impl<A: Actor> RawActor for Handler<A> {
    type Msg = A::Msg;

    fn readiness() -> Readiness {
        Readiness::AfterInit
    }

    /// Runs one incarnation, leaning on the raw incarnation boundary for the
    /// panic machinery: a callback panic unwinds straight through this frame,
    /// and the raw runner freezes the mailbox and incarnation resources,
    /// joins them, and only then drops this handler (§6.5's
    /// resource-before-actor order), preserving the panic as the
    /// authoritative exit. Storing the actor in `self` rather than a frame
    /// local is what keeps its drop after that join on the `Err` and panic
    /// paths. Callback *errors* cannot lean on that boundary the same way:
    /// this wrapper is the advertised raw-decorator composition point, so a
    /// decorator — not the raw runner — may resume after this future
    /// returns, and it must not observe still-live incarnation resources;
    /// every error exit therefore freezes and joins them here first. That
    /// half-teardown touches only the resource half: it leaves the mailbox
    /// binding exactly as this loop left it — still accepting when the error
    /// preceded any intake freeze, already frozen when it was raised during
    /// frozen-prefix drain or after a `Context::stop` request, since
    /// [`RawContext::stop`] and the [`recv`](RawContext::recv) /
    /// [`try_recv`](RawContext::try_recv) shutdown boundary freeze intake
    /// themselves. Either way the decorator finds the resource capabilities
    /// already rejected. Resuming the raw loop past this point is
    /// unsupported, and unenforced beyond a second call to this method
    /// panicking: receives keep delivering whatever the mailbox state still
    /// allows, while `continue_with`, `set_timeout`, `set_interval` and
    /// `offload` silently return `Rejected` instead of diagnosing. Once the
    /// decorated raw stack returns, the raw incarnation boundary freezes the
    /// mailbox before joining resources and destroying the context and
    /// actor, which is where acceptance closes for an incarnation that ends
    /// without a stop phase (§5.4).
    async fn run(&mut self, raw: &mut RawContext<Self::Msg>) -> ExitResult {
        let HandlerState::Uninit(args) = std::mem::replace(&mut self.state, HandlerState::Spent)
        else {
            panic!("handler actor initialization invoked more than once");
        };
        let initialized = {
            let mut context = Context::<A>::new_initializing(raw);
            let initialized = A::init(args, &mut context).await;
            context.initialization_returned();
            initialized
        };
        match initialized {
            Ok(actor) => self.state = HandlerState::Running(actor),
            Err(error) => {
                raw.finish_callback_init(false);
                return fail_after_teardown(raw, error).await;
            }
        }
        let HandlerState::Running(actor) = &mut self.state else {
            unreachable!("successful initialization installs the running actor")
        };
        raw.finish_callback_init(true);

        while let Some(message) = raw.recv().await {
            let handled = {
                let mut context = Context::<A>::new_live(raw);
                actor.handle(message, &mut context).await
            };
            if let Err(error) = handled {
                return fail_after_teardown(raw, error).await;
            }
        }

        match raw.mailbox_shutdown() {
            MailboxShutdown::Drain => {
                while let Some(message) = raw.try_recv() {
                    let handled = {
                        let mut context = Context::<A>::new_draining(raw);
                        actor.handle(message, &mut context).await
                    };
                    if let Err(error) = handled {
                        return fail_after_teardown(raw, error).await;
                    }
                }
            }
            // The raw-loop contract assigns disposal of the frozen prefix to
            // the framework. Returning without draining keeps hostile message
            // destructors off the actor task and out of its exit verdict.
            MailboxShutdown::Discard => {}
        }

        let mut context = StopContext::<A>::new(raw);
        actor.on_stop(&mut context).await;
        Ok(())
    }
}

/// Propagates a callback error after §6.5's orderly teardown: incarnation-owned
/// work is frozen, cancelled, and joined before control returns to the caller,
/// which at the advertised composition point may be a raw decorator rather
/// than the raw incarnation boundary itself. This helper touches only the
/// resource half, leaving the mailbox binding as it found it: still accepting
/// when the error preceded any intake freeze, already frozen when it was
/// raised during frozen-prefix drain or after a stop request. The outer raw
/// incarnation boundary freezes the mailbox when the decorated stack returns.
/// Returning to the loop after this resource-only teardown is unsupported.
async fn fail_after_teardown<M: Send + 'static>(
    raw: &mut RawContext<M>,
    error: ExitError,
) -> ExitResult {
    raw.freeze_resources();
    raw.join_resources().await;
    Err(error)
}

type ArgsFactory<A> = Box<dyn Fn() -> <A as Actor>::Args + Send + Sync + 'static>;

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
    ///
    /// The retained restartable source is shared, so the captured args must be
    /// [`Sync`] even though each clone is made by one incarnation at a time.
    pub fn cloned(args: A::Args) -> Self
    where
        A::Args: Clone + Sync,
    {
        Self::factory(move || args.clone())
    }

    /// Creates a restartable definition from a fresh per-incarnation args source.
    pub fn factory(factory: impl Fn() -> A::Args + Send + Sync + 'static) -> Self {
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
        actor_readiness,
        structural_readiness_deadline,
        retention,
    );

    pub(crate) fn into_raw(self) -> RawDef<Handler<A>> {
        let factory = self.factory;
        let mut raw = RawDef::factory(move || {
            let args = factory();
            Handler::new(args)
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

    common_options_setters!(
        shutdown,
        mailbox,
        mailbox_shutdown,
        actor_readiness,
        structural_readiness_deadline,
        retention,
    );

    pub(crate) fn into_raw(self) -> RawOnceDef<Handler<A>> {
        let mut raw = RawOnceDef::new(Handler::new(self.args));
        raw.options = self.options;
        raw
    }
}
