use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{POLL_TIMEOUT, ReleaseGate, assert_quiet, poll_until};
use shelterwood::{
    Actor, ActorDef, ActorOnceDef, ActorRef, Context, DynamicTree, ExitError, ExitResult, Handler,
    Mailbox, RawActor, RawContext, RawOnceDef, Readiness, ScopeRef, SendErrorKind, StopContext,
    TaskDef, Tree,
};

#[derive(Clone)]
struct BasicArgs {
    events: Arc<Mutex<Vec<&'static str>>>,
}

enum BasicMessage {
    Stop,
}

struct BasicActor {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for BasicActor {
    type Msg = BasicMessage;
    type Args = BasicArgs;

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        assert!(!context.is_draining());
        args.events
            .lock()
            .expect("events mutex poisoned")
            .push("init");
        Ok(Self {
            events: args.events,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            BasicMessage::Stop => {
                self.events
                    .lock()
                    .expect("events mutex poisoned")
                    .push("handle");
                context.stop();
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
        assert_eq!(context.id().as_str(), "actor");
        self.events
            .lock()
            .expect("events mutex poisoned")
            .push("stop");
    }
}

struct Audited<A: Actor> {
    inner: A,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl<A: Actor> Actor for Audited<A> {
    type Msg = A::Msg;
    type Args = (A::Args, Arc<Mutex<Vec<&'static str>>>);

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.1
            .lock()
            .expect("events mutex poisoned")
            .push("audit-init");
        let inner = A::init(args.0, &mut context.for_actor::<A>()).await?;
        Ok(Self {
            inner,
            events: args.1,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        self.events
            .lock()
            .expect("events mutex poisoned")
            .push("audit-handle");
        self.inner
            .handle(message, &mut context.for_actor::<A>())
            .await
    }

    async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
        self.events
            .lock()
            .expect("events mutex poisoned")
            .push("audit-stop");
        self.inner.on_stop(&mut context.for_actor::<A>()).await;
    }
}

#[tokio::test]
async fn handler_actor_runs_init_handle_and_stop_through_the_raw_wrapper() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<BasicActor>::new(BasicArgs {
                events: Arc::clone(&events),
            }),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor initializes");
    actor.send(BasicMessage::Stop).await.expect("actor is live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *events.lock().expect("events mutex poisoned"),
        ["init", "handle", "stop"]
    );
}

#[tokio::test]
async fn handler_decorator_reenters_the_inner_actor_context_across_callbacks() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<Audited<BasicActor>>::new((
                BasicArgs {
                    events: Arc::clone(&events),
                },
                Arc::clone(&events),
            )),
        )
        .expect("valid decorated actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor initializes");
    actor.send(BasicMessage::Stop).await.expect("actor is live");
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    assert_eq!(
        *events.lock().expect("events mutex poisoned"),
        [
            "audit-init",
            "init",
            "audit-handle",
            "handle",
            "audit-stop",
            "stop"
        ]
    );
}

struct ManualActor;

impl Actor for ManualActor {
    type Msg = ();
    type Args = ReleaseGate;

    async fn init(entered: ReleaseGate, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        entered.release();
        Ok(Self)
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        context.mark_ready();
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn manual_handler_readiness_is_not_released_automatically_after_init() {
    let init_entered = ReleaseGate::default();
    let sibling_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "actor",
            ActorOnceDef::<ManualActor>::new(init_entered.clone()).readiness(Readiness::Manual),
        )
        .expect("valid actor");
    let observed = Arc::clone(&sibling_started);
    let _sibling = tree
        .add_task_once(
            "sibling",
            shelterwood::TaskOnceDef::new(move |_| async move {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            }),
        )
        .expect("valid sibling");
    let system = tree.spawn().expect("runtime is available");
    init_entered.wait().await;
    assert_quiet(Duration::from_millis(20), || {
        sibling_started.load(Ordering::SeqCst)
    })
    .await;
    actor
        .send(())
        .await
        .expect("mailbox accepts during startup");
    system.wait_started().await.expect("manual mark opens gate");
    assert!(sibling_started.load(Ordering::SeqCst));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

struct GatedActor;

impl Actor for GatedActor {
    type Msg = ();
    type Args = ReleaseGate;

    async fn init(gate: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        gate.wait().await;
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct AwaitingDecorator<R> {
    inner: R,
    entered: ReleaseGate,
    release: ReleaseGate,
}

impl<R: RawActor> RawActor for AwaitingDecorator<R> {
    type Msg = R::Msg;

    fn readiness(&self) -> Readiness {
        self.inner.readiness()
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        self.entered.release();
        self.release.wait().await;
        self.inner.run(context).await
    }
}

#[tokio::test(start_paused = true)]
async fn raw_decorator_await_before_handler_delegate_preserves_declared_readiness() {
    let decorator_entered = ReleaseGate::default();
    let release_decorator = ReleaseGate::default();
    let release_init = ReleaseGate::default();
    let sibling_started = Arc::new(AtomicBool::new(false));
    let mut tree = Tree::new();
    tree.add_raw_once(
        "actor",
        RawOnceDef::new(AwaitingDecorator {
            inner: Handler::<GatedActor>::new(release_init.clone()),
            entered: decorator_entered.clone(),
            release: release_decorator.clone(),
        }),
    )
    .expect("valid decorated actor");
    let observed = Arc::clone(&sibling_started);
    tree.add_task(
        "sibling",
        TaskDef::new(move |context| {
            let observed = Arc::clone(&observed);
            async move {
                observed.store(true, Ordering::SeqCst);
                context.shutdown_token().cancelled().await;
                Ok(())
            }
        }),
    )
    .expect("valid sibling");
    let system = tree.spawn().expect("runtime is available");
    decorator_entered.wait().await;
    assert_quiet(Duration::from_millis(20), || {
        sibling_started.load(Ordering::SeqCst)
    })
    .await;
    release_decorator.release();
    release_init.release();
    system
        .wait_started()
        .await
        .expect("decorated actor becomes ready");
    assert!(sibling_started.load(Ordering::SeqCst));
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test]
async fn restartable_and_dynamic_actor_definition_surfaces_work() {
    let inits = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut tree = DynamicTree::new();
    let count = Arc::clone(&inits);
    let events_for_args = Arc::clone(&events);
    tree.add_actor(
        "initial",
        ActorDef::<BasicActor>::factory(move || {
            count.fetch_add(1, Ordering::SeqCst);
            BasicArgs {
                events: Arc::clone(&events_for_args),
            }
        }),
    )
    .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("initial actor starts");
    assert_eq!(inits.load(Ordering::SeqCst), 1);

    let dynamic_events = Arc::new(Mutex::new(Vec::new()));
    let dynamic = system.scope();
    let actor = dynamic
        .add_actor_once(
            "dynamic",
            ActorOnceDef::<BasicActor>::new(BasicArgs {
                events: Arc::clone(&dynamic_events),
            }),
        )
        .await
        .expect("dynamic actor admitted");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            dynamic_events
                .lock()
                .expect("events mutex poisoned")
                .contains(&"init")
        })
        .await
    );
    actor
        .send(BasicMessage::Stop)
        .await
        .expect("dynamic actor live");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

struct ResumeProbeDecorator<R> {
    inner: R,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl<R: RawActor> RawActor for ResumeProbeDecorator<R> {
    type Msg = R::Msg;

    fn readiness(&self) -> Readiness {
        self.inner.readiness()
    }

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        let result = self.inner.run(context).await;
        self.log
            .lock()
            .expect("teardown log mutex poisoned")
            .push("decorator-resumed");
        result
    }
}

struct TeardownDropLog {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for TeardownDropLog {
    fn drop(&mut self) {
        self.log
            .lock()
            .expect("teardown log mutex poisoned")
            .push("offload-destroyed");
    }
}

enum OffloadThenFailMessage {
    Start,
}

struct OffloadThenFailActor {
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl Actor for OffloadThenFailActor {
    type Msg = OffloadThenFailMessage;
    type Args = Arc<Mutex<Vec<&'static str>>>;

    async fn init(log: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { log })
    }

    async fn handle(
        &mut self,
        OffloadThenFailMessage::Start: Self::Msg,
        context: &mut Context<'_, Self>,
    ) -> ExitResult {
        let guard = TeardownDropLog {
            log: Arc::clone(&self.log),
        };
        context
            .offload(
                async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                },
                |_| OffloadThenFailMessage::Start,
                Duration::MAX,
            )
            .expect("live offload accepted");
        Err(ExitError::message("injected handler failure"))
    }
}

/// `Handler` is the advertised raw-decorator composition point, so the raw
/// incarnation boundary is not necessarily its immediate caller: a callback
/// error must freeze and join incarnation-owned work inside `Handler::run`,
/// before a decorator that resumes after delegating can observe still-live
/// offloads.
#[tokio::test]
async fn handler_error_joins_offloads_before_returning_to_a_raw_decorator() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "erroring",
            RawOnceDef::new(ResumeProbeDecorator {
                inner: Handler::<OffloadThenFailActor>::new(Arc::clone(&log)),
                log: Arc::clone(&log),
            }),
        )
        .expect("valid decorated actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");
    actor
        .send(OffloadThenFailMessage::Start)
        .await
        .expect("actor live");
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            log.lock().expect("teardown log mutex poisoned").len() == 2
        })
        .await
    );
    assert_eq!(
        *log.lock().expect("teardown log mutex poisoned"),
        ["offload-destroyed", "decorator-resumed"]
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

struct InertActor;

impl Actor for InertActor {
    type Msg = ();
    type Args = ReleaseGate;

    async fn init(entered: ReleaseGate, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        entered.release();
        Ok(Self)
    }

    async fn handle(&mut self, (): Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        context.mark_ready();
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn manual_readiness_override_on_a_wrapped_handler_stays_gated() {
    let init_entered = ReleaseGate::default();
    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "gated",
            RawOnceDef::new(Handler::<InertActor>::new(init_entered.clone()))
                .readiness(Readiness::Manual)
                .expect("manual readiness override"),
        )
        .expect("valid raw actor");
    let system = tree.spawn().expect("runtime is available");
    init_entered.wait().await;
    // The engine gates on the Manual override; the blanket handler loop must
    // consult the same resolved mode instead of auto-firing after init.
    let mut started = Box::pin(system.wait_started());
    assert_quiet(Duration::from_millis(50), || {
        crate::common::poll_once(started.as_mut()).is_ready()
    })
    .await;
    drop(started);
    actor.send(()).await.expect("gated actor accepts messages");
    system
        .wait_started()
        .await
        .expect("an explicit mark_ready releases the gate");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

enum ContextSurfaceMessage {
    Enter,
    Queued,
    SelfSent,
}

struct ContextSurfaceActor {
    entered: ReleaseGate,
    release: ReleaseGate,
    observed: Option<tokio::sync::oneshot::Sender<(ActorRef<ContextSurfaceMessage>, ScopeRef)>>,
    stopped: Option<tokio::sync::oneshot::Sender<(ActorRef<ContextSurfaceMessage>, ScopeRef)>>,
}

impl Actor for ContextSurfaceActor {
    type Msg = ContextSurfaceMessage;
    type Args = (
        ReleaseGate,
        ReleaseGate,
        tokio::sync::oneshot::Sender<(ActorRef<ContextSurfaceMessage>, ScopeRef)>,
        tokio::sync::oneshot::Sender<(ActorRef<ContextSurfaceMessage>, ScopeRef)>,
    );

    async fn init(
        (entered, release, observed, stopped): Self::Args,
        _: &mut Context<'_, Self>,
    ) -> Result<Self, ExitError> {
        Ok(Self {
            entered,
            release,
            observed: Some(observed),
            stopped: Some(stopped),
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ContextSurfaceMessage::Enter => {
                self.entered.release();
                self.release.wait().await;

                let myself: ActorRef<ContextSurfaceMessage> = context.myself();
                let scope: ScopeRef = context.scope();
                let error = myself
                    .try_send(ContextSurfaceMessage::SelfSent)
                    .expect_err("the one-slot mailbox is already full");
                assert_eq!(error.kind, SendErrorKind::Full);
                assert!(matches!(error.message, ContextSurfaceMessage::SelfSent));
                self.observed
                    .take()
                    .expect("surface observation is sent once")
                    .send((myself, scope))
                    .expect("test still awaits typed context handles");
            }
            ContextSurfaceMessage::Queued => context.stop(),
            ContextSurfaceMessage::SelfSent => {
                panic!("a self-send rejected from the full mailbox must not be delivered")
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
        let myself: ActorRef<ContextSurfaceMessage> = context.myself();
        let scope: ScopeRef = context.scope();
        self.stopped
            .take()
            .expect("stop-context observation is sent once")
            .send((myself, scope))
            .expect("test still awaits typed stop-context handles");
    }
}

#[tokio::test]
async fn typed_context_handles_preserve_identity_and_self_send_capacity() {
    let entered = ReleaseGate::default();
    let release = ReleaseGate::default();
    let (observed, observation) = tokio::sync::oneshot::channel();
    let (stopped, stop_observation) = tokio::sync::oneshot::channel();
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once(
            "context-surface",
            ActorOnceDef::<ContextSurfaceActor>::new((
                entered.clone(),
                release.clone(),
                observed,
                stopped,
            ))
            .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid actor");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("actor starts");

    actor
        .send(ContextSurfaceMessage::Enter)
        .await
        .expect("actor enters handler");
    entered.wait().await;
    actor
        .try_send(ContextSurfaceMessage::Queued)
        .expect("the sole pending slot is filled");
    release.release();

    let (myself, scope) = observation.await.expect("actor reports its handles");
    assert_eq!(myself, actor);
    assert_eq!(scope, system.scope());
    assert_eq!(system.wait().await, shelterwood::StopReason::Finished);
    let (stopping_myself, stopping_scope) = stop_observation
        .await
        .expect("actor reports stop-context handles");
    assert_eq!(stopping_myself, actor);
    assert_eq!(stopping_scope, scope);
}

struct HandlerScopeQuitter;

impl Actor for HandlerScopeQuitter {
    type Msg = ();
    type Args = ();

    async fn init((): (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        context.request_scope_shutdown();
        Ok(())
    }
}

/// `Context::request_scope_shutdown` from a live handler drains the
/// supervising scope as `ShutdownRequested`. The parked sibling only exits
/// under the shutdown token, so the recorded stop reason proves the
/// request — not natural completion — ended the tree.
#[tokio::test]
async fn handler_context_scope_shutdown_request_stops_the_tree() {
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once("quitter", ActorOnceDef::<HandlerScopeQuitter>::new(()))
        .expect("valid actor");
    tree.add_task(
        "parked",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        }),
    )
    .expect("valid parked task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    actor.send(()).await.expect("quitter is live");
    assert_eq!(
        system.wait().await,
        shelterwood::StopReason::ShutdownRequested
    );
}

struct StopEscalatingActor;

impl Actor for StopEscalatingActor {
    type Msg = ();
    type Args = ();

    async fn init((): (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        context.stop();
        Ok(())
    }

    async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
        context.request_scope_shutdown();
    }
}

/// `StopContext::request_scope_shutdown` still reaches the supervising
/// scope: a clean local stop escalates from teardown into a whole-scope
/// shutdown that also releases the parked sibling.
#[tokio::test]
async fn stop_context_scope_shutdown_request_escalates_a_local_stop() {
    let mut tree = Tree::new();
    let actor = tree
        .add_actor_once("escalating", ActorOnceDef::<StopEscalatingActor>::new(()))
        .expect("valid actor");
    tree.add_task(
        "parked",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        }),
    )
    .expect("valid parked task");
    let system = tree.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    actor.send(()).await.expect("escalating actor is live");
    assert_eq!(
        system.wait().await,
        shelterwood::StopReason::ShutdownRequested
    );
}
