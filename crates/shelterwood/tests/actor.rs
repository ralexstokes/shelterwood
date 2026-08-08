use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{ReleaseGate, assert_quiet, poll_until};
use shelterwood::{
    Actor, ActorDef, ActorOnceDef, Context, DynamicTree, ExitError, ExitResult, Handler, RawActor,
    RawContext, RawOnceDef, Readiness, StopContext, TaskDef, Tree,
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
        .expect("dynamic actor admitted")
        .into_handles();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
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
