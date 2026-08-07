use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorOnceDef, ActorRef, Context, DynamicTree, ExitError, ExitResult, RemoveOutcome,
    Reply, ReserveError, StopContext, SubtreeOnceDef, Tree,
};
use shelterwood_test_support::{ReleaseGate, poll_until};

enum ShardMessage {
    Put,
}

struct ShardActor {
    puts: Arc<AtomicUsize>,
}

impl Actor for ShardActor {
    type Msg = ShardMessage;
    type Args = (Arc<AtomicBool>, Arc<AtomicUsize>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.store(true, Ordering::SeqCst);
        Ok(Self { puts: args.1 })
    }

    async fn handle(
        &mut self,
        ShardMessage::Put: Self::Msg,
        _: &mut Context<'_, Self>,
    ) -> ExitResult {
        self.puts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

enum DirectoryMessage {
    Cutover {
        shard: ActorRef<ShardMessage>,
        reply: Reply<()>,
    },
    Lookup {
        reply: Reply<Option<ActorRef<ShardMessage>>>,
    },
}

struct DirectoryActor {
    shard: Option<ActorRef<ShardMessage>>,
}

impl Actor for DirectoryActor {
    type Msg = DirectoryMessage;
    type Args = ();

    async fn init(_: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { shard: None })
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        match message {
            DirectoryMessage::Cutover { shard, reply } => {
                self.shard = Some(shard);
                reply.send(());
            }
            DirectoryMessage::Lookup { reply } => reply.send(self.shard.clone()),
        }
        Ok(())
    }
}

#[tokio::test]
async fn shard_store_spike_mounts_cuts_over_and_retires_by_exact_handle() {
    let mut root = Tree::new();
    let directory = root
        .add_actor_once("directory", ActorOnceDef::<DirectoryActor>::new(()))
        .expect("valid directory");
    let ranges = root
        .add_subtree_once("ranges", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("valid range scope");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("store starts");

    let first_ready = Arc::new(AtomicBool::new(false));
    let first_puts = Arc::new(AtomicUsize::new(0));
    let first = ranges
        .add_actor_once(
            "range-v1",
            ActorOnceDef::<ShardActor>::new((Arc::clone(&first_ready), Arc::clone(&first_puts))),
        )
        .await
        .expect("first shard admitted")
        .into_handles();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            first_ready.load(Ordering::SeqCst)
        })
        .await
    );
    let first_route = first.clone();
    directory
        .call(
            move |reply| DirectoryMessage::Cutover {
                shard: first_route,
                reply,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("first cutover acknowledged");

    let second_ready = Arc::new(AtomicBool::new(false));
    let second_puts = Arc::new(AtomicUsize::new(0));
    let second = ranges
        .add_actor_once(
            "range-v2",
            ActorOnceDef::<ShardActor>::new((Arc::clone(&second_ready), Arc::clone(&second_puts))),
        )
        .await
        .expect("replacement shard admitted")
        .into_handles();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            second_ready.load(Ordering::SeqCst)
        })
        .await
    );
    let second_route = second.clone();
    directory
        .call(
            move |reply| DirectoryMessage::Cutover {
                shard: second_route,
                reply,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("replacement cutover acknowledged");
    assert_eq!(ranges.remove_actor(&first).await, RemoveOutcome::Removed);

    let route = directory
        .call(
            |reply| DirectoryMessage::Lookup { reply },
            Duration::from_secs(1),
        )
        .await
        .expect("directory lookup replies")
        .value
        .expect("route installed");
    assert_eq!(route.membership(), second.membership());
    route
        .send(ShardMessage::Put)
        .await
        .expect("new route is live");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            second_puts.load(Ordering::SeqCst) == 1
        })
        .await
    );
    assert_eq!(first_puts.load(Ordering::SeqCst), 0);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("store shuts down");
}

struct Audited<A: Actor> {
    inner: A,
    callbacks: Arc<Mutex<Vec<&'static str>>>,
}

impl<A: Actor> Actor for Audited<A> {
    type Msg = A::Msg;
    type Args = (A::Args, Arc<Mutex<Vec<&'static str>>>);

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.1
            .lock()
            .expect("callback log mutex poisoned")
            .push("init");
        let inner = A::init(args.0, &mut context.for_actor::<A>()).await?;
        Ok(Self {
            inner,
            callbacks: args.1,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        self.callbacks
            .lock()
            .expect("callback log mutex poisoned")
            .push("handle");
        self.inner
            .handle(message, &mut context.for_actor::<A>())
            .await
    }

    async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
        self.callbacks
            .lock()
            .expect("callback log mutex poisoned")
            .push("stop");
        self.inner.on_stop(&mut context.for_actor::<A>()).await;
    }
}

struct BridgeArgs {
    init_gate: ReleaseGate,
    initialized: Arc<AtomicBool>,
    stop_entered: Arc<AtomicBool>,
    stop_gate: ReleaseGate,
}

struct BridgeActor {
    stop_entered: Arc<AtomicBool>,
    stop_gate: ReleaseGate,
}

impl Actor for BridgeActor {
    type Msg = ();
    type Args = BridgeArgs;

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        assert_eq!(context.id().as_str(), "bridge");
        args.init_gate.wait().await;
        args.initialized.store(true, Ordering::SeqCst);
        Ok(Self {
            stop_entered: args.stop_entered,
            stop_gate: args.stop_gate,
        })
    }

    async fn handle(&mut self, (): (), context: &mut Context<'_, Self>) -> ExitResult {
        assert_eq!(context.id().as_str(), "bridge");
        Ok(())
    }

    async fn on_stop(&mut self, context: &mut StopContext<'_, Self>) {
        assert_eq!(context.id().as_str(), "bridge");
        self.stop_entered.store(true, Ordering::SeqCst);
        self.stop_gate.wait().await;
    }
}

struct SessionFixture {
    tree: DynamicTree,
    init_gate: ReleaseGate,
    initialized: Arc<AtomicBool>,
    stop_entered: Arc<AtomicBool>,
    stop_gate: ReleaseGate,
    callbacks: Arc<Mutex<Vec<&'static str>>>,
}

fn session_fixture() -> SessionFixture {
    let init_gate = ReleaseGate::default();
    let initialized = Arc::new(AtomicBool::new(false));
    let stop_entered = Arc::new(AtomicBool::new(false));
    let stop_gate = ReleaseGate::default();
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let mut tree = DynamicTree::new();
    tree.add_subtree_once("tools", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("valid nested tools scope");
    tree.add_actor_once(
        "bridge",
        ActorOnceDef::<Audited<BridgeActor>>::new((
            BridgeArgs {
                init_gate: init_gate.clone(),
                initialized: Arc::clone(&initialized),
                stop_entered: Arc::clone(&stop_entered),
                stop_gate: stop_gate.clone(),
            },
            Arc::clone(&callbacks),
        )),
    )
    .expect("valid bridge");
    SessionFixture {
        tree,
        init_gate,
        initialized,
        stop_entered,
        stop_gate,
        callbacks,
    }
}

#[tokio::test]
async fn assistant_control_plane_spike_composes_nested_dynamic_scopes_and_reentry() {
    let mut root = Tree::new();
    let sessions = root
        .add_subtree_once("sessions", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("valid session scope");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("control plane starts");

    let first = session_fixture();
    let first_init = first.init_gate.clone();
    let first_initialized = Arc::clone(&first.initialized);
    let first_stop_entered = Arc::clone(&first.stop_entered);
    let first_stop = first.stop_gate.clone();
    let first_callbacks = Arc::clone(&first.callbacks);
    let session = sessions
        .add_subtree_once("session", SubtreeOnceDef::new(first.tree))
        .await
        .expect("session admitted before bridge readiness")
        .into_handles();
    assert!(!first_initialized.load(Ordering::SeqCst));
    first_init.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            first_initialized.load(Ordering::SeqCst)
        })
        .await
    );

    let removal = sessions.remove_dynamic_scope(&session);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            first_stop_entered.load(Ordering::SeqCst)
        })
        .await
    );
    let racing = session_fixture();
    let error = sessions
        .add_subtree_once("session", SubtreeOnceDef::new(racing.tree))
        .await
        .expect_err("same id cannot pass an in-progress exact removal");
    assert!(matches!(error, ReserveError::RemovalInProgress(id) if id.as_str() == "session"));

    first_stop.release();
    assert_eq!(removal.await, RemoveOutcome::Removed);
    assert_eq!(
        *first_callbacks.lock().expect("callback log mutex poisoned"),
        ["init", "stop"]
    );

    let replacement = session_fixture();
    let replacement_init = replacement.init_gate.clone();
    let replacement_stop = replacement.stop_gate.clone();
    let replacement_initialized = Arc::clone(&replacement.initialized);
    sessions
        .add_subtree_once("session", SubtreeOnceDef::new(replacement.tree))
        .await
        .expect("same id is free after exact removal");
    replacement_init.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            replacement_initialized.load(Ordering::SeqCst)
        })
        .await
    );
    replacement_stop.release();
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("control plane shuts down");
}
