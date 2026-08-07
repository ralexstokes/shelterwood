use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorDef, ActorOnceDef, ActorRef, ChildState, Context, DeadlineElapsed, DynamicScopeRef,
    DynamicTree, ExitError, ExitResult, LifecycleEvent, LifecycleEventKind, LifecycleEvents,
    LifecycleItem, Mailbox, Membership, RemoveOutcome, Reply, ReserveError, RestartPolicy,
    ScopeState, StopContext, SubtreeOnceDef, Tree,
};
use shelterwood_test_support::{ReleaseGate, poll_until};

async fn next_event(events: &mut LifecycleEvents) -> LifecycleEvent {
    loop {
        let item = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("lifecycle wait is bounded")
            .expect("lifecycle stream remains open");
        if let LifecycleItem::Event(event) = item {
            return event;
        }
    }
}

#[derive(Clone, Default)]
struct TransportState {
    delivered: Arc<Mutex<HashSet<u64>>>,
    processed: Arc<AtomicUsize>,
    duplicate_notices: Arc<AtomicUsize>,
    fail_once: Arc<AtomicBool>,
}

enum IngressMessage {
    Deliver { id: u64, reply: Reply<()> },
    DuplicateObserved,
}

enum JournalMessage {
    Persist { id: u64, reply: Reply<()> },
}

#[derive(Clone)]
struct IngressArgs {
    journal: ActorRef<JournalMessage>,
    duplicate_notices: Arc<AtomicUsize>,
}

struct IngressActor(IngressArgs);

impl Actor for IngressActor {
    type Msg = IngressMessage;
    type Args = IngressArgs;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        match message {
            IngressMessage::Deliver { id, reply } => {
                self.0
                    .journal
                    .call(
                        move |journal_reply| JournalMessage::Persist {
                            id,
                            reply: journal_reply,
                        },
                        Duration::from_secs(1),
                    )
                    .await
                    .map_err(|error| {
                        ExitError::message(format!("journal acknowledgement failed: {error}"))
                    })?;
                reply.send(());
            }
            IngressMessage::DuplicateObserved => {
                self.0.duplicate_notices.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct JournalArgs {
    ingress: ActorRef<IngressMessage>,
    state: TransportState,
}

struct JournalActor(JournalArgs);

impl Actor for JournalActor {
    type Msg = JournalMessage;
    type Args = JournalArgs;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        let JournalMessage::Persist { id, reply } = message;
        let inserted = self
            .0
            .state
            .delivered
            .lock()
            .expect("transport journal mutex poisoned")
            .insert(id);
        if inserted {
            self.0.state.processed.fetch_add(1, Ordering::SeqCst);
        }
        if inserted && self.0.state.fail_once.swap(false, Ordering::SeqCst) {
            return Err(ExitError::message(
                "injected crash after durable write before acknowledgement",
            ));
        }
        if !inserted {
            let _ = self.0.ingress.try_send(IngressMessage::DuplicateObserved);
        }
        reply.send(());
        Ok(())
    }
}

enum BridgeMessage {
    Begin { reply: Reply<()> },
    Finish,
}

struct BridgeActor {
    pending: Option<Reply<()>>,
}

impl Actor for BridgeActor {
    type Msg = BridgeMessage;
    type Args = ReleaseGate;

    async fn init(gate: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        gate.wait().await;
        Ok(Self { pending: None })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            BridgeMessage::Begin { reply } => {
                self.pending = Some(reply);
                context
                    .continue_with(BridgeMessage::Finish)
                    .expect("live bridge accepts its continuation");
            }
            BridgeMessage::Finish => {
                self.pending
                    .take()
                    .expect("begin stores one pending acknowledgement")
                    .send(());
            }
        }
        Ok(())
    }
}

struct GatewayFixture {
    tree: Tree,
    ingress: ActorRef<IngressMessage>,
    bridge: ActorRef<BridgeMessage>,
    bridge_gate: ReleaseGate,
}

fn gateway(state: TransportState) -> GatewayFixture {
    let mut tree = Tree::new();
    let ingress_slot = tree
        .reserve_actor::<IngressMessage>("ingress")
        .expect("ingress slot reserved");
    let ingress = ingress_slot.actor_ref();
    let journal_slot = tree
        .reserve_actor::<JournalMessage>("journal")
        .expect("journal slot reserved");
    let journal = journal_slot.actor_ref();

    // Both refs exist before either factory is defined: cyclic wiring needs
    // no registry and no Option<ActorRef> initialization phase.
    let _defined_ingress = ingress_slot.define(ActorDef::<IngressActor>::cloned(IngressArgs {
        journal: journal.clone(),
        duplicate_notices: Arc::clone(&state.duplicate_notices),
    }));
    let _defined_journal = journal_slot.define(ActorDef::<JournalActor>::cloned(JournalArgs {
        ingress: ingress.clone(),
        state,
    }));
    let bridge_gate = ReleaseGate::default();
    let bridge = tree
        .add_actor_once(
            "bridge",
            ActorOnceDef::<BridgeActor>::new(bridge_gate.clone()),
        )
        .expect("bridge actor declared");
    GatewayFixture {
        tree,
        ingress,
        bridge,
        bridge_gate,
    }
}

#[derive(Clone)]
struct SessionControlArgs {
    crash_once: Arc<AtomicBool>,
    rehydrated: Arc<AtomicUsize>,
    stop_entered: Arc<AtomicBool>,
    stop_gate: ReleaseGate,
}

enum SessionControlMessage {
    Rehydrate,
    Crash,
}

struct SessionControlActor(SessionControlArgs);

impl Actor for SessionControlActor {
    type Msg = SessionControlMessage;
    type Args = SessionControlArgs;

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        context
            .continue_with(SessionControlMessage::Rehydrate)
            .expect("live control actor accepts rehydration");
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        match message {
            SessionControlMessage::Rehydrate => {
                self.0.rehydrated.fetch_add(1, Ordering::SeqCst);
            }
            SessionControlMessage::Crash => {
                if self.0.crash_once.swap(false, Ordering::SeqCst) {
                    panic!("injected session-control panic");
                }
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {
        self.0.stop_entered.store(true, Ordering::SeqCst);
        self.0.stop_gate.wait().await;
    }
}

struct StreamActor {
    values: Arc<Mutex<Vec<u64>>>,
}

impl Actor for StreamActor {
    type Msg = u64;
    type Args = (ReleaseGate, Arc<Mutex<Vec<u64>>>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.wait().await;
        Ok(Self { values: args.1 })
    }

    async fn handle(&mut self, value: u64, _: &mut Context<'_, Self>) -> ExitResult {
        self.values
            .lock()
            .expect("stream values mutex poisoned")
            .push(value);
        Ok(())
    }
}

#[derive(Clone)]
struct ToolArgs {
    crash_once: Arc<AtomicBool>,
    completions: Arc<AtomicUsize>,
}

enum ToolMessage {
    Crash,
    Work,
    Completed(Result<u64, DeadlineElapsed>),
}

struct ToolActor(ToolArgs);

impl Actor for ToolActor {
    type Msg = ToolMessage;
    type Args = ToolArgs;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            ToolMessage::Crash => {
                if self.0.crash_once.swap(false, Ordering::SeqCst) {
                    panic!("injected tool panic");
                }
            }
            ToolMessage::Work => {
                context
                    .offload(
                        async { 7_u64 },
                        ToolMessage::Completed,
                        Duration::from_secs(1),
                    )
                    .expect("live tool accepts incarnation-owned offload");
            }
            ToolMessage::Completed(Ok(7)) => {
                self.0.completions.fetch_add(1, Ordering::SeqCst);
            }
            ToolMessage::Completed(Ok(value)) => {
                return Err(ExitError::message(format!(
                    "unexpected offload value {value}"
                )));
            }
            ToolMessage::Completed(Err(_)) => {
                return Err(ExitError::message("tool offload unexpectedly timed out"));
            }
        }
        Ok(())
    }
}

struct SessionFixture {
    tree: Tree,
    tools: DynamicScopeRef,
    control: ActorRef<SessionControlMessage>,
    stream: ActorRef<u64>,
    stream_gate: ReleaseGate,
    stream_values: Arc<Mutex<Vec<u64>>>,
    rehydrated: Arc<AtomicUsize>,
    stop_entered: Arc<AtomicBool>,
    stop_gate: ReleaseGate,
}

fn session_fixture() -> SessionFixture {
    let tools_tree = DynamicTree::new();
    let mut tree = Tree::new();
    let tools = tree
        .add_subtree_once("tools", SubtreeOnceDef::new(tools_tree))
        .expect("nested tool scope declared");
    let stream_gate = ReleaseGate::default();
    let stream_values = Arc::new(Mutex::new(Vec::new()));
    let stream = tree
        .add_actor_once(
            "stream",
            ActorOnceDef::<StreamActor>::new((stream_gate.clone(), Arc::clone(&stream_values)))
                .mailbox(Mailbox::latest()),
        )
        .expect("stream actor declared");
    let rehydrated = Arc::new(AtomicUsize::new(0));
    let stop_entered = Arc::new(AtomicBool::new(false));
    let stop_gate = ReleaseGate::default();
    let control = tree
        .add_actor(
            "control",
            ActorDef::<SessionControlActor>::cloned(SessionControlArgs {
                crash_once: Arc::new(AtomicBool::new(true)),
                rehydrated: Arc::clone(&rehydrated),
                stop_entered: Arc::clone(&stop_entered),
                stop_gate: stop_gate.clone(),
            })
            .restart(RestartPolicy::default()),
        )
        .expect("session control declared");
    SessionFixture {
        tree,
        tools,
        control,
        stream,
        stream_gate,
        stream_values,
        rehydrated,
        stop_entered,
        stop_gate,
    }
}

fn descendant<'a>(
    snapshot: &'a shelterwood::ScopeSnapshot,
    path: &[&str],
) -> &'a shelterwood::ChildSnapshot {
    snapshot.descendant(path).expect("descendant is present")
}

fn event_is_restart_for(event: &LifecycleEvent, membership: Membership) -> bool {
    matches!(
        event.kind,
        LifecycleEventKind::RestartScheduled {
            membership: event_membership,
            ..
        } if event_membership == membership
    )
}

#[tokio::test]
async fn assistant_control_plane_composes_nested_recovery_redelivery_streaming_and_remount() {
    let transport = TransportState {
        fail_once: Arc::new(AtomicBool::new(true)),
        ..TransportState::default()
    };
    let gateway = gateway(transport.clone());
    let mut root_tree = Tree::new();
    let sessions = root_tree
        .add_subtree_once("sessions", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("session scope declared");
    let gateway_scope = root_tree
        .add_subtree_once("gateway", SubtreeOnceDef::new(gateway.tree))
        .expect("gateway scope declared");
    let system = root_tree.spawn().expect("runtime is available");
    let root = system.scope();
    let mut lifecycle = root.subscribe_lifecycle();

    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            gateway_scope
                .snapshot()
                .child("bridge")
                .is_some_and(|child| matches!(child.state, ChildState::Starting))
        })
        .await
    );
    gateway.bridge_gate.release();
    system.wait_started().await.expect("control plane starts");
    gateway
        .bridge
        .call(
            |reply| BridgeMessage::Begin { reply },
            Duration::from_secs(1),
        )
        .await
        .expect("bridge holds and later acknowledges a Reply");

    let session_data = session_fixture();
    let stream_gate = session_data.stream_gate.clone();
    let stream_values = Arc::clone(&session_data.stream_values);
    let rehydrated = Arc::clone(&session_data.rehydrated);
    let stop_entered = Arc::clone(&session_data.stop_entered);
    let stop_gate = session_data.stop_gate.clone();
    let tools = session_data.tools.clone();
    let control = session_data.control.clone();
    let stream = session_data.stream.clone();
    let session = sessions
        .add_subtree_once("session", SubtreeOnceDef::new(session_data.tree))
        .await
        .expect("session admitted")
        .into_handles();
    let session_membership = session.membership();

    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            session
                .snapshot()
                .child("stream")
                .is_some_and(|child| matches!(child.state, ChildState::Starting))
        })
        .await
    );
    stream.send(1).await.expect("first stream update accepted");
    stream.send(2).await.expect("second stream update accepted");
    stream.send(3).await.expect("third stream update accepted");
    stream_gate.release();
    sessions
        .wait_for_child(
            "session",
            |child| matches!(child.state, ChildState::Running),
            Duration::from_secs(1),
        )
        .await
        .expect("session aggregate readiness completes");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            *stream_values.lock().expect("stream values mutex poisoned") == [3]
        })
        .await,
        "latest mailbox keeps the newest accepted streaming update"
    );
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            rehydrated.load(Ordering::SeqCst) >= 1
        })
        .await
    );

    let forwarded = loop {
        let event = next_event(&mut lifecycle).await;
        if event.scope == session_membership
            && matches!(
                event.kind,
                LifecycleEventKind::ScopeState {
                    state: ScopeState::Running
                }
            )
        {
            break event;
        }
    };
    assert_eq!(
        forwarded
            .scope_path
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        ["sessions", "session"]
    );
    assert_eq!(
        descendant(&root.snapshot(), &["sessions", "session"]).membership,
        session_membership
    );

    let first_control_incarnation = control
        .send(SessionControlMessage::Crash)
        .await
        .expect("control panic request accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            session.snapshot().child("control").is_some_and(|child| {
                matches!(child.state, ChildState::Running)
                    && child.restart_count == 1
                    && child.incarnation.is_some_and(|incarnation| {
                        incarnation.supersedes(first_control_incarnation)
                    })
            })
        })
        .await,
        "session actor panic is isolated and restarted"
    );

    let tool_args = ToolArgs {
        crash_once: Arc::new(AtomicBool::new(true)),
        completions: Arc::new(AtomicUsize::new(0)),
    };
    let tool = tools
        .add_actor(
            "temporary-tool",
            ActorDef::<ToolActor>::cloned(tool_args.clone()).restart(RestartPolicy::default()),
        )
        .await
        .expect("temporary tool admitted")
        .into_handles();
    tools
        .wait_for_child(
            "temporary-tool",
            |child| matches!(child.state, ChildState::Running),
            Duration::from_secs(1),
        )
        .await
        .expect("tool becomes ready");
    let first_tool_incarnation = tool
        .send(ToolMessage::Crash)
        .await
        .expect("tool panic request accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            tools
                .snapshot()
                .child("temporary-tool")
                .is_some_and(|child| {
                    matches!(child.state, ChildState::Running)
                        && child.restart_count == 1
                        && child.incarnation.is_some_and(|incarnation| {
                            incarnation.supersedes(first_tool_incarnation)
                        })
                })
        })
        .await,
        "nested tool panic is isolated one scope level deeper"
    );
    tool.send(ToolMessage::Work)
        .await
        .expect("offload request accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            tool_args.completions.load(Ordering::SeqCst) == 1
        })
        .await,
        "incarnation-owned offload completes through the actor loop"
    );
    assert_eq!(
        tools.remove_actor(&tool).await,
        RemoveOutcome::Removed,
        "temporary children retire by exact handle"
    );

    let first_delivery = gateway
        .ingress
        .call(
            |reply| IngressMessage::Deliver { id: 7, reply },
            Duration::from_secs(1),
        )
        .await
        .expect_err("durable write crash loses the first acknowledgement");
    assert_eq!(
        first_delivery.kind,
        shelterwood::CallErrorKind::ReplyDropped
    );
    gateway
        .ingress
        .call(
            |reply| IngressMessage::Deliver { id: 7, reply },
            Duration::from_secs(1),
        )
        .await
        .expect("redelivery of the same journal id is acknowledged");
    assert_eq!(transport.processed.load(Ordering::SeqCst), 1);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            transport.duplicate_notices.load(Ordering::SeqCst) == 1
        })
        .await
    );

    let mut saw_control_restart = false;
    let mut saw_tool_restart = false;
    let mut saw_gateway_restart = false;
    while let Ok(item) = lifecycle.try_recv() {
        let LifecycleItem::Event(event) = item else {
            continue;
        };
        saw_control_restart |= event_is_restart_for(&event, control.membership());
        saw_tool_restart |= event_is_restart_for(&event, tool.membership());
        saw_gateway_restart |= event_is_restart_for(&event, gateway.ingress.membership());
    }
    assert!(saw_control_restart && saw_tool_restart && saw_gateway_restart);

    let removal = sessions.remove_scope(&session);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            stop_entered.load(Ordering::SeqCst)
        })
        .await
    );
    let racing = session_fixture();
    racing.stream_gate.release();
    racing.stop_gate.release();
    let error = sessions
        .add_subtree_once("session", SubtreeOnceDef::new(racing.tree))
        .await
        .expect_err("same id is fenced while exact removal is in progress");
    assert!(matches!(
        error,
        ReserveError::RemovalInProgress(id) if id.as_str() == "session"
    ));
    stop_gate.release();
    assert_eq!(removal.await, RemoveOutcome::Removed);

    let replacement_data = session_fixture();
    replacement_data.stream_gate.release();
    let replacement_stop = replacement_data.stop_gate.clone();
    let replacement = sessions
        .add_subtree_once("session", SubtreeOnceDef::new(replacement_data.tree))
        .await
        .expect("same id is reusable after removal completes")
        .into_handles();
    assert!(replacement.membership().supersedes(session_membership));
    sessions
        .wait_for_child(
            "session",
            |child| matches!(child.state, ChildState::Running),
            Duration::from_secs(1),
        )
        .await
        .expect("replacement session becomes ready");
    let replacement_snapshot = sessions
        .snapshot()
        .child("session")
        .expect("replacement is resident")
        .clone();
    assert_eq!(replacement_snapshot.membership, replacement.membership());
    assert_eq!(replacement_snapshot.restart_count, 0);
    replacement_stop.release();

    assert_eq!(
        root.snapshot()
            .child("gateway")
            .expect("gateway remains resident")
            .membership,
        gateway_scope.membership()
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("staged control-plane shutdown completes");
}

enum IdleMessage {
    Activity,
    IdleExpired,
}

struct IdleSessionActor {
    evictions: tokio::sync::mpsc::UnboundedSender<()>,
    activities: Arc<AtomicUsize>,
}

const IDLE_AFTER: Duration = Duration::from_secs(60);

impl Actor for IdleSessionActor {
    type Msg = IdleMessage;
    type Args = (tokio::sync::mpsc::UnboundedSender<()>, Arc<AtomicUsize>);

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        context
            .set_timeout("idle", IdleMessage::IdleExpired, IDLE_AFTER)
            .expect("live session arms its idle timer");
        let (evictions, activities) = args;
        Ok(Self {
            evictions,
            activities,
        })
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            IdleMessage::Activity => {
                // Re-keying replaces the pending deadline: activity pushes
                // idleness out instead of stacking timers (§5.3).
                context
                    .set_timeout("idle", IdleMessage::IdleExpired, IDLE_AFTER)
                    .expect("live session re-arms its idle timer");
                self.activities.fetch_add(1, Ordering::SeqCst);
            }
            IdleMessage::IdleExpired => {
                let _ = self.evictions.send(());
            }
        }
        Ok(())
    }
}

/// C.5's idle eviction and cancellable streaming: idleness is detected by an
/// actor-local keyed timer that activity re-arms, eviction is the session
/// scope's removal, and a stream cut off mid-flight fails senders with
/// `Terminated` while never leaking another value.
#[tokio::test(start_paused = true)]
async fn assistant_sessions_idle_evict_on_timers_and_streams_cancel_mid_flight() {
    let (evictions, mut evicted) = tokio::sync::mpsc::unbounded_channel();
    let activities = Arc::new(AtomicUsize::new(0));
    let stream_values = Arc::new(Mutex::new(Vec::new()));
    let mut session = Tree::new();
    let idle = session
        .add_actor(
            "idle",
            ActorDef::<IdleSessionActor>::cloned((evictions, Arc::clone(&activities))),
        )
        .expect("idle actor declared");
    let stream_gate = ReleaseGate::default();
    let stream = session
        .add_actor_once(
            "stream",
            ActorOnceDef::<StreamActor>::new((stream_gate.clone(), Arc::clone(&stream_values)))
                .mailbox(Mailbox::latest()),
        )
        .expect("stream actor declared");
    stream_gate.release();

    let sessions = DynamicTree::new();
    let mut root = Tree::new();
    let sessions = root
        .add_subtree_once("sessions", SubtreeOnceDef::new(sessions))
        .expect("session scope declared");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("assistant starts");
    let session = sessions
        .add_subtree_once("session-1", SubtreeOnceDef::new(session))
        .await
        .expect("session admitted")
        .into_handles();

    // Mid-life streaming works and the idle timer is armed from init.
    stream.send(5).await.expect("stream accepts mid-life value");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            stream_values
                .lock()
                .expect("stream values mutex poisoned")
                .as_slice()
                == [5]
        })
        .await
    );

    // Activity half-way through the idle window replaces the deadline …
    tokio::time::advance(Duration::from_secs(30)).await;
    idle.send(IdleMessage::Activity)
        .await
        .expect("live session accepts activity");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            activities.load(Ordering::SeqCst) == 1
        })
        .await,
        "the activity is handled (and the timer re-armed) before time moves"
    );
    // … so the original deadline passing evicts nothing …
    tokio::time::advance(Duration::from_secs(40)).await;
    assert!(
        evicted.try_recv().is_err(),
        "activity must push the idle deadline out"
    );
    // … and only the re-armed deadline fires.
    tokio::time::advance(Duration::from_secs(25)).await;
    tokio::time::timeout(Duration::from_secs(1), evicted.recv())
        .await
        .expect("idle timer fires after the re-armed window")
        .expect("eviction signal arrives");

    // Eviction is removal of the session scope; the stream is cut off
    // mid-flight: senders now fail terminally and no value ever leaks.
    assert_eq!(
        sessions.remove_scope(&session).await,
        RemoveOutcome::Removed
    );
    let error = stream
        .try_send(9)
        .expect_err("a cancelled stream rejects new values");
    assert_eq!(error.kind, shelterwood::SendErrorKind::Terminated);
    assert_eq!(
        stream_values
            .lock()
            .expect("stream values mutex poisoned")
            .as_slice(),
        [5],
        "no value leaks past cancellation"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("the remaining root joins teardown before the runtime drops");
}
