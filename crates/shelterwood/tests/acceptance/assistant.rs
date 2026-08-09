use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::common::{POLL_TIMEOUT, ReleaseGate, poll_until};
use shelterwood::{
    Actor, ActorDef, ActorOnceDef, ActorRef, ChildState, Context, DeadlineElapsed, DynamicScopeRef,
    DynamicTree, ExitError, ExitResult, LifecycleEvent, LifecycleEventKind, LifecycleEvents,
    LifecycleItem, Mailbox, Membership, RemoveOutcome, Reply, ReserveError, RestartPolicy,
    ScopeState, StopContext, SubtreeOnceDef, Tree,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

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

#[derive(Clone)]
struct TransportState {
    /// The journal's durable ledger. It must survive the injected crash of
    /// `JournalActor` (a restart re-inits from cloned `Args`), so it models
    /// the disk underneath the actor rather than actor-owned state.
    delivered: Arc<Mutex<HashSet<u64>>>,
    processed: UnboundedSender<u64>,
    /// Crash injection, not observation: the flag must survive the restart it
    /// causes and is read-and-cleared by the actor itself, which a channel
    /// cannot express — so it stays an atomic.
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
    duplicate_notices: UnboundedSender<()>,
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
                let _ = self.0.duplicate_notices.send(());
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
            let _ = self.0.state.processed.send(id);
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
    processed: UnboundedReceiver<u64>,
    duplicate_notices: UnboundedReceiver<()>,
}

fn gateway(fail_once: bool) -> GatewayFixture {
    let (processed, processed_log) = unbounded_channel();
    let (duplicate_notices, duplicates_log) = unbounded_channel();
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
        duplicate_notices,
    }));
    let _defined_journal = journal_slot.define(ActorDef::<JournalActor>::cloned(JournalArgs {
        ingress: ingress.clone(),
        state: TransportState {
            delivered: Arc::default(),
            processed,
            fail_once: Arc::new(AtomicBool::new(fail_once)),
        },
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
        processed: processed_log,
        duplicate_notices: duplicates_log,
    }
}

#[derive(Clone)]
struct SessionControlArgs {
    /// Crash injection, kept as an atomic: it must survive the restart it
    /// causes and the actor read-and-clears it itself.
    crash_once: Arc<AtomicBool>,
    rehydrated: UnboundedSender<()>,
    stop_entered: UnboundedSender<()>,
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
                let _ = self.0.rehydrated.send(());
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
        let _ = self.0.stop_entered.send(());
        self.0.stop_gate.wait().await;
    }
}

struct StreamActor {
    values: UnboundedSender<u64>,
}

impl Actor for StreamActor {
    type Msg = u64;
    type Args = (ReleaseGate, UnboundedSender<u64>);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.0.wait().await;
        Ok(Self { values: args.1 })
    }

    async fn handle(&mut self, value: u64, _: &mut Context<'_, Self>) -> ExitResult {
        // A fixture whose log is never read has dropped the receiver.
        let _ = self.values.send(value);
        Ok(())
    }
}

#[derive(Clone)]
struct ToolArgs {
    /// Crash injection, kept as an atomic: it must survive the restart it
    /// causes and the actor read-and-clears it itself.
    crash_once: Arc<AtomicBool>,
    completions: UnboundedSender<()>,
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
                let _ = self.0.completions.send(());
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
    stream_log: UnboundedReceiver<u64>,
    rehydrated: UnboundedReceiver<()>,
    stop_entered: UnboundedReceiver<()>,
    stop_gate: ReleaseGate,
}

fn session_fixture() -> SessionFixture {
    let tools_tree = DynamicTree::new();
    let mut tree = Tree::new();
    let tools = tree
        .add_subtree_once("tools", SubtreeOnceDef::new(tools_tree))
        .expect("nested tool scope declared");
    let stream_gate = ReleaseGate::default();
    let (stream_values, stream_log) = unbounded_channel();
    let stream = tree
        .add_actor_once(
            "stream",
            ActorOnceDef::<StreamActor>::new((stream_gate.clone(), stream_values))
                .mailbox(Mailbox::latest()),
        )
        .expect("stream actor declared");
    let (rehydrated, rehydration_log) = unbounded_channel();
    let (stop_entered, stop_log) = unbounded_channel();
    let stop_gate = ReleaseGate::default();
    let control = tree
        .add_actor(
            "control",
            ActorDef::<SessionControlActor>::cloned(SessionControlArgs {
                crash_once: Arc::new(AtomicBool::new(true)),
                rehydrated,
                stop_entered,
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
        stream_log,
        rehydrated: rehydration_log,
        stop_entered: stop_log,
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
    let mut gateway = gateway(true);
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
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
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
    let mut stream_log = session_data.stream_log;
    let mut rehydrated = session_data.rehydrated;
    let mut stop_entered = session_data.stop_entered;
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
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
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
    let newest = tokio::time::timeout(POLL_TIMEOUT, stream_log.recv())
        .await
        .expect("released stream handles its retained update")
        .expect("stream actor is alive");
    assert_eq!(
        newest, 3,
        "latest mailbox keeps the newest accepted streaming update"
    );
    tokio::time::timeout(POLL_TIMEOUT, rehydrated.recv())
        .await
        .expect("session control rehydrates from its init continuation")
        .expect("control actor is alive");

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
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
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

    let (completions, mut completed) = unbounded_channel();
    let tool_args = ToolArgs {
        crash_once: Arc::new(AtomicBool::new(true)),
        completions,
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
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
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
    tokio::time::timeout(POLL_TIMEOUT, completed.recv())
        .await
        .expect("incarnation-owned offload completes through the actor loop")
        .expect("tool actor is alive");
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
    assert_eq!(
        gateway.processed.try_recv().ok(),
        Some(7),
        "the durable write happened exactly once"
    );
    assert!(gateway.processed.try_recv().is_err());
    tokio::time::timeout(POLL_TIMEOUT, gateway.duplicate_notices.recv())
        .await
        .expect("the redelivered id surfaces as a duplicate notice")
        .expect("ingress actor is alive");

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
    tokio::time::timeout(POLL_TIMEOUT, stop_entered.recv())
        .await
        .expect("removal drives the control actor into on_stop")
        .expect("control actor reports entering on_stop");
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
    evictions: UnboundedSender<()>,
    activities: UnboundedSender<()>,
}

const IDLE_AFTER: Duration = Duration::from_secs(60);

impl Actor for IdleSessionActor {
    type Msg = IdleMessage;
    type Args = (UnboundedSender<()>, UnboundedSender<()>);

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
                let _ = self.activities.send(());
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
    let (evictions, mut evicted) = unbounded_channel();
    let (activities, mut activity_log) = unbounded_channel();
    let (stream_values, mut stream_log) = unbounded_channel();
    let mut session = Tree::new();
    let idle = session
        .add_actor(
            "idle",
            ActorDef::<IdleSessionActor>::cloned((evictions, activities)),
        )
        .expect("idle actor declared");
    let stream_gate = ReleaseGate::default();
    let stream = session
        .add_actor_once(
            "stream",
            ActorOnceDef::<StreamActor>::new((stream_gate.clone(), stream_values))
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
    let mid_life = tokio::time::timeout(POLL_TIMEOUT, stream_log.recv())
        .await
        .expect("live stream handles the mid-life value")
        .expect("stream actor is alive");
    assert_eq!(mid_life, 5);

    // Activity half-way through the idle window replaces the deadline …
    tokio::time::advance(Duration::from_secs(30)).await;
    idle.send(IdleMessage::Activity)
        .await
        .expect("live session accepts activity");
    tokio::time::timeout(POLL_TIMEOUT, activity_log.recv())
        .await
        .expect("the activity is handled (and the timer re-armed) before time moves")
        .expect("idle actor is alive");
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
    assert!(
        stream_log.try_recv().is_err(),
        "no value leaks past cancellation"
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("the remaining root joins teardown before the runtime drops");
}
