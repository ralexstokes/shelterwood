use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use shelterwood::{
    Actor, ActorDef, ActorRef, CallErrorKind, Context, DynamicScopeRef, DynamicTree, ExitError,
    ExitResult, Mailbox, Membership, RemoveOutcome, Reply, ScopeRef, SubtreeOnceDef, Tree,
};
use shelterwood_test_support::ReleaseGate;

#[derive(Clone, Debug, Default)]
struct DurableShard(Arc<Mutex<HashMap<String, u64>>>);

impl DurableShard {
    fn get(&self, key: &str) -> Option<u64> {
        self.0
            .lock()
            .expect("durable shard mutex poisoned")
            .get(key)
            .copied()
    }
}

enum ShardMessage {
    Put {
        key: String,
        value: u64,
        reply: Reply<()>,
    },
}

struct ShardActor(DurableShard);

impl Actor for ShardActor {
    type Msg = ShardMessage;
    type Args = DurableShard;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        let ShardMessage::Put { key, value, reply } = message;
        self.0
            .0
            .lock()
            .expect("durable shard mutex poisoned")
            .insert(key, value);
        reply.send(());
        Ok(())
    }
}

#[derive(Clone)]
struct Route {
    operation: u64,
    membership: Membership,
    actor: ActorRef<ShardMessage>,
}

enum DirectoryMessage {
    Cutover { route: Route, reply: Reply<()> },
    Lookup { reply: Reply<Option<Route>> },
}

struct DirectoryActor {
    route: Option<Route>,
}

impl Actor for DirectoryActor {
    type Msg = DirectoryMessage;
    type Args = ();

    async fn init(_: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self { route: None })
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        match message {
            DirectoryMessage::Cutover { route, reply } => {
                if self
                    .route
                    .as_ref()
                    .is_none_or(|current| current.operation <= route.operation)
                {
                    self.route = Some(route);
                }
                reply.send(());
            }
            DirectoryMessage::Lookup { reply } => reply.send(self.route.clone()),
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Mount {
    scope: ScopeRef,
    route: Route,
    durable: DurableShard,
}

#[derive(Clone)]
enum OperationRecord {
    Mounted {
        candidate: Mount,
        previous: Option<Mount>,
    },
    Committed {
        candidate: Mount,
        previous: Option<Mount>,
    },
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    BeforeCommit,
    AfterCommitBeforeReply,
    AfterCommitPark,
}

#[derive(Clone, Default)]
struct DurableTopology {
    operations: Arc<Mutex<HashMap<u64, OperationRecord>>>,
    current: Arc<Mutex<Option<Mount>>>,
    faults: Arc<Mutex<HashMap<u64, Fault>>>,
    aborted: Arc<Mutex<Vec<(u64, Mount)>>>,
    retry_kinds: Arc<Mutex<Vec<(u64, CallErrorKind)>>>,
    acceptances: Arc<AtomicUsize>,
    response_gate: ReleaseGate,
}

impl DurableTopology {
    fn inject(&self, operation: u64, fault: Fault) {
        self.faults
            .lock()
            .expect("fault map mutex poisoned")
            .insert(operation, fault);
    }

    fn take_fault(&self, operation: u64) -> Option<Fault> {
        self.faults
            .lock()
            .expect("fault map mutex poisoned")
            .remove(&operation)
    }

    fn operation(&self, operation: u64) -> Option<OperationRecord> {
        self.operations
            .lock()
            .expect("operation journal mutex poisoned")
            .get(&operation)
            .cloned()
    }

    fn acceptances(&self) -> usize {
        self.acceptances.load(Ordering::SeqCst)
    }

    fn record_retry(&self, operation: u64, kind: CallErrorKind) {
        self.retry_kinds
            .lock()
            .expect("retry-kind mutex poisoned")
            .push((operation, kind));
    }

    fn retry_kinds(&self, operation: u64) -> Vec<CallErrorKind> {
        self.retry_kinds
            .lock()
            .expect("retry-kind mutex poisoned")
            .iter()
            .filter_map(|(recorded, kind)| (*recorded == operation).then_some(*kind))
            .collect()
    }

    fn aborted(&self, operation: u64) -> Mount {
        self.aborted
            .lock()
            .expect("aborted-mount mutex poisoned")
            .iter()
            .find_map(|(recorded, mount)| (*recorded == operation).then(|| mount.clone()))
            .expect("operation has an aborted mount")
    }
}

enum RouterMessage {
    Replace { operation: u64, reply: Reply<Route> },
}

#[derive(Clone)]
struct RouterArgs {
    ranges: DynamicScopeRef,
    directory: ActorRef<DirectoryMessage>,
    durable: DurableTopology,
}

async fn directory_route(args: &RouterArgs) -> Result<Option<Route>, ExitError> {
    args.directory
        .call(
            |reply| DirectoryMessage::Lookup { reply },
            Duration::from_secs(1),
        )
        .await
        .map(|reply| reply.value)
        .map_err(|error| ExitError::message(format!("directory lookup failed: {error}")))
}

async fn reconcile_topology(
    args: &RouterArgs,
    candidate: Mount,
    previous: Option<Mount>,
) -> Result<Route, ExitError> {
    let installed = directory_route(args).await?;
    if installed
        .as_ref()
        .is_none_or(|route| route.membership != candidate.route.membership)
    {
        args.directory
            .call(
                {
                    let route = candidate.route.clone();
                    move |reply| DirectoryMessage::Cutover { route, reply }
                },
                Duration::from_secs(1),
            )
            .await
            .map_err(|error| ExitError::message(format!("directory cutover failed: {error}")))?;
    }
    if let Some(previous) = previous
        && previous.scope.membership() != candidate.scope.membership()
    {
        let outcome = args.ranges.remove_scope(&previous.scope).await;
        if outcome != RemoveOutcome::Removed && outcome != RemoveOutcome::AlreadyAbsent {
            return Err(ExitError::message("unexpected exact-retire outcome"));
        }
    }
    *args
        .durable
        .current
        .lock()
        .expect("current mount mutex poisoned") = Some(candidate.clone());
    Ok(candidate.route)
}

struct RouterActor(RouterArgs);

impl RouterActor {
    async fn directory_route(&self) -> Result<Option<Route>, ExitError> {
        directory_route(&self.0).await
    }

    async fn reconcile(
        &self,
        candidate: Mount,
        previous: Option<Mount>,
    ) -> Result<Route, ExitError> {
        reconcile_topology(&self.0, candidate, previous).await
    }

    async fn mount(&self, operation: u64) -> Result<Mount, ExitError> {
        let durable = DurableShard::default();
        let mut tree = Tree::new();
        let actor = tree
            .add_actor("shard", ActorDef::<ShardActor>::cloned(durable.clone()))
            .map_err(|error| ExitError::message(format!("shard declaration failed: {error}")))?;
        let id = format!("range-{operation}");
        let scope = self
            .0
            .ranges
            .add_subtree_once(id.clone(), SubtreeOnceDef::new(tree))
            .await
            .map_err(|error| ExitError::message(format!("range admission failed: {error}")))?
            .into_handles();
        let ready = self
            .0
            .ranges
            .wait_for_child(
                id,
                |child| matches!(child.state, shelterwood::ChildState::Running),
                Duration::from_secs(1),
            )
            .await
            .map_err(|error| ExitError::message(format!("range readiness failed: {error}")))?;
        if ready.membership != scope.membership() {
            return Err(ExitError::message(
                "readiness resolved a replacement membership",
            ));
        }
        Ok(Mount {
            route: Route {
                operation,
                membership: scope.membership(),
                actor,
            },
            scope,
            durable,
        })
    }

    async fn replace(&self, operation: u64) -> Result<Route, ExitError> {
        if let Some(record) = self.0.durable.operation(operation) {
            match record {
                OperationRecord::Committed {
                    candidate,
                    previous,
                } => return self.reconcile(candidate, previous).await,
                OperationRecord::Mounted {
                    candidate,
                    previous,
                } => {
                    // A pre-commit crash has no externally visible cutover.
                    // Compensate its exact candidate before retrying the same
                    // operation id, never by its reusable string label alone.
                    let installed = self.directory_route().await?;
                    if installed.as_ref().map(|route| route.membership)
                        != previous.as_ref().map(|mount| mount.route.membership)
                    {
                        return Err(ExitError::message(
                            "pre-commit abort found an unexpected directory route",
                        ));
                    }
                    self.0
                        .durable
                        .aborted
                        .lock()
                        .expect("aborted-mount mutex poisoned")
                        .push((operation, candidate.clone()));
                    let _ = self.0.ranges.remove_scope(&candidate.scope).await;
                    self.0
                        .durable
                        .operations
                        .lock()
                        .expect("operation journal mutex poisoned")
                        .remove(&operation);
                }
            }
        }

        let previous = self
            .0
            .durable
            .current
            .lock()
            .expect("current mount mutex poisoned")
            .clone();
        let candidate = self.mount(operation).await?;
        self.0
            .durable
            .operations
            .lock()
            .expect("operation journal mutex poisoned")
            .insert(
                operation,
                OperationRecord::Mounted {
                    candidate: candidate.clone(),
                    previous: previous.clone(),
                },
            );

        let fault = self.0.durable.take_fault(operation);
        if matches!(fault, Some(Fault::BeforeCommit)) {
            return Err(ExitError::message("injected pre-commit router crash"));
        }

        self.0
            .directory
            .call(
                {
                    let route = candidate.route.clone();
                    move |reply| DirectoryMessage::Cutover { route, reply }
                },
                Duration::from_secs(1),
            )
            .await
            .map_err(|error| ExitError::message(format!("directory cutover failed: {error}")))?;
        self.0
            .durable
            .operations
            .lock()
            .expect("operation journal mutex poisoned")
            .insert(
                operation,
                OperationRecord::Committed {
                    candidate: candidate.clone(),
                    previous: previous.clone(),
                },
            );

        if matches!(fault, Some(Fault::AfterCommitBeforeReply)) {
            return Err(ExitError::message("injected post-commit reply loss"));
        }
        if matches!(fault, Some(Fault::AfterCommitPark)) {
            self.0.durable.response_gate.wait().await;
        }
        self.reconcile(candidate, previous).await
    }
}

impl Actor for RouterActor {
    type Msg = RouterMessage;
    type Args = RouterArgs;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        let RouterMessage::Replace { operation, reply } = message;
        self.0.durable.acceptances.fetch_add(1, Ordering::SeqCst);
        let route = self.replace(operation).await?;
        reply.send(route);
        Ok(())
    }
}

async fn replace_with_retry(
    router: &ActorRef<RouterMessage>,
    root: &ScopeRef,
    args: &RouterArgs,
    operation: u64,
    per_attempt: Duration,
    overall: Duration,
    acceptance_timeout_observed: Option<ReleaseGate>,
) -> Route {
    assert!(!per_attempt.is_zero());
    assert!(!overall.is_zero());
    let deadline = Instant::now()
        .checked_add(overall)
        .expect("test retry deadline is representable");

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "idempotent topology retry budget exhausted"
        );
        let slice = per_attempt.min(remaining);
        match router
            .call(
                move |reply| RouterMessage::Replace { operation, reply },
                slice,
            )
            .await
        {
            Ok(reply) => return reply.value,
            Err(error) => {
                args.durable.record_retry(operation, error.kind);
                match error.kind {
                    CallErrorKind::AcceptanceTimedOut => {
                        // Successful withdrawal is the only arm that may retry
                        // without reconciliation or an incarnation transition.
                        if let Some(observed) = &acceptance_timeout_observed {
                            observed.release();
                        }
                    }
                    CallErrorKind::ReplyDropped => {
                        let accepting = error
                            .incarnation_observed
                            .expect("reply loss carries the accepting incarnation");
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        assert!(!remaining.is_zero(), "budget expired before restart wait");
                        root.wait_for_child(
                            "topology-writer",
                            move |child| {
                                matches!(child.state, shelterwood::ChildState::Running)
                                    && child
                                        .incarnation
                                        .is_some_and(|current| current.supersedes(accepting))
                            },
                            remaining,
                        )
                        .await
                        .expect("reply-loss retry observes a superseding incarnation");
                    }
                    CallErrorKind::ResponseTimedOut => {
                        // Acceptance makes resend unsafe. Reconcile the same
                        // durable journal record outside the parked actor instead.
                        let record = args
                            .durable
                            .operation(operation)
                            .expect("accepted operation has a journal record");
                        return match record {
                            OperationRecord::Committed {
                                candidate,
                                previous,
                            } => reconcile_topology(args, candidate, previous)
                                .await
                                .expect("committed response timeout reconciles"),
                            OperationRecord::Mounted { .. } => {
                                panic!("response timed out before a durable commit verdict")
                            }
                        };
                    }
                    CallErrorKind::Terminated => {
                        panic!("topology writer terminated during retry")
                    }
                    _ => panic!("unexpected call error kind"),
                }
            }
        }
    }
}

async fn lookup(directory: &ActorRef<DirectoryMessage>) -> Option<Route> {
    directory
        .call(
            |reply| DirectoryMessage::Lookup { reply },
            Duration::from_secs(1),
        )
        .await
        .expect("directory lookup replies")
        .value
}

#[tokio::test]
async fn shard_store_reconciles_retry_outcomes_with_one_overall_budget() {
    let durable = DurableTopology::default();
    let mut root = Tree::new();
    let directory = root
        .add_actor("directory", ActorDef::<DirectoryActor>::cloned(()))
        .expect("valid directory");
    let ranges = root
        .add_subtree_once("ranges", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("valid dynamic range scope");
    let router_args = RouterArgs {
        ranges: ranges.clone(),
        directory: directory.clone(),
        durable: durable.clone(),
    };
    let router = root
        .add_actor(
            "topology-writer",
            ActorDef::<RouterActor>::cloned(router_args.clone())
                .mailbox(Mailbox::queue(1).expect("non-zero capacity")),
        )
        .expect("valid topology writer");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("store starts");
    let root_scope = system.scope();

    durable.inject(1, Fault::BeforeCommit);
    let first = replace_with_retry(
        &router,
        &root_scope,
        &router_args,
        1,
        Duration::from_millis(500),
        Duration::from_secs(3),
        None,
    )
    .await;
    assert_eq!(
        durable.retry_kinds(1),
        [CallErrorKind::ReplyDropped],
        "pre-commit fault exercises the superseding-incarnation retry"
    );
    let failed_candidate = durable.aborted(1);
    assert!(
        first
            .membership
            .supersedes(failed_candidate.route.membership)
    );
    assert_eq!(
        lookup(&directory)
            .await
            .expect("route installed")
            .membership,
        first.membership
    );
    assert_eq!(
        failed_candidate.scope.wait_stopped().await,
        shelterwood::StopReason::ShutdownRequested
    );

    durable.inject(2, Fault::AfterCommitBeforeReply);
    let second = replace_with_retry(
        &router,
        &root_scope,
        &router_args,
        2,
        Duration::from_millis(500),
        Duration::from_secs(3),
        None,
    )
    .await;
    assert_eq!(
        durable.retry_kinds(2),
        [CallErrorKind::ReplyDropped],
        "post-commit reply loss also waits for the restarted writer"
    );
    let committed_candidate = match durable.operation(2).expect("commit was journaled") {
        OperationRecord::Committed { candidate, .. } => candidate,
        OperationRecord::Mounted { .. } => panic!("directory cutover must be committed"),
    };
    assert_eq!(
        lookup(&directory)
            .await
            .expect("post-commit route is visible")
            .membership,
        committed_candidate.route.membership
    );

    assert_eq!(second.membership, committed_candidate.route.membership);
    assert_eq!(
        replace_with_retry(
            &router,
            &root_scope,
            &router_args,
            2,
            Duration::from_millis(500),
            Duration::from_secs(3),
            None,
        )
        .await
        .membership,
        second.membership,
        "replaying a committed operation is idempotent"
    );

    second
        .actor
        .call(
            |reply| ShardMessage::Put {
                key: "alpha".to_owned(),
                value: 41,
                reply,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("new route accepts a write");
    assert_eq!(committed_candidate.durable.get("alpha"), Some(41));
    assert_eq!(failed_candidate.durable.get("alpha"), None);

    durable.inject(3, Fault::AfterCommitPark);
    let accepted_before_timeout = durable.acceptances();
    let third = replace_with_retry(
        &router,
        &root_scope,
        &router_args,
        3,
        Duration::from_millis(500),
        Duration::from_secs(3),
        None,
    )
    .await;
    assert_eq!(
        durable.retry_kinds(3),
        [CallErrorKind::ResponseTimedOut],
        "parked accepted request reconciles instead of resending"
    );
    assert_eq!(
        durable.acceptances(),
        accepted_before_timeout + 1,
        "response timeout reaches the handler exactly once"
    );
    assert_eq!(
        lookup(&directory)
            .await
            .expect("timed-out committed route remains visible")
            .membership,
        third.membership
    );
    assert_eq!(
        durable
            .current
            .lock()
            .expect("current mount mutex poisoned")
            .as_ref()
            .expect("response-timeout reconciliation records current")
            .route
            .membership,
        third.membership
    );

    // While operation 3's accepted handler is still parked, occupy the
    // one-slot mailbox with an idempotent replay. Operation 4 therefore
    // proves withdrawal with AcceptanceTimedOut before retrying. The gate is
    // the deterministic observation edge; no timing sleep releases the actor.
    let (queued_reply, queued_response) = Reply::channel();
    router
        .try_send(RouterMessage::Replace {
            operation: 3,
            reply: queued_reply,
        })
        .expect("empty one-slot mailbox proves no retry was accepted after response timeout");
    drop(queued_response);

    let acceptance_timeout_observed = ReleaseGate::default();
    let retry_task = {
        let router = router.clone();
        let root_scope = root_scope.clone();
        let router_args = router_args.clone();
        let observed = acceptance_timeout_observed.clone();
        tokio::spawn(async move {
            replace_with_retry(
                &router,
                &root_scope,
                &router_args,
                4,
                Duration::from_millis(500),
                Duration::from_secs(3),
                Some(observed),
            )
            .await
        })
    };
    acceptance_timeout_observed.wait().await;
    durable.response_gate.release();
    let fourth = retry_task.await.expect("retry task remains joinable");
    assert_eq!(fourth.operation, 4);
    let fourth_retries = durable.retry_kinds(4);
    assert!(!fourth_retries.is_empty());
    assert!(
        fourth_retries
            .iter()
            .all(|kind| *kind == CallErrorKind::AcceptanceTimedOut),
        "only proven-unaccepted attempts retry without reconciliation"
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("store shuts down");
}

struct GatedShardActor {
    durable: DurableShard,
    entered: Arc<std::sync::atomic::AtomicBool>,
    gate: Arc<tokio::sync::Notify>,
}

impl Actor for GatedShardActor {
    type Msg = ShardMessage;
    type Args = (
        DurableShard,
        Arc<std::sync::atomic::AtomicBool>,
        Arc<tokio::sync::Notify>,
    );

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        let (durable, entered, gate) = args;
        Ok(Self {
            durable,
            entered,
            gate,
        })
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        let ShardMessage::Put { key, value, reply } = message;
        self.entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.gate.notified().await;
        self.durable
            .0
            .lock()
            .expect("durable shard mutex poisoned")
            .insert(key, value);
        reply.send(());
        Ok(())
    }
}

/// C.1's accepted-request quiescence: a write accepted by the retiring mount
/// completes durably — reply delivered, value persisted — even though the
/// mount's removal begins while the request is still in flight.
#[tokio::test]
async fn shard_store_retire_waits_for_accepted_requests() {
    let durable = DurableShard::default();
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = Arc::new(tokio::sync::Notify::new());
    let mut root = Tree::new();
    let ranges = root
        .add_subtree_once("ranges", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("valid dynamic range scope");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("store starts");

    let mut mount = Tree::new();
    let actor = mount
        .add_actor(
            "shard",
            ActorDef::<GatedShardActor>::cloned((
                durable.clone(),
                Arc::clone(&entered),
                Arc::clone(&gate),
            )),
        )
        .expect("valid shard");
    let scope = ranges
        .add_subtree_once("range-live", SubtreeOnceDef::new(mount))
        .await
        .expect("range admission")
        .into_handles();

    let write = tokio::spawn({
        let actor = actor.clone();
        async move {
            actor
                .call(
                    |reply| ShardMessage::Put {
                        key: "alpha".to_owned(),
                        value: 7,
                        reply,
                    },
                    Duration::from_secs(4),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !entered.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the accepted write reaches the handler");

    // Retirement starts while the accepted write is mid-handling;
    // `membership_status` flips synchronously at the call (§13.12).
    let removal = tokio::spawn({
        let ranges = ranges.clone();
        let scope = scope.clone();
        async move { ranges.remove_scope(&scope).await }
    });
    ranges
        .wait_for_child(
            "range-live",
            |child| {
                matches!(
                    child.membership_status,
                    shelterwood::MembershipStatus::Removing
                )
            },
            Duration::from_secs(1),
        )
        .await
        .expect("removal is underway");

    gate.notify_one();
    write
        .await
        .expect("write task joins")
        .expect("the accepted write completes across retirement");
    assert_eq!(durable.get("alpha"), Some(7));
    assert_eq!(
        removal.await.expect("removal joins"),
        RemoveOutcome::Removed
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("store shuts down");
}
