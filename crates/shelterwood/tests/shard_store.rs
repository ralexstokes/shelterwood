use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use shelterwood::{
    Actor, ActorDef, ActorRef, CallErrorKind, Context, DynamicScopeRef, DynamicTree, ExitError,
    ExitResult, Membership, RemoveOutcome, Reply, ScopeRef, SubtreeOnceDef, Tree,
};

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
}

#[derive(Clone, Default)]
struct DurableTopology {
    operations: Arc<Mutex<HashMap<u64, OperationRecord>>>,
    current: Arc<Mutex<Option<Mount>>>,
    faults: Arc<Mutex<HashMap<u64, Fault>>>,
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

struct RouterActor(RouterArgs);

impl RouterActor {
    async fn directory_route(&self) -> Result<Option<Route>, ExitError> {
        self.0
            .directory
            .call(
                |reply| DirectoryMessage::Lookup { reply },
                Duration::from_secs(1),
            )
            .await
            .map(|reply| reply.value)
            .map_err(|error| ExitError::message(format!("directory lookup failed: {error}")))
    }

    async fn reconcile(
        &self,
        candidate: Mount,
        previous: Option<Mount>,
    ) -> Result<Route, ExitError> {
        let installed = self.directory_route().await?;
        if installed
            .as_ref()
            .is_none_or(|route| route.membership != candidate.route.membership)
        {
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
                .map_err(|error| {
                    ExitError::message(format!("directory cutover failed: {error}"))
                })?;
        }
        if let Some(previous) = previous
            && previous.scope.membership() != candidate.scope.membership()
        {
            let outcome = self.0.ranges.remove_scope(&previous.scope).await;
            if outcome != RemoveOutcome::Removed && outcome != RemoveOutcome::AlreadyAbsent {
                return Err(ExitError::message("unexpected exact-retire outcome"));
            }
        }
        *self
            .0
            .durable
            .current
            .lock()
            .expect("current mount mutex poisoned") = Some(candidate.clone());
        Ok(candidate.route)
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
        let route = self.replace(operation).await?;
        reply.send(route);
        Ok(())
    }
}

/// The §3.3 retry discipline, hand-rolled: one overall deadline for the whole
/// logical operation, retries only of the same durable idempotent operation
/// id, and a resend after `ReplyDropped` only once a *superseding* router
/// incarnation is running — never into the same doomed mailbox or the rebind
/// window.
async fn replace_with_retry(
    scope: &ScopeRef,
    router: &ActorRef<RouterMessage>,
    operation: u64,
) -> Route {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    for _ in 0..4 {
        // Every attempt spends from the one overall budget (§3.3 step 1),
        // the call's own acceptance deadline included.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "the overall deadline bounds the whole logical operation"
        );
        match router
            .call(
                move |reply| RouterMessage::Replace { operation, reply },
                Duration::from_secs(1).min(remaining),
            )
            .await
        {
            Ok(reply) => return reply.value,
            Err(error) => match error.kind {
                CallErrorKind::ReplyDropped => {
                    // Acceptance happened and the accepting incarnation lost
                    // the reply (B.3 guarantees its token). Await the
                    // incarnation-after before resending (§3.3 step 2).
                    let observed = error
                        .incarnation_observed
                        .expect("ReplyDropped carries the accepting incarnation");
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    scope
                        .wait_for_child(
                            "topology-writer",
                            move |child| {
                                child
                                    .incarnation
                                    .is_some_and(|current| current.supersedes(observed))
                            },
                            remaining,
                        )
                        .await
                        .expect("a superseding router incarnation runs");
                }
                CallErrorKind::AcceptanceTimedOut => {
                    // Guaranteed-not-accepted; always safe to retry
                    // (§3.3 step 4).
                }
                other => panic!(
                    "never blindly retry an accepted request with an unknown \
                     outcome — reconcile against durable evidence instead \
                     (§3.3 step 3): {other:?}"
                ),
            },
        }
    }
    panic!("idempotent topology operation did not reconcile")
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
async fn shard_store_reconciles_both_crash_windows_with_exact_idempotent_retries() {
    let durable = DurableTopology::default();
    let mut root = Tree::new();
    let directory = root
        .add_actor("directory", ActorDef::<DirectoryActor>::cloned(()))
        .expect("valid directory");
    let ranges = root
        .add_subtree_once("ranges", SubtreeOnceDef::new(DynamicTree::new()))
        .expect("valid dynamic range scope");
    let router = root
        .add_actor(
            "topology-writer",
            ActorDef::<RouterActor>::cloned(RouterArgs {
                ranges: ranges.clone(),
                directory: directory.clone(),
                durable: durable.clone(),
            }),
        )
        .expect("valid topology writer");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("store starts");
    let root_scope = system.scope();

    durable.inject(1, Fault::BeforeCommit);
    let first_error = router
        .call(
            |reply| RouterMessage::Replace {
                operation: 1,
                reply,
            },
            Duration::from_secs(1),
        )
        .await
        .expect_err("pre-commit crash drops its reply");
    assert_eq!(first_error.kind, CallErrorKind::ReplyDropped);
    assert!(lookup(&directory).await.is_none(), "cutover did not happen");
    let failed_candidate = match durable.operation(1).expect("mount was journaled") {
        OperationRecord::Mounted { candidate, .. } => candidate,
        OperationRecord::Committed { .. } => panic!("pre-commit crash cannot be committed"),
    };

    let first = replace_with_retry(&root_scope, &router, 1).await;
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
    let second_error = router
        .call(
            |reply| RouterMessage::Replace {
                operation: 2,
                reply,
            },
            Duration::from_secs(1),
        )
        .await
        .expect_err("post-commit crash drops its reply");
    assert_eq!(second_error.kind, CallErrorKind::ReplyDropped);
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

    let second = replace_with_retry(&root_scope, &router, 2).await;
    assert_eq!(second.membership, committed_candidate.route.membership);
    assert_eq!(
        replace_with_retry(&root_scope, &router, 2).await.membership,
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
