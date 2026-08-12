use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::common::ReleaseGate;
use shelterwood::{
    Actor, ActorDef, ActorRef, CallErrorKind, Context, DynamicScopeRef, DynamicTree, ExitError,
    ExitResult, Incarnation, Membership, RemoveOutcome, Reply, ScopeRef, SubtreeOnceDef, Tree,
};

/// A shard's durable contents. Deliberately not actor-owned state: the test
/// reads a retired mount's data after its serving actor is gone, so this
/// models the disk underneath the actor.
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

/// The router's durable operation journal. Router incarnations are replaced
/// across the injected crashes and each re-init starts from cloned `Args`, so
/// this is the durable store the §3.3 retry discipline reconciles against —
/// state that must outlive any incarnation, not shared actor state.
#[derive(Clone, Default)]
struct DurableTopology {
    operations: Arc<Mutex<HashMap<u64, OperationRecord>>>,
    current: Arc<Mutex<Option<Mount>>>,
}

impl DurableTopology {
    fn operation(&self, operation: u64) -> Option<OperationRecord> {
        self.operations
            .lock()
            .expect("operation journal mutex poisoned")
            .get(&operation)
            .cloned()
    }

    /// The journal's idempotency keys, so a retry can be shown to reuse the
    /// key its first attempt wrote rather than minting a fresh one.
    fn journalled_operations(&self) -> Vec<u64> {
        let mut operations: Vec<u64> = self
            .operations
            .lock()
            .expect("operation journal mutex poisoned")
            .keys()
            .copied()
            .collect();
        operations.sort_unstable();
        operations
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
    /// Fault injection as plain per-incarnation config: once-semantics come
    /// from the durable journal (a retry finds the record its first attempt
    /// wrote before crashing), not from mutating shared state.
    faults: HashMap<u64, Fault>,
}

struct RouterActor(RouterArgs);

impl RouterActor {
    async fn directory_route(&self) -> Result<Option<Route>, ExitError> {
        self.0
            .directory
            .call(
                |reply| DirectoryMessage::Lookup { reply },
                Duration::from_secs(1).into(),
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
                    Duration::from_secs(1).into(),
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
            .map_err(|error| ExitError::message(format!("range admission failed: {error}")))?;
        let ready = self
            .0
            .ranges
            .as_scope()
            .wait_for_child(
                id,
                |child| matches!(child.state, shelterwood::ChildState::Running),
                Duration::from_secs(1).into(),
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
        let record = self.0.durable.operation(operation);
        // A configured fault fires only on the operation's first attempt,
        // recognized by the absence of a durable record: every crash window
        // under test opens after the attempt journaled itself, so a retry
        // can never re-arm the fault.
        let fault = record
            .is_none()
            .then(|| self.0.faults.get(&operation).copied())
            .flatten();
        if let Some(record) = record {
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
                Duration::from_secs(1).into(),
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
struct RetryOutcome {
    route: Route,
    fault_record: Option<OperationRecord>,
    directory_at_fault: Option<Option<Route>>,
    attempted_operations: Vec<u64>,
    dropped_incarnation: Option<Incarnation>,
    reply_incarnation: Incarnation,
}

async fn replace_with_retry(
    scope: &ScopeRef,
    router: &ActorRef<RouterMessage>,
    directory: &ActorRef<DirectoryMessage>,
    durable: &DurableTopology,
    operation: u64,
) -> RetryOutcome {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let journalled_before = durable.journalled_operations();
    let mut fault_record = None;
    let mut directory_at_fault = None;
    let mut attempted_operations = Vec::new();
    let mut dropped_incarnation = None;
    for _ in 0..4 {
        // Every attempt spends from the one overall budget (§3.3 step 1),
        // the call's own acceptance deadline included.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "the overall deadline bounds the whole logical operation"
        );
        attempted_operations.push(operation);
        match router
            .call(
                move |reply| RouterMessage::Replace { operation, reply },
                Duration::from_secs(1).min(remaining).into(),
            )
            .await
        {
            Ok(reply) => {
                assert!(
                    tokio::time::Instant::now() <= deadline,
                    "the successful retry remains inside the one overall deadline"
                );
                if let Some(observed) = dropped_incarnation {
                    assert!(
                        reply.incarnation.supersedes(observed),
                        "the retry is accepted only by a superseding incarnation"
                    );
                }
                // Exact idempotency key: however many attempts ran, they all
                // journalled under this operation's key. A retry that minted
                // a fresh key would leave a second new journal entry.
                let journalled_after = durable.journalled_operations();
                let minted: Vec<u64> = journalled_after
                    .into_iter()
                    .filter(|id| *id != operation && !journalled_before.contains(id))
                    .collect();
                assert!(
                    minted.is_empty(),
                    "retries reuse operation {operation}'s idempotency key, but {minted:?} \
                     were journalled"
                );
                return RetryOutcome {
                    route: reply.value,
                    fault_record,
                    directory_at_fault,
                    attempted_operations,
                    dropped_incarnation,
                    reply_incarnation: reply.incarnation,
                };
            }
            Err(error) => match error.kind {
                CallErrorKind::ReplyDropped => {
                    // Acceptance happened and the accepting incarnation lost
                    // the reply (B.3 guarantees its token). Await the
                    // incarnation-after before resending (§3.3 step 2).
                    let observed = error
                        .incarnation_observed
                        .expect("ReplyDropped carries the accepting incarnation");
                    assert!(
                        dropped_incarnation.replace(observed).is_none(),
                        "each injected logical operation faults only once"
                    );
                    fault_record = Some(
                        durable
                            .operation(operation)
                            .expect("the faulting attempt journaled durable evidence"),
                    );
                    directory_at_fault = Some(lookup(directory).await);
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let replacement = scope
                        .wait_for_child(
                            "topology-writer",
                            move |child| {
                                child
                                    .incarnation
                                    .is_some_and(|current| current.supersedes(observed))
                            },
                            remaining.into(),
                        )
                        .await
                        .expect("a superseding router incarnation runs");
                    assert!(
                        replacement
                            .incarnation
                            .expect("running replacement has an incarnation")
                            .supersedes(observed)
                    );
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
    panic!(
        "idempotent topology operation {operation} did not reconcile within one overall deadline"
    )
}

async fn lookup(directory: &ActorRef<DirectoryMessage>) -> Option<Route> {
    directory
        .call(
            |reply| DirectoryMessage::Lookup { reply },
            Duration::from_secs(1).into(),
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
                faults: HashMap::from([
                    (1, Fault::BeforeCommit),
                    (2, Fault::AfterCommitBeforeReply),
                ]),
            }),
        )
        .expect("valid topology writer");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("store starts");
    let root_scope = system.scope();

    let first_retry = replace_with_retry(&root_scope, &router, &directory, &durable, 1).await;
    assert_eq!(first_retry.attempted_operations, [1, 1]);
    assert!(first_retry.dropped_incarnation.is_some());
    assert!(
        first_retry.reply_incarnation.supersedes(
            first_retry
                .dropped_incarnation
                .expect("the helper observed its injected fault")
        )
    );
    assert!(
        first_retry
            .directory_at_fault
            .expect("the helper captured the pre-retry directory")
            .is_none(),
        "pre-commit cutover did not happen"
    );
    let failed_candidate = match first_retry
        .fault_record
        .expect("the helper captured the pre-commit journal")
    {
        OperationRecord::Mounted { candidate, .. } => candidate,
        OperationRecord::Committed { .. } => panic!("pre-commit crash cannot be committed"),
    };

    let first = first_retry.route;
    assert!(
        !first
            .membership
            .supersedes(failed_candidate.route.membership)
    );
    assert!(
        !failed_candidate
            .route
            .membership
            .supersedes(first.membership)
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

    let second_retry = replace_with_retry(&root_scope, &router, &directory, &durable, 2).await;
    assert_eq!(second_retry.attempted_operations, [2, 2]);
    let committed_candidate = match second_retry
        .fault_record
        .expect("the helper captured the post-commit journal")
    {
        OperationRecord::Committed { candidate, .. } => candidate,
        OperationRecord::Mounted { .. } => panic!("directory cutover must be committed"),
    };
    assert_eq!(
        second_retry
            .directory_at_fault
            .expect("the helper captured the post-commit directory")
            .expect("post-commit route is visible")
            .membership,
        committed_candidate.route.membership
    );

    let second = second_retry.route;
    assert_eq!(second.membership, committed_candidate.route.membership);
    let replay = replace_with_retry(&root_scope, &router, &directory, &durable, 2).await;
    assert_eq!(replay.attempted_operations, [2]);
    assert!(replay.fault_record.is_none());
    assert_eq!(
        replay.route.membership, second.membership,
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
            Duration::from_secs(1).into(),
        )
        .await
        .expect("new route accepts a write");
    assert_eq!(committed_candidate.durable.get("alpha"), Some(41));
    assert_eq!(failed_candidate.durable.get("alpha"), None);

    system
        .shutdown(Duration::from_secs(1).into())
        .await
        .expect("store shuts down");
}

struct GatedShardActor {
    durable: DurableShard,
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    gate: ReleaseGate,
}

impl Actor for GatedShardActor {
    type Msg = ShardMessage;
    type Args = (
        DurableShard,
        tokio::sync::mpsc::UnboundedSender<()>,
        ReleaseGate,
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
        let _ = self.entered.send(());
        self.gate.wait().await;
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
    let (entered, mut entered_log) = tokio::sync::mpsc::unbounded_channel();
    let gate = ReleaseGate::default();
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
            ActorDef::<GatedShardActor>::cloned((durable.clone(), entered, gate.clone())),
        )
        .expect("valid shard");
    let scope = ranges
        .add_subtree_once("range-live", SubtreeOnceDef::new(mount))
        .await
        .expect("range admission");

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
                    Duration::from_secs(4).into(),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), entered_log.recv())
        .await
        .expect("the accepted write reaches the handler")
        .expect("shard actor is alive");

    // Retirement starts while the accepted write is mid-handling;
    // `membership_status` flips synchronously at the call (§13.12).
    let removal = tokio::spawn({
        let ranges = ranges.clone();
        let scope = scope.clone();
        async move { ranges.remove_scope(&scope).await }
    });
    ranges
        .as_scope()
        .wait_for_child(
            "range-live",
            |child| {
                matches!(
                    child.membership_status,
                    shelterwood::MembershipStatus::Removing
                )
            },
            Duration::from_secs(1).into(),
        )
        .await
        .expect("removal is underway");

    gate.release();
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
        .shutdown(Duration::from_secs(1).into())
        .await
        .expect("store shuts down");
}
