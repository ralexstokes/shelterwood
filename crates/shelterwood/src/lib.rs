#![warn(missing_docs)]

//! Structured supervision and actors for asynchronous Rust systems.

mod actor;
mod driver;
mod engine;
mod exit;
mod identity;
mod mailbox;
mod monitor;
mod observe;
mod policy;
mod raw;
mod task;
mod tree;

// M0 deliberately establishes the complete runtime boundary before its first
// consumer lands in M1.
#[allow(dead_code)]
mod runtime;

pub use actor::{Actor, ActorDef, ActorOnceDef, Context, Handler, StopContext};
pub use exit::{
    Exit, ExitError, ExitKind, ExitResult, IntensityTrip, ShutdownStraggler, ShutdownTimeout,
    StartupFailure, StartupFailureCause,
};
pub use identity::{Incarnation, Membership};
pub use mailbox::{
    ActorRef, Attempt, AttemptEnd, CallError, CallErrorKind, CallFuture, IdempotentCallError,
    IdempotentCallErrorKind, IdempotentCallFuture, NextIncarnation, NextIncarnationError,
    PinnedRef, Replied, Reply, ReplyError, ReplyReceive, ReplyReceiver, RetryPolicy, SendError,
    SendErrorKind, SendFuture, SendPayload, SendTimeout,
};
pub use monitor::{
    MONITOR_EVENT_CAPACITY, MonitorEvent, MonitorEventKind, MonitorMemberKind, WatchTarget,
};
pub use observe::{
    ActorKind, ActorStats, ActorStatsSnapshot, ChildObservation, ChildObserver, ChildSnapshot,
    ChildState, LIFECYCLE_EVENT_CAPACITY, LifecycleEvent, LifecycleEventKind, LifecycleEvents,
    LifecycleItem, LifecycleTryRecvError, MembershipStatus, RecursiveActorStats, RestartCount,
    RestartCounter, ScopeKind, ScopeSnapshot, ScopeState, SnapshotClosed, SnapshotReceiver,
    WaitError,
};
pub use policy::{
    Backoff, BackoffFactor, ChildId, DefaultsInheritance, Intensity, Jitter, KeyedCapacity,
    Mailbox, MailboxShutdown, PolicyError, Readiness, ReadinessDeadline, RestartCondition,
    RestartPolicy, Retention, ScopeDefaults, Shutdown, Strategy,
};
pub use raw::{
    Blocking, DeadlineElapsed, Guard, RawActor, RawContext, RawDef, RawOnceDef, Rejected,
    RejectedKind,
};
pub use task::{CancellationToken, OneShotTaskRef, TaskContext, TaskDef, TaskOnceDef, TaskRef};
pub use tree::{
    ActorSlot, Admission, AdmissionReceipt, BuildError, DynamicActorSlot, DynamicScopeRef,
    DynamicSubtreeSlot, DynamicTaskSlot, DynamicTree, NotAdmittingCause, Removal, RemoveOutcome,
    ReserveError, ScopeRef, StartOrShutdownError, StartupError, StopReason, Subtree, SubtreeDef,
    SubtreeOnceDef, SubtreeSlot, System, TaskSlot, Tree,
};
