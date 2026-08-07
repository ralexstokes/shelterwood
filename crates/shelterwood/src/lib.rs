#![warn(missing_docs)]

//! Structured supervision and actors for asynchronous Rust systems.

mod actor;
mod deadline;
mod driver;
mod engine;
mod exit;
mod identity;
mod mailbox;
mod observe;
mod policy;
mod raw;
mod runtime;
mod task;
mod tree;

pub use actor::{Actor, ActorDef, ActorOnceDef, Context, Handler, StopContext};
pub use exit::{
    Exit, ExitError, ExitKind, ExitResult, IntensityTrip, ShutdownStraggler, ShutdownTimeout,
    StartupFailure, StartupFailureCause,
};
pub use identity::{Incarnation, Membership};
pub use mailbox::{
    ActorRef, CallError, CallErrorKind, CallFuture, Replied, Reply, ReplyError, ReplyReceive,
    ReplyReceiver, SendError, SendErrorKind, SendFuture, SendTimeout,
};
pub use observe::{
    ChildSnapshot, ChildState, LIFECYCLE_EVENT_CAPACITY, LifecycleEvent, LifecycleEventKind,
    LifecycleEvents, LifecycleItem, LifecycleTryRecvError, MembershipStatus, ScopeKind,
    ScopeSnapshot, ScopeState, SnapshotClosed, SnapshotReceiver, WaitError,
};
pub use policy::{
    Backoff, BackoffFactor, ChildId, DefaultsInheritance, Intensity, Jitter, Mailbox,
    MailboxShutdown, PolicyError, Readiness, ReadinessDeadline, RestartCondition, RestartPolicy,
    Retention, ScopeDefaults, Shutdown, Strategy,
};
pub use raw::{
    Blocking, DeadlineElapsed, Guard, RawActor, RawContext, RawDef, RawOnceDef, Rejected,
};
pub use task::{CancellationToken, OneShotTaskRef, TaskContext, TaskDef, TaskOnceDef, TaskRef};
pub use tree::{
    ActorSlot, Admission, AdmissionReceipt, BuildError, DynamicActorSlot, DynamicScopeRef,
    DynamicSubtreeSlot, DynamicTaskSlot, DynamicTree, NotAdmittingCause, Removal, RemoveOutcome,
    ReserveError, ScopeRef, StartOrShutdownError, StartupError, StopReason, Subtree, SubtreeDef,
    SubtreeOnceDef, SubtreeSlot, System, TaskSlot, Tree,
};
