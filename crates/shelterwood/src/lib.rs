#![warn(missing_docs)]

//! Structured supervision and actors for asynchronous Rust systems.

mod driver;
mod engine;
mod exit;
mod identity;
mod mailbox;
mod policy;
mod raw;
mod task;
mod tree;

// M0 deliberately establishes the complete runtime boundary before its first
// consumer lands in M1.
#[allow(dead_code)]
mod runtime;

pub use exit::{
    Exit, ExitError, ExitKind, ExitResult, IntensityTrip, ShutdownStraggler, ShutdownTimeout,
    StartupFailure, StartupFailureCause,
};
pub use identity::{Incarnation, Membership};
pub use mailbox::{
    ActorRef, CallError, CallErrorKind, CallFuture, Replied, Reply, ReplyError, ReplyReceive,
    ReplyReceiver, SendError, SendErrorKind, SendFuture, SendTimeout,
};
pub use policy::{
    Backoff, BackoffFactor, ChildId, DefaultsInheritance, Intensity, Jitter, Mailbox,
    MailboxShutdown, PolicyError, Readiness, ReadinessDeadline, RestartCondition, RestartPolicy,
    Retention, ScopeDefaults, Shutdown, Strategy,
};
pub use raw::{RawActor, RawContext, RawDef, RawOnceDef};
pub use task::{CancellationToken, OneShotTaskRef, TaskContext, TaskDef, TaskOnceDef, TaskRef};
pub use tree::{
    ActorSlot, Admission, AdmissionReceipt, BuildError, DynamicActorSlot, DynamicScopeRef,
    DynamicSubtreeSlot, DynamicTaskSlot, DynamicTree, NotAdmittingCause, Removal, RemoveOutcome,
    ReserveError, ScopeRef, ScopeState, StartOrShutdownError, StartupError, StopReason, Subtree,
    SubtreeDef, SubtreeOnceDef, SubtreeSlot, System, TaskSlot, Tree,
};
