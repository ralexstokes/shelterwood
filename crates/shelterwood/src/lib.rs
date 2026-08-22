#![warn(missing_docs)]

//! Structured supervision and actors for asynchronous Rust systems.

#[macro_use]
mod definition;
mod actor;
mod cells;
mod deadline;
mod driver;
mod engine;
mod exit;
mod identity;
mod mailbox;
mod plan;
mod policy;
mod raw;
mod runtime;
mod scope;
mod task;
mod tree;

pub use actor::{Actor, ActorDef, ActorOnceDef, Context, Handler, StopContext};
pub use cells::{
    CancellationToken, ChildSnapshot, ChildState, LifecycleEvent, LifecycleEventKind,
    LifecycleEvents, LifecycleItem, LifecycleSeq, LifecycleTryRecvError, NotAdmittingCause,
    RemoveOutcome, ReserveError, ScopeSnapshot, SnapshotClosed, SnapshotReceiver, WaitError,
};
pub use deadline::DeadlineBudget;
pub use engine::{MembershipStatus, ScopeState};
pub use exit::{
    Cancellation, Exit, ExitError, ExitKind, ExitResult, GracePhase, IntensityTrip,
    ShutdownStraggler, ShutdownTimeout, StartupError, StartupFailure, StartupFailureCause,
    StopReason,
};
pub use identity::{ChildId, Incarnation, Membership};
pub use mailbox::{
    ActorRef, CallError, CallErrorKind, CallFuture, Replied, Reply, ReplyError, ReplyReceive,
    ReplyReceiver, SendError, SendErrorKind, SendFuture, SendTimeout,
};
pub use policy::{
    Backoff, BackoffFactor, DefaultsInheritance, ExponentialBackoff, FixedBackoff, Intensity,
    Jitter, JitterSample, Mailbox, MailboxShutdown, NonZeroDuration, PolicyError, Readiness,
    ReadinessDeadline, RestartAttempt, RestartCondition, RestartCount, RestartPolicy, Retention,
    ScopeDefaults, ScopeFlavor, Shutdown, Strategy, TotalRestarts,
};
pub use raw::{
    Blocking, DeadlineElapsed, Guard, RawActor, RawContext, RawDef, RawOnceDef, Rejected,
};
pub use scope::{DynamicScopeRef, ScopeRef};
pub use task::{OneShotTaskRef, TaskContext, TaskDef, TaskOnceDef, TaskRef};
pub use tree::{
    ActorSlot, Admission, BuildError, DynamicActorSlot, DynamicSubtreeSlot, DynamicTaskSlot,
    DynamicTree, Removal, StartOrShutdownError, Subtree, SubtreeDef, SubtreeOnceDef, SubtreeSlot,
    System, TaskSlot, Tree,
};

/// Long-form operational guides, rendered with the API reference.
///
/// Each page documents a contract that spans many items — the material that
/// does not fit a single type's documentation. The modules under this one
/// exist only to carry the pages; they export nothing.
pub mod guides {
    #[doc = include_str!("../docs/retry-and-ordering.md")]
    pub mod retry_and_ordering {}

    #[doc = include_str!("../docs/shutdown-and-resources.md")]
    pub mod shutdown_and_resources {}
}

// Keep the repository-root narrative documents in the doctest compilation lane
// so their examples stay compiled against the current API. The paths reach
// outside the package directory, which is fine here: `cfg(doctest)` is active
// only for repository-local `cargo test --doc`, so packaged builds and docs.rs
// never resolve them.
#[cfg(doctest)]
mod repository_docs {
    #[doc = include_str!("../../../README.md")]
    mod readme {}

    #[doc = include_str!("../../../docs/embedding.md")]
    mod embedding {}

    #[doc = include_str!("../../../docs/observation.md")]
    mod observation {}
}
