#![warn(missing_docs)]

//! Structured supervision and actors for asynchronous Rust systems.

#[macro_use]
mod definition;
mod actor;
mod admission;
mod cancellation;
mod cells;
mod deadline;
mod driver;
mod engine;
mod exit;
mod identity;
mod mailbox;
mod observe;
mod plan;
mod policy;
mod raw;
mod runtime;
mod scope;
mod task;
mod tree;

pub use actor::{Actor, ActorDef, ActorOnceDef, Context, Handler, StopContext};
pub use admission::{NotAdmittingCause, RemoveOutcome, ReserveError};
pub use cancellation::CancellationToken;
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
pub use observe::{
    ChildSnapshot, ChildState, LIFECYCLE_EVENT_CAPACITY, LifecycleEvent, LifecycleEventKind,
    LifecycleEvents, LifecycleItem, LifecycleSeq, LifecycleTryRecvError, ScopeKind, ScopeSnapshot,
    SnapshotClosed, SnapshotReceiver, WaitError,
};
pub use policy::{
    Backoff, BackoffFactor, BoundedReadinessDeadline, DefaultsInheritance, ExponentialBackoff,
    FixedBackoff, Intensity, InvalidPolicy, Jitter, JitterSample, Mailbox, MailboxShutdown,
    PolicyError, PolicyField, Readiness, ReadinessDeadline, RestartAttempt, RestartCondition,
    RestartCount, RestartPolicy, Retention, ScopeDefaults, Shutdown, Strategy, TotalRestarts,
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

// Keep the repository-facing examples in the same rustdoc compilation lane as
// the crate API without adding documentation-only modules to the public surface.
// These synchronized copies live inside the crate so packaged doctests use the
// same sources as repository doctests.
#[cfg(doctest)]
mod repository_docs {
    #[doc = include_str!("../doctests/README.md")]
    mod readme {}

    #[doc = include_str!("../doctests/docs/embedding.md")]
    mod embedding {}

    #[doc = include_str!("../doctests/docs/observation.md")]
    mod observation {}

    #[doc = include_str!("../doctests/docs/retry-and-ordering.md")]
    mod retry_and_ordering {}

    #[doc = include_str!("../doctests/docs/shutdown-and-resources.md")]
    mod shutdown_and_resources {}
}
