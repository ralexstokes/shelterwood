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
mod task;
mod tree;

pub use actor::{Actor, ActorDef, ActorOnceDef, Context, Handler, StopContext};
pub use admission::{NotAdmittingCause, RemoveOutcome, ReserveError};
pub use cancellation::CancellationToken;
pub use exit::{
    Exit, ExitError, ExitKind, ExitResult, IntensityTrip, ShutdownStraggler, ShutdownTimeout,
    StartupError, StartupFailure, StartupFailureCause, StopReason,
};
pub use identity::{ChildId, Incarnation, Membership};
pub use mailbox::{
    ActorRef, CallError, CallErrorKind, CallFuture, Replied, Reply, ReplyError, ReplyReceive,
    ReplyReceiver, SendError, SendErrorKind, SendFuture, SendTimeout,
};
pub use observe::{
    ChildSnapshot, ChildState, LIFECYCLE_EVENT_CAPACITY, LifecycleEvent, LifecycleEventKind,
    LifecycleEvents, LifecycleItem, LifecycleSeq, LifecycleTryRecvError, MembershipStatus,
    ScopeKind, ScopeSnapshot, ScopeState, SnapshotClosed, SnapshotReceiver, WaitError,
};
pub use policy::{
    Backoff, BackoffFactor, DefaultsInheritance, Intensity, InvalidPolicy, Jitter, Mailbox,
    MailboxShutdown, PolicyError, PolicyField, Readiness, ReadinessDeadline, RestartCondition,
    RestartPolicy, Retention, ScopeDefaults, Shutdown, Strategy,
};
pub use raw::{
    Blocking, DeadlineElapsed, Guard, RawActor, RawContext, RawDef, RawOnceDef, Rejected,
};
pub use task::{OneShotTaskRef, TaskContext, TaskDef, TaskOnceDef, TaskRef};
pub use tree::{
    ActorSlot, Admission, AdmissionReceipt, BuildError, DynamicActorSlot, DynamicScopeRef,
    DynamicSubtreeSlot, DynamicTaskSlot, DynamicTree, Removal, ScopeRef, StartOrShutdownError,
    Subtree, SubtreeDef, SubtreeOnceDef, SubtreeSlot, System, TaskSlot, Tree,
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
