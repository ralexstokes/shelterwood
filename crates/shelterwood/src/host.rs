//! Degenerate one-incarnation hosting on the ordinary supervision runner.

use std::{future::Future, marker::PhantomData, time::Duration};

use crate::{
    Actor, ActorOnceDef, ActorRef, BuildError, Exit, ExitResult, Mailbox, PolicyError, RawActor,
    RawOnceDef, Readiness, ReadinessDeadline, ReserveError, ScopeRef, Shutdown, System,
    TaskContext, TaskOnceDef, TaskRef, Tree,
};

const HOSTED_ID: &str = "hosted";

/// Validated policy data for a structurally non-restarting hosted incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostOptions {
    mailbox: Mailbox,
    readiness: Option<Readiness>,
    readiness_deadline: ReadinessDeadline,
    shutdown: Shutdown,
    shutdown_grace: Duration,
}

impl Default for HostOptions {
    fn default() -> Self {
        Self {
            mailbox: Mailbox::default(),
            readiness: None,
            readiness_deadline: ReadinessDeadline::Bounded(
                crate::policy::DEFAULT_READINESS_DEADLINE,
            ),
            shutdown: Shutdown::default(),
            shutdown_grace: crate::policy::DEFAULT_SHUTDOWN_GRACE,
        }
    }
}

impl HostOptions {
    /// Creates host options with the ordinary library policy defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects the actor mailbox policy. Hosted tasks ignore this field.
    #[must_use]
    pub fn mailbox(mut self, mailbox: Mailbox) -> Self {
        self.mailbox = mailbox;
        self
    }

    /// Overrides the child-kind default readiness mode.
    #[must_use]
    pub fn readiness(mut self, readiness: Readiness) -> Self {
        self.readiness = Some(readiness);
        self
    }

    /// Sets the structural readiness deadline.
    #[must_use]
    pub fn readiness_deadline(mut self, deadline: ReadinessDeadline) -> Self {
        self.readiness_deadline = deadline;
        self
    }

    /// Sets the incarnation's cooperative shutdown policy.
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Sets the owning handle's overall drop-time shutdown grace.
    #[must_use]
    pub fn shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Returns the actor mailbox policy.
    #[must_use]
    pub const fn mailbox_policy(&self) -> Mailbox {
        self.mailbox
    }

    /// Returns the explicit readiness override, if any.
    #[must_use]
    pub const fn readiness_mode(&self) -> Option<Readiness> {
        self.readiness
    }

    /// Returns the readiness deadline.
    #[must_use]
    pub const fn readiness_deadline_policy(&self) -> ReadinessDeadline {
        self.readiness_deadline
    }

    /// Returns the child shutdown policy.
    #[must_use]
    pub const fn shutdown_policy(&self) -> Shutdown {
        self.shutdown
    }

    /// Returns the owning handle's drop-time shutdown grace.
    #[must_use]
    pub const fn configured_shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }

    fn validate(&self, kind: HostedKind) -> Result<(), HostError> {
        if kind == HostedKind::Task && self.mailbox != Mailbox::default() {
            return Err(HostError::TaskMailbox);
        }
        if matches!(self.readiness_deadline, ReadinessDeadline::Inherit) {
            return Err(HostError::UnresolvedReadinessDeadline);
        }
        if matches!(
            self.readiness_deadline,
            ReadinessDeadline::Bounded(deadline) if deadline.is_zero()
        ) {
            return Err(HostError::InvalidPolicy(PolicyError::ZeroDuration));
        }
        if kind != HostedKind::Actor && self.readiness == Some(Readiness::AfterInit) {
            return Err(HostError::InvalidPolicy(PolicyError::UnsupportedReadiness));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HostedKind {
    Actor,
    Raw,
    Task,
}

/// Failure to validate or start a hosted incarnation.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum HostError {
    /// No ambient supported async runtime exists.
    #[error("no ambient Tokio runtime is available")]
    NoRuntime,
    /// The standalone host cannot resolve an inherited readiness deadline.
    #[error("hosted readiness deadline must be bounded or unbounded")]
    UnresolvedReadinessDeadline,
    /// A supplied policy is invalid for the hosted child kind.
    #[error("invalid host policy: {0}")]
    InvalidPolicy(PolicyError),
    /// A mailbox policy was supplied for a hosted task.
    #[error("hosted tasks do not have mailboxes")]
    TaskMailbox,
    /// The private one-child declaration could not be built.
    #[error("host declaration failed: {0}")]
    Declaration(ReserveError),
    /// The ordinary supervision runner could not be started.
    #[error("host runner failed: {0}")]
    Build(BuildError),
}

/// A hosted incarnation exited before satisfying its readiness contract.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("hosted incarnation exited before becoming ready")]
pub struct HostedReadyError {
    /// Classified incarnation exit.
    pub exit: Exit,
}

/// Namespace for hosting one callback-oriented actor incarnation.
pub struct Hosted<A: Actor>(PhantomData<fn() -> A>);

impl<A: Actor> Hosted<A> {
    /// Starts one callback-oriented actor incarnation on the ordinary runner.
    pub fn spawn(
        args: A::Args,
        options: HostOptions,
    ) -> Result<(ActorRef<A::Msg>, HostedHandle), HostError> {
        options.validate(HostedKind::Actor)?;
        let mut definition = ActorOnceDef::<A>::new(args)
            .mailbox(options.mailbox)
            .shutdown(options.shutdown)
            .readiness_deadline(options.readiness_deadline);
        if let Some(readiness) = options.readiness {
            definition = definition.readiness(readiness);
        }
        let mut tree = Tree::new();
        let actor = tree
            .add_actor_once(HOSTED_ID, definition)
            .map_err(HostError::Declaration)?;
        let task = TaskRef::new(actor.member_cell());
        let system = tree.spawn().map_err(map_build_error)?;
        Ok((
            actor,
            HostedHandle::new(system, task, options.shutdown_grace),
        ))
    }
}

/// Namespace for hosting one loop-owning raw actor incarnation.
pub struct HostedRaw<M>(PhantomData<fn(M)>);

impl<M: Send + 'static> HostedRaw<M> {
    /// Starts one owned raw actor incarnation on the ordinary runner.
    pub fn spawn<R>(
        raw_actor: R,
        options: HostOptions,
    ) -> Result<(ActorRef<M>, HostedHandle), HostError>
    where
        R: RawActor<Msg = M>,
    {
        options.validate(HostedKind::Raw)?;
        let mut definition = RawOnceDef::new(raw_actor)
            .mailbox(options.mailbox)
            .shutdown(options.shutdown)
            .readiness_deadline(options.readiness_deadline);
        if let Some(readiness) = options.readiness {
            definition = definition
                .readiness(readiness)
                .map_err(HostError::InvalidPolicy)?;
        }
        let mut tree = Tree::new();
        let actor = tree
            .add_raw_once(HOSTED_ID, definition)
            .map_err(HostError::Declaration)?;
        let task = TaskRef::new(actor.member_cell());
        let system = tree.spawn().map_err(map_build_error)?;
        Ok((
            actor,
            HostedHandle::new(system, task, options.shutdown_grace),
        ))
    }
}

/// Namespace for hosting one supervised task incarnation.
pub struct HostedTask;

impl HostedTask {
    /// Starts one consuming task body on the ordinary runner.
    pub fn spawn<F, Fut>(
        task_factory: F,
        options: HostOptions,
    ) -> Result<(TaskRef, HostedHandle), HostError>
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = ExitResult> + Send + 'static,
    {
        options.validate(HostedKind::Task)?;
        let mut definition = TaskOnceDef::new(task_factory)
            .shutdown(options.shutdown)
            .readiness_deadline(options.readiness_deadline);
        if let Some(readiness) = options.readiness {
            definition = definition
                .readiness(readiness)
                .map_err(HostError::InvalidPolicy)?;
        }
        let mut tree = Tree::new();
        let (task, completion) = tree
            .add_task_once(HOSTED_ID, definition)
            .map_err(HostError::Declaration)?;
        drop(completion);
        let system = tree.spawn().map_err(map_build_error)?;
        Ok((
            task.clone(),
            HostedHandle::new(system, task, options.shutdown_grace),
        ))
    }
}

fn map_build_error(error: BuildError) -> HostError {
    match error {
        BuildError::NoRuntime => HostError::NoRuntime,
        other => HostError::Build(other),
    }
}

/// Sole owning handle for a hosted one-incarnation runner.
#[must_use = "dropping the hosted owner begins configured graceful shutdown"]
pub struct HostedHandle {
    system: Option<System<ScopeRef>>,
    task: TaskRef,
    drop_grace: Duration,
    spawner: crate::runtime::RuntimeSpawner,
}

impl std::fmt::Debug for HostedHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostedHandle")
            .field("membership", &self.task.membership())
            .field("drop_grace", &self.drop_grace)
            .finish_non_exhaustive()
    }
}

impl HostedHandle {
    fn new(system: System<ScopeRef>, task: TaskRef, drop_grace: Duration) -> Self {
        Self {
            system: Some(system),
            task,
            drop_grace,
            spawner: crate::runtime::RuntimeSpawner::current()
                .expect("a hosted system is constructed inside its originating runtime"),
        }
    }

    /// Waits until the hosted incarnation becomes ready.
    pub async fn wait_ready(&self) -> Result<(), HostedReadyError> {
        let system = self
            .system
            .as_ref()
            .expect("an unconsumed hosted handle retains its system");
        match system.wait_started().await {
            Ok(()) => Ok(()),
            Err(_) => Err(HostedReadyError {
                exit: self.task.wait().await,
            }),
        }
    }

    /// Requests shutdown, escalates at `grace`, joins, and returns the child exit.
    pub async fn shutdown(mut self, grace: Duration) -> Result<Exit, crate::ShutdownTimeout> {
        let system = self
            .system
            .take()
            .expect("an unconsumed hosted handle retains its system");
        system.shutdown(grace).await?;
        Ok(self.task.wait().await)
    }

    /// Waits for natural or externally requested completion and returns the child exit.
    pub async fn wait(mut self) -> Exit {
        let system = self
            .system
            .take()
            .expect("an unconsumed hosted handle retains its system");
        let exit = self.task.wait().await;
        let _ = system.shutdown(self.drop_grace).await;
        exit
    }
}

impl Drop for HostedHandle {
    fn drop(&mut self) {
        let Some(system) = self.system.take() else {
            return;
        };
        let grace = self.drop_grace;
        let _ = self.spawner.spawn((), async move {
            let _ = system.shutdown(grace).await;
        });
    }
}
