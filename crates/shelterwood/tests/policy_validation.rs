use std::time::Duration;

use shelterwood::{
    Actor, ActorDef, Backoff, BackoffFactor, BuildError, ChildId, Context, DynamicTree, ExitError,
    ExitKind, ExitResult, Intensity, InvalidPolicy, Jitter, PolicyError, PolicyField, RawActor,
    RawContext, RawDef, Readiness, ReadinessDeadline, ReserveError, RestartCondition,
    RestartPolicy, ScopeDefaults, StartupError, StartupFailureCause, SubtreeDef, SubtreeOnceDef,
    TaskDef, Tree,
};

struct InertActor;

impl Actor for InertActor {
    type Msg = ();
    type Args = ();

    async fn init((): (), _context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, (): (), _context: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

struct InertRaw;

impl RawActor for InertRaw {
    type Msg = ();

    async fn run(&mut self, context: &mut RawContext<Self::Msg>) -> ExitResult {
        context.shutdown_token().cancelled().await;
        Ok(())
    }
}

fn zero_fixed_restart() -> RestartPolicy {
    RestartPolicy::new(
        RestartCondition::Always,
        Backoff::Fixed {
            delay: Duration::ZERO,
            jitter: Jitter::None,
        },
    )
}

fn reversed_exponential_restart() -> RestartPolicy {
    RestartPolicy::new(
        RestartCondition::Always,
        Backoff::Exponential {
            base: Duration::from_secs(2),
            factor: BackoffFactor::new(2.0).expect("valid factor"),
            max: Duration::from_secs(1),
            jitter: Jitter::None,
        },
    )
}

fn assert_invalid(invalid: InvalidPolicy, path: &[&str], field: PolicyField, error: PolicyError) {
    assert_eq!(
        invalid.path,
        path.iter().copied().map(ChildId::from).collect::<Vec<_>>()
    );
    assert_eq!(invalid.field, field);
    assert_eq!(invalid.error, error);
}

fn build_invalid(tree: Tree) -> InvalidPolicy {
    match tree.spawn() {
        Err(BuildError::InvalidPolicy(invalid)) => invalid,
        Err(other) => panic!("expected invalid policy, got {other:?}"),
        Ok(_) => panic!("invalid policy unexpectedly built"),
    }
}

fn admission_invalid(error: ReserveError) -> InvalidPolicy {
    match error {
        ReserveError::InvalidPolicy(invalid) => invalid,
        other => panic!("expected invalid policy, got {other:?}"),
    }
}

#[tokio::test]
async fn static_build_revalidates_actor_task_raw_and_subtree_literals() {
    let mut actor = Tree::new();
    actor
        .add_actor(
            "actor",
            ActorDef::<InertActor>::cloned(())
                .readiness_deadline(ReadinessDeadline::Bounded(Duration::ZERO)),
        )
        .expect("valid id");
    assert_invalid(
        build_invalid(actor),
        &["actor"],
        PolicyField::ReadinessDeadline,
        PolicyError::ZeroDuration,
    );

    let mut task = Tree::new();
    task.add_task(
        "task",
        TaskDef::new(|_| async { Ok(()) }).restart(zero_fixed_restart()),
    )
    .expect("valid id");
    assert_invalid(
        build_invalid(task),
        &["task"],
        PolicyField::RestartBackoff,
        PolicyError::ZeroDuration,
    );

    let mut raw = Tree::new();
    raw.add_raw(
        "raw",
        RawDef::factory(|| InertRaw).restart(reversed_exponential_restart()),
    )
    .expect("valid id");
    assert_invalid(
        build_invalid(raw),
        &["raw"],
        PolicyField::RestartBackoff,
        PolicyError::BackoffMaximumBeforeBase,
    );

    let mut subtree = Tree::new();
    subtree
        .add_subtree(
            "subtree",
            SubtreeDef::factory(Tree::new)
                .readiness_deadline(ReadinessDeadline::Bounded(Duration::ZERO)),
        )
        .expect("valid id");
    assert_invalid(
        build_invalid(subtree),
        &["subtree"],
        PolicyField::ReadinessDeadline,
        PolicyError::ZeroDuration,
    );
}

#[tokio::test]
async fn static_build_revalidates_scope_defaults_and_owned_nested_policy() {
    let mut intensity = Tree::new();
    intensity.intensity(Intensity {
        max_restarts: 1,
        within: Duration::ZERO,
    });
    assert_invalid(
        build_invalid(intensity),
        &[],
        PolicyField::Intensity,
        PolicyError::ZeroDuration,
    );

    let mut defaults = Tree::new();
    defaults.defaults(ScopeDefaults {
        child_restart: Some(zero_fixed_restart()),
        ..ScopeDefaults::default()
    });
    assert_invalid(
        build_invalid(defaults),
        &[],
        PolicyField::RestartBackoff,
        PolicyError::ZeroDuration,
    );

    let mut nested = Tree::new();
    nested.intensity(Intensity {
        max_restarts: 1,
        within: Duration::ZERO,
    });
    let mut root = Tree::new();
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid id");
    assert_invalid(
        build_invalid(root),
        &["nested"],
        PolicyField::Intensity,
        PolicyError::ZeroDuration,
    );
}

#[tokio::test]
async fn dynamic_admission_revalidates_actor_task_raw_and_subtree_literals() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let actor = scope
        .add_actor(
            "actor",
            ActorDef::<InertActor>::cloned(())
                .readiness_deadline(ReadinessDeadline::Bounded(Duration::ZERO)),
        )
        .await
        .expect_err("zero actor deadline is rejected");
    assert_invalid(
        admission_invalid(actor),
        &["actor"],
        PolicyField::ReadinessDeadline,
        PolicyError::ZeroDuration,
    );

    let task = scope
        .add_task(
            "task",
            TaskDef::new(|_| async { Ok(()) }).restart(zero_fixed_restart()),
        )
        .await
        .expect_err("zero task backoff is rejected");
    assert_invalid(
        admission_invalid(task),
        &["task"],
        PolicyField::RestartBackoff,
        PolicyError::ZeroDuration,
    );

    let raw = scope
        .add_raw(
            "raw",
            RawDef::factory(|| InertRaw).restart(reversed_exponential_restart()),
        )
        .await
        .expect_err("reversed raw backoff is rejected");
    assert_invalid(
        admission_invalid(raw),
        &["raw"],
        PolicyField::RestartBackoff,
        PolicyError::BackoffMaximumBeforeBase,
    );

    let subtree = scope
        .add_subtree(
            "subtree",
            SubtreeDef::factory(Tree::new)
                .readiness_deadline(ReadinessDeadline::Bounded(Duration::ZERO)),
        )
        .await
        .expect_err("zero subtree deadline is rejected");
    assert_invalid(
        admission_invalid(subtree),
        &["subtree"],
        PolicyField::ReadinessDeadline,
        PolicyError::ZeroDuration,
    );

    let split = scope.reserve_task("split").expect("valid reservation");
    let split = split
        .define(TaskDef::new(|_| async { Ok(()) }).restart(zero_fixed_restart()))
        .await
        .expect_err("split definition validates at admission");
    assert_invalid(
        admission_invalid(split),
        &["split"],
        PolicyField::RestartBackoff,
        PolicyError::ZeroDuration,
    );

    scope
        .reserve_task("split")
        .expect("rejected admission releases the id");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn dynamic_owned_subtree_policy_is_rejected_before_incarnation_start() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let mut nested = Tree::new();
    nested.intensity(Intensity {
        max_restarts: 0,
        within: Duration::ZERO,
    });

    let error = scope
        .add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .await
        .expect_err("owned invalid subtree is rejected at admission");
    assert_invalid(
        admission_invalid(error),
        &["nested"],
        PolicyField::Intensity,
        PolicyError::ZeroDuration,
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn restartable_subtree_factory_output_reports_structured_policy_failure() {
    let mut root = Tree::new();
    root.add_subtree(
        "nested",
        SubtreeDef::factory(|| {
            let mut nested = Tree::new();
            nested.intensity(Intensity {
                max_restarts: 1,
                within: Duration::ZERO,
            });
            nested
        })
        .restart(RestartPolicy::new(
            RestartCondition::Never,
            Backoff::Immediate,
        )),
    )
    .expect("valid subtree edge");
    let system = root
        .spawn()
        .expect("factory output is produced after root lowering");
    let startup = system
        .wait_started()
        .await
        .expect_err("factory output fails policy validation");
    let outer_source =
        std::error::Error::source(&startup).expect("startup error exposes its outer child failure");
    let structured_source = outer_source
        .source()
        .expect("the child exit exposes the nested structured failure");
    let invalid_source = structured_source
        .source()
        .expect("the erased structured error forwards invalid-policy detail");
    assert!(
        invalid_source
            .to_string()
            .contains("invalid restart intensity")
    );
    let StartupError::StartupFailed(failure) = startup else {
        panic!("expected structured startup failure");
    };
    let StartupFailureCause::Child { id, exit, .. } = failure.cause else {
        panic!("root failure must name its nested child");
    };
    assert_eq!(id.as_str(), "nested");
    let ExitKind::Failed(error) = exit.kind() else {
        panic!("nested lowering is a child failure");
    };
    let nested = error
        .startup_failure()
        .expect("framework provenance is retained");
    let StartupFailureCause::InvalidPolicy(invalid) = &nested.cause else {
        panic!("nested failure must retain invalid-policy evidence");
    };
    assert_invalid(
        invalid.clone(),
        &[],
        PolicyField::Intensity,
        PolicyError::ZeroDuration,
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("failed root rolls back");
}

#[tokio::test]
async fn minimum_valid_policy_literals_survive_lowering_and_admission() {
    let factor = BackoffFactor::new(1.0).expect("minimum factor is valid");
    let valid_restart = RestartPolicy::new(
        RestartCondition::Always,
        Backoff::Exponential {
            base: Duration::from_nanos(1),
            factor,
            max: Duration::from_nanos(1),
            jitter: Jitter::None,
        },
    );
    let mut tree = DynamicTree::new();
    tree.intensity(Intensity {
        max_restarts: 0,
        within: Duration::from_nanos(1),
    });
    tree.defaults(ScopeDefaults {
        child_restart: Some(valid_restart),
        readiness_deadline: Some(ReadinessDeadline::Inherit),
        ..ScopeDefaults::default()
    });
    tree.add_actor(
        "actor",
        ActorDef::<InertActor>::cloned(())
            .readiness(Readiness::Immediate)
            .readiness_deadline(ReadinessDeadline::Bounded(Duration::from_nanos(1))),
    )
    .expect("minimum bounded deadline is valid");
    tree.add_raw("raw", RawDef::factory(|| InertRaw).restart(valid_restart))
        .expect("minimum exponential backoff is valid");
    tree.add_task(
        "task",
        TaskDef::new(|context| async move {
            context.shutdown_token().cancelled().await;
            Ok(())
        })
        .restart(RestartPolicy::new(
            RestartCondition::Always,
            Backoff::Fixed {
                delay: Duration::from_nanos(1),
                jitter: Jitter::Equal,
            },
        )),
    )
    .expect("minimum fixed backoff is valid");

    let system = tree.spawn().expect("valid edge policies lower");
    system
        .wait_started()
        .await
        .expect("valid edge policies start");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}
