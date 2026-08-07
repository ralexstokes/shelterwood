#![cfg(feature = "serde")]

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorDef, Backoff, ChildId, Context, DefaultsInheritance, DynamicTree, ExitError,
    ExitResult, Intensity, Jitter, KeyedCapacity, Mailbox, MailboxShutdown, OutlineChildKind,
    OutlineError, OutlineInterior, RawActor, RawContext, RawDef, Readiness, ReadinessDeadline,
    ResolvedMailbox, RestartCondition, RestartPolicy, Retention, ScopeDefaults, ScopeKind,
    Shutdown, StopContext, Strategy, SubtreeDef, SubtreeOnceDef, TaskDef, Tree,
};

struct Callback;

impl Actor for Callback {
    type Msg = (&'static str, usize);
    type Args = ();

    async fn init(_: (), _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, _: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {}
}

struct Raw;

impl RawActor for Raw {
    type Msg = ();

    async fn run(&mut self, _: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

fn never() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
}

#[test]
fn outline_is_a_pure_resolved_declaration_ordered_projection() {
    let factory_calls = Arc::new(AtomicUsize::new(0));
    let mut nested = DynamicTree::new();
    nested.defaults(ScopeDefaults {
        mailbox_shutdown: Some(MailboxShutdown::Discard),
        ..ScopeDefaults::default()
    });
    nested
        .add_raw(
            "nested-raw",
            RawDef::factory(|| Raw)
                .readiness(Readiness::Manual)
                .expect("manual raw readiness is valid"),
        )
        .expect("nested raw id is valid");

    let bounded = ReadinessDeadline::bounded(Duration::from_secs(9)).expect("non-zero deadline");
    let mut tree = Tree::new();
    tree.strategy(Strategy::RestForOne)
        .intensity(Intensity::new(11, Duration::from_secs(17)).expect("valid intensity"))
        .defaults(ScopeDefaults {
            child_restart: Some(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::fixed(Duration::from_millis(4), Jitter::Equal).expect("non-zero backoff"),
            )),
            child_shutdown: Some(Shutdown::Abort),
            mailbox: Some(Mailbox::queue(7).expect("non-zero mailbox")),
            mailbox_shutdown: Some(MailboxShutdown::Drain),
            readiness_deadline: Some(bounded),
        });
    tree.add_actor(
        "callback",
        ActorDef::<Callback>::cloned(())
            .latest_by_key(
                KeyedCapacity::Explicit(NonZeroUsize::new(3).expect("non-zero capacity")),
                |message| message.0,
            )
            .readiness(Readiness::Manual)
            .retention(Retention::Remove),
    )
    .expect("callback id is valid");
    tree.add_raw(
        "raw",
        RawDef::factory(|| Raw)
            .mailbox(Mailbox::latest())
            .shutdown(Shutdown::Graceful {
                grace: Duration::from_millis(23),
            }),
    )
    .expect("raw id is valid");
    tree.add_task("task", TaskDef::new(|_| async { Ok(()) }).restart(never()))
        .expect("task id is valid");
    tree.add_subtree_once(
        "recursive",
        SubtreeOnceDef::new(nested).defaults(DefaultsInheritance::Inherit),
    )
    .expect("one-shot subtree id is valid");
    tree.add_subtree(
        "opaque",
        SubtreeDef::<Tree>::factory({
            let factory_calls = Arc::clone(&factory_calls);
            move || {
                factory_calls.fetch_add(1, Ordering::SeqCst);
                panic!("an outline must not execute a subtree factory")
            }
        })
        .defaults(DefaultsInheritance::Reset),
    )
    .expect("restartable subtree id is valid");

    let outline = tree.outline().expect("tree is fully declared");
    assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    assert_eq!(outline.root.kind, ScopeKind::Ordered);
    assert_eq!(outline.root.strategy, Some(Strategy::RestForOne));
    assert_eq!(
        outline
            .root
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        ["callback", "raw", "task", "recursive", "opaque"]
    );

    let callback = &outline.root.children[0];
    assert_eq!(callback.kind, OutlineChildKind::Actor);
    assert_eq!(callback.readiness, Readiness::Manual);
    assert_eq!(callback.readiness_deadline, bounded);
    assert_eq!(callback.retention, Retention::Remove);
    assert_eq!(
        callback.mailbox,
        Some(ResolvedMailbox::LatestByKey {
            capacity: NonZeroUsize::new(3).expect("non-zero capacity"),
        })
    );

    let raw = &outline.root.children[1];
    assert_eq!(raw.kind, OutlineChildKind::RawActor);
    assert_eq!(raw.mailbox, Some(ResolvedMailbox::Latest));
    assert_eq!(raw.readiness, Readiness::Immediate);

    let task = &outline.root.children[2];
    assert_eq!(task.kind, OutlineChildKind::Task);
    assert_eq!(task.mailbox, None);
    assert_eq!(task.restart, never());

    let recursive = &outline.root.children[3];
    let Some(OutlineInterior::Recursive(nested)) = &recursive.interior else {
        panic!("one-shot subtree must be recursively projected");
    };
    assert_eq!(nested.kind, ScopeKind::Dynamic);
    assert_eq!(nested.strategy, None);
    assert_eq!(nested.children[0].kind, OutlineChildKind::RawActor);
    assert_eq!(
        nested.children[0].mailbox,
        Some(ResolvedMailbox::Queue {
            capacity: NonZeroUsize::new(7).expect("non-zero capacity"),
        })
    );
    assert_eq!(
        nested.children[0].mailbox_shutdown,
        Some(MailboxShutdown::Discard)
    );
    assert_eq!(
        outline.root.children[4].interior,
        Some(OutlineInterior::Opaque)
    );

    let json = serde_json::to_value(&outline).expect("outline serializes");
    let round_trip = serde_json::from_value(json.clone()).expect("outline deserializes");
    assert_eq!(outline, round_trip);

    let mut unknown = json.clone();
    unknown
        .as_object_mut()
        .expect("outline is an object")
        .insert("unexpected".to_owned(), serde_json::Value::Null);
    assert!(
        serde_json::from_value::<shelterwood::Outline>(unknown).is_err(),
        "unknown outline fields must fail loudly"
    );

    let mut missing = json;
    missing["root"]
        .as_object_mut()
        .expect("root is an object")
        .remove("children");
    assert!(
        serde_json::from_value::<shelterwood::Outline>(missing).is_err(),
        "missing outline fields must fail loudly"
    );
}

#[test]
fn outline_reports_every_unfilled_reservation_with_its_full_path() {
    let mut nested = Tree::new();
    nested
        .reserve_task("deep")
        .expect("nested reservation is valid");

    let mut tree = Tree::new();
    tree.reserve_actor::<()>("first")
        .expect("root reservation is valid");
    tree.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("nested id is valid");
    tree.reserve_task("last")
        .expect("root reservation is valid");

    assert_eq!(
        tree.outline(),
        Err(OutlineError::UnfilledReservations {
            paths: vec![
                vec![ChildId::from("first")],
                vec![ChildId::from("nested"), ChildId::from("deep")],
                vec![ChildId::from("last")],
            ],
        })
    );
}

#[derive(Clone, Copy)]
enum OutlineDifference {
    Baseline,
    ScopeDefault,
    MailboxKind,
    MailboxCapacity,
    ReadinessDeadline,
    RestartPolicy,
}

fn one_child_outline(difference: OutlineDifference) -> serde_json::Value {
    let mut tree = Tree::new();
    if matches!(difference, OutlineDifference::ScopeDefault) {
        tree.defaults(ScopeDefaults {
            child_shutdown: Some(Shutdown::Abort),
            ..ScopeDefaults::default()
        });
    }
    let mut definition = RawDef::factory(|| Raw);
    definition = match difference {
        OutlineDifference::MailboxKind => definition.mailbox(Mailbox::latest()),
        OutlineDifference::MailboxCapacity => {
            definition.mailbox(Mailbox::queue(5).expect("non-zero mailbox capacity"))
        }
        OutlineDifference::ReadinessDeadline => definition.readiness_deadline(
            ReadinessDeadline::bounded(Duration::from_secs(3)).expect("non-zero deadline"),
        ),
        OutlineDifference::RestartPolicy => definition.restart(never()),
        OutlineDifference::Baseline | OutlineDifference::ScopeDefault => definition,
    };
    tree.add_raw("worker", definition)
        .expect("worker id is valid");
    serde_json::to_value(tree.outline().expect("tree is complete")).expect("outline serializes")
}

#[test]
fn carried_dimensions_are_injective_and_the_baseline_is_a_golden_example() {
    let variants = [
        OutlineDifference::Baseline,
        OutlineDifference::ScopeDefault,
        OutlineDifference::MailboxKind,
        OutlineDifference::MailboxCapacity,
        OutlineDifference::ReadinessDeadline,
        OutlineDifference::RestartPolicy,
    ]
    .map(one_child_outline);
    for (index, left) in variants.iter().enumerate() {
        for right in &variants[index + 1..] {
            assert_ne!(left, right, "one carried dimension must change the outline");
        }
    }

    assert_eq!(
        variants[0],
        serde_json::json!({
            "root": {
                "kind": "Ordered",
                "strategy": "OneForOne",
                "intensity": {
                    "max_restarts": 5,
                    "within": { "secs": 30, "nanos": 0 }
                },
                "defaults": {
                    "child_restart": {
                        "condition": "OnFailure",
                        "backoff": "Immediate"
                    },
                    "child_shutdown": {
                        "Graceful": { "grace": { "secs": 5, "nanos": 0 } }
                    },
                    "mailbox": { "Queue": { "capacity": 64 } },
                    "mailbox_shutdown": "Drain",
                    "readiness_deadline": {
                        "Bounded": { "secs": 30, "nanos": 0 }
                    }
                },
                "children": [{
                    "id": "worker",
                    "kind": "RawActor",
                    "restart": {
                        "condition": "OnFailure",
                        "backoff": "Immediate"
                    },
                    "shutdown": {
                        "Graceful": { "grace": { "secs": 5, "nanos": 0 } }
                    },
                    "readiness": "Immediate",
                    "readiness_deadline": {
                        "Bounded": { "secs": 30, "nanos": 0 }
                    },
                    "retention": "Retain",
                    "mailbox": { "Queue": { "capacity": 64 } },
                    "mailbox_shutdown": "Drain",
                    "interior": null
                }]
            }
        })
    );
}
