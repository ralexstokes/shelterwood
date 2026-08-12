use std::{future, time::Duration};

use crate::common::{POLL_TIMEOUT, ReleaseGate, policy::never, poll_until};
use shelterwood::{
    Actor, ActorOnceDef, ChildState, Context, ExitError, ExitKind, ExitResult, GracePhase,
    Readiness, ReadinessDeadline, Shutdown, StartOrShutdownError, StopContext, SubtreeOnceDef,
    TaskDef, TaskRef, Tree,
};

type JournalEvent = (usize, &'static str, &'static str);

/// The sender half handed to children: events reach the host by message
/// passing, so no child ever shares mutable journal state with the test.
#[derive(Clone, Debug)]
struct HostJournal(tokio::sync::mpsc::UnboundedSender<JournalEvent>);

impl HostJournal {
    fn new() -> (Self, JournalLog) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            Self(sender),
            JournalLog {
                receiver,
                entries: Vec::new(),
            },
        )
    }

    fn push(&self, cycle: usize, child: &'static str, event: &'static str) {
        // A host that never reads its log has dropped the receiver.
        let _ = self.0.send((cycle, child, event));
    }
}

/// The host's receiving half of the journal.
struct JournalLog {
    receiver: tokio::sync::mpsc::UnboundedReceiver<JournalEvent>,
    entries: Vec<JournalEvent>,
}

impl JournalLog {
    fn events(&mut self, cycle: usize, child: &'static str) -> Vec<&'static str> {
        while let Ok(event) = self.receiver.try_recv() {
            self.entries.push(event);
        }
        self.entries
            .iter()
            .filter_map(|(entry_cycle, entry_child, event)| {
                (*entry_cycle == cycle && *entry_child == child).then_some(*event)
            })
            .collect()
    }
}

fn cooperative_task(cycle: usize, child: &'static str, journal: HostJournal) -> TaskDef {
    TaskDef::new(move |context| {
        let journal = journal.clone();
        async move {
            journal.push(cycle, child, "started");
            context.shutdown_token().cancelled().await;
            journal.push(cycle, child, "cancelled");
            Ok(())
        }
    })
    .restart(never())
}

fn gated_task(cycle: usize, gate: ReleaseGate, journal: HostJournal) -> TaskDef {
    TaskDef::new(move |context| {
        let gate = gate.clone();
        let journal = journal.clone();
        async move {
            journal.push(cycle, "config", "started");
            gate.wait().await;
            context.mark_ready();
            journal.push(cycle, "config", "ready");
            context.shutdown_token().cancelled().await;
            journal.push(cycle, "config", "cancelled");
            Ok(())
        }
    })
    .restart(never())
    .readiness(Readiness::Manual)
    .expect("manual task readiness is supported")
}

fn stubborn_task(
    cycle: usize,
    child: &'static str,
    policy: Shutdown,
    journal: HostJournal,
) -> TaskDef {
    TaskDef::new(move |context| {
        let journal = journal.clone();
        async move {
            journal.push(cycle, child, "started");
            context.shutdown_token().cancelled().await;
            journal.push(cycle, child, "cancelled");
            context.abort_token().cancelled().await;
            journal.push(cycle, child, "aborted");
            future::pending::<ExitResult>().await
        }
    })
    .restart(never())
    .shutdown(policy)
}

struct ProbeActor {
    cycle: usize,
    journal: HostJournal,
}

impl Actor for ProbeActor {
    type Msg = ();
    type Args = (usize, HostJournal);

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        args.1.push(args.0, "actor", "started");
        Ok(Self {
            cycle: args.0,
            journal: args.1,
        })
    }

    async fn handle(&mut self, (): (), _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {
        self.journal.push(self.cycle, "actor", "stopped");
    }
}

struct SidecarFixture {
    tree: Tree,
    readiness: ReleaseGate,
    abort_task: TaskRef,
    graceful_task: TaskRef,
}

fn sidecar(cycle: usize, journal: HostJournal) -> SidecarFixture {
    let readiness = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "config",
        gated_task(cycle, readiness.clone(), journal.clone()),
    )
    .expect("valid config task");
    tree.add_task(
        "telemetry",
        cooperative_task(cycle, "telemetry", journal.clone()),
    )
    .expect("valid telemetry task");
    let abort_task = tree
        .add_task(
            "abort-worker",
            stubborn_task(cycle, "abort-worker", Shutdown::Abort, journal.clone()),
        )
        .expect("valid abort task");
    let graceful_task = tree
        .add_task(
            "graceful-worker",
            stubborn_task(
                cycle,
                "graceful-worker",
                Shutdown::graceful(Duration::from_millis(5)).expect("grace is non-zero"),
                journal.clone(),
            ),
        )
        .expect("valid graceful task");
    let mut actors = Tree::new();
    actors
        .add_actor_once("probe", ActorOnceDef::<ProbeActor>::new((cycle, journal)))
        .expect("valid probe actor");
    tree.add_subtree_once("actors", SubtreeOnceDef::new(actors))
        .expect("valid actor subtree");
    SidecarFixture {
        tree,
        readiness,
        abort_task,
        graceful_task,
    }
}

#[tokio::test]
async fn sidecar_runs_two_host_owned_cycles_with_readiness_and_policy_exact_shutdown() {
    let (journal, mut log) = HostJournal::new();

    for cycle in 0..2 {
        let fixture = sidecar(cycle, journal.clone());
        let system = fixture.tree.spawn().expect("runtime is available");
        let scope = system.scope();
        assert!(
            poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
                log.events(cycle, "config") == ["started"]
            })
            .await
        );
        let parked = scope.snapshot();
        assert!(matches!(
            parked.child("config").expect("config child").state,
            ChildState::Starting
        ));
        assert!(matches!(
            parked.child("telemetry").expect("telemetry child").state,
            ChildState::Admitted
        ));

        fixture.readiness.release();
        let system = system
            .start_or_shutdown(Duration::from_secs(1).into())
            .await
            .expect("host keeps a successfully started sidecar");
        assert!(
            scope
                .snapshot()
                .children
                .iter()
                .all(|child| { matches!(child.state, ChildState::Running | ChildState::Starting) })
        );
        system
            .shutdown(Duration::from_secs(1).into())
            .await
            .expect("host resolves shutdown before dropping its runtime");

        let abort_exit = fixture.abort_task.wait().await;
        let graceful_exit = fixture.graceful_task.wait().await;
        assert!(matches!(
            abort_exit.kind(),
            ExitKind::Aborted {
                phase: GracePhase::WithinGrace
            }
        ));
        assert!(matches!(
            graceful_exit.kind(),
            ExitKind::Aborted {
                phase: GracePhase::AfterGrace
            }
        ));
        assert_eq!(
            log.events(cycle, "abort-worker"),
            ["started", "cancelled", "aborted"]
        );
        assert_eq!(
            log.events(cycle, "graceful-worker"),
            ["started", "cancelled", "aborted"]
        );
        assert_eq!(log.events(cycle, "actor"), ["started", "stopped"]);
    }
}

struct FailedStartupFixture {
    tree: Tree,
    gate: ReleaseGate,
    log: JournalLog,
}

fn failing_sidecar() -> FailedStartupFixture {
    let (journal, log) = HostJournal::new();
    let gate = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task("config", gated_task(99, gate.clone(), journal.clone()))
        .expect("valid config task");
    tree.add_task(
        "telemetry",
        cooperative_task(99, "telemetry", journal.clone()),
    )
    .expect("valid telemetry task");
    tree.add_task(
        "failing-readiness",
        TaskDef::new(|_| future::pending())
            .restart(never())
            .shutdown(Shutdown::Abort)
            .readiness(Readiness::Manual)
            .expect("manual readiness")
            .readiness_deadline(
                ReadinessDeadline::bounded(Duration::from_millis(5))
                    .expect("non-zero readiness deadline"),
            ),
    )
    .expect("valid failing task");
    tree.add_task(
        "suffix",
        TaskDef::new({
            let journal = journal.clone();
            move |_| {
                journal.push(99, "suffix", "started");
                future::pending()
            }
        })
        .restart(never()),
    )
    .expect("valid suffix task");
    let mut actors = Tree::new();
    actors
        .add_actor_once("probe", ActorOnceDef::<ProbeActor>::new((99, journal)))
        .expect("valid actor subtree");
    tree.add_subtree_once("actors", SubtreeOnceDef::new(actors))
        .expect("valid actor subtree");
    FailedStartupFixture { tree, gate, log }
}

#[tokio::test]
async fn sidecar_startup_failure_leaves_prefix_supervised_until_host_rolls_it_back() {
    let mut fixture = failing_sidecar();
    let system = fixture.tree.spawn().expect("runtime is available");
    let scope = system.scope();
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            fixture.log.events(99, "config") == ["started"]
        })
        .await
    );
    fixture.gate.release();
    let startup = system
        .wait_started()
        .await
        .expect_err("manual readiness times out");
    assert_eq!(fixture.log.events(99, "config"), ["started", "ready"]);
    assert_eq!(fixture.log.events(99, "telemetry"), ["started"]);
    assert!(
        fixture.log.events(99, "suffix").is_empty(),
        "ordered startup never reaches the suffix"
    );
    let snapshot = scope.snapshot();
    assert_eq!(snapshot.state, shelterwood::ScopeState::StartupFailed);
    assert!(matches!(
        snapshot
            .child("failing-readiness")
            .expect("failed child retained")
            .state,
        ChildState::StartupAborted { .. }
    ));
    // The park is the point of the state: the started prefix stays running
    // and supervised until the host decides (Appendix C.2), rather than
    // being eagerly torn down with the failure.
    for prefix in ["config", "telemetry"] {
        assert!(
            matches!(
                snapshot.child(prefix).expect("prefix child resident").state,
                ChildState::Running
            ),
            "started prefix `{prefix}` must stay running through the park"
        );
    }

    let StartOrShutdownError {
        startup: rollback_startup,
        rollback_timeout,
    } = system
        .start_or_shutdown(Duration::from_secs(1).into())
        .await
        .expect_err("host requests rollback after failed startup");
    assert_eq!(rollback_startup, startup);
    assert!(rollback_timeout.is_none());
    assert!(matches!(
        scope.snapshot().state,
        shelterwood::ScopeState::Stopped { .. }
    ));
}
