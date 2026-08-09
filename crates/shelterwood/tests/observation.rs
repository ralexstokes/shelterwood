use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{
    POLL_TIMEOUT, ReleaseGate, assert_quiet, poll_once, poll_until,
    waiting::{task as waiting_task, tree as waiting_tree},
};
use shelterwood::{
    Backoff, ChildState, DynamicScopeRef, DynamicTree, Intensity, Jitter, LIFECYCLE_EVENT_CAPACITY,
    LifecycleEvent, LifecycleEventKind, LifecycleEvents, LifecycleItem, LifecycleSeq,
    LifecycleTryRecvError, MembershipStatus, RemoveOutcome, RestartCondition, RestartPolicy,
    Retention, ScopeKind, ScopeRef, ScopeState, StopReason, Strategy, SubtreeDef, SubtreeOnceDef,
    TaskDef, TaskOnceDef, TaskRef, Tree, WaitError,
};

async fn next_item(events: &mut LifecycleEvents) -> LifecycleItem {
    tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("lifecycle receive is bounded")
        .expect("lifecycle stream remains open")
}

async fn next_event(events: &mut LifecycleEvents) -> LifecycleEvent {
    match next_item(events).await {
        LifecycleItem::Event(event) => event,
        LifecycleItem::Lagged { dropped } => panic!("unexpected lag marker dropping {dropped}"),
        _ => panic!("unexpected future lifecycle item"),
    }
}

fn event_watermark(scope: &ScopeRef, event: &LifecycleEvent) -> Option<LifecycleSeq> {
    let snapshot = scope.snapshot();
    if event.scope == scope.membership() {
        Some(snapshot.lifecycle_seq)
    } else {
        snapshot.watermark(event.scope)
    }
}

#[tokio::test]
async fn event_woken_pull_snapshots_are_consistent_at_first_start_and_final_stop() {
    let mut outer = Tree::new();
    let nested = outer
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .expect("valid subtree");
    let initial = nested.snapshot();
    assert_eq!(initial.state, ScopeState::Unstarted);
    assert_eq!(initial.lifecycle_seq.get(), 0);

    // Deliberately create no snapshot receiver: the pull path must not depend
    // on watch publication having any subscribers.
    let mut events = nested.subscribe_lifecycle();
    let system = outer.spawn().expect("runtime is available");
    let nested_membership = nested.membership();

    loop {
        let event = next_event(&mut events).await;
        assert_eq!(event.scope, nested_membership);
        assert!(
            event_watermark(&nested, &event).is_some_and(|watermark| watermark >= event.seq),
            "snapshot read synchronously from the event arm must reflect the event"
        );
        if matches!(
            event.kind,
            LifecycleEventKind::ScopeState {
                state: ScopeState::Starting
            }
        ) {
            break;
        }
    }

    system.wait_started().await.expect("tree starts");
    let root = system.scope();
    assert_eq!(
        root.snapshot()
            .child("nested")
            .expect("nested membership remains resident")
            .membership,
        nested_membership
    );
    let shutdown = tokio::spawn(async move { system.shutdown(Duration::from_secs(1)).await });
    let mut saw_final_stop = false;
    while let Some(item) = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("terminal lifecycle receive is bounded")
    {
        let LifecycleItem::Event(event) = item else {
            panic!("small lifecycle fixture must not lag");
        };
        assert!(event_watermark(&nested, &event).is_some_and(|watermark| watermark >= event.seq));
        if matches!(
            event.kind,
            LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped { .. }
            }
        ) {
            saw_final_stop = true;
        }
    }
    assert!(
        saw_final_stop,
        "closure is preceded by the final scope event"
    );
    assert!(matches!(
        nested.snapshot().state,
        ScopeState::Stopped { .. }
    ));
    shutdown
        .await
        .expect("shutdown task joins")
        .expect("tree shuts down");
}

async fn admit_waiter(scope: &DynamicScopeRef, id: String) {
    scope
        .add_task(id, waiting_task())
        .await
        .expect("task is admitted");
}

async fn drain_added_started_ready(events: &mut LifecycleEvents) {
    for expected in ["added", "started", "ready"] {
        let event = next_event(events).await;
        let actual = match event.kind {
            LifecycleEventKind::Added { .. } => "added",
            LifecycleEventKind::Started { .. } => "started",
            LifecycleEventKind::Ready { .. } => "ready",
            other => panic!("unexpected event while draining fast subscriber: {other:?}"),
        };
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn lifecycle_lag_is_exact_coalesced_per_episode_and_subscribers_are_isolated() {
    const EVENTS_PER_ADMISSION: usize = 3;

    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let mut slow = scope.subscribe_lifecycle();
    let mut fast = scope.subscribe_lifecycle();

    let admissions_per_episode = LIFECYCLE_EVENT_CAPACITY.div_ceil(EVENTS_PER_ADMISSION) + 1;
    let emitted_per_episode = admissions_per_episode * EVENTS_PER_ADMISSION;
    let dropped_per_episode = emitted_per_episode - LIFECYCLE_EVENT_CAPACITY;

    for episode in 0..2 {
        for index in 0..admissions_per_episode {
            admit_waiter(&scope, format!("worker-{episode}-{index}")).await;
            drain_added_started_ready(&mut fast).await;
        }

        let watermark = scope.snapshot().lifecycle_seq;
        assert_eq!(
            next_item(&mut slow).await,
            LifecycleItem::Lagged {
                dropped: dropped_per_episode as u64,
            },
            "each overflow episode reports its capacity-derived exact drop count"
        );
        for _ in 0..LIFECYCLE_EVENT_CAPACITY {
            let LifecycleItem::Event(event) = next_item(&mut slow).await else {
                panic!("one coalesced marker must lead each overflow episode");
            };
            assert!(event.seq <= watermark, "post-lag snapshot is a watermark");
        }
        assert_eq!(slow.try_recv(), Err(LifecycleTryRecvError::Empty));
        assert_eq!(fast.try_recv(), Err(LifecycleTryRecvError::Empty));
    }

    drop(slow);
    drop(fast);
    system
        .shutdown(Duration::from_secs(2))
        .await
        .expect("all waiting tasks shut down");
}

#[tokio::test]
async fn catch_up_watermarks_dedupe_initial_events_discard_stale_scopes_and_introduce_new_ones() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let root = system.scope();
    let mut events = root.subscribe_lifecycle();
    let before = root.snapshot();
    assert!(before.child("nested").is_none());

    let nested = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("subtree admitted")
        .into_handles();
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            root.snapshot()
                .child("nested")
                .is_some_and(|child| matches!(child.state, ChildState::Running))
        })
        .await
    );

    // Prescribed acquisition order: subscribe first, snapshot second. Every
    // item already queued is reflected by the recursive watermark.
    let caught_up = root.snapshot();
    let mut initial = Vec::new();
    while let Ok(LifecycleItem::Event(event)) = events.try_recv() {
        let watermark = if event.scope == root.membership() {
            Some(caught_up.lifecycle_seq)
        } else {
            caught_up.watermark(event.scope)
        };
        assert!(watermark.is_some_and(|watermark| event.seq <= watermark));
        initial.push(event);
    }
    assert!(initial.iter().any(|event| {
        matches!(
            event.kind,
            LifecycleEventKind::Added { membership, .. }
                if membership == nested.membership()
        )
    }));

    assert_eq!(root.remove_scope(&nested).await, RemoveOutcome::Removed);
    let after_removal = root.snapshot();
    assert!(after_removal.child("nested").is_none());
    let mut saw_stale_descendant = false;
    while let Ok(LifecycleItem::Event(event)) = events.try_recv() {
        if event.scope == nested.membership() {
            saw_stale_descendant = true;
            assert_eq!(after_removal.watermark(event.scope), None);
        } else {
            assert!(event.seq <= after_removal.lifecycle_seq);
        }
    }
    assert!(saw_stale_descendant);

    // A membership absent from the old snapshot becomes applicable only via
    // its post-watermark causal introduction.
    let replacement = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("replacement admitted")
        .into_handles();
    assert_ne!(replacement.membership(), nested.membership());
    let mut introduced = false;
    loop {
        let event = next_event(&mut events).await;
        if event.scope == root.membership()
            && matches!(
                event.kind,
                LifecycleEventKind::Added { membership, .. }
                    if membership == replacement.membership()
            )
        {
            introduced = true;
        }
        if event.scope == replacement.membership() {
            assert!(introduced, "Added causally introduces a new scope token");
            break;
        }
    }

    assert_eq!(
        root.remove_scope(&replacement).await,
        RemoveOutcome::Removed
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root shuts down");
}

#[tokio::test]
async fn removed_is_the_pruning_edge_not_the_retained_terminal_edge() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let root = system.scope();
    let mut events = root.subscribe_lifecycle();
    let (task, completion) = root
        .add_task_once(
            "retained",
            TaskOnceDef::new(|_| async { Ok(()) }).retention(Retention::Retain),
        )
        .await
        .expect("retained task admitted")
        .into_handles();
    drop(completion);
    let membership = task.membership();
    task.wait().await;
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            root.snapshot()
                .child("retained")
                .is_some_and(|child| child.state.is_terminal())
        })
        .await
    );

    let mut saw_exit = false;
    let mut saw_restart = false;
    while let Ok(LifecycleItem::Event(event)) = events.try_recv() {
        if matches!(
            event.kind,
            LifecycleEventKind::Exited {
                membership: event_membership,
                ..
            } if event_membership == membership
        ) {
            saw_exit = true;
        }
        if matches!(
            event.kind,
            LifecycleEventKind::RestartScheduled {
                membership: event_membership,
                ..
            } if event_membership == membership
        ) {
            saw_restart = true;
        }
        assert!(
            !matches!(
                event.kind,
                LifecycleEventKind::Removed {
                    membership: event_membership,
                    ..
                } if event_membership == membership
            ),
            "a retained terminal membership has not been pruned"
        );
    }
    assert!(saw_exit);
    assert!(!saw_restart, "planned remove/add is not a crash restart");

    assert_eq!(root.remove_task(&task).await, RemoveOutcome::Removed);
    let removed = loop {
        let event = next_event(&mut events).await;
        if matches!(
            event.kind,
            LifecycleEventKind::Removed {
                membership: event_membership,
                ..
            } if event_membership == membership
        ) {
            break event;
        }
    };
    assert_eq!(removed.scope, root.membership());
    assert!(root.snapshot().child("retained").is_none());

    let (teardown_task, teardown_completion) = root
        .add_task_once(
            "teardown-tombstone",
            TaskOnceDef::new(|_| async { Ok(()) }).retention(Retention::Retain),
        )
        .await
        .expect("teardown tombstone admitted")
        .into_handles();
    drop(teardown_completion);
    let teardown_membership = teardown_task.membership();
    teardown_task.wait().await;
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            root.snapshot()
                .child("teardown-tombstone")
                .is_some_and(|child| child.state.is_terminal())
        })
        .await
    );
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root shuts down");
    let mut saw_teardown_prune = false;
    let mut saw_scope_stop = false;
    while let Some(item) = events.recv().await {
        let LifecycleItem::Event(event) = item else {
            panic!("small retention fixture must not lag");
        };
        if matches!(
            event.kind,
            LifecycleEventKind::Removed {
                membership: event_membership,
                ..
            } if event_membership == teardown_membership
        ) {
            saw_teardown_prune = true;
        }
        if matches!(
            event.kind,
            LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped { .. }
            }
        ) {
            assert!(saw_teardown_prune, "scope teardown prunes tombstones first");
            saw_scope_stop = true;
        }
    }
    assert!(saw_scope_stop);
    assert!(root.snapshot().child("teardown-tombstone").is_none());
}

#[tokio::test]
async fn descendant_events_forward_with_origin_identity_path_and_causal_order() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let root = system.scope();
    let mut events = root.subscribe_lifecycle();

    let nested = root
        .add_subtree_once("nested", SubtreeOnceDef::new(waiting_tree()))
        .await
        .expect("subtree admitted")
        .into_handles();
    let nested_membership = nested.membership();
    let root_membership = root.membership();

    let mut seen = Vec::new();
    loop {
        let event = next_event(&mut events).await;
        let ready = matches!(
            event.kind,
            LifecycleEventKind::Ready { membership, .. } if membership == nested_membership
        );
        seen.push(event);
        if ready {
            break;
        }
    }

    let added = seen
        .iter()
        .position(|event| {
            event.scope == root_membership
                && matches!(
                    event.kind,
                    LifecycleEventKind::Added { membership, .. }
                        if membership == nested_membership
                )
        })
        .expect("parent Added is present");
    let first_inside = seen
        .iter()
        .position(|event| event.scope == nested_membership)
        .expect("nested origin is forwarded");
    assert!(
        added < first_inside,
        "causal introduction precedes inside events"
    );
    for event in seen.iter().filter(|event| event.scope == nested_membership) {
        assert_eq!(
            event
                .scope_path
                .iter()
                .map(|id| id.as_str())
                .collect::<Vec<_>>(),
            ["nested"]
        );
    }

    let mut last_by_origin = HashMap::new();
    for event in &seen {
        if let Some(previous) = last_by_origin.insert(event.scope, event.seq) {
            assert_eq!(
                event.seq.get(),
                previous.get() + 1,
                "origin sequences stay gap-free"
            );
        }
    }
    let snapshot = root.snapshot();
    let nested_child = snapshot.child("nested").expect("nested child is resident");
    let recursive = nested_child.nested.as_ref().expect("nested scope is live");
    assert_eq!(nested_child.scope_seq, Some(recursive.lifecycle_seq));

    let mut nested_events = nested.subscribe_lifecycle();
    let removal = root.remove_scope(&nested);
    let mut nested_stopped_before_parent_removed = false;
    loop {
        let event = next_event(&mut events).await;
        if event.scope == nested_membership
            && matches!(
                event.kind,
                LifecycleEventKind::ScopeState {
                    state: ScopeState::Stopped { .. }
                }
            )
        {
            nested_stopped_before_parent_removed = true;
        }
        if event.scope == root_membership
            && matches!(
                event.kind,
                LifecycleEventKind::Removed { membership, .. }
                    if membership == nested_membership
            )
        {
            assert!(nested_stopped_before_parent_removed);
            break;
        }
    }
    assert_eq!(removal.await, RemoveOutcome::Removed);
    while nested_events.recv().await.is_some() {}
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root shuts down");
}

#[tokio::test(start_paused = true)]
async fn subtree_restart_keeps_scope_stream_and_sequence_but_refreshes_descendant_memberships() {
    let builds = Arc::new(AtomicUsize::new(0));
    let mut outer = Tree::new();
    let nested = outer
        .add_subtree(
            "nested",
            SubtreeDef::factory({
                let builds = Arc::clone(&builds);
                move || {
                    let mut tree = Tree::new();
                    if builds.fetch_add(1, Ordering::SeqCst) == 0 {
                        let attempts = Arc::new(AtomicUsize::new(0));
                        tree.add_task(
                            "worker",
                            TaskDef::new(move |_| {
                                let fail = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                                async move {
                                    if fail {
                                        Err(shelterwood::ExitError::message("retry once"))
                                    } else {
                                        Ok(())
                                    }
                                }
                            })
                            .restart(RestartPolicy::new(
                                RestartCondition::OnFailure,
                                Backoff::Immediate,
                            )),
                        )
                        .expect("valid restarting task");
                    } else {
                        tree.add_task("worker", waiting_task())
                            .expect("valid waiting task");
                    }
                    tree
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::fixed(Duration::from_secs(10), Jitter::None).expect("valid backoff"),
            )),
        )
        .expect("valid subtree");
    let scope_membership = nested.membership();
    let mut events = nested.subscribe_lifecycle();
    let system = outer.spawn().expect("runtime is available");
    let root = system.scope();

    let mut first_child = None;
    let stopped_seq = loop {
        let event = next_event(&mut events).await;
        assert_eq!(event.scope, scope_membership);
        if let LifecycleEventKind::Added { membership, .. } = event.kind {
            first_child.get_or_insert(membership);
        }
        if matches!(
            event.kind,
            LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped {
                    reason: StopReason::Finished
                }
            }
        ) {
            break event.seq;
        }
    };

    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            root.snapshot()
                .child("nested")
                .is_some_and(|child| matches!(child.state, ChildState::Restarting))
        })
        .await
    );
    let restart_window = root.snapshot();
    let child = restart_window
        .child("nested")
        .expect("membership remains resident");
    assert!(matches!(child.state, ChildState::Restarting));
    assert!(child.incarnation.is_none());
    assert!(child.nested.is_none());
    assert!(child.restart_at.is_some());
    assert_eq!(child.restart_count, 1);
    assert_eq!(restart_window.total_restarts, 1);
    assert_eq!(child.scope_seq, Some(stopped_seq));
    assert_eq!(nested.snapshot().total_restarts, 1);
    assert_eq!(
        restart_window
            .descendant(["nested"])
            .map(|child| child.id.as_str()),
        Some("nested"),
        "a path may end at a scope child inside its restart window"
    );
    assert!(
        restart_window.descendant(["nested", "worker"]).is_none(),
        "a restart window has no nested snapshot to advance past"
    );

    tokio::time::advance(Duration::from_secs(10)).await;
    let starting = loop {
        let event = next_event(&mut events).await;
        if matches!(
            event.kind,
            LifecycleEventKind::ScopeState {
                state: ScopeState::Starting
            }
        ) {
            break event;
        }
    };
    assert_eq!(starting.seq.get(), stopped_seq.get() + 1);
    assert_eq!(starting.scope, scope_membership);

    let second_child = loop {
        let event = next_event(&mut events).await;
        if let LifecycleEventKind::Added { membership, .. } = event.kind {
            break membership;
        }
    };
    let first_child = first_child.expect("first incarnation admits its child");
    assert!(
        second_child.supersedes(first_child),
        "corresponding descendants retain ordering across a scope restart"
    );
    assert_eq!(nested.membership(), scope_membership);
    assert_eq!(nested.snapshot().total_restarts, 0);
    assert!(nested.snapshot().lifecycle_seq >= starting.seq);
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            root.snapshot().child("nested").is_some_and(|child| {
                matches!(child.state, ChildState::Running) && child.restart_at.is_none()
            })
        })
        .await
    );

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test]
async fn rebased_declared_handles_and_incarnations_keep_identity() {
    fn hashed(value: &impl std::hash::Hash) -> u64 {
        use std::hash::Hasher;
        let mut hasher = std::hash::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    let stash: Arc<Mutex<Vec<(TaskRef, u64, shelterwood::Membership)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let builds = Arc::new(AtomicUsize::new(0));
    let mut outer = Tree::new();
    let nested = outer
        .add_subtree(
            "nested",
            SubtreeDef::factory({
                let stash = Arc::clone(&stash);
                let builds = Arc::clone(&builds);
                move || {
                    let mut tree = Tree::new();
                    let handle = if builds.fetch_add(1, Ordering::SeqCst) == 0 {
                        tree.add_task("worker", TaskDef::new(|_| async { Ok(()) }))
                            .expect("valid finishing task")
                    } else {
                        tree.add_task("worker", waiting_task())
                            .expect("valid waiting task")
                    };
                    // Capture identity before lowering rebases a rebuilt
                    // declaration onto the stable scope.
                    stash.lock().expect("stash mutex intact").push((
                        handle.clone(),
                        hashed(&handle),
                        handle.membership(),
                    ));
                    tree
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Immediate,
            )),
        )
        .expect("valid subtree");
    let mut events = nested.subscribe_lifecycle();
    let system = outer.spawn().expect("runtime is available");

    let mut admissions = Vec::new();
    let mut starts = Vec::new();
    while admissions.len() < 2 || starts.len() < 2 {
        match next_event(&mut events).await.kind {
            LifecycleEventKind::Added { membership, .. } => admissions.push(membership),
            LifecycleEventKind::Started {
                membership,
                incarnation,
                ..
            } => starts.push((membership, incarnation)),
            _ => {}
        }
    }

    let (first, first_hash, first_declared) = {
        let stash = stash.lock().expect("stash mutex intact");
        assert_eq!(stash.len(), 2);
        stash[0].clone()
    };
    let (second, second_hash, second_declared) =
        stash.lock().expect("stash mutex intact")[1].clone();

    assert_eq!(first.membership(), first_declared);
    assert_eq!(hashed(&first), first_hash);
    assert!(
        second.membership().supersedes(first.membership()),
        "the rebuilt declaration is rebased onto the stable scope"
    );
    assert_ne!(second.membership(), second_declared);
    assert_eq!(
        hashed(&second),
        second_hash,
        "handle identity survives the rebase that refreshed its membership"
    );
    assert_eq!(second.membership(), admissions[1]);
    assert_eq!(starts[1].0, second.membership());
    assert_eq!(starts[1].1.membership(), second.membership());

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test]
async fn wait_for_child_handles_later_ids_terminal_children_timeouts_and_scope_termination() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let waiting_scope = scope.clone();
    let waiter = tokio::spawn(async move {
        waiting_scope
            .wait_for_child(
                "later",
                |child| matches!(child.state, ChildState::Running),
                Duration::from_secs(1),
            )
            .await
    });
    let admitted = scope
        .add_task("later", waiting_task())
        .await
        .expect("later id is admitted")
        .into_handles();
    let running = waiter
        .await
        .expect("wait task joins")
        .expect("later child matches");
    assert_eq!(running.membership, admitted.membership());

    assert_eq!(
        scope
            .wait_for_child("missing", |_| true, Duration::ZERO)
            .await,
        Err(WaitError::TimedOut)
    );
    assert_eq!(
        scope
            .wait_for_child("still-missing", |_| true, Duration::from_millis(5))
            .await,
        Err(WaitError::TimedOut)
    );

    let terminal = scope
        .add_task_once(
            "terminal",
            TaskOnceDef::new(|_| async { Ok(()) }).retention(Retention::Retain),
        )
        .await
        .expect("one-shot task admitted")
        .into_handles()
        .0;
    let terminal_snapshot = scope
        .wait_for_child(
            "terminal",
            |child| child.state.is_terminal(),
            Duration::from_secs(1),
        )
        .await
        .expect("terminal child is delivered to the predicate");
    assert_eq!(terminal_snapshot.membership, terminal.membership());

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root shuts down");

    let mut declaration = Tree::new();
    let never_spawned = declaration
        .add_subtree_once("nested", SubtreeOnceDef::new(Tree::new()))
        .expect("valid subtree");
    drop(declaration);
    assert!(matches!(
        never_spawned
            .wait_for_child("missing", |_| false, Duration::from_secs(1))
            .await,
        Err(WaitError::ScopeTerminated {
            state: ScopeState::Stopped {
                reason: StopReason::NeverStarted
            }
        })
    ));
}

#[tokio::test]
async fn undefined_dynamic_reservations_are_absent_and_emit_no_membership_edges() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let root = system.scope();
    let mut events = root.subscribe_lifecycle();

    let reserved = root.reserve_task("reserved").expect("id is reserved");
    assert!(root.snapshot().child("reserved").is_none());
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));
    drop(reserved);
    assert!(root.snapshot().child("reserved").is_none());
    assert_eq!(events.try_recv(), Err(LifecycleTryRecvError::Empty));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root shuts down");
}

#[tokio::test]
async fn withdrawn_queued_admission_never_publishes_an_added_child() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let root = system.scope();
    let mut events = root.subscribe_lifecycle();
    let slot = root
        .reserve_task("withdrawn")
        .expect("reservation succeeds");
    let membership = slot.task_ref().membership();
    let removal = root.remove("withdrawn");
    let admission = slot.define(waiting_task());

    assert_eq!(removal.await, RemoveOutcome::Removed);
    assert!(matches!(
        admission.await,
        Err(shelterwood::ReserveError::NotAdmitting(
            shelterwood::NotAdmittingCause::ReservationEnded
        ))
    ));
    assert!(root.snapshot().child("withdrawn").is_none());
    while let Ok(item) = events.try_recv() {
        if let LifecycleItem::Event(event) = item {
            assert!(!matches!(
                event.kind,
                LifecycleEventKind::Added {
                    membership: added,
                    ..
                } if added == membership
            ));
        }
    }

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test]
async fn unspawned_scope_publishes_terminal_values_before_both_observers_close() {
    let mut declaration = Tree::new();
    let nested = declaration
        .add_subtree_once("nested", SubtreeOnceDef::new(Tree::new()))
        .expect("valid subtree");
    let mut snapshots = nested.subscribe_snapshots();
    let mut events = nested.subscribe_lifecycle();
    assert_eq!(snapshots.borrow_latest().state, ScopeState::Unstarted);

    drop(declaration);

    let event = next_event(&mut events).await;
    assert!(matches!(
        event.kind,
        LifecycleEventKind::ScopeState {
            state: ScopeState::Stopped {
                reason: StopReason::NeverStarted
            }
        }
    ));
    assert!(events.recv().await.is_none());
    let terminal = snapshots
        .changed()
        .await
        .expect("final snapshot is published");
    assert!(matches!(
        terminal.state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    ));
    assert!(snapshots.changed().await.is_err());
    assert_eq!(snapshots.borrow_latest(), terminal);
}

/// A hard-aborted scope incarnation still pairs every `Added` with its
/// terminal edges: the active child's `Exited` and `Removed` are published
/// before the scope's final event, no snapshot row of a stopped scope keeps
/// a live incarnation, and the member's handles resolve terminal (§3.2's
/// exact pairing; §13.7 on the abort path).
#[tokio::test]
async fn hard_aborted_scope_pairs_added_with_exited_and_removed() {
    let mut nested = Tree::new();
    let stuck = nested
        .add_task(
            "stuck",
            TaskDef::new(|_| std::future::pending()).shutdown(shelterwood::Shutdown::Graceful {
                grace: Duration::from_secs(30),
            }),
        )
        .expect("valid stuck task");
    let mut root = Tree::new();
    let sub = root
        .add_subtree_once(
            "nested",
            SubtreeOnceDef::new(nested).shutdown(shelterwood::Shutdown::Abort),
        )
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let mut events = sub.subscribe_lifecycle();
    system
        .shutdown(Duration::from_secs(5))
        .await
        .expect("the subtree's abort policy bounds teardown");

    let stuck_membership = stuck.membership();
    let mut saw_exited = false;
    let mut saw_removed = false;
    loop {
        let event = next_event(&mut events).await;
        match &event.kind {
            LifecycleEventKind::Exited { membership, .. } if *membership == stuck_membership => {
                assert!(!saw_removed, "Exited precedes Removed");
                saw_exited = true;
            }
            LifecycleEventKind::Removed { membership, .. } if *membership == stuck_membership => {
                saw_removed = true;
            }
            LifecycleEventKind::ScopeState {
                state: ScopeState::Stopped { .. },
                ..
            } => {
                break;
            }
            _ => {}
        }
    }
    assert!(saw_exited, "the aborted child's exit is published");
    assert!(saw_removed, "the aborted child's membership is pruned");

    let snapshot = sub.snapshot();
    assert!(matches!(snapshot.state, ScopeState::Stopped { .. }));
    for child in snapshot.children.iter() {
        assert!(
            child.incarnation.is_none(),
            "a stopped scope never shows a live incarnation: {child:?}"
        );
    }
    let exit = tokio::time::timeout(Duration::from_secs(1), stuck.wait())
        .await
        .expect("hard-aborted descendants terminalize");
    assert!(matches!(exit.kind(), shelterwood::ExitKind::Aborted { .. }));
}

/// A member that never spawned is the plain `Stopped { NeverStarted }`
/// terminal (B.6) — `StartupAborted` is reserved for the §6 case: a
/// membership that ran and failed before its initial readiness edge.
#[tokio::test]
async fn never_ran_members_stop_rather_than_report_startup_abort() {
    let mut tree = Tree::new();
    tree.add_task(
        "failing-readiness",
        TaskDef::new(|_| async { Err(shelterwood::ExitError::message("fails before ready")) })
            .readiness(shelterwood::Readiness::Manual)
            .expect("manual readiness")
            .restart(RestartPolicy::new(
                RestartCondition::Never,
                Backoff::Immediate,
            )),
    )
    .expect("valid failing task");
    tree.add_task("never-ran", waiting_task())
        .expect("valid suffix task");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    system
        .wait_started()
        .await
        .expect_err("pre-ready terminal exit aborts startup");
    scope
        .wait_for_child(
            "never-ran",
            |child| matches!(child.state, ChildState::Stopped { .. }),
            Duration::from_secs(1),
        )
        .await
        .expect("joined suffix disposal publishes terminality");
    let snapshot = scope.snapshot();
    assert!(matches!(
        snapshot
            .child("failing-readiness")
            .expect("aborted child resident")
            .state,
        ChildState::StartupAborted { .. }
    ));
    assert!(
        matches!(
            &snapshot
                .child("never-ran")
                .expect("never-ran child resident")
                .state,
            ChildState::Stopped { exit } if matches!(exit.kind(), shelterwood::ExitKind::NeverStarted)
        ),
        "a never-ran terminal is Stopped, not StartupAborted: {:?}",
        snapshot.child("never-ran")
    );
}

/// A descendant path may end at any child kind: a direct task, a leaf actor
/// through a scope, or a scope itself — the nested snapshot is required only
/// to advance past a component, never of the final one.
#[tokio::test]
async fn descendant_resolves_leaf_and_scope_path_endings() {
    let mut nested = Tree::new();
    nested
        .add_task("worker", waiting_task())
        .expect("valid nested task");
    let mut root = Tree::new();
    root.add_task("direct", waiting_task())
        .expect("valid direct task");
    root.add_subtree_once("nested", SubtreeOnceDef::new(nested))
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("tree starts");
    let snapshot = system.scope().snapshot();
    assert_eq!(
        snapshot
            .descendant(["direct"])
            .expect("a path may end at a direct task")
            .id
            .as_str(),
        "direct"
    );
    assert_eq!(
        snapshot
            .descendant(["nested"])
            .expect("a path may end at a scope")
            .id
            .as_str(),
        "nested"
    );
    assert_eq!(
        snapshot
            .descendant(["nested", "worker"])
            .expect("a path may end at a leaf below a scope")
            .id
            .as_str(),
        "worker"
    );
    assert!(snapshot.descendant(["nested", "missing"]).is_none());
    assert!(snapshot.descendant(["direct", "too-deep"]).is_none());
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

/// A wait deadline too large for the clock is a deadline that never
/// arrives — not one that is already due.
#[tokio::test(start_paused = true)]
async fn wait_for_child_with_a_far_future_deadline_stays_pending() {
    let gate = ReleaseGate::default();
    let mut tree = Tree::new();
    tree.add_task(
        "gated",
        TaskDef::new({
            let gate = gate.clone();
            move |context| {
                let gate = gate.clone();
                async move {
                    gate.wait().await;
                    context.mark_ready();
                    context.shutdown_token().cancelled().await;
                    Ok(())
                }
            }
        })
        .readiness(shelterwood::Readiness::Manual)
        .expect("manual readiness"),
    )
    .expect("valid task");
    let system = tree.spawn().expect("runtime is available");
    let scope = system.scope();
    let mut waiter = Box::pin(scope.wait_for_child(
        "gated",
        |child| matches!(child.state, ChildState::Running),
        Duration::MAX,
    ));
    assert!(
        poll_once(waiter.as_mut()).is_pending(),
        "an unsatisfied far-future wait is registered rather than timing out"
    );
    gate.release();
    waiter
        .await
        .expect("a far-future deadline waits for the condition instead of expiring");
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("tree shuts down");
}

#[tokio::test(start_paused = true)]
async fn snapshot_subscriptions_conflate_unobserved_transitions_to_the_latest_value() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let mut snapshots = scope.subscribe_snapshots();

    let task = scope
        .add_task(
            "ephemeral",
            TaskDef::new(|_| async { Ok(()) })
                .restart(RestartPolicy::new(
                    RestartCondition::Never,
                    Backoff::Immediate,
                ))
                .retention(Retention::Remove),
        )
        .await
        .expect("ephemeral task is admitted")
        .into_handles();
    task.wait().await;
    assert!(
        poll_until(POLL_TIMEOUT, Duration::from_millis(1), || {
            scope.child("ephemeral").is_none()
        })
        .await,
        "terminal removal reaches the latest snapshot"
    );

    let latest = snapshots
        .changed()
        .await
        .expect("one notification exposes the newest conflated snapshot");
    assert!(latest.child("ephemeral").is_none());
    let mut next = Box::pin(snapshots.changed());
    assert_quiet(Duration::from_secs(1), || {
        poll_once(next.as_mut()).is_ready()
    })
    .await;

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

#[tokio::test(start_paused = true)]
async fn lifecycle_subscriptions_start_now_without_replaying_prior_history() {
    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    let old = scope
        .add_task("old", waiting_task())
        .await
        .expect("old membership is admitted")
        .into_handles();
    assert_eq!(scope.remove_task(&old).await, RemoveOutcome::Removed);

    let mut events = scope.subscribe_lifecycle();
    assert_quiet(Duration::from_secs(1), || {
        !matches!(events.try_recv(), Err(LifecycleTryRecvError::Empty))
    })
    .await;

    let new = scope
        .add_task("new", waiting_task())
        .await
        .expect("new membership is admitted")
        .into_handles();
    let event = next_event(&mut events).await;
    assert!(matches!(
        event.kind,
        LifecycleEventKind::Added { ref id, membership }
            if id.as_str() == "new" && membership == new.membership()
    ));

    assert_eq!(scope.remove_task(&new).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
}

/// One representative tree exercises the full projected snapshot shape at
/// once: dynamic and ordered scope kinds, strategy, intensity budgets,
/// per-child restart policy and retention, active versus removing membership,
/// a child held mid-stop, and recursive scope rows with their watermarks.
#[tokio::test]
async fn end_to_end_snapshot_projects_kinds_policies_membership_status_and_stopping() {
    let worker_policy = RestartPolicy::new(
        RestartCondition::Always,
        Backoff::fixed(Duration::from_secs(5), Jitter::None).expect("valid backoff"),
    );
    let mut nested = Tree::new();
    nested.strategy(Strategy::OneForOne);
    nested.intensity(Intensity::new(3, Duration::from_secs(45)).expect("valid intensity"));
    nested
        .add_task(
            "worker",
            waiting_task()
                .restart(worker_policy)
                .retention(Retention::Retain),
        )
        .expect("valid nested task");
    let mut root = DynamicTree::new();
    root.intensity(Intensity::new(7, Duration::from_secs(90)).expect("valid intensity"));
    let nested_scope = root
        .add_subtree_once(
            "nested",
            SubtreeOnceDef::new(nested).retention(Retention::Retain),
        )
        .expect("valid subtree");
    let system = root.spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();

    let stop_entered = ReleaseGate::default();
    let release_stop = ReleaseGate::default();
    let departing = scope
        .add_task(
            "departing",
            TaskDef::new({
                let stop_entered = stop_entered.clone();
                let release_stop = release_stop.clone();
                move |context| {
                    let stop_entered = stop_entered.clone();
                    let release_stop = release_stop.clone();
                    async move {
                        context.shutdown_token().cancelled().await;
                        stop_entered.release();
                        release_stop.wait().await;
                        Ok(())
                    }
                }
            })
            .restart(RestartPolicy::new(
                RestartCondition::Never,
                Backoff::Immediate,
            ))
            .retention(Retention::Remove)
            .shutdown(shelterwood::Shutdown::Graceful {
                grace: Duration::from_secs(30),
            }),
        )
        .await
        .expect("departing task is admitted")
        .into_handles();
    scope
        .wait_for_child(
            "departing",
            |child| matches!(child.state, ChildState::Running),
            POLL_TIMEOUT,
        )
        .await
        .expect("departing child runs");

    let running = scope.snapshot();
    assert_eq!(running.state, ScopeState::Running);
    assert_eq!(running.kind, ScopeKind::Dynamic);
    assert_eq!(
        running.strategy, None,
        "dynamic scopes have no fate-sharing strategy"
    );
    assert_eq!(
        running.intensity,
        Intensity::new(7, Duration::from_secs(90)).expect("valid intensity")
    );
    assert_eq!(running.total_restarts, 0);
    assert_eq!(
        running
            .children
            .iter()
            .map(|child| child.id.as_str())
            .collect::<Vec<_>>(),
        ["nested", "departing"],
        "children project in admission order"
    );

    let nested_row = running.child("nested").expect("nested scope is resident");
    assert_eq!(nested_row.membership, nested_scope.membership());
    assert!(nested_row.incarnation.is_some());
    assert!(matches!(nested_row.state, ChildState::Running));
    assert_eq!(nested_row.last_exit, None);
    assert_eq!(nested_row.membership_status, MembershipStatus::Active);
    assert_eq!(nested_row.restart_count, 0);
    assert!(
        nested_row.restart_policy.is_never(),
        "a one-shot subtree cannot restart"
    );
    assert_eq!(nested_row.retention, Retention::Retain);
    assert_eq!(nested_row.restart_at, None);

    let recursive = nested_row.nested.as_ref().expect("nested scope is live");
    assert_eq!(nested_row.scope_seq, Some(recursive.lifecycle_seq));
    assert_eq!(recursive.state, ScopeState::Running);
    assert_eq!(recursive.kind, ScopeKind::Ordered);
    assert_eq!(recursive.strategy, Some(Strategy::OneForOne));
    assert_eq!(
        recursive.intensity,
        Intensity::new(3, Duration::from_secs(45)).expect("valid intensity")
    );
    let worker_row = running
        .descendant(["nested", "worker"])
        .expect("recursion reaches the nested worker");
    assert_eq!(worker_row.restart_policy, worker_policy);
    assert_eq!(worker_row.retention, Retention::Retain);
    assert_eq!(worker_row.membership_status, MembershipStatus::Active);

    let departing_row = running
        .child("departing")
        .expect("departing task is resident");
    assert_eq!(departing_row.membership, departing.membership());
    assert!(matches!(departing_row.state, ChildState::Running));
    assert_eq!(
        departing_row.restart_policy,
        RestartPolicy::new(RestartCondition::Never, Backoff::Immediate)
    );
    assert_eq!(departing_row.retention, Retention::Remove);
    assert_eq!(departing_row.membership_status, MembershipStatus::Active);
    assert!(departing_row.nested.is_none());
    assert!(departing_row.scope_seq.is_none());

    let removal = scope.remove_task(&departing);
    stop_entered.wait().await;
    let removing = scope
        .wait_for_child(
            "departing",
            |child| {
                child.membership_status == MembershipStatus::Removing
                    && matches!(child.state, ChildState::Stopping)
            },
            POLL_TIMEOUT,
        )
        .await
        .expect("planned removal projects a removing, stopping child");
    assert_eq!(removing.membership_status, MembershipStatus::Removing);
    assert!(matches!(removing.state, ChildState::Stopping));
    assert!(
        removing.incarnation.is_some(),
        "a stopping child still has its live incarnation"
    );

    release_stop.release();
    assert_eq!(removal.await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
    assert!(nested_scope.snapshot().state.is_stopped());
}

/// `ScopeState::is_stopped` and `ChildState::is_terminal` are true exactly
/// for terminal projections, across unstarted, running, and stopped values.
#[tokio::test]
async fn state_predicates_hold_only_for_terminal_projections() {
    let mut declaration = Tree::new();
    let unstarted = declaration
        .add_subtree_once("nested", SubtreeOnceDef::new(Tree::new()))
        .expect("valid subtree");
    assert_eq!(unstarted.snapshot().state, ScopeState::Unstarted);
    assert!(!unstarted.snapshot().state.is_stopped());
    drop(declaration);
    assert_eq!(unstarted.wait_stopped().await, StopReason::NeverStarted);
    assert!(unstarted.snapshot().state.is_stopped());

    let system = DynamicTree::new().spawn().expect("runtime is available");
    system.wait_started().await.expect("root starts");
    let scope = system.scope();
    assert!(!scope.snapshot().state.is_stopped());

    let runner = scope
        .add_task("runner", waiting_task())
        .await
        .expect("waiting task admitted")
        .into_handles();
    let (done, completion) = scope
        .add_task_once(
            "done",
            TaskOnceDef::new(|_| async { Ok(()) }).retention(Retention::Retain),
        )
        .await
        .expect("one-shot task admitted")
        .into_handles();
    drop(completion);
    done.wait().await;
    let terminal = scope
        .wait_for_child("done", |child| child.state.is_terminal(), POLL_TIMEOUT)
        .await
        .expect("finished child projects terminal state");
    assert!(matches!(terminal.state, ChildState::Stopped { .. }));

    let running = scope.child("runner").expect("waiting child is resident");
    assert!(matches!(running.state, ChildState::Running));
    assert!(!running.state.is_terminal());

    assert_eq!(scope.remove_task(&runner).await, RemoveOutcome::Removed);
    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("root stops");
    let stopped = scope.snapshot().state.clone();
    assert!(matches!(stopped, ScopeState::Stopped { .. }));
    assert!(stopped.is_stopped());
}
