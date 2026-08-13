use super::support::*;

// The key's controlled membership mutation is exactly the regression under
// test: pointer-based handle identity must keep the set addressable.
#[allow(clippy::mutable_key_type)]
#[test]
fn handle_identity_is_stable_across_membership_rebase() {
    fn hashed(value: &impl std::hash::Hash) -> u64 {
        use std::hash::Hasher;
        let mut hasher = std::hash::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox: Arc<MailboxCell<u8>> =
        MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), mailbox);
    let peer = actor.clone();
    let task = crate::TaskRef::new(Arc::clone(&member));
    let declared = actor.membership();
    let actor_hash = hashed(&actor);
    let task_hash = hashed(&task);
    let actor_keys = std::collections::HashSet::from([actor.clone()]);
    let task_keys = std::collections::HashSet::from([task.clone()]);

    member.rebase_membership(
        identity
            .mint_membership(&id)
            .expect("successor membership available"),
    );

    assert!(actor.membership().supersedes(declared));
    assert_eq!(actor, peer);
    assert_eq!(hashed(&actor), actor_hash);
    assert_eq!(hashed(&task), task_hash);
    assert!(
        actor_keys.contains(&actor),
        "membership rebasing cannot strand a keyed actor handle"
    );
    assert!(
        task_keys.contains(&task),
        "membership rebasing cannot strand a keyed task handle"
    );
}

#[crate::runtime::test]
async fn attaching_after_terminality_closes_the_mailbox() {
    let mut identity = ScopeIdentity::new();
    let id = ChildId::from("worker");
    let member = MemberCell::new(
        id.clone(),
        identity.mint_membership(&id).expect("membership available"),
    );
    let mailbox = MailboxCell::new(member.id().clone(), crate::runtime::mailbox_runtime());
    let actor = actor_ref_from_parts(Arc::clone(&member), Arc::clone(&mailbox));
    let mut parked = Box::pin(actor.send(1));
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(parked.as_mut().poll(context))).await;
    assert!(first_poll.is_pending());

    member.terminalize(Exit::never_started(), StartupDisposition::Unchanged);
    member.attach_mailbox(mailbox);

    let parked = match crate::runtime::timeout(Duration::from_secs(1), parked).await {
        crate::runtime::Timeout::Completed(result) => {
            result.expect_err("parked send is terminated")
        }
        crate::runtime::Timeout::Elapsed => panic!("parked send must not remain pending"),
    };
    assert_eq!(parked.kind, SendErrorKind::Terminated);
    let immediate = actor.try_send(2).expect_err("terminal send is rejected");
    assert_eq!(immediate.kind, SendErrorKind::Terminated);
}

#[crate::runtime::test]
async fn task_aborted_scope_driver_resolves_startup() {
    let mut tree = Tree::new();
    tree.add_task(
        "not-ready",
        TaskDef::new(|_| future::pending())
            .readiness(Readiness::Manual)
            .expect("manual readiness is valid"),
    )
    .expect("valid task");
    let plan = tree.lower_for_test();
    let scope = Arc::clone(&plan.root);
    let epoch = ScopeEpochGuard::begin(&scope).expect("test scope epoch is available");
    let driver = crate::runtime::spawn(run_scope_incarnation(
        plan,
        ScopeRole::Nested(NestedScopeLatches {
            parent_ready: CompletionGatedLatch::default(),
            ancestor: AncestorCommandLatches {
                shutdown: Latch::default(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        }),
        epoch,
    ));
    let abort = driver.abort_handle();
    let reached_startup = crate::runtime::timeout(Duration::from_secs(1), async {
        while !matches!(scope.record().state, ScopeState::Starting) {
            crate::runtime::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        reached_startup,
        crate::runtime::Timeout::Completed(())
    ));
    let waiter_scope = Arc::clone(&scope);
    let mut waiter = Box::pin(waiter_scope.wait_started());
    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(waiter.as_mut().poll(context))).await;
    assert!(first_poll.is_pending());

    abort.abort();
    assert!(matches!(
        crate::runtime::join(driver).await,
        crate::runtime::JoinOutcome::Cancelled
    ));
    let result = crate::runtime::timeout(Duration::from_secs(1), waiter).await;
    assert!(matches!(
        result,
        crate::runtime::Timeout::Completed(Err(crate::StartupError::ShutdownRequested))
    ));
    assert!(matches!(scope.record().state, ScopeState::Stopped { .. }));
    assert!(scope.record().startup.is_some());
}

#[crate::runtime::test]
async fn dropped_unpolled_scope_plan_terminalizes_its_root() {
    let plan = Tree::new().lower_for_test();
    let root = Arc::clone(&plan.root);

    drop(plan);

    assert_eq!(
        root.wait_started().await,
        Err(crate::StartupError::ShutdownRequested)
    );
    assert_eq!(root.wait_stopped().await, StopReason::NeverStarted);
}

#[crate::runtime::test]
async fn dropped_unpolled_scope_plan_terminalizes_nested_declarations() {
    let mut inner = Tree::new();
    let leaf = inner
        .add_task("leaf", TaskDef::new(|_| future::pending()))
        .expect("valid nested task");
    let mut outer = Tree::new();
    let nested = outer
        .add_subtree_once("nested", SubtreeOnceDef::new(inner))
        .expect("valid nested scope");
    let mut snapshots = nested.subscribe_snapshots();
    let mut events = nested.subscribe_lifecycle();
    let plan = outer.lower_for_test();

    drop(plan);

    assert_eq!(nested.wait_stopped().await, StopReason::NeverStarted);
    snapshots
        .changed()
        .await
        .expect("the final nested snapshot is delivered before closure");
    assert!(matches!(
        snapshots.borrow_latest().state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    ));
    assert!(snapshots.changed().await.is_err());
    assert!(matches!(leaf.wait().await.kind(), ExitKind::NeverStarted));
    let mut saw_stopped = false;
    while let Some(item) = events.recv().await {
        saw_stopped |= matches!(
            item,
            LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::ScopeState {
                    state: ScopeState::Stopped {
                        reason: StopReason::NeverStarted
                    }
                },
                ..
            })
        );
    }
    assert!(
        saw_stopped,
        "nested observation closes after its final event"
    );
}

#[crate::runtime::test]
async fn converted_nested_child_without_residency_closes_its_scope_on_drop() {
    let mut outer = Tree::new();
    let nested = outer
        .add_subtree_once("nested", SubtreeOnceDef::new(Tree::new()))
        .expect("valid nested scope");
    let mut snapshots = nested.subscribe_snapshots();
    let mut plan = outer.lower_for_test();
    let root = Arc::clone(&plan.root);
    let nested_index = plan
        .children
        .iter()
        .position(|child| child.slot.member.id().as_str() == "nested")
        .expect("nested child is present");
    let nested_runtime = ChildRuntime::from_plan(plan.children.remove(nested_index), &root);

    assert!(
        root.snapshot().children.is_empty(),
        "initial-child conversion precedes residency publication"
    );
    // Models an unwind while converting a later child: the converted prefix
    // is dropped while its nested slot is not yet discoverable as a resident.
    drop(nested_runtime);

    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(1), nested.wait_stopped()).await,
        crate::runtime::Timeout::Completed(StopReason::NeverStarted)
    ));
    snapshots
        .changed()
        .await
        .expect("the final nested snapshot is delivered before closure");
    assert!(matches!(
        snapshots.borrow_latest().state,
        ScopeState::Stopped {
            reason: StopReason::NeverStarted
        }
    ));
    assert!(snapshots.changed().await.is_err());
}

#[crate::runtime::test]
async fn scope_plan_conversion_panic_terminalizes_every_child() {
    let mut tree = Tree::new();
    for id in ["first", "second"] {
        tree.add_task(id, TaskDef::new(|_| future::pending()))
            .expect("valid task");
    }
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let mut events = root.subscribe_lifecycle();
    let children: Vec<_> = plan
        .children
        .iter()
        .map(|child| Arc::clone(&child.slot.member))
        .collect();
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");

    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _incarnations = children[1].lock_incarnation_counter();
            panic!("inject child conversion failure");
        }))
        .is_err()
    );

    let mut driver = Box::pin(run_scope_incarnation(
        plan,
        ScopeRole::Nested(NestedScopeLatches {
            parent_ready: CompletionGatedLatch::default(),
            ancestor: AncestorCommandLatches {
                shutdown: Latch::default(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        }),
        epoch,
    ));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let mut context = Context::from_waker(Waker::noop());
            let _ = driver.as_mut().poll(&mut context);
        }))
        .is_err()
    );
    drop(driver);

    for child in children {
        assert!(
            matches!(child.record().stage, MemberStage::Terminal(_)),
            "every transferred or pending child must terminalize"
        );
    }
    assert_eq!(
        root.wait_started().await,
        Err(crate::StartupError::ShutdownRequested)
    );
    assert_eq!(
        root.wait_stopped().await,
        StopReason::NeverStarted,
        "the plan fallback's never-started verdict outranks the epoch guard's"
    );
    assert!(
        root.snapshot().children.is_empty(),
        "the fallback must release every admitted residency"
    );
    let mut removed = 0;
    while let Some(item) = events.recv().await {
        if matches!(
            item,
            LifecycleItem::Event(crate::LifecycleEvent {
                kind: LifecycleEventKind::Removed { .. },
                ..
            })
        ) {
            removed += 1;
        }
    }
    assert_eq!(
        removed, 0,
        "conversion must finish before publication begins"
    );
}

#[crate::runtime::test]
async fn conversion_unwind_evicts_never_started_child_identities() {
    let mut tree = Tree::new();
    for id in ["first", "second"] {
        tree.add_task(id, TaskDef::new(|_| future::pending()))
            .expect("valid task");
    }
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let children: Vec<_> = plan
        .children
        .iter()
        .map(|child| Arc::clone(&child.slot.member))
        .collect();
    let terminalized: Vec<_> = children
        .iter()
        .map(|member| (member.id().clone(), member.membership()))
        .collect();
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");

    // Converting "second" panics after "first" was converted, so "first"
    // reaches its Obligation fallback never started and not yet resident,
    // while "second" unwinds out of its own conversion.
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _incarnations = children[1].lock_incarnation_counter();
            panic!("inject child conversion failure");
        }))
        .is_err()
    );
    let mut driver = Box::pin(run_scope_incarnation(
        plan,
        ScopeRole::Nested(NestedScopeLatches {
            parent_ready: CompletionGatedLatch::default(),
            ancestor: AncestorCommandLatches {
                shutdown: Latch::default(),
                abort: Latch::default(),
                abort_ack: Latch::default(),
            },
        }),
        epoch,
    ));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let mut context = Context::from_waker(Waker::noop());
            let _ = driver.as_mut().poll(&mut context);
        }))
        .is_err()
    );
    drop(driver);
    for child in &children {
        assert!(
            matches!(child.record().stage, MemberStage::Terminal(_)),
            "every transferred or pending child must terminalize"
        );
    }

    // The supervisor restart rebuilds the declarations against the same
    // stable scope. Terminalization must have evicted each lineage, so the
    // rebuild adopts fresh, incomparable memberships; a retained lineage
    // would mint an ordered successor that supersedes its terminalized
    // predecessor.
    let mut rebuild = BuilderCore::new(ScopeFlavor::Ordered);
    for id in ["first", "second"] {
        let slot = rebuild
            .reserve(id, None)
            .expect("re-added id is reservable");
        slot.define(ChildConstruction::Task(TaskDef::new(|_| future::pending())));
    }
    let replacement = rebuild
        .lower(ResolvedDefaults::default(), Some(Arc::clone(&root)))
        .expect("the restart rebuild lowers");
    for child in &replacement.children {
        let (_, predecessor) = terminalized
            .iter()
            .find(|(id, _)| id == child.slot.member.id())
            .expect("the rebuild redeclares every terminalized id");
        let replacement = child.slot.member.membership();
        assert!(
            !replacement.supersedes(*predecessor),
            "a re-minted membership must not supersede its terminalized predecessor"
        );
        assert!(
            !predecessor.supersedes(replacement),
            "a terminalized predecessor must not order against its replacement"
        );
    }
}

struct ReserveOnLifecycleWake {
    scope: Arc<ScopeCell>,
    result: Mutex<Option<Result<(), ReserveError>>>,
    observed: Latch,
}

impl ReserveOnLifecycleWake {
    fn observe(&self) {
        let mut result = self.result.lock().expect("observation mutex poisoned");
        if result.is_none() {
            *result = Some(
                reserve_dynamic(&self.scope, ChildId::from("reentrant"), None).map(|reservation| {
                    cancel_dynamic_reservation(
                        &reservation.scope,
                        reservation.control.as_ref(),
                        &reservation.slot,
                    );
                }),
            );
            self.observed.fire();
        }
    }
}

impl Wake for ReserveOnLifecycleWake {
    fn wake(self: Arc<Self>) {
        self.observe();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.observe();
    }
}

#[crate::runtime::test]
async fn initial_added_wake_observes_the_keyed_dynamic_route() {
    let mut tree = DynamicTree::new();
    tree.add_task("initial", TaskDef::new(|_| future::pending()))
        .expect("valid task");
    let plan = tree.lower_for_test();
    let root = Arc::clone(&plan.root);
    let mut events = root.subscribe_lifecycle();
    let epoch = ScopeEpochGuard::begin(&root).expect("test scope epoch is available");
    assert!(events.recv().await.is_some(), "Starting is observed first");

    let probe = Arc::new(ReserveOnLifecycleWake {
        scope: Arc::clone(&root),
        result: Mutex::new(None),
        observed: Latch::default(),
    });
    let waker = Waker::from(Arc::clone(&probe));
    let mut added = Box::pin(events.recv());
    assert!(
        added
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );

    let driver = crate::runtime::spawn(run_scope_incarnation(plan, ScopeRole::Root, epoch));
    let abort = driver.abort_handle();
    assert!(matches!(
        crate::runtime::timeout(Duration::from_secs(1), probe.observed.fired()).await,
        crate::runtime::Timeout::Completed(())
    ));
    drop(added);
    assert!(matches!(
        probe
            .result
            .lock()
            .expect("observation mutex poisoned")
            .as_ref(),
        Some(Ok(()))
    ));
    abort.abort();
    assert!(matches!(
        crate::runtime::join(driver).await,
        crate::runtime::JoinOutcome::Cancelled
    ));
}
