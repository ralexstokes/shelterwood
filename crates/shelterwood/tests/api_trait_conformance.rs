use std::{cell::Cell, error::Error, hash::Hash, rc::Rc, time::Duration};

use shelterwood::{
    Actor, ActorDef, ActorOnceDef, ActorRef, ActorSlot, Admission, Blocking, BuildError, CallError,
    CallFuture, Cancellation, CancellationToken, ChildId, Context, DeadlineBudget, DeadlineElapsed,
    DefaultsInheritance, DynamicActorSlot, DynamicScopeRef, DynamicSubtreeSlot, DynamicTaskSlot,
    DynamicTree, ExitError, ExitResult, GracePhase, Guard, Handler, Incarnation, Intensity,
    LifecycleEvents, LifecycleTryRecvError, Mailbox, MailboxShutdown, Membership, MembershipStatus,
    NonZeroDuration, OneShotTaskRef, RawActor, RawContext, RawDef, RawOnceDef, Readiness,
    ReadinessDeadline, Removal, Reply, ReplyReceive, ReplyReceiver, ReserveError, RestartAttempt,
    RestartCount, RestartPolicy, Retention, ScopeDefaults, ScopeRef, SendError, SendFuture,
    SendTimeout, Shutdown, SnapshotClosed, SnapshotReceiver, StopContext, Strategy, SubtreeDef,
    SubtreeOnceDef, SubtreeSlot, System, TaskDef, TaskOnceDef, TaskRef, TaskSlot, TotalRestarts,
    Tree, WaitError,
};

fn assert_error<T: Error>() {}
fn assert_send<T: Send>(_: T) {}
fn assert_send_type<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}
fn assert_copy_eq_hash_send_sync<T: Copy + Eq + Hash + Send + Sync>() {}
fn assert_clone_eq_hash_send_sync<T: Clone + Eq + Hash + Send + Sync>() {}
fn assert_static<T: 'static>() {}

macro_rules! assert_not_impl {
    ($type:ty: $trait:path) => {
        const _: fn() = || {
            struct Check<T: ?Sized>(std::marker::PhantomData<T>);
            trait AmbiguousIfImpl<A> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for Check<T> {}
            impl<T: ?Sized + $trait> AmbiguousIfImpl<u8> for Check<T> {}
            let _ = <Check<$type> as AmbiguousIfImpl<_>>::check;
        };
    };
}

assert_not_impl!(System<ScopeRef>: Clone);
assert_not_impl!(System<DynamicScopeRef>: Clone);
assert_not_impl!(ActorSlot<Cell<()>>: Clone);
assert_not_impl!(DynamicActorSlot<Cell<()>>: Clone);
assert_not_impl!(TaskSlot: Clone);
assert_not_impl!(DynamicTaskSlot: Clone);
assert_not_impl!(SubtreeSlot<Tree>: Clone);
assert_not_impl!(SubtreeSlot<DynamicTree>: Clone);
assert_not_impl!(DynamicSubtreeSlot<Tree>: Clone);
assert_not_impl!(DynamicSubtreeSlot<DynamicTree>: Clone);
assert_not_impl!(Reply<Cell<()>>: Clone);
assert_not_impl!(ReplyReceiver<Cell<()>>: Clone);
assert_not_impl!(Guard: Clone);
assert_not_impl!(OneShotTaskRef<Cell<()>>: Clone);
assert_not_impl!(Membership: Ord);
assert_not_impl!(Incarnation: Ord);
assert_not_impl!(DynamicScopeRef: std::ops::Deref);

#[test]
fn documented_identity_handle_token_and_owned_value_bounds_compile() {
    struct RcId(Rc<()>, String);

    impl From<RcId> for ChildId {
        fn from(value: RcId) -> Self {
            let RcId(not_send, id) = value;
            drop(not_send);
            id.into()
        }
    }

    assert_copy_eq_hash_send_sync::<Membership>();
    assert_copy_eq_hash_send_sync::<Incarnation>();
    assert_copy_eq_hash_send_sync::<Cancellation>();
    assert_copy_eq_hash_send_sync::<GracePhase>();
    assert_copy_eq_hash_send_sync::<RestartAttempt>();
    assert_copy_eq_hash_send_sync::<RestartCount>();
    assert_copy_eq_hash_send_sync::<TotalRestarts>();
    assert_copy_eq_hash_send_sync::<DeadlineBudget>();
    assert_copy_eq_hash_send_sync::<NonZeroDuration>();
    assert_eq!(RestartAttempt::ZERO.bump().get(), 1);
    assert_eq!(RestartCount::ZERO.bump().get(), 1);
    assert_eq!(TotalRestarts::ZERO.bump().get(), 1);
    let grace = NonZeroDuration::new(Duration::from_nanos(1)).expect("non-zero public grace");
    assert_eq!(grace.get(), Duration::from_nanos(1));
    assert_eq!(
        Shutdown::Graceful { grace },
        Shutdown::graceful(grace.get()).expect("validated grace remains accepted")
    );

    assert_clone_eq_hash_send_sync::<ActorRef<Cell<()>>>();
    assert_clone_eq_hash_send_sync::<TaskRef>();
    assert_clone_eq_hash_send_sync::<ScopeRef>();
    assert_clone_eq_hash_send_sync::<DynamicScopeRef>();
    // Shared scope operations are available through one explicit capability
    // boundary; DynamicScopeRef neither mirrors them nor dereferences.
    let _assert_dynamic_scope_method = |scope: &DynamicScopeRef| {
        let _: &ScopeRef = scope.as_scope();
        let _ = scope.as_scope().id();
    };
    assert_send_sync::<SnapshotReceiver>();
    assert_send_sync::<LifecycleEvents>();
    assert_send_sync::<CancellationToken>();
    assert_clone::<CancellationToken>();

    // These owned, at-most-once values promise Send. Deliberately do not
    // turn their current implementation's incidental Sync-ness into API.
    assert_send_type::<System<ScopeRef>>();
    assert_send_type::<System<DynamicScopeRef>>();
    assert_send_type::<ActorSlot<Cell<()>>>();
    assert_send_type::<DynamicActorSlot<Cell<()>>>();
    assert_send_type::<TaskSlot>();
    assert_send_type::<DynamicTaskSlot>();
    assert_send_type::<SubtreeSlot<Tree>>();
    assert_send_type::<SubtreeSlot<DynamicTree>>();
    assert_send_type::<DynamicSubtreeSlot<Tree>>();
    assert_send_type::<DynamicSubtreeSlot<DynamicTree>>();
    assert_send_type::<OneShotTaskRef<Cell<()>>>();
    assert_send_type::<Reply<Cell<()>>>();
    assert_send_type::<ReplyReceiver<Cell<()>>>();
    assert_send_type::<Guard>();

    assert_send_type::<Admission<TaskRef>>();
    assert_send_type::<Removal>();
    assert_send_type::<SendFuture<Cell<()>>>();
    assert_send_type::<SendTimeout<Cell<()>>>();
    assert_send_type::<CallFuture<Cell<()>, Cell<()>>>();
    assert_send_type::<ReplyReceive<Cell<()>>>();

    let _assert_token_future = |token: &CancellationToken| assert_send(token.cancelled());
    let _assert_guard_future = |guard: &Guard| assert_send(guard.finished());
    let _assert_task_future = |task: &TaskRef| assert_send(task.wait());
    let _assert_one_shot_future = |task: OneShotTaskRef<Cell<()>>| assert_send(task.wait());
    let _assert_snapshot_future = |receiver: &mut SnapshotReceiver| assert_send(receiver.changed());
    let _assert_lifecycle_future = |events: &mut LifecycleEvents| assert_send(events.recv());
    let _assert_scope_futures = |scope: &ScopeRef| {
        assert_send(scope.wait_stopped());
        assert_send(scope.shutdown_and_wait(DeadlineBudget::ZERO));
        // The ordinary `&str` id, then a deliberately non-`Send` one: the
        // conversion happens before the future exists, so both are `Send`.
        assert_send(scope.wait_for_child("child", |_| true, DeadlineBudget::ZERO));
        assert_send(scope.wait_for_child(
            RcId(Rc::new(()), "child".into()),
            |_| true,
            DeadlineBudget::ZERO,
        ));
    };
    let _assert_dynamic_scope_futures = |scope: &DynamicScopeRef| {
        assert_send(scope.as_scope().wait_stopped());
        assert_send(scope.as_scope().shutdown_and_wait(DeadlineBudget::ZERO));
        assert_send(
            scope
                .as_scope()
                .wait_for_child("child", |_| true, DeadlineBudget::ZERO),
        );
        assert_send(scope.as_scope().wait_for_child(
            RcId(Rc::new(()), "child".into()),
            |_| true,
            DeadlineBudget::ZERO,
        ));
    };
    let _assert_start_future = |system: System<ScopeRef>| {
        assert_send(system.start_or_shutdown(DeadlineBudget::ZERO));
    };
    let _assert_shutdown_future = |system: System<ScopeRef>| {
        assert_send(system.shutdown(DeadlineBudget::ZERO));
    };
    let _assert_wait_future = |system: System<ScopeRef>| assert_send(system.wait());
}

fn assert_clone<T: Clone>() {}

struct OpaqueActor {
    _not_sync: Cell<()>,
}

impl Actor for OpaqueActor {
    type Msg = Cell<()>;
    type Args = Cell<()>;

    async fn init(_: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self {
            _not_sync: Cell::new(()),
        })
    }

    async fn handle(&mut self, _: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

fn assert_raw<T: RawActor<Msg = Cell<()>>>() {}

struct ClonedActor;

impl Actor for ClonedActor {
    type Msg = ();
    type Args = ();

    async fn init(_: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self)
    }

    async fn handle(&mut self, _: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        Ok(())
    }
}

#[test]
fn actor_types_obey_resource_and_payload_trait_contracts() {
    assert_error::<DeadlineElapsed>();
    assert_raw::<Handler<OpaqueActor>>();
    assert_send_type::<Context<'static, OpaqueActor>>();
    assert_send_type::<StopContext<'static, OpaqueActor>>();
    assert_send_type::<Blocking<Cell<()>>>();
    assert_static::<Blocking<Cell<()>>>();
    assert_send_type::<Guard>();

    // The repeatable factory itself is Send + Sync after #37, but its
    // per-incarnation result and the one-shot path remain Send-only.
    let _ = ActorDef::<OpaqueActor>::factory(|| Cell::new(()));
    let _ = ActorDef::<ClonedActor>::cloned(());
    let _ = ActorOnceDef::<OpaqueActor>::new(Cell::new(()));
    let _ = TaskDef::new(|context| async move {
        let send_only_state = Cell::new(());
        context.shutdown_token().cancelled().await;
        send_only_state.set(());
        Ok(())
    });
    let captured = Cell::new(());
    let _ = TaskOnceDef::new(move |_| async move {
        captured.set(());
        Ok::<_, ExitError>(captured)
    });
    let _ = SubtreeDef::factory(Tree::new);
    let _ = SubtreeOnceDef::new(Tree::new());
}

#[test]
#[allow(clippy::type_complexity)]
fn slot_method_signatures_remain_nominal_and_parallel() {
    let _: fn(&ActorSlot<Cell<()>>) -> ActorRef<Cell<()>> = ActorSlot::actor_ref;
    let _: fn(ActorSlot<Cell<()>>, ActorDef<OpaqueActor>) -> ActorRef<Cell<()>> = ActorSlot::define;
    let _: fn(ActorSlot<Cell<()>>, ActorOnceDef<OpaqueActor>) -> ActorRef<Cell<()>> =
        ActorSlot::define_once;
    let _: fn(ActorSlot<Cell<()>>, RawDef<OpaqueRaw>) -> ActorRef<Cell<()>> = ActorSlot::define_raw;
    let _: fn(ActorSlot<Cell<()>>, RawOnceDef<OpaqueRaw>) -> ActorRef<Cell<()>> =
        ActorSlot::define_once_raw;

    let _: fn(&TaskSlot) -> TaskRef = TaskSlot::task_ref;
    let _: fn(TaskSlot, TaskDef) -> TaskRef = TaskSlot::define;
    let _: fn(TaskSlot, TaskOnceDef<Cell<()>>) -> (TaskRef, OneShotTaskRef<Cell<()>>) =
        TaskSlot::define_once;

    let _: fn(&SubtreeSlot<Tree>) -> ScopeRef = SubtreeSlot::<Tree>::scope_ref;
    let _: fn(SubtreeSlot<Tree>, SubtreeDef<Tree>) -> ScopeRef = SubtreeSlot::<Tree>::define;
    let _: fn(SubtreeSlot<Tree>, SubtreeOnceDef<Tree>) -> ScopeRef =
        SubtreeSlot::<Tree>::define_once;

    let _: fn(&DynamicActorSlot<Cell<()>>) -> ActorRef<Cell<()>> = DynamicActorSlot::actor_ref;
    let _: fn(DynamicActorSlot<Cell<()>>, ActorDef<OpaqueActor>) -> Admission<ActorRef<Cell<()>>> =
        DynamicActorSlot::define;
    let _: fn(
        DynamicActorSlot<Cell<()>>,
        ActorOnceDef<OpaqueActor>,
    ) -> Admission<ActorRef<Cell<()>>> = DynamicActorSlot::define_once;
    let _: fn(DynamicActorSlot<Cell<()>>, RawDef<OpaqueRaw>) -> Admission<ActorRef<Cell<()>>> =
        DynamicActorSlot::define_raw;
    let _: fn(DynamicActorSlot<Cell<()>>, RawOnceDef<OpaqueRaw>) -> Admission<ActorRef<Cell<()>>> =
        DynamicActorSlot::define_once_raw;

    let _: fn(&DynamicTaskSlot) -> TaskRef = DynamicTaskSlot::task_ref;
    let _: fn(DynamicTaskSlot, TaskDef) -> Admission<TaskRef> = DynamicTaskSlot::define;
    let _: fn(
        DynamicTaskSlot,
        TaskOnceDef<Cell<()>>,
    ) -> Admission<(TaskRef, OneShotTaskRef<Cell<()>>)> = DynamicTaskSlot::define_once;

    let _: fn(&DynamicSubtreeSlot<Tree>) -> ScopeRef = DynamicSubtreeSlot::<Tree>::scope_ref;
    let _: fn(DynamicSubtreeSlot<Tree>, SubtreeDef<Tree>) -> Admission<ScopeRef> =
        DynamicSubtreeSlot::<Tree>::define;
    let _: fn(DynamicSubtreeSlot<Tree>, SubtreeOnceDef<Tree>) -> Admission<ScopeRef> =
        DynamicSubtreeSlot::<Tree>::define_once;
}

struct OpaqueRaw {
    _not_sync: Cell<()>,
}

impl RawActor for OpaqueRaw {
    type Msg = Cell<()>;

    async fn run(&mut self, _context: &mut RawContext<Self::Msg>) -> ExitResult {
        Ok(())
    }
}

#[test]
fn ordered_and_dynamic_builders_expose_the_parallel_typed_surface() {
    let mut ordered = Tree::new();
    let _: &mut Tree = ordered.strategy(Strategy::default());
    let _: &mut Tree = ordered.intensity(Intensity::default());
    let _: &mut Tree = ordered.defaults(ScopeDefaults::default());
    let _: Result<ActorSlot<Cell<()>>, ReserveError> = ordered.reserve_actor("ordered-actor-slot");
    let _: Result<ActorRef<()>, ReserveError> =
        ordered.add_actor("ordered-actor", ActorDef::<ClonedActor>::cloned(()));
    let _: Result<ActorRef<()>, ReserveError> =
        ordered.add_actor_once("ordered-actor-once", ActorOnceDef::<ClonedActor>::new(()));
    let _: Result<ActorRef<Cell<()>>, ReserveError> = ordered.add_raw(
        "ordered-raw",
        RawDef::<OpaqueRaw>::factory(|| OpaqueRaw {
            _not_sync: Cell::new(()),
        }),
    );
    let _: Result<ActorRef<Cell<()>>, ReserveError> = ordered.add_raw_once(
        "ordered-raw-once",
        RawOnceDef::new(OpaqueRaw {
            _not_sync: Cell::new(()),
        }),
    );
    let _: Result<TaskSlot, ReserveError> = ordered.reserve_task("ordered-task-slot");
    let _: Result<TaskRef, ReserveError> =
        ordered.add_task("ordered-task", TaskDef::new(|_| async { Ok(()) }));
    let _: Result<(TaskRef, OneShotTaskRef<()>), ReserveError> = ordered.add_task_once(
        "ordered-task-once",
        TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }),
    );
    let _: Result<SubtreeSlot<Tree>, ReserveError> =
        ordered.reserve_subtree("ordered-subtree-slot");
    let _: Result<ScopeRef, ReserveError> =
        ordered.add_subtree("ordered-subtree", SubtreeDef::factory(Tree::new));
    let _: Result<ScopeRef, ReserveError> =
        ordered.add_subtree_once("ordered-subtree-once", SubtreeOnceDef::new(Tree::new()));

    let mut dynamic = DynamicTree::new();
    let _: &mut DynamicTree = dynamic.intensity(Intensity::default());
    let _: &mut DynamicTree = dynamic.defaults(ScopeDefaults::default());
    let _: Result<ActorSlot<Cell<()>>, ReserveError> = dynamic.reserve_actor("dynamic-actor-slot");
    let _: Result<ActorRef<()>, ReserveError> =
        dynamic.add_actor("dynamic-actor", ActorDef::<ClonedActor>::cloned(()));
    let _: Result<ActorRef<()>, ReserveError> =
        dynamic.add_actor_once("dynamic-actor-once", ActorOnceDef::<ClonedActor>::new(()));
    let _: Result<ActorRef<Cell<()>>, ReserveError> = dynamic.add_raw(
        "dynamic-raw",
        RawDef::<OpaqueRaw>::factory(|| OpaqueRaw {
            _not_sync: Cell::new(()),
        }),
    );
    let _: Result<ActorRef<Cell<()>>, ReserveError> = dynamic.add_raw_once(
        "dynamic-raw-once",
        RawOnceDef::new(OpaqueRaw {
            _not_sync: Cell::new(()),
        }),
    );
    let _: Result<TaskSlot, ReserveError> = dynamic.reserve_task("dynamic-task-slot");
    let _: Result<TaskRef, ReserveError> =
        dynamic.add_task("dynamic-task", TaskDef::new(|_| async { Ok(()) }));
    let _: Result<(TaskRef, OneShotTaskRef<()>), ReserveError> = dynamic.add_task_once(
        "dynamic-task-once",
        TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }),
    );
    let _: Result<SubtreeSlot<Tree>, ReserveError> =
        dynamic.reserve_subtree("dynamic-subtree-slot");
    let _: Result<ScopeRef, ReserveError> =
        dynamic.add_subtree("dynamic-subtree", SubtreeDef::factory(Tree::new));
    let _: Result<ScopeRef, ReserveError> =
        dynamic.add_subtree_once("dynamic-subtree-once", SubtreeOnceDef::new(Tree::new()));

    let _: fn(Tree) -> Result<System<ScopeRef>, BuildError> = Tree::spawn;
    let _: fn(DynamicTree) -> Result<System<DynamicScopeRef>, BuildError> = DynamicTree::spawn;
}

#[test]
fn raw_types_obey_error_and_future_trait_contracts() {
    assert_error::<SendError<Cell<()>>>();
    assert_error::<CallError>();
    assert_send_sync::<ActorRef<Cell<()>>>();

    let _ = RawDef::<OpaqueRaw>::factory(|| OpaqueRaw {
        _not_sync: Cell::new(()),
    });

    let mut tree = Tree::new();
    let actor = tree
        .add_raw_once(
            "traits",
            RawOnceDef::new(OpaqueRaw {
                _not_sync: Cell::new(()),
            }),
        )
        .expect("valid actor");
    assert_send(actor.send(Cell::new(())));
    assert_send(actor.send_timeout(Cell::new(()), DeadlineBudget::new(Duration::from_secs(1))));
    let call_state = Cell::new(());
    assert_send(actor.call(
        move |_reply: Reply<()>| {
            call_state.set(());
            Cell::new(())
        },
        DeadlineBudget::new(Duration::from_secs(1)),
    ));
    let (reply, receiver) = Reply::<Cell<()>>::channel();
    assert_send(reply);
    assert_send(receiver);
    let (_reply, receiver) = Reply::<Cell<()>>::channel();
    assert_send(receiver.recv(DeadlineBudget::new(Duration::from_secs(1))));
}

#[test]
fn all_definition_option_setters_compile() {
    let _ = RawDef::<OpaqueRaw>::factory(|| OpaqueRaw {
        _not_sync: Cell::new(()),
    })
    .restart(RestartPolicy::default())
    .shutdown(Shutdown::default())
    .mailbox(Mailbox::default())
    .mailbox_shutdown(MailboxShutdown::default())
    .readiness(Readiness::Immediate)
    .expect("immediate raw readiness is supported")
    .readiness_deadline(ReadinessDeadline::Inherit)
    .retention(Retention::Retain);

    let _ = RawOnceDef::new(OpaqueRaw {
        _not_sync: Cell::new(()),
    })
    .shutdown(Shutdown::default())
    .mailbox(Mailbox::default())
    .mailbox_shutdown(MailboxShutdown::default())
    .readiness(Readiness::Manual)
    .expect("manual raw readiness is supported")
    .readiness_deadline(ReadinessDeadline::Inherit)
    .retention(Retention::Retain);

    let _ = ActorDef::<OpaqueActor>::factory(|| Cell::new(()))
        .restart(RestartPolicy::default())
        .shutdown(Shutdown::default())
        .mailbox(Mailbox::default())
        .mailbox_shutdown(MailboxShutdown::default())
        .readiness(Readiness::AfterInit)
        .readiness_deadline(ReadinessDeadline::Inherit)
        .retention(Retention::Retain);

    let _ = ActorOnceDef::<OpaqueActor>::new(Cell::new(()))
        .shutdown(Shutdown::default())
        .mailbox(Mailbox::default())
        .mailbox_shutdown(MailboxShutdown::default())
        .readiness(Readiness::AfterInit)
        .readiness_deadline(ReadinessDeadline::Inherit)
        .retention(Retention::Retain);

    let _ = TaskDef::new(|_| async { Ok(()) })
        .restart(RestartPolicy::default())
        .shutdown(Shutdown::default())
        .readiness(Readiness::Immediate)
        .expect("immediate task readiness is supported")
        .readiness_deadline(ReadinessDeadline::Inherit)
        .retention(Retention::Retain);

    let _ = TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) })
        .shutdown(Shutdown::default())
        .readiness(Readiness::Manual)
        .expect("manual task readiness is supported")
        .readiness_deadline(ReadinessDeadline::Inherit)
        .retention(Retention::Retain);

    let _ = SubtreeDef::factory(Tree::new)
        .restart(RestartPolicy::default())
        .shutdown(Shutdown::default())
        .readiness_deadline(ReadinessDeadline::Inherit)
        .retention(Retention::Retain)
        .defaults(DefaultsInheritance::Inherit);

    let _ = SubtreeOnceDef::new(Tree::new())
        .shutdown(Shutdown::default())
        .readiness_deadline(ReadinessDeadline::Inherit)
        .retention(Retention::Retain)
        .defaults(DefaultsInheritance::Reset);
}

#[test]
fn observation_types_obey_error_and_thread_safety_contracts() {
    assert_copy_eq_hash_send_sync::<MembershipStatus>();
    assert_error::<LifecycleTryRecvError>();
    assert_error::<SnapshotClosed>();
    assert_error::<WaitError>();
    assert_send_sync::<LifecycleEvents>();
    assert_send_sync::<SnapshotReceiver>();
}
