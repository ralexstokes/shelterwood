//! One scope incarnation: its epoch guard, entry point and driver loop.

use super::*;
use shelterwood_core::panic::PanicAccumulator;

/// Owns a scope epoch and its matching initial lifecycle until a
/// `ScopeRuntime` has taken over teardown.
///
/// Nested lowering can await isolated disposal before a driver exists. If
/// that setup future is cancelled or unwinds, dropping this guard retires the
/// epoch so a later restart finds an idle epoch plane to begin from.
pub(super) struct ScopeEpochGuard {
    scope: Arc<ScopeCell>,
    epoch: Option<Epoch>,
    pub(super) lifecycle: ScopeLifecycle,
    // The core lifecycle retains raw structured stop reasons. Keep the
    // cells-layer guards last so unwind retires the reasons before detaching
    // their nested failed exits.
    pub(super) retained_exits: Vec<Retained<Exit>>,
}

impl ScopeEpochGuard {
    pub(super) fn begin(scope: &Arc<ScopeCell>) -> Option<Self> {
        // Own the epoch before startup publication can wake user observers.
        // The cell fills this slot while its transaction is held; on panic,
        // that transaction unlocks before this outer guard runs its epilogue.
        let mut guard = Self {
            scope: Arc::clone(scope),
            epoch: None,
            lifecycle: ScopeLifecycle::starting(),
            retained_exits: Vec::new(),
        };
        scope.begin_incarnation_into(guard.lifecycle.state(), &mut guard.epoch);
        guard.epoch.map(|_| guard)
    }

    pub(super) fn lifecycle(&self) -> ScopeLifecycle {
        self.lifecycle.clone()
    }

    pub(super) fn epoch(&self) -> Epoch {
        self.epoch
            .expect("an owned scope epoch remains available until transfer or finish")
    }

    pub(super) fn finish(mut self, reason: StopReason) {
        let epoch = self
            .epoch
            .take()
            .expect("an owned scope epoch finishes at most once");
        self.scope.finish_incarnation(epoch, reason);
    }

    pub(super) fn transfer(mut self) -> Epoch {
        self.epoch
            .take()
            .expect("an owned scope epoch transfers at most once")
    }
}

impl Drop for ScopeEpochGuard {
    fn drop(&mut self) {
        if let Some(epoch) = self.epoch.take() {
            // Drop runs on unwind paths, so a poisoned control mutex must not
            // turn this retirement into a second panic.
            self.scope
                .finish_incarnation_ignoring_poison(epoch, StopReason::ShutdownRequested);
        }
    }
}

pub(super) async fn run_scope(plan: ScopePlan, role: ScopeRole) -> Guarded<StopReason> {
    let root = Arc::clone(&plan.root);
    let Some(epoch) = ScopeEpochGuard::begin(&root) else {
        // Dropping the still-owned plan terminalizes every never-started
        // declaration and the root; no aliased driver epoch is created.
        drop(plan);
        return Guarded::new(StopReason::NeverStarted);
    };
    Guarded::new(run_scope_incarnation(plan, role, epoch).await)
}

/// Which source ended a [`wait_scope`].
pub(super) enum ScopeWake<T> {
    Signal,
    ParentShutdown,
    Message(Option<T>),
    ControlMessage(Option<T>),
    Deadline,
}

/// The two command sources a [`wait_scope`] polls ahead of its event lanes.
pub(super) struct ScopeWait<S, C> {
    pub(super) signal: S,
    pub(super) parent_shutdown: C,
}

/// Waits for the first of the scope's wake sources.
///
/// Ties resolve in the fixed order signal, parent shutdown, primary message,
/// control message, deadline. The event sources nest in two left-biased
/// `select_two`s, commands on the left and lanes on the right, so one poll
/// visits them in exactly that order.
pub(super) async fn wait_scope<S, C, T>(
    wait: ScopeWait<S, C>,
    receiver: &mut runtime::UnboundedMpscReceiver<T>,
    control_receiver: Option<&mut runtime::UnboundedMpscReceiver<T>>,
    deadline: Option<Instant>,
) -> ScopeWake<T>
where
    S: Future<Output = ()> + Send,
    C: Future<Output = ()> + Send,
    T: Send,
{
    let ScopeWait {
        signal,
        parent_shutdown,
    } = wait;
    let control_message = async move {
        if let Some(receiver) = control_receiver {
            receiver.recv().await
        } else {
            std::future::pending().await
        }
    };
    let commands = runtime::select_two(signal, parent_shutdown);
    let messages = runtime::select_two(receiver.recv(), control_message);
    let event = async move {
        match runtime::select_two(commands, messages).await {
            runtime::Either::Left(runtime::Either::Left(())) => ScopeWake::Signal,
            runtime::Either::Left(runtime::Either::Right(())) => ScopeWake::ParentShutdown,
            runtime::Either::Right(runtime::Either::Left(message)) => ScopeWake::Message(message),
            runtime::Either::Right(runtime::Either::Right(message)) => {
                ScopeWake::ControlMessage(message)
            }
        }
    };
    // The deadline stays outside the whole event selection so every event
    // winner retires the timer through `timeout_at`'s synchronous poll-path
    // boundary. Burying the sleep in a select arm would run its drop-glue
    // disposal venue every time another arm won -- one blocking-lane
    // submission per driver wakeup while any deadline is armed.
    match deadline {
        Some(deadline) => match runtime::timeout_at(deadline, event).await {
            runtime::Timeout::Completed(wake) => wake,
            runtime::Timeout::Elapsed => ScopeWake::Deadline,
        },
        None => event.await,
    }
}

/// Blocks the driver loop until one lane wakes it, returning `Some` only when
/// the wake staged a scope completion the loop must return.
#[must_use = "a staged fail-closed completion must end the driver loop"]
pub(super) async fn wait_for_scope_wake(
    scope: &mut ScopeRuntime,
    signal: &mut runtime::WatchReceiver<crate::cells::Guarded<crate::cells::MemberRecord>>,
    event_receiver: &mut runtime::UnboundedMpscReceiver<DriverEvent>,
    dynamic_event_receiver: Option<&mut runtime::UnboundedMpscReceiver<DriverEvent>>,
    pending: &mut Vec<(ArbitrationClass, Pending)>,
) -> Option<StopReason> {
    let ancestor_shutdown = scope
        .role
        .ancestor()
        .filter(|_| !scope.ancestor_shutdown_seen)
        .map(|latches| latches.framework_shutdown.clone());
    let ancestor_abort = scope
        .role
        .ancestor()
        .filter(|_| !scope.ancestor_abort_seen)
        .map(|latches| latches.abort.clone());
    let ancestor_command = async move {
        let shutdown = async move {
            if let Some(shutdown) = ancestor_shutdown {
                shutdown.fired().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let abort = async move {
            if let Some(abort) = ancestor_abort {
                abort.fired().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let _ = runtime::select_two(shutdown, abort).await;
    };
    match wait_scope(
        ScopeWait {
            signal: signal.changed(),
            parent_shutdown: ancestor_command,
        },
        event_receiver,
        dynamic_event_receiver,
        scope.deadlines.next_deadline(),
    )
    .await
    {
        ScopeWake::Signal | ScopeWake::ParentShutdown => {}
        ScopeWake::Message(None) | ScopeWake::ControlMessage(None) => {
            // Every lane sender is retained by the scope runtime or one of its
            // registered children until the driver loop exits. A closed
            // receiver would otherwise make this select arm permanently ready
            // and spin. Fail closed by returning a retained shutdown
            // completion: a framework panic here would unwind the other
            // receiver and any queued admission response wakers outside
            // ScopeRuntime's contained epilogue.
            let reason = StopReason::ShutdownRequested;
            scope.completion = Some(Guarded::new(reason.clone()));
            return Some(reason);
        }
        ScopeWake::Deadline => {
            // What SPEC §7/§13 guarantee is that a readiness latch already
            // fired when its deadline is handled wins the tie:
            // `handle_deadline` feeds the retained latch into the engine.
            // This yield gives producers woken by the same clock edge a
            // best-effort opportunity to publish readiness. It supports the
            // paused current-thread deadline-tie test, but does not guarantee
            // a poll order on either runtime flavor. In particular, another
            // worker may not yet have polled a ready producer when this
            // driver handles its deadline, so the deadline can still win.
            runtime::yield_now().await;
        }
        ScopeWake::Message(Some(event)) | ScopeWake::ControlMessage(Some(event)) => {
            // Retain the head that ended the wait and return to the single
            // collection site, so it is arbitrated with every input that
            // became eligible before the wake was observed. It keeps its own
            // lane's FIFO position but sits ahead of the whole re-entered
            // collection, so a woken control head precedes the primary lane.
            // `MembershipRemoval` is the only class both lanes produce
            // (`Removal` and `SelfStop`), and neither order of that pair
            // changes a verdict: readiness publication consults the removal
            // sources at execution time.
            pending.push(Pending::from(event).classified());
        }
    }
    None
}

pub(super) async fn run_scope_incarnation(
    mut plan: ScopePlan,
    role: ScopeRole,
    epoch: ScopeEpochGuard,
) -> StopReason {
    let root = Arc::clone(&plan.root);
    if role.is_root() {
        let running = root.with_observation_gate(|txn| {
            // The root scope is never admitted, so its own reservation is the
            // documented `Reserved -> Running` source stage.
            root.member
                .transition_locked(txn, MemberTransition::Running)
        });
        if !running {
            // A refused root projection cannot support a driver. Establish
            // the invariant first, then retire the still-owned plan and epoch
            // through contained ordinary calls before resuming it. Definitions,
            // response wakes, and resident projections therefore cannot run
            // as ordinary framework-panic stack destruction.
            //
            // `PanicAccumulator` contains rather than resumes on an
            // already-unwinding stack, so record the diagnostic only when
            // there is a stack to raise it on and fall through to the drain
            // verdict otherwise. `begin_incarnation` already published
            // `Starting`, so the epoch finishes as a requested stop rather
            // than claiming no incarnation ever began.
            let mut panics = PanicAccumulator::default();
            if !std::thread::panicking() {
                panics.run(|| {
                    panic!("a root incarnation promotes its own never-admitted reservation")
                });
            }
            panics.run(|| drop(plan));
            panics.run(|| epoch.finish(StopReason::ShutdownRequested));
            drop(panics);
            return StopReason::ShutdownRequested;
        }
    }
    // Both lanes are unbounded so their producers can publish synchronously.
    // Keep child lifecycle events separate from externally generated dynamic
    // control traffic: a large admission prefix must not strand the exit that
    // completes shutdown. Bound each lane's per-wake collection so traffic
    // cannot defer signals or deadlines indefinitely. The cap adds one
    // ordering surface: when a wake finds more than a full batch of primary
    // events, the deferred suffix (an intensity-tripping exit, say) is
    // processed one wake after control-lane admissions enqueued earlier.
    // `arbitrate` promises order only within a batch, so no promised order
    // is violated.
    let event_batch_limit = plan
        .children
        .len()
        .saturating_mul(3)
        .max(MIN_EVENT_BATCH_LIMIT);
    let (events, mut event_receiver) = runtime::unbounded_mpsc();
    let (disposal_events, disposal_event_receiver) = runtime::unbounded_mpsc();
    let (dynamic, mut dynamic_event_receiver) = if plan.root.flavor == ScopeFlavor::Dynamic {
        let (dynamic_events, receiver) = runtime::unbounded_mpsc();
        (Some(DynamicControl::new(dynamic_events)), Some(receiver))
    } else {
        (None, None)
    };
    // Transfer children one at a time. The not-yet-converted suffix remains
    // owned by ScopePlan, while ChildRuntime::from_plan arms the current
    // child's obligation before fallible setup. Thus a panic at any point has
    // exactly one terminality owner for every child.
    let mut supervisor = SupervisorState::new(root.flavor, epoch.lifecycle());
    let mut children = BTreeMap::new();
    plan.children.reverse();
    while let Some(child) = plan.children.pop() {
        let child = ChildRuntime::from_plan(child, &root);
        let key = supervisor_admit(&mut supervisor, child.slot.member.membership(), true);
        let _ = children.insert(key, child);
    }
    if let Some(control) = &dynamic {
        root.with_observation_gate(|txn| {
            control.register_initial(children.iter().map(|(key, child)| (&child.slot, *key)), txn);
        });
    }
    let mut scope = ScopeRuntime::new(
        ScopeRuntimeWiring {
            root: Arc::clone(&root),
            defaults: plan.defaults.clone(),
            intensity_policy: plan.intensity_policy(),
            children,
            supervisor,
            events,
            disposal_events,
            disposal_event_receiver,
            role,
            dynamic,
        },
        epoch,
    );
    plan.finish_transfer();
    scope.publish_initial_children();

    // SPEC §11: a stop never constructs the incarnation it stops. A request
    // latched before this first settlement is consumed ahead of it, so the
    // incarnation enters `Draining` with its initial children still unspawned
    // and drain entry terminalizes each as `NeverStarted`.
    if root.take_shutdown_request(scope.epoch) {
        scope.accept_shutdown_request();
    }
    scope.settle_supervisor();
    // That drain can finish the incarnation before any event exists to
    // wake the loop below.
    if let Some(reason) = scope.take_completion() {
        return reason;
    }

    let mut signal = root.signal().watcher();
    let mut pending = Vec::new();
    loop {
        if root.take_shutdown_request(scope.epoch) {
            pending.push(Pending::Shutdown.classified());
        }
        for event in root.take_control_events() {
            if let Some(work) = scope.control_event_work(event) {
                pending.push(work);
            }
        }
        if !scope.ancestor_shutdown_seen
            && scope
                .role
                .ancestor()
                .is_some_and(|latches| latches.framework_shutdown.is_fired())
        {
            scope.ancestor_shutdown_seen = true;
            pending.push(Pending::AncestorShutdown.classified());
        }
        if !scope.ancestor_abort_seen
            && scope
                .role
                .ancestor()
                .is_some_and(|latches| latches.abort.is_fired())
        {
            scope.ancestor_abort_seen = true;
            pending.push(Pending::AncestorAbort.classified());
        }
        if root.take_force_request(scope.epoch) {
            // Force owns shutdown arbitration: readiness from the same wake
            // cannot publish Running after the stop boundary.
            pending.push(Pending::Force.classified());
        }
        let lane_batch_full = collect_event_lanes(
            EventLanes {
                primary: &mut event_receiver,
                control: dynamic_event_receiver.as_mut(),
                disposal: &mut scope.disposal_event_receiver,
            },
            event_batch_limit,
            &mut pending,
        );
        let now = runtime::now();
        while let Some(deadline) = scope.deadlines.pop_due(now) {
            pending.push(Pending::Deadline(deadline).classified());
        }

        if pending.is_empty() {
            if let Some(reason) = wait_for_scope_wake(
                &mut scope,
                &mut signal,
                &mut event_receiver,
                dynamic_event_receiver.as_mut(),
                &mut pending,
            )
            .await
            {
                return reason;
            }
            // Every wake re-enters the collection site above. Nothing is
            // dispatched from this arm.
            continue;
        }

        arbitrate(&mut pending);
        for (_, event) in pending.drain(..) {
            match event {
                Pending::Shutdown => scope.accept_shutdown_request(),
                Pending::WindowStop { child, target } => {
                    scope.resolve_window_stop(child, target);
                }
                Pending::AncestorShutdown => {
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::AncestorAbort => {
                    if let Some(latches) = scope.role.ancestor() {
                        latches.abort_ack.fire();
                    }
                    // A scheduled framework driver recursively hard-drains
                    // and joins its children. Its parent only task-aborts it
                    // at the tidy-beat backstop when this acknowledgement is
                    // never published.
                    scope.force_all();
                }
                Pending::Force => {
                    scope.force_all();
                }
                Pending::Removal(removal) => scope.handle_removal(removal),
                Pending::Admission(request) => {
                    scope.handle_admission(request);
                }
                Pending::Child(ChildEvent::Ready { child, incarnation }) => {
                    scope.handle_ready(child, incarnation);
                }
                Pending::Child(ChildEvent::SelfStop { child, incarnation }) => {
                    scope.handle_self_stop(child, incarnation)
                }
                Pending::Child(ChildEvent::Exited {
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                }) => scope.handle_exit(
                    child,
                    incarnation,
                    recorded,
                    join,
                    cancellation,
                    readiness_signal_seen,
                ),
                Pending::Child(ChildEvent::ConstructionDisposed { child }) => {
                    scope.handle_construction_disposed(child);
                }
                Pending::Deadline(deadline) => scope.handle_deadline(deadline),
            }
        }

        // Settlement is level-triggered from authoritative state after every
        // batch. Any transition that changes the startup aggregate therefore
        // gets the same recomputation point as terminal completion.
        scope.settle_supervisor();
        // A removal response is also an observation edge: SPEC §7 promises
        // that a returned `Removed` has already been incorporated into the
        // startup aggregate. Finalization retains starting-phase obligations
        // until the recomputation above establishes that order.
        scope.publish_startup_removals();
        if let Some(reason) = scope.take_completion() {
            return reason;
        }

        // A full lane may still have a queued suffix. On a current-thread
        // runtime, immediately collecting the next batch would prevent the
        // child, timer, and helper tasks whose events this loop prioritizes
        // from running at all. Give those producers one scheduler turn before
        // returning to any saturated lane.
        if lane_batch_full {
            runtime::yield_now().await;
        }
    }
}
