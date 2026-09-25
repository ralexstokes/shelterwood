//! A child's runtime resources, exit dispatch, terminal publication and
//! release edge.

use super::*;

pub(super) struct ActiveChild {
    pub(super) incarnation: Incarnation,
    pub(super) started_at: Instant,
    pub(super) shutdown: Latch,
    pub(super) abort: Latch,
    pub(super) abort_handle: runtime::AbortHandle,
    pub(super) ladder: Option<StopLadder>,
    pub(super) forced_outcome: Option<RecordedOutcome>,
    pub(super) hard_abort_phase: Option<GracePhase>,
    pub(super) readiness: ReadinessGate,
    pub(super) readiness_deadline: Option<DeadlineHandle>,
    pub(super) ready_signal: CompletionGatedLatch,
    pub(super) framework_shutdown: Option<Latch>,
    pub(super) framework_abort: Option<Latch>,
    pub(super) framework_abort_ack: Option<Latch>,
    pub(super) stop_deadline: Option<DeadlineHandle>,
}

pub(super) struct ChildTerminality {
    pub(super) root: Arc<ScopeCell>,
    pub(super) slot: Arc<SlotCell>,
}

pub(super) fn discharge_child_terminality(completion: ChildTerminality) {
    let record = completion.slot.member.record();
    if matches!(record.stage, MemberStage::Terminal(_)) {
        return;
    }
    let never_started = record.last_incarnation.is_none();
    let (exit, exited_incarnation) = if never_started {
        (Exit::never_started(), None)
    } else {
        (
            classify_exit_retaining(
                None,
                runtime::JoinOutcome::Cancelled,
                None,
                Cancellation::Observed,
            ),
            record.incarnation,
        )
    };
    // Initial-child conversion precedes residency publication. If a later
    // conversion unwinds, use the slot-owned gate for the converted prefix;
    // `terminalize_child` cannot discover those slots through the parent's
    // resident list yet. Once resident, the parent path synthesizes a nested
    // NeverStarted scope stop in the same observation transaction as the
    // membership edge. A restarting scope already published its real prior-
    // incarnation reason.
    if never_started && !completion.root.has_resident_child(&completion.slot.member) {
        // Every terminalization evicts the lineage; the slot-owned path
        // passes the owning scope so a restart's rebuild adopts a fresh,
        // incomparable membership instead of an ordered successor.
        completion.slot.terminalize_never_started(&completion.root);
        return;
    }
    completion.root.terminalize_child(
        &completion.slot.member,
        exit,
        exited_incarnation,
        StartupDisposition::NotAborted,
    );
}

pub(super) struct ChildRuntime {
    pub(super) slot: Arc<SlotCell>,
    pub(super) mailbox: Option<Arc<dyn MailboxControl>>,
    pub(super) mailbox_bind: Option<MailboxBindToken>,
    pub(super) terminality: Obligation<ChildTerminality>,
    pub(super) construction: runtime::Isolated<ChildConstruction>,
    pub(super) options: crate::policy::ResolvedCommonOptions,
    pub(super) incarnations: IncarnationCounter,
    pub(super) restarts: RestartState,
    pub(super) restart_deadline: Option<DeadlineHandle>,
    pub(super) active: Option<ActiveChild>,
}

impl ChildRuntime {
    pub(super) fn from_plan(plan: ChildPlan, scope: &Arc<ScopeCell>) -> Self {
        let ChildPlan {
            slot,
            construction,
            options,
        } = plan;
        // Arm terminality before any fallible setup. If a poisoned lock or
        // mailbox callback unwinds construction, this child has already left
        // ScopePlan and therefore needs its own synchronous fallback.
        let terminality = Obligation::new(
            ChildTerminality {
                root: Arc::clone(scope),
                slot: Arc::clone(&slot),
            },
            discharge_child_terminality,
        );
        let incarnations = slot.member.take_incarnation_counter();
        let mailbox = slot.member.mailbox();
        let mailbox_bind = if let Some(mailbox) = &mailbox {
            let mut effects = MailboxEffectQueue::default();
            Some(mailbox.configure(options.mailbox, &mut effects))
        } else {
            None
        };
        Self {
            terminality,
            slot,
            mailbox,
            mailbox_bind,
            construction,
            options,
            incarnations,
            restarts: RestartState::new(),
            restart_deadline: None,
            active: None,
        }
    }

    #[cfg(test)]
    pub(super) fn is_terminal(&self) -> bool {
        matches!(self.slot.member.record().stage, MemberStage::Terminal(_))
    }

    pub(super) fn terminalize(
        &mut self,
        root: &ScopeCell,
        exit: Retained<Exit>,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        let terminalized = runtime::catch_panic(|| {
            root.terminalize_child(&self.slot.member, exit, exited_incarnation, startup)
        });
        if matches!(self.slot.member.record().stage, MemberStage::Terminal(_)) {
            self.terminality.complete(drop);
        }
        match terminalized {
            Ok(changed) => changed,
            Err(payload) => runtime::resume_panic(payload),
        }
    }

    pub(super) fn complete_terminality(&mut self) {
        self.terminality.complete(drop);
    }
}

impl ScopeRuntime {
    /// Publishes a terminal exit and joins the membership in one step.
    ///
    /// Production splits these edges: `begin_terminal_disposal` publishes at
    /// dispatch and `handle_construction_disposed` joins at the release edge.
    /// A few structural tests synthesize the already-released boundary
    /// directly and drive both halves here.
    #[cfg(test)]
    pub(super) fn terminalize_child(
        &mut self,
        key: ChildKey,
        exit: impl Into<Retained<Exit>>,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        // Protect the raw user error before every resource lookup and reducer
        // invariant. A malformed key may still be diagnosed, but it cannot
        // unwind the Exit payload on the driver stack.
        let exit = exit.into();
        // Normalize through the same reducer predecessor production uses,
        // instead of allowing `Terminalized` to skip arbitrary incarnation
        // states.
        if !self.supervisor.is_disposing(key) && !self.supervisor.joined(key) {
            self.reduce(SupervisorEvent::DisposalStarted { child: key });
        }
        let changed = self.publish_terminal(key, exit, exited_incarnation, startup);
        self.join_terminal(key);
        changed
    }

    /// Publishes a disposing membership's final exit through the cell layer.
    ///
    /// The exit is final at dispatch (SPEC §9): publication never waits on
    /// the retained construction's destruction, which is only the release
    /// edge `join_terminal` records.
    fn publish_terminal(
        &mut self,
        key: ChildKey,
        exit: Retained<Exit>,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) -> bool {
        let mut exit = Some(exit);
        let child = self
            .children
            .get_mut(&key)
            .expect("terminalized child remains registered");
        let member = Arc::clone(&child.slot.member);
        let changed = child.terminalize(
            &self.root,
            exit.take()
                .expect("terminal publication consumes its retained exit once"),
            exited_incarnation,
            startup,
        );
        // A drain entry may have marked this member as pending terminal
        // cleanup. Clear the marker only after terminal publication has
        // committed, so a concurrent shutdown sampler sees either the marker
        // or a terminal member, never the gap between those two
        // representations.
        //
        // Argued, not pinned: clearing the marker before publication reopens
        // that gap for a few instructions, which no test in the suite can
        // provoke deterministically.
        member.set_terminal_disposal_pending(false);
        changed
    }

    /// Records the release edge: the retained construction is destroyed, so
    /// the membership joins.
    fn join_terminal(&mut self, key: ChildKey) {
        self.reduce(SupervisorEvent::Terminalized { child: key });
        // The reducer drops an event whose predecessor never ran, which keeps
        // `step` total but leaves the shell no return channel. A child that
        // never reached `Joined` would never count toward completion, so the
        // scope would simply never finish. Assert the transition landed
        // rather than discovering it as a stall.
        assert!(
            self.supervisor.joined(key),
            "the release edge must leave the reducer's membership joined"
        );
    }

    pub(super) fn handle_exit(
        &mut self,
        key: ChildKey,
        incarnation: Incarnation,
        recorded: Option<Retained<RecordedOutcome>>,
        join: runtime::JoinOutcome<()>,
        cancellation: Cancellation,
        readiness_signal_seen: bool,
    ) {
        let readiness_effect = self
            .children
            .get_mut(&key)
            .and_then(|child| child.active.as_mut())
            .filter(|active| active.incarnation == incarnation)
            .and_then(|active| {
                active.readiness.step(ReadinessEvent::Exit {
                    signal_seen: readiness_signal_seen,
                })
            });
        let became_ready = readiness_effect
            .map(|effect| self.apply_readiness_effect(key, incarnation, effect))
            .unwrap_or(false);
        if became_ready {
            // Match the natural signal-before-exit order: ordered startup may
            // advance, and a sole ready child completes aggregate startup
            // before its post-ready exit is classified.
            self.settle_supervisor();
        }

        let Some(child) = self.children.get_mut(&key) else {
            return;
        };
        let Some(mut active) = child.active.take() else {
            return;
        };
        if active.incarnation != incarnation {
            child.active = Some(active);
            return;
        }
        if let Some(deadline) = active.readiness_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        if let Some(deadline) = active.stop_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        if let Some(mailbox) = &child.mailbox {
            let mut effects = MailboxEffectQueue::default();
            let closed = mailbox.close(incarnation, &mut effects);
            drop(effects);
            if let Some(closed) = closed {
                let (token, teardown) = closed.into_parts();
                child.mailbox_bind = Some(token);
                runtime::dispose_detached(teardown);
            }
        }
        let recorded = reconcile_recorded_outcomes_retaining(recorded, active.forced_outcome);
        let exit = Retained::new(classify_exit_retaining(
            recorded,
            join,
            active.hard_abort_phase,
            cancellation,
        ));
        child.restarts.settle_if_stable(
            IncarnationRun {
                started_at: active.started_at,
                stopped_at: runtime::now(),
            },
            self.intensity_policy.within(),
        );
        self.reduce(SupervisorEvent::IncarnationComplete { child: key });

        // Fused cancellation is a level-triggered source. It can linearize
        // before the forwarded Removal event or its public status projection
        // reaches this driver, so exit dispatch must consult the
        // removal sources directly before charging or publishing a restart.
        // Only removal sources classify the membership here: a latched but
        // unprocessed scope stop (shutdown request or ancestor latch) must
        // not turn this exit Terminal, or a restartable initial child
        // failing pre-ready would publish `StartupFailed` where the stop's
        // own follow-up event owns the verdict. The broader
        // `construction_is_suppressed` still gates the restart deadline arm,
        // where every suppression source has a guaranteed follow-up event.
        let membership_status = self.dispatch_membership_status(key);
        let startup = self.terminal_startup_disposition(key);
        let child = self
            .children
            .get_mut(&key)
            .expect("the exiting child remains registered");

        match dispatch_exit(
            exit.get(),
            child.options.restart,
            self.supervisor.lifecycle().is_draining(),
            membership_status,
        ) {
            ExitDispatch::Terminal => {
                self.begin_terminal_disposal(key, exit, Some(incarnation), startup);
            }
            ExitDispatch::ScheduleRestart => {
                let sample =
                    JitterSample::from_u64_ratio(self.jitter.sample(0..u64::MAX), u64::MAX);
                let now = runtime::now();
                let decision = schedule_restart(
                    &mut child.restarts,
                    &mut self.intensity,
                    self.intensity_policy,
                    child.options.restart,
                    now,
                    sample,
                );
                // The exiting incarnation's projection is `Starting`,
                // `Running` or `Stopping`, all accepted sources.
                // `schedule_restart` above has already charged this attempt
                // against the child and the intensity window, so a refusal
                // would restart the child with neither `Exited` nor
                // `RestartScheduled` published.
                let raw_exit = exit.get().clone();
                let published = self.root.publish_child_restart(
                    &child.slot.member,
                    decision.total_restarts(),
                    exit,
                    MemberTransition::RestartScheduled {
                        exit: raw_exit.clone(),
                        restart_count: decision.restart_count(),
                        // Publish the derived schedule even when intensity prevents spawning it.
                        // `None` means the exact clock point cannot be represented and armed; no
                        // substitute restart is scheduled.
                        restart_at: decision.restart_at(),
                    },
                    LifecycleEventKind::Exited {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        incarnation,
                        exit: raw_exit,
                    },
                    LifecycleEventKind::RestartScheduled {
                        id: child.slot.member.id().clone(),
                        membership: child.slot.member.membership(),
                        attempt: decision.attempt(),
                        delay: decision.delay(),
                    },
                );
                assert!(
                    published,
                    "a restart is scheduled from an active incarnation's exit"
                );
                let trip = decision.intensity_trip();
                if trip.is_none()
                    && let Some(restart_at) = decision.restart_at()
                {
                    child.restart_deadline = Some(
                        self.deadlines
                            .push(restart_at, DeadlineKind::Restart { child: key }),
                    );
                }
                let startup_pending = self.supervisor.lifecycle().is_starting();
                self.reduce(SupervisorEvent::RestartPending { child: key });
                if let Some(trip) = trip {
                    if startup_pending {
                        self.begin_drain_with_startup(
                            StopReason::IntensityTripped(trip.clone()),
                            Err(StartupError::IntensityTripped(trip)),
                        );
                    } else {
                        self.begin_drain(StopReason::IntensityTripped(trip));
                    }
                }
            }
        }
        // A window stop can beat this exit into an earlier batch, where the
        // resolver found the incarnation still active and left the request
        // pending. Re-check the level now that the member is inactive. The
        // resolver constructs nothing, so resolving mid-batch cannot start a
        // doomed incarnation ahead of a same-batch intensity trip.
        if let Some(target) = self
            .children
            .get(&key)
            .and_then(|child| child.slot.scope.as_ref())
            .and_then(|scope| scope.pending_incarnation_shutdown())
        {
            self.resolve_window_stop(key, target);
        }
    }

    pub(super) fn terminal_startup_disposition(&self, key: ChildKey) -> StartupDisposition {
        // §7's startup abort is a startup-sequence property: the membership
        // failed before its *initial* readiness edge. A later incarnation
        // stopped pre-ready (for example during drain) does not rewind it.
        // A drain has already taken the startup verdict — an owner or
        // ancestor shutdown, or this scope's own rollback — and dispatches
        // every exit terminal regardless of policy, so an exit it dispatches
        // is the drain's, never the §7 terminal pre-ready failure (B.6).
        // Likewise, removal sampled before dispatch owns the terminal: §7
        // shrinks the initial set instead of reporting a startup failure.
        if self.supervisor.is_initial(key)
            && !self.supervisor.lifecycle().startup_complete()
            && !self.supervisor.lifecycle().is_draining()
            && self.supervisor.membership_status(key) != MembershipStatus::Removing
            && !self.supervisor.initial_ready(key)
        {
            StartupDisposition::Aborted
        } else {
            StartupDisposition::NotAborted
        }
    }

    pub(super) fn begin_terminal_disposal(
        &mut self,
        key: ChildKey,
        exit: Retained<Exit>,
        exited_incarnation: Option<Incarnation>,
        startup: StartupDisposition,
    ) {
        // Keep the caller's guard through every refusal and invariant verdict.
        // A failed Exit can own hostile user error drop glue, so no call-site
        // argument window or local path may unwind or return it directly on
        // the driver thread.
        let mut exit = Some(exit);
        if !self.supervisor.contains(key)
            || self.supervisor.is_disposing(key)
            || self.supervisor.joined(key)
            || !self.children.contains_key(&key)
        {
            return;
        }
        self.reduce(SupervisorEvent::DisposalStarted { child: key });
        // A dropped `DisposalStarted` would make the later `Terminalized`
        // unreachable too, stranding the membership short of `Joined` with
        // no loud failure. `Disposing` is also the one-disposal-in-flight
        // guard: the refusal above admits a single terminal per membership.
        assert!(
            self.supervisor.is_disposing(key),
            "terminal disposal must leave the reducer's incarnation disposing"
        );
        let exit = exit
            .take()
            .expect("terminal disposal publishes its retained exit once");

        // The exit is final at dispatch, and so is its publication (SPEC
        // §9). §7's `StartupAborted` is a startup-sequence property of a
        // membership that *ran* and failed before its initial readiness
        // edge. A membership that never spawned publishes the plain
        // `Stopped { NeverStarted }` verdict (B.6) even when its
        // pre-readiness position still routes the scope's startup failure
        // below. One stopped in its restart window did run: its terminal
        // carries its last exit and keeps the startup disposition, though it
        // has no exiting incarnation to name.
        //
        // Hand the publication seam a guarded clone rather than a raw one, so
        // no window between here and the cell layer's own retention holds the
        // user error unguarded. The cell layer surrenders its copy inside the
        // publishing transaction, where the terminal member record is its
        // structural co-owner.
        let publication = if exited_incarnation.is_some() || self.supervisor.spawned_once(key) {
            startup
        } else {
            StartupDisposition::NotAborted
        };
        self.publish_terminal(key, exit.clone(), exited_incarnation, publication);

        // §7: the scope leaves `Starting` the moment the exit funnel
        // dispatches a terminal pre-ready exit, and that exit names the
        // failure. Routing it after publication is what makes SPEC §12's
        // guarantee hold: a reported child-caused `StartupFailed` is never
        // ahead of its child, whose published exit is exactly the payload's.
        if startup == StartupDisposition::Aborted
            && self.supervisor.membership_status(key) != MembershipStatus::Removing
            && !self.supervisor.lifecycle().is_draining()
        {
            self.fail_startup(key, &exit);
        }
        // Both routes above are fallible, so the guard retires once, here, by
        // falling out of scope whichever route ran. A driver-layer caller
        // cannot surrender a guard, and the driver owns no observation
        // transaction to surrender into, so `Retained::drop` is the venue:
        // it retires a failed user error through critical disposal at the cost
        // of one blocking-pool job.
        drop(exit);

        // Release edge. Startup routing can reenter teardown, so re-sample
        // the membership: a hard-force fallback may already have joined it,
        // and a joined remove-retained member may already be pruned.
        let construction = match self.children.get_mut(&key) {
            Some(child) => child.construction.take(),
            None => return,
        };
        if !self.supervisor.is_disposing(key) {
            if let Some(construction) = construction {
                runtime::dispose_detached(construction);
            }
            return;
        }
        let Some(construction) = construction else {
            self.handle_construction_disposed(key);
            return;
        };
        if self.supervisor.hard_forced() {
            runtime::dispose_detached(construction);
            self.handle_construction_disposed(key);
            return;
        }

        // The retained factory is user-owned. Destroy it on the blocking
        // pool. The disposal job itself owns completion, so cancellation or
        // failure to spawn an auxiliary async joiner cannot strand the child.
        // A destructor panic is a disposal fault outside the published
        // verdict (SPEC §8); the job contains it.
        let sender = self.disposal_events.clone();
        let signal = self.root.signal().clone();
        runtime::dispose_then(construction, move || {
            if sender
                .send(DriverEvent::Child(ChildEvent::ConstructionDisposed {
                    child: key,
                }))
                .is_ok()
            {
                signal.pulse();
            }
        });
    }

    /// Crosses a terminal membership's release edge (SPEC §9).
    ///
    /// The exit already published at dispatch. Joining gates pruning,
    /// `remove`'s resolution, the ordered-teardown cursor and the drained
    /// test. A completion for a membership that already joined — a hard
    /// force or driver teardown stopped waiting for it — is a no-op.
    pub(super) fn handle_construction_disposed(&mut self, key: ChildKey) {
        if !self.supervisor.is_disposing(key) || !self.children.contains_key(&key) {
            return;
        }
        self.join_terminal(key);
        if self.supervisor.membership_status(key) == MembershipStatus::Removing {
            self.flush_supervisor_effects();
        } else if self.children[&key].options.retention == crate::Retention::Remove {
            self.prune_terminal(key);
        }
    }
}
