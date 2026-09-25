use super::*;

impl ScopeRuntime {
    pub(super) fn begin_stop_child(&mut self, key: ChildKey, forced: Option<RecordedOutcome>) {
        let Some(child) = self.children.get(&key) else {
            return;
        };
        if self.supervisor.joined(key) || self.supervisor.is_disposing(key) {
            return;
        }
        if child
            .active
            .as_ref()
            .is_some_and(|active| active.ladder.is_some())
        {
            // A readiness timeout is the only forced stop, and it comes from
            // a `Waiting` gate. Arming the ladder below leaves the gate
            // `Ready` or `Disarmed`, neither of which can time out.
            debug_assert!(
                forced.is_none(),
                "a readiness timeout cannot follow an armed stop ladder"
            );
            return;
        }
        // Shutdown outranks queued readiness (§13). Disarm below without
        // replaying a fired latch: readiness cannot publish during drain.
        // Local self-stop credits readiness in its own handler before this.
        if child.active.is_some() {
            self.reduce(SupervisorEvent::StopStarted { child: key });
            let child = self
                .children
                .get_mut(&key)
                .expect("the stopped child remains registered");
            let active = child
                .active
                .as_mut()
                .expect("the stopped child remains active");
            // The stop ladder is armed only for an active incarnation, whose
            // projection is `Starting` or `Running`.
            let stopping = self.root.transition_child_stage(
                &child.slot.member,
                MemberTransition::Stopping,
                None,
            );
            assert!(
                stopping,
                "a stop ladder begins on a starting or running member"
            );
            if let Some(mailbox) = &child.mailbox {
                let mut effects = MailboxEffectQueue::default();
                mailbox.freeze(active.incarnation, &mut effects);
            }
            active.forced_outcome = forced;
            if let Some(deadline) = active.readiness_deadline.take() {
                self.deadlines.cancel(deadline);
            }
            if active.forced_outcome.is_none() {
                active.readiness.step(ReadinessEvent::Shutdown);
            }
            active.ladder = Some(if active.framework_abort.is_some() {
                StopLadder::for_framework_driver(child.options.shutdown)
            } else {
                StopLadder::new(child.options.shutdown)
            });
            self.advance_ladder(key, runtime::now());
        } else {
            // A drain has already taken the startup verdict, and removal
            // shrinks the initial set, so neither reports a startup abort.
            self.terminate_inactive(key, StartupDisposition::NotAborted);
        }
    }

    /// Terminalizes a membership with no live incarnation: one that never
    /// ran, or one stopped between restart incarnations. Both share one
    /// route — cancel any pending restart, publish the last exit (or
    /// `NeverStarted`), then release through disposal. Hard shutdown still
    /// detaches that disposal through `hard_forced`.
    pub(super) fn terminate_inactive(&mut self, key: ChildKey, startup: StartupDisposition) {
        let Some(child) = self.children.get_mut(&key) else {
            return;
        };
        if let Some(deadline) = child.restart_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        let record = child.slot.member.record();
        let exit = record.last_exit.clone().unwrap_or_else(Exit::never_started);
        self.begin_terminal_disposal(key, Retained::new(exit), None, startup);
    }

    pub(super) fn advance_ladder(&mut self, key: ChildKey, now: Instant) {
        let Some(child) = self.children.get_mut(&key) else {
            return;
        };
        let Some(active) = &mut child.active else {
            return;
        };
        if let Some(deadline) = active.stop_deadline.take() {
            self.deadlines.cancel(deadline);
        }
        let Some(ladder) = &mut active.ladder else {
            return;
        };
        if active
            .framework_abort_ack
            .as_ref()
            .is_some_and(Latch::is_fired)
        {
            ladder.acknowledge_framework_abort();
        }
        while let Some(action) = ladder.advance(now) {
            match action {
                StopAction::Cancel => {
                    // Publish the framework-only edge independently so a
                    // hostile user cancellation waiter cannot strand the
                    // nested scope driver. The latch implementation itself
                    // finishes every waiter before this call resumes a panic.
                    fire_shutdown_edges(&active.shutdown, active.framework_shutdown.as_ref());
                }
                StopAction::Escalate => {
                    active.abort.fire();
                }
                StopAction::AbortFramework { phase } => {
                    active.hard_abort_phase = Some(phase);
                    if active.forced_outcome.is_none() {
                        active.forced_outcome = Some(RecordedOutcome::aborted(phase));
                    }
                    active
                        .framework_abort
                        .as_ref()
                        .expect("framework action belongs only to a framework driver")
                        .fire();
                }
                StopAction::HardAbort { phase } => {
                    active.hard_abort_phase = Some(phase);
                    active.abort_handle.abort();
                }
            }
        }
        let ladder_deadline = ladder.deadline();
        if let Some(deadline) = ladder_deadline {
            active.stop_deadline = Some(self.deadlines.push(
                deadline,
                DeadlineKind::Stop {
                    child: key,
                    incarnation: active.incarnation,
                },
            ));
        }
    }

    pub(super) fn handle_self_stop(&mut self, key: ChildKey, incarnation: Incarnation) {
        let ready_before_stop = self
            .children
            .get(&key)
            .and_then(|child| child.active.as_ref())
            .is_some_and(|active| {
                active.incarnation == incarnation && active.ready_signal.is_fired()
            });
        if ready_before_stop {
            // A local stop is reported on a separate helper task. Preserve
            // the application task's mark-ready-before-stop order even when
            // arbitration observes the stop before the readiness event.
            // An inverted `stop(); mark_ready()` sequence may also count as
            // ready here when its latch fires before the driver observes the
            // stop — licensed by the spec's "fired before ... a clean
            // self-stop is observed" wording (§7).
            self.handle_ready(key, incarnation);
        }
        if self
            .children
            .get(&key)
            .and_then(|child| child.active.as_ref())
            .is_some_and(|active| active.incarnation == incarnation)
        {
            self.begin_stop_child(key, None);
        }
    }
}
