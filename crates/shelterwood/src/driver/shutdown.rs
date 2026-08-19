use super::*;

fn collect_stragglers(scope: &ScopeCell, prefix: &[ChildId], out: &mut Vec<ShutdownStraggler>) {
    let children = scope.resident_projections();
    for child in children {
        // Read the release/acquire marker before the terminal projection. If
        // terminal publication has already cleared it, that acquire orders
        // the following record read after publication; if cleanup is still
        // pending, the marker itself excludes the child. Reading the record
        // first could pair a stale nonterminal projection with the later
        // cleared marker and recreate a narrow trailing gap.
        //
        // Argued, not pinned: the window is too narrow to provoke
        // deterministically, and reverting to record-first leaves the suite
        // green.
        if child.member.terminal_disposal_pending()
            || matches!(child.member.record().stage, MemberStage::Terminal(_))
        {
            continue;
        }
        let mut path = prefix.to_vec();
        path.push(child.member.id().clone());
        let before = out.len();
        if let Some(nested) = &child.scope {
            collect_stragglers(nested, &path, out);
        }
        if out.len() == before {
            out.push(ShutdownStraggler {
                path,
                membership: child.member.membership(),
            });
        }
    }
}

async fn wait_for_incarnation(scope: &ScopeCell, epoch: Epoch) {
    let mut watcher = scope.signal().watcher();
    loop {
        if scope.settled(Some(epoch)) {
            return;
        }
        watcher.changed().await;
    }
}

pub(crate) async fn shutdown_scope(
    scope: Arc<ScopeCell>,
    timeout: DeadlineBudget,
) -> Result<(), ShutdownTimeout> {
    if scope.settled(None) {
        return Ok(());
    }
    let Some(epoch) = scope.request_shutdown() else {
        // An exhausted idle scope has no incarnation that can remain live.
        return Ok(());
    };
    let mut watcher = scope.signal().watcher();
    loop {
        if scope.settled(Some(epoch)) {
            return Ok(());
        }
        if matches!(scope.record().state, ScopeState::Draining) {
            break;
        }
        watcher.changed().await;
    }

    // Zero selects immediate escalation (SPEC Appendix B): it is an escalation
    // budget, not an observation opportunity, so the cooperative request above
    // is still delivered but its wait is skipped even when the incarnation
    // could settle on an immediate poll.
    let outcome = if timeout.is_zero() {
        runtime::Timeout::Elapsed
    } else {
        runtime::timeout(timeout.duration(), wait_for_incarnation(&scope, epoch)).await
    };
    match outcome {
        runtime::Timeout::Completed(()) => Ok(()),
        runtime::Timeout::Elapsed => {
            let mut stragglers = Vec::new();
            collect_stragglers(&scope, &[], &mut stragglers);
            scope.force_shutdown(epoch);
            wait_for_incarnation(&scope, epoch).await;
            if stragglers.is_empty() {
                Ok(())
            } else {
                Err(ShutdownTimeout { stragglers })
            }
        }
    }
}

impl ScopeRuntime {
    fn drain_entry_terminal_disposals(&self, effects: &[SupervisorEffect]) -> Vec<Arc<MemberCell>> {
        let mut keys = BTreeSet::new();
        for effect in effects {
            let key = match effect {
                SupervisorEffect::StopChild { child } | SupervisorEffect::ForceChild { child } => {
                    *child
                }
                _ => continue,
            };
            if self.supervisor.joined(key)
                || self.supervisor.is_disposing(key)
                || self
                    .children
                    .get(key)
                    .is_none_or(|child| child.active.is_some())
            {
                continue;
            }
            keys.insert(key);
        }
        keys.into_iter()
            .filter_map(|key| {
                self.children
                    .get(key)
                    .map(|child| Arc::clone(&child.slot.member))
            })
            .collect()
    }

    pub(super) fn begin_drain(&mut self, reason: StopReason) {
        let startup = self
            .supervisor
            .lifecycle()
            .is_starting()
            .then_some(Err(StartupError::ShutdownRequested));
        self.begin_drain_transition(reason, startup);
    }

    pub(super) fn begin_drain_with_startup(
        &mut self,
        reason: StopReason,
        startup: Result<(), StartupError>,
    ) {
        self.begin_drain_transition(reason, Some(startup));
    }

    fn begin_drain_transition(
        &mut self,
        reason: StopReason,
        startup: Option<Result<(), StartupError>>,
    ) {
        // `ScopeLifecycle`, its emitted effects, and the driver's completion
        // slots all retain this reason as ordinary core data. A structured
        // startup failure recursively owns the triggering child's raw Exit,
        // so keep one cells-layer guard until every one of those slots has
        // retired.
        RetainedExit::retain_stop_reason(&mut self.retained_exits, &reason);
        let before = self.supervisor_effects.len();
        self.reduce(SupervisorEvent::BeginDrain { reason });
        let Some(position) = self.supervisor_effects[before..]
            .iter()
            .position(|effect| matches!(effect, SupervisorEffect::DrainStarted { .. }))
            .map(|position| before + position)
        else {
            return;
        };
        let SupervisorEffect::DrainStarted {
            state,
            startup_pending,
        } = self.supervisor_effects.remove(position)
        else {
            unreachable!()
        };
        debug_assert!(startup.is_some() || !startup_pending);
        // Ordered drain exposes its first stop only through `Settle`; derive
        // that command before publication so both scope flavors can commit an
        // inactive child's terminal-cleanup intent with the `Draining` edge.
        // Later ordered siblings are deliberately absent: their cooperative
        // stop has not begun and a zero-budget sample must still report them.
        self.reduce(SupervisorEvent::Settle);
        let terminal_disposals =
            self.drain_entry_terminal_disposals(&self.supervisor_effects[before..]);
        self.root.publish_drain(state, startup, &terminal_disposals);
        self.flush_supervisor_effects();
        self.settle_supervisor();
    }

    pub(super) fn force_all(&mut self) {
        let before = self.supervisor_effects.len();
        self.reduce(SupervisorEvent::Force);
        if let Some(position) = self.supervisor_effects[before..]
            .iter()
            .position(|effect| matches!(effect, SupervisorEffect::DrainStarted { .. }))
            .map(|position| before + position)
        {
            let SupervisorEffect::DrainStarted {
                state,
                startup_pending,
            } = self.supervisor_effects.remove(position)
            else {
                unreachable!()
            };
            // No pre-`Settle` here, unlike `begin_drain_transition`.
            // `SupervisorEvent::Force` already pushes `ForceChild` for every
            // non-joined child, a strict superset of the single `StopChild` an
            // ordered scope's `Settle` could add, so settling early cannot
            // contribute a member this selection would otherwise miss.
            let terminal_disposals =
                self.drain_entry_terminal_disposals(&self.supervisor_effects[before..]);
            self.root.publish_drain(
                state,
                startup_pending.then_some(Err(StartupError::ShutdownRequested)),
                &terminal_disposals,
            );
        }
        self.flush_supervisor_effects();
        self.settle_supervisor();
    }

    pub(super) fn force_child(&mut self, key: ChildKey) {
        let now = runtime::now();
        // Every live membership enters the same stop funnel first. That owns
        // mailbox freeze, readiness disarm, and the initial cooperative action.
        self.begin_stop_child(key, None);
        if let Some(ladder) = self
            .children
            .get_mut(key)
            .and_then(|child| child.active.as_mut())
            .and_then(|active| active.ladder.as_mut())
        {
            ladder.force(now);
        }
        self.advance_ladder(key, now);
        // A retained-factory destructor may already have completed even
        // though its disposal event has not reached ordinary dispatch. Fold
        // every arrived completion before the hard-force fallback; the drain
        // is non-blocking, so still-running disposal remains detached.
        self.drain_arrived_disposal_events();
        if self.supervisor.is_disposing(key) {
            // The incarnation has already exited; only its retained factory
            // remains, and the fold above found no completion reported for
            // it. Hard escalation detaches that cleanup and keeps the
            // recorded verdict.
            self.handle_construction_disposed(key, None);
        }
    }

    #[cfg(test)]
    pub(super) fn finish_if_ready(&mut self) -> Option<StopReason> {
        self.settle_supervisor();
        self.finished.take()
    }
}
