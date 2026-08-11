use super::*;

fn collect_stragglers(scope: &ScopeCell, prefix: &[ChildId], out: &mut Vec<ShutdownStraggler>) {
    let children = scope.resident_projections();
    for child in children {
        if matches!(child.member.record().stage, MemberStage::Terminal(_))
            || child.member.terminal_disposal_pending()
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
    timeout: Duration,
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

    match runtime::timeout(timeout, wait_for_incarnation(&scope, epoch)).await {
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
        if let Some(startup) = startup {
            self.root.set_state_and_startup(state, startup);
        } else {
            debug_assert!(!startup_pending);
            self.root.set_state(state);
        }
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
            if startup_pending {
                self.root
                    .set_state_and_startup(state, Err(StartupError::ShutdownRequested));
            } else {
                self.root.set_state(state);
            }
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
        if self.supervisor.is_disposing(key) {
            // The incarnation has already exited; only its retained factory
            // remains. Hard escalation detaches that cleanup, but must not
            // rewrite the actor's recorded verdict.
            self.handle_construction_disposed(key, None);
        }
    }

    #[cfg(test)]
    pub(super) fn finish_if_ready(&mut self) -> Option<StopReason> {
        self.settle_supervisor();
        self.finished.take()
    }
}
