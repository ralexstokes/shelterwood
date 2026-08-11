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
        if scope.incarnation_finished(epoch)
            || matches!(scope.member.record().stage, MemberStage::Terminal(_))
        {
            return;
        }
        watcher.changed().await;
    }
}

pub(crate) async fn shutdown_scope(
    scope: Arc<ScopeCell>,
    timeout: Duration,
) -> Result<(), ShutdownTimeout> {
    if matches!(scope.member.record().stage, MemberStage::Terminal(_)) {
        return Ok(());
    }
    let Some(epoch) = scope.request_shutdown() else {
        // An exhausted idle scope has no incarnation that can remain live.
        return Ok(());
    };
    let mut watcher = scope.signal().watcher();
    loop {
        if scope.incarnation_finished(epoch)
            || matches!(scope.member.record().stage, MemberStage::Terminal(_))
        {
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
            .lifecycle
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
        let Some(effect) = self.lifecycle.begin_drain(reason) else {
            return;
        };
        if let Some(startup) = startup {
            self.root.set_state_and_startup(effect.state, startup);
        } else {
            debug_assert!(!effect.startup_pending);
            self.root.set_state(effect.state);
        }
        match self.root.flavor {
            ScopeFlavor::Ordered => {
                self.ordered_stop_cursor = self.children.keys().next_back();
                self.stop_next_ordered();
            }
            ScopeFlavor::Dynamic => {
                let children: Vec<_> = self.children.keys().collect();
                for child in children {
                    self.begin_stop_child(child, None);
                }
            }
        }
    }

    pub(super) fn stop_next_ordered(&mut self) {
        if self.root.flavor != ScopeFlavor::Ordered
            || !self.lifecycle.is_draining()
            || self.ordered_stop_progressing
        {
            return;
        }
        self.ordered_stop_progressing = true;
        if let Some(key) = self.ordered_stop_waiting {
            let waiting = self
                .children
                .get(key)
                .is_some_and(ChildRuntime::is_incomplete);
            if waiting {
                self.ordered_stop_progressing = false;
                return;
            }
            self.ordered_stop_waiting = None;
        }
        while let Some(key) = self.ordered_stop_cursor {
            self.ordered_stop_cursor = self.children.previous_key(key);
            #[cfg(test)]
            {
                self.ordered_stop_inspections += 1;
            }
            // The cursor key is held across await boundaries, so never index
            // the arena with it: a reclaimed slot is treated as already gone.
            let Some(child) = self.children.get(key) else {
                continue;
            };
            if !child.is_incomplete() {
                continue;
            }
            self.begin_stop_child(key, None);
            let Some(child) = self.children.get(key) else {
                continue;
            };
            if child.active.is_some() || child.is_disposing() {
                self.ordered_stop_waiting = Some(key);
                break;
            }
        }
        self.ordered_stop_progressing = false;
    }

    pub(super) fn force_all(&mut self) {
        self.hard_forced = true;
        // Unconditional: on an already-draining scope this is a pure monotone
        // reason upgrade (no drain-entry side effects are replayed), and a
        // hard-forced scope must terminalize as ShutdownRequested even if it
        // was mid-drain for a lower-precedence reason.
        self.begin_drain(StopReason::ShutdownRequested);
        let now = runtime::now();
        let children: Vec<_> = self.children.keys().collect();
        for key in children {
            // Every live membership enters the same stop funnel first. That
            // owns mailbox freeze, readiness disarm, ordered-child handling,
            // and the initial cooperative action.
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
        }
        let disposing = self
            .children
            .keys()
            .filter(|key| self.children[*key].pending_terminal.is_some())
            .collect::<Vec<_>>();
        for key in disposing {
            // The incarnation has already exited; only its retained factory
            // remains. Hard escalation detaches that cleanup, but must not
            // rewrite the actor's recorded verdict.
            self.handle_construction_disposed(key, None);
        }
    }

    pub(super) fn finish_if_ready(&mut self) -> Option<StopReason> {
        self.lifecycle.finish_if_ready(
            self.root.flavor,
            ChildCompletionState {
                has_children: !self.children.is_empty(),
                all_terminal: self.incomplete_children == 0,
            },
        )
    }
}
