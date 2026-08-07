//! Incarnation-owned peer monitoring and its separate actor-loop source.

use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
};

use crate::{
    ActorRef, ChildId, Exit, Incarnation, Membership, ScopeRef, TaskRef,
    driver::{Latch, MemberCell, Signal, SignalWatcher},
};

/// Number of raw monitor edges retained independently for each watch.
pub const MONITOR_EVENT_CAPACITY: usize = 128;

/// Kind of membership named by a monitor event.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MonitorMemberKind {
    /// Actor membership.
    Actor,
    /// Task membership.
    Task,
    /// Scope membership.
    Scope,
}

/// One peer-membership event delivered through an actor's monitor source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MonitorEvent {
    /// Target's child id in its supervising scope.
    pub member_id: ChildId,
    /// Target handle kind.
    pub member_kind: MonitorMemberKind,
    /// Stable target membership identity.
    pub membership: Membership,
    /// Target transition or loss marker.
    pub kind: MonitorEventKind,
}

/// Peer-monitor event inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MonitorEventKind {
    /// A target incarnation started.
    Started {
        /// Newly running incarnation.
        incarnation: Incarnation,
    },
    /// A target incarnation exited.
    Exited {
        /// Exited incarnation.
        incarnation: Incarnation,
        /// Classified exit.
        exit: Exit,
    },
    /// Older events were dropped from this watch's private queue.
    Lagged {
        /// Exact number of dropped events in this overflow episode.
        dropped: u64,
    },
    /// The target membership terminalized.
    Removed {
        /// Last target incarnation, or `None` when it never started.
        last_incarnation: Option<Incarnation>,
    },
}

/// Sealed membership target accepted by actor-context watch operations.
pub trait WatchTarget: sealed::Sealed {}

impl<M> WatchTarget for ActorRef<M> {}
impl WatchTarget for TaskRef {}
impl WatchTarget for ScopeRef {}

pub struct MonitorTarget {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) kind: MonitorMemberKind,
}

pub(crate) mod sealed {
    use super::MonitorTarget;

    pub trait Sealed {
        fn monitor_target(&self) -> MonitorTarget;
    }
}

impl<M> sealed::Sealed for ActorRef<M> {
    fn monitor_target(&self) -> MonitorTarget {
        MonitorTarget {
            member: self.member_cell(),
            kind: MonitorMemberKind::Actor,
        }
    }
}

impl sealed::Sealed for TaskRef {
    fn monitor_target(&self) -> MonitorTarget {
        MonitorTarget {
            member: Arc::clone(&self.cell),
            kind: MonitorMemberKind::Task,
        }
    }
}

impl sealed::Sealed for ScopeRef {
    fn monitor_target(&self) -> MonitorTarget {
        MonitorTarget {
            member: Arc::clone(&self.cell.member),
            kind: MonitorMemberKind::Scope,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MonitorSource {
    next_sequence: Mutex<u64>,
    signal: Signal,
}

impl Default for MonitorSource {
    fn default() -> Self {
        Self {
            next_sequence: Mutex::new(0),
            signal: Signal::default(),
        }
    }
}

impl MonitorSource {
    fn next_sequence(&self) -> u64 {
        let mut sequence = self
            .next_sequence
            .lock()
            .expect("monitor sequence mutex poisoned");
        *sequence = sequence.saturating_add(1);
        *sequence
    }

    pub(crate) fn watermark(&self) -> u64 {
        *self
            .next_sequence
            .lock()
            .expect("monitor sequence mutex poisoned")
    }

    pub(crate) fn watcher(&self) -> SignalWatcher {
        self.signal.watcher()
    }

    fn pulse(&self) {
        self.signal.pulse();
    }
}

type MonitorWrap<M> = Arc<dyn Fn(MonitorEvent) -> M + Send + Sync + 'static>;

struct SequencedMonitorEvent {
    sequence: u64,
    event: MonitorEvent,
}

struct LagMarker {
    sequence: u64,
    dropped: u64,
}

struct MonitorQueueState<M> {
    wrap: MonitorWrap<M>,
    events: VecDeque<SequencedMonitorEvent>,
    lagged: Option<LagMarker>,
    active: bool,
    terminal: bool,
    last_started: Option<Incarnation>,
    last_exited: Option<Incarnation>,
}

pub(crate) struct MonitorDelivery<M> {
    pub(crate) event: MonitorEvent,
    pub(crate) wrap: MonitorWrap<M>,
}

pub(crate) trait MonitorSubscriber: fmt::Debug + Send + Sync {
    fn publish(&self, kind: MonitorEventKind);
}

pub(crate) struct MonitorSink<M> {
    member_id: ChildId,
    member_kind: MonitorMemberKind,
    membership: Membership,
    source: Arc<MonitorSource>,
    state: Mutex<MonitorQueueState<M>>,
    finished: Latch,
}

impl<M> fmt::Debug for MonitorSink<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().expect("monitor queue mutex poisoned");
        formatter
            .debug_struct("MonitorSink")
            .field("membership", &self.membership)
            .field("queued", &state.events.len())
            .field(
                "lagged",
                &state.lagged.as_ref().map(|lagged| lagged.dropped),
            )
            .field("active", &state.active)
            .field("terminal", &state.terminal)
            .finish()
    }
}

impl<M> MonitorSink<M> {
    pub(crate) fn new<W>(
        target: &MonitorTarget,
        source: Arc<MonitorSource>,
        wrap: W,
        finished: Latch,
    ) -> Arc<Self>
    where
        W: Fn(MonitorEvent) -> M + Send + Sync + 'static,
    {
        Arc::new(Self {
            member_id: target.member.id().clone(),
            member_kind: target.kind,
            membership: target.member.membership(),
            source,
            state: Mutex::new(MonitorQueueState {
                wrap: Arc::new(wrap),
                events: VecDeque::new(),
                lagged: None,
                active: true,
                terminal: false,
                last_started: None,
                last_exited: None,
            }),
            finished,
        })
    }

    pub(crate) fn membership(&self) -> Membership {
        self.membership
    }

    pub(crate) fn finished_latch(&self) -> Latch {
        self.finished.clone()
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state
            .lock()
            .expect("monitor queue mutex poisoned")
            .active
    }

    pub(crate) fn replace<W>(&self, wrap: W)
    where
        W: Fn(MonitorEvent) -> M + Send + Sync + 'static,
    {
        let mut state = self.state.lock().expect("monitor queue mutex poisoned");
        state.wrap = Arc::new(wrap);
        state.events.clear();
        state.lagged = None;
        self.source.pulse();
    }

    pub(crate) fn cancel(&self) -> bool {
        let mut state = self.state.lock().expect("monitor queue mutex poisoned");
        if !state.active {
            return false;
        }
        state.active = false;
        state.events.clear();
        state.lagged = None;
        drop(state);
        self.finished.fire();
        self.source.pulse();
        true
    }

    pub(crate) fn pop_through(&self, limit: u64) -> Option<MonitorDelivery<M>> {
        let mut state = self.state.lock().expect("monitor queue mutex poisoned");
        if !state.active {
            return None;
        }
        let event = if state
            .lagged
            .as_ref()
            .is_some_and(|lagged| lagged.sequence <= limit)
        {
            let lagged = state.lagged.take().expect("checked lag marker");
            Some(MonitorEvent {
                member_id: self.member_id.clone(),
                member_kind: self.member_kind,
                membership: self.membership,
                kind: MonitorEventKind::Lagged {
                    dropped: lagged.dropped,
                },
            })
        } else if state
            .events
            .front()
            .is_some_and(|event| event.sequence <= limit)
        {
            state.events.pop_front().map(|event| event.event)
        } else {
            None
        }?;
        let terminal = matches!(event.kind, MonitorEventKind::Removed { .. });
        if terminal {
            state.active = false;
        }
        let wrap = Arc::clone(&state.wrap);
        drop(state);
        if terminal {
            self.finished.fire();
        }
        Some(MonitorDelivery { event, wrap })
    }
}

impl<M: Send + 'static> MonitorSubscriber for MonitorSink<M> {
    fn publish(&self, kind: MonitorEventKind) {
        let terminal = matches!(kind, MonitorEventKind::Removed { .. });
        let sequence = self.source.next_sequence();
        let mut state = self.state.lock().expect("monitor queue mutex poisoned");
        if !state.active || state.terminal {
            return;
        }
        match &kind {
            MonitorEventKind::Started { incarnation } => {
                if state.last_started == Some(*incarnation) {
                    return;
                }
                state.last_started = Some(*incarnation);
            }
            MonitorEventKind::Exited { incarnation, .. } => {
                if state.last_exited == Some(*incarnation) {
                    return;
                }
                state.last_exited = Some(*incarnation);
            }
            MonitorEventKind::Lagged { .. } | MonitorEventKind::Removed { .. } => {}
        }
        if state.events.len() == MONITOR_EVENT_CAPACITY {
            let dropped = state
                .events
                .pop_front()
                .expect("a full monitor queue has a front event");
            match &mut state.lagged {
                Some(lagged) => lagged.dropped = lagged.dropped.saturating_add(1),
                None => {
                    state.lagged = Some(LagMarker {
                        sequence: dropped.sequence,
                        dropped: 1,
                    });
                }
            }
        }
        state.events.push_back(SequencedMonitorEvent {
            sequence,
            event: MonitorEvent {
                member_id: self.member_id.clone(),
                member_kind: self.member_kind,
                membership: self.membership,
                kind,
            },
        });
        state.terminal = terminal;
        drop(state);
        self.source.pulse();
    }
}
