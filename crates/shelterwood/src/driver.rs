//! Mutable runtime shell and shared handle state.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use crate::{
    ChildId, Exit, ExitKind, Incarnation, IntensityTrip, Membership, Readiness, ReadinessDeadline,
    ShutdownStraggler, ShutdownTimeout, StartupFailure, StartupFailureCause,
    engine::{
        ArbitrationClass, DeadlineQueue, ExitDispatch, IntensityState, MembershipMode,
        ReadinessEffect, ReadinessEvent, ReadinessGate, RestartState, ScopeMode, StopAction,
        StopLadder, arbitrate, dispatch_exit, schedule_restart,
    },
    exit::{JoinVerdict, RecordedOutcome, classify_exit},
    identity::{FenceCounter, ScopeIdentity},
    policy::{DefaultsInheritance, ResolvedDefaults},
    runtime,
    task::{OnceTaskBody, TaskContext, TaskFactory},
    tree::{
        BuilderCore, ChildConstruction, ChildPlan, NotAdmittingCause, RemoveOutcome, ReserveError,
        ScopeFactory, ScopeFlavor, ScopePlan, ScopeSource, ScopeState, SlotCell, StartupError,
        StopReason,
    },
};

#[derive(Debug, Default)]
pub(crate) struct Signal {
    inner: Arc<SignalInner>,
}

#[derive(Debug, Default)]
struct SignalInner {
    generation: AtomicU64,
    waiters: Mutex<Vec<Weak<Waiter>>>,
}

impl Clone for Signal {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Signal {
    pub(crate) fn pulse(&self) {
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        let mut waiters = self.inner.waiters.lock().expect("signal mutex poisoned");
        waiters.retain(|waiter| {
            if let Some(waiter) = waiter.upgrade() {
                waiter.notify();
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn watcher(&self) -> SignalWatcher {
        SignalWatcher {
            signal: self.clone(),
            seen: self.inner.generation.load(Ordering::Acquire),
        }
    }
}

pub(crate) struct SignalWatcher {
    signal: Signal,
    seen: u64,
}

impl SignalWatcher {
    pub(crate) async fn changed(&mut self) {
        loop {
            let current = self.signal.inner.generation.load(Ordering::Acquire);
            if current != self.seen {
                self.seen = current;
                return;
            }

            let waiter = Arc::new(Waiter::default());
            self.signal
                .inner
                .waiters
                .lock()
                .expect("signal mutex poisoned")
                .push(Arc::downgrade(&waiter));

            let current = self.signal.inner.generation.load(Ordering::Acquire);
            if current != self.seen {
                self.seen = current;
                return;
            }
            WaiterFuture(waiter).await;
        }
    }
}

#[derive(Debug, Default)]
struct Waiter {
    notified: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Waiter {
    fn notify(&self) {
        self.notified.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().expect("waiter mutex poisoned").take() {
            waker.wake();
        }
    }
}

struct WaiterFuture(Arc<Waiter>);

impl Future for WaiterFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.notified.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *self.0.waker.lock().expect("waiter mutex poisoned") = Some(context.waker().clone());
        if self.0.notified.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Latch {
    fired: Arc<AtomicBool>,
    signal: Signal,
}

impl Latch {
    pub(crate) fn fire(&self) -> bool {
        if self
            .fired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.signal.pulse();
            true
        } else {
            false
        }
    }

    pub(crate) fn is_fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    pub(crate) async fn fired(&self) {
        let mut watcher = self.signal.watcher();
        while !self.is_fired() {
            watcher.changed().await;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MemberStage {
    Reserved,
    Admitted,
    Starting,
    Running,
    Restarting,
    Stopping,
    Terminal(Exit),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemberRecord {
    pub(crate) stage: MemberStage,
    pub(crate) incarnation: Option<Incarnation>,
    pub(crate) last_exit: Option<Exit>,
    pub(crate) restart_count: u64,
    pub(crate) removing: bool,
}

#[derive(Debug)]
pub(crate) struct MemberCell {
    id: ChildId,
    membership: Membership,
    record: Mutex<MemberRecord>,
    changed: Signal,
    terminal_signals: Mutex<Vec<Signal>>,
    pub(crate) removal: Latch,
}

impl MemberCell {
    pub(crate) fn new(id: ChildId, membership: Membership) -> Arc<Self> {
        Arc::new(Self {
            id,
            membership,
            record: Mutex::new(MemberRecord {
                stage: MemberStage::Reserved,
                incarnation: None,
                last_exit: None,
                restart_count: 0,
                removing: false,
            }),
            changed: Signal::default(),
            terminal_signals: Mutex::new(Vec::new()),
            removal: Latch::default(),
        })
    }

    pub(crate) fn id(&self) -> &ChildId {
        &self.id
    }

    pub(crate) fn membership(&self) -> Membership {
        self.membership
    }

    pub(crate) fn record(&self) -> MemberRecord {
        self.record.lock().expect("member mutex poisoned").clone()
    }

    pub(crate) fn update(&self, update: impl FnOnce(&mut MemberRecord)) {
        update(&mut self.record.lock().expect("member mutex poisoned"));
        self.changed.pulse();
    }

    pub(crate) fn add_terminal_signal(&self, signal: Signal) {
        self.terminal_signals
            .lock()
            .expect("member terminal-signal mutex poisoned")
            .push(signal);
    }

    pub(crate) fn terminalize(&self, exit: Exit) {
        self.update(|record| {
            if !matches!(record.stage, MemberStage::Terminal(_)) {
                record.incarnation = None;
                record.last_exit = Some(exit.clone());
                record.stage = MemberStage::Terminal(exit);
            }
        });
        for signal in self
            .terminal_signals
            .lock()
            .expect("member terminal-signal mutex poisoned")
            .iter()
        {
            signal.pulse();
        }
    }

    pub(crate) async fn wait_terminal(&self) -> Exit {
        let mut watcher = self.changed.watcher();
        loop {
            if let MemberStage::Terminal(exit) = self.record().stage {
                return exit;
            }
            watcher.changed().await;
        }
    }
}

enum ReportState {
    Armed(mpsc::Sender<Option<RecordedOutcome>>),
    Spent,
}

pub(crate) struct ReportToken {
    state: ReportState,
}

pub(crate) struct ReportReceiver(mpsc::Receiver<Option<RecordedOutcome>>);

pub(crate) fn report_channel() -> (ReportToken, ReportReceiver) {
    let (sender, receiver) = mpsc::channel();
    (
        ReportToken {
            state: ReportState::Armed(sender),
        },
        ReportReceiver(receiver),
    )
}

impl ReportToken {
    pub(crate) fn record(mut self, outcome: RecordedOutcome) {
        let state = std::mem::replace(&mut self.state, ReportState::Spent);
        if let ReportState::Armed(sender) = state {
            let _ = sender.send(Some(outcome));
        }
    }
}

impl Drop for ReportToken {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, ReportState::Spent);
        if let ReportState::Armed(sender) = state {
            let _ = sender.send(None);
        }
    }
}

impl ReportReceiver {
    pub(crate) fn receive(self) -> Option<RecordedOutcome> {
        self.0
            .recv()
            .expect("owned report token must record or fall back")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScopeRecord {
    pub(crate) state: ScopeState,
    pub(crate) startup: Option<Result<(), StartupError>>,
}

pub(crate) struct ScopeCell {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) flavor: ScopeFlavor,
    pub(crate) child_identity: Mutex<ScopeIdentity>,
    record: Mutex<ScopeRecord>,
    changed: Signal,
    control: Mutex<ScopeControl>,
    current_dynamic: Mutex<Option<Arc<DynamicControl>>>,
    current_children: Mutex<Vec<Weak<SlotCell>>>,
}

#[derive(Debug, Default)]
struct ScopeControl {
    current_epoch: u64,
    live: bool,
    last_stopped_epoch: u64,
    shutdown: Option<ScopeRequest>,
    force: Option<ScopeRequest>,
}

#[derive(Clone, Copy, Debug)]
struct ScopeRequest {
    epoch: u64,
    consumed: bool,
}

impl ScopeCell {
    pub(crate) fn new(
        member: Arc<MemberCell>,
        flavor: ScopeFlavor,
        child_identity: ScopeIdentity,
    ) -> Arc<Self> {
        Arc::new(Self {
            member,
            flavor,
            child_identity: Mutex::new(child_identity),
            record: Mutex::new(ScopeRecord {
                state: ScopeState::Unstarted,
                startup: None,
            }),
            changed: Signal::default(),
            control: Mutex::new(ScopeControl::default()),
            current_dynamic: Mutex::new(None),
            current_children: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn record(&self) -> ScopeRecord {
        self.record.lock().expect("scope mutex poisoned").clone()
    }

    pub(crate) fn set_state(&self, state: ScopeState) {
        self.record.lock().expect("scope mutex poisoned").state = state;
        self.changed.pulse();
    }

    pub(crate) fn set_startup(&self, startup: Result<(), StartupError>) {
        let mut record = self.record.lock().expect("scope mutex poisoned");
        if record.startup.is_none() {
            record.startup = Some(startup);
            drop(record);
            self.changed.pulse();
        }
    }

    fn begin_incarnation(&self) -> u64 {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        control.current_epoch = control.current_epoch.saturating_add(1);
        control.live = true;
        let epoch = control.current_epoch;
        drop(control);
        self.changed.pulse();
        epoch
    }

    fn finish_incarnation(&self, epoch: u64, reason: StopReason) {
        self.finish_incarnation_with_terminal(epoch, reason, None);
    }

    fn finish_root_incarnation(&self, epoch: u64, reason: StopReason, exit: Exit) {
        self.finish_incarnation_with_terminal(epoch, reason, Some(exit));
    }

    fn finish_incarnation_with_terminal(
        &self,
        epoch: u64,
        reason: StopReason,
        terminal_exit: Option<Exit>,
    ) {
        {
            let mut record = self.record.lock().expect("scope mutex poisoned");
            record.state = ScopeState::Stopped { reason };
        }
        if let Some(exit) = terminal_exit {
            self.member.terminalize(exit);
        }
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        if control.current_epoch == epoch {
            control.live = false;
            control.last_stopped_epoch = control.last_stopped_epoch.max(epoch);
            if control
                .shutdown
                .is_some_and(|request| request.epoch <= epoch)
            {
                control.shutdown = None;
            }
            if control.force.is_some_and(|request| request.epoch <= epoch) {
                control.force = None;
            }
        }
        drop(control);
        self.changed.pulse();
    }

    fn finish_live_root_incarnation(&self, reason: StopReason, exit: Exit) {
        let epoch = {
            let control = self.control.lock().expect("scope control mutex poisoned");
            control.live.then_some(control.current_epoch)
        };
        if let Some(epoch) = epoch {
            self.finish_root_incarnation(epoch, reason, exit);
        } else {
            self.record.lock().expect("scope mutex poisoned").state =
                ScopeState::Stopped { reason };
            self.member.terminalize(exit);
            self.changed.pulse();
        }
    }

    pub(crate) fn request_shutdown(&self) -> u64 {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        let target = if control.live {
            control.current_epoch
        } else {
            control.current_epoch.saturating_add(1)
        };
        if control
            .shutdown
            .is_none_or(|request| request.epoch < target)
        {
            control.shutdown = Some(ScopeRequest {
                epoch: target,
                consumed: false,
            });
        }
        drop(control);
        self.changed.pulse();
        target
    }

    fn take_shutdown_request(&self, epoch: u64) -> bool {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        match control.shutdown.as_mut() {
            Some(request) if request.epoch == epoch && !request.consumed => {
                request.consumed = true;
                true
            }
            _ => false,
        }
    }

    fn force_shutdown(&self, epoch: u64) {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        if control.live && control.current_epoch == epoch {
            control.force = Some(ScopeRequest {
                epoch,
                consumed: false,
            });
        }
        drop(control);
        self.changed.pulse();
    }

    fn take_force_request(&self, epoch: u64) -> bool {
        let mut control = self.control.lock().expect("scope control mutex poisoned");
        match control.force.as_mut() {
            Some(request) if request.epoch == epoch && !request.consumed => {
                request.consumed = true;
                true
            }
            _ => false,
        }
    }

    fn incarnation_finished(&self, epoch: u64) -> bool {
        let control = self.control.lock().expect("scope control mutex poisoned");
        control.last_stopped_epoch >= epoch
    }

    fn set_children(&self, children: impl IntoIterator<Item = Arc<SlotCell>>) {
        *self
            .current_children
            .lock()
            .expect("scope children mutex poisoned") = children
            .into_iter()
            .map(|child| Arc::downgrade(&child))
            .collect();
    }

    fn add_child(&self, child: &Arc<SlotCell>) {
        self.current_children
            .lock()
            .expect("scope children mutex poisoned")
            .push(Arc::downgrade(child));
    }

    fn set_dynamic(&self, control: Option<Arc<DynamicControl>>) {
        *self
            .current_dynamic
            .lock()
            .expect("scope dynamic-control mutex poisoned") = control;
        self.changed.pulse();
    }

    fn dynamic(&self) -> Option<Arc<DynamicControl>> {
        self.current_dynamic
            .lock()
            .expect("scope dynamic-control mutex poisoned")
            .clone()
    }

    pub(crate) fn signal(&self) -> &Signal {
        &self.changed
    }

    pub(crate) async fn wait_started(&self) -> Result<(), StartupError> {
        let mut watcher = self.changed.watcher();
        loop {
            if let Some(result) = self.record().startup {
                return result;
            }
            watcher.changed().await;
        }
    }

    pub(crate) async fn wait_stopped(&self) -> StopReason {
        self.member.wait_terminal().await;
        match self.record().state {
            ScopeState::Stopped { reason } => reason,
            ScopeState::Unstarted
            | ScopeState::Starting
            | ScopeState::Running
            | ScopeState::StartupFailed
            | ScopeState::Draining => StopReason::NeverStarted,
        }
    }

    pub(crate) fn terminalize_never_started(&self) {
        self.member.terminalize(Exit::never_started());
        self.set_startup(Err(StartupError::ShutdownRequested));
        self.set_state(ScopeState::Stopped {
            reason: StopReason::NeverStarted,
        });
    }
}

pub(crate) struct SystemRun {
    pub(crate) root: Arc<ScopeCell>,
}

pub(crate) struct AdmissionResponse {
    result: Mutex<Option<Result<(), ReserveError>>>,
    changed: Signal,
}

impl AdmissionResponse {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            changed: Signal::default(),
        })
    }

    fn complete(&self, result: Result<(), ReserveError>) {
        let mut current = self.result.lock().expect("admission mutex poisoned");
        if current.is_none() {
            *current = Some(result);
            drop(current);
            self.changed.pulse();
        }
    }

    pub(crate) async fn wait(&self) -> Result<(), ReserveError> {
        let mut watcher = self.changed.watcher();
        loop {
            if let Some(result) = self
                .result
                .lock()
                .expect("admission mutex poisoned")
                .clone()
            {
                return result;
            }
            watcher.changed().await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct RemovalResponse {
    result: Mutex<Option<RemoveOutcome>>,
    changed: Signal,
}

impl RemovalResponse {
    fn pending() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            changed: Signal::default(),
        })
    }

    fn completed(outcome: RemoveOutcome) -> Arc<Self> {
        let response = Self::pending();
        response.complete(outcome);
        response
    }

    fn complete(&self, outcome: RemoveOutcome) {
        let mut current = self.result.lock().expect("removal mutex poisoned");
        if current.is_none() {
            *current = Some(outcome);
            drop(current);
            self.changed.pulse();
        }
    }

    pub(crate) async fn wait(&self) -> RemoveOutcome {
        let mut watcher = self.changed.watcher();
        loop {
            if let Some(result) = *self.result.lock().expect("removal mutex poisoned") {
                return result;
            }
            watcher.changed().await;
        }
    }
}

struct DynamicEntry {
    slot: Arc<SlotCell>,
    admitted: bool,
    fused_cancel: Option<Latch>,
    removal: Arc<RemovalResponse>,
    removal_started: bool,
}

struct DynamicState {
    accepting: bool,
    entries: HashMap<ChildId, DynamicEntry>,
}

pub(crate) struct DynamicControl {
    scope: Weak<ScopeCell>,
    events: runtime::MpscSender<DriverEvent>,
    state: Mutex<DynamicState>,
}

impl DynamicControl {
    fn new(scope: &Arc<ScopeCell>, events: runtime::MpscSender<DriverEvent>) -> Arc<Self> {
        Arc::new(Self {
            scope: Arc::downgrade(scope),
            events,
            state: Mutex::new(DynamicState {
                accepting: true,
                entries: HashMap::new(),
            }),
        })
    }

    fn close(&self) {
        let mut state = self.state.lock().expect("dynamic-state mutex poisoned");
        state.accepting = false;
        for entry in state.entries.values() {
            if !entry.admitted {
                entry.slot.member.terminalize(Exit::never_started());
                if let Some(scope) = &entry.slot.scope {
                    scope.terminalize_never_started();
                }
            }
        }
    }
}

pub(crate) struct DynamicReservation {
    pub(crate) slot: Arc<SlotCell>,
    pub(crate) control: Arc<DynamicControl>,
}

pub(crate) fn reserve_dynamic(
    scope: &Arc<ScopeCell>,
    id: ChildId,
    child_scope: Option<ScopeFlavor>,
) -> Result<DynamicReservation, ReserveError> {
    if matches!(scope.member.record().stage, MemberStage::Terminal(_)) {
        return Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal));
    }
    let control = scope.dynamic().ok_or(ReserveError::NotAdmitting(
        NotAdmittingCause::NoLiveIncarnation,
    ))?;
    match scope.record().state {
        ScopeState::Starting | ScopeState::Running => {}
        ScopeState::Draining => {
            return Err(ReserveError::NotAdmitting(NotAdmittingCause::Draining));
        }
        ScopeState::StartupFailed => {
            return Err(ReserveError::NotAdmitting(NotAdmittingCause::StartupFailed));
        }
        ScopeState::Unstarted | ScopeState::Stopped { .. } => {
            return Err(ReserveError::NotAdmitting(
                NotAdmittingCause::NoLiveIncarnation,
            ));
        }
    }
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    if !state.accepting {
        return Err(ReserveError::NotAdmitting(NotAdmittingCause::Draining));
    }
    if let Some(existing) = state.entries.get(&id) {
        if existing.slot.member.record().removing {
            return Err(ReserveError::RemovalInProgress(id));
        }
        return Err(ReserveError::DuplicateId(id));
    }
    let membership = scope
        .child_identity
        .lock()
        .expect("scope identity mutex poisoned")
        .mint_membership()
        .ok_or(ReserveError::IdentityExhausted)?;
    let member = MemberCell::new(id.clone(), membership);
    let child_scope = child_scope.map(|flavor| {
        let identity = ScopeIdentity::new().expect("global scope identity space exhausted");
        ScopeCell::new(Arc::clone(&member), flavor, identity)
    });
    let slot = SlotCell::new(member, child_scope);
    state.entries.insert(
        id,
        DynamicEntry {
            slot: Arc::clone(&slot),
            admitted: false,
            fused_cancel: None,
            removal: RemovalResponse::pending(),
            removal_started: false,
        },
    );
    Ok(DynamicReservation {
        slot,
        control: Arc::clone(&control),
    })
}

pub(crate) fn start_admission(
    control: Arc<DynamicControl>,
    slot: Arc<SlotCell>,
    fused_cancel: Option<Latch>,
) -> Arc<AdmissionResponse> {
    let response = AdmissionResponse::new();
    let request = AdmissionRequest {
        control: Arc::downgrade(&control),
        slot,
        fused_cancel,
        response: Arc::clone(&response),
    };
    let send_response = Arc::clone(&response);
    runtime::spawn((), async move {
        if runtime::mpsc_send(&control.events, DriverEvent::Admission(request))
            .await
            .is_err()
        {
            send_response.complete(Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal)));
        }
    });
    response
}

pub(crate) fn cancel_dynamic_reservation(control: &Arc<DynamicControl>, slot: &Arc<SlotCell>) {
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    let id = slot.member.id().clone();
    if state.entries.get(&id).is_some_and(|entry| {
        entry.slot.member.membership() == slot.member.membership() && !entry.admitted
    }) {
        state.entries.remove(&id);
        slot.member.terminalize(Exit::never_started());
        if let Some(scope) = &slot.scope {
            scope.terminalize_never_started();
        }
    }
}

pub(crate) fn signal_fused_cancel(control: &Arc<DynamicControl>, latch: &Latch) {
    latch.fire();
    if let Some(scope) = control.scope.upgrade() {
        scope.signal().pulse();
    }
}

pub(crate) fn remove_dynamic(
    scope: &Arc<ScopeCell>,
    id: &ChildId,
    exact: Option<Membership>,
) -> Arc<RemovalResponse> {
    if matches!(
        scope.record().state,
        ScopeState::Draining | ScopeState::Stopped { .. }
    ) {
        return RemovalResponse::completed(RemoveOutcome::AlreadyAbsent);
    }
    let Some(control) = scope.dynamic() else {
        return RemovalResponse::completed(RemoveOutcome::AlreadyAbsent);
    };
    let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
    let Some(entry) = state.entries.get_mut(id) else {
        return RemovalResponse::completed(RemoveOutcome::AlreadyAbsent);
    };
    if exact.is_some_and(|membership| membership != entry.slot.member.membership()) {
        return RemovalResponse::completed(RemoveOutcome::AlreadyAbsent);
    }
    let response = Arc::clone(&entry.removal);
    if !entry.admitted {
        let entry = state.entries.remove(id).expect("entry was just resolved");
        entry.slot.member.terminalize(Exit::never_started());
        if let Some(scope) = &entry.slot.scope {
            scope.terminalize_never_started();
        }
        response.complete(RemoveOutcome::Removed);
        return response;
    }
    if matches!(entry.slot.member.record().stage, MemberStage::Terminal(_)) {
        state.entries.remove(id);
        response.complete(RemoveOutcome::Removed);
        return response;
    }
    entry.slot.member.update(|record| record.removing = true);
    entry.slot.member.removal.fire();
    drop(state);
    scope.signal().pulse();
    response
}

struct AdmissionRequest {
    control: Weak<DynamicControl>,
    slot: Arc<SlotCell>,
    fused_cancel: Option<Latch>,
    response: Arc<AdmissionResponse>,
}

enum DriverEvent {
    Child(ChildEvent),
    Admission(AdmissionRequest),
}

impl SystemRun {
    pub(crate) fn request_shutdown(&self) {
        self.root.request_shutdown();
    }

    pub(crate) async fn shutdown(&self, timeout: Duration) -> Result<(), ShutdownTimeout> {
        shutdown_scope(Arc::clone(&self.root), timeout).await
    }
}

pub(crate) fn runtime_available() -> bool {
    runtime::is_available()
}

fn collect_stragglers(scope: &ScopeCell, prefix: &[ChildId], out: &mut Vec<ShutdownStraggler>) {
    let children = scope
        .current_children
        .lock()
        .expect("scope children mutex poisoned")
        .iter()
        .filter_map(Weak::upgrade)
        .collect::<Vec<_>>();
    for child in children {
        if matches!(child.member.record().stage, MemberStage::Terminal(_)) {
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

async fn wait_for_incarnation(scope: &ScopeCell, epoch: u64) {
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
    let epoch = scope.request_shutdown();
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
            Err(ShutdownTimeout { stragglers })
        }
    }
}

pub(crate) fn spawn_system(plan: ScopePlan) -> SystemRun {
    let root = Arc::clone(&plan.root);
    let monitor_root = Arc::clone(&root);
    let handle = runtime::spawn((), async move { run_scope(plan, true, None).await });
    runtime::spawn((), async move {
        match runtime::join(handle).await {
            runtime::JoinOutcome::Ok { .. } => {}
            runtime::JoinOutcome::Panic { message, .. } => {
                let exit = Exit::new(ExitKind::Panicked { message }, false);
                monitor_root.set_startup(Err(StartupError::ShutdownRequested));
                monitor_root.finish_live_root_incarnation(StopReason::ShutdownRequested, exit);
            }
            runtime::JoinOutcome::Cancelled { .. } => {
                monitor_root.set_startup(Err(StartupError::ShutdownRequested));
                monitor_root.finish_live_root_incarnation(
                    StopReason::ShutdownRequested,
                    Exit::new(ExitKind::Aborted { after_grace: false }, true),
                );
            }
        }
    });
    SystemRun { root }
}

enum ChildEvent {
    Ready {
        index: usize,
        incarnation: Incarnation,
    },
    Exited {
        index: usize,
        incarnation: Incarnation,
        recorded: Option<RecordedOutcome>,
        join: JoinVerdict,
        cancelled: bool,
    },
}

enum DeadlineKind {
    Readiness {
        index: usize,
        incarnation: Incarnation,
    },
    Restart {
        index: usize,
    },
    Stop {
        index: usize,
        incarnation: Incarnation,
    },
}

struct ActiveChild {
    incarnation: Incarnation,
    started_at: Instant,
    shutdown: Latch,
    abort: Latch,
    abort_handle: runtime::AbortHandle,
    ladder: Option<StopLadder>,
    forced_outcome: Option<RecordedOutcome>,
    hard_abort_after_grace: Option<bool>,
    readiness: ReadinessGate,
    ready_signal: Latch,
}

struct ChildRuntime {
    slot: Arc<crate::tree::SlotCell>,
    construction: ChildConstruction,
    options: crate::policy::ResolvedCommonOptions,
    incarnations: FenceCounter,
    restarts: RestartState,
    active: Option<ActiveChild>,
    initial_ready: bool,
    initial: bool,
    spawned_once: bool,
}

impl ChildRuntime {
    fn from_plan(plan: ChildPlan, scope: &ScopeCell) -> Self {
        let incarnations = scope
            .child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .incarnation_counter(plan.slot.member.membership());
        Self {
            slot: plan.slot,
            construction: plan.construction,
            options: plan.options,
            incarnations,
            restarts: RestartState::new(),
            active: None,
            initial_ready: false,
            initial: true,
            spawned_once: false,
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self.slot.member.record().stage, MemberStage::Terminal(_))
    }
}

struct ScopeRuntime {
    root: Arc<ScopeCell>,
    flavor: ScopeFlavor,
    defaults: ResolvedDefaults,
    intensity_policy: crate::Intensity,
    intensity: IntensityState,
    children: Vec<ChildRuntime>,
    events: runtime::MpscSender<DriverEvent>,
    deadlines: DeadlineQueue<DeadlineKind>,
    jitter: runtime::JitterRng,
    startup_complete: bool,
    startup_failed: bool,
    next_ordered_start: usize,
    draining: Option<StopReason>,
    is_root: bool,
    parent_ready: Option<Latch>,
    dynamic: Option<Arc<DynamicControl>>,
    epoch: u64,
    ancestor_shutdown: Option<Latch>,
    ancestor_shutdown_seen: bool,
}

impl Drop for ScopeRuntime {
    fn drop(&mut self) {
        if let Some(dynamic) = &self.dynamic {
            dynamic.close();
            self.root.set_dynamic(None);
        }
        for child in &mut self.children {
            if let Some(active) = child.active.take() {
                active.shutdown.fire();
                active.abort.fire();
                active.abort_handle.abort();
            } else if !child.is_terminal() {
                child.slot.member.terminalize(Exit::never_started());
            }
        }
        if !matches!(self.root.record().state, ScopeState::Stopped { .. }) {
            self.root.finish_incarnation(
                self.epoch,
                self.draining
                    .clone()
                    .unwrap_or(StopReason::ShutdownRequested),
            );
        }
    }
}

enum SpawnBody {
    TaskRestartable(TaskFactory),
    TaskOnce(Box<dyn FnOnce(TaskContext) -> crate::task::TaskFuture + Send + 'static>),
    ScopeRestartable {
        factory: ScopeFactory,
        scope: Arc<ScopeCell>,
        inherited: ResolvedDefaults,
    },
    ScopeOnce {
        tree: Box<BuilderCore>,
        scope: Arc<ScopeCell>,
        inherited: ResolvedDefaults,
    },
}

impl ScopeRuntime {
    fn spawn_child(&mut self, index: usize) {
        if self.draining.is_some() || self.children[index].is_terminal() {
            return;
        }
        let child = &mut self.children[index];
        let Some(incarnation) = mint_child_incarnation(&child.slot.member, &mut child.incarnations)
        else {
            return;
        };

        let shutdown = Latch::default();
        let abort = Latch::default();
        let ready = Latch::default();
        let ended = Latch::default();
        let id = child.slot.member.id().clone();
        let context = TaskContext::new(
            id,
            incarnation,
            shutdown.clone(),
            abort.clone(),
            ready.clone(),
        );
        let scope_child = matches!(child.construction, ChildConstruction::Scope(_));
        let gated = scope_child || child.options.readiness == Readiness::Manual;
        let body = match &mut child.construction {
            ChildConstruction::Task(definition) => {
                SpawnBody::TaskRestartable(Arc::clone(&definition.factory))
            }
            ChildConstruction::TaskOnce(definition) => {
                let body = std::mem::replace(&mut definition.body, OnceTaskBody::Spent);
                match body {
                    OnceTaskBody::Available(body) => SpawnBody::TaskOnce(body),
                    OnceTaskBody::Spent => {
                        panic!("one-shot task construction invoked more than once")
                    }
                }
            }
            ChildConstruction::Scope(definition) => {
                let inherited = match definition.defaults {
                    DefaultsInheritance::Inherit => self.defaults.clone(),
                    DefaultsInheritance::Reset => ResolvedDefaults::default(),
                };
                match &mut definition.source {
                    ScopeSource::Restartable(factory) => SpawnBody::ScopeRestartable {
                        factory: Arc::clone(factory),
                        scope: Arc::clone(
                            child
                                .slot
                                .scope
                                .as_ref()
                                .expect("scope construction needs a scope cell"),
                        ),
                        inherited,
                    },
                    ScopeSource::OneShot(_) => {
                        let source = std::mem::replace(&mut definition.source, ScopeSource::Spent);
                        let ScopeSource::OneShot(tree) = source else {
                            unreachable!()
                        };
                        SpawnBody::ScopeOnce {
                            tree,
                            scope: Arc::clone(
                                child
                                    .slot
                                    .scope
                                    .as_ref()
                                    .expect("scope construction needs a scope cell"),
                            ),
                            inherited,
                        }
                    }
                    ScopeSource::Spent => {
                        panic!("one-shot subtree construction invoked more than once")
                    }
                }
            }
        };

        let now = runtime::now();
        child.spawned_once = true;
        child.slot.member.update(|record| {
            record.stage = MemberStage::Starting;
            record.incarnation = Some(incarnation);
        });

        let readiness = if !gated {
            ready.fire();
            child.initial_ready = true;
            child
                .slot
                .member
                .update(|record| record.stage = MemberStage::Running);
            ReadinessGate::Immediate
        } else {
            let deadline = match child.options.readiness_deadline {
                ReadinessDeadline::Bounded(duration) => {
                    let deadline = now.checked_add(duration).unwrap_or(now);
                    self.deadlines
                        .push(deadline, DeadlineKind::Readiness { index, incarnation });
                    Some(deadline)
                }
                ReadinessDeadline::Unbounded | ReadinessDeadline::Inherit => None,
            };
            ReadinessGate::Waiting { deadline }
        };

        let (report, report_receiver) = report_channel();
        let nested_ready = ready.clone();
        let nested_cancel = shutdown.clone();
        let handle = runtime::spawn(incarnation, async move {
            let result = match body {
                SpawnBody::TaskRestartable(factory) => {
                    let future = (factory
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner))(
                        context
                    );
                    future.await
                }
                SpawnBody::TaskOnce(body) => body(context).await,
                SpawnBody::ScopeRestartable {
                    factory,
                    scope,
                    inherited,
                } => {
                    let tree = (factory
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner))(
                    );
                    run_nested_tree(tree, scope, inherited, nested_ready, nested_cancel).await
                }
                SpawnBody::ScopeOnce {
                    tree,
                    scope,
                    inherited,
                } => run_nested_tree(*tree, scope, inherited, nested_ready, nested_cancel).await,
            };
            report.record(RecordedOutcome::Returned(result));
        });
        let abort_handle = handle.abort_handle();
        let exit_sender = self.events.clone();
        let exit_shutdown = shutdown.clone();
        let exit_ended = ended.clone();
        runtime::spawn((), async move {
            let join = match runtime::join(handle).await {
                runtime::JoinOutcome::Ok { .. } => JoinVerdict::Completed,
                runtime::JoinOutcome::Panic { message, .. } => JoinVerdict::Panicked { message },
                runtime::JoinOutcome::Cancelled { .. } => {
                    JoinVerdict::Cancelled { after_grace: false }
                }
            };
            exit_ended.fire();
            let recorded = report_receiver.receive();
            let _ = runtime::mpsc_send(
                &exit_sender,
                DriverEvent::Child(ChildEvent::Exited {
                    index,
                    incarnation,
                    recorded,
                    join,
                    cancelled: exit_shutdown.is_fired(),
                }),
            )
            .await;
        });

        let readiness_signal = ready.clone();
        if gated {
            let ready_sender = self.events.clone();
            runtime::spawn((), async move {
                if matches!(
                    runtime::select_two(ready.fired(), ended.fired()).await,
                    runtime::Either::Left(())
                ) {
                    let _ = runtime::mpsc_send(
                        &ready_sender,
                        DriverEvent::Child(ChildEvent::Ready { index, incarnation }),
                    )
                    .await;
                }
            });
        }

        child.active = Some(ActiveChild {
            incarnation,
            started_at: now,
            shutdown,
            abort,
            abort_handle,
            ladder: None,
            forced_outcome: None,
            hard_abort_after_grace: None,
            readiness,
            ready_signal: readiness_signal,
        });
    }

    fn progress_startup(&mut self) {
        if self.startup_complete || self.startup_failed || self.draining.is_some() {
            return;
        }
        match self.flavor {
            ScopeFlavor::Ordered => {
                while self.next_ordered_start < self.children.len() {
                    let index = self.next_ordered_start;
                    if !self.children[index].spawned_once {
                        self.spawn_child(index);
                    }
                    if self.children[index].initial_ready {
                        self.next_ordered_start += 1;
                    } else {
                        return;
                    }
                }
                if self
                    .children
                    .iter()
                    .filter(|child| child.initial)
                    .all(|child| child.initial_ready)
                {
                    self.complete_startup();
                }
            }
            ScopeFlavor::Dynamic => {
                if self
                    .children
                    .iter()
                    .filter(|child| child.initial)
                    .all(|child| child.initial_ready)
                {
                    self.complete_startup();
                }
            }
        }
    }

    fn complete_startup(&mut self) {
        self.startup_complete = true;
        self.root.set_state(ScopeState::Running);
        self.root.set_startup(Ok(()));
        if let Some(parent_ready) = &self.parent_ready {
            parent_ready.fire();
        }
    }

    fn begin_stop_child(&mut self, index: usize, forced: Option<RecordedOutcome>) {
        let child = &mut self.children[index];
        if child.is_terminal() {
            return;
        }
        if let Some(active) = &mut child.active {
            if active.ladder.is_some() {
                if forced.is_some() {
                    active.forced_outcome = forced;
                }
                return;
            }
            child
                .slot
                .member
                .update(|record| record.stage = MemberStage::Stopping);
            active.forced_outcome = forced;
            if active.forced_outcome.is_none() {
                active.readiness.step(ReadinessEvent::Shutdown);
            }
            active.ladder = Some(StopLadder::new(child.options.shutdown));
            self.advance_ladder(index, runtime::now());
        } else {
            let exit = child
                .slot
                .member
                .record()
                .last_exit
                .unwrap_or_else(Exit::never_started);
            child.slot.member.terminalize(exit);
        }
    }

    fn advance_ladder(&mut self, index: usize, now: Instant) {
        let child = &mut self.children[index];
        let Some(active) = &mut child.active else {
            return;
        };
        let Some(ladder) = &mut active.ladder else {
            return;
        };
        while let Some(action) = ladder.advance(now) {
            match action {
                StopAction::Cancel => {
                    active.shutdown.fire();
                }
                StopAction::Escalate => {
                    active.abort.fire();
                }
                StopAction::HardAbort { after_grace } => {
                    active.hard_abort_after_grace = Some(after_grace);
                    active.abort_handle.abort();
                }
            }
        }
        if let Some(deadline) = ladder.deadline() {
            self.deadlines.push(
                deadline,
                DeadlineKind::Stop {
                    index,
                    incarnation: active.incarnation,
                },
            );
        }
    }

    fn begin_drain(&mut self, reason: StopReason) {
        if self.draining.is_some() {
            return;
        }
        if !self.startup_complete && !self.startup_failed {
            self.root.set_startup(Err(StartupError::ShutdownRequested));
        }
        self.draining = Some(reason);
        self.root.set_state(ScopeState::Draining);
        match self.flavor {
            ScopeFlavor::Ordered => self.stop_next_ordered(),
            ScopeFlavor::Dynamic => {
                for index in 0..self.children.len() {
                    self.begin_stop_child(index, None);
                }
            }
        }
    }

    fn stop_next_ordered(&mut self) {
        if self.flavor != ScopeFlavor::Ordered || self.draining.is_none() {
            return;
        }
        loop {
            let Some(index) = (0..self.children.len())
                .rev()
                .find(|index| !self.children[*index].is_terminal())
            else {
                return;
            };
            self.begin_stop_child(index, None);
            if self.children[index].active.is_some() {
                return;
            }
        }
    }

    fn force_all(&mut self) {
        if self.draining.is_none() {
            self.begin_drain(StopReason::ShutdownRequested);
        }
        let now = runtime::now();
        for index in 0..self.children.len() {
            let child = &mut self.children[index];
            let Some(active) = &mut child.active else {
                continue;
            };
            let mut ladder = StopLadder::new(crate::Shutdown::Abort);
            let _ = ladder.advance(now);
            let _ = ladder.advance(now);
            active.shutdown.fire();
            active.abort.fire();
            active.ladder = Some(ladder);
            self.advance_ladder(index, now);
        }
    }

    fn handle_ready(&mut self, index: usize, incarnation: Incarnation) {
        let child = &mut self.children[index];
        let Some(active) = child.active.as_mut() else {
            return;
        };
        if active.incarnation != incarnation
            || !matches!(
                active.readiness.step(ReadinessEvent::Signal),
                Some(ReadinessEffect::BecameReady)
            )
        {
            return;
        }
        if !self.startup_complete {
            child.initial_ready = true;
        }
        child
            .slot
            .member
            .update(|record| record.stage = MemberStage::Running);
        self.progress_startup();
    }

    fn handle_exit(
        &mut self,
        index: usize,
        incarnation: Incarnation,
        recorded: Option<RecordedOutcome>,
        mut join: JoinVerdict,
        cancelled: bool,
    ) {
        let child = &mut self.children[index];
        let Some(mut active) = child.active.take() else {
            return;
        };
        if active.incarnation != incarnation {
            child.active = Some(active);
            return;
        }
        active.readiness.step(ReadinessEvent::Exit);
        if let (JoinVerdict::Cancelled { .. }, Some(after_grace)) =
            (&join, active.hard_abort_after_grace)
        {
            join = JoinVerdict::Cancelled { after_grace };
        }
        let recorded = active.forced_outcome.or(recorded);
        let exit = classify_exit(recorded, join, cancelled);
        let ran_for = runtime::now().saturating_duration_since(active.started_at);
        if ran_for >= self.intensity_policy.within {
            child.restarts.settled();
        }
        child.slot.member.update(|record| {
            record.incarnation = None;
            record.last_exit = Some(exit.clone());
        });

        let mode = if self.draining.is_some() {
            ScopeMode::Draining
        } else {
            ScopeMode::Running
        };
        let member_mode = if child.slot.member.record().removing {
            MembershipMode::Removing
        } else {
            MembershipMode::Active
        };
        match dispatch_exit(&exit, child.options.restart, mode, member_mode) {
            ExitDispatch::Terminal => {
                let pre_ready = child.initial && !self.startup_complete && !child.initial_ready;
                child.slot.member.terminalize(exit.clone());
                let removing = child.slot.member.record().removing;
                if removing {
                    self.finalize_removal(index);
                } else if pre_ready && self.draining.is_none() {
                    self.fail_startup(index, exit);
                } else {
                    if child.options.retention == crate::Retention::Remove {
                        self.prune_terminal(index);
                    }
                    if self.draining.is_some() {
                        self.stop_next_ordered();
                    }
                }
            }
            ExitDispatch::ScheduleRestart => {
                if !self.startup_complete {
                    child.initial_ready = false;
                }
                let sample = self.jitter.sample(0..u64::MAX) as f64 / u64::MAX as f64;
                let now = runtime::now();
                let mut effects = Vec::new();
                let decision = schedule_restart(
                    &mut child.restarts,
                    &mut self.intensity,
                    self.intensity_policy,
                    child.options.restart,
                    now,
                    sample,
                    &mut effects,
                );
                child.slot.member.update(|record| {
                    record.restart_count = decision.restart_count;
                    record.stage = MemberStage::Restarting;
                });
                debug_assert!(matches!(
                    effects.first(),
                    Some(crate::engine::RestartEffect::Scheduled { .. })
                ));
                if decision.charge.tripped {
                    let trip = IntensityTrip {
                        max_restarts: self.intensity_policy.max_restarts,
                        observed_restarts: decision.charge.in_window,
                        within: self.intensity_policy.within,
                    };
                    if !self.startup_complete && !self.startup_failed {
                        self.root
                            .set_startup(Err(StartupError::IntensityTripped(trip.clone())));
                    }
                    self.begin_drain(StopReason::IntensityTripped(trip));
                } else {
                    self.deadlines.push(
                        decision
                            .restart_at
                            .expect("a permitted restart has a deadline"),
                        DeadlineKind::Restart { index },
                    );
                }
            }
        }
    }

    fn fail_startup(&mut self, index: usize, exit: Exit) {
        let child = &self.children[index];
        let failure = StartupFailure {
            cause: StartupFailureCause::Child {
                id: child.slot.member.id().clone(),
                membership: child.slot.member.membership(),
                exit,
            },
        };
        self.startup_failed = true;
        self.root
            .set_startup(Err(StartupError::StartupFailed(failure.clone())));
        if self.flavor == ScopeFlavor::Ordered {
            for later in index + 1..self.children.len() {
                if !self.children[later].spawned_once {
                    self.children[later]
                        .slot
                        .member
                        .terminalize(Exit::never_started());
                    if let Some(scope) = &self.children[later].slot.scope {
                        scope.terminalize_never_started();
                    }
                }
            }
        }
        if self.is_root {
            self.root.set_state(ScopeState::StartupFailed);
        } else {
            self.begin_drain(StopReason::StartupFailed(failure));
        }
    }

    fn handle_deadline(&mut self, deadline: DeadlineKind) {
        match deadline {
            DeadlineKind::Readiness { index, incarnation } => {
                if self.children[index].active.as_ref().is_some_and(|active| {
                    active.incarnation == incarnation && active.ready_signal.is_fired()
                }) {
                    self.handle_ready(index, incarnation);
                    return;
                }
                let child = &mut self.children[index];
                let Some(active) = child.active.as_mut() else {
                    return;
                };
                if active.incarnation != incarnation {
                    return;
                }
                let Some(ReadinessEffect::TimedOut { deadline }) = active
                    .readiness
                    .step(ReadinessEvent::Deadline(runtime::now()))
                else {
                    return;
                };
                self.begin_stop_child(index, Some(RecordedOutcome::ReadinessTimedOut { deadline }));
            }
            DeadlineKind::Restart { index } => self.spawn_child(index),
            DeadlineKind::Stop { index, incarnation } => {
                if self.children[index]
                    .active
                    .as_ref()
                    .is_some_and(|active| active.incarnation == incarnation)
                {
                    self.advance_ladder(index, runtime::now());
                }
            }
        }
    }

    fn finish_if_ready(&mut self) -> Option<StopReason> {
        if let Some(reason) = &self.draining {
            if self.children.iter().all(ChildRuntime::is_terminal) {
                return Some(reason.clone());
            }
            return None;
        }
        if !self.startup_failed
            && self.flavor == ScopeFlavor::Ordered
            && !self.children.is_empty()
            && self.children.iter().all(ChildRuntime::is_terminal)
        {
            return Some(StopReason::Finished);
        }
        None
    }

    fn handle_admission(&mut self, request: AdmissionRequest) {
        let Some(control) = request.control.upgrade() else {
            request
                .response
                .complete(Err(ReserveError::NotAdmitting(NotAdmittingCause::Terminal)));
            return;
        };
        if self
            .dynamic
            .as_ref()
            .is_none_or(|current| !Arc::ptr_eq(current, &control))
            || self.draining.is_some()
            || self.startup_failed
        {
            cancel_dynamic_reservation(&control, &request.slot);
            let cause = if self.draining.is_some() {
                NotAdmittingCause::Draining
            } else if self.startup_failed {
                NotAdmittingCause::StartupFailed
            } else {
                NotAdmittingCause::NoLiveIncarnation
            };
            request
                .response
                .complete(Err(ReserveError::NotAdmitting(cause)));
            return;
        }
        if request.fused_cancel.as_ref().is_some_and(Latch::is_fired) {
            cancel_dynamic_reservation(&control, &request.slot);
            request.response.complete(Err(ReserveError::NotAdmitting(
                NotAdmittingCause::ReservationEnded,
            )));
            return;
        }

        let Some(definition) = request.slot.take_definition() else {
            cancel_dynamic_reservation(&control, &request.slot);
            request.response.complete(Err(ReserveError::NotAdmitting(
                NotAdmittingCause::ReservationEnded,
            )));
            return;
        };
        let (options, one_shot) = match &definition {
            ChildConstruction::Task(definition) => (&definition.options, false),
            ChildConstruction::TaskOnce(definition) => (&definition.options, true),
            ChildConstruction::Scope(definition) => (&definition.options, definition.one_shot()),
        };
        let resolved =
            crate::policy::resolve_common(options, &self.defaults, one_shot, Readiness::Immediate);
        let plan = ChildPlan {
            slot: Arc::clone(&request.slot),
            construction: definition,
            options: resolved,
        };
        let mut child = ChildRuntime::from_plan(plan, &self.root);
        child.initial = false;
        let index = self.children.len();
        self.root.add_child(&request.slot);
        self.children.push(child);
        request
            .slot
            .member
            .update(|record| record.stage = MemberStage::Admitted);
        {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            let Some(entry) = state.entries.get_mut(request.slot.member.id()) else {
                request.response.complete(Err(ReserveError::NotAdmitting(
                    NotAdmittingCause::ReservationEnded,
                )));
                self.children[index]
                    .slot
                    .member
                    .terminalize(Exit::never_started());
                return;
            };
            entry.admitted = true;
            entry.fused_cancel = request.fused_cancel;
        }
        request.response.complete(Ok(()));
        self.spawn_child(index);
    }

    fn pending_removals(&self) -> Vec<Membership> {
        let Some(control) = &self.dynamic else {
            return Vec::new();
        };
        control
            .state
            .lock()
            .expect("dynamic-state mutex poisoned")
            .entries
            .values()
            .filter(|entry| {
                entry.admitted
                    && !entry.removal_started
                    && (entry.slot.member.removal.is_fired()
                        || entry.fused_cancel.as_ref().is_some_and(Latch::is_fired))
            })
            .map(|entry| entry.slot.member.membership())
            .collect()
    }

    fn handle_removal(&mut self, membership: Membership) {
        if let Some(control) = &self.dynamic {
            let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
            if let Some(entry) = state
                .entries
                .values_mut()
                .find(|entry| entry.slot.member.membership() == membership)
            {
                if entry.removal_started {
                    return;
                }
                entry.removal_started = true;
            }
        }
        let Some(index) = self
            .children
            .iter()
            .position(|child| child.slot.member.membership() == membership)
        else {
            return;
        };
        self.children[index]
            .slot
            .member
            .update(|record| record.removing = true);
        if self.children[index].is_terminal() {
            self.finalize_removal(index);
        } else {
            self.begin_stop_child(index, None);
            if self.children[index].is_terminal() {
                self.finalize_removal(index);
            }
        }
    }

    fn finalize_removal(&mut self, index: usize) {
        let Some(control) = &self.dynamic else {
            return;
        };
        let child = &self.children[index];
        let id = child.slot.member.id().clone();
        let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
        if state
            .entries
            .get(&id)
            .is_some_and(|entry| entry.slot.member.membership() == child.slot.member.membership())
        {
            let entry = state.entries.remove(&id).expect("entry was just resolved");
            entry.removal.complete(RemoveOutcome::Removed);
        }
    }

    fn prune_terminal(&mut self, index: usize) {
        let Some(control) = &self.dynamic else {
            return;
        };
        let child = &self.children[index];
        let id = child.slot.member.id().clone();
        let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
        if state
            .entries
            .get(&id)
            .is_some_and(|entry| entry.slot.member.membership() == child.slot.member.membership())
        {
            state.entries.remove(&id);
        }
    }
}

fn mint_child_incarnation(
    member: &Arc<MemberCell>,
    counter: &mut FenceCounter,
) -> Option<Incarnation> {
    let incarnation = ScopeIdentity::mint_incarnation(member.membership(), counter);
    if incarnation.is_none() {
        let exit = member
            .record()
            .last_exit
            .unwrap_or_else(Exit::never_started);
        member.terminalize(exit);
    }
    incarnation
}

async fn run_nested_tree(
    tree: BuilderCore,
    scope: Arc<ScopeCell>,
    inherited: ResolvedDefaults,
    ready: Latch,
    cancel: Latch,
) -> crate::ExitResult {
    let epoch = scope.begin_incarnation();
    scope.set_state(ScopeState::Starting);
    let plan = match tree.lower(inherited, Some(Arc::clone(&scope))) {
        Ok(plan) => plan,
        Err(undefined) => {
            let failure = StartupFailure {
                cause: StartupFailureCause::Lowering { undefined },
            };
            scope.set_startup(Err(StartupError::StartupFailed(failure.clone())));
            scope.finish_incarnation(epoch, StopReason::StartupFailed(failure.clone()));
            return Err(crate::ExitError::from_startup_failure(failure));
        }
    };
    match run_scope_incarnation(plan, false, Some(ready), Some(cancel), epoch).await {
        StopReason::Finished | StopReason::ShutdownRequested => Ok(()),
        StopReason::IntensityTripped(trip) => Err(crate::ExitError::from_intensity_trip(trip)),
        StopReason::StartupFailed(failure) => Err(crate::ExitError::from_startup_failure(failure)),
        StopReason::NeverStarted => Err(crate::ExitError::message("nested scope never started")),
    }
}

async fn run_scope(plan: ScopePlan, is_root: bool, parent_ready: Option<Latch>) -> StopReason {
    let root = Arc::clone(&plan.root);
    let epoch = root.begin_incarnation();
    run_scope_incarnation(plan, is_root, parent_ready, None, epoch).await
}

async fn run_scope_incarnation(
    plan: ScopePlan,
    is_root: bool,
    parent_ready: Option<Latch>,
    incarnation_cancel: Option<Latch>,
    epoch: u64,
) -> StopReason {
    let root = Arc::clone(&plan.root);
    root.set_state(ScopeState::Starting);
    if is_root {
        root.member
            .update(|record| record.stage = MemberStage::Running);
    }
    let capacity = plan.children.len().saturating_mul(3).max(64);
    let (events, mut event_receiver) = runtime::bounded_mpsc(capacity);
    let dynamic =
        (plan.flavor == ScopeFlavor::Dynamic).then(|| DynamicControl::new(&root, events.clone()));
    if let Some(control) = &dynamic {
        let mut state = control.state.lock().expect("dynamic-state mutex poisoned");
        for child in &plan.children {
            state.entries.insert(
                child.slot.member.id().clone(),
                DynamicEntry {
                    slot: Arc::clone(&child.slot),
                    admitted: true,
                    fused_cancel: None,
                    removal: RemovalResponse::pending(),
                    removal_started: false,
                },
            );
        }
        drop(state);
        root.set_dynamic(Some(Arc::clone(control)));
    }
    root.set_children(plan.children.iter().map(|child| Arc::clone(&child.slot)));
    let children = plan
        .children
        .into_iter()
        .map(|child| {
            child
                .slot
                .member
                .update(|record| record.stage = MemberStage::Admitted);
            ChildRuntime::from_plan(child, &root)
        })
        .collect();
    let mut scope = ScopeRuntime {
        root: Arc::clone(&root),
        flavor: plan.flavor,
        defaults: plan.defaults,
        intensity_policy: plan.config.intensity,
        intensity: IntensityState::default(),
        children,
        events,
        deadlines: DeadlineQueue::default(),
        jitter: runtime::JitterRng::from_system_entropy(),
        startup_complete: false,
        startup_failed: false,
        next_ordered_start: 0,
        draining: None,
        is_root,
        parent_ready,
        dynamic,
        epoch,
        ancestor_shutdown: incarnation_cancel,
        ancestor_shutdown_seen: false,
    };

    match scope.flavor {
        ScopeFlavor::Ordered => scope.progress_startup(),
        ScopeFlavor::Dynamic => {
            for index in 0..scope.children.len() {
                scope.spawn_child(index);
            }
            scope.progress_startup();
        }
    }

    let mut signal = root.signal().watcher();
    loop {
        let mut pending = Vec::new();
        if root.take_shutdown_request(epoch) {
            pending.push((ArbitrationClass::ScopeShutdown, Pending::Shutdown));
        }
        if !scope.ancestor_shutdown_seen
            && scope
                .ancestor_shutdown
                .as_ref()
                .is_some_and(Latch::is_fired)
        {
            scope.ancestor_shutdown_seen = true;
            pending.push((ArbitrationClass::ScopeShutdown, Pending::AncestorShutdown));
        }
        if root.take_force_request(epoch) {
            pending.push((ArbitrationClass::StopDeadline, Pending::Force));
        }
        for membership in scope.pending_removals() {
            pending.push((
                ArbitrationClass::MembershipRemoval,
                Pending::Removal(membership),
            ));
        }
        while let Some(event) = runtime::mpsc_try_recv(&mut event_receiver) {
            let class = match event {
                DriverEvent::Child(ChildEvent::Ready { .. }) => ArbitrationClass::ReadinessSignal,
                DriverEvent::Child(ChildEvent::Exited { .. }) => ArbitrationClass::ChildExit,
                DriverEvent::Admission(_) => ArbitrationClass::Admission,
            };
            pending.push((class, Pending::Driver(event)));
        }
        let now = runtime::now();
        while let Some(deadline) = scope.deadlines.pop_due(now) {
            let class = match deadline {
                DeadlineKind::Readiness { .. } => ArbitrationClass::ReadinessDeadline,
                DeadlineKind::Restart { .. } => ArbitrationClass::BackoffDue,
                DeadlineKind::Stop { .. } => ArbitrationClass::StopDeadline,
            };
            pending.push((class, Pending::Deadline(deadline)));
        }

        if pending.is_empty() {
            let ancestor_shutdown = async {
                if !scope.ancestor_shutdown_seen
                    && let Some(shutdown) = &scope.ancestor_shutdown
                {
                    shutdown.fired().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            match runtime::wait_scope(
                signal.changed(),
                ancestor_shutdown,
                &mut event_receiver,
                scope.deadlines.next(),
            )
            .await
            {
                runtime::ScopeWake::Signal
                | runtime::ScopeWake::ParentShutdown
                | runtime::ScopeWake::Deadline => continue,
                runtime::ScopeWake::Message(Some(event)) => {
                    let class = match event {
                        DriverEvent::Child(ChildEvent::Ready { .. }) => {
                            ArbitrationClass::ReadinessSignal
                        }
                        DriverEvent::Child(ChildEvent::Exited { .. }) => {
                            ArbitrationClass::ChildExit
                        }
                        DriverEvent::Admission(_) => ArbitrationClass::Admission,
                    };
                    pending.push((class, Pending::Driver(event)));
                }
                runtime::ScopeWake::Message(None) => continue,
            }
        }

        arbitrate(&mut pending);
        for (_, event) in pending {
            match event {
                Pending::Shutdown => {
                    if let Some(cancel) = &scope.ancestor_shutdown {
                        cancel.fire();
                        scope.ancestor_shutdown_seen = true;
                    }
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::AncestorShutdown => {
                    scope.begin_drain(StopReason::ShutdownRequested);
                }
                Pending::Force => {
                    scope.force_all();
                }
                Pending::Removal(membership) => scope.handle_removal(membership),
                Pending::Driver(DriverEvent::Admission(request)) => {
                    scope.handle_admission(request);
                }
                Pending::Driver(DriverEvent::Child(ChildEvent::Ready { index, incarnation })) => {
                    scope.handle_ready(index, incarnation);
                }
                Pending::Driver(DriverEvent::Child(ChildEvent::Exited {
                    index,
                    incarnation,
                    recorded,
                    join,
                    cancelled,
                })) => scope.handle_exit(index, incarnation, recorded, join, cancelled),
                Pending::Deadline(deadline) => scope.handle_deadline(deadline),
            }
        }

        if let Some(reason) = scope.finish_if_ready() {
            if is_root {
                let exit = match &reason {
                    StopReason::Finished | StopReason::ShutdownRequested => {
                        Exit::new(ExitKind::Completed, reason == StopReason::ShutdownRequested)
                    }
                    StopReason::IntensityTripped(trip) => Exit::new(
                        ExitKind::Failed(crate::ExitError::from_intensity_trip(trip.clone())),
                        false,
                    ),
                    StopReason::StartupFailed(failure) => Exit::new(
                        ExitKind::Failed(crate::ExitError::from_startup_failure(failure.clone())),
                        false,
                    ),
                    StopReason::NeverStarted => Exit::never_started(),
                };
                root.finish_root_incarnation(epoch, reason.clone(), exit);
            } else {
                root.finish_incarnation(epoch, reason.clone());
            }
            scope.children.clear();
            return reason;
        }
    }
}

enum Pending {
    Shutdown,
    AncestorShutdown,
    Force,
    Removal(Membership),
    Driver(DriverEvent),
    Deadline(DeadlineKind),
}

#[cfg(test)]
mod tests {
    use crate::{
        ChildId, Exit, ExitError, ExitKind,
        exit::RecordedOutcome,
        identity::{FenceCounter, ScopeIdentity},
    };

    use super::{MemberCell, MemberStage, mint_child_incarnation, report_channel};

    #[test]
    fn owned_report_token_consumes_or_falls_back_once() {
        let (token, receiver) = report_channel();
        token.record(RecordedOutcome::Returned(Ok(())));
        assert!(matches!(
            receiver.receive(),
            Some(RecordedOutcome::Returned(Ok(())))
        ));

        let (token, receiver) = report_channel();
        drop(token);
        assert!(receiver.receive().is_none());
    }

    #[test]
    fn incarnation_exhaustion_terminalizes_the_membership_as_never_restart() {
        let mut identity = ScopeIdentity::new().expect("scope identity available");
        let membership = identity.mint_membership().expect("membership available");
        let member = MemberCell::new(ChildId::from("worker"), membership);
        let previous = Exit::new(
            ExitKind::Failed(ExitError::message("last completed incarnation")),
            false,
        );
        member.update(|record| {
            record.stage = MemberStage::Restarting;
            record.last_exit = Some(previous.clone());
        });
        let mut counter = FenceCounter::near_exhaustion(71);

        assert!(mint_child_incarnation(&member, &mut counter).is_some());
        assert!(mint_child_incarnation(&member, &mut counter).is_none());
        assert!(matches!(
            member.record().stage,
            MemberStage::Terminal(ref exit) if exit == &previous
        ));
        assert!(mint_child_incarnation(&member, &mut counter).is_none());
    }
}
