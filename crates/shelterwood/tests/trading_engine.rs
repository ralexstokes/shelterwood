#![cfg(feature = "metrics")]

use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use shelterwood::{
    Actor, ActorDef, Backoff, Context, ExitError, ExitResult, Intensity, KeyedCapacity, Mailbox,
    MonitorEvent, MonitorEventKind, Reply, RestartCondition, RestartPolicy, ScopeState,
    StopContext, SubtreeOnceDef, Tree,
};
use shelterwood_test_support::{ReleaseGate, poll_until};

#[derive(Debug)]
struct MetricHandle;

impl metrics::CounterFn for MetricHandle {
    fn increment(&self, _: u64) {}
    fn absolute(&self, _: u64) {}
}

impl metrics::GaugeFn for MetricHandle {
    fn increment(&self, _: f64) {}
    fn decrement(&self, _: f64) {}
    fn set(&self, _: f64) {}
}

impl metrics::HistogramFn for MetricHandle {
    fn record(&self, _: f64) {}
}

#[derive(Debug)]
struct RecordingMetrics {
    keys: Arc<Mutex<Vec<metrics::Key>>>,
}

impl metrics::Recorder for RecordingMetrics {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        self.keys
            .lock()
            .expect("metrics key mutex poisoned")
            .push(key.clone());
        metrics::Counter::from_arc(Arc::new(MetricHandle))
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        self.keys
            .lock()
            .expect("metrics key mutex poisoned")
            .push(key.clone());
        metrics::Gauge::from_arc(Arc::new(MetricHandle))
    }

    fn register_histogram(
        &self,
        key: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        self.keys
            .lock()
            .expect("metrics key mutex poisoned")
            .push(key.clone());
        metrics::Histogram::from_arc(Arc::new(MetricHandle))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum FeedKey {
    Symbol(&'static str),
    Control,
}

enum FeedMessage {
    Price { symbol: &'static str, value: u64 },
    UrgentControl,
    Crash,
}

#[derive(Clone)]
struct FeedArgs {
    first_init: Arc<AtomicBool>,
    initial_gate: ReleaseGate,
    crash_once: Arc<AtomicBool>,
    handled: Arc<Mutex<Vec<&'static str>>>,
}

struct FeedActor(FeedArgs);

impl Actor for FeedActor {
    type Msg = FeedMessage;
    type Args = FeedArgs;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        if args.first_init.swap(false, Ordering::SeqCst) {
            args.initial_gate.wait().await;
        }
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        match message {
            FeedMessage::Price { symbol, value } => {
                let _ = value;
                self.0
                    .handled
                    .lock()
                    .expect("feed log mutex poisoned")
                    .push(symbol);
            }
            FeedMessage::UrgentControl => {
                self.0
                    .handled
                    .lock()
                    .expect("feed log mutex poisoned")
                    .push("control");
            }
            FeedMessage::Crash => {
                if self.0.crash_once.swap(false, Ordering::SeqCst) {
                    return Err(ExitError::message("feed disconnected"));
                }
            }
        }
        Ok(())
    }
}

enum VenueMessage {
    Pause,
    Execute { order: u64, reply: Reply<u64> },
}

#[derive(Clone)]
struct VenueArgs {
    coordinator: shelterwood::ActorRef<CoordinatorMessage>,
    pause_entered: Arc<AtomicBool>,
    pause_release: ReleaseGate,
}

struct VenueActor(VenueArgs);

impl Actor for VenueActor {
    type Msg = VenueMessage;
    type Args = VenueArgs;

    async fn init(args: Self::Args, _: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, _: &mut Context<'_, Self>) -> ExitResult {
        match message {
            VenueMessage::Pause => {
                self.0.pause_entered.store(true, Ordering::SeqCst);
                self.0.pause_release.wait().await;
            }
            VenueMessage::Execute { order, reply } => {
                self.0
                    .coordinator
                    .try_send(CoordinatorMessage::VenueObserved(order))
                    .map_err(|_| ExitError::message("coordinator mailbox rejected venue event"))?;
                reply.send(order * 2);
            }
        }
        Ok(())
    }
}

enum CoordinatorMessage {
    BeginBatch,
    Completed { ok: bool },
    VenueObserved(u64),
    FeedEvent(MonitorEvent),
}

#[derive(Clone)]
struct CoordinatorArgs {
    feed: shelterwood::ActorRef<FeedMessage>,
    venue: shelterwood::ActorRef<VenueMessage>,
    completions: Arc<AtomicUsize>,
    observations: Arc<AtomicUsize>,
    stale_events: Arc<AtomicUsize>,
}

struct CoordinatorActor(CoordinatorArgs);

impl Actor for CoordinatorActor {
    type Msg = CoordinatorMessage;
    type Args = CoordinatorArgs;

    async fn init(args: Self::Args, context: &mut Context<'_, Self>) -> Result<Self, ExitError> {
        context
            .watch(&args.feed, CoordinatorMessage::FeedEvent)
            .map_err(|_| ExitError::message("feed watch rejected during coordinator init"))?;
        Ok(Self(args))
    }

    async fn handle(&mut self, message: Self::Msg, context: &mut Context<'_, Self>) -> ExitResult {
        match message {
            CoordinatorMessage::BeginBatch => {
                for order in 1..=3 {
                    let venue = self.0.venue.clone();
                    context
                        .offload(
                            async move {
                                venue
                                    .call(
                                        move |reply| VenueMessage::Execute { order, reply },
                                        Duration::from_secs(1),
                                    )
                                    .await
                            },
                            move |result| {
                                let ok = matches!(
                                    result,
                                    Ok(Ok(reply)) if reply.value == order * 2
                                );
                                CoordinatorMessage::Completed { ok }
                            },
                            Duration::from_secs(1),
                        )
                        .expect("live coordinator accepts deadline-owned call offload");
                }
            }
            CoordinatorMessage::Completed { ok } => {
                if !ok {
                    return Err(ExitError::message("venue call failed its shared budget"));
                }
                self.0.completions.fetch_add(1, Ordering::SeqCst);
            }
            CoordinatorMessage::VenueObserved(order) => {
                assert!((1..=3).contains(&order));
                self.0.observations.fetch_add(1, Ordering::SeqCst);
            }
            CoordinatorMessage::FeedEvent(event) => {
                if matches!(event.kind, MonitorEventKind::Exited { .. }) {
                    self.0.stale_events.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    async fn on_stop(&mut self, _: &mut StopContext<'_, Self>) {}
}

fn restart_on_failure() -> RestartPolicy {
    RestartPolicy::new(RestartCondition::OnFailure, Backoff::Immediate)
}

#[tokio::test]
async fn trading_engine_composes_part_two_without_a_registry_or_priority_lane() {
    let metric_keys = Arc::new(Mutex::new(Vec::new()));
    metrics::set_global_recorder(RecordingMetrics {
        keys: Arc::clone(&metric_keys),
    })
    .expect("trading test installs its process-global recorder once");
    let initial_gate = ReleaseGate::default();
    let feed_log = Arc::new(Mutex::new(Vec::new()));
    let completions = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(AtomicUsize::new(0));
    let stale_events = Arc::new(AtomicUsize::new(0));
    let pause_entered = Arc::new(AtomicBool::new(false));
    let pause_release = ReleaseGate::default();

    let mut root_tree = Tree::new();
    let feed_slot = root_tree
        .reserve_actor::<FeedMessage>("feed")
        .expect("feed slot is reserved before its factory exists");
    let feed = feed_slot.actor_ref();
    let coordinator_slot = root_tree
        .reserve_actor::<CoordinatorMessage>("coordinator")
        .expect("coordinator slot is reserved before its factory exists");
    let coordinator = coordinator_slot.actor_ref();
    let venue_scope_slot = root_tree
        .reserve_subtree::<Tree>("venues")
        .expect("venue scope is reserved before its declaration exists");

    let venue_intensity =
        Intensity::new(2, Duration::from_secs(5)).expect("non-zero intensity window");
    let mut venue_tree = Tree::new();
    venue_tree.intensity(venue_intensity);
    let venue_slot = venue_tree
        .reserve_actor::<VenueMessage>("primary")
        .expect("venue ref is minted before coordinator definition");
    let venue = venue_slot.actor_ref();

    // Every reference is now real, so the cyclic factories can be defined
    // without a registry or an Option<ActorRef> initialization phase.
    let _ = feed_slot.define(
        ActorDef::<FeedActor>::cloned(FeedArgs {
            first_init: Arc::new(AtomicBool::new(true)),
            initial_gate: initial_gate.clone(),
            crash_once: Arc::new(AtomicBool::new(true)),
            handled: Arc::clone(&feed_log),
        })
        .latest_by_key(
            KeyedCapacity::Explicit(NonZeroUsize::new(2).expect("two expected key classes")),
            |message| match message {
                FeedMessage::Price { symbol, .. } => FeedKey::Symbol(symbol),
                FeedMessage::UrgentControl | FeedMessage::Crash => FeedKey::Control,
            },
        )
        .restart(restart_on_failure()),
    );
    let _ = venue_slot.define(
        ActorDef::<VenueActor>::cloned(VenueArgs {
            coordinator: coordinator.clone(),
            pause_entered: Arc::clone(&pause_entered),
            pause_release: pause_release.clone(),
        })
        .restart(restart_on_failure()),
    );
    let _ = coordinator_slot.define(
        ActorDef::<CoordinatorActor>::cloned(CoordinatorArgs {
            feed: feed.clone(),
            venue: venue.clone(),
            completions: Arc::clone(&completions),
            observations: Arc::clone(&observations),
            stale_events: Arc::clone(&stale_events),
        })
        .mailbox(Mailbox::queue(8).expect("bounded coordinator FIFO")),
    );
    let venues = venue_scope_slot.define_once(SubtreeOnceDef::new(venue_tree));

    let system = root_tree.spawn().expect("runtime is available");
    let root = system.scope();

    // The first feed incarnation is parked in init. A flood for one data key
    // conflates to one slot, so try_send can still accept the distinct urgent
    // control key at the explicitly sized cardinality. No priority lane is
    // needed or implied; this only proves admission under flood.
    for value in 0..8 {
        feed.send(FeedMessage::Price {
            symbol: "EURUSD",
            value,
        })
        .await
        .expect("feed price is accepted while init is parked");
    }
    feed.try_send(FeedMessage::UrgentControl)
        .expect("adequately sized keyed mailbox admits control under data flood");
    let flooded = feed.stats().stats;
    assert_eq!(flooded.messages_accepted, 9);
    assert_eq!(flooded.messages_conflated, 7);
    assert_eq!(flooded.mailbox_depth, 2);
    assert_eq!(flooded.mailbox_capacity, 2);

    initial_gate.release();
    system.wait_started().await.expect("trading tree starts");
    assert_eq!(venues.snapshot().intensity, venue_intensity);
    assert_eq!(venues.snapshot().state, ScopeState::Running);
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            feed_log
                .lock()
                .expect("feed log mutex poisoned")
                .contains(&"control")
        })
        .await
    );

    // Park the venue handler, then launch three call futures from independent
    // incarnation-owned offloads. All three accept into its FIFO before the
    // first reply can exist, demonstrating genuine pipelining under one
    // deadline budget per offload/call pair.
    venue
        .send(VenueMessage::Pause)
        .await
        .expect("venue pause is accepted");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            pause_entered.load(Ordering::SeqCst)
        })
        .await
    );
    coordinator
        .send(CoordinatorMessage::BeginBatch)
        .await
        .expect("coordinator starts a batch");
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            coordinator.stats().stats.outstanding_offloads == 3
                && venue.stats().stats.mailbox_depth == 3
        })
        .await,
        "all calls must be in flight before venue processing resumes"
    );
    pause_release.release();
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            completions.load(Ordering::SeqCst) == 3
                && observations.load(Ordering::SeqCst) == 3
                && coordinator.stats().stats.outstanding_offloads == 0
        })
        .await
    );

    // The health breaker consumes the packaged cumulative restart reducer,
    // while the coordinator independently receives peer-watch staleness.
    let mut restarts = root.restart_counts();
    assert_eq!(
        restarts.next().await.expect("initial breaker sample").total,
        0
    );
    let breaker_open = Arc::new(AtomicBool::new(false));
    let breaker = {
        let breaker_open = Arc::clone(&breaker_open);
        tokio::spawn(async move {
            while let Some(sample) = restarts.next().await {
                if sample.total >= 1 {
                    breaker_open.store(true, Ordering::SeqCst);
                    return sample;
                }
            }
            panic!("restart stream closed before breaker input")
        })
    };
    let failed = feed
        .send(FeedMessage::Crash)
        .await
        .expect("feed crash request is accepted");
    feed.next_incarnation(failed, Duration::from_secs(1))
        .await
        .expect("feed membership rebinds after failure");
    let sample = breaker.await.expect("breaker consumer joins");
    assert_eq!(sample.total, 1);
    assert_eq!(sample.delta, 1);
    assert!(breaker_open.load(Ordering::SeqCst));
    assert!(
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || {
            stale_events.load(Ordering::SeqCst) >= 1
        })
        .await,
        "peer watch marks the failed feed incarnation stale"
    );

    let recorded = metric_keys
        .lock()
        .expect("metrics key mutex poisoned")
        .clone();
    let names = recorded
        .iter()
        .map(|key| key.name())
        .collect::<HashSet<_>>();
    assert!(names.contains("shelterwood.actor.messages_accepted"));
    assert!(names.contains("shelterwood.actor.messages_conflated"));
    assert!(names.contains("shelterwood.actor.outstanding_offloads"));
    assert!(names.contains("shelterwood.actor.mailbox_capacity"));
    assert!(recorded.iter().any(|key| {
        key.name() == "shelterwood.actor.messages_accepted"
            && key
                .labels()
                .any(|label| label.key() == "actor.id" && label.value() == "feed")
    }));

    system
        .shutdown(Duration::from_secs(1))
        .await
        .expect("trading engine shuts down");
}
