use std::task::Waker;

use crate::panic::PanicAccumulator;

/// The only storage surface for a caller-owned waker.
///
/// Its value is private even from the rest of this crate, and every mutating
/// operation requires an effects sink, so replacing or taking an
/// `Option<Waker>` and accidentally dropping it beside a guard does not
/// type-check.
///
/// # Implementation boundary
///
/// `WakerSlot`, [`WakerAction`], and [`WakerEffects`] are doc-hidden
/// cross-crate seams for the façade's mailbox and proxy code, not user
/// extension points. A direct `shelterwood-core` dependent that constructs
/// them is outside the supported façade contract, and the supported façade
/// re-exports none of them.
#[derive(Default)]
#[doc(hidden)]
pub struct WakerSlot(Option<Waker>);

/// Post-unlock disposition for a waker leaving a [`WakerSlot`].
///
/// `Run` is the one runtime-facing disposition: the adapter supplies a plain
/// disposer function (its detached disposal lane), so core names no runtime
/// type. See [`WakerSlot`]'s implementation boundary.
#[doc(hidden)]
pub enum WakerAction {
    Wake,
    DropInline,
    Run(fn(Waker)),
}

struct WakerEffect {
    waker: Waker,
    action: WakerAction,
}

/// Deferred waker effects, flushed only with no framework mutex held.
///
/// See [`WakerSlot`]'s implementation boundary. `is_empty` is `pub` because
/// the façade's mailbox effect batches probe their collected sinks.
#[derive(Default)]
#[doc(hidden)]
pub struct WakerEffects(Vec<WakerEffect>);

impl WakerSlot {
    pub fn will_wake(&self, waker: &Waker) -> bool {
        self.0
            .as_ref()
            .is_some_and(|registered| registered.will_wake(waker))
    }

    pub fn replace(&mut self, waker: Waker, effects: &mut WakerEffects) {
        if let Some(displaced) = self.0.replace(waker) {
            effects.push(displaced, WakerAction::DropInline);
        }
    }

    pub fn take(&mut self, action: WakerAction, effects: &mut WakerEffects) {
        if let Some(waker) = self.0.take() {
            effects.push(waker, action);
        }
    }
}

impl WakerEffects {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn push(&mut self, waker: Waker, action: WakerAction) {
        self.0.push(WakerEffect { waker, action });
    }

    pub fn flush(&mut self, panics: &mut PanicAccumulator) {
        for WakerEffect { waker, action } in self.0.drain(..) {
            match action {
                WakerAction::Wake => panics.run(|| waker.wake()),
                WakerAction::DropInline => panics.run(|| drop(waker)),
                WakerAction::Run(effect) => panics.run(|| effect(waker)),
            }
        }
    }
}

impl Drop for WakerEffects {
    fn drop(&mut self) {
        self.flush(&mut PanicAccumulator::default());
    }
}

#[cfg(test)]
mod tests {
    //! Direct pins for the flush primitive every lock-rule argument rests on.
    //!
    //! The probes are safe `Wake` wakers whose hostile `wake`/`drop` panic
    //! after recording themselves. Nothing here asserts inside a probe or a
    //! disposer: those run under `PanicAccumulator`'s `catch_unwind`, which
    //! would swallow the assertion. They publish to a log that the test body
    //! judges instead.

    use std::{
        cell::RefCell,
        panic::{AssertUnwindSafe, catch_unwind, panic_any},
        sync::{Arc, Mutex},
        task::{Wake, Waker},
    };

    use super::{WakerAction, WakerEffects, WakerSlot};
    use crate::panic::{PanicAccumulator, PanicPayload};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Event {
        Woken(u8),
        Dropped(u8),
    }

    type Log = Arc<Mutex<Vec<Event>>>;

    /// The payload a hostile probe panics with, so a test can tell which
    /// action's panic survived.
    struct HostileTag(u8);

    #[derive(Clone, Copy)]
    enum Hostility {
        None,
        Wake,
        Drop,
    }

    struct Probe {
        tag: u8,
        hostility: Hostility,
        log: Log,
    }

    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            self.log.lock().unwrap().push(Event::Woken(self.tag));
            if let Hostility::Wake = self.hostility {
                panic_any(HostileTag(self.tag));
            }
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            self.log.lock().unwrap().push(Event::Dropped(self.tag));
            if let Hostility::Drop = self.hostility {
                panic_any(HostileTag(self.tag));
            }
        }
    }

    fn probe(tag: u8, hostility: Hostility, log: &Log) -> Waker {
        Waker::from(Arc::new(Probe {
            tag,
            hostility,
            log: Arc::clone(log),
        }))
    }

    fn events(log: &Log) -> Vec<Event> {
        log.lock().unwrap().clone()
    }

    fn tag_of(payload: &PanicPayload) -> Option<u8> {
        payload.downcast_ref::<HostileTag>().map(|tag| tag.0)
    }

    thread_local! {
        static DISPOSED: RefCell<Vec<Waker>> = const { RefCell::new(Vec::new()) };
    }

    /// A `WakerAction::Run` disposer that only takes custody of the waker, so
    /// a test can see exactly what reached it. Flushes run on the calling
    /// thread, so a thread-local is per-test.
    fn stash(waker: Waker) {
        DISPOSED.with(|disposed| disposed.borrow_mut().push(waker));
    }

    fn take_disposed() -> Vec<Waker> {
        DISPOSED.with(|disposed| std::mem::take(&mut *disposed.borrow_mut()))
    }

    #[test]
    fn flush_runs_every_action_after_a_panicking_one_and_keeps_the_first_panic() {
        let log = Log::default();
        let mut effects = WakerEffects::default();
        effects.push(probe(1, Hostility::Wake, &log), WakerAction::Wake);
        effects.push(probe(2, Hostility::Drop, &log), WakerAction::DropInline);
        effects.push(probe(3, Hostility::None, &log), WakerAction::Wake);
        effects.push(probe(4, Hostility::None, &log), WakerAction::Run(stash));

        let mut panics = PanicAccumulator::default();
        effects.flush(&mut panics);

        assert_eq!(
            events(&log),
            [
                Event::Woken(1),
                Event::Dropped(1),
                Event::Dropped(2),
                Event::Woken(3),
                Event::Dropped(3),
            ],
            "every action ran, in push order, past both hostile ones"
        );
        let disposed = take_disposed();
        assert_eq!(disposed.len(), 1, "the trailing disposer still ran");
        assert!(effects.is_empty(), "flush drains every action");

        // `flush` itself does not resume: the caller's accumulator holds the
        // first panic and has already discarded the second.
        let first = panics.take().expect("the first action's panic is retained");
        assert_eq!(tag_of(&first), Some(1));
        assert!(panics.take().is_none());

        drop(disposed);
        assert_eq!(events(&log).last(), Some(&Event::Dropped(4)));
    }

    #[test]
    fn run_hands_the_waker_to_its_disposer_without_waking_it() {
        let log = Log::default();
        let waker = probe(1, Hostility::None, &log);
        let reference = waker.clone();
        let mut effects = WakerEffects::default();
        effects.push(waker, WakerAction::Run(stash));

        let mut panics = PanicAccumulator::default();
        effects.flush(&mut panics);

        assert!(panics.take().is_none());
        let disposed = take_disposed();
        assert_eq!(disposed.len(), 1, "the disposer received one waker");
        assert!(
            disposed[0].will_wake(&reference),
            "the disposer received the pushed waker itself"
        );
        assert_eq!(
            events(&log),
            [],
            "a disposed waker is neither woken nor dropped"
        );

        drop(reference);
        drop(disposed);
        assert_eq!(
            events(&log),
            [Event::Dropped(1)],
            "the disposer's custody was the only thing keeping it alive"
        );
    }

    #[test]
    fn dropping_unflushed_effects_runs_every_action() {
        let log = Log::default();
        let mut effects = WakerEffects::default();
        effects.push(probe(1, Hostility::None, &log), WakerAction::Wake);
        effects.push(probe(2, Hostility::None, &log), WakerAction::DropInline);
        effects.push(probe(3, Hostility::None, &log), WakerAction::Run(stash));

        drop(effects);

        assert_eq!(
            events(&log),
            [Event::Woken(1), Event::Dropped(1), Event::Dropped(2)]
        );
        assert_eq!(take_disposed().len(), 1);
    }

    #[test]
    fn dropping_unflushed_effects_runs_later_actions_then_resumes_the_first_panic() {
        let log = Log::default();
        let mut effects = WakerEffects::default();
        effects.push(probe(1, Hostility::Wake, &log), WakerAction::Wake);
        effects.push(probe(2, Hostility::Drop, &log), WakerAction::DropInline);
        effects.push(probe(3, Hostility::None, &log), WakerAction::Wake);

        let payload = catch_unwind(AssertUnwindSafe(move || drop(effects)))
            .expect_err("outside an unwind, the drop fallback resumes its first panic");

        assert_eq!(tag_of(&payload), Some(1));
        assert_eq!(
            events(&log),
            [
                Event::Woken(1),
                Event::Dropped(1),
                Event::Dropped(2),
                Event::Woken(3),
                Event::Dropped(3),
            ],
            "every action ran before the first panic was resumed"
        );
    }

    /// Abort-class: a resumed panic here would be a panic in a destructor
    /// during unwinding, which aborts the process. nextest runs each test in
    /// its own process and reports that abort as a failure.
    #[test]
    fn dropping_effects_during_an_unwind_contains_a_panicking_action() {
        let log = Log::default();
        let mut effects = WakerEffects::default();
        effects.push(probe(1, Hostility::Wake, &log), WakerAction::Wake);
        effects.push(probe(2, Hostility::Drop, &log), WakerAction::DropInline);
        effects.push(probe(3, Hostility::None, &log), WakerAction::Wake);

        let payload = catch_unwind(AssertUnwindSafe(move || {
            let _effects = effects;
            panic_any("outer panic");
        }))
        .expect_err("the outer unwind reaches its boundary");

        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"outer panic"),
            "the in-flight unwind keeps its own payload"
        );
        assert_eq!(
            events(&log),
            [
                Event::Woken(1),
                Event::Dropped(1),
                Event::Dropped(2),
                Event::Woken(3),
                Event::Dropped(3),
            ],
            "every action still ran during the unwind"
        );
    }

    #[test]
    fn slot_transitions_defer_every_departing_waker_to_the_sink() {
        let log = Log::default();
        let mut slot = WakerSlot::default();
        let mut effects = WakerEffects::default();

        slot.take(WakerAction::Wake, &mut effects);
        assert!(effects.is_empty(), "taking an empty slot defers nothing");

        let first = probe(1, Hostility::None, &log);
        slot.replace(first.clone(), &mut effects);
        assert!(
            effects.is_empty(),
            "filling an empty slot displaces nothing"
        );
        assert!(slot.will_wake(&first));
        drop(first);

        slot.replace(probe(2, Hostility::None, &log), &mut effects);
        assert!(!effects.is_empty());
        assert_eq!(events(&log), [], "the displaced waker waits for the flush");

        slot.take(WakerAction::Wake, &mut effects);
        assert_eq!(events(&log), [], "the taken waker waits for the flush");
        slot.take(WakerAction::Wake, &mut effects);

        let mut panics = PanicAccumulator::default();
        effects.flush(&mut panics);
        assert!(panics.take().is_none());
        assert_eq!(
            events(&log),
            [Event::Dropped(1), Event::Woken(2), Event::Dropped(2)],
            "the displaced waker is dropped, never woken; the taken one gets its action"
        );
    }
}
