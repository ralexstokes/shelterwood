//! Declaration lowering and its owned construction plan.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use crate::{
    ChildId, DefaultsInheritance, Exit, Intensity, Readiness, ScopeDefaults,
    admission::ReserveError,
    cells::{MemberCell, ObservationTxn, ScopeCell},
    definition::DefinitionSource,
    identity::{MembershipReconciliation, ScopeIdentity},
    policy::{
        ChildMode, CommonOptions, ResolvedCommonOptions, ResolvedDefaults, ScopeFlavor,
        resolve_common,
    },
    raw::RawConstruction,
    runtime::{self, Isolated, Latch},
    task::{OnceTask, TaskDef},
};

#[derive(Clone, Debug, Default)]
struct ScopeConfig {
    intensity: Intensity,
    defaults: ScopeDefaults,
}

pub(crate) enum ChildConstruction {
    Raw(RawConstruction),
    Task(TaskDef),
    TaskOnce(OnceTask),
    Scope(Box<ScopeConstruction>),
}

pub(crate) fn mint_reserved_slot(
    parent: &ScopeCell,
    id: &ChildId,
    child_scope: Option<ScopeFlavor>,
) -> Result<Arc<SlotCell>, ReserveError> {
    let membership = parent
        .mint_membership(id)
        .ok_or(ReserveError::IdentityExhausted)?;
    let member = MemberCell::new(id.clone(), membership);
    let scope = child_scope.map(|flavor| {
        let identity = ScopeIdentity::new();
        ScopeCell::new(Arc::clone(&member), flavor, identity)
    });
    Ok(SlotCell::new(member, scope))
}

/// Installs a valid option set for a manually constructed resident fixture,
/// preserving production lowering's resolve-before-residency ordering.
#[cfg(test)]
pub(crate) fn resolve_fixture_options(member: &MemberCell) {
    let options = resolve_common(
        &CommonOptions::default(),
        &ResolvedDefaults::default(),
        ChildMode::Restartable,
        Readiness::Immediate,
    );
    member.set_options(options);
}

enum DefinitionState {
    Undefined,
    Defined(Isolated<ChildConstruction>),
    Lowered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DefinitionAlreadyLowered;

/// Stable declaration slot joining identity, observation, and owned construction.
pub(crate) struct SlotCell {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) scope: Option<Arc<ScopeCell>>,
    definition: Mutex<DefinitionState>,
}

impl SlotCell {
    pub(crate) fn new(member: Arc<MemberCell>, scope: Option<Arc<ScopeCell>>) -> Arc<Self> {
        Arc::new(Self {
            member,
            scope,
            definition: Mutex::new(DefinitionState::Undefined),
        })
    }

    pub(crate) fn define(&self, definition: ChildConstruction) {
        // A rejected definition owns user construction closures, and the
        // panic below is a caller-contract violation rather than a broken
        // slot. Release the lock before either destroys anything, so a hostile
        // closure destructor cannot poison the definition mutex for every
        // later lowering step. `Isolated` keeps the destruction itself off
        // this thread.
        let rejected = {
            let mut state = self.definition.lock().expect("definition mutex poisoned");
            match *state {
                DefinitionState::Undefined => {
                    *state = DefinitionState::Defined(Isolated::new(definition));
                    None
                }
                DefinitionState::Defined(_) | DefinitionState::Lowered => {
                    Some(Isolated::new(definition))
                }
            }
        };
        assert!(
            rejected.is_none(),
            "a child slot was defined more than once"
        );
    }

    pub(crate) fn is_undefined(&self) -> bool {
        matches!(
            *self.definition.lock().expect("definition mutex poisoned"),
            DefinitionState::Undefined
        )
    }

    fn take_definition(
        &self,
    ) -> Result<Option<Isolated<ChildConstruction>>, DefinitionAlreadyLowered> {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        match std::mem::replace(&mut *state, DefinitionState::Lowered) {
            DefinitionState::Defined(definition) => Ok(Some(definition)),
            DefinitionState::Undefined => {
                *state = DefinitionState::Undefined;
                Ok(None)
            }
            DefinitionState::Lowered => Err(DefinitionAlreadyLowered),
        }
    }

    /// Publishes one never-started slot under the gate its observers use and
    /// evicts its lineage from `owner`'s identity map. Every terminalization
    /// evicts the lineage (SPEC §3.4): a later same-id declaration must mint
    /// an incomparable membership, never an ordered successor of the
    /// terminal predecessor. Taking the owning scope here keeps the pairing
    /// structural instead of repeated at each call site.
    pub(crate) fn terminalize_never_started(&self, owner: &ScopeCell) {
        self.publish_never_started();
        owner.evict_child_identity(&self.member);
    }

    fn publish_never_started(&self) {
        if let Some(scope) = &self.scope {
            scope.terminalize_never_started();
        } else {
            self.member.terminalize(
                Exit::never_started(),
                crate::cells::StartupDisposition::Unchanged,
            );
        }
    }

    fn terminalize_never_started_contained(
        &self,
        owner: &ScopeCell,
        panics: &mut runtime::PanicAccumulator,
    ) {
        // Publication can resume a hostile observer. Identity eviction is a
        // distinct teardown effect and must still run before the next slot.
        panics.run(|| self.publish_never_started());
        panics.run(|| owner.evict_child_identity(&self.member));
    }

    pub(crate) fn terminalize_never_started_locked(
        &self,
        owner: &ScopeCell,
        txn: &mut ObservationTxn<'_>,
    ) {
        if let Some(scope) = &self.scope {
            scope.terminalize_never_started_locked(txn);
        } else {
            self.member.terminalize_locked(
                Exit::never_started(),
                crate::cells::StartupDisposition::Unchanged,
                txn,
            );
        }
        owner.evict_child_identity(&self.member);
    }

    pub(crate) fn take_never_started_locked(
        &self,
        owner: &ScopeCell,
        txn: &mut ObservationTxn<'_>,
    ) -> Option<Isolated<ChildConstruction>> {
        let definition = self.take_definition().ok().flatten();
        self.terminalize_never_started_locked(owner, txn);
        definition
    }

    pub(crate) fn resolve_policy(
        &self,
        defaults: &ResolvedDefaults,
    ) -> Option<ResolvedCommonOptions> {
        let state = self.definition.lock().expect("definition mutex poisoned");
        let DefinitionState::Defined(definition) = &*state else {
            return None;
        };
        Some(self.resolve_defined_policy(definition, defaults))
    }

    /// Resolves policy and claims the matching construction under one
    /// definition lock. Dynamic removal can therefore observe either the
    /// unclaimed definition or the completed claim, never the gap between
    /// those operations.
    pub(crate) fn resolve_and_take_defined(
        &self,
        defaults: &ResolvedDefaults,
    ) -> Option<(Isolated<ChildConstruction>, ResolvedCommonOptions)> {
        self.resolve_and_take_defined_with(defaults, || {})
    }

    /// The body of [`Self::resolve_and_take_defined`], with a seam between
    /// resolution and claim.
    ///
    /// Production always passes an empty closure; the sole consumer of a
    /// non-empty one is
    /// `dynamic_policy_resolution_and_definition_claim_are_atomic`, which
    /// releases a competing remover through it. Delegating rather than
    /// duplicating is what keeps that test pinned to the shipped path: an
    /// unlock injected here fails it.
    fn resolve_and_take_defined_with(
        &self,
        defaults: &ResolvedDefaults,
        before_claim: impl FnOnce(),
    ) -> Option<(Isolated<ChildConstruction>, ResolvedCommonOptions)> {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        let DefinitionState::Defined(definition) = &*state else {
            return None;
        };
        let resolved = self.resolve_defined_policy(definition, defaults);
        before_claim();
        let DefinitionState::Defined(definition) =
            std::mem::replace(&mut *state, DefinitionState::Lowered)
        else {
            unreachable!("the definition lock keeps resolution and claim atomic")
        };
        Some((definition, resolved))
    }

    fn resolve_defined_policy(
        &self,
        definition: &Isolated<ChildConstruction>,
        defaults: &ResolvedDefaults,
    ) -> ResolvedCommonOptions {
        let (options, mode, default_readiness) = match definition.get() {
            // A raw definition resolved its effective mode at erasure, where
            // the actor type's `RawActor::readiness` metadata was still
            // nameable, so it arrives here as the kind default.
            ChildConstruction::Raw(definition) => (
                definition.options(),
                definition.mode(),
                definition.readiness(),
            ),
            ChildConstruction::Task(definition) => (
                &definition.options,
                ChildMode::Restartable,
                Readiness::Immediate,
            ),
            ChildConstruction::TaskOnce(definition) => (
                &definition.options,
                ChildMode::OneShot,
                Readiness::Immediate,
            ),
            ChildConstruction::Scope(definition) => {
                let options = CommonOptions {
                    readiness: None,
                    ..definition.options.clone()
                };
                return resolve_common(&options, defaults, definition.mode(), Readiness::Manual);
            }
        };
        resolve_common(options, defaults, mode, default_readiness)
    }
}

/// Erased declaration storage before inherited defaults and identities are lowered.
pub(crate) struct BuilderCore {
    pub(crate) root: Arc<ScopeCell>,
    config: ScopeConfig,
    pub(crate) slots: Vec<Arc<SlotCell>>,
    ids: HashSet<ChildId>,
    /// The stable scope whose identity map `lower(root_override)` adopts the
    /// slots' lineages into. Eviction must target the map a lineage actually
    /// entered: before adoption begins the lineages live in `root`'s map,
    /// afterwards they live here.
    adopting_root: Option<Arc<ScopeCell>>,
    armed: bool,
}

impl BuilderCore {
    pub(crate) fn set_intensity(&mut self, intensity: Intensity) {
        self.config.intensity = intensity;
    }

    pub(crate) fn set_defaults(&mut self, defaults: ScopeDefaults) {
        self.config.defaults = defaults;
    }

    pub(crate) fn config_debug(&self) -> impl std::fmt::Debug + '_ {
        &self.config
    }

    fn begin_failed_disposal(&self) -> Latch {
        let definitions = self
            .slots
            .iter()
            .filter_map(|slot| slot.take_definition().ok().flatten())
            .filter_map(|mut definition| definition.take())
            .collect();
        runtime::dispose_all(definitions)
    }

    pub(crate) fn new(flavor: ScopeFlavor) -> Self {
        let root_id = ChildId::from("$root");
        let mut root_identity = ScopeIdentity::new();
        let membership = root_identity
            .mint_membership(&root_id)
            .expect("fresh scope identity must mint its root membership");
        let member = MemberCell::new(root_id, membership);
        let child_identity = ScopeIdentity::new();
        let root = ScopeCell::new(member, flavor, child_identity);
        Self {
            root,
            config: ScopeConfig::default(),
            slots: Vec::new(),
            ids: HashSet::new(),
            adopting_root: None,
            armed: true,
        }
    }

    pub(crate) fn reserve(
        &mut self,
        id: impl Into<ChildId>,
        scope: Option<ScopeFlavor>,
    ) -> Result<Arc<SlotCell>, ReserveError> {
        let id = checked_id(id)?;
        if self.ids.contains(&id) {
            return Err(ReserveError::DuplicateId(id));
        }
        let slot = mint_reserved_slot(&self.root, &id, scope)?;
        self.ids.insert(id);
        self.slots.push(Arc::clone(&slot));
        Ok(slot)
    }

    pub(crate) fn lower(
        mut self,
        inherited: ResolvedDefaults,
        root_override: Option<Arc<ScopeCell>>,
    ) -> Result<ScopePlan, LowerError> {
        let root = root_override.unwrap_or_else(|| Arc::clone(&self.root));
        let undefined: Vec<_> = self
            .slots
            .iter()
            .filter(|slot| slot.is_undefined())
            .map(|slot| slot.member.id().clone())
            .collect();
        if !undefined.is_empty() {
            let disposal = self.begin_failed_disposal();
            return Err(LowerError::Undefined {
                paths: undefined,
                disposal,
            });
        }
        let (defaults, resolved) = self.resolve_policies(&inherited);
        if !Arc::ptr_eq(&root, &self.root) {
            // From the first adoption on, a failed or unwound lowering must
            // evict the terminalized slots from the override's identity map,
            // not this builder's throwaway root.
            self.adopting_root = Some(Arc::clone(&root));
            for slot in &self.slots {
                match root.adopt_or_mint_membership(slot.member.id(), slot.member.membership()) {
                    MembershipReconciliation::Adopted => {}
                    MembershipReconciliation::Minted(identity) => {
                        slot.member.rebase_membership(identity);
                    }
                    MembershipReconciliation::Exhausted => {
                        let id = slot.member.id().clone();
                        let disposal = self.begin_failed_disposal();
                        return Err(LowerError::IdentityExhausted { id, disposal });
                    }
                }
            }
        }
        root.set_observation_config(self.config.intensity);
        let mut children = Vec::with_capacity(self.slots.len());
        debug_assert_eq!(
            self.slots.len(),
            resolved.len(),
            "policy resolution is one entry per slot, in slot order"
        );
        for (slot, resolved) in self.slots.iter().zip(resolved) {
            let resolved = resolved.expect("defined slot must have resolved options");
            let definition = slot
                .take_definition()
                .expect("a tree must be lowered at most once")
                .expect("validated slot must have a definition");
            children.push(ChildPlan::with_options(
                Arc::clone(slot),
                definition,
                resolved,
            ));
        }
        self.armed = false;
        Ok(ScopePlan {
            root,
            config: self.config.clone(),
            defaults,
            children,
            terminality: Some(ScopePlanTerminality),
        })
    }

    fn resolve_policies(
        &self,
        inherited: &ResolvedDefaults,
    ) -> (ResolvedDefaults, Vec<Option<ResolvedCommonOptions>>) {
        let defaults = inherited.overlay(&self.config.defaults);
        // One entry per slot, in slot order. An undefined slot resolves to
        // `None` rather than being skipped, so this vector can never shift a
        // later child onto another child's options.
        let mut resolved = Vec::with_capacity(self.slots.len());
        for slot in &self.slots {
            resolved.push(slot.resolve_policy(&defaults));
        }
        (defaults, resolved)
    }

    fn terminalize(&self) {
        // A failed `lower(root_override)` may have adopted only a prefix of
        // these lineages into the override's map before the error or unwind.
        // Slots past the failure point never left `self.root`'s map, so their
        // eviction from the override is a fence-mismatch no-op (fail closed).
        let identity_root = self.adopting_root.as_ref().unwrap_or(&self.root);
        let mut panics = runtime::PanicAccumulator::default();
        for slot in &self.slots {
            slot.terminalize_never_started_contained(identity_root, &mut panics);
        }
        panics.run(|| self.root.terminalize_never_started());
    }
}

impl Drop for BuilderCore {
    fn drop(&mut self) {
        if self.armed {
            self.terminalize();
        }
    }
}

/// Fully lowered scope plan whose construction payloads still have one owner.
pub(crate) struct ScopePlan {
    pub(crate) root: Arc<ScopeCell>,
    config: ScopeConfig,
    pub(crate) defaults: ResolvedDefaults,
    pub(crate) children: Vec<ChildPlan>,
    terminality: Option<ScopePlanTerminality>,
}

struct ScopePlanTerminality;

impl ScopePlan {
    pub(crate) fn intensity_policy(&self) -> Intensity {
        self.config.intensity
    }
}

fn terminalize_plan(
    root: &ScopeCell,
    children: &[ChildPlan],
    terminality: &mut Option<ScopePlanTerminality>,
) {
    if terminality.take().is_some() {
        let mut panics = runtime::PanicAccumulator::default();
        for child in children {
            child
                .slot
                .terminalize_never_started_contained(root, &mut panics);
        }
        panics.run(|| {
            root.with_observation_gate(|txn| {
                // Lowering can publish the planned children before ScopeRuntime
                // takes ownership. The plan fallback commits residency withdrawal
                // and root closure as one root-scope observation.
                root.clear_residents_locked(txn);
                root.terminalize_never_started_locked(txn);
            });
        });
    }
}

impl ScopePlan {
    /// Finishes the transfer after every child has installed its own
    /// terminality obligation. Consuming `self` makes a partial handoff
    /// impossible to mistake for a completed one.
    pub(crate) fn finish_transfer(mut self) {
        assert!(
            self.children.is_empty(),
            "runtime transfer completes only after every child owns terminality"
        );
        self.terminality
            .take()
            .expect("runtime plan transfer completes exactly once");
    }
}

impl Drop for ScopePlan {
    fn drop(&mut self) {
        terminalize_plan(&self.root, &self.children, &mut self.terminality);
    }
}

pub(crate) struct ChildPlan {
    pub(crate) slot: Arc<SlotCell>,
    pub(crate) construction: Isolated<ChildConstruction>,
    pub(crate) options: ResolvedCommonOptions,
}

impl ChildPlan {
    pub(crate) fn with_options(
        slot: Arc<SlotCell>,
        construction: Isolated<ChildConstruction>,
        options: ResolvedCommonOptions,
    ) -> Self {
        slot.member.set_options(options.clone());
        Self {
            slot,
            construction,
            options,
        }
    }
}

#[derive(Debug)]
pub(crate) enum LowerError {
    Undefined {
        paths: Vec<ChildId>,
        disposal: Latch,
    },
    IdentityExhausted {
        id: ChildId,
        disposal: Latch,
    },
}

pub(crate) struct ScopeConstruction {
    pub(crate) source: DefinitionSource<ScopeFactory, Box<BuilderCore>>,
    pub(crate) options: CommonOptions,
    pub(crate) defaults: DefaultsInheritance,
}

pub(crate) type ScopeFactory = Arc<dyn Fn() -> BuilderCore + Send + Sync + 'static>;

impl ScopeConstruction {
    pub(crate) fn mode(&self) -> ChildMode {
        if self.source.is_one_shot() {
            ChildMode::OneShot
        } else {
            ChildMode::Restartable
        }
    }

    pub(crate) fn restartable(&self) -> Option<&ScopeFactory> {
        self.source.restartable()
    }

    pub(crate) fn take_one_shot(&mut self) -> Option<Box<BuilderCore>> {
        self.source.take_one_shot()
    }
}

pub(crate) fn checked_id(id: impl Into<ChildId>) -> Result<ChildId, ReserveError> {
    let id = id.into();
    if id.as_str().is_empty() {
        Err(ReserveError::EmptyId)
    } else {
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::{
            Arc, Barrier,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Wake, Waker},
        time::Duration,
    };

    use crate::{
        Backoff, ChildId, DefaultsInheritance, ExitError, Readiness, RestartCondition,
        RestartPolicy, Retention, Shutdown, TaskOnceDef,
        cells::{MemberCell, MemberStage, ResidentProjection, ScopeCell},
        definition::DefinitionSource,
        identity::ScopeIdentity,
        policy::{CommonOptions, ResolvedDefaults, ScopeFlavor},
        raw::RawConstruction,
        runtime::{self, Timeout},
        task::TaskDef,
    };

    use super::{
        BuilderCore, ChildConstruction, ChildPlan, LowerError, ScopeConstruction, SlotCell,
    };

    fn configured_task() -> TaskDef {
        TaskDef::new(|_| async { Ok(()) })
            .restart(RestartPolicy::new(
                RestartCondition::Always,
                Backoff::Immediate,
            ))
            .shutdown(Shutdown::Abort)
            .readiness(Readiness::Manual)
            .expect("manual task readiness is valid")
            .retention(Retention::Remove)
    }

    struct PanicWake(&'static str);

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(self.0);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            std::panic::panic_any(self.0);
        }
    }

    fn two_slot_builder() -> (BuilderCore, Vec<Arc<SlotCell>>) {
        let mut builder = BuilderCore::new(ScopeFlavor::Ordered);
        let mut slots = Vec::new();
        for id in ["first", "second"] {
            let slot = builder.reserve(id, None).expect("reservation succeeds");
            slot.define(ChildConstruction::Task(configured_task()));
            slots.push(slot);
        }
        (builder, slots)
    }

    #[test]
    fn builder_drop_terminalizes_every_slot_and_root_after_a_hostile_wake() {
        const PANIC: &str = "injected builder terminalization wake panic";

        let (builder, slots) = two_slot_builder();
        let root = Arc::clone(&builder.root);
        let mut first_terminal = Box::pin(slots[0].member.wait_terminal());
        let hostile = Waker::from(Arc::new(PanicWake(PANIC)));
        assert!(
            first_terminal
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );

        let result = catch_unwind(AssertUnwindSafe(|| drop(builder)));

        let payload = result.expect_err("the first hostile slot wake still surfaces");
        assert_eq!(payload.downcast_ref::<&'static str>().copied(), Some(PANIC));
        for slot in &slots {
            assert!(
                matches!(slot.member.record().stage, MemberStage::Terminal(_)),
                "builder teardown terminalizes every declared slot"
            );
        }
        assert!(
            matches!(root.member.record().stage, MemberStage::Terminal(_)),
            "builder teardown terminalizes its root after every slot"
        );
    }

    #[test]
    fn plan_drop_terminalizes_every_child_and_root_after_a_hostile_wake() {
        const PANIC: &str = "injected plan terminalization wake panic";

        let (builder, slots) = two_slot_builder();
        let plan = builder
            .lower(ResolvedDefaults::default(), None)
            .expect("the defined builder lowers");
        let root = Arc::clone(&plan.root);
        root.set_admitted_children(
            plan.children
                .iter()
                .map(|child| ResidentProjection::new(Arc::clone(&child.slot.member), None))
                .collect(),
        );
        let mut first_terminal = Box::pin(slots[0].member.wait_terminal());
        let hostile = Waker::from(Arc::new(PanicWake(PANIC)));
        assert!(
            first_terminal
                .as_mut()
                .poll(&mut Context::from_waker(&hostile))
                .is_pending()
        );

        let result = catch_unwind(AssertUnwindSafe(|| drop(plan)));

        let payload = result.expect_err("the first hostile child wake still surfaces");
        assert_eq!(payload.downcast_ref::<&'static str>().copied(), Some(PANIC));
        for slot in &slots {
            assert!(
                matches!(slot.member.record().stage, MemberStage::Terminal(_)),
                "plan teardown terminalizes every transferred child"
            );
        }
        assert!(
            matches!(root.member.record().stage, MemberStage::Terminal(_)),
            "plan teardown closes the root after every child"
        );
        assert!(
            root.snapshot().children.is_empty(),
            "plan teardown withdraws every published residency before root closure"
        );
    }

    #[test]
    fn static_and_dynamic_constructions_share_option_resolution() {
        let defaults = ResolvedDefaults::default();
        let mut declaration = BuilderCore::new(ScopeFlavor::Ordered);
        let static_slot = declaration
            .reserve("static", None)
            .expect("static reservation succeeds");
        static_slot.define(ChildConstruction::Task(configured_task()));
        let static_plan = declaration
            .lower(defaults.clone(), None)
            .expect("defined declaration lowers");

        let mut admission = BuilderCore::new(ScopeFlavor::Dynamic);
        let dynamic_slot = admission
            .reserve("dynamic", None)
            .expect("dynamic reservation succeeds");
        dynamic_slot.define(ChildConstruction::Task(configured_task()));
        let dynamic_options = dynamic_slot
            .resolve_policy(&defaults)
            .expect("dynamic slot is defined");
        let dynamic_definition = dynamic_slot
            .take_definition()
            .expect("dynamic slot must not already be lowered")
            .expect("dynamic slot remains claimable");
        let dynamic_plan =
            ChildPlan::with_options(dynamic_slot, dynamic_definition, dynamic_options);

        assert_eq!(static_plan.children[0].options, dynamic_plan.options);
        assert_eq!(dynamic_plan.options.readiness, Readiness::Manual);
    }

    fn resolved_readiness(construction: ChildConstruction) -> Readiness {
        let declaration = BuilderCore::new(ScopeFlavor::Dynamic);
        let slot = SlotCell::new(Arc::clone(&declaration.root.member), None);
        slot.define(construction);
        slot.resolve_policy(&ResolvedDefaults::default())
            .expect("slot is defined")
            .readiness
    }

    fn raw_construction(options: CommonOptions) -> ChildConstruction {
        // Mirrors `RawDef::erase`: the effective mode is resolved against the
        // actor type's `RawActor::readiness` metadata (`Manual` here, distinct
        // from every plan-level kind default) before the construction is
        // erased.
        let readiness = options.readiness.unwrap_or(Readiness::Manual);
        ChildConstruction::Raw(RawConstruction::for_policy_test(options, readiness))
    }

    fn scope_construction(options: CommonOptions) -> ChildConstruction {
        ChildConstruction::Scope(Box::new(ScopeConstruction {
            source: DefinitionSource::Restartable(Arc::new(|| {
                BuilderCore::new(ScopeFlavor::Ordered)
            })),
            options,
            defaults: DefaultsInheritance::Inherit,
        }))
    }

    #[test]
    fn resolution_applies_per_kind_readiness_defaults() {
        // Undeclared readiness resolves per construction kind: tasks gate
        // immediately, scopes wait for their startup barrier, and a raw
        // definition carries the mode already resolved from its actor type's
        // metadata at erasure.
        assert_eq!(
            resolved_readiness(ChildConstruction::Task(TaskDef::new(|_| async { Ok(()) }))),
            Readiness::Immediate
        );
        let (completion, _claim) = runtime::oneshot();
        assert_eq!(
            resolved_readiness(ChildConstruction::TaskOnce(
                TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) }).erase(completion)
            )),
            Readiness::Immediate
        );
        assert_eq!(
            resolved_readiness(scope_construction(CommonOptions::default())),
            Readiness::Manual
        );
        assert_eq!(
            resolved_readiness(raw_construction(CommonOptions::default())),
            Readiness::Manual
        );

        // Task and raw declarations expose readiness overrides. Scope
        // readiness is structural even if an internal fixture forges the
        // otherwise-unreachable common-options field.
        assert_eq!(
            resolved_readiness(ChildConstruction::Task(
                TaskDef::new(|_| async { Ok(()) })
                    .readiness(Readiness::Manual)
                    .expect("manual task readiness is valid")
            )),
            Readiness::Manual
        );
        let (completion, _claim) = runtime::oneshot();
        assert_eq!(
            resolved_readiness(ChildConstruction::TaskOnce(
                TaskOnceDef::new(|_| async { Ok::<_, ExitError>(()) })
                    .readiness(Readiness::Manual)
                    .expect("manual task readiness is valid")
                    .erase(completion)
            )),
            Readiness::Manual
        );
        assert_eq!(
            resolved_readiness(scope_construction(CommonOptions {
                readiness: Some(Readiness::Immediate),
                ..CommonOptions::default()
            })),
            Readiness::Manual
        );
        assert_eq!(
            resolved_readiness(raw_construction(CommonOptions {
                readiness: Some(Readiness::Immediate),
                ..CommonOptions::default()
            })),
            Readiness::Immediate
        );
    }

    #[test]
    fn dynamic_policy_resolution_and_definition_claim_are_atomic() {
        let defaults = ResolvedDefaults::default();
        let mut declaration = BuilderCore::new(ScopeFlavor::Dynamic);
        let slot = declaration
            .reserve("dynamic", None)
            .expect("dynamic reservation succeeds");
        slot.define(ChildConstruction::Task(configured_task()));

        let race = Arc::new(Barrier::new(2));
        let competing_slot = Arc::clone(&slot);
        let competing_race = Arc::clone(&race);
        let competing_claim = std::thread::spawn(move || {
            competing_race.wait();
            competing_slot.take_definition().ok().flatten()
        });

        let claim = slot
            .resolve_and_take_defined_with(&defaults, || {
                // The competing remover is released only after resolution,
                // while this method still owns the definition lock.
                race.wait();
                std::thread::yield_now();
            })
            .expect("admission claims the defined construction");
        assert!(
            competing_claim
                .join()
                .expect("competing claim thread does not panic")
                .is_none(),
            "removal cannot claim between policy resolution and admission"
        );
        drop(claim);
    }

    #[crate::runtime::test]
    async fn failed_override_lowering_evicts_adopted_lineages_from_the_override() {
        // A stable scope whose identity domain for "exhausted" has no
        // generations left, so adoption of that id must fail after the
        // preceding slot's lineage was already adopted.
        let root_id = ChildId::from("$root");
        let mut root_identity = ScopeIdentity::new();
        let root_membership = root_identity
            .mint_membership(&root_id)
            .expect("fresh scope identity must mint its root membership");
        let member = MemberCell::new(root_id, root_membership);
        let exhausted_id = ChildId::from("exhausted");
        let mut child_identity = ScopeIdentity::near_exhaustion(exhausted_id.clone(), 7);
        let _ = child_identity
            .mint_membership(&exhausted_id)
            .expect("the final generation is mintable");
        let stable = ScopeCell::new(member, ScopeFlavor::Ordered, child_identity);

        let mut builder = BuilderCore::new(ScopeFlavor::Ordered);
        let adopted = builder
            .reserve("adopted", None)
            .expect("reservation succeeds");
        adopted.define(ChildConstruction::Task(configured_task()));
        let adopted_membership = adopted.member.membership();
        let failing = builder
            .reserve("exhausted", None)
            .expect("reservation succeeds");
        failing.define(ChildConstruction::Task(configured_task()));

        let Err(error) = builder.lower(ResolvedDefaults::default(), Some(Arc::clone(&stable)))
        else {
            panic!("the exhausted id fails adoption");
        };
        let LowerError::IdentityExhausted { id, disposal } = error else {
            panic!("partial adoption fails with identity exhaustion");
        };
        assert_eq!(id.as_str(), "exhausted");
        disposal.fired().await;

        // A restart-style rebuild reconciles against the same stable scope.
        // The terminalized slot's lineage must have been evicted from the
        // override's identity map — not the failed builder's throwaway root —
        // so the re-added id donates a fresh lineage that is incomparable in
        // both directions instead of minting an ordered successor.
        let mut rebuild = BuilderCore::new(ScopeFlavor::Ordered);
        let readded = rebuild
            .reserve("adopted", None)
            .expect("re-added id is reservable");
        readded.define(ChildConstruction::Task(configured_task()));
        let replacement = rebuild
            .lower(ResolvedDefaults::default(), Some(stable))
            .expect("the rebuild lowers");
        let readded_membership = replacement.children[0].slot.member.membership();
        assert!(
            !readded_membership.supersedes(adopted_membership),
            "a rebuilt membership must not supersede its terminalized predecessor"
        );
        assert!(
            !adopted_membership.supersedes(readded_membership),
            "a terminalized predecessor must not order against its replacement"
        );
    }

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[crate::runtime::test]
    async fn resolving_one_shot_construction_preserves_owned_drop() {
        let dropped = Arc::new(AtomicBool::new(false));
        let capture = DropFlag(Arc::clone(&dropped));
        let (completion, _claim) = runtime::oneshot();
        let construction = TaskOnceDef::new(move |_| {
            let _ = &capture;
            async { Ok::<_, ExitError>(()) }
        })
        .erase(completion);
        let mut declaration = BuilderCore::new(ScopeFlavor::Dynamic);
        let slot = declaration
            .reserve("one-shot", None)
            .expect("reservation succeeds");
        slot.define(ChildConstruction::TaskOnce(construction));
        let defaults = ResolvedDefaults::default();
        let options = slot
            .resolve_policy(&defaults)
            .expect("one-shot slot is defined");
        let construction = slot
            .take_definition()
            .expect("one-shot slot must not already be lowered")
            .expect("one-shot slot remains claimable");
        let plan = ChildPlan::with_options(slot, construction, options);
        assert!(plan.options.restart.is_never());
        assert_eq!(plan.options.retention, Retention::Remove);
        assert!(!dropped.load(Ordering::SeqCst));

        drop(plan);
        let disposed = runtime::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                runtime::yield_now().await;
            }
        })
        .await;
        assert!(matches!(disposed, Timeout::Completed(())));
    }
}
