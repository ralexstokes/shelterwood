//! Declaration lowering and its owned construction plan.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    ChildId, DefaultsInheritance, Exit, Intensity, Readiness, ScopeDefaults, Strategy,
    admission::ReserveError,
    cells::{ErasedDynamicSlot, MemberCell, ObservationTxn, ScopeCell},
    definition::DefinitionSource,
    identity::{IdError, ScopeIdentity},
    policy::{
        ChildMode, CommonOptions, InvalidPolicy, PolicyField, ResolvedCommonOptions,
        ResolvedDefaults, ScopeFlavor, resolve_common,
    },
    raw::RawConstruction,
    runtime::{self, Isolated, Latch},
    task::{OnceTask, TaskDef},
};

#[derive(Clone, Debug, Default)]
pub(crate) struct ScopeConfig {
    pub(crate) strategy: Strategy,
    pub(crate) intensity: Intensity,
    pub(crate) defaults: ScopeDefaults,
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
        .child_identity
        .lock()
        .expect("scope identity mutex poisoned")
        .mint_membership(id)
        .ok_or(ReserveError::IdentityExhausted)?;
    let member = MemberCell::new(id.clone(), membership);
    let scope = child_scope.map(|flavor| {
        let identity = ScopeIdentity::new();
        ScopeCell::new(Arc::clone(&member), flavor, identity)
    });
    Ok(SlotCell::new(member, scope))
}

enum DefinitionState {
    Undefined,
    Defined(Isolated<ChildConstruction>),
    Lowered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DefinitionAlreadyLowered;

/// Stable declaration slot joining identity, observation, and owned construction.
pub(crate) struct SlotCell {
    pub(crate) member: Arc<MemberCell>,
    pub(crate) scope: Option<Arc<ScopeCell>>,
    definition: Mutex<DefinitionState>,
}

/// Erases a plan-owned slot without replacing its canonical `Arc` allocation.
///
/// Dynamic-route calls cross the lower cell layer through this representation;
/// only the paired recovery helpers below consume values from that boundary.
pub(crate) fn erase_dynamic_slot(slot: Arc<SlotCell>) -> Arc<ErasedDynamicSlot> {
    slot
}

/// Recovers the plan-owned slot returned by the crate's dynamic route.
pub(crate) fn concrete_dynamic_slot(slot: Arc<ErasedDynamicSlot>) -> Arc<SlotCell> {
    Arc::downcast(slot).expect("the dynamic route must return its plan-owned slot type")
}

/// Borrows the plan-owned slot passed back to the crate's dynamic route.
pub(crate) fn concrete_dynamic_slot_ref(slot: &ErasedDynamicSlot) -> &SlotCell {
    slot.downcast_ref()
        .expect("the dynamic route must receive its plan-owned slot type")
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
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        match *state {
            DefinitionState::Undefined => {
                *state = DefinitionState::Defined(Isolated::new(definition));
            }
            DefinitionState::Defined(_) | DefinitionState::Lowered => {
                panic!("a child slot was defined more than once")
            }
        }
    }

    pub(crate) fn is_undefined(&self) -> bool {
        matches!(
            *self.definition.lock().expect("definition mutex poisoned"),
            DefinitionState::Undefined
        )
    }

    pub(crate) fn take_definition(
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
        if let Some(scope) = &self.scope {
            scope.terminalize_never_started();
        } else {
            self.member.terminalize(Exit::never_started());
        }
        owner.evict_child_identity(&self.member);
    }

    pub(crate) fn terminalize_never_started_locked(
        &self,
        owner: &ScopeCell,
        txn: &mut ObservationTxn<'_>,
    ) {
        self.member.terminalize_locked(Exit::never_started(), txn);
        if let Some(scope) = &self.scope {
            scope.terminalize_never_started_locked(txn);
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
    ) -> Result<Option<ResolvedCommonOptions>, InvalidPolicy> {
        let state = self.definition.lock().expect("definition mutex poisoned");
        let DefinitionState::Defined(definition) = &*state else {
            return Ok(None);
        };
        self.resolve_defined_policy(definition, defaults).map(Some)
    }

    /// Resolves policy and claims the matching construction under one
    /// definition lock. Dynamic removal can therefore observe either the
    /// unclaimed definition or the completed claim, never the gap between
    /// those operations.
    pub(crate) fn resolve_and_take_defined(
        &self,
        defaults: &ResolvedDefaults,
    ) -> Result<Option<(Isolated<ChildConstruction>, ResolvedCommonOptions)>, InvalidPolicy> {
        self.resolve_and_take_defined_with(defaults, || {})
    }

    fn resolve_and_take_defined_with(
        &self,
        defaults: &ResolvedDefaults,
        before_claim: impl FnOnce(),
    ) -> Result<Option<(Isolated<ChildConstruction>, ResolvedCommonOptions)>, InvalidPolicy> {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        let DefinitionState::Defined(definition) = &*state else {
            return Ok(None);
        };
        let resolved = self.resolve_defined_policy(definition, defaults)?;
        before_claim();
        let DefinitionState::Defined(definition) =
            std::mem::replace(&mut *state, DefinitionState::Lowered)
        else {
            unreachable!("the definition lock keeps resolution and claim atomic")
        };
        Ok(Some((definition, resolved)))
    }

    fn resolve_defined_policy(
        &self,
        definition: &Isolated<ChildConstruction>,
        defaults: &ResolvedDefaults,
    ) -> Result<ResolvedCommonOptions, InvalidPolicy> {
        let (options, mode, default_readiness) = match definition.get() {
            // A raw definition resolved its effective mode at erasure, where
            // the actor type's `RawActor::readiness` metadata was still
            // nameable, so it arrives here as the kind default.
            ChildConstruction::Raw(definition) => {
                (&definition.options, definition.mode(), definition.readiness)
            }
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
                if let DefinitionSource::OneShot(tree) = &definition.source {
                    let inherited = match definition.defaults {
                        DefaultsInheritance::Inherit => defaults.clone(),
                        DefaultsInheritance::Reset => ResolvedDefaults::default(),
                    };
                    tree.validate_policies(&inherited)
                        .map_err(|invalid| invalid.prepend(self.member.id()))?;
                }
                (&definition.options, definition.mode(), Readiness::Manual)
            }
        };
        resolve_common(options, defaults, mode, default_readiness)
            .map_err(|invalid| invalid.prepend(self.member.id()))
    }
}

/// Erased declaration storage before inherited defaults and identities are lowered.
pub(crate) struct BuilderCore {
    pub(crate) root: Arc<ScopeCell>,
    pub(crate) config: ScopeConfig,
    pub(crate) slots: Vec<Arc<SlotCell>>,
    ids: HashMap<ChildId, Arc<SlotCell>>,
    armed: bool,
}

impl BuilderCore {
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
            ids: HashMap::new(),
            armed: true,
        }
    }

    pub(crate) fn reserve(
        &mut self,
        id: impl Into<ChildId>,
        scope: Option<ScopeFlavor>,
    ) -> Result<Arc<SlotCell>, ReserveError> {
        let id = checked_id(id)?;
        if self.ids.contains_key(&id) {
            return Err(ReserveError::DuplicateId(id));
        }
        let slot = mint_reserved_slot(&self.root, &id, scope)?;
        self.ids.insert(id, Arc::clone(&slot));
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
            .map(|slot| vec![slot.member.id().clone()])
            .collect();
        if !undefined.is_empty() {
            let disposal = self.begin_failed_disposal();
            return Err(LowerError::Undefined {
                paths: undefined,
                disposal,
            });
        }
        let (defaults, resolved) = match self.validate_policies(&inherited) {
            Ok(resolved) => resolved,
            Err(invalid) => {
                let disposal = self.begin_failed_disposal();
                return Err(LowerError::InvalidPolicy { invalid, disposal });
            }
        };
        if !Arc::ptr_eq(&root, &self.root) {
            let mut identity = root
                .child_identity
                .lock()
                .expect("scope identity mutex poisoned");
            for slot in &self.slots {
                let Some(rebased) =
                    identity.adopt_or_mint_membership(slot.member.id(), slot.member.membership())
                else {
                    let id = slot.member.id().clone();
                    drop(identity);
                    let disposal = self.begin_failed_disposal();
                    return Err(LowerError::IdentityExhausted { id, disposal });
                };
                if let Some(identity) = rebased {
                    slot.member.rebase_membership(identity);
                }
            }
        }
        root.set_observation_config(self.config.strategy, self.config.intensity);
        let mut children = Vec::with_capacity(self.slots.len());
        debug_assert_eq!(
            self.slots.len(),
            resolved.len(),
            "policy resolution is one entry per slot, in slot order"
        );
        for (slot, resolved) in self.slots.iter().zip(resolved) {
            let resolved = resolved.expect("validated slot must have resolved options");
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

    fn validate_policies(
        &self,
        inherited: &ResolvedDefaults,
    ) -> Result<(ResolvedDefaults, Vec<Option<ResolvedCommonOptions>>), InvalidPolicy> {
        self.config
            .intensity
            .validate()
            .map_err(|error| InvalidPolicy::new(PolicyField::Intensity, error))?;
        let defaults = inherited.overlay(&self.config.defaults)?;
        // One entry per slot, in slot order. An undefined slot resolves to
        // `None` rather than being skipped, so this vector can never shift a
        // later child onto another child's options.
        let mut resolved = Vec::with_capacity(self.slots.len());
        for slot in &self.slots {
            resolved.push(slot.resolve_policy(&defaults)?);
        }
        Ok((defaults, resolved))
    }

    fn terminalize(&self) {
        for slot in &self.slots {
            slot.terminalize_never_started(&self.root);
        }
        self.root.terminalize_never_started();
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
    pub(crate) config: ScopeConfig,
    pub(crate) defaults: ResolvedDefaults,
    pub(crate) children: Vec<ChildPlan>,
    terminality: Option<ScopePlanTerminality>,
}

struct ScopePlanTerminality;

pub(crate) struct RuntimeScopePlan {
    pub(crate) root: Arc<ScopeCell>,
    pub(crate) config: ScopeConfig,
    pub(crate) defaults: ResolvedDefaults,
    pub(crate) children: Vec<ChildPlan>,
    terminality: Option<ScopePlanTerminality>,
}

fn terminalize_plan(
    root: &ScopeCell,
    children: &[ChildPlan],
    terminality: &mut Option<ScopePlanTerminality>,
) {
    if terminality.take().is_some() {
        for child in children {
            child.slot.terminalize_never_started(root);
        }
        root.with_observation_gate(|txn| {
            // Lowering can publish the planned children before ScopeRuntime
            // takes ownership. The plan fallback commits residency withdrawal
            // and root closure as one root-scope observation.
            root.clear_residents_locked(txn);
            root.terminalize_never_started_locked(txn);
        });
    }
}

impl ScopePlan {
    /// Consumes declaration ownership and transfers its terminality obligation
    /// to the runtime plan. There is no independently mutable disarm bit: a
    /// caller either owns the declaration plan or has consumed it here.
    pub(crate) fn take_for_runtime(mut self) -> RuntimeScopePlan {
        RuntimeScopePlan {
            root: Arc::clone(&self.root),
            config: self.config.clone(),
            defaults: self.defaults.clone(),
            children: std::mem::take(&mut self.children),
            terminality: self.terminality.take(),
        }
    }
}

impl Drop for ScopePlan {
    fn drop(&mut self) {
        terminalize_plan(&self.root, &self.children, &mut self.terminality);
    }
}

impl RuntimeScopePlan {
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

impl Drop for RuntimeScopePlan {
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
        paths: Vec<Vec<ChildId>>,
        disposal: Latch,
    },
    IdentityExhausted {
        id: ChildId,
        disposal: Latch,
    },
    InvalidPolicy {
        invalid: InvalidPolicy,
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
    ChildId::validate(id.as_str().to_owned()).map_err(|error| match error {
        IdError::Empty => ReserveError::EmptyId,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use crate::{
        Backoff, DefaultsInheritance, ExitError, Readiness, RestartCondition, RestartPolicy,
        Retention, Shutdown, TaskOnceDef,
        definition::DefinitionSource,
        policy::{CommonOptions, ResolvedDefaults, ScopeFlavor},
        raw::RawConstruction,
        runtime::{self, Timeout},
        task::TaskDef,
    };

    use super::{
        BuilderCore, ChildConstruction, ChildPlan, ScopeConstruction, SlotCell,
        concrete_dynamic_slot, concrete_dynamic_slot_ref, erase_dynamic_slot,
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

    #[test]
    fn dynamic_slot_erasure_preserves_the_canonical_allocation() {
        let declaration = BuilderCore::new(ScopeFlavor::Dynamic);
        let slot = SlotCell::new(Arc::clone(&declaration.root.member), None);

        let erased = erase_dynamic_slot(Arc::clone(&slot));
        assert!(std::ptr::eq(
            concrete_dynamic_slot_ref(erased.as_ref()),
            slot.as_ref(),
        ));

        let restored = concrete_dynamic_slot(erased);
        assert!(Arc::ptr_eq(&restored, &slot));
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
            .expect("dynamic policy is valid")
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
            .expect("policy is valid")
            .expect("slot is defined")
            .readiness
    }

    fn raw_construction(options: CommonOptions) -> ChildConstruction {
        // Mirrors `RawDef::erase`: the effective mode is resolved against the
        // actor type's `RawActor::readiness` metadata (`Manual` here, distinct
        // from every plan-level kind default) before the construction is
        // erased.
        let readiness = options.readiness.unwrap_or(Readiness::Manual);
        ChildConstruction::Raw(RawConstruction {
            source: DefinitionSource::Restartable(Arc::new(|| {
                unreachable!("policy resolution never constructs the actor")
            })),
            options,
            readiness,
        })
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

        // A declared override wins over every per-kind default.
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
            Readiness::Immediate
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
            .expect("dynamic policy is valid")
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
            .expect("one-shot policy is valid")
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
