//! Declaration lowering and its owned construction plan.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    ChildId, DefaultsInheritance, Exit, Intensity, Readiness, ScopeDefaults, Strategy,
    cells::{MemberCell, ScopeCell},
    definition::DefinitionSource,
    identity::{IdError, ScopeIdentity},
    policy::{
        CommonOptions, InvalidPolicy, PolicyField, ResolvedCommonOptions, ResolvedDefaults,
        ScopeFlavor, resolve_common,
    },
    raw::RawConstruction,
    runtime::{self, Isolated, Latch},
    task::{OnceTask, TaskDef},
};

/// A child reservation or dynamic admission error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ReserveError {
    /// No ambient supported async runtime exists.
    #[error("no ambient Tokio runtime is available")]
    NoRuntime,
    /// The child id was empty.
    #[error("child id must not be empty")]
    EmptyId,
    /// A resident membership already occupies the id.
    #[error("child id `{0}` is already resident")]
    DuplicateId(ChildId),
    /// A same-id membership is currently being removed.
    #[error("child id `{0}` is being removed")]
    RemovalInProgress(ChildId),
    /// The target dynamic scope is not admitting.
    #[error("scope is not admitting: {0:?}")]
    NotAdmitting(NotAdmittingCause),
    /// The scope can mint no further membership identities.
    #[error("membership identity space is exhausted")]
    IdentityExhausted,
    /// A public policy representation contained an invalid literal value.
    #[error(transparent)]
    InvalidPolicy(InvalidPolicy),
}

/// Exact reason an admission operation could not proceed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NotAdmittingCause {
    /// The scope membership is terminal.
    Terminal,
    /// The live scope incarnation is draining.
    Draining,
    /// The dynamic root is parked after startup failure.
    StartupFailed,
    /// No scope incarnation is currently live.
    NoLiveIncarnation,
    /// This operation's reservation ended before admission.
    ReservationEnded,
}

/// Outcome of an idempotent dynamic removal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveOutcome {
    /// A reservation or resident membership was removed.
    Removed,
    /// No matching membership remained to remove.
    AlreadyAbsent,
}

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

    /// Publishes the canonical never-started terminal state for this slot.
    ///
    /// Member terminality closes any mailbox before the nested scope closes
    /// its observation surfaces. Every static and dynamic path uses this
    /// method so those edges cannot drift independently.
    pub(crate) fn terminalize_never_started(&self) {
        self.member.terminalize(Exit::never_started());
        if let Some(scope) = &self.scope {
            scope.terminalize_never_started();
        }
    }

    /// Claims an unlowered definition and publishes never-started terminality.
    pub(crate) fn take_never_started(&self) -> Option<Isolated<ChildConstruction>> {
        let definition = self.take_definition().ok().flatten();
        self.terminalize_never_started();
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
        let (options, one_shot) = match definition.get() {
            ChildConstruction::Raw(definition) => (&definition.options, definition.one_shot()),
            ChildConstruction::Task(definition) => (&definition.options, false),
            ChildConstruction::TaskOnce(definition) => (&definition.options, true),
            ChildConstruction::Scope(definition) => {
                if let DefinitionSource::OneShot(tree) = &definition.source {
                    let inherited = match definition.defaults {
                        DefaultsInheritance::Inherit => defaults.clone(),
                        DefaultsInheritance::Reset => ResolvedDefaults::default(),
                    };
                    tree.validate_policies(&inherited)
                        .map_err(|invalid| invalid.prepend(self.member.id()))?;
                }
                (&definition.options, definition.one_shot())
            }
        };
        resolve_common(options, defaults, one_shot, Readiness::Immediate)
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
                let Some(membership) =
                    identity.adopt_or_mint_membership(slot.member.id(), slot.member.membership())
                else {
                    let id = slot.member.id().clone();
                    drop(identity);
                    let disposal = self.begin_failed_disposal();
                    return Err(LowerError::IdentityExhausted { id, disposal });
                };
                if membership != slot.member.membership() {
                    slot.member.rebase_membership(membership);
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
            armed: true,
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
            slot.terminalize_never_started();
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
    pub(crate) armed: bool,
}

impl Drop for ScopePlan {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for child in &self.children {
            child.slot.terminalize_never_started();
        }
        // Lowering can publish the planned children before ScopeRuntime takes
        // ownership. If construction then unwinds, the plan fallback also
        // owns those residencies and their matching Removed edges.
        self.root.clear_residents();
        self.root.terminalize_never_started();
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
    pub(crate) fn one_shot(&self) -> bool {
        self.source.is_one_shot()
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
        Backoff, ExitError, Readiness, RestartCondition, RestartPolicy, Retention, Shutdown,
        TaskOnceDef,
        policy::{ResolvedDefaults, ScopeFlavor},
        runtime::{self, Timeout},
        task::TaskDef,
    };

    use super::{BuilderCore, ChildConstruction, ChildPlan};

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
