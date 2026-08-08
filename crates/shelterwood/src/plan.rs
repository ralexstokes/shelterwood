//! Declaration lowering and its owned construction plan.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    ChildId, DefaultsInheritance, Exit, Intensity, Readiness, ScopeDefaults, Strategy,
    cells::{MemberCell, ScopeCell},
    definition::DefinitionSource,
    identity::ScopeIdentity,
    policy::{
        CommonOptions, IdError, ResolvedCommonOptions, ResolvedDefaults, ScopeFlavor,
        resolve_common,
    },
    raw::RawConstruction,
    runtime::{self, Isolated, Latch},
    task::{OnceTask, TaskDef},
};

/// A declaration-time reservation error.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum ReserveError {
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

enum DefinitionState {
    Undefined,
    Defined(Isolated<ChildConstruction>),
    Lowered,
}

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

    pub(crate) fn take_definition(&self) -> Option<Isolated<ChildConstruction>> {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        match std::mem::replace(&mut *state, DefinitionState::Lowered) {
            DefinitionState::Defined(definition) => Some(definition),
            DefinitionState::Undefined => {
                *state = DefinitionState::Undefined;
                None
            }
            DefinitionState::Lowered => panic!("a tree was lowered more than once"),
        }
    }

    pub(crate) fn take_defined(&self) -> Option<Isolated<ChildConstruction>> {
        let mut state = self.definition.lock().expect("definition mutex poisoned");
        match std::mem::replace(&mut *state, DefinitionState::Lowered) {
            DefinitionState::Defined(definition) => Some(definition),
            DefinitionState::Undefined => {
                *state = DefinitionState::Undefined;
                None
            }
            DefinitionState::Lowered => None,
        }
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
            .filter_map(|slot| slot.take_defined())
            .filter_map(|mut definition| definition.take())
            .collect();
        runtime::dispose_all(definitions)
    }

    pub(crate) fn new(flavor: ScopeFlavor) -> Self {
        let root_id = ChildId::from("$root");
        let mut root_identity =
            ScopeIdentity::new().expect("global scope identity space exhausted");
        let membership = root_identity
            .mint_membership(&root_id)
            .expect("fresh scope identity must mint its root membership");
        let member = MemberCell::new(root_id, membership);
        let child_identity = ScopeIdentity::new().expect("global scope identity space exhausted");
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
        let membership = self
            .root
            .child_identity
            .lock()
            .expect("scope identity mutex poisoned")
            .mint_membership(&id)
            .ok_or(ReserveError::IdentityExhausted)?;
        let member = MemberCell::new(id.clone(), membership);
        let scope = scope.map(|flavor| {
            let identity = ScopeIdentity::new().expect("global scope identity space exhausted");
            ScopeCell::new(Arc::clone(&member), flavor, identity)
        });
        let slot = SlotCell::new(member, scope);
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
        let defaults = inherited.overlay(&self.config.defaults);
        let mut children = Vec::with_capacity(self.slots.len());
        for slot in &self.slots {
            let definition = slot
                .take_definition()
                .expect("validated slot must have a definition");
            let (options, one_shot) = match definition.get() {
                ChildConstruction::Raw(definition) => (&definition.options, definition.one_shot()),
                ChildConstruction::Task(definition) => (&definition.options, false),
                ChildConstruction::TaskOnce(definition) => (&definition.options, true),
                ChildConstruction::Scope(definition) => {
                    (&definition.options, definition.one_shot())
                }
            };
            let resolved = resolve_common(options, &defaults, one_shot, Readiness::Immediate);
            slot.member.set_options(resolved.clone());
            children.push(ChildPlan {
                slot: Arc::clone(slot),
                construction: definition,
                options: resolved,
            });
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

    fn terminalize(&self) {
        for slot in &self.slots {
            slot.member.terminalize(Exit::never_started());
            if let Some(scope) = &slot.scope {
                scope.terminalize_never_started();
            }
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
            child.slot.member.terminalize(Exit::never_started());
            if let Some(scope) = &child.slot.scope {
                scope.terminalize_never_started();
            }
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
