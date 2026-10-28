use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;
use tokio::time::{Instant, timeout_at};

use crate::{ConversationId, KitError, WorkspaceId};

#[derive(Clone, Default)]
pub(crate) struct HierarchyRuntime {
    inner: Arc<(Mutex<HierarchyStatus>, Notify)>,
}

#[derive(Default)]
struct HierarchyStatus {
    phase: HierarchyPhase,
    agent_work: usize,
    workspaces: BTreeMap<WorkspaceId, WorkspaceStatus>,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum HierarchyPhase {
    #[default]
    Open,
    Sealing,
    Closed,
}

impl HierarchyPhase {
    fn ensure_open(self, message: &'static str) -> Result<(), KitError> {
        (self == Self::Open)
            .then_some(())
            .ok_or_else(|| KitError::closed(message))
    }

    fn ensure_live(self) -> Result<(), KitError> {
        (self != Self::Closed)
            .then_some(())
            .ok_or_else(|| KitError::closed("agent runtime is closed"))
    }
}

#[derive(Default)]
struct WorkspaceStatus {
    phase: HierarchyPhase,
    revision: Option<u64>,
    epoch: u64,
    operations: usize,
    update_checks: usize,
    mutations: usize,
    claims: BTreeSet<Option<ConversationId>>,
    closed_conversations: BTreeSet<ConversationId>,
}

pub(crate) struct HierarchyClaim {
    runtime: HierarchyRuntime,
    workspace_id: WorkspaceId,
    conversation_id: Option<ConversationId>,
    revision: u64,
    epoch: u64,
}

pub(crate) struct HierarchyAdmission {
    state: HierarchyAdmissionState,
}

enum HierarchyAdmissionState {
    Unregistered(HierarchyAdmissionKind),
    Registered {
        runtime: HierarchyRuntime,
        kind: HierarchyAdmissionKind,
    },
    ReopensClosed {
        runtime: HierarchyRuntime,
        kind: HierarchyAdmissionKind,
    },
    Released,
}

enum HierarchyAdmissionKind {
    AgentWork,
    Work(WorkspaceId),
    WorkspaceReopen(WorkspaceId),
    Read(WorkspaceId),
    UpdateCheck(WorkspaceId),
    Mutation(WorkspaceId),
    ConversationClose(WorkspaceId, ConversationId),
}

impl HierarchyRuntime {
    pub(crate) fn claim_workspace(
        &self,
        id: WorkspaceId,
        revision: u64,
    ) -> Result<HierarchyClaim, KitError> {
        self.claim(id, None, revision)
    }

    pub(crate) fn claim_conversation(
        &self,
        id: ConversationId,
        workspace_id: WorkspaceId,
        revision: u64,
    ) -> Result<HierarchyClaim, KitError> {
        self.claim(workspace_id, Some(id), revision)
    }

    pub(crate) fn close_conversation(&self, workspace: &WorkspaceId, id: &ConversationId) {
        self.status()
            .workspace_mut(workspace)
            .closed_conversations
            .insert(id.clone());
        self.notify();
    }

    pub(crate) fn conversation_is_terminal(
        &self,
        workspace: &WorkspaceId,
        conversation: &ConversationId,
    ) -> bool {
        let status = self.status();
        status.phase == HierarchyPhase::Closed
            || status.workspaces.get(workspace).is_some_and(|state| {
                state.phase == HierarchyPhase::Closed
                    || state.closed_conversations.contains(conversation)
            })
    }

    pub(crate) fn invalidate_workspace(&self, id: &WorkspaceId) {
        self.status().workspace_mut(id).phase = HierarchyPhase::Closed;
        self.notify();
    }

    pub(crate) fn advance_workspace_revision(&self, id: &WorkspaceId, revision: u64) {
        let mut status = self.status();
        let workspace = status.workspace_mut(id);
        if workspace.revision.is_none_or(|current| current < revision) {
            workspace.revision = Some(revision);
            workspace.claims.clear();
        }
    }

    pub(crate) fn seal(&self) {
        let mut status = self.status();
        if status.phase == HierarchyPhase::Open {
            status.phase = HierarchyPhase::Sealing;
        }
        drop(status);
        self.notify();
    }

    pub(crate) async fn await_agent_quiescence(&self, deadline: Instant) -> Result<(), KitError> {
        self.await_quiescence(None, deadline).await
    }

    pub(crate) fn seal_workspace(&self, id: &WorkspaceId, revision: u64) -> Result<(), KitError> {
        let mut status = self.status();
        status.phase.ensure_open("agent runtime is closed")?;
        let workspace = status.workspace_mut(id);
        workspace.ensure_open(None)?;
        workspace.observe_revision(revision)?;
        workspace.phase = HierarchyPhase::Sealing;
        drop(status);
        self.notify();
        Ok(())
    }

    pub(crate) async fn await_workspace_quiescence(
        &self,
        id: &WorkspaceId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        self.await_quiescence(Some(id), deadline).await
    }

    async fn await_quiescence(
        &self,
        workspace_id: Option<&WorkspaceId>,
        deadline: Instant,
    ) -> Result<(), KitError> {
        loop {
            let mut changed = std::pin::pin!(self.inner.1.notified());
            changed.as_mut().enable();
            let quiescent = {
                let mut status = self.status();
                match workspace_id {
                    Some(id) => {
                        let workspace = status.workspace_mut(id);
                        if workspace.is_empty() {
                            workspace.phase = HierarchyPhase::Closed;
                            true
                        } else {
                            false
                        }
                    }
                    None if status.agent_work == 0
                        && status.workspaces.values().all(WorkspaceStatus::is_empty) =>
                    {
                        status.phase = HierarchyPhase::Closed;
                        true
                    }
                    None => false,
                }
            };
            if quiescent {
                self.notify();
                return Ok(());
            }
            if timeout_at(deadline, changed).await.is_err() {
                let message = if let Some(id) = workspace_id {
                    self.status().workspace_mut(id).phase = HierarchyPhase::Closed;
                    "workspace hierarchy did not quiesce before close"
                } else {
                    self.status().phase = HierarchyPhase::Closed;
                    "agent hierarchy did not quiesce before shutdown"
                };
                self.notify();
                return Err(KitError::timeout(message));
            }
        }
    }

    pub(crate) fn invalidate(&self) {
        self.status().phase = HierarchyPhase::Closed;
        self.notify();
    }

    pub(crate) fn admit_agent_work(&self) -> Result<HierarchyAdmission, KitError> {
        HierarchyAdmission::new(HierarchyAdmissionKind::AgentWork)
            .register(self)
            .map(|(admission, _)| admission)
    }

    pub(crate) fn admit_work(&self, id: &WorkspaceId) -> Result<HierarchyAdmission, KitError> {
        HierarchyAdmission::new(HierarchyAdmissionKind::Work(id.clone()))
            .register(self)
            .map(|(admission, _)| admission)
    }

    pub(crate) fn admit_workspace_reopen(
        &self,
        id: &WorkspaceId,
    ) -> Result<HierarchyAdmission, KitError> {
        HierarchyAdmission::new(HierarchyAdmissionKind::WorkspaceReopen(id.clone()))
            .register(self)
            .map(|(admission, _)| admission)
    }

    pub(crate) async fn admit_read(
        &self,
        id: &WorkspaceId,
    ) -> Result<HierarchyAdmission, KitError> {
        HierarchyAdmission::new(HierarchyAdmissionKind::Read(id.clone()))
            .register(self)?
            .0
            .wait_for_mutation(None)
            .await
    }

    pub(crate) fn admit_mutation(&self, id: &WorkspaceId) -> Result<HierarchyAdmission, KitError> {
        HierarchyAdmission::new(HierarchyAdmissionKind::Mutation(id.clone()))
            .register(self)
            .map(|(admission, _)| admission)
    }

    pub(crate) async fn admit_conversation_close(
        &self,
        workspace: &WorkspaceId,
        conversation: &ConversationId,
        deadline: Instant,
    ) -> Result<HierarchyAdmission, KitError> {
        HierarchyAdmission::new(HierarchyAdmissionKind::ConversationClose(
            workspace.clone(),
            conversation.clone(),
        ))
        .register(self)?
        .0
        .wait_for_mutation(Some(deadline))
        .await
    }

    pub(crate) fn admit_update_check(
        &self,
        id: &WorkspaceId,
    ) -> Result<(HierarchyAdmission, bool), KitError> {
        HierarchyAdmission::new(HierarchyAdmissionKind::UpdateCheck(id.clone())).register(self)
    }

    pub(crate) fn workspace_update_busy(&self, id: &WorkspaceId) -> Result<bool, KitError> {
        let status = self.status();
        status.phase.ensure_open("agent runtime is closed")?;
        let Some(workspace) = status.workspaces.get(id) else {
            return Ok(false);
        };
        workspace.ensure_open(None)?;
        Ok(workspace.blocks_update_check())
    }

    fn claim(
        &self,
        id: WorkspaceId,
        conversation: Option<ConversationId>,
        revision: u64,
    ) -> Result<HierarchyClaim, KitError> {
        let mut status = self.status();
        status.phase.ensure_live()?;
        let workspace = status.workspace_mut(&id);
        workspace.ensure_open(conversation.as_ref())?;
        workspace.observe_revision(revision)?;
        if !workspace.claims.insert(conversation.clone()) {
            return Err(KitError::busy("current durable handle is already live"));
        }
        let epoch = workspace.epoch;
        Ok(HierarchyClaim {
            runtime: self.clone(),
            workspace_id: id,
            conversation_id: conversation,
            revision,
            epoch,
        })
    }

    fn status(&self) -> MutexGuard<'_, HierarchyStatus> {
        self.inner
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn notify(&self) {
        self.inner.1.notify_waiters();
    }
}

impl HierarchyStatus {
    fn workspace_mut(&mut self, id: &WorkspaceId) -> &mut WorkspaceStatus {
        self.workspaces.entry(id.clone()).or_default()
    }
}

impl WorkspaceStatus {
    fn ensure_open(&self, conversation: Option<&ConversationId>) -> Result<(), KitError> {
        self.phase.ensure_open("workspace runtime is closed")?;
        if conversation.is_some_and(|id| self.closed_conversations.contains(id)) {
            Err(KitError::closed("conversation runtime is closed"))
        } else {
            Ok(())
        }
    }

    fn observe_revision(&mut self, revision: u64) -> Result<(), KitError> {
        if self.revision.is_some_and(|current| current != revision) {
            Err(KitError::conflict("workspace handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision)
                .in_phase(crate::error::ErrorPhase::Admission))
        } else {
            self.revision = Some(revision);
            Ok(())
        }
    }

    fn blocks_update_check(&self) -> bool {
        self.operations != 0 || self.mutations != 0 || self.update_checks > 1
    }

    fn blocks_mutation(&self) -> bool {
        self.operations != 0 || self.update_checks != 0
    }

    fn is_empty(&self) -> bool {
        self.operations == 0 && self.update_checks == 0 && self.mutations == 0
    }
}

impl HierarchyClaim {
    pub(crate) fn ensure_open(&self) -> Result<(), KitError> {
        let status = self.runtime.status();
        status.phase.ensure_live()?;
        let workspace = status
            .workspaces
            .get(&self.workspace_id)
            .ok_or_else(|| KitError::closed("workspace runtime is closed"))?;
        workspace.ensure_open(self.conversation_id.as_ref())?;
        if workspace.revision != Some(self.revision) {
            return Err(KitError::conflict("workspace handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision)
                .in_phase(crate::error::ErrorPhase::Admission));
        }
        if workspace.epoch != self.epoch {
            return Err(KitError::closed("workspace runtime is closed"));
        }
        Ok(())
    }
}

impl Drop for HierarchyClaim {
    fn drop(&mut self) {
        let mut status = self.runtime.status();
        if let Some(workspace) = status.workspaces.get_mut(&self.workspace_id)
            && workspace.revision == Some(self.revision)
            && workspace.epoch == self.epoch
        {
            workspace.claims.remove(&self.conversation_id);
        }
    }
}

impl HierarchyAdmission {
    fn new(kind: HierarchyAdmissionKind) -> Self {
        Self {
            state: HierarchyAdmissionState::Unregistered(kind),
        }
    }

    pub(crate) fn publish<T>(mut self, result: Result<T, KitError>) -> Result<T, KitError> {
        let state = std::mem::replace(&mut self.state, HierarchyAdmissionState::Released);
        let (runtime, kind, reopens_closed) = match state {
            HierarchyAdmissionState::Registered { runtime, kind } => (runtime, kind, false),
            HierarchyAdmissionState::ReopensClosed { runtime, kind } => (runtime, kind, true),
            HierarchyAdmissionState::Unregistered(_) | HierarchyAdmissionState::Released => {
                return Err(KitError::invalid_state(
                    "hierarchy admission was not registered",
                ));
            }
        };
        let target = kind.target();
        let mut status = runtime.status();
        let succeeded = result.is_ok();
        let committed = succeeded
            && matches!(
                &kind,
                HierarchyAdmissionKind::Mutation(_)
                    | HierarchyAdmissionKind::ConversationClose(_, _)
            );
        let workspace_closed = target.is_some_and(|(id, _)| {
            status
                .workspaces
                .get(id)
                .is_some_and(|workspace| workspace.phase == HierarchyPhase::Closed)
        });
        let accepts_closed_workspace = reopens_closed
            || matches!(
                &kind,
                HierarchyAdmissionKind::Read(_) | HierarchyAdmissionKind::Mutation(_)
            );
        let publishable = match (
            committed,
            status.phase,
            workspace_closed,
            reopens_closed,
            succeeded,
        ) {
            (false, HierarchyPhase::Closed, _, _, _) => {
                Err(KitError::closed("agent runtime is closed"))
            }
            (false, _, true, _, _) if !accepts_closed_workspace => {
                Err(KitError::closed("workspace runtime is closed"))
            }
            (false, _, true, true, true) => Err(KitError::invalid_state(
                "workspace reopen did not establish runtime ownership",
            )),
            _ => Ok(()),
        };
        kind.release(&mut status);
        drop(status);
        runtime.notify();
        match (publishable, result) {
            (Ok(()), result) => result,
            (Err(error), Ok(_)) => Err(error.at_phase(crate::error::ErrorPhase::Publication)),
            (Err(error), Err(failure)) => Err(error
                .at_phase(crate::error::ErrorPhase::Publication)
                .with_supplementary(failure)),
        }
    }

    fn register(mut self, runtime: &HierarchyRuntime) -> Result<(Self, bool), KitError> {
        let kind = match std::mem::replace(&mut self.state, HierarchyAdmissionState::Released) {
            HierarchyAdmissionState::Unregistered(kind) => kind,
            HierarchyAdmissionState::Registered { .. }
            | HierarchyAdmissionState::ReopensClosed { .. }
            | HierarchyAdmissionState::Released => {
                return Err(KitError::invalid_state(
                    "hierarchy admission was already registered",
                ));
            }
        };
        let mut status = runtime.status();
        status.phase.ensure_open("agent runtime is closed")?;
        if let HierarchyAdmissionKind::WorkspaceReopen(id) = &kind {
            let id = id.clone();
            let workspace = status.workspace_mut(&id);
            let reopens_closed = match workspace.phase {
                HierarchyPhase::Open => {
                    if workspace.mutations != 0 {
                        return Err(KitError::busy("workspace mutation is in progress"));
                    }
                    false
                }
                HierarchyPhase::Sealing => {
                    return Err(KitError::closed("workspace runtime is closed"));
                }
                HierarchyPhase::Closed
                    if workspace.claims.contains(&None) || !workspace.is_empty() =>
                {
                    return Err(KitError::busy("workspace has admitted work"));
                }
                HierarchyPhase::Closed => true,
            };
            workspace.operations = workspace
                .operations
                .checked_add(1)
                .ok_or_else(|| KitError::invalid_state("workspace admission count is exhausted"))?;
            self.state = if reopens_closed {
                HierarchyAdmissionState::ReopensClosed {
                    runtime: runtime.clone(),
                    kind,
                }
            } else {
                HierarchyAdmissionState::Registered {
                    runtime: runtime.clone(),
                    kind,
                }
            };
            return Ok((self, false));
        }
        let Some((id, conversation)) = kind.target() else {
            status.agent_work = status
                .agent_work
                .checked_add(1)
                .ok_or_else(|| KitError::invalid_state("agent admission count is exhausted"))?;
            self.state = HierarchyAdmissionState::Registered {
                runtime: runtime.clone(),
                kind,
            };
            return Ok((self, false));
        };
        let workspace = status.workspace_mut(id);
        let accepts_closed_workspace = workspace.phase == HierarchyPhase::Closed
            && matches!(
                &kind,
                HierarchyAdmissionKind::Read(_) | HierarchyAdmissionKind::Mutation(_)
            );
        if !accepts_closed_workspace {
            workspace.ensure_open(conversation)?;
        }
        match &kind {
            HierarchyAdmissionKind::Work(_) if workspace.mutations != 0 => {
                return Err(KitError::busy("workspace mutation is in progress"));
            }
            HierarchyAdmissionKind::Mutation(_) if workspace.blocks_mutation() => {
                return Err(KitError::busy("workspace has admitted work"));
            }
            _ => {}
        }
        let counter = kind.counter_mut(workspace);
        *counter = counter
            .checked_add(1)
            .ok_or_else(|| KitError::invalid_state("workspace admission count is exhausted"))?;
        let busy = matches!(&kind, HierarchyAdmissionKind::UpdateCheck(_))
            && workspace.blocks_update_check();
        self.state = HierarchyAdmissionState::Registered {
            runtime: runtime.clone(),
            kind,
        };
        Ok((self, busy))
    }

    pub(crate) fn claim_workspace(&self, revision: u64) -> Result<HierarchyClaim, KitError> {
        let (runtime, kind, reopens_closed) = self.registered()?;
        let HierarchyAdmissionKind::WorkspaceReopen(id) = kind else {
            return Err(KitError::invalid_state(
                "workspace claim lacks reopen admission",
            ));
        };
        let mut status = runtime.status();
        status.phase.ensure_open("agent runtime is closed")?;
        let workspace = status.workspace_mut(id);
        if reopens_closed {
            if workspace.phase != HierarchyPhase::Closed {
                return Err(KitError::invalid_state(
                    "workspace reopen phase changed before publication",
                ));
            }
            workspace.observe_revision(revision)?;
            let epoch = workspace
                .epoch
                .checked_add(1)
                .ok_or_else(|| KitError::invalid_state("workspace runtime epoch is exhausted"))?;
            workspace.epoch = epoch;
            workspace.claims.clear();
            workspace.phase = HierarchyPhase::Open;
        } else {
            workspace.ensure_open(None)?;
            workspace.observe_revision(revision)?;
        }
        if !workspace.claims.insert(None) {
            return Err(KitError::busy("current durable handle is already live"));
        }
        Ok(HierarchyClaim {
            runtime: runtime.clone(),
            workspace_id: id.clone(),
            conversation_id: None,
            revision,
            epoch: workspace.epoch,
        })
    }

    pub(crate) fn mark_conversation_terminal(
        &self,
        expected_revision: u64,
    ) -> Result<(), KitError> {
        let (runtime, kind, _) = self.registered()?;
        let HierarchyAdmissionKind::ConversationClose(workspace_id, conversation) = kind else {
            return Err(KitError::invalid_state(
                "terminal conversation marker lacks close ownership",
            ));
        };
        let mut status = runtime.status();
        status.phase.ensure_open("agent runtime is closed")?;
        let workspace = status
            .workspaces
            .get_mut(workspace_id)
            .ok_or_else(|| KitError::closed("workspace runtime is closed"))?;
        workspace.ensure_open(Some(conversation))?;
        if workspace.revision != Some(expected_revision) {
            return Err(KitError::conflict("workspace handle is stale")
                .with_detail(crate::error::ErrorDetail::StaleRevision)
                .in_phase(crate::error::ErrorPhase::Admission));
        }
        workspace.closed_conversations.insert(conversation.clone());
        drop(status);
        runtime.notify();
        Ok(())
    }

    async fn wait_for_mutation(self, deadline: Option<Instant>) -> Result<Self, KitError> {
        let (runtime, _, _) = self.registered()?;
        let runtime = runtime.clone();
        loop {
            let mut changed = std::pin::pin!(runtime.inner.1.notified());
            changed.as_mut().enable();
            let waiting = {
                let status = runtime.status();
                status.phase.ensure_open("agent runtime is closed")?;
                let (_, kind, _) = self.registered()?;
                let (id, conversation) = kind
                    .target()
                    .ok_or_else(|| KitError::invalid_state("admission has no workspace"))?;
                let workspace = &status.workspaces[id];
                if workspace.phase != HierarchyPhase::Closed
                    || !matches!(kind, HierarchyAdmissionKind::Read(_))
                {
                    workspace.ensure_open(conversation)?;
                }
                workspace.mutations != 0
            };
            if !waiting {
                return Ok(self);
            }
            match deadline {
                Some(deadline) => timeout_at(deadline, changed).await.map_err(|_| {
                    KitError::timeout("conversation close waited for a workspace mutation")
                })?,
                None => changed.await,
            }
        }
    }

    fn registered(&self) -> Result<(&HierarchyRuntime, &HierarchyAdmissionKind, bool), KitError> {
        match &self.state {
            HierarchyAdmissionState::Registered { runtime, kind } => Ok((runtime, kind, false)),
            HierarchyAdmissionState::ReopensClosed { runtime, kind } => Ok((runtime, kind, true)),
            HierarchyAdmissionState::Unregistered(_) | HierarchyAdmissionState::Released => Err(
                KitError::invalid_state("hierarchy admission was not registered"),
            ),
        }
    }
}

impl HierarchyAdmissionKind {
    fn target(&self) -> Option<(&WorkspaceId, Option<&ConversationId>)> {
        match self {
            Self::AgentWork => None,
            Self::Work(id)
            | Self::WorkspaceReopen(id)
            | Self::Read(id)
            | Self::UpdateCheck(id)
            | Self::Mutation(id) => Some((id, None)),
            Self::ConversationClose(id, conversation) => Some((id, Some(conversation))),
        }
    }

    fn counter_mut<'a>(&self, workspace: &'a mut WorkspaceStatus) -> &'a mut usize {
        match self {
            HierarchyAdmissionKind::AgentWork => {
                unreachable!("agent admission has no workspace counter")
            }
            HierarchyAdmissionKind::Work(_)
            | HierarchyAdmissionKind::WorkspaceReopen(_)
            | HierarchyAdmissionKind::Read(_)
            | HierarchyAdmissionKind::ConversationClose(_, _) => &mut workspace.operations,
            HierarchyAdmissionKind::UpdateCheck(_) => &mut workspace.update_checks,
            HierarchyAdmissionKind::Mutation(_) => &mut workspace.mutations,
        }
    }

    fn release(&self, status: &mut HierarchyStatus) {
        let Some((id, _)) = self.target() else {
            debug_assert_ne!(status.agent_work, 0);
            status.agent_work = status.agent_work.saturating_sub(1);
            return;
        };
        let discard = {
            let workspace = status
                .workspaces
                .get_mut(id)
                .expect("admission has workspace");
            let counter = self.counter_mut(workspace);
            debug_assert_ne!(*counter, 0);
            *counter = counter.saturating_sub(1);
            workspace.phase == HierarchyPhase::Open
                && workspace.revision.is_none()
                && workspace.is_empty()
                && workspace.closed_conversations.is_empty()
        };
        if discard {
            status.workspaces.remove(id);
        }
    }
}

impl Drop for HierarchyAdmission {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, HierarchyAdmissionState::Released);
        let (runtime, kind) = match state {
            HierarchyAdmissionState::Registered { runtime, kind }
            | HierarchyAdmissionState::ReopensClosed { runtime, kind } => (runtime, kind),
            HierarchyAdmissionState::Unregistered(_) | HierarchyAdmissionState::Released => return,
        };
        let mut status = runtime.status();
        kind.release(&mut status);
        drop(status);
        runtime.notify();
    }
}
