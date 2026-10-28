use crate::error::{ErrorDetail, ErrorKind, ErrorPhase};
use tools_kit::ToolsetBackend;
pub(crate) mod wire;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use storage_kit::{StateStoreBackend, StateTransactionBackend};
use tokio::task::{AbortHandle, Id};

use tools_kit::ToolHandlerBackend;
use tools_kit::declaration::ToolName;
use tools_kit::execution::{
    ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult,
};
use tools_kit::interaction::{
    ToolInteractionId, ToolInteractionKind, ToolInteractionRequest, ToolInteractionResponse,
};

use crate::backends::codex::adapters::conversation::CodexWorkspacePolicy;
use crate::core::notification_store::{
    NotificationSettlementError, NotificationStore, ResolutionDisposition, ResolutionSettlement,
};
use crate::{
    AgentId, AgentStorage, ConversationId, ConversationNotificationKind, KitError, NotificationId,
    NotificationResolution, ToolInfo, WorkspaceContext, WorkspaceId,
};

const CODEX_TOOL_REGISTRATION_NAMESPACE: &str = "agents_kit_codex_tool_registrations_v1";
const CODEX_TOOL_REGISTRATION_VERSION: u32 = 1;
const CODEX_TOOL_INTERACTION_NAMESPACE: &str = "agents_kit_codex_tool_interactions_v1";
const CODEX_TOOL_INTERACTION_VERSION: u32 = 1;

pub(crate) struct CodexTools<S: StateStoreBackend, T: ToolsetBackend> {
    inner: Arc<CodexToolsInner<S, T>>,
}

struct CodexToolsInner<S: StateStoreBackend, T: ToolsetBackend> {
    agent_id: AgentId,
    storage: AgentStorage<S>,
    notifications: NotificationStore<S>,
    toolsets: Mutex<Option<Vec<Arc<T>>>>,
    routes: BTreeMap<ToolName, usize>,
    specs: Vec<CodexDynamicToolSpec>,
    work_limit: usize,
    lifecycle_timeout: std::time::Duration,
    pending: Mutex<BTreeMap<NotificationId, CodexPendingToolInteraction>>,
    state: Mutex<CodexToolState>,
    generation_cancellation: tokio::sync::Mutex<()>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct CodexToolState {
    closing: bool,
    closing_workspaces: BTreeSet<WorkspaceId>,
    closing_conversations: BTreeSet<ConversationId>,
    generation_fences: BTreeMap<CodexToolGeneration, CodexToolGenerationFence>,
    tasks: BTreeMap<Id, CodexManagedWork>,
    publications: BTreeMap<CodexInteractionOwner, usize>,
    responses: BTreeMap<CodexToolGeneration, usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodexToolGenerationFence {
    Draining,
    Reclaimable,
}

struct CodexToolPublication<S: StateStoreBackend, T: ToolsetBackend> {
    inner: Arc<CodexToolsInner<S, T>>,
    owner: CodexInteractionOwner,
}

pub(crate) struct CodexResponseAdmission<S: StateStoreBackend, T: ToolsetBackend> {
    inner: Arc<CodexToolsInner<S, T>>,
    generation: CodexToolGeneration,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CodexToolGeneration {
    thread_id: String,
    turn_id: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CodexInteractionOwner {
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    generation: CodexToolGeneration,
}

struct CodexManagedWork {
    generation: CodexToolGeneration,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    abort: AbortHandle,
}

impl CodexToolGeneration {
    pub(crate) fn new(thread_id: &str, turn_id: &str) -> Self {
        Self {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
        }
    }
}

impl CodexInteractionOwner {
    fn new(
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
        generation: CodexToolGeneration,
    ) -> Self {
        Self {
            workspace_id: workspace_id.clone(),
            conversation_id: conversation_id.clone(),
            generation,
        }
    }
}

impl CodexToolState {
    fn generation_is_fenced(&self, generation: &CodexToolGeneration) -> bool {
        self.generation_fences.contains_key(generation)
    }

    fn begin_generation_drain(&mut self, generation: CodexToolGeneration) {
        self.generation_fences
            .insert(generation, CodexToolGenerationFence::Draining);
    }

    fn mark_generation_reclaimable(&mut self, generation: &CodexToolGeneration) {
        if let Some(fence) = self.generation_fences.get_mut(generation) {
            *fence = CodexToolGenerationFence::Reclaimable;
        }
    }

    fn reclaim_generation(
        &mut self,
        generation: &CodexToolGeneration,
        has_pending_interaction: bool,
    ) -> bool {
        let reclaimable = matches!(
            self.generation_fences.get(generation),
            Some(CodexToolGenerationFence::Reclaimable)
        ) && !has_pending_interaction
            && !self
                .tasks
                .values()
                .any(|task| &task.generation == generation)
            && !self.responses.contains_key(generation)
            && !self
                .publications
                .keys()
                .any(|owner| &owner.generation == generation);
        if reclaimable {
            self.generation_fences.remove(generation);
        }
        reclaimable
    }
}

impl<S: StateStoreBackend, T: ToolsetBackend> Drop for CodexToolPublication<S, T> {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let remove = match state.publications.get_mut(&self.owner) {
            Some(publications) => match publications.checked_sub(1) {
                Some(remaining) => {
                    *publications = remaining;
                    remaining == 0
                }
                None => {
                    debug_assert!(false, "tool publication count must be positive");
                    false
                }
            },
            None => {
                debug_assert!(false, "tool publication must retain its owner");
                false
            }
        };
        if remove {
            state.publications.remove(&self.owner);
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

impl<S: StateStoreBackend, T: ToolsetBackend> Drop for CodexResponseAdmission<S, T> {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let remove = match state.responses.get_mut(&self.generation) {
            Some(responses) => match responses.checked_sub(1) {
                Some(remaining) => {
                    *responses = remaining;
                    remaining == 0
                }
                None => {
                    debug_assert!(false, "tool response count must be positive");
                    false
                }
            },
            None => {
                debug_assert!(false, "tool response permit must retain its generation");
                false
            }
        };
        if remove {
            state.responses.remove(&self.generation);
        }
        drop(state);
        self.inner.changed.notify_waiters();
    }
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CodexToolRegistrationRecord {
    version: u32,
    agent_id: AgentId,
    tools: Vec<CodexManagedToolDescriptor>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CodexManagedToolDescriptor {
    name: String,
    description: Option<String>,
    input_schema: serde_json::Value,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CodexToolInteractionRecord {
    version: u32,
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
    notification_id: NotificationId,
}

impl CodexToolInteractionRecord {
    const MAX_PAGES: usize = 1_024;

    fn new(
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        notification_id: NotificationId,
    ) -> Self {
        Self {
            version: CODEX_TOOL_INTERACTION_VERSION,
            agent_id,
            workspace_id,
            conversation_id,
            notification_id,
        }
    }

    async fn put_in_transaction(
        &self,
        transaction: &mut impl StateTransactionBackend,
    ) -> Result<(), KitError> {
        let value = serde_json::to_value(self)
            .map_err(|_| KitError::invalid_state("tool interaction record could not be encoded"))?;
        transaction
            .put(storage_kit::state::record::StateRecord::new(
                Self::key(&self.agent_id, &self.notification_id)?,
                value,
            ))
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("tool interaction record could not be stored")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                }
                error => {
                    KitError::from_state_store(error, "tool interaction record could not be stored")
                        .in_phase(ErrorPhase::StateWrite)
                }
            })
    }

    fn key(
        agent_id: &AgentId,
        notification_id: &NotificationId,
    ) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(
            CODEX_TOOL_INTERACTION_NAMESPACE,
            format!("{}:{}", agent_id.as_str(), notification_id.as_str()),
        )
        .map_err(|_| KitError::invalid_state("tool interaction record key is invalid"))
    }

    fn decode(
        snapshot: storage_kit::state::record::StateRecordSnapshot,
        agent_id: &AgentId,
    ) -> Result<Self, KitError> {
        let key = snapshot.record.key.clone();
        let record: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| KitError::invalid_state("stored tool interaction is malformed"))?;
        if record.version != CODEX_TOOL_INTERACTION_VERSION || &record.agent_id != agent_id {
            return Err(KitError::invalid_state(
                "stored tool interaction ownership is contradictory",
            ));
        }
        if key != Self::key(agent_id, &record.notification_id)? {
            return Err(KitError::invalid_state(
                "stored tool interaction key is contradictory",
            ));
        }
        Ok(record)
    }

    async fn for_agent<S: StateStoreBackend>(
        storage: &AgentStorage<S>,
        agent_id: &AgentId,
    ) -> Result<Vec<(storage_kit::state::record::StateKey, Self)>, KitError> {
        let prefix =
            storage_kit::state::record::StateKeyPrefix::new(format!("{}:", agent_id.as_str()))
                .map_err(|_| KitError::invalid_state("tool interaction prefix is invalid"))?;
        let store = storage.store();
        let mut cursor = None;
        let mut records = Vec::new();
        for _ in 0..Self::MAX_PAGES {
            let mut request =
                storage_kit::state::record::StateListRequest::new(CODEX_TOOL_INTERACTION_NAMESPACE)
                    .map_err(|_| {
                        KitError::invalid_state("tool interaction inventory request is invalid")
                    })?
                    .with_key_prefix(prefix.clone())
                    .with_limit(storage_kit::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| {
                        KitError::invalid_state("tool interaction inventory limit is invalid")
                    })?;
            if let Some(next) = cursor.take() {
                request = request.start_after(next).map_err(|_| {
                    KitError::invalid_state("tool interaction inventory cursor is invalid")
                })?;
            }
            let page = store.list(request).await.map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("tool interaction inventory could not be loaded")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateRead)
                }
                error => KitError::from_state_store(
                    error,
                    "tool interaction inventory could not be loaded",
                )
                .in_phase(ErrorPhase::StateRead),
            })?;
            for snapshot in page.records {
                let key = snapshot.record.key.clone();
                records.push((key, Self::decode(snapshot, agent_id)?));
            }
            let Some(next) = page.next_start_after else {
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(KitError::invalid_state(
            "tool interaction inventory exceeds its private page bound",
        ))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CodexDynamicToolCall {
    call_id: String,
    thread_id: String,
    turn_id: String,
    namespace: Option<String>,
    tool: String,
    arguments: serde_json::Value,
}

#[derive(Clone, Debug)]
pub(crate) struct CodexDynamicToolCallResponse {
    content_items: Vec<serde_json::Value>,
    success: bool,
}

#[derive(Clone, Debug)]
pub(super) struct CodexDynamicToolSpec {
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

struct CodexPendingToolInteraction {
    owner: CodexInteractionOwner,
    request: ToolInteractionRequest,
    response: tokio::sync::oneshot::Sender<ToolInteractionResponse>,
}

impl<S: StateStoreBackend, T: ToolsetBackend> Clone for CodexTools<S, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: StateStoreBackend, T: ToolsetBackend> CodexTools<S, T> {
    pub(crate) async fn validate_registrations(toolsets: &[Arc<T>]) -> Result<(), KitError> {
        CodexToolRegistrationRecord::describe(toolsets)
            .await
            .map(|_| ())
    }

    pub(crate) async fn shutdown_registrations(
        mut toolsets: Vec<Arc<T>>,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        let mut first_error: Option<KitError> = None;
        while let Some(toolset) = toolsets.pop() {
            let result =
                tokio::time::timeout_at(deadline, async { toolset.shutdown().await }).await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(KitError::from(error)),
                Err(_) => Some(KitError::timeout(
                    "managed Toolset shutdown deadline elapsed",
                )),
            };
            if let Some(error) = error {
                first_error = Some(match first_error.take() {
                    Some(primary) => {
                        primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                    }
                    None => error,
                });
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn build(
        agent_id: AgentId,
        storage: AgentStorage<S>,
        notifications: NotificationStore<S>,
        toolsets: Vec<Arc<T>>,
        work_limit: usize,
        lifecycle_timeout: std::time::Duration,
        reopen: bool,
    ) -> Result<Self, KitError> {
        let (descriptors, specs, routes) = match CodexToolRegistrationRecord::describe(&toolsets)
            .await
        {
            Ok(registration) => registration,
            Err(error) => {
                let error = match Self::shutdown_registrations(
                    toolsets,
                    tokio::time::Instant::now() + lifecycle_timeout,
                )
                .await
                {
                    Ok(()) => error,
                    Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
                };
                return Err(error);
            }
        };
        let record = CodexToolRegistrationRecord {
            version: CODEX_TOOL_REGISTRATION_VERSION,
            agent_id: agent_id.clone(),
            tools: descriptors,
        };
        let persistence = if reopen {
            record.verify(&storage).await
        } else {
            record.create(&storage).await
        };
        if let Err(error) = persistence {
            let error = match Self::shutdown_registrations(
                toolsets,
                tokio::time::Instant::now() + lifecycle_timeout,
            )
            .await
            {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            };
            return Err(error);
        }
        let tools = Self {
            inner: Arc::new(CodexToolsInner {
                agent_id,
                notifications,
                storage,
                toolsets: Mutex::new(Some(toolsets)),
                routes,
                specs,
                work_limit,
                lifecycle_timeout,
                pending: Mutex::new(BTreeMap::new()),
                state: Mutex::new(CodexToolState::default()),
                generation_cancellation: tokio::sync::Mutex::new(()),
                changed: tokio::sync::Notify::new(),
            }),
        };
        if reopen && let Err(error) = tools.reconcile_lost_interactions().await {
            let error = match tools
                .shutdown(tokio::time::Instant::now() + lifecycle_timeout)
                .await
            {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            };
            return Err(error);
        }
        Ok(tools)
    }

    pub(crate) fn list(&self) -> Result<Vec<ToolInfo>, KitError> {
        let mut tools = self
            .inner
            .specs
            .iter()
            .map(|spec| {
                ToolInfo::new(
                    spec.name.clone(),
                    (!spec.description.is_empty()).then(|| spec.description.clone()),
                    Some(spec.input_schema.clone()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        tools.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(tools)
    }

    pub(crate) fn thread_declarations(&self) -> Result<Vec<serde_json::Value>, KitError> {
        let declarations = wire::CodexDynamicToolDeclarations::from_specs(&self.inner.specs)
            .map_err(|_| KitError::protocol("managed tool declarations are invalid"))?;
        declarations
            .into_values()
            .map_err(|_| KitError::protocol("managed tool declarations are invalid"))
    }

    pub(crate) fn workspace_policy(
        &self,
        context: &WorkspaceContext,
    ) -> Result<CodexWorkspacePolicy, KitError> {
        CodexWorkspacePolicy::new(context, !self.inner.specs.is_empty())
    }

    pub(crate) fn work_limit(&self) -> usize {
        self.inner.work_limit
    }

    pub(crate) fn cancellation_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.inner.lifecycle_timeout
    }

    pub(crate) fn register_task(
        &self,
        generation: CodexToolGeneration,
        workspace_id: WorkspaceId,
        conversation_id: ConversationId,
        abort: AbortHandle,
        start: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), KitError> {
        let id = abort.id();
        let mut state = self.state();
        if state.tasks.contains_key(&id) {
            abort.abort();
            return Err(KitError::invalid_state(
                "managed tool task identity was reused",
            ));
        }
        let cancel = state.closing
            || state.closing_workspaces.contains(&workspace_id)
            || state.closing_conversations.contains(&conversation_id)
            || state.generation_is_fenced(&generation);
        state.tasks.insert(
            id,
            CodexManagedWork {
                generation,
                workspace_id,
                conversation_id,
                abort: abort.clone(),
            },
        );
        if cancel {
            drop(state);
            drop(start);
            abort.abort();
        } else {
            let _ = start.send(());
            drop(state);
        }
        self.inner.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn response_admission(
        &self,
        id: Id,
    ) -> Result<Option<CodexResponseAdmission<S, T>>, KitError> {
        let mut state = self.state();
        let task = state
            .tasks
            .get(&id)
            .ok_or_else(|| KitError::invalid_state("managed tool task owner is missing"))?;
        let generation = task.generation.clone();
        let permitted = !state.closing
            && !state.generation_is_fenced(&generation)
            && !state.closing_workspaces.contains(&task.workspace_id)
            && !state.closing_conversations.contains(&task.conversation_id);
        if !permitted {
            return Ok(None);
        }
        let responses = state.responses.entry(generation.clone()).or_default();
        *responses = responses
            .checked_add(1)
            .ok_or_else(|| KitError::invalid_state("managed tool response count is exhausted"))?;
        Ok(Some(CodexResponseAdmission {
            inner: self.inner.clone(),
            generation,
        }))
    }

    pub(crate) fn generation_response_admission(
        &self,
        generation: &CodexToolGeneration,
    ) -> Result<Option<CodexResponseAdmission<S, T>>, KitError> {
        let mut state = self.state();
        if state.closing || state.generation_is_fenced(generation) {
            return Ok(None);
        }
        let responses = state.responses.entry(generation.clone()).or_default();
        *responses = responses
            .checked_add(1)
            .ok_or_else(|| KitError::invalid_state("managed tool response count is exhausted"))?;
        Ok(Some(CodexResponseAdmission {
            inner: self.inner.clone(),
            generation: generation.clone(),
        }))
    }

    pub(crate) fn task_cancellation_expected(&self, id: Id) -> bool {
        let state = self.state();
        state.tasks.get(&id).is_some_and(|task| {
            state.closing
                || state.generation_is_fenced(&task.generation)
                || state.closing_workspaces.contains(&task.workspace_id)
                || state.closing_conversations.contains(&task.conversation_id)
        })
    }

    pub(crate) fn finish_task(&self, id: Id) -> Result<(), KitError> {
        let generation = self
            .state()
            .tasks
            .remove(&id)
            .ok_or_else(|| KitError::invalid_state("managed tool task owner is missing"))?
            .generation;
        self.reclaim_generation(&generation);
        self.inner.changed.notify_waiters();
        Ok(())
    }

    pub(crate) async fn cancel_generation(
        &self,
        generation: CodexToolGeneration,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        let _ownership =
            tokio::time::timeout_at(deadline, self.inner.generation_cancellation.lock())
                .await
                .map_err(|_| {
                    KitError::timeout("managed tool cancellation ownership did not settle")
                })?;
        {
            let mut state = self.state();
            state.begin_generation_drain(generation.clone());
        }
        self.inner.changed.notify_waiters();
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if !self.state().responses.contains_key(&generation) {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "managed tool response did not settle before cancellation",
                ));
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if !self
                .state()
                .publications
                .keys()
                .any(|owner| owner.generation == generation)
            {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "managed tool publication did not settle before cancellation",
                ));
            }
        }
        let notifications = self
            .pending()
            .iter()
            .filter(|(_, pending)| pending.owner.generation == generation)
            .map(|(id, pending)| (pending.owner.conversation_id.clone(), id.clone()))
            .collect::<Vec<_>>();
        let mut first_error: Option<KitError> = None;
        for (conversation_id, notification_id) in notifications {
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool interaction cancellation did not settle",
                    ));
                }
                break;
            }
            if let Err(error) = self
                .cancel_interaction(&conversation_id, notification_id, deadline)
                .await
            {
                first_error = Some(match first_error.take() {
                    Some(primary) => {
                        primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                    }
                    None => error,
                });
            }
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool interaction cancellation did not settle",
                    ));
                }
                break;
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if !self
                .state()
                .tasks
                .values()
                .any(|task| task.generation == generation)
            {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                let aborts = self
                    .state()
                    .tasks
                    .values()
                    .filter(|task| task.generation == generation)
                    .map(|task| task.abort.clone())
                    .collect::<Vec<_>>();
                for abort in aborts {
                    abort.abort();
                }
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool task did not settle before cancellation",
                    ));
                }
                break;
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn release_generation(&self, generation: CodexToolGeneration) {
        let has_pending_interaction = self
            .pending()
            .values()
            .any(|pending| pending.owner.generation == generation);
        let mut state = self.state();
        state.mark_generation_reclaimable(&generation);
        state.reclaim_generation(&generation, has_pending_interaction);
        drop(state);
        self.inner.changed.notify_waiters();
    }

    pub(crate) async fn execute(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
        call: CodexDynamicToolCall,
    ) -> Result<CodexDynamicToolCallResponse, KitError> {
        let owner = CodexInteractionOwner::new(workspace_id, conversation_id, call.generation());
        let tool_name = ToolName::new(call.tool.clone())
            .map_err(|_| KitError::protocol("requested an invalid managed tool name"))?;
        let toolset_index = self
            .inner
            .routes
            .get(&tool_name)
            .copied()
            .ok_or_else(|| KitError::protocol("requested an unregistered managed tool"))?;
        let toolset = {
            let toolsets = self.toolsets();
            toolsets
                .as_ref()
                .and_then(|toolsets| toolsets.get(toolset_index))
                .cloned()
        }
        .ok_or_else(|| KitError::protocol("requested an unregistered managed tool"))?;
        let mut handler = toolset
            .tool_handler(tool_name)
            .await
            .map_err(KitError::from)?;
        let mut input = ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some(call.call_id.clone()),
            arguments: call.arguments,
        });
        loop {
            let output = handler.step(input).await;
            match output {
                Ok(ToolHandlerOutput::Completed(result)) => {
                    return CodexDynamicToolCallResponse::success(result);
                }
                Err(error) => {
                    return Ok(CodexDynamicToolCallResponse::tool_error(error));
                }
                Ok(ToolHandlerOutput::Interaction(request)) => {
                    input = match self.request_interaction(&owner, request).await? {
                        ToolInteractionResponse::Cancel { reason, .. } => {
                            ToolHandlerInput::Cancel { reason }
                        }
                        response => ToolHandlerInput::Interaction(response),
                    };
                }
            }
        }
    }

    pub(crate) async fn resolve(
        &self,
        conversation_id: &ConversationId,
        notification_id: NotificationId,
        resolution: NotificationResolution,
        deadline: tokio::time::Instant,
    ) -> Result<bool, KitError> {
        let request = {
            let pending = self.pending();
            let Some(pending) = pending.get(&notification_id) else {
                return Ok(false);
            };
            if &pending.owner.conversation_id != conversation_id {
                return Err(KitError::not_found(
                    "tool interaction does not belong to this conversation",
                ));
            }
            pending.request.clone()
        };
        let response = CodexToolInteractionResolution::new(&request, resolution.clone())?
            .into_response(request.id.clone());
        let intent = self
            .inner
            .notifications
            .begin_resolution(
                conversation_id,
                notification_id.clone(),
                resolution,
                deadline,
            )
            .await?;
        let pending = self.pending().remove(&notification_id).ok_or_else(|| {
            KitError::conflict("tool interaction changed during durable resolution")
        })?;
        let generation = pending.owner.generation.clone();
        let settlement = if pending.response.send(response).is_ok() {
            ResolutionSettlement::Accepted
        } else {
            ResolutionSettlement::Cancelled
        };
        self.reclaim_generation(&generation);
        let store = self.inner.storage.store();
        let mut transaction = store
            .begin_transaction()
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("tool interaction settlement could not begin")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                }
                error => {
                    KitError::from_state_store(error, "tool interaction settlement could not begin")
                        .in_phase(ErrorPhase::StateWrite)
                }
            })?;
        let disposition = NotificationStore::<S>::settle_resolution_in_transaction(
            &mut transaction,
            &intent,
            settlement,
        )
        .await?;
        transaction
            .delete(CodexToolInteractionRecord::key(
                &self.inner.agent_id,
                &notification_id,
            )?)
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("tool interaction record could not be removed")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                }
                error => KitError::from_state_store(
                    error,
                    "tool interaction record could not be removed",
                )
                .in_phase(ErrorPhase::StateWrite),
            })?;
        transaction.commit().await.map_err(|error| match error {
            storage_kit::StateStoreError::TransactionConflict { .. } => {
                KitError::integration("tool interaction settlement could not commit")
                    .with_detail(ErrorDetail::TransactionConflict)
                    .in_phase(ErrorPhase::StateCommit)
            }
            error => {
                KitError::from_state_store(error, "tool interaction settlement could not commit")
                    .in_phase(ErrorPhase::StateCommit)
            }
        })?;
        self.inner.notifications.wake_receivers();
        match disposition {
            ResolutionDisposition::Applied(actual) if actual == settlement => Ok(true),
            ResolutionDisposition::AlreadyAccepted
                if settlement == ResolutionSettlement::Accepted =>
            {
                Ok(true)
            }
            ResolutionDisposition::Applied(_) | ResolutionDisposition::AlreadyAccepted => Err(
                KitError::invalid_state("tool interaction settled with a contradictory outcome"),
            ),
        }
    }

    pub(crate) async fn cancel_conversation(
        &self,
        conversation_id: &ConversationId,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        {
            let mut state = self.state();
            state.closing_conversations.insert(conversation_id.clone());
        }
        self.inner.changed.notify_waiters();
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if self.state().responses.is_empty() {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "managed tool response did not settle before conversation close",
                ));
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if !self
                .state()
                .publications
                .keys()
                .any(|owner| &owner.conversation_id == conversation_id)
            {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "managed tool publication did not settle before conversation close",
                ));
            }
        }
        let notifications = self
            .pending()
            .iter()
            .filter(|(_, pending)| &pending.owner.conversation_id == conversation_id)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let mut first_error: Option<KitError> = None;
        for notification_id in notifications {
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool interaction did not settle before conversation close",
                    ));
                }
                break;
            }
            if let Err(error) = self
                .cancel_interaction(conversation_id, notification_id, deadline)
                .await
            {
                first_error = Some(match first_error.take() {
                    Some(primary) => {
                        primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                    }
                    None => error,
                });
            }
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool interaction did not settle before conversation close",
                    ));
                }
                break;
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            let active = self
                .state()
                .tasks
                .values()
                .any(|task| &task.conversation_id == conversation_id);
            if !active {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                let aborts = self
                    .state()
                    .tasks
                    .values()
                    .filter(|task| &task.conversation_id == conversation_id)
                    .map(|task| task.abort.clone())
                    .collect::<Vec<_>>();
                for abort in aborts {
                    abort.abort();
                }
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool task did not settle before conversation close",
                    ));
                }
                break;
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn cancel_workspace(
        &self,
        workspace_id: &WorkspaceId,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        {
            let mut state = self.state();
            state.closing_workspaces.insert(workspace_id.clone());
        }
        self.inner.changed.notify_waiters();
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if self.state().responses.is_empty() {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "managed tool response did not settle before workspace close",
                ));
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if !self
                .state()
                .publications
                .keys()
                .any(|owner| &owner.workspace_id == workspace_id)
            {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "tool interaction publication did not settle before workspace close",
                ));
            }
        }
        let pending = self
            .pending()
            .iter()
            .filter(|(_, pending)| &pending.owner.workspace_id == workspace_id)
            .map(|(id, pending)| (pending.owner.conversation_id.clone(), id.clone()))
            .collect::<Vec<_>>();
        let mut first_error: Option<KitError> = None;
        for (conversation_id, notification_id) in pending {
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "tool interaction cancellation did not settle before workspace close",
                    ));
                }
                break;
            }
            if let Err(error) = self
                .cancel_interaction(&conversation_id, notification_id, deadline)
                .await
            {
                first_error = Some(match first_error.take() {
                    Some(primary) => {
                        primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                    }
                    None => error,
                });
            }
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "tool interaction cancellation did not settle before workspace close",
                    ));
                }
                break;
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            let active = self
                .state()
                .tasks
                .values()
                .any(|task| &task.workspace_id == workspace_id);
            if !active {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                let aborts = self
                    .state()
                    .tasks
                    .values()
                    .filter(|task| &task.workspace_id == workspace_id)
                    .map(|task| task.abort.clone())
                    .collect::<Vec<_>>();
                for abort in aborts {
                    abort.abort();
                }
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool task did not settle before workspace close",
                    ));
                }
                break;
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn cancel_all(&self, deadline: tokio::time::Instant) -> Result<(), KitError> {
        self.state().closing = true;
        self.inner.changed.notify_waiters();
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if self.state().responses.is_empty() {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "managed tool response did not settle before Agent shutdown",
                ));
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if self.state().publications.is_empty() {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Err(KitError::timeout(
                    "tool interaction publication did not settle before Agent shutdown",
                ));
            }
        }
        let pending = self
            .pending()
            .iter()
            .map(|(id, pending)| (pending.owner.conversation_id.clone(), id.clone()))
            .collect::<Vec<_>>();
        let mut first_error: Option<KitError> = None;
        for (conversation_id, notification_id) in pending {
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "tool interaction cancellation did not settle before Agent shutdown",
                    ));
                }
                break;
            }
            if let Err(error) = self
                .cancel_interaction(&conversation_id, notification_id, deadline)
                .await
            {
                first_error = Some(match first_error.take() {
                    Some(primary) => {
                        primary.with_supplementary(error.in_phase(ErrorPhase::Cleanup))
                    }
                    None => error,
                });
            }
            if tokio::time::Instant::now() >= deadline {
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "tool interaction cancellation did not settle before Agent shutdown",
                    ));
                }
                break;
            }
        }
        loop {
            let mut changed = std::pin::pin!(self.inner.changed.notified());
            changed.as_mut().enable();
            if self.state().tasks.is_empty() {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                let aborts = self
                    .state()
                    .tasks
                    .values()
                    .map(|task| task.abort.clone())
                    .collect::<Vec<_>>();
                for abort in aborts {
                    abort.abort();
                }
                if first_error.is_none() {
                    first_error = Some(KitError::timeout(
                        "managed tool task did not settle before Agent shutdown",
                    ));
                }
                break;
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) async fn shutdown(&self, deadline: tokio::time::Instant) -> Result<(), KitError> {
        let cancellation_error = self.cancel_all(deadline).await.err();
        let toolsets = self.toolsets().take().unwrap_or_default();
        let shutdown_error = Self::shutdown_registrations(toolsets, deadline).await.err();
        if let Some(error) = cancellation_error.or(shutdown_error) {
            return Err(error);
        }
        Ok(())
    }

    async fn cancel_interaction(
        &self,
        conversation_id: &ConversationId,
        notification_id: NotificationId,
        deadline: tokio::time::Instant,
    ) -> Result<(), KitError> {
        let resolution = self
            .resolve(
                conversation_id,
                notification_id.clone(),
                NotificationResolution::Dismiss {
                    reason: "managed tool work ended".to_owned(),
                },
                deadline,
            )
            .await;
        if matches!(resolution, Ok(true)) {
            return Ok(());
        }
        if let Some(pending) = self.pending().remove(&notification_id) {
            let generation = pending.owner.generation.clone();
            let _ = pending.response.send(ToolInteractionResponse::cancel(
                pending.request.id,
                Some("managed tool work ended".to_owned()),
            ));
            self.reclaim_generation(&generation);
        }
        match resolution {
            Ok(_) => Ok(()),
            Err(error) if matches!(error.kind(), ErrorKind::Closed | ErrorKind::NotFound) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn request_interaction(
        &self,
        owner: &CodexInteractionOwner,
        request: ToolInteractionRequest,
    ) -> Result<ToolInteractionResponse, KitError> {
        let publication = {
            let mut state = self.state();
            if state.closing
                || state.closing_workspaces.contains(&owner.workspace_id)
                || state.closing_conversations.contains(&owner.conversation_id)
                || state.generation_is_fenced(&owner.generation)
            {
                return Ok(ToolInteractionResponse::cancel(
                    request.id.clone(),
                    Some("managed tool generation ended".to_owned()),
                ));
            }
            let publications = state.publications.entry(owner.clone()).or_default();
            *publications = publications.checked_add(1).ok_or_else(|| {
                KitError::invalid_state("tool interaction publication count is exhausted")
            })?;
            CodexToolPublication {
                inner: self.inner.clone(),
                owner: owner.clone(),
            }
        };
        let published = self.publish_interaction(owner, request).await;
        drop(publication);
        published?
            .await
            .map_err(|_| KitError::cancelled("tool interaction was cancelled"))
    }

    async fn publish_interaction(
        &self,
        owner: &CodexInteractionOwner,
        request: ToolInteractionRequest,
    ) -> Result<tokio::sync::oneshot::Receiver<ToolInteractionResponse>, KitError> {
        let kind = CodexToolInteractionResolution::notification(&request)?;
        let (response, receiver) = tokio::sync::oneshot::channel();
        let store = self.inner.storage.store();
        let mut transaction = store
            .begin_transaction()
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("tool interaction publication could not begin")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                }
                error => KitError::from_state_store(
                    error,
                    "tool interaction publication could not begin",
                )
                .in_phase(ErrorPhase::StateWrite),
            })?;
        let (notification_id, _) = NotificationStore::<S>::publish_in_transaction(
            &mut transaction,
            &owner.conversation_id,
            kind,
        )
        .await?;
        let record = CodexToolInteractionRecord::new(
            self.inner.agent_id.clone(),
            owner.workspace_id.clone(),
            owner.conversation_id.clone(),
            notification_id.clone(),
        );
        record.put_in_transaction(&mut transaction).await?;
        if self
            .pending()
            .insert(
                notification_id.clone(),
                CodexPendingToolInteraction {
                    owner: owner.clone(),
                    request,
                    response,
                },
            )
            .is_some()
        {
            return Err(KitError::invalid_state(
                "tool interaction notification identity was reused",
            ));
        }
        if transaction.commit().await.is_err() {
            self.pending().remove(&notification_id);
            return Err(KitError::integration(
                "tool interaction publication could not commit",
            ));
        }
        self.inner.notifications.wake_receivers();
        Ok(receiver)
    }

    async fn reconcile_lost_interactions(&self) -> Result<(), KitError> {
        for (key, expected) in
            CodexToolInteractionRecord::for_agent(&self.inner.storage, &self.inner.agent_id).await?
        {
            let store = self.inner.storage.store();
            let mut transaction = store
                .begin_transaction()
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::integration(
                            "lost tool interaction reconciliation could not begin",
                        )
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                    }
                    error => KitError::from_state_store(
                        error,
                        "lost tool interaction reconciliation could not begin",
                    )
                    .in_phase(ErrorPhase::StateWrite),
                })?;
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| match error {
                    storage_kit::StateStoreError::TransactionConflict { .. } => {
                        KitError::integration("tool interaction record could not be loaded")
                            .with_detail(ErrorDetail::TransactionConflict)
                            .in_phase(ErrorPhase::StateRead)
                    }
                    error => KitError::from_state_store(
                        error,
                        "tool interaction record could not be loaded",
                    )
                    .in_phase(ErrorPhase::StateRead),
                })?
                .ok_or_else(|| KitError::invalid_state("tool interaction record is missing"))?;
            let actual = CodexToolInteractionRecord::decode(snapshot, &self.inner.agent_id)?;
            if actual != expected {
                return Err(KitError::invalid_state(
                    "tool interaction changed during reconciliation",
                ));
            }
            NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                &mut transaction,
                &actual.conversation_id,
                &actual.notification_id,
                ResolutionSettlement::Cancelled,
            )
            .await
            .map_err(NotificationSettlementError::into_reconciliation_sdk)?;
            transaction.delete(key).await.map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("lost tool interaction record could not be removed")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                }
                error => KitError::from_state_store(
                    error,
                    "lost tool interaction record could not be removed",
                )
                .in_phase(ErrorPhase::StateWrite),
            })?;
            transaction.commit().await.map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("lost tool interaction reconciliation could not commit")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateCommit)
                }
                error => KitError::from_state_store(
                    error,
                    "lost tool interaction reconciliation could not commit",
                )
                .in_phase(ErrorPhase::StateCommit),
            })?;
            self.inner
                .notifications
                .finish_reconciliation(&actual.conversation_id);
        }
        Ok(())
    }

    fn reclaim_generation(&self, generation: &CodexToolGeneration) {
        let has_pending_interaction = self
            .pending()
            .values()
            .any(|pending| &pending.owner.generation == generation);
        let reclaimed = self
            .state()
            .reclaim_generation(generation, has_pending_interaction);
        if reclaimed {
            self.inner.changed.notify_waiters();
        }
    }

    fn pending(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<NotificationId, CodexPendingToolInteraction>> {
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn toolsets(&self) -> std::sync::MutexGuard<'_, Option<Vec<Arc<T>>>> {
        self.inner
            .toolsets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn state(&self) -> std::sync::MutexGuard<'_, CodexToolState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

enum CodexToolInteractionResolution {
    Submit(serde_json::Value),
    Decline(Option<String>),
    Cancel(Option<String>),
}

impl CodexToolInteractionResolution {
    fn new(
        request: &ToolInteractionRequest,
        resolution: NotificationResolution,
    ) -> Result<Self, KitError> {
        match (&request.kind, resolution) {
            (ToolInteractionKind::Text { .. }, NotificationResolution::Submit { content })
                if content.is_string() =>
            {
                Ok(Self::Submit(content))
            }
            (ToolInteractionKind::Form { .. }, NotificationResolution::Submit { content })
                if content.is_object() =>
            {
                Ok(Self::Submit(content))
            }
            (_, NotificationResolution::Deny { reason }) => Ok(Self::Decline(Some(reason))),
            (_, NotificationResolution::Dismiss { reason }) => Ok(Self::Cancel(Some(reason))),
            _ => Err(KitError::invalid_input(
                "notification resolution does not match the tool interaction",
            )),
        }
    }

    fn notification(
        request: &ToolInteractionRequest,
    ) -> Result<ConversationNotificationKind, KitError> {
        let response_schema = match &request.kind {
            ToolInteractionKind::Text { .. } => Some(serde_json::json!({ "type": "string" })),
            ToolInteractionKind::Form { schema, .. } => Some(schema.clone()),
        };
        ToolName::new(request.tool_name.as_str())
            .map_err(|_| KitError::protocol("tool interaction contains an invalid tool name"))?;
        Ok(ConversationNotificationKind::Interaction {
            title: format!("Tool input: {}", request.tool_name.as_str()),
            body: request.prompt.clone(),
            response_schema,
            dismissible: true,
        })
    }

    fn into_response(self, id: ToolInteractionId) -> ToolInteractionResponse {
        match self {
            Self::Submit(content) => ToolInteractionResponse::submit(id, content),
            Self::Decline(reason) => ToolInteractionResponse::decline(id, reason),
            Self::Cancel(reason) => ToolInteractionResponse::cancel(id, reason),
        }
    }
}

impl CodexToolRegistrationRecord {
    async fn describe<T: ToolsetBackend>(
        toolsets: &[Arc<T>],
    ) -> Result<
        (
            Vec<CodexManagedToolDescriptor>,
            Vec<CodexDynamicToolSpec>,
            BTreeMap<ToolName, usize>,
        ),
        KitError,
    > {
        let mut descriptors = Vec::new();
        let mut specs = Vec::new();
        let mut routes = BTreeMap::new();
        for (toolset_index, toolset) in toolsets.iter().enumerate() {
            for tool in toolset.list_tools().await? {
                let tool_name = tool.name.clone();
                let name = tool.name.as_str().to_owned();
                if routes.insert(tool_name, toolset_index).is_some() {
                    return Err(KitError::invalid_config(
                        "configured tool names must be unique",
                    ));
                }
                let description = tool.description;
                let input_schema = tool.input_schema.schema;
                descriptors.push(CodexManagedToolDescriptor {
                    name: name.clone(),
                    description: description.clone(),
                    input_schema: input_schema.clone(),
                });
                specs.push(CodexDynamicToolSpec {
                    name,
                    description: description.unwrap_or_default(),
                    input_schema,
                });
            }
        }
        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        specs.sort_by(|left, right| left.name.cmp(&right.name));
        Ok((descriptors, specs, routes))
    }

    async fn create<S: StateStoreBackend>(
        &self,
        storage: &AgentStorage<S>,
    ) -> Result<(), KitError> {
        self.validate()?;
        let key = Self::key(&self.agent_id)?;
        let store = storage.store();
        let mut transaction = store
            .begin_transaction()
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("managed tool registration transaction could not begin")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                }
                error => KitError::from_state_store(
                    error,
                    "managed tool registration transaction could not begin",
                )
                .in_phase(ErrorPhase::StateWrite),
            })?;
        if transaction
            .load(key.clone())
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("managed tool registration could not be checked")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateRead)
                }
                error => KitError::from_state_store(
                    error,
                    "managed tool registration could not be checked",
                )
                .in_phase(ErrorPhase::StateRead),
            })?
            .is_some()
        {
            return Err(KitError::conflict(
                "managed tool registration already exists",
            ));
        }
        transaction
            .put(storage_kit::state::record::StateRecord::new(
                key,
                serde_json::to_value(self).map_err(|_| {
                    KitError::invalid_state("managed tool registration could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("managed tool registration could not be stored")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateWrite)
                }
                error => KitError::from_state_store(
                    error,
                    "managed tool registration could not be stored",
                )
                .in_phase(ErrorPhase::StateWrite),
            })?;
        transaction.commit().await.map_err(|error| match error {
            storage_kit::StateStoreError::TransactionConflict { .. } => {
                KitError::integration("managed tool registration could not commit")
                    .with_detail(ErrorDetail::TransactionConflict)
                    .in_phase(ErrorPhase::StateCommit)
            }
            error => {
                KitError::from_state_store(error, "managed tool registration could not commit")
                    .in_phase(ErrorPhase::StateCommit)
            }
        })?;
        Ok(())
    }

    async fn verify<S: StateStoreBackend>(
        &self,
        storage: &AgentStorage<S>,
    ) -> Result<(), KitError> {
        self.validate()?;
        let store = storage.store();
        let snapshot = store
            .load(Self::key(&self.agent_id)?)
            .await
            .map_err(|error| match error {
                storage_kit::StateStoreError::TransactionConflict { .. } => {
                    KitError::integration("managed tool registration could not be loaded")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(ErrorPhase::StateRead)
                }
                error => KitError::from_state_store(
                    error,
                    "managed tool registration could not be loaded",
                )
                .in_phase(ErrorPhase::StateRead),
            })?
            .ok_or_else(|| KitError::conflict("managed tool registration is missing"))?;
        let actual: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| KitError::invalid_state("managed tool registration is malformed"))?;
        actual.validate()?;
        if actual != *self {
            return Err(KitError::conflict(
                "managed tool registration changed during reopen",
            ));
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), KitError> {
        if self.version != CODEX_TOOL_REGISTRATION_VERSION {
            return Err(KitError::invalid_state(
                "managed tool registration version is unsupported",
            ));
        }
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            ToolName::new(tool.name.clone()).map_err(|_| {
                KitError::invalid_state("managed tool registration name is malformed")
            })?;
            if !names.insert(tool.name.as_str()) {
                return Err(KitError::invalid_state(
                    "managed tool registration contains duplicate names",
                ));
            }
            if !tool.input_schema.is_object() {
                return Err(KitError::invalid_state(
                    "managed tool registration schema is malformed",
                ));
            }
        }
        Ok(())
    }

    fn key(agent_id: &AgentId) -> Result<storage_kit::state::record::StateKey, KitError> {
        storage_kit::state::record::StateKey::new(
            CODEX_TOOL_REGISTRATION_NAMESPACE,
            agent_id.as_str(),
        )
        .map_err(|_| KitError::invalid_state("managed tool registration key is invalid"))
    }
}

impl CodexDynamicToolCall {
    pub(crate) fn decode(params: serde_json::Value) -> Result<Self, KitError> {
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Wire {
            thread_id: String,
            turn_id: String,
            call_id: String,
            namespace: Option<String>,
            tool: String,
            arguments: serde_json::Value,
        }

        let wire: Wire = serde_json::from_value(params)
            .map_err(|_| KitError::protocol("dynamic tool request is malformed"))?;
        for value in [
            wire.thread_id.as_str(),
            wire.turn_id.as_str(),
            wire.call_id.as_str(),
            wire.tool.as_str(),
        ] {
            if value.is_empty() || value.trim() != value {
                return Err(KitError::protocol("managed tool request is malformed"));
            }
        }
        Ok(Self {
            call_id: wire.call_id,
            thread_id: wire.thread_id,
            turn_id: wire.turn_id,
            namespace: wire.namespace,
            tool: wire.tool,
            arguments: wire.arguments,
        })
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub(crate) fn generation(&self) -> CodexToolGeneration {
        CodexToolGeneration::new(&self.thread_id, &self.turn_id)
    }

    pub(crate) fn has_namespace(&self) -> bool {
        self.namespace.is_some()
    }
}

impl CodexDynamicToolCallResponse {
    fn success(result: ToolResult) -> Result<Self, KitError> {
        let (content_items, representable) = CodexToolResultContent::new(&result).into_values();
        Ok(Self {
            content_items,
            success: !result.is_error && representable,
        })
    }

    fn tool_error(error: tools_kit::ToolError) -> Self {
        Self {
            content_items: vec![serde_json::json!({
                "type": "inputText",
                "text": error.to_string(),
            })],
            success: false,
        }
    }

    pub(crate) fn into_result(self) -> serde_json::Value {
        serde_json::json!({
            "contentItems": self.content_items,
            "success": self.success,
        })
    }
}

struct CodexToolResultContent<'a> {
    result: &'a ToolResult,
}

impl<'a> CodexToolResultContent<'a> {
    fn new(result: &'a ToolResult) -> Self {
        Self { result }
    }

    fn into_values(self) -> (Vec<serde_json::Value>, bool) {
        let mut representable = true;
        let mut values = Vec::new();
        for content in &self.result.content {
            match Self::value(content) {
                Some(value) => values.push(value),
                None => representable = false,
            }
        }
        if !representable {
            return (
                vec![serde_json::json!({
                    "type": "inputText",
                    "text": "managed tool result contains content Codex cannot represent",
                })],
                false,
            );
        }
        if values.is_empty()
            && let Some(value) = &self.result.structured_content
        {
            values.push(serde_json::json!({
                "type": "inputText",
                "text": value.to_string(),
            }));
        }
        (values, true)
    }

    fn value(content: &ToolContent) -> Option<serde_json::Value> {
        match content {
            ToolContent::Text { text, .. } => Some(serde_json::json!({
                "type": "inputText",
                "text": text,
            })),
            ToolContent::Json { value, .. } => Some(serde_json::json!({
                "type": "inputText",
                "text": value.to_string(),
            })),
            ToolContent::Image {
                mime_type,
                data_base64,
                ..
            } => Some(serde_json::json!({
                "type": "inputImage",
                "imageUrl": format!("data:{mime_type};base64,{data_base64}"),
            })),
            ToolContent::Audio { .. }
            | ToolContent::ResourceLink { .. }
            | ToolContent::EmbeddedResource { .. } => None,
        }
    }
}
