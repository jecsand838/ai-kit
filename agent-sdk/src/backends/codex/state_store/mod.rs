use crate::error::{ErrorDetail, ErrorKind, ErrorPhase};
mod failure;
mod prompt;
pub(crate) mod records;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use storage_sdk::{StateStoreBackend, StateTransactionBackend};

use crate::agent_storage::AgentStorageKnowledgeError;
use crate::backends::codex::adapters::conversation::CodexThread;
use crate::backends::codex::adapters::prompt::{CodexPromptSelection, CodexStructuredOutput};
use crate::backends::codex::conversations::thread::CodexThreadBinding;
use crate::backends::codex::notifications::CodexPromptTerminal;
use crate::backends::codex::server_requests::CodexInteractionRequest;
use crate::core::notification_store::{
    NotificationCloseError, NotificationDeleteError, NotificationInitializationError,
    NotificationInteractionCheckError, NotificationSettlementError, NotificationStore,
    ResolutionDisposition, ResolutionIntent, ResolutionSettlement,
};
use crate::core::state::{ConversationRecord, WorkspaceRecord};
use crate::prompt::PromptOutput;
use crate::{
    AgentId, AgentInfo, AgentStorage, ConversationId, KnowledgeOwnerId, NewWorkspace,
    NotificationId, PromptResponse, SdkError, UpdateWorkspaceRequest, WorkspaceId,
};

use prompt::{CodexPromptContext, CodexPromptTasks};
use records::{
    CodexAttachment, CodexCleanup, CodexContinuation, CodexContinuationState, CodexNativeSelection,
    CodexNativeSelectionRecord, CodexPromptRecord, CodexThreadCreation, CodexToolAdmission,
};

const CODEX_ATTACHMENT_NAMESPACE: &str = "agent_sdk_codex_attachments_v1";
const CODEX_THREAD_NAMESPACE: &str = "agent_sdk_codex_threads_v1";
const CODEX_THREAD_ROUTE_NAMESPACE: &str = "agent_sdk_codex_thread_routes_v1";
const CODEX_CLEANUP_NAMESPACE: &str = "agent_sdk_codex_cleanup_v1";
const CODEX_PROMPT_NAMESPACE: &str = "agent_sdk_codex_prompts_v1";
const CODEX_CONTINUATION_NAMESPACE: &str = "agent_sdk_codex_continuations_v1";
const CODEX_THREAD_CREATION_NAMESPACE: &str = "agent_sdk_codex_thread_creations_v2";
const CODEX_NATIVE_SELECTION_NAMESPACE: &str = "agent_sdk_codex_native_selection_v1";
const MAX_STATE_PAGES: usize = 1_024;

pub(crate) struct CodexStateStore<S: StateStoreBackend> {
    agent_id: AgentId,
    storage: AgentStorage<S>,
    knowledge_enabled: bool,
    notifications: NotificationStore<S>,
    prompt_permits: Arc<tokio::sync::Semaphore>,
    prompt_changes: tokio::sync::watch::Sender<u64>,
    runtime_open: Arc<AtomicBool>,
    instructions: Arc<Option<String>>,
    prompt_contexts: Arc<Mutex<BTreeMap<ConversationId, CodexPromptContext>>>,
    prompt_tasks: Arc<Mutex<CodexPromptTasks>>,
    retained_cleanups: Arc<Mutex<Vec<CodexCleanup>>>,
}

pub(crate) enum CodexTerminalDisposition {
    Applied,
    Reconcile(Option<Box<CodexPromptRecord>>),
}

pub(crate) enum CodexAbort {
    Idle,
    Join {
        client_message_id: String,
        turn_id: Option<String>,
    },
}

enum CodexConversationCloseError {
    TransactionConflict,
    Failure(SdkError),
}

enum CodexAbortRequestError {
    TransactionConflict,
    Failure(SdkError),
}

pub(crate) enum CodexPromptStartError {
    TransactionConflict,
    Failure(SdkError),
}

impl CodexConversationCloseError {
    fn storage(
        error: storage_sdk::StateStoreError,
        integration_message: &'static str,
        phase: ErrorPhase,
    ) -> Self {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => Self::TransactionConflict,
            error => Self::Failure(
                SdkError::from_state_store(error, integration_message).in_phase(phase),
            ),
        }
    }
}

impl CodexAbortRequestError {
    fn storage(
        error: storage_sdk::StateStoreError,
        integration_message: &'static str,
        phase: ErrorPhase,
    ) -> Self {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => Self::TransactionConflict,
            error => Self::Failure(
                SdkError::from_state_store(error, integration_message).in_phase(phase),
            ),
        }
    }
}

impl CodexPromptStartError {
    fn storage(
        error: storage_sdk::StateStoreError,
        integration_message: &'static str,
        phase: ErrorPhase,
    ) -> Self {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => Self::TransactionConflict,
            error => Self::Failure(
                SdkError::from_state_store(error, integration_message).in_phase(phase),
            ),
        }
    }
}

impl From<SdkError> for CodexAbortRequestError {
    fn from(error: SdkError) -> Self {
        Self::Failure(error)
    }
}

impl From<SdkError> for CodexPromptStartError {
    fn from(error: SdkError) -> Self {
        Self::Failure(error)
    }
}

impl<S: StateStoreBackend> Clone for CodexStateStore<S> {
    fn clone(&self) -> Self {
        Self {
            agent_id: self.agent_id.clone(),
            storage: self.storage.clone(),
            knowledge_enabled: self.knowledge_enabled,
            notifications: self.notifications.clone(),
            prompt_permits: self.prompt_permits.clone(),
            prompt_changes: self.prompt_changes.clone(),
            runtime_open: self.runtime_open.clone(),
            instructions: self.instructions.clone(),
            prompt_contexts: Arc::clone(&self.prompt_contexts),
            prompt_tasks: Arc::clone(&self.prompt_tasks),
            retained_cleanups: self.retained_cleanups.clone(),
        }
    }
}

impl<S: StateStoreBackend> CodexStateStore<S> {
    fn storage_error(
        error: storage_sdk::StateStoreError,
        message: &'static str,
        phase: ErrorPhase,
    ) -> SdkError {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                SdkError::integration(message)
                    .with_detail(ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            error => SdkError::from_state_store(error, message).in_phase(phase),
        }
    }

    fn workspace_update_error(
        error: storage_sdk::StateStoreError,
        integration_message: &'static str,
        phase: ErrorPhase,
    ) -> SdkError {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                SdkError::conflict("workspace update conflicted")
                    .with_detail(ErrorDetail::TransactionConflict)
            }
            error => SdkError::from_state_store(error, integration_message),
        }
        .in_phase(phase)
    }

    fn workspace_delete_error(
        error: storage_sdk::StateStoreError,
        integration_message: &'static str,
        phase: ErrorPhase,
    ) -> SdkError {
        match error {
            storage_sdk::StateStoreError::TransactionConflict { .. } => {
                SdkError::conflict("workspace deletion conflicted")
                    .with_detail(ErrorDetail::TransactionConflict)
            }
            error => SdkError::from_state_store(error, integration_message),
        }
        .in_phase(phase)
    }

    pub(crate) fn new(
        agent_id: AgentId,
        storage: AgentStorage<S>,
        knowledge_enabled: bool,
        prompt_limit: usize,
        instructions: Option<String>,
    ) -> Result<Self, SdkError> {
        let (prompt_changes, _) = tokio::sync::watch::channel(0);
        let notifications = NotificationStore::<S>::new(storage.clone())?;
        Ok(Self {
            agent_id,
            notifications,
            storage,
            knowledge_enabled,
            prompt_permits: Arc::new(tokio::sync::Semaphore::new(prompt_limit)),
            prompt_changes,
            runtime_open: Arc::new(AtomicBool::new(true)),
            instructions: Arc::new(instructions),
            prompt_contexts: Arc::new(Mutex::new(BTreeMap::new())),
            prompt_tasks: Arc::new(Mutex::new(CodexPromptTasks::new())),
            retained_cleanups: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub(crate) fn notifications(&self) -> &NotificationStore<S> {
        &self.notifications
    }

    pub(crate) fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    async fn stage_knowledge_forget(
        &self,
        transaction: &mut S::Transaction,
        owner: &KnowledgeOwnerId,
    ) -> Result<(), AgentStorageKnowledgeError> {
        if !self.knowledge_enabled {
            return Ok(());
        }
        self.storage
            .stage_knowledge_forget(transaction, owner)
            .await
    }

    pub(crate) fn prompt_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, SdkError> {
        self.prompt_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SdkError::busy("agent prompt concurrency is saturated"))
    }

    pub(crate) fn prompt_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.prompt_changes.subscribe()
    }

    pub(crate) fn invalidate_runtime(&self) {
        self.runtime_open.store(false, Ordering::Release);
        self.notifications.invalidate_all();
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    pub(crate) fn spawn_prompt<F>(&self, future: F) -> Result<(), F>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let result = self
            .prompt_tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .spawn(future);
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        result
    }

    pub(crate) async fn prompt_task_exit(&self) -> Result<(), SdkError> {
        let mut changed = self.prompt_changes();
        loop {
            tokio::select! {
                result = std::future::poll_fn(|context| self.prompt_tasks.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).poll_exit(context)) => {
                    return result.map_err(|_| SdkError::integration("prompt session task failed"));
                }
                _ = changed.changed() => {}
            }
        }
    }

    pub(crate) fn abort_prompt_tasks(&self) {
        drop(
            self.prompt_tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .seal(),
        );
    }

    pub(crate) async fn join_prompt_tasks(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), SdkError> {
        let tasks = self
            .prompt_tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .seal();
        let Some(mut tasks) = tasks else {
            return Ok(());
        };
        let mut first_error = None;
        while !tasks.is_empty() {
            match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(Ok(()))) | Ok(None) => {}
                Ok(Some(Err(_))) => {
                    first_error
                        .get_or_insert_with(|| SdkError::integration("prompt session task failed"));
                }
                Err(_) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    first_error.get_or_insert_with(|| {
                        SdkError::timeout("prompt session cleanup timed out")
                    });
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn fail_prompt_observers(&self, error: &SdkError) {
        for context in self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
        {
            context.fail(error);
        }
    }

    pub(crate) fn retain_prompt_context(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        validator: Option<CodexStructuredOutput>,
        output: Option<Arc<PromptOutput>>,
    ) -> Result<(), SdkError> {
        let mut contexts = self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match contexts.entry(conversation_id.clone()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                if !entry.get().owns(client_message_id) {
                    return Err(SdkError::conflict(
                        "prompt context belongs to another generation",
                    ));
                }
                entry.get_mut().retain_output(output);
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(CodexPromptContext::new(
                    client_message_id.to_owned(),
                    validator,
                    output,
                ));
            }
        }
        Ok(())
    }

    pub(crate) async fn observe_prompt_delta(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
        text: &str,
    ) -> Result<(), SdkError> {
        let Some(record) = self.prompt_record(conversation_id).await? else {
            return Ok(());
        };
        if record.terminal_result().is_some() {
            return Ok(());
        }
        if let Some(context) = self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(conversation_id)
            && context.owns(record.client_message_id())
        {
            context.fragment(turn_id, text);
        }
        Ok(())
    }

    pub(crate) fn instructions(&self) -> Option<&str> {
        self.instructions.as_ref().as_deref()
    }

    fn validate_attachment_owner(
        &self,
        info: &AgentInfo,
        attachment: &CodexAttachment,
    ) -> Result<(), SdkError> {
        if info.id() != &self.agent_id || info.id() != attachment.agent_id() {
            return Err(SdkError::invalid_state(
                "attachment owner does not match agent identity",
            ));
        }
        Ok(())
    }

    pub(crate) async fn native_selection(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<CodexNativeSelection, SdkError> {
        let store = self.storage.store();
        let snapshot = store
            .load(Self::native_selection_key(conversation_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "runtime selection could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?;
        match snapshot {
            Some(snapshot) => Self::decode_native_selection(snapshot, conversation_id)
                .map(|record| record.selection().clone()),
            None => Ok(CodexNativeSelection::default()),
        }
    }

    pub(crate) async fn create_prompt(
        &self,
        binding: &CodexThreadBinding,
        client_message_id: String,
        structured_output: Option<CodexStructuredOutput>,
        selection: &CodexPromptSelection,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), CodexPromptStartError> {
        let workspace_id = binding.workspace_id();
        let workspace_revision = binding.workspace_revision();
        let conversation_id = binding.conversation_id();
        if self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(conversation_id)
        {
            return Err(CodexPromptStartError::Failure(SdkError::busy(
                "conversation already owns a structured prompt validator",
            )));
        }
        let record = CodexPromptRecord::prepared(
            conversation_id.clone(),
            client_message_id.clone(),
            structured_output
                .as_ref()
                .map(|output| output.schema().clone()),
            selection.previous().clone(),
            selection.accepted().clone(),
            timeout,
        )?;
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            CodexPromptStartError::storage(
                error,
                "prompt transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(&self.agent_id, workspace_id)?)
            .await
            .map_err(|error| {
                CodexPromptStartError::storage(
                    error,
                    "prompt workspace could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| {
                CodexPromptStartError::Failure(SdkError::not_found("workspace does not exist"))
            })?;
        let workspace = WorkspaceRecord::decode(workspace, &self.agent_id, Some(workspace_id))?;
        if workspace.revision() != workspace_revision {
            return Err(CodexPromptStartError::Failure(SdkError::conflict(
                "conversation handle is stale",
            )));
        }
        let conversation = transaction
            .load(ConversationRecord::key(workspace_id, conversation_id)?)
            .await
            .map_err(|error| {
                CodexPromptStartError::storage(
                    error,
                    "prompt conversation could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| {
                CodexPromptStartError::Failure(SdkError::not_found("conversation does not exist"))
            })?;
        let conversation =
            ConversationRecord::decode(conversation, workspace_id, Some(conversation_id))?;
        if conversation.workspace_revision() != workspace_revision {
            return Err(CodexPromptStartError::Failure(SdkError::conflict(
                "conversation handle is stale",
            )));
        }
        let durable_binding = transaction
            .load(Self::binding_key(workspace_id, conversation_id)?)
            .await
            .map_err(|error| {
                CodexPromptStartError::storage(
                    error,
                    "prompt binding could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| {
                CodexPromptStartError::Failure(SdkError::invalid_state("prompt binding is missing"))
            })?;
        let durable_binding = Self::decode_binding(durable_binding, workspace_id, conversation_id)?;
        if &durable_binding != binding {
            return Err(CodexPromptStartError::Failure(SdkError::conflict(
                "conversation handle is stale",
            )));
        }
        durable_binding.verify_owner(&conversation, &workspace, self.instructions())?;
        durable_binding.require_materialized()?;
        match NotificationStore::<S>::check_no_unresolved_interaction_in_transaction(
            &mut transaction,
            conversation_id,
        )
        .await
        {
            Ok(()) => {}
            Err(NotificationInteractionCheckError::TransactionConflict) => {
                return Err(CodexPromptStartError::TransactionConflict);
            }
            Err(NotificationInteractionCheckError::Failure(error)) => {
                return Err(CodexPromptStartError::Failure(error));
            }
        }
        let native_selection = match transaction
            .load(Self::native_selection_key(conversation_id)?)
            .await
            .map_err(|error| {
                CodexPromptStartError::storage(
                    error,
                    "runtime selection could not be checked",
                    ErrorPhase::StateRead,
                )
            })? {
            Some(snapshot) => Self::decode_native_selection(snapshot, conversation_id)?
                .selection()
                .clone(),
            None => CodexNativeSelection::default(),
        };
        if &native_selection != record.previous_selection() {
            return Err(CodexPromptStartError::Failure(SdkError::conflict(
                "runtime selection changed during prompt preflight",
            )));
        }
        if transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                CodexPromptStartError::storage(
                    error,
                    "prompt state could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .is_some()
        {
            return Err(CodexPromptStartError::Failure(SdkError::busy(
                "conversation has active or unreconciled prompt work",
            )));
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record)
                    .map_err(|_| SdkError::invalid_state("prompt could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                CodexPromptStartError::storage(
                    error,
                    "prompt state could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            CodexPromptStartError::storage(
                error,
                "prompt transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.retain_prompt_context(conversation_id, &client_message_id, structured_output, None)?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    pub(crate) async fn mark_prompt_may_be_accepted(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<(), SdkError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt send boundary could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let mut record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.client_message_id() != client_message_id {
            return Err(SdkError::conflict(
                "prompt send boundary belongs to another generation",
            ));
        }
        record.mark_may_be_accepted()?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record).map_err(|_| {
                    SdkError::invalid_state("prompt send boundary could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt send boundary could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt send boundary could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }
    pub(crate) async fn accept_prompt(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        turn_id: &str,
    ) -> Result<(), SdkError> {
        if let Some(context) = self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(conversation_id)
            && context.owns(client_message_id)
        {
            context.accept_output(turn_id);
        }
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt acceptance transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let mut record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.client_message_id() != client_message_id {
            return Err(SdkError::conflict(
                "prompt acceptance belongs to another generation",
            ));
        }
        record.accept(client_message_id, turn_id)?;
        Self::persist_native_selection_in_transaction(&mut transaction, conversation_id, &record)
            .await?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record)
                    .map_err(|_| SdkError::invalid_state("prompt could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt acceptance could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt acceptance could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    pub(crate) async fn reject_unaccepted_prompt(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<(), SdkError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt rejection could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt state could not be loaded",
                ErrorPhase::StateRead,
            )
        })?;
        let Some(snapshot) = snapshot else {
            drop(transaction);
            self.prompt_contexts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(conversation_id);
            return Ok(());
        };
        let record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.client_message_id() != client_message_id {
            return Err(SdkError::conflict(
                "prompt rejection belongs to another generation",
            ));
        }
        if !record.rejectable_without_turn() {
            return Err(SdkError::conflict(
                "prompt was accepted before rejection settled",
            ));
        }
        transaction.delete(key).await.map_err(|error| {
            Self::storage_error(
                error,
                "rejected prompt could not be removed",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt rejection could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        self.prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(conversation_id);
        Ok(())
    }

    pub(crate) async fn mark_prompt_timed_out(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<Option<bool>, SdkError> {
        self.mutate_optional_prompt(conversation_id, client_message_id, |record| {
            record.mark_waiter_timed_out()
        })
        .await
    }

    pub(crate) async fn claim_prompt_delivery(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<bool, SdkError> {
        self.mutate_prompt(conversation_id, client_message_id, |record| {
            record.claim_waiter_delivery()
        })
        .await
    }

    pub(crate) async fn mark_prompt_waiter_lost(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<(), SdkError> {
        self.mutate_prompt(conversation_id, client_message_id, |record| {
            record.mark_waiter_lost()
        })
        .await
    }

    pub(crate) async fn settle_prompt_delivery(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        delivered: bool,
    ) -> Result<(), SdkError> {
        self.mutate_prompt(conversation_id, client_message_id, |record| {
            record.mark_waiter_delivery(delivered)
        })
        .await
    }

    async fn mutate_prompt<T>(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        mutation: impl FnOnce(&mut CodexPromptRecord) -> Result<T, SdkError>,
    ) -> Result<T, SdkError> {
        self.mutate_optional_prompt(conversation_id, client_message_id, mutation)
            .await?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))
    }

    async fn mutate_optional_prompt<T>(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        mutation: impl FnOnce(&mut CodexPromptRecord) -> Result<T, SdkError>,
    ) -> Result<Option<T>, SdkError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt mutation could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt state could not be loaded",
                ErrorPhase::StateRead,
            )
        })?;
        let Some(snapshot) = snapshot else {
            drop(transaction);
            return Ok(None);
        };
        let mut record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.client_message_id() != client_message_id {
            return Err(SdkError::conflict(
                "prompt mutation belongs to another generation",
            ));
        }
        let original = record.clone();
        let result = mutation(&mut record)?;
        if record == original {
            drop(transaction);
            return Ok(Some(result));
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record)
                    .map_err(|_| SdkError::invalid_state("prompt mutation could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt mutation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt mutation could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(Some(result))
    }

    pub(crate) async fn observe_prompt_turn(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
    ) -> Result<CodexToolAdmission, SdkError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt observation could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt state could not be loaded",
                ErrorPhase::StateRead,
            )
        })?;
        let Some(snapshot) = snapshot else {
            drop(transaction);
            return Ok(CodexToolAdmission::Suppress);
        };
        let mut record = Self::decode_prompt(snapshot, conversation_id)?;
        let original = record.clone();
        let admission = record.observe_tool_turn(turn_id)?;
        if record == original {
            drop(transaction);
            return Ok(admission);
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record)
                    .map_err(|_| SdkError::invalid_state("prompt state could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt observation could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(admission)
    }

    pub(crate) async fn confirm_historical_terminal(
        &self,
        conversation_id: &ConversationId,
        expected: Option<&CodexPromptRecord>,
    ) -> Result<bool, SdkError> {
        let snapshot = self
            .storage
            .store()
            .load(Self::prompt_key(conversation_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt history ownership could not be revalidated",
                    ErrorPhase::StateRead,
                )
            })?;
        let current = snapshot
            .map(|snapshot| Self::decode_prompt(snapshot, conversation_id))
            .transpose()?;
        if current.as_ref() != expected {
            return Ok(false);
        }
        if current.is_none() {
            let context = self
                .prompt_contexts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(conversation_id)
                .map(CodexPromptContext::is_clearing);
            match context {
                Some(true) => return Ok(false),
                Some(false) => {
                    // A newly admitted generation may have installed its
                    // context after the load. Re-read before diagnosing loss.
                    if self
                        .storage
                        .store()
                        .load(Self::prompt_key(conversation_id)?)
                        .await
                        .map_err(|error| {
                            Self::storage_error(
                                error,
                                "prompt owner could not be revalidated",
                                ErrorPhase::StateRead,
                            )
                        })?
                        .is_some()
                    {
                        return Ok(false);
                    }
                    let context = self
                        .prompt_contexts
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .get(conversation_id)
                        .map(CodexPromptContext::is_clearing);
                    match context {
                        None => {}
                        Some(true) => return Ok(false),
                        Some(false) => {
                            return Err(SdkError::invalid_state(
                                "prompt state is missing for its active owner",
                            ));
                        }
                    }
                }
                None => {}
            }
        }
        Ok(true)
    }

    pub(crate) async fn observe_prompt_output(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
        text: &str,
        eligible: bool,
    ) -> Result<(), SdkError> {
        loop {
            match self
                .observe_prompt_output_attempt(conversation_id, turn_id, text, eligible)
                .await
            {
                Err(error)
                    if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                result => return result,
            }
        }
    }

    async fn observe_prompt_output_attempt(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
        text: &str,
        eligible: bool,
    ) -> Result<(), SdkError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt output transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let mut record = Self::decode_prompt(snapshot, conversation_id)?;
        let original = record.clone();
        record.observe_output(turn_id, text, eligible)?;
        if record == original {
            drop(transaction);
            return Ok(());
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record)
                    .map_err(|_| SdkError::invalid_state("prompt output could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt output could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt output transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    pub(crate) async fn prompt_terminal(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<Option<(String, Result<PromptResponse, SdkError>)>, SdkError> {
        let store = self.storage.store();
        let snapshot = store
            .load(Self::prompt_key(conversation_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.client_message_id() != client_message_id {
            return Err(SdkError::conflict(
                "prompt result belongs to another generation",
            ));
        }
        let Some(result) = record.terminal_result() else {
            if !self.runtime_open.load(Ordering::Acquire) {
                return Err(SdkError::closed("runtime is closed"));
            }
            return Ok(None);
        };
        if !record.cancellation_releasable() {
            if !self.runtime_open.load(Ordering::Acquire) {
                return Err(SdkError::closed("runtime is closed"));
            }
            return Ok(None);
        }
        if let Ok(response) = &result
            && let Some(schema) = record.output_schema()
        {
            let structured_output = self
                .prompt_validator(conversation_id, record.client_message_id(), schema)?
                .parse(response.output())
                .map_err(|_| SdkError::invalid_state("stored structured output is malformed"))?;
            if response.structured_output() != Some(&structured_output) {
                return Err(SdkError::invalid_state(
                    "stored structured output is malformed",
                ));
            }
        }
        let turn_id = record
            .turn_id()
            .ok_or_else(|| SdkError::invalid_state("terminal prompt has no prompt execution id"))?;
        Ok(Some((turn_id.to_owned(), result)))
    }

    pub(crate) async fn recover_completed_prompt(
        &self,
        conversation_id: &ConversationId,
        output: Option<String>,
    ) -> Result<PromptResponse, SdkError> {
        let output = output.ok_or_else(|| {
            SdkError::not_yet_implemented("completed without a source-backed portable final answer")
        })?;
        let record = self
            .prompt_record(conversation_id)
            .await?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let structured_output = record
            .output_schema()
            .map(|schema| {
                self.prompt_validator(conversation_id, record.client_message_id(), schema)?
                    .parse(&output)
            })
            .transpose()?;
        Ok(PromptResponse::from_parts(output, structured_output, None))
    }

    pub(crate) async fn prompt_record(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<Option<CodexPromptRecord>, SdkError> {
        let store = self.storage.store();
        store
            .load(Self::prompt_key(conversation_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .map(|snapshot| Self::decode_prompt(snapshot, conversation_id))
            .transpose()
    }

    pub(crate) async fn terminalize_prompt(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
        result: &Result<PromptResponse, SdkError>,
    ) -> Result<(), SdkError> {
        match self
            .terminalize_prompt_with(conversation_id, turn_id, |record| {
                let record =
                    record.ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
                record.terminal(turn_id, result)?;
                Ok(true)
            })
            .await?
        {
            CodexTerminalDisposition::Applied => Ok(()),
            CodexTerminalDisposition::Reconcile(_) => {
                Err(SdkError::invalid_state("foreground prompt lost its owner"))
            }
        }
    }

    pub(crate) async fn terminalize_observed_prompt(
        &self,
        conversation_id: &ConversationId,
        terminal: CodexPromptTerminal,
        expected_generation: Option<&str>,
    ) -> Result<CodexTerminalDisposition, SdkError> {
        let turn_id = terminal.turn_id().to_owned();
        let durable_turn_id = turn_id.clone();
        self.terminalize_prompt_with(conversation_id, &turn_id, move |record| {
            let Some(record) = record else {
                return Ok(false);
            };
            let matches = match expected_generation {
                Some(generation) => {
                    record.client_message_id() == generation
                        && record
                            .turn_id()
                            .is_none_or(|owned| owned == durable_turn_id)
                }
                None => record.turn_id() == Some(durable_turn_id.as_str()),
            };
            if !matches {
                return Ok(false);
            }
            let output = record.output().map(str::to_owned);
            let structured_output = match record
                .output_schema()
                .filter(|_| terminal.requires_structured_output())
            {
                Some(schema) => {
                    let validator =
                        self.prompt_validator(conversation_id, record.client_message_id(), schema)?;
                    output
                        .as_deref()
                        .map(|output| validator.parse(output))
                        .transpose()
                }
                None => Ok(None),
            };
            let result = match structured_output {
                Ok(structured_output) => terminal.clone().into_result(output, structured_output),
                Err(error) => Err(error),
            };
            record.terminal(&durable_turn_id, &result)?;
            Ok(true)
        })
        .await
    }

    async fn terminalize_prompt_with(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
        mut terminalize: impl FnMut(Option<&mut CodexPromptRecord>) -> Result<bool, SdkError>,
    ) -> Result<CodexTerminalDisposition, SdkError> {
        let mut reconciled_interaction = false;
        loop {
            match self
                .terminalize_prompt_attempt(
                    conversation_id,
                    turn_id,
                    &mut terminalize,
                    &mut reconciled_interaction,
                )
                .await
            {
                Ok(CodexTerminalDisposition::Applied) => {
                    if reconciled_interaction {
                        self.notifications.finish_reconciliation(conversation_id);
                    } else {
                        self.notifications.wake_receivers();
                    }
                    self.prompt_changes
                        .send_modify(|version| *version = version.wrapping_add(1));
                    return Ok(CodexTerminalDisposition::Applied);
                }
                Ok(reconcile @ CodexTerminalDisposition::Reconcile(_)) => return Ok(reconcile),
                Err(error)
                    if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout)
                        && error.context().phase != ErrorPhase::Cleanup
                        && error.supplementary().is_empty() =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn terminalize_prompt_attempt(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
        terminalize: &mut impl FnMut(Option<&mut CodexPromptRecord>) -> Result<bool, SdkError>,
        reconciled_interaction: &mut bool,
    ) -> Result<CodexTerminalDisposition, SdkError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let continuations = Self::continuations(store, conversation_id)
            .await?
            .into_iter()
            .filter(|(_, continuation)| continuation.turn_id() == Some(turn_id))
            .collect::<Vec<_>>();
        *reconciled_interaction |= !continuations.is_empty();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt terminal transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt state could not be loaded",
                ErrorPhase::StateRead,
            )
        })?;
        let mut record = snapshot
            .map(|snapshot| Self::decode_prompt(snapshot, conversation_id))
            .transpose()?;
        let original = record.clone();
        if !terminalize(record.as_mut())? {
            let primary = async {
                for (continuation_key, _) in &continuations {
                    if let Some(snapshot) = transaction
                        .load(continuation_key.clone())
                        .await
                        .map_err(|error| {
                            Self::storage_error(
                                error,
                                "historical continuation could not be loaded",
                                ErrorPhase::StateRead,
                            )
                        })?
                    {
                        let continuation = Self::decode_continuation(snapshot, conversation_id)?;
                        if continuation.turn_id() == Some(turn_id) {
                            return Err(SdkError::invalid_state(
                                "historical prompt still owns an interaction continuation",
                            ));
                        }
                    }
                }
                Ok(())
            }
            .await
            .err();
            if let Err(error) = transaction.rollback().await {
                let cleanup = Self::storage_error(
                    error,
                    "prompt observation cleanup failed",
                    ErrorPhase::Cleanup,
                );
                return Err(match primary {
                    Some(primary) => primary.with_supplementary(cleanup),
                    None => cleanup,
                });
            }
            if let Some(primary) = primary {
                return Err(primary);
            }
            return Ok(CodexTerminalDisposition::Reconcile(record.map(Box::new)));
        }
        let record = record.ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        Self::persist_native_selection_in_transaction(&mut transaction, conversation_id, &record)
            .await?;
        if Some(&record) != original.as_ref() {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    serde_json::to_value(record).map_err(|_| {
                        SdkError::invalid_state("prompt terminal could not be encoded")
                    })?,
                ))
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "prompt terminal could not be stored",
                        ErrorPhase::StateWrite,
                    )
                })?;
        }
        for (continuation_key, continuation) in continuations {
            let snapshot = transaction
                .load(continuation_key.clone())
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "continuation could not be loaded",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| SdkError::invalid_state("continuation is missing"))?;
            if Self::decode_continuation(snapshot, conversation_id)? != continuation {
                return Err(SdkError::invalid_state(
                    "continuation changed during prompt terminalization",
                ));
            }
            if Self::terminal_settlement(continuation.state()).is_none() {
                NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                    &mut transaction,
                    conversation_id,
                    continuation.notification_id(),
                    ResolutionSettlement::Cancelled,
                )
                .await
                .map_err(NotificationSettlementError::into_reconciliation_sdk)?;
            }
            transaction
                .delete(continuation_key)
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "terminal continuation could not be removed",
                        ErrorPhase::StateWrite,
                    )
                })?;
        }
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt terminal transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(CodexTerminalDisposition::Applied)
    }

    fn prompt_validator(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        schema: &serde_json::Value,
    ) -> Result<CodexStructuredOutput, SdkError> {
        let mut validators = self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let context = validators
            .entry(conversation_id.clone())
            .or_insert_with(|| CodexPromptContext::new(client_message_id.to_owned(), None, None));
        if !context.owns(client_message_id) {
            return Err(SdkError::conflict(
                "structured validator belongs to another generation",
            ));
        }
        context.validator(schema)
    }

    pub(crate) async fn clear_prompt(
        &self,
        conversation_id: &ConversationId,
        turn_id: &str,
    ) -> Result<(), SdkError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt cleanup could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt state could not be loaded",
                ErrorPhase::StateRead,
            )
        })?;
        let Some(snapshot) = snapshot else {
            drop(transaction);
            let mut contexts = self
                .prompt_contexts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(context) = contexts.get(conversation_id) {
                if !context.clears_turn(turn_id) {
                    return Err(SdkError::invalid_state(
                        "prompt cleanup lost its matching durable owner",
                    ));
                }
                contexts.remove(conversation_id);
            }
            return Ok(());
        };
        let record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.turn_id() != Some(turn_id) {
            return Err(SdkError::conflict(
                "prompt cleanup belongs to another generation",
            ));
        }
        if !record.clearable() {
            return Err(SdkError::busy(
                "prompt is not terminal with settled waiter and cancellation state",
            ));
        }
        if let Some(context) = self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(conversation_id)
        {
            context.begin_clear(record.client_message_id(), turn_id)?;
        }
        transaction.delete(key).await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt state could not be cleared",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt cleanup could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        let mut contexts = self
            .prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(context) = contexts.get(conversation_id) {
            if !context.owns(record.client_message_id()) || !context.clears_turn(turn_id) {
                return Err(SdkError::conflict(
                    "prompt cleanup context belongs to another generation",
                ));
            }
            contexts.remove(conversation_id);
        }
        Ok(())
    }

    pub(crate) async fn repair_reopen_prompt(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<(), SdkError> {
        let store = self.storage.store();
        let continuations = Self::continuations(store, conversation_id).await?;
        let had_continuations = !continuations.is_empty();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt reconciliation could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let key = Self::prompt_key(conversation_id)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("prompt state is missing"))?;
        let record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.client_message_id() != client_message_id {
            return Err(SdkError::conflict(
                "prompt reconciliation belongs to another generation",
            ));
        }
        for (continuation_key, continuation) in continuations {
            if continuation.is_unresolved() {
                NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                    &mut transaction,
                    conversation_id,
                    continuation.notification_id(),
                    ResolutionSettlement::Cancelled,
                )
                .await
                .map_err(NotificationSettlementError::into_reconciliation_sdk)?;
                transaction
                    .put(storage_sdk::state::record::StateRecord::new(
                        continuation_key,
                        serde_json::to_value(
                            continuation.with_state(CodexContinuationState::Cancelled)?,
                        )
                        .map_err(|_| {
                            SdkError::invalid_state("cancelled continuation could not be encoded")
                        })?,
                    ))
                    .await
                    .map_err(|error| {
                        Self::storage_error(
                            error,
                            "cancelled continuation could not be stored",
                            ErrorPhase::StateWrite,
                        )
                    })?;
            }
        }
        transaction.delete(key).await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt state could not be repaired",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "prompt reconciliation could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        if had_continuations {
            self.notifications.finish_reconciliation(conversation_id);
        }
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        self.prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(conversation_id);
        Ok(())
    }

    pub(crate) async fn request_abort(
        &self,
        conversation_id: &ConversationId,
        deadline: tokio::time::Instant,
    ) -> Result<CodexAbort, SdkError> {
        loop {
            match self.request_abort_transaction(conversation_id).await {
                Ok(abort) => return Ok(abort),
                Err(CodexAbortRequestError::TransactionConflict) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(SdkError::timeout("prompt cancellation deadline elapsed"));
                    }
                    tokio::task::yield_now().await;
                }
                Err(CodexAbortRequestError::Failure(error)) => return Err(error),
            }
        }
    }

    async fn request_abort_transaction(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<CodexAbort, CodexAbortRequestError> {
        let key = Self::prompt_key(conversation_id)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            CodexAbortRequestError::storage(
                error,
                "cancellation could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            CodexAbortRequestError::storage(
                error,
                "prompt state could not be loaded",
                ErrorPhase::StateRead,
            )
        })?
        else {
            drop(transaction);
            return Ok(CodexAbort::Idle);
        };
        let mut record = Self::decode_prompt(snapshot, conversation_id)?;
        let client_message_id = record.client_message_id().to_owned();
        let turn_id = record.turn_id().map(str::to_owned);
        if record.is_terminal() && record.cancellation_releasable() {
            drop(transaction);
            return Ok(CodexAbort::Join {
                client_message_id,
                turn_id,
            });
        }
        if !record.request_cancellation()? {
            drop(transaction);
            self.prompt_changes
                .send_modify(|version| *version = version.wrapping_add(1));
            return Ok(CodexAbort::Join {
                client_message_id,
                turn_id,
            });
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record).map_err(|_| {
                    SdkError::invalid_state("cancellation state could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                CodexAbortRequestError::storage(
                    error,
                    "cancellation state could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        let commit = transaction.commit().await;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        commit.map_err(|error| {
            CodexAbortRequestError::storage(
                error,
                "cancellation could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(CodexAbort::Join {
            client_message_id,
            turn_id,
        })
    }

    pub(crate) async fn claim_abort(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        retry_ambiguous: bool,
    ) -> Result<Option<String>, SdkError> {
        self.mutate_prompt(conversation_id, client_message_id, |record| {
            record.claim_cancellation(retry_ambiguous)
        })
        .await
    }

    pub(crate) async fn settle_abort(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
        turn_id: &str,
        ambiguous: bool,
    ) -> Result<(), SdkError> {
        self.mutate_prompt(conversation_id, client_message_id, |record| {
            record.settle_cancellation(turn_id, ambiguous)
        })
        .await
    }

    pub(crate) async fn abort_complete(
        &self,
        conversation_id: &ConversationId,
        client_message_id: &str,
    ) -> Result<bool, SdkError> {
        let store = self.storage.store();
        let Some(snapshot) = store
            .load(Self::prompt_key(conversation_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "prompt state could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
        else {
            return Ok(true);
        };
        let record = Self::decode_prompt(snapshot, conversation_id)?;
        if record.client_message_id() != client_message_id {
            return Err(SdkError::conflict(
                "cancellation target changed before settlement",
            ));
        }
        Ok(false)
    }

    pub(crate) fn retain_cleanup(&self, cleanup: CodexCleanup) {
        self.retained_cleanups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(cleanup);
    }

    pub(crate) fn take_retained_cleanups(&self) -> Vec<CodexCleanup> {
        std::mem::take(
            &mut *self
                .retained_cleanups
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    pub(crate) async fn create(
        &self,
        info: &AgentInfo,
        attachment: &CodexAttachment,
    ) -> Result<(), SdkError> {
        self.validate_attachment_owner(info, attachment)?;
        let key = Self::key(attachment.agent_id())?;
        let value = serde_json::to_value(attachment)
            .map_err(|_| SdkError::invalid_state("attachment could not be encoded"))?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "readiness transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        if transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "attachment could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .is_some()
        {
            return Err(SdkError::conflict("attachment already exists"));
        }
        self.storage
            .create_agent_in_transaction(&mut transaction, info)
            .await
            .map_err(crate::agent_storage::AgentStorageCreationError::into_sdk)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(key, value))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "attachment could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "readiness transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(())
    }

    pub(crate) async fn verify(&self, expected: &CodexAttachment) -> Result<(), SdkError> {
        if expected.agent_id() != &self.agent_id {
            return Err(SdkError::invalid_state(
                "attachment owner does not match agent identity",
            ));
        }
        let key = Self::key(expected.agent_id())?;
        let store = self.storage.store();
        let snapshot = store
            .load(key)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "attachment could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::conflict("durable agent is not attached to"))?;
        let actual: CodexAttachment =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| SdkError::invalid_state("stored attachment is malformed"))?;
        actual.validate()?;
        if actual.agent_id() != expected.agent_id() {
            return Err(SdkError::invalid_state(
                "stored attachment contradicts its durable owner",
            ));
        }
        if actual != *expected {
            return Err(SdkError::conflict(
                "attachment or instruction policy changed",
            ));
        }
        Ok(())
    }

    pub(crate) async fn create_workspace(
        &self,
        agent_id: &AgentId,
        request: &NewWorkspace,
    ) -> Result<WorkspaceRecord, SdkError> {
        let record = WorkspaceRecord::new(agent_id.clone(), WorkspaceId::new(), request);
        let key = WorkspaceRecord::key(agent_id, record.id())?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "workspace transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        if transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "workspace identity could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .is_some()
        {
            return Err(SdkError::conflict("workspace identity already exists"));
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                record.encode()?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "workspace could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "workspace transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(record)
    }

    pub(crate) async fn workspaces(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<WorkspaceRecord>, SdkError> {
        let store = self.storage.store();
        WorkspaceRecord::list(store, agent_id).await
    }

    pub(crate) async fn workspace(
        &self,
        agent_id: &AgentId,
        id: &WorkspaceId,
    ) -> Result<WorkspaceRecord, SdkError> {
        let key = WorkspaceRecord::key(agent_id, id)?;
        let store = self.storage.store();
        let snapshot = store
            .load(key)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "workspace could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        WorkspaceRecord::decode(snapshot, agent_id, Some(id))
    }

    pub(crate) async fn prepare_thread_creation(
        &self,
        creation: &CodexThreadCreation,
    ) -> Result<(), SdkError> {
        creation.validate()?;
        if creation.agent_id() != &self.agent_id {
            return Err(SdkError::invalid_state(
                "conversation creation has a foreign agent owner",
            ));
        }
        let record = creation.conversation();
        let creation_key = Self::thread_creation_key(creation)?;
        let store = self.storage.store();
        if Self::all_thread_creations(store)
            .await?
            .into_iter()
            .any(|stored| stored.thread_source() == creation.thread_source())
        {
            return Err(SdkError::conflict(
                "conversation creation source already exists",
            ));
        }
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(&self.agent_id, record.workspace_id())?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation workspace could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let workspace =
            WorkspaceRecord::decode(workspace, &self.agent_id, Some(record.workspace_id()))?;
        if workspace.revision() != record.workspace_revision() {
            return Err(SdkError::conflict("workspace handle is stale"));
        }
        if transaction
            .load(creation_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .is_some()
            || transaction
                .load(ConversationRecord::key(record.workspace_id(), record.id())?)
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "conversation could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
            || transaction
                .load(Self::binding_key(record.workspace_id(), record.id())?)
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "conversation binding could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
        {
            return Err(SdkError::conflict(
                "conversation creation identity already exists",
            ));
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                creation_key,
                serde_json::to_value(creation).map_err(|_| {
                    SdkError::invalid_state("conversation creation could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map(|_| ()).map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation could not commit",
                ErrorPhase::StateCommit,
            )
        })
    }

    pub(crate) async fn mark_thread_creation_knowledge_ready(
        &self,
        creation: &CodexThreadCreation,
    ) -> Result<CodexThreadCreation, SdkError> {
        let key = Self::thread_creation_key(creation)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation update could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation creation is missing"))?;
        let actual = Self::decode_thread_creation(snapshot)?;
        if actual != *creation {
            return Err(SdkError::conflict(
                "conversation creation changed before knowledge publication",
            ));
        }
        let ready = actual.with_knowledge_ready()?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(&ready).map_err(|_| {
                    SdkError::invalid_state("conversation creation could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation update could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(ready)
    }

    pub(crate) async fn mark_thread_creation_dispatch_started(
        &self,
        creation: &CodexThreadCreation,
    ) -> Result<CodexThreadCreation, SdkError> {
        let key = Self::thread_creation_key(creation)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation dispatch transition could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation creation is missing"))?;
        let actual = Self::decode_thread_creation(snapshot)?;
        if actual != *creation {
            return Err(SdkError::conflict(
                "conversation creation changed before runtime dispatch",
            ));
        }
        let dispatching = actual.with_dispatch_started()?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(&dispatching).map_err(|_| {
                    SdkError::invalid_state("conversation creation could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation dispatch transition could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(dispatching)
    }

    pub(crate) async fn thread_creations(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<CodexThreadCreation>, SdkError> {
        let store = self.storage.store();
        Self::all_thread_creations(store).await.map(|creations| {
            creations
                .into_iter()
                .filter(|creation| creation.agent_id() == agent_id)
                .collect()
        })
    }

    pub(crate) async fn thread_creation_by_source(
        &self,
        agent_id: &AgentId,
        thread_source: &str,
    ) -> Result<CodexThreadCreation, SdkError> {
        let store = self.storage.store();
        let mut matches = Self::all_thread_creations(store)
            .await?
            .into_iter()
            .filter(|creation| {
                creation.agent_id() == agent_id && creation.thread_source() == thread_source
            });
        let creation = matches
            .next()
            .ok_or_else(|| SdkError::invalid_state("conversation creation is missing"))?;
        if matches.next().is_some() {
            return Err(SdkError::invalid_state(
                "conversation creation source is duplicated",
            ));
        }
        Ok(creation)
    }

    pub(crate) async fn wait_for_thread_creation_observation(
        &self,
        agent_id: &AgentId,
        thread_source: &str,
        deadline: tokio::time::Instant,
    ) -> Result<String, SdkError> {
        loop {
            match self
                .thread_creation_by_source(agent_id, thread_source)
                .await
            {
                Ok(creation) => {
                    if let Some(thread_id) = creation.native_thread_id() {
                        return Ok(thread_id.to_owned());
                    }
                }
                Err(error)
                    if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout)
                        && tokio::time::Instant::now() < deadline => {}
                Err(error) => return Err(error),
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(SdkError::busy(
                    "conversation creation remains durably unresolved",
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(25).min(remaining)).await;
        }
    }

    pub(crate) async fn observe_thread_creation(
        &self,
        agent_id: &AgentId,
        thread_source: &str,
        thread_id: &str,
    ) -> Result<bool, SdkError> {
        let store = self.storage.store();
        let mut matches = Self::all_thread_creations(store)
            .await?
            .into_iter()
            .filter(|creation| {
                creation.agent_id() == agent_id && creation.thread_source() == thread_source
            });
        let Some(expected) = matches.next() else {
            return Ok(false);
        };
        if matches.next().is_some() {
            return Err(SdkError::invalid_state(
                "conversation creation source is duplicated",
            ));
        }
        let key = Self::thread_creation_key(&expected)?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation observation could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = match transaction.load(key.clone()).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                transaction.rollback().await.map_err(|error| {
                    Self::storage_error(
                        error,
                        "conversation creation observation could not roll back",
                        ErrorPhase::StateWrite,
                    )
                })?;
                return Ok(false);
            }
            Err(error) => {
                let conflict = matches!(
                    error,
                    storage_sdk::StateStoreError::TransactionConflict { .. }
                );
                let primary = Self::storage_error(
                    error,
                    "conversation creation could not be loaded",
                    ErrorPhase::StateRead,
                );
                if let Err(error) = transaction.rollback().await {
                    return Err(primary.with_supplementary(Self::storage_error(
                        error,
                        "conversation creation observation could not roll back",
                        ErrorPhase::Cleanup,
                    )));
                }
                return if conflict {
                    self.reconcile_thread_creation_observation(&expected, thread_id, primary)
                        .await
                } else {
                    Err(primary)
                };
            }
        };
        let actual = Self::decode_thread_creation(snapshot)?;
        if !actual.matches_obligation(&expected, thread_id) {
            return Err(SdkError::conflict(
                "conversation creation changed during runtime observation",
            ));
        }
        let observed = actual.with_native_thread_id(thread_id)?;
        if observed == actual {
            transaction.rollback().await.map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation observation could not roll back",
                    ErrorPhase::StateWrite,
                )
            })?;
            return Ok(true);
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(observed).map_err(|_| {
                    SdkError::invalid_state("conversation creation could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        match transaction.commit().await {
            Ok(()) => Ok(true),
            Err(error) => {
                let conflict = matches!(
                    error,
                    storage_sdk::StateStoreError::TransactionConflict { .. }
                );
                let primary = Self::storage_error(
                    error,
                    "conversation creation could not commit",
                    ErrorPhase::StateCommit,
                );
                if conflict {
                    self.reconcile_thread_creation_observation(&expected, thread_id, primary)
                        .await
                } else {
                    Err(primary)
                }
            }
        }
    }

    async fn reconcile_thread_creation_observation(
        &self,
        expected: &CodexThreadCreation,
        thread_id: &str,
        primary: SdkError,
    ) -> Result<bool, SdkError> {
        let key = Self::thread_creation_key(expected)?;
        let snapshot = match self.storage.store().load(key).await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => return Ok(false),
            Err(error) => {
                return Err(primary.with_supplementary(Self::storage_error(
                    error,
                    "conversation creation could not be reconciled",
                    ErrorPhase::StateRead,
                )));
            }
        };
        let current = match Self::decode_thread_creation(snapshot) {
            Ok(current) => current,
            Err(error) => {
                return Err(primary.with_supplementary(error.in_phase(ErrorPhase::StateRead)));
            }
        };
        if current.matches_obligation(expected, thread_id)
            && current.native_thread_id() == Some(thread_id)
        {
            Ok(true)
        } else {
            Err(primary)
        }
    }

    pub(crate) async fn complete_thread_creation(
        &self,
        creation: &CodexThreadCreation,
        binding: &CodexThreadBinding,
    ) -> Result<(), SdkError> {
        creation.validate()?;
        if !creation.dispatch_may_be_accepted() {
            return Err(SdkError::conflict(
                "conversation creation did not cross its durable dispatch boundary",
            ));
        }
        let agent_id = creation.agent_id();
        let record = creation.conversation();
        binding.validate()?;
        if binding.conversation_id() != record.id()
            || binding.workspace_id() != record.workspace_id()
            || binding.workspace_revision() != record.workspace_revision()
        {
            return Err(SdkError::invalid_state(
                "conversation binding contradicts portable conversation state",
            ));
        }
        let conversation_key = ConversationRecord::key(record.workspace_id(), record.id())?;
        let creation_key = Self::thread_creation_key(creation)?;
        let binding_key = Self::binding_key(record.workspace_id(), record.id())?;
        let route_key = Self::route_key(binding.thread_id())?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(agent_id, record.workspace_id())?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "workspace could not be checked for conversation",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(record.workspace_id()))?;
        if workspace.revision() != record.workspace_revision() {
            return Err(SdkError::conflict("workspace handle is stale"));
        }
        binding.verify_owner(record, &workspace, self.instructions())?;
        let stored_creation = transaction
            .load(creation_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation creation is missing"))?;
        if !Self::decode_thread_creation(stored_creation)?
            .matches_obligation(creation, binding.thread_id())
        {
            return Err(SdkError::conflict(
                "conversation creation changed before publication",
            ));
        }
        if transaction
            .load(conversation_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .is_some()
            || transaction
                .load(binding_key.clone())
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "conversation binding could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
            || transaction
                .load(route_key.clone())
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "conversation route could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
        {
            return Err(SdkError::conflict("conversation identity already exists"));
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                conversation_key,
                record.encode()?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        let value = serde_json::to_value(binding)
            .map_err(|_| SdkError::invalid_state("conversation binding could not be encoded"))?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                binding_key,
                value.clone(),
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation binding could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                route_key, value,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation route could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        NotificationStore::<S>::initialize_in_transaction(
            &mut transaction,
            record.workspace_id(),
            record.id(),
        )
        .await
        .map_err(NotificationInitializationError::into_sdk)?;
        transaction.delete(creation_key).await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation could not be completed",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(())
    }

    pub(crate) async fn abandon_thread_creation(
        &self,
        creation: &CodexThreadCreation,
    ) -> Result<(), SdkError> {
        let key = Self::thread_creation_key(creation)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation cleanup could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation creation is missing"))?;
        if Self::decode_thread_creation(snapshot)? != *creation {
            return Err(SdkError::conflict(
                "conversation creation changed before cleanup",
            ));
        }
        self.stage_knowledge_forget(
            &mut transaction,
            &KnowledgeOwnerId::Conversation {
                agent_id: creation.agent_id().clone(),
                workspace_id: creation.conversation().workspace_id().clone(),
                conversation_id: creation.conversation().id().clone(),
            },
        )
        .await
        .map_err(AgentStorageKnowledgeError::into_sdk)?;
        transaction.delete(key).await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation could not be removed",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction.commit().await.map(|_| ()).map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation cleanup could not commit",
                ErrorPhase::StateCommit,
            )
        })
    }

    pub(crate) async fn fail_thread_creation(
        &self,
        creation: &CodexThreadCreation,
        cleanup: &CodexCleanup,
    ) -> Result<(), SdkError> {
        creation.validate()?;
        cleanup.validate()?;
        if cleanup.agent_id() != creation.agent_id() {
            return Err(SdkError::invalid_state(
                "failed creation cleanup has a foreign agent owner",
            ));
        }
        let [thread_id] = cleanup.thread_ids() else {
            return Err(SdkError::invalid_state(
                "failed creation cleanup must own exactly one runtime conversation",
            ));
        };
        if !cleanup.matches_conversation_deletion(creation.conversation().id(), thread_id) {
            return Err(SdkError::invalid_state(
                "failed creation cleanup contradicts its conversation owner",
            ));
        }
        let creation_key = Self::thread_creation_key(creation)?;
        let cleanup_key = Self::cleanup_key(cleanup)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "failed creation transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(creation_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation creation is missing"))?;
        if !Self::decode_thread_creation(snapshot)?.matches_obligation(creation, thread_id) {
            return Err(SdkError::conflict(
                "conversation creation changed before failure cleanup",
            ));
        }
        self.stage_knowledge_forget(
            &mut transaction,
            &KnowledgeOwnerId::Conversation {
                agent_id: creation.agent_id().clone(),
                workspace_id: creation.conversation().workspace_id().clone(),
                conversation_id: creation.conversation().id().clone(),
            },
        )
        .await
        .map_err(AgentStorageKnowledgeError::into_sdk)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                cleanup_key,
                serde_json::to_value(cleanup).map_err(|_| {
                    SdkError::invalid_state("cleanup obligation could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "cleanup obligation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.delete(creation_key).await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation creation could not be removed",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction.commit().await.map(|_| ()).map_err(|error| {
            Self::storage_error(
                error,
                "failed creation could not commit",
                ErrorPhase::StateCommit,
            )
        })
    }

    pub(crate) async fn conversations(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ConversationRecord>, SdkError> {
        let store = self.storage.store();
        ConversationRecord::list(store, workspace_id).await
    }

    pub(crate) async fn conversation(
        &self,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<ConversationRecord, SdkError> {
        let key = ConversationRecord::key(workspace_id, id)?;
        let store = self.storage.store();
        let snapshot = store
            .load(key)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("conversation does not exist"))?;
        ConversationRecord::decode(snapshot, workspace_id, Some(id))
    }

    pub(crate) async fn binding(
        &self,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<CodexThreadBinding, SdkError> {
        let store = self.storage.store();
        Self::load_binding(store, workspace_id, id).await
    }

    pub(crate) async fn materialize_binding(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
        expected: &CodexThreadBinding,
        thread: &CodexThread,
    ) -> Result<CodexThreadBinding, SdkError> {
        let binding_key = Self::binding_key(workspace_id, conversation_id)?;
        let route_key = Self::route_key(expected.thread_id())?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation rebind transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(agent_id, workspace_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "rebound workspace could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(workspace_id))?;
        let conversation = transaction
            .load(ConversationRecord::key(workspace_id, conversation_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "rebound conversation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("conversation does not exist"))?;
        let conversation =
            ConversationRecord::decode(conversation, workspace_id, Some(conversation_id))?;
        let binding = transaction
            .load(binding_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation binding could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation binding is missing"))?;
        let binding = Self::decode_binding(binding, workspace_id, conversation_id)?;
        if &binding != expected {
            return Err(SdkError::conflict(
                "conversation binding changed during runtime rebind",
            ));
        }
        let route = transaction
            .load(route_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation route could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation route is missing"))?;
        if Self::decode_route(route, expected.thread_id())? != binding {
            return Err(SdkError::invalid_state(
                "conversation route contradicts its binding",
            ));
        }
        binding.verify_owner(&conversation, &workspace, self.instructions())?;
        binding.verify_native(thread)?;
        let materialized = binding.with_materialized_thread(thread, workspace.revision())?;
        let value = serde_json::to_value(&materialized)
            .map_err(|_| SdkError::invalid_state("rebound binding could not be encoded"))?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                binding_key,
                value.clone(),
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "rebound binding could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                route_key, value,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "rebound route could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "conversation rebind transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(materialized)
    }

    pub(crate) async fn workspace_has_unsettled_work(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<bool, SdkError> {
        let store = self.storage.store();
        if Self::all_thread_creations(store)
            .await?
            .iter()
            .any(|creation| {
                creation.agent_id() == &self.agent_id
                    && creation.conversation().workspace_id() == workspace_id
            })
        {
            return Ok(true);
        }
        let conversations = ConversationRecord::list(store, workspace_id).await?;
        for conversation in &conversations {
            if store
                .load(Self::prompt_key(conversation.id())?)
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "prompt state could not be checked",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
                || Self::continuations(store, conversation.id())
                    .await?
                    .iter()
                    .any(|(_, continuation)| continuation.is_unresolved())
            {
                return Ok(true);
            }
        }
        for conversation in conversations {
            match self
                .notifications
                .ensure_no_unresolved_interaction(conversation.id())
                .await
            {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::Busy => return Ok(true),
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    pub(crate) async fn conversation_for_thread(
        &self,
        agent_id: &AgentId,
        thread_id: &str,
    ) -> Result<Option<(ConversationRecord, CodexThreadBinding)>, SdkError> {
        let store = self.storage.store();
        let Some(snapshot) = store
            .load(Self::route_key(thread_id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation route could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
        else {
            return Ok(None);
        };
        let binding = Self::decode_route(snapshot, thread_id)?;
        let primary =
            Self::load_binding(store, binding.workspace_id(), binding.conversation_id()).await?;
        if primary != binding {
            return Err(SdkError::invalid_state(
                "conversation route contradicts its durable binding",
            ));
        }
        let workspace = store
            .load(WorkspaceRecord::key(agent_id, binding.workspace_id())?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "routed workspace could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("routed workspace is missing"))?;
        let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(binding.workspace_id()))?;
        let conversation = store
            .load(ConversationRecord::key(
                binding.workspace_id(),
                binding.conversation_id(),
            )?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "routed conversation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("routed conversation is missing"))?;
        let conversation = ConversationRecord::decode(
            conversation,
            binding.workspace_id(),
            Some(binding.conversation_id()),
        )?;
        binding.verify_owner(&conversation, &workspace, self.instructions())?;
        Ok(Some((conversation, binding)))
    }

    pub(crate) async fn reconcile_deleted_thread(
        &self,
        conversation: &ConversationRecord,
        binding: &CodexThreadBinding,
    ) -> Result<(), SdkError> {
        let store = self.storage.store();
        let continuations = Self::continuations(store, conversation.id()).await?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "runtime deletion could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let workspace = transaction
            .load(WorkspaceRecord::key(
                &self.agent_id,
                conversation.workspace_id(),
            )?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted conversation owner could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("deleted conversation workspace is missing"))?;
        let workspace =
            WorkspaceRecord::decode(workspace, &self.agent_id, Some(conversation.workspace_id()))?;
        let conversation_key =
            ConversationRecord::key(conversation.workspace_id(), conversation.id())?;
        let stored_conversation = transaction
            .load(conversation_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted conversation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("deleted conversation is missing"))?;
        if ConversationRecord::decode(
            stored_conversation,
            conversation.workspace_id(),
            Some(conversation.id()),
        )? != *conversation
        {
            return Err(SdkError::invalid_state(
                "deleted conversation changed during reconciliation",
            ));
        }
        let binding_key = Self::binding_key(conversation.workspace_id(), conversation.id())?;
        let stored_binding = transaction
            .load(binding_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted conversation binding could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("deleted conversation binding is missing"))?;
        if Self::decode_binding(
            stored_binding,
            conversation.workspace_id(),
            conversation.id(),
        )? != *binding
        {
            return Err(SdkError::invalid_state(
                "deleted conversation binding changed during reconciliation",
            ));
        }
        binding.verify_owner(conversation, &workspace, self.instructions())?;
        let route_key = Self::route_key(binding.thread_id())?;
        let route = transaction
            .load(route_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted conversation route could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("deleted conversation route is missing"))?;
        if Self::decode_route(route, binding.thread_id())? != *binding {
            return Err(SdkError::invalid_state(
                "deleted conversation route contradicts its binding",
            ));
        }

        for (key, continuation) in continuations {
            if Self::terminal_settlement(continuation.state()).is_none() {
                NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                    &mut transaction,
                    conversation.id(),
                    continuation.notification_id(),
                    ResolutionSettlement::Cancelled,
                )
                .await
                .map_err(NotificationSettlementError::into_reconciliation_sdk)?;
            }
            transaction.delete(key).await.map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted conversation continuation could not be removed",
                    ErrorPhase::StateWrite,
                )
            })?;
        }
        transaction
            .delete(conversation_key)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted conversation could not be removed",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.delete(binding_key).await.map_err(|error| {
            Self::storage_error(
                error,
                "deleted conversation binding could not be removed",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction
            .delete(Self::native_selection_key(conversation.id())?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "runtime selection could not be removed",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.delete(route_key).await.map_err(|error| {
            Self::storage_error(
                error,
                "deleted conversation route could not be removed",
                ErrorPhase::StateWrite,
            )
        })?;
        let prompt_key = Self::prompt_key(conversation.id())?;
        if transaction
            .load(prompt_key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted prompt could not be checked",
                    ErrorPhase::StateRead,
                )
            })?
            .is_some()
        {
            transaction.delete(prompt_key).await.map_err(|error| {
                Self::storage_error(
                    error,
                    "deleted prompt could not be removed",
                    ErrorPhase::StateWrite,
                )
            })?;
        }
        NotificationStore::<S>::delete_in_transaction(&mut transaction, conversation.id())
            .await
            .map_err(NotificationDeleteError::into_sdk)?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "runtime deletion could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.prompt_changes
            .send_modify(|version| *version = version.wrapping_add(1));
        self.prompt_contexts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(conversation.id());
        self.notifications
            .invalidate_conversation(conversation.id());
        Ok(())
    }

    fn require_interaction_admission(deadline: tokio::time::Instant) -> Result<(), SdkError> {
        if tokio::time::Instant::now() >= deadline {
            Err(
                SdkError::timeout("interaction publication deadline elapsed")
                    .in_phase(ErrorPhase::StateWrite),
            )
        } else {
            Ok(())
        }
    }

    pub(crate) async fn publish_interaction(
        &self,
        conversation_id: &ConversationId,
        request: CodexInteractionRequest,
        deadline: tokio::time::Instant,
    ) -> Result<NotificationId, SdkError> {
        let (request_id, thread_id, turn_id, kind, notification) = request.into_parts();
        kind.validate_notification(&notification)?;
        let store = self.storage.store();
        Self::require_interaction_admission(deadline)?;
        let mut matching = Self::continuations(store, conversation_id)
            .await?
            .into_iter()
            .filter(|(_, continuation)| {
                continuation.agent_id() == &self.agent_id
                    && continuation.request_id() == &request_id
            });
        let existing = matching.next();
        if matching.next().is_some() {
            return Err(SdkError::invalid_state(
                "server request identity is duplicated",
            ));
        }
        Self::require_interaction_admission(deadline)?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "interaction transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let staged = async {
            Self::require_interaction_admission(deadline)?;
            if let Some(turn_id) = turn_id.as_deref() {
                let prompt_key = Self::prompt_key(conversation_id)?;
                let snapshot = transaction
                    .load(prompt_key.clone())
                    .await
                    .map_err(|error| {
                        Self::storage_error(
                            error,
                            "prompt state could not be loaded",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .ok_or_else(|| {
                        SdkError::invalid_state("interaction has no active prompt owner")
                    })?;
                let mut prompt = Self::decode_prompt(snapshot, conversation_id)?;
                prompt.observe_turn(turn_id)?;
                Self::require_interaction_admission(deadline)?;
                transaction
                    .put(storage_sdk::state::record::StateRecord::new(
                        prompt_key,
                        serde_json::to_value(prompt).map_err(|_| {
                            SdkError::invalid_state("prompt state could not be encoded")
                        })?,
                    ))
                    .await
                    .map_err(|error| {
                        Self::storage_error(
                            error,
                            "prompt state could not be stored",
                            ErrorPhase::StateWrite,
                        )
                    })?;
            }
            Self::require_interaction_admission(deadline)?;
            if let Some((key, _)) = existing {
                let snapshot = transaction
                    .load(key)
                    .await
                    .map_err(|error| {
                        Self::storage_error(
                            error,
                            "continuation could not be loaded",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .ok_or_else(|| {
                        SdkError::protocol("server request identity no longer exists")
                    })?;
                let existing = Self::decode_continuation(snapshot, conversation_id)?;
                if existing.state() != CodexContinuationState::Pending {
                    return Err(SdkError::protocol(
                        "server request identity was reused after settlement",
                    ));
                }
                if existing.agent_id() != &self.agent_id
                    || existing.request_id() != &request_id
                    || existing.thread_id() != thread_id
                    || existing.turn_id() != turn_id.as_deref()
                    || existing.kind() != &kind
                {
                    return Err(SdkError::protocol("server request identity was reused"));
                }
                Self::require_interaction_admission(deadline)?;
                NotificationStore::<S>::ensure_interaction_matches_in_transaction(
                    &mut transaction,
                    conversation_id,
                    existing.notification_id(),
                    &notification,
                )
                .await
                .map_err(NotificationInteractionCheckError::into_match_sdk)?;
                return Ok((existing.notification_id().clone(), false));
            }
            let (notification_id, _) = NotificationStore::<S>::publish_in_transaction(
                &mut transaction,
                conversation_id,
                notification,
            )
            .await?;
            let continuation = CodexContinuation::new(
                self.agent_id.clone(),
                conversation_id.clone(),
                notification_id.clone(),
                request_id,
                thread_id,
                turn_id,
                kind,
            )?;
            let key = Self::continuation_key(&continuation)?;
            Self::require_interaction_admission(deadline)?;
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    serde_json::to_value(continuation).map_err(|_| {
                        SdkError::invalid_state("continuation could not be encoded")
                    })?,
                ))
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "continuation could not be stored",
                        ErrorPhase::StateWrite,
                    )
                })?;
            Ok::<_, SdkError>((notification_id, true))
        }
        .await;
        // A stage admitted before expiry is awaited to completion. Deadline
        // expiry prevents commit admission and explicitly rolls back staging.
        let staged = staged.and_then(|publication| {
            Self::require_interaction_admission(deadline).map(|()| publication)
        });
        let (notification_id, inserted) = match staged {
            Ok(publication) => publication,
            Err(error) => {
                return Err(
                    error.with_cleanup_result(transaction.rollback().await.map_err(|cleanup| {
                        Self::storage_error(
                            cleanup,
                            "interaction transaction could not close",
                            ErrorPhase::Cleanup,
                        )
                    })),
                );
            }
        };
        if !inserted {
            transaction.rollback().await.map_err(|error| {
                Self::storage_error(
                    error,
                    "interaction verification transaction could not close",
                    ErrorPhase::Cleanup,
                )
            })?;
            return Ok(notification_id);
        }
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "interaction transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        self.notifications.wake_receivers();
        Ok(notification_id)
    }

    pub(crate) async fn continuation(
        &self,
        conversation_id: &ConversationId,
        notification_id: &NotificationId,
    ) -> Result<CodexContinuation, SdkError> {
        let store = self.storage.store();
        let mut matches = Self::continuations(store, conversation_id)
            .await?
            .into_iter()
            .map(|(_, continuation)| continuation)
            .filter(|continuation| continuation.notification_id() == notification_id);
        let continuation = matches
            .next()
            .ok_or_else(|| SdkError::not_found("notification continuation does not exist"))?;
        if matches.next().is_some() {
            return Err(SdkError::invalid_state(
                "notification continuation identity is duplicated",
            ));
        }
        if continuation.state() != CodexContinuationState::Pending {
            return Err(SdkError::conflict(
                "notification interaction is already settled or recovery-owned",
            ));
        }
        Ok(continuation)
    }

    pub(crate) async fn settle_interaction(
        &self,
        intent: &ResolutionIntent,
        continuation: &CodexContinuation,
        settlement: ResolutionSettlement,
    ) -> Result<ResolutionSettlement, SdkError> {
        if continuation.conversation_id() != intent.conversation_id()
            || continuation.notification_id() != intent.notification_id()
        {
            return Err(SdkError::invalid_state(
                "resolution intent contradicts its continuation",
            ));
        }
        let key = Self::continuation_key(continuation)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "interaction settlement could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "continuation could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("continuation is missing"))?;
        let actual = Self::decode_continuation(snapshot, intent.conversation_id())?;
        if !actual.has_same_request(continuation) {
            return Err(SdkError::invalid_state(
                "continuation changed during resolution",
            ));
        }
        if let Some(actual) = Self::terminal_settlement(actual.state()) {
            return Ok(actual);
        }
        if actual.state() == CodexContinuationState::Ambiguous {
            return Ok(ResolutionSettlement::Ambiguous);
        }
        let next = actual.with_state(Self::continuation_state(settlement))?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(next)
                    .map_err(|_| SdkError::invalid_state("continuation could not be encoded"))?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "continuation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        let disposition = NotificationStore::<S>::settle_resolution_in_transaction(
            &mut transaction,
            intent,
            settlement,
        )
        .await?;
        let actual = match disposition {
            ResolutionDisposition::Applied(actual) => actual,
            ResolutionDisposition::AlreadyAccepted => {
                return Err(SdkError::invalid_state(
                    "continuation and interaction settlement disagree",
                ));
            }
        };
        self.notifications
            .reclaim_terminal_in_transaction(&mut transaction, intent.conversation_id())
            .await?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "interaction settlement could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        if actual == ResolutionSettlement::Ambiguous {
            self.notifications.wake_receivers();
        } else {
            self.notifications
                .finish_reconciliation(intent.conversation_id());
        }
        Ok(actual)
    }

    pub(crate) async fn cancel_lost_continuations(
        &self,
        agent_id: &AgentId,
    ) -> Result<(), SdkError> {
        let store = self.storage.store();
        let continuations = Self::all_continuations(store)
            .await?
            .into_iter()
            .filter(|(_, continuation)| continuation.agent_id() == agent_id)
            .collect::<Vec<_>>();

        for (key, continuation) in continuations {
            let store = self.storage.store();
            let mut transaction = store.begin_transaction().await.map_err(|error| {
                Self::storage_error(
                    error,
                    "continuation reconciliation could not begin",
                    ErrorPhase::StateWrite,
                )
            })?;
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    Self::storage_error(
                        error,
                        "continuation could not be loaded",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| SdkError::invalid_state("continuation is missing"))?;
            if Self::decode_continuation(snapshot, continuation.conversation_id())? != continuation
            {
                return Err(SdkError::invalid_state(
                    "continuation changed during reconciliation",
                ));
            }
            if Self::terminal_settlement(continuation.state()).is_none() {
                NotificationStore::<S>::reconcile_native_resolution_in_transaction(
                    &mut transaction,
                    continuation.conversation_id(),
                    continuation.notification_id(),
                    ResolutionSettlement::Cancelled,
                )
                .await
                .map_err(NotificationSettlementError::into_reconciliation_sdk)?;
            }
            transaction.delete(key).await.map_err(|error| {
                Self::storage_error(
                    error,
                    "lost continuation could not be removed",
                    ErrorPhase::StateWrite,
                )
            })?;
            transaction.commit().await.map_err(|error| {
                Self::storage_error(
                    error,
                    "continuation reconciliation could not commit",
                    ErrorPhase::StateCommit,
                )
            })?;
            self.notifications
                .finish_reconciliation(continuation.conversation_id());
        }
        Ok(())
    }

    pub(crate) async fn update_workspace(
        &self,
        agent_id: &AgentId,
        request: &UpdateWorkspaceRequest,
    ) -> Result<WorkspaceRecord, SdkError> {
        let store = self.storage.store();
        let conversations = ConversationRecord::list(store, request.id()).await?;
        for conversation in &conversations {
            if Self::continuations(store, conversation.id())
                .await?
                .iter()
                .any(|(_, continuation)| continuation.is_unresolved())
            {
                return Err(SdkError::busy(
                    "workspace contains an unresolved interaction",
                ));
            }
        }
        let workspace_key = WorkspaceRecord::key(agent_id, request.id())?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::workspace_update_error(
                error,
                "workspace update transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(workspace_key.clone())
            .await
            .map_err(|error| {
                Self::workspace_update_error(
                    error,
                    "workspace could not be loaded for update",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let current = WorkspaceRecord::decode(snapshot, agent_id, Some(request.id()))?;
        let replacement = current.replace(request)?;
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                workspace_key,
                replacement.encode()?,
            ))
            .await
            .map_err(|error| {
                Self::workspace_update_error(
                    error,
                    "workspace update could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        for listed in conversations {
            if transaction
                .load(Self::prompt_key(listed.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "prompt state could not be checked for update",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
            {
                return Err(SdkError::busy(
                    "workspace contains active or unreconciled prompt work",
                ));
            }
            NotificationStore::<S>::ensure_no_unresolved_interaction_in_transaction(
                &mut transaction,
                listed.id(),
            )
            .await?;
            let key = ConversationRecord::key(request.id(), listed.id())?;
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation could not be loaded for update",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| SdkError::invalid_state("conversation disappeared during update"))?;
            let current_conversation =
                ConversationRecord::decode(snapshot, request.id(), Some(listed.id()))?;
            let conversation =
                current_conversation.with_workspace_revision(replacement.revision())?;
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    conversation.encode()?,
                ))
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation revision could not be stored",
                        ErrorPhase::StateWrite,
                    )
                })?;

            let key = Self::binding_key(request.id(), listed.id())?;
            let snapshot = transaction
                .load(key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation binding could not be loaded for update",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| SdkError::invalid_state("conversation binding is missing"))?;
            let current_binding = Self::decode_binding(snapshot, request.id(), listed.id())?;
            let route_key = Self::route_key(current_binding.thread_id())?;
            let route = transaction
                .load(route_key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation route could not be loaded for update",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| SdkError::invalid_state("conversation route is missing"))?;
            if Self::decode_route(route, current_binding.thread_id())? != current_binding {
                return Err(SdkError::invalid_state(
                    "conversation route contradicts its binding",
                ));
            }
            current_binding.verify_owner(&current_conversation, &current, self.instructions())?;
            let binding = current_binding.with_rebind_required(replacement.revision())?;
            let value = serde_json::to_value(binding).map_err(|_| {
                SdkError::invalid_state("conversation binding could not be encoded")
            })?;
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    key,
                    value.clone(),
                ))
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation binding revision could not be stored",
                        ErrorPhase::StateWrite,
                    )
                })?;
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    route_key, value,
                ))
                .await
                .map_err(|error| {
                    Self::workspace_update_error(
                        error,
                        "conversation route revision could not be stored",
                        ErrorPhase::StateWrite,
                    )
                })?;
        }
        transaction.commit().await.map_err(|error| {
            Self::workspace_update_error(
                error,
                "workspace update transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(replacement)
    }

    pub(crate) async fn delete_workspace(
        &self,
        agent_id: &AgentId,
        id: &WorkspaceId,
        expected_revision: u64,
    ) -> Result<Option<CodexCleanup>, SdkError> {
        let store = self.storage.store();
        if Self::all_thread_creations(store)
            .await?
            .iter()
            .any(|creation| {
                creation.agent_id() == agent_id && creation.conversation().workspace_id() == id
            })
        {
            return Err(SdkError::busy(
                "workspace contains unresolved conversation creation",
            ));
        }
        let conversations = ConversationRecord::list(store, id).await?;
        let mut continuation_keys = Vec::new();
        for conversation in &conversations {
            let continuations = Self::continuations(store, conversation.id()).await?;
            if continuations
                .iter()
                .any(|(_, continuation)| continuation.is_unresolved())
            {
                return Err(SdkError::busy(
                    "workspace contains an unresolved interaction",
                ));
            }
            continuation_keys.extend(continuations.into_iter().map(|(key, _)| key));
        }
        let workspace_key = WorkspaceRecord::key(agent_id, id)?;
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::workspace_delete_error(
                error,
                "workspace delete transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let snapshot = transaction
            .load(workspace_key.clone())
            .await
            .map_err(|error| {
                Self::workspace_delete_error(
                    error,
                    "workspace could not be loaded for delete",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::not_found("workspace does not exist"))?;
        let workspace = WorkspaceRecord::decode(snapshot, agent_id, Some(id))?;
        if workspace.revision() != expected_revision {
            return Err(SdkError::conflict("workspace revision is stale"));
        }
        let mut thread_ids = Vec::new();
        for conversation in conversations {
            if transaction
                .load(Self::prompt_key(conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "prompt state could not be checked for delete",
                        ErrorPhase::StateRead,
                    )
                })?
                .is_some()
            {
                return Err(SdkError::busy(
                    "workspace contains active or unreconciled prompt work",
                ));
            }
            let binding_key = Self::binding_key(id, conversation.id())?;
            let snapshot = transaction
                .load(binding_key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "conversation binding could not be loaded for delete",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| SdkError::invalid_state("conversation binding is missing"))?;
            let binding = Self::decode_binding(snapshot, id, conversation.id())?;
            let route_key = Self::route_key(binding.thread_id())?;
            let route = transaction
                .load(route_key.clone())
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "conversation route could not be loaded for delete",
                        ErrorPhase::StateRead,
                    )
                })?
                .ok_or_else(|| SdkError::invalid_state("conversation route is missing"))?;
            if Self::decode_route(route, binding.thread_id())? != binding {
                return Err(SdkError::invalid_state(
                    "conversation route contradicts its binding",
                ));
            }
            self.stage_knowledge_forget(
                &mut transaction,
                &KnowledgeOwnerId::Conversation {
                    agent_id: agent_id.clone(),
                    workspace_id: id.clone(),
                    conversation_id: conversation.id().clone(),
                },
            )
            .await
            .map_err(|error| match error {
                AgentStorageKnowledgeError::TransactionConflict(phase) => {
                    SdkError::conflict("workspace deletion conflicted")
                        .with_detail(ErrorDetail::TransactionConflict)
                        .in_phase(phase)
                }
                AgentStorageKnowledgeError::Failure(error) => error,
            })?;
            thread_ids.push(binding.thread_id().to_owned());
            transaction
                .delete(ConversationRecord::key(id, conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "conversation could not be deleted",
                        ErrorPhase::StateWrite,
                    )
                })?;
            transaction.delete(binding_key).await.map_err(|error| {
                Self::workspace_delete_error(
                    error,
                    "conversation binding could not be deleted",
                    ErrorPhase::StateWrite,
                )
            })?;
            transaction.delete(route_key).await.map_err(|error| {
                Self::workspace_delete_error(
                    error,
                    "conversation route could not be deleted",
                    ErrorPhase::StateWrite,
                )
            })?;
            transaction
                .delete(Self::native_selection_key(conversation.id())?)
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "runtime selection could not be deleted",
                        ErrorPhase::StateWrite,
                    )
                })?;
            NotificationStore::<S>::delete_in_transaction(&mut transaction, conversation.id())
                .await
                .map_err(NotificationDeleteError::into_sdk)?;
        }
        for key in continuation_keys {
            transaction.delete(key).await.map_err(|error| {
                Self::workspace_delete_error(
                    error,
                    "settled continuation could not be deleted",
                    ErrorPhase::StateWrite,
                )
            })?;
        }
        self.stage_knowledge_forget(
            &mut transaction,
            &KnowledgeOwnerId::Workspace {
                agent_id: agent_id.clone(),
                workspace_id: id.clone(),
            },
        )
        .await
        .map_err(|error| match error {
            AgentStorageKnowledgeError::TransactionConflict(phase) => {
                SdkError::conflict("workspace deletion conflicted")
                    .with_detail(ErrorDetail::TransactionConflict)
                    .in_phase(phase)
            }
            AgentStorageKnowledgeError::Failure(error) => error,
        })?;
        transaction.delete(workspace_key).await.map_err(|error| {
            Self::workspace_delete_error(
                error,
                "workspace could not be deleted",
                ErrorPhase::StateWrite,
            )
        })?;
        let cleanup = if thread_ids.is_empty() {
            None
        } else {
            Some(CodexCleanup::workspace(
                agent_id.clone(),
                id.clone(),
                thread_ids,
            )?)
        };
        if let Some(cleanup) = &cleanup {
            transaction
                .put(storage_sdk::state::record::StateRecord::new(
                    Self::cleanup_key(cleanup)?,
                    serde_json::to_value(cleanup).map_err(|_| {
                        SdkError::invalid_state("cleanup obligation could not be encoded")
                    })?,
                ))
                .await
                .map_err(|error| {
                    Self::workspace_delete_error(
                        error,
                        "cleanup obligation could not be stored",
                        ErrorPhase::StateWrite,
                    )
                })?;
        }
        transaction.commit().await.map_err(|error| {
            Self::workspace_delete_error(
                error,
                "workspace delete transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(cleanup)
    }

    pub(crate) async fn close_conversation(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        workspace_revision: u64,
        id: &ConversationId,
        deadline: tokio::time::Instant,
    ) -> Result<CodexCleanup, SdkError> {
        let store = self.storage.store();
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(SdkError::timeout("conversation close deadline elapsed"));
            }
            let continuations = Self::continuations(store, id).await?;
            let mut transaction = match store.begin_transaction().await {
                Ok(transaction) => transaction,
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "conversation close transaction could not begin",
                    )
                    .in_phase(ErrorPhase::StateWrite));
                }
            };
            let mutation = async {
                let workspace_key = WorkspaceRecord::key(agent_id, workspace_id)
                    .map_err(CodexConversationCloseError::Failure)?;
                let workspace = transaction
                    .load(workspace_key)
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "workspace could not be loaded for close",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .ok_or_else(|| {
                        CodexConversationCloseError::Failure(SdkError::not_found(
                            "workspace does not exist",
                        ))
                    })?;
                let workspace = WorkspaceRecord::decode(workspace, agent_id, Some(workspace_id))
                    .map_err(CodexConversationCloseError::Failure)?;
                if workspace.revision() != workspace_revision {
                    return Err(CodexConversationCloseError::Failure(SdkError::conflict(
                        "workspace handle is stale",
                    )));
                }
                let conversation_key = ConversationRecord::key(workspace_id, id)
                    .map_err(CodexConversationCloseError::Failure)?;
                let conversation = transaction
                    .load(conversation_key.clone())
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "conversation could not be loaded for close",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .ok_or_else(|| {
                        CodexConversationCloseError::Failure(SdkError::not_found(
                            "conversation does not exist",
                        ))
                    })?;
                let conversation = ConversationRecord::decode(conversation, workspace_id, Some(id))
                    .map_err(CodexConversationCloseError::Failure)?;
                if conversation.workspace_revision() != workspace_revision {
                    return Err(CodexConversationCloseError::Failure(SdkError::conflict(
                        "conversation handle is stale",
                    )));
                }
                let prompt_key =
                    Self::prompt_key(id).map_err(CodexConversationCloseError::Failure)?;
                if transaction
                    .load(prompt_key)
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "prompt state could not be checked for close",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .is_some()
                {
                    return Err(CodexConversationCloseError::Failure(SdkError::busy(
                        "conversation has active or unreconciled prompt work",
                    )));
                }
                match NotificationStore::<S>::check_no_resolving_interaction_in_transaction(
                    &mut transaction,
                    id,
                )
                .await
                {
                    Ok(()) => {}
                    Err(NotificationInteractionCheckError::TransactionConflict) => {
                        return Err(CodexConversationCloseError::TransactionConflict);
                    }
                    Err(NotificationInteractionCheckError::Failure(error)) => {
                        return Err(CodexConversationCloseError::Failure(error));
                    }
                }
                let binding_key = Self::binding_key(workspace_id, id)
                    .map_err(CodexConversationCloseError::Failure)?;
                let binding = transaction
                    .load(binding_key.clone())
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "conversation binding could not be loaded for close",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .ok_or_else(|| {
                        CodexConversationCloseError::Failure(SdkError::invalid_state(
                            "conversation binding is missing",
                        ))
                    })?;
                let binding = Self::decode_binding(binding, workspace_id, id)
                    .map_err(CodexConversationCloseError::Failure)?;
                let route_key = Self::route_key(binding.thread_id())
                    .map_err(CodexConversationCloseError::Failure)?;
                let route = transaction
                    .load(route_key.clone())
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "conversation route could not be loaded for close",
                            ErrorPhase::StateRead,
                        )
                    })?
                    .ok_or_else(|| {
                        CodexConversationCloseError::Failure(SdkError::invalid_state(
                            "conversation route is missing",
                        ))
                    })?;
                if Self::decode_route(route, binding.thread_id())
                    .map_err(CodexConversationCloseError::Failure)?
                    != binding
                {
                    return Err(CodexConversationCloseError::Failure(
                        SdkError::invalid_state("conversation route contradicts its binding"),
                    ));
                }
                let cleanup = CodexCleanup::conversation(
                    agent_id.clone(),
                    id.clone(),
                    binding.thread_id().to_owned(),
                )
                .map_err(CodexConversationCloseError::Failure)?;
                match self
                    .stage_knowledge_forget(
                        &mut transaction,
                        &KnowledgeOwnerId::Conversation {
                            agent_id: agent_id.clone(),
                            workspace_id: workspace_id.clone(),
                            conversation_id: id.clone(),
                        },
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(AgentStorageKnowledgeError::TransactionConflict(_)) => {
                        return Err(CodexConversationCloseError::TransactionConflict);
                    }
                    Err(AgentStorageKnowledgeError::Failure(error)) => {
                        return Err(CodexConversationCloseError::Failure(error));
                    }
                }
                transaction
                    .delete(conversation_key)
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "conversation could not be closed",
                            ErrorPhase::StateWrite,
                        )
                    })?;
                transaction.delete(binding_key).await.map_err(|error| {
                    CodexConversationCloseError::storage(
                        error,
                        "conversation binding could not be removed",
                        ErrorPhase::StateWrite,
                    )
                })?;
                transaction.delete(route_key).await.map_err(|error| {
                    CodexConversationCloseError::storage(
                        error,
                        "conversation route could not be removed",
                        ErrorPhase::StateWrite,
                    )
                })?;
                let native_selection_key =
                    Self::native_selection_key(id).map_err(CodexConversationCloseError::Failure)?;
                transaction
                    .delete(native_selection_key)
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "runtime selection could not be removed",
                            ErrorPhase::StateWrite,
                        )
                    })?;
                let cleanup_key =
                    Self::cleanup_key(&cleanup).map_err(CodexConversationCloseError::Failure)?;
                let cleanup_value = serde_json::to_value(&cleanup).map_err(|_| {
                    CodexConversationCloseError::Failure(SdkError::invalid_state(
                        "cleanup obligation could not be encoded",
                    ))
                })?;
                transaction
                    .put(storage_sdk::state::record::StateRecord::new(
                        cleanup_key,
                        cleanup_value,
                    ))
                    .await
                    .map_err(|error| {
                        CodexConversationCloseError::storage(
                            error,
                            "cleanup obligation could not be stored",
                            ErrorPhase::StateWrite,
                        )
                    })?;
                let cancelled = match self
                    .notifications
                    .close_in_transaction(&mut transaction, id)
                    .await
                {
                    Ok(cancelled) => cancelled,
                    Err(NotificationCloseError::TransactionConflict(_)) => {
                        return Err(CodexConversationCloseError::TransactionConflict);
                    }
                    Err(NotificationCloseError::Failure(error)) => {
                        return Err(CodexConversationCloseError::Failure(error));
                    }
                };
                if continuations.iter().any(|(_, continuation)| {
                    continuation.is_unresolved()
                        && !cancelled.contains(continuation.notification_id())
                }) {
                    return Err(CodexConversationCloseError::Failure(SdkError::conflict(
                        "interaction resolution won the conversation-close race",
                    )));
                }
                for (key, continuation) in continuations {
                    if !continuation.is_unresolved()
                        || cancelled.iter().any(|notification_id| {
                            notification_id == continuation.notification_id()
                        })
                    {
                        transaction.delete(key).await.map_err(|error| {
                            CodexConversationCloseError::storage(
                                error,
                                "cancelled continuation could not be removed",
                                ErrorPhase::StateWrite,
                            )
                        })?;
                    }
                }
                Ok(cleanup)
            }
            .await;
            let cleanup = match mutation {
                Ok(cleanup) => cleanup,
                Err(CodexConversationCloseError::TransactionConflict) => {
                    let _ = transaction.rollback().await;
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(CodexConversationCloseError::Failure(error)) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };
            match transaction.commit().await {
                Ok(_) => {
                    self.notifications.finish_close(id);
                    return Ok(cleanup);
                }
                Err(storage_sdk::StateStoreError::TransactionConflict { .. }) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => {
                    return Err(SdkError::from_state_store(
                        error,
                        "conversation close transaction could not commit",
                    )
                    .in_phase(ErrorPhase::StateCommit));
                }
            }
        }
    }

    pub(crate) async fn bindings_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<CodexThreadBinding>, SdkError> {
        let conversations = self.conversations(workspace_id).await?;
        let mut bindings = Vec::with_capacity(conversations.len());
        for conversation in conversations {
            bindings.push(self.binding(workspace_id, conversation.id()).await?);
        }
        Ok(bindings)
    }

    pub(crate) async fn cleanup_obligations(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<CodexCleanup>, SdkError> {
        let store = self.storage.store();
        let mut records = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_STATE_PAGES {
            let prefix =
                storage_sdk::state::record::StateKeyPrefix::new(format!("{}:", agent_id.as_str()))
                    .map_err(|_| SdkError::invalid_state("cleanup prefix is invalid"))?;
            let mut request =
                storage_sdk::state::record::StateListRequest::new(CODEX_CLEANUP_NAMESPACE)
                    .map_err(|_| SdkError::invalid_state("cleanup request is invalid"))?
                    .with_key_prefix(prefix)
                    .with_limit(storage_sdk::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| SdkError::invalid_state("cleanup limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request
                    .start_after(next)
                    .map_err(|_| SdkError::invalid_state("cleanup cursor is invalid"))?;
            }
            let page = store.list(request).await.map_err(|error| {
                Self::storage_error(
                    error,
                    "cleanup inventory could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?;
            for snapshot in page.records {
                let key = snapshot.record.key.key().to_owned();
                let cleanup: CodexCleanup =
                    serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                        .map_err(|_| SdkError::invalid_state("stored cleanup is malformed"))?;
                cleanup.validate()?;
                if cleanup.agent_id() != agent_id || cleanup.key_fragment() != key {
                    return Err(SdkError::invalid_state(
                        "stored cleanup ownership contradicts its key",
                    ));
                }
                records.push(cleanup);
            }
            let Some(next) = page.next_start_after else {
                records.sort_by_key(CodexCleanup::key_fragment);
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "cleanup inventory exceeds its private page bound",
        ))
    }

    pub(crate) async fn record_cleanup(&self, cleanup: &CodexCleanup) -> Result<(), SdkError> {
        cleanup.validate()?;
        let key = Self::cleanup_key(cleanup)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "cleanup transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        if let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "cleanup obligation could not be checked",
                ErrorPhase::StateRead,
            )
        })? {
            let existing: CodexCleanup =
                serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                    .map_err(|_| SdkError::invalid_state("stored cleanup is malformed"))?;
            existing.validate()?;
            if existing != *cleanup {
                return Err(SdkError::conflict(
                    "cleanup obligation changed while pending",
                ));
            }
            return Ok(());
        }
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(cleanup).map_err(|_| {
                    SdkError::invalid_state("cleanup obligation could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "cleanup obligation could not be stored",
                    ErrorPhase::StateWrite,
                )
            })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "cleanup transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(())
    }

    pub(crate) async fn finish_cleanup(&self, cleanup: &CodexCleanup) -> Result<(), SdkError> {
        cleanup.validate()?;
        let key = Self::cleanup_key(cleanup)?;
        let store = self.storage.store();
        let mut transaction = store.begin_transaction().await.map_err(|error| {
            Self::storage_error(
                error,
                "cleanup transaction could not begin",
                ErrorPhase::StateWrite,
            )
        })?;
        let Some(snapshot) = transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "cleanup obligation could not be loaded",
                ErrorPhase::StateRead,
            )
        })?
        else {
            return Ok(());
        };
        let existing: CodexCleanup =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| SdkError::invalid_state("stored cleanup is malformed"))?;
        existing.validate()?;
        if existing != *cleanup || Self::cleanup_key(&existing)? != key {
            return Err(SdkError::conflict(
                "cleanup obligation changed before completion",
            ));
        }
        transaction.delete(key).await.map_err(|error| {
            Self::storage_error(
                error,
                "cleanup obligation could not be removed",
                ErrorPhase::StateWrite,
            )
        })?;
        transaction.commit().await.map_err(|error| {
            Self::storage_error(
                error,
                "cleanup transaction could not commit",
                ErrorPhase::StateCommit,
            )
        })?;
        Ok(())
    }

    fn key(id: &crate::AgentId) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(CODEX_ATTACHMENT_NAMESPACE, id.as_str())
            .map_err(|_| SdkError::invalid_state("attachment key is invalid"))
    }

    fn thread_creation_key(
        creation: &CodexThreadCreation,
    ) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(
            CODEX_THREAD_CREATION_NAMESPACE,
            creation.key_fragment(),
        )
        .map_err(|_| SdkError::invalid_state("conversation creation key is invalid"))
    }

    fn decode_thread_creation(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
    ) -> Result<CodexThreadCreation, SdkError> {
        let key = snapshot.record.key.key().to_owned();
        let creation: CodexThreadCreation =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref()).map_err(|_| {
                SdkError::invalid_state("stored conversation creation is malformed")
            })?;
        creation.validate()?;
        if Self::thread_creation_key(&creation)?.key() != key {
            return Err(SdkError::invalid_state(
                "stored conversation creation contradicts its key owner",
            ));
        }
        Ok(creation)
    }

    async fn all_thread_creations(store: &S) -> Result<Vec<CodexThreadCreation>, SdkError> {
        let mut records = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_STATE_PAGES {
            let mut request =
                storage_sdk::state::record::StateListRequest::new(CODEX_THREAD_CREATION_NAMESPACE)
                    .map_err(|_| {
                        SdkError::invalid_state("conversation creation request is invalid")
                    })?
                    .with_limit(storage_sdk::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| {
                        SdkError::invalid_state("conversation creation limit is invalid")
                    })?;
            if let Some(next) = cursor.take() {
                request = request.start_after(next).map_err(|_| {
                    SdkError::invalid_state("conversation creation cursor is invalid")
                })?;
            }
            let page = store.list(request).await.map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation creations could not be listed",
                    ErrorPhase::StateRead,
                )
            })?;
            for snapshot in page.records {
                records.push(Self::decode_thread_creation(snapshot)?);
            }
            let Some(next) = page.next_start_after else {
                records.sort_by(|left, right| left.key_fragment().cmp(right.key_fragment()));
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "conversation creation inventory exceeds its private page bound",
        ))
    }

    fn prompt_key(id: &ConversationId) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(CODEX_PROMPT_NAMESPACE, id.as_str())
            .map_err(|_| SdkError::invalid_state("prompt key is invalid"))
    }

    fn native_selection_key(
        id: &ConversationId,
    ) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(CODEX_NATIVE_SELECTION_NAMESPACE, id.as_str())
            .map_err(|_| SdkError::invalid_state("runtime selection key is invalid"))
    }

    fn decode_native_selection(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        conversation_id: &ConversationId,
    ) -> Result<CodexNativeSelectionRecord, SdkError> {
        let key = snapshot.record.key.key().to_owned();
        let record: CodexNativeSelectionRecord =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| SdkError::invalid_state("stored runtime selection is malformed"))?;
        record.validate()?;
        if record.conversation_id() != conversation_id
            || Self::native_selection_key(conversation_id)?.key() != key
        {
            return Err(SdkError::invalid_state(
                "stored runtime selection contradicts its key owner",
            ));
        }
        Ok(record)
    }

    async fn persist_native_selection_in_transaction(
        transaction: &mut S::Transaction,
        conversation_id: &ConversationId,
        prompt: &CodexPromptRecord,
    ) -> Result<(), SdkError> {
        let key = Self::native_selection_key(conversation_id)?;
        let current = match transaction.load(key.clone()).await.map_err(|error| {
            Self::storage_error(
                error,
                "runtime selection could not be loaded",
                ErrorPhase::StateRead,
            )
        })? {
            Some(snapshot) => Self::decode_native_selection(snapshot, conversation_id)?
                .selection()
                .clone(),
            None => CodexNativeSelection::default(),
        };
        if current != *prompt.previous_selection() && current != *prompt.accepted_selection() {
            return Err(SdkError::conflict(
                "runtime selection changed before prompt acceptance",
            ));
        }
        if current == *prompt.accepted_selection() {
            return Ok(());
        }
        let record = CodexNativeSelectionRecord::new(
            conversation_id.clone(),
            prompt.accepted_selection().clone(),
        );
        transaction
            .put(storage_sdk::state::record::StateRecord::new(
                key,
                serde_json::to_value(record).map_err(|_| {
                    SdkError::invalid_state("runtime selection could not be encoded")
                })?,
            ))
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "runtime selection could not be stored",
                    ErrorPhase::StateWrite,
                )
            })
    }

    fn decode_prompt(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        conversation_id: &ConversationId,
    ) -> Result<CodexPromptRecord, SdkError> {
        let key = snapshot.record.key.key().to_owned();
        CodexPromptRecord::check_version(snapshot.record.value.as_ref())?;
        let record: CodexPromptRecord =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref()).map_err(|_| {
                SdkError::invalid_state("stored prompt state is malformed")
                    .in_phase(ErrorPhase::StateRead)
                    .with_detail(ErrorDetail::MalformedData)
            })?;
        record.validate().map_err(|error| {
            error
                .in_phase(ErrorPhase::StateRead)
                .with_detail(ErrorDetail::InvalidData)
        })?;
        if record.conversation_id() != conversation_id
            || Self::prompt_key(conversation_id)?.key() != key
        {
            return Err(SdkError::invalid_state(
                "stored prompt contradicts its key owner",
            ));
        }
        Ok(record)
    }

    fn continuation_key(
        continuation: &CodexContinuation,
    ) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(
            CODEX_CONTINUATION_NAMESPACE,
            continuation.key_fragment(),
        )
        .map_err(|_| SdkError::invalid_state("continuation key is invalid"))
    }

    async fn continuations(
        store: &S,
        conversation_id: &ConversationId,
    ) -> Result<Vec<(storage_sdk::state::record::StateKey, CodexContinuation)>, SdkError> {
        let prefix = storage_sdk::state::record::StateKeyPrefix::new(format!(
            "{}:",
            conversation_id.as_str()
        ))
        .map_err(|_| SdkError::invalid_state("continuation prefix is invalid"))?;
        let mut records = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_STATE_PAGES {
            let mut request =
                storage_sdk::state::record::StateListRequest::new(CODEX_CONTINUATION_NAMESPACE)
                    .map_err(|_| SdkError::invalid_state("continuation request is invalid"))?
                    .with_key_prefix(prefix.clone())
                    .with_limit(storage_sdk::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| SdkError::invalid_state("continuation limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request
                    .start_after(next)
                    .map_err(|_| SdkError::invalid_state("continuation cursor is invalid"))?;
            }
            let page = store.list(request).await.map_err(|error| {
                Self::storage_error(
                    error,
                    "continuations could not be listed",
                    ErrorPhase::StateRead,
                )
            })?;
            for snapshot in page.records {
                let key = snapshot.record.key.clone();
                records.push((key, Self::decode_continuation(snapshot, conversation_id)?));
            }
            let Some(next) = page.next_start_after else {
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "continuation inventory exceeds its private page bound",
        ))
    }

    async fn all_continuations(
        store: &S,
    ) -> Result<Vec<(storage_sdk::state::record::StateKey, CodexContinuation)>, SdkError> {
        let mut records = Vec::new();
        let mut cursor = None;
        for _ in 0..MAX_STATE_PAGES {
            let mut request =
                storage_sdk::state::record::StateListRequest::new(CODEX_CONTINUATION_NAMESPACE)
                    .map_err(|_| SdkError::invalid_state("continuation request is invalid"))?
                    .with_limit(storage_sdk::state::record::StateListRequest::MAX_LIMIT)
                    .map_err(|_| SdkError::invalid_state("continuation limit is invalid"))?;
            if let Some(next) = cursor.take() {
                request = request
                    .start_after(next)
                    .map_err(|_| SdkError::invalid_state("continuation cursor is invalid"))?;
            }
            let page = store.list(request).await.map_err(|error| {
                Self::storage_error(
                    error,
                    "continuations could not be listed",
                    ErrorPhase::StateRead,
                )
            })?;
            for snapshot in page.records {
                let key = snapshot.record.key.clone();
                records.push((key, Self::decode_any_continuation(snapshot)?));
            }
            let Some(next) = page.next_start_after else {
                return Ok(records);
            };
            cursor = Some(next);
        }
        Err(SdkError::invalid_state(
            "continuation inventory exceeds its private page bound",
        ))
    }

    fn decode_continuation(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        conversation_id: &ConversationId,
    ) -> Result<CodexContinuation, SdkError> {
        let continuation = Self::decode_any_continuation(snapshot)?;
        if continuation.conversation_id() != conversation_id {
            return Err(SdkError::invalid_state(
                "stored continuation contradicts its key owner",
            ));
        }
        Ok(continuation)
    }

    fn decode_any_continuation(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
    ) -> Result<CodexContinuation, SdkError> {
        let key = snapshot.record.key.key().to_owned();
        let continuation: CodexContinuation =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| SdkError::invalid_state("stored continuation is malformed"))?;
        continuation.validate()?;
        if Self::continuation_key(&continuation)?.key() != key {
            return Err(SdkError::invalid_state(
                "stored continuation contradicts its key owner",
            ));
        }
        Ok(continuation)
    }

    fn binding_key(
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(
            CODEX_THREAD_NAMESPACE,
            format!("{}:{}", workspace_id.as_str(), id.as_str()),
        )
        .map_err(|_| SdkError::invalid_state("conversation binding key is invalid"))
    }

    fn decode_binding(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<CodexThreadBinding, SdkError> {
        let key = snapshot.record.key.key().to_owned();
        let binding: CodexThreadBinding =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| SdkError::invalid_state("stored conversation binding is malformed"))?;
        binding.validate()?;
        if binding.workspace_id() != workspace_id || binding.conversation_id() != id {
            return Err(SdkError::invalid_state(
                "stored conversation binding has a foreign owner",
            ));
        }
        if Self::binding_key(workspace_id, id)?.key() != key {
            return Err(SdkError::invalid_state(
                "stored conversation binding contradicts its key",
            ));
        }
        Ok(binding)
    }

    async fn load_binding(
        store: &S,
        workspace_id: &WorkspaceId,
        id: &ConversationId,
    ) -> Result<CodexThreadBinding, SdkError> {
        let snapshot = store
            .load(Self::binding_key(workspace_id, id)?)
            .await
            .map_err(|error| {
                Self::storage_error(
                    error,
                    "conversation binding could not be loaded",
                    ErrorPhase::StateRead,
                )
            })?
            .ok_or_else(|| SdkError::invalid_state("conversation binding is missing"))?;
        Self::decode_binding(snapshot, workspace_id, id)
    }

    fn route_key(thread_id: &str) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        if thread_id.is_empty() || thread_id.trim() != thread_id {
            return Err(SdkError::invalid_state(
                "conversation route id is malformed",
            ));
        }
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(thread_id.len().saturating_mul(2));
        for byte in thread_id.as_bytes() {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        storage_sdk::state::record::StateKey::new(CODEX_THREAD_ROUTE_NAMESPACE, encoded)
            .map_err(|_| SdkError::invalid_state("conversation route key is invalid"))
    }

    fn decode_route(
        snapshot: storage_sdk::state::record::StateRecordSnapshot,
        thread_id: &str,
    ) -> Result<CodexThreadBinding, SdkError> {
        let key = snapshot.record.key.key().to_owned();
        let binding: CodexThreadBinding =
            serde::Deserialize::deserialize(snapshot.record.value.as_ref())
                .map_err(|_| SdkError::invalid_state("stored conversation route is malformed"))?;
        binding.validate()?;
        if binding.thread_id() != thread_id || Self::route_key(thread_id)?.key() != key {
            return Err(SdkError::invalid_state(
                "stored conversation route contradicts its key",
            ));
        }
        Ok(binding)
    }

    fn cleanup_key(
        cleanup: &CodexCleanup,
    ) -> Result<storage_sdk::state::record::StateKey, SdkError> {
        storage_sdk::state::record::StateKey::new(CODEX_CLEANUP_NAMESPACE, cleanup.key_fragment())
            .map_err(|_| SdkError::invalid_state("cleanup key is invalid"))
    }

    fn continuation_state(settlement: ResolutionSettlement) -> CodexContinuationState {
        match settlement {
            ResolutionSettlement::Accepted => CodexContinuationState::Accepted,
            ResolutionSettlement::Rejected => CodexContinuationState::Pending,
            ResolutionSettlement::Ambiguous => CodexContinuationState::Ambiguous,
            ResolutionSettlement::Cancelled => CodexContinuationState::Cancelled,
        }
    }

    fn terminal_settlement(state: CodexContinuationState) -> Option<ResolutionSettlement> {
        match state {
            CodexContinuationState::Accepted => Some(ResolutionSettlement::Accepted),
            CodexContinuationState::Cancelled => Some(ResolutionSettlement::Cancelled),
            CodexContinuationState::Pending | CodexContinuationState::Ambiguous => None,
        }
    }
}
