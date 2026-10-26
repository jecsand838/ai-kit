mod caller_shell;
mod lance_state_support;
mod native_cleanup;

use lance_state_support::LanceStateDirectory;

use futures_util::{FutureExt, Stream};
use native_cleanup::NativeCleanup;
use provider_sdk::error::{
    ErrorKind as ProviderErrorKind, ErrorPhase as ProviderErrorPhase,
    Operation as ProviderOperation, ResponseViolation,
};
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
use agent_sdk::conversation::NewConversation;
use agent_sdk::knowledge::{
    IngestKnowledgeRequest, KnowledgePolicy, KnowledgeScope, KnowledgeSource,
};
use agent_sdk::model::ModelSelector;
use agent_sdk::notification::{
    ConversationNotificationKind, ConversationNotificationReceiver, NotificationResolution,
};
use agent_sdk::prompt::{
    PromptEvent, PromptRequest, PromptStream, StructuredOutputRequest, UsageInfo,
};
use agent_sdk::tool::ToolSelection;
use agent_sdk::workspace::{
    NewWorkspace, UpdateWorkspaceRequest, WorkspaceContext, WorkspaceDefaults,
    WorkspaceExecutionPolicy, WorkspaceNetworkAccess, WorkspaceRoot, WorkspaceRootAccess,
    WorkspaceSandboxMode,
};
use agent_sdk::{
    Agent, AgentBuilder, AgentStorage, Conversation, ConversationBackend, SdkError, Workspace,
};
use agent_sdk::{AgentBackend, WorkspaceBackend};
use provider_sdk::generation::{
    AllowedToolMode, ContentPart, FinishReason, FunctionCall, GenerateEvent, GenerateEventStream,
    GenerateRequest, GenerateResponse, Message, Role, ToolChoice, Usage,
};
use provider_sdk::models::{ModelId, ModelListRequest, ModelPage, ModelSummary};
use provider_sdk::{ModelBackend, ProviderBackend, SdkError as ProviderError};
use storage_sdk::state::config::{
    LanceStateStoreConfig, LanceStateStoreLocation, StateStoreConfig,
};
use storage_sdk::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};
use tool_sdk::declaration::{ToolInfo, ToolName};
use tool_sdk::execution::{ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolResult};
use tool_sdk::interaction::{
    ToolInteractionCapabilities, ToolInteractionId, ToolInteractionKind, ToolInteractionRequest,
};
use tool_sdk::schema::JsonSchema;
use tool_sdk::{ToolError, ToolHandlerBackend, ToolsetBackend};

const MODEL: &str = "fixture-model";
const TOOL: &str = "fixture_tool";

struct ScriptedProvider {
    state: Arc<ScriptedState>,
}

struct ScriptedModel {
    state: Arc<ScriptedState>,
    model: ModelId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScriptedContinuation(String);

struct ScriptedState {
    responses: Mutex<VecDeque<GenerateResponse<ScriptedContinuation>>>,
    streams: Mutex<VecDeque<ScriptedStream>>,
    model_pages: Mutex<VecDeque<ModelPage>>,
    model_requests: Mutex<Vec<ModelListRequest>>,
    choices: Mutex<Vec<ToolChoice>>,
    reasoning_configs: Mutex<Vec<Option<provider_sdk::generation::ReasoningConfig>>>,
    requests: Mutex<Vec<Vec<Message<ScriptedContinuation>>>>,
}

enum ScriptedStream {
    Events(VecDeque<Result<GenerateEvent<ScriptedContinuation>, ProviderError>>),
    Ready {
        observed: Arc<AtomicUsize>,
        limit: usize,
        reasoning: bool,
    },
    Done,
}

impl Stream for ScriptedStream {
    type Item = Result<GenerateEvent<ScriptedContinuation>, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        Poll::Ready(match stream {
            Self::Events(events) => events.pop_front(),
            Self::Ready {
                observed,
                limit,
                reasoning,
            } => {
                let index = observed.fetch_add(1, Ordering::SeqCst);
                if index >= *limit {
                    *stream = Self::Done;
                    Some(Ok(GenerateEvent::Completed(Box::new(
                        ScriptedState::terminal_response("source exhausted"),
                    ))))
                } else if *reasoning && index > 0 {
                    Some(Ok(GenerateEvent::ThinkingDelta {
                        delta: "private".to_owned(),
                    }))
                } else {
                    Some(Ok(GenerateEvent::ContentDelta {
                        delta: "ready fragment".to_owned(),
                    }))
                }
            }
            Self::Done => None,
        })
    }
}

impl ScriptedState {
    fn new(responses: Vec<GenerateResponse<ScriptedContinuation>>) -> Arc<Self> {
        Arc::new(Self {
            responses: Mutex::new(responses.into()),
            streams: Mutex::new(VecDeque::new()),
            model_pages: Mutex::new(VecDeque::new()),
            model_requests: Mutex::new(Vec::new()),
            choices: Mutex::new(Vec::new()),
            reasoning_configs: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        })
    }

    fn set_model_pages(&self, pages: Vec<ModelPage>) {
        *self
            .model_pages
            .lock()
            .expect("scripted model pages must not be poisoned") = pages.into();
        self.model_requests
            .lock()
            .expect("scripted model requests must not be poisoned")
            .clear();
    }

    fn set_streams(
        &self,
        streams: Vec<Vec<Result<GenerateEvent<ScriptedContinuation>, ProviderError>>>,
    ) {
        *self.streams.lock().expect("scripted streams mutex") = streams
            .into_iter()
            .map(|events| ScriptedStream::Events(events.into()))
            .collect();
    }

    fn record_request(&self, request: &GenerateRequest<ScriptedContinuation>) {
        self.reasoning_configs
            .lock()
            .expect("scripted reasoning configs must not be poisoned")
            .push(request.reasoning().cloned());
        self.choices
            .lock()
            .expect("scripted choices must not be poisoned")
            .push(
                request
                    .generation()
                    .tool_choice()
                    .cloned()
                    .expect("Custom requests must select a tool policy"),
            );
        self.requests
            .lock()
            .expect("scripted requests must not be poisoned")
            .push(request.messages().to_vec());
    }

    fn choices(&self) -> Vec<ToolChoice> {
        self.choices
            .lock()
            .expect("scripted choices must not be poisoned")
            .clone()
    }

    fn requests(&self) -> Vec<Vec<Message<ScriptedContinuation>>> {
        self.requests
            .lock()
            .expect("scripted requests must not be poisoned")
            .clone()
    }

    fn response(
        message: Message<ScriptedContinuation>,
        finish_reason: FinishReason,
    ) -> GenerateResponse<ScriptedContinuation> {
        GenerateResponse {
            model: ModelId::new(MODEL).expect("fixture model id must be valid"),
            message,
            finish_reason: Some(finish_reason),
            citations: Vec::new(),
            artifacts: Vec::new(),
            hosted_tool_outputs: Vec::new(),
            usage: Usage {
                input_tokens: Some(1),
                output_tokens: Some(1),
                ..Usage::default()
            },
        }
    }

    fn assert_finish_failure(reason: Option<&FinishReason>, error: &SdkError) {
        let correct_category = match reason {
            None | Some(FinishReason::Stop | FinishReason::ToolCall) => {
                matches!((error).kind(), agent_sdk::error::ErrorKind::Protocol)
            }
            Some(FinishReason::Length | FinishReason::ContentFilter | FinishReason::Error) => {
                matches!((error).kind(), agent_sdk::error::ErrorKind::Integration)
            }
            Some(FinishReason::Paused) => matches!(
                (error).kind(),
                agent_sdk::error::ErrorKind::NotYetImplemented
            ),
        };
        assert!(
            correct_category,
            "completion {reason:?} returned the wrong error category: {error:?}"
        );
        let detail = match reason {
            None => Some(agent_sdk::error::ErrorDetail::MissingData),
            Some(FinishReason::Stop | FinishReason::ToolCall) => {
                Some(agent_sdk::error::ErrorDetail::InvalidSequence)
            }
            Some(FinishReason::Length) => Some(agent_sdk::error::ErrorDetail::OutputLimit),
            Some(FinishReason::ContentFilter) => {
                Some(agent_sdk::error::ErrorDetail::ContentFiltered)
            }
            Some(FinishReason::Paused) => Some(agent_sdk::error::ErrorDetail::ContinuationRequired),
            Some(FinishReason::Error) => None,
        };
        assert_eq!(error.detail(), detail.as_ref());
        assert_eq!(
            error.context().operation,
            agent_sdk::error::Operation::Prompt
        );
        assert_eq!(
            error.context().phase,
            agent_sdk::error::ErrorPhase::CompletionValidation
        );
    }

    fn function_response() -> GenerateResponse<ScriptedContinuation> {
        let call = FunctionCall::new(Some("fixture-call".to_owned()), TOOL, serde_json::json!({}))
            .expect("fixture function call must be valid");
        Self::response(
            Message::assistant(Vec::new(), vec![call])
                .expect("assistant function call must be valid")
                .with_continuation(ScriptedContinuation("fixture-continuation".to_owned()))
                .expect("assistant continuation must be valid"),
            FinishReason::ToolCall,
        )
    }

    fn terminal_response(text: &str) -> GenerateResponse<ScriptedContinuation> {
        Self::response(
            Message::assistant_text(text)
                .expect("assistant text must be valid")
                .with_continuation(ScriptedContinuation(
                    "fixture-terminal-continuation".to_owned(),
                ))
                .expect("terminal assistant continuation must be valid"),
            FinishReason::Stop,
        )
    }
}

impl ProviderBackend for ScriptedProvider {
    type Model = ScriptedModel;

    fn model(&self, id: ModelId) -> Result<Self::Model, ProviderError> {
        if id.as_str() == "unsupported-model" {
            return Err(ProviderError::new(
                ProviderErrorKind::Unsupported,
                ProviderOperation::SelectModel,
                ProviderErrorPhase::InputValidation,
            ));
        }
        Ok(ScriptedModel {
            state: Arc::clone(&self.state),
            model: id,
        })
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, ProviderError> {
        self.state
            .model_requests
            .lock()
            .expect("scripted model requests must not be poisoned")
            .push(request);
        if let Some(page) = self
            .state
            .model_pages
            .lock()
            .expect("scripted model pages must not be poisoned")
            .pop_front()
        {
            return Ok(page);
        }
        Ok(ModelPage {
            models: vec![ModelSummary {
                id: ModelId::new(MODEL).expect("fixture model id must be valid"),
                display_name: Some("Fixture Model".to_owned()),
                description: Some("Deterministic test model".to_owned()),
                context_window_tokens: Some(4096),
            }],
            next_cursor: None,
        })
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

impl ModelBackend for ScriptedModel {
    type Continuation = ScriptedContinuation;
    type Session = std::convert::Infallible;

    fn model_id(&self) -> &ModelId {
        &self.model
    }

    async fn generate(
        &self,
        request: &GenerateRequest<Self::Continuation>,
    ) -> Result<GenerateResponse<Self::Continuation>, ProviderError> {
        self.state.record_request(request);
        self.state
            .responses
            .lock()
            .expect("scripted responses must not be poisoned")
            .pop_front()
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::InvalidState,
                    ProviderOperation::Generate,
                    ProviderErrorPhase::Lifecycle,
                )
            })
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<Self::Continuation>,
    ) -> Result<GenerateEventStream<Self::Continuation>, ProviderError> {
        let events = self
            .state
            .streams
            .lock()
            .expect("scripted streams mutex")
            .pop_front()
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::Unsupported,
                    ProviderOperation::GenerateStream,
                    ProviderErrorPhase::InputValidation,
                )
            })?;
        self.state.record_request(request);
        Ok(GenerateEventStream::new(events))
    }
}

#[derive(Clone)]
struct NotificationConflictStore {
    inner: StateStore,
    commit_timeout: Arc<Mutex<Option<bool>>>,
    receipt_read_hold: Arc<AtomicBool>,
    receipt_read_observation: Arc<AtomicBool>,
    receipt_read_entered: Arc<tokio::sync::Semaphore>,
    receipt_read_release: Arc<tokio::sync::Semaphore>,
    conflict: Arc<AtomicBool>,
    persistent_conflict: Arc<AtomicBool>,
    persistent_attempts: Arc<AtomicUsize>,
    prompt_waiter_conflict: Arc<AtomicBool>,
    prompt_waiter_attempts: Arc<AtomicUsize>,
    prompt_waiter_hold: Arc<Mutex<Option<PromptWaiterCommitHold>>>,
    prompt_waiter_release: Arc<tokio::sync::Semaphore>,
    prompt_waiter_commits: Arc<AtomicUsize>,
    prompt_waiter_receipts: Arc<AtomicUsize>,
    ambiguous: Arc<AtomicBool>,
    unavailable_observation: Arc<AtomicBool>,
    ambiguous_intent: Arc<AtomicBool>,
    intent_observation_faults: Arc<AtomicUsize>,
    intent_writes: Arc<AtomicUsize>,
    intent_observations: Arc<AtomicUsize>,
    ambiguous_settlement: Arc<AtomicBool>,
    stale_settlement_observation: Arc<AtomicBool>,
    pre_settlement_snapshot: Arc<Mutex<Option<StateRecordSnapshot>>>,
    settlement_observation_faults: Arc<AtomicUsize>,
    settlement_writes: Arc<AtomicUsize>,
    settlement_observations: Arc<AtomicUsize>,
    injected: Arc<tokio::sync::Semaphore>,
}

struct NotificationConflictTransaction {
    inner: StateTransaction,
    owner: NotificationConflictStore,
    read_notification: bool,
    wrote_notification: bool,
    wrote_resolution_intent: bool,
    wrote_resolution_settlement: bool,
    wrote_prompt_waiter: bool,
}

#[derive(Clone, Copy)]
enum PromptWaiterCommitHold {
    Before,
    After,
}

impl NotificationConflictStore {
    fn new(inner: StateStore) -> Self {
        Self {
            inner,
            commit_timeout: Arc::new(Mutex::new(None)),
            receipt_read_hold: Arc::new(AtomicBool::new(false)),
            receipt_read_observation: Arc::new(AtomicBool::new(false)),
            receipt_read_entered: Arc::new(tokio::sync::Semaphore::new(0)),
            receipt_read_release: Arc::new(tokio::sync::Semaphore::new(0)),
            conflict: Arc::new(AtomicBool::new(false)),
            persistent_conflict: Arc::new(AtomicBool::new(false)),
            persistent_attempts: Arc::new(AtomicUsize::new(0)),
            prompt_waiter_conflict: Arc::new(AtomicBool::new(false)),
            prompt_waiter_attempts: Arc::new(AtomicUsize::new(0)),
            prompt_waiter_hold: Arc::new(Mutex::new(None)),
            prompt_waiter_release: Arc::new(tokio::sync::Semaphore::new(0)),
            prompt_waiter_commits: Arc::new(AtomicUsize::new(0)),
            prompt_waiter_receipts: Arc::new(AtomicUsize::new(0)),
            ambiguous: Arc::new(AtomicBool::new(false)),
            unavailable_observation: Arc::new(AtomicBool::new(false)),
            ambiguous_intent: Arc::new(AtomicBool::new(false)),
            intent_observation_faults: Arc::new(AtomicUsize::new(0)),
            intent_writes: Arc::new(AtomicUsize::new(0)),
            intent_observations: Arc::new(AtomicUsize::new(0)),
            ambiguous_settlement: Arc::new(AtomicBool::new(false)),
            stale_settlement_observation: Arc::new(AtomicBool::new(false)),
            pre_settlement_snapshot: Arc::new(Mutex::new(None)),
            settlement_observation_faults: Arc::new(AtomicUsize::new(0)),
            settlement_writes: Arc::new(AtomicUsize::new(0)),
            settlement_observations: Arc::new(AtomicUsize::new(0)),
            injected: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }

    async fn hold_receipt_read(&self) {
        self.receipt_read_entered.add_permits(1);
        self.receipt_read_release.acquire().await.unwrap().forget();
    }

    fn arm_conflict(&self) {
        assert!(
            !self.conflict.swap(true, Ordering::SeqCst),
            "notification conflict must not already be armed"
        );
    }

    fn arm_prompt_waiter_commit(&self, hold: PromptWaiterCommitHold) {
        let previous = self
            .prompt_waiter_hold
            .lock()
            .expect("prompt waiter gate mutex")
            .replace(hold);
        assert!(previous.is_none(), "prompt waiter gate must be unarmed");
    }

    async fn hold_prompt_waiter_commit(&self) {
        self.injected.add_permits(1);
        self.prompt_waiter_release
            .acquire()
            .await
            .expect("prompt waiter release gate stays open")
            .forget();
    }

    fn release_prompt_waiter_fault(&self) {
        self.prompt_waiter_conflict.store(false, Ordering::SeqCst);
        self.prompt_waiter_hold
            .lock()
            .expect("prompt waiter gate mutex")
            .take();
        self.prompt_waiter_release.add_permits(1);
    }

    fn arm_ambiguous_commit(&self) {
        assert!(
            !self.ambiguous.swap(true, Ordering::SeqCst),
            "ambiguous notification commit must not already be armed"
        );
        assert!(
            !self.unavailable_observation.load(Ordering::SeqCst),
            "notification observation failure must not already be pending"
        );
    }

    fn arm_persistent_conflict(&self) {
        assert_eq!(
            self.persistent_attempts.load(Ordering::SeqCst),
            0,
            "persistent notification attempts must start empty"
        );
        assert!(
            !self.persistent_conflict.swap(true, Ordering::SeqCst),
            "persistent notification conflict must not already be armed"
        );
    }

    fn arm_ambiguous_intent(&self, persistent_observation: bool) {
        assert!(
            !self.ambiguous_intent.swap(true, Ordering::SeqCst),
            "ambiguous interaction intent must not already be armed"
        );
        self.intent_writes.store(0, Ordering::SeqCst);
        self.intent_observations.store(0, Ordering::SeqCst);
        self.intent_observation_faults.store(
            if persistent_observation {
                usize::MAX
            } else {
                1
            },
            Ordering::SeqCst,
        );
    }

    fn arm_ambiguous_settlement(&self, persistent_observation: bool) {
        assert!(
            !self.ambiguous_settlement.swap(true, Ordering::SeqCst),
            "ambiguous interaction settlement must not already be armed"
        );
        self.settlement_writes.store(0, Ordering::SeqCst);
        self.settlement_observations.store(0, Ordering::SeqCst);
        self.settlement_observation_faults.store(
            if persistent_observation {
                usize::MAX
            } else {
                1
            },
            Ordering::SeqCst,
        );
    }

    fn inject_intent_observation_failure(&self) -> bool {
        let injected = self
            .intent_observation_faults
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                if remaining == 0 {
                    None
                } else if remaining == usize::MAX {
                    Some(remaining)
                } else {
                    Some(remaining - 1)
                }
            })
            .is_ok();
        if injected && self.intent_observations.fetch_add(1, Ordering::SeqCst) == 0 {
            self.injected.add_permits(1);
        }
        injected
    }

    fn inject_settlement_observation_failure(&self) -> bool {
        let injected = self
            .settlement_observation_faults
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                if remaining == 0 {
                    None
                } else if remaining == usize::MAX {
                    Some(remaining)
                } else {
                    Some(remaining - 1)
                }
            })
            .is_ok();
        if injected && self.settlement_observations.fetch_add(1, Ordering::SeqCst) == 0 {
            self.injected.add_permits(1);
        }
        injected
    }
}

impl StateStoreBackend for NotificationConflictStore {
    type Transaction = NotificationConflictTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let notification = key.namespace().as_str() == "agent_sdk_conversation_notifications_v1";
        if notification && self.receipt_read_observation.swap(false, Ordering::SeqCst) {
            self.hold_receipt_read().await;
        }
        if notification && self.unavailable_observation.swap(false, Ordering::SeqCst) {
            self.injected.add_permits(1);
            Err(StateStoreError::unavailable(
                "load",
                "injected notification observation failure",
            ))
        } else {
            let observed = self.inner.load(key).await?;
            let resolving = notification
                && observed.as_ref().is_some_and(|snapshot| {
                    snapshot
                        .record
                        .value
                        .pointer("/pending/0/state/state")
                        .and_then(serde_json::Value::as_str)
                        == Some("resolving")
                });
            let terminal = notification
                && observed.as_ref().is_some_and(|snapshot| {
                    snapshot
                        .record
                        .value
                        .pointer("/pending/0/state/state")
                        .and_then(serde_json::Value::as_str)
                        == Some("terminal")
                });
            if terminal
                && let Some(before) = self
                    .pre_settlement_snapshot
                    .lock()
                    .expect("settlement snapshot mutex")
                    .take()
            {
                self.settlement_observations.fetch_add(1, Ordering::SeqCst);
                return Ok(Some(before));
            }
            if terminal && self.inject_settlement_observation_failure() {
                Err(StateStoreError::unavailable(
                    "load",
                    "injected interaction settlement observation failure",
                ))
            } else if resolving && self.inject_intent_observation_failure() {
                Err(StateStoreError::unavailable(
                    "load",
                    "injected interaction intent observation failure",
                ))
            } else {
                Ok(observed)
            }
        }
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        Ok(NotificationConflictTransaction {
            inner: self.inner.begin_transaction().await?,
            owner: self.clone(),
            read_notification: false,
            wrote_notification: false,
            wrote_resolution_intent: false,
            wrote_resolution_settlement: false,
            wrote_prompt_waiter: false,
        })
    }
}

impl StateTransactionBackend for NotificationConflictTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.read_notification |=
            key.namespace().as_str() == "agent_sdk_conversation_notifications_v1";
        self.inner.load(key).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let prompt_waiter = record.key.namespace().as_str() == "agent_sdk_custom_continuations_v1"
            && record
                .value
                .pointer("/terminal/waiter")
                .and_then(serde_json::Value::as_str)
                == Some("discarded");
        if prompt_waiter && self.owner.prompt_waiter_conflict.load(Ordering::SeqCst) {
            if self
                .owner
                .prompt_waiter_attempts
                .fetch_add(1, Ordering::SeqCst)
                == 0
            {
                self.owner.injected.add_permits(1);
            }
            return Err(StateStoreError::transaction_conflict(
                "put",
                "injected prompt waiter conflict",
            ));
        }
        self.wrote_prompt_waiter |= prompt_waiter
            && self
                .owner
                .prompt_waiter_hold
                .lock()
                .expect("prompt waiter gate mutex")
                .is_some();
        let notification =
            record.key.namespace().as_str() == "agent_sdk_conversation_notifications_v1";
        let resolution_intent = notification
            && record
                .value
                .pointer("/pending/0/state/state")
                .and_then(serde_json::Value::as_str)
                == Some("resolving")
            && record
                .value
                .pointer("/pending/0/state/detail/acceptance")
                .and_then(serde_json::Value::as_str)
                == Some("unconfirmed");
        let resolution_settlement = notification
            && record
                .value
                .pointer("/pending/0/state/state")
                .and_then(serde_json::Value::as_str)
                == Some("terminal");
        if resolution_settlement
            && self
                .owner
                .stale_settlement_observation
                .swap(false, Ordering::SeqCst)
        {
            let before = self.inner.load(record.key.clone()).await?;
            *self
                .owner
                .pre_settlement_snapshot
                .lock()
                .expect("settlement snapshot mutex") = before;
        }
        if notification && self.owner.conflict.swap(false, Ordering::SeqCst) {
            self.owner.injected.add_permits(1);
            Err(StateStoreError::transaction_conflict(
                "put",
                "injected notification publication conflict",
            ))
        } else if notification && self.owner.persistent_conflict.load(Ordering::SeqCst) {
            if self
                .owner
                .persistent_attempts
                .fetch_add(1, Ordering::SeqCst)
                == 0
            {
                self.owner.injected.add_permits(1);
            }
            Err(StateStoreError::transaction_conflict(
                "put",
                "injected permanent notification publication conflict",
            ))
        } else {
            self.wrote_notification |= notification;
            self.wrote_resolution_intent |= resolution_intent;
            self.wrote_resolution_settlement |= resolution_settlement;
            if resolution_intent {
                self.owner.intent_writes.fetch_add(1, Ordering::SeqCst);
            }
            if resolution_settlement {
                self.owner.settlement_writes.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.put(record).await
        }
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.wrote_notification |=
            key.namespace().as_str() == "agent_sdk_conversation_notifications_v1";
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        let Self {
            inner,
            owner,
            read_notification,
            wrote_notification,
            wrote_resolution_intent,
            wrote_resolution_settlement,
            wrote_prompt_waiter,
        } = self;
        if read_notification
            && !wrote_notification
            && owner.receipt_read_hold.swap(false, Ordering::SeqCst)
        {
            inner.commit().await?;
            owner.receipt_read_observation.store(true, Ordering::SeqCst);
            return Ok(());
        }
        let timeout = owner
            .commit_timeout
            .lock()
            .expect("commit fault mutex")
            .take();
        if let Some(commit_first) = timeout {
            if commit_first {
                inner.commit().await?;
            } else {
                inner.rollback().await?;
            }
            return Err(StateStoreError::TimedOut {
                operation: "secret-commit-operation".into(),
                timeout_ms: 41,
            });
        }
        let hold = if wrote_prompt_waiter {
            owner
                .prompt_waiter_hold
                .lock()
                .expect("prompt waiter gate mutex")
                .take()
        } else {
            None
        };
        if let Some(hold) = hold {
            if matches!(hold, PromptWaiterCommitHold::Before) {
                owner.hold_prompt_waiter_commit().await;
            }
            owner.prompt_waiter_commits.fetch_add(1, Ordering::SeqCst);
            let result = inner.commit().await;
            if matches!(hold, PromptWaiterCommitHold::After) {
                owner.hold_prompt_waiter_commit().await;
            }
            owner.prompt_waiter_receipts.fetch_add(1, Ordering::SeqCst);
            return result;
        }
        if wrote_resolution_settlement && owner.ambiguous_settlement.swap(false, Ordering::SeqCst) {
            inner.commit().await?;
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous interaction settlement commit",
            ))
        } else if wrote_resolution_intent && owner.ambiguous_intent.swap(false, Ordering::SeqCst) {
            inner.commit().await?;
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous interaction intent commit",
            ))
        } else if wrote_notification && owner.ambiguous.swap(false, Ordering::SeqCst) {
            inner.commit().await?;
            owner.unavailable_observation.store(true, Ordering::SeqCst);
            Err(StateStoreError::unavailable(
                "commit",
                "injected ambiguous notification commit",
            ))
        } else {
            inner.commit().await
        }
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        let hold = self.read_notification
            && !self.wrote_notification
            && self.owner.receipt_read_hold.swap(false, Ordering::SeqCst);
        self.inner.rollback().await?;
        if hold {
            self.owner.hold_receipt_read().await;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum HandlerBehavior {
    Complete,
    ModelError,
    Media,
    MalformedMedia,
    Empty { is_error: bool, metadata_only: bool },
    Structured { is_error: bool, null: bool },
    Interaction,
}

struct ScriptedToolset {
    name: ToolName,
    schema: JsonSchema,
    behavior: HandlerBehavior,
    calls: Arc<AtomicUsize>,
    cancellation: Arc<CancellationProbe>,
}

struct ScriptedHandler {
    name: ToolName,
    behavior: HandlerBehavior,
    phase: HandlerPhase,
    calls: Arc<AtomicUsize>,
    cancellation: Arc<CancellationProbe>,
}

enum HandlerPhase {
    Ready,
    Waiting,
    Finished,
}

#[derive(Default)]
struct CancellationProbe {
    created: AtomicUsize,
    count: AtomicUsize,
    shutdowns: AtomicUsize,
    shutdown_timeout: AtomicBool,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl ScriptedToolset {
    fn new(
        name: &str,
        behavior: HandlerBehavior,
        calls: Arc<AtomicUsize>,
        cancellation: Arc<CancellationProbe>,
    ) -> Self {
        Self {
            name: ToolName::new(name).expect("fixture tool name must be valid"),
            schema: JsonSchema::strict(serde_json::json!({
                "type": "object",
                "additionalProperties": false
            })),
            behavior,
            calls,
            cancellation,
        }
    }

    fn info(&self) -> ToolInfo {
        let mut info = ToolInfo::new(self.name.clone(), self.schema.clone());
        if matches!(self.behavior, HandlerBehavior::Interaction) {
            info.interactions = ToolInteractionCapabilities {
                text: true,
                ..ToolInteractionCapabilities::default()
            };
        }
        info
    }
}

impl ToolsetBackend for ScriptedToolset {
    type Handler = ScriptedHandler;

    fn list_tools(&self) -> impl Future<Output = Result<Vec<ToolInfo>, ToolError>> + Send + '_ {
        std::future::ready(Ok(vec![self.info()]))
    }

    fn tool_handler(
        &self,
        name: ToolName,
    ) -> impl Future<Output = Result<Self::Handler, ToolError>> + Send + '_ {
        let result = if name == self.name {
            self.cancellation.created.fetch_add(1, Ordering::SeqCst);
            Ok(ScriptedHandler {
                name,
                behavior: self.behavior,
                phase: HandlerPhase::Ready,
                calls: Arc::clone(&self.calls),
                cancellation: Arc::clone(&self.cancellation),
            })
        } else {
            Err(ToolError::UnknownTool { name })
        };
        std::future::ready(result)
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        self.cancellation.shutdowns.fetch_add(1, Ordering::SeqCst);
        if self.cancellation.shutdown_timeout.load(Ordering::SeqCst) {
            Err(ToolError::Timeout {
                tool: self.name.clone(),
                timeout_secs: 5,
            })
        } else {
            Ok(())
        }
    }
}

impl ToolHandlerBackend for ScriptedHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match (&self.phase, input) {
            (HandlerPhase::Ready, ToolHandlerInput::Invoke(_)) => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                match self.behavior {
                    HandlerBehavior::Complete => {
                        self.phase = HandlerPhase::Finished;
                        Ok(ToolHandlerOutput::Completed(ToolResult::text(
                            "fixture result",
                        )))
                    }
                    HandlerBehavior::ModelError => {
                        self.phase = HandlerPhase::Finished;
                        Ok(ToolHandlerOutput::Completed(
                            ToolResult::text("caller tool declined this action").with_error(true),
                        ))
                    }
                    HandlerBehavior::Media => {
                        self.phase = HandlerPhase::Finished;
                        Ok(ToolHandlerOutput::Completed(
                            ToolResult::new(vec![
                                ToolContent::Image {
                                    mime_type: "image/png".to_owned(),
                                    data_base64: "AQID".to_owned(),
                                    metadata: None,
                                },
                                ToolContent::Audio {
                                    mime_type: "audio/wav".to_owned(),
                                    data_base64: "BAU=".to_owned(),
                                    metadata: None,
                                },
                            ])
                            .expect("fixture media result must be valid")
                            .with_structured_content(serde_json::json!({"structured": "fixture"})),
                        ))
                    }
                    HandlerBehavior::MalformedMedia => {
                        self.phase = HandlerPhase::Finished;
                        Ok(ToolHandlerOutput::Completed(
                            ToolResult::new(vec![ToolContent::Image {
                                mime_type: "image/png".to_owned(),
                                data_base64: "not-base64".to_owned(),
                                metadata: None,
                            }])
                            .expect("fixture malformed media envelope must be valid"),
                        ))
                    }
                    HandlerBehavior::Empty {
                        is_error,
                        metadata_only,
                    } => {
                        self.phase = HandlerPhase::Finished;
                        let mut metadata = std::collections::BTreeMap::new();
                        if metadata_only {
                            metadata.insert(
                                "diagnostic".to_owned(),
                                serde_json::json!("not model-visible"),
                            );
                        }
                        Ok(ToolHandlerOutput::Completed(ToolResult {
                            content: Vec::new(),
                            structured_content: None,
                            is_error,
                            metadata,
                        }))
                    }
                    HandlerBehavior::Structured { is_error, null } => {
                        self.phase = HandlerPhase::Finished;
                        Ok(ToolHandlerOutput::Completed(ToolResult {
                            content: Vec::new(),
                            structured_content: Some(if null {
                                serde_json::Value::Null
                            } else {
                                serde_json::json!({"result": "structured only"})
                            }),
                            is_error,
                            metadata: std::collections::BTreeMap::new(),
                        }))
                    }
                    HandlerBehavior::Interaction => {
                        self.phase = HandlerPhase::Waiting;
                        Ok(ToolHandlerOutput::Interaction(ToolInteractionRequest {
                            id: ToolInteractionId::new(),
                            tool_name: self.name.clone(),
                            call_id: Some("fixture-call".to_owned()),
                            prompt: "wait for cancellation".to_owned(),
                            kind: ToolInteractionKind::Text { multiline: false },
                        }))
                    }
                }
            }
            (HandlerPhase::Waiting, ToolHandlerInput::Cancel { reason }) => {
                self.phase = HandlerPhase::Finished;
                self.cancellation.count.fetch_add(1, Ordering::SeqCst);
                self.cancellation.started.notify_waiters();
                self.cancellation.release.notified().await;
                Err(ToolError::Cancelled {
                    tool: self.name.clone(),
                    reason,
                })
            }
            (_, ToolHandlerInput::Cancel { reason }) => Err(ToolError::Cancelled {
                tool: self.name.clone(),
                reason,
            }),
            _ => Err(ToolError::InvalidConfiguration {
                message: "fixture handler received an invalid transition".to_owned(),
            }),
        }
    }
}

struct CustomPromptFixture<S: StateStoreBackend = StateStore> {
    storage: AgentStorage<S>,
    raw_store: Option<StateStore>,
    agent: Agent<S, storage_sdk::Database, ScriptedProvider, ScriptedToolset>,
    workspace: Workspace<S, storage_sdk::Database, ScriptedProvider, ScriptedToolset>,
    conversation: Conversation<S, storage_sdk::Database, ScriptedProvider, ScriptedToolset>,
    root: PathBuf,
    state: Arc<ScriptedState>,
    calls: Arc<AtomicUsize>,
    cancellation: Arc<CancellationProbe>,
    toolset_count: usize,
}

enum CallerTools {
    Scripted(ScriptedToolset),
    Local(tool_sdk::Toolset),
}

enum CallerHandler {
    Scripted(ScriptedHandler),
    Local(tool_sdk::ToolHandler),
}

impl ToolsetBackend for CallerTools {
    type Handler = CallerHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        match self {
            Self::Scripted(tools) => tools.list_tools().await,
            Self::Local(tools) => tools.list_tools().await,
        }
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        match self {
            Self::Scripted(tools) => tools.tool_handler(name).await.map(CallerHandler::Scripted),
            Self::Local(tools) => tools.tool_handler(name).await.map(CallerHandler::Local),
        }
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        match self {
            Self::Scripted(tools) => tools.shutdown().await,
            Self::Local(tools) => tools.shutdown().await,
        }
    }
}

impl ToolHandlerBackend for CallerHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match self {
            Self::Scripted(handler) => handler.step(input).await,
            Self::Local(handler) => handler.step(input).await,
        }
    }
}

impl CustomPromptFixture<StateStore> {
    async fn build(
        selection: ToolSelection,
        responses: Vec<GenerateResponse<ScriptedContinuation>>,
        behavior: HandlerBehavior,
        root: Option<PathBuf>,
    ) -> Self {
        Self::build_with_config(
            selection,
            responses,
            behavior,
            root,
            StateStoreConfig::Lance(LanceStateStoreConfig::memory()),
        )
        .await
    }

    async fn build_with_config(
        selection: ToolSelection,
        responses: Vec<GenerateResponse<ScriptedContinuation>>,
        behavior: HandlerBehavior,
        root: Option<PathBuf>,
        config: StateStoreConfig,
    ) -> Self {
        let store = StateStore::connect(config)
            .await
            .expect("fixture StateStore must build");
        let mut fixture = Self::build_with_store(
            selection,
            responses,
            behavior,
            root,
            CustomToolApproval::Allow,
            store.clone(),
        )
        .await;
        fixture.raw_store = Some(store);
        fixture
    }
}

impl CustomPromptFixture<NotificationConflictStore> {
    async fn build_waiter_deadline(primary_failure: bool) -> (Self, NotificationConflictStore) {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("waiter deadline store");
        let store = NotificationConflictStore::new(raw.clone());
        let mut fixture = Self::build_with_store(
            ToolSelection::None,
            vec![ScriptedState::terminal_response("reused after deadline")],
            HandlerBehavior::Complete,
            None,
            CustomToolApproval::Allow,
            store.clone(),
        )
        .await;
        fixture.raw_store = Some(raw);
        fixture.state.set_streams(vec![if primary_failure {
            vec![Err(ProviderError::new(
                ProviderErrorKind::InvalidResponse(ResponseViolation::UnexpectedEnd),
                ProviderOperation::GenerateStream,
                ProviderErrorPhase::Stream,
            ))]
        } else {
            vec![Ok(GenerateEvent::Completed(Box::new(
                ScriptedState::terminal_response("durable terminal"),
            )))]
        }]);
        (fixture, store)
    }

    async fn stream_after_waiter_fault(
        &self,
        store: &NotificationConflictStore,
        request: PromptRequest,
    ) -> Result<PromptStream, SdkError> {
        let stream = self.conversation.prompt_stream(request).await?;
        store
            .injected
            .acquire()
            .await
            .map_err(|_| SdkError::closed("waiter injection gate closed"))?
            .forget();
        Ok(stream)
    }

    async fn waiter_is_discarded(&self) -> bool {
        let key = StateKey::new(
            "agent_sdk_custom_continuations_v1",
            self.conversation.id().as_str(),
        )
        .expect("continuation key");
        let store = self.raw_store.as_ref().expect("raw deadline store");
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if store.load(key.clone()).await.is_ok_and(|record| {
                    record.is_some_and(|record| {
                        record.record.value.pointer("/terminal/waiter")
                            == Some(&serde_json::json!("discarded"))
                    })
                }) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok()
    }
}

impl<S: StateStoreBackend> CustomPromptFixture<S> {
    async fn build_with_store(
        selection: ToolSelection,
        responses: Vec<GenerateResponse<ScriptedContinuation>>,
        behavior: HandlerBehavior,
        root: Option<PathBuf>,
        approval: CustomToolApproval,
        store: S,
    ) -> Self {
        Self::build_with_tools(
            selection,
            responses,
            behavior,
            root,
            approval,
            store,
            &[TOOL],
        )
        .await
    }

    async fn build_with_tools(
        selection: ToolSelection,
        responses: Vec<GenerateResponse<ScriptedContinuation>>,
        behavior: HandlerBehavior,
        root: Option<PathBuf>,
        approval: CustomToolApproval,
        store: S,
        tool_names: &[&str],
    ) -> Self {
        let state = ScriptedState::new(responses);
        let calls = Arc::new(AtomicUsize::new(0));
        let cancellation = Arc::new(CancellationProbe::default());
        let custom = CustomAgentConfig::new(
            ModelSelector::new(MODEL).expect("fixture model selector must be valid"),
            selection,
            CustomLoopLimits::new(4, 4, 1).expect("fixture limits must be valid"),
            approval,
        )
        .expect("fixture Custom config must be valid");
        let config = AgentConfig::new(
            custom.into(),
            Duration::from_secs(3),
            NonZeroUsize::new(1).expect("one prompt must be nonzero"),
        )
        .expect("fixture Agent config must be valid");
        let storage = AgentStorage::new(store);
        let builder = AgentBuilder::create_for_backends::<ScriptedProvider, ScriptedToolset>(
            config,
            storage.clone(),
        )
        .with_provider(ScriptedProvider {
            state: Arc::clone(&state),
        });
        let mut builder = builder;
        for name in tool_names {
            builder = builder.with_toolset(ScriptedToolset::new(
                name,
                behavior,
                Arc::clone(&calls),
                Arc::clone(&cancellation),
            ));
        }
        let agent = builder.build().await.expect("fixture Agent must build");
        let root = root.unwrap_or_else(|| {
            std::env::temp_dir().join(format!(
                "agent-sdk-custom-prompt-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4().simple()
            ))
        });
        std::fs::create_dir_all(&root).expect("fixture root must be created");
        let root = std::fs::canonicalize(root).expect("fixture root must canonicalize");
        let context = WorkspaceContext::new(
            root.clone(),
            vec![
                WorkspaceRoot::new(root.clone(), WorkspaceRootAccess::ReadWrite)
                    .expect("fixture workspace root must be valid"),
            ],
            WorkspaceExecutionPolicy::new(
                WorkspaceSandboxMode::FullAccess,
                WorkspaceNetworkAccess::Enabled,
            ),
        )
        .expect("fixture workspace context must be valid");
        let workspace = agent
            .new_workspace(
                NewWorkspace::new("custom-prompt", context, WorkspaceDefaults::empty())
                    .expect("fixture workspace request must be valid"),
            )
            .await
            .expect("fixture workspace must build");
        let conversation = workspace
            .new_conversation(
                NewConversation::new(None, None, Vec::new())
                    .expect("fixture conversation request must be valid"),
            )
            .await
            .expect("fixture conversation must build");
        Self {
            storage,
            raw_store: None,
            agent,
            workspace,
            conversation,
            root,
            state,
            calls,
            cancellation,
            toolset_count: tool_names.len(),
        }
    }

    async fn approve_prompt(
        &self,
        conflicts: &NotificationConflictStore,
        receiver: &mut ConversationNotificationReceiver,
        output: &str,
    ) {
        let mut prompt =
            Box::pin(self.conversation.prompt(
                PromptRequest::new("approve the tool").expect("fixture prompt must be valid"),
            ));
        let injection = conflicts.injected.acquire();
        tokio::pin!(injection);
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                permit = &mut injection => permit
                    .expect("notification injection barrier must remain open")
                    .forget(),
                result = &mut prompt => {
                    panic!("prompt settled before its publication fault was injected: {result:?}")
                }
            }
        })
        .await
        .expect("approval publication fault injection must remain bounded");
        let notification = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                item = receiver.recv() => item
                    .expect("notification stream must remain open")
                    .expect("notification delivery must succeed"),
                result = &mut prompt => {
                    panic!("prompt settled before approval delivery: {result:?}")
                }
            }
        })
        .await
        .expect("approval publication must remain bounded");
        assert!(matches!(
            notification.kind(),
            ConversationNotificationKind::Interaction {
                response_schema: None,
                dismissible: false,
                ..
            }
        ));
        self.conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow)
            .await
            .expect("approval must resolve");
        let response = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
            .await
            .expect("approved prompt must remain bounded")
            .expect("approved prompt must complete");
        assert_eq!(response.output(), output);
    }

    async fn shutdown(self) {
        assert_eq!(self.cancellation.shutdowns.load(Ordering::SeqCst), 0);
        self.conversation
            .close()
            .await
            .expect("fixture conversation must close");
        self.workspace
            .close()
            .await
            .expect("fixture workspace must close");
        assert_eq!(
            self.cancellation.shutdowns.load(Ordering::SeqCst),
            0,
            "child cleanup must retain the Agent-owned Toolset"
        );
        self.agent
            .shutdown()
            .await
            .expect("fixture Agent must shut down");
        assert_eq!(
            self.cancellation.shutdowns.load(Ordering::SeqCst),
            self.toolset_count
        );
        std::fs::remove_dir_all(self.root).expect("fixture root must be removed");
    }
}

#[tokio::test]
async fn custom_reasoning_preserves_exact_portable_vocabulary_and_omission() {
    use agent_sdk::model::ReasoningEffort;
    use provider_sdk::generation::{ReasoningConfig, ReasoningEffort as ProviderEffort};

    let mappings = [
        ("minimal", ProviderEffort::Minimal),
        ("low", ProviderEffort::Low),
        ("medium", ProviderEffort::Medium),
        ("high", ProviderEffort::High),
        ("maximum", ProviderEffort::Maximum),
    ];
    let fixture = CustomPromptFixture::build(
        ToolSelection::Auto,
        vec![ScriptedState::terminal_response("exact answer"); mappings.len() + 1],
        HandlerBehavior::Complete,
        None,
    )
    .await;
    let observed = std::panic::AssertUnwindSafe(async {
        for (selection, expected) in mappings {
            let response = fixture
                .conversation
                .prompt(
                    PromptRequest::new("explicit effort")
                        .unwrap()
                        .with_reasoning_effort(ReasoningEffort::new(selection).unwrap()),
                )
                .await
                .unwrap();
            assert_eq!(response.output(), "exact answer");
            assert_eq!(
                fixture.state.reasoning_configs.lock().unwrap().last(),
                Some(&Some(ReasoningConfig {
                    effort: Some(expected),
                    ..ReasoningConfig::default()
                }))
            );
        }
        let accepted = fixture.state.requests().len();
        for selection in ["none", "xhigh", "max", "ultra", "High"] {
            let error = fixture
                .conversation
                .prompt(
                    PromptRequest::new("unsupported effort")
                        .unwrap()
                        .with_reasoning_effort(ReasoningEffort::new(selection).unwrap()),
                )
                .await
                .expect_err("unsupported vocabulary cannot reach the Provider");
            assert_eq!(error.kind(), agent_sdk::error::ErrorKind::Unsupported);
            assert_eq!(fixture.state.requests().len(), accepted);
        }
        let response = fixture
            .conversation
            .prompt(PromptRequest::new("omitted effort").unwrap())
            .await
            .unwrap();
        assert_eq!(response.output(), "exact answer");
        assert_eq!(
            fixture.state.reasoning_configs.lock().unwrap().last(),
            Some(&None)
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    })
    .catch_unwind()
    .await;
    fixture.shutdown().await;
    if let Err(panic) = observed {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn custom_approval_publication_reconciles_transaction_faults() {
    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("fixture StateStore must build");
    let conflicts = NotificationConflictStore::new(inner);
    let fixture = CustomPromptFixture::build_with_store(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        vec![
            ScriptedState::function_response(),
            ScriptedState::function_response(),
            ScriptedState::terminal_response("after-conflict"),
            ScriptedState::function_response(),
            ScriptedState::terminal_response("after-ambiguous-commit"),
            ScriptedState::function_response(),
            ScriptedState::terminal_response("after-ambiguous-intent"),
            ScriptedState::function_response(),
            ScriptedState::terminal_response("after-ambiguous-settlement"),
        ],
        HandlerBehavior::Complete,
        None,
        CustomToolApproval::Interaction,
        conflicts.clone(),
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("fixture notification receiver must open");

    conflicts.arm_persistent_conflict();
    let persistent_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut prompt = Box::pin(
        fixture.conversation.prompt(
            PromptRequest::new("time out the approval publication")
                .expect("fixture prompt must be valid"),
        ),
    );
    let injection = conflicts.injected.acquire();
    tokio::pin!(injection);
    tokio::time::timeout_at(persistent_deadline, async {
        tokio::select! {
            permit = &mut injection => permit
                .expect("notification injection barrier must remain open")
                .forget(),
            result = &mut prompt => {
                panic!("prompt settled before its persistent publication fault was injected: {result:?}")
            }
        }
    })
    .await
    .expect("persistent approval publication fault injection must remain bounded");
    let error = tokio::time::timeout_at(persistent_deadline, async {
        tokio::select! {
            result = &mut prompt => result,
            item = receiver.recv() => {
                panic!("permanently conflicted publication must not deliver a notification: {item:?}")
            }
        }
    })
    .await
    .expect("permanent publication conflict must stop within the original lifecycle deadline")
    .expect_err("permanent publication conflict must time out");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::Timeout
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    let attempts = conflicts.persistent_attempts.load(Ordering::SeqCst);
    assert!(
        attempts > 1,
        "persistent conflict must retry exact attempts"
    );
    tokio::task::yield_now().await;
    assert_eq!(
        conflicts.persistent_attempts.load(Ordering::SeqCst),
        attempts
    );
    assert!(conflicts.persistent_conflict.swap(false, Ordering::SeqCst));
    drop(prompt);

    conflicts.arm_conflict();
    fixture
        .approve_prompt(&conflicts, &mut receiver, "after-conflict")
        .await;
    assert!(
        !conflicts.conflict.load(Ordering::SeqCst),
        "approval publication must consume the exact injected conflict"
    );

    conflicts.arm_ambiguous_commit();
    fixture
        .approve_prompt(&conflicts, &mut receiver, "after-ambiguous-commit")
        .await;
    assert!(
        !conflicts.ambiguous.load(Ordering::SeqCst)
            && !conflicts.unavailable_observation.load(Ordering::SeqCst),
        "approval publication must reconcile the ambiguous commit and transient observation"
    );

    let mut prompt = Box::pin(fixture.conversation.prompt(
        PromptRequest::new("reconcile the approval intent").expect("fixture prompt must be valid"),
    ));
    let notification = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            item = receiver.recv() => item
                .expect("notification stream must remain open")
                .expect("notification delivery must succeed"),
            result = &mut prompt => {
                panic!("prompt settled before intent approval delivery: {result:?}")
            }
        }
    })
    .await
    .expect("intent approval publication must remain bounded");
    conflicts.arm_ambiguous_intent(false);
    let mut resolution = Box::pin(
        fixture
            .conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow),
    );
    let injection = conflicts.injected.acquire();
    tokio::pin!(injection);
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            permit = &mut injection => permit
                .expect("notification injection barrier must remain open")
                .forget(),
            result = &mut resolution => {
                panic!("resolution settled before its intent observation fault was injected: {result:?}")
            }
        }
    })
    .await
    .expect("intent observation fault injection must remain bounded");
    resolution
        .await
        .expect("ambiguous intent commit must reconcile without another attempt");
    let response = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
        .await
        .expect("intent-reconciled prompt must remain bounded")
        .expect("intent-reconciled prompt must complete");
    assert_eq!(response.output(), "after-ambiguous-intent");
    assert_eq!(conflicts.intent_writes.load(Ordering::SeqCst), 1);
    assert_eq!(conflicts.intent_observations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 3);
    drop(prompt);

    let mut prompt = Box::pin(
        fixture.conversation.prompt(
            PromptRequest::new("reconcile the approval settlement")
                .expect("fixture prompt must be valid"),
        ),
    );
    let notification = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            item = receiver.recv() => item
                .expect("notification stream must remain open")
                .expect("notification delivery must succeed"),
            result = &mut prompt => {
                panic!("prompt settled before settlement approval delivery: {result:?}")
            }
        }
    })
    .await
    .expect("settlement approval publication must remain bounded");
    conflicts.arm_ambiguous_settlement(false);
    let mut resolution = Box::pin(
        fixture
            .conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow),
    );
    let injection = conflicts.injected.acquire();
    tokio::pin!(injection);
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            permit = &mut injection => permit
                .expect("settlement observation barrier must remain open")
                .forget(),
            result = &mut resolution => {
                panic!("resolution settled before its settlement observation fault was injected: {result:?}")
            }
        }
    })
    .await
    .expect("settlement observation fault must be injected");
    resolution
        .await
        .expect("ambiguous settlement must reconcile without another mutation");
    let response = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
        .await
        .expect("settlement-reconciled prompt must remain bounded")
        .expect("settlement-reconciled prompt must complete");
    assert_eq!(response.output(), "after-ambiguous-settlement");
    assert_eq!(conflicts.settlement_writes.load(Ordering::SeqCst), 1);
    assert_eq!(conflicts.settlement_observations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 4);

    drop(prompt);
    drop(receiver);
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_approval_confirmed_settlement_never_replays_after_stale_observation() {
    use agent_sdk::error::ErrorKind;

    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("settlement fixture store");
    let conflicts = NotificationConflictStore::new(inner);
    let fixture = CustomPromptFixture::build_with_store(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        vec![ScriptedState::function_response()],
        HandlerBehavior::Complete,
        None,
        CustomToolApproval::Interaction,
        conflicts.clone(),
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("notification observer");
    let mut prompt =
        Box::pin(fixture.conversation.prompt(
            PromptRequest::new("retain the confirmed settlement receipt").expect("prompt"),
        ));
    let outcome = async {
        let notification = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                item = receiver.recv() => item
                    .ok_or_else(|| SdkError::closed("notification stream ended"))?,
                result = &mut prompt => Err(result.err().unwrap_or_else(||
                    SdkError::invalid_state("prompt completed before approval"))),
            }
        })
        .await
        .map_err(|_| SdkError::timeout("approval publication timed out"))??;
        conflicts
            .stale_settlement_observation
            .store(true, Ordering::SeqCst);
        let resolution = tokio::time::timeout(
            Duration::from_secs(3),
            fixture
                .conversation
                .resolve_notification(notification.id().clone(), NotificationResolution::Allow),
        )
        .await
        .map_err(|_| SdkError::timeout("approval resolution timed out"))?;
        let completion = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
            .await
            .map_err(|_| SdkError::timeout("prompt settlement timed out"))?;
        Ok::<_, SdkError>((resolution, completion))
    }
    .await;
    drop(prompt);
    drop(receiver);
    conflicts
        .stale_settlement_observation
        .store(false, Ordering::SeqCst);
    conflicts
        .pre_settlement_snapshot
        .lock()
        .expect("settlement snapshot mutex")
        .take();
    let abort = tokio::time::timeout(Duration::from_secs(3), fixture.conversation.abort()).await;
    let close = fixture.conversation.close().await;
    let workspace_close = fixture.workspace.close().await;
    let shutdown = fixture.agent.shutdown().await;
    let observations = conflicts.settlement_observations.load(Ordering::SeqCst);
    let writes = conflicts.settlement_writes.load(Ordering::SeqCst);
    let calls = fixture.calls.load(Ordering::SeqCst);
    let removed = std::fs::remove_dir_all(fixture.root);

    removed.expect("fixture root removed after all owned cleanup");
    abort
        .expect("abort stayed bounded")
        .expect("retained settlement reconciled");
    close.expect("conversation closed");
    workspace_close.expect("workspace closed");
    shutdown.expect("Agent shut down");
    let (resolution, completion) = outcome.expect("settlement scenario remained bounded");
    assert_eq!(
        resolution
            .expect_err("stale confirmation cannot prove settlement")
            .kind(),
        ErrorKind::InvalidState
    );
    assert!(
        completion.is_err(),
        "unconfirmed delivery cannot execute the tool"
    );
    assert_eq!(
        observations, 1,
        "the committed settlement returned one stale read"
    );
    assert_eq!(
        writes, 1,
        "confirmed commit must remain read-only during reconciliation"
    );
    assert_eq!(
        calls, 0,
        "failed approval delivery cannot execute a handler"
    );
}

#[tokio::test]
async fn custom_approval_settlement_observation_timeout_requires_recovery() {
    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("fixture StateStore must build");
    let conflicts = NotificationConflictStore::new(inner);
    let fixture = CustomPromptFixture::build_with_store(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        vec![ScriptedState::function_response()],
        HandlerBehavior::Complete,
        None,
        CustomToolApproval::Interaction,
        conflicts.clone(),
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("fixture notification receiver must open");
    let mut prompt = Box::pin(
        fixture.conversation.prompt(
            PromptRequest::new("retain the unobserved approval settlement")
                .expect("fixture prompt must be valid"),
        ),
    );
    let notification = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            item = receiver.recv() => item
                .expect("notification stream must remain open")
                .expect("notification delivery must succeed"),
            result = &mut prompt => {
                panic!("prompt settled before settlement recovery delivery: {result:?}")
            }
        }
    })
    .await
    .expect("settlement recovery publication must remain bounded");

    conflicts.arm_ambiguous_settlement(true);
    let observation_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut resolution = Box::pin(
        fixture
            .conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow),
    );
    {
        let injection = conflicts.injected.acquire();
        tokio::pin!(injection);
        tokio::time::timeout_at(observation_deadline, async {
            tokio::select! {
                permit = &mut injection => permit
                    .expect("settlement observation barrier must remain open")
                    .forget(),
                result = &mut resolution => {
                    panic!("resolution settled before persistent settlement observation began: {result:?}")
                }
            }
        })
        .await
        .expect("persistent settlement observation must begin within its operation bound");
    }
    let error = tokio::time::timeout_at(observation_deadline, &mut resolution)
        .await
        .expect("persistent settlement observation must stop within its original operation bound")
        .expect_err("unconfirmed settlement must require recovery");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::Conflict
    ));

    fixture
        .conversation
        .resolve_notification(notification.id().clone(), NotificationResolution::Allow)
        .await
        .expect_err("recovery-owned settlement must reject another resolution attempt");
    drop(resolution);
    drop(prompt);
    drop(receiver);
    let agent_id = fixture.agent.id();
    let workspace_id = fixture.workspace.id();
    let conversation_id = fixture.conversation.id();
    let CustomPromptFixture {
        storage: _,
        raw_store: _,
        agent,
        workspace,
        conversation,
        root,
        state,
        calls,
        cancellation,
        toolset_count: _,
    } = fixture;
    drop(conversation);
    drop(workspace);
    let shutdown_error = agent
        .shutdown()
        .await
        .expect_err("unconfirmed settlement must exhaust the original shutdown deadline");
    // The same absolute deadline may expire in reconciliation or cancellation's
    // wait branch. Classification and retained recovery authority are the contract;
    // the scheduler does not choose one required diagnostic message.
    assert_eq!(shutdown_error.kind(), agent_sdk::error::ErrorKind::Timeout);
    assert_eq!(
        shutdown_error.context().operation,
        agent_sdk::error::Operation::Shutdown
    );
    assert_eq!(
        shutdown_error.context().phase,
        agent_sdk::error::ErrorPhase::Cleanup
    );
    assert_eq!(
        shutdown_error.detail(),
        Some(&agent_sdk::error::ErrorDetail::DeadlineElapsed {
            kind: agent_sdk::error::DeadlineKind::Operation,
            timeout: None,
        })
    );
    let writes = conflicts.settlement_writes.load(Ordering::SeqCst);
    let observations = conflicts.settlement_observations.load(Ordering::SeqCst);
    assert_eq!(writes, 1);
    assert!(observations > 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    conflicts
        .settlement_observation_faults
        .store(0, Ordering::SeqCst);
    let custom = CustomAgentConfig::new(
        ModelSelector::new(MODEL).expect("fixture model selector must be valid"),
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        CustomLoopLimits::new(4, 4, 1).expect("fixture limits must be valid"),
        CustomToolApproval::Interaction,
    )
    .expect("fixture Custom config must be valid");
    let config = AgentConfig::new(
        custom.into(),
        Duration::from_secs(3),
        NonZeroUsize::new(1).expect("one prompt must be nonzero"),
    )
    .expect("fixture Agent config must be valid");
    let reopened = AgentBuilder::reopen_for_backends::<ScriptedProvider, ScriptedToolset>(
        config,
        AgentStorage::new(conflicts.clone()),
        agent_id,
    )
    .with_provider(ScriptedProvider { state })
    .with_toolset(ScriptedToolset::new(
        TOOL,
        HandlerBehavior::Complete,
        Arc::clone(&calls),
        cancellation,
    ))
    .build()
    .await
    .expect("fixture Agent must reopen and reconcile the unconfirmed settlement");
    assert_eq!(conflicts.settlement_writes.load(Ordering::SeqCst), writes);
    assert_eq!(
        conflicts.settlement_observations.load(Ordering::SeqCst),
        observations
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let reopened_workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("fixture workspace must reopen");
    let reopened_conversation = reopened_workspace
        .get_conversation(conversation_id)
        .await
        .expect("fixture conversation must reopen after settlement reconciliation");
    reopened_conversation
        .close()
        .await
        .expect("reconciled fixture conversation must close");
    reopened_workspace
        .close()
        .await
        .expect("reconciled fixture workspace must close");
    reopened
        .shutdown()
        .await
        .expect("reconciled fixture Agent must shut down");
    std::fs::remove_dir_all(root).expect("fixture root must be removed");
}

#[tokio::test]
async fn custom_approval_intent_observation_timeout_requires_recovery() {
    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("fixture StateStore must build");
    let conflicts = NotificationConflictStore::new(inner);
    let fixture = CustomPromptFixture::build_with_store(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        vec![ScriptedState::function_response()],
        HandlerBehavior::Complete,
        None,
        CustomToolApproval::Interaction,
        conflicts.clone(),
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("fixture notification receiver must open");
    let mut prompt = Box::pin(
        fixture.conversation.prompt(
            PromptRequest::new("retain the unresolved approval intent")
                .expect("fixture prompt must be valid"),
        ),
    );
    let notification = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            item = receiver.recv() => item
                .expect("notification stream must remain open")
                .expect("notification delivery must succeed"),
            result = &mut prompt => {
                panic!("prompt settled before recovery approval delivery: {result:?}")
            }
        }
    })
    .await
    .expect("recovery approval publication must remain bounded");

    conflicts.arm_ambiguous_intent(true);
    let observation_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut resolution = Box::pin(
        fixture
            .conversation
            .resolve_notification(notification.id().clone(), NotificationResolution::Allow),
    );
    {
        let injection = conflicts.injected.acquire();
        tokio::pin!(injection);
        tokio::time::timeout_at(observation_deadline, async {
            tokio::select! {
                permit = &mut injection => permit
                    .expect("notification injection barrier must remain open")
                    .forget(),
                result = &mut resolution => {
                    panic!("resolution settled before persistent intent observation began: {result:?}")
                }
            }
        })
        .await
        .expect("persistent intent observation must begin within its operation bound");
    }
    let error = tokio::time::timeout_at(observation_deadline, &mut resolution)
        .await
        .expect("persistent intent observation must stop within its original operation bound")
        .expect_err("unconfirmed intent must require recovery");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::Conflict
    ));
    assert_eq!(conflicts.intent_writes.load(Ordering::SeqCst), 1);
    assert!(conflicts.intent_observations.load(Ordering::SeqCst) > 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);

    fixture
        .conversation
        .resolve_notification(notification.id().clone(), NotificationResolution::Allow)
        .await
        .expect_err("recovery-owned intent must reject another resolution attempt");
    let writes = conflicts.intent_writes.load(Ordering::SeqCst);
    let observations = conflicts.intent_observations.load(Ordering::SeqCst);
    tokio::task::yield_now().await;
    assert_eq!(conflicts.intent_writes.load(Ordering::SeqCst), writes);
    assert_eq!(
        conflicts.intent_observations.load(Ordering::SeqCst),
        observations
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);

    conflicts
        .intent_observation_faults
        .store(0, Ordering::SeqCst);
    drop(resolution);
    drop(prompt);
    drop(receiver);
    let agent_id = fixture.agent.id();
    let workspace_id = fixture.workspace.id();
    let conversation_id = fixture.conversation.id();
    let CustomPromptFixture {
        storage: _,
        raw_store: _,
        agent,
        workspace,
        conversation,
        root,
        state,
        calls,
        cancellation,
        toolset_count: _,
    } = fixture;
    drop(conversation);
    drop(workspace);
    let shutdown_error = agent
        .shutdown()
        .await
        .expect_err("unconfirmed intent must make the original owner require recovery");
    assert!(matches!(
        (shutdown_error).kind(),
        agent_sdk::error::ErrorKind::Conflict
    ));

    let custom = CustomAgentConfig::new(
        ModelSelector::new(MODEL).expect("fixture model selector must be valid"),
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        CustomLoopLimits::new(4, 4, 1).expect("fixture limits must be valid"),
        CustomToolApproval::Interaction,
    )
    .expect("fixture Custom config must be valid");
    let config = AgentConfig::new(
        custom.into(),
        Duration::from_secs(3),
        NonZeroUsize::new(1).expect("one prompt must be nonzero"),
    )
    .expect("fixture Agent config must be valid");
    let reopened = AgentBuilder::reopen_for_backends::<ScriptedProvider, ScriptedToolset>(
        config,
        AgentStorage::new(conflicts),
        agent_id,
    )
    .with_provider(ScriptedProvider { state })
    .with_toolset(ScriptedToolset::new(
        TOOL,
        HandlerBehavior::Complete,
        Arc::clone(&calls),
        cancellation,
    ))
    .build()
    .await
    .expect("fixture Agent must reopen and reconcile the unconfirmed intent");
    let reopened_workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("fixture workspace must reopen");
    let reopened_conversation = reopened_workspace
        .get_conversation(conversation_id)
        .await
        .expect("fixture conversation must reopen after intent reconciliation");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    reopened_conversation
        .close()
        .await
        .expect("reconciled fixture conversation must close");
    reopened_workspace
        .close()
        .await
        .expect("reconciled fixture workspace must close");
    reopened
        .shutdown()
        .await
        .expect("reconciled fixture Agent must shut down");
    std::fs::remove_dir_all(root).expect("fixture root must be removed");
}

#[tokio::test]
async fn required_tool_choice_relaxes_after_one_function_call() {
    let function_message = Message::assistant(
        vec![
            ContentPart::Thinking {
                text: Some("retained model reasoning".to_owned()),
            },
            ContentPart::Text {
                text: "calling the requested tool".to_owned(),
            },
        ],
        vec![
            FunctionCall::new(Some("fixture-call".to_owned()), TOOL, serde_json::json!({}))
                .expect("fixture function call must validate"),
        ],
    )
    .expect("assistant message must validate")
    .with_continuation(ScriptedContinuation("fixture-continuation".to_owned()))
    .expect("assistant continuation must validate");
    let fixture = CustomPromptFixture::build(
        ToolSelection::Required,
        vec![
            ScriptedState::response(function_message.clone(), FinishReason::ToolCall),
            ScriptedState::terminal_response("done"),
        ],
        HandlerBehavior::Media,
        None,
    )
    .await;

    let response = fixture
        .conversation
        .prompt(PromptRequest::new("run required tool").expect("fixture prompt must be valid"))
        .await
        .expect("required prompt must complete");
    assert_eq!(response.output(), "done");
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.state.choices(),
        vec![ToolChoice::Required, ToolChoice::Auto]
    );
    let requests = fixture.state.requests();
    assert_eq!(requests.len(), 2);
    let second = &requests[1];
    let assistant = second
        .iter()
        .position(|message| message.role() == Role::Assistant)
        .expect("continued request must retain the assistant turn");
    assert_eq!(second[assistant], function_message);
    assert_eq!(second[assistant + 1].role(), Role::Tool);
    assert_eq!(second[assistant + 1].function_results().len(), 1);
    let result = &second[assistant + 1].function_results()[0];
    assert_eq!(result.call_id(), Some("fixture-call"));
    assert_eq!(result.name(), TOOL);
    assert!(!result.is_error());
    assert_eq!(
        result.content(),
        [
            ContentPart::Image {
                media_type: "image/png".to_owned(),
                data: vec![1, 2, 3],
            },
            ContentPart::Audio {
                media_type: "audio/wav".to_owned(),
                data: vec![4, 5],
            },
            ContentPart::Json {
                value: serde_json::json!({"structured": "fixture"}),
            },
        ]
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_rejects_nonterminal_completion_reasons_before_success() {
    for finish_reason in [
        None,
        Some(FinishReason::Length),
        Some(FinishReason::ContentFilter),
        Some(FinishReason::Error),
        Some(FinishReason::Paused),
        Some(FinishReason::ToolCall),
    ] {
        let mut rejected = ScriptedState::terminal_response("nonempty rejected partial answer");
        rejected.finish_reason = finish_reason.clone();
        let fixture = CustomPromptFixture::build(
            ToolSelection::None,
            vec![
                rejected,
                ScriptedState::terminal_response("reused after rejection"),
            ],
            HandlerBehavior::Complete,
            None,
        )
        .await;
        let result = fixture
            .conversation
            .prompt(PromptRequest::new("reject this incomplete answer").expect("prompt"))
            .await;
        let reused = fixture
            .conversation
            .prompt(PromptRequest::new("fresh prompt after rejection").expect("reuse prompt"))
            .await;
        let requests = fixture.state.requests();
        fixture.shutdown().await;

        let error = result.expect_err("a nonterminal finish cannot become prompt success");
        ScriptedState::assert_finish_failure(finish_reason.as_ref(), &error);
        assert_eq!(
            reused
                .expect("rejected generation releases admission")
                .output(),
            "reused after rejection"
        );
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1],
            vec![
                Message::user_text("fresh prompt after rejection").expect("expected fresh message")
            ]
        );
    }
}

#[tokio::test]
async fn custom_stream_rejects_nonterminal_completion_after_partial_output() {
    for finish_reason in [
        None,
        Some(FinishReason::Length),
        Some(FinishReason::ContentFilter),
        Some(FinishReason::Error),
        Some(FinishReason::Paused),
        Some(FinishReason::ToolCall),
    ] {
        let mut rejected = ScriptedState::terminal_response("nonempty rejected final answer");
        rejected.finish_reason = finish_reason.clone();
        let fixture = CustomPromptFixture::build(
            ToolSelection::None,
            vec![ScriptedState::terminal_response(
                "reused after rejected stream",
            )],
            HandlerBehavior::Complete,
            None,
        )
        .await;
        fixture.state.set_streams(vec![vec![
            Ok(GenerateEvent::ContentDelta {
                delta: "observed partial fragment".to_owned(),
            }),
            Ok(GenerateEvent::Completed(Box::new(rejected))),
        ]]);
        let mut output = fixture
            .conversation
            .prompt_stream(PromptRequest::new("incomplete streamed response").expect("prompt"))
            .await
            .expect("stream admission");
        let partial = tokio::time::timeout(Duration::from_secs(3), output.recv())
            .await
            .expect("partial output");
        let terminal = tokio::time::timeout(Duration::from_secs(3), output.recv())
            .await
            .expect("terminal output");
        let eof = tokio::time::timeout(Duration::from_secs(3), output.recv())
            .await
            .expect("stream EOF");
        let fused = tokio::time::timeout(Duration::from_secs(3), output.recv())
            .await
            .expect("fused stream EOF");
        let reused = fixture
            .conversation
            .prompt(PromptRequest::new("fresh prompt after rejected stream").expect("reuse prompt"))
            .await;
        let requests = fixture.state.requests();
        fixture.shutdown().await;

        assert!(
            matches!(partial, Some(Ok(PromptEvent::OutputDelta { text })) if text == "observed partial fragment")
        );
        let error = terminal
            .expect("one terminal error")
            .expect_err("partial output cannot authorize successful completion");
        ScriptedState::assert_finish_failure(finish_reason.as_ref(), &error);
        assert!(eof.is_none());
        assert!(fused.is_none());
        assert_eq!(
            reused.expect("failed stream releases admission").output(),
            "reused after rejected stream"
        );
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1],
            vec![Message::user_text("fresh prompt after rejected stream").expect("fresh message")]
        );
    }
}

#[tokio::test]
async fn custom_rejects_unsuccessful_function_responses_before_handler_creation() {
    for streamed in [false, true] {
        for finish_reason in [
            None,
            Some(FinishReason::Stop),
            Some(FinishReason::Length),
            Some(FinishReason::ContentFilter),
            Some(FinishReason::Error),
            Some(FinishReason::Paused),
        ] {
            let mut rejected = ScriptedState::function_response();
            rejected.finish_reason = finish_reason.clone();
            let fixture = CustomPromptFixture::build(
                ToolSelection::Required,
                if streamed {
                    Vec::new()
                } else {
                    vec![
                        rejected.clone(),
                        ScriptedState::terminal_response("must not continue"),
                    ]
                },
                HandlerBehavior::Complete,
                None,
            )
            .await;
            let result = if streamed {
                fixture.state.set_streams(vec![
                    vec![Ok(GenerateEvent::Completed(Box::new(rejected)))],
                    vec![Ok(GenerateEvent::Completed(Box::new(
                        ScriptedState::terminal_response("must not continue"),
                    )))],
                ]);
                let mut output = fixture
                    .conversation
                    .prompt_stream(
                        PromptRequest::new("do not execute rejected calls").expect("prompt"),
                    )
                    .await
                    .expect("stream admission");
                let terminal = tokio::time::timeout(Duration::from_secs(3), output.recv())
                    .await
                    .expect("terminal stream result")
                    .expect("terminal event");
                assert!(
                    tokio::time::timeout(Duration::from_secs(3), output.recv())
                        .await
                        .expect("stream EOF must remain bounded")
                        .is_none()
                );
                terminal.map(|event| match event {
                    PromptEvent::Completed(response) => response,
                    PromptEvent::OutputDelta { .. } => panic!("source has no delta"),
                })
            } else {
                fixture
                    .conversation
                    .prompt(PromptRequest::new("do not execute rejected calls").expect("prompt"))
                    .await
            };
            let requests = fixture.state.requests();
            let created = fixture.cancellation.created.load(Ordering::SeqCst);
            let calls = fixture.calls.load(Ordering::SeqCst);
            fixture.shutdown().await;

            let error = result.expect_err("unsuccessful function calls cannot execute");
            ScriptedState::assert_finish_failure(finish_reason.as_ref(), &error);
            assert_eq!(requests.len(), 1, "failed model turn must not continue");
            assert_eq!(created, 0, "failed model turn must not construct handlers");
            assert_eq!(calls, 0, "failed model turn must not execute handlers");
        }
    }
}

#[tokio::test]
async fn custom_failed_completion_after_a_tool_turn_discards_the_whole_prompt() {
    for streamed in [false, true] {
        let mut rejected = ScriptedState::function_response();
        rejected.finish_reason = Some(FinishReason::Error);
        let fixture = CustomPromptFixture::build(
            ToolSelection::Required,
            if streamed {
                vec![ScriptedState::terminal_response(
                    "fresh answer after tool failure",
                )]
            } else {
                vec![
                    ScriptedState::function_response(),
                    rejected.clone(),
                    ScriptedState::terminal_response("fresh answer after tool failure"),
                ]
            },
            HandlerBehavior::Complete,
            None,
        )
        .await;
        let result = if streamed {
            fixture.state.set_streams(vec![
                vec![Ok(GenerateEvent::Completed(Box::new(
                    ScriptedState::function_response(),
                )))],
                vec![
                    Ok(GenerateEvent::ContentDelta {
                        delta: "failed second turn fragment".to_owned(),
                    }),
                    Ok(GenerateEvent::Completed(Box::new(rejected))),
                ],
                vec![Ok(GenerateEvent::Completed(Box::new(
                    ScriptedState::terminal_response("must not continue"),
                )))],
            ]);
            let mut output = fixture
                .conversation
                .prompt_stream(PromptRequest::new("tool succeeds but model fails").expect("prompt"))
                .await
                .expect("stream admission");
            assert!(
                matches!(tokio::time::timeout(Duration::from_secs(3), output.recv()).await.expect("partial output"), Some(Ok(PromptEvent::OutputDelta { text })) if text == "failed second turn fragment")
            );
            let terminal = tokio::time::timeout(Duration::from_secs(3), output.recv())
                .await
                .expect("stream terminal")
                .expect("terminal event");
            assert!(
                tokio::time::timeout(Duration::from_secs(3), output.recv())
                    .await
                    .expect("stream EOF must remain bounded")
                    .is_none()
            );
            terminal.map(|event| match event {
                PromptEvent::Completed(response) => response,
                PromptEvent::OutputDelta { .. } => panic!("one source delta"),
            })
        } else {
            fixture
                .conversation
                .prompt(PromptRequest::new("tool succeeds but model fails").expect("prompt"))
                .await
        };
        let failed_requests = fixture.state.requests();
        let calls = fixture.calls.load(Ordering::SeqCst);
        let created = fixture.cancellation.created.load(Ordering::SeqCst);
        let reused = fixture
            .conversation
            .prompt(
                PromptRequest::new("fresh prompt after failed tool loop")
                    .expect("reuse prompt")
                    .with_tool_selection(ToolSelection::None)
                    .expect("reuse tool policy"),
            )
            .await;
        let requests = fixture.state.requests();
        fixture.shutdown().await;

        assert!(
            matches!(result, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Integration))
        );
        assert_eq!(failed_requests.len(), 2);
        assert_eq!(
            created, 1,
            "only the successful first model turn may construct a handler"
        );
        assert_eq!(
            calls, 1,
            "the rejected second model turn cannot execute another call"
        );
        assert_eq!(
            reused.expect("failed loop releases admission").output(),
            "fresh answer after tool failure"
        );
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[2],
            vec![Message::user_text("fresh prompt after failed tool loop").expect("fresh message")]
        );
    }
}

#[tokio::test]
async fn custom_rejected_completion_cannot_pass_structured_output_validation() {
    for streamed in [false, true] {
        let mut rejected =
            ScriptedState::terminal_response("{\"answer\":\"valid JSON is not success\"}");
        rejected.finish_reason = Some(FinishReason::Error);
        let fixture = CustomPromptFixture::build(
            ToolSelection::None,
            if streamed {
                Vec::new()
            } else {
                vec![rejected.clone()]
            },
            HandlerBehavior::Complete,
            None,
        )
        .await;
        let request = PromptRequest::new("return structured output")
            .expect("prompt")
            .with_structured_output(
                StructuredOutputRequest::new(serde_json::json!({
                    "type": "object", "properties": {"answer": {"type": "string"}},
                    "required": ["answer"], "additionalProperties": false
                }))
                .expect("object schema"),
            );
        let result = if streamed {
            fixture
                .state
                .set_streams(vec![vec![Ok(GenerateEvent::Completed(Box::new(rejected)))]]);
            let mut output = fixture
                .conversation
                .prompt_stream(request)
                .await
                .expect("stream admission");
            let result = tokio::time::timeout(Duration::from_secs(3), output.recv())
                .await
                .expect("stream terminal")
                .expect("terminal event");
            assert!(
                tokio::time::timeout(Duration::from_secs(3), output.recv())
                    .await
                    .expect("stream EOF must remain bounded")
                    .is_none()
            );
            result.map(|event| match event {
                PromptEvent::Completed(response) => response,
                PromptEvent::OutputDelta { .. } => panic!("source has no delta"),
            })
        } else {
            fixture.conversation.prompt(request).await
        };
        fixture.shutdown().await;
        assert!(
            matches!(result, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Integration))
        );
    }
}

#[tokio::test]
async fn empty_tool_results_fail_without_synthetic_continuation() {
    for streamed in [false, true] {
        for is_error in [false, true] {
            for metadata_only in [false, true] {
                let fixture = CustomPromptFixture::build(
                    ToolSelection::Allowed(vec![TOOL.to_owned()]),
                    if streamed {
                        vec![ScriptedState::terminal_response(
                            "fresh answer after malformed tool",
                        )]
                    } else {
                        vec![
                            ScriptedState::function_response(),
                            ScriptedState::terminal_response("fresh answer after malformed tool"),
                        ]
                    },
                    HandlerBehavior::Empty {
                        is_error,
                        metadata_only,
                    },
                    None,
                )
                .await;
                let result = if streamed {
                    fixture.state.set_streams(vec![
                        vec![Ok(GenerateEvent::Completed(Box::new(
                            ScriptedState::function_response(),
                        )))],
                        vec![Ok(GenerateEvent::Completed(Box::new(
                            ScriptedState::terminal_response(
                                "must not continue after empty tool output",
                            ),
                        )))],
                    ]);
                    let mut output = fixture
                        .conversation
                        .prompt_stream(
                            PromptRequest::new("reject empty tool output").expect("prompt"),
                        )
                        .await
                        .expect("stream admission");
                    let terminal = tokio::time::timeout(Duration::from_secs(3), output.recv())
                        .await
                        .expect("stream terminal must remain bounded")
                        .expect("terminal event");
                    assert!(
                        tokio::time::timeout(Duration::from_secs(3), output.recv())
                            .await
                            .expect("stream EOF must remain bounded")
                            .is_none()
                    );
                    terminal.map(|event| match event {
                        PromptEvent::Completed(response) => response,
                        PromptEvent::OutputDelta { .. } => panic!("source has no delta"),
                    })
                } else {
                    fixture
                        .conversation
                        .prompt(PromptRequest::new("reject empty tool output").expect("prompt"))
                        .await
                };
                let failed_requests = fixture.state.requests();
                let reused = fixture
                    .conversation
                    .prompt(
                        PromptRequest::new("fresh prompt after malformed tool")
                            .expect("reuse prompt"),
                    )
                    .await;
                let requests = fixture.state.requests();
                let calls = fixture.calls.load(Ordering::SeqCst);
                fixture.shutdown().await;

                assert!(
                    matches!(result, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Integration)),
                    "empty tool results must fail regardless of delivery mode, metadata and is_error"
                );
                assert_eq!(
                    failed_requests.len(),
                    1,
                    "no fabricated content may be sent to the Model"
                );
                assert_eq!(calls, 1);
                assert_eq!(
                    reused
                        .expect("malformed tool output releases admission")
                        .output(),
                    "fresh answer after malformed tool"
                );
                assert_eq!(requests.len(), 2);
                assert_eq!(
                    requests[1],
                    vec![
                        Message::user_text("fresh prompt after malformed tool")
                            .expect("fresh message")
                    ]
                );
            }
        }
    }
}

#[tokio::test]
async fn structured_only_tool_results_preserve_null_and_error_flags() {
    for is_error in [false, true] {
        for null in [false, true] {
            let function_response = ScriptedState::function_response();
            let exact_message = function_response.message.clone();
            let fixture = CustomPromptFixture::build(
                ToolSelection::Required,
                vec![
                    function_response,
                    ScriptedState::terminal_response("structured tool done"),
                ],
                HandlerBehavior::Structured { is_error, null },
                None,
            )
            .await;
            let result = fixture
                .conversation
                .prompt(PromptRequest::new("return structured tool result").expect("prompt"))
                .await;
            let requests = fixture.state.requests();
            let calls = fixture.calls.load(Ordering::SeqCst);
            fixture.shutdown().await;

            let response = result.expect("structured-only output is genuine tool content");
            assert_eq!(response.output(), "structured tool done");
            assert_eq!(response.usage(), Some(&UsageInfo::new(2, 2)));
            assert_eq!(calls, 1);
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[1][1], exact_message);
            let result = &requests[1][2].function_results()[0];
            assert_eq!(result.call_id(), Some("fixture-call"));
            assert_eq!(result.name(), TOOL);
            assert_eq!(result.is_error(), is_error);
            assert_eq!(
                result.content(),
                [ContentPart::Json {
                    value: if null {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!({"result": "structured only"})
                    }
                }]
            );
        }
    }
}

#[tokio::test]
async fn malformed_tool_media_fails_before_another_model_call() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        vec![
            ScriptedState::function_response(),
            ScriptedState::terminal_response("must remain unused"),
        ],
        HandlerBehavior::MalformedMedia,
        None,
    )
    .await;

    let error = fixture
        .conversation
        .prompt(PromptRequest::new("return malformed media").expect("fixture prompt must be valid"))
        .await
        .expect_err("malformed tool media must fail the prompt");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::Integration
    ));
    assert_eq!(fixture.state.requests().len(), 1);
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_rejects_non_object_output_schema_before_provider_io() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::None,
        vec![ScriptedState::terminal_response("must remain unused")],
        HandlerBehavior::Complete,
        None,
    )
    .await;
    let schema = StructuredOutputRequest::new(serde_json::json!({"type": "string"}))
        .expect("Agent schema must be valid outside the Custom provider boundary");
    let request = PromptRequest::new("return a string")
        .expect("fixture prompt must be valid")
        .with_structured_output(schema);
    let error = fixture
        .conversation
        .prompt(request)
        .await
        .expect_err("Custom must require an object-root schema");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::InvalidInput
    ));
    assert!(fixture.state.requests().is_empty());
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_runtime_model_validation_is_request_input() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::None,
        vec![ScriptedState::terminal_response("must remain unused")],
        HandlerBehavior::Complete,
        None,
    )
    .await;
    let oversized = "m".repeat(257);
    let selector =
        ModelSelector::new(oversized.clone()).expect("Agent selector remains source-neutral");
    let request = PromptRequest::new("select an oversized provider model")
        .expect("fixture prompt must be valid")
        .with_model(selector.clone());
    assert!(
        matches!(fixture.conversation.prompt(request).await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput))
    );
    let info = fixture.workspace.info().await.expect("workspace must load");
    let defaults = WorkspaceDefaults::new(Some(selector), None, None)
        .expect("workspace defaults must be valid");
    let request = NewConversation::new(None, Some(defaults.clone()), Vec::new())
        .expect("conversation request must be valid");
    assert!(
        matches!(fixture.workspace.new_conversation(request).await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput))
    );
    let request = UpdateWorkspaceRequest::new(
        info.id().clone(),
        info.revision(),
        info.name(),
        info.context().clone(),
        defaults,
    )
    .expect("workspace update must be valid");
    assert!(
        matches!(fixture.agent.check_workspace_update(&request).await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidInput))
    );
    let key = StateKey::new(
        "agent_sdk_workspaces_v1",
        format!("{}:{}", fixture.agent.id().as_str(), info.id().as_str()),
    )
    .expect("workspace key must be valid");
    let store = fixture
        .raw_store
        .as_ref()
        .expect("fixture store must exist");
    let mut transaction = store
        .begin_transaction()
        .await
        .expect("corruption transaction must begin");
    let snapshot = transaction
        .load(key.clone())
        .await
        .expect("workspace must load")
        .expect("workspace record must exist");
    let original = snapshot.record.value;
    let mut corrupted = original.as_ref().clone();
    corrupted["defaults"]["model"] = serde_json::Value::String(oversized);
    transaction
        .put(StateRecord::new(key.clone(), corrupted))
        .await
        .expect("corruption must be staged");
    transaction.commit().await.expect("corruption must commit");
    assert!(
        matches!(fixture.agent.workspace_info(info.id().clone()).await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidState))
    );
    let mut transaction = store
        .begin_transaction()
        .await
        .expect("repair transaction must begin");
    transaction
        .put(StateRecord {
            key,
            value: original,
        })
        .await
        .expect("repair must be staged");
    transaction.commit().await.expect("repair must commit");
    assert!(fixture.state.requests().is_empty());
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_model_inventory_enforces_pagination_and_metadata_contracts() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::None,
        Vec::new(),
        HandlerBehavior::Complete,
        None,
    )
    .await;
    let summary = |id: &str| ModelSummary {
        id: ModelId::new(id).expect("fixture model id must be valid"),
        display_name: Some(format!("{id} display")),
        description: Some("fixture model".to_owned()),
        context_window_tokens: Some(4_096),
    };
    let assert_requests = |cursors: &[Option<&str>]| {
        let requests = fixture
            .state
            .model_requests
            .lock()
            .expect("scripted model requests must not be poisoned");
        assert_eq!(requests.len(), cursors.len());
        for (request, cursor) in requests.iter().zip(cursors) {
            assert_eq!(request.limit(), NonZeroUsize::new(256));
            assert_eq!(request.cursor(), *cursor);
        }
    };

    fixture.state.set_model_pages(vec![
        ModelPage {
            models: vec![
                summary("zeta-model"),
                summary("unsupported-model"),
                summary("alpha-model"),
            ],
            next_cursor: Some("next-page".to_owned()),
        },
        ModelPage {
            models: vec![summary("beta-model")],
            next_cursor: None,
        },
    ]);
    assert_eq!(
        fixture
            .agent
            .list_models()
            .await
            .expect("valid inventory must normalize")
            .iter()
            .map(|model| model.selector().as_str())
            .collect::<Vec<_>>(),
        ["alpha-model", "beta-model", "zeta-model"]
    );
    assert_requests(&[None, Some("next-page")]);

    fixture.state.set_model_pages(vec![ModelPage {
        models: vec![summary("duplicate-model"), summary("duplicate-model")],
        next_cursor: None,
    }]);
    assert!(
        matches!(fixture.agent.list_models().await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::InvalidState))
    );
    assert_requests(&[None]);

    let mut malformed = summary("malformed-model");
    malformed.display_name = Some(" padded ".to_owned());
    fixture.state.set_model_pages(vec![ModelPage {
        models: vec![malformed],
        next_cursor: None,
    }]);
    assert!(
        matches!(fixture.agent.list_models().await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol))
    );
    assert_requests(&[None]);

    fixture.state.set_model_pages(vec![
        ModelPage {
            models: Vec::new(),
            next_cursor: Some("cycle".to_owned()),
        },
        ModelPage {
            models: Vec::new(),
            next_cursor: Some("cycle".to_owned()),
        },
    ]);
    assert!(
        matches!(fixture.agent.list_models().await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol))
    );
    assert_requests(&[None, Some("cycle")]);

    fixture.state.set_model_pages(vec![ModelPage {
        models: Vec::new(),
        next_cursor: Some(String::new()),
    }]);
    assert!(
        matches!(fixture.agent.list_models().await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol))
    );
    assert_requests(&[None]);

    fixture.state.set_model_pages(vec![ModelPage {
        models: vec![summary("oversized-page"); 257],
        next_cursor: None,
    }]);
    assert!(
        matches!(fixture.agent.list_models().await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol))
    );
    assert_requests(&[None]);

    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_build_rejects_provider_incompatible_tool_schema() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("fixture StateStore must build");
    let custom = CustomAgentConfig::new(
        ModelSelector::new(MODEL).expect("fixture model selector must be valid"),
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        CustomLoopLimits::new(2, 1, 1).expect("fixture limits must be valid"),
        CustomToolApproval::Allow,
    )
    .expect("fixture Custom config must be valid");
    let config = AgentConfig::new(
        custom.into(),
        Duration::from_secs(3),
        NonZeroUsize::new(1).expect("one prompt must be nonzero"),
    )
    .expect("fixture Agent config must be valid");
    for schema in [
        serde_json::json!(true),
        serde_json::json!(false),
        serde_json::json!({"type": "string"}),
    ] {
        let state = ScriptedState::new(Vec::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut toolset = ScriptedToolset::new(
            TOOL,
            HandlerBehavior::Complete,
            Arc::clone(&calls),
            Arc::new(CancellationProbe::default()),
        );
        toolset.schema = JsonSchema::strict(schema);
        let result = AgentBuilder::create_for_backends::<ScriptedProvider, ScriptedToolset>(
            config.clone(),
            AgentStorage::new(store.clone()),
        )
        .with_provider(ScriptedProvider {
            state: Arc::clone(&state),
        })
        .with_toolset(toolset)
        .build()
        .await;
        let error = match result {
            Ok(_) => panic!("provider-incompatible tool schema must fail Custom construction"),
            Err(error) => error,
        };
        assert!(matches!(
            (error).kind(),
            agent_sdk::error::ErrorKind::InvalidConfig
        ));
        assert!(state.requests().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn custom_reopen_rejects_corrupted_persisted_schema_as_invalid_state() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::None,
        Vec::new(),
        HandlerBehavior::Complete,
        None,
    )
    .await;
    let agent_id = fixture.agent.id();
    let conversation_id = fixture.conversation.id();
    let CustomPromptFixture {
        storage,
        raw_store,
        agent,
        workspace,
        conversation,
        root,
        state,
        calls,
        cancellation,
        toolset_count: _,
    } = fixture;
    drop(conversation);
    drop(workspace);
    agent
        .shutdown()
        .await
        .expect("original fixture Agent must shut down");

    let key = StateKey::new(
        "agent_sdk_custom_continuations_v1",
        conversation_id.as_str(),
    )
    .expect("fixture continuation key must be valid");
    let raw_store = raw_store.expect("default fixture must retain its raw StateStore");
    let mut transaction = raw_store
        .begin_transaction()
        .await
        .expect("corruption transaction must begin");
    let snapshot = transaction
        .load(key.clone())
        .await
        .expect("continuation record must load")
        .expect("continuation record must exist");
    let mut value = snapshot.record.value.as_ref().clone();
    value["next_generation"] = serde_json::json!(2);
    value["state"] = serde_json::json!({
        "phase": "active",
        "generation": 1,
        "accepted": false,
        "cancellation": null,
        "caller_text": "corrupted schema",
        "contract": {
            "structured_schema": {
                "type": "object",
                "properties": {
                    "value": {"$ref": "https://example.invalid/schema.json"}
                }
            },
            "tool_selection": {"mode": "none"}
        }
    });
    transaction
        .put(StateRecord::new(key, value))
        .await
        .expect("corrupted continuation must be staged");
    transaction
        .commit()
        .await
        .expect("corrupted continuation must be committed");

    let custom = CustomAgentConfig::new(
        ModelSelector::new(MODEL).expect("fixture model selector must be valid"),
        ToolSelection::None,
        CustomLoopLimits::new(4, 4, 1).expect("fixture limits must be valid"),
        CustomToolApproval::Allow,
    )
    .expect("fixture Custom config must be valid");
    let config = AgentConfig::new(
        custom.into(),
        Duration::from_secs(3),
        NonZeroUsize::new(1).expect("one prompt must be nonzero"),
    )
    .expect("fixture Agent config must be valid");
    let result = AgentBuilder::reopen_for_backends::<ScriptedProvider, ScriptedToolset>(
        config, storage, agent_id,
    )
    .with_provider(ScriptedProvider { state })
    .with_toolset(ScriptedToolset::new(
        TOOL,
        HandlerBehavior::Complete,
        calls,
        cancellation,
    ))
    .build()
    .await;
    let error = match result {
        Ok(_) => panic!("corrupted persisted schema must prevent reopen"),
        Err(error) => error,
    };
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::InvalidState
    ));
    std::fs::remove_dir_all(root).expect("fixture root must be removed");
}

#[tokio::test]
async fn lance_shared_directory_knowledge_preserves_exact_model_input_and_terminal_text() {
    const DATABASE: &str = "STRICT_DATABASE_C0123456789ABCDEF0123456789ABCDE";
    const PRIMARY: &str = "STRICT_AGENT_A_C1123456789ABCDEF0123456789ABCDE";
    const SECONDARY: &str = "STRICT_AGENT_B_C2123456789ABCDEF0123456789ABCDE";
    let directory = LanceStateDirectory::new();
    let store_config = directory.config();
    let StateStoreConfig::Lance(lance) = &store_config else {
        panic!("the owned fixture must use Lance");
    };
    let LanceStateStoreLocation::Directory(path) = lance.location() else {
        panic!("the owned fixture must retain one shared directory");
    };
    let output = format!(" \n{DATABASE} {SECONDARY} 雪\\literal\n ");
    let state = ScriptedState::new(vec![ScriptedState::terminal_response(&output)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let cancellation = Arc::new(CancellationProbe::default());
    let root = std::env::current_dir().expect("the fixture workspace must exist");
    for primary in [true, false] {
        let database =
            storage_sdk::Database::connect(storage_sdk::database::config::DatabaseConfig::lance(
                storage_sdk::database::config::LanceConfig::embedded_path(path),
            ))
            .await
            .expect("knowledge must independently open the exact StateStore directory");
        let store = StateStore::connect(store_config.clone())
            .await
            .expect("control-plane state must independently open the shared directory");
        let custom = CustomAgentConfig::new(
            ModelSelector::new(MODEL).expect("model"),
            ToolSelection::None,
            CustomLoopLimits::new(2, 0, 1).expect("limits"),
            CustomToolApproval::Deny,
        )
        .expect("Custom config");
        let config = AgentConfig::new(
            custom.into(),
            Duration::from_secs(3),
            NonZeroUsize::new(1).expect("one prompt"),
        )
        .expect("Agent config");
        let agent = AgentBuilder::create_for_backends::<ScriptedProvider, ScriptedToolset>(
            config,
            AgentStorage::new(store),
        )
        .with_provider(ScriptedProvider {
            state: Arc::clone(&state),
        })
        .with_toolset(ScriptedToolset::new(
            TOOL,
            HandlerBehavior::Complete,
            Arc::clone(&calls),
            Arc::clone(&cancellation),
        ))
        .with_knowledge_database(database)
        .build()
        .await
        .expect("the exact recorded Model and real Lance products must compose");
        let mut workspace = None;
        let mut conversation = None;
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(30),
            async {
                if primary {
                    agent
                        .ingest_knowledge(
                            IngestKnowledgeRequest::new(vec![
                                KnowledgeSource::text(
                                    "database-marker",
                                    "Database marker",
                                    format!("The managed marker value is {DATABASE}."),
                                )
                                .expect("Database source"),
                            ])
                            .expect("Database ingestion")
                            .with_scope(KnowledgeScope::Database),
                        )
                        .await
                        .expect("the first owner publishes shared knowledge");
                }
                let marker = if primary { PRIMARY } else { SECONDARY };
                agent
                    .ingest_knowledge(
                        IngestKnowledgeRequest::new(vec![
                            KnowledgeSource::text(
                                "agent-marker",
                                "Agent marker",
                                format!("The managed marker value is {marker}."),
                            )
                            .expect("Agent source"),
                        ])
                        .expect("Agent ingestion"),
                    )
                    .await
                    .expect("each owner publishes its own exact knowledge");
                if primary {
                    assert!(
                        state.requests().is_empty(),
                        "ingestion must not generate text"
                    );
                    return;
                }
                let workspace = workspace.insert(
                    agent
                        .new_workspace(
                            NewWorkspace::new(
                                "exact-knowledge-fidelity",
                                WorkspaceContext::new(
                                    root.clone(),
                                    vec![
                                        WorkspaceRoot::new(
                                            root.clone(),
                                            WorkspaceRootAccess::ReadWrite,
                                        )
                                        .expect("root"),
                                    ],
                                    WorkspaceExecutionPolicy::new(
                                        WorkspaceSandboxMode::FullAccess,
                                        WorkspaceNetworkAccess::Enabled,
                                    ),
                                )
                                .expect("context"),
                                WorkspaceDefaults::empty(),
                            )
                            .expect("workspace request"),
                        )
                        .await
                        .expect("workspace"),
                );
                let conversation = conversation.insert(
                    workspace
                        .new_conversation(
                            NewConversation::new(None, None, Vec::new())
                                .expect("conversation request"),
                        )
                        .await
                        .expect("conversation"),
                );
                let response = conversation
                    .prompt(
                        PromptRequest::new("Return the managed marker values from reference data.")
                            .expect("prompt")
                            .with_knowledge_policy(
                                KnowledgePolicy::only(vec![
                                    KnowledgeScope::Database,
                                    KnowledgeScope::Agent,
                                ])
                                .expect("scope selection"),
                            )
                            .expect("prompt knowledge policy"),
                    )
                    .await
                    .expect("one recorded Model call must complete");
                assert_eq!(
                    response.output().as_bytes(),
                    output.as_bytes(),
                    "terminal text must remain exact, including whitespace and Unicode"
                );
                let requests = state.requests();
                assert_eq!(
                    requests.len(),
                    1,
                    "neither owner nor knowledge recovery may replay the Model"
                );
                assert_eq!(
                    requests[0].iter().map(Message::role).collect::<Vec<_>>(),
                    vec![Role::System, Role::User]
                );
                let [ContentPart::Text { text }] = requests[0][0].content() else {
                    panic!("managed reference data must reach the recorded Model as text");
                };
                for marker in [DATABASE, SECONDARY] {
                    assert_eq!(
                        text.matches(marker).count(),
                        1,
                        "the exact whole marker must survive real storage, retrieval, and rendering"
                    );
                    assert!(
                        text.contains(&format!("The managed marker value is {marker}.")),
                        "the complete source text must reach the Model unchanged"
                    );
                }
                assert!(
                    !text.contains(PRIMARY),
                    "another Agent's private scope must not leak into the model input"
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    0,
                    "retrieval does not invoke a tool"
                );
            },
        )
        .await;
        // Join consuming lifecycle calls before reporting an observation failure.
        // A stuck owner remains subject to the supervised target's outer budget.
        let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
        if let Some(conversation) = conversation.take() {
            let result = std::panic::AssertUnwindSafe(conversation.close())
                .catch_unwind()
                .await;
            cleanup
                .settle(async {
                    match result {
                        Ok(result) => result,
                        Err(panic) => std::panic::resume_unwind(panic),
                    }
                })
                .await;
        }
        if let Some(workspace) = workspace.take() {
            let result = std::panic::AssertUnwindSafe(workspace.close())
                .catch_unwind()
                .await;
            cleanup
                .settle(async {
                    match result {
                        Ok(result) => result,
                        Err(panic) => std::panic::resume_unwind(panic),
                    }
                })
                .await;
        }
        let result = std::panic::AssertUnwindSafe(agent.shutdown())
            .catch_unwind()
            .await;
        cleanup
            .settle(async {
                match result {
                    Ok(result) => result,
                    Err(panic) => std::panic::resume_unwind(panic),
                }
            })
            .await;
        cleanup.finish(observed);
        assert_eq!(
            cancellation.shutdowns.load(Ordering::SeqCst),
            if primary { 1 } else { 2 },
            "every Agent-owned Toolset must have joined shutdown"
        );
    }
}

#[tokio::test]
async fn reopen_rebuilds_provider_neutral_history_without_continuation() {
    CustomPromptFixture::reopen_case(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await;
}

#[tokio::test]
async fn lance_state_memory_custom_reopen_preserves_provider_neutral_history() {
    CustomPromptFixture::reopen_case(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await;
}

#[tokio::test]
async fn lance_state_directory_custom_reopen_preserves_provider_neutral_history() {
    let directory = LanceStateDirectory::new();
    CustomPromptFixture::reopen_case(directory.config()).await;
}

impl CustomPromptFixture<StateStore> {
    async fn reopen_case(config: StateStoreConfig) {
        for streamed in [false, true] {
            let mut rejected =
                ScriptedState::terminal_response("failed answer must not survive reopen");
            rejected.finish_reason = Some(FinishReason::Error);
            let fixture = CustomPromptFixture::build_with_config(
                ToolSelection::Allowed(vec![TOOL.to_owned()]),
                if streamed {
                    vec![ScriptedState::terminal_response("first answer")]
                } else {
                    vec![
                        ScriptedState::terminal_response("first answer"),
                        ScriptedState::function_response(),
                        rejected.clone(),
                    ]
                },
                HandlerBehavior::Complete,
                None,
                config.clone(),
            )
            .await;
            fixture
                .conversation
                .prompt(PromptRequest::new("first question").expect("fixture prompt must be valid"))
                .await
                .expect("first prompt must complete");

            let (rejected, streamed_eof) = if streamed {
                fixture.state.set_streams(vec![
                    vec![Ok(GenerateEvent::Completed(Box::new(
                        ScriptedState::function_response(),
                    )))],
                    vec![Ok(GenerateEvent::Completed(Box::new(rejected)))],
                ]);
                let mut output = fixture
                    .conversation
                    .prompt_stream(
                        PromptRequest::new("failed question must not survive reopen")
                            .expect("failed prompt"),
                    )
                    .await
                    .expect("stream admission");
                let terminal = tokio::time::timeout(Duration::from_secs(3), output.recv())
                    .await
                    .expect("stream terminal must remain bounded")
                    .expect("terminal event");
                let eof = tokio::time::timeout(Duration::from_secs(3), output.recv())
                    .await
                    .expect("stream EOF must remain bounded");
                (
                    terminal.map(|event| match event {
                        PromptEvent::Completed(response) => response,
                        PromptEvent::OutputDelta { .. } => panic!("source has no delta"),
                    }),
                    Some(eof.is_none()),
                )
            } else {
                (
                    fixture
                        .conversation
                        .prompt(
                            PromptRequest::new("failed question must not survive reopen")
                                .expect("failed prompt"),
                        )
                        .await,
                    None,
                )
            };
            let tool_calls = fixture.calls.load(Ordering::SeqCst);
            let agent_id = fixture.agent.id();
            let workspace_id = fixture.workspace.id();
            let conversation_id = fixture.conversation.id();
            let CustomPromptFixture {
                storage,
                raw_store,
                agent,
                workspace,
                conversation,
                root,
                state,
                calls,
                cancellation,
                toolset_count: _,
            } = fixture;
            drop(conversation);
            drop(workspace);
            agent
                .shutdown()
                .await
                .expect("original fixture Agent must shut down");
            let original_requests = state.requests();
            let reopened_state =
                ScriptedState::new(vec![ScriptedState::terminal_response("second answer")]);
            let custom = CustomAgentConfig::new(
                ModelSelector::new(MODEL).expect("fixture model selector must be valid"),
                ToolSelection::Allowed(vec![TOOL.to_owned()]),
                CustomLoopLimits::new(4, 4, 1).expect("fixture limits must be valid"),
                CustomToolApproval::Allow,
            )
            .expect("fixture Custom config must be valid");
            let agent_config = AgentConfig::new(
                custom.into(),
                Duration::from_secs(3),
                NonZeroUsize::new(1).expect("one prompt must be nonzero"),
            )
            .expect("fixture Agent config must be valid");
            drop(raw_store);
            let storage = match &config {
                StateStoreConfig::Lance(lance)
                    if matches!(lance.location(), LanceStateStoreLocation::Directory(_)) =>
                {
                    drop(storage);
                    AgentStorage::new(
                        StateStore::connect(config.clone())
                            .await
                            .expect("independently reopen Lance directory"),
                    )
                }
                _ => storage,
            };
            let reopened = AgentBuilder::reopen_for_backends::<ScriptedProvider, ScriptedToolset>(
                agent_config,
                storage,
                agent_id,
            )
            .with_provider(ScriptedProvider {
                state: Arc::clone(&reopened_state),
            })
            .with_toolset(ScriptedToolset::new(
                TOOL,
                HandlerBehavior::Complete,
                calls,
                cancellation,
            ))
            .build()
            .await
            .expect("fixture Agent must reopen");
            let reopened_workspace = reopened
                .get_workspace(workspace_id)
                .await
                .expect("fixture workspace must reopen");
            let reopened_conversation = reopened_workspace
                .get_conversation(conversation_id)
                .await
                .expect("fixture conversation must reopen");
            let response = reopened_conversation
                .prompt(
                    PromptRequest::new("second question").expect("fixture prompt must be valid"),
                )
                .await;
            let requests = reopened_state.requests();
            let old_requests = state.requests();
            reopened_conversation
                .close()
                .await
                .expect("reopened fixture conversation must close");
            reopened_workspace
                .close()
                .await
                .expect("reopened fixture workspace must close");
            reopened
                .shutdown()
                .await
                .expect("reopened fixture Agent must shut down");
            std::fs::remove_dir_all(root).expect("fixture root must be removed");

            assert!(
                matches!(rejected, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Integration))
            );
            if let Some(eof) = streamed_eof {
                assert!(eof);
            }
            assert_eq!(tool_calls, 1);
            assert_eq!(original_requests.len(), 3);
            assert_eq!(
                old_requests, original_requests,
                "reopen cannot replay the original provider's work"
            );
            assert_eq!(
                response.expect("reopened prompt must complete").output(),
                "second answer"
            );
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].iter().map(Message::role).collect::<Vec<_>>(),
                vec![Role::User, Role::Assistant, Role::User]
            );
            assert_eq!(
                requests[0][0].content(),
                [ContentPart::Text {
                    text: "first question".to_owned()
                }]
            );
            assert_eq!(
                requests[0][1].content(),
                [ContentPart::Text {
                    text: "first answer".to_owned()
                }]
            );
            assert_eq!(
                requests[0][2].content(),
                [ContentPart::Text {
                    text: "second question".to_owned()
                }]
            );
            assert!(
                requests[0]
                    .iter()
                    .all(|message| message.continuation().is_none())
            );
        }
    }
}

#[tokio::test]
async fn required_one_of_relaxes_to_allowed_auto_after_one_function_call() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
        vec![
            ScriptedState::function_response(),
            ScriptedState::terminal_response("done"),
        ],
        HandlerBehavior::Complete,
        None,
    )
    .await;

    fixture
        .conversation
        .prompt(PromptRequest::new("run selected tool").expect("fixture prompt must be valid"))
        .await
        .expect("required-one-of prompt must complete");
    assert_eq!(
        fixture.state.choices(),
        vec![
            ToolChoice::Allowed {
                mode: AllowedToolMode::Required,
                names: vec![TOOL.to_owned()],
            },
            ToolChoice::Allowed {
                mode: AllowedToolMode::Auto,
                names: vec![TOOL.to_owned()],
            },
        ]
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn required_tool_choice_rejects_terminal_output_before_activity() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::Required,
        vec![ScriptedState::terminal_response("too early")],
        HandlerBehavior::Complete,
        None,
    )
    .await;

    let error = fixture
        .conversation
        .prompt(PromptRequest::new("must call a tool").expect("fixture prompt must be valid"))
        .await
        .expect_err("terminal output must not satisfy a required selection");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::Protocol
    ));
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    fixture.shutdown().await;
}

#[tokio::test]
async fn abort_cancels_and_drains_an_interactive_tool_before_route_reuse() {
    let calls = vec![
        FunctionCall::new(Some("fixture-call".to_owned()), TOOL, serde_json::json!({}))
            .expect("fixture function call must be valid"),
        FunctionCall::new(Some("queued-call".to_owned()), TOOL, serde_json::json!({}))
            .expect("queued fixture call must be valid"),
    ];
    let tool_response = ScriptedState::response(
        Message::assistant(Vec::new(), calls)
            .expect("assistant function calls must be valid")
            .with_continuation(ScriptedContinuation("fixture-continuation".to_owned()))
            .expect("assistant continuation must be valid"),
        FinishReason::ToolCall,
    );
    let fixture = CustomPromptFixture::build(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        vec![tool_response, ScriptedState::terminal_response("reused")],
        HandlerBehavior::Interaction,
        None,
    )
    .await;
    let mut receiver = fixture
        .conversation
        .notification_stream()
        .expect("fixture notification receiver must open");
    let mut prompt = Box::pin(
        fixture
            .conversation
            .prompt(PromptRequest::new("wait in the tool").expect("fixture prompt must be valid")),
    );
    let notification = tokio::select! {
        item = receiver.recv() => item
            .expect("notification stream must remain open")
            .expect("notification delivery must succeed"),
        result = &mut prompt => panic!("prompt settled before interaction: {result:?}"),
    };
    assert!(matches!(
        notification.kind(),
        ConversationNotificationKind::Interaction { .. }
    ));

    let cancellation = Arc::clone(&fixture.cancellation);
    let mut abort = Box::pin(fixture.conversation.abort());
    {
        let cancellation_started = cancellation.started.notified();
        tokio::pin!(cancellation_started);
        cancellation_started.as_mut().enable();
        tokio::select! {
            () = &mut cancellation_started => {}
            result = &mut prompt => panic!("prompt settled before handler cancellation: {result:?}"),
            result = &mut abort => panic!("abort settled before handler cancellation: {result:?}"),
        }
    }
    assert_eq!(cancellation.count.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut prompt)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut abort)
            .await
            .is_err()
    );

    cancellation.release.notify_waiters();
    let aborted = tokio::time::timeout(Duration::from_secs(3), &mut abort).await;
    let error = tokio::time::timeout(Duration::from_secs(3), &mut prompt)
        .await
        .expect("cancelled prompt must settle")
        .expect_err("cancelled prompt must fail");
    assert!(matches!(
        (error).kind(),
        agent_sdk::error::ErrorKind::Cancelled
    ));
    drop(prompt);
    drop(abort);
    drop(receiver);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);

    let response = fixture
        .conversation
        .prompt(
            PromptRequest::new("reuse the route")
                .expect("fixture reuse prompt must be valid")
                .with_tool_selection(ToolSelection::None)
                .expect("fixture reuse selection must be valid"),
        )
        .await
        .expect("route must admit a later prompt");
    assert_eq!(response.output(), "reused");
    fixture.shutdown().await;
    aborted
        .expect("abort must settle without polling the prompt")
        .expect("abort must succeed");
}

#[tokio::test]
async fn caller_tool_enum_preserves_associated_handlers_errors_and_explicit_shell_cwd() {
    let root =
        std::env::temp_dir().join(format!("agent-sdk-caller-shell-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).expect("shell test root must be created");
    let root = root
        .canonicalize()
        .expect("shell test root must canonicalize");
    std::fs::create_dir(root.join("child")).expect("shell child must be created");
    #[cfg(unix)]
    let command = "printf explicit-cwd > sentinel.txt";
    #[cfg(windows)]
    let command = "echo explicit-cwd>sentinel.txt";
    let call = FunctionCall::new(
        Some("shell-call".to_owned()),
        "shell",
        serde_json::json!({
            "command": command, "cwd": "."
        }),
    )
    .expect("shell call must be valid");
    let state = ScriptedState::new(vec![
        ScriptedState::response(
            Message::assistant(Vec::new(), vec![call]).expect("tool call message is valid"),
            FinishReason::ToolCall,
        ),
        ScriptedState::function_response(),
        ScriptedState::terminal_response("shell complete"),
    ]);
    let custom = CustomAgentConfig::new(
        ModelSelector::new(MODEL).expect("fixture model is valid"),
        ToolSelection::Allowed(vec!["shell".to_owned(), TOOL.to_owned()]),
        CustomLoopLimits::new(3, 2, 1).expect("fixture loop limits are valid"),
        CustomToolApproval::Allow,
    )
    .expect("fixture Custom policy is valid");
    let config = AgentConfig::new(
        custom.into(),
        Duration::from_secs(3),
        NonZeroUsize::new(1).expect("one is nonzero"),
    )
    .expect("fixture config is valid");
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("fixture store must open");
    let calls = Arc::new(AtomicUsize::new(0));
    let shell_observer = Arc::new(caller_shell::CallerShellObserver::default());
    let cancellation = Arc::new(CancellationProbe::default());
    let agent = AgentBuilder::create_for_backends::<provider_sdk::Provider, CallerTools>(
        config,
        AgentStorage::new(store),
    )
    .with_toolset(CallerTools::Local(caller_shell::CallerShell::toolset(
        root.clone(),
        Duration::from_secs(1),
        1024,
        Some(Arc::clone(&shell_observer)),
    )))
    .with_toolset(CallerTools::Scripted(ScriptedToolset::new(
        TOOL,
        HandlerBehavior::ModelError,
        Arc::clone(&calls),
        Arc::clone(&cancellation),
    )))
    .with_provider(ScriptedProvider {
        state: Arc::clone(&state),
    })
    .build()
    .await
    .expect("caller shell Agent must build");
    let context = WorkspaceContext::new(
        root.clone(),
        vec![
            WorkspaceRoot::new(root.clone(), WorkspaceRootAccess::ReadWrite)
                .expect("root is valid"),
        ],
        WorkspaceExecutionPolicy::new(
            WorkspaceSandboxMode::FullAccess,
            WorkspaceNetworkAccess::Enabled,
        ),
    )
    .expect("workspace context is valid");
    let workspace = agent
        .new_workspace(
            NewWorkspace::new("caller-shell", context, WorkspaceDefaults::empty())
                .expect("workspace request is valid"),
        )
        .await
        .expect("shell workspace must open");
    let conversation = workspace
        .new_conversation(
            NewConversation::new(None, None, Vec::new()).expect("conversation request is valid"),
        )
        .await
        .expect("shell conversation must open");
    let response = conversation
        .prompt(
            PromptRequest::new("execute with explicit tool input")
                .expect("prompt is valid")
                .with_cwd(PathBuf::from("child"))
                .expect("prompt cwd is valid"),
        )
        .await
        .expect("shell prompt must finish");
    assert_eq!(response.output(), "shell complete");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let requests = state.requests();
    assert_eq!(requests.len(), 3);
    let errors = requests[2]
        .iter()
        .flat_map(Message::function_results)
        .filter(|result| result.is_error())
        .count();
    assert_eq!(
        errors, 1,
        "completed model-visible failure must advance the provider loop"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("sentinel.txt"))
            .expect("shell wrote sentinel")
            .trim(),
        "explicit-cwd"
    );
    assert!(
        !root.join("child/sentinel.txt").exists(),
        "prompt cwd cannot replace explicit tool input"
    );
    conversation.close().await.expect("conversation must close");
    workspace.close().await.expect("workspace must close");
    assert_eq!(cancellation.shutdowns.load(Ordering::SeqCst), 0);
    agent.shutdown().await.expect("Agent must shut down");
    assert_eq!(cancellation.shutdowns.load(Ordering::SeqCst), 1);
    std::fs::remove_dir_all(root).expect("shell test tree must be removed");
    assert_eq!(
        serde_json::to_value(shell_observer.snapshot()).expect("shell counters serialize"),
        serde_json::json!({"invoked":1,"started":1,"reaped":1,"returned":1}),
        "the single shell invocation must finish and reap its owned child"
    );
}

#[tokio::test]
async fn custom_prompt_stream_uses_real_deltas_and_exact_tool_continuation() {
    CustomPromptFixture::stream_case(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await;
}

#[tokio::test]
async fn lance_state_memory_custom_stream_preserves_tool_continuation() {
    CustomPromptFixture::stream_case(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await;
}

#[tokio::test]
async fn lance_state_directory_custom_stream_preserves_tool_continuation() {
    let directory = LanceStateDirectory::new();
    CustomPromptFixture::stream_case(directory.config()).await;
}

impl CustomPromptFixture<StateStore> {
    async fn stream_case(config: StateStoreConfig) {
        let store = StateStore::connect(config)
            .await
            .expect("stream state store");
        let fixture = CustomPromptFixture::build_with_store(
            ToolSelection::Required,
            Vec::new(),
            HandlerBehavior::Complete,
            None,
            CustomToolApproval::Interaction,
            store.clone(),
        )
        .await;
        let function_response = ScriptedState::function_response();
        let exact_function_message = function_response.message.clone();
        fixture.state.set_streams(vec![
            vec![
                Ok(GenerateEvent::Started {
                    model: ModelId::new(MODEL).expect("model"),
                }),
                Ok(GenerateEvent::ThinkingDelta {
                    delta: "private reasoning must not be public text".to_owned(),
                }),
                Ok(GenerateEvent::ContentDelta {
                    delta: "stream-fragment-only".to_owned(),
                }),
                Ok(GenerateEvent::Completed(Box::new(function_response))),
            ],
            vec![
                Ok(GenerateEvent::ContentDelta {
                    delta: "after-tool-fragment".to_owned(),
                }),
                Ok(GenerateEvent::Usage(Usage {
                    input_tokens: Some(99),
                    output_tokens: Some(99),
                    ..Usage::default()
                })),
                Ok(GenerateEvent::Completed(Box::new(
                    ScriptedState::terminal_response("{\"answer\":\"terminal-only-final\"}"),
                ))),
            ],
        ]);
        let mut notifications = fixture
            .conversation
            .notification_stream()
            .expect("independent notification receiver");
        let schema = StructuredOutputRequest::new(serde_json::json!({
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": false
        }))
        .expect("strict object schema");
        let mut output = tokio::time::timeout(
            Duration::from_secs(3),
            fixture.conversation.prompt_stream(
                PromptRequest::new("perform the approved action")
                    .expect("prompt")
                    .with_structured_output(schema),
            ),
        )
        .await
        .expect("stream admission cannot wait for tool approval")
        .expect("Custom supports actual model streams");
        let partial = tokio::time::timeout(Duration::from_secs(3), output.recv())
            .await
            .expect("real delta must arrive while tool approval prevents completion");
        assert!(matches!(partial,
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "stream-fragment-only"));
        let approval = tokio::time::timeout(Duration::from_secs(3), notifications.recv())
            .await
            .expect("streaming cannot block durable interaction delivery")
            .expect("notification receiver stays open")
            .expect("approval delivery succeeds");
        assert!(matches!(
            approval.kind(),
            ConversationNotificationKind::Interaction { .. }
        ));
        assert_eq!(fixture.state.requests().len(), 1);
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        let key = StateKey::new(
            "agent_sdk_custom_continuations_v1",
            fixture.conversation.id().as_str(),
        )
        .expect("durable continuity key");
        let pending = store
            .load(key.clone())
            .await
            .expect("load pending continuity")
            .expect("accepted prompt owns continuity");
        assert!(
            !pending
                .record
                .value
                .to_string()
                .contains("stream-fragment-only")
        );
        fixture
            .conversation
            .resolve_notification(approval.id().clone(), NotificationResolution::Allow)
            .await
            .expect("approval still resolves during streaming");
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), output.recv()).await.expect("second delta"),
            Some(Ok(PromptEvent::OutputDelta { text })) if text == "after-tool-fragment"
        ));
        let Some(Ok(PromptEvent::Completed(response))) =
            tokio::time::timeout(Duration::from_secs(3), output.recv())
                .await
                .expect("durable terminal observation")
        else {
            panic!("stream must contain one exact terminal response");
        };
        assert_eq!(response.output(), "{\"answer\":\"terminal-only-final\"}");
        assert_eq!(
            response.structured_output(),
            Some(&serde_json::json!({"answer": "terminal-only-final"}))
        );
        assert_eq!(response.usage(), Some(&UsageInfo::new(2, 2)));
        assert!(output.recv().await.is_none());
        let requests = fixture.state.requests();
        assert_eq!(requests.len(), 2);
        let assistant = requests[1]
            .iter()
            .position(|message| message.role() == Role::Assistant)
            .expect("returned assistant preserved");
        assert_eq!(requests[1][assistant], exact_function_message);
        assert_eq!(
            requests[1][assistant + 1].function_results()[0].call_id(),
            Some("fixture-call")
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        let committed = store
            .load(key)
            .await
            .expect("load committed continuity")
            .expect("quiescent continuity remains reopenable");
        let durable = committed.record.value.to_string();
        assert!(durable.contains("terminal-only-final"));
        assert!(!durable.contains("stream-fragment-only"));
        assert!(!durable.contains("after-tool-fragment"));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), notifications.recv())
                .await
                .is_err()
        );
        drop(notifications);
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn custom_prompt_stream_unsupported_retains_generate_only_prompt() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::None,
        vec![ScriptedState::terminal_response(
            "ordinary prompt remains supported",
        )],
        HandlerBehavior::Complete,
        None,
    )
    .await;
    match fixture
        .conversation
        .prompt_stream(PromptRequest::new("unsupported streaming").expect("prompt"))
        .await
    {
        Err(error) if error.kind() == agent_sdk::error::ErrorKind::Unsupported => {}
        Ok(mut output) => {
            assert!(
                matches!(tokio::time::timeout(Duration::from_secs(3), output.recv())
                    .await
                    .expect("unsupported settles"), Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Unsupported))
            );
            assert!(output.recv().await.is_none());
        }
        Err(error) => panic!("expected honest streaming Unsupported, got {error}"),
    }
    assert!(fixture.state.requests().is_empty());
    let response = fixture
        .conversation
        .prompt(PromptRequest::new("ordinary request").expect("prompt"))
        .await
        .expect("generate-only Models remain usable");
    assert_eq!(response.output(), "ordinary prompt remains supported");
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_prompt_stream_premature_eof_fails_without_committing_partial_history() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::None,
        vec![ScriptedState::terminal_response(
            "ordinary after failed stream",
        )],
        HandlerBehavior::Complete,
        None,
    )
    .await;
    fixture
        .state
        .set_streams(vec![vec![Ok(GenerateEvent::ContentDelta {
            delta: "incomplete source text".to_owned(),
        })]]);
    let mut output = fixture
        .conversation
        .prompt_stream(PromptRequest::new("incomplete stream").expect("prompt"))
        .await
        .expect("stream is admitted");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), output.recv()).await.expect("partial text"),
        Some(Ok(PromptEvent::OutputDelta { text })) if text == "incomplete source text"
    ));
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(3), output.recv())
            .await
            .expect("EOF settles"), Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol))
    );
    assert!(output.recv().await.is_none());
    let response = fixture
        .conversation
        .prompt(PromptRequest::new("ordinary after EOF").expect("prompt"))
        .await
        .expect("failed stream releases admission after terminalization");
    assert_eq!(response.output(), "ordinary after failed stream");
    let requests = fixture.state.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]
            .iter()
            .all(|message| message.role() != Role::Assistant)
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_prompt_stream_drop_and_overflow_do_not_abort_accepted_work() {
    for overflow in [false, true] {
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("stream continuity store");
        let fixture = CustomPromptFixture::build_with_store(
            ToolSelection::Allowed(vec![TOOL.to_owned()]),
            vec![ScriptedState::terminal_response(
                "ordinary after observation loss",
            )],
            HandlerBehavior::Complete,
            None,
            CustomToolApproval::Interaction,
            store.clone(),
        )
        .await;
        let mut first = (0..if overflow { 128 } else { 1 })
            .map(|_| {
                Ok(GenerateEvent::ContentDelta {
                    delta: "fragment".to_owned(),
                })
            })
            .collect::<Vec<_>>();
        first.push(Ok(GenerateEvent::Completed(Box::new(
            ScriptedState::function_response(),
        ))));
        fixture.state.set_streams(vec![
            first,
            vec![Ok(GenerateEvent::Completed(Box::new(
                ScriptedState::terminal_response("unobserved durable final answer"),
            )))],
        ]);
        let mut notifications = fixture
            .conversation
            .notification_stream()
            .expect("notifications");
        let mut output = tokio::time::timeout(
            Duration::from_secs(3),
            fixture
                .conversation
                .prompt_stream(PromptRequest::new("observe the tool turn").expect("prompt")),
        )
        .await
        .expect("stream admission is bounded")
        .expect("stream admitted");
        let approval = tokio::time::timeout(Duration::from_secs(3), notifications.recv())
            .await
            .expect("output pressure cannot block approval delivery")
            .expect("receiver stays open")
            .expect("approval delivered");
        assert!(matches!(
            approval.kind(),
            ConversationNotificationKind::Interaction { .. }
        ));
        if overflow {
            let mut fragments = 0;
            loop {
                match tokio::time::timeout(Duration::from_secs(3), output.recv())
                    .await
                    .expect("overflow observation settles")
                {
                    Some(Ok(PromptEvent::OutputDelta { .. })) => fragments += 1,
                    Some(Err(error))
                        if error.kind() == agent_sdk::error::ErrorKind::Integration
                            && error.message().contains("overflow") =>
                    {
                        break;
                    }
                    _ => panic!("overflow must report explicit observation loss without Completed"),
                }
            }
            assert!(
                fragments <= 64,
                "supplied output queue cannot grow without bound"
            );
            assert!(output.recv().await.is_none());
        } else {
            assert!(matches!(
                output.recv().await,
                Some(Ok(PromptEvent::OutputDelta { .. }))
            ));
        }
        drop(output);
        assert!(matches!(fixture
                .conversation
                .prompt(PromptRequest::new("must not steal the active generation").expect("prompt"))
                .await, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Busy)));
        fixture
            .conversation
            .resolve_notification(approval.id().clone(), NotificationResolution::Allow)
            .await
            .expect("accepted tool turn survives lost observation");
        let key = StateKey::new(
            "agent_sdk_custom_continuations_v1",
            fixture.conversation.id().as_str(),
        )
        .expect("continuity key");
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let record = store
                    .load(key.clone())
                    .await
                    .expect("durable observation")
                    .expect("continuity retained");
                if record
                    .record
                    .value
                    .to_string()
                    .contains("unobserved durable final answer")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runtime must commit final answer despite observer loss");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        let response = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match fixture
                    .conversation
                    .prompt(
                        PromptRequest::new("new generation after old terminal").expect("prompt"),
                    )
                    .await
                {
                    Err(error) if error.kind() == agent_sdk::error::ErrorKind::Busy => {
                        tokio::task::yield_now().await
                    }
                    result => {
                        break result.expect("admission releases after accepted work completes");
                    }
                }
            }
        })
        .await
        .expect("settlement cannot depend on the lost observer");
        assert_eq!(response.output(), "ordinary after observation loss");
        assert_eq!(fixture.state.requests().len(), 3);
        drop(notifications);
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn custom_prompt_stream_abort_settles_old_generation_before_new_output() {
    let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("stream fixture store");
    let fixture = CustomPromptFixture::build_with_store(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        Vec::new(),
        HandlerBehavior::Complete,
        None,
        CustomToolApproval::Interaction,
        store,
    )
    .await;
    fixture.state.set_streams(vec![
        vec![
            Ok(GenerateEvent::ContentDelta {
                delta: "old generation".to_owned(),
            }),
            Ok(GenerateEvent::Completed(Box::new(
                ScriptedState::function_response(),
            ))),
        ],
        vec![
            Ok(GenerateEvent::ContentDelta {
                delta: "new generation".to_owned(),
            }),
            Ok(GenerateEvent::Completed(Box::new(
                ScriptedState::terminal_response("new final"),
            ))),
        ],
    ]);
    let mut notifications = fixture
        .conversation
        .notification_stream()
        .expect("notifications");
    let mut old = fixture
        .conversation
        .prompt_stream(PromptRequest::new("wait for approval").expect("prompt"))
        .await
        .expect("stream admitted");
    assert_eq!(
        old.recv().await.expect("old delta").expect("output"),
        PromptEvent::OutputDelta {
            text: "old generation".to_owned()
        }
    );
    let approval = tokio::time::timeout(Duration::from_secs(3), notifications.recv())
        .await
        .expect("approval progress")
        .expect("receiver open")
        .expect("approval");
    assert!(
        tokio::time::timeout(Duration::from_millis(25), old.recv())
            .await
            .is_err(),
        "cancelled observation must leave generation running for explicit abort"
    );
    tokio::time::timeout(Duration::from_secs(3), fixture.conversation.abort())
        .await
        .expect("abort remains bounded")
        .expect("abort settles");
    assert!(
        matches!(old.recv().await, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Cancelled))
    );
    assert!(old.recv().await.is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert!(
        fixture
            .conversation
            .resolve_notification(approval.id().clone(), NotificationResolution::Allow)
            .await
            .is_err(),
        "old approval cannot resume the cancelled generation"
    );
    let mut new = fixture
        .conversation
        .prompt_stream(PromptRequest::new("new prompt").expect("prompt"))
        .await
        .expect("later generation admitted");
    assert_eq!(
        new.recv().await.expect("new delta").expect("output"),
        PromptEvent::OutputDelta {
            text: "new generation".to_owned()
        }
    );
    let Some(Ok(PromptEvent::Completed(response))) = new.recv().await else {
        panic!("new terminal response")
    };
    assert_eq!(response.output(), "new final");
    assert!(new.recv().await.is_none());
    assert_eq!(fixture.state.requests().len(), 2);
    drop(notifications);
    fixture.shutdown().await;
}

#[tokio::test]
async fn custom_prompt_stream_waiter_deadline_bounds_observation_during_retries() {
    for primary_failure in [false, true] {
        let (fixture, store) = CustomPromptFixture::build_waiter_deadline(primary_failure).await;
        let request = PromptRequest::new("settle before the request deadline")
            .expect("deadline prompt")
            .with_timeout(Duration::from_secs(1))
            .expect("request deadline");
        let busy_request = PromptRequest::new("must remain fenced").expect("busy prompt");
        store.prompt_waiter_conflict.store(true, Ordering::SeqCst);
        let outer_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let observed = tokio::time::timeout_at(outer_deadline, async {
            let mut stream = fixture.stream_after_waiter_fault(&store, request).await?;
            let terminal = stream.recv().await;
            let busy = fixture.conversation.prompt(busy_request).await;
            Ok::<_, SdkError>((stream, terminal, busy))
        })
        .await;
        store.release_prompt_waiter_fault();
        let discarded = fixture.waiter_is_discarded().await;
        let attempts = store.prompt_waiter_attempts.load(Ordering::SeqCst);
        let requests = fixture.state.requests().len();
        fixture.shutdown().await;

        let (mut stream, terminal, busy) = observed
            .expect("the request deadline must end observation before the lifecycle deadline")
            .expect("the prompt must reach waiter settlement");
        assert!(attempts > 0, "the waiter conflict must be exercised");
        if primary_failure {
            assert!(
                matches!(terminal, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol) && (error.detail() == Some(&agent_sdk::error::ErrorDetail::IncompleteStream))),
                "the known execution error must survive the observation deadline"
            );
        } else {
            assert!(
                matches!(terminal, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Timeout))
            );
        }
        assert!(
            matches!(busy, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Busy))
        );
        assert!(discarded, "settlement continues after observation expires");
        assert_eq!(requests, 1, "timeout cannot replay the model call");
        assert!(
            stream.recv().await.is_none(),
            "late settlement cannot publish another terminal"
        );
    }
}

#[tokio::test]
async fn custom_prompt_stream_waiter_deadline_retains_admitted_commit() {
    for hold in [
        PromptWaiterCommitHold::Before,
        PromptWaiterCommitHold::After,
    ] {
        let (fixture, store) = CustomPromptFixture::build_waiter_deadline(false).await;
        let request = PromptRequest::new("hold the original waiter commit")
            .expect("held commit prompt")
            .with_timeout(Duration::from_secs(1))
            .expect("request deadline");
        let busy_request = PromptRequest::new("must remain fenced").expect("busy prompt");
        store.arm_prompt_waiter_commit(hold);
        let outer_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let observed = tokio::time::timeout_at(outer_deadline, async {
            let mut stream = fixture.stream_after_waiter_fault(&store, request).await?;
            let terminal = stream.recv().await;
            let busy = fixture.conversation.prompt(busy_request).await;
            let commits = store.prompt_waiter_commits.load(Ordering::SeqCst);
            let receipts = store.prompt_waiter_receipts.load(Ordering::SeqCst);
            Ok::<_, SdkError>((stream, terminal, busy, commits, receipts))
        })
        .await;
        store.release_prompt_waiter_fault();
        let discarded = fixture.waiter_is_discarded().await;
        let joined = fixture.conversation.abort().await;
        let commits = store.prompt_waiter_commits.load(Ordering::SeqCst);
        let receipts = store.prompt_waiter_receipts.load(Ordering::SeqCst);
        let original_requests = fixture.state.requests().len();
        let reused = fixture
            .conversation
            .prompt(PromptRequest::new("reuse the settled conversation").expect("reuse prompt"))
            .await;
        let requests_after_reuse = fixture.state.requests().len();
        fixture.shutdown().await;

        let (mut stream, terminal, busy, held_commits, held_receipts) = observed
            .expect("observation must expire while the commit gate remains closed")
            .expect("the prompt must reach its original waiter commit");
        assert!(
            matches!(terminal, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Timeout))
        );
        assert!(
            matches!(busy, Err(error) if matches!(error.kind(), agent_sdk::error::ErrorKind::Busy))
        );
        assert_eq!(
            held_commits,
            usize::from(matches!(hold, PromptWaiterCommitHold::After))
        );
        assert_eq!(
            held_receipts, 0,
            "no commit receipt was released before timeout"
        );
        assert!(discarded, "the original admitted write must still commit");
        joined.expect("abort joins settlement after its original receipt");
        assert_eq!(commits, 1, "the native waiter commit must not be replayed");
        assert_eq!(receipts, 1, "the retained commit receipt must be observed");
        assert_eq!(original_requests, 1);
        assert_eq!(
            reused.expect("reuse after settlement").output(),
            "reused after deadline"
        );
        assert_eq!(requests_after_reuse, 2);
        assert!(
            stream.recv().await.is_none(),
            "the expired stream stays fused"
        );
    }
}

#[test]
fn custom_prompt_stream_waiter_deadline_wins_when_settlement_is_ready() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread deadline runtime");
    let (fixture, store, ready, started, outer_deadline) = runtime.block_on(async {
        let (fixture, store) = CustomPromptFixture::build_waiter_deadline(false).await;
        let request = PromptRequest::new("release a receipt after its deadline")
            .expect("same-ready prompt")
            .with_timeout(Duration::from_secs(1))
            .expect("request deadline");
        store.arm_prompt_waiter_commit(PromptWaiterCommitHold::After);
        let started = std::time::Instant::now();
        let outer_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let ready = tokio::time::timeout_at(
            outer_deadline,
            fixture.stream_after_waiter_fault(&store, request),
        )
        .await;
        (fixture, store, ready, started, outer_deadline)
    });
    let reached_before_deadline = started.elapsed() < Duration::from_secs(1);
    // Time advances outside the async runtime. The expired timer and released
    // receipt are both ready when its existing prompt task is polled again.
    if matches!(&ready, Ok(Ok(_))) {
        std::thread::sleep(
            Duration::from_secs(1).saturating_sub(started.elapsed()) + Duration::from_millis(25),
        );
    }
    store.release_prompt_waiter_fault();
    let observed = runtime.block_on(async {
        match ready {
            Ok(Ok(mut stream)) => Some(
                tokio::time::timeout_at(outer_deadline, async {
                    let terminal = stream.recv().await;
                    (stream, terminal)
                })
                .await,
            ),
            Ok(Err(_)) | Err(_) => None,
        }
    });
    let (discarded, requests, commits, receipts) = runtime.block_on(async {
        let discarded = fixture.waiter_is_discarded().await;
        let requests = fixture.state.requests().len();
        let commits = store.prompt_waiter_commits.load(Ordering::SeqCst);
        let receipts = store.prompt_waiter_receipts.load(Ordering::SeqCst);
        fixture.shutdown().await;
        (discarded, requests, commits, receipts)
    });

    assert!(
        reached_before_deadline,
        "the receipt gate must precede request expiry"
    );
    let (mut stream, terminal) = observed
        .expect("the prompt must reach its original receipt gate")
        .expect("the original absolute observation bound must hold");
    assert!(
        matches!(terminal, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Timeout))
    );
    assert!(discarded);
    assert_eq!(requests, 1);
    assert_eq!(commits, 1);
    assert_eq!(receipts, 1);
    assert!(runtime.block_on(stream.recv()).is_none());
}

#[tokio::test]
async fn custom_prompt_stream_waiter_deadline_allows_early_settlement() {
    let (fixture, store) = CustomPromptFixture::build_waiter_deadline(false).await;
    let request = PromptRequest::new("release the receipt before its deadline")
        .expect("early settlement prompt")
        .with_timeout(Duration::from_secs(1))
        .expect("request deadline");
    store.arm_prompt_waiter_commit(PromptWaiterCommitHold::After);
    let started = tokio::time::Instant::now();
    let outer_deadline = started + Duration::from_secs(2);
    let observed = tokio::time::timeout_at(outer_deadline, async {
        let mut stream = fixture.stream_after_waiter_fault(&store, request).await?;
        store.release_prompt_waiter_fault();
        let terminal = stream.recv().await;
        Ok::<_, SdkError>((stream, terminal, started.elapsed()))
    })
    .await;
    store.release_prompt_waiter_fault();
    let discarded = fixture.waiter_is_discarded().await;
    let requests = fixture.state.requests().len();
    let commits = store.prompt_waiter_commits.load(Ordering::SeqCst);
    let receipts = store.prompt_waiter_receipts.load(Ordering::SeqCst);
    fixture.shutdown().await;

    let (mut stream, terminal, elapsed) = observed
        .expect("early settlement stays within the absolute observation bound")
        .expect("the prompt reaches waiter settlement");
    let Some(Ok(PromptEvent::Completed(response))) = terminal else {
        panic!("settlement before the deadline must preserve successful completion")
    };
    assert_eq!(response.output(), "durable terminal");
    assert!(elapsed < Duration::from_secs(1));
    assert!(discarded);
    assert_eq!(requests, 1);
    assert_eq!(commits, 1);
    assert_eq!(receipts, 1);
    assert!(stream.recv().await.is_none());
}

#[tokio::test]
async fn custom_prompt_stream_waiter_failure_is_terminal_and_preserves_primary_error() {
    for (primary_failure, rejected_finish) in [
        (false, None),
        (true, None),
        (true, Some(FinishReason::Error)),
        (true, Some(FinishReason::Paused)),
    ] {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("waiter fault store");
        let store = NotificationConflictStore::new(raw.clone());
        let fixture = CustomPromptFixture::build_with_store(
            ToolSelection::None,
            Vec::new(),
            HandlerBehavior::Complete,
            None,
            CustomToolApproval::Allow,
            store.clone(),
        )
        .await;
        fixture
            .state
            .set_streams(vec![if let Some(reason) = &rejected_finish {
                let mut rejected =
                    ScriptedState::terminal_response("rejected before cleanup failure");
                rejected.finish_reason = Some(reason.clone());
                vec![Ok(GenerateEvent::Completed(Box::new(rejected)))]
            } else if primary_failure {
                vec![Err(ProviderError::new(
                    ProviderErrorKind::InvalidResponse(ResponseViolation::UnexpectedEnd),
                    ProviderOperation::GenerateStream,
                    ProviderErrorPhase::Stream,
                ))]
            } else {
                vec![Ok(GenerateEvent::Completed(Box::new(
                    ScriptedState::terminal_response("durable terminal"),
                )))]
            }]);
        store.prompt_waiter_conflict.store(true, Ordering::SeqCst);
        let mut stream = fixture
            .conversation
            .prompt_stream(PromptRequest::new("settle waiter").expect("prompt"))
            .await
            .expect("stream admission");
        let terminal = tokio::time::timeout(Duration::from_secs(5), stream.recv()).await;
        let attempts = store.prompt_waiter_attempts.load(Ordering::SeqCst);
        let key = StateKey::new(
            "agent_sdk_custom_continuations_v1",
            fixture.conversation.id().as_str(),
        )
        .expect("key");
        let pending = tokio::time::timeout(Duration::from_secs(3), raw.load(key.clone())).await;
        let eof = tokio::time::timeout(Duration::from_secs(3), stream.recv()).await;
        drop(stream);
        store.prompt_waiter_conflict.store(false, Ordering::SeqCst);
        let aborted =
            tokio::time::timeout(Duration::from_secs(5), fixture.conversation.abort()).await;
        let recovered = tokio::time::timeout(Duration::from_secs(3), raw.load(key)).await;
        fixture.shutdown().await;

        let terminal = terminal.expect("waiter conflict deadline");
        assert!(attempts > 0, "fault must reach terminal waiter mutation");
        let pending = pending
            .expect("retained record read must remain bounded")
            .expect("retained record")
            .expect("continuity");
        assert_eq!(
            pending
                .record
                .value
                .pointer("/terminal/waiter")
                .and_then(serde_json::Value::as_str),
            Some("pending")
        );
        if let Some(reason) = &rejected_finish {
            let error = terminal
                .expect("terminal error")
                .expect_err("execution error must survive waiter conflict");
            ScriptedState::assert_finish_failure(Some(reason), &error);
        } else if primary_failure {
            assert!(
                matches!(terminal, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol)),
                "execution failure must survive later waiter failure"
            );
        } else {
            assert!(
                matches!(terminal, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Timeout)),
                "failed waiter settlement cannot claim Completed"
            );
        }
        assert!(eof.expect("stream EOF must remain bounded").is_none());
        aborted
            .expect("recovery abort must remain bounded")
            .expect("recovered storage must settle the retained generation");
        let recovered = recovered
            .expect("recovered record read must remain bounded")
            .expect("recovered record")
            .expect("continuity");
        assert_eq!(
            recovered
                .record
                .value
                .pointer("/terminal/waiter")
                .and_then(serde_json::Value::as_str),
            Some("discarded")
        );
    }
}

#[tokio::test]
async fn custom_prompt_stream_waiter_failure_survives_agent_shutdown_and_reopen() {
    let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .expect("waiter recovery store");
    let store = NotificationConflictStore::new(raw.clone());
    let fixture = CustomPromptFixture::build_with_store(
        ToolSelection::None,
        Vec::new(),
        HandlerBehavior::Complete,
        None,
        CustomToolApproval::Allow,
        store.clone(),
    )
    .await;
    fixture
        .state
        .set_streams(vec![vec![Ok(GenerateEvent::Completed(Box::new(
            ScriptedState::terminal_response("retained answer"),
        )))]]);
    store.prompt_waiter_conflict.store(true, Ordering::SeqCst);
    let mut stream = fixture
        .conversation
        .prompt_stream(PromptRequest::new("retained question").expect("prompt"))
        .await
        .expect("stream admission");
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(5), stream.recv())
            .await
            .expect("settlement deadline"), Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Timeout))
    );
    assert!(store.prompt_waiter_attempts.load(Ordering::SeqCst) > 0);
    assert!(stream.recv().await.is_none());
    store.prompt_waiter_conflict.store(false, Ordering::SeqCst);
    let agent_id = fixture.agent.id();
    let workspace_id = fixture.workspace.id();
    let conversation_id = fixture.conversation.id();
    let key = StateKey::new(
        "agent_sdk_custom_continuations_v1",
        conversation_id.as_str(),
    )
    .expect("continuation key");
    let CustomPromptFixture {
        storage: _,
        raw_store: _,
        agent,
        workspace,
        conversation,
        root,
        state,
        calls,
        cancellation,
        toolset_count: _,
    } = fixture;
    drop(conversation);
    drop(workspace);
    agent.shutdown().await.expect("Agent-owned cleanup");
    assert_eq!(cancellation.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(state.requests().len(), 1);
    let retained = raw
        .load(key.clone())
        .await
        .expect("retained state")
        .expect("retained continuity");
    assert_eq!(
        retained.record.value.pointer("/terminal/waiter"),
        Some(&serde_json::json!("pending")),
        "parent shutdown retains already reported failed waiter evidence for reopen"
    );
    let reopened_state = ScriptedState::new(vec![ScriptedState::terminal_response("new answer")]);
    let custom = CustomAgentConfig::new(
        ModelSelector::new(MODEL).expect("model"),
        ToolSelection::None,
        CustomLoopLimits::new(4, 4, 1).expect("limits"),
        CustomToolApproval::Allow,
    )
    .expect("Custom config");
    let config = AgentConfig::new(
        custom.into(),
        Duration::from_secs(3),
        NonZeroUsize::new(1).expect("one prompt"),
    )
    .expect("Agent config");
    let reopened_cancellation = Arc::new(CancellationProbe::default());
    let reopened = AgentBuilder::reopen_for_backends::<ScriptedProvider, ScriptedToolset>(
        config,
        AgentStorage::new(NotificationConflictStore::new(raw.clone())),
        agent_id,
    )
    .with_provider(ScriptedProvider {
        state: Arc::clone(&reopened_state),
    })
    .with_toolset(ScriptedToolset::new(
        TOOL,
        HandlerBehavior::Complete,
        Arc::clone(&calls),
        Arc::clone(&reopened_cancellation),
    ))
    .build()
    .await
    .expect("fresh integrations reconcile retained waiter");
    let recovered = raw
        .load(key)
        .await
        .expect("recovered state")
        .expect("recovered continuity");
    assert_eq!(
        recovered.record.value.pointer("/terminal/waiter"),
        Some(&serde_json::json!("discarded"))
    );
    let workspace = reopened
        .get_workspace(workspace_id)
        .await
        .expect("workspace");
    let conversation = workspace
        .get_conversation(conversation_id)
        .await
        .expect("conversation");
    let response = conversation
        .prompt(PromptRequest::new("new question").expect("prompt"))
        .await
        .expect("recovered prompt");
    assert_eq!(response.output(), "new answer");
    let requests = reopened_state.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].iter().map(Message::role).collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant, Role::User]
    );
    assert_eq!(
        requests[0][1].content(),
        [ContentPart::Text {
            text: "retained answer".to_owned()
        }]
    );
    assert!(
        requests[0]
            .iter()
            .all(|message| message.continuation().is_none())
    );
    assert_eq!(
        state.requests().len(),
        1,
        "reopen cannot replay the old model turn"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    conversation.close().await.expect("conversation cleanup");
    workspace.close().await.expect("workspace cleanup");
    reopened.shutdown().await.expect("reopened Agent cleanup");
    assert_eq!(reopened_cancellation.shutdowns.load(Ordering::SeqCst), 1);
    assert!(
        stream.recv().await.is_none(),
        "reopen cannot republish an old terminal"
    );
    std::fs::remove_dir_all(root).expect("fixture directory cleanup");
}

#[tokio::test]
async fn custom_prompt_stream_ready_only_source_yields_for_observation_and_abort() {
    for reasoning in [false, true] {
        let fixture = CustomPromptFixture::build(
            ToolSelection::None,
            Vec::new(),
            HandlerBehavior::Complete,
            None,
        )
        .await;
        let observed = Arc::new(AtomicUsize::new(0));
        let limit = 200_000;
        fixture
            .state
            .streams
            .lock()
            .expect("scripted streams")
            .push_back(ScriptedStream::Ready {
                observed: Arc::clone(&observed),
                limit,
                reasoning,
            });
        let mut stream = fixture
            .conversation
            .prompt_stream(PromptRequest::new("continuously ready source").expect("prompt"))
            .await
            .expect("stream admission");
        assert!(matches!(
            stream.recv().await,
            Some(Ok(PromptEvent::OutputDelta { .. }))
        ));
        assert!(
            observed.load(Ordering::SeqCst) < limit,
            "finite always-ready source must yield before exhausting to let caller observe and abort"
        );
        tokio::time::timeout(Duration::from_secs(3), fixture.conversation.abort())
            .await
            .expect("abort stays responsive")
            .expect("abort settles");
        let stopped = observed.load(Ordering::SeqCst);
        assert!(
            stopped < limit,
            "explicit abort stops source consumption before natural completion"
        );
        loop {
            match stream.recv().await {
                Some(Ok(PromptEvent::OutputDelta { .. })) => {}
                Some(Err(error))
                    if matches!(
                        error.kind(),
                        agent_sdk::error::ErrorKind::Cancelled
                            | agent_sdk::error::ErrorKind::Integration
                    ) =>
                {
                    break;
                }
                _ => panic!("aborted source cannot claim completed output"),
            }
        }
        assert!(stream.recv().await.is_none());
        tokio::task::yield_now().await;
        assert_eq!(observed.load(Ordering::SeqCst), stopped);
        fixture.shutdown().await;
    }
}

#[tokio::test]
async fn custom_prompt_commit_timeout_preserves_evidence_and_authoritative_success() {
    use agent_sdk::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
    for commit_first in [false, true] {
        let raw = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .expect("store");
        let store = NotificationConflictStore::new(raw);
        let fixture = CustomPromptFixture::build_with_store(
            ToolSelection::None,
            vec![ScriptedState::terminal_response("completed")],
            HandlerBehavior::Complete,
            None,
            CustomToolApproval::Allow,
            store.clone(),
        )
        .await;
        *store.commit_timeout.lock().unwrap() = Some(commit_first);
        let result = fixture
            .conversation
            .prompt(PromptRequest::new("exercise commit receipt").unwrap())
            .await;
        let requests = fixture.state.requests().len();
        fixture.shutdown().await;
        if commit_first {
            assert_eq!(result.unwrap().output(), "completed");
            assert_eq!(requests, 1);
        } else {
            let error = result.expect_err("uncommitted admission cannot execute a model");
            assert_eq!(requests, 0);
            assert_eq!(error.kind(), ErrorKind::Timeout);
            assert_eq!(error.context().phase, ErrorPhase::StateCommit);
            assert_eq!(
                error.detail(),
                Some(&ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Operation,
                    timeout: Some(Duration::from_millis(41))
                })
            );
            assert!(!format!("{error:?}").contains("secret-commit-operation"));
        }
    }
}

#[tokio::test]
async fn custom_shutdown_does_not_duplicate_a_dependency_timeout() {
    let fixture = CustomPromptFixture::build(
        ToolSelection::None,
        Vec::new(),
        HandlerBehavior::Complete,
        None,
    )
    .await;
    fixture
        .cancellation
        .shutdown_timeout
        .store(true, Ordering::SeqCst);
    let CustomPromptFixture {
        agent,
        workspace,
        conversation,
        root,
        ..
    } = fixture;
    drop(conversation);
    drop(workspace);
    let error = agent.shutdown().await.expect_err("tool shutdown timeout");
    std::fs::remove_dir_all(root).expect("fixture directory cleanup");
    assert_eq!(error.kind(), agent_sdk::error::ErrorKind::Timeout);
    assert!(
        error.supplementary().is_empty(),
        "settled failure must not be duplicated as observation expiry: {error:?}"
    );
}

impl CustomPromptFixture<StateStore> {
    async fn reject_excluded_calls(
        selection: ToolSelection,
        names: &[&str],
        streamed: bool,
        approval: CustomToolApproval,
        prior_valid_turn: bool,
    ) {
        let calls = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                FunctionCall::new(
                    Some(format!("selection-call-{index}")),
                    *name,
                    serde_json::json!({}),
                )
                .unwrap()
            })
            .collect();
        let rejected = ScriptedState::response(
            Message::assistant(Vec::new(), calls).unwrap(),
            FinishReason::ToolCall,
        );
        let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
            .await
            .unwrap();
        let mut responses = vec![
            rejected.clone(),
            ScriptedState::terminal_response("unexpected continuation"),
        ];
        if prior_valid_turn {
            responses.insert(0, ScriptedState::function_response());
        }
        let fixture = Self::build_with_tools(
            selection,
            responses.clone(),
            HandlerBehavior::Complete,
            None,
            approval,
            store,
            &[TOOL, "excluded_tool"],
        )
        .await;
        if streamed {
            fixture.state.set_streams(
                responses
                    .into_iter()
                    .map(|response| vec![Ok(GenerateEvent::Completed(Box::new(response)))])
                    .collect(),
            );
        }
        let mut notifications = fixture.conversation.notification_stream().unwrap();
        let observed = NativeCleanup::observe(
            tokio::time::Instant::now() + Duration::from_secs(12),
            async {
        let mut approvals = 0;
        let result = {
            let prompt = async {
                let request = PromptRequest::new("reject the excluded tool batch").unwrap();
                if streamed {
                    let mut output = fixture.conversation.prompt_stream(request).await?;
                    loop {
                        match output.recv().await {
                            Some(Ok(PromptEvent::OutputDelta { .. })) => {}
                            Some(Ok(PromptEvent::Completed(response))) => break Ok(response),
                            Some(Err(error)) => break Err(error),
                            None => panic!("a selection failure must publish a terminal result"),
                        }
                    }
                } else {
                    fixture.conversation.prompt(request).await
                }
            };
            tokio::pin!(prompt);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    tokio::select! {
                        biased;
                        notification = notifications.recv() => {
                            let notification = notification.expect("notification source remains open").unwrap();
                            if matches!(notification.kind(), ConversationNotificationKind::Interaction { .. }) {
                                approvals += 1;
                                let (aborted, result) = futures_util::future::join(
                                    fixture.conversation.abort(), &mut prompt,
                                ).await;
                                if let Err(error) = aborted {
                                    eprintln!("excluded-call abort cleanup failed: {error:?}");
                                }
                                break result;
                            }
                        }
                        result = &mut prompt => break result,
                    }
                }
            }).await
        };
        let requests_before_reuse = fixture.state.requests();
        let created = fixture.cancellation.created.load(Ordering::SeqCst);
        let calls = fixture.calls.load(Ordering::SeqCst);
        // Reset only the future scripted replies after recording the rejected attempt.
        *fixture.state.responses.lock().unwrap() =
            VecDeque::from([ScriptedState::terminal_response("fresh selection answer")]);
        let reused = tokio::time::timeout(Duration::from_secs(3), fixture
            .conversation
            .prompt(
                PromptRequest::new("fresh prompt after excluded call")
                    .unwrap()
                    .with_tool_selection(ToolSelection::None)
                    .unwrap(),
            ))
            .await;
        let requests = fixture.state.requests();
        assert_eq!(
            approvals, 0,
            "excluded tools must not publish approval requests"
        );
        assert_eq!(
            created, usize::from(prior_valid_turn),
            "reject the entire batch before constructing any handler"
        );
        assert_eq!(calls, usize::from(prior_valid_turn), "no installed but excluded tool may execute; prior accepted effects never replay");
        let error = result
            .expect("selection rejection must remain bounded")
            .expect_err("excluded calls must fail");
        assert_eq!(error.kind(), agent_sdk::error::ErrorKind::Protocol);
        assert_eq!(error.context().operation, agent_sdk::error::Operation::Prompt);
        assert_eq!(error.context().phase, agent_sdk::error::ErrorPhase::CompletionValidation);
        assert_eq!(error.detail(), Some(&agent_sdk::error::ErrorDetail::InvalidData));
        assert_eq!(
            requests_before_reuse.len(),
            1 + usize::from(prior_valid_turn),
            "do not continue rejected model output"
        );
        assert_eq!(reused.expect("reuse stays bounded").unwrap().output(), "fresh selection answer");
        assert_eq!(
            requests.last().unwrap(),
            &vec![Message::user_text("fresh prompt after excluded call").unwrap()]
        );
            },
        ).await;
        let CustomPromptFixture {
            agent,
            workspace,
            conversation,
            cancellation,
            toolset_count,
            root,
            ..
        } = fixture;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
        cleanup.settle(conversation.close()).await;
        cleanup
            .settle(async {
                while let Some(notification) = notifications.recv().await {
                    notification?;
                }
                Ok::<(), SdkError>(())
            })
            .await;
        cleanup.settle(workspace.close()).await;
        cleanup.settle(agent.shutdown()).await;
        cleanup
            .settle(async { std::fs::remove_dir_all(root) })
            .await;
        cleanup.finish(observed);
        assert_eq!(cancellation.shutdowns.load(Ordering::SeqCst), toolset_count);
    }
}

#[tokio::test]
async fn custom_excluded_none_unary_is_rejected_before_execution() {
    CustomPromptFixture::reject_excluded_calls(
        ToolSelection::None,
        &["excluded_tool"],
        false,
        CustomToolApproval::Allow,
        false,
    )
    .await;
}

#[tokio::test]
async fn custom_excluded_allowed_unary_is_rejected_before_execution() {
    CustomPromptFixture::reject_excluded_calls(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        &["excluded_tool"],
        false,
        CustomToolApproval::Allow,
        false,
    )
    .await;
}

#[tokio::test]
async fn custom_excluded_required_one_of_unary_is_rejected_before_execution() {
    CustomPromptFixture::reject_excluded_calls(
        ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
        &["excluded_tool"],
        false,
        CustomToolApproval::Allow,
        false,
    )
    .await;
}

#[tokio::test]
async fn custom_mixed_tool_batch_unary_is_rejected_before_any_execution() {
    for names in [[TOOL, "excluded_tool"], ["excluded_tool", TOOL]] {
        for selection in [
            ToolSelection::Allowed(vec![TOOL.to_owned()]),
            ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
        ] {
            CustomPromptFixture::reject_excluded_calls(
                selection,
                &names,
                false,
                CustomToolApproval::Allow,
                false,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn custom_excluded_unary_publishes_no_approval() {
    for selection in [
        ToolSelection::None,
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
    ] {
        CustomPromptFixture::reject_excluded_calls(
            selection,
            &["excluded_tool"],
            false,
            CustomToolApproval::Interaction,
            false,
        )
        .await;
    }
}

#[tokio::test]
async fn custom_excluded_none_stream_is_rejected_before_execution() {
    CustomPromptFixture::reject_excluded_calls(
        ToolSelection::None,
        &["excluded_tool"],
        true,
        CustomToolApproval::Allow,
        false,
    )
    .await;
}

#[tokio::test]
async fn custom_excluded_allowed_stream_is_rejected_before_execution() {
    CustomPromptFixture::reject_excluded_calls(
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        &["excluded_tool"],
        true,
        CustomToolApproval::Allow,
        false,
    )
    .await;
}

#[tokio::test]
async fn custom_excluded_required_one_of_stream_is_rejected_before_execution() {
    CustomPromptFixture::reject_excluded_calls(
        ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
        &["excluded_tool"],
        true,
        CustomToolApproval::Allow,
        false,
    )
    .await;
}

#[tokio::test]
async fn custom_mixed_tool_batch_stream_is_rejected_before_any_execution() {
    for names in [[TOOL, "excluded_tool"], ["excluded_tool", TOOL]] {
        for selection in [
            ToolSelection::Allowed(vec![TOOL.to_owned()]),
            ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
        ] {
            CustomPromptFixture::reject_excluded_calls(
                selection,
                &names,
                true,
                CustomToolApproval::Allow,
                false,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn custom_excluded_stream_publishes_no_approval() {
    for selection in [
        ToolSelection::None,
        ToolSelection::Allowed(vec![TOOL.to_owned()]),
        ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
    ] {
        CustomPromptFixture::reject_excluded_calls(
            selection,
            &["excluded_tool"],
            true,
            CustomToolApproval::Interaction,
            false,
        )
        .await;
    }
}

#[tokio::test]
async fn custom_selected_tool_calls_still_execute_and_continue() {
    for streamed in [false, true] {
        for selection in [
            ToolSelection::Auto,
            ToolSelection::Allowed(vec![TOOL.to_owned()]),
            ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
        ] {
            let fixture = CustomPromptFixture::build(
                selection,
                vec![
                    ScriptedState::function_response(),
                    ScriptedState::terminal_response("selected tool completed"),
                ],
                HandlerBehavior::Complete,
                None,
            )
            .await;
            fixture.state.set_streams(vec![
                vec![Ok(GenerateEvent::Completed(Box::new(
                    ScriptedState::function_response(),
                )))],
                vec![Ok(GenerateEvent::Completed(Box::new(
                    ScriptedState::terminal_response("selected tool completed"),
                )))],
            ]);
            let observed = NativeCleanup::observe(
                tokio::time::Instant::now() + Duration::from_secs(5),
                async {
                    let result = if streamed {
                        let mut output = fixture
                            .conversation
                            .prompt_stream(PromptRequest::new("run selected tool").unwrap())
                            .await
                            .unwrap();
                        match output.recv().await.unwrap().unwrap() {
                            PromptEvent::Completed(response) => response,
                            PromptEvent::OutputDelta { .. } => {
                                panic!("script contains no text delta")
                            }
                        }
                    } else {
                        fixture
                            .conversation
                            .prompt(PromptRequest::new("run selected tool").unwrap())
                            .await
                            .unwrap()
                    };
                    let requests = fixture.state.requests();
                    let calls = fixture.calls.load(Ordering::SeqCst);
                    assert_eq!(result.output(), "selected tool completed");
                    assert_eq!(calls, 1);
                    assert_eq!(requests.len(), 2);
                    assert_eq!(requests[1][1], ScriptedState::function_response().message);
                    assert_eq!(
                        requests[1][2].function_results()[0].content(),
                        [ContentPart::Text {
                            text: "fixture result".to_owned()
                        }]
                    );
                },
            )
            .await;
            let CustomPromptFixture {
                agent,
                workspace,
                conversation,
                cancellation,
                toolset_count,
                root,
                ..
            } = fixture;
            let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
            cleanup.settle(conversation.close()).await;
            cleanup.settle(workspace.close()).await;
            cleanup.settle(agent.shutdown()).await;
            cleanup
                .settle(async { std::fs::remove_dir_all(root) })
                .await;
            cleanup.finish(observed);
            assert_eq!(cancellation.shutdowns.load(Ordering::SeqCst), toolset_count);
        }
    }
}

#[tokio::test]
async fn custom_unknown_tool_calls_are_rejected_before_approval() {
    for streamed in [false, true] {
        for selection in [ToolSelection::Auto, ToolSelection::Required] {
            CustomPromptFixture::reject_excluded_calls(
                selection,
                &["not_installed"],
                streamed,
                CustomToolApproval::Interaction,
                false,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn custom_excluded_second_turn_preserves_prior_effect_and_discards_failed_history() {
    for streamed in [false, true] {
        CustomPromptFixture::reject_excluded_calls(
            ToolSelection::RequiredOneOf(vec![TOOL.to_owned()]),
            &["excluded_tool"],
            streamed,
            CustomToolApproval::Allow,
            true,
        )
        .await;
    }
}

#[tokio::test]
async fn custom_cancelled_notification_read_does_not_freeze_a_closed_snapshot() {
    let inner = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory()))
        .await
        .unwrap();
    let store = NotificationConflictStore::new(inner);
    let fixture = CustomPromptFixture::build_with_store(
        ToolSelection::None,
        Vec::new(),
        HandlerBehavior::Complete,
        None,
        CustomToolApproval::Allow,
        store.clone(),
    )
    .await;
    let mut receiver = fixture.conversation.notification_stream().unwrap();
    let observed = NativeCleanup::observe(
        tokio::time::Instant::now() + Duration::from_secs(8),
        async {
            store.receipt_read_hold.store(true, Ordering::SeqCst);
            {
                let receive = receiver.recv();
                tokio::pin!(receive);
                tokio::select! {
                    item = &mut receive => panic!("empty open journal cannot complete: {item:?}"),
                    permit = store.receipt_read_entered.acquire() => permit.unwrap().forget(),
                }
                // Cancel only the observer after its native read-only settlement.
            }
            // Close changes delivery lifecycle while the cancelled read retains its snapshot.
        },
    )
    .await;
    store.receipt_read_hold.store(false, Ordering::SeqCst);
    store
        .receipt_read_observation
        .store(false, Ordering::SeqCst);
    store.receipt_read_release.add_permits(1);
    let CustomPromptFixture {
        conversation,
        workspace,
        agent,
        root,
        ..
    } = fixture;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(3));
    cleanup.settle(conversation.close()).await;
    cleanup
        .settle(async {
            assert!(
                receiver.recv().await.is_none(),
                "closed empty journal must drain without a stale read-receipt contradiction"
            );
            Ok::<(), SdkError>(())
        })
        .await;
    cleanup.settle(workspace.close()).await;
    cleanup.settle(agent.shutdown()).await;
    cleanup
        .settle(async { std::fs::remove_dir_all(root) })
        .await;
    cleanup.finish(observed);
}
