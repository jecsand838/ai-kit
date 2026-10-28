mod metadata;
use serde_json::Value;
pub(crate) mod inbound;
mod items;
pub(crate) mod jsonrpc;
pub(crate) mod notifications;
pub(crate) mod requests;
pub(crate) mod responses;
pub(crate) mod stdio;
pub(crate) mod tcp;
pub(crate) mod unix;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::backends::codex::adapters::conversation::{
    CodexFeatureList, CodexMcpStatusList, CodexTerminalList, CodexThread, CodexThreadList,
    CodexWorkspacePolicy,
};
use crate::backends::codex::adapters::file::CodexFileRead;
use crate::backends::codex::adapters::model::{CodexModelInventoryEntry, CodexModelListResponse};
use crate::backends::codex::adapters::prompt::{CodexPromptSelection, CodexTurnStart};
use crate::backends::codex::notifications::CodexPromptTerminal;
use crate::backends::codex::protocol::inbound::{CodexInbound, CodexInboundFrame};
use crate::backends::codex::protocol::notifications::CodexTurnError;
use crate::backends::codex::protocol::requests::{
    CodexThreadCreateRequest, CodexThreadResumeRequest, CodexTurnStartRequest,
};
use crate::backends::codex::protocol::responses::CodexServerResponse;
use crate::backends::codex::state_store::records::CodexPromptRecord;
use crate::backends::codex::state_store::records::{
    CodexCleanup, CodexCleanupAction, CodexNativeSelection,
};
use crate::config::CodexMode;
use crate::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
use crate::{
    CodexConfig, FileRead, KitError, ModelInfo, ModelSelector, PersonalitySelector, ReasoningEffort,
};

use jsonrpc::{CodexRequestError, CodexRpc};

const SELECTED_CODEX_VERSION: &str = "0.163.0";
const AGENTS_KIT_CLIENT_VERSION: &str = "0.0.1";
const MAX_MODEL_PAGES: usize = 1_024;
const MAX_PROTOCOL_PAGES: usize = 1_024;
const PROTOCOL_PAGE_LIMIT: usize = 100;
const THREAD_SOURCE_KINDS: [&str; 10] = [
    "cli",
    "vscode",
    "exec",
    "appServer",
    "subAgent",
    "subAgentReview",
    "subAgentCompact",
    "subAgentThreadSpawn",
    "subAgentOther",
    "unknown",
];

#[derive(Clone)]
pub(crate) struct CodexProtocol {
    rpc: CodexRpc,
    loaded_threads: Arc<Mutex<BTreeSet<String>>>,
    reconciliation_timeout: Duration,
}

pub(crate) struct CodexInboundReceiver {
    frames: tokio::sync::mpsc::Receiver<Result<serde_json::Value, KitError>>,
}

pub(crate) enum CodexThreadAcquisition {
    Response(CodexThread),
    Correlated(CodexThread),
}

#[derive(Clone, Copy)]
enum CodexThreadInventoryScope {
    DefaultInteractive,
    ExplicitKinds,
}

pub(crate) enum CodexPromptTurn {
    Absent,
    Active {
        turn_id: String,
    },
    Terminal {
        turn_id: String,
        status: CodexPromptTurnStatus,
        output: Option<String>,
        error: Option<KitError>,
    },
}

impl CodexPromptTurn {
    pub(crate) fn turn_id(&self) -> Option<&str> {
        match self {
            Self::Absent => None,
            Self::Active { turn_id } | Self::Terminal { turn_id, .. } => Some(turn_id),
        }
    }
}

impl CodexThreadInventoryScope {
    fn request_params(self, cursor: Option<&str>, archived: bool) -> serde_json::Value {
        let mut params = serde_json::Map::from_iter([
            (
                "cursor".to_owned(),
                cursor.map_or(serde_json::Value::Null, |cursor| {
                    serde_json::Value::String(cursor.to_owned())
                }),
            ),
            (
                "limit".to_owned(),
                serde_json::Value::from(PROTOCOL_PAGE_LIMIT),
            ),
            ("archived".to_owned(), serde_json::Value::Bool(archived)),
            ("useStateDbOnly".to_owned(), serde_json::Value::Bool(false)),
        ]);
        if matches!(self, Self::ExplicitKinds) {
            params.insert(
                "sourceKinds".to_owned(),
                serde_json::Value::Array(
                    THREAD_SOURCE_KINDS
                        .iter()
                        .map(|kind| serde_json::Value::String((*kind).to_owned()))
                        .collect(),
                ),
            );
        }
        serde_json::Value::Object(params)
    }
}

#[derive(serde::Deserialize)]
struct CodexPromptThreadRead {
    thread: CodexPromptThread,
}

#[derive(serde::Deserialize)]
pub(crate) struct CodexPromptThread {
    id: String,
    turns: Vec<CodexPromptThreadTurn>,
}

#[derive(serde::Deserialize)]
struct CodexPromptThreadTurn {
    id: String,
    status: CodexPromptTurnStatus,
    error: Option<CodexTurnError>,
    items: Vec<CodexHistoryItem>,
    #[serde(rename = "itemsView")]
    items_view: CodexPromptItemsView,
}

impl CodexPromptThread {
    fn validate_terminal(&self, terminal: &CodexPromptTerminal) -> Result<(), KitError> {
        if self
            .turns
            .iter()
            .any(|turn| turn.items_view != CodexPromptItemsView::Full)
        {
            return Err(KitError::invalid_state("terminal history is incomplete"));
        }
        let mut generations = BTreeSet::new();
        for turn in &self.turns {
            for client in turn
                .items
                .iter()
                .filter_map(CodexHistoryItem::client_message_id)
            {
                if client.is_empty() || client.trim() != client || !generations.insert(client) {
                    return Err(KitError::invalid_state(
                        "prompt history has ambiguous generation identity",
                    ));
                }
            }
        }
        if self
            .turns
            .iter()
            .filter(|turn| matches!(turn.status, CodexPromptTurnStatus::InProgress))
            .count()
            > 1
        {
            return Err(KitError::invalid_state(
                "conversation has multiple active prompt executions",
            ));
        }
        let target = self
            .turns
            .iter()
            .find(|turn| turn.id == terminal.turn_id())
            .ok_or_else(|| KitError::invalid_state("terminal has no native history"))?;
        target.generation()?;
        if !terminal.matches_history(
            target.status,
            target.error.as_ref().map(CodexTurnError::to_error).as_ref(),
        ) {
            return Err(KitError::invalid_state(
                "historical terminal disagrees with native history",
            ));
        }
        Ok(())
    }

    pub(crate) fn current_generation(
        &self,
        terminal: &CodexPromptTerminal,
        current: Option<&CodexPromptRecord>,
    ) -> Result<Option<String>, KitError> {
        let target_index = self
            .turns
            .iter()
            .position(|turn| turn.id == terminal.turn_id())
            .ok_or_else(|| KitError::invalid_state("terminal has no native history"))?;
        let target = &self.turns[target_index];
        let target_generation = target.generation()?;
        let active = self
            .turns
            .iter()
            .find(|turn| matches!(turn.status, CodexPromptTurnStatus::InProgress));
        if let Some(current) = current {
            if current.client_message_id() == target_generation {
                if current.turn_id().is_some_and(|owned| owned != target.id) {
                    return Err(KitError::conflict("terminal contradicts its accepted turn"));
                }
                if active.is_some_and(|turn| turn.id != target.id) {
                    return Err(KitError::invalid_state(
                        "prompt history contains unrelated active work",
                    ));
                }
                return Ok(Some(target_generation.to_owned()));
            }
            let current_index = self.turns.iter().position(|turn| {
                turn.items
                    .iter()
                    .any(|item| item.client_message_id() == Some(current.client_message_id()))
            });
            if let Some(index) = current_index
                && self.turns[index].generation()? != current.client_message_id()
            {
                return Err(KitError::invalid_state(
                    "current prompt has ambiguous native generation identity",
                ));
            }
            if let Some(owned) = current.turn_id() {
                let index = current_index.ok_or_else(|| {
                    KitError::invalid_state("current prompt is absent from native history")
                })?;
                if self.turns[index].id != owned || target_index >= index {
                    return Err(KitError::invalid_state(
                        "terminal contradicts current generation ordering",
                    ));
                }
            } else if current_index.is_some_and(|index| target_index >= index) {
                return Err(KitError::invalid_state(
                    "terminal follows its current generation",
                ));
            }
            if active.is_some_and(|turn| {
                current_index.is_none_or(|index| self.turns[index].id != turn.id)
            }) {
                return Err(KitError::invalid_state(
                    "prompt history contains unrelated active work",
                ));
            }
        } else if active.is_some() {
            return Err(KitError::invalid_state(
                "prompt history is active without its durable owner",
            ));
        }
        Ok(None)
    }
}

impl CodexPromptThreadTurn {
    fn generation(&self) -> Result<&str, KitError> {
        let mut users = self.items.iter().filter_map(|item| match item {
            CodexHistoryItem::Message(CodexPromptThreadItem::UserMessage { client_id }) => {
                Some(client_id.as_deref())
            }
            CodexHistoryItem::Auxiliary(_)
            | CodexHistoryItem::Message(
                CodexPromptThreadItem::AgentMessage(_) | CodexPromptThreadItem::Other,
            ) => None,
        });
        let generation = users
            .next()
            .flatten()
            .ok_or_else(|| KitError::invalid_state("terminal lacks native generation identity"))?;
        if users.next().is_some() {
            return Err(KitError::invalid_state(
                "terminal has multiple native generations",
            ));
        }
        Ok(generation)
    }

    fn has_consistent_error_presence(&self) -> bool {
        match self.status {
            CodexPromptTurnStatus::Failed => self.error.is_some(),
            CodexPromptTurnStatus::Interrupted => true,
            CodexPromptTurnStatus::Completed | CodexPromptTurnStatus::InProgress => {
                self.error.is_none()
            }
        }
    }
}

#[derive(Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum CodexPromptTurnStatus {
    Completed,
    Interrupted,
    Failed,
    InProgress,
}

enum CodexHistoryItem {
    Auxiliary(items::CodexAuxiliaryItem),
    Message(CodexPromptThreadItem),
}

impl<'de> serde::Deserialize<'de> for CodexHistoryItem {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = <Value as serde::Deserialize>::deserialize(deserializer)?;
        match value.get("type").and_then(Value::as_str) {
            Some(
                "functionCallOutput"
                | "dynamicToolCall"
                | "collabAgentToolCall"
                | "subAgentActivity",
            ) => serde_json::from_value(value)
                .map(Self::Auxiliary)
                .map_err(serde::de::Error::custom),
            _ => serde_json::from_value(value)
                .map(Self::Message)
                .map_err(serde::de::Error::custom),
        }
    }
}

impl CodexHistoryItem {
    fn client_message_id(&self) -> Option<&str> {
        match self {
            Self::Auxiliary(_) => None,
            Self::Message(item) => item.client_message_id(),
        }
    }
    fn eligible_output(&self) -> Option<&str> {
        match self {
            Self::Auxiliary(_) => None,
            Self::Message(item) => item.eligible_output(),
        }
    }
    fn validate_support(&self) -> Result<(), KitError> {
        match self {
            Self::Auxiliary(item) => item.validate_support(),
            Self::Message(CodexPromptThreadItem::AgentMessage(message)) => {
                message.validate_support()
            }
            Self::Message(
                CodexPromptThreadItem::UserMessage { .. } | CodexPromptThreadItem::Other,
            ) => Ok(()),
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum CodexPromptThreadItem {
    UserMessage {
        #[serde(rename = "clientId", default)]
        client_id: Option<String>,
    },
    AgentMessage(notifications::CodexAgentMessage),
    #[serde(other)]
    Other,
}

#[derive(Clone, Copy, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
enum CodexPromptItemsView {
    NotLoaded,
    Summary,
    Full,
}

impl CodexPromptThreadItem {
    fn client_message_id(&self) -> Option<&str> {
        match self {
            Self::UserMessage { client_id } => client_id.as_deref(),
            Self::AgentMessage(_) | Self::Other => None,
        }
    }

    fn eligible_output(&self) -> Option<&str> {
        match self {
            Self::AgentMessage(message)
                if message.is_eligible_output() && !message.text().trim().is_empty() =>
            {
                Some(message.text())
            }
            Self::UserMessage { .. } | Self::AgentMessage(_) | Self::Other => None,
        }
    }
}

impl CodexProtocol {
    pub(crate) async fn connect(
        config: &CodexConfig,
    ) -> Result<(Self, CodexInboundReceiver), KitError> {
        config
            .validate()
            .map_err(|error| Self::caller_safe_error(&error))?;
        let request_timeout = config.mode().request_timeout();
        let (runtime, initialization_timeout) = match config.mode() {
            CodexMode::Spawn {
                startup_timeout, ..
            } => (
                stdio::CodexStdio::spawn()
                    .map_err(|error| error.in_phase(ErrorPhase::ProcessStart))?,
                *startup_timeout,
            ),
            CodexMode::Tcp {
                address,
                connect_timeout,
                ..
            } => (
                tcp::CodexTcp::connect(*address, *connect_timeout)
                    .await
                    .map_err(|error| error.in_phase(ErrorPhase::ClientSetup))?,
                request_timeout,
            ),
            CodexMode::Unix {
                path,
                connect_timeout,
                ..
            } => (
                unix::CodexUnix::connect(path, *connect_timeout)
                    .await
                    .map_err(|error| error.in_phase(ErrorPhase::ClientSetup))?,
                request_timeout,
            ),
        };

        let (rpc, frames) = CodexRpc::new(runtime, request_timeout);
        let protocol = Self {
            rpc,
            loaded_threads: Arc::new(Mutex::new(BTreeSet::new())),
            reconciliation_timeout: request_timeout,
        };
        if let Err(error) = protocol.initialize(initialization_timeout).await {
            let error = match protocol.shutdown(initialization_timeout).await {
                Ok(()) => error,
                Err(cleanup) => error.with_supplementary(cleanup.in_phase(ErrorPhase::Cleanup)),
            };
            return Err(error);
        }
        Ok((protocol, CodexInboundReceiver { frames }))
    }

    pub(crate) async fn shutdown(self, timeout: Duration) -> Result<(), KitError> {
        self.rpc
            .shutdown(timeout)
            .await
            .map_err(|error| Self::caller_safe_error(&error))
    }

    pub(crate) async fn respond(&self, response: CodexServerResponse) -> Result<(), KitError> {
        self.rpc
            .respond(response.into_value())
            .await
            .map_err(|error| Self::caller_safe_error(&error))
    }

    async fn request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, KitError> {
        self.rpc
            .request(method, params)
            .await
            .map_err(|error| Self::caller_safe_error(&error))
    }

    pub(crate) fn caller_safe_request_error(error: CodexRequestError) -> KitError {
        Self::caller_safe_error(&error.into_sdk_error())
    }

    pub(crate) fn caller_safe_error(error: &KitError) -> KitError {
        error.clone()
    }

    pub(crate) async fn start_turn(
        &self,
        request: CodexTurnStartRequest,
    ) -> Result<CodexTurnStart, CodexRequestError> {
        let response = self
            .rpc
            .request_with_acceptance("turn/start", request.into_params())
            .await?;
        CodexTurnStart::decode(response).map_err(|error| {
            CodexRequestError::Ambiguous(error.in_phase(ErrorPhase::ResponseDecode))
        })
    }

    pub(crate) async fn interrupt_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<(), CodexRequestError> {
        let response = self
            .rpc
            .request_with_acceptance(
                "turn/interrupt",
                serde_json::json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                }),
            )
            .await?;
        if response.as_object().is_none_or(|object| !object.is_empty()) {
            return Err(CodexRequestError::Ambiguous(KitError::protocol(
                "prompt cancellation response is malformed",
            )));
        }
        Ok(())
    }

    pub(crate) async fn list_models(&self) -> Result<Vec<ModelInfo>, KitError> {
        self.model_inventory().await.map(|entries| {
            entries
                .into_iter()
                .map(CodexModelInventoryEntry::into_info)
                .collect()
        })
    }

    async fn model_inventory(&self) -> Result<Vec<CodexModelInventoryEntry>, KitError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut seen_models = BTreeSet::new();
        let mut models = Vec::new();
        let mut defaults = 0_usize;
        for _ in 0..MAX_MODEL_PAGES {
            let page = CodexModelListResponse::decode(
                self.request(
                    "model/list",
                    serde_json::json!({
                        "cursor": cursor,
                        "limit": PROTOCOL_PAGE_LIMIT,
                        "includeHidden": false,
                    }),
                )
                .await?,
            )?;
            let (entries, next_cursor) = page.into_page()?;
            for entry in entries {
                if !seen_models.insert(entry.selector().clone()) {
                    return Err(KitError::protocol("model inventory repeats a selector"));
                }
                defaults += usize::from(entry.is_default());
                models.push(entry);
            }
            match next_cursor {
                Some(next) => {
                    cursor = Some(Self::checked_cursor(&mut seen_cursors, next)?);
                }
                None => {
                    if defaults > 1 {
                        return Err(KitError::protocol(
                            "model inventory declares multiple defaults",
                        ));
                    }
                    models.sort_by(|left, right| left.selector().cmp(right.selector()));
                    return Ok(models);
                }
            }
        }
        Err(KitError::protocol(
            "model inventory exceeded its pagination bound",
        ))
    }

    pub(crate) async fn resolve_prompt_selection(
        &self,
        model: Option<&ModelSelector>,
        reasoning_effort: Option<&ReasoningEffort>,
        personality: Option<&PersonalitySelector>,
        previous: CodexNativeSelection,
    ) -> Result<CodexPromptSelection, KitError> {
        CodexPromptSelection::validate_personality(personality)?;
        CodexPromptSelection::validate_personality(previous.personality())?;
        if model.is_none()
            && reasoning_effort.is_none()
            && personality.is_none()
            && previous.is_empty()
        {
            return Ok(CodexPromptSelection::new(
                None,
                None,
                previous.clone(),
                previous,
            ));
        }
        let models = self.model_inventory().await?;
        let default = models.iter().find(|candidate| candidate.is_default());
        let selected = match model.or(previous.model()) {
            Some(selector) if model.is_some() => models
                .iter()
                .find(|candidate| candidate.selector() == selector)
                .ok_or_else(|| KitError::invalid_input("model selector is not selectable"))?,
            Some(_) | None => default.ok_or_else(|| {
                KitError::unsupported(
                    "cannot restore or validate an omitted model without one sourced default",
                )
            })?,
        };
        if let Some(reasoning_effort) = reasoning_effort
            && !selected
                .info()
                .reasoning_efforts()
                .contains(reasoning_effort)
        {
            return Err(KitError::invalid_input(
                "model does not advertise the requested reasoning effort",
            ));
        }
        let desired_model = match model {
            Some(model) => Some(model.clone()),
            None if previous.model().is_some() => Some(
                default
                    .ok_or_else(|| {
                        KitError::unsupported(
                            "cannot restore an omitted model without one sourced default",
                        )
                    })?
                    .selector()
                    .clone(),
            ),
            None => None,
        };
        let wire_model = desired_model
            .as_ref()
            .filter(|desired| previous.model() != Some(*desired))
            .cloned();

        let desired_effort = match reasoning_effort {
            Some(effort) => Some(effort.clone()),
            None if previous.reasoning_effort().is_some() => {
                Some(selected.default_reasoning_effort().clone())
            }
            None => None,
        };
        let wire_effort = desired_effort
            .as_ref()
            .filter(|desired| previous.reasoning_effort() != Some(*desired))
            .cloned();
        let accepted = CodexNativeSelection::from_parts(
            desired_model.or_else(|| previous.model().cloned()),
            desired_effort.or_else(|| previous.reasoning_effort().cloned()),
            None,
        );
        Ok(CodexPromptSelection::new(
            wire_model,
            wire_effort,
            previous,
            accepted,
        ))
    }

    pub(crate) async fn read_file(&self, request: CodexFileRead) -> Result<FileRead, KitError> {
        let params = request
            .params()
            .map_err(|error| Self::caller_safe_error(&error))?;
        let response = match self
            .rpc
            .request_with_acceptance("fs/readFile", params)
            .await
        {
            Ok(response) => response,
            Err(error) => return Err(Self::caller_safe_request_error(error)),
        };
        request
            .decode(response)
            .map_err(|error| error.in_phase(ErrorPhase::ResponseDecode))
    }

    pub(crate) async fn parent_thread_id(
        &self,
        thread_id: &str,
    ) -> Result<Option<String>, KitError> {
        let response = self
            .request(
                "thread/read",
                serde_json::json!({
                    "threadId": thread_id,
                    "includeTurns": false,
                }),
            )
            .await?;
        let thread = response
            .get("thread")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| KitError::protocol("conversation reload omitted conversation"))?;
        let actual = thread
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|actual| !actual.is_empty() && actual.trim() == *actual)
            .ok_or_else(|| KitError::protocol("conversation reload returned a malformed id"))?;
        if actual != thread_id {
            return Err(KitError::invalid_state(
                "conversation reload returned a different conversation",
            ));
        }
        match thread.get("parentThreadId") {
            None => Err(KitError::protocol(
                "conversation reload omitted its parent identity",
            )),
            Some(serde_json::Value::Null) => Ok(None),
            Some(value) => value
                .as_str()
                .filter(|parent| !parent.is_empty() && parent.trim() == *parent)
                .map(|parent| Some(parent.to_owned()))
                .ok_or_else(|| {
                    KitError::protocol("conversation reload returned a malformed parent")
                }),
        }
    }

    async fn prompt_history(&self, thread_id: &str) -> Result<CodexPromptThread, KitError> {
        let response: CodexPromptThreadRead = serde_json::from_value(
            self.request(
                "thread/read",
                serde_json::json!({
                    "threadId": thread_id,
                    "includeTurns": true,
                }),
            )
            .await?,
        )
        .map_err(|_| KitError::protocol("conversation reload prompt history is malformed"))?;
        if response.thread.id != thread_id {
            return Err(KitError::invalid_state(
                "prompt history belongs to another conversation",
            ));
        }
        if response
            .thread
            .turns
            .iter()
            .any(|turn| !turn.has_consistent_error_presence())
        {
            return Err(KitError::protocol(
                "prompt history contains inconsistent prompt execution error state",
            ));
        }
        for turn in &response.thread.turns {
            for item in &turn.items {
                item.validate_support()?;
            }
            if let Some(error) = &turn.error {
                error.validate()?;
            }
        }
        let mut turn_ids = BTreeSet::new();
        if response.thread.turns.iter().any(|turn| {
            turn.id.is_empty() || turn.id.trim() != turn.id || !turn_ids.insert(turn.id.clone())
        }) {
            return Err(KitError::protocol(
                "prompt history contains malformed prompt execution identities",
            ));
        }
        Ok(response.thread)
    }

    pub(crate) async fn prompt_turn(
        &self,
        thread_id: &str,
        client_message_id: Option<&str>,
        turn_id: Option<&str>,
    ) -> Result<CodexPromptTurn, KitError> {
        if turn_id.is_some() && client_message_id.is_none() {
            return Err(KitError::invalid_state(
                "prompt reconciliation lacks its generation identity",
            ));
        }
        let history = self.prompt_history(thread_id).await?;
        let mut active_turns = history
            .turns
            .iter()
            .filter(|turn| matches!(turn.status, CodexPromptTurnStatus::InProgress));
        let active = active_turns.next();
        if active_turns.next().is_some() {
            return Err(KitError::invalid_state(
                "conversation has multiple active prompt executions",
            ));
        }
        if client_message_id.is_none() {
            return match active {
                Some(turn) => Ok(CodexPromptTurn::Active {
                    turn_id: turn.id.clone(),
                }),
                None => Ok(CodexPromptTurn::Absent),
            };
        }
        let client_message_id = client_message_id.ok_or_else(|| {
            KitError::invalid_state("prompt reconciliation lacks its generation identity")
        })?;
        let mut matching = history.turns.iter().flat_map(|turn| {
            turn.items
                .iter()
                .enumerate()
                .filter_map(move |(index, item)| {
                    (item.client_message_id() == Some(client_message_id)).then_some((turn, index))
                })
        });
        let matched = matching.next();
        if matching.next().is_some() {
            return Err(KitError::invalid_state(
                "prompt identity matches multiple runtime executions",
            ));
        }
        let Some((matched, user_index)) = matched else {
            if active.is_some() {
                return Err(KitError::invalid_state(
                    "active prompt execution belongs to another prompt generation",
                ));
            }
            return Ok(CodexPromptTurn::Absent);
        };
        if turn_id.is_some_and(|expected| matched.id != expected) {
            return Err(KitError::conflict(
                "prompt history contradicts its accepted prompt execution identity",
            ));
        }
        if active.is_some_and(|active| active.id != matched.id) {
            return Err(KitError::invalid_state(
                "prompt history contains unrelated active work",
            ));
        }
        match matched.status {
            CodexPromptTurnStatus::InProgress => Ok(CodexPromptTurn::Active {
                turn_id: matched.id.clone(),
            }),
            status @ (CodexPromptTurnStatus::Completed
            | CodexPromptTurnStatus::Interrupted
            | CodexPromptTurnStatus::Failed) => {
                if matched.items_view != CodexPromptItemsView::Full {
                    return Err(KitError::invalid_state(
                        "prompt history is incomplete during reconciliation",
                    ));
                }
                for item in &matched.items[user_index + 1..] {
                    item.validate_support()?;
                }
                let output = matched.items[user_index + 1..]
                    .iter()
                    .rev()
                    .find_map(CodexHistoryItem::eligible_output)
                    .map(str::to_owned);
                Ok(CodexPromptTurn::Terminal {
                    turn_id: matched.id.clone(),
                    status,
                    output,
                    error: matched.error.as_ref().map(CodexTurnError::to_error),
                })
            }
        }
    }

    pub(crate) async fn terminal_history(
        &self,
        thread_id: &str,
        terminal: &CodexPromptTerminal,
    ) -> Result<CodexPromptThread, KitError> {
        if terminal.thread_id() != thread_id {
            return Err(KitError::invalid_state(
                "terminal belongs to another bound thread",
            ));
        }
        let history = self.prompt_history(thread_id).await?;
        history.validate_terminal(terminal)?;
        Ok(history)
    }

    pub(crate) async fn reconcile_prompt_start(
        &self,
        thread_id: &str,
        client_message_id: &str,
    ) -> Result<CodexPromptTurn, KitError> {
        let deadline = tokio::time::Instant::now() + self.reconciliation_timeout;
        loop {
            let observed = tokio::time::timeout_at(
                deadline,
                self.prompt_turn(thread_id, Some(client_message_id), None),
            )
            .await
            .map_err(|_| KitError::timeout("prompt acceptance reconciliation elapsed"))??;
            if !matches!(observed, CodexPromptTurn::Absent) {
                return Ok(observed);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(CodexPromptTurn::Absent);
            }
            tokio::time::sleep(
                std::time::Duration::from_millis(25).min(deadline.saturating_duration_since(now)),
            )
            .await;
        }
    }

    pub(crate) async fn start_thread(
        &self,
        policy: &CodexWorkspacePolicy,
        instructions: Option<&str>,
        thread_source: &str,
        dynamic_tools: Vec<serde_json::Value>,
    ) -> Result<CodexThreadAcquisition, CodexRequestError> {
        self.validate_workspace_policy(policy)
            .await
            .map_err(CodexRequestError::Rejected)?;
        let request = CodexThreadCreateRequest::new(
            policy.wire_policy().map_err(CodexRequestError::Rejected)?,
            instructions,
            thread_source,
            dynamic_tools,
        )
        .map_err(CodexRequestError::Rejected)?;
        let thread = CodexThread::from_start(
            self.rpc
                .request_with_acceptance("thread/start", request.into_params())
                .await?,
        )
        .map_err(CodexRequestError::Ambiguous)?;
        thread
            .require_thread_source(thread_source)
            .map_err(CodexRequestError::Ambiguous)?;
        let read = self
            .read_thread_exact(thread.id())
            .await
            .map_err(CodexRequestError::Ambiguous)?;
        read.require_thread_source(thread_source)
            .map_err(CodexRequestError::Ambiguous)?;
        if !thread.has_same_durable_binding(&read) {
            return Err(CodexRequestError::Ambiguous(KitError::invalid_state(
                "conversation creation and reload disagree about durable state",
            )));
        }
        self.loaded_threads().insert(thread.id().to_owned());
        Ok(CodexThreadAcquisition::Response(thread))
    }

    pub(crate) async fn recover_thread_creation(
        &self,
        thread_source: &str,
        observed_thread_id: Option<&str>,
        observation: impl std::future::Future<Output = Result<String, KitError>>,
    ) -> Result<CodexThreadAcquisition, KitError> {
        if observed_thread_id.is_some() {
            return self
                .recover_thread_creation_inventory(thread_source, observed_thread_id)
                .await;
        }
        let inventory = self.recover_thread_creation_inventory(thread_source, None);
        tokio::pin!(inventory);
        tokio::pin!(observation);
        tokio::select! {
            biased;
            observed = &mut observation => {
                let observed = observed?;
                self.recover_thread_creation_inventory(thread_source, Some(&observed)).await
            }
            recovered = &mut inventory => match recovered {
                Ok(recovered) => Ok(recovered),
                Err(error) if matches!(error.kind(), ErrorKind::Busy | ErrorKind::Timeout) => {
                    let observed = (&mut observation).await?;
                    self.recover_thread_creation_inventory(thread_source, Some(&observed))
                        .await
                }
                Err(error) => Err(error),
            },
        }
    }

    pub(crate) fn rejected_before_dispatch(&self, error: &CodexRequestError) -> bool {
        error.is_rejected() && matches!(error, CodexRequestError::Rejected(_))
    }

    async fn recover_thread_creation_inventory(
        &self,
        thread_source: &str,
        observed_thread_id: Option<&str>,
    ) -> Result<CodexThreadAcquisition, KitError> {
        let deadline = tokio::time::Instant::now() + self.reconciliation_timeout;
        loop {
            let mut matching = BTreeSet::new();
            if let Some(thread_id) = observed_thread_id {
                if thread_id.is_empty() || thread_id.trim() != thread_id {
                    return Err(KitError::invalid_state(
                        "observed conversation creation state is malformed",
                    ));
                }
                matching.insert(thread_id.to_owned());
            }
            let mut seen_threads = BTreeMap::new();
            for archived in [false, true] {
                for scope in [
                    CodexThreadInventoryScope::DefaultInteractive,
                    CodexThreadInventoryScope::ExplicitKinds,
                ] {
                    let mut cursor = None;
                    let mut seen_cursors = BTreeSet::new();
                    let mut scoped_threads = BTreeSet::new();
                    let mut complete = false;
                    for _ in 0..MAX_PROTOCOL_PAGES {
                        if tokio::time::Instant::now() >= deadline && matching.is_empty() {
                            return Err(KitError::busy(
                                "conversation creation remains durably unresolved",
                            ));
                        }
                        let response = tokio::time::timeout_at(
                            deadline,
                            self.request(
                                "thread/list",
                                scope.request_params(cursor.as_deref(), archived),
                            ),
                        )
                        .await
                        .map_err(|_| {
                            KitError::timeout("conversation creation reconciliation elapsed")
                        })??;
                        let page = CodexThreadList::decode(response)?;
                        let (entries, next) = page.into_parts();
                        for entry in entries {
                            let id = entry.id().to_owned();
                            if !scoped_threads.insert(id.clone()) {
                                return Err(KitError::protocol(
                                    "conversation creation inventory contains duplicate entries",
                                ));
                            }
                            if seen_threads
                                .insert(id.clone(), archived)
                                .is_some_and(|previous| previous != archived)
                            {
                                return Err(KitError::protocol(
                                    "conversation creation inventory contradicts archive state",
                                ));
                            }
                            if entry.has_thread_source(thread_source) {
                                matching.insert(id);
                            }
                        }
                        let Some(next) = next else {
                            complete = true;
                            break;
                        };
                        cursor = Some(Self::checked_cursor(&mut seen_cursors, next)?);
                    }
                    if !complete {
                        return Err(KitError::protocol(
                            "conversation creation inventory exceeds its pagination bound",
                        ));
                    }
                }
            }
            match matching.len() {
                0 if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(
                        std::time::Duration::from_millis(25)
                            .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
                    )
                    .await;
                }
                0 => {
                    return Err(KitError::busy(
                        "conversation creation remains durably unresolved",
                    ));
                }
                1 => {
                    let id = matching.into_iter().next().ok_or_else(|| {
                        KitError::invalid_state("conversation creation match disappeared")
                    })?;
                    let read = tokio::time::timeout_at(deadline, self.read_thread_exact(&id))
                        .await
                        .map_err(|_| {
                            KitError::timeout("conversation creation reconciliation elapsed")
                        })??;
                    read.require_thread_source(thread_source)?;
                    self.loaded_threads().insert(id);
                    return Ok(CodexThreadAcquisition::Correlated(read));
                }
                _ => {
                    return Err(KitError::invalid_state(
                        "conversation creation state matched multiple runtime conversations",
                    ));
                }
            }
        }
    }

    async fn read_thread_exact(&self, thread_id: &str) -> Result<CodexThread, KitError> {
        let thread = CodexThread::from_read(
            self.request(
                "thread/read",
                serde_json::json!({
                    "threadId": thread_id,
                    "includeTurns": false,
                }),
            )
            .await?,
        )?;
        if thread.id() != thread_id {
            return Err(KitError::invalid_state(
                "conversation reload returned a different conversation",
            ));
        }
        Ok(thread)
    }

    pub(crate) async fn validate_workspace_policy(
        &self,
        policy: &CodexWorkspacePolicy,
    ) -> Result<(), KitError> {
        if policy.requires_empty_mcp() {
            self.require_empty_mcp(None).await?;
        }
        Ok(())
    }

    async fn verify_created_thread(
        &self,
        thread: &CodexThread,
        policy: &CodexWorkspacePolicy,
    ) -> Result<(), KitError> {
        policy.verify_materialized(thread)?;
        if policy.requires_empty_mcp() {
            self.require_empty_mcp(Some(thread.id())).await?;
        }
        self.verify_foreground_features(thread.id()).await
    }

    fn verify_correlated_created_thread(
        &self,
        thread: &CodexThread,
        policy: &CodexWorkspacePolicy,
    ) -> Result<(), KitError> {
        policy.verify_correlated_creation(thread)?;
        Err(KitError::unsupported(
            "workspace execution policy cannot be verified after response loss",
        ))
    }

    pub(crate) async fn materialize_thread(
        &self,
        thread: &CodexThread,
        name: &str,
        thread_source: &str,
    ) -> Result<(), KitError> {
        let response = self
            .request(
                "thread/name/set",
                serde_json::json!({
                    "threadId": thread.id(),
                    "name": name,
                }),
            )
            .await?;
        if response.as_object().is_none_or(|object| !object.is_empty()) {
            return Err(KitError::protocol(
                "conversation naming response is malformed",
            ));
        }
        // Verify the named thread's exact durable binding and empty full history
        // before publishing the portable conversation. The read is not a flush API.
        let response = self
            .request(
                "thread/read",
                serde_json::json!({ "threadId": thread.id(), "includeTurns": true }),
            )
            .await?;
        let materialized = CodexThread::from_read(response.clone())?;
        materialized.require_thread_source(thread_source)?;
        if !thread.has_same_durable_binding(&materialized) {
            return Err(KitError::invalid_state(
                "materialized conversation contradicts its creation binding",
            ));
        }
        let history: CodexPromptThreadRead = serde_json::from_value(response)
            .map_err(|_| KitError::protocol("new conversation history is malformed"))?;
        if !history.thread.turns.is_empty() {
            return Err(KitError::invalid_state(
                "new conversation already contains prompt history",
            ));
        }
        Ok(())
    }

    pub(crate) async fn verify_thread(
        &self,
        thread_id: &str,
        policy: &CodexWorkspacePolicy,
        instructions: Option<&str>,
    ) -> Result<CodexThread, KitError> {
        if policy.requires_empty_mcp() {
            self.require_empty_mcp(None).await?;
        }
        let request =
            CodexThreadResumeRequest::new(thread_id, policy.wire_policy()?, instructions)?;
        let params = request.into_params();
        let deadline = tokio::time::Instant::now() + self.reconciliation_timeout;
        let response = loop {
            match self
                .rpc
                .request_with_acceptance("thread/resume", params.clone())
                .await
            {
                Ok(response) => break response,
                Err(error)
                    if Self::is_thread_closing(&error, thread_id)
                        && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(error) => return Err(Self::caller_safe_request_error(error)),
            }
        };
        let resumed = CodexThread::from_resume(response)?;
        if resumed.id() != thread_id {
            return Err(KitError::invalid_state(
                "resumed conversation contradicts its durable binding",
            ));
        }
        policy.verify_materialized(&resumed)?;
        let read = CodexThread::from_read(
            self.request(
                "thread/read",
                serde_json::json!({
                    "threadId": thread_id,
                    "includeTurns": false,
                }),
            )
            .await?,
        )?;
        if !resumed.has_same_durable_binding(&read) || read.id() != thread_id {
            return Err(KitError::invalid_state(
                "read and resume disagree about the bound conversation",
            ));
        }
        if policy.requires_empty_mcp() {
            self.require_empty_mcp(Some(thread_id)).await?;
        }
        self.verify_foreground_features(thread_id).await?;
        self.loaded_threads().insert(thread_id.to_owned());
        Ok(resumed)
    }

    fn is_thread_closing(error: &CodexRequestError, thread_id: &str) -> bool {
        matches!(
            error,
            CodexRequestError::ServerRejected { message, .. }
                if message
                    == &format!(
                        "thread {thread_id} is closing; retry thread/resume after the thread is closed"
                    )
        )
    }

    pub(crate) async fn rebind_thread(
        &self,
        thread_id: &str,
        policy: &CodexWorkspacePolicy,
        instructions: Option<&str>,
    ) -> Result<CodexThread, KitError> {
        let response = self
            .request(
                "thread/unsubscribe",
                serde_json::json!({ "threadId": thread_id }),
            )
            .await?;
        let status = response
            .get("status")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| KitError::protocol("conversation release response is malformed"))?;
        if !matches!(status, "notLoaded" | "notSubscribed" | "unsubscribed") {
            return Err(KitError::protocol(
                "conversation release returned an unknown status",
            ));
        }
        self.verify_thread(thread_id, policy, instructions).await
    }

    pub(crate) async fn cleanup_preserved_thread(
        &self,
        root_thread_id: &str,
    ) -> Result<(), KitError> {
        let mut ids = self.descendant_threads(root_thread_id).await?;
        ids.push(root_thread_id.to_owned());
        ids.sort();
        ids.dedup();
        for id in &ids {
            self.request(
                "thread/backgroundTerminals/clean",
                serde_json::json!({ "threadId": id }),
            )
            .await?;
        }
        for id in &ids {
            self.require_no_background_terminals(id).await?;
        }
        for id in ids {
            let response = self
                .request("thread/unsubscribe", serde_json::json!({ "threadId": id }))
                .await?;
            let status = response
                .get("status")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| KitError::protocol("conversation release response is malformed"))?;
            if !matches!(status, "notLoaded" | "notSubscribed" | "unsubscribed") {
                return Err(KitError::protocol(
                    "conversation release returned an unknown status",
                ));
            }
            self.loaded_threads().remove(&id);
        }
        Ok(())
    }

    pub(crate) async fn delete_thread(&self, thread_id: &str) -> Result<(), KitError> {
        let response = match self
            .rpc
            .request_with_acceptance(
                "thread/delete",
                serde_json::json!({ "threadId": thread_id }),
            )
            .await
        {
            Ok(response) => response,
            Err(CodexRequestError::ServerRejected { message, .. })
                if message == format!("thread not found: {thread_id}") =>
            {
                self.loaded_threads().remove(thread_id);
                return Ok(());
            }
            Err(error) => return Err(Self::caller_safe_request_error(error)),
        };
        if response.as_object().is_none_or(|object| !object.is_empty()) {
            return Err(KitError::protocol(
                "conversation deletion response is malformed",
            ));
        }
        self.loaded_threads().remove(thread_id);
        Ok(())
    }

    pub(crate) async fn settle_cleanup(&self, cleanup: &CodexCleanup) -> Result<(), KitError> {
        for thread_id in cleanup.thread_ids() {
            match cleanup.action() {
                CodexCleanupAction::Delete => {
                    self.delete_thread(thread_id).await?;
                }
                CodexCleanupAction::Detach => {
                    if !self.is_thread_loaded(thread_id) {
                        continue;
                    }
                    self.cleanup_preserved_thread(thread_id).await?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn is_thread_loaded(&self, thread_id: &str) -> bool {
        self.loaded_threads().contains(thread_id)
    }

    async fn require_empty_mcp(&self, thread_id: Option<&str>) -> Result<(), KitError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        for _ in 0..MAX_PROTOCOL_PAGES {
            let page = CodexMcpStatusList::decode(
                self.request(
                    "mcpServerStatus/list",
                    serde_json::json!({
                        "cursor": cursor,
                        "limit": PROTOCOL_PAGE_LIMIT,
                        "detail": "toolsAndAuthOnly",
                        "threadId": thread_id,
                    }),
                )
                .await?,
            )?;
            let (empty, next) = page.into_parts();
            if !empty {
                return Err(KitError::unsupported(
                    "restricted policy cannot confine configured tool sources",
                ));
            }
            let Some(next) = next else {
                return Ok(());
            };
            cursor = Some(Self::checked_cursor(&mut seen_cursors, next)?);
        }
        Err(KitError::protocol(
            "configured tool-source inventory exceeds its pagination bound",
        ))
    }

    async fn verify_foreground_features(&self, thread_id: &str) -> Result<(), KitError> {
        let required = ["multi_agent", "multi_agent_v2"];
        let mut states = BTreeMap::new();
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        for _ in 0..MAX_PROTOCOL_PAGES {
            let page = CodexFeatureList::decode(
                self.request(
                    "experimentalFeature/list",
                    serde_json::json!({
                        "cursor": cursor,
                        "limit": PROTOCOL_PAGE_LIMIT,
                        "threadId": thread_id,
                    }),
                )
                .await?,
            )?;
            let (entries, next) = page.into_parts();
            for (name, enabled) in entries {
                if required.contains(&name.as_str()) && states.insert(name, enabled).is_some() {
                    return Err(KitError::protocol(
                        "workspace execution policy inventory contains a duplicate entry",
                    ));
                }
            }
            let Some(next) = next else {
                for name in required {
                    match states.get(name) {
                        Some(false) => {}
                        Some(true) => {
                            return Err(KitError::unsupported(
                                "background-capable workspace behavior must be disabled",
                            ));
                        }
                        None => {
                            return Err(KitError::protocol(
                                "workspace execution policy inventory omitted required effective state",
                            ));
                        }
                    }
                }
                return Ok(());
            };
            cursor = Some(Self::checked_cursor(&mut seen_cursors, next)?);
        }
        Err(KitError::protocol(
            "workspace execution policy inventory exceeds its pagination bound",
        ))
    }

    async fn descendant_threads(&self, root_thread_id: &str) -> Result<Vec<String>, KitError> {
        let mut ids = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        for _ in 0..MAX_PROTOCOL_PAGES {
            let page = CodexThreadList::decode(
                self.request(
                    "thread/list",
                    serde_json::json!({
                        "cursor": cursor,
                        "limit": PROTOCOL_PAGE_LIMIT,
                        "ancestorThreadId": root_thread_id,
                        "sourceKinds": THREAD_SOURCE_KINDS,
                    }),
                )
                .await?,
            )?;
            let (entries, next) = page.into_parts();
            ids.extend(entries.into_iter().map(|entry| entry.id().to_owned()));
            let Some(next) = next else {
                return Ok(ids);
            };
            cursor = Some(Self::checked_cursor(&mut seen_cursors, next)?);
        }
        Err(KitError::protocol(
            "conversation inventory exceeds its pagination bound",
        ))
    }

    async fn require_no_background_terminals(&self, thread_id: &str) -> Result<(), KitError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        for _ in 0..MAX_PROTOCOL_PAGES {
            let page = CodexTerminalList::decode(
                self.request(
                    "thread/backgroundTerminals/list",
                    serde_json::json!({
                        "threadId": thread_id,
                        "cursor": cursor,
                        "limit": PROTOCOL_PAGE_LIMIT,
                    }),
                )
                .await?,
            )?;
            let (empty, next) = page.into_parts();
            if !empty {
                return Err(KitError::busy("conversation retains background work"));
            }
            let Some(next) = next else {
                return Ok(());
            };
            cursor = Some(Self::checked_cursor(&mut seen_cursors, next)?);
        }
        Err(KitError::protocol(
            "background work inventory exceeds its pagination bound",
        ))
    }

    fn checked_cursor(seen: &mut BTreeSet<String>, cursor: String) -> Result<String, KitError> {
        if cursor.is_empty() || cursor.trim() != cursor {
            return Err(KitError::protocol(
                "runtime inventory returned a malformed pagination cursor",
            ));
        }
        if !seen.insert(cursor.clone()) {
            return Err(KitError::protocol(
                "runtime inventory repeats a pagination cursor",
            ));
        }
        Ok(cursor)
    }

    fn loaded_threads(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        self.loaded_threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    async fn initialize(&self, timeout: Duration) -> Result<(), KitError> {
        let response = tokio::time::timeout(
            timeout,
            self.request(
                "initialize",
                serde_json::json!({
                    "clientInfo": {
                        "name": "agents-kit",
                        "title": "agents-kit",
                        "version": AGENTS_KIT_CLIENT_VERSION,
                    },
                    "capabilities": {
                        "experimentalApi": true,
                        "requestAttestation": false,
                        "optOutNotificationMethods": [
                            "thread/name/updated",
                            "thread/status/changed",
                            "thread/closed",
                        ],
                    }
                }),
            ),
        )
        .await
        .map_err(|_| {
            KitError::timeout("initialization deadline elapsed")
                .at_phase(ErrorPhase::ServerVerification)
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Lifecycle,
                    timeout: Some(timeout),
                })
        })?
        .map_err(|error| {
            if error.kind() == ErrorKind::Timeout {
                // Either existing deadline may win while initialization is pending.
                // Keep that timer's evidence and identify the owning startup step.
                error.at_phase(ErrorPhase::ServerVerification)
            } else {
                error
            }
        })?;
        let user_agent = response
            .get("userAgent")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| KitError::protocol("initialization omitted runtime identity"))?;
        let reported = user_agent
            .split_once('/')
            .and_then(|(_, suffix)| suffix.split_whitespace().next())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| KitError::protocol("runtime identity version is malformed"))?;
        if !Self::version_is_valid(reported) {
            return Err(KitError::protocol("runtime identity version is malformed"));
        }
        if reported != SELECTED_CODEX_VERSION {
            return Err(KitError::invalid_config(
                "connected runtime version is incompatible",
            ));
        }
        self.rpc
            .notify("initialized")
            .await
            .map_err(|error| Self::caller_safe_error(&error))
    }

    fn version_is_valid(version: &str) -> bool {
        let (version, build) = match version.split_once('+') {
            Some((version, build)) => (version, Some(build)),
            None => (version, None),
        };
        if build.is_some_and(|build| {
            build.split('.').any(|identifier| {
                identifier.is_empty()
                    || !identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        }) {
            return false;
        }

        let (core, prerelease) = match version.split_once('-') {
            Some((core, prerelease)) => (core, Some(prerelease)),
            None => (version, None),
        };
        let mut components = core.split('.');
        for _ in 0..3 {
            let Some(component) = components.next() else {
                return false;
            };
            if component.is_empty()
                || !component.bytes().all(|byte| byte.is_ascii_digit())
                || (component.len() > 1 && component.starts_with('0'))
            {
                return false;
            }
        }
        if components.next().is_some() {
            return false;
        }

        !prerelease.is_some_and(|prerelease| {
            prerelease.split('.').any(|identifier| {
                identifier.is_empty()
                    || !identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    || (identifier.len() > 1
                        && identifier.bytes().all(|byte| byte.is_ascii_digit())
                        && identifier.starts_with('0'))
            })
        })
    }
}

impl CodexThreadAcquisition {
    pub(crate) fn thread(&self) -> &CodexThread {
        match self {
            Self::Response(thread) | Self::Correlated(thread) => thread,
        }
    }

    pub(crate) async fn verify(
        &self,
        protocol: &CodexProtocol,
        policy: &CodexWorkspacePolicy,
    ) -> Result<(), KitError> {
        match self {
            Self::Response(thread) => protocol.verify_created_thread(thread, policy).await,
            Self::Correlated(thread) => protocol.verify_correlated_created_thread(thread, policy),
        }
    }
}

impl CodexInboundReceiver {
    pub(crate) async fn recv(&mut self) -> Option<Result<CodexInbound, KitError>> {
        self.frames.recv().await.map(|frame| {
            frame
                .map_err(|error| CodexProtocol::caller_safe_error(&error))
                .and_then(Self::decode)
                .map_err(|error| error.in_phase(ErrorPhase::ResponseDecode))
        })
    }

    fn decode(frame: serde_json::Value) -> Result<CodexInbound, KitError> {
        let serde_json::Value::Object(mut frame) = frame else {
            return Err(KitError::protocol("runtime protocol data is malformed")
                .with_detail(ErrorDetail::MalformedData));
        };
        let method = frame
            .remove("method")
            .and_then(|method| method.as_str().map(str::to_owned))
            .ok_or_else(|| {
                KitError::protocol("runtime protocol data omitted its operation")
                    .with_detail(ErrorDetail::MissingData)
            })?;
        let params = frame.remove("params").unwrap_or(serde_json::Value::Null);
        let frame = match frame.remove("id") {
            Some(request_id) => CodexInboundFrame::request(request_id, method, params),
            None => CodexInboundFrame::notification(method, params),
        };
        CodexInbound::decode(frame)
    }
}
