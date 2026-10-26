use std::collections::BTreeSet;

use provider_sdk::generation::{JsonSchema, Message};
use storage_sdk::state::record::{StateKey, StateRecordSnapshot};

use crate::{
    AgentId, ConversationId, CustomLoopLimits, CustomToolApproval, SdkError,
    StructuredOutputRequest, ToolSelection,
};

use super::CustomTerminalAttempt;

const CUSTOM_ATTACHMENT_VERSION: u32 = 5;
const CUSTOM_CONTINUATION_VERSION: u32 = 6;
const CUSTOM_ATTACHMENT_NAMESPACE: &str = "agent_sdk_custom_attachments_v1";
const CUSTOM_CONTINUATION_NAMESPACE: &str = "agent_sdk_custom_continuations_v1";
const MAX_CONTINUATION_TURNS: usize = 32;
const MAX_CONTINUATION_BYTES: usize = 1 << 20;
const MAX_PROMPT_CONTRACT_BYTES: usize = 1 << 20;
const MAX_CATALOG_ENTRIES: usize = 4_096;
const MAX_CATALOG_BYTES: usize = 8 << 20;
const MAX_COMPATIBILITY_BYTES: usize = 16 << 20;

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomAttachment {
    version: u32,
    agent_id: AgentId,
    compatibility: CustomCompatibilityFloor,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomCompatibilityFloor {
    default_model: String,
    catalog: CustomCatalogFloor,
    tool_approval: CustomToolApproval,
    tool_selection: ToolSelection,
    limits: CustomLoopLimits,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomCatalogFloor {
    executable: Vec<CustomToolShape>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomToolShape {
    name: String,
    description: Option<String>,
    input_schema: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "phase", deny_unknown_fields)]
enum CustomConversationState {
    Provisioning,
    Ready,
    Active {
        generation: u64,
        accepted: bool,
        cancellation: Option<CustomCancellationIntent>,
        caller_text: String,
        contract: CustomPromptContract,
    },
    Sealed,
    Closing,
    ClosingActive {
        generation: u64,
        accepted: bool,
        cancellation: Option<CustomCancellationIntent>,
        caller_text: String,
        contract: CustomPromptContract,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CustomTurn {
    caller_text: String,
    assistant_text: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CustomCancellationIntent {
    Caller,
    Deadline,
    ConversationClose,
    WorkspaceClose,
    AgentShutdown,
    RuntimeFailure,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CustomPromptContract {
    structured_schema: Option<serde_json::Value>,
    tool_selection: ToolSelection,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "outcome", deny_unknown_fields)]
enum CustomTerminalState {
    Committed {
        generation: u64,
        accepted: bool,
        cancellation: Option<CustomCancellationIntent>,
        contract: CustomPromptContract,
        waiter: CustomWaiterState,
    },
    Failed {
        generation: u64,
        accepted: bool,
        cancellation: Option<CustomCancellationIntent>,
        contract: CustomPromptContract,
        waiter: CustomWaiterState,
    },
    Sealed {
        generation: u64,
        attempt: [u8; 16],
        accepted: bool,
        cancellation: Option<CustomCancellationIntent>,
        contract: CustomPromptContract,
        waiter: CustomWaiterState,
    },
    Interrupted {
        generation: u64,
        accepted: bool,
        cancellation: Option<CustomCancellationIntent>,
        contract: CustomPromptContract,
        waiter: CustomWaiterState,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum CustomWaiterState {
    Pending,
    Delivered,
    Discarded,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomContinuation {
    version: u32,
    conversation_id: ConversationId,
    next_generation: u64,
    state: CustomConversationState,
    turns: Vec<CustomTurn>,
    terminal: Option<CustomTerminalState>,
    lost_generations: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CustomGeneration {
    id: u64,
    turns: Vec<CustomTurn>,
    caller_text: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CustomContinuationDisposition {
    Ready,
    Sealed,
}

impl CustomCompatibilityFloor {
    pub(crate) fn new(
        default_model: String,
        catalog: CustomCatalogFloor,
        tool_approval: CustomToolApproval,
        tool_selection: &ToolSelection,
        limits: CustomLoopLimits,
    ) -> Result<Self, SdkError> {
        let floor = Self {
            default_model,
            catalog,
            tool_approval,
            tool_selection: tool_selection.clone(),
            limits,
        };
        floor.validate()?;
        Ok(floor)
    }

    fn validate(&self) -> Result<(), SdkError> {
        if self.default_model.is_empty() || self.default_model.trim() != self.default_model {
            return Err(SdkError::invalid_state(
                "stored agent configuration identity is malformed",
            ));
        }
        self.catalog.validate()?;
        self.tool_selection.validate()?;
        self.limits.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|_| SdkError::invalid_state("stored agent configuration is malformed"))?;
        if bytes.len() > MAX_COMPATIBILITY_BYTES {
            return Err(SdkError::invalid_state(
                "stored agent configuration exceeds the supported limit",
            ));
        }
        Ok(())
    }
}

impl CustomPromptContract {
    fn from_request(
        structured: Option<&StructuredOutputRequest>,
        tool_selection: &ToolSelection,
    ) -> Result<Self, SdkError> {
        if let Some(request) = structured {
            JsonSchema::new(request.schema().clone()).map_err(|_| {
                SdkError::invalid_input("Custom structured output requires an object-root schema")
            })?;
        }
        let contract = Self {
            structured_schema: structured.map(|request| request.schema().clone()),
            tool_selection: tool_selection.clone(),
        };
        if contract.encoded_len()? > MAX_PROMPT_CONTRACT_BYTES {
            return Err(SdkError::invalid_input(
                "prompt configuration exceeds the supported limit",
            ));
        }
        Ok(contract)
    }

    fn validate(&self) -> Result<(), SdkError> {
        self.validate_shape()?;
        if self.encoded_len()? > MAX_PROMPT_CONTRACT_BYTES {
            return Err(SdkError::invalid_state(
                "stored prompt configuration exceeds the supported limit",
            ));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), SdkError> {
        if let Some(schema) = &self.structured_schema {
            StructuredOutputRequest::new(schema.clone()).map_err(|_| {
                SdkError::invalid_state("stored structured-output contract is malformed")
            })?;
            JsonSchema::new(schema.clone()).map_err(|_| {
                SdkError::invalid_state("stored structured-output contract is malformed")
            })?;
        }
        self.tool_selection.validate()
    }

    fn encoded_len(&self) -> Result<usize, SdkError> {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .map_err(|_| SdkError::invalid_state("stored prompt configuration is malformed"))
    }
}

impl CustomToolShape {
    pub(crate) fn new(
        name: String,
        description: Option<String>,
        input_schema: serde_json::Value,
    ) -> Result<Self, SdkError> {
        let shape = Self {
            name,
            description,
            input_schema,
        };
        shape.validate()?;
        Ok(shape)
    }

    fn validate(&self) -> Result<(), SdkError> {
        if self.name.is_empty()
            || self.name.trim() != self.name
            || self
                .description
                .as_deref()
                .is_some_and(|value| value.is_empty() || value.trim() != value)
            || !self.input_schema.is_object()
        {
            return Err(SdkError::invalid_state(
                "stored tool configuration is malformed",
            ));
        }
        Ok(())
    }
}

impl CustomCatalogFloor {
    pub(crate) fn new(mut executable: Vec<CustomToolShape>) -> Result<Self, SdkError> {
        executable.sort_by(|left, right| left.name.cmp(&right.name));
        let floor = Self { executable };
        floor.validate()?;
        Ok(floor)
    }

    fn validate(&self) -> Result<(), SdkError> {
        if self.executable.len() > MAX_CATALOG_ENTRIES {
            return Err(SdkError::invalid_state(
                "stored tool configuration exceeds the supported limit",
            ));
        }
        let mut names = BTreeSet::new();
        for shape in &self.executable {
            shape.validate()?;
            if !names.insert(shape.name.clone()) {
                return Err(SdkError::invalid_state(
                    "stored tool configuration contains duplicate names",
                ));
            }
        }
        if self
            .executable
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
        {
            return Err(SdkError::invalid_state(
                "stored tool configuration order is malformed",
            ));
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|_| SdkError::invalid_state("stored tool configuration is malformed"))?;
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(SdkError::invalid_state(
                "stored tool configuration exceeds the supported limit",
            ));
        }
        Ok(())
    }
}

impl CustomAttachment {
    pub(crate) fn new(
        agent_id: AgentId,
        compatibility: CustomCompatibilityFloor,
    ) -> Result<Self, SdkError> {
        let attachment = Self {
            version: CUSTOM_ATTACHMENT_VERSION,
            agent_id,
            compatibility,
        };
        attachment.validate()?;
        Ok(attachment)
    }

    pub(crate) fn key(agent_id: &AgentId) -> Result<StateKey, SdkError> {
        StateKey::new(CUSTOM_ATTACHMENT_NAMESPACE, agent_id.as_str())
            .map_err(|_| SdkError::invalid_state("stored agent state is malformed"))
    }

    pub(crate) fn encode(&self) -> Result<serde_json::Value, SdkError> {
        self.validate()?;
        serde_json::to_value(self)
            .map_err(|_| SdkError::invalid_state("stored agent state is malformed"))
    }

    pub(crate) fn decode(
        snapshot: StateRecordSnapshot,
        expected_agent_id: &AgentId,
    ) -> Result<Self, SdkError> {
        let value = snapshot.record.value;
        if value.get("version").and_then(serde_json::Value::as_u64)
            != Some(u64::from(CUSTOM_ATTACHMENT_VERSION))
        {
            return Err(SdkError::conflict("stored agent state is incompatible")
                .with_detail(crate::error::ErrorDetail::AttachmentMismatch));
        }
        let attachment: Self = serde::Deserialize::deserialize(value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored agent state is malformed"))?;
        attachment.validate()?;
        if &attachment.agent_id != expected_agent_id
            || snapshot.record.key.key() != expected_agent_id.as_str()
        {
            return Err(SdkError::invalid_state(
                "stored agent state contradicts its identity",
            ));
        }
        Ok(attachment)
    }

    fn validate(&self) -> Result<(), SdkError> {
        if self.version != CUSTOM_ATTACHMENT_VERSION {
            return Err(
                SdkError::invalid_state("stored agent state is incompatible")
                    .with_detail(crate::error::ErrorDetail::AttachmentMismatch),
            );
        }
        self.compatibility.validate()
    }
}

impl CustomTurn {
    fn new(caller_text: String, assistant_text: String) -> Self {
        Self {
            caller_text,
            assistant_text,
        }
    }
}

impl CustomGeneration {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn into_messages<C>(self) -> Result<Vec<Message<C>>, SdkError> {
        let mut messages = Vec::with_capacity(self.turns.len().saturating_mul(2).saturating_add(1));
        let malformed = |_| SdkError::invalid_state("stored conversation state is malformed");
        for turn in self.turns {
            messages.push(Message::user_text(turn.caller_text).map_err(malformed)?);
            messages.push(Message::assistant_text(turn.assistant_text).map_err(malformed)?);
        }
        messages.push(
            Message::user_text(self.caller_text)
                .map_err(|_| SdkError::invalid_input("prompt text must not be blank"))?,
        );
        Ok(messages)
    }
}

impl CustomContinuation {
    pub(crate) fn provisioning(conversation_id: ConversationId) -> Self {
        Self {
            version: CUSTOM_CONTINUATION_VERSION,
            conversation_id,
            next_generation: 1,
            state: CustomConversationState::Provisioning,
            turns: Vec::new(),
            terminal: None,
            lost_generations: 0,
        }
    }

    pub(crate) fn promote(&mut self) -> Result<(), SdkError> {
        self.validate(&self.conversation_id)?;
        let pristine = self.next_generation == 1
            && self.turns.is_empty()
            && self.terminal.is_none()
            && self.lost_generations == 0;
        match self.state {
            CustomConversationState::Provisioning if pristine => {
                self.state = CustomConversationState::Ready;
                Ok(())
            }
            CustomConversationState::Ready if pristine => Ok(()),
            _ => Err(SdkError::conflict(
                "conversation setup state is inconsistent",
            )),
        }
    }

    pub(crate) fn begin(
        &mut self,
        caller_text: String,
        structured: Option<&StructuredOutputRequest>,
        tool_selection: &ToolSelection,
    ) -> Result<CustomGeneration, SdkError> {
        self.validate(&self.conversation_id)?;
        if self
            .terminal
            .as_ref()
            .is_some_and(CustomTerminalState::has_pending_waiter)
        {
            return Err(SdkError::busy("prior prompt completion is still settling"));
        }
        match &self.state {
            CustomConversationState::Ready => {}
            CustomConversationState::Provisioning => {
                return Err(SdkError::closed("conversation is not available"));
            }
            CustomConversationState::Active { .. } => {
                return Err(SdkError::busy("conversation already has an active prompt"));
            }
            CustomConversationState::Sealed
            | CustomConversationState::Closing
            | CustomConversationState::ClosingActive { .. } => {
                return Err(SdkError::closed("conversation state is sealed"));
            }
        }
        let contract = CustomPromptContract::from_request(structured, tool_selection)?;
        Self::preflight_caller(&self.turns, &caller_text)?;
        let id = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .ok_or_else(|| SdkError::invalid_state("prompt identity is exhausted"))?;
        self.state = CustomConversationState::Active {
            generation: id,
            accepted: false,
            cancellation: None,
            caller_text: caller_text.clone(),
            contract,
        };
        Ok(CustomGeneration {
            id,
            turns: self.turns.clone(),
            caller_text,
        })
    }

    pub(crate) fn accept(&mut self, generation: u64) -> Result<(), SdkError> {
        match &mut self.state {
            CustomConversationState::Active {
                generation: active,
                accepted,
                ..
            }
            | CustomConversationState::ClosingActive {
                generation: active,
                accepted,
                ..
            } if *active == generation => {
                *accepted = true;
                Ok(())
            }
            _ => Err(SdkError::conflict(
                "prompt identity changed before acceptance",
            )),
        }
    }

    pub(crate) fn request_cancellation(
        &mut self,
        generation: u64,
        intent: CustomCancellationIntent,
    ) -> Result<bool, SdkError> {
        let terminal_generation = self.terminal.as_ref().map(CustomTerminalState::generation);
        match &mut self.state {
            CustomConversationState::Active {
                generation: active,
                cancellation,
                ..
            }
            | CustomConversationState::ClosingActive {
                generation: active,
                cancellation,
                ..
            } if *active == generation => {
                if cancellation.is_none() {
                    *cancellation = Some(intent);
                }
                Ok(true)
            }
            CustomConversationState::Provisioning
            | CustomConversationState::Ready
            | CustomConversationState::Sealed
            | CustomConversationState::Closing
                if terminal_generation == Some(generation) =>
            {
                Ok(false)
            }
            _ => Err(SdkError::conflict(
                "prompt identity changed before cancellation intent was recorded",
            )),
        }
    }

    pub(crate) fn request_active_cancellation(
        &mut self,
        intent: CustomCancellationIntent,
    ) -> Result<Option<u64>, SdkError> {
        let Some(generation) = self.active_generation() else {
            return Ok(None);
        };
        self.request_cancellation(generation, intent)?;
        Ok(Some(generation))
    }

    pub(crate) fn finish(
        &mut self,
        generation: u64,
        attempt: &CustomTerminalAttempt,
    ) -> Result<CustomContinuationDisposition, SdkError> {
        if let Some(disposition) = self.finished_disposition(generation, attempt)? {
            return Ok(disposition);
        }
        let (accepted, cancellation, caller_text, contract, closing) = match &self.state {
            CustomConversationState::Active {
                generation: active,
                accepted,
                cancellation,
                caller_text,
                contract,
            } if *active == generation => (
                *accepted,
                *cancellation,
                caller_text.clone(),
                contract.clone(),
                false,
            ),
            CustomConversationState::ClosingActive {
                generation: active,
                accepted,
                cancellation,
                caller_text,
                contract,
            } if *active == generation => (
                *accepted,
                *cancellation,
                caller_text.clone(),
                contract.clone(),
                true,
            ),
            _ => {
                return Err(SdkError::conflict(
                    "prompt identity changed before terminalization",
                ));
            }
        };
        let Some(final_text) = attempt.text() else {
            self.state = if closing {
                CustomConversationState::Closing
            } else {
                CustomConversationState::Ready
            };
            self.terminal = Some(CustomTerminalState::Failed {
                generation,
                accepted,
                cancellation,
                contract,
                waiter: CustomWaiterState::Pending,
            });
            return Ok(CustomContinuationDisposition::Ready);
        };

        let mut proposed = self.turns.clone();
        proposed.push(CustomTurn::new(caller_text, final_text.to_owned()));
        if Self::validate_turns(&proposed).is_err() {
            let attempt = attempt.identity().copied().ok_or_else(|| {
                SdkError::invalid_state("terminal output attempt identity is missing")
            })?;
            self.state = if closing {
                CustomConversationState::Closing
            } else {
                CustomConversationState::Sealed
            };
            self.terminal = Some(CustomTerminalState::Sealed {
                generation,
                attempt,
                accepted,
                cancellation,
                contract,
                waiter: CustomWaiterState::Pending,
            });
            return Ok(CustomContinuationDisposition::Sealed);
        }
        self.turns = proposed;
        self.state = if closing {
            CustomConversationState::Closing
        } else {
            CustomConversationState::Ready
        };
        self.terminal = Some(CustomTerminalState::Committed {
            generation,
            accepted,
            cancellation,
            contract,
            waiter: CustomWaiterState::Pending,
        });
        Ok(CustomContinuationDisposition::Ready)
    }

    fn finished_disposition(
        &self,
        generation: u64,
        attempt: &CustomTerminalAttempt,
    ) -> Result<Option<CustomContinuationDisposition>, SdkError> {
        let Some(terminal) = self.terminal.as_ref() else {
            return Ok(None);
        };
        if terminal.generation() != generation {
            return Ok(None);
        }
        match (terminal, attempt.text()) {
            (CustomTerminalState::Failed { .. }, None) => {
                Ok(Some(CustomContinuationDisposition::Ready))
            }
            (CustomTerminalState::Committed { .. }, Some(expected))
                if self
                    .turns
                    .last()
                    .is_some_and(|turn| turn.assistant_text == expected) =>
            {
                Ok(Some(CustomContinuationDisposition::Ready))
            }
            (
                CustomTerminalState::Sealed {
                    attempt: stored_attempt,
                    ..
                },
                Some(_),
            ) if attempt
                .identity()
                .is_some_and(|attempt| stored_attempt == attempt) =>
            {
                Ok(Some(CustomContinuationDisposition::Sealed))
            }
            (
                CustomTerminalState::Committed { .. }
                | CustomTerminalState::Failed { .. }
                | CustomTerminalState::Sealed { .. }
                | CustomTerminalState::Interrupted { .. },
                _,
            ) => Err(SdkError::conflict(
                "prompt terminal outcome contradicts its durable record",
            )),
        }
    }

    pub(crate) fn settle_waiter(
        &mut self,
        generation: u64,
        delivered: bool,
    ) -> Result<(), SdkError> {
        let terminal = self
            .terminal
            .as_mut()
            .ok_or_else(|| SdkError::conflict("prompt completion state is unavailable"))?;
        // A later generation cannot start while its predecessor has a pending waiter.
        if generation != 0 && terminal.generation() > generation {
            return Ok(());
        }
        if terminal.generation() != generation {
            return Err(SdkError::conflict("prompt completion state changed"));
        }
        terminal.settle_waiter(delivered)
    }

    pub(crate) fn active_generation(&self) -> Option<u64> {
        match &self.state {
            CustomConversationState::Active { generation, .. }
            | CustomConversationState::ClosingActive { generation, .. } => Some(*generation),
            CustomConversationState::Provisioning
            | CustomConversationState::Ready
            | CustomConversationState::Sealed
            | CustomConversationState::Closing => None,
        }
    }

    pub(crate) fn is_reopenable(&self) -> bool {
        !matches!(
            &self.state,
            CustomConversationState::Provisioning
                | CustomConversationState::Closing
                | CustomConversationState::ClosingActive { .. }
        )
    }

    pub(crate) fn is_provisioning(&self) -> bool {
        matches!(&self.state, CustomConversationState::Provisioning)
    }

    pub(crate) fn is_closing(&self) -> bool {
        matches!(
            &self.state,
            CustomConversationState::Closing | CustomConversationState::ClosingActive { .. }
        )
    }

    pub(crate) fn ensure_quiescent_open(&self) -> Result<(), SdkError> {
        self.validate(&self.conversation_id)?;
        match &self.state {
            CustomConversationState::Provisioning
            | CustomConversationState::Ready
            | CustomConversationState::Sealed => {}
            CustomConversationState::Active { .. }
            | CustomConversationState::ClosingActive { .. } => {
                return Err(SdkError::busy(
                    "conversation still has unsettled prompt work",
                ));
            }
            CustomConversationState::Closing => {
                return Err(SdkError::closed("conversation is closing"));
            }
        }
        if self
            .terminal
            .as_ref()
            .is_some_and(CustomTerminalState::has_pending_waiter)
        {
            return Err(SdkError::busy("prior prompt completion is still settling"));
        }
        Ok(())
    }

    pub(crate) fn abort_target(&self) -> Result<Option<u64>, SdkError> {
        match &self.state {
            CustomConversationState::Active { generation, .. }
            | CustomConversationState::ClosingActive { generation, .. } => Ok(Some(*generation)),
            CustomConversationState::Provisioning
            | CustomConversationState::Ready
            | CustomConversationState::Closing => Ok(None),
            CustomConversationState::Sealed => {
                Err(SdkError::closed("conversation state is sealed"))
            }
        }
    }

    pub(crate) fn begin_close(&mut self) -> Result<Option<u64>, SdkError> {
        self.validate(&self.conversation_id)?;
        let state = std::mem::replace(&mut self.state, CustomConversationState::Closing);
        match state {
            CustomConversationState::Active {
                generation,
                accepted,
                cancellation,
                caller_text,
                contract,
            }
            | CustomConversationState::ClosingActive {
                generation,
                accepted,
                cancellation,
                caller_text,
                contract,
            } => {
                self.state = CustomConversationState::ClosingActive {
                    generation,
                    accepted,
                    cancellation,
                    caller_text,
                    contract,
                };
                Ok(Some(generation))
            }
            CustomConversationState::Provisioning
            | CustomConversationState::Ready
            | CustomConversationState::Sealed
            | CustomConversationState::Closing => Ok(None),
        }
    }

    pub(crate) fn reconcile_reopen(&mut self) -> Result<bool, SdkError> {
        self.validate(&self.conversation_id)?;
        if self.is_provisioning() {
            self.state = CustomConversationState::Closing;
            return Ok(true);
        }
        let waiter_reconciled = self
            .terminal
            .as_mut()
            .is_some_and(CustomTerminalState::discard_pending_waiter);
        let (generation, accepted, cancellation, contract, closing) = match &self.state {
            CustomConversationState::Active {
                generation,
                accepted,
                cancellation,
                contract,
                ..
            } => (
                *generation,
                *accepted,
                *cancellation,
                contract.clone(),
                false,
            ),
            CustomConversationState::ClosingActive {
                generation,
                accepted,
                cancellation,
                contract,
                ..
            } => (
                *generation,
                *accepted,
                *cancellation,
                contract.clone(),
                true,
            ),
            CustomConversationState::Provisioning
            | CustomConversationState::Ready
            | CustomConversationState::Sealed
            | CustomConversationState::Closing => return Ok(waiter_reconciled),
        };
        self.state = if closing {
            CustomConversationState::Closing
        } else {
            CustomConversationState::Ready
        };
        self.terminal = Some(CustomTerminalState::Interrupted {
            generation,
            accepted,
            cancellation,
            contract,
            waiter: CustomWaiterState::Discarded,
        });
        self.lost_generations = self
            .lost_generations
            .checked_add(1)
            .ok_or_else(|| SdkError::invalid_state("lost prompt identity count is exhausted"))?;
        Ok(true)
    }

    pub(crate) fn key(conversation_id: &ConversationId) -> Result<StateKey, SdkError> {
        StateKey::new(CUSTOM_CONTINUATION_NAMESPACE, conversation_id.as_str())
            .map_err(|_| SdkError::invalid_state("stored conversation identity is malformed"))
    }

    pub(crate) fn encode(&self) -> Result<serde_json::Value, SdkError> {
        self.validate(&self.conversation_id)?;
        serde_json::to_value(self)
            .map_err(|_| SdkError::invalid_state("conversation state could not be encoded"))
    }

    pub(crate) fn decode(
        snapshot: StateRecordSnapshot,
        expected_conversation_id: &ConversationId,
    ) -> Result<Self, SdkError> {
        match snapshot
            .record
            .value
            .get("version")
            .and_then(serde_json::Value::as_u64)
        {
            Some(version) if version == u64::from(CUSTOM_CONTINUATION_VERSION) => {}
            Some(_) => {
                return Err(
                    SdkError::conflict("stored conversation state is incompatible")
                        .with_detail(crate::error::ErrorDetail::AttachmentMismatch),
                );
            }
            None => {
                return Err(SdkError::invalid_state(
                    "stored conversation state is malformed",
                ));
            }
        }
        let continuation: Self = serde::Deserialize::deserialize(snapshot.record.value.as_ref())
            .map_err(|_| SdkError::invalid_state("stored conversation state is malformed"))?;
        continuation.validate(expected_conversation_id)?;
        if snapshot.record.key.key() != expected_conversation_id.as_str() {
            return Err(SdkError::invalid_state(
                "stored conversation state contradicts its identity",
            ));
        }
        Ok(continuation)
    }

    fn validate(&self, expected_conversation_id: &ConversationId) -> Result<(), SdkError> {
        if self.version != CUSTOM_CONTINUATION_VERSION
            || &self.conversation_id != expected_conversation_id
            || self.next_generation == 0
            || matches!(
                &self.state,
                CustomConversationState::Active { generation: 0, .. }
                    | CustomConversationState::ClosingActive { generation: 0, .. }
            )
            || match &self.state {
                CustomConversationState::Active { generation, .. }
                | CustomConversationState::ClosingActive { generation, .. } => {
                    *generation >= self.next_generation
                }
                CustomConversationState::Provisioning
                | CustomConversationState::Ready
                | CustomConversationState::Sealed
                | CustomConversationState::Closing => false,
            }
            || (matches!(&self.state, CustomConversationState::Provisioning)
                && (self.next_generation != 1
                    || !self.turns.is_empty()
                    || self.terminal.is_some()
                    || self.lost_generations != 0))
            || self.terminal.as_ref().is_some_and(|terminal| {
                terminal.generation() == 0 || terminal.generation() >= self.next_generation
            })
            || self.active_generation().is_some_and(|active| {
                self.terminal
                    .as_ref()
                    .is_some_and(|terminal| terminal.generation() >= active)
            })
        {
            return Err(SdkError::invalid_state(
                "stored conversation state is malformed",
            ));
        }
        Self::validate_turns(&self.turns)?;
        if let CustomConversationState::Active { caller_text, .. }
        | CustomConversationState::ClosingActive { caller_text, .. } = &self.state
        {
            Self::preflight_caller(&self.turns, caller_text)
                .map_err(|_| SdkError::invalid_state("stored conversation state is malformed"))?;
        }
        match &self.state {
            CustomConversationState::Active { contract, .. }
            | CustomConversationState::ClosingActive { contract, .. } => contract.validate()?,
            CustomConversationState::Provisioning
            | CustomConversationState::Ready
            | CustomConversationState::Sealed
            | CustomConversationState::Closing => {}
        }
        if let Some(terminal) = &self.terminal {
            terminal.contract().validate()?;
            if let CustomTerminalState::Sealed { attempt, .. } = terminal {
                let attempt = uuid::Uuid::from_bytes(*attempt);
                if attempt.get_version() != Some(uuid::Version::Random)
                    || attempt.get_variant() != uuid::Variant::RFC4122
                {
                    return Err(SdkError::invalid_state(
                        "stored terminal attempt identity is malformed",
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_turns(turns: &[CustomTurn]) -> Result<(), SdkError> {
        if turns
            .iter()
            .any(|turn| turn.caller_text.trim().is_empty() || turn.assistant_text.trim().is_empty())
        {
            return Err(SdkError::invalid_state(
                "stored conversation state is malformed",
            ));
        }
        if turns.len() > MAX_CONTINUATION_TURNS {
            return Err(SdkError::invalid_state(
                "stored conversation state exceeds the supported limit",
            ));
        }
        let bytes = serde_json::to_vec(turns)
            .map_err(|_| SdkError::invalid_state("conversation state could not be encoded"))?;
        if bytes.len() > MAX_CONTINUATION_BYTES {
            return Err(SdkError::invalid_state(
                "stored conversation state exceeds the supported limit",
            ));
        }
        Ok(())
    }

    fn preflight_caller(turns: &[CustomTurn], caller_text: &str) -> Result<(), SdkError> {
        if caller_text.trim().is_empty() {
            return Err(SdkError::invalid_input("prompt text must not be blank"));
        }
        if turns.len() >= MAX_CONTINUATION_TURNS {
            return Err(SdkError::invalid_input(
                "prompt would exceed the supported conversation history limit",
            ));
        }
        let mut proposed = turns.to_vec();
        proposed.push(CustomTurn::new(caller_text.to_owned(), String::new()));
        let bytes = serde_json::to_vec(&proposed)
            .map_err(|_| SdkError::invalid_state("prompt state could not be encoded"))?;
        if bytes.len() > MAX_CONTINUATION_BYTES {
            return Err(SdkError::invalid_input(
                "prompt would exceed the supported conversation history limit",
            ));
        }
        Ok(())
    }
}

impl CustomTerminalState {
    fn generation(&self) -> u64 {
        match self {
            Self::Committed { generation, .. }
            | Self::Failed { generation, .. }
            | Self::Sealed { generation, .. }
            | Self::Interrupted { generation, .. } => *generation,
        }
    }

    fn contract(&self) -> &CustomPromptContract {
        match self {
            Self::Committed { contract, .. }
            | Self::Failed { contract, .. }
            | Self::Sealed { contract, .. }
            | Self::Interrupted { contract, .. } => contract,
        }
    }

    fn settle_waiter(&mut self, delivered: bool) -> Result<(), SdkError> {
        let expected = if delivered {
            CustomWaiterState::Delivered
        } else {
            CustomWaiterState::Discarded
        };
        let waiter = self.waiter_mut();
        match *waiter {
            CustomWaiterState::Pending => {
                *waiter = expected;
                Ok(())
            }
            current if current == expected => Ok(()),
            CustomWaiterState::Delivered | CustomWaiterState::Discarded => Err(SdkError::conflict(
                "prompt completion state is inconsistent",
            )),
        }
    }

    fn discard_pending_waiter(&mut self) -> bool {
        let waiter = self.waiter_mut();
        if *waiter != CustomWaiterState::Pending {
            return false;
        }
        *waiter = CustomWaiterState::Discarded;
        true
    }

    fn has_pending_waiter(&self) -> bool {
        match self {
            Self::Committed { waiter, .. }
            | Self::Failed { waiter, .. }
            | Self::Sealed { waiter, .. }
            | Self::Interrupted { waiter, .. } => *waiter == CustomWaiterState::Pending,
        }
    }

    fn waiter_mut(&mut self) -> &mut CustomWaiterState {
        match self {
            Self::Committed { waiter, .. }
            | Self::Failed { waiter, .. }
            | Self::Sealed { waiter, .. }
            | Self::Interrupted { waiter, .. } => waiter,
        }
    }
}
