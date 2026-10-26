use crate::backends::codex::protocol::CodexPromptTurnStatus;
use crate::error::{ErrorDetail, ErrorPhase, Operation};
use serde_json::Value;

use crate::backends::codex::protocol::notifications::{
    CodexAgentMessage, CodexAgentMessageDelta, CodexCommandExecution, CodexCommandExecutionStatus,
    CodexConnectionProcessNotification, CodexItemLifecycle, CodexKnownNotification,
    CodexKnownNotificationKind, CodexOtherThreadItemKind, CodexScope, CodexServerNotification,
    CodexThreadItem, CodexTurnStatus, CodexTurnTerminal, CodexUnknownNotification, CodexWarning,
};
use crate::{ConversationNotificationKind, ProcessState, PromptResponse, SdkError};

const COMMAND_PROCESS_LABEL: &str = "Command";

pub(crate) enum CodexNotificationAction {
    OutputDelta(CodexAgentMessageDelta),
    PromptOutput(CodexPromptOutput),
    PromptTerminal(CodexPromptTerminal),
    RequestResolved {
        thread_id: String,
        request_id: Value,
    },
    ThreadDeleted {
        thread_id: String,
    },
    ThreadStarted {
        thread_id: String,
        thread_source: Option<String>,
    },
    Publish {
        thread_id: String,
        kind: ConversationNotificationKind,
    },
    Observe {
        scope: CodexScope,
    },
    HistoryReverted {
        thread_id: String,
    },
    State {
        thread_id: Option<String>,
    },
    Unsupported {
        thread_id: Option<String>,
    },
    Diagnostic {
        thread_id: Option<String>,
        method: String,
    },
}

pub(crate) struct CodexPromptOutput {
    thread_id: String,
    turn_id: String,
    text: String,
    eligible: bool,
}

#[derive(Clone)]
pub(crate) struct CodexPromptTerminal {
    thread_id: String,
    turn_id: String,
    outcome: CodexPromptTerminalOutcome,
}

#[derive(Clone)]
enum CodexPromptTerminalOutcome {
    Completed,
    Interrupted(Option<SdkError>),
    Failed(SdkError),
}

impl CodexNotificationAction {
    pub(crate) fn from_wire(notification: CodexServerNotification) -> Result<Self, SdkError> {
        match notification {
            CodexServerNotification::AgentMessageDelta(delta) => Ok(Self::OutputDelta(delta)),
            CodexServerNotification::Item(item) => {
                let thread_id = item.thread_id().to_owned();
                let turn_id = item.turn_id().to_owned();
                let lifecycle = item.lifecycle();
                Self::from_item(thread_id, turn_id, lifecycle, item.into_item())
            }
            CodexServerNotification::TurnTerminal(terminal) => {
                CodexPromptTerminal::from_wire(terminal).map(Self::PromptTerminal)
            }
            CodexServerNotification::RequestResolved(resolved) => Ok(Self::RequestResolved {
                thread_id: resolved.thread_id().to_owned(),
                request_id: resolved.request_id().clone(),
            }),
            CodexServerNotification::ThreadDeleted(deleted) => Ok(Self::ThreadDeleted {
                thread_id: deleted.thread_id().to_owned(),
            }),
            CodexServerNotification::ThreadStarted(started) => Ok(Self::ThreadStarted {
                thread_id: started.thread_id().to_owned(),
                thread_source: started.thread_source().map(str::to_owned),
            }),
            CodexServerNotification::Warning(warning)
            | CodexServerNotification::GuardianWarning(warning) => Ok(Self::from_warning(warning)),
            CodexServerNotification::ConnectionProcess(process) => {
                Ok(Self::from_connection_process(process))
            }
            CodexServerNotification::Known(known) => Ok(Self::from_known(known)),
            CodexServerNotification::UnknownFuture(unknown) => Ok(Self::from_unknown(unknown)),
        }
    }

    fn from_item(
        thread_id: String,
        turn_id: String,
        lifecycle: CodexItemLifecycle,
        item: CodexThreadItem,
    ) -> Result<Self, SdkError> {
        match (lifecycle, item) {
            (CodexItemLifecycle::Completed, CodexThreadItem::AgentMessage(message)) => {
                message.validate_support()?;
                Ok(Self::PromptOutput(CodexPromptOutput::new(
                    thread_id, turn_id, message,
                )))
            }
            (CodexItemLifecycle::Started, CodexThreadItem::AgentMessage(message)) => {
                message.validate_support()?;
                Ok(Self::State {
                    thread_id: Some(thread_id),
                })
            }
            (CodexItemLifecycle::Started, CodexThreadItem::CommandExecution(command))
                if matches!(command.status(), CodexCommandExecutionStatus::InProgress) =>
            {
                Ok(Self::from_command(thread_id, command))
            }
            (CodexItemLifecycle::Completed, CodexThreadItem::CommandExecution(command))
                if matches!(
                    command.status(),
                    CodexCommandExecutionStatus::Completed
                        | CodexCommandExecutionStatus::Failed
                        | CodexCommandExecutionStatus::Declined
                ) =>
            {
                Ok(Self::from_command(thread_id, command))
            }
            (
                CodexItemLifecycle::Started | CodexItemLifecycle::Completed,
                CodexThreadItem::Other(
                    CodexOtherThreadItemKind::UserMessage
                    | CodexOtherThreadItemKind::HookPrompt
                    | CodexOtherThreadItemKind::Plan
                    | CodexOtherThreadItemKind::Reasoning
                    | CodexOtherThreadItemKind::FileChange
                    | CodexOtherThreadItemKind::McpToolCall
                    | CodexOtherThreadItemKind::ToolOutput
                    | CodexOtherThreadItemKind::WebSearch
                    | CodexOtherThreadItemKind::ImageView
                    | CodexOtherThreadItemKind::ImageGeneration
                    | CodexOtherThreadItemKind::Sleep
                    | CodexOtherThreadItemKind::EnteredReviewMode
                    | CodexOtherThreadItemKind::ExitedReviewMode
                    | CodexOtherThreadItemKind::ContextCompaction,
                ),
            ) => Ok(Self::State {
                thread_id: Some(thread_id),
            }),
            (CodexItemLifecycle::Completed, CodexThreadItem::CommandExecution(_))
            | (CodexItemLifecycle::Started, CodexThreadItem::CommandExecution(_)) => Err(
                SdkError::protocol("runtime event lifecycle contradicts its semantic state"),
            ),
        }
    }

    fn from_warning(warning: CodexWarning) -> Self {
        match warning.thread_id() {
            Some(thread_id) => Self::Publish {
                thread_id: thread_id.to_owned(),
                kind: ConversationNotificationKind::Warning {
                    message: warning.message().to_owned(),
                },
            },
            None => Self::State { thread_id: None },
        }
    }

    fn from_command(thread_id: String, command: CodexCommandExecution) -> Self {
        let state = match command.status() {
            CodexCommandExecutionStatus::InProgress => ProcessState::Running,
            CodexCommandExecutionStatus::Completed
            | CodexCommandExecutionStatus::Failed
            | CodexCommandExecutionStatus::Declined => ProcessState::Exited {
                exit_code: command.exit_code(),
            },
        };
        Self::Publish {
            thread_id,
            kind: ConversationNotificationKind::Process {
                label: COMMAND_PROCESS_LABEL.to_owned(),
                state,
            },
        }
    }

    fn from_connection_process(process: CodexConnectionProcessNotification) -> Self {
        match process {
            CodexConnectionProcessNotification::OutputDelta { .. }
            | CodexConnectionProcessNotification::Exited { .. } => Self::State { thread_id: None },
        }
    }

    fn from_known(known: CodexKnownNotification) -> Self {
        let thread_id = known.thread_id().map(str::to_owned);
        match known.kind() {
            CodexKnownNotificationKind::ThreadReverted => match known.into_scope() {
                CodexScope::Thread { thread_id } => Self::HistoryReverted { thread_id },
                CodexScope::Connection | CodexScope::Turn { .. } => {
                    unreachable!("decoded reverted notification has thread scope")
                }
            },
            CodexKnownNotificationKind::ThreadPredictionUpdated
            | CodexKnownNotificationKind::ThreadQueueChanged
            | CodexKnownNotificationKind::McpEvent
            | CodexKnownNotificationKind::RealtimeItemStarted
            | CodexKnownNotificationKind::RealtimeItemCompleted
            | CodexKnownNotificationKind::RealtimeTranscriptDelta => {
                Self::Unsupported { thread_id }
            }
            CodexKnownNotificationKind::ThreadReadStateChanged
            | CodexKnownNotificationKind::ThreadAttachmentUpdated
            | CodexKnownNotificationKind::ProjectChanged
            | CodexKnownNotificationKind::ThreadProjectUpdated
            | CodexKnownNotificationKind::EnvironmentConnected
            | CodexKnownNotificationKind::EnvironmentDisconnected
            | CodexKnownNotificationKind::StrictReviewRequired
            | CodexKnownNotificationKind::RawResponseCompleted
            | CodexKnownNotificationKind::GatewayOAuthChanged
            | CodexKnownNotificationKind::AuthRecoveryStarted
            | CodexKnownNotificationKind::AuthRecoveryCompleted => Self::Observe {
                scope: known.into_scope(),
            },
            CodexKnownNotificationKind::ThreadRealtimeStarted
            | CodexKnownNotificationKind::ThreadRealtimeItemAdded
            | CodexKnownNotificationKind::ThreadRealtimeTranscriptDelta
            | CodexKnownNotificationKind::ThreadRealtimeTranscriptDone
            | CodexKnownNotificationKind::ThreadRealtimeOutputAudioDelta
            | CodexKnownNotificationKind::ThreadRealtimeSdp
            | CodexKnownNotificationKind::ThreadRealtimeError
            | CodexKnownNotificationKind::ThreadRealtimeClosed => Self::Unsupported { thread_id },
            CodexKnownNotificationKind::Error
            | CodexKnownNotificationKind::TurnStarted
            | CodexKnownNotificationKind::HookStarted
            | CodexKnownNotificationKind::HookCompleted
            | CodexKnownNotificationKind::TurnDiffUpdated
            | CodexKnownNotificationKind::TurnPlanUpdated
            | CodexKnownNotificationKind::ItemGuardianApprovalReviewStarted
            | CodexKnownNotificationKind::ItemGuardianApprovalReviewCompleted
            | CodexKnownNotificationKind::RawResponseItemCompleted
            | CodexKnownNotificationKind::PlanDelta
            | CodexKnownNotificationKind::CommandExecOutputDelta
            | CodexKnownNotificationKind::CommandExecutionOutputDelta
            | CodexKnownNotificationKind::TerminalInteraction
            | CodexKnownNotificationKind::FileChangeOutputDelta
            | CodexKnownNotificationKind::FileChangePatchUpdated
            | CodexKnownNotificationKind::McpToolCallProgress
            | CodexKnownNotificationKind::ReasoningSummaryTextDelta
            | CodexKnownNotificationKind::ReasoningSummaryPartAdded
            | CodexKnownNotificationKind::ReasoningTextDelta
            | CodexKnownNotificationKind::TurnModerationMetadata
            | CodexKnownNotificationKind::ModelRerouted
            | CodexKnownNotificationKind::ModelVerification
            | CodexKnownNotificationKind::ModelSafetyBufferingUpdated
            | CodexKnownNotificationKind::ThreadGoalUpdated
            | CodexKnownNotificationKind::ThreadTokenUsageUpdated
            | CodexKnownNotificationKind::ThreadCompacted
            | CodexKnownNotificationKind::ThreadStatusChanged
            | CodexKnownNotificationKind::ThreadArchived
            | CodexKnownNotificationKind::ThreadUnarchived
            | CodexKnownNotificationKind::ThreadClosed
            | CodexKnownNotificationKind::ThreadNameUpdated
            | CodexKnownNotificationKind::ThreadGoalCleared
            | CodexKnownNotificationKind::ThreadSettingsUpdated
            | CodexKnownNotificationKind::SkillsChanged
            | CodexKnownNotificationKind::McpServerOauthLoginCompleted
            | CodexKnownNotificationKind::McpServerStartupStatusUpdated
            | CodexKnownNotificationKind::AccountUpdated
            | CodexKnownNotificationKind::AccountRateLimitsUpdated
            | CodexKnownNotificationKind::AppListUpdated
            | CodexKnownNotificationKind::RemoteControlStatusChanged
            | CodexKnownNotificationKind::ExternalAgentConfigImportProgress
            | CodexKnownNotificationKind::ExternalAgentConfigImportCompleted
            | CodexKnownNotificationKind::FsChanged
            | CodexKnownNotificationKind::DeprecationNotice
            | CodexKnownNotificationKind::ConfigWarning
            | CodexKnownNotificationKind::FuzzyFileSearchSessionUpdated
            | CodexKnownNotificationKind::FuzzyFileSearchSessionCompleted
            | CodexKnownNotificationKind::WindowsWorldWritableWarning
            | CodexKnownNotificationKind::WindowsSandboxSetupCompleted
            | CodexKnownNotificationKind::AccountLoginCompleted => Self::State { thread_id },
        }
    }

    fn from_unknown(unknown: CodexUnknownNotification) -> Self {
        Self::Diagnostic {
            thread_id: unknown.thread_id().map(str::to_owned),
            method: unknown.method().to_owned(),
        }
    }
}

impl CodexPromptOutput {
    fn new(thread_id: String, turn_id: String, message: CodexAgentMessage) -> Self {
        Self {
            thread_id,
            turn_id,
            text: message.text().to_owned(),
            eligible: message.is_eligible_output(),
        }
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    pub(crate) fn is_eligible_output(&self) -> bool {
        self.eligible
    }
}

impl CodexPromptTerminal {
    pub(crate) fn matches_history(
        &self,
        status: CodexPromptTurnStatus,
        error: Option<&SdkError>,
    ) -> bool {
        match (&self.outcome, status, error) {
            (CodexPromptTerminalOutcome::Completed, CodexPromptTurnStatus::Completed, None)
            | (
                CodexPromptTerminalOutcome::Interrupted(None),
                CodexPromptTurnStatus::Interrupted,
                None,
            ) => true,
            (
                CodexPromptTerminalOutcome::Failed(observed),
                CodexPromptTurnStatus::Failed,
                Some(expected),
            )
            | (
                CodexPromptTerminalOutcome::Interrupted(Some(observed)),
                CodexPromptTurnStatus::Interrupted,
                Some(expected),
            ) => {
                // Both errors come from CodexTurnError::to_error, which emits
                // only these normalized fields and no nested evidence.
                observed.kind() == expected.kind()
                    && observed.context() == expected.context()
                    && observed.detail() == expected.detail()
                    && observed.http_status() == expected.http_status()
            }
            _ => false,
        }
    }

    fn from_wire(terminal: CodexTurnTerminal) -> Result<Self, SdkError> {
        let outcome = match (terminal.status(), terminal.error()) {
            (CodexTurnStatus::Completed, None) => CodexPromptTerminalOutcome::Completed,
            (CodexTurnStatus::Interrupted, error) => {
                CodexPromptTerminalOutcome::Interrupted(error.map(|error| error.to_error()))
            }
            (CodexTurnStatus::Failed, Some(error)) => {
                CodexPromptTerminalOutcome::Failed(error.to_error())
            }
            (CodexTurnStatus::InProgress, _) => {
                return Err(SdkError::protocol(
                    "prompt completion reports an active execution",
                ));
            }
            (CodexTurnStatus::Completed, Some(_)) | (CodexTurnStatus::Failed, None) => {
                return Err(SdkError::protocol(
                    "prompt completion status and error disagree",
                ));
            }
        };
        Ok(Self {
            thread_id: terminal.thread_id().to_owned(),
            turn_id: terminal.turn_id().to_owned(),
            outcome,
        })
    }

    pub(crate) fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub(crate) fn requires_structured_output(&self) -> bool {
        matches!(self.outcome, CodexPromptTerminalOutcome::Completed)
    }

    pub(crate) fn into_result(
        self,
        output: Option<String>,
        structured_output: Option<Value>,
    ) -> Result<PromptResponse, SdkError> {
        match self.outcome {
            CodexPromptTerminalOutcome::Completed => output
                .map(|output| PromptResponse::from_parts(output, structured_output, None))
                .ok_or_else(|| {
                    SdkError::not_yet_implemented("completed without a portable final answer")
                        .with_context(Operation::Prompt, ErrorPhase::CompletionValidation)
                        .with_detail(ErrorDetail::MissingData)
                }),
            CodexPromptTerminalOutcome::Interrupted(cause) => {
                let error = SdkError::cancelled("prompt was interrupted")
                    .with_context(Operation::Prompt, ErrorPhase::Cancellation);
                Err(match cause {
                    Some(cause) => error.with_cause(cause),
                    None => error,
                })
            }
            CodexPromptTerminalOutcome::Failed(error) => Err(error),
        }
    }
}
