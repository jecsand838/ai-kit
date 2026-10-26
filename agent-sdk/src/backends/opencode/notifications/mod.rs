use std::path::{Path, PathBuf};

use crate::backends::opencode::protocol::inbound::{
    OpenCodeKnownEvent, OpenCodePrivateGlobalEvent,
};
use crate::backends::opencode::protocol::schemas::{
    OpenCodeMessageUpdatedEvent, OpenCodePartEffect, OpenCodePartUpdatedEvent,
    OpenCodePermissionReply, OpenCodePermissionRequest, OpenCodeProcessEffect,
    OpenCodeQuestionRequest, OpenCodeSessionDiffEvent,
};
use crate::backends::opencode::state_store::records::{
    OpenCodeContinuationKind, OpenCodeContinuationRequest, OpenCodeNativeResolution,
};
use crate::{ConversationNotificationKind, ProcessState, SdkError};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OpenCodeNotifications;

#[derive(Clone, Debug)]
pub(crate) enum OpenCodeEventSemantics {
    Private,
    UnknownFuture,
    PromptStatus(OpenCodePromptStatus),
    PromptMessage {
        session_id: String,
        message_id: String,
    },
    PromptPart {
        session_id: String,
        message_id: String,
        part_id: String,
    },
    ConversationDeleted,
    CatalogInvalidated,
    Process {
        process: OpenCodeProcessEffect,
        kind: ConversationNotificationKind,
    },
    Notifications(Vec<ConversationNotificationKind>),
    Interaction {
        kind: ConversationNotificationKind,
        request: OpenCodeContinuationRequest,
    },
    InteractionResolved {
        request_id: String,
        kind: OpenCodeContinuationKind,
        response_count: Option<usize>,
        outcome: OpenCodeNativeResolution,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenCodePromptStatus {
    Active,
    Idle,
}

#[derive(Clone, Debug)]
struct OpenCodeFileFact {
    path: PathBuf,
    summary: Option<String>,
}

impl OpenCodeNotifications {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) fn classify(
        &self,
        event: &OpenCodeKnownEvent,
        workspace_directory: Option<&Path>,
    ) -> Result<OpenCodeEventSemantics, SdkError> {
        match event {
            OpenCodeKnownEvent::Connected
            | OpenCodeKnownEvent::Heartbeat
            | OpenCodeKnownEvent::Disposed
            | OpenCodeKnownEvent::InstanceDisposed(_)
            | OpenCodeKnownEvent::SessionCreated(_)
            | OpenCodeKnownEvent::SessionUpdated(_)
            | OpenCodeKnownEvent::SessionCompacted(_)
            | OpenCodeKnownEvent::MessageRemoved(_)
            | OpenCodeKnownEvent::MessagePartRemoved(_)
            | OpenCodeKnownEvent::MessagePartDelta(_)
            | OpenCodeKnownEvent::SessionError(_)
            | OpenCodeKnownEvent::TodoUpdated(_) => Ok(OpenCodeEventSemantics::Private),
            OpenCodeKnownEvent::SessionIdle(_) => Ok(OpenCodeEventSemantics::PromptStatus(
                OpenCodePromptStatus::Idle,
            )),
            OpenCodeKnownEvent::SessionStatus(event) => {
                Ok(OpenCodeEventSemantics::PromptStatus(if event.is_active() {
                    OpenCodePromptStatus::Active
                } else {
                    OpenCodePromptStatus::Idle
                }))
            }
            OpenCodeKnownEvent::SessionDeleted(_) => {
                Ok(OpenCodeEventSemantics::ConversationDeleted)
            }
            OpenCodeKnownEvent::MessageUpdated(event) => self.message(event),
            OpenCodeKnownEvent::MessagePartUpdated(event) => self.part(event, workspace_directory),
            OpenCodeKnownEvent::SessionDiff(event) => self.diff(event, workspace_directory),
            OpenCodeKnownEvent::PermissionAsked(request) => self.permission(request),
            OpenCodeKnownEvent::PermissionReplied(event) => match event.reply() {
                OpenCodePermissionReply::Once
                | OpenCodePermissionReply::Always
                | OpenCodePermissionReply::Reject => {
                    Ok(OpenCodeEventSemantics::InteractionResolved {
                        request_id: event.request_id().to_owned(),
                        kind: OpenCodeContinuationKind::Permission,
                        response_count: None,
                        outcome: match event.reply() {
                            OpenCodePermissionReply::Reject => OpenCodeNativeResolution::Rejected,
                            OpenCodePermissionReply::Once | OpenCodePermissionReply::Always => {
                                OpenCodeNativeResolution::Answered
                            }
                        },
                    })
                }
            },
            OpenCodeKnownEvent::QuestionAsked(request) => self.question(request),
            OpenCodeKnownEvent::QuestionReplied(event) => {
                Ok(OpenCodeEventSemantics::InteractionResolved {
                    request_id: event.request_id().to_owned(),
                    kind: OpenCodeContinuationKind::Question,
                    response_count: Some(event.answer_count()),
                    outcome: OpenCodeNativeResolution::Answered,
                })
            }
            OpenCodeKnownEvent::QuestionRejected(event) => {
                Ok(OpenCodeEventSemantics::InteractionResolved {
                    request_id: event.request_id().to_owned(),
                    kind: OpenCodeContinuationKind::Question,
                    response_count: None,
                    outcome: OpenCodeNativeResolution::Rejected,
                })
            }
            OpenCodeKnownEvent::PermissionV2Asked(_)
            | OpenCodeKnownEvent::PermissionV2Replied(_)
            | OpenCodeKnownEvent::QuestionV2Asked(_)
            | OpenCodeKnownEvent::QuestionV2Replied(_)
            | OpenCodeKnownEvent::QuestionV2Rejected(_) => Err(SdkError::not_yet_implemented(
                "selected version-two interaction mapping is not implemented",
            )),
            OpenCodeKnownEvent::SessionNext(event) => {
                debug_assert!(event.is_private_state());
                Ok(OpenCodeEventSemantics::Private)
            }
            OpenCodeKnownEvent::PrivateSession(event) => {
                debug_assert!(event.is_private_state());
                Ok(OpenCodeEventSemantics::Private)
            }
            OpenCodeKnownEvent::PrivateGlobal(event) => Ok(self.private_global(*event)),
            OpenCodeKnownEvent::UnknownFuture => Ok(OpenCodeEventSemantics::UnknownFuture),
        }
    }

    fn message(
        &self,
        event: &OpenCodeMessageUpdatedEvent,
    ) -> Result<OpenCodeEventSemantics, SdkError> {
        let Some(evidence) = event.prompt_message_evidence() else {
            return Ok(OpenCodeEventSemantics::Private);
        };
        Ok(OpenCodeEventSemantics::PromptMessage {
            session_id: evidence.session_id().to_owned(),
            message_id: evidence.message_id().to_owned(),
        })
    }

    pub(crate) fn permission(
        &self,
        request: &OpenCodePermissionRequest,
    ) -> Result<OpenCodeEventSemantics, SdkError> {
        request.clone().validate_v1()?;
        let body = if request.patterns().is_empty() {
            format!("Allow {}?", request.permission())
        } else {
            format!(
                "Allow {} for {}?",
                request.permission(),
                request.patterns().join(", ")
            )
        };
        Ok(OpenCodeEventSemantics::Interaction {
            kind: ConversationNotificationKind::Interaction {
                title: "Permission request".to_owned(),
                body,
                response_schema: None,
                dismissible: false,
            },
            request: OpenCodeContinuationRequest::Permission(request.clone()),
        })
    }

    pub(crate) fn question(
        &self,
        request: &OpenCodeQuestionRequest,
    ) -> Result<OpenCodeEventSemantics, SdkError> {
        request.clone().validate_v1()?;
        let questions = request.questions();
        let title = match questions {
            [question] => question.header().to_owned(),
            _ => "Questions".to_owned(),
        };
        let body = questions
            .iter()
            .enumerate()
            .map(|(index, question)| format!("{}. {}", index + 1, question.question()))
            .collect::<Vec<_>>()
            .join("\n");
        let count = questions.len();
        let response_schema = self.question_schema(count)?;
        Ok(OpenCodeEventSemantics::Interaction {
            kind: ConversationNotificationKind::Interaction {
                title,
                body,
                response_schema: Some(response_schema),
                dismissible: true,
            },
            request: OpenCodeContinuationRequest::Question(request.clone()),
        })
    }

    fn part(
        &self,
        event: &OpenCodePartUpdatedEvent,
        workspace_directory: Option<&Path>,
    ) -> Result<OpenCodeEventSemantics, SdkError> {
        if let Some(evidence) = event.prompt_part_evidence() {
            return Ok(OpenCodeEventSemantics::PromptPart {
                session_id: evidence.session_id().to_owned(),
                message_id: evidence.message_id().to_owned(),
                part_id: evidence.part_id().to_owned(),
            });
        }
        match event.effect()? {
            None => Ok(OpenCodeEventSemantics::Private),
            Some(OpenCodePartEffect::Process(process)) => {
                let kind = match &process {
                    OpenCodeProcessEffect::Running { .. } => {
                        ConversationNotificationKind::Process {
                            label: "Command".to_owned(),
                            state: ProcessState::Running,
                        }
                    }
                    OpenCodeProcessEffect::Exited { exit_code, .. } => {
                        ConversationNotificationKind::Process {
                            label: "Command".to_owned(),
                            state: ProcessState::Exited {
                                exit_code: *exit_code,
                            },
                        }
                    }
                };
                Ok(OpenCodeEventSemantics::Process { process, kind })
            }
            Some(OpenCodePartEffect::Files(files)) => self.files(
                self.require_workspace(workspace_directory)?,
                files
                    .into_iter()
                    .map(|file| OpenCodeFileFact::patch(PathBuf::from(file)))
                    .collect(),
            ),
        }
    }

    fn diff(
        &self,
        event: &OpenCodeSessionDiffEvent,
        workspace_directory: Option<&Path>,
    ) -> Result<OpenCodeEventSemantics, SdkError> {
        self.files(
            self.require_workspace(workspace_directory)?,
            event
                .files()
                .into_iter()
                .map(|file| OpenCodeFileFact::patch(PathBuf::from(file)))
                .collect(),
        )
    }

    fn files(
        &self,
        workspace_directory: &Path,
        files: Vec<OpenCodeFileFact>,
    ) -> Result<OpenCodeEventSemantics, SdkError> {
        let mut notifications = Vec::with_capacity(files.len());
        for file in files {
            notifications.push(ConversationNotificationKind::FileChanged {
                path: self.workspace_relative(workspace_directory, &file.path)?,
                summary: file.summary,
            });
        }
        if notifications.is_empty() {
            Ok(OpenCodeEventSemantics::Private)
        } else {
            Ok(OpenCodeEventSemantics::Notifications(notifications))
        }
    }

    fn private_global(&self, event: OpenCodePrivateGlobalEvent) -> OpenCodeEventSemantics {
        match event {
            OpenCodePrivateGlobalEvent::McpToolsChanged => {
                OpenCodeEventSemantics::CatalogInvalidated
            }
            OpenCodePrivateGlobalEvent::ModelsRefreshed
            | OpenCodePrivateGlobalEvent::IntegrationUpdated
            | OpenCodePrivateGlobalEvent::IntegrationConnectionUpdated
            | OpenCodePrivateGlobalEvent::CatalogUpdated
            | OpenCodePrivateGlobalEvent::FileEdited
            | OpenCodePrivateGlobalEvent::ReferenceUpdated
            | OpenCodePrivateGlobalEvent::PluginAdded
            | OpenCodePrivateGlobalEvent::ProjectDirectoriesUpdated
            | OpenCodePrivateGlobalEvent::FileWatcherUpdated
            | OpenCodePrivateGlobalEvent::PtyCreated
            | OpenCodePrivateGlobalEvent::PtyUpdated
            | OpenCodePrivateGlobalEvent::PtyExited
            | OpenCodePrivateGlobalEvent::PtyDeleted
            | OpenCodePrivateGlobalEvent::InstallationUpdated
            | OpenCodePrivateGlobalEvent::InstallationUpdateAvailable
            | OpenCodePrivateGlobalEvent::LspUpdated
            | OpenCodePrivateGlobalEvent::McpBrowserOpenFailed
            | OpenCodePrivateGlobalEvent::ProjectUpdated
            | OpenCodePrivateGlobalEvent::TuiPromptAppend
            | OpenCodePrivateGlobalEvent::TuiCommandExecute
            | OpenCodePrivateGlobalEvent::TuiToastShow
            | OpenCodePrivateGlobalEvent::VcsBranchUpdated
            | OpenCodePrivateGlobalEvent::WorkspaceReady
            | OpenCodePrivateGlobalEvent::WorkspaceFailed
            | OpenCodePrivateGlobalEvent::WorkspaceStatus
            | OpenCodePrivateGlobalEvent::WorktreeReady
            | OpenCodePrivateGlobalEvent::WorktreeFailed => OpenCodeEventSemantics::Private,
        }
    }

    fn question_schema(&self, count: usize) -> Result<serde_json::Value, SdkError> {
        let count = u64::try_from(count)
            .map_err(|_| SdkError::invalid_state("question cardinality exceeds its bound"))?;
        let mut string = serde_json::Map::new();
        string.insert(
            "type".to_owned(),
            serde_json::Value::String("string".to_owned()),
        );
        let mut group = serde_json::Map::new();
        group.insert(
            "type".to_owned(),
            serde_json::Value::String("array".to_owned()),
        );
        group.insert("items".to_owned(), serde_json::Value::Object(string));
        let groups = (0..count)
            .map(|_| serde_json::Value::Object(group.clone()))
            .collect::<Vec<_>>();
        let mut answers = serde_json::Map::new();
        answers.insert(
            "type".to_owned(),
            serde_json::Value::String("array".to_owned()),
        );
        if !groups.is_empty() {
            answers.insert("prefixItems".to_owned(), serde_json::Value::Array(groups));
        }
        answers.insert("items".to_owned(), serde_json::Value::Bool(false));
        answers.insert("minItems".to_owned(), serde_json::Value::from(count));
        answers.insert("maxItems".to_owned(), serde_json::Value::from(count));
        let mut properties = serde_json::Map::new();
        properties.insert("answers".to_owned(), serde_json::Value::Object(answers));
        let mut schema = serde_json::Map::new();
        schema.insert(
            "$schema".to_owned(),
            serde_json::Value::String("https://json-schema.org/draft/2020-12/schema".to_owned()),
        );
        schema.insert(
            "type".to_owned(),
            serde_json::Value::String("object".to_owned()),
        );
        schema.insert(
            "properties".to_owned(),
            serde_json::Value::Object(properties),
        );
        schema.insert(
            "required".to_owned(),
            serde_json::Value::Array(vec![serde_json::Value::String("answers".to_owned())]),
        );
        schema.insert(
            "additionalProperties".to_owned(),
            serde_json::Value::Bool(false),
        );
        Ok(serde_json::Value::Object(schema))
    }

    fn require_workspace<'a>(
        &self,
        workspace_directory: Option<&'a Path>,
    ) -> Result<&'a Path, SdkError> {
        workspace_directory
            .ok_or_else(|| SdkError::invalid_state("file event has no workspace context"))
    }

    fn workspace_relative(
        &self,
        workspace_directory: &Path,
        native_path: &Path,
    ) -> Result<PathBuf, SdkError> {
        let workspace = crate::WorkspaceRoot::new(
            workspace_directory.to_path_buf(),
            crate::WorkspaceRootAccess::ReadOnly,
        )
        .map_err(|_| SdkError::invalid_state("file change workspace is not a backend-host root"))?;
        workspace.relative_native_file(native_path).map_err(|_| {
            SdkError::invalid_state("file change path does not identify a portable workspace file")
        })
    }
}

impl OpenCodeFileFact {
    fn patch(path: PathBuf) -> Self {
        Self {
            path,
            summary: None,
        }
    }
}
