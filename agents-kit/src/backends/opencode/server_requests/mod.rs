use serde::Deserialize;

use crate::backends::opencode::notifications::{OpenCodeEventSemantics, OpenCodeNotifications};
use crate::backends::opencode::protocol::http::{OpenCodeHttp, OpenCodeMutationError};
use crate::backends::opencode::protocol::requests::{
    OpenCodePermissionReply, OpenCodeQuestionReply, OpenCodeRouting,
};
use crate::backends::opencode::protocol::schemas::{
    OpenCodePermissionRequest, OpenCodeQuestionRequest,
};
use crate::backends::opencode::state_store::records::{
    OpenCodeContinuation, OpenCodeContinuationKind, OpenCodeContinuationState,
    OpenCodeSessionBinding,
};
use crate::{KitError, NotificationResolution};

pub(crate) struct OpenCodeServerRequests {
    session_id: String,
    routing: OpenCodeRouting,
    notifications: OpenCodeNotifications,
}

pub(crate) struct OpenCodeNativeRequest {
    session_id: String,
    request_id: String,
    action: OpenCodeNativeAction,
}

enum OpenCodeNativeAction {
    Permission(OpenCodePermissionReply),
    QuestionReply(OpenCodeQuestionReply),
    QuestionReject,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeQuestionSubmission {
    #[serde(rename = "answers")]
    answers: Vec<Vec<String>>,
}

impl OpenCodeServerRequests {
    pub(crate) fn for_binding(binding: &OpenCodeSessionBinding) -> Result<Self, KitError> {
        Ok(Self {
            session_id: binding.require_session_id()?.to_owned(),
            routing: OpenCodeRouting::for_directory(binding.directory().to_path_buf())?,
            notifications: OpenCodeNotifications::new(),
        })
    }

    pub(crate) fn permission(
        &self,
        request: &OpenCodePermissionRequest,
    ) -> Result<OpenCodeEventSemantics, KitError> {
        self.verify_session(request.session_id())?;
        self.notifications.permission(request)
    }

    pub(crate) fn question(
        &self,
        request: &OpenCodeQuestionRequest,
    ) -> Result<OpenCodeEventSemantics, KitError> {
        self.verify_session(request.session_id())?;
        self.notifications.question(request)
    }

    pub(crate) fn native(
        &self,
        continuation: &OpenCodeContinuation,
        resolution: &NotificationResolution,
    ) -> Result<OpenCodeNativeRequest, KitError> {
        self.verify_session(continuation.session_id())?;
        if continuation.state() != OpenCodeContinuationState::Pending {
            return Err(KitError::conflict(
                "interaction is not pending native resolution",
            ));
        }
        resolution.validate()?;
        let action = match (continuation.kind(), resolution) {
            (OpenCodeContinuationKind::Permission, NotificationResolution::Allow) => {
                if continuation.question_count().is_some() {
                    return Err(KitError::invalid_state(
                        "permission continuation has question cardinality",
                    ));
                }
                OpenCodeNativeAction::Permission(OpenCodePermissionReply::allow_once())
            }
            (OpenCodeContinuationKind::Permission, NotificationResolution::Deny { reason }) => {
                if continuation.question_count().is_some() {
                    return Err(KitError::invalid_state(
                        "permission continuation has question cardinality",
                    ));
                }
                OpenCodeNativeAction::Permission(OpenCodePermissionReply::deny(reason.clone()))
            }
            (OpenCodeContinuationKind::Question, NotificationResolution::Submit { content }) => {
                let expected = continuation.question_count().ok_or_else(|| {
                    KitError::invalid_state("question continuation has no cardinality")
                })?;
                OpenCodeNativeAction::QuestionReply(
                    OpenCodeQuestionSubmission::parse(content, expected)?.into_reply(),
                )
            }
            (OpenCodeContinuationKind::Question, NotificationResolution::Dismiss { .. }) => {
                if continuation.question_count().is_none() {
                    return Err(KitError::invalid_state(
                        "question continuation has no cardinality",
                    ));
                }
                OpenCodeNativeAction::QuestionReject
            }
            (OpenCodeContinuationKind::Permission, _) | (OpenCodeContinuationKind::Question, _) => {
                return Err(KitError::invalid_input(
                    "notification resolution does not match the interaction",
                ));
            }
        };
        Ok(OpenCodeNativeRequest {
            session_id: self.session_id.clone(),
            request_id: continuation.request_id().to_owned(),
            action,
        })
    }

    pub(crate) fn cancellation(
        &self,
        continuation: &OpenCodeContinuation,
    ) -> Result<OpenCodeNativeRequest, KitError> {
        self.verify_session(continuation.session_id())?;
        if continuation.cancellation_owner().is_none() {
            return Err(KitError::conflict(
                "interaction cancellation has no durable dispatch owner",
            ));
        }
        Ok(OpenCodeNativeRequest {
            session_id: self.session_id.clone(),
            request_id: continuation.request_id().to_owned(),
            action: match continuation.kind() {
                OpenCodeContinuationKind::Permission => OpenCodeNativeAction::Permission(
                    OpenCodePermissionReply::deny("Conversation cancelled".to_owned()),
                ),
                OpenCodeContinuationKind::Question => OpenCodeNativeAction::QuestionReject,
            },
        })
    }

    pub(crate) async fn send(
        &self,
        http: &OpenCodeHttp,
        request: OpenCodeNativeRequest,
    ) -> Result<(), OpenCodeMutationError> {
        self.verify_session(&request.session_id)
            .map_err(OpenCodeMutationError::Rejected)?;
        match request.action {
            OpenCodeNativeAction::Permission(reply) => {
                http.permission_reply(&self.routing, &request.request_id, &reply)
                    .await
            }
            OpenCodeNativeAction::QuestionReply(reply) => {
                http.question_reply(&self.routing, &request.request_id, &reply)
                    .await
            }
            OpenCodeNativeAction::QuestionReject => {
                http.question_reject(&self.routing, &request.request_id)
                    .await
            }
        }
    }

    fn verify_session(&self, session_id: &str) -> Result<(), KitError> {
        if session_id != self.session_id {
            return Err(KitError::not_found(
                "interaction does not belong to this conversation",
            ));
        }
        Ok(())
    }
}

impl OpenCodeQuestionSubmission {
    fn parse(content: &serde_json::Value, expected: usize) -> Result<Self, KitError> {
        let submission = serde_json::from_value::<Self>(content.clone())
            .map_err(|_| KitError::invalid_input("submitted question response is malformed"))?;
        let min_items = expected;
        let max_items = expected;
        if submission.answers.len() < min_items || submission.answers.len() > max_items {
            return Err(KitError::invalid_input(
                "submitted question response has the wrong cardinality",
            ));
        }
        Ok(submission)
    }

    fn into_reply(self) -> OpenCodeQuestionReply {
        OpenCodeQuestionReply::new(self.answers)
    }
}
