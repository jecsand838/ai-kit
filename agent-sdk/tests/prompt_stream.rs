use std::marker::PhantomPinned;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use agent_sdk::conversation::{ConversationInfo, FileRead};
use agent_sdk::identity::{ConversationId, NotificationId, WorkspaceId};
use agent_sdk::notification::{ConversationNotificationReceiver, NotificationResolution};
use agent_sdk::prompt::{PromptEvent, PromptRequest, PromptResponse, PromptStream, UsageInfo};
use agent_sdk::{ConversationBackend, SdkError};
use futures_util::{Stream, stream};
use serde_json::json;

// A direct caller implementation with no supplied Conversation handle,
// normalization ingress, Clone requirement, or extra behavior trait.
struct CallerConversation;

impl CallerConversation {
    fn response() -> PromptResponse {
        PromptResponse::from_parts(
            "authoritative final output".to_owned(),
            Some(json!({"complete": true})),
            Some(UsageInfo::new(17, 9)),
        )
    }
}

impl ConversationBackend for CallerConversation {
    fn id(&self) -> ConversationId {
        "conversation_00000000000000000000000000000071"
            .parse()
            .expect("caller conversation id")
    }

    async fn info(&self) -> Result<ConversationInfo, SdkError> {
        let workspace: WorkspaceId = "workspace_00000000000000000000000000000071"
            .parse()
            .expect("caller workspace id");
        ConversationInfo::new(self.id(), workspace, None)
    }

    async fn prompt(&self, _request: PromptRequest) -> Result<PromptResponse, SdkError> {
        Ok(Self::response())
    }

    async fn prompt_stream(&self, _request: PromptRequest) -> Result<PromptStream, SdkError> {
        Ok(PromptStream::from_stream(stream::iter(vec![
            Ok(PromptEvent::OutputDelta {
                text: "intermediate ".to_owned(),
            }),
            Ok(PromptEvent::OutputDelta {
                text: "assistant text".to_owned(),
            }),
            Ok(PromptEvent::Completed(Self::response())),
            Ok(PromptEvent::OutputDelta {
                text: "must never escape after completion".to_owned(),
            }),
        ])))
    }

    async fn abort(&self) -> Result<(), SdkError> {
        Ok(())
    }

    async fn read_file(&self, _path: PathBuf) -> Result<FileRead, SdkError> {
        Err(SdkError::unsupported("caller has no file access"))
    }

    fn notification_stream(&self) -> Result<ConversationNotificationReceiver, SdkError> {
        Ok(ConversationNotificationReceiver::from_stream(
            stream::empty(),
        ))
    }

    async fn resolve_notification(
        &self,
        _id: NotificationId,
        _resolution: NotificationResolution,
    ) -> Result<(), SdkError> {
        Err(SdkError::not_found("caller has no interaction"))
    }

    async fn close(self) -> Result<(), SdkError> {
        Ok(())
    }
}

#[tokio::test]
async fn direct_non_clone_conversation_stream_preserves_order_and_authoritative_terminal() {
    let conversation = CallerConversation;
    let mut output = conversation
        .prompt_stream(PromptRequest::new("stream caller output").expect("prompt"))
        .await
        .expect("direct caller stream");
    // Moving the public observation proves Send without imposing Clone, Unpin,
    // or any identity requirement on the caller's implementation.
    tokio::spawn(async move {
        assert!(matches!(output.recv().await,
            Some(Ok(PromptEvent::OutputDelta { text })) if text == "intermediate "));
        assert!(matches!(output.recv().await,
            Some(Ok(PromptEvent::OutputDelta { text })) if text == "assistant text"));
        let Some(Ok(PromptEvent::Completed(response))) = output.recv().await else {
            panic!("stream must publish the exact terminal response");
        };
        assert_eq!(response, CallerConversation::response());
        assert!(output.recv().await.is_none());
        assert!(output.recv().await.is_none());
    })
    .await
    .expect("stream observer joins");
    conversation.close().await.expect("caller closes");
}

#[tokio::test]
async fn prompt_stream_preserves_one_error_and_fuses_without_later_output() {
    let mut output = PromptStream::from_stream(stream::iter(vec![
        Ok(PromptEvent::OutputDelta {
            text: "partial".to_owned(),
        }),
        Err(SdkError::cancelled("caller cancelled this prompt")),
        Ok(PromptEvent::Completed(CallerConversation::response())),
    ]));
    assert!(matches!(
        output.recv().await,
        Some(Ok(PromptEvent::OutputDelta { .. }))
    ));
    assert!(
        matches!(output.recv().await, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Cancelled) && (error.message() == "caller cancelled this prompt"))
    );
    assert!(output.recv().await.is_none());
    assert!(output.recv().await.is_none());
}

#[tokio::test]
async fn prompt_stream_reports_premature_eof_once() {
    for events in [
        Vec::new(),
        vec![Ok(PromptEvent::OutputDelta {
            text: "incomplete".to_owned(),
        })],
    ] {
        let has_delta = !events.is_empty();
        let mut output = PromptStream::from_stream(stream::iter(events));
        if has_delta {
            assert!(matches!(
                output.recv().await,
                Some(Ok(PromptEvent::OutputDelta { .. }))
            ));
        }
        assert!(
            matches!(output.recv().await, Some(Err(error)) if matches!(error.kind(), agent_sdk::error::ErrorKind::Protocol))
        );
        assert!(output.recv().await.is_none());
        assert!(output.recv().await.is_none());
    }
}

struct Observation {
    dropped: Arc<AtomicBool>,
    _pinned: PhantomPinned,
}

impl Stream for Observation {
    type Item = Result<PromptEvent, SdkError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[test]
fn dropping_prompt_stream_releases_the_supplied_observation() {
    let dropped = Arc::new(AtomicBool::new(false));
    let output = PromptStream::from_stream(Observation {
        dropped: Arc::clone(&dropped),
        _pinned: PhantomPinned,
    });
    assert!(!dropped.load(Ordering::SeqCst));
    drop(output);
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn prompt_stream_preserves_structured_failure_evidence() {
    use agent_sdk::error::{ErrorDetail, ErrorKind, ErrorPhase, Operation};
    let error = SdkError::integration("authentication rejected")
        .with_context(Operation::Prompt, ErrorPhase::ModelGeneration)
        .with_detail(ErrorDetail::AuthenticationRejected)
        .with_http_status(401)
        .with_cause(SdkError::integration("safe cause").with_detail(ErrorDetail::RemoteRejected))
        .with_supplementary(
            SdkError::integration("safe cleanup")
                .with_context(Operation::Prompt, ErrorPhase::Cleanup),
        );
    let mut output = PromptStream::from_stream(stream::iter(vec![Err(error)]));
    let error = output.recv().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Integration);
    assert_eq!(error.context().operation, Operation::Prompt);
    assert_eq!(error.context().phase, ErrorPhase::ModelGeneration);
    assert_eq!(error.detail(), Some(&ErrorDetail::AuthenticationRejected));
    assert_eq!(error.http_status(), Some(401));
    assert_eq!(
        error.cause().unwrap().detail(),
        Some(&ErrorDetail::RemoteRejected)
    );
    assert_eq!(error.supplementary().len(), 1);
    assert!(output.recv().await.is_none());
}
