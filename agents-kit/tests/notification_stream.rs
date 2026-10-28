use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, UNIX_EPOCH};

use agents_kit::KitError;
use agents_kit::identity::NotificationId;
use agents_kit::notification::{
    ConversationNotification, ConversationNotificationKind, ConversationNotificationReceiver,
};
use agents_kit::prompt::{PromptResponse, UsageInfo};
use futures_util::Stream;

struct NotificationFixture;

struct NonFusedTerminalStream {
    terminal_returned: bool,
}

impl NotificationFixture {
    fn id(suffix: u8) -> NotificationId {
        format!("notification_{suffix:032x}")
            .parse()
            .expect("fixture notification id must be canonical")
    }

    fn notification(suffix: u8, kind: ConversationNotificationKind) -> ConversationNotification {
        ConversationNotification::new(
            Self::id(suffix),
            UNIX_EPOCH + Duration::from_secs(u64::from(suffix)),
            kind,
        )
        .expect("fixture notification must be valid")
    }
}

impl Stream for NonFusedTerminalStream {
    type Item = Result<ConversationNotification, KitError>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal_returned {
            panic!("wrapper polled a non-fused extension stream after terminal None");
        }
        self.terminal_returned = true;
        Poll::Ready(None)
    }
}

#[tokio::test]
async fn arbitrary_extension_stream_preserves_retryable_items_errors_and_eof_in_order() {
    let first = NotificationFixture::notification(
        1,
        ConversationNotificationKind::Message {
            text: "first".to_owned(),
        },
    );
    let last = NotificationFixture::notification(
        2,
        ConversationNotificationKind::Warning {
            message: "last".to_owned(),
        },
    );
    let expected_first = first.id().clone();
    let expected_last = last.id().clone();
    let mut receiver = ConversationNotificationReceiver::from_stream(futures_util::stream::iter([
        Ok(first),
        Err(KitError::integration("ordered stream error")),
        Ok(last),
    ]));

    let first = receiver
        .recv()
        .await
        .expect("first stream item must exist")
        .expect("first stream item must succeed");
    assert_eq!(first.id(), &expected_first);

    let error = receiver
        .recv()
        .await
        .expect("error stream item must exist")
        .expect_err("second stream item must retain its error");
    assert!(matches!(
        (error).kind(),
        agents_kit::error::ErrorKind::Integration
    ));
    assert_eq!(error.message(), "ordered stream error");

    let last = receiver
        .recv()
        .await
        .expect("last stream item must exist")
        .expect("last stream item must succeed");
    assert_eq!(last.id(), &expected_last);
    assert!(
        receiver.recv().await.is_none(),
        "normal end-of-stream must remain distinct from an error item"
    );
    assert!(
        receiver.recv().await.is_none(),
        "the fused terminal observation must not invent another item"
    );
}

#[tokio::test]
async fn terminal_extension_errors_are_returned_once_and_fuse_the_receiver() {
    for error in [
        KitError::conflict("terminal conflict"),
        KitError::closed("terminal close"),
        KitError::unsupported("terminal unsupported"),
        KitError::not_yet_implemented("terminal incomplete adapter"),
        KitError::protocol("terminal protocol"),
        KitError::invalid_state("terminal invalid state"),
    ] {
        let unreachable = NotificationFixture::notification(
            9,
            ConversationNotificationKind::Message {
                text: "must remain unread".to_owned(),
            },
        );
        let mut receiver =
            ConversationNotificationReceiver::from_stream(futures_util::stream::iter([
                Err(error),
                Ok(unreachable),
            ]));

        assert!(
            receiver
                .recv()
                .await
                .expect("terminal error must be returned once")
                .is_err()
        );
        assert!(
            receiver.recv().await.is_none(),
            "terminal error category must fuse the receiver before another stream poll"
        );
    }
}

#[tokio::test]
async fn dropping_one_extension_receiver_drops_its_stream_without_background_work() {
    let (sender, stream) = tokio::sync::mpsc::unbounded_channel();
    let receiver = ConversationNotificationReceiver::from_stream(
        tokio_stream::wrappers::UnboundedReceiverStream::new(stream),
    );
    assert!(
        sender
            .send(Ok(NotificationFixture::notification(
                3,
                ConversationNotificationKind::Progress {
                    label: "queued".to_owned(),
                    detail: None,
                },
            )))
            .is_ok()
    );

    drop(receiver);
    assert!(
        sender
            .send(Ok(NotificationFixture::notification(
                4,
                ConversationNotificationKind::Progress {
                    label: "not delivered".to_owned(),
                    detail: None,
                },
            )))
            .is_err(),
        "receiver drop must release the caller-owned stream directly"
    );
}

#[tokio::test]
async fn receiver_fuses_terminal_none_without_repolling_the_extension_stream() {
    let mut receiver = ConversationNotificationReceiver::from_stream(NonFusedTerminalStream {
        terminal_returned: false,
    });

    assert!(receiver.recv().await.is_none());
    assert!(
        receiver.recv().await.is_none(),
        "the wrapper must own terminal fusion after the first None"
    );
}

#[test]
fn prompt_response_has_only_terminal_output_structured_value_and_usage() {
    let response = PromptResponse::from_parts(
        "terminal".to_owned(),
        Some(serde_json::json!({"answer": 42})),
        Some(UsageInfo::new(3, 5)),
    );
    let (output, structured, usage) = response.into_parts();

    assert_eq!(output, "terminal");
    assert_eq!(structured, Some(serde_json::json!({"answer": 42})));
    assert_eq!(usage, Some(UsageInfo::new(3, 5)));
}
