use crate::{CodexFixture, CodexPeer, FirstStart};
use agents_kit::ConversationBackend;
use agents_kit::error::ErrorKind;
use agents_kit::prompt::{PromptEvent, PromptRequest};
use futures_util::FutureExt;
use serde_json::{Value, json};
use std::time::Duration;

#[tokio::test]
async fn item_times_reject_wrong_types_and_overflow_in_live_and_history() {
    for item_type in ["agentMessage", "commandExecution", "functionCallOutput"] {
        for field in ["startedAtMs", "completedAtMs"] {
            for value in [
                json!(true),
                json!("1"),
                json!(1.5),
                json!(9223372036854775808_u64),
            ] {
                let mut item = json!({"type":item_type,"id":"item","text":" exact answer \n","phase":"final_answer",
                    "status":"completed","command":"private-command","cwd":std::env::temp_dir(),
                    "commandActions":[],"aggregatedOutput":"private-output","exitCode":0,
                    "name":"tool","output":"private-tool-output"});
                item[field] = value;
                CodexFixture::release_event("item/completed", json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":1,"item":item}), Some(ErrorKind::Protocol)).await;
                let result =
                    CodexFixture::recovered_result("completed", Value::Null, vec![item]).await;
                assert_eq!(
                    result.unwrap_err().kind(),
                    ErrorKind::Protocol,
                    "history {item_type}/{field}"
                );
            }
        }
    }
}

#[tokio::test]
async fn item_times_preserve_flat_items_and_completion_order() {
    for times in [
        json!({}),
        json!({"startedAtMs":null,"completedAtMs":null}),
        json!({"startedAtMs":i64::MIN,"completedAtMs":i64::MAX}),
    ] {
        for recovered in [false, true] {
            let mut first = json!({"type":"agentMessage","id":"first","text":"earlier completion","phase":"final_answer"});
            first
                .as_object_mut()
                .unwrap()
                .extend(times.as_object().unwrap().clone());
            let last = json!({"type":"agentMessage","id":"last","text":" exact final \n","phase":"final_answer","startedAtMs":1,"completedAtMs":-1});
            let peer = CodexPeer::start_with(if recovered {
                FirstStart::RecoveredTurn {
                    status: "completed".into(),
                    error: Value::Null,
                    items: vec![first.clone(), last.clone()],
                }
            } else {
                FirstStart::Ordinary
            })
            .await;
            let mut fixture = CodexFixture::build_with_peer(peer).await;
            let observed = std::panic::AssertUnwindSafe(async {
                let mut stream = fixture.conversation.prompt_stream(PromptRequest::new("timestamps").unwrap()).await.unwrap();
                let turn = fixture.peer.turn().await;
                if !recovered {
                    for item in [first, last] {
                        fixture.peer.notify("item/completed", json!({"threadId":"stream-thread","turnId":turn,"completedAtMs":1,"item":item})).await;
                    }
                    fixture.peer.notify("turn/completed", json!({"threadId":"stream-thread","turn":{"id":turn,"status":"completed","error":null,"items":[]}})).await;
                }
                assert!(matches!(tokio::time::timeout(Duration::from_secs(3), stream.recv()).await.unwrap(), Some(Ok(PromptEvent::Completed(response))) if response.output() == " exact final \n"));
                assert!(stream.recv().await.is_none());
            }).catch_unwind().await;
            fixture.finish_release(observed, None).await;
        }
    }
}

#[tokio::test]
async fn media_references_and_function_audio_remain_private_nonanswers() {
    let items = [
        json!({"type":"imageGeneration","id":"generated-file","status":"completed","revisedPrompt":null,"failure":null,"generationId":null,"fileId":"private-file"}),
        json!({"type":"imageGeneration","id":"generated-inline","status":"completed","revisedPrompt":null,"failure":null,"generationId":null,"result":"private-inline-data"}),
        json!({"type":"userMessage","id":"audio-file","content":[{"type":"audio","fileId":"private-audio"}]}),
        json!({"type":"userMessage","id":"audio-inline","content":[{"type":"audio","url":"data:audio/wav;base64,cHJpdmF0ZQ=="}]}),
        json!({"type":"functionCallOutput","id":"audio-tool","name":"tool","output":[{"type":"input_audio","audio_url":"data:audio/wav;base64,cHJpdmF0ZQ=="}]}),
    ];
    for item in items {
        CodexFixture::release_event(
            "item/completed",
            json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":1,"item":item}),
            None,
        )
        .await;
        let response = CodexFixture::recovered_result("completed", Value::Null,
            vec![item, json!({"type":"agentMessage","id":"answer","text":"ordinary final","phase":"final_answer"})]).await.unwrap();
        assert_eq!(response.output(), "ordinary final");
    }
}
