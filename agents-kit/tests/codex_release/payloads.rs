use crate::CodexFixture;
use agents_kit::ConversationBackend;
use agents_kit::error::{ErrorDetail, ErrorKind};
use agents_kit::prompt::PromptRequest;
use futures_util::FutureExt;
use serde_json::{Value, json};
use std::time::Duration;

#[tokio::test]
async fn selected_media_malformed_fields_fail_in_live_and_history() {
    let malformed = [
        json!({"type":"imageGeneration","id":"image","status":"completed","fileId":42}),
        json!({"type":"imageGeneration","id":"image","status":42,"result":""}),
        json!({"type":"imageGeneration","id":"image","status":"completed","result":"","inputImages":[{"path":"relative","sha256":"private"}]}),
        json!({"type":"imageGeneration","id":"image","status":"completed","result":"","inputImages":[{"path":"/private/input","sha256":false}]}),
        json!({"type":"imageGeneration","id":"image","status":"completed","result":"","transparentBackground":"false"}),
        json!({"type":"imageGeneration","id":"image","status":"failed","result":"","failure":{"type":"usageLimitExceeded","limitId":"private","resetsAt":true}}),
        json!({"type":"imageGeneration","id":"image","status":"completed","result":"","savedPath":"relative"}),
        json!({"type":"userMessage","id":"user","content":[{"type":"image","fileId":false}]}),
        json!({"type":"userMessage","id":"user","content":[{"type":"image","url":"private","detail":false}]}),
        json!({"type":"userMessage","id":"user","content":[{"type":"audio","fileId":42}]}),
        json!({"type":"userMessage","id":"user","content":[{"type":"localAudio","path":false}]}),
        json!({"type":"userMessage","id":"user","content":false}),
    ];
    for item in malformed {
        CodexFixture::release_event(
            "item/completed",
            json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":0,"item":item}),
            Some(ErrorKind::Protocol),
        )
        .await;
        assert_eq!(
            CodexFixture::recovered_result("completed", Value::Null, vec![item.clone()])
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Protocol,
            "{item}"
        );
    }
}

#[tokio::test]
async fn selected_media_migration_and_input_metadata_stay_private() {
    let input_path = std::env::temp_dir().join("private-input");
    let output_path = std::env::temp_dir().join("private-output");
    for item in [
        json!({"type":"imageGeneration","id":"image","status":"native-open-status","fileId":"private","result":"","inputImages":[{"path":input_path,"sha256":"private"}],"savedPath":output_path,"transparentBackground":true,"generationId":"private"}),
        json!({"type":"imageGeneration","id":"image","status":"failed","result":"","failure":{"type":"usageLimitExceeded","limitId":"private","resetsAt":-1}}),
        json!({"type":"userMessage","id":"user","content":[{"type":"image","fileId":"private","detail":"original"},{"type":"localAudio","path":"relative-native-path"},{"type":"audio","url":"private"}]}),
    ] {
        CodexFixture::release_event(
            "item/completed",
            json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":0,"item":item}),
            None,
        )
        .await;
        assert_eq!(CodexFixture::recovered_result("completed", Value::Null, vec![item, json!({"type":"agentMessage","id":"answer","text":"ordinary final","phase":"final_answer"})]).await.unwrap().output(), "ordinary final");
    }
}

#[tokio::test]
async fn selected_media_identity_is_required_in_live_and_history() {
    for base in [
        json!({"type":"imageGeneration","id":"image","status":"completed","result":""}),
        json!({"type":"userMessage","id":"user","content":[]}),
    ] {
        for id in [
            None,
            Some(json!(42)),
            Some(json!("")),
            Some(json!(" spaced ")),
        ] {
            let mut item = base.clone();
            if let Some(id) = id {
                item["id"] = id;
            } else {
                item.as_object_mut().unwrap().remove("id");
            }
            CodexFixture::release_event(
                "item/completed",
                json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":0,"item":item}),
                Some(ErrorKind::Protocol),
            )
            .await;
            assert_eq!(CodexFixture::recovered_result("completed", Value::Null, vec![item, json!({"type":"agentMessage","id":"answer","text":"ordinary final","phase":"final_answer"})]).await.unwrap_err().kind(), ErrorKind::Protocol);
        }
    }
}

#[tokio::test]
async fn selected_media_client_identity_is_typed_in_live_and_history() {
    let item = json!({"type":"userMessage","id":"user","clientId":42,"content":[]});
    CodexFixture::release_event(
        "item/completed",
        json!({"threadId":"stream-thread","turnId":"turn","completedAtMs":0,"item":item}),
        Some(ErrorKind::Protocol),
    )
    .await;
    assert_eq!(
        CodexFixture::recovered_result("completed", Value::Null, vec![item])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Protocol
    );
}

impl CodexFixture {
    fn selected_hook() -> Value {
        json!({"threadId":"stream-thread","turnId":"turn","run":{"id":"private","eventName":"preModelRequest","handlerType":"command","executionMode":"async","scope":"turn","sourcePath":"remote://private/hook","displayOrder":-1,"status":"running","startedAt":-1,"completedAt":null,"durationMs":null,"entries":[{"kind":"context","text":"secret-token"}]}})
    }
}

#[tokio::test]
async fn selected_hook_metadata_is_typed_and_private() {
    for method in ["hook/started", "hook/completed"] {
        for event in ["preModelRequest", "postModelRequest"] {
            let mut params = CodexFixture::selected_hook();
            params["run"]["eventName"] = json!(event);
            CodexFixture::release_event(method, params.clone(), None).await;
            for (field, value) in [
                ("eventName", json!("future")),
                ("sourcePath", json!(42)),
                ("source", json!("future")),
                ("startedAt", json!(true)),
                ("durationMs", json!(1.5)),
                ("entries", json!([{"kind":"future","text":"private"}])),
            ] {
                let mut invalid = params.clone();
                invalid["run"][field] = value;
                CodexFixture::release_event(method, invalid, Some(ErrorKind::Protocol)).await;
            }
        }
        let mut missing = CodexFixture::selected_hook();
        missing.as_object_mut().unwrap().remove("run");
        CodexFixture::release_event(method, missing, Some(ErrorKind::Protocol)).await;
    }
}

#[tokio::test]
async fn selected_process_drain_status_is_private_and_typed() {
    for drain in [
        None,
        Some(Value::Null),
        Some(json!(false)),
        Some(json!(true)),
        Some(json!("false")),
        Some(json!(0)),
    ] {
        let mut params = json!({"processHandle":"private","exitCode":0,"stdout":"private","stderr":"private","stdoutCapReached":false,"stderrCapReached":false});
        let expected = drain
            .as_ref()
            .filter(|v| !v.is_null() && !v.is_boolean())
            .map(|_| ErrorKind::Protocol);
        if let Some(drain) = drain {
            params["outputDrainTimedOut"] = drain;
        }
        CodexFixture::release_event("process/exited", params, expected).await;
    }
}

#[tokio::test]
async fn structured_rejections_preserve_reader_and_explicit_next_prompt() {
    for (data, kind, detail) in [
        (
            json!({"type":"modelProviderMismatch","modelProvider":"secret-token","currentModelProvider":"private"}),
            ErrorKind::Integration,
            ErrorDetail::PreconditionFailed,
        ),
        (
            json!({"type":"modelProviderNotConfigured","modelProvider":"secret-token"}),
            ErrorKind::Integration,
            ErrorDetail::PreconditionFailed,
        ),
        (
            json!({"type":"modelProviderPolicyConflict","modelProvider":"secret-token","requiredModelProvider":"private"}),
            ErrorKind::Integration,
            ErrorDetail::PolicyRejected,
        ),
        (
            json!({"type":"future","modelProvider":false}),
            ErrorKind::Integration,
            ErrorDetail::InvalidRequest,
        ),
        (
            json!({"type":"modelProviderMismatch","modelProvider":"secret-token"}),
            ErrorKind::Protocol,
            ErrorDetail::MalformedData,
        ),
        (
            json!({"type":"modelProviderNotConfigured","modelProvider":false}),
            ErrorKind::Protocol,
            ErrorDetail::MalformedData,
        ),
        (
            json!({"type":"modelProviderPolicyConflict","modelProvider":"secret-token","requiredModelProvider":42}),
            ErrorKind::Protocol,
            ErrorDetail::MalformedData,
        ),
    ] {
        let mut fixture = CodexFixture::build().await;
        let observed = std::panic::AssertUnwindSafe(async {
            fixture.peer.rejections.lock().unwrap().insert("fs/readFile".into(), json!({"code":-32600,"message":"secret-token","data":data}));
            let error = tokio::time::timeout(Duration::from_secs(3), fixture.conversation.read_file(std::path::PathBuf::from("answer.txt"))).await.unwrap().unwrap_err();
            assert_eq!(error.kind(), kind, "{data}: {error:?}");
            assert_eq!(error.detail(), Some(&detail));
            assert!(!format!("{error} {error:?}").contains("secret-token"));
            fixture.peer.rejections.lock().unwrap().clear();
            let prompt = fixture.conversation.prompt(PromptRequest::new("next").unwrap());
            tokio::pin!(prompt);
            let turn = tokio::select! { turn = fixture.peer.turn() => turn, result = &mut prompt => panic!("admission must remain reusable: {result:?}") };
            fixture.peer.complete(&turn, "next succeeded").await;
            assert_eq!(prompt.await.unwrap().output(), "next succeeded");
            assert_eq!(fixture.peer.requests.lock().unwrap().iter().filter(|r| r["method"] == "turn/start").count(), 1, "only the explicit prompt; no resend");
        }).catch_unwind().await;
        fixture.finish_release(observed, None).await;
    }
}
