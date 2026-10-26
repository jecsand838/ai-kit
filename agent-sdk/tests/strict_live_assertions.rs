mod caller_shell;
mod codex_input_evidence;
mod custom_provider_evidence;
mod custom_provider_observation;
mod custom_provider_observation_tests;
mod knowledge_evidence;
mod native_cleanup;
mod opencode_question_evidence;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::FutureExt;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::Instant;

use native_cleanup::NativeCleanup;
use opencode_question_evidence::readback::{FreshOutputExpectation, NativeQuestionReadback};
use opencode_question_evidence::{QuestionEvidence, QuestionExpectation, QuestionGrade};

#[tokio::test]
async fn codex_input_capture_file_boundary_and_deadline() {
    use codex_input_evidence::CodexInputObservation;
    let directory =
        std::env::temp_dir().join(format!("codex-rollout-control-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let home = std::fs::canonicalize(&directory).unwrap();
    let observed = NativeCleanup::observe(Instant::now() + Duration::from_secs(10), async {
        let sessions = home.join("sessions");
        std::fs::create_dir(&sessions).unwrap();
        let path = sessions.join("owned.jsonl");
        let bytes = b"{\"type\":\"example\"}\n";
        std::fs::write(&path, bytes).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert_eq!(
            CodexInputObservation::read_rollout(&home, &path, deadline)
                .await
                .unwrap(),
            bytes
        );
        assert!(
            CodexInputObservation::read_rollout(
                &home,
                std::path::Path::new("relative.jsonl"),
                deadline
            )
            .await
            .is_err()
        );
        let foreign = home.join("foreign.jsonl");
        std::fs::write(&foreign, bytes).unwrap();
        assert!(
            CodexInputObservation::read_rollout(&home, &foreign, deadline)
                .await
                .is_err()
        );
        assert!(
            CodexInputObservation::read_rollout(&home, &sessions, deadline)
                .await
                .is_err()
        );
        assert_eq!(
            CodexInputObservation::read_rollout(&home, &path, Instant::now())
                .await
                .unwrap_err(),
            "rollout_snapshot_deadline"
        );
        let metadata = std::fs::metadata(&path).unwrap();
        let replacement = sessions.join("replacement.jsonl");
        std::fs::write(&replacement, bytes).unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        assert!(!CodexInputObservation::same_rollout_metadata(
            &metadata,
            &std::fs::metadata(&path).unwrap()
        ));
        #[cfg(unix)]
        {
            let link = sessions.join("linked.jsonl");
            std::os::unix::fs::symlink(&path, &link).unwrap();
            assert!(
                CodexInputObservation::read_rollout(&home, &link, deadline)
                    .await
                    .is_err()
            );
            let redirected = sessions.join("redirected");
            std::os::unix::fs::symlink(&sessions, &redirected).unwrap();
            assert!(
                CodexInputObservation::read_rollout(
                    &home,
                    &redirected.join("owned.jsonl"),
                    deadline
                )
                .await
                .is_err()
            );
        }
        std::fs::File::create(&path)
            .unwrap()
            .set_len(8 * 1024 * 1024 + 1)
            .unwrap();
        assert_eq!(
            CodexInputObservation::read_rollout(&home, &path, deadline)
                .await
                .unwrap_err(),
            "rollout_byte_bound"
        );
        let stderr = home.join("stderr.log");
        std::fs::write(&stderr, b"safe local warning").unwrap();
        assert_eq!(
            CodexInputObservation::read_stderr(&stderr, deadline)
                .await
                .unwrap(),
            b"safe local warning"
        );
        assert!(
            CodexInputObservation::read_stderr(&home.join("missing.log"), deadline)
                .await
                .is_err()
        );
        std::fs::File::create(&stderr)
            .unwrap()
            .set_len(1024 * 1024 + 1)
            .unwrap();
        assert_eq!(
            CodexInputObservation::read_stderr(&stderr, deadline)
                .await
                .unwrap_err(),
            "stderr_byte_bound"
        );
    })
    .await;
    let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
    cleanup.settle(tokio::fs::remove_dir_all(&directory)).await;
    cleanup.finish(observed);
}

#[test]
fn codex_input_capture_preserves_original_callback_assertions() {
    use codex_input_evidence::CodexInputObservation;
    let mut observation = CodexInputObservation::default();
    observation.observe_native(&json!({"id":100,"method":"item/tool/requestUserInput",
        "params":{"isBlocking":false}}));
    observation.finish_native_forwarding(true);
    observation.observe_sdk(&json!({"id":100,"error":{"code":-32601}}));
    observation.finish_sdk_forwarding(true);
    observation.assert_nonblocking_response();
    assert_eq!(observation.snapshot()["callback_responses"], 1);
    assert!(
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            observation.observe_native(&json!({"id":101,"method":"item/tool/requestUserInput",
            "params":{"isBlocking":false}}));
        }))
        .is_err()
    );
}

#[test]
fn codex_input_capture_records_passive_identity_and_forwarding() {
    use codex_input_evidence::CodexInputObservation;
    let caller_prompt = "Stay in Default mode. Call the native request_user_input tool exactly once with one question: id=choice, header=Choice, question=Choose a value?, options=[{label:First,description:First choice},{label:Second,description:Second choice}]. Do not imitate the tool call in text. After the tool returns, including cancellation or unsupported response, reply exactly INPUT_SETTLED. Do not call other tools.";
    let native_prompt = format!("Managed context:\n(none)\n\nUser request:\n{caller_prompt}");
    assert_eq!(caller_prompt.len(), 400);
    assert_eq!(native_prompt.len(), 439);
    let mut observation = CodexInputObservation::default();
    observation.prepare_expected(
        std::path::Path::new("/owned/cwd"),
        "gpt-6-luna",
        caller_prompt,
    );
    let frames = [
        (
            true,
            json!({"id":1,"method":"initialize","params":{"capabilities":{"experimentalApi":true}}}),
        ),
        (
            false,
            json!({"id":1,"result":{"userAgent":"codex/0.163.0"}}),
        ),
        (
            true,
            json!({"id":2,"method":"thread/start","params":{"cwd":"/owned/cwd","config":{"features.multi_agent":false,"features.multi_agent_v2":false},"dynamicTools":[]}}),
        ),
        (
            false,
            json!({"id":2,"result":{"thread":{"id":"thread-a","cwd":"/owned/cwd","path":"/owned/home/sessions/run.jsonl","parentThreadId":null},"model":"gpt-6-luna"}}),
        ),
        (
            true,
            json!({"id":3,"method":"experimentalFeature/list","params":{"threadId":"thread-a"}}),
        ),
        (
            false,
            json!({"id":3,"result":{"data":[{"name":"default_mode_request_user_input","enabled":true},{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":false}],"nextCursor":null}}),
        ),
        (
            true,
            json!({"id":4,"method":"turn/start","params":{"threadId":"thread-a","clientUserMessageId":"generation-a","cwd":"/owned/cwd","model":"gpt-6-luna","input":[{"type":"text","text":native_prompt,"textElements":[]}]}}),
        ),
        (
            false,
            json!({"id":4,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}),
        ),
        (
            false,
            json!({"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"completed","error":null,"items":[]}}}),
        ),
    ];
    for (sdk, frame) in &frames {
        if *sdk {
            observation.observe_sdk(frame);
            observation.finish_sdk_forwarding(true);
        } else {
            observation.observe_native(frame);
            observation.finish_native_forwarding(true);
        }
    }
    let snapshot = observation.snapshot();
    assert_eq!(snapshot["frames"][6]["submission"]["prompt"]["bytes"], 439);
    assert_eq!(
        snapshot["frames"][6]["submission"]["prompt"]["fingerprint"],
        "116bb674842d77b5"
    );
    assert_eq!(
        snapshot["wire_complete"], true,
        "the exact empty-context native rendering must bind to the original caller input"
    );
    assert_eq!(
        snapshot["prompt_binding"]["rendering"],
        "empty_managed_context"
    );
    assert_eq!(snapshot["prompt_binding"]["caller"]["bytes"], 400);
    assert_eq!(
        snapshot["prompt_binding"]["native"],
        snapshot["frames"][6]["submission"]["prompt"]
    );
    assert_eq!(snapshot["prompt_binding"]["exact"], true);
    assert_eq!(snapshot["frames"].as_array().unwrap().len(), 9);
    assert_eq!(
        snapshot["effective_features"]["default_mode_request_user_input"],
        true
    );
    assert_eq!(
        observation.rollout_path(),
        Some(std::path::PathBuf::from("/owned/home/sessions/run.jsonl"))
    );
    assert!(!snapshot.to_string().contains(caller_prompt));
    assert!(!snapshot.to_string().contains(&native_prompt));
    for wrong_prompt in [
        caller_prompt.to_owned(),
        format!("Managed context:\nchanged\n\nUser request:\n{caller_prompt}"),
        format!("{native_prompt} "),
        format!("{native_prompt}extra"),
    ] {
        let mut wrong = CodexInputObservation::default();
        wrong.prepare_expected(
            std::path::Path::new("/owned/cwd"),
            "gpt-6-luna",
            caller_prompt,
        );
        for (sdk, frame) in &frames {
            let mut frame = frame.clone();
            if frame["method"] == "turn/start" {
                frame["params"]["input"][0]["text"] = json!(wrong_prompt);
            }
            if *sdk {
                wrong.observe_sdk(&frame);
                wrong.finish_sdk_forwarding(true);
            } else {
                wrong.observe_native(&frame);
                wrong.finish_native_forwarding(true);
            }
        }
        assert_eq!(wrong.snapshot()["wire_complete"], false);
        assert_eq!(wrong.snapshot()["prompt_binding"]["exact"], false);
    }
    for (id, include_turns, turns) in [(5, false, None), (6, true, Some(json!([])))] {
        observation.observe_sdk(&json!({"id":id,"method":"thread/read","params":{"threadId":"thread-a","includeTurns":include_turns}}));
        observation.finish_sdk_forwarding(true);
        let mut result = json!({"thread":{"id":"thread-a"}});
        if let Some(turns) = turns {
            result["thread"]["turns"] = turns;
        }
        observation.observe_native(&json!({"id":id,"result":result}));
        observation.finish_native_forwarding(true);
        assert_eq!(observation.snapshot()["wire_complete"], true);
    }
    observation.observe_sdk(&json!({"id":7,"method":"thread/read","params":{"threadId":"thread-a","includeTurns":true}}));
    observation.finish_sdk_forwarding(true);
    observation.observe_native(&json!({"id":7,"result":{"thread":{"id":"thread-a"}}}));
    observation.finish_native_forwarding(true);
    assert_eq!(observation.snapshot()["wire_complete"], false);
    observation.mark_incomplete("control-forwarding-failure");
    assert_eq!(observation.snapshot()["capture_complete"], false);
}

#[test]
fn codex_input_capture_reads_correlated_rollout_without_native_prose() {
    use codex_input_evidence::CodexInputObservation;
    let make_observation = |callback: bool| {
        let mut observation = CodexInputObservation::default();
        for (sdk, frame) in [
            (
                true,
                json!({"id":1,"method":"initialize","params":{"capabilities":{"experimentalApi":true}}}),
            ),
            (
                false,
                json!({"id":1,"result":{"userAgent":"codex/0.163.0"}}),
            ),
            (
                true,
                json!({"id":2,"method":"thread/start","params":{"cwd":"/owned/cwd","config":{},"dynamicTools":[]}}),
            ),
            (
                false,
                json!({"id":2,"result":{"thread":{"id":"thread-a","cwd":"/owned/cwd","path":"/owned/home/sessions/run.jsonl","parentThreadId":null},"model":"gpt-6-luna"}}),
            ),
            (
                true,
                json!({"id":3,"method":"experimentalFeature/list","params":{"threadId":"thread-a"}}),
            ),
            (
                false,
                json!({"id":3,"result":{"data":[{"name":"default_mode_request_user_input","enabled":true},{"name":"multi_agent","enabled":false},{"name":"multi_agent_v2","enabled":false}],"nextCursor":null}}),
            ),
            (
                true,
                json!({"id":4,"method":"turn/start","params":{"threadId":"thread-a","clientUserMessageId":"generation-a","cwd":"/owned/cwd","model":"gpt-6-luna","input":[{"type":"text","text":"Managed context:\n(none)\n\nUser request:\nunchanged prompt","textElements":[]}]}}),
            ),
            (
                false,
                json!({"id":4,"result":{"turn":{"id":"turn-a","status":"inProgress"}}}),
            ),
            (
                false,
                json!({"method":"turn/completed","params":{"threadId":"thread-a","turn":{"id":"turn-a","status":"completed","error":null,"items":[]}}}),
            ),
        ] {
            if frame["method"] == "turn/start" {
                observation.prepare_expected(
                    std::path::Path::new("/owned/cwd"),
                    "gpt-6-luna",
                    "unchanged prompt",
                );
            }
            if callback && frame["method"] == "turn/completed" {
                observation.observe_native(&json!({"id":100,"method":"item/tool/requestUserInput","params":{"threadId":"thread-a","turnId":"turn-a","itemId":"call-a","isBlocking":false,"questions":[]}}));
                observation.finish_native_forwarding(true);
                observation.observe_sdk(&json!({"id":100,"error":{"code":-32601}}));
                observation.finish_sdk_forwarding(true);
            }
            if sdk {
                observation.observe_sdk(&frame);
                observation.finish_sdk_forwarding(true);
            } else {
                observation.observe_native(&frame);
                observation.finish_native_forwarding(true);
            }
        }
        observation.observe_stderr(b"");
        observation
    };
    let mut observation = make_observation(false);
    let arguments = json!({"questions":[{"id":"choice","header":"Choice","question":"private question","options":[{"label":"First","description":"one"},{"label":"Second","description":"two"}]}]}).to_string();
    let records = vec![
        json!({"type":"session_meta","payload":{"id":"thread-a","session_id":"thread-a","cwd":"/owned/cwd","cli_version":"0.163.0","source":"vscode","parent_thread_id":null}}),
        json!({"type":"turn_context","payload":{"turn_id":"turn-a","model":"gpt-6-luna","cwd":"/owned/cwd","collaboration_mode":{"mode":"default"}}}),
        json!({"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Managed context:\n(none)\n\nUser request:\nunchanged prompt"}]}}),
        json!({"type":"response_item","payload":{"type":"function_call","name":"request_user_input","call_id":"call-a","arguments":arguments}}),
        json!({"type":"response_item","payload":{"type":"function_call_output","call_id":"call-a","output":"request_user_input requires non-empty options for every question"}}),
        json!({"type":"response_item","payload":{"type":"reasoning","summary":[{"text":"private reasoning"}]}}),
        json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-a","last_agent_message":"INPUT_SETTLED","error":null}}),
    ];
    let encode = |records: &[Value]| {
        let mut bytes = Vec::new();
        for record in records {
            serde_json::to_writer(&mut bytes, record).unwrap();
            bytes.push(b'\n');
        }
        bytes
    };
    let bytes = encode(&records);
    observation
        .observe_rollout(&bytes)
        .expect("own bounded rollout is correlated");
    let snapshot = observation.snapshot();
    assert_eq!(snapshot["rollout"]["complete"], true);
    assert_eq!(snapshot["rollout"]["calls"].as_array().unwrap().len(), 1);
    assert_eq!(
        snapshot["rollout"]["calls"][0]["output"]["error_class"],
        "missing_options"
    );
    let exported = snapshot.to_string();
    for private in [
        "private question",
        "private reasoning",
        "request_user_input requires non-empty options for every question",
        "unchanged prompt",
    ] {
        assert!(!exported.contains(private));
    }
    assert_eq!(
        snapshot["capture_complete"], true,
        "absence of callback is evidence, not capture loss"
    );
    assert!(
        std::panic::catch_unwind(AssertUnwindSafe(
            || observation.assert_nonblocking_response()
        ))
        .is_err()
    );

    let mut completed_callback = records.clone();
    completed_callback[4]["payload"]["output"] = json!("{\"answers\":{}}");
    let mut observed = make_observation(true);
    observed
        .observe_rollout(&encode(&completed_callback))
        .unwrap();
    observed.assert_nonblocking_response();
    assert_eq!(observed.snapshot()["capture_complete"], true);
    assert_eq!(observed.snapshot()["rollout"]["callback_correlated"], true);
    assert_eq!(observed.snapshot()["callback_responses"], 1);
    // RequestUserInput is transient in pinned native policy; its absence is not a lost callback.
    assert!(
        observed.snapshot()["rollout"]["request_user_input_events"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    completed_callback.insert(4, json!({"type":"event_msg","payload":{"type":"request_user_input","call_id":"call-a","turn_id":"turn-a","isBlocking":false,"questions":[]}}));
    let mut observed = make_observation(true);
    observed
        .observe_rollout(&encode(&completed_callback))
        .unwrap();
    assert_eq!(
        observed.snapshot()["rollout"]["request_user_input_events"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(observed.snapshot()["capture_complete"], true);
    let mut no_call = records.clone();
    no_call.remove(4);
    no_call.remove(3);
    let mut observed = make_observation(false);
    observed.observe_rollout(&encode(&no_call)).unwrap();
    assert_eq!(observed.snapshot()["capture_complete"], true);
    assert!(
        observed.snapshot()["rollout"]["calls"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        std::panic::catch_unwind(AssertUnwindSafe(|| observed.assert_nonblocking_response()))
            .is_err()
    );

    // Pinned app-server selects paginated history for new persisted root threads.
    // A full root file starts ordinal zero and contains every consecutive record.
    let mut paginated = records.clone();
    paginated[0]["payload"]["history_mode"] = json!("paginated");
    paginated.insert(1, json!({"type":"world_state","payload":{"full":true,"state":{"private_system_context":"must not be retained"}}}));
    let usage = json!({"input_tokens":10,"cached_input_tokens":0,"output_tokens":2,"reasoning_output_tokens":0,"total_tokens":12});
    paginated.insert(paginated.len() - 1, json!({"type":"token_usage_record","payload":{
        "thread_id":"thread-a","session_id":"thread-a","turn_id":"turn-a","root_turn_id":"turn-a","response_id":"response-private",
        "usage":usage,"turn_token_usage":usage,"thread_token_usage":usage}}));
    for (ordinal, record) in paginated.iter_mut().enumerate() {
        record["ordinal"] = json!(ordinal);
        record["timestamp"] = json!("2026-10-22T18:00:00.000Z");
    }
    let mut observed = make_observation(false);
    observed
        .observe_rollout(&encode(&paginated))
        .expect("complete paginated native root history must be captured");
    assert_eq!(observed.snapshot()["capture_complete"], true);
    assert_eq!(observed.snapshot()["rollout"]["history_mode"], "paginated");
    assert_eq!(observed.snapshot()["rollout"]["ordinals_complete"], true);
    assert_eq!(
        observed.snapshot()["rollout"]["record_kinds"]["world_state"],
        1
    );
    assert_eq!(
        observed.snapshot()["rollout"]["record_kinds"]["token_usage_record"],
        1
    );
    for private in [
        "private_system_context",
        "must not be retained",
        "response-private",
    ] {
        assert!(!observed.snapshot().to_string().contains(private));
    }

    for (index, ordinal) in [
        (0, json!(1)),
        (2, json!(1)),
        (2, json!(3)),
        (2, json!(-1)),
        (2, json!(1.5)),
        (2, json!("2")),
        (2, Value::Null),
    ] {
        let mut changed = paginated.clone();
        changed[index]["ordinal"] = ordinal;
        let mut observed = make_observation(false);
        assert_eq!(
            observed.observe_rollout(&encode(&changed)),
            Err("rollout_ordinal_discontinuity")
        );
    }
    let mut changed = paginated.clone();
    changed[2].as_object_mut().unwrap().remove("ordinal");
    let mut observed = make_observation(false);
    assert_eq!(
        observed.observe_rollout(&encode(&changed)),
        Err("rollout_ordinal_discontinuity")
    );
    for marker in [
        "history_base",
        "forked_from_ordinal_exclusive",
        "subagent_history_start_ordinal",
    ] {
        let mut changed = paginated.clone();
        changed[0]["payload"][marker] = json!(4);
        assert!(
            make_observation(false)
                .observe_rollout(&encode(&changed))
                .is_err()
        );
    }
    let mut changed = records.clone();
    changed[0]["ordinal"] = json!(0);
    assert_eq!(
        make_observation(false).observe_rollout(&encode(&changed)),
        Err("rollout_legacy_ordinal")
    );
    let usage_index = paginated.len() - 2;
    for key in ["thread_id", "session_id", "turn_id", "root_turn_id"] {
        let mut changed = paginated.clone();
        changed[usage_index]["payload"][key] = json!("foreign");
        assert_eq!(
            make_observation(false).observe_rollout(&encode(&changed)),
            Err("rollout_token_usage_identity_or_shape")
        );
    }
    let mut changed = paginated.clone();
    changed[1]["payload"]["state"] = json!([]);
    assert_eq!(
        make_observation(false).observe_rollout(&encode(&changed)),
        Err("rollout_world_state_shape")
    );

    // Native invalid arguments/errors remain complete facts; they are not observer parse failures.
    for arguments in ["{", "{}", "{\"questions\":[{\"options\":[]}]}"] {
        let mut changed = records.clone();
        changed[3]["payload"]["arguments"] = json!(arguments);
        let mut observed = make_observation(false);
        observed.observe_rollout(&encode(&changed)).unwrap();
        assert_eq!(observed.snapshot()["rollout"]["complete"], true);
        assert_eq!(
            observed.snapshot()["rollout"]["calls"][0]["argument_shape"]["json_valid"],
            arguments != "{"
        );
    }
    for options in [Value::Null, json!([])] {
        let mut changed = records.clone();
        changed[3]["payload"]["arguments"] = json!(json!({"questions":[{"id":"choice","header":"Choice","question":"private question","options":options}]}).to_string());
        let mut observed = make_observation(false);
        observed.observe_rollout(&encode(&changed)).unwrap();
        let shape = observed.snapshot()["rollout"]["calls"][0]["argument_shape"].clone();
        assert_eq!(shape["typed_fields_valid"], true);
        assert_eq!(shape["all_options_nonempty"], false);
    }
    for (output, class) in [
        (
            "request_user_input is unavailable in Default mode",
            "unavailable_default_mode",
        ),
        (
            "request_user_input can only be used by the root thread",
            "non_root_thread",
        ),
        (
            "failed to parse function arguments: private parse detail",
            "argument_parse_error",
        ),
        ("arbitrary native prose", "unknown"),
    ] {
        let mut changed = records.clone();
        changed[4]["payload"]["output"] = json!(output);
        let mut observed = make_observation(false);
        observed.observe_rollout(&encode(&changed)).unwrap();
        assert_eq!(
            observed.snapshot()["rollout"]["calls"][0]["output"]["error_class"],
            class
        );
        assert!(!observed.snapshot().to_string().contains(output));
    }
    let mut changed = records.clone();
    changed[4]["payload"]["output"] = json!([
        {"type":"input_text","text":"first private result"},
        {"type":"input_audio","audio_url":"private audio"},
        {"type":"encrypted_content","encrypted_content":"private encrypted text"},
        {"type":"input_text","text":"last private result"}
    ]);
    let mut observed = make_observation(false);
    observed.observe_rollout(&encode(&changed)).unwrap();
    let output = observed.snapshot()["rollout"]["calls"][0]["output"].clone();
    assert_eq!(output["kind"], "content_items");
    assert_eq!(output["items"].as_array().unwrap().len(), 4);
    assert_eq!(output["items"][1]["kind"], "input_audio");
    assert_eq!(output["items"][2]["kind"], "encrypted_content");
    assert!(!observed.snapshot().to_string().contains("private"));

    // Wrong native identity, ambiguous call pairing, incomplete files, and routed paths cannot qualify.
    let mut invalid = Vec::new();
    for (index, key, value) in [
        (0, "id", "foreign"),
        (0, "session_id", "foreign"),
        (1, "turn_id", "foreign"),
        (1, "model", "foreign"),
        (1, "cwd", "/foreign"),
        (4, "call_id", "foreign"),
        (6, "turn_id", "foreign"),
    ] {
        let mut changed = records.clone();
        changed[index]["payload"][key] = json!(value);
        invalid.push(encode(&changed));
    }
    for wrong_prompt in [
        "wrong prompt",
        "unchanged prompt",
        "Managed context:\nchanged\n\nUser request:\nunchanged prompt",
        "Managed context:\n(none)\n\nUser request:\nunchanged prompt ",
        "Managed context:\n(none)\n\nUser request:\nunchanged promptextra",
    ] {
        let mut changed = records.clone();
        changed[2]["payload"]["content"][0]["text"] = json!(wrong_prompt);
        invalid.push(encode(&changed));
    }
    let mut changed = records.clone();
    changed.insert(4, changed[3].clone());
    invalid.push(encode(&changed));
    let mut changed = records.clone();
    changed.insert(5, changed[4].clone());
    invalid.push(encode(&changed));
    let mut changed = records.clone();
    changed.remove(4);
    invalid.push(encode(&changed));
    let mut changed = records.clone();
    changed[3]["payload"]["type"] = json!("custom_tool_call");
    invalid.push(encode(&changed));
    let mut changed = records.clone();
    changed[0]["payload"]["history_base"] = json!({"reference":"foreign"});
    invalid.push(encode(&changed));
    let mut changed = bytes.clone();
    changed.pop();
    invalid.push(changed);
    invalid.push(b"{not JSON}\n".to_vec());
    invalid.push(vec![b'x'; 8 * 1_048_576 + 1]);
    for bytes in invalid {
        let mut observed = make_observation(false);
        assert!(observed.observe_rollout(&bytes).is_err());
        assert_eq!(observed.snapshot()["capture_complete"], false);
    }
    let mut observed = make_observation(false);
    observed.observe_native(&json!({"id":100,"method":"item/tool/requestUserInput","params":{"threadId":"thread-a","turnId":"turn-a","itemId":"wrong-call","isBlocking":false}}));
    observed.finish_native_forwarding(true);
    assert_eq!(
        observed.observe_rollout(&bytes),
        Err("rollout_callback_identity_mismatch")
    );

    let mut observed = make_observation(false);
    observed.observe_native(
        &json!({"method":"thread/tokenUsage/updated","params":{"threadId":"thread-a"}}),
    );
    assert_eq!(
        observed.snapshot()["wire_complete"],
        false,
        "unsettled forwarding is incomplete"
    );
    observed.finish_native_forwarding(false);
    assert_eq!(observed.snapshot()["wire_complete"], false);
    let mut observed = make_observation(false);
    observed.observe_stderr(b"failed to flush rollout before completing turn: private error");
    assert_eq!(observed.snapshot()["capture_complete"], false);
    assert!(!observed.snapshot().to_string().contains("private error"));
    let mut observed = make_observation(false);
    observed.observe_stderr(
        b"failed to append rollout writer; reopening and retrying: private first error",
    );
    assert!(
        observed.snapshot()["failures"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "native_rollout_persistence_warning")
    );
    assert!(
        !observed
            .snapshot()
            .to_string()
            .contains("private first error")
    );

    let path = std::env::temp_dir().join(format!(
        "codex-input-evidence-{}.json",
        uuid::Uuid::new_v4()
    ));
    observation.record_cleanup(true, true);
    let written = observation.export_after_cleanup(&path, false, true);
    let body = std::fs::read(&path);
    let duplicate = observation.export_after_cleanup(&path, false, true);
    let removed = std::fs::remove_file(&path);
    written.unwrap();
    removed.unwrap();
    assert!(duplicate.is_err());
    let exported: Value = serde_json::from_slice(&body.unwrap()).unwrap();
    assert_eq!(exported["capture_complete"], true);
    assert_eq!(exported["scenario_succeeded"], false);
    assert_eq!(exported["hierarchy_cleanup_succeeded"], true);
    assert_eq!(exported["process_cleanup_succeeded"], true);
    assert!(
        observation
            .export_after_cleanup(std::path::Path::new("relative.json"), false, true)
            .is_err()
    );
}

#[test]
fn knowledge_capture_retains_ordered_original_request() {
    use knowledge_evidence::KnowledgeEvidence;
    use provider_sdk::generation::{GenerateRequest, Message};

    let mut evidence = KnowledgeEvidence::default();
    evidence.prepare(
        "secondary",
        [
            "DB",
            "AGENT_A",
            "WORKSPACE",
            "CONVERSATION",
            "REOPEN",
            "AGENT_B",
        ]
        .map(str::to_owned),
        [1, 0, 0, 0, 0, 1],
    );
    let request = GenerateRequest::new(vec![
        Message::system(vec![provider_sdk::generation::ContentPart::Text {
            text: "DB reference".to_owned(),
        }])
        .unwrap(),
        Message::user_text("AGENT_B question").unwrap(),
    ])
    .unwrap();
    evidence.request(
        &provider_sdk::ModelId::new("exact-model").unwrap(),
        &request,
        false,
    );
    let snapshot = evidence.snapshot("knowledge-test", false, true);
    assert_eq!(
        snapshot["phases"][0]["request_counts"],
        serde_json::json!([1, 0, 0, 0, 0, 1])
    );
    assert_eq!(
        snapshot["phases"][0]["requests"][0]["messages"],
        serde_json::json!([
            {"role":"system", "text_parts":["DB reference"]}, {"role":"user", "text_parts":["AGENT_B question"]}
        ])
    );
}

#[test]
fn knowledge_capture_retains_asserted_phase() {
    use knowledge_evidence::KnowledgeEvidence;
    use provider_sdk::ModelId;
    use provider_sdk::generation::{
        FinishReason, GenerateRequest, GenerateResponse, Message, Usage,
    };

    let mut evidence = KnowledgeEvidence::default();
    evidence.prepare(
        "primary",
        [
            "DB",
            "AGENT_A",
            "WORKSPACE",
            "CONVERSATION",
            "REOPEN",
            "AGENT_B",
        ]
        .map(str::to_owned),
        [0; 6],
    );
    evidence.request(
        &provider_sdk::ModelId::new("exact-model").unwrap(),
        &GenerateRequest::new(vec![Message::user_text("question").unwrap()]).unwrap(),
        false,
    );
    let response = Ok(GenerateResponse {
        model: ModelId::new("exact-model").unwrap(),
        message: Message::assistant_text("unchanged").unwrap(),
        finish_reason: Some(FinishReason::Stop),
        citations: Vec::new(),
        artifacts: Vec::new(),
        hosted_tool_outputs: Vec::new(),
        usage: Usage::default(),
    });
    evidence.response(&response);
    evidence.agent_output("unchanged");
    evidence.assert_phase();
    let snapshot = evidence.snapshot("knowledge-test", true, true);
    assert_eq!(snapshot["phases"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["phases"][0]["assertions_succeeded"], true);
    assert_eq!(snapshot["phases"][0]["provider_text"], "unchanged");
}

#[test]
fn knowledge_capture_exports_failed_phase_original_outputs() {
    use knowledge_evidence::KnowledgeEvidence;
    use provider_sdk::ModelId;
    use provider_sdk::generation::{
        FinishReason, GenerateRequest, GenerateResponse, Message, Usage,
    };

    let mut evidence = KnowledgeEvidence::default();
    evidence.prepare(
        "secondary",
        [
            "DB",
            "AGENT_A",
            "WORKSPACE",
            "CONVERSATION",
            "REOPEN",
            "AGENT_B",
        ]
        .map(str::to_owned),
        [0; 6],
    );
    evidence.request(
        &provider_sdk::ModelId::new("exact-model").unwrap(),
        &GenerateRequest::new(vec![Message::user_text("question").unwrap()]).unwrap(),
        false,
    );
    let response = Ok(GenerateResponse {
        model: ModelId::new("exact-model").unwrap(),
        message: Message::assistant_text("wrong native answer").unwrap(),
        finish_reason: Some(FinishReason::Stop),
        citations: Vec::new(),
        artifacts: Vec::new(),
        hosted_tool_outputs: Vec::new(),
        usage: Usage::default(),
    });
    evidence.response(&response);
    evidence.agent_output("wrong native answer");
    let path =
        std::env::temp_dir().join(format!("knowledge-evidence-{}.json", uuid::Uuid::new_v4()));
    let exported = evidence.export_after_cleanup(&path, "knowledge-test", false, true);
    let bytes = std::fs::read(&path);
    let removed = std::fs::remove_file(&path);
    exported.unwrap();
    removed.unwrap();
    let snapshot: serde_json::Value = serde_json::from_slice(&bytes.unwrap()).unwrap();
    assert_eq!(snapshot["scenario_succeeded"], false);
    assert_eq!(snapshot["scenario_cleanup_succeeded"], true);
    assert_eq!(
        snapshot["phases"][0]["provider_text"],
        "wrong native answer"
    );
    assert_eq!(snapshot["phases"][0]["agent_text"], "wrong native answer");
    assert_eq!(snapshot["phases"][0]["assertions_succeeded"], false);
}

#[test]
fn knowledge_capture_bounds_original_request_without_mutating_it() {
    use knowledge_evidence::KnowledgeEvidence;
    use provider_sdk::generation::{GenerateRequest, Message};
    let mut evidence = KnowledgeEvidence::default();
    evidence.prepare(
        "primary",
        [
            "DB",
            "AGENT_A",
            "WORKSPACE",
            "CONVERSATION",
            "REOPEN",
            "AGENT_B",
        ]
        .map(str::to_owned),
        [0; 6],
    );
    let text = "x".repeat(65_537);
    let request = GenerateRequest::new(vec![Message::user_text(text.clone()).unwrap()]).unwrap();
    evidence.request(
        &provider_sdk::ModelId::new("exact-model").unwrap(),
        &request,
        false,
    );
    let snapshot = evidence.snapshot("knowledge-test", false, true);
    assert_eq!(snapshot["capture_complete"], false);
    assert_eq!(snapshot["capture_failure"], "request_text_bound");
    assert!(
        snapshot["phases"][0]["requests"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        matches!(&request.messages()[0].content()[0], provider_sdk::generation::ContentPart::Text {text: actual} if actual == &text)
    );
}

#[test]
fn knowledge_capture_retains_complete_provider_error_prefix_without_agent_output() {
    use knowledge_evidence::KnowledgeEvidence;
    use provider_sdk::error::{ErrorKind, ErrorPhase, Operation};
    use provider_sdk::generation::{GenerateRequest, Message};
    let mut evidence = KnowledgeEvidence::default();
    assert_eq!(
        evidence.snapshot("knowledge-test", false, true)["capture_complete"],
        false
    );
    evidence.prepare(
        "primary",
        [
            "DB",
            "AGENT_A",
            "WORKSPACE",
            "CONVERSATION",
            "REOPEN",
            "AGENT_B",
        ]
        .map(str::to_owned),
        [0; 6],
    );
    evidence.request(
        &provider_sdk::ModelId::new("exact-model").unwrap(),
        &GenerateRequest::new(vec![Message::user_text("question").unwrap()]).unwrap(),
        false,
    );
    let result = Err(provider_sdk::SdkError::new(
        ErrorKind::Transport,
        Operation::Generate,
        ErrorPhase::RequestSend,
    ));
    evidence.response(&result);
    let snapshot = evidence.snapshot("knowledge-test", false, true);
    assert_eq!(snapshot["capture_complete"], true);
    assert_eq!(snapshot["phases"][0]["provider_error_kind"], "Transport");
    assert_eq!(snapshot["phases"][0]["response_received"], true);
    assert!(snapshot["phases"][0]["agent_text"].is_null());
    assert!(matches!(&result, Err(error) if error.kind() == ErrorKind::Transport));
    evidence.response(&Err(provider_sdk::SdkError::new(
        ErrorKind::Timeout,
        Operation::Generate,
        ErrorPhase::RequestSend,
    )));
    let duplicate = evidence.snapshot("knowledge-test", false, true);
    assert_eq!(duplicate["capture_complete"], false);
    assert_eq!(duplicate["capture_failure"], "duplicate_response");
    assert_eq!(duplicate["phases"][0]["provider_error_kind"], "Transport");
}

#[tokio::test]
async fn knowledge_capture_export_failure_cannot_replace_primary_failure() {
    use knowledge_evidence::KnowledgeEvidence;
    let evidence = KnowledgeEvidence::default();
    let path =
        std::env::temp_dir().join(format!("knowledge-evidence-{}.json", uuid::Uuid::new_v4()));
    std::fs::write(&path, b"original evidence").unwrap();
    let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
    cleanup
        .settle(async { evidence.export_after_cleanup(&path, "knowledge-test", false, true) })
        .await;
    let bytes = std::fs::read(&path);
    let removed = std::fs::remove_file(&path);
    let observed: Result<Result<(), tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>> =
        Err(Box::new("original scenario failure".to_owned()));
    let failed =
        std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish(observed))).unwrap_err();
    removed.unwrap();
    assert_eq!(bytes.unwrap(), b"original evidence");
    assert_eq!(
        failed.downcast_ref::<String>().unwrap(),
        "original scenario failure"
    );
}

#[tokio::test]
async fn custom_anthropic_tap_retains_normalization_rule_without_changing_error() {
    use custom_provider_evidence::{AnthropicEvidence, CloudPhase};
    use futures_util::StreamExt;
    use provider_sdk::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
    use provider_sdk::generation::{GenerateEvent, GenerateEventStream, GenerateRequest, Message};
    use provider_sdk::{ModelId, SdkError};
    use std::sync::Mutex;

    let model = ModelId::new("claude-sonnet-5-5").unwrap();
    let request =
        GenerateRequest::new(vec![Message::user_text("PRIVATE_PROMPT").unwrap()]).unwrap();
    let evidence = Arc::new(Mutex::new(AnthropicEvidence::new(model.as_str())));
    let generation = evidence
        .lock()
        .unwrap()
        .begin(CloudPhase::Initial, &model, &request, true);
    let error = SdkError::new(
        ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
            rule: "nonempty_generation",
        }),
        Operation::GenerateStream,
        ErrorPhase::Normalize,
    );
    let kind = error.kind();
    let context = *error.context();
    let source = GenerateEventStream::new(futures_util::stream::iter([
        Ok(GenerateEvent::ThinkingDelta {
            delta: "PRIVATE_REASONING".to_owned(),
        }),
        Err(error),
    ]));
    let mut observed = AnthropicEvidence::inspect_stream(&evidence, generation, source);
    assert!(
        matches!(observed.next().await, Some(Ok(GenerateEvent::ThinkingDelta { delta })) if delta == "PRIVATE_REASONING")
    );
    let returned = observed.next().await.unwrap().unwrap_err();
    assert_eq!(returned.kind(), kind);
    assert_eq!(*returned.context(), context);
    assert!(observed.next().await.is_none());
    let snapshot = evidence.lock().unwrap().snapshot(false, true);
    assert!(!snapshot.to_string().contains("PRIVATE_"));
    assert_eq!(
        snapshot["generations"][0]["error"]["protocol_rule"],
        "nonempty_generation"
    );
    assert_eq!(snapshot["generations"][0]["error"]["phase"], "Normalize");
    assert_eq!(snapshot["capture_complete"], true);
}

#[test]
fn custom_anthropic_tap_retains_acquisition_error_before_any_event() {
    use custom_provider_evidence::{AnthropicEvidence, CloudPhase};
    use provider_sdk::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
    use provider_sdk::generation::{GenerateRequest, Message};
    use provider_sdk::{ModelId, SdkError};

    let model = ModelId::new("claude-sonnet-5-5").unwrap();
    let request =
        GenerateRequest::new(vec![Message::user_text("PRIVATE_PROMPT").unwrap()]).unwrap();
    let mut evidence = AnthropicEvidence::new(model.as_str());
    let generation = evidence.begin(CloudPhase::Reopened, &model, &request, true);
    let error = SdkError::new(
        ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
            rule: "original_normalization_rule",
        }),
        Operation::GenerateStream,
        ErrorPhase::Normalize,
    );
    evidence.acquisition_error(generation, &error);
    let snapshot = evidence.snapshot(false, true);
    assert_eq!(snapshot["generations"][0]["acquisition_error"], true);
    assert_eq!(snapshot["generations"][0]["phase"], "reopened");
    assert_eq!(
        snapshot["generations"][0]["error"]["protocol_rule"],
        "original_normalization_rule"
    );
    assert_eq!(snapshot["generations"][0]["event_count"], 0);
    assert_eq!(snapshot["generations"][0]["events"]["error"], 0);
}

#[tokio::test]
async fn custom_anthropic_tap_preserves_text_and_incomplete_stream() {
    use custom_provider_evidence::{AnthropicEvidence, CloudPhase};
    use futures_util::StreamExt;
    use provider_sdk::ModelId;
    use provider_sdk::generation::{GenerateEvent, GenerateEventStream, GenerateRequest, Message};
    use std::sync::Mutex;

    let model = ModelId::new("claude-sonnet-5-5").unwrap();
    let request =
        GenerateRequest::new(vec![Message::user_text("PRIVATE_PROMPT").unwrap()]).unwrap();
    let evidence = Arc::new(Mutex::new(AnthropicEvidence::new(model.as_str())));
    let generation = evidence
        .lock()
        .unwrap()
        .begin(CloudPhase::Initial, &model, &request, true);
    let source = GenerateEventStream::new(futures_util::stream::iter([
        Ok(GenerateEvent::Started { model }),
        Ok(GenerateEvent::ContentDelta {
            delta: "PRIVATE_TEXT_é".to_owned(),
        }),
    ]));
    let mut observed = AnthropicEvidence::inspect_stream(&evidence, generation, source);
    assert!(matches!(
        observed.next().await,
        Some(Ok(GenerateEvent::Started { .. }))
    ));
    assert!(
        matches!(observed.next().await, Some(Ok(GenerateEvent::ContentDelta { delta })) if delta == "PRIVATE_TEXT_é")
    );
    assert!(observed.next().await.is_none());
    let snapshot = evidence.lock().unwrap().snapshot(false, true);
    assert_eq!(snapshot["capture_complete"], false);
    assert_eq!(snapshot["generations"][0]["terminal"], "pending");
    assert_eq!(
        snapshot["generations"][0]["delta_text_bytes"],
        "PRIVATE_TEXT_é".len()
    );
    assert!(!snapshot.to_string().contains("PRIVATE_"));
}

#[tokio::test]
async fn custom_anthropic_tap_preserves_completion_and_detects_events_after_terminal() {
    use custom_provider_evidence::{AnthropicEvidence, CloudPhase};
    use futures_util::StreamExt;
    use provider_sdk::ModelId;
    use provider_sdk::generation::{
        FinishReason, GenerateEvent, GenerateEventStream, GenerateRequest, GenerateResponse,
        Message, Usage,
    };
    use std::sync::Mutex;

    let model = ModelId::new("claude-sonnet-5-5").unwrap();
    let request =
        GenerateRequest::new(vec![Message::user_text("PRIVATE_PROMPT").unwrap()]).unwrap();
    let evidence = Arc::new(Mutex::new(AnthropicEvidence::new(model.as_str())));
    assert_eq!(
        evidence.lock().unwrap().snapshot(false, true)["capture_complete"],
        false
    );
    let generation = evidence
        .lock()
        .unwrap()
        .begin(CloudPhase::Initial, &model, &request, true);
    let response = GenerateResponse {
        model,
        message: Message::assistant_text(" PRIVATE_OUTPUT_é\n").unwrap(),
        finish_reason: Some(FinishReason::Stop),
        citations: Vec::new(),
        artifacts: Vec::new(),
        hosted_tool_outputs: Vec::new(),
        usage: Usage::default(),
    };
    let expected = response.clone();
    let source = GenerateEventStream::new(futures_util::stream::iter([
        Ok(GenerateEvent::Completed(Box::new(response))),
        Ok(GenerateEvent::ContentDelta {
            delta: "AFTER_TERMINAL".to_owned(),
        }),
    ]));
    let mut observed = AnthropicEvidence::inspect_stream(&evidence, generation, source);
    assert!(
        matches!(observed.next().await, Some(Ok(GenerateEvent::Completed(response))) if *response == expected)
    );
    let snapshot = evidence.lock().unwrap().snapshot(true, false);
    assert_eq!(snapshot["capture_complete"], true);
    assert_eq!(snapshot["scenario_cleanup_succeeded"], false);
    assert_eq!(
        snapshot["generations"][0]["completed_text_bytes"],
        " PRIVATE_OUTPUT_é\n".len()
    );
    assert_eq!(snapshot["generations"][0]["finish_reason"], "stop");
    assert!(!snapshot.to_string().contains("PRIVATE_"));
    assert!(
        matches!(observed.next().await, Some(Ok(GenerateEvent::ContentDelta { delta })) if delta == "AFTER_TERMINAL")
    );
    assert!(observed.next().await.is_none());
    let snapshot = evidence.lock().unwrap().snapshot(false, true);
    assert_eq!(snapshot["capture_complete"], false);
    assert_eq!(snapshot["capture_failure"], "event_after_terminal");
}

#[tokio::test]
async fn custom_anthropic_tap_drop_releases_unpolled_stream_without_forwarding_task() {
    use custom_provider_evidence::AnthropicEvidence;
    use provider_sdk::SdkError;
    use provider_sdk::generation::{GenerateEvent, GenerateEventStream};
    use std::sync::Mutex;

    let held = Arc::new(AtomicBool::new(false));
    let weak = Arc::downgrade(&held);
    let source = futures_util::stream::unfold(held, |held| async move {
        held.store(true, Ordering::SeqCst);
        std::future::pending::<()>().await;
        Some((
            Ok::<GenerateEvent, SdkError>(GenerateEvent::ContentDelta {
                delta: "unused".to_owned(),
            }),
            held,
        ))
    });
    let evidence = Arc::new(Mutex::new(AnthropicEvidence::new("claude-sonnet-5-5")));
    let observed =
        AnthropicEvidence::inspect_stream(&evidence, None, GenerateEventStream::new(source));
    assert!(!weak.upgrade().unwrap().load(Ordering::SeqCst));
    drop(observed);
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn custom_anthropic_tap_preserves_closed_provider_and_fresh_phase_ownership() {
    use custom_provider_evidence::{AnthropicEvidence, CloudPhase};
    use custom_provider_observation::ObservedProvider;
    use provider_sdk::config::{AnthropicConfig, ApiKey, ProviderKind, TransportConfig};
    use provider_sdk::error::ErrorKind;
    use provider_sdk::generation::{GenerateRequest, Message};
    use provider_sdk::{ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig};
    use std::sync::Mutex;

    let model_id = ModelId::new("claude-sonnet-5-5").unwrap();
    let evidence = Arc::new(Mutex::new(AnthropicEvidence::new(model_id.as_str())));
    let request =
        GenerateRequest::new(vec![Message::user_text("PRIVATE_PROMPT").unwrap()]).unwrap();
    for phase in [CloudPhase::Initial, CloudPhase::Reopened] {
        let provider = Provider::from_config(ProviderConfig::new(
            ProviderKind::Anthropic(AnthropicConfig::new(
                ApiKey::new("synthetic-test-key").unwrap(),
            )),
            TransportConfig::default(),
        ))
        .unwrap();
        let tapped = ObservedProvider::anthropic(provider, Arc::clone(&evidence));
        let model = tapped.model(model_id.clone()).unwrap();
        let selected_matches = model.model_id() == &model_id;
        let shutdown = tapped.shutdown().await;
        let error = match phase {
            CloudPhase::Initial => match model.generate_stream(&request).await {
                Ok(_) => panic!("closed lower Provider must reject streaming"),
                Err(error) => error,
            },
            CloudPhase::Reopened => model.generate(&request).await.unwrap_err(),
        };
        evidence
            .lock()
            .unwrap()
            .phase_cleanup(phase, shutdown.is_ok());
        assert!(shutdown.is_ok());
        assert!(selected_matches);
        assert_eq!(error.kind(), ErrorKind::Closed);
    }
    let snapshot = evidence.lock().unwrap().snapshot(false, true);
    assert_eq!(snapshot["capture_complete"], true);
    assert_eq!(snapshot["provider_shutdown"].as_array().unwrap().len(), 2);
    for (index, phase) in ["initial", "reopened"].into_iter().enumerate() {
        assert_eq!(snapshot["provider_shutdown"][index]["phase"], phase);
        assert_eq!(snapshot["provider_shutdown"][index]["succeeded"], true);
        assert_eq!(snapshot["generations"][index]["phase"], phase);
        assert_eq!(snapshot["phase_cleanup"][index]["succeeded"], true);
    }
}

#[test]
fn custom_anthropic_tap_bounds_diagnostics_without_retaining_payloads() {
    use custom_provider_evidence::{AnthropicEvidence, CloudPhase};
    use provider_sdk::error::{ErrorKind, ErrorPhase, Operation, ResponseViolation};
    use provider_sdk::generation::{GenerateEvent, GenerateRequest, Message};
    use provider_sdk::{ModelId, SdkError};

    let model = ModelId::new("claude-sonnet-5-5").unwrap();
    let request =
        GenerateRequest::new(vec![Message::user_text("PRIVATE_PROMPT").unwrap()]).unwrap();
    let error = SdkError::new(
        ErrorKind::InvalidResponse(ResponseViolation::ProtocolViolation {
            rule: "unsafe\nrule",
        }),
        Operation::GenerateStream,
        ErrorPhase::Normalize,
    );
    let mut evidence = AnthropicEvidence::new(model.as_str());
    let generation = evidence.begin(CloudPhase::Initial, &model, &request, true);
    evidence.acquisition_error(generation, &error);
    assert_eq!(
        evidence.snapshot(false, true)["capture_failure"],
        "protocol_rule_bound"
    );
    assert!(
        !evidence
            .snapshot(false, true)
            .to_string()
            .contains("unsafe")
    );

    let mut evidence = AnthropicEvidence::new(model.as_str());
    let generation = evidence.begin(CloudPhase::Initial, &model, &request, true);
    evidence.observe(
        generation,
        &Ok(GenerateEvent::ContentDelta {
            delta: "x".repeat(1_048_577),
        }),
    );
    let snapshot = evidence.snapshot(false, true);
    assert_eq!(snapshot["capture_failure"], "text_observation_bound");
    assert_eq!(snapshot["generations"][0]["delta_text_bytes"], 1_048_576);
    assert!(snapshot.to_string().len() < 2_048);

    let mut evidence = AnthropicEvidence::new(model.as_str());
    for _ in 0..13 {
        evidence.begin(CloudPhase::Initial, &model, &request, true);
    }
    assert_eq!(
        evidence.snapshot(false, true)["capture_failure"],
        "generation_bound"
    );
    assert_eq!(
        evidence.snapshot(false, true)["generations"]
            .as_array()
            .unwrap()
            .len(),
        12
    );
    let oversized_model = AnthropicEvidence::new(&"x".repeat(1_025));
    assert_eq!(
        oversized_model.snapshot(false, true)["capture_failure"],
        "selected_model_bound"
    );
    assert_eq!(
        oversized_model.snapshot(false, true)["configured_model"],
        ""
    );
}

#[tokio::test]
async fn custom_anthropic_tap_private_export_does_not_overwrite_or_replace_primary_failure() {
    use custom_provider_evidence::AnthropicEvidence;

    let path = std::env::temp_dir().join(format!("custom-tap-{}.json", uuid::Uuid::new_v4()));
    let evidence = AnthropicEvidence::new("claude-sonnet-5-5");
    evidence.export_after_cleanup(&path, false, true).unwrap();
    let retained = std::fs::read(&path).unwrap();
    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777
    };
    let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
    cleanup
        .settle(async { evidence.export_after_cleanup(&path, false, true) })
        .await;
    let unchanged = std::fs::read(&path).unwrap() == retained;
    std::fs::remove_file(&path).unwrap();
    let failed = !cleanup.diagnostics().is_empty();
    let primary = std::panic::catch_unwind(AssertUnwindSafe(|| {
        cleanup.finish::<()>(Err(Box::new("original observation failure")));
    }))
    .unwrap_err();
    assert!(failed);
    assert!(unchanged);
    assert_eq!(
        primary.downcast_ref::<&str>().copied(),
        Some("original observation failure")
    );
    #[cfg(unix)]
    assert_eq!(mode, 0o600);
    let value: Value = serde_json::from_slice(&retained).unwrap();
    assert_eq!(value["scenario_cleanup_succeeded"], true);
    assert_eq!(value["capture_complete"], false);
}

struct EvidenceFixture {
    pending: Value,
    assistant: Value,
    user: Value,
    providers: Value,
    completed: Value,
}

impl EvidenceFixture {
    const PROMPT: &str = "Issue the specified question, then copy its fresh custom answer exactly.";
    const ANSWER: &str = "independent-fresh-answer-927";
    const MODEL: &str = "Configured Model";

    fn new() -> Self {
        let questions = json!([{"header":"Continue","question":"Continue?","multiple":false,
            "options":[{"label":"Yes","description":"Continue"},{"label":"No","description":"Stop"}]}]);
        let pending = json!([{"id":"que_001","sessionID":"ses_001", "questions":questions,
            "tool":{"messageID":"msg_200","callID":"call_001"}}]);
        let user = json!({"info":{"id":"msg_100","sessionID":"ses_001","role":"user",
            "model":{"providerID":"provider","modelID":"model"}},
            "parts":[{"id":"prt_100","sessionID":"ses_001","messageID":"msg_100","type":"text","text":Self::PROMPT}]});
        let assistant = json!({"info":{"id":"msg_200","sessionID":"ses_001","role":"assistant",
            "parentID":"msg_100","providerID":"provider","modelID":"model","time":{"created":10}},
            "parts":[{"id":"prt_200","sessionID":"ses_001","messageID":"msg_200","type":"tool",
                "callID":"call_001","tool":"question","state":{"status":"running","input":{"questions":questions}}}]});
        let providers = json!({"connected":["provider"],"all":[{"id":"provider","models":{"model":{
            "id":"model","providerID":"provider","name":Self::MODEL}}}]});
        let mut tool = assistant.clone();
        tool["info"]["time"]["completed"] = json!(20);
        tool["info"]["finish"] = json!("tool-calls");
        tool["parts"][0]["state"] = json!({"status":"completed","input":{"questions":questions},
            "output":format!("User has answered your questions: \"Continue?\"=\"{}\". You can now continue with the user's answers in mind.",Self::ANSWER),
            "metadata":{"answers":[[Self::ANSWER]],"truncated":false},"time":{"start":10,"end":20}});
        let terminal = json!({"info":{"id":"msg_300","sessionID":"ses_001","role":"assistant",
            "parentID":"msg_100","providerID":"provider","modelID":"model","time":{"created":21,"completed":30},"finish":"stop"},
            "parts":[{"id":"prt_300","sessionID":"ses_001","messageID":"msg_300","type":"text","text":Self::ANSWER}]});
        let completed = json!([user.clone(), tool, terminal]);
        Self {
            pending,
            assistant,
            user,
            providers,
            completed,
        }
    }

    fn descriptions(&mut self, yes: &str, no: &str) {
        for (index, description) in [yes, no].into_iter().enumerate() {
            self.pending[0]["questions"][0]["options"][index]["description"] = json!(description);
            self.assistant["parts"][0]["state"]["input"]["questions"][0]["options"][index]["description"] =
                json!(description);
            self.completed[1]["parts"][0]["state"]["input"]["questions"][0]["options"][index]["description"] =
                json!(description);
        }
    }

    fn begin(&self) -> Result<QuestionEvidence, String> {
        let pending = serde_json::to_vec(&self.pending).unwrap();
        let assistant = serde_json::to_vec(&self.assistant).unwrap();
        QuestionEvidence::begin(
            &pending,
            &assistant,
            &serde_json::to_vec(&self.user).unwrap(),
            &serde_json::to_vec(&self.providers).unwrap(),
            QuestionExpectation::new("ses_001", Self::PROMPT, Self::MODEL),
        )
    }

    fn recovered_prompt(native_output: &str) -> Self {
        let mut fixture = Self::new();
        fixture.pending = json!([]);
        fixture.completed[1]["info"]["error"] =
            json!({"name":"MessageAbortedError","data":{"message":"PRIVATE_OLD_ERROR"}});
        fixture.completed[1]["parts"][0]["state"] =
            json!({"status":"error","input":{"PRIVATE_OLD_INPUT":true}});
        fixture.completed.as_array_mut().unwrap().truncate(2);
        fixture.completed.as_array_mut().unwrap().extend([
            json!({"info":{"id":"msg_400","sessionID":"ses_001","role":"user",
                "time":{"created":30},"model":{"providerID":"provider","modelID":"model"}},
                "parts":[{"id":"prt_400","sessionID":"ses_001","messageID":"msg_400","type":"text",
                    "text":"Do not call any tool. Reply with exactly PRIVATE_RECOVERED_TOKEN and nothing else."}]}),
            json!({"info":{"id":"msg_500","sessionID":"ses_001","role":"assistant",
                "parentID":"msg_400","providerID":"provider","modelID":"model",
                "time":{"created":40,"completed":50},"finish":"stop"},
                "parts":[{"id":"prt_500","sessionID":"ses_001","messageID":"msg_500","type":"text","text":native_output}]}),
        ]);
        fixture
    }

    fn pending_mismatch_context(&self) -> Value {
        let error = match self.begin() {
            Ok(_) => panic!("typed question mismatch must remain an error"),
            Err(error) => error,
        };
        assert!(
            error.starts_with("native pending question differs from the original typed question")
        );
        assert!(
            error.len() <= 4096,
            "typed question diagnostic must be bounded"
        );
        assert!(error.is_ascii(), "native Unicode must be escaped");
        assert!(!error.chars().any(char::is_control));
        let (_, context) = error
            .split_once("; typed_question=")
            .expect("typed mismatch must identify expected and observed fields");
        serde_json::from_str(context).expect("typed mismatch context must be complete JSON")
    }

    fn tool_mismatch_context(&self) -> Value {
        let error = match self.begin() {
            Ok(_) => panic!("tool identity/input mismatch must remain an error"),
            Err(error) => error,
        };
        assert!(error.starts_with("native question tool identity or arguments differ"));
        assert!(error.len() <= 4096, "tool input diagnostic must be bounded");
        assert!(error.is_ascii(), "native Unicode must be escaped");
        assert!(!error.chars().any(char::is_control));
        let (_, context) = error
            .split_once("; tool_input=")
            .expect("tool mismatch must identify exact branch and bounded input context");
        serde_json::from_str(context).expect("tool mismatch context must be complete JSON")
    }

    fn grade(&self, portable: &str) -> Result<QuestionGrade, String> {
        self.begin()?.assess(
            &serde_json::to_vec(&self.completed).unwrap(),
            Self::ANSWER,
            portable,
        )
    }
}

#[test]
fn exact_native_question_and_copy_produce_both_successful_grades() {
    let fixture = EvidenceFixture::new();
    assert!(
        fixture
            .grade(EvidenceFixture::ANSWER)
            .unwrap()
            .model_exact_copy()
    );
    assert_eq!(
        QuestionEvidence::pending_origin(&serde_json::to_vec(&fixture.pending).unwrap()).unwrap(),
        ("ses_001".to_owned(), "msg_200".to_owned())
    );
    assert_eq!(
        QuestionEvidence::parent_user(&serde_json::to_vec(&fixture.assistant).unwrap()).unwrap(),
        "msg_100"
    );
    assert!(QuestionEvidence::require_no_pending(b"[]").is_ok());
    assert!(
        QuestionEvidence::require_no_pending(&serde_json::to_vec(&fixture.pending).unwrap())
            .is_err()
    );
    assert!(QuestionEvidence::require_no_pending(b"{}").is_err());
}

#[test]
fn question_description_copy_is_nonblocking_only_with_exact_native_inputs() {
    for (yes, no, expected) in [
        (" Continue", " Stop", [false, false]),
        ("Continue", "Stop", [true, true]),
        ("continue", "Stop", [false, true]),
        ("Continue", "Stop!", [true, false]),
        ("Continue ", "prefix Stop", [false, false]),
        ("", "", [false, false]),
        ("\nContinue\t", "\"Stop\"", [false, false]),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.descriptions(yes, no);
        let original_pending = fixture.pending.clone();
        let original_assistant = fixture.assistant.clone();
        let original_completed = fixture.completed.clone();
        let grade = fixture
            .grade(EvidenceFixture::ANSWER)
            .expect("unchanged native descriptions must not fail the SDK scenario");
        let grade = serde_json::to_value(grade).unwrap();
        assert_eq!(grade["model_exact_copy"], true);
        assert_eq!(grade["model_yes_description_exact_copy"], expected[0]);
        assert_eq!(grade["model_no_description_exact_copy"], expected[1]);
        assert_eq!(fixture.pending, original_pending);
        assert_eq!(fixture.assistant, original_assistant);
        assert_eq!(fixture.completed, original_completed);
    }
}

#[test]
fn question_description_changes_between_native_observations_remain_blocking() {
    for boundary in ["pending", "running", "completed"] {
        for description in [
            Some(json!("Continue")),
            Some(Value::Null),
            Some(json!(true)),
            None,
        ] {
            let mut fixture = EvidenceFixture::new();
            fixture.descriptions(" Continue", " Stop");
            let option = match boundary {
                "pending" => &mut fixture.pending[0]["questions"][0]["options"][0],
                "running" => {
                    &mut fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["options"]
                        [0]
                }
                "completed" => {
                    &mut fixture.completed[1]["parts"][0]["state"]["input"]["questions"][0]["options"]
                        [0]
                }
                _ => unreachable!(),
            };
            match description {
                Some(value) => option["description"] = value,
                None => {
                    option.as_object_mut().unwrap().remove("description");
                }
            }
            assert!(
                fixture.grade(EvidenceFixture::ANSWER).is_err(),
                "{boundary}"
            );
        }
    }
    for (index, actual) in [(0, "continuE"), (1, "stop")] {
        let mut fixture = EvidenceFixture::new();
        fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["options"][index]["description"] =
            json!(actual);
        let context = fixture.tool_mismatch_context();
        let description = &context["fields"][format!("description{index}")];
        assert_eq!(description["kind"], "string");
        assert_eq!(description["matches"], false);
        assert_eq!(description["text"]["preview"], actual);
        assert_eq!(description["text"]["bytes"], actual.len());
        assert_eq!(description["text"]["truncated"], false);
    }
    let mut fixture = EvidenceFixture::new();
    fixture.descriptions(" Continue", " Stop");
    fixture.completed[2]["parts"][0]["text"] = json!("native output differs");
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
}

#[test]
fn sdk_correct_native_answer_with_model_wrong_answer_preserves_failed_quality_grade() {
    let mut fixture = EvidenceFixture::new();
    let raw = "```\n</think>\n\nContinuing...";
    fixture.completed[2]["parts"][0]["text"] = json!(raw);
    let grade = fixture
        .grade(raw)
        .expect("exact native/portable fidelity remains SDK-correct");
    assert!(!grade.model_exact_copy());
    // Exercises serialization only; this deterministic target is not the live
    // receipt log consumed by the canonical verifier.
    grade.emit("deterministic-quality-control", "1.19.3");
}

#[test]
fn terminal_text_uses_part_identity_order_without_trimming_or_synthetic_text() {
    let mut fixture = EvidenceFixture::new();
    let base = fixture.completed[2]["parts"][0].clone();
    let mut last = base.clone();
    last["id"] = json!("prt_303");
    last["text"] = json!("\nright ");
    let mut first = base.clone();
    first["id"] = json!("prt_301");
    first["text"] = json!(" left");
    first["ignored"] = json!(true);
    let mut synthetic = base;
    synthetic["id"] = json!("prt_302");
    synthetic["text"] = json!("MUST NOT LEAK");
    synthetic["synthetic"] = json!(true);
    fixture.completed[2]["parts"] = json!([last, synthetic, first]);
    assert!(!fixture.grade(" left\nright ").unwrap().model_exact_copy());
    assert!(fixture.grade("left\nright").is_err());
}

#[test]
fn complete_empty_native_terminal_is_fidelity_success_and_failed_copy_quality() {
    let mut fixture = EvidenceFixture::new();
    fixture.completed[2]["parts"] = json!([]);
    assert!(!fixture.grade("").unwrap().model_exact_copy());
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
}

#[test]
fn pending_question_missing_duplicate_wrong_shape_and_disabled_custom_fail() {
    for replacement in [
        json!([]),
        json!([
            EvidenceFixture::new().pending[0].clone(),
            EvidenceFixture::new().pending[0].clone()
        ]),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.pending = replacement;
        assert!(fixture.begin().is_err());
    }
    for (pointer, value) in [
        ("/0/questions/0/header", json!("wrong")),
        ("/0/questions/0/custom", json!(false)),
        ("/0/questions/0/multiple", json!(true)),
        ("/0/questions/0/options/0/label", json!("wrong")),
        ("/0/sessionID", json!("ses_foreign")),
        ("/0/tool/callID", json!("wrong")),
        ("/0/tool/messageID", json!("msg_stale")),
        ("/0/id", json!("que_bad ")),
    ] {
        let mut fixture = EvidenceFixture::new();
        if pointer.ends_with("/custom") {
            fixture.pending[0]["questions"][0]["custom"] = value;
        } else {
            *fixture.pending.pointer_mut(pointer).unwrap() = value;
        }
        assert!(fixture.begin().is_err(), "{pointer}");
    }
}

#[test]
fn pending_question_mismatch_reports_each_field_without_accepting_it() {
    for (field, observed, expected) in [
        ("/header", json!("ContinuE"), json!("Continue")),
        ("/question", json!("Continue!"), json!("Continue?")),
        ("/options/0/label", json!("YES"), json!("Yes")),
        ("/options/1/label", json!("NO"), json!("No")),
        ("/multiple", json!(true), json!(false)),
        ("/custom", json!(false), json!(true)),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.pending[0]["questions"][0]["custom"] = json!(true);
        *fixture.pending[0]["questions"][0]
            .pointer_mut(field)
            .unwrap() = observed.clone();
        let context = fixture.pending_mismatch_context();
        assert_eq!(
            context["expected"].pointer(field),
            Some(&expected),
            "{field}"
        );
        let actual = context["observed"].pointer(field).unwrap();
        if let Some(text) = observed.as_str() {
            assert_eq!(actual["preview"], observed, "{field}");
            assert_eq!(actual["bytes"], json!(text.len()), "{field}");
            assert_eq!(actual["truncated"], false, "{field}");
        } else {
            assert_eq!(*actual, observed, "{field}");
        }
        assert_eq!(context["observed"]["option_count"], 2);
        assert_eq!(context["observed"]["options_truncated"], false);
    }
}

#[test]
fn pending_question_mismatch_reports_option_cardinality_and_order() {
    for count in [0, 1, 3, 4096] {
        let mut fixture = EvidenceFixture::new();
        let option = fixture.pending[0]["questions"][0]["options"][0].clone();
        fixture.pending[0]["questions"][0]["options"] = json!(vec![option; count]);
        let context = fixture.pending_mismatch_context();
        assert_eq!(context["observed"]["option_count"], json!(count));
        assert_eq!(
            context["observed"]["options"].as_array().unwrap().len(),
            count.min(2)
        );
        assert_eq!(context["observed"]["options_truncated"], json!(count > 2));
    }
    let mut fixture = EvidenceFixture::new();
    fixture.pending[0]["questions"][0]["options"]
        .as_array_mut()
        .unwrap()
        .swap(0, 1);
    let context = fixture.pending_mismatch_context();
    assert_eq!(context["observed"]["option_count"], 2);
    assert_eq!(context["observed"]["options"][0]["label"]["preview"], "No");
    assert_eq!(context["observed"]["options"][1]["label"]["preview"], "Yes");
    assert_eq!(context["expected"]["options"][0]["label"], "Yes");
    assert_eq!(context["expected"]["options"][1]["label"], "No");
}

#[test]
fn pending_question_mismatch_bounds_hostile_text_and_excludes_unrelated_bodies() {
    let mut fixture = EvidenceFixture::new();
    let hostile = "\u{10ffff}\u{202e}\u{0}\n\r\t\"\\秘密".repeat(4096);
    for field in ["header", "question"] {
        fixture.pending[0]["questions"][0][field] = json!(hostile);
    }
    for option in fixture.pending[0]["questions"][0]["options"]
        .as_array_mut()
        .unwrap()
    {
        option["label"] = json!(hostile);
        option["description"] = json!(hostile);
    }
    fixture.pending[0]["questions"][0]["options"].as_array_mut().unwrap().push(
        json!({"label":"unreported-extra-option", "description":"unreported-extra-description"}),
    );
    fixture.pending[0]["credentials"] = json!("unrelated-pending-secret");
    fixture.assistant = json!({"private":"unrelated-assistant-secret"});
    fixture.user = json!({"private":"unrelated-user-secret"});
    fixture.providers = json!({"credentials":"unrelated-provider-secret"});
    let context = fixture.pending_mismatch_context();
    for field in [
        "/header",
        "/question",
        "/options/0/label",
        "/options/0/description",
        "/options/1/label",
        "/options/1/description",
    ] {
        let actual = context["observed"].pointer(field).unwrap();
        assert_eq!(actual["bytes"], json!(hostile.len()));
        assert_eq!(actual["truncated"], true);
        let preview = actual["preview"].as_str().unwrap();
        assert!(preview.len() <= 320);
        assert!(preview.starts_with(r"\u{10ffff}\u{202e}\u{0}\n\r\t"));
        assert!(!preview.chars().any(char::is_control));
    }
    let rendered = context.to_string();
    for secret in [
        "unreported-extra-option",
        "unreported-extra-description",
        "unrelated-pending-secret",
        "unrelated-assistant-secret",
        "unrelated-user-secret",
        "unrelated-provider-secret",
        "que_001",
        "ses_001",
        "msg_200",
        "call_001",
    ] {
        assert!(
            !rendered.contains(secret),
            "non-allowlisted evidence must be excluded"
        );
    }
    assert_eq!(context["observed"]["option_count"], 3);
    assert_eq!(context["observed"]["options_truncated"], true);
}

#[test]
fn pending_question_diagnostic_truncation_does_not_normalize_the_oracle() {
    for count in [31, 32, 33] {
        let mut fixture = EvidenceFixture::new();
        fixture.pending[0]["questions"][0]["header"] = json!("é".repeat(count));
        let context = fixture.pending_mismatch_context();
        let header = &context["observed"]["header"];
        assert_eq!(header["bytes"], json!(2 * count));
        assert_eq!(header["preview"], json!(r"\u{e9}".repeat(count.min(32))));
        assert_eq!(header["truncated"], json!(count > 32));
    }
    let fixture = EvidenceFixture::new();
    assert!(fixture.begin().is_ok(), "omitted custom still means true");
    let mut fixture = EvidenceFixture::new();
    fixture.pending[0]["questions"][0]["custom"] = json!(true);
    assert!(
        fixture.begin().is_ok(),
        "explicit true still satisfies the original oracle"
    );
    fixture.pending[0]["questions"][0]["custom"] = json!(false);
    assert_eq!(
        fixture.pending_mismatch_context()["observed"]["custom"],
        false
    );
    fixture.pending[0]["questions"][0]["custom"] = Value::Null;
    assert!(
        fixture.begin().is_err(),
        "null must not acquire omission semantics"
    );
}

#[test]
fn explicit_null_flags_cannot_impersonate_valid_omission_defaults() {
    let mut fixture = EvidenceFixture::new();
    fixture.pending[0]["questions"][0]["custom"] = Value::Null;
    assert!(fixture.begin().is_err());
    let mut fixture = EvidenceFixture::new();
    fixture.completed[2]["parts"][0]["synthetic"] = Value::Null;
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
}

#[test]
fn native_root_and_tool_correlation_cannot_be_inferred_from_matching_text() {
    for (pointer, value) in [
        ("/info/parentID", json!("msg_foreign")),
        ("/info/modelID", json!("other")),
        ("/info/sessionID", json!("ses_foreign")),
        ("/parts/0/messageID", json!("msg_foreign")),
        ("/parts/0/state/input/questions/0/question", json!("Wrong?")),
        ("/parts/0/id", json!("prt_200 ")),
    ] {
        let mut fixture = EvidenceFixture::new();
        *fixture.assistant.pointer_mut(pointer).unwrap() = value;
        assert!(fixture.begin().is_err(), "{pointer}");
    }
    for parent in [Value::Null, json!("msg_parent"), json!(17)] {
        let mut fixture = EvidenceFixture::new();
        fixture.user["info"]["parentID"] = parent;
        assert!(fixture.begin().is_err());
    }
    let mut fixture = EvidenceFixture::new();
    fixture.user["parts"][0]["text"] = json!("stale prompt");
    assert!(fixture.begin().is_err());
}

#[test]
fn native_tool_mismatch_identifies_each_branch_without_exposing_identity() {
    for (pointer, value, expected) in [
        (
            "/parts/0/callID",
            json!("private-foreign-call"),
            [false, true, true],
        ),
        (
            "/parts/0/tool",
            json!("private-other-tool"),
            [true, false, true],
        ),
        (
            "/parts/0/state/input/questions/0/question",
            json!("Wrong?"),
            [true, true, false],
        ),
    ] {
        let mut fixture = EvidenceFixture::new();
        *fixture.assistant.pointer_mut(pointer).unwrap() = value;
        let context = fixture.tool_mismatch_context();
        assert_eq!(context["call_id_matches"], expected[0]);
        assert_eq!(context["tool_name_matches"], expected[1]);
        assert_eq!(context["input_matches"], expected[2]);
        assert_eq!(context["state"], "running");
        assert_eq!(context["fields"]["custom"]["kind"], "missing");
        assert_eq!(context["fields"]["custom"]["matches"], true);
        let rendered = context.to_string();
        for excluded in [
            "private-foreign-call",
            "private-other-tool",
            "call_001",
            "msg_200",
            "ses_001",
        ] {
            assert!(!rendered.contains(excluded));
        }
    }
}

#[test]
fn native_tool_mismatch_preserves_absent_default_and_extra_field_distinctions() {
    let mut fixture = EvidenceFixture::new();
    fixture.pending[0]["questions"][0]["custom"] = json!(true);
    assert!(
        fixture.begin().is_ok(),
        "pending default does not add an input field"
    );
    for (value, kind) in [
        (json!(true), "boolean"),
        (Value::Null, "null"),
        (json!("true"), "string"),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["custom"] = value.clone();
        let context = fixture.tool_mismatch_context();
        assert_eq!(context["input_matches"], false);
        assert_eq!(context["fields"]["custom"]["kind"], kind);
        assert_eq!(context["fields"]["custom"]["matches"], false);
        assert_eq!(context["fields"]["entry"]["extra"], 1);
        if value.is_boolean() {
            assert_eq!(context["fields"]["custom"]["value"], value);
        }
    }
    let mut fixture = EvidenceFixture::new();
    fixture.assistant["parts"][0]["state"]["input"]["questions"][0]
        .as_object_mut()
        .unwrap()
        .remove("multiple");
    let context = fixture.tool_mismatch_context();
    assert_eq!(context["fields"]["multiple"]["kind"], "missing");
    assert_eq!(context["fields"]["multiple"]["matches"], false);
    for (value, kind) in [
        (json!(true), "boolean"),
        (Value::Null, "null"),
        (json!("false"), "string"),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["multiple"] = value;
        let context = fixture.tool_mismatch_context();
        assert_eq!(context["fields"]["multiple"]["kind"], kind);
        assert_eq!(context["fields"]["multiple"]["matches"], false);
    }
    let mut fixture = EvidenceFixture::new();
    fixture.assistant["parts"][0]["state"]["input"]["unreported-secret-key"] =
        json!({"secret":"unreported-value"});
    let context = fixture.tool_mismatch_context();
    assert_eq!(context["fields"]["root"]["extra"], 1);
    assert!(!context.to_string().contains("unreported"));
    for (value, kind) in [
        (Value::Null, "null"),
        (json!(false), "boolean"),
        (json!(7), "number"),
        (json!("unreported-root-string"), "string"),
        (json!([]), "array"),
        (json!({}), "object"),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.assistant["parts"][0]["state"]["input"] = value;
        let context = fixture.tool_mismatch_context();
        assert_eq!(context["fields"]["root"]["kind"], kind);
        assert_eq!(context["fields"]["questions"]["kind"], "missing");
        assert!(!context.to_string().contains("unreported-root-string"));
    }
}

#[test]
fn native_tool_mismatch_reports_exact_strings_order_and_cardinality() {
    for (path, field, expected) in [
        ("/header", "header", "Continue"),
        ("/question", "question", "Continue?"),
        ("/options/0/label", "label0", "Yes"),
        ("/options/0/description", "description0", "Continue"),
        ("/options/1/label", "label1", "No"),
        ("/options/1/description", "description1", "Stop"),
    ] {
        let mut fixture = EvidenceFixture::new();
        let changed = format!("{expected} ");
        *fixture.assistant["parts"][0]["state"]["input"]["questions"][0]
            .pointer_mut(path)
            .unwrap() = json!(changed);
        let context = fixture.tool_mismatch_context();
        assert_eq!(context["fields"][field]["matches"], false);
        assert_eq!(context["fields"][field]["text"]["preview"], changed);
        assert_eq!(
            context["fields"][field]["text"]["bytes"],
            expected.len() + 1
        );
        assert_eq!(context["fields"][field]["text"]["truncated"], false);
    }
    let mut fixture = EvidenceFixture::new();
    fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["options"]
        .as_array_mut()
        .unwrap()
        .swap(0, 1);
    let context = fixture.tool_mismatch_context();
    assert_eq!(context["fields"]["options"]["count"], 2);
    assert_eq!(context["fields"]["label0"]["matches"], false);
    assert_eq!(context["fields"]["label0"]["text"]["preview"], "No");
    for options in [
        json!([]),
        json!([{"label":"Yes","description":"Continue"}]),
        json!(null),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["options"] = options;
        let context = fixture.tool_mismatch_context();
        assert_eq!(context["fields"]["options"]["matches"], false);
        assert_eq!(context["fields"]["option1"]["kind"], "missing");
    }
    for count in [31, 32, 33] {
        let mut fixture = EvidenceFixture::new();
        fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["header"] =
            json!("é".repeat(count));
        let context = fixture.tool_mismatch_context();
        assert_eq!(context["fields"]["header"]["text"]["bytes"], 2 * count);
        assert_eq!(context["fields"]["header"]["text"]["truncated"], count > 32);
    }
}

#[test]
fn native_tool_mismatch_bounds_hostile_inputs_without_changing_rejection() {
    let mut fixture = EvidenceFixture::new();
    let hostile = "\u{10ffff}\u{202e}\u{0}\n\r\t\"\\秘密".repeat(4096);
    for path in [
        "/header",
        "/question",
        "/options/0/label",
        "/options/0/description",
        "/options/1/label",
        "/options/1/description",
    ] {
        *fixture.assistant["parts"][0]["state"]["input"]["questions"][0]
            .pointer_mut(path)
            .unwrap() = json!(hostile);
    }
    fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["options"]
        .as_array_mut()
        .unwrap()
        .push(json!({"private-extra-key":"private-extra-value"}));
    fixture.assistant["parts"][0]["state"]["input"]["questions"][0]["private-entry-key"] =
        json!(hostile);
    fixture.assistant["parts"][0]["state"]["input"]["private-root-key"] = json!(hostile);
    fixture.assistant["info"]["private-attribute"] = json!("unrelated-assistant-secret");
    fixture.user["private-attribute"] = json!("unrelated-user-secret");
    fixture.providers["private-attribute"] = json!("unrelated-provider-secret");
    let context = fixture.tool_mismatch_context();
    assert_eq!(context["input_matches"], false);
    assert_eq!(context["fields"]["root"]["extra"], 1);
    assert_eq!(context["fields"]["entry"]["extra"], 1);
    assert_eq!(context["fields"]["options"]["count"], 3);
    for name in [
        "header",
        "question",
        "label0",
        "description0",
        "label1",
        "description1",
    ] {
        assert_eq!(context["fields"][name]["text"]["bytes"], hostile.len());
        assert_eq!(context["fields"][name]["text"]["truncated"], true);
        let preview = context["fields"][name]["text"]["preview"].as_str().unwrap();
        assert!(preview.len() <= 320);
        assert!(preview.starts_with(r"\u{10ffff}\u{202e}\u{0}\n\r\t"));
    }
    for path in [
        "/header",
        "/question",
        "/options/0/label",
        "/options/0/description",
        "/options/1/label",
        "/options/1/description",
    ] {
        *fixture.assistant["parts"][0]["state"]["input"]["questions"][0]
            .pointer_mut(path)
            .unwrap() = json!("\u{10ffff}".repeat(33));
    }
    let maximum_previews = fixture.tool_mismatch_context();
    assert_eq!(
        maximum_previews["fields"]["header"]["text"]["preview"],
        r"\u{10ffff}".repeat(32)
    );
    assert_eq!(
        maximum_previews["fields"]["header"]["text"]["truncated"],
        true
    );
    let rendered = context.to_string();
    for excluded in [
        "private-extra-key",
        "private-extra-value",
        "private-entry-key",
        "private-root-key",
        "unrelated-assistant-secret",
        "unrelated-user-secret",
        "unrelated-provider-secret",
        "que_001",
        "ses_001",
        "msg_200",
        "call_001",
    ] {
        assert!(
            !rendered.contains(excluded),
            "non-allowlisted evidence must stay absent"
        );
    }
}

#[test]
fn configured_native_model_requires_one_connected_consistent_inventory_match() {
    for variant in 0..4 {
        let mut fixture = EvidenceFixture::new();
        match variant {
            0 => fixture.providers["connected"] = json!([]),
            1 => {
                fixture.providers["all"][0]["models"]["other"] =
                    json!({"id":"other","providerID":"provider","name":EvidenceFixture::MODEL})
            }
            2 => fixture.providers["all"][0]["models"]["model"]["providerID"] = json!("foreign"),
            3 => {
                let duplicate = fixture.providers["all"][0].clone();
                fixture.providers["all"]
                    .as_array_mut()
                    .unwrap()
                    .push(duplicate);
            }
            _ => unreachable!(),
        }
        assert!(fixture.begin().is_err());
    }
}

#[test]
fn completed_answer_missing_wrong_duplicate_or_stale_fails_sdk_grade() {
    for value in [
        Value::Null,
        json!([]),
        json!([["stale-answer"]]),
        json!([[EvidenceFixture::ANSWER, EvidenceFixture::ANSWER]]),
        json!([[EvidenceFixture::ANSWER], [EvidenceFixture::ANSWER]]),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.completed[1]["parts"][0]["state"]["metadata"]["answers"] = value;
        assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    }
    for pointer in ["output", "status"] {
        let mut fixture = EvidenceFixture::new();
        fixture.completed[1]["parts"][0]["state"][pointer] = json!("wrong");
        assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    }
    for status in ["pending", "running", "error"] {
        let mut fixture = EvidenceFixture::new();
        fixture.completed[1]["parts"][0]["state"]["status"] = json!(status);
        assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    }
    let mut fixture = EvidenceFixture::new();
    fixture.completed[1]["parts"][0]["state"]["time"]["compacted"] = json!(18);
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    for value in [Value::Null, json!(true)] {
        let mut fixture = EvidenceFixture::new();
        fixture.completed[1]["parts"][0]["state"]["metadata"]["truncated"] = value;
        assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    }
    let mut fixture = EvidenceFixture::new();
    fixture.completed[1]["parts"][0]["state"]["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("truncated");
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
}

#[test]
fn incomplete_duplicate_foreign_or_stale_terminal_cannot_supply_fidelity() {
    for (pointer, value) in [
        ("/2/info/time/completed", Value::Null),
        ("/2/info/finish", json!("length")),
        ("/2/info/parentID", json!("msg_foreign")),
        ("/2/info/sessionID", json!("ses_foreign")),
        ("/2/info/modelID", json!("wrong")),
        ("/2/parts/0/text", json!("native mismatch")),
        ("/1/parts/0/callID", json!("call_foreign")),
        ("/0/parts/0/text", json!("changed original prompt")),
    ] {
        let mut fixture = EvidenceFixture::new();
        *fixture.completed.pointer_mut(pointer).unwrap() = value;
        assert!(fixture.grade(EvidenceFixture::ANSWER).is_err(), "{pointer}");
    }
    let mut fixture = EvidenceFixture::new();
    fixture.completed[2]["info"]["id"] = json!("msg_150");
    fixture.completed[2]["parts"][0]["messageID"] = json!("msg_150");
    assert!(
        fixture.grade(EvidenceFixture::ANSWER).is_err(),
        "terminal before Question must fail"
    );
    for index in [0, 1, 2] {
        let mut fixture = EvidenceFixture::new();
        let duplicate = fixture.completed[index].clone();
        fixture.completed.as_array_mut().unwrap().push(duplicate);
        assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    }
    let mut fixture = EvidenceFixture::new();
    fixture.completed.as_array_mut().unwrap().pop();
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
}

#[test]
fn extra_tool_calls_and_part_identity_collisions_fail_even_with_a_correct_answer() {
    let mut fixture = EvidenceFixture::new();
    let mut extra = fixture.completed[1]["parts"][0].clone();
    extra["id"] = json!("prt_201");
    extra["callID"] = json!("call_second");
    fixture.completed[1]["parts"]
        .as_array_mut()
        .unwrap()
        .push(extra);
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    let mut fixture = EvidenceFixture::new();
    fixture.completed[2]["parts"][0]["id"] = json!("prt_200");
    assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    for error in [Value::Null, json!({"name":"UnknownError"})] {
        let mut fixture = EvidenceFixture::new();
        fixture.completed[2]["info"]["error"] = error;
        assert!(fixture.grade(EvidenceFixture::ANSWER).is_err());
    }
}

#[test]
fn malformed_and_incomplete_json_never_counts_as_complete_native_evidence() {
    let fixture = EvidenceFixture::new();
    assert!(QuestionEvidence::pending_origin(b"[{\"id\":").is_err());
    assert!(QuestionEvidence::parent_user(b"{}").is_err());
    assert!(
        fixture
            .begin()
            .unwrap()
            .assess(b"[", EvidenceFixture::ANSWER, EvidenceFixture::ANSWER)
            .is_err()
    );
    for answer in ["", "Yes", "No", "question"] {
        assert!(
            fixture
                .begin()
                .unwrap()
                .assess(
                    &serde_json::to_vec(&fixture.completed).unwrap(),
                    answer,
                    EvidenceFixture::ANSWER
                )
                .is_err()
        );
    }
}

struct ReadbackProbe {
    endpoint: String,
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    held_read: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

#[tokio::test]
async fn recovery_output_readback_distinguishes_native_parity_from_exact_copy() {
    for (native, portable, equal) in [
        (
            "PRIVATE\\_RECOVERED\\_TOKEN",
            "PRIVATE\\_RECOVERED\\_TOKEN",
            true,
        ),
        (
            "PRIVATE_RECOVERED_TOKEN",
            "PRIVATE\\_RECOVERED\\_TOKEN",
            false,
        ),
    ] {
        let fixture = EvidenceFixture::recovered_prompt(native);
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        let requests = probe.requests.clone();
        probe.observe(async {
            let reader = NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5)).await;
            let context = reader.prompt_output(
                "Do not call any tool. Reply with exactly PRIVATE_RECOVERED_TOKEN and nothing else.",
                "provider", "model", "PRIVATE_RECOVERED_TOKEN", portable,
            ).await.unwrap();
            assert_eq!(context["capture"], "complete");
            assert_eq!(context["native_equals_portable"], equal);
            assert_eq!(context["native_equals_expected"], native == "PRIVATE_RECOVERED_TOKEN");
            assert_eq!(context["portable_equals_expected"], false);
            assert_eq!(context["matching_users"], 1);
            assert_eq!(context["terminal_count"], 1);
            assert_eq!(context["native"]["bytes"], native.len());
            assert_eq!(*requests.lock().unwrap(), [
                "GET /global/health", "GET /experimental/session", "GET /session/ses_001/message",
            ]);
            assert!(!context.to_string().contains("PRIVATE"));
            assert!(!context.to_string().contains("msg_"));
            assert!(!context.to_string().contains("ses_"));
        }).await;
    }
}

#[tokio::test]
async fn recovery_output_readback_rejects_ambiguous_foreign_or_incomplete_terminals() {
    for variant in 0..15 {
        let mut fixture = EvidenceFixture::recovered_prompt("PRIVATE_RECOVERED_TOKEN");
        match variant {
            0 => {
                let mut duplicate = fixture.completed[2].clone();
                duplicate["info"]["id"] = json!("msg_401");
                duplicate["parts"][0]["id"] = json!("prt_401");
                duplicate["parts"][0]["messageID"] = json!("msg_401");
                fixture.completed.as_array_mut().unwrap().push(duplicate);
            }
            1 => {
                let mut duplicate = fixture.completed[3].clone();
                duplicate["info"]["id"] = json!("msg_501");
                duplicate["parts"][0]["id"] = json!("prt_501");
                duplicate["parts"][0]["messageID"] = json!("msg_501");
                fixture.completed.as_array_mut().unwrap().push(duplicate);
            }
            2 => fixture.completed[3]["info"]["parentID"] = json!("msg_100"),
            3 => fixture.completed[3]["info"]["modelID"] = json!("foreign"),
            4 => fixture.completed[3]["parts"][0]["messageID"] = json!("msg_400"),
            5 => fixture.completed[3]["info"]["time"]["completed"] = Value::Null,
            6 => fixture.completed[3]["info"]["finish"] = json!("length"),
            7 => fixture.completed[3]["info"]["error"] = json!({"name":"APIError"}),
            8 => fixture.completed[3]["parts"][0]["id"] = json!("prt_400"),
            9 => fixture.completed = json!({"not":"a message inventory"}),
            10 => {
                fixture.completed[2]["info"]["model"]["providerID"] = json!("foreign");
                fixture.completed[3]["info"]["providerID"] = json!("foreign");
            }
            11 => fixture.completed[3]["info"]["time"]["created"] = json!(20),
            12 => {
                let mut later = fixture.completed[3].clone();
                later["info"]["id"] = json!("msg_600");
                later["info"]["time"] = json!({"created":60,"completed":70});
                later["info"]["finish"] = json!("tool-calls");
                later["parts"] = json!([]);
                fixture.completed.as_array_mut().unwrap().push(later);
            }
            13 => fixture.completed[3]["info"]["id"] = json!("malformed"),
            14 => fixture.completed[2]["info"]["time"] = Value::Null,
            _ => unreachable!(),
        }
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        probe.observe(async {
            let reader = NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5)).await;
            assert!(reader.prompt_output(
                "Do not call any tool. Reply with exactly PRIVATE_RECOVERED_TOKEN and nothing else.",
                "provider", "model", "PRIVATE_RECOVERED_TOKEN", "PRIVATE_RECOVERED_TOKEN",
            ).await.is_err(), "variant {variant} must not establish parity");
        }).await;
    }
}

#[tokio::test]
async fn recovery_output_readback_preserves_native_part_order_and_exact_text() {
    let mut fixture = EvidenceFixture::recovered_prompt("unused");
    fixture.completed[3]["parts"] = json!([
        {"id":"prt_503","sessionID":"ses_001","messageID":"msg_500","type":"text","text":"_TOKEN ","ignored":true},
        {"id":"prt_504","sessionID":"ses_001","messageID":"msg_500","type":"text","text":"PRIVATE_SYNTHETIC","synthetic":true},
        {"id":"prt_501","sessionID":"ses_001","messageID":"msg_500","type":"text","text":" PRIVATE_"},
        {"id":"prt_502","sessionID":"ses_001","messageID":"msg_500","type":"text","text":"RECOVERED"},
    ]);
    // The earlier tool step deliberately has a lexically greater ID and a later
    // array position. Creation time, not either incidental order, is authority.
    fixture.completed.as_array_mut().unwrap().push(json!({
        "info":{"id":"msg_900","sessionID":"ses_001","role":"assistant",
            "parentID":"msg_400","providerID":"provider","modelID":"model",
            "time":{"created":35,"completed":39},"finish":"tool-calls"},
        "parts":[],
    }));
    let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
    let endpoint = probe.endpoint.clone();
    probe
        .observe(async {
            let reader =
                NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                    .await;
            let context = reader.prompt_output(
            "Do not call any tool. Reply with exactly PRIVATE_RECOVERED_TOKEN and nothing else.",
            "provider", "model", "PRIVATE_RECOVERED_TOKEN", " PRIVATE_RECOVERED_TOKEN ",
        ).await.unwrap();
            assert_eq!(context["native_equals_portable"], true);
            assert_eq!(context["native_equals_expected"], false);
            assert_eq!(
                context["native"]["fingerprint"],
                context["portable"]["fingerprint"]
            );
            assert_ne!(
                context["native"]["fingerprint"],
                context["expected"]["fingerprint"]
            );
            assert!(!context.to_string().contains("PRIVATE"));
        })
        .await;
}

#[tokio::test]
async fn recovery_output_readback_timeout_preserves_original_assertion_and_cleanup() {
    let fixture = EvidenceFixture::recovered_prompt("PRIVATE_RECOVERED_TOKEN");
    let (probe, _) =
        ReadbackProbe::complete_question_fixture(&fixture, Some("/session/ses_001/message")).await;
    let endpoint = probe.endpoint.clone();
    let requests = probe.requests.clone();
    let held_read = probe.held_read.clone();
    probe
        .observe(async {
            let reader =
                NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                    .await;
            let original = std::panic::catch_unwind(|| assert_eq!("actual", "expected"))
                .expect_err("original exact-copy assertion must fail");
            let original_text = original.downcast_ref::<String>().unwrap().clone();
            let mut primary = Some(original);
            let mut capture = Box::pin(reader.prompt_output(
            "Do not call any tool. Reply with exactly PRIVATE_RECOVERED_TOKEN and nothing else.",
            "provider", "model", "PRIVATE_RECOVERED_TOKEN", "PRIVATE_RECOVERED_TOKEN",
        ));
            tokio::select! {
                result = &mut capture => panic!("capture unexpectedly completed: {result:?}"),
                _ = held_read.notified() => {}
            }
            let diagnostic = NativeCleanup::observe(Instant::now(), capture).await;
            assert!(matches!(diagnostic, Ok(Err(_))));
            let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
            let mut settled = false;
            cleanup
                .settle(async {
                    settled = true;
                    Err::<(), _>("supplementary cleanup failure")
                })
                .await;
            let restored = std::panic::catch_unwind(AssertUnwindSafe(|| {
                cleanup.finish::<()>(Err(primary.take().unwrap()));
            }))
            .unwrap_err();
            assert!(settled);
            assert_eq!(restored.downcast_ref::<String>(), Some(&original_text));
            assert_eq!(
                *requests.lock().unwrap(),
                [
                    "GET /global/health",
                    "GET /experimental/session",
                    "GET /session/ses_001/message",
                ]
            );
        })
        .await;
}

impl ReadbackProbe {
    async fn start(response: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut buffer = [0_u8; 4096];
            let read = socket.read(&mut buffer).await?;
            if read == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }
            socket.write_all(&response).await?;
            socket.shutdown().await
        });
        Self {
            endpoint,
            task,
            requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            held_read: Arc::new(tokio::sync::Notify::new()),
        }
    }

    async fn complete_question_fixture(
        fixture: &EvidenceFixture,
        held_route: Option<&str>,
    ) -> (Self, Arc<AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let responses = BTreeMap::from([
            ("/global/health", json!({"healthy":true,"version":"1.19.3"})),
            ("/experimental/session", json!([{"id":"ses_001"}])),
            ("/question", fixture.pending.clone()),
            (
                "/session/ses_001/message/msg_200",
                fixture.assistant.clone(),
            ),
            ("/session/ses_001/message/msg_100", fixture.user.clone()),
            ("/provider", fixture.providers.clone()),
            ("/session/ses_001/message", fixture.completed.clone()),
        ]);
        let completed = Arc::new(AtomicBool::new(false));
        let observed_completion = completed.clone();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let held_read = Arc::new(tokio::sync::Notify::new());
        let held_seen = held_read.clone();
        let held_route = held_route.map(str::to_owned);
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut request = Vec::new();
                while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    let mut chunk = [0_u8; 1024];
                    let count = socket.read(&mut chunk).await?;
                    if count == 0 || request.len() + count > 4096 {
                        return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
                    }
                    request.extend_from_slice(&chunk[..count]);
                }
                let request = std::str::from_utf8(&request).map_err(std::io::Error::other)?;
                let mut words = request
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .split_whitespace();
                let method = words.next().unwrap_or_default();
                let target = words.next().unwrap_or_default();
                let url = reqwest::Url::parse(&format!("http://localhost{target}"))
                    .map_err(std::io::Error::other)?;
                if !url
                    .query_pairs()
                    .any(|(key, value)| key == "directory" && value == env!("CARGO_MANIFEST_DIR"))
                {
                    return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
                }
                let route = url.path();
                recorded.lock().unwrap().push(format!("{method} {route}"));
                if held_route.as_deref() == Some(route) {
                    held_seen.notify_one();
                    std::future::pending::<()>().await;
                }
                let value = responses.get(route);
                let (status, value) = match (method, route, value) {
                    ("GET", "/question", Some(_)) if observed_completion.load(Ordering::SeqCst) => {
                        ("200 OK", json!([]))
                    }
                    ("GET", _, Some(value)) => ("200 OK", value.clone()),
                    _ => ("404 Not Found", json!({"error":"unknown test-local route"})),
                };
                let body = serde_json::to_vec(&value).map_err(std::io::Error::other)?;
                let header = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await?;
                socket.write_all(&body).await?;
                socket.shutdown().await?;
            }
        });
        (
            Self {
                endpoint,
                task,
                requests,
                held_read,
            },
            completed,
        )
    }

    async fn observe(self, observation: impl Future<Output = ()>) {
        let observed = AssertUnwindSafe(observation).catch_unwind().await;
        // This single fixture task only accepts/reads/writes this one socket.
        // Stop it after observation, and retain its handle through the join.
        self.task.abort();
        let joined = self.task.await;
        let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
        cleanup
            .settle(async {
                match joined {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error))
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                        ) =>
                    {
                        Ok(())
                    }
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(error) if error.is_cancelled() => Ok(()),
                    Err(error) => Err(error.to_string()),
                }
            })
            .await;
        cleanup.finish(observed.map(Ok));
    }
}

#[tokio::test]
async fn native_readback_rejects_incomplete_bodies_and_paginated_evidence_before_grading() {
    let health = br#"{"healthy":true,"version":"1.19.3"}"#;
    let paginated = [format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nX-Next-Cursor: next\r\nConnection: close\r\n\r\n", health.len()).into_bytes(), health.to_vec()].concat();
    let mut oversized = health.to_vec();
    oversized.resize((16 << 20) + 1, b' ');
    let oversized = [
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            oversized.len()
        )
        .into_bytes(),
        oversized,
    ]
    .concat();
    for response in [
        b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"healthy\":true"
            .to_vec(),
        paginated,
        oversized,
    ] {
        let probe = ReadbackProbe::start(response).await;
        let endpoint = probe.endpoint.clone();
        probe
            .observe(async {
                let result = AssertUnwindSafe(NativeQuestionReadback::new(
                    &endpoint,
                    Instant::now() + Duration::from_secs(5),
                ))
                .catch_unwind()
                .await;
                assert!(
                    result.is_err(),
                    "incomplete or paginated native evidence must not yield a reader"
                );
            })
            .await;
    }
}

#[tokio::test]
async fn native_readback_accepts_a_complete_health_response() {
    let body = br#"{"healthy":true,"version":"1.19.3"}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    let probe = ReadbackProbe::start([response, body.to_vec()].concat()).await;
    let endpoint = probe.endpoint.clone();
    probe
        .observe(async {
            let reader =
                NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                    .await;
            assert_eq!(reader.version(), "1.19.3");
        })
        .await;
}

#[tokio::test]
async fn complete_native_readback_preserves_pending_to_completed_identity_and_answer() {
    let fixture = EvidenceFixture::new();
    let (probe, completed) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
    let endpoint = probe.endpoint.clone();
    probe
        .observe(async {
            let reader =
                NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                    .await;
            assert_eq!(
                reader.sessions().await,
                BTreeSet::from(["ses_001".to_owned()])
            );
            let session = reader.created_session(&BTreeSet::new()).await;
            let evidence = reader
                .pending(&session, EvidenceFixture::PROMPT, EvidenceFixture::MODEL)
                .await;
            completed.store(true, Ordering::SeqCst);
            let grade = reader
                .completed(
                    evidence,
                    &session,
                    EvidenceFixture::ANSWER,
                    EvidenceFixture::ANSWER,
                )
                .await;
            assert!(grade.model_exact_copy());
            assert_eq!(reader.version(), "1.19.3");
        })
        .await;
}

#[tokio::test]
async fn failed_question_capture_distinguishes_native_state_without_regrading() {
    for (state, text, error) in [
        (None, "", false),
        (Some("running"), "", false),
        (Some("completed"), "answer", false),
        (Some("error"), "", true),
    ] {
        let mut fixture = EvidenceFixture::new();
        if let Some(state) = state {
            fixture.completed[1]["parts"][0]["state"]["status"] = json!(state);
        }
        fixture.completed[2]["parts"][0]["text"] = json!(text);
        if error {
            fixture.completed[2]["info"]["error"] =
                json!({"name":"APIError","data":{"message":"PRIVATE ERROR"}});
        }
        if state.is_none() {
            fixture.completed.as_array_mut().unwrap().remove(1);
            fixture.pending = json!([]);
        }
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        let requests = probe.requests.clone();
        probe
            .observe(async {
                let reader =
                    NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                        .await;
                assert_eq!(
                    requests.lock().unwrap().len(),
                    1,
                    "ordinary reader setup adds no failure reads"
                );
                let mut primary = None;
                let report = reader
                    .capture_failure(
                        "ses_001",
                        EvidenceFixture::PROMPT,
                        &mut primary,
                        Box::new("original failure"),
                    )
                    .await;
                let context: Value = serde_json::from_str(&report).unwrap();
                assert_eq!(context["capture"], "complete");
                assert_eq!(context["question_tool_calls"], usize::from(state.is_some()));
                assert_eq!(
                    context["pending_same_session"],
                    usize::from(state.is_some())
                );
                if let Some(state) = state {
                    assert_eq!(context["assistants"][0]["question_states"][state], 1);
                }
                let terminal = context["assistants"].as_array().unwrap().last().unwrap();
                assert_eq!(terminal["text_bytes"], text.len());
                assert_eq!(terminal["completion"], "present");
                assert_eq!(terminal["finish"], "stop");
                assert_eq!(terminal["error"], if error { "api" } else { "absent" });
                assert_eq!(
                    primary.take().unwrap().downcast_ref::<&str>(),
                    Some(&"original failure")
                );
                assert_eq!(
                    *requests.lock().unwrap(),
                    [
                        "GET /global/health",
                        "GET /session/ses_001/message",
                        "GET /question"
                    ]
                );
                assert!(
                    !report.contains("PRIVATE ERROR")
                        && !report.contains("ses_001")
                        && !report.contains("msg_")
                );
            })
            .await;
    }
}

#[tokio::test]
async fn failed_question_capture_retains_counts_and_field_kinds_when_typed_shape_fails() {
    for (multiple, kind, valid) in [
        (None, "missing", false),
        (Some(Value::Null), "null", false),
        (Some(json!("PRIVATE MULTIPLE")), "invalid", false),
        (Some(json!(false)), "false", true),
        (Some(json!(true)), "true", true),
    ] {
        let mut fixture = EvidenceFixture::new();
        for question in [
            &mut fixture.pending[0]["questions"][0],
            &mut fixture.completed[1]["parts"][0]["state"]["input"]["questions"][0],
        ] {
            match &multiple {
                Some(value) => question["multiple"] = value.clone(),
                None => {
                    question.as_object_mut().unwrap().remove("multiple");
                }
            }
        }
        assert_eq!(
            QuestionEvidence::pending_origin(&serde_json::to_vec(&fixture.pending).unwrap())
                .is_ok(),
            valid,
            "diagnostic counting must not change strict decoding"
        );
        if multiple != Some(json!(false)) {
            assert!(
                fixture.begin().is_err(),
                "the exact false oracle remains blocking"
            );
        }
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        probe
            .observe(async {
                let reader =
                    NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                        .await;
                let mut primary = None;
                let report = reader
                    .capture_failure(
                        "ses_001",
                        EvidenceFixture::PROMPT,
                        &mut primary,
                        Box::new("original failure"),
                    )
                    .await;
                let context: Value = serde_json::from_str(&report).unwrap();
                assert_eq!(context["pending_questions"], 1);
                assert_eq!(context["pending_same_session"], 1);
                assert_eq!(context["questions_snapshot"], "after_failure");
                assert_eq!(
                    context["capture"],
                    if valid { "complete" } else { "incomplete" }
                );
                assert_eq!(
                    context["questions_read"],
                    if valid { "complete" } else { "questions_shape" }
                );
                for observed_kind in ["missing", "null", "invalid", "false", "true"] {
                    assert_eq!(
                        context["pending_multiple"][observed_kind],
                        usize::from(observed_kind == kind)
                    );
                    assert_eq!(
                        context["assistants"][0]["question_multiple"][observed_kind],
                        usize::from(observed_kind == kind)
                    );
                }
                assert_eq!(context["assistants"][0]["question_states"]["completed"], 1);
                assert!(report.len() <= 4096);
                for secret in [
                    "PRIVATE",
                    "ses_",
                    "msg_",
                    "que_",
                    "call_",
                    "Continue?",
                    EvidenceFixture::PROMPT,
                ] {
                    assert!(!report.contains(secret), "diagnostics leaked {secret}");
                }
                assert_eq!(
                    primary.take().unwrap().downcast_ref::<&str>(),
                    Some(&"original failure")
                );
            })
            .await;
    }
}

#[tokio::test]
async fn failed_question_capture_distinguishes_empty_ambiguous_and_invalid_inventories() {
    let fixture = EvidenceFixture::new();
    let mut foreign = fixture.pending[0].clone();
    foreign["id"] = json!("que_PRIVATE");
    foreign["sessionID"] = json!("ses_PRIVATE");
    for (inventory, count, same_session, valid) in [
        (json!([]), Some(0), Some(0), true),
        (json!([fixture.pending[0], foreign]), Some(2), Some(1), true),
        (json!([null]), Some(1), Some(0), false),
        (json!({"PRIVATE":"BODY"}), None, None, false),
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.pending = inventory;
        assert!(
            QuestionEvidence::pending_origin(&serde_json::to_vec(&fixture.pending).unwrap())
                .is_err()
        );
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        probe
            .observe(async {
                let reader =
                    NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                        .await;
                let mut primary = None;
                let report = reader
                    .capture_failure(
                        "ses_001",
                        EvidenceFixture::PROMPT,
                        &mut primary,
                        Box::new("original failure"),
                    )
                    .await;
                let context: Value = serde_json::from_str(&report).unwrap();
                assert_eq!(context["pending_questions"], json!(count));
                assert_eq!(context["pending_same_session"], json!(same_session));
                assert_eq!(context["questions_snapshot"], "after_failure");
                assert_eq!(
                    context["questions_read"],
                    if valid { "complete" } else { "questions_shape" }
                );
                assert_eq!(
                    context["capture"],
                    if valid { "complete" } else { "incomplete" }
                );
                assert!(primary.is_some());
                for secret in ["PRIVATE", "BODY", "ses_", "que_"] {
                    assert!(!report.contains(secret));
                }
            })
            .await;
    }
}

#[tokio::test]
async fn failed_question_capture_labels_later_inventory_without_replacing_original_panic() {
    let mut fixture = EvidenceFixture::new();
    fixture.pending[0]["questions"][0]
        .as_object_mut()
        .unwrap()
        .remove("multiple");
    let (probe, completed) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
    let endpoint = probe.endpoint.clone();
    let requests = probe.requests.clone();
    probe
        .observe(async {
            let reader =
                NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                    .await;
            let failure = match AssertUnwindSafe(reader.pending(
                "ses_001",
                EvidenceFixture::PROMPT,
                EvidenceFixture::MODEL,
            ))
            .catch_unwind()
            .await
            {
                Ok(_) => panic!("omitted multiple must still reject pending evidence"),
                Err(failure) => failure,
            };
            let original = failure.downcast_ref::<String>().unwrap().clone();
            assert!(original.contains("native question must have one exact tool origin"));
            assert!(original.contains("missing field `multiple`"));
            completed.store(true, Ordering::SeqCst);
            let mut primary = None;
            let report = reader
                .capture_failure("ses_001", EvidenceFixture::PROMPT, &mut primary, failure)
                .await;
            let context: Value = serde_json::from_str(&report).unwrap();
            assert_eq!(context["questions_snapshot"], "after_failure");
            assert_eq!(
                context["pending_questions"], 0,
                "later native state is not the original failed snapshot"
            );
            assert_eq!(
                primary.take().unwrap().downcast_ref::<String>(),
                Some(&original)
            );
            assert_eq!(
                *requests.lock().unwrap(),
                [
                    "GET /global/health",
                    "GET /question",
                    "GET /session/ses_001/message",
                    "GET /question"
                ]
            );
        })
        .await;
}

#[tokio::test]
async fn failed_question_capture_bounds_hostile_rows_and_reports_missing_correlation() {
    for malformed in [false, true] {
        let mut fixture = EvidenceFixture::new();
        fixture.pending = json!([]);
        let mut messages = vec![fixture.user.clone()];
        for index in 0..64 {
            let mut assistant = fixture.completed[2].clone();
            let id = format!("msg_{:04}", index + 200);
            assistant["info"]["id"] = json!(id);
            assistant["parts"][0]["messageID"] = json!(id);
            assistant["parts"][0]["text"] = json!("PRIVATE OUTPUT".repeat(1000));
            assistant["info"]["finish"] = json!("PRIVATE FINISH".repeat(1000));
            assistant["info"]["error"] =
                json!({"name":"PRIVATE ERROR","private_key":"DO NOT EMIT"});
            messages.push(assistant);
        }
        if malformed {
            messages[0]["parts"][0]["text"] = json!("a different prompt");
        }
        fixture.completed = json!(messages);
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        probe
            .observe(async {
                let reader =
                    NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                        .await;
                let mut primary = None;
                let report = reader
                    .capture_failure(
                        "ses_001",
                        EvidenceFixture::PROMPT,
                        &mut primary,
                        Box::new("original failure"),
                    )
                    .await;
                let context: Value = serde_json::from_str(&report).unwrap();
                if malformed {
                    assert_eq!(context["capture"], "incomplete");
                    assert_eq!(context["reason"], "root_correlation");
                } else {
                    assert_eq!(context["capture"], "complete");
                    assert_eq!(context["assistant_count"], 64);
                    assert_eq!(context["assistant_summaries_omitted"], 60);
                    assert_eq!(context["assistants"].as_array().unwrap().len(), 4);
                    assert_eq!(context["question_tool_calls"], 0);
                    assert_eq!(context["pending_same_session"], 0);
                }
                assert!(report.len() <= 4 * 1024);
                for secret in [
                    "PRIVATE",
                    "DO NOT EMIT",
                    "private_key",
                    "msg_",
                    "ses_",
                    EvidenceFixture::PROMPT,
                ] {
                    assert!(!report.contains(secret));
                }
                assert!(
                    primary.is_some(),
                    "diagnostics cannot turn the scenario failure into success"
                );
            })
            .await;
    }
}

#[tokio::test]
async fn failed_question_capture_keeps_primary_when_original_deadline_drops_reads() {
    let fixture = EvidenceFixture::new();
    let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, Some("/question")).await;
    let endpoint = probe.endpoint.clone();
    let requests = probe.requests.clone();
    let held_read = probe.held_read.clone();
    probe
        .observe(async {
            let reader =
                NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                    .await;
            let mut primary = None;
            let mut capture = Box::pin(reader.capture_failure(
                "ses_001",
                EvidenceFixture::PROMPT,
                &mut primary,
                Box::new("original failure"),
            ));
            tokio::select! {
                result = &mut capture => panic!("capture completed before its held read: {result}"),
                _ = held_read.notified() => {}
            }
            let observed = NativeCleanup::observe(Instant::now(), capture).await;
            assert!(
                matches!(observed, Ok(Err(_))),
                "the original cutoff must cancel diagnostic observation"
            );
            let failure = primary
                .take()
                .expect("the failure must outlive the dropped read");
            let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
            let mut settled = false;
            cleanup
                .settle(async {
                    settled = true;
                    Ok::<(), ()>(())
                })
                .await;
            let restored =
                std::panic::catch_unwind(AssertUnwindSafe(|| cleanup.finish::<()>(Err(failure))));
            assert!(settled);
            assert_eq!(
                restored.unwrap_err().downcast_ref::<&str>(),
                Some(&"original failure")
            );
            assert_eq!(
                *requests.lock().unwrap(),
                [
                    "GET /global/health",
                    "GET /session/ses_001/message",
                    "GET /question"
                ]
            );
        })
        .await;
}

#[tokio::test]
async fn failed_question_capture_reports_transport_and_shape_failures_without_masking_primary() {
    let mut fixture = EvidenceFixture::new();
    fixture.completed = json!({"private_key":"PRIVATE BODY"});
    let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
    let endpoint = probe.endpoint.clone();
    let requests = probe.requests.clone();
    probe
        .observe(async {
            let reader =
                NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                    .await;
            let mut primary = None;
            let report = reader
                .capture_failure(
                    "ses_001",
                    EvidenceFixture::PROMPT,
                    &mut primary,
                    Box::new("first failure"),
                )
                .await;
            assert_eq!(
                serde_json::from_str::<Value>(&report).unwrap()["reason"],
                "messages_shape"
            );
            assert!(!report.contains("PRIVATE BODY"));
            let missing = reader
                .capture_failure(
                    "ses_missing",
                    EvidenceFixture::PROMPT,
                    &mut primary,
                    Box::new("later failure"),
                )
                .await;
            let context: Value = serde_json::from_str(&missing).unwrap();
            assert_eq!(context["capture"], "incomplete");
            assert_eq!(context["messages"], "http_status");
            assert!(!missing.contains("ses_missing"));
            assert_eq!(
                primary.take().unwrap().downcast_ref::<&str>(),
                Some(&"first failure")
            );
            assert_eq!(
                requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|request| request.as_str() == "GET /session/ses_missing/message")
                    .count(),
                1
            );
        })
        .await;
}

#[test]
fn fresh_reopen_expectation_preserves_exact_text_and_bounds() {
    for expected in ["PENDING_INTERACTION_REOPENED-123", "a*é\n"] {
        let fresh = FreshOutputExpectation::new(expected.to_owned());
        assert_eq!(fresh.expected(), expected);
        assert_eq!(
            fresh.prompt(),
            format!("Do not call any tool. Reply with exactly {expected} and nothing else.")
        );
    }
    let maximum = "é".repeat(128);
    assert_eq!(
        FreshOutputExpectation::new(maximum.clone()).expected(),
        maximum
    );
    for invalid in [String::new(), "é".repeat(129)] {
        let rejected = std::panic::catch_unwind(|| FreshOutputExpectation::new(invalid));
        assert!(rejected.is_err());
    }
}

#[tokio::test]
async fn fresh_reopen_capture_observes_success_without_manufacturing_failure() {
    let mut fixture = EvidenceFixture::new();
    fixture.completed[0]["info"]["time"] = json!({"created":1});
    let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
    let endpoint = probe.endpoint.clone();
    probe
        .observe(async {
            let deadline = Instant::now() + Duration::from_secs(5);
            let observed = NativeCleanup::observe(deadline, async {
                assert_eq!(
                    fixture.completed[2]["parts"][0]["text"],
                    EvidenceFixture::ANSWER
                );
            })
            .await;
            let capture = NativeCleanup::observe(deadline, async {
                let reader = NativeQuestionReadback::new(&endpoint, deadline).await;
                reader
                    .prompt_output(
                        EvidenceFixture::PROMPT,
                        "provider",
                        "model",
                        EvidenceFixture::ANSWER,
                        EvidenceFixture::ANSWER,
                    )
                    .await
            })
            .await;
            let context = capture.unwrap().unwrap().unwrap();
            let report = context.to_string();
            assert_eq!(context["capture"], "complete");
            assert_eq!(context["kind"], "native_terminal_snapshot");
            assert_eq!(context["foreground_http_body_observed"], false);
            assert_eq!(context["portable_equals_expected"], true);
            assert_eq!(context["matching_users"], 1);
            assert_eq!(context["terminal_count"], 1);
            assert_eq!(context["model_matches_configured"], true);
            assert_eq!(context["native_equals_expected"], true);
            assert_eq!(context["native_equals_portable"], true);
            assert!(!report.contains(EvidenceFixture::ANSWER));
            NativeCleanup::new(Duration::from_secs(1)).finish(observed);
        })
        .await;
}

#[tokio::test]
async fn fresh_reopen_capture_elapsed_deadline_preserves_assertion_and_cleanup() {
    for matches_expected in [true, false] {
        let fixture = EvidenceFixture::new();
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        let requests = probe.requests.clone();
        probe
            .observe(async {
                let observed =
                    NativeCleanup::observe(Instant::now() + Duration::from_secs(5), async {
                        assert!(matches_expected, "retained exact-output failure");
                    })
                    .await;
                let deadline = Instant::now();
                let capture = NativeCleanup::observe(deadline, async {
                    let reader = NativeQuestionReadback::new(&endpoint, deadline).await;
                    reader
                        .prompt_output(
                            EvidenceFixture::PROMPT,
                            "provider",
                            "model",
                            EvidenceFixture::ANSWER,
                            EvidenceFixture::ANSWER,
                        )
                        .await
                })
                .await;
                assert!(!matches!(capture, Ok(Ok(_))));
                assert!(requests.lock().unwrap().is_empty());
                let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
                let mut settled = false;
                cleanup
                    .settle(async {
                        settled = true;
                        Ok::<(), ()>(())
                    })
                    .await;
                let finished = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    cleanup.finish(observed);
                }));
                assert!(settled);
                if matches_expected {
                    assert!(finished.is_ok());
                } else {
                    let failure = finished.unwrap_err();
                    let message = failure
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| failure.downcast_ref::<String>().map(String::as_str));
                    assert_eq!(message, Some("retained exact-output failure"));
                }
            })
            .await;
    }
}

#[tokio::test]
async fn fresh_reopen_capture_setup_failure_keeps_success_and_cleanup() {
    let body = json!({"healthy": "PRIVATE_NATIVE_HEALTH_PROSE", "version": "1.19.3"}).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let probe = ReadbackProbe::start(response.into_bytes()).await;
    let endpoint = probe.endpoint.clone();
    probe
        .observe(async {
            let deadline = Instant::now() + Duration::from_secs(5);
            let observed = NativeCleanup::observe(deadline, async {}).await;
            let capture = NativeCleanup::observe(deadline, async {
                NativeQuestionReadback::new(&endpoint, deadline).await;
            })
            .await;
            assert!(capture.is_err());
            let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
            let mut settled = false;
            cleanup
                .settle(async {
                    settled = true;
                    Ok::<(), ()>(())
                })
                .await;
            cleanup.finish(observed);
            assert!(settled);
        })
        .await;
}

#[tokio::test]
async fn fresh_reopen_capture_setup_redacts_native_health_without_replacing_primary() {
    let native_marker = "PRIVATE_NATIVE_HEALTH_PROSE";
    let body = json!({"healthy":native_marker,"version":"1.19.3"}).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let probe = ReadbackProbe::start(response.into_bytes()).await;
    let endpoint = probe.endpoint.clone();
    probe
        .observe(async {
            let original = std::panic::catch_unwind(|| {
                assert_eq!("No", "FRESH_REOPEN_TOKEN", "original exact-output failure");
            })
            .unwrap_err();
            let original_message = original.downcast_ref::<String>().unwrap().clone();
            // Match the live caller's ownership: the first failure is saved before
            // reader setup and remains outside the cancellable observation.
            let mut primary = Some(original);
            let deadline = Instant::now() + Duration::from_secs(5);
            let setup = NativeCleanup::observe(deadline, async {
                NativeQuestionReadback::new(&endpoint, deadline).await;
            })
            .await;
            let setup_failure = setup.expect_err("malformed native health must fail setup");
            let setup_message = setup_failure
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| setup_failure.downcast_ref::<&str>().copied())
                .expect("setup failure must retain a safe diagnostic message");
            assert!(
                !setup_message.contains(native_marker),
                "reader setup exposed arbitrary native health prose"
            );
            assert!(setup_message.len() < 256);

            let mut cleanup = NativeCleanup::new(Duration::from_secs(1));
            let mut settled = false;
            cleanup
                .settle(async {
                    settled = true;
                    Ok::<(), ()>(())
                })
                .await;
            let restored = std::panic::catch_unwind(AssertUnwindSafe(|| {
                cleanup.finish::<()>(Err(primary.take().expect("original failure stays owned")));
            }));
            assert!(settled);
            assert_eq!(
                restored.unwrap_err().downcast_ref::<String>(),
                Some(&original_message)
            );
        })
        .await;
}

#[tokio::test]
async fn fresh_reopen_capture_links_output_without_printing_native_content() {
    for parent in ["msg_100", "msg_stale"] {
        let mut fixture = EvidenceFixture::new();
        fixture.completed[0]["info"]["time"] = json!({"created":1});
        fixture.completed[2]["info"]["parentID"] = json!(parent);
        fixture.completed[2]["parts"][0]["text"] = json!("PRIVATE NATIVE ANSWER");
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        let requests = probe.requests.clone();
        probe
            .observe(async {
                let reader =
                    NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                        .await;
                let captured = reader
                    .prompt_output(
                        EvidenceFixture::PROMPT,
                        "provider",
                        "model",
                        EvidenceFixture::ANSWER,
                        "PRIVATE NATIVE ANSWER",
                    )
                    .await;
                let context = if parent == "msg_100" {
                    let context =
                        captured.expect("matching parent must establish the terminal snapshot");
                    assert_eq!(context["capture"], "complete");
                    assert_eq!(context["kind"], "native_terminal_snapshot");
                    assert_eq!(context["foreground_http_body_observed"], false);
                    assert_eq!(context["matching_users"], 1);
                    assert_eq!(context["terminal_count"], 1);
                    assert_eq!(context["model_matches_configured"], true);
                    assert_eq!(context["native_equals_portable"], true);
                    assert_eq!(context["native_equals_expected"], false);
                    assert_eq!(context["finish"], "stop");
                    context
                } else {
                    // The retained live snapshot contract refuses attribution to
                    // a stale parent; a diagnostic summary cannot establish parity.
                    let reason =
                        captured.expect_err("stale parent must not establish native parity");
                    assert_eq!(reason, "terminal_missing");
                    json!({"capture":"incomplete","reason":reason})
                };
                let report = context.to_string();
                for secret in [
                    "PRIVATE",
                    "msg_",
                    "ses_",
                    "prt_",
                    EvidenceFixture::PROMPT,
                    // Exact serialized values avoid matching the intentional
                    // model_matches_configured schema key.
                    "\"provider\"",
                    "\"model\"",
                    EvidenceFixture::ANSWER,
                ] {
                    assert!(!report.contains(secret), "diagnostic leaked {secret}");
                }
                assert!(report.len() < 4096);
                assert_eq!(
                    *requests.lock().unwrap(),
                    [
                        "GET /global/health",
                        "GET /experimental/session",
                        "GET /session/ses_001/message"
                    ]
                );
            })
            .await;
    }
}

#[tokio::test]
async fn fresh_reopen_capture_refuses_ambiguous_or_invalid_evidence() {
    for malformed in [
        "duplicate_user",
        "duplicate_part",
        "foreign_part",
        "unknown_finish",
    ] {
        let mut fixture = EvidenceFixture::new();
        fixture.completed[0]["info"]["time"] = json!({"created":1});
        match malformed {
            "duplicate_user" => {
                let mut user = fixture.completed[0].clone();
                user["info"]["id"] = json!("msg_other");
                user["parts"][0]["id"] = json!("prt_other");
                user["parts"][0]["messageID"] = json!("msg_other");
                fixture.completed.as_array_mut().unwrap().push(user);
            }
            "duplicate_part" => fixture.completed[2]["parts"][0]["id"] = json!("prt_100"),
            "foreign_part" => fixture.completed[2]["parts"][0]["sessionID"] = json!("ses_foreign"),
            "unknown_finish" => fixture.completed[2]["info"]["finish"] = json!("unknown"),
            _ => unreachable!(),
        }
        let (probe, _) = ReadbackProbe::complete_question_fixture(&fixture, None).await;
        let endpoint = probe.endpoint.clone();
        probe
            .observe(async {
                let reader =
                    NativeQuestionReadback::new(&endpoint, Instant::now() + Duration::from_secs(5))
                        .await;
                let captured = reader
                    .prompt_output(
                        EvidenceFixture::PROMPT,
                        "provider",
                        "model",
                        EvidenceFixture::ANSWER,
                        "No",
                    )
                    .await;
                // Unknown completion is no longer only a nonterminal summary: the
                // authoritative snapshot rejects it, like the other invalid evidence.
                assert!(captured.is_err(), "{malformed}: {captured:?}");
            })
            .await;
    }
}
