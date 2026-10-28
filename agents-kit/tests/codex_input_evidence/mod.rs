//! Bounded, passive facts from the existing Codex stdio bridge and its own rollout.
mod rollout;

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

#[derive(Default)]
pub(crate) struct CodexInputObservation {
    request: Option<Value>,
    responses: usize,
    unsupported: bool,
    expected: Option<(PathBuf, String, String, String)>,
    frames: Vec<Value>,
    projected_bytes: usize,
    callback_identity: Option<(String, String, String)>,
    pending_sdk: Option<usize>,
    pending_native: Option<usize>,
    requests: BTreeMap<String, (String, Option<bool>)>,
    initialized: bool,
    thread: Option<(String, PathBuf, PathBuf)>,
    turn: Option<String>,
    submitted: Option<Value>,
    submitted_exact: bool,
    terminal_identity: Option<(String, String)>,
    stderr_observed: bool,
    cleanup: Option<(bool, bool)>,
    terminal: Option<Value>,
    effective_features: BTreeMap<String, bool>,
    features_complete: bool,
    rollout: Option<Value>,
    failures: BTreeSet<&'static str>,
}

impl CodexInputObservation {
    const EMPTY_CONTEXT_PREFIX: &'static str = "Managed context:\n(none)\n\nUser request:\n";
    const MAX_RECORDS: usize = 8_192;
    const MAX_LINE: usize = 1_048_576;
    const MAX_ROLLOUT: usize = 8 * 1_048_576;
    const CASE: &'static str = "strict_live_suite::delivery_and_events::codex_nonblocking_input_returns_unsupported_without_actionable_notification";

    pub(crate) fn prepare_expected(&mut self, cwd: &Path, model: &str, prompt: &str) {
        if self.expected.is_some()
            || !cwd.is_absolute()
            || model.is_empty()
            || model.len() > 1_024
            || prompt.is_empty()
            || prompt
                .len()
                .saturating_add(Self::EMPTY_CONTEXT_PREFIX.len())
                > Self::MAX_LINE
        {
            self.mark_incomplete("expected_identity_invalid");
            return;
        }
        // This fixture has no managed knowledge. Bind the complete Kit rendering as well
        // as the original caller text; do not accept a native prefix or suffix by stripping it.
        let native_prompt = format!("{}{prompt}", Self::EMPTY_CONTEXT_PREFIX);
        self.expected = Some((
            cwd.to_owned(),
            model.to_owned(),
            prompt.to_owned(),
            native_prompt,
        ));
    }

    fn facts(text: &str) -> Value {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        json!({"bytes":text.len(), "fingerprint":format!("{:016x}",hasher.finish())})
    }

    fn identity(value: &Value) -> Option<String> {
        match value {
            Value::String(text) if !text.is_empty() && text.len() <= 1_024 => {
                Some(value.to_string())
            }
            Value::Number(number) if number.is_i64() || number.is_u64() => Some(value.to_string()),
            _ => None,
        }
    }

    fn known_method(method: &str) -> &str {
        match method {
            "initialize"
            | "initialized"
            | "model/list"
            | "thread/start"
            | "thread/resume"
            | "thread/read"
            | "thread/started"
            | "thread/readState/changed"
            | "thread/status/changed"
            | "experimentalFeature/list"
            | "turn/start"
            | "turn/started"
            | "turn/completed"
            | "item/started"
            | "item/completed"
            | "item/tool/requestUserInput"
            | "item/agentMessage/delta"
            | "item/reasoning/summaryTextDelta"
            | "item/reasoning/summaryPartAdded"
            | "item/reasoning/textDelta"
            | "thread/tokenUsage/updated"
            | "error"
            | "warning" => method,
            _ => "unknown",
        }
    }

    fn begin_frame(&mut self, frame: &Value, sdk: bool) -> Option<usize> {
        let pending = if sdk {
            self.pending_sdk
        } else {
            self.pending_native
        };
        if pending.is_some() {
            self.mark_incomplete("forward_completion_missing");
        }
        if !frame.is_object()
            || frame.to_string().len() > Self::MAX_LINE
            || self.frames.len() >= Self::MAX_RECORDS
            || self.projected_bytes > Self::MAX_LINE
        {
            self.mark_incomplete("wire_frame_bound_or_shape");
            return None;
        }
        let method = frame.get("method").and_then(Value::as_str);
        let id = frame.get("id").and_then(Self::identity);
        let index = self.frames.len();
        let mut projected = json!({"sequence":index, "direction":if sdk {"sdk_to_native"} else {"native_to_sdk"},
            "method":method.map(Self::known_method), "request_id":id.as_deref().map(Self::facts),
            "forwarded":null, "error_code":frame["error"]["code"].as_i64()});
        if method.is_some_and(|method| Self::known_method(method) == "unknown") {
            projected["unknown_method"] = Self::facts(method.unwrap_or_default());
        }
        if frame.get("id").is_some() && id.is_none() {
            self.mark_incomplete("wire_request_id_invalid");
        }
        self.frames.push(projected);
        if sdk {
            self.pending_sdk = Some(index);
        } else {
            self.pending_native = Some(index);
        }
        Some(index)
    }

    pub(crate) fn observe_sdk(&mut self, frame: &Value) {
        if frame.get("method").is_none() && self.request.as_ref() == frame.get("id") {
            self.responses += 1;
            self.unsupported = frame["error"]["code"] == -32601 && frame.get("result").is_none();
        }
        let Some(index) = self.begin_frame(frame, true) else {
            return;
        };
        let Some(method) = frame["method"].as_str() else {
            return;
        };
        if method.len() > 256 || self.requests.len() >= 256 {
            self.mark_incomplete("pending_request_bound");
            return;
        }
        if let Some(id) = Self::identity(&frame["id"])
            && self
                .requests
                .insert(
                    id,
                    (method.to_owned(), frame["params"]["includeTurns"].as_bool()),
                )
                .is_some()
        {
            self.mark_incomplete("duplicate_sdk_request_id");
        }
        let params = &frame["params"];
        match method {
            "initialize" => {
                self.frames[index]["experimental_api"] = params["capabilities"]["experimentalApi"]
                    .as_bool()
                    .map(Value::Bool)
                    .unwrap_or(Value::Null);
                let excluded = params["capabilities"]["optOutNotificationMethods"].as_array();
                self.frames[index]["callback_notification_opted_out"] =
                    json!(excluded.is_some_and(|methods| {
                        methods
                            .iter()
                            .any(|value| value == "item/tool/requestUserInput")
                    }));
            }
            "thread/start" | "thread/resume" => {
                let mut settings = serde_json::Map::new();
                for key in [
                    "features.multi_agent",
                    "features.multi_agent_v2",
                    "features.default_mode_request_user_input",
                    "features.apps",
                    "features.code_mode",
                    "tools.experimental_request_user_input.enabled",
                ] {
                    if let Some(value) = params["config"].get(key) {
                        if value.is_boolean() {
                            settings.insert(key.to_owned(), value.clone());
                        } else {
                            self.mark_incomplete("selected_config_type_invalid");
                        }
                    }
                }
                self.frames[index]["selected_config"] = Value::Object(settings);
                self.frames[index]["cwd"] = params["cwd"]
                    .as_str()
                    .map(Self::facts)
                    .unwrap_or(Value::Null);
                self.frames[index]["dynamic_tools_count"] = params["dynamicTools"]
                    .as_array()
                    .map(|items| json!(items.len()))
                    .unwrap_or(Value::Null);
            }
            "experimentalFeature/list" | "thread/read" => {
                self.frames[index]["thread"] = params["threadId"]
                    .as_str()
                    .map(Self::facts)
                    .unwrap_or(Value::Null);
                if let Some((thread, _, _)) = &self.thread
                    && params["threadId"].as_str() != Some(thread.as_str())
                {
                    self.mark_incomplete("read_thread_mismatch");
                }
                self.frames[index]["include_turns"] = params["includeTurns"]
                    .as_bool()
                    .map(Value::Bool)
                    .unwrap_or(Value::Null);
            }
            "turn/start" => {
                if self.submitted.is_some() {
                    self.mark_incomplete("multiple_turn_submissions");
                }
                let input = params["input"].as_array();
                let prompt = input.filter(|items| items.len() == 1).and_then(|items| {
                    (items[0]["type"] == "text")
                        .then(|| items[0]["text"].as_str())
                        .flatten()
                });
                let projected = json!({"thread":params["threadId"].as_str().map(Self::facts),
                    "cwd":params["cwd"].as_str().map(Self::facts), "model":params["model"].as_str().map(Self::facts),
                    "prompt":prompt.map(Self::facts), "input_count":input.map(Vec::len),
                    "client_message_id":params["clientUserMessageId"].as_str().map(Self::facts),
                    "collaboration_mode_present":params.get("collaborationMode").is_some(),
                    "effort_present":params.get("effort").is_some(), "output_schema_present":params.get("outputSchema").is_some()});
                self.submitted_exact = self.expected.as_ref().is_some_and(
                    |(cwd, model, _, expected_native_prompt)| {
                        params["cwd"].as_str() == cwd.to_str()
                            && params["model"].as_str() == Some(model.as_str())
                            && prompt == Some(expected_native_prompt.as_str())
                            && self.thread.as_ref().is_some_and(|thread| {
                                params["threadId"].as_str() == Some(thread.0.as_str())
                            })
                    },
                );
                self.frames[index]["submission"] = projected.clone();
                self.submitted = Some(projected);
                if prompt.is_none() || params["clientUserMessageId"].as_str().is_none() {
                    self.mark_incomplete("turn_submission_shape");
                }
            }
            _ => {}
        }
    }

    pub(crate) fn observe_native(&mut self, frame: &Value) {
        if frame["method"] == "item/tool/requestUserInput" {
            assert!(
                self.request.is_none(),
                "the native request must occur exactly once"
            );
            assert_eq!(
                frame["params"]["isBlocking"], false,
                "real default-mode input must be nonblocking"
            );
            self.request = Some(frame["id"].clone());
        }
        let Some(index) = self.begin_frame(frame, false) else {
            return;
        };
        if let Some(method) = frame["method"].as_str() {
            let params = &frame["params"];
            if let Some(thread) = params["threadId"].as_str() {
                self.frames[index]["thread"] = Self::facts(thread);
                if self.thread.as_ref().is_some_and(|known| known.0 != thread) {
                    self.mark_incomplete("notification_thread_mismatch");
                }
            }
            if let Some(turn) = params["turnId"].as_str() {
                self.frames[index]["turn"] = Self::facts(turn);
                // Notifications may precede the response accepting the same turn.
                if self.turn.as_ref().is_some_and(|known| known != turn) {
                    self.mark_incomplete("notification_turn_mismatch");
                }
            }
            match method {
                "turn/completed" => {
                    if self.terminal.is_some() {
                        self.mark_incomplete("duplicate_terminal");
                    }
                    if let (Some(thread), Some(turn)) =
                        (params["threadId"].as_str(), params["turn"]["id"].as_str())
                    {
                        self.terminal_identity = Some((thread.to_owned(), turn.to_owned()));
                    }
                    self.terminal = Some(
                        json!({"thread":params["threadId"].as_str().map(Self::facts),
                        "turn":params["turn"]["id"].as_str().map(Self::facts),
                        "status":Self::terminal_status(&params["turn"]["status"]),
                        "error_present":!params["turn"]["error"].is_null()}),
                    );
                    self.frames[index]["terminal"] = self.terminal.clone().unwrap_or(Value::Null);
                }
                "item/tool/requestUserInput" => {
                    match (
                        params["threadId"].as_str(),
                        params["turnId"].as_str(),
                        params["itemId"].as_str(),
                    ) {
                        (Some(thread), Some(turn), Some(item)) => {
                            self.callback_identity =
                                Some((thread.to_owned(), turn.to_owned(), item.to_owned()))
                        }
                        _ => self.mark_incomplete("callback_identity_missing"),
                    }
                    self.frames[index]["callback"] = json!({"is_blocking":params["isBlocking"].as_bool(),
                        "item":params["itemId"].as_str().map(Self::facts), "question_count":params["questions"].as_array().map(Vec::len)});
                }
                "item/started" | "item/completed" => {
                    self.frames[index]["item"] = Self::item_facts(&params["item"]);
                }
                "warning" => {
                    self.mark_incomplete("native_warning_observed");
                }
                "error" => {
                    self.frames[index]["native_error"] = json!({"will_retry":params["willRetry"].as_bool(),
                        "message":params["error"]["message"].as_str().map(Self::facts)});
                }
                _ => {}
            }
            return;
        }
        let Some(id) = Self::identity(&frame["id"]) else {
            self.mark_incomplete("native_response_id_invalid");
            return;
        };
        let Some((method, include_turns)) = self.requests.remove(&id) else {
            self.mark_incomplete("unmatched_native_response");
            return;
        };
        self.frames[index]["response_to"] = json!(Self::known_method(&method));
        if !frame["error"].is_null() {
            self.frames[index]["native_error"] = json!({"code":frame["error"]["code"].as_i64(),
                "message":frame["error"]["message"].as_str().map(Self::facts)});
            return;
        }
        let result = &frame["result"];
        match method.as_str() {
            "initialize" => {
                self.initialized = result.is_object();
            }
            "thread/start" | "thread/resume" => {
                let thread = &result["thread"];
                if self.thread.is_some() {
                    self.mark_incomplete("multiple_threads");
                }
                match (
                    thread["id"].as_str(),
                    thread["cwd"].as_str(),
                    thread["path"].as_str(),
                ) {
                    (Some(id), Some(cwd), Some(path))
                        if !id.is_empty()
                            && Path::new(cwd).is_absolute()
                            && Path::new(path).is_absolute()
                            && thread["parentThreadId"].is_null() =>
                    {
                        self.thread =
                            Some((id.to_owned(), PathBuf::from(cwd), PathBuf::from(path)));
                        self.frames[index]["thread_identity"] = json!({"id":Self::facts(id),"cwd":Self::facts(cwd),"path":Self::facts(path),"root":true,
                            "model":result["model"].as_str().map(Self::facts)});
                    }
                    _ => self.mark_incomplete("native_thread_identity_invalid"),
                }
            }
            "turn/start" => {
                match result["turn"]["id"].as_str() {
                    Some(turn) if !turn.is_empty() && self.turn.is_none() => {
                        self.turn = Some(turn.to_owned());
                    }
                    _ => self.mark_incomplete("accepted_turn_identity_invalid"),
                }
                self.frames[index]["accepted_turn"] = json!({"id":result["turn"]["id"].as_str().map(Self::facts),"status":Self::terminal_status(&result["turn"]["status"])});
            }
            "experimentalFeature/list" => {
                if let Some(entries) = result["data"]
                    .as_array()
                    .filter(|entries| entries.len() <= Self::MAX_RECORDS)
                {
                    for entry in entries {
                        if let Some(
                            name @ ("default_mode_request_user_input"
                            | "multi_agent"
                            | "multi_agent_v2"
                            | "apps"
                            | "code_mode"),
                        ) = entry["name"].as_str()
                        {
                            if let Some(enabled) = entry["enabled"].as_bool() {
                                if self
                                    .effective_features
                                    .insert(name.to_owned(), enabled)
                                    .is_some()
                                {
                                    self.mark_incomplete("duplicate_effective_feature");
                                }
                            } else {
                                self.mark_incomplete("effective_feature_type_invalid");
                            }
                        }
                    }
                    self.features_complete = result["nextCursor"].is_null();
                } else {
                    self.mark_incomplete("effective_feature_list_invalid");
                }
            }
            "thread/read" => {
                let thread = &result["thread"];
                if self
                    .thread
                    .as_ref()
                    .is_some_and(|known| thread["id"].as_str() != Some(known.0.as_str()))
                {
                    self.mark_incomplete("history_thread_mismatch");
                }
                let turns = thread["turns"].as_array();
                if thread.get("turns").is_none() && include_turns == Some(false) {
                    self.frames[index]["history"] =
                        json!({"scope":"turns_not_requested","turns_present":false});
                } else if let Some(turns) = turns.filter(|turns| turns.len() <= 32) {
                    if turns.iter().any(|turn| {
                        turn["items"]
                            .as_array()
                            .is_some_and(|items| items.len() > 256)
                    }) {
                        self.mark_incomplete("history_item_bound");
                    }
                    self.frames[index]["history"] = json!({"scope":"ordinary_app_server_history_not_exhaustive", "turns":turns.iter().map(|turn| json!({
                        "id":turn["id"].as_str().map(Self::facts), "status":Self::terminal_status(&turn["status"]),
                        "items_view":turn["itemsView"].as_str().filter(|value| matches!(*value,"notLoaded"|"loaded")),
                        "items":turn["items"].as_array().map(|items| items.iter().take(256).map(Self::item_facts).collect::<Vec<_>>())})).collect::<Vec<_>>()});
                } else {
                    self.mark_incomplete("history_shape_or_bound");
                }
            }
            _ => {}
        }
    }

    fn terminal_status(value: &Value) -> &str {
        match value.as_str() {
            Some(status @ ("inProgress" | "completed" | "failed" | "interrupted")) => status,
            _ => "unknown",
        }
    }

    fn item_facts(item: &Value) -> Value {
        let kind = item["type"]
            .as_str()
            .filter(|kind| {
                matches!(
                    *kind,
                    "userMessage"
                        | "agentMessage"
                        | "reasoning"
                        | "commandExecution"
                        | "fileChange"
                        | "mcpToolCall"
                        | "dynamicToolCall"
                        | "collabAgentToolCall"
                        | "webSearch"
                        | "imageView"
                        | "enteredReviewMode"
                        | "exitedReviewMode"
                        | "contextCompaction"
                )
            })
            .unwrap_or("unknown");
        let mut result = json!({"kind":kind,"id":item["id"].as_str().map(Self::facts)});
        if kind == "agentMessage" {
            result["text"] = item["text"]
                .as_str()
                .map(Self::facts)
                .unwrap_or(Value::Null);
        }
        result
    }

    pub(crate) fn finish_sdk_forwarding(&mut self, succeeded: bool) {
        self.finish_forwarding(true, succeeded);
    }
    pub(crate) fn finish_native_forwarding(&mut self, succeeded: bool) {
        self.finish_forwarding(false, succeeded);
    }
    fn finish_forwarding(&mut self, sdk: bool, succeeded: bool) {
        let pending = if sdk {
            self.pending_sdk.take()
        } else {
            self.pending_native.take()
        };
        if let Some(index) = pending {
            self.frames[index]["forwarded"] = json!(succeeded);
            self.projected_bytes = self
                .projected_bytes
                .saturating_add(self.frames[index].to_string().len());
            if self.projected_bytes > Self::MAX_LINE {
                self.mark_incomplete("wire_projection_bound");
            }
        } else {
            self.mark_incomplete("forward_completion_without_frame");
        }
        if !succeeded {
            self.mark_incomplete("forwarding_failed");
        }
    }

    pub(crate) fn observe_stderr(&mut self, bytes: &[u8]) {
        if self.stderr_observed {
            self.mark_incomplete("duplicate_stderr_read");
        }
        self.stderr_observed = true;
        if bytes.len() > Self::MAX_LINE {
            self.mark_incomplete("stderr_bound");
            return;
        }
        let Ok(text) = std::str::from_utf8(bytes) else {
            self.mark_incomplete("stderr_utf8");
            return;
        };
        if [
            "failed to flush rollout",
            "failed to record rollout items",
            "rollout writer task failed",
            "rollout writer failed",
            "failed to queue rollout",
            "failed waiting for rollout",
            "failed to materialize thread persistence",
            "failed to check whether thread persistence is materialized",
            "buffered rollout items will be retried",
            "retrying rollout writer",
            "failed to flush interrupted-turn marker",
        ]
        .iter()
        .any(|needle| text.contains(needle))
            || (text.contains("rollout writer") && text.contains("failed to "))
        {
            self.mark_incomplete("native_rollout_persistence_warning");
        }
    }

    pub(crate) fn record_cleanup(&mut self, hierarchy: bool, process: bool) {
        self.cleanup = Some((hierarchy, process));
    }

    pub(crate) fn mark_incomplete(&mut self, reason: &'static str) {
        self.failures.insert(reason);
    }
    pub(crate) fn rollout_path(&self) -> Option<PathBuf> {
        self.thread.as_ref().map(|thread| thread.2.clone())
    }

    fn identity_matches(&self) -> bool {
        let (
            Some((cwd, model, _, native_prompt)),
            Some((thread, native_cwd, _)),
            Some(turn),
            Some(submitted),
            Some(terminal),
        ) = (
            &self.expected,
            &self.thread,
            &self.turn,
            &self.submitted,
            &self.terminal,
        )
        else {
            return false;
        };
        self.submitted_exact
            && self
                .terminal_identity
                .as_ref()
                .is_some_and(|native| &native.0 == thread && &native.1 == turn)
            && native_cwd == cwd
            && submitted["cwd"] == Self::facts(cwd.to_str().unwrap_or_default())
            && submitted["model"] == Self::facts(model)
            && submitted["prompt"] == Self::facts(native_prompt)
            && submitted["thread"] == Self::facts(thread)
            && terminal["thread"] == Self::facts(thread)
            && terminal["turn"] == Self::facts(turn)
            && terminal["status"] != "unknown"
    }

    pub(crate) fn observe_rollout(&mut self, bytes: &[u8]) -> Result<(), &'static str> {
        if self.rollout.is_some() {
            self.mark_incomplete("duplicate_rollout_read");
            return Err("duplicate_rollout_read");
        }
        match self.project_rollout(bytes) {
            Ok(projection) => {
                self.rollout = Some(projection);
                Ok(())
            }
            Err(reason) => {
                self.mark_incomplete(reason);
                self.rollout = Some(json!({"complete":false,"failure":reason}));
                Err(reason)
            }
        }
    }

    fn project_rollout(&self, bytes: &[u8]) -> Result<Value, &'static str> {
        if bytes.is_empty() || bytes.len() > Self::MAX_ROLLOUT || bytes.last() != Some(&b'\n') {
            return Err("rollout_size_or_final_newline");
        }
        let text = std::str::from_utf8(bytes).map_err(|_| "rollout_utf8")?;
        let (Some((cwd, model, _, native_prompt)), Some((thread, _, _)), Some(turn)) =
            (&self.expected, &self.thread, &self.turn)
        else {
            return Err("rollout_missing_wire_identity");
        };
        if !self.identity_matches() {
            return Err("rollout_wire_identity_mismatch");
        }
        let mut meta = false;
        let mut history_mode = "legacy";
        let mut record_kinds = BTreeMap::<String, usize>::new();
        let mut context = false;
        let mut prompt_count = 0;
        let mut terminal = false;
        let mut records = 0;
        let mut projection_bytes = 0_usize;
        let mut input_events = Vec::new();
        let mut calls: Vec<Value> = Vec::new();
        let mut call_ids = BTreeMap::new();
        let mut response_kinds = BTreeMap::<String, usize>::new();
        for line in text.lines() {
            records += 1;
            if records > Self::MAX_RECORDS || line.len() > Self::MAX_LINE {
                return Err("rollout_record_bound");
            }
            let record: Value = serde_json::from_str(line).map_err(|_| "rollout_json")?;
            let payload = record
                .get("payload")
                .filter(|value| value.is_object())
                .ok_or("rollout_payload_shape")?;
            let record_kind = record["type"].as_str().ok_or("rollout_type_shape")?;
            if records == 1 {
                history_mode = match payload.get("history_mode") {
                    None => "legacy",
                    Some(Value::String(mode)) if mode == "legacy" => "legacy",
                    Some(Value::String(mode)) if mode == "paginated" => "paginated",
                    _ => return Err("rollout_history_mode"),
                };
            }
            if history_mode == "paginated" {
                if record["ordinal"].as_u64() != Some((records - 1) as u64) {
                    return Err("rollout_ordinal_discontinuity");
                }
            } else if record.get("ordinal").is_some() {
                return Err("rollout_legacy_ordinal");
            }
            match record_kind {
                "session_meta" => {
                    if meta
                        || records != 1
                        || payload["id"].as_str() != Some(thread.as_str())
                        || payload["session_id"].as_str() != Some(thread.as_str())
                        || payload["cwd"].as_str() != cwd.to_str()
                        || payload["cli_version"] != "0.163.0"
                        || !payload["parent_thread_id"].is_null()
                        || !payload["forked_from_id"].is_null()
                        || !payload["history_base"].is_null()
                        || !payload["forked_from_ordinal_exclusive"].is_null()
                        || !payload["subagent_history_start_ordinal"].is_null()
                    {
                        return Err("rollout_session_identity");
                    }
                    meta = true;
                }
                "world_state" => {
                    if !meta
                        || terminal
                        || !payload["full"].is_boolean()
                        || !payload["state"].is_object()
                    {
                        return Err("rollout_world_state_shape");
                    }
                    // Model-visible context can precede TurnContext. The opaque state is never retained.
                }
                "token_usage_record" => {
                    if !context
                        || payload["thread_id"].as_str() != Some(thread.as_str())
                        || payload["session_id"].as_str() != Some(thread.as_str())
                        || payload["turn_id"].as_str() != Some(turn.as_str())
                        || payload["root_turn_id"].as_str() != Some(turn.as_str())
                        || payload["response_id"]
                            .as_str()
                            .is_none_or(|id| id.is_empty())
                        || ["usage", "turn_token_usage", "thread_token_usage"]
                            .iter()
                            .any(|key| !payload[*key].is_object())
                    {
                        return Err("rollout_token_usage_identity_or_shape");
                    }
                    // Usage values and response identifiers are outside this callback diagnostic.
                }
                "turn_context" => {
                    if !meta
                        || context
                        || terminal
                        || payload["turn_id"].as_str() != Some(turn.as_str())
                        || payload["cwd"].as_str() != cwd.to_str()
                        || payload["model"].as_str() != Some(model.as_str())
                        || payload["collaboration_mode"]["mode"] != "default"
                    {
                        return Err("rollout_turn_context_identity");
                    }
                    context = true;
                }
                "response_item" => {
                    let kind = payload["type"].as_str().ok_or("rollout_item_type")?;
                    let known_kind = match kind {
                        "message" | "reasoning" | "function_call" | "function_call_output" => kind,
                        "custom_tool_call" | "custom_tool_call_output" | "agent_message" => {
                            return Err("rollout_code_mode_or_routed_path");
                        }
                        _ => return Err("rollout_unsupported_response_item"),
                    };
                    *response_kinds.entry(known_kind.to_owned()).or_default() += 1;
                    if terminal {
                        return Err("rollout_item_after_terminal");
                    }
                    match kind {
                        "message" if payload["role"] == "user" => {
                            let content = payload["content"]
                                .as_array()
                                .ok_or("rollout_user_content")?;
                            if content.len() == 1
                                && content[0]["type"] == "input_text"
                                && content[0]["text"].as_str() == Some(native_prompt.as_str())
                            {
                                prompt_count += 1;
                            }
                        }
                        "function_call" => {
                            if !context {
                                return Err("rollout_call_before_context");
                            }
                            let call_id = payload["call_id"]
                                .as_str()
                                .filter(|id| !id.is_empty())
                                .ok_or("rollout_call_identity")?;
                            let name = payload["name"].as_str().ok_or("rollout_call_name")?;
                            if call_ids.insert(call_id.to_owned(), calls.len()).is_some() {
                                return Err("rollout_duplicate_call");
                            }
                            let arguments = payload["arguments"]
                                .as_str()
                                .ok_or("rollout_arguments_type")?;
                            let selected = name == "request_user_input"
                                && payload["namespace"]
                                    .as_str()
                                    .is_none_or(|namespace| namespace == "functions");
                            calls.push(json!({"sequence":records,"call_id":Self::facts(call_id), "name":if selected {json!("request_user_input")} else {json!("other")},
                                "name_facts":Self::facts(name),"namespace":payload["namespace"].as_str().map(Self::facts),
                                "arguments":Self::facts(arguments),"argument_shape":Self::argument_shape(arguments),"output":null}));
                            projection_bytes = projection_bytes.saturating_add(
                                calls.last().map_or(0, |call| call.to_string().len()),
                            );
                        }
                        "function_call_output" => {
                            let call_id = payload["call_id"]
                                .as_str()
                                .ok_or("rollout_output_identity")?;
                            let index = *call_ids.get(call_id).ok_or("rollout_unmatched_output")?;
                            if !calls[index]["output"].is_null() {
                                return Err("rollout_duplicate_output");
                            }
                            calls[index]["output"] = Self::output_facts(&payload["output"])?;
                            calls[index]["output_sequence"] = json!(records);
                            projection_bytes = projection_bytes
                                .saturating_add(calls[index]["output"].to_string().len());
                        }
                        _ => {}
                    }
                }
                "event_msg" => match payload["type"].as_str().ok_or("rollout_event_type")? {
                    "task_started" => {
                        if !meta || terminal || payload["turn_id"].as_str() != Some(turn.as_str()) {
                            return Err("rollout_started_identity");
                        }
                    }
                    "request_user_input" => {
                        let id = payload["call_id"]
                            .as_str()
                            .ok_or("rollout_input_event_identity")?;
                        let index = *call_ids.get(id).ok_or("rollout_input_event_without_call")?;
                        if payload["turn_id"].as_str() != Some(turn.as_str())
                            || calls[index]["name"] != "request_user_input"
                        {
                            return Err("rollout_input_event_mismatch");
                        }
                        input_events.push(json!({"sequence":records,"call_id":Self::facts(id),
                                "is_blocking":payload["isBlocking"].as_bool(), "question_count":payload["questions"].as_array().map(Vec::len)}));
                        projection_bytes = projection_bytes.saturating_add(256);
                    }
                    "task_complete" => {
                        if !context
                            || terminal
                            || payload["turn_id"].as_str() != Some(turn.as_str())
                        {
                            return Err("rollout_terminal_identity");
                        }
                        terminal = true;
                    }
                    "turn_aborted" | "thread_rolled_back" => {
                        return Err("rollout_noncomplete_terminal_or_rollback");
                    }
                    "warning" => return Err("rollout_warning_observed"),
                    _ => {}
                },
                "compacted" | "context_reference" => return Err("rollout_partial_or_compacted"),
                _ => return Err("rollout_unsupported_record"),
            }
            *record_kinds.entry(record_kind.to_owned()).or_default() += 1;
            if projection_bytes > Self::MAX_LINE {
                return Err("rollout_projection_bound");
            }
        }
        if !meta || !context || !terminal || prompt_count != 1 {
            return Err("rollout_missing_or_ambiguous_identity");
        }
        if calls.iter().any(|call| call["output"].is_null()) {
            return Err("rollout_missing_output");
        }
        let callback_correlated =
            self.callback_identity
                .as_ref()
                .map(|(callback_thread, callback_turn, call_id)| {
                    callback_thread == thread
                        && callback_turn == turn
                        && call_ids
                            .get(call_id)
                            .is_some_and(|index| calls[*index]["name"] == "request_user_input")
                });
        if self.request.is_some() && callback_correlated != Some(true) {
            return Err("rollout_callback_identity_mismatch");
        }
        Ok(
            json!({"complete":true,"bytes":bytes.len(),"records":records,"thread":Self::facts(thread),"turn":Self::facts(turn),
            "model":Self::facts(model),"prompt":Self::facts(native_prompt),"prompt_count":prompt_count,"response_kinds":response_kinds,"calls":calls,
            "history_mode":history_mode,"ordinals_complete":history_mode == "paginated","record_kinds":record_kinds,
            "request_user_input_events":input_events,"callback_correlated":callback_correlated,
            "scope":"own_correlated_native_rollout_not_model_wire"}),
        )
    }

    fn argument_shape(arguments: &str) -> Value {
        let Ok(value) = serde_json::from_str::<Value>(arguments) else {
            return json!({"json_valid":false});
        };
        let questions = value["questions"].as_array();
        json!({"json_valid":true,"object":value.is_object(),"questions_count":questions.map(Vec::len),
            "typed_fields_valid":questions.is_some_and(|questions| questions.iter().all(|question| {
                ["id","header","question"].iter().all(|key| question[*key].is_string())
                    && ["isOther","isSecret"].iter().all(|key| question.get(*key).is_none_or(Value::is_boolean))
                    && (question["options"].is_null() || question["options"].as_array().is_some_and(|options| options.iter().all(|option| option["label"].is_string() && option["description"].is_string())))
            })),
            "all_options_nonempty":questions.is_some_and(|questions| questions.iter().all(|question| question["options"].as_array().is_some_and(|options| !options.is_empty()))),
            "detail_limit":64,
            "questions":questions.map(|questions| questions.iter().take(64).map(|question| json!({
                "id":question["id"].as_str().map(Self::facts),"header_bytes":question["header"].as_str().map(str::len),
                "question_bytes":question["question"].as_str().map(str::len),"options_count":question["options"].as_array().map(Vec::len),
                "option_shapes":question["options"].as_array().map(|options| options.iter().take(64).map(|option| json!({
                    "label_bytes":option["label"].as_str().map(str::len),"description_bytes":option["description"].as_str().map(str::len)})).collect::<Vec<_>>())})).collect::<Vec<_>>())})
    }

    fn output_facts(output: &Value) -> Result<Value, &'static str> {
        if let Some(text) = output.as_str() {
            return Ok(
                json!({"kind":"string","text":Self::facts(text),"error_class":Self::error_class(text)}),
            );
        }
        let items = output
            .as_array()
            .filter(|items| items.len() <= Self::MAX_RECORDS)
            .ok_or("rollout_output_shape")?;
        let mut facts = Vec::new();
        for item in items {
            match item["type"].as_str() {
                Some("input_text") => {
                    let text = item["text"].as_str().ok_or("rollout_output_text_shape")?;
                    facts.push(json!({"kind":"input_text","text":Self::facts(text),"error_class":Self::error_class(text)}));
                }
                Some(kind @ ("input_image" | "input_audio" | "encrypted_content")) => {
                    facts.push(json!({"kind":kind}))
                }
                _ => return Err("rollout_output_item_shape"),
            }
        }
        Ok(json!({"kind":"content_items","items":facts,"error_class":"unknown"}))
    }

    fn error_class(text: &str) -> &'static str {
        match text {
            "request_user_input requires non-empty options for every question" => "missing_options",
            "request_user_input is unavailable in Default mode" => "unavailable_default_mode",
            "request_user_input can only be used by the root thread" => "non_root_thread",
            "request_user_input handler received unsupported payload" => "unsupported_payload",
            "request_user_input was cancelled before receiving a response" => {
                "cancelled_before_response"
            }
            _ if text.starts_with("failed to parse function arguments: ") => "argument_parse_error",
            _ => "unknown",
        }
    }

    pub(crate) fn assert_nonblocking_response(&self) {
        assert!(
            self.request.is_some(),
            "real native request was not observed"
        );
        assert_eq!(
            self.responses, 1,
            "exactly one correlated response is required"
        );
        assert!(
            self.unsupported,
            "native callback must receive Unsupported without a fabricated answer"
        );
    }

    pub(crate) fn snapshot(&self) -> Value {
        let wire_complete = self.failures.is_empty()
            && self.pending_sdk.is_none()
            && self.pending_native.is_none()
            && self.requests.is_empty()
            && self.initialized
            && self.features_complete
            && [
                "default_mode_request_user_input",
                "multi_agent",
                "multi_agent_v2",
            ]
            .iter()
            .all(|name| self.effective_features.contains_key(*name))
            && self.identity_matches();
        let rollout = self
            .rollout
            .clone()
            .unwrap_or_else(|| json!({"complete":false}));
        let prompt_binding = self.expected.as_ref().map(|(_, _, caller, native)| {
            json!({
            "rendering":"empty_managed_context", "caller":Self::facts(caller),
            "native":Self::facts(native), "exact":self.submitted_exact})
        });
        json!({"version":1,"case":Self::CASE,"fingerprint_algorithm":"std_default_hasher64_noncryptographic",
            "prompt_binding":prompt_binding,
            "capture_complete":wire_complete && self.stderr_observed && rollout["complete"] == true,"wire_complete":wire_complete,
            "identity_matches":self.identity_matches(),"frames":self.frames,"effective_features":self.effective_features,
            "stderr_observed":self.stderr_observed,"features_complete":self.features_complete,"rollout":rollout,"failures":self.failures,
            "callback_observed":self.request.is_some(),"callback_responses":self.responses,"callback_unsupported":self.unsupported})
    }

    pub(crate) fn export_after_cleanup(
        &self,
        path: &Path,
        scenario_succeeded: bool,
        cleanup_succeeded: bool,
    ) -> std::io::Result<()> {
        if !path.is_absolute() {
            return Err(std::io::Error::other(
                "Codex evidence path must be absolute",
            ));
        }
        let mut snapshot = self.snapshot();
        snapshot["scenario_succeeded"] = json!(scenario_succeeded);
        snapshot["cleanup_succeeded"] = json!(cleanup_succeeded);
        snapshot["hierarchy_cleanup_succeeded"] = self
            .cleanup
            .map(|cleanup| json!(cleanup.0))
            .unwrap_or(Value::Null);
        snapshot["process_cleanup_succeeded"] = self
            .cleanup
            .map(|cleanup| json!(cleanup.1))
            .unwrap_or(Value::Null);
        if self
            .cleanup
            .is_some_and(|(hierarchy, process)| cleanup_succeeded != (hierarchy && process))
        {
            return Err(std::io::Error::other(
                "Codex evidence cleanup result inconsistent",
            ));
        }
        let body = serde_json::to_vec(&snapshot)?;
        if body.len() > Self::MAX_LINE {
            return Err(std::io::Error::other("Codex evidence output bound"));
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(&body)?;
        file.flush()
    }
}
