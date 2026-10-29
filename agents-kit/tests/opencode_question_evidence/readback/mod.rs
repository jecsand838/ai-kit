use std::collections::BTreeSet;
use std::time::Duration;

use serde::Deserialize;
use tokio::time::Instant;

use super::{QuestionEvidence, QuestionExpectation, QuestionGrade};

pub struct FreshOutputExpectation {
    prompt: String,
    expected: String,
}

impl FreshOutputExpectation {
    pub fn new(expected: String) -> Self {
        assert!(
            !expected.is_empty() && expected.len() <= 256,
            "fresh output expectation must contain at most 256 bytes"
        );
        let prompt =
            format!("Do not call any tool. Reply with exactly {expected} and nothing else.");
        assert!(
            prompt.len() <= 4 * 1024,
            "fresh output prompt must contain at most 4 KiB"
        );
        Self { prompt, expected }
    }

    pub fn prompt(&self) -> &str {
        &self.prompt
    }

    pub fn expected(&self) -> &str {
        &self.expected
    }
}

pub struct NativeQuestionReadback {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    deadline: Instant,
    version: String,
}

#[derive(Deserialize)]
struct NativeHealth {
    healthy: bool,
    version: String,
}

#[derive(Deserialize)]
struct NativeSession {
    id: String,
}

impl NativeQuestionReadback {
    const BODY_LIMIT: usize = 16 << 20;

    pub async fn new(endpoint: &str, deadline: Instant) -> Self {
        let client = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .expect("native evidence client must build");
        let mut reader = Self {
            client,
            endpoint: reqwest::Url::parse(endpoint).expect("owned native endpoint must parse"),
            deadline,
            version: String::new(),
        };
        let health: NativeHealth =
            serde_json::from_slice(&reader.read(&["global", "health"]).await)
                .unwrap_or_else(|_| panic!("native health evidence must decode"));
        assert!(
            health.healthy && !health.version.trim().is_empty(),
            "native health must be authoritative"
        );
        reader.version = health.version;
        reader
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub async fn prompt_output(
        &self,
        prompt: &str,
        configured_provider: &str,
        configured_model: &str,
        expected: &str,
        portable: &str,
    ) -> Result<serde_json::Value, &'static str> {
        if self.version.len() > 128
            || !self
                .version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".+_-".contains(&byte))
        {
            return Err("version_shape");
        }
        let sessions: Vec<NativeSession> =
            serde_json::from_slice(&self.read_result(&["experimental", "session"]).await?)
                .map_err(|_| "sessions_shape")?;
        let [session] = sessions.as_slice() else {
            return Err("session_correlation");
        };
        if !QuestionEvidence::identity(&session.id, "ses") {
            return Err("session_identity");
        }
        let messages = self
            .read_result(&["session", &session.id, "message"])
            .await?;
        let messages: serde_json::Value =
            serde_json::from_slice(&messages).map_err(|_| "messages_shape")?;
        let (mut context, _) = QuestionExpectation::new(&session.id, prompt, configured_model)
            .prompt_output(&messages, configured_provider, expected, portable)?;
        context["native_version"] = serde_json::json!(self.version);
        Ok(context)
    }

    pub async fn sessions(&self) -> BTreeSet<String> {
        let sessions: Vec<NativeSession> =
            serde_json::from_slice(&self.read(&["experimental", "session"]).await)
                .expect("complete native session inventory must decode");
        let mut ids = BTreeSet::new();
        for session in sessions {
            assert!(
                session.id.starts_with("ses") && ids.insert(session.id),
                "native inventory identities must be unique"
            );
        }
        ids
    }

    pub async fn created_session(&self, before: &BTreeSet<String>) -> String {
        let after = self.sessions().await;
        assert!(
            before.is_subset(&after),
            "native inventory lost an existing session during public creation"
        );
        let mut created = after.difference(before);
        let session = created
            .next()
            .expect("public creation must create a native session")
            .clone();
        assert!(
            created.next().is_none(),
            "public creation must have one unambiguous native session"
        );
        session
    }

    pub async fn pending(
        &self,
        session: &str,
        prompt: &str,
        configured_model: &str,
    ) -> QuestionEvidence {
        let pending = self.read(&["question"]).await;
        let origin = QuestionEvidence::pending_origin(&pending);
        if origin.is_err() {
            let mut context = Self::questions_context(&pending, session);
            context["questions_snapshot"] = serde_json::json!("original_pending_read");
            eprintln!("question native failure original_pending={context}");
        }
        let (native_session, assistant_id) =
            origin.expect("native question must have one exact tool origin");
        assert_eq!(
            native_session, session,
            "pending question must belong to the newly created session"
        );
        let assistant = self
            .read(&["session", session, "message", &assistant_id])
            .await;
        let parent = QuestionEvidence::parent_user(&assistant)
            .expect("native tool origin must identify its root User");
        let user = self.read(&["session", session, "message", &parent]).await;
        let providers = self.read(&["provider"]).await;
        QuestionEvidence::begin(
            &pending,
            &assistant,
            &user,
            &providers,
            QuestionExpectation::new(session, prompt, configured_model),
        )
        .expect("native pending evidence must match the original public prompt")
    }

    pub async fn completed(
        &self,
        evidence: QuestionEvidence,
        session: &str,
        answer: &str,
        portable: &str,
    ) -> QuestionGrade {
        let pending = self.read(&["question"]).await;
        QuestionEvidence::require_no_pending(&pending)
            .expect("resolved native question must no longer be pending");
        // Omit limit: selected server returns the complete session message list.
        let messages = self.read(&["session", session, "message"]).await;
        evidence
            .assess(&messages, answer, portable)
            .expect("native answer, call ownership, completion and portable fidelity must agree")
    }

    pub async fn capture_failure(
        &self,
        session: &str,
        prompt: &str,
        primary: &mut Option<Box<dyn std::any::Any + Send>>,
        failure: Box<dyn std::any::Any + Send>,
    ) -> String {
        // The caller retains this slot outside its original absolute timeout.
        // Dropping diagnostic reads must not drop the already-observed failure.
        if primary.is_none() {
            *primary = Some(failure);
        }
        let messages = self.read_result(&["session", session, "message"]).await;
        let mut context = match messages {
            Ok(messages) => Self::failure_context(&messages, session, prompt).unwrap_or_else(
                |reason| serde_json::json!({"capture":"incomplete","reason":reason}),
            ),
            Err(reason) => serde_json::json!({"capture":"incomplete","messages":reason}),
        };
        context["questions_snapshot"] = serde_json::json!("after_failure");
        // Retain the first read even if the original deadline interrupts the second.
        context["questions_read"] = serde_json::json!("not_read");
        let first = context.to_string();
        if first.len() <= 4 * 1024 {
            eprintln!("question native failure messages={first}");
        } else {
            eprintln!(
                "question native failure messages={{\"capture\":\"incomplete\",\"reason\":\"diagnostic_bound\"}}"
            );
        }
        let questions = self.read_result(&["question"]).await;
        match questions {
            Ok(body) => {
                let questions = Self::questions_context(&body, session);
                if questions["questions_read"] != "complete" {
                    context["capture"] = serde_json::json!("incomplete");
                }
                if let Some(questions) = questions.as_object() {
                    for (key, value) in questions {
                        context[key] = value.clone();
                    }
                }
            }
            Err(reason) => {
                context["capture"] = serde_json::json!("incomplete");
                context["questions_read"] = serde_json::json!(reason);
            }
        }
        let context = context.to_string();
        if context.len() > 4 * 1024 {
            return "{\"capture\":\"incomplete\",\"reason\":\"diagnostic_bound\"}".to_owned();
        }
        context
    }

    fn questions_context(body: &[u8], session: &str) -> serde_json::Value {
        let mut context = serde_json::json!({"questions_read":"questions_shape"});
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
            return context;
        };
        let Some(questions) = value.as_array() else {
            return context;
        };
        context["pending_questions"] = serde_json::json!(questions.len());
        context["pending_same_session"] = serde_json::json!(
            questions
                .iter()
                .filter(|question| {
                    question
                        .get("sessionID")
                        .and_then(serde_json::Value::as_str)
                        == Some(session)
                })
                .count()
        );
        let mut multiple = [0_usize; 5];
        for question in questions {
            if let Some(infos) = question
                .get("questions")
                .and_then(serde_json::Value::as_array)
            {
                for info in infos {
                    multiple[Self::multiple_kind(info)] += 1;
                }
            }
        }
        context["pending_multiple"] = serde_json::json!({
            "missing":multiple[0],"null":multiple[1],"invalid":multiple[2],
            "false":multiple[3],"true":multiple[4],
        });
        // Counts survive a failed decode, but they never make malformed evidence valid.
        if serde_json::from_value::<Vec<super::NativeQuestion>>(value).is_ok() {
            context["questions_read"] = serde_json::json!("complete");
        }
        context
    }

    fn multiple_kind(question: &serde_json::Value) -> usize {
        match question.get("multiple") {
            None if question.is_object() => 0,
            Some(serde_json::Value::Null) => 1,
            Some(serde_json::Value::Bool(false)) => 3,
            Some(serde_json::Value::Bool(true)) => 4,
            _ => 2,
        }
    }

    fn failure_context(
        messages: &[u8],
        session: &str,
        prompt: &str,
    ) -> Result<serde_json::Value, &'static str> {
        let messages: serde_json::Value =
            serde_json::from_slice(messages).map_err(|_| "messages_json")?;
        let messages = messages.as_array().ok_or("messages_shape")?;
        let mut users = Vec::new();
        let mut ids = BTreeSet::new();
        for message in messages {
            let id = message
                .pointer("/info/id")
                .and_then(serde_json::Value::as_str)
                .ok_or("message_identity")?;
            if !ids.insert(id)
                || message
                    .pointer("/info/sessionID")
                    .and_then(serde_json::Value::as_str)
                    != Some(session)
            {
                return Err("message_ownership");
            }
            let parts = message
                .get("parts")
                .and_then(serde_json::Value::as_array)
                .ok_or("parts_shape")?;
            for part in parts {
                if part.get("sessionID").and_then(serde_json::Value::as_str) != Some(session)
                    || part.get("messageID").and_then(serde_json::Value::as_str) != Some(id)
                {
                    return Err("part_ownership");
                }
                match part.get("type").and_then(serde_json::Value::as_str) {
                    Some("text") => {
                        if !part.get("text").is_some_and(serde_json::Value::is_string)
                            || part
                                .get("synthetic")
                                .is_some_and(|value| !value.is_boolean())
                        {
                            return Err("text_shape");
                        }
                    }
                    Some("tool") => {
                        if !part.get("tool").is_some_and(serde_json::Value::is_string)
                            || !part.get("callID").is_some_and(serde_json::Value::is_string)
                            || !part
                                .pointer("/state/status")
                                .is_some_and(serde_json::Value::is_string)
                        {
                            return Err("tool_shape");
                        }
                    }
                    Some(_) => {}
                    None => return Err("part_shape"),
                }
            }
            match message
                .pointer("/info/role")
                .and_then(serde_json::Value::as_str)
            {
                Some("user")
                    if parts.iter().any(|part| {
                        part.get("type").and_then(serde_json::Value::as_str) == Some("text")
                            && part.get("synthetic").and_then(serde_json::Value::as_bool)
                                != Some(true)
                            && part.get("text").and_then(serde_json::Value::as_str) == Some(prompt)
                    }) =>
                {
                    users.push(id)
                }
                Some("user") => {}
                Some("assistant")
                    if message
                        .pointer("/info/parentID")
                        .is_some_and(serde_json::Value::is_string) => {}
                _ => return Err("message_shape"),
            }
        }
        if users.len() != 1 {
            return Ok(
                serde_json::json!({"capture":"incomplete","reason":"root_correlation","matching_users":users.len()}),
            );
        }
        use std::hash::{Hash, Hasher};

        let mut request_parts = Vec::new();
        let mut request_part_count = 0_usize;
        for message in messages {
            if message
                .pointer("/info/id")
                .and_then(serde_json::Value::as_str)
                != Some(users[0])
            {
                continue;
            }
            for part in message["parts"].as_array().ok_or("parts_shape")? {
                if part.get("type").and_then(serde_json::Value::as_str) != Some("text")
                    || part.get("synthetic").and_then(serde_json::Value::as_bool) == Some(true)
                    || part.get("text").and_then(serde_json::Value::as_str) != Some(prompt)
                {
                    continue;
                }
                request_part_count += 1;
                if request_parts.len() < 4 {
                    let id = part.get("id").and_then(serde_json::Value::as_str);
                    let mut fingerprint = std::collections::hash_map::DefaultHasher::new();
                    id.hash(&mut fingerprint);
                    request_parts.push(serde_json::json!({
                        "present":id.is_some(),
                        "fingerprint":format!("{:016x}",fingerprint.finish()),
                    }));
                }
            }
        }
        let mut assistants: Vec<_> = messages
            .iter()
            .filter(|message| {
                message
                    .pointer("/info/role")
                    .and_then(serde_json::Value::as_str)
                    == Some("assistant")
                    && message
                        .pointer("/info/parentID")
                        .and_then(serde_json::Value::as_str)
                        == Some(users[0])
            })
            .collect();
        assistants.sort_by_key(|message| {
            message
                .pointer("/info/id")
                .and_then(serde_json::Value::as_str)
        });
        let mut summaries = Vec::new();
        let mut question_calls = 0_usize;
        for (position, message) in assistants.iter().enumerate() {
            let parts = message["parts"].as_array().ok_or("parts_shape")?;
            let mut text_parts = 0_usize;
            let mut text_bytes = 0_usize;
            let mut tools = 0_usize;
            let mut question_states = [0_usize; 5];
            let mut question_multiple = [0_usize; 5];
            let mut question_input_shape_valid = true;
            for part in parts {
                match part.get("type").and_then(serde_json::Value::as_str) {
                    Some("text")
                        if part.get("synthetic").and_then(serde_json::Value::as_bool)
                            != Some(true) =>
                    {
                        text_parts += 1;
                        text_bytes += part["text"].as_str().ok_or("text_shape")?.len();
                    }
                    Some("tool") => {
                        tools += 1;
                        if part["tool"].as_str() == Some("question") {
                            question_calls += 1;
                            if let Some(questions) = part
                                .pointer("/state/input/questions")
                                .and_then(serde_json::Value::as_array)
                            {
                                for question in questions {
                                    question_multiple[Self::multiple_kind(question)] += 1;
                                }
                            } else {
                                question_input_shape_valid = false;
                            }
                            let index = match part
                                .pointer("/state/status")
                                .and_then(serde_json::Value::as_str)
                            {
                                Some("pending") => 0,
                                Some("running") => 1,
                                Some("completed") => 2,
                                Some("error") => 3,
                                _ => 4,
                            };
                            question_states[index] += 1;
                        }
                    }
                    _ => {}
                }
            }
            // These are selected literal tags, never arbitrary native values.
            let finish = match message
                .pointer("/info/finish")
                .and_then(serde_json::Value::as_str)
            {
                Some("stop") => "stop",
                Some("tool-calls") => "tool-calls",
                Some("length") => "length",
                Some("content-filter") => "content-filter",
                Some("error") => "error",
                Some(_) => "unknown",
                None if message["info"].get("finish").is_none() => "absent",
                None => "invalid",
            };
            let error = match message
                .pointer("/info/error/name")
                .and_then(serde_json::Value::as_str)
            {
                Some("MessageAbortedError") => "aborted",
                Some("APIError") => "api",
                Some("ProviderAuthError") => "authentication",
                Some("ContextOverflowError") => "context_limit",
                Some("MessageOutputLengthError") => "output_limit",
                Some("ContentFilterError") => "filtered",
                Some("StructuredOutputError") => "structured_output",
                Some("UnknownError") => "unknown_error",
                _ if message["info"].get("error").is_none() => "absent",
                _ => "unknown",
            };
            let completion = match message
                .pointer("/info/time")
                .and_then(serde_json::Value::as_object)
            {
                Some(time) => match time.get("completed") {
                    Some(value) if value.as_u64().is_some() => "present",
                    None => "absent",
                    Some(_) => "invalid",
                },
                None => "invalid",
            };
            if position >= assistants.len().saturating_sub(4) {
                summaries.push(serde_json::json!({
                "completion":completion,"finish":finish,"error":error,"text_parts":text_parts,"text_bytes":text_bytes,
                "tools":tools,"question_states":{"pending":question_states[0],"running":question_states[1],
                    "completed":question_states[2],"error":question_states[3],"unknown":question_states[4]},
                "question_input_shape_valid":question_input_shape_valid,
                "question_multiple":{"missing":question_multiple[0],"null":question_multiple[1],
                    "invalid":question_multiple[2],"false":question_multiple[3],"true":question_multiple[4]},
            }));
            }
        }
        let omitted = assistants.len().saturating_sub(summaries.len());
        // MessageID order matches the native latest-message ordering; omit identities.
        Ok(serde_json::json!({
            "capture":"complete","kind":"native_state_snapshot","matching_users":1,
            "matching_root_request_parts":request_part_count,"request_parts":request_parts,
            "assistant_count":assistants.len(),"assistant_summaries_omitted":omitted,"assistants":summaries,
            "question_tool_calls":question_calls,
        }))
    }

    async fn read(&self, segments: &[&str]) -> Vec<u8> {
        self.read_result(segments)
            .await
            .expect("native evidence must be complete within its original deadline")
    }

    async fn read_result(&self, segments: &[&str]) -> Result<Vec<u8>, &'static str> {
        if Instant::now() >= self.deadline {
            return Err("deadline");
        }
        tokio::time::timeout_at(self.deadline, async {
            let mut url = self.endpoint.clone();
            url.path_segments_mut()
                .map_err(|_| "routing")?
                .clear()
                .extend(segments);
            let mut request = self
                .client
                .get(url)
                .query(&[("directory", env!("CARGO_MANIFEST_DIR"))]);
            if segments == ["experimental", "session"] {
                request = request.query(&[("archived", "true"), ("limit", "32")]);
            }
            let mut response = request.send().await.map_err(|_| "transport")?;
            if response.status() != reqwest::StatusCode::OK {
                return Err("http_status");
            }
            if response.headers().contains_key("x-next-cursor")
                || response.headers().contains_key("link")
            {
                return Err("paginated");
            }
            if response
                .content_length()
                .is_some_and(|size| size > Self::BODY_LIMIT as u64)
            {
                return Err("body_limit");
            }
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| "incomplete_body")? {
                if chunk.len() > Self::BODY_LIMIT - body.len() {
                    return Err("body_limit");
                }
                body.extend_from_slice(&chunk);
            }
            Ok(body)
        })
        .await
        .map_err(|_| "deadline")?
    }
}
