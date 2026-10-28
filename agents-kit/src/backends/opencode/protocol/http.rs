use crate::error::{DeadlineKind, ErrorDetail, ErrorKind, ErrorPhase};
use std::time::Duration;

use futures_util::StreamExt;
use serde::Serialize;
use tokio::time::{Instant, sleep, timeout_at};

use crate::backends::opencode::state_store::records::{OpenCodeCleanup, OpenCodeSessionBinding};
use crate::{AgentId, KitError, OpenCodeConfig, WorkspaceId};

use super::requests::{
    OpenCodeMessageRequest, OpenCodePermissionReply, OpenCodeQuestionReply, OpenCodeRouting,
    OpenCodeSessionCreateRequest, OpenCodeSessionInventory, OpenCodeSessionInventoryEntry,
    OpenCodeSessionState, OpenCodeSessionStatuses,
};
use super::responses::{
    OpenCodeCapabilities, OpenCodeEffectiveConfig, OpenCodeHealth, OpenCodeMcpStatuses,
    OpenCodeMessageResponse, OpenCodeNativeModel, OpenCodePath, OpenCodeProviderInventory,
    OpenCodeSession, OpenCodeToolIds, OpenCodeToolInventory,
};
use super::schemas::{OpenCodePermissionRequest, OpenCodeQuestionRequest};
use super::sync::{OpenCodeHistoryRequest, OpenCodeHistoryRow};

const MAX_RESPONSE_BYTES: usize = 16 << 20;
const MAX_PROVIDERS: usize = 1_024;
const MAX_MODELS: usize = 65_536;
const MAX_TOOLS: usize = 65_536;
const MAX_MCP_SERVERS: usize = 4_096;
const MAX_SESSION_INVENTORY: usize = 4_096;
const MAX_HISTORY_ROWS: usize = 65_536;

#[derive(Clone)]
pub(crate) struct OpenCodeHttp {
    base_url: reqwest::Url,
    request_timeout: Duration,
    client: reqwest::Client,
}

pub(crate) enum OpenCodeMutationError {
    Rejected(KitError),
    Ambiguous(KitError),
}

impl OpenCodeMutationError {
    pub(crate) fn is_ambiguous(&self) -> bool {
        matches!(self, Self::Ambiguous(_))
    }

    pub(crate) fn into_sdk_error(self) -> KitError {
        match self {
            Self::Rejected(error) | Self::Ambiguous(error) => error,
        }
    }
}

impl OpenCodeHttp {
    pub(crate) fn from_config(config: &OpenCodeConfig) -> Result<Self, KitError> {
        let mode = config.mode();
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
        if let Some(timeout) = mode.connect_timeout() {
            builder = builder.connect_timeout(timeout);
        }
        let client = builder.build().map_err(|error| {
            KitError::invalid_config("HTTP client could not be built")
                .at_phase(ErrorPhase::ClientSetup)
                .with_cause(KitError::from(error))
        })?;
        Ok(Self {
            base_url: mode.base_url(),
            request_timeout: mode.request_timeout(),
            client,
        })
    }

    pub(crate) async fn directory(
        &self,
        requested: &std::path::Path,
        deadline: Instant,
    ) -> Result<std::path::PathBuf, KitError> {
        timeout_at(deadline, async {
            let routing = OpenCodeRouting::for_directory(requested.to_path_buf())?;
            let response: OpenCodePath = self.get_json("path", Some(&routing)).await?;
            response.into_directory()
        })
        .await
        .map_err(|_| {
            KitError::timeout("workspace routing deadline elapsed")
                .at_phase(ErrorPhase::InventoryVerification)
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Lifecycle,
                    timeout: None,
                })
        })?
    }

    pub(crate) async fn verify_directory(
        &self,
        requested: &std::path::Path,
        expected: &std::path::Path,
        deadline: Instant,
    ) -> Result<(), KitError> {
        if self.directory(requested, deadline).await? != expected {
            return Err(KitError::invalid_state(
                "native workspace routing contradicts its durable binding",
            )
            .at_phase(ErrorPhase::InventoryVerification));
        }
        Ok(())
    }

    pub(crate) async fn verify_server(&self, deadline: Instant) -> Result<(), KitError> {
        timeout_at(deadline, async {
            self.health().await?.validate()?;
            self.capabilities().await?.validate()
        })
        .await
        .map_err(|_| {
            KitError::timeout("server verification deadline elapsed")
                .at_phase(ErrorPhase::ServerVerification)
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Lifecycle,
                    timeout: None,
                })
        })?
    }

    pub(crate) async fn wait_until_healthy(&self, deadline: Instant) -> Result<(), KitError> {
        let mut last_error = None;
        loop {
            let probe_deadline = deadline.min(Instant::now() + Duration::from_secs(1));
            match timeout_at(probe_deadline, self.health()).await {
                Err(_) if Instant::now() < deadline => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    sleep(remaining.min(Duration::from_millis(100))).await;
                }
                Err(_) => {
                    let error = KitError::timeout("startup health deadline elapsed")
                        .at_phase(ErrorPhase::ServerVerification);
                    return Err(match last_error {
                        Some(attempt) => error.with_supplementary(attempt),
                        None => error,
                    });
                }
                Ok(Ok(health)) => return health.validate(),
                Ok(Err(error))
                    if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout)
                        && Instant::now() < deadline =>
                {
                    last_error = Some(error);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    sleep(remaining.min(Duration::from_millis(100))).await;
                }
                Ok(Err(error))
                    if matches!(error.kind(), ErrorKind::Integration | ErrorKind::Timeout) =>
                {
                    return Err(KitError::timeout("startup health deadline elapsed")
                        .at_phase(ErrorPhase::ServerVerification)
                        .with_supplementary(error));
                }
                Ok(Err(error)) => return Err(error),
            }
        }
    }

    pub(crate) async fn verify_capabilities(&self, deadline: Instant) -> Result<(), KitError> {
        timeout_at(deadline, async { self.capabilities().await?.validate() })
            .await
            .map_err(|_| {
                KitError::timeout("capability verification deadline elapsed")
                    .at_phase(ErrorPhase::ServerVerification)
            })?
    }

    pub(crate) async fn verify_disabled_sharing(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<(), KitError> {
        self.get_json("config", Some(routing))
            .await
            .and_then(OpenCodeEffectiveConfig::require_disabled_sharing)
    }

    pub(crate) async fn create_session(
        &self,
        routing: &OpenCodeRouting,
        request: &OpenCodeSessionCreateRequest,
    ) -> Result<OpenCodeSession, OpenCodeMutationError> {
        self.verify_disabled_sharing(routing)
            .await
            .map_err(OpenCodeMutationError::Rejected)?;
        let url = self
            .url("session", Some(routing))
            .map_err(OpenCodeMutationError::Rejected)?;
        let response = self
            .client
            .post(url)
            .timeout(self.request_timeout)
            .json(request)
            .send()
            .await
            .map_err(Self::mutation_send_error)?;
        self.decode_mutation(response).await
    }

    pub(crate) async fn providers(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<OpenCodeProviderInventory, KitError> {
        let inventory: OpenCodeProviderInventory = self.get_json("provider", Some(routing)).await?;
        inventory.validate(MAX_PROVIDERS, MAX_MODELS)
    }

    pub(crate) async fn tool_ids(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<OpenCodeToolIds, KitError> {
        let inventory: OpenCodeToolIds = self
            .get_json("experimental/tool/ids", Some(routing))
            .await?;
        inventory.validate(MAX_TOOLS)
    }

    pub(crate) async fn tools(
        &self,
        routing: &OpenCodeRouting,
        model: &OpenCodeNativeModel,
    ) -> Result<OpenCodeToolInventory, KitError> {
        let mut url = self.url("experimental/tool", Some(routing))?;
        url.query_pairs_mut()
            .append_pair("provider", model.provider_id())
            .append_pair("model", model.model_id());
        let response = self
            .client
            .get(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::request_error)?;
        self.decode::<OpenCodeToolInventory>(response)
            .await?
            .validate(MAX_TOOLS)
    }

    pub(crate) async fn mcp_statuses(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<OpenCodeMcpStatuses, KitError> {
        let statuses: OpenCodeMcpStatuses = self.get_json("mcp", Some(routing)).await?;
        statuses.validate(MAX_MCP_SERVERS)
    }

    pub(crate) async fn message(
        &self,
        routing: &OpenCodeRouting,
        session_id: &str,
        request: &OpenCodeMessageRequest,
    ) -> Result<OpenCodeMessageResponse, OpenCodeMutationError> {
        let url = self
            .url_with_segments(&["session", session_id, "message"], routing)
            .map_err(OpenCodeMutationError::Rejected)?;
        let response = self
            .client
            .post(url)
            .timeout(self.request_timeout)
            .json(request)
            .send()
            .await
            .map_err(Self::mutation_send_error)?;
        self.decode_mutation(response).await
    }

    pub(crate) async fn abort(
        &self,
        routing: &OpenCodeRouting,
        session_id: &str,
    ) -> Result<(), OpenCodeMutationError> {
        let url = self
            .url_with_segments(&["session", session_id, "abort"], routing)
            .map_err(OpenCodeMutationError::Rejected)?;
        let response = self
            .client
            .post(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::mutation_send_error)?;
        let accepted: bool = self.decode_abort(response).await?;
        if !accepted {
            return Err(OpenCodeMutationError::Rejected(KitError::invalid_state(
                "session abort was not accepted",
            )));
        }
        Ok(())
    }

    pub(crate) async fn message_by_id(
        &self,
        routing: &OpenCodeRouting,
        session_id: &str,
        message_id: &str,
    ) -> Result<OpenCodeMessageResponse, KitError> {
        let url =
            self.url_with_segments(&["session", session_id, "message", message_id], routing)?;
        let response = self
            .client
            .get(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::request_error)?;
        if !response.status().is_success() {
            return Err(match response.status() {
                reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::NOT_FOUND => {
                    KitError::invalid_state("foreground parent is unavailable")
                }
                _ => KitError::integration(
                    "foreground parent lookup returned an unsuccessful response",
                ),
            });
        }
        self.decode(response).await
    }

    pub(crate) async fn global_event(&self) -> Result<reqwest::Response, KitError> {
        let url = self.url("global/event", None)?;
        let response = timeout_at(
            Instant::now() + self.request_timeout,
            self.client.get(url).send(),
        )
        .await
        .map_err(|_| {
            KitError::timeout("event-stream connection deadline elapsed")
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Request,
                    timeout: Some(self.request_timeout),
                })
                .at_phase(ErrorPhase::RequestSend)
        })?
        .map_err(Self::request_error)?;
        if !response.status().is_success() {
            return Err(Self::status_error(response.status()));
        }
        let is_event_stream = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"));
        if !is_event_stream {
            return Err(KitError::protocol(
                "event response has an unexpected content type",
            ));
        }
        Ok(response)
    }

    pub(crate) async fn permissions(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<Vec<OpenCodePermissionRequest>, KitError> {
        self.get_json("permission", Some(routing)).await
    }

    pub(crate) async fn permission_reply(
        &self,
        routing: &OpenCodeRouting,
        request_id: &str,
        reply: &OpenCodePermissionReply,
    ) -> Result<(), OpenCodeMutationError> {
        let url = self
            .url_with_segments(&["permission", request_id, "reply"], routing)
            .map_err(OpenCodeMutationError::Rejected)?;
        let response = self
            .client
            .post(url)
            .timeout(self.request_timeout)
            .json(reply)
            .send()
            .await
            .map_err(Self::mutation_send_error)?;
        self.require_accepted(response, "permission reply").await
    }

    pub(crate) async fn questions(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<Vec<OpenCodeQuestionRequest>, KitError> {
        self.get_json("question", Some(routing)).await
    }

    pub(crate) async fn question_reply(
        &self,
        routing: &OpenCodeRouting,
        request_id: &str,
        reply: &OpenCodeQuestionReply,
    ) -> Result<(), OpenCodeMutationError> {
        let url = self
            .url_with_segments(&["question", request_id, "reply"], routing)
            .map_err(OpenCodeMutationError::Rejected)?;
        let response = self
            .client
            .post(url)
            .timeout(self.request_timeout)
            .json(reply)
            .send()
            .await
            .map_err(Self::mutation_send_error)?;
        self.require_accepted(response, "question reply").await
    }

    pub(crate) async fn question_reject(
        &self,
        routing: &OpenCodeRouting,
        request_id: &str,
    ) -> Result<(), OpenCodeMutationError> {
        let url = self
            .url_with_segments(&["question", request_id, "reject"], routing)
            .map_err(OpenCodeMutationError::Rejected)?;
        let response = self
            .client
            .post(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::mutation_send_error)?;
        self.require_accepted(response, "question rejection").await
    }

    pub(crate) async fn session(
        &self,
        routing: &OpenCodeRouting,
        id: &str,
    ) -> Result<Option<OpenCodeSessionState>, KitError> {
        let url = self.url_with_segments(&["session", id], routing)?;
        let response = self
            .client
            .get(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::request_error)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        self.decode(response).await.map(Some)
    }

    pub(crate) async fn session_status(
        &self,
        routing: &OpenCodeRouting,
    ) -> Result<OpenCodeSessionStatuses, KitError> {
        self.get_json("session/status", Some(routing)).await
    }

    pub(crate) async fn session_inventory(&self) -> Result<OpenCodeSessionInventory, KitError> {
        let mut url = self.url("experimental/session", None)?;
        url.query_pairs_mut()
            .append_pair("archived", "true")
            .append_pair("limit", &(MAX_SESSION_INVENTORY + 1).to_string());
        let response = self
            .client
            .get(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::request_error)?;
        let has_continuation = response.headers().contains_key("x-next-cursor");
        let entries: Vec<OpenCodeSessionInventoryEntry> = self.decode(response).await?;
        let complete = !has_continuation && entries.len() <= MAX_SESSION_INVENTORY;
        OpenCodeSessionInventory::new(entries, complete)
    }

    pub(crate) async fn history(
        &self,
        request: &OpenCodeHistoryRequest,
    ) -> Result<Vec<OpenCodeHistoryRow>, KitError> {
        let rows: Vec<OpenCodeHistoryRow> = self.post_json("sync/history", None, request).await?;
        if rows.len() > MAX_HISTORY_ROWS {
            return Err(KitError::integration(
                "sync history exceeds its configured bound",
            ));
        }
        for row in &rows {
            row.validate()?;
        }
        Ok(rows)
    }

    pub(crate) async fn capture_subtree(
        &self,
        root_session_ids: &[String],
        directory: &std::path::Path,
    ) -> Result<Vec<String>, KitError> {
        self.session_inventory()
            .await?
            .subtree(root_session_ids, directory)
    }

    pub(crate) async fn capture_workspace_cleanup(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        bindings: &[OpenCodeSessionBinding],
    ) -> Result<Option<OpenCodeCleanup>, KitError> {
        let Some(first) = bindings.first() else {
            return Ok(None);
        };
        let directory = first.directory().to_path_buf();
        if bindings
            .iter()
            .any(|binding| binding.directory() != directory)
        {
            return Err(KitError::invalid_state(
                "workspace bindings disagree about their directory",
            ));
        }
        let roots = bindings
            .iter()
            .map(|binding| binding.require_session_id().map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()?;
        let sessions = self.capture_subtree(&roots, &directory).await?;
        OpenCodeCleanup::workspace(
            agent_id.clone(),
            workspace_id.clone(),
            directory,
            roots,
            sessions,
        )
        .map(Some)
    }

    pub(crate) async fn settle_cleanup(&self, cleanup: &OpenCodeCleanup) -> Result<(), KitError> {
        let inventory = self.session_inventory().await?;
        let routing = OpenCodeRouting::for_directory(cleanup.directory().to_path_buf())?;
        for root in cleanup.root_session_ids() {
            if inventory.contains(root) {
                match self.delete_session(&routing, root).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        let inventory = self.session_inventory().await?;
        let history = self.history(&OpenCodeHistoryRequest::empty()).await?;
        if inventory.contains_any(cleanup.session_ids())
            || history.iter().any(|row| {
                cleanup
                    .session_ids()
                    .iter()
                    .any(|id| id == row.aggregate_id())
            })
        {
            return Err(KitError::busy("native cleanup remains recovery-owned"));
        }
        Ok(())
    }

    pub(crate) async fn verify_inventory_ownership(
        &self,
        bindings: &[OpenCodeSessionBinding],
        cleanups: &[OpenCodeCleanup],
    ) -> Result<(), KitError> {
        let inventory = self.session_inventory().await?;
        let mut owned = std::collections::BTreeSet::new();
        for binding in bindings {
            let root = binding.require_session_id()?.to_owned();
            owned.insert(root.clone());
            if inventory.contains(&root) {
                owned.extend(inventory.subtree(std::slice::from_ref(&root), binding.directory())?);
            }
        }
        for cleanup in cleanups {
            owned.extend(cleanup.session_ids().iter().cloned());
        }
        let history = self.history(&OpenCodeHistoryRequest::empty()).await?;
        if !inventory.contains_only(&owned)
            || !history.iter().all(|row| owned.contains(row.aggregate_id()))
        {
            return Err(KitError::conflict(
                "server contains foreign native ownership",
            ));
        }
        Ok(())
    }

    pub(crate) async fn delete_session(
        &self,
        routing: &OpenCodeRouting,
        id: &str,
    ) -> Result<(), KitError> {
        let url = self.url_with_segments(&["session", id], routing)?;
        let response = self
            .client
            .delete(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::request_error)?;
        let removed: bool = self.decode(response).await?;
        if !removed {
            return Err(KitError::invalid_state("session deletion was not accepted"));
        }
        Ok(())
    }

    async fn health(&self) -> Result<OpenCodeHealth, KitError> {
        self.get_json("global/health", None).await
    }

    async fn capabilities(&self) -> Result<OpenCodeCapabilities, KitError> {
        self.get_json("experimental/capabilities", None).await
    }

    async fn get_json<T>(
        &self,
        path: &str,
        routing: Option<&OpenCodeRouting>,
    ) -> Result<T, KitError>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = self.url(path, routing)?;
        let response = self
            .client
            .get(url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(Self::request_error)?;
        self.decode(response).await
    }

    async fn post_json<T, B>(
        &self,
        path: &str,
        routing: Option<&OpenCodeRouting>,
        body: &B,
    ) -> Result<T, KitError>
    where
        T: serde::de::DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let url = self.url(path, routing)?;
        let response = self
            .client
            .post(url)
            .timeout(self.request_timeout)
            .json(body)
            .send()
            .await
            .map_err(Self::request_error)?;
        self.decode(response).await
    }

    fn url(&self, path: &str, routing: Option<&OpenCodeRouting>) -> Result<reqwest::Url, KitError> {
        let mut url = self
            .base_url
            .join(path)
            .map_err(|_| KitError::invalid_state("route could not be formed"))?;
        if let Some(routing) = routing {
            routing.apply(&mut url);
        }
        Ok(url)
    }

    fn url_with_segments(
        &self,
        segments: &[&str],
        routing: &OpenCodeRouting,
    ) -> Result<reqwest::Url, KitError> {
        if segments.is_empty()
            || segments
                .iter()
                .any(|segment| segment.is_empty() || segment.trim() != *segment)
        {
            return Err(KitError::invalid_state("route identity is malformed"));
        }
        let mut url = self.base_url.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| KitError::invalid_state("route cannot accept path segments"))?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        routing.apply(&mut url);
        Ok(url)
    }

    async fn require_accepted(
        &self,
        response: reqwest::Response,
        operation: &str,
    ) -> Result<(), OpenCodeMutationError> {
        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            return Err(OpenCodeMutationError::Rejected(Self::status_error(
                response.status(),
            )));
        }
        let accepted: bool = self
            .decode(response)
            .await
            .map_err(OpenCodeMutationError::Ambiguous)?;
        if !accepted {
            return Err(OpenCodeMutationError::Ambiguous(KitError::invalid_state(
                format!("{operation} was not accepted"),
            )));
        }
        Ok(())
    }

    async fn decode<T>(&self, response: reqwest::Response) -> Result<T, KitError>
    where
        T: serde::de::DeserializeOwned,
    {
        if !response.status().is_success() {
            return Err(Self::status_error(response.status()));
        }
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > MAX_RESPONSE_BYTES as u64) {
            return Err(KitError::protocol("response exceeds its configured bound")
                .with_detail(ErrorDetail::ResponseLimit {
                    limit: MAX_RESPONSE_BYTES,
                })
                .at_phase(ErrorPhase::ResponseRead));
        }
        let mut body = Vec::with_capacity(content_length.unwrap_or(0) as usize);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|error| Self::request_error(error).at_phase(ErrorPhase::ResponseRead))?;
            let next_length = body.len().checked_add(chunk.len()).ok_or_else(|| {
                KitError::protocol("response exceeds its configured bound")
                    .with_detail(ErrorDetail::ResponseLimit {
                        limit: MAX_RESPONSE_BYTES,
                    })
                    .at_phase(ErrorPhase::ResponseRead)
            })?;
            if next_length > MAX_RESPONSE_BYTES {
                return Err(KitError::protocol("response exceeds its configured bound")
                    .with_detail(ErrorDetail::ResponseLimit {
                        limit: MAX_RESPONSE_BYTES,
                    })
                    .at_phase(ErrorPhase::ResponseRead));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| {
            KitError::protocol("response payload is malformed")
                .with_detail(ErrorDetail::MalformedData)
                .at_phase(ErrorPhase::ResponseDecode)
        })
    }

    async fn decode_mutation<T>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, OpenCodeMutationError>
    where
        T: serde::de::DeserializeOwned,
    {
        if !response.status().is_success() {
            let status = response.status();
            let error = Self::status_error(status);
            return Err(if status == reqwest::StatusCode::NOT_FOUND {
                OpenCodeMutationError::Rejected(error)
            } else {
                OpenCodeMutationError::Ambiguous(error)
            });
        }
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > MAX_RESPONSE_BYTES as u64) {
            return Err(OpenCodeMutationError::Ambiguous(
                KitError::protocol("response exceeds its configured bound")
                    .with_detail(ErrorDetail::ResponseLimit {
                        limit: MAX_RESPONSE_BYTES,
                    })
                    .at_phase(ErrorPhase::ResponseRead),
            ));
        }
        let mut body = Vec::with_capacity(content_length.unwrap_or(0) as usize);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                OpenCodeMutationError::Ambiguous(
                    Self::request_error(error).at_phase(ErrorPhase::ResponseRead),
                )
            })?;
            let next_length = body.len().checked_add(chunk.len()).ok_or_else(|| {
                OpenCodeMutationError::Ambiguous(
                    KitError::protocol("response exceeds its configured bound")
                        .with_detail(ErrorDetail::ResponseLimit {
                            limit: MAX_RESPONSE_BYTES,
                        })
                        .at_phase(ErrorPhase::ResponseRead),
                )
            })?;
            if next_length > MAX_RESPONSE_BYTES {
                return Err(OpenCodeMutationError::Ambiguous(
                    KitError::protocol("response exceeds its configured bound")
                        .with_detail(ErrorDetail::ResponseLimit {
                            limit: MAX_RESPONSE_BYTES,
                        })
                        .at_phase(ErrorPhase::ResponseRead),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| {
            OpenCodeMutationError::Ambiguous(
                KitError::protocol("response payload is malformed")
                    .with_detail(ErrorDetail::MalformedData)
                    .at_phase(ErrorPhase::ResponseDecode),
            )
        })
    }

    async fn decode_abort<T>(&self, response: reqwest::Response) -> Result<T, OpenCodeMutationError>
    where
        T: serde::de::DeserializeOwned,
    {
        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            return Err(OpenCodeMutationError::Rejected(Self::status_error(
                response.status(),
            )));
        }
        self.decode_mutation(response).await
    }

    fn mutation_send_error(error: reqwest::Error) -> OpenCodeMutationError {
        // Redirect following and reqwest retries are disabled. A failed
        // initial connection therefore precedes admission of this request.
        let is_definitely_local = error.is_builder() || error.is_connect();
        let error = Self::request_error(error);
        if is_definitely_local {
            OpenCodeMutationError::Rejected(error)
        } else {
            OpenCodeMutationError::Ambiguous(error)
        }
    }

    fn request_error(error: reqwest::Error) -> KitError {
        if error.is_timeout() {
            KitError::timeout("request deadline elapsed")
                .with_detail(ErrorDetail::DeadlineElapsed {
                    kind: DeadlineKind::Request,
                    timeout: None,
                })
                .at_phase(ErrorPhase::RequestSend)
        } else {
            KitError::integration("HTTP request failed")
                .with_detail(ErrorDetail::TransportFailure)
                .at_phase(if error.is_builder() {
                    ErrorPhase::RequestBuild
                } else {
                    ErrorPhase::RequestSend
                })
        }
    }

    fn status_error(status: reqwest::StatusCode) -> KitError {
        Self::status_error_code(status.as_u16())
    }

    pub(super) fn status_error_code(status: u16) -> KitError {
        let (error, detail) = match status {
            400 => (
                KitError::invalid_input("server rejected the request as invalid"),
                ErrorDetail::InvalidRequest,
            ),
            401 => (
                KitError::integration("server authentication was rejected"),
                ErrorDetail::AuthenticationRejected,
            ),
            402 => (
                KitError::integration("billing prevents server execution"),
                ErrorDetail::BillingRequired,
            ),
            403 => (
                KitError::integration("server permission was denied"),
                ErrorDetail::PermissionDenied,
            ),
            404 => (
                KitError::not_found("server resource does not exist"),
                ErrorDetail::ResourceMissing,
            ),
            408 | 504 => (
                KitError::integration("server reported deadline expiry"),
                ErrorDetail::RemoteDeadlineExceeded,
            ),
            409 => (
                KitError::busy("server resource is busy"),
                ErrorDetail::RemoteConflict,
            ),
            412 => (
                KitError::integration("server precondition was not satisfied"),
                ErrorDetail::PreconditionFailed,
            ),
            413 => (
                KitError::integration("request exceeds the server size limit"),
                ErrorDetail::RequestTooLarge,
            ),
            422 => (
                KitError::integration("server rejected the request as invalid"),
                ErrorDetail::InvalidRequest,
            ),
            429 => (
                KitError::integration("server resources are exhausted"),
                ErrorDetail::ResourceExhausted,
            ),
            500 => (
                KitError::integration("server reported an internal failure"),
                ErrorDetail::RemoteInternal,
            ),
            502 | 503 | 529 => (
                KitError::integration("server is unavailable"),
                ErrorDetail::ServiceUnavailable,
            ),
            _ => (
                KitError::integration("server rejected the request"),
                ErrorDetail::RemoteRejected,
            ),
        };
        error
            .with_detail(detail)
            .with_http_status(status)
            .at_phase(ErrorPhase::ResponseRead)
    }
}
