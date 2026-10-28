use std::collections::{BTreeMap, BTreeSet};

use serde::de::IgnoredAny;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::{KitError, PromptResponse};

use super::requests::{OpenCodeCreationCorrelation, OpenCodeModelRequest, OpenCodeSessionMetadata};
use super::schemas::{OpenCodeForegroundMessageInfo, OpenCodeMessageParts, OpenCodeSharePolicy};

#[derive(Debug, Deserialize)]
pub(crate) struct OpenCodePath {
    directory: std::path::PathBuf,
}

impl OpenCodePath {
    pub(crate) fn into_directory(self) -> Result<std::path::PathBuf, KitError> {
        crate::workspace::WorkspaceRoot::new(
            self.directory.clone(),
            crate::workspace::WorkspaceRootAccess::ReadWrite,
        )
        .map_err(|_| KitError::protocol("native workspace directory is malformed"))?;
        Ok(self.directory)
    }
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct OpenCodeMessageResponse {
    info: OpenCodeMessageResponseInfo,
    parts: OpenCodeMessageParts,
}

#[derive(Debug, Deserialize)]
struct OpenCodeMessageResponseInfo {
    #[serde(flatten)]
    message: OpenCodeForegroundMessageInfo,
    #[serde(default)]
    structured: OpenCodeStructuredState,
}

#[derive(Debug, Default)]
enum OpenCodeStructuredState {
    #[default]
    Missing,
    Present(Value),
}

pub(crate) const SELECTED_OPENCODE_VERSION: &str = "1.19.3";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeProviderInventory {
    all: Vec<OpenCodeProvider>,
    default: BTreeMap<String, String>,
    connected: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProvider {
    id: String,
    #[serde(rename = "name")]
    _name: String,
    #[serde(rename = "source")]
    _source: OpenCodeProviderSource,
    #[serde(rename = "env")]
    _env: Vec<String>,
    #[serde(rename = "key")]
    _key: Option<String>,
    #[serde(rename = "options")]
    _options: BTreeMap<String, IgnoredAny>,
    models: BTreeMap<String, OpenCodeProviderModel>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum OpenCodeProviderSource {
    Env,
    Config,
    Custom,
    Api,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderModel {
    id: String,
    #[serde(rename = "providerID")]
    provider_id: String,
    #[serde(rename = "api")]
    _api: OpenCodeProviderApi,
    name: String,
    #[serde(rename = "family")]
    _family: Option<String>,
    #[serde(rename = "capabilities")]
    _capabilities: OpenCodeProviderCapabilities,
    cost: OpenCodeProviderCost,
    limit: OpenCodeProviderLimit,
    #[serde(rename = "status")]
    _status: OpenCodeModelStatus,
    #[serde(rename = "options")]
    _options: BTreeMap<String, IgnoredAny>,
    #[serde(rename = "headers")]
    _headers: BTreeMap<String, String>,
    #[serde(rename = "release_date")]
    _release_date: String,
    #[serde(rename = "variants")]
    _variants: Option<BTreeMap<String, BTreeMap<String, IgnoredAny>>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderApi {
    #[serde(rename = "id")]
    _id: String,
    #[serde(rename = "url")]
    _url: String,
    #[serde(rename = "npm")]
    _npm: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderModalities {
    #[serde(rename = "text")]
    _text: bool,
    #[serde(rename = "audio")]
    _audio: bool,
    #[serde(rename = "image")]
    _image: bool,
    #[serde(rename = "video")]
    _video: bool,
    #[serde(rename = "pdf")]
    _pdf: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum OpenCodeProviderInterleaved {
    Boolean(#[serde(deserialize_with = "OpenCodeProviderInterleaved::deserialize_boolean")] ()),
    Field {
        #[serde(rename = "field")]
        _field: String,
    },
}

impl OpenCodeProviderInterleaved {
    fn deserialize_boolean<'de, D: Deserializer<'de>>(deserializer: D) -> Result<(), D::Error> {
        // This metadata is shape-checked but does not select portable reasoning.
        bool::deserialize(deserializer).map(|_| ())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderCapabilities {
    #[serde(rename = "temperature")]
    _temperature: bool,
    #[serde(rename = "reasoning")]
    _reasoning: bool,
    #[serde(rename = "attachment")]
    _attachment: bool,
    #[serde(rename = "toolcall")]
    _toolcall: bool,
    #[serde(rename = "input")]
    _input: OpenCodeProviderModalities,
    #[serde(rename = "output")]
    _output: OpenCodeProviderModalities,
    #[serde(rename = "interleaved")]
    _interleaved: OpenCodeProviderInterleaved,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderCacheCost {
    read: f64,
    write: f64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderCostTier {
    input: f64,
    output: f64,
    cache: OpenCodeProviderCacheCost,
    tier: OpenCodeProviderTier,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderTier {
    #[serde(rename = "type")]
    _kind: OpenCodeProviderTierKind,
    size: f64,
}

#[derive(Debug, Deserialize)]
enum OpenCodeProviderTierKind {
    #[serde(rename = "context")]
    Context,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderCost {
    input: f64,
    output: f64,
    cache: OpenCodeProviderCacheCost,
    tiers: Option<Vec<OpenCodeProviderCostTier>>,
    #[serde(rename = "experimentalOver200K")]
    experimental_over_200k: Option<OpenCodeProviderOverageCost>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderOverageCost {
    input: f64,
    output: f64,
    cache: OpenCodeProviderCacheCost,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeProviderLimit {
    context: f64,
    input: Option<f64>,
    output: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum OpenCodeModelStatus {
    Alpha,
    Beta,
    Deprecated,
    Active,
}

#[derive(Clone, Debug)]
pub(crate) struct OpenCodeNativeModel {
    request: OpenCodeModelRequest,
    display_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub(crate) struct OpenCodeToolIds(Vec<String>);

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub(crate) struct OpenCodeToolInventory(Vec<OpenCodeToolDescription>);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeToolDescription {
    id: String,
    description: String,
    parameters: Value,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub(crate) struct OpenCodeMcpStatuses(BTreeMap<String, OpenCodeMcpStatus>);

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum OpenCodeMcpStatus {
    Connected,
    Disabled,
    Failed { error: String },
    NeedsAuth,
    NeedsClientRegistration { error: String },
}

impl OpenCodeProviderInventory {
    pub(crate) fn validate(
        self,
        provider_bound: usize,
        model_bound: usize,
    ) -> Result<Self, KitError> {
        let model_count = self.all.iter().try_fold(0_usize, |count, provider| {
            count
                .checked_add(provider.models.len())
                .ok_or_else(|| KitError::protocol("provider inventory model count overflowed"))
        })?;
        if self.all.len() > provider_bound || model_count > model_bound {
            return Err(KitError::integration(
                "provider inventory exceeds its configured bound",
            ));
        }
        let mut provider_ids = BTreeSet::new();
        for provider in &self.all {
            if !provider_ids.insert(provider.id.as_str()) {
                return Err(KitError::protocol(
                    "provider inventory repeats a provider identity",
                ));
            }
            for (model_key, model) in &provider.models {
                if model_key != &model.id
                    || model.provider_id != provider.id
                    || !model.numeric_fields_are_finite()
                {
                    return Err(KitError::protocol("provider inventory is malformed"));
                }
            }
        }
        let mut connected_ids = BTreeSet::new();
        if self.connected.iter().any(|connected| {
            !connected_ids.insert(connected.as_str())
                || !self.all.iter().any(|provider| provider.id == *connected)
        }) || self
            .all
            .iter()
            .any(|provider| !self.default.contains_key(&provider.id))
            || self.default.iter().any(|(provider_id, model_id)| {
                !self.all.iter().any(|provider| {
                    provider.id == *provider_id
                        && provider.models.values().any(|model| model.id == *model_id)
                })
            })
        {
            return Err(KitError::protocol(
                "provider inventory contradicts its indexes",
            ));
        }
        Ok(self)
    }

    pub(crate) fn connected_models(&self) -> Result<Vec<OpenCodeNativeModel>, KitError> {
        let mut models = Vec::new();
        for provider in &self.all {
            if !self
                .connected
                .iter()
                .any(|connected| connected == &provider.id)
            {
                continue;
            }
            for model in provider.models.values() {
                models.push(OpenCodeNativeModel {
                    request: OpenCodeModelRequest::new(provider.id.clone(), model.id.clone())?,
                    display_name: model.name.clone(),
                });
            }
        }
        Ok(models)
    }
}

impl OpenCodeProviderModel {
    fn numeric_fields_are_finite(&self) -> bool {
        self.cost.is_finite() && self.limit.is_finite()
    }
}

impl OpenCodeProviderCacheCost {
    fn is_finite(&self) -> bool {
        self.read.is_finite() && self.write.is_finite()
    }
}

impl OpenCodeProviderCostTier {
    fn is_finite(&self) -> bool {
        self.input.is_finite()
            && self.output.is_finite()
            && self.cache.is_finite()
            && self.tier.size.is_finite()
    }
}

impl OpenCodeProviderCost {
    fn is_finite(&self) -> bool {
        self.input.is_finite()
            && self.output.is_finite()
            && self.cache.is_finite()
            && self
                .tiers
                .as_ref()
                .is_none_or(|tiers| tiers.iter().all(OpenCodeProviderCostTier::is_finite))
            && self
                .experimental_over_200k
                .as_ref()
                .is_none_or(OpenCodeProviderOverageCost::is_finite)
    }
}

impl OpenCodeProviderOverageCost {
    fn is_finite(&self) -> bool {
        self.input.is_finite() && self.output.is_finite() && self.cache.is_finite()
    }
}

impl OpenCodeProviderLimit {
    fn is_finite(&self) -> bool {
        self.context.is_finite() && self.input.is_none_or(f64::is_finite) && self.output.is_finite()
    }
}

impl OpenCodeNativeModel {
    pub(crate) fn request(&self) -> &OpenCodeModelRequest {
        &self.request
    }

    pub(crate) fn provider_id(&self) -> &str {
        self.request.provider_id()
    }

    pub(crate) fn model_id(&self) -> &str {
        self.request.model_id()
    }

    pub(crate) fn display_name(&self) -> &str {
        &self.display_name
    }
}

impl OpenCodeToolIds {
    pub(crate) fn validate(self, bound: usize) -> Result<Self, KitError> {
        if self.0.len() > bound {
            return Err(KitError::integration(
                "native tool-id inventory exceeds its configured bound",
            ));
        }
        Ok(self)
    }

    pub(crate) fn ids(&self) -> &[String] {
        &self.0
    }
}

impl OpenCodeToolInventory {
    pub(crate) fn validate(self, bound: usize) -> Result<Self, KitError> {
        if self.0.len() > bound {
            return Err(KitError::integration(
                "native tool inventory exceeds its configured bound",
            ));
        }
        Ok(self)
    }

    pub(crate) fn descriptions(&self) -> &[OpenCodeToolDescription] {
        &self.0
    }
}

impl OpenCodeToolDescription {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn description(&self) -> &str {
        &self.description
    }

    pub(crate) fn parameters(&self) -> &Value {
        &self.parameters
    }
}

impl OpenCodeMcpStatuses {
    pub(crate) fn validate(self, bound: usize) -> Result<Self, KitError> {
        if self.0.len() > bound {
            return Err(KitError::integration(
                "MCP status inventory exceeds its configured bound",
            ));
        }
        for status in self.0.values() {
            match status {
                OpenCodeMcpStatus::Failed { error }
                | OpenCodeMcpStatus::NeedsClientRegistration { error } => {
                    let _ = error;
                }
                OpenCodeMcpStatus::Connected
                | OpenCodeMcpStatus::Disabled
                | OpenCodeMcpStatus::NeedsAuth => {}
            }
        }
        Ok(self)
    }

    pub(crate) fn has_connected(&self) -> bool {
        self.0
            .values()
            .any(|status| matches!(status, OpenCodeMcpStatus::Connected))
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeHealth {
    healthy: bool,
    version: String,
}

impl OpenCodeHealth {
    pub(crate) fn validate(self) -> Result<(), KitError> {
        if !self.healthy {
            return Err(KitError::protocol(
                "health response did not report a healthy server",
            ));
        }
        if self.version != "local" && !Self::version_is_valid(&self.version) {
            return Err(KitError::protocol("health version is malformed"));
        }
        if self.version != SELECTED_OPENCODE_VERSION {
            return Err(KitError::invalid_config(
                "server version does not match the selected release",
            ));
        }
        Ok(())
    }

    fn version_is_valid(version: &str) -> bool {
        let (version, build) = match version.split_once('+') {
            Some((version, build)) => (version, Some(build)),
            None => (version, None),
        };
        if build.is_some_and(|build| {
            build.split('.').any(|identifier| {
                identifier.is_empty()
                    || !identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            })
        }) {
            return false;
        }

        let (core, prerelease) = match version.split_once('-') {
            Some((core, prerelease)) => (core, Some(prerelease)),
            None => (version, None),
        };
        let mut components = core.split('.');
        for _ in 0..3 {
            let Some(component) = components.next() else {
                return false;
            };
            if component.is_empty()
                || !component.bytes().all(|byte| byte.is_ascii_digit())
                || (component.len() > 1 && component.starts_with('0'))
            {
                return false;
            }
        }
        if components.next().is_some() {
            return false;
        }

        !prerelease.is_some_and(|prerelease| {
            prerelease.split('.').any(|identifier| {
                identifier.is_empty()
                    || !identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                    || (identifier.len() > 1
                        && identifier.bytes().all(|byte| byte.is_ascii_digit())
                        && identifier.starts_with('0'))
            })
        })
    }
}

impl OpenCodeMessageResponse {
    pub(crate) fn message_id(&self) -> &str {
        self.info.message.message_id()
    }

    pub(crate) fn parent_id(&self, session_id: &str) -> Result<&str, KitError> {
        self.info.message.assistant_parent_id(session_id)
    }

    pub(crate) fn cancelled_tool_parent(
        &self,
        session_id: &str,
        message_id: &str,
        call_id: &str,
    ) -> Result<String, KitError> {
        let parent_id = self
            .info
            .message
            .cancelled_parent_id(session_id, message_id)?;
        self.parts
            .verify_interrupted_call(session_id, message_id, call_id)?;
        Ok(parent_id.to_owned())
    }

    pub(crate) fn verify_prompt_parent(
        self,
        session_id: &str,
        message_id: &str,
        part_id: &str,
    ) -> Result<(), KitError> {
        self.info
            .message
            .verify_prompt_parent(session_id, message_id)?;
        self.parts
            .verify_prompt_parent(session_id, message_id, part_id)
    }

    pub(crate) fn into_prompt_response(
        self,
        session_id: &str,
        requires_structured: bool,
    ) -> Result<PromptResponse, KitError> {
        let OpenCodeMessageResponse { info, parts } = self;
        let OpenCodeMessageResponseInfo {
            message,
            structured,
        } = info;
        let (response_message_id, error) = message.into_assistant(session_id)?;
        if let Some(error) = error {
            return Err(error.into_sdk_error());
        }
        let mut text_parts = parts.into_output_texts(session_id, &response_message_id)?;
        if text_parts
            .iter()
            .any(|(id, _)| id.is_empty() || id.trim() != id)
        {
            return Err(KitError::protocol(
                "foreground response contains a malformed part identity",
            ));
        }
        text_parts.sort_by(|left, right| left.0.cmp(&right.0));
        if text_parts.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(KitError::protocol(
                "foreground response repeats a part identity",
            ));
        }
        let structured = match (requires_structured, structured) {
            (true, OpenCodeStructuredState::Present(value)) => Some(value),
            (false, OpenCodeStructuredState::Missing) => None,
            (true, OpenCodeStructuredState::Missing) => {
                return Err(KitError::protocol(
                    "structured response is missing typed Assistant state",
                ));
            }
            (false, OpenCodeStructuredState::Present(_)) => {
                return Err(KitError::protocol(
                    "plain response unexpectedly contains structured Assistant state",
                ));
            }
        };
        Ok(PromptResponse::from_parts(
            text_parts.into_iter().map(|(_, text)| text).collect(),
            structured,
            None,
        ))
    }
}

impl<'de> Deserialize<'de> for OpenCodeStructuredState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Value::deserialize(deserializer).map(Self::Present)
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct OpenCodeCapabilities {
    background_subagents: bool,
}

impl OpenCodeCapabilities {
    pub(crate) fn validate(self) -> Result<(), KitError> {
        if self.background_subagents {
            return Err(KitError::unsupported(
                "background subagents violate foreground prompt ownership",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct OpenCodeEffectiveConfig {
    share: Option<OpenCodeSharePolicy>,
}

impl OpenCodeEffectiveConfig {
    pub(crate) fn require_disabled_sharing(self) -> Result<(), KitError> {
        if self.share != Some(OpenCodeSharePolicy::Disabled) {
            return Err(KitError::invalid_config(
                "effective sharing must be disabled",
            ));
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
pub(crate) struct OpenCodeSession {
    id: String,
    #[serde(rename = "parentID")]
    parent_id: Option<String>,
    share: Option<OpenCodeShareBinding>,
    metadata: Option<OpenCodeSessionMetadata>,
}

#[derive(serde::Deserialize)]
struct OpenCodeShareBinding {
    #[serde(rename = "url")]
    _url: String,
}

impl OpenCodeSession {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn require_private(&self) -> Result<(), KitError> {
        if self.parent_id.is_some() {
            return Err(KitError::invalid_state(
                "native descendant cannot own a portable conversation",
            ));
        }
        if self.share.is_some() {
            return Err(KitError::invalid_state(
                "session unexpectedly has a native share binding",
            ));
        }
        if let Some(metadata) = &self.metadata {
            metadata.validate()?;
        }
        Ok(())
    }

    pub(crate) fn verify_correlation(
        &self,
        expected: &OpenCodeCreationCorrelation,
    ) -> Result<(), KitError> {
        if !self
            .metadata
            .as_ref()
            .is_some_and(|value| value.matches(expected))
        {
            return Err(KitError::invalid_state(
                "created session correlation does not match",
            ));
        }
        Ok(())
    }
}
