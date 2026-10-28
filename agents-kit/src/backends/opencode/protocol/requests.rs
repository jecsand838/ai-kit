use std::{collections::BTreeMap, collections::BTreeSet, path::Path};

use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};

use super::schemas::{OpenCodePermissionRule, OpenCodeSessionStatus};
use crate::KitError;

pub(crate) const MAX_STRUCTURED_SCHEMA_BYTES: usize = 64 << 10;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpenCodeMessageRequest {
    no_reply: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<OpenCodeModelRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<OpenCodeStructuredFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    parts: [OpenCodeTextInput; 1],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenCodeModelRequest {
    #[serde(rename = "providerID")]
    provider_id: String,
    #[serde(rename = "modelID")]
    model_id: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpenCodeStructuredFormat {
    #[serde(rename = "type")]
    kind: &'static str,
    schema: serde_json::Value,
    retry_count: u64,
}

#[derive(Clone, Debug, Serialize)]
struct OpenCodeTextInput {
    id: String,
    #[serde(rename = "type")]
    kind: &'static str,
    text: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OpenCodePermissionReply {
    reply: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OpenCodeQuestionReply {
    answers: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OpenCodeRouting {
    directory: Option<String>,
}

impl OpenCodeRouting {
    pub(crate) fn for_directory(directory: std::path::PathBuf) -> Result<Self, KitError> {
        let directory = directory
            .into_os_string()
            .into_string()
            .map_err(|_| KitError::unsupported("cwd must be valid UTF-8"))?;
        Ok(Self {
            directory: Some(directory),
        })
    }

    pub(crate) fn apply(&self, url: &mut reqwest::Url) {
        if let Some(directory) = &self.directory {
            url.query_pairs_mut().append_pair("directory", directory);
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct OpenCodeSessionCreateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    metadata: OpenCodeSessionMetadata,
    permission: Vec<OpenCodePermissionRule>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct OpenCodeCreationCorrelation(String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct OpenCodeSessionMetadata {
    #[serde(
        rename = "agents-kit.creation-correlation",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    creation_correlation: Option<String>,
    #[serde(flatten, default, skip_serializing)]
    _other: BTreeMap<String, IgnoredAny>,
}

impl OpenCodeSessionCreateRequest {
    pub(crate) fn new(title: Option<String>) -> Self {
        let correlation = OpenCodeCreationCorrelation::fresh();
        Self {
            title,
            metadata: OpenCodeSessionMetadata::for_creation(&correlation),
            permission: vec![OpenCodePermissionRule::full_access()],
        }
    }

    pub(crate) fn correlation(&self) -> Result<OpenCodeCreationCorrelation, KitError> {
        self.metadata
            .creation_correlation
            .as_ref()
            .ok_or_else(|| KitError::invalid_state("creation request lacks its correlation"))
            .and_then(|value| OpenCodeCreationCorrelation::from_stored(value))
    }
}

impl OpenCodeMessageRequest {
    pub(crate) fn new(text: String, system: Option<String>) -> Result<Self, KitError> {
        if text.is_empty()
            || system
                .as_deref()
                .is_some_and(|instructions| instructions.is_empty())
        {
            return Err(KitError::invalid_state("message request is malformed"));
        }
        let part_id = format!("prt_{}", uuid::Uuid::new_v4().simple());
        Ok(Self {
            no_reply: false,
            model: None,
            format: None,
            system,
            parts: [OpenCodeTextInput {
                id: part_id,
                kind: "text",
                text,
            }],
        })
    }

    pub(crate) fn part_id(&self) -> &str {
        &self.parts[0].id
    }

    pub(crate) fn model(&self) -> Option<&OpenCodeModelRequest> {
        self.model.as_ref()
    }

    pub(crate) fn structured_schema(&self) -> Option<&serde_json::Value> {
        self.format.as_ref().map(|format| &format.schema)
    }

    pub(crate) fn select_model(mut self, model: OpenCodeModelRequest) -> Self {
        self.model = Some(model);
        self
    }

    pub(crate) fn require_structured(
        mut self,
        schema: serde_json::Value,
    ) -> Result<Self, KitError> {
        if schema.as_object().is_none_or(serde_json::Map::is_empty) {
            return Err(KitError::invalid_input("structured schema is malformed"));
        }
        if serde_json::to_vec(&schema)
            .map_err(|_| KitError::invalid_input("structured schema is not encodable"))?
            .len()
            > MAX_STRUCTURED_SCHEMA_BYTES
        {
            return Err(KitError::invalid_input(
                "structured schema exceeds its private request bound",
            ));
        }
        self.format = Some(OpenCodeStructuredFormat {
            kind: "json_schema",
            schema,
            retry_count: 0,
        });
        Ok(self)
    }

    pub(crate) fn requires_structured(&self) -> bool {
        self.format.is_some()
    }
}

impl OpenCodeModelRequest {
    pub(crate) fn new(provider_id: String, model_id: String) -> Result<Self, KitError> {
        if provider_id.is_empty()
            || provider_id.trim() != provider_id
            || model_id.is_empty()
            || model_id.trim() != model_id
        {
            return Err(KitError::protocol("provider/model identity is malformed"));
        }
        Ok(Self {
            provider_id,
            model_id,
        })
    }

    pub(crate) fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub(crate) fn model_id(&self) -> &str {
        &self.model_id
    }
}

impl OpenCodeCreationCorrelation {
    fn fresh() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }

    pub(crate) fn from_stored(value: &str) -> Result<Self, KitError> {
        if !Self::is_valid(value) {
            return Err(KitError::invalid_state(
                "stored creation correlation is malformed",
            ));
        }
        Ok(Self(value.to_owned()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    fn is_valid(value: &str) -> bool {
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }
}

impl OpenCodeSessionMetadata {
    fn for_creation(correlation: &OpenCodeCreationCorrelation) -> Self {
        Self {
            creation_correlation: Some(correlation.as_str().to_owned()),
            _other: BTreeMap::new(),
        }
    }

    pub(super) fn validate(&self) -> Result<(), KitError> {
        if self
            .creation_correlation
            .as_deref()
            .is_some_and(|value| !OpenCodeCreationCorrelation::is_valid(value))
        {
            return Err(KitError::protocol(
                "session creation correlation is malformed",
            ));
        }
        Ok(())
    }

    pub(super) fn matches(&self, correlation: &OpenCodeCreationCorrelation) -> bool {
        self.creation_correlation.as_deref() == Some(correlation.as_str())
    }
}

impl OpenCodePermissionReply {
    pub(crate) fn allow_once() -> Self {
        Self {
            reply: "once",
            message: None,
        }
    }

    pub(crate) fn deny(reason: String) -> Self {
        Self {
            reply: "reject",
            message: Some(reason),
        }
    }
}

impl OpenCodeQuestionReply {
    pub(crate) fn new(answers: Vec<Vec<String>>) -> Self {
        Self { answers }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OpenCodeSessionState {
    id: String,
    directory: String,
    #[serde(rename = "parentID")]
    parent_id: Option<String>,
    share: Option<OpenCodeShareState>,
    metadata: Option<OpenCodeSessionMetadata>,
    permission: Option<Vec<OpenCodePermissionState>>,
}

#[derive(Clone, Debug, Deserialize)]
struct OpenCodeShareState {
    #[serde(rename = "url")]
    _url: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
struct OpenCodePermissionState {
    permission: String,
    pattern: String,
    action: String,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub(crate) struct OpenCodeSessionStatuses(BTreeMap<String, OpenCodeSessionStatus>);

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct OpenCodeSessionInventoryEntry {
    id: String,
    directory: String,
    #[serde(rename = "parentID")]
    parent_id: Option<String>,
    metadata: Option<OpenCodeSessionMetadata>,
}

pub(crate) struct OpenCodeSessionInventory {
    entries: BTreeMap<String, OpenCodeSessionInventoryEntry>,
}

impl OpenCodeSessionState {
    pub(crate) fn verify(&self, expected_id: &str, cwd: &Path) -> Result<(), KitError> {
        let cwd = cwd
            .to_str()
            .ok_or_else(|| KitError::unsupported("cwd must be valid UTF-8"))?;
        if self.id != expected_id || self.directory != cwd {
            return Err(KitError::invalid_state(
                "session contradicts its durable binding",
            ));
        }
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
        let expected = OpenCodePermissionState {
            permission: "*".to_owned(),
            pattern: "*".to_owned(),
            action: "allow".to_owned(),
        };
        if self.permission.as_ref().and_then(|rules| rules.last()) != Some(&expected) {
            return Err(KitError::invalid_state(
                "session lacks the required unrestricted policy",
            ));
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
                "session creation correlation does not match",
            ));
        }
        Ok(())
    }
}

impl OpenCodeSessionStatuses {
    pub(crate) fn is_active(&self, session_id: &str) -> bool {
        self.0.contains_key(session_id)
    }

    pub(crate) fn require_idle(&self, session_id: &str) -> Result<(), KitError> {
        if let Some(status) = self.0.get(session_id) {
            match status {
                OpenCodeSessionStatus::Idle | OpenCodeSessionStatus::Busy => {}
                OpenCodeSessionStatus::Retry {
                    attempt,
                    message,
                    next,
                    ..
                } => {
                    let _ = (attempt, message, next);
                }
            }
            return Err(KitError::busy(
                "session retains native active or recovery state",
            ));
        }
        Ok(())
    }
}

impl OpenCodeSessionInventory {
    pub(crate) fn new(
        entries: Vec<OpenCodeSessionInventoryEntry>,
        complete: bool,
    ) -> Result<Self, KitError> {
        if !complete {
            return Err(KitError::integration(
                "session inventory exceeds its configured bound",
            ));
        }
        let mut indexed = BTreeMap::new();
        for entry in entries {
            if entry.id.is_empty()
                || entry.id.trim() != entry.id
                || entry.directory.is_empty()
                || entry
                    .parent_id
                    .as_ref()
                    .is_some_and(|id| id.is_empty() || id.trim() != id)
                || entry
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.validate().is_err())
                || indexed.insert(entry.id.clone(), entry).is_some()
            {
                return Err(KitError::protocol("session inventory is malformed"));
            }
        }
        Ok(Self { entries: indexed })
    }

    pub(crate) fn subtree(
        &self,
        roots: &[String],
        directory: &Path,
    ) -> Result<Vec<String>, KitError> {
        let directory = directory
            .to_str()
            .ok_or_else(|| KitError::unsupported("cwd must be valid UTF-8"))?;
        let mut owned = BTreeSet::new();
        for root in roots {
            let entry = self
                .entries
                .get(root)
                .ok_or_else(|| KitError::conflict("bound session does not exist"))?;
            if entry.directory != directory {
                return Err(KitError::invalid_state(
                    "session inventory contradicts its durable directory",
                ));
            }
            owned.insert(root.clone());
        }
        loop {
            let before = owned.len();
            for entry in self.entries.values() {
                if entry
                    .parent_id
                    .as_ref()
                    .is_some_and(|parent| owned.contains(parent))
                {
                    if entry.directory != directory {
                        return Err(KitError::invalid_state(
                            "descendant session changed workspace directory",
                        ));
                    }
                    owned.insert(entry.id.clone());
                }
            }
            if owned.len() == before {
                return Ok(owned.into_iter().collect());
            }
        }
    }

    pub(crate) fn contains_any(&self, ids: &[String]) -> bool {
        ids.iter().any(|id| self.entries.contains_key(id))
    }

    pub(crate) fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    pub(crate) fn contains_only(&self, owned: &BTreeSet<String>) -> bool {
        self.entries.keys().all(|id| owned.contains(id))
    }

    pub(crate) fn correlation_matches(
        &self,
        expected: &OpenCodeCreationCorrelation,
    ) -> Vec<String> {
        self.entries
            .values()
            .filter(|entry| {
                entry
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.matches(expected))
            })
            .map(|entry| entry.id.clone())
            .collect()
    }
}
