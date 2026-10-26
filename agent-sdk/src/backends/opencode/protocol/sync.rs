use std::collections::BTreeMap;

use crate::SdkError;

#[derive(Clone, Debug, serde::Serialize)]
#[serde(transparent)]
pub(crate) struct OpenCodeHistoryRequest {
    high_water: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(crate) struct OpenCodeHistoryRow {
    pub(super) id: String,
    pub(super) aggregate_id: String,
    pub(super) seq: u64,
    #[serde(rename = "type")]
    pub(super) event_type: String,
    pub(super) data: serde_json::Map<String, serde_json::Value>,
}

impl OpenCodeHistoryRequest {
    pub(crate) fn new(high_water: BTreeMap<String, u64>) -> Result<Self, SdkError> {
        if high_water.keys().any(|id| id.is_empty() || id.trim() != id) {
            return Err(SdkError::invalid_state(
                "recovery high-water identity is malformed",
            ));
        }
        Ok(Self { high_water })
    }

    pub(crate) fn empty() -> Self {
        Self {
            high_water: BTreeMap::new(),
        }
    }
}

impl OpenCodeHistoryRow {
    pub(crate) fn encoded_len(&self) -> usize {
        self.id
            .len()
            .saturating_add(self.aggregate_id.len())
            .saturating_add(self.event_type.len())
            .saturating_add(Self::map_encoded_len(&self.data))
            .saturating_add(64)
    }

    pub(crate) fn into_validated(self) -> Result<Self, SdkError> {
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        let versioned = self
            .event_type
            .rsplit_once('.')
            .is_some_and(|(base, version)| {
                !base.is_empty()
                    && !version.is_empty()
                    && version.bytes().all(|byte| byte.is_ascii_digit())
            });
        if !self.id.starts_with("evt_")
            || self.id.trim() != self.id
            || self.aggregate_id.is_empty()
            || self.aggregate_id.trim() != self.aggregate_id
            || self.event_type.is_empty()
            || self.event_type.trim() != self.event_type
            || !versioned
        {
            return Err(SdkError::protocol("history row is malformed"));
        }
        Ok(())
    }

    pub(crate) fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }

    fn map_encoded_len(values: &serde_json::Map<String, serde_json::Value>) -> usize {
        values.iter().fold(2usize, |length, (key, value)| {
            length
                .saturating_add(Self::string_encoded_len(key))
                .saturating_add(Self::value_encoded_len(value))
                .saturating_add(2)
        })
    }

    fn value_encoded_len(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::Null => 4,
            serde_json::Value::Bool(_) => 5,
            serde_json::Value::Number(_) => 32,
            serde_json::Value::String(value) => Self::string_encoded_len(value),
            serde_json::Value::Array(values) => values.iter().fold(2usize, |length, value| {
                length
                    .saturating_add(Self::value_encoded_len(value))
                    .saturating_add(1)
            }),
            serde_json::Value::Object(values) => Self::map_encoded_len(values),
        }
    }

    fn string_encoded_len(value: &str) -> usize {
        value.len().saturating_mul(6).saturating_add(2)
    }
}
