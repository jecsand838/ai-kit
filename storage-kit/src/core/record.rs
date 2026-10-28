use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::vector::DenseVector;
use crate::StorageError;

/// Logical record identifier preserved by implementations supporting logical IDs.
///
/// Constructing an identifier does not enable ID persistence or lookup on every
/// backend. Consult the backend contract and handle authoritative operation errors. The supplied LanceDB adapter does not support
/// logical IDs; store an application key as a normal field and use a filter,
/// as shown in the [`crate::database`] walkthrough.
///
/// # Examples
///
/// ```
/// let id = storage_kit::database::data::RecordId::String("doc-1".into());
/// assert_eq!(id.as_string_lossy(), "doc-1");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RecordId {
    /// UTF-8 string id.
    String(String),
    /// Unsigned 64-bit id.
    U64(u64),
    /// Signed 64-bit id.
    I64(i64),
    /// UUID id.
    Uuid(Uuid),
    /// Opaque application-defined id that should round-trip as text.
    Opaque(String),
}

impl RecordId {
    /// Converts the identifier into a human-readable string.
    ///
    /// # Examples
    ///
    /// ```
    /// let id = storage_kit::database::data::RecordId::U64(42);
    /// assert_eq!(id.as_string_lossy(), "42");
    /// ```
    pub fn as_string_lossy(&self) -> String {
        match self {
            Self::String(v) => v.clone(),
            Self::U64(v) => v.to_string(),
            Self::I64(v) => v.to_string(),
            Self::Uuid(v) => v.to_string(),
            Self::Opaque(v) => v.clone(),
        }
    }
}

impl From<&str> for RecordId {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<String> for RecordId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<u64> for RecordId {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<i64> for RecordId {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<Uuid> for RecordId {
    fn from(value: Uuid) -> Self {
        Self::Uuid(value)
    }
}

/// Portable record payload used for inserts, updates, and query responses.
///
/// # Examples
///
/// ```
/// let mut record = storage_kit::database::data::DataRecord {
///     id: Some(storage_kit::database::data::RecordId::String("doc-1".into())),
///     ..Default::default()
/// };
/// record
///     .fields
///     .insert("title".into(), serde_json::json!("Storage Kit"));
/// assert_eq!(record.id.as_ref().unwrap().as_string_lossy(), "doc-1");
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DataRecord {
    /// Logical record identifier, when the selected backend supports ID persistence.
    /// Unsupported backends reject a populated ID rather than synthesizing one.
    pub id: Option<RecordId>,
    /// Named vector payloads stored with the record.
    pub vectors: BTreeMap<String, DenseVector>,
    /// JSON document fields stored alongside vectors and the optional logical id.
    pub fields: serde_json::Map<String, Value>,
}

impl DataRecord {
    /// Creates a record with a logical id.
    ///
    /// # Examples
    ///
    /// ```
    /// let record = storage_kit::database::data::DataRecord::new("doc-1");
    /// assert_eq!(record.id.unwrap().as_string_lossy(), "doc-1");
    /// ```
    #[must_use]
    pub fn new(id: impl Into<RecordId>) -> Self {
        Self {
            id: Some(id.into()),
            ..Self::default()
        }
    }

    /// Creates a record without a logical id.
    ///
    /// # Examples
    ///
    /// ```
    /// let record = storage_kit::database::data::DataRecord::without_id();
    /// assert!(record.id.is_none());
    /// ```
    #[must_use]
    pub fn without_id() -> Self {
        Self::default()
    }

    /// Adds one serializable JSON field.
    ///
    /// # Examples
    ///
    /// ```
    /// let record = storage_kit::database::data::DataRecord::new("doc-1")
    ///     .field("title", "Storage Kit")?;
    /// assert_eq!(record.fields["title"], serde_json::json!("Storage Kit"));
    /// # Ok::<(), storage_kit::StorageError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be serialized to JSON.
    pub fn field<T>(mut self, key: impl Into<String>, value: T) -> Result<Self, StorageError>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value).map_err(|error| {
            StorageError::InvalidConfig(format!("failed to serialize record field: {error}"))
        })?;
        self.fields.insert(key.into(), value);
        Ok(self)
    }

    /// Adds one already-materialized JSON field.
    ///
    /// # Examples
    ///
    /// ```
    /// let record = storage_kit::database::data::DataRecord::new("doc-1")
    ///     .json_field("title", serde_json::json!("Storage Kit"));
    /// assert_eq!(record.fields["title"], serde_json::json!("Storage Kit"));
    /// ```
    #[must_use]
    pub fn json_field(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// Adds one named vector field.
    ///
    /// # Examples
    ///
    /// ```
    /// let record = storage_kit::database::data::DataRecord::new("doc-1").vector(
    ///     "embedding",
    ///     storage_kit::database::data::DenseVector::F32(vec![0.1, 0.2]),
    /// );
    /// assert_eq!(record.vectors.len(), 1);
    /// ```
    #[must_use]
    pub fn vector(mut self, key: impl Into<String>, vector: DenseVector) -> Self {
        self.vectors.insert(key.into(), vector);
        self
    }
}
