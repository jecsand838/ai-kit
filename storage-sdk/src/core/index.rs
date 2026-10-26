use serde::{Deserialize, Serialize};

use crate::{DistanceMetric, StorageError, StorageLocator};

use super::{ApplyDefaultLocator, HasLocator};

/// Describes an index exposed by a backend.
///
/// # Examples
///
/// ```
/// let info = storage_sdk::database::index::IndexInfo {
///     name: Some("idx_documents_vector".into()),
///     columns: vec!["vector".into()],
///     ready: Some(true),
///     ..Default::default()
/// };
/// assert_eq!(info.ready, Some(true));
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexInfo {
    /// Index name, when available.
    pub name: Option<String>,
    /// Indexed columns or fields covered by the index.
    pub columns: Vec<String>,
    /// Whether the index is ready for use.
    pub ready: Option<bool>,
}

/// Portable index kind requested from a backend.
///
/// # Examples
///
/// ```
/// let kind = storage_sdk::database::index::IndexKind::Vector {
///     dimension: 384,
///     distance: Some(storage_sdk::database::query::DistanceMetric::Cosine),
/// };
///
/// assert!(matches!(
///     kind,
///     storage_sdk::database::index::IndexKind::Vector { .. }
/// ));
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexKind {
    /// Requests a scalar filter index using the backend's native scalar index.
    Scalar,
    /// Requests a full-text index using the portable `simple` analyzer.
    FullText,
    /// Requests a vector index while leaving the native algorithm private.
    Vector {
        /// Required vector dimensionality.
        dimension: usize,
        /// Distance metric to preserve across indexing and search.
        distance: Option<DistanceMetric>,
    },
}

/// Request to create an index.
///
/// Columns must be nonempty names; vector indexes require exactly one column
/// and a positive dimension. A configured wait timeout must be positive.
/// Request construction does not establish support for a kind or combination;
/// [`crate::DatabaseBackend::index`] validates and executes it.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::index::CreateIndexRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     columns: vec!["title".into()],
///     kind: storage_sdk::database::index::IndexKind::FullText,
///     wait_timeout_ms: Some(5_000),
/// };
/// assert!(matches!(
///     request.kind,
///     storage_sdk::database::index::IndexKind::FullText
/// ));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateIndexRequest {
    /// Collection that should receive the new index.
    pub locator: StorageLocator,
    /// Columns or fields to index.
    pub columns: Vec<String>,
    /// Requested index kind.
    pub kind: IndexKind,
    /// Optional positive index-readiness wait timeout in milliseconds.
    ///
    /// Without a requested readiness wait, creation completion does not by
    /// itself establish that the index is ready for queries.
    pub wait_timeout_ms: Option<u64>,
}

impl HasLocator for CreateIndexRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

impl CreateIndexRequest {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if self.columns.is_empty() {
            return Err(StorageError::InvalidRequest(
                "index creation requires at least one column".into(),
            ));
        }
        if self.columns.iter().any(|column| column.trim().is_empty()) {
            return Err(StorageError::InvalidRequest(
                "index columns must not be empty".into(),
            ));
        }
        if let IndexKind::Vector { dimension, .. } = &self.kind {
            if self.columns.len() != 1 {
                return Err(StorageError::InvalidRequest(
                    "vector index creation requires exactly one column".into(),
                ));
            }
            if *dimension == 0 {
                return Err(StorageError::InvalidRequest(
                    "vector index dimension must be positive".into(),
                ));
            }
        }
        if self.wait_timeout_ms == Some(0) {
            return Err(StorageError::InvalidRequest(
                "index wait timeout must be positive when configured".into(),
            ));
        }
        Ok(())
    }
}

/// Request to wait until named indexes are ready.
///
/// At least one nonempty name and a positive timeout are required. An already
/// ready index can complete immediately; an absent index remains a readiness
/// failure. The requested wait bound applies in addition to the enclosing
/// configured operation deadline.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::index::WaitForIndexRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     index_names: vec!["idx_documents_vector".into()],
///     timeout_ms: 5_000,
/// };
/// assert_eq!(request.index_names.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WaitForIndexRequest {
    /// Collection whose indexes should be observed.
    pub locator: StorageLocator,
    /// Index names to wait for.
    pub index_names: Vec<String>,
    /// Maximum wait time in milliseconds before the request should fail.
    pub timeout_ms: u64,
}

impl HasLocator for WaitForIndexRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

impl WaitForIndexRequest {
    fn validate(&self) -> Result<(), StorageError> {
        if self.index_names.is_empty() {
            return Err(StorageError::InvalidRequest(
                "wait-for-index requires at least one index name".into(),
            ));
        }
        if self.index_names.iter().any(|name| name.trim().is_empty()) {
            return Err(StorageError::InvalidRequest(
                "index names must not be empty".into(),
            ));
        }
        if self.timeout_ms == 0 {
            return Err(StorageError::InvalidRequest(
                "wait-for-index timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Portable index-management request.
///
/// Index creation and readiness waiting are separate operations. A completed
/// creation does not imply every index is ready for use unless readiness was
/// requested and established. Unsupported index kinds fail explicitly.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::StorageLocator;
/// use storage_sdk::database::index::IndexRequest;
/// let request = IndexRequest::List { locator: StorageLocator::new("documents") };
/// assert!(matches!(request, IndexRequest::List { .. }));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexRequest {
    /// Creates an index with the requested kind and tuning options.
    Create(CreateIndexRequest),
    /// Lists indexes that already exist for the target collection.
    List {
        /// Collection whose indexes should be listed.
        locator: StorageLocator,
    },
    /// Drops a named index from the target collection.
    Drop {
        /// Collection whose index should be dropped.
        locator: StorageLocator,
        /// Index name that should be dropped.
        name: String,
    },
    /// Waits for named indexes to become ready.
    WaitFor(WaitForIndexRequest),
}

impl IndexRequest {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        match self {
            Self::Create(request) => request.validate(),
            Self::Drop { name, .. } if name.trim().is_empty() => Err(StorageError::InvalidRequest(
                "index name must not be empty".into(),
            )),
            Self::WaitFor(request) => request.validate(),
            Self::List { .. } | Self::Drop { .. } => Ok(()),
        }
    }
}

impl ApplyDefaultLocator for IndexRequest {
    fn apply_default_locator(self, default: &StorageLocator) -> Self {
        match self {
            Self::Create(req) => Self::Create(req.apply_default_locator(default)),
            Self::List { locator } => Self::List {
                locator: locator.with_defaults_from(default),
            },
            Self::Drop { locator, name } => Self::Drop {
                locator: locator.with_defaults_from(default),
                name,
            },
            Self::WaitFor(req) => Self::WaitFor(req.apply_default_locator(default)),
        }
    }
}

/// Portable index-management response.
///
/// # Examples
///
/// ```
/// let response = storage_sdk::database::index::IndexResponse::Indexes(Vec::new());
/// assert!(matches!(response, storage_sdk::database::index::IndexResponse::Indexes(_)));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexResponse {
    /// A mutation completed without a backend-native acknowledgement envelope.
    Completed,
    /// Returns index metadata for the target collection.
    Indexes(Vec<IndexInfo>),
}
