use serde::{Deserialize, Serialize};

use crate::core::ApplyDefaultLocator;
use crate::{DenseVector, RecordId, StorageLocator};

use super::{ScopedQuery, VectorQueryOptions};
/// Request to perform nearest-neighbor vector search.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::query::NearestRequest {
///     query: storage_sdk::database::query::ScopedQuery::new(
///         storage_sdk::database::StorageLocator::new("documents"),
///         storage_sdk::database::query::VectorQueryOptions { vector_field: Some("vector".into()), ..Default::default() },
///     ),
///     vector: storage_sdk::database::data::DenseVector::F32(vec![0.1, 0.2, 0.3]),
/// };
/// assert_eq!(request.query.options.vector_field.as_deref(), Some("vector"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NearestRequest {
    /// Shared query scope and vector options.
    pub query: ScopedQuery<VectorQueryOptions>,
    /// Dense vector used as the search seed.
    pub vector: DenseVector,
}

impl ApplyDefaultLocator for NearestRequest {
    fn apply_default_locator(mut self, default: &StorageLocator) -> Self {
        self.query = self.query.apply_default_locator(default);
        self
    }
}

impl NearestRequest {
    /// Creates a nearest-neighbor request with default vector parameters.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::query::NearestRequest::new(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     storage_sdk::database::data::DenseVector::F32(vec![0.1, 0.2]),
    /// );
    /// assert_eq!(
    ///     request.query.options.params.limit,
    ///     storage_sdk::database::query::VectorSearchParams::default().limit
    /// );
    /// ```
    #[must_use]
    pub fn new(locator: StorageLocator, vector: DenseVector) -> Self {
        Self {
            query: ScopedQuery::vector(locator),
            vector,
        }
    }
}

/// Request to search using the vector stored for an existing record id.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::query::SearchByIdRequest {
///     query: storage_sdk::database::query::ScopedQuery::new(
///         storage_sdk::database::StorageLocator::new("documents"),
///         storage_sdk::database::query::VectorQueryOptions { vector_field: Some("vector".into()), ..Default::default() },
///     ),
///     id: storage_sdk::database::data::RecordId::String("doc-1".into()),
/// };
/// assert!(matches!(request.id, storage_sdk::database::data::RecordId::String(_)));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchByIdRequest {
    /// Shared query scope and vector options.
    pub query: ScopedQuery<VectorQueryOptions>,
    /// Record id whose stored vector should be used as the query seed.
    pub id: RecordId,
}

impl ApplyDefaultLocator for SearchByIdRequest {
    fn apply_default_locator(mut self, default: &StorageLocator) -> Self {
        self.query = self.query.apply_default_locator(default);
        self
    }
}
