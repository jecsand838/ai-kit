use serde::{Deserialize, Serialize};

use crate::core::ApplyDefaultLocator;
use crate::{DenseVector, StorageLocator};

use super::{ScopedQuery, VectorQueryOptions};
/// Request to perform nearest-neighbor vector search.
///
/// # Examples
///
/// ```
/// use storage_kit::database::StorageLocator;
/// use storage_kit::database::query::{DenseVector, NearestRequest};
/// let mut request = NearestRequest::new(
///     StorageLocator::new("documents"), DenseVector::F32(vec![0.1, 0.2, 0.3]),
/// );
/// request.query.options.vector_field = Some("embedding".into());
/// assert_eq!(request.query.options.vector_field.as_deref(), Some("embedding"));
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
    /// See [`NearestRequest`] for construction and field selection.
    #[must_use]
    pub fn new(locator: StorageLocator, vector: DenseVector) -> Self {
        Self {
            query: ScopedQuery::vector(locator),
            vector,
        }
    }
}
