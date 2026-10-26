use serde::{Deserialize, Serialize};

use crate::StorageLocator;
use crate::core::ApplyDefaultLocator;

use super::{ScopedQuery, TextQueryOptions};
/// Request to run a full-text search.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::query::TextSearchRequest {
///     query: storage_sdk::database::query::ScopedQuery::text(storage_sdk::database::StorageLocator::new("documents"))
///         .text_field("body")
///         .limit(10),
///     text: "rust storage".into(),
/// };
/// assert_eq!(request.text, "rust storage");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TextSearchRequest {
    /// Shared query scope and text options.
    pub query: ScopedQuery<TextQueryOptions>,
    /// Full-text query string sent to the backend.
    pub text: String,
}

impl ApplyDefaultLocator for TextSearchRequest {
    fn apply_default_locator(mut self, default: &StorageLocator) -> Self {
        self.query = self.query.apply_default_locator(default);
        self
    }
}

impl TextSearchRequest {
    /// Creates a text search request with all fields projected.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::query::TextSearchRequest::new(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     "rust storage",
    /// );
    /// assert_eq!(request.text, "rust storage");
    /// ```
    #[must_use]
    pub fn new(locator: StorageLocator, query: impl Into<String>) -> Self {
        Self {
            query: ScopedQuery::text(locator),
            text: query.into(),
        }
    }
}
