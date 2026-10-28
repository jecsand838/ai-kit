use serde::{Deserialize, Serialize};

use crate::{FilterExpr, StorageLocator, core::HasLocator};

use super::{PageToken, Projection};

/// Request to iterate records through a query-scoped scroll API.
///
/// # Examples
///
/// ```
/// let request = storage_kit::database::query::ScrollRequest {
///     locator: storage_kit::database::StorageLocator::new("documents"),
///     filter: None,
///     projection: storage_kit::database::query::Projection::All,
///     limit: Some(50),
///     offset: None,
///     page_token: None,
/// };
/// assert_eq!(request.limit, Some(50));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScrollRequest {
    /// Collection that should be scanned through the scroll API.
    pub locator: StorageLocator,
    /// Optional predicate used to restrict the scrolled rows.
    pub filter: Option<FilterExpr>,
    /// Projection to apply to returned records.
    pub projection: Projection,
    /// Maximum number of rows to return in one page.
    pub limit: Option<usize>,
    /// Number of rows to skip before producing the current page.
    pub offset: Option<usize>,
    /// Continuation token returned by a previous scroll page.
    pub page_token: Option<PageToken>,
}

impl HasLocator for ScrollRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}
