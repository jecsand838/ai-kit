use serde::{Deserialize, Serialize};

use crate::core::{ApplyDefaultLocator, HasLocator};
use crate::{DistanceMetric, FilterExpr, StorageLocator};

/// Fields returned for a record.
///
/// # Examples
///
/// ```
/// use storage_kit::database::query::Projection;
/// let projection = Projection::Columns(vec!["title".to_owned()]);
/// assert!(matches!(projection, Projection::Columns(columns) if columns == ["title"]));
/// ```
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum Projection {
    /// Returns every portable field.
    #[default]
    All,
    /// Returns a fixed field subset.
    Columns(Vec<String>),
}

/// Shared collection and result scope.
///
/// The scope is shared by vector and text requests. Construction only builds
/// a request; the selected backend validates fields and supported combinations
/// when executing it.
///
/// # Examples
///
/// ```
/// use storage_kit::database::StorageLocator;
/// use storage_kit::database::data::FilterExpr;
/// use storage_kit::database::query::{Projection, QueryScope};
/// let scope = QueryScope::new(StorageLocator::new("documents"))
///     .filter(FilterExpr::eq("published", true))
///     .projection(Projection::Columns(vec!["title".to_owned()]));
/// assert!(scope.filter.is_some());
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct QueryScope {
    /// Logical collection.
    pub locator: StorageLocator,
    /// Optional structured filter.
    pub filter: Option<FilterExpr>,
    /// Returned fields.
    pub projection: Projection,
}

impl QueryScope {
    /// Creates an unfiltered all-field scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::{Projection, QueryScope};
    /// let scope = QueryScope::new(StorageLocator::new("documents"));
    /// assert_eq!(scope.projection, Projection::All);
    /// assert!(scope.filter.is_none());
    /// ```
    #[must_use]
    pub fn new(locator: StorageLocator) -> Self {
        Self {
            locator,
            filter: None,
            projection: Projection::All,
        }
    }

    /// Applies a filter.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::data::FilterExpr;
    /// use storage_kit::database::query::QueryScope;
    /// let scope = QueryScope::new(StorageLocator::new("documents"))
    ///     .filter(FilterExpr::eq("published", true));
    /// assert!(scope.filter.is_some());
    /// ```
    #[must_use]
    pub fn filter(mut self, filter: FilterExpr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Replaces returned fields.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::{Projection, QueryScope};
    /// let scope = QueryScope::new(StorageLocator::new("documents"))
    ///     .projection(Projection::Columns(vec!["title".to_owned()]));
    /// assert_eq!(scope.projection, Projection::Columns(vec!["title".to_owned()]));
    /// ```
    #[must_use]
    pub fn projection(mut self, projection: Projection) -> Self {
        self.projection = projection;
        self
    }
}

impl From<StorageLocator> for QueryScope {
    fn from(locator: StorageLocator) -> Self {
        Self::new(locator)
    }
}

impl HasLocator for QueryScope {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Shared scope plus query-family options.
///
/// Use [`Self::new`] with explicit options or the `vector` and `text`
/// constructors, then embed the result in a concrete query request. This value
/// does not execute work or contain a database connection.
///
/// # Examples
///
/// ```
/// use storage_kit::database::StorageLocator;
/// use storage_kit::database::query::ScopedQuery;
/// let query = ScopedQuery::text(StorageLocator::new("documents"))
///     .text_field("body").limit(5);
/// assert_eq!(query.options.text_field.as_deref(), Some("body"));
/// assert_eq!(query.options.limit, Some(5));
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScopedQuery<Options> {
    /// Collection and result scope.
    pub scope: QueryScope,
    /// Query-family options.
    pub options: Options,
}

impl<Options> ScopedQuery<Options> {
    /// Creates a scoped query.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::{ScopedQuery, TextQueryOptions};
    /// let query = ScopedQuery::new(StorageLocator::new("documents"), TextQueryOptions::default());
    /// assert_eq!(query.scope.locator.collection, "documents");
    /// assert!(query.options.text_field.is_none());
    /// ```
    #[must_use]
    pub fn new(scope: impl Into<QueryScope>, options: Options) -> Self {
        Self {
            scope: scope.into(),
            options,
        }
    }

    pub(crate) fn collection(&self) -> &str {
        &self.scope.locator.collection
    }

    /// Replaces the shared scope.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::{QueryScope, ScopedQuery};
    /// let scope = QueryScope::new(StorageLocator::new("published"));
    /// let query = ScopedQuery::text(StorageLocator::new("drafts")).with_scope(scope);
    /// assert_eq!(query.scope.locator.collection, "published");
    /// ```
    #[must_use]
    pub fn with_scope(mut self, scope: QueryScope) -> Self {
        self.scope = scope;
        self
    }

    /// Applies a filter.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::data::FilterExpr;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::text(StorageLocator::new("documents"))
    ///     .filter(FilterExpr::eq("published", true));
    /// assert!(query.scope.filter.is_some());
    /// ```
    #[must_use]
    pub fn filter(mut self, filter: FilterExpr) -> Self {
        self.scope = self.scope.filter(filter);
        self
    }

    /// Replaces returned fields.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::{Projection, ScopedQuery};
    /// let query = ScopedQuery::vector(StorageLocator::new("documents"))
    ///     .projection(Projection::Columns(vec!["title".to_owned()]));
    /// assert_eq!(query.scope.projection, Projection::Columns(vec!["title".to_owned()]));
    /// ```
    #[must_use]
    pub fn projection(mut self, projection: Projection) -> Self {
        self.scope = self.scope.projection(projection);
        self
    }

    /// Mutates family options.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::text(StorageLocator::new("documents"))
    ///     .map_options(|options| { options.set_field("body"); options.set_limit(5); });
    /// assert_eq!(query.options.limit, Some(5));
    /// ```
    #[must_use]
    pub fn map_options(mut self, update: impl FnOnce(&mut Options)) -> Self {
        update(&mut self.options);
        self
    }
}

impl<Options> ApplyDefaultLocator for ScopedQuery<Options> {
    fn apply_default_locator(mut self, default: &StorageLocator) -> Self {
        self.scope = self.scope.apply_default_locator(default);
        self
    }
}

/// Vector search shaping and portable tuning.
///
/// Defaults request 10 hits and no offset or explicit distance/search breadth.
/// Tuning support depends on the backend and query;
/// a capability bit does not promise acceptance of every combination.
///
/// # Examples
///
/// ```
/// use storage_kit::database::query::VectorSearchParams;
/// let params = VectorSearchParams { limit: 5, ..Default::default() };
/// assert_eq!(params.limit, 5);
/// assert_eq!(params.offset, None);
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct VectorSearchParams {
    /// Maximum hits.
    pub limit: usize,
    /// Hit offset.
    pub offset: Option<usize>,
    /// Requested distance metric.
    pub distance: Option<DistanceMetric>,
    /// HNSW search breadth.
    pub ef: Option<usize>,
}

impl Default for VectorSearchParams {
    fn default() -> Self {
        Self {
            limit: 10,
            offset: None,
            distance: None,
            ef: None,
        }
    }
}

/// Vector field and result options.
///
/// An absent field selects the configured backend default where supported.
/// The default parameters come from [`VectorSearchParams::default`].
///
/// # Examples
///
/// ```
/// use storage_kit::database::query::VectorQueryOptions;
/// let mut options = VectorQueryOptions::default();
/// options.set_field("embedding");
/// options.set_limit(5);
/// assert_eq!(options.vector_field.as_deref(), Some("embedding"));
/// assert_eq!(options.params.limit, 5);
/// ```
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct VectorQueryOptions {
    /// Vector field.
    pub vector_field: Option<String>,
    /// Search shaping.
    pub params: VectorSearchParams,
}

impl VectorQueryOptions {
    /// Selects a vector field.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::query::VectorQueryOptions;
    /// let mut options = VectorQueryOptions::default();
    /// options.set_field("embedding");
    /// assert_eq!(options.vector_field.as_deref(), Some("embedding"));
    /// ```
    pub fn set_field(&mut self, field: impl Into<String>) {
        self.vector_field = Some(field.into());
    }

    /// Sets the maximum hits.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::query::VectorQueryOptions;
    /// let mut options = VectorQueryOptions::default();
    /// options.set_limit(5);
    /// assert_eq!(options.params.limit, 5);
    /// ```
    pub fn set_limit(&mut self, limit: usize) {
        self.params.limit = limit;
    }

    /// Replaces search shaping.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::query::{VectorQueryOptions, VectorSearchParams};
    /// let mut options = VectorQueryOptions::default();
    /// options.set_params(VectorSearchParams { limit: 5, ..Default::default() });
    /// assert_eq!(options.params.limit, 5);
    /// ```
    pub fn set_params(&mut self, params: VectorSearchParams) {
        self.params = params;
    }
}

impl ScopedQuery<VectorQueryOptions> {
    /// Creates a vector query.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::vector(StorageLocator::new("documents"));
    /// assert_eq!(query.options.params.limit, 10);
    /// assert!(query.options.vector_field.is_none());
    /// ```
    #[must_use]
    pub fn vector(locator: StorageLocator) -> Self {
        Self::new(locator, VectorQueryOptions::default())
    }

    /// Selects a vector field.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::vector(StorageLocator::new("documents")).vector_field("embedding");
    /// assert_eq!(query.options.vector_field.as_deref(), Some("embedding"));
    /// ```
    #[must_use]
    pub fn vector_field(self, field: impl Into<String>) -> Self {
        self.map_options(|options| options.set_field(field))
    }

    /// Sets the maximum hits.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::vector(StorageLocator::new("documents")).limit(5);
    /// assert_eq!(query.options.params.limit, 5);
    /// ```
    #[must_use]
    pub fn limit(self, limit: usize) -> Self {
        self.map_options(|options| options.set_limit(limit))
    }

    /// Replaces search shaping.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::{ScopedQuery, VectorSearchParams};
    /// let query = ScopedQuery::vector(StorageLocator::new("documents"))
    ///     .params(VectorSearchParams { limit: 5, offset: Some(2), ..Default::default() });
    /// assert_eq!(query.options.params.offset, Some(2));
    /// ```
    #[must_use]
    pub fn params(self, params: VectorSearchParams) -> Self {
        self.map_options(|options| options.set_params(params))
    }
}

/// Text field and result options.
///
/// The default leaves field, limit, and offset unset. Select a text field on
/// the request when the backend requires one; configuration does not provide
/// a universal text-field fallback.
///
/// # Examples
///
/// ```
/// use storage_kit::database::query::TextQueryOptions;
/// let mut options = TextQueryOptions::default();
/// options.set_field("body");
/// options.set_limit(5);
/// assert_eq!(options.text_field.as_deref(), Some("body"));
/// assert_eq!(options.limit, Some(5));
/// ```
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct TextQueryOptions {
    /// Text field.
    pub text_field: Option<String>,
    /// Maximum hits.
    pub limit: Option<usize>,
    /// Hit offset.
    pub offset: Option<usize>,
}

impl TextQueryOptions {
    /// Selects a text field.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::query::TextQueryOptions;
    /// let mut options = TextQueryOptions::default();
    /// options.set_field("body");
    /// assert_eq!(options.text_field.as_deref(), Some("body"));
    /// ```
    pub fn set_field(&mut self, field: impl Into<String>) {
        self.text_field = Some(field.into());
    }

    /// Sets maximum hits.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::query::TextQueryOptions;
    /// let mut options = TextQueryOptions::default();
    /// options.set_limit(5);
    /// assert_eq!(options.limit, Some(5));
    /// ```
    pub fn set_limit(&mut self, limit: usize) {
        self.limit = Some(limit);
    }

    /// Sets hit offset.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::query::TextQueryOptions;
    /// let mut options = TextQueryOptions::default();
    /// options.set_offset(10);
    /// assert_eq!(options.offset, Some(10));
    /// ```
    pub fn set_offset(&mut self, offset: usize) {
        self.offset = Some(offset);
    }
}

impl ScopedQuery<TextQueryOptions> {
    /// Creates a text query.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::text(StorageLocator::new("documents"));
    /// assert!(query.options.text_field.is_none());
    /// assert!(query.options.limit.is_none());
    /// ```
    #[must_use]
    pub fn text(locator: StorageLocator) -> Self {
        Self::new(locator, TextQueryOptions::default())
    }

    /// Selects a text field.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::text(StorageLocator::new("documents")).text_field("body");
    /// assert_eq!(query.options.text_field.as_deref(), Some("body"));
    /// ```
    #[must_use]
    pub fn text_field(self, field: impl Into<String>) -> Self {
        self.map_options(|options| options.set_field(field))
    }

    /// Sets maximum hits.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::text(StorageLocator::new("documents")).limit(5);
    /// assert_eq!(query.options.limit, Some(5));
    /// ```
    #[must_use]
    pub fn limit(self, limit: usize) -> Self {
        self.map_options(|options| options.set_limit(limit))
    }

    /// Sets hit offset.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::query::ScopedQuery;
    /// let query = ScopedQuery::text(StorageLocator::new("documents")).offset(10);
    /// assert_eq!(query.options.offset, Some(10));
    /// ```
    #[must_use]
    pub fn offset(self, offset: usize) -> Self {
        self.map_options(|options| options.set_offset(offset))
    }
}
