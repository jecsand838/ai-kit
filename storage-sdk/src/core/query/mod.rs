//! Retained portable query requests and responses.

mod common;
mod sampling;
mod text;
mod vector;

use std::{fmt, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::DataRecord;

use super::ApplyDefaultLocator;

pub use common::{
    Projection, QueryScope, ScopedQuery, TextQueryOptions, VectorQueryOptions, VectorSearchParams,
};
pub use sampling::{RandomSampleRequest, ScrollRequest};
pub use text::TextSearchRequest;
pub use vector::{NearestRequest, SearchByIdRequest};

/// Opaque continuation token.
///
/// A token belongs to the backend and operation that returned it. Return it
/// unchanged to the same query or scan with the same applicable scope. Do not
/// interpret it as a universally numeric offset or use it to infer a snapshot.
/// Constructing or deserializing this value does not validate backend acceptance.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::StorageLocator;
/// use storage_sdk::database::data::{PageToken, ScanRequest};
/// let token = PageToken::new("opaque-cursor");
/// let request = ScanRequest::new(StorageLocator::new("documents")).page_token(token.clone());
/// assert_eq!(request.page_token, Some(token));
/// ```
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PageToken(String);

impl PageToken {
    /// Creates a token from the returned continuation value.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::data::PageToken;
    /// let token = PageToken::new("opaque-cursor");
    /// assert_eq!(token.as_str(), "opaque-cursor");
    /// ```
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the continuation value.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::data::PageToken;
    /// let token = PageToken::new("opaque-cursor");
    /// assert_eq!(token.as_str(), "opaque-cursor");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the token.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::data::PageToken;
    /// let token = PageToken::new("opaque-cursor");
    /// let saved = token.into_string();
    /// assert_eq!(PageToken::new(saved).as_str(), "opaque-cursor");
    /// ```
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }

    pub(crate) fn effective_offset(
        page_token: Option<&str>,
        offset: Option<usize>,
    ) -> Result<Option<usize>, crate::StorageError> {
        match page_token {
            Some(token) => token.parse::<usize>().map(Some).map_err(|error| {
                crate::StorageError::InvalidConfig(format!(
                    "invalid numeric page token `{token}`: {error}"
                ))
            }),
            None => Ok(offset),
        }
    }

    #[must_use]
    pub(crate) fn next_offset(
        limit: Option<usize>,
        effective_offset: Option<usize>,
        count: usize,
    ) -> Option<Self> {
        let limit = limit?;
        if count < limit {
            None
        } else {
            Some((effective_offset.unwrap_or(0) + count).to_string().into())
        }
    }
}

impl AsRef<str> for PageToken {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for PageToken {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for PageToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for PageToken {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for PageToken {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<&String> for PageToken {
    fn from(value: &String) -> Self {
        Self(value.clone())
    }
}

impl From<PageToken> for String {
    fn from(value: PageToken) -> Self {
        value.into_string()
    }
}

/// Meaning of a returned score.
///
/// Scores retain their reported semantics; distance, similarity, and text
/// relevance are not interchangeable scales. [`ScoredRecord::raw_score`] may
/// be absent even when a record is returned.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::data::DataRecord;
/// use storage_sdk::database::query::{ScoreKind, ScoredRecord};
/// let hit = ScoredRecord { record: DataRecord::default(), raw_score: Some(0.25),
///     score_kind: ScoreKind::Distance };
/// assert!(matches!(hit.score_kind, ScoreKind::Distance));
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ScoreKind {
    /// Larger is more similar.
    Similarity,
    /// Smaller is closer.
    Distance,
    /// Text relevance.
    Relevance,
    /// The implementation cannot expose stronger portable semantics.
    Unknown,
}

/// One ranked record.
///
/// A missing raw score remains `None`; consumers must not substitute zero
/// or infer a score from fields in the returned record.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::data::DataRecord;
/// use storage_sdk::database::query::{ScoreKind, ScoredRecord};
/// let hit = ScoredRecord { record: DataRecord::default(), raw_score: None,
///     score_kind: ScoreKind::Unknown };
/// assert!(hit.raw_score.is_none());
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScoredRecord {
    /// Returned record.
    pub record: DataRecord,
    /// Implementation-reported score.
    pub raw_score: Option<f64>,
    /// Score semantics.
    pub score_kind: ScoreKind,
}

/// Ranked hits and optional continuation.
///
/// Some operations produce unranked records with no raw scores. Return a
/// continuation unchanged to the same applicable operation; it does not promise
/// a stable historical view across separate requests.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::data::{DataRecord, PageToken};
/// use storage_sdk::database::query::{ScoreKind, ScoredRecord, QueryResponse};
/// let response = QueryResponse {
///     hits: vec![ScoredRecord { record: DataRecord::default(), raw_score: None,
///         score_kind: ScoreKind::Unknown }],
///     next_page: Some(PageToken::new("opaque-cursor")),
/// };
/// assert_eq!(response.hits.len(), 1);
/// assert_eq!(response.next_page.as_deref(), Some("opaque-cursor"));
/// ```
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct QueryResponse {
    /// Ranked or unranked records.
    pub hits: Vec<ScoredRecord>,
    /// Continuation token.
    pub next_page: Option<PageToken>,
}

impl QueryResponse {
    pub(crate) fn from_hits(hits: Vec<ScoredRecord>, next_page: Option<PageToken>) -> Self {
        Self { hits, next_page }
    }
}

/// Retained cross-backend query operations.
///
/// # Examples
///
/// ```
/// use storage_sdk::database::StorageLocator;
/// use storage_sdk::database::query::{ScopedQuery, QueryRequest, TextSearchRequest};
/// let request = QueryRequest::Text(TextSearchRequest {
///     query: ScopedQuery::text(StorageLocator::new("documents")).text_field("body"),
///     text: "rust".to_owned(),
/// });
/// assert!(matches!(request, QueryRequest::Text(_)));
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum QueryRequest {
    /// Nearest-neighbor search using an explicit dense vector.
    Nearest(NearestRequest),
    /// Search using a stored vector selected by record id.
    SearchById(SearchByIdRequest),
    /// Full-text search.
    Text(TextSearchRequest),
    /// Ordered or cursor-based scan through the query surface.
    Scroll(ScrollRequest),
    /// Random record sample.
    RandomSample(RandomSampleRequest),
}

impl ApplyDefaultLocator for QueryRequest {
    fn apply_default_locator(self, default: &crate::StorageLocator) -> Self {
        match self {
            Self::Nearest(request) => Self::Nearest(request.apply_default_locator(default)),
            Self::SearchById(request) => Self::SearchById(request.apply_default_locator(default)),
            Self::Text(request) => Self::Text(request.apply_default_locator(default)),
            Self::Scroll(request) => Self::Scroll(request.apply_default_locator(default)),
            Self::RandomSample(request) => {
                Self::RandomSample(request.apply_default_locator(default))
            }
        }
    }
}
