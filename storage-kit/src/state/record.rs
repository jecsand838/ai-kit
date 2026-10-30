//! Revisioned state records, validated keys, and pagination.
//!
//! [`StateKey`](crate::state::record::StateKey) separates a validated namespace from a namespace-local key.
//! Write a [`StateRecord`](crate::state::record::StateRecord) through [`crate::StateTransactionBackend`] and read
//! a [`StateRecordSnapshot`](crate::state::record::StateRecordSnapshot) to observe its per-key revision. Listing uses
//! literal prefixes and exclusive key cursors; multiple pages do not form a
//! historical snapshot. JSON payload clones support caller-owned copy-on-write
//! editing through [`Arc::make_mut`](std::sync::Arc::make_mut), without publishing edits automatically.

use std::{num::NonZeroU64, sync::Arc};

use crate::StateStoreError;

const MAX_NAMESPACE_BYTES: usize = 256;
const MAX_KEY_BYTES: usize = 1024;
const DEFAULT_LIST_LIMIT: usize = 100;
const MAX_LIST_LIMIT: usize = 1000;

/// Validated state namespace.
///
/// Namespaces are nonempty and contain at most 256 UTF-8 bytes. Leading or
/// trailing whitespace and control characters are rejected.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::StateNamespace;
/// let namespace = StateNamespace::new("workflow")?;
/// assert_eq!(namespace.as_str(), "workflow");
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StateNamespace(String);

impl StateNamespace {
    /// Validates a namespace.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidRequest`] for an empty namespace, more
    /// than 256 UTF-8 bytes, surrounding whitespace, or a control character.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateNamespace;
    /// assert!(StateNamespace::new(" padded").is_err());
    /// let namespace = StateNamespace::new("workflow")?;
    /// assert_eq!(namespace.as_str(), "workflow");
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn new(namespace: impl Into<String>) -> Result<Self, StateStoreError> {
        Self::validate_fragment("namespace", namespace.into(), MAX_NAMESPACE_BYTES).map(Self)
    }

    /// Borrows the namespace.
    ///
    /// See [`StateNamespace`] for a complete construction example.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(super) fn validate_fragment(
        label: &'static str,
        value: String,
        maximum: usize,
    ) -> Result<String, StateStoreError> {
        if value.is_empty() {
            return Err(StateStoreError::invalid_request(format!(
                "state {label} must not be empty"
            )));
        }
        if value.len() > maximum {
            return Err(StateStoreError::invalid_request(format!(
                "state {label} must contain at most {maximum} bytes"
            )));
        }
        if value.trim() != value {
            return Err(StateStoreError::invalid_request(format!(
                "state {label} must not contain leading or trailing whitespace"
            )));
        }
        if value.chars().any(char::is_control) {
            return Err(StateStoreError::invalid_request(format!(
                "state {label} must not contain control characters"
            )));
        }
        Ok(value)
    }
}

/// Validated key prefix for list requests.
///
/// Prefixes use literal, case-sensitive matching: `%`, `_`, and `*` are not
/// wildcards. A prefix has the same 1024-byte and character bounds as a key;
/// omit the prefix to select the entire namespace.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::StateKeyPrefix;
/// let prefix = StateKeyPrefix::new("run/")?;
/// assert_eq!(prefix.as_str(), "run/");
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StateKeyPrefix(String);

impl StateKeyPrefix {
    /// Validates a key prefix.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidRequest`] for an empty prefix, more than
    /// 1024 UTF-8 bytes, surrounding whitespace, or a control character.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateKeyPrefix;
    /// let prefix = StateKeyPrefix::new("run/%")?;
    /// assert_eq!(prefix.as_str(), "run/%");
    /// assert!(StateKeyPrefix::new("").is_err());
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn new(prefix: impl Into<String>) -> Result<Self, StateStoreError> {
        StateNamespace::validate_fragment("prefix", prefix.into(), MAX_KEY_BYTES).map(Self)
    }

    /// Borrows the prefix.
    ///
    /// See [`StateKeyPrefix`] for a complete construction example.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Fully qualified state-record key.
///
/// The namespace and namespace-local key are separate values. Keys are
/// nonempty, contain at most 1024 UTF-8 bytes, and reject surrounding
/// whitespace and control characters. A slash has no special storage meaning.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::StateKey;
/// let key = StateKey::new("workflow", "run/1")?;
/// assert_eq!(key.namespace().as_str(), "workflow");
/// assert_eq!(key.key(), "run/1");
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StateKey {
    namespace: StateNamespace,
    key: String,
}

impl StateKey {
    /// Validates a namespace and key fragment.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidRequest`] when the namespace exceeds
    /// 256 UTF-8 bytes or the key exceeds 1024, or either fragment is empty,
    /// has surrounding whitespace, or contains a control character.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateKey;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// assert_eq!(key.key(), "run/1");
    /// assert!(StateKey::new("workflow", "run/1\n").is_err());
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn new(
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, StateStoreError> {
        Ok(Self {
            namespace: StateNamespace::new(namespace)?,
            key: StateNamespace::validate_fragment("key", key.into(), MAX_KEY_BYTES)?,
        })
    }

    /// Returns the namespace.
    ///
    /// See [`StateKey`] for a complete construction example.
    #[must_use]
    pub fn namespace(&self) -> &StateNamespace {
        &self.namespace
    }

    /// Returns the namespace-local key.
    ///
    /// See [`StateKey`] for a complete construction example.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Positive monotonically increasing state revision.
///
/// Revisions belong to individual keys. They are not global commit ids,
/// workflow sequence numbers, or historical snapshot selectors. Puts and
/// deletes increment the key revision; tombstones retain it across recreation.
/// The positive `u64` representation does not promise every backend can
/// persist its full range.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::StateRevision;
/// let revision = StateRevision::new(3)?;
/// assert_eq!(revision.get(), 3);
/// assert!(StateRevision::new(0).is_err());
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StateRevision(NonZeroU64);

impl StateRevision {
    /// Validates a positive revision.
    ///
    /// This validates a representation; it does not assign a revision to stored data.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidRequest`] when `value` is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateRevision;
    /// assert!(StateRevision::new(0).is_err());
    /// assert_eq!(StateRevision::new(1)?, StateRevision::first());
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn new(value: u64) -> Result<Self, StateStoreError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| StateStoreError::invalid_request("state revision must be positive"))
    }

    /// Returns revision one.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateRevision;
    /// assert_eq!(StateRevision::first().get(), 1);
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn first() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Returns the revision value.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateRevision;
    /// assert_eq!(StateRevision::new(8)?.get(), 8);
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }

    #[cfg(any(feature = "redb", feature = "sqlite"))]
    pub(crate) fn next(self) -> Result<Self, StateStoreError> {
        self.get()
            .checked_add(1)
            .ok_or_else(|| StateStoreError::invalid_request("state revision overflow"))
            .and_then(Self::new)
    }

    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn from_i64(value: i64) -> Result<Self, StateStoreError> {
        u64::try_from(value)
            .map_err(|error| {
                StateStoreError::invalid_request(format!(
                    "state revision conversion failed: {error}"
                ))
            })
            .and_then(Self::new)
    }

    #[cfg(feature = "redb")]
    pub(crate) fn from_backend_u64(value: u64) -> Result<Self, StateStoreError> {
        Self::new(value)
    }

    #[cfg(feature = "sqlite")]
    pub(crate) fn to_backend_i64(self) -> Result<i64, StateStoreError> {
        i64::try_from(self.get()).map_err(|error| {
            StateStoreError::invalid_request(format!(
                "state revision exceeds backend signed 64-bit range: {error}"
            ))
        })
    }
}

/// Caller-authored state record input.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::{StateKey, StateRecord};
/// let original = StateRecord::new(StateKey::new("workflow", "run/1")?,
///     serde_json::json!({"status": "pending"}));
/// let mut edited = original.clone();
/// std::sync::Arc::make_mut(&mut edited.value)["status"] = serde_json::json!("done");
/// assert_eq!(original.value["status"], "pending");
/// assert_eq!(edited.value["status"], "done");
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateRecord {
    /// Fully qualified key.
    pub key: StateKey,
    /// Shared JSON content. Use [`Arc::make_mut`] to edit a caller-owned copy.
    ///
    /// Clones retain their original content when another copy is edited. Stores
    /// do not promise pointer identity across separately returned observations.
    pub value: Arc<serde_json::Value>,
}

impl StateRecord {
    /// Creates a record input.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::{StateKey, StateRecord};
    /// let record = StateRecord::new(StateKey::new("workflow", "run/1")?,
    ///     serde_json::json!({"status": "pending"}));
    /// assert_eq!(record.value["status"], "pending");
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn new(key: StateKey, value: serde_json::Value) -> Self {
        Self {
            key,
            value: Arc::new(value),
        }
    }
}

/// Observed record and its backend revision.
///
/// This value describes one observed record, not a historical store snapshot.
/// Constructing it does not publish a record or reserve a backend revision.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::{StateKey, StateRevision, StateRecord, StateRecordSnapshot};
/// let record = StateRecord::new(StateKey::new("workflow", "run/1")?, serde_json::json!(42));
/// let snapshot = StateRecordSnapshot::new(record, StateRevision::first());
/// assert_eq!(snapshot.revision.get(), 1);
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateRecordSnapshot {
    /// Stored record.
    pub record: StateRecord,
    /// Observed revision.
    pub revision: StateRevision,
}

impl StateRecordSnapshot {
    /// Creates an observed snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::{StateKey, StateRevision, StateRecord, StateRecordSnapshot};
    /// let record = StateRecord::new(StateKey::new("workflow", "run/1")?, serde_json::json!(42));
    /// let snapshot = StateRecordSnapshot::new(record, StateRevision::new(2)?);
    /// assert_eq!(snapshot.record.value.as_ref(), &serde_json::json!(42));
    /// assert_eq!(snapshot.revision.get(), 2);
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn new(record: StateRecord, revision: StateRevision) -> Self {
        Self { record, revision }
    }
}

/// Key-ordered namespace list request.
///
/// Results use ascending lexicographic UTF-8 key order within one exact
/// namespace. The optional prefix is literal and case-sensitive; the cursor
/// is exclusive. Each page has a consistent read view, but separate pages are
/// not a historical snapshot. The default page size is 100; accepted sizes
/// are 1 through 1000 inclusive.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::{StateKeyPrefix, StateListRequest};
/// let request = StateListRequest::new("workflow")?
///     .with_key_prefix(StateKeyPrefix::new("run/")?)
///     .with_limit(25)?
///     .start_after("run/10")?;
/// assert_eq!(request.prefix(), Some("run/"));
/// assert_eq!(request.start_after_key(), Some("run/10"));
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Debug)]
pub struct StateListRequest {
    namespace: StateNamespace,
    prefix: Option<StateKeyPrefix>,
    limit: usize,
    start_after: Option<String>,
}

impl StateListRequest {
    /// Default page size.
    pub const DEFAULT_LIMIT: usize = DEFAULT_LIST_LIMIT;
    /// Maximum page size.
    pub const MAX_LIMIT: usize = MAX_LIST_LIMIT;

    /// Creates a namespace-scoped request.
    ///
    /// The new request has no prefix or cursor and uses [`Self::DEFAULT_LIMIT`].
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidRequest`] when the namespace does not
    /// satisfy [`StateNamespace::new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateListRequest;
    /// let request = StateListRequest::new("workflow")?;
    /// assert_eq!(request.limit(), StateListRequest::DEFAULT_LIMIT);
    /// assert_eq!(request.start_after_key(), None);
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn new(namespace: impl Into<String>) -> Result<Self, StateStoreError> {
        Ok(Self {
            namespace: StateNamespace::new(namespace)?,
            prefix: None,
            limit: DEFAULT_LIST_LIMIT,
            start_after: None,
        })
    }

    /// Restricts results to a validated key prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::{StateKeyPrefix, StateListRequest};
    /// let request = StateListRequest::new("workflow")?
    ///     .with_key_prefix(StateKeyPrefix::new("run/%")?);
    /// assert_eq!(request.prefix(), Some("run/%"));
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    #[must_use]
    pub fn with_key_prefix(mut self, prefix: StateKeyPrefix) -> Self {
        self.prefix = Some(prefix);
        self
    }

    /// Replaces the page size.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidRequest`] unless `limit` is between
    /// 1 and [`Self::MAX_LIMIT`] inclusive.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateListRequest;
    /// let request = StateListRequest::new("workflow")?.with_limit(25)?;
    /// assert_eq!(request.limit(), 25);
    /// assert!(StateListRequest::new("workflow")?.with_limit(0).is_err());
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn with_limit(mut self, limit: usize) -> Result<Self, StateStoreError> {
        if !(1..=MAX_LIST_LIMIT).contains(&limit) {
            return Err(StateStoreError::invalid_request(format!(
                "state list limit must be between 1 and {MAX_LIST_LIMIT}"
            )));
        }
        self.limit = limit;
        Ok(self)
    }

    /// Sets the namespace-local exclusive cursor.
    ///
    /// Pass the previous response's `next_start_after` unchanged with the same
    /// namespace and prefix. The cursor need not name a record that still exists.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidRequest`] for an empty cursor, more than
    /// 1024 UTF-8 bytes, surrounding whitespace, or a control character.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_kit::state::record::StateListRequest;
    /// let request = StateListRequest::new("workflow")?.start_after("run/10")?;
    /// assert_eq!(request.start_after_key(), Some("run/10"));
    /// # Ok::<(), storage_kit::StateStoreError>(())
    /// ```
    pub fn start_after(mut self, cursor: impl Into<String>) -> Result<Self, StateStoreError> {
        self.start_after = Some(StateNamespace::validate_fragment(
            "start_after",
            cursor.into(),
            MAX_KEY_BYTES,
        )?);
        Ok(self)
    }

    /// Returns the namespace.
    ///
    /// See [`StateListRequest`] for a complete construction example.
    #[must_use]
    pub fn namespace(&self) -> &StateNamespace {
        &self.namespace
    }

    /// Returns the optional typed prefix.
    ///
    /// See [`StateListRequest`] for a complete construction example.
    #[must_use]
    pub fn key_prefix(&self) -> Option<&StateKeyPrefix> {
        self.prefix.as_ref()
    }

    /// Returns the optional prefix string.
    ///
    /// See [`StateListRequest`] for a complete construction example.
    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_ref().map(StateKeyPrefix::as_str)
    }

    /// Returns the page size.
    ///
    /// See [`StateListRequest`] for a complete construction example.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Returns the optional cursor.
    ///
    /// See [`StateListRequest`] for a complete construction example.
    #[must_use]
    pub fn start_after_key(&self) -> Option<&str> {
        self.start_after.as_deref()
    }
}

/// One page of observed records.
///
/// Records are present values only; tombstones are omitted. A continuation is
/// the last returned key only when another matching record exists in this
/// page's read view. An empty result has no continuation. Reusing a cursor
/// starts a new read view and does not freeze concurrently changing records.
///
/// # Examples
///
/// ```
/// use storage_kit::state::record::{StateKey, StateRevision, StateRecord, StateRecordSnapshot, StateListRequest, StateListResponse};
/// let record = StateRecord::new(StateKey::new("workflow", "run/1")?, serde_json::json!(42));
/// let page = StateListResponse {
///     records: vec![StateRecordSnapshot::new(record, StateRevision::first())],
///     next_start_after: Some("run/1".to_owned()),
/// };
/// let next = StateListRequest::new("workflow")?
///     .start_after(page.next_start_after.as_deref().unwrap())?;
/// assert_eq!(next.start_after_key(), Some("run/1"));
/// # Ok::<(), storage_kit::StateStoreError>(())
/// ```
#[derive(Clone, Debug, Default)]
pub struct StateListResponse {
    /// Returned records.
    pub records: Vec<StateRecordSnapshot>,
    /// Cursor for a subsequent page.
    pub next_start_after: Option<String>,
}
