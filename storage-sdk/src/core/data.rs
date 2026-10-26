use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{DataRecord, FilterExpr, RecordId, StorageError, StorageLocator};

use super::{
    ApplyDefaultLocator, HasLocator,
    query::{PageToken, Projection},
};

/// Behavior to apply when writing rows to an existing collection.
///
/// # Examples
///
/// ```
/// let mode = storage_sdk::database::data::WriteMode::Append;
/// assert!(matches!(mode, storage_sdk::database::data::WriteMode::Append));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WriteMode {
    /// Append new rows to the existing collection.
    Append,
    /// Replace all collection records, including clearing an empty replacement.
    ///
    /// Preserves the collection schema and indexes. Catalog overwrite instead
    /// recreates the collection. Ordinary Database writes do not provide
    /// rollback across multiple native requests; a failure can leave an
    /// uncertain or partial outcome.
    Overwrite,
}

/// Request to insert records.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::InsertRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     records: vec![storage_sdk::database::data::DataRecord::default()],
///     mode: storage_sdk::database::data::WriteMode::Append,
/// };
/// assert_eq!(request.records.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InsertRequest {
    /// Collection that should receive the inserted records.
    pub locator: StorageLocator,
    /// Records to write.
    pub records: Vec<DataRecord>,
    /// Write behavior to apply.
    pub mode: WriteMode,
}

impl HasLocator for InsertRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

impl InsertRequest {
    /// Creates an append insert request.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::InsertRequest::append(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     vec![storage_sdk::database::data::DataRecord::without_id()],
    /// );
    /// assert!(matches!(request.mode, storage_sdk::database::data::WriteMode::Append));
    /// ```
    #[must_use]
    pub fn append(locator: StorageLocator, records: Vec<DataRecord>) -> Self {
        Self {
            locator,
            records,
            mode: WriteMode::Append,
        }
    }

    /// Creates an overwrite insert request.
    ///
    /// The complete supplied set replaces existing records; an empty set clears
    /// them while preserving schema and indexes. This constructor only builds
    /// the request. Execution can reject unsupported combinations before
    /// mutation, and native execution failures may leave an uncertain outcome.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::InsertRequest::overwrite(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     vec![storage_sdk::database::data::DataRecord::without_id()],
    /// );
    /// assert!(matches!(request.mode, storage_sdk::database::data::WriteMode::Overwrite));
    /// ```
    #[must_use]
    pub fn overwrite(locator: StorageLocator, records: Vec<DataRecord>) -> Self {
        Self {
            locator,
            records,
            mode: WriteMode::Overwrite,
        }
    }
}

/// Request to fetch records by logical id.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::LookupRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     ids: vec![storage_sdk::database::data::RecordId::String("doc-1".into())],
///     projection: storage_sdk::database::query::Projection::All,
/// };
/// assert_eq!(request.ids.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LookupRequest {
    /// Collection that should be queried for the requested record ids.
    pub locator: StorageLocator,
    /// Logical ids to fetch.
    pub ids: Vec<RecordId>,
    /// Projection to apply to returned records.
    pub projection: Projection,
}

impl HasLocator for LookupRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

impl LookupRequest {
    /// Creates a lookup request with all fields projected.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::LookupRequest::new(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     ["doc-1", "doc-2"],
    /// );
    /// assert_eq!(request.ids.len(), 2);
    /// ```
    #[must_use]
    pub fn new<I>(locator: StorageLocator, ids: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<RecordId>,
    {
        Self {
            locator,
            ids: ids.into_iter().map(Into::into).collect(),
            projection: Projection::All,
        }
    }

    /// Replaces the projection.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::LookupRequest::new(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     ["doc-1"],
    /// )
    /// .projection(storage_sdk::database::query::Projection::Columns(vec!["title".to_owned()]));
    /// assert!(matches!(request.projection, storage_sdk::database::query::Projection::Columns(_)));
    /// ```
    #[must_use]
    pub fn projection(mut self, projection: Projection) -> Self {
        self.projection = projection;
        self
    }
}

/// Request to scan a collection with optional filtering and pagination.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::ScanRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     filter: None,
///     projection: storage_sdk::database::query::Projection::All,
///     limit: Some(10),
///     offset: None,
///     page_token: None,
/// };
/// assert_eq!(request.limit, Some(10));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScanRequest {
    /// Collection that should be scanned.
    pub locator: StorageLocator,
    /// Filter predicate used when the scan should exclude some rows.
    pub filter: Option<FilterExpr>,
    /// Projection to apply to returned records.
    pub projection: Projection,
    /// Page size to request when the backend supports paginated scans.
    pub limit: Option<usize>,
    /// Row offset to apply when the backend supports offset-based scans.
    pub offset: Option<usize>,
    /// Continuation token returned by a previous scan page.
    ///
    /// Return it unchanged with the same applicable scope; do not construct a
    /// numeric offset by inspecting a token from another backend or query.
    pub page_token: Option<PageToken>,
}

impl HasLocator for ScanRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

impl ScanRequest {
    /// Creates a scan request with all fields projected.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::ScanRequest::new(storage_sdk::database::StorageLocator::new("documents"));
    /// assert_eq!(request.limit, None);
    /// ```
    #[must_use]
    pub fn new(locator: StorageLocator) -> Self {
        Self {
            locator,
            filter: None,
            projection: Projection::All,
            limit: None,
            offset: None,
            page_token: None,
        }
    }

    /// Applies a filter.
    ///
    /// # Examples
    ///
    /// ```
    /// let filter = storage_sdk::database::data::FilterExpr::Compare {
    ///     field: "kind".to_owned(),
    ///     op: storage_sdk::database::data::ComparisonOp::Eq,
    ///     value: serde_json::json!("note"),
    /// };
    /// let request = storage_sdk::database::data::ScanRequest::new(storage_sdk::database::StorageLocator::new("documents"))
    ///     .filter(filter);
    /// assert!(request.filter.is_some());
    /// ```
    #[must_use]
    pub fn filter(mut self, filter: FilterExpr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Replaces the projection.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::ScanRequest::new(storage_sdk::database::StorageLocator::new("documents"))
    ///     .projection(storage_sdk::database::query::Projection::Columns(vec!["title".to_owned()]));
    /// assert!(matches!(request.projection, storage_sdk::database::query::Projection::Columns(_)));
    /// ```
    #[must_use]
    pub fn projection(mut self, projection: Projection) -> Self {
        self.projection = projection;
        self
    }

    /// Applies a result limit.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::ScanRequest::new(storage_sdk::database::StorageLocator::new("documents"))
    ///     .limit(5);
    /// assert_eq!(request.limit, Some(5));
    /// ```
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Applies an offset.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::ScanRequest::new(storage_sdk::database::StorageLocator::new("documents"))
    ///     .offset(10);
    /// assert_eq!(request.offset, Some(10));
    /// ```
    #[must_use]
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Applies a continuation page token.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::ScanRequest::new(storage_sdk::database::StorageLocator::new("documents"))
    ///     .page_token("page-2");
    /// assert_eq!(request.page_token.as_deref(), Some("page-2"));
    /// ```
    #[must_use]
    pub fn page_token(mut self, page_token: impl Into<PageToken>) -> Self {
        self.page_token = Some(page_token.into());
        self
    }

    pub(crate) fn effective_offset(&self) -> Result<Option<usize>, StorageError> {
        PageToken::effective_offset(self.page_token.as_deref(), self.offset)
    }

    #[must_use]
    #[cfg(feature = "qdrant")]
    pub(crate) fn client_offset(&self) -> usize {
        if self.page_token.is_some() {
            0
        } else {
            self.offset.unwrap_or(0)
        }
    }

    #[must_use]
    #[cfg(feature = "qdrant")]
    pub(crate) fn effective_limit(&self, default_limit: usize) -> usize {
        self.limit.unwrap_or(default_limit)
    }

    #[must_use]
    #[cfg(feature = "qdrant")]
    pub(crate) fn fetch_limit(&self, default_limit: usize) -> usize {
        self.effective_limit(default_limit)
            .saturating_add(self.client_offset())
    }

    #[must_use]
    #[cfg(feature = "qdrant")]
    pub(crate) fn apply_window<T>(&self, items: Vec<T>, default_limit: usize) -> Vec<T> {
        items
            .into_iter()
            .skip(self.client_offset())
            .take(self.effective_limit(default_limit))
            .collect()
    }
}

/// Single assignment used by update-style mutations.
///
/// # Examples
///
/// ```
/// let assignment = storage_sdk::database::data::UpdateAssignment {
///     column: "status".into(),
///     value: serde_json::json!("published"),
/// };
/// assert_eq!(assignment.value, serde_json::json!("published"));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpdateAssignment {
    /// Column name to assign.
    pub column: String,
    /// JSON literal assigned to the column.
    pub value: Value,
}

/// Request to update rows that match a filter.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::UpdateRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     filter: Some(storage_sdk::database::data::FilterExpr::eq("id", "doc-1")),
///     assignments: vec![storage_sdk::database::data::UpdateAssignment {
///         column: "status".into(),
///         value: serde_json::json!("published"),
///     }],
/// };
/// assert_eq!(request.assignments.len(), 1);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpdateRequest {
    /// Collection whose rows should be updated.
    pub locator: StorageLocator,
    /// Filter predicate used when the update should only affect matching rows.
    pub filter: Option<FilterExpr>,
    /// Assignments to apply.
    pub assignments: Vec<UpdateAssignment>,
}

impl HasLocator for UpdateRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Request to delete rows that match a filter.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::DeleteRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     filter: storage_sdk::database::data::FilterExpr::eq("status", "archived"),
/// };
/// assert_eq!(request.locator.collection, "documents");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeleteRequest {
    /// Collection whose rows should be deleted.
    pub locator: StorageLocator,
    /// Filter predicate that selects rows to delete.
    pub filter: FilterExpr,
}

impl HasLocator for DeleteRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Merge behavior used by [`MergeRequest`].
///
/// # Examples
///
/// ```
/// let actions = storage_sdk::database::data::MergeActions::default();
/// assert!(actions.update_matched);
/// assert!(actions.insert_unmatched);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergeActions {
    /// Update rows that match the merge key.
    pub update_matched: bool,
    /// Insert source rows that do not match a target row.
    pub insert_unmatched: bool,
    /// Delete target rows that are not present in the source set.
    pub delete_not_matched_by_source: bool,
}

impl Default for MergeActions {
    fn default() -> Self {
        Self {
            update_matched: true,
            insert_unmatched: true,
            delete_not_matched_by_source: false,
        }
    }
}

/// Request to merge source records into an existing collection.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::MergeRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     on: vec!["id".into()],
///     records: Vec::new(),
///     actions: storage_sdk::database::data::MergeActions::default(),
/// };
/// assert_eq!(request.on, vec!["id"]);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MergeRequest {
    /// Collection that should receive the merged source records.
    pub locator: StorageLocator,
    /// Column set used to match source and target rows.
    pub on: Vec<String>,
    /// Source records to merge.
    pub records: Vec<DataRecord>,
    /// Merge behavior toggles that control how matched and unmatched rows are handled.
    pub actions: MergeActions,
}

impl HasLocator for MergeRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Atomically replaces all records selected by one bounded partition.
///
/// The identity field is a collection-global unique scalar payload field used
/// to match replacement records to existing rows. Partition fields and the
/// identity field must be ordinary payload fields rather than the reserved
/// `id` field.
///
/// # Examples
///
/// ```
/// let record = storage_sdk::database::data::DataRecord::without_id()
///     .json_field("tenant", serde_json::json!("acme"))
///     .json_field("record_key", serde_json::json!("chunk-0"));
/// let request = storage_sdk::database::data::ReplacePartitionRequest::new(
///     storage_sdk::database::StorageLocator::new("managed_knowledge"),
///     storage_sdk::database::data::FilterExpr::eq("tenant", "acme"),
///     "record_key",
///     vec![record],
/// )?;
/// assert_eq!(request.records.len(), 1);
/// # Ok::<(), storage_sdk::StorageError>(())
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplacePartitionRequest {
    /// Collection whose selected partition should be replaced.
    pub locator: StorageLocator,
    /// Bounded equality predicate selecting the complete target partition.
    pub partition: FilterExpr,
    /// Collection-global unique scalar payload field used to match records.
    pub identity_field: String,
    /// Complete replacement set for the selected partition.
    pub records: Vec<DataRecord>,
}

impl HasLocator for ReplacePartitionRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

impl ReplacePartitionRequest {
    /// Creates and validates an atomic partition replacement request.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::ReplacePartitionRequest::new(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     storage_sdk::database::data::FilterExpr::eq("tenant", "acme"),
    ///     "record_key",
    ///     Vec::new(),
    /// )?;
    /// assert!(request.records.is_empty());
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidRequest`] when the partition, identity
    /// field, or replacement records do not satisfy the bounded replacement
    /// contract.
    pub fn new(
        locator: StorageLocator,
        partition: FilterExpr,
        identity_field: impl Into<String>,
        records: Vec<DataRecord>,
    ) -> Result<Self, StorageError> {
        let request = Self {
            locator,
            partition,
            identity_field: identity_field.into(),
            records,
        };
        request.validate()?;
        Ok(request)
    }

    /// Validates the bounded partition and complete replacement record set.
    ///
    /// This method is also applied after default-locator normalization at the
    /// common database boundary, so direct construction and deserialization
    /// cannot bypass validation before backend I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// let request = storage_sdk::database::data::ReplacePartitionRequest::new(
    ///     storage_sdk::database::StorageLocator::new("documents"),
    ///     storage_sdk::database::data::FilterExpr::eq("tenant", "acme"),
    ///     "record_key",
    ///     Vec::new(),
    /// )?;
    /// request.validate()?;
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidRequest`] for an unbounded or nested
    /// predicate, invalid or reserved field names, non-scalar values,
    /// mismatched records, or duplicate replacement identities.
    pub fn validate(&self) -> Result<(), StorageError> {
        let mut partition_fields = BTreeSet::new();
        match &self.partition {
            FilterExpr::Compare { .. } | FilterExpr::In { .. } => {
                self.validate_partition_term(&self.partition, &mut partition_fields)?;
            }
            FilterExpr::And(terms) if !terms.is_empty() => {
                for term in terms {
                    self.validate_partition_term(term, &mut partition_fields)?;
                }
            }
            FilterExpr::And(_) => {
                return Err(StorageError::InvalidRequest(
                    "replace partition requires a nonempty conjunction".to_owned(),
                ));
            }
            _ => {
                return Err(StorageError::InvalidRequest(
                    "replace partition accepts only equality, nonempty membership, or a flat nonempty conjunction of those predicates"
                        .to_owned(),
                ));
            }
        }

        self.validate_field_name(&self.identity_field, "identity")?;
        if partition_fields.contains(&self.identity_field) {
            return Err(StorageError::InvalidRequest(format!(
                "replace partition identity field `{}` must differ from every partition field",
                self.identity_field
            )));
        }

        let mut identities = HashSet::new();
        for (index, record) in self.records.iter().enumerate() {
            if record.id.is_some() {
                return Err(StorageError::InvalidRequest(format!(
                    "replace partition record {index} must not carry DataRecord.id"
                )));
            }
            if record.fields.contains_key("id") || record.vectors.contains_key("id") {
                return Err(StorageError::InvalidRequest(format!(
                    "replace partition record {index} must not contain a field or vector named `id`"
                )));
            }
            if record.fields.contains_key(&self.identity_field)
                && record.vectors.contains_key(&self.identity_field)
            {
                return Err(StorageError::InvalidRequest(format!(
                    "replace partition record {index} must not contain identity field `{}` as both a scalar field and a vector",
                    self.identity_field
                )));
            }
            if let Some(field) = partition_fields.iter().find(|field| {
                record.fields.contains_key(field.as_str())
                    && record.vectors.contains_key(field.as_str())
            }) {
                return Err(StorageError::InvalidRequest(format!(
                    "replace partition record {index} must not contain partition field `{field}` as both a scalar field and a vector"
                )));
            }
            if !self.record_matches_partition(record) {
                return Err(StorageError::InvalidRequest(format!(
                    "replace partition record {index} does not contain and satisfy every partition field"
                )));
            }

            let identity = record.fields.get(&self.identity_field).ok_or_else(|| {
                StorageError::InvalidRequest(format!(
                    "replace partition record {index} is missing identity field `{}`",
                    self.identity_field
                ))
            })?;
            self.validate_scalar(identity, "replacement identity")?;
            if !identities.insert(identity) {
                return Err(StorageError::InvalidRequest(format!(
                    "replace partition identity field `{}` must be unique within the replacement set",
                    self.identity_field
                )));
            }
        }

        Ok(())
    }

    fn validate_partition_term(
        &self,
        term: &FilterExpr,
        partition_fields: &mut BTreeSet<String>,
    ) -> Result<(), StorageError> {
        match term {
            FilterExpr::Compare {
                field,
                op: super::ComparisonOp::Eq,
                value,
            } => {
                self.validate_partition_field(field, partition_fields)?;
                self.validate_scalar(value, "partition value")
            }
            FilterExpr::In { field, values } if !values.is_empty() => {
                self.validate_partition_field(field, partition_fields)?;
                let mut unique_values = HashSet::new();
                for value in values {
                    self.validate_scalar(value, "partition membership value")?;
                    if !unique_values.insert(value) {
                        return Err(StorageError::InvalidRequest(format!(
                            "replace partition membership values for `{field}` must be duplicate-free"
                        )));
                    }
                }
                Ok(())
            }
            FilterExpr::In { .. } => Err(StorageError::InvalidRequest(
                "replace partition membership values must be nonempty".to_owned(),
            )),
            FilterExpr::Compare { .. } => Err(StorageError::InvalidRequest(
                "replace partition comparisons must use equality".to_owned(),
            )),
            FilterExpr::And(_) => Err(StorageError::InvalidRequest(
                "replace partition conjunctions must be flat".to_owned(),
            )),
            _ => Err(StorageError::InvalidRequest(
                "replace partition conjunction terms must be equality or nonempty membership predicates"
                    .to_owned(),
            )),
        }
    }

    fn validate_partition_field(
        &self,
        field: &str,
        partition_fields: &mut BTreeSet<String>,
    ) -> Result<(), StorageError> {
        self.validate_field_name(field, "partition")?;
        if !partition_fields.insert(field.to_owned()) {
            return Err(StorageError::InvalidRequest(format!(
                "replace partition field `{field}` must appear only once"
            )));
        }
        Ok(())
    }

    fn validate_field_name(&self, field: &str, role: &str) -> Result<(), StorageError> {
        let mut bytes = field.bytes();
        let valid_first = bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic());
        let valid_rest = bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric());
        if !valid_first || !valid_rest || field == "id" {
            return Err(StorageError::InvalidRequest(format!(
                "replace partition {role} field `{field}` must match [A-Za-z_][A-Za-z0-9_]* and must not be `id`"
            )));
        }
        Ok(())
    }

    fn validate_scalar(&self, value: &Value, role: &str) -> Result<(), StorageError> {
        if matches!(value, Value::Bool(_) | Value::Number(_) | Value::String(_)) {
            Ok(())
        } else {
            Err(StorageError::InvalidRequest(format!(
                "replace partition {role} must be a non-null JSON scalar"
            )))
        }
    }

    fn record_matches_partition(&self, record: &DataRecord) -> bool {
        match &self.partition {
            FilterExpr::And(terms) => terms
                .iter()
                .all(|term| self.record_matches_partition_term(record, term)),
            term => self.record_matches_partition_term(record, term),
        }
    }

    fn record_matches_partition_term(&self, record: &DataRecord, term: &FilterExpr) -> bool {
        match term {
            FilterExpr::Compare { field, value, .. } => record
                .fields
                .get(field)
                .is_some_and(|actual| actual == value),
            FilterExpr::In { field, values } => record
                .fields
                .get(field)
                .is_some_and(|actual| values.iter().any(|candidate| actual == candidate)),
            _ => false,
        }
    }
}

/// Request to count rows matching an optional filter.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::CountRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     filter: None,
/// };
/// assert!(request.filter.is_none());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CountRequest {
    /// Collection whose rows should be counted.
    pub locator: StorageLocator,
    /// Filter predicate used when only matching rows should be counted.
    pub filter: Option<FilterExpr>,
}

impl HasLocator for CountRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

/// Page of records returned by lookup or scan flows.
///
/// # Examples
///
/// ```
/// let page = storage_sdk::database::data::RecordsPage {
///     records: vec![storage_sdk::database::data::DataRecord::default()],
///     total: Some(1),
///     next_page: None,
/// };
/// assert_eq!(page.total, Some(1));
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RecordsPage {
    /// Records returned in this page.
    pub records: Vec<DataRecord>,
    /// Total matching row count when the backend reports it alongside the page.
    pub total: Option<u64>,
    /// Continuation token for fetching the next page of records.
    pub next_page: Option<PageToken>,
}

/// Portable mutation counts.
///
/// Count fields are populated only when the backend returns data that proves
/// them. If the backend only acknowledges a write or returns non-count
/// metadata, the count fields remain `None` and `counts_exact` is `false`.
///
/// # Examples
///
/// ```
/// let result = storage_sdk::database::data::MutationResult {
///     inserted: Some(2),
///     counts_exact: true,
///     ..Default::default()
/// };
/// assert_eq!(result.inserted, Some(2));
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MutationResult {
    /// Inserted row count.
    pub inserted: Option<u64>,
    /// Updated row count.
    pub updated: Option<u64>,
    /// Deleted row count.
    pub deleted: Option<u64>,
    /// Upserted row count.
    pub upserted: Option<u64>,
    /// Matched row count.
    pub matched: Option<u64>,
    /// Whether the populated counts are exact backend-reported counts.
    pub counts_exact: bool,
}

impl MutationResult {
    #[must_use]
    pub(crate) fn counts(
        inserted: Option<u64>,
        updated: Option<u64>,
        deleted: Option<u64>,
        upserted: Option<u64>,
        matched: Option<u64>,
        counts_exact: bool,
    ) -> Self {
        Self {
            inserted,
            updated,
            deleted,
            upserted,
            matched,
            counts_exact,
        }
    }

    #[must_use]
    pub(crate) fn no_counts() -> Self {
        Self::counts(None, None, None, None, None, false)
    }

    #[must_use]
    #[cfg(feature = "surrealdb")]
    pub(crate) fn matched_operation(matched: u64, deleted: bool, counts_exact: bool) -> Self {
        Self::counts(
            Some(0),
            Some(if deleted { 0 } else { matched }),
            Some(if deleted { matched } else { 0 }),
            Some(0),
            Some(matched),
            counts_exact,
        )
    }
}

/// Portable data request.
///
/// # Examples
///
/// ```
/// let request = storage_sdk::database::data::DataRequest::Count(storage_sdk::database::data::CountRequest {
///     locator: storage_sdk::database::StorageLocator::new("documents"),
///     filter: None,
/// });
/// assert!(matches!(request, storage_sdk::database::data::DataRequest::Count(_)));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataRequest {
    /// Insert records.
    Insert(InsertRequest),
    /// Lookup records by id.
    Lookup(LookupRequest),
    /// Scan a collection.
    Scan(ScanRequest),
    /// Update matching rows.
    Update(UpdateRequest),
    /// Delete matching rows.
    Delete(DeleteRequest),
    /// Merge source records.
    Merge(MergeRequest),
    /// Atomically replace all records in one bounded partition.
    ReplacePartition(ReplacePartitionRequest),
    /// Count matching rows.
    Count(CountRequest),
}

impl DataRequest {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        match self {
            Self::ReplacePartition(request) => request.validate(),
            _ => Ok(()),
        }
    }
}

impl ApplyDefaultLocator for DataRequest {
    fn apply_default_locator(self, default: &StorageLocator) -> Self {
        match self {
            Self::Insert(req) => Self::Insert(req.apply_default_locator(default)),
            Self::Lookup(req) => Self::Lookup(req.apply_default_locator(default)),
            Self::Scan(req) => Self::Scan(req.apply_default_locator(default)),
            Self::Update(req) => Self::Update(req.apply_default_locator(default)),
            Self::Delete(req) => Self::Delete(req.apply_default_locator(default)),
            Self::Merge(req) => Self::Merge(req.apply_default_locator(default)),
            Self::ReplacePartition(req) => {
                Self::ReplacePartition(req.apply_default_locator(default))
            }
            Self::Count(req) => Self::Count(req.apply_default_locator(default)),
        }
    }
}

/// Portable data response.
///
/// # Examples
///
/// ```
/// let response = storage_sdk::database::data::DataResponse::Count(3);
/// assert!(matches!(response, storage_sdk::database::data::DataResponse::Count(3)));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataResponse {
    /// Returns portable mutation counts.
    Mutation(MutationResult),
    /// Returns one page of records.
    Records(RecordsPage),
    /// Returns only the matching row count.
    Count(u64),
}
