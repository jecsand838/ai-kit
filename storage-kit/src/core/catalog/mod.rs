mod schema;

use serde::{Deserialize, Serialize};

use crate::{CollectionSchema, DataRecord, StorageError, StorageLocator};

use super::{ApplyDefaultLocator, HasLocator};

pub use schema::{
    AddColumnsRequest, AlterColumnsRequest, ColumnAlterationSpec, ComputedColumn,
    DropColumnsRequest, SchemaEvolutionRequest,
};

/// Portable collection description.
///
/// An omitted schema or record count means the backend cannot report that
/// value for this observation; it must not be interpreted as an empty schema
/// or a count of zero.
///
/// # Examples
///
/// ```
/// use storage_kit::database::StorageLocator;
/// use storage_kit::database::catalog::CollectionInfo;
/// let info = CollectionInfo { locator: StorageLocator::new("documents"),
///     schema: None, record_count: None };
/// assert!(info.record_count.is_none());
/// ```
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CollectionInfo {
    /// Logical collection address.
    pub locator: StorageLocator,
    /// Portable schema when the implementation can report it.
    pub schema: Option<CollectionSchema>,
    /// Exact record count when the implementation can report it.
    pub record_count: Option<u64>,
}

/// Existing-collection policy for creation.
///
/// Catalog overwrite recreates the collection and its schema/index state.
/// To replace records while preserving an existing schema and indexes, use
/// [`WriteMode::Overwrite`](crate::database::data::WriteMode::Overwrite).
///
/// # Examples
///
/// ```
/// use storage_kit::database::catalog::CreateCollectionMode;
/// let mode = CreateCollectionMode::CreateIfMissing;
/// assert_eq!(mode, CreateCollectionMode::CreateIfMissing);
/// ```
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CreateCollectionMode {
    /// Fail when the collection exists.
    Create,
    /// Drop and recreate an existing collection.
    Overwrite,
    /// Reuse an existing collection.
    CreateIfMissing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateCollectionAction {
    Create,
    Recreate,
    Reuse,
    ErrorExists,
}

impl CreateCollectionMode {
    pub(crate) fn resolve(self, exists: bool) -> CreateCollectionAction {
        match (self, exists) {
            (Self::Create, true) => CreateCollectionAction::ErrorExists,
            (Self::Create, false) => CreateCollectionAction::Create,
            (Self::Overwrite, true) => CreateCollectionAction::Recreate,
            (Self::Overwrite, false) => CreateCollectionAction::Create,
            (Self::CreateIfMissing, true) => CreateCollectionAction::Reuse,
            (Self::CreateIfMissing, false) => CreateCollectionAction::Create,
        }
    }
}

/// Creates a collection, optionally with schema or seed records.
///
/// An explicit schema or seed records may be required by the selected
/// backend. [`CreateCollectionMode::CreateIfMissing`] reuses an existing
/// collection; it does not append seed records or alter its schema.
///
/// # Examples
///
/// ```
/// use storage_kit::database::StorageLocator;
/// use storage_kit::database::catalog::{CreateCollectionMode, CreateCollectionRequest};
/// let request = CreateCollectionRequest { locator: StorageLocator::new("documents"),
///     schema: None, records: Vec::new(), mode: CreateCollectionMode::Create };
/// assert_eq!(request.locator.collection, "documents");
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CreateCollectionRequest {
    /// Logical target.
    pub locator: StorageLocator,
    /// Explicit portable schema.
    pub schema: Option<CollectionSchema>,
    /// Initial records.
    pub records: Vec<DataRecord>,
    /// Existing-collection policy.
    pub mode: CreateCollectionMode,
}

impl HasLocator for CreateCollectionRequest {
    fn locator_mut(&mut self) -> &mut StorageLocator {
        &mut self.locator
    }
}

impl CreateCollectionRequest {
    pub(crate) fn already_exists_error(&self, noun: &'static str) -> StorageError {
        StorageError::backend_op(
            "create_collection",
            format!("{noun} `{}` already exists", self.locator.collection),
        )
    }
}

/// Lists collections under a logical namespace.
///
/// Collection names are returned in ascending order, strictly after the
/// optional name cursor. Unsupported namespace depth must be rejected rather
/// than redirected. Separate list calls are not a historical snapshot.
///
/// # Examples
///
/// ```
/// use storage_kit::database::catalog::ListCollectionsRequest;
/// let request = ListCollectionsRequest { namespace: Vec::new(), limit: Some(10),
///     start_after: Some("archive".to_owned()) };
/// assert_eq!(request.start_after.as_deref(), Some("archive"));
/// ```
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListCollectionsRequest {
    /// Logical namespace prefix.
    pub namespace: Vec<String>,
    /// Optional page size.
    pub limit: Option<u32>,
    /// Exclusive logical collection-name cursor.
    pub start_after: Option<String>,
}

impl ListCollectionsRequest {
    fn apply_default_namespace(mut self, default: &StorageLocator) -> Self {
        if self.namespace.is_empty() {
            self.namespace = default.namespace.clone();
        }
        self
    }

    pub(crate) fn apply_to_collections(
        &self,
        mut collections: Vec<CollectionInfo>,
    ) -> Vec<CollectionInfo> {
        collections.sort_by(|left, right| left.locator.collection.cmp(&right.locator.collection));
        if let Some(start_after) = self.start_after.as_deref() {
            collections.retain(|info| info.locator.collection.as_str() > start_after);
        }
        if let Some(limit) = self.limit {
            collections.truncate(limit as usize);
        }
        collections
    }
}

impl ApplyDefaultLocator for ListCollectionsRequest {
    fn apply_default_locator(self, default: &StorageLocator) -> Self {
        self.apply_default_namespace(default)
    }
}

/// Retained catalog operations.
///
/// # Examples
///
/// ```
/// use storage_kit::database::StorageLocator;
/// use storage_kit::database::catalog::CatalogRequest;
/// let request = CatalogRequest::OpenCollection { locator: StorageLocator::new("documents") };
/// assert!(matches!(request, CatalogRequest::OpenCollection { .. }));
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum CatalogRequest {
    /// Creates a collection.
    CreateCollection(CreateCollectionRequest),
    /// Opens or describes a collection.
    OpenCollection {
        /// Logical target.
        locator: StorageLocator,
    },
    /// Lists collections.
    ListCollections(ListCollectionsRequest),
    /// Drops a collection.
    DropCollection {
        /// Logical target.
        locator: StorageLocator,
        /// Tolerates an already-absent target.
        ignore_missing: bool,
    },
    /// Alters collection schema.
    AlterCollection(SchemaEvolutionRequest),
}

impl ApplyDefaultLocator for CatalogRequest {
    fn apply_default_locator(self, default: &StorageLocator) -> Self {
        match self {
            Self::CreateCollection(request) => {
                Self::CreateCollection(request.apply_default_locator(default))
            }
            Self::OpenCollection { locator } => Self::OpenCollection {
                locator: locator.with_defaults_from(default),
            },
            Self::ListCollections(request) => {
                Self::ListCollections(request.apply_default_locator(default))
            }
            Self::DropCollection {
                locator,
                ignore_missing,
            } => Self::DropCollection {
                locator: locator.with_defaults_from(default),
                ignore_missing,
            },
            Self::AlterCollection(request) => {
                Self::AlterCollection(request.apply_default_locator(default))
            }
        }
    }
}

/// Retained catalog responses.
///
/// Only information the implementation can report appears in collection
/// descriptions. `Completed` acknowledges successful completion of the
/// requested operation, not a backend-native status or transaction receipt.
///
/// # Examples
///
/// ```
/// use storage_kit::database::StorageLocator;
/// use storage_kit::database::catalog::{CollectionInfo, CatalogResponse};
/// let response = CatalogResponse::Collection(CollectionInfo {
///     locator: StorageLocator::new("documents"), schema: None, record_count: None,
/// });
/// if let CatalogResponse::Collection(info) = response {
///     assert_eq!(info.locator.collection, "documents");
/// }
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum CatalogResponse {
    /// A mutation completed without a backend-native status envelope.
    Completed,
    /// One collection description.
    Collection(CollectionInfo),
    /// Multiple collection descriptions.
    Collections(Vec<CollectionInfo>),
}
