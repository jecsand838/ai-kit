//! Private implementation of the root database owner.

use std::{fmt, future::Future, sync::Arc};

#[cfg(feature = "qdrant")]
use crate::databases::qdrant::backend::QdrantBackend;
#[cfg(feature = "surrealdb")]
use crate::databases::surrealdb::backend::SurrealBackend;
use crate::{
    CapabilitySet, CatalogRequest, CatalogResponse, DataRequest, DataResponse, IndexRequest,
    IndexResponse, QueryRequest, QueryResponse, StorageError,
    databases::{config::DatabaseConfig, lancedb::backend::LanceBackend},
};

/// Portable data, catalog, query, and index behavior.
///
/// Implement this trait directly on a caller-owned type; `Clone` is not
/// required. Operations are authoritative: an optional capability bit does not
/// guarantee that an arbitrary request is supported. Unsupported operations
/// fail before mutation, and responses preserve real ids, counts and score
/// semantics. Supporting DTOs live in [`crate::database`].
///
/// # Examples
///
/// ```
/// use storage_sdk::{Database, DatabaseBackend};
/// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
/// use storage_sdk::database::CapabilitySet;
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
/// let capabilities = database.capabilities();
/// assert!(capabilities.contains(CapabilitySet::FILTERING));
/// # Ok::<(), storage_sdk::StorageError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Caller-owned exhaustive composition
///
/// A caller can choose a database once and keep dispatch concrete. Here `Primary`
/// and `Archive` are application roles over real supplied stores, not new backend
/// capabilities. Every request and error is delegated unchanged. The same pattern
/// can contain different concrete implementations of this trait; there is no
/// normalization constructor and no obligation for the caller root to be Clone.
///
/// ```
/// use storage_sdk::{Database, DatabaseBackend, StorageError};
/// use storage_sdk::database::CapabilitySet;
/// use storage_sdk::database::catalog::{CatalogRequest, CatalogResponse, ListCollectionsRequest};
/// use storage_sdk::database::data::{DataRequest, DataResponse};
/// use storage_sdk::database::query::{QueryRequest, QueryResponse};
/// use storage_sdk::database::index::{IndexRequest, IndexResponse};
/// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
///
/// enum ApplicationDatabase { Primary(Database), Archive(Database) }
/// impl DatabaseBackend for ApplicationDatabase {
///     fn capabilities(&self) -> CapabilitySet {
///         match self { Self::Primary(db) => db.capabilities(), Self::Archive(db) => db.capabilities() }
///     }
///     async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
///         match self { Self::Primary(db) => db.catalog(request).await, Self::Archive(db) => db.catalog(request).await }
///     }
///     async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
///         match self { Self::Primary(db) => db.data(request).await, Self::Archive(db) => db.data(request).await }
///     }
///     async fn query(&self, request: QueryRequest) -> Result<QueryResponse, StorageError> {
///         match self { Self::Primary(db) => db.query(request).await, Self::Archive(db) => db.query(request).await }
///     }
///     async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
///         match self { Self::Primary(db) => db.index(request).await, Self::Archive(db) => db.index(request).await }
///     }
/// }
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let primary = ApplicationDatabase::Primary(Database::connect(
///     DatabaseConfig::lance(LanceConfig::embedded_path("memory://")),
/// ).await?);
/// let archive = ApplicationDatabase::Archive(Database::connect(
///     DatabaseConfig::lance(LanceConfig::embedded_path("memory://")),
/// ).await?);
/// for selected in [&primary, &archive] {
///     let CatalogResponse::Collections(page) = selected.catalog(CatalogRequest::ListCollections(
///         ListCollectionsRequest { namespace: Vec::new(), limit: Some(10), start_after: None },
///     )).await? else { panic!("catalog listing returns collections") };
///     assert!(page.is_empty());
/// }
/// drop((primary, archive));
/// # Ok::<(), StorageError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait DatabaseBackend: Send + Sync + 'static {
    /// Returns optional retained capabilities.
    ///
    /// Use this to discover optional operation families, not to identify the
    /// implementation or assume an arbitrary request will succeed.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{Database, DatabaseBackend};
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_sdk::database::CapabilitySet;
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// let optional = database.capabilities();
    /// assert!(optional.contains(CapabilitySet::FILTERING));
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn capabilities(&self) -> CapabilitySet;

    /// Executes a retained catalog operation.
    ///
    /// Creation, opening, listing, deletion, and schema evolution remain distinct
    /// from data writes and StateStore transactions.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] for invalid scope or request data, unsupported
    /// operations, and native connection, deadline, or execution failures. An
    /// unsupported operation must fail before mutation.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{Database, DatabaseBackend};
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_sdk::database::catalog::{CatalogRequest, CatalogResponse, ListCollectionsRequest};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// let response = database.catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
    ///     namespace: Vec::new(), limit: Some(10), start_after: None,
    /// })).await?;
    /// assert!(matches!(response, CatalogResponse::Collections(_)));
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn catalog(
        &self,
        request: CatalogRequest,
    ) -> impl Future<Output = Result<CatalogResponse, StorageError>> + Send + '_;

    /// Executes a retained data operation.
    ///
    /// Ordinary writes are not rollback-capable multi-request transactions. A
    /// native failure or timeout can leave an uncertain or partial outcome and
    /// must not trigger automatic replay. Partition replacement retains its
    /// explicit atomic contract.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] for invalid input, unsupported request combinations,
    /// read-only policy, serialization failures, and native execution/deadline
    /// failures. Unsupported behavior must be rejected before mutation.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{Database, DatabaseBackend};
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_sdk::database::StorageLocator;
    /// use storage_sdk::database::catalog::{CatalogRequest, CreateCollectionRequest, CreateCollectionMode};
    /// use storage_sdk::database::data::{DataRecord, DataRequest, DataResponse, CountRequest};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// let locator = StorageLocator::new("documents");
    /// database.catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
    ///     locator: locator.clone(), schema: None,
    ///     records: vec![DataRecord::without_id().json_field("text", serde_json::json!("example"))],
    ///     mode: CreateCollectionMode::Create,
    /// })).await?;
    /// let response = database.data(DataRequest::Count(CountRequest { locator, filter: None })).await?;
    /// assert!(matches!(response, DataResponse::Count(1)));
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn data(
        &self,
        request: DataRequest,
    ) -> impl Future<Output = Result<DataResponse, StorageError>> + Send + '_;

    /// Executes a retained query operation.
    ///
    /// Return continuation tokens unchanged to the same applicable operation.
    /// Scores preserve their explicit kind; a missing score is not zero.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] for invalid scope, vectors, fields, filters, or
    /// tuning, unsupported query combinations, and native execution/deadline
    /// failures. Optional capability bits do not override request validation.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{Database, DatabaseBackend};
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_sdk::database::StorageLocator;
    /// use storage_sdk::database::catalog::{CatalogRequest, CreateCollectionRequest, CreateCollectionMode};
    /// use storage_sdk::database::data::DataRecord;
    /// use storage_sdk::database::query::{QueryRequest, ScrollRequest, Projection};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// let locator = StorageLocator::new("documents");
    /// database.catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
    ///     locator: locator.clone(), schema: None,
    ///     records: vec![DataRecord::without_id().json_field("text", serde_json::json!("example"))],
    ///     mode: CreateCollectionMode::Create,
    /// })).await?;
    /// let response = database.query(QueryRequest::Scroll(ScrollRequest {
    ///     locator, filter: None, projection: Projection::All, limit: Some(10),
    ///     offset: None, page_token: None,
    /// })).await?;
    /// assert_eq!(response.hits.len(), 1);
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn query(
        &self,
        request: QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, StorageError>> + Send + '_;

    /// Executes a retained index operation.
    ///
    /// Creating an index and observing its readiness are separate operations.
    /// Readiness requests must finish within both their requested wait bound and
    /// the enclosing configured operation deadline.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] for malformed index requests, unsupported kinds,
    /// read-only policy, missing targets, unconfirmed completion, or native
    /// execution/readiness deadline failures.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{Database, DatabaseBackend};
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_sdk::database::StorageLocator;
    /// use storage_sdk::database::catalog::{CatalogRequest, CreateCollectionRequest, CreateCollectionMode};
    /// use storage_sdk::database::data::DataRecord;
    /// use storage_sdk::database::index::{IndexRequest, IndexResponse};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// let locator = StorageLocator::new("documents");
    /// database.catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
    ///     locator: locator.clone(), schema: None,
    ///     records: vec![DataRecord::without_id().json_field("text", serde_json::json!("example"))],
    ///     mode: CreateCollectionMode::Create,
    /// })).await?;
    /// let response = database.index(IndexRequest::List { locator }).await?;
    /// assert!(matches!(response, IndexResponse::Indexes(_)));
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn index(
        &self,
        request: IndexRequest,
    ) -> impl Future<Output = Result<IndexResponse, StorageError>> + Send + '_;
}

/// Cloneable opaque database handle.
///
/// Clones share the same opened database. Import [`DatabaseBackend`] to call
/// its catalog, data, query, index, and capability methods. This supplied
/// handle contains only built-ins; caller implementations retain their own
/// concrete type and implement the trait directly.
///
/// # Examples
///
/// ```
/// use storage_sdk::{Database, DatabaseBackend};
/// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
/// let shared = database.clone();
/// assert_eq!(database.capabilities(), shared.capabilities());
/// # Ok::<(), storage_sdk::StorageError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

enum DatabaseInner {
    Lance(LanceBackend),
    #[cfg(feature = "surrealdb")]
    Surreal(SurrealBackend),
    #[cfg(feature = "qdrant")]
    Qdrant(QdrantBackend),
}

impl Database {
    /// Opens a built-in database.
    ///
    /// The configuration is validated before backend resources are opened.
    /// LanceDB is always available. SurrealDB requires `surrealdb`, and Qdrant
    /// requires `qdrant`. Disabled implementations return
    /// [`StorageError::InvalidConfig`] before opening native resources.
    /// The example uses an in-memory LanceDB instance and needs no service.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidConfig`] for invalid configuration or
    /// required capabilities the selected backend cannot provide, or the
    /// owning connection, I/O, or timeout error.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{Database, DatabaseBackend};
    /// use storage_sdk::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_sdk::database::CapabilitySet;
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// assert!(database.capabilities().contains(CapabilitySet::FILTERING));
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn connect(config: DatabaseConfig) -> Result<Self, StorageError> {
        config.validate()?;
        match config {
            DatabaseConfig::Lance { config, common } => LanceBackend::connect(common, config)
                .await
                .map(|backend| Self {
                    inner: Arc::new(DatabaseInner::Lance(backend)),
                }),
            DatabaseConfig::Surreal { config, common } => {
                if cfg!(feature = "surrealdb") {
                    #[cfg(feature = "surrealdb")]
                    return SurrealBackend::connect(common, config)
                        .await
                        .map(|backend| Self {
                            inner: Arc::new(DatabaseInner::Surreal(backend)),
                        });
                }
                drop((config, common));
                Err(StorageError::InvalidConfig(
                    "SurrealDB requires the `surrealdb` feature".into(),
                ))
            }
            #[cfg(feature = "qdrant")]
            DatabaseConfig::Qdrant { config, common } => QdrantBackend::connect(common, config)
                .await
                .map(|backend| Self {
                    inner: Arc::new(DatabaseInner::Qdrant(backend)),
                }),
        }
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Database { .. }")
    }
}

impl DatabaseBackend for Database {
    fn capabilities(&self) -> CapabilitySet {
        self.inner.capabilities()
    }

    // Keep the declared Send bound opaque to callers so their future proofs do
    // not recursively expand the native database query implementation.
    fn catalog(
        &self,
        request: CatalogRequest,
    ) -> impl Future<Output = Result<CatalogResponse, StorageError>> + Send + '_ {
        self.inner.catalog(request)
    }

    fn data(
        &self,
        request: DataRequest,
    ) -> impl Future<Output = Result<DataResponse, StorageError>> + Send + '_ {
        self.inner.data(request)
    }

    fn query(
        &self,
        request: QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, StorageError>> + Send + '_ {
        self.inner.query(request)
    }

    fn index(
        &self,
        request: IndexRequest,
    ) -> impl Future<Output = Result<IndexResponse, StorageError>> + Send + '_ {
        self.inner.index(request)
    }
}

impl DatabaseBackend for DatabaseInner {
    fn capabilities(&self) -> CapabilitySet {
        match self {
            Self::Lance(backend) => backend.capabilities(),
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.capabilities(),
            #[cfg(feature = "qdrant")]
            Self::Qdrant(backend) => backend.capabilities(),
        }
    }

    async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        match self {
            Self::Lance(backend) => backend.catalog(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.catalog(request).await,
            #[cfg(feature = "qdrant")]
            Self::Qdrant(backend) => backend.catalog(request).await,
        }
    }

    async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
        request.validate()?;
        match self {
            Self::Lance(backend) => backend.data(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.data(request).await,
            #[cfg(feature = "qdrant")]
            Self::Qdrant(backend) => backend.data(request).await,
        }
    }

    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, StorageError> {
        match self {
            Self::Lance(backend) => backend.query(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.query(request).await,
            #[cfg(feature = "qdrant")]
            Self::Qdrant(backend) => backend.query(request).await,
        }
    }

    async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
        request.validate()?;
        match self {
            Self::Lance(backend) => backend.index(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.index(request).await,
            #[cfg(feature = "qdrant")]
            Self::Qdrant(backend) => backend.index(request).await,
        }
    }
}
