//! Private implementation of the root database owner.

use std::{fmt, future::Future, sync::Arc};

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
/// # Caller-owned catalog example
///
/// This small test double validates one request and returns an empty catalog.
/// It demonstrates direct trait consumption, not a database engine. Other
/// operations reject explicitly; no native storage or service is constructed.
///
/// ```
/// use storage_kit::{DatabaseBackend, StorageError};
/// use storage_kit::database::CapabilitySet;
/// use storage_kit::database::catalog::{CatalogRequest, CatalogResponse, ListCollectionsRequest};
/// # use storage_kit::database::data::{DataRequest, DataResponse};
/// # use storage_kit::database::query::{QueryRequest, QueryResponse};
/// # use storage_kit::database::index::{IndexRequest, IndexResponse};
///
/// struct EmptyCatalog;
/// impl DatabaseBackend for EmptyCatalog {
///     fn capabilities(&self) -> CapabilitySet { CapabilitySet::empty() }
///     async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
///         assert_eq!(request, CatalogRequest::ListCollections(ListCollectionsRequest {
///             namespace: Vec::new(), limit: Some(10), start_after: None,
///         }));
///         Ok(CatalogResponse::Collections(Vec::new()))
///     }
/// #     async fn data(&self, _: DataRequest) -> Result<DataResponse, StorageError> {
/// #         Err(StorageError::unsupported_capability("data", "catalog-only example"))
/// #     }
/// #     async fn query(&self, _: QueryRequest) -> Result<QueryResponse, StorageError> {
/// #         Err(StorageError::unsupported_capability("query", "catalog-only example"))
/// #     }
/// #     async fn index(&self, _: IndexRequest) -> Result<IndexResponse, StorageError> {
/// #         Err(StorageError::unsupported_capability("index", "catalog-only example"))
/// #     }
/// }
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let database = EmptyCatalog;
/// let response = database.catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
///     namespace: Vec::new(), limit: Some(10), start_after: None,
/// })).await?;
/// let CatalogResponse::Collections(collections) = response else {
///     panic!("expected the catalog response");
/// };
/// assert!(collections.is_empty());
/// assert!(database.capabilities().is_empty());
/// # Ok::<(), StorageError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// An application may compose different implementations in its own exhaustive
/// enum, forwarding requests and errors unchanged. See the
/// [executable caller composition tests](https://github.com/jecsand838/ai-kit/blob/main/storage-kit/tests/mock_contracts.rs).
/// The [local collection workflow](crate::database) uses real embedded storage.
pub trait DatabaseBackend: Send + Sync + 'static {
    /// Returns optional retained capabilities.
    ///
    /// Use this to discover optional operation families, not to identify the
    /// implementation or assume an arbitrary request will succeed.
    ///
    /// See the [caller-owned catalog example](DatabaseBackend) for an executable example.
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
    /// See the [caller-owned catalog example](DatabaseBackend) for an executable example.
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
    /// See the [local collection workflow](crate::database) for an executable example.
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
    /// use storage_kit::{Database, DatabaseBackend};
    /// use storage_kit::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::catalog::{CatalogRequest, CreateCollectionRequest, CreateCollectionMode};
    /// use storage_kit::database::data::DataRecord;
    /// use storage_kit::database::query::{QueryRequest, ScrollRequest, Projection};
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
    /// # Ok::<(), storage_kit::StorageError>(())
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
    /// use storage_kit::{Database, DatabaseBackend};
    /// use storage_kit::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_kit::database::StorageLocator;
    /// use storage_kit::database::catalog::{CatalogRequest, CreateCollectionRequest, CreateCollectionMode};
    /// use storage_kit::database::data::DataRecord;
    /// use storage_kit::database::index::{IndexRequest, IndexResponse};
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
    /// # Ok::<(), storage_kit::StorageError>(())
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
/// use storage_kit::{Database, DatabaseBackend};
/// use storage_kit::database::config::{DatabaseConfig, LanceConfig};
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
/// let shared = database.clone();
/// assert_eq!(database.capabilities(), shared.capabilities());
/// # Ok::<(), storage_kit::StorageError>(())
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
}

impl Database {
    /// Opens a built-in database.
    ///
    /// The configuration is validated before backend resources are opened.
    /// LanceDB is always available. SurrealDB requires `surrealdb`.
    /// Disabled implementations return
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
    /// use storage_kit::{Database, DatabaseBackend};
    /// use storage_kit::database::config::{DatabaseConfig, LanceConfig};
    /// use storage_kit::database::CapabilitySet;
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
    /// assert!(database.capabilities().contains(CapabilitySet::FILTERING));
    /// # Ok::<(), storage_kit::StorageError>(())
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
        }
    }

    async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        match self {
            Self::Lance(backend) => backend.catalog(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.catalog(request).await,
        }
    }

    async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
        request.validate()?;
        match self {
            Self::Lance(backend) => backend.data(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.data(request).await,
        }
    }

    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, StorageError> {
        match self {
            Self::Lance(backend) => backend.query(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.query(request).await,
        }
    }

    async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
        request.validate()?;
        match self {
            Self::Lance(backend) => backend.index(request).await,
            #[cfg(feature = "surrealdb")]
            Self::Surreal(backend) => backend.index(request).await,
        }
    }
}
