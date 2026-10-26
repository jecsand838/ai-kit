//! Collection storage, retrieval, and index request/response types.
//!
//! Open a supplied [`crate::Database`] with [`config::DatabaseConfig`] and
//! import [`crate::DatabaseBackend`] to execute requests. [`catalog`] owns
//! collections and schema evolution, [`data`] owns records and mutations,
//! [`query`] owns search, and [`index`] owns index lifecycle. [`StorageLocator`]
//! is a logical address, not a native connection configuration.
//!
//! Optional [`CapabilitySet`] bits are hints about retained operation families;
//! each call validates the actual request and may reject unsupported options.
//! Database writes do not share the transactional guarantees of
//! [`crate::StateStore`].

//! # Local collection, retrieval and cleanup
//!
//! This executable example uses a tiny process-local LanceDB collection. Logical
//! identity lives in an application field: this LanceDB adapter does not support
//! portable `DataRecord.id` or ID lookup. A scalar index works at this scale; a vector query
//! does not require a trained vector index. Other backends can reject unsupported
//! index kinds or options explicitly. Do not interpret every score as similarity.
//!
//! ```
//! use storage_sdk::{Database, DatabaseBackend};
//! use storage_sdk::database::{StorageLocator, config::{DatabaseConfig, LanceConfig}};
//! use storage_sdk::database::catalog::{CatalogRequest, CreateCollectionMode, CreateCollectionRequest};
//! use storage_sdk::database::data::{DataRecord, DataRequest, DataResponse, DeleteRequest,
//!     DenseVector, FilterExpr, InsertRequest, ScanRequest, CountRequest};
//! use storage_sdk::database::query::{DistanceMetric, NearestRequest, Projection,
//!     QueryRequest, QueryScope, ScopedQuery, ScoreKind, VectorQueryOptions, VectorSearchParams};
//! use storage_sdk::database::index::{CreateIndexRequest, IndexKind, IndexRequest,
//!     IndexResponse, WaitForIndexRequest};
//! # tokio::runtime::Runtime::new()?.block_on(async {
//! let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
//! let collection = StorageLocator::new("articles");
//! let first = DataRecord::without_id().field("article", "intro")?.field("title", "Introduction")?
//!     .field("published", true)?.vector("embedding", DenseVector::F32(vec![1.0, 0.0]));
//! database.catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
//!     locator: collection.clone(), schema: None, records: vec![first],
//!     mode: CreateCollectionMode::Create,
//! })).await?;
//! database.data(DataRequest::Insert(InsertRequest::append(collection.clone(), vec![
//!     DataRecord::without_id().field("article", "draft")?.field("title", "Draft")?.field("published", false)?
//!         .vector("embedding", DenseVector::F32(vec![0.0, 1.0])),
//! ]))).await?;
//! let page = database.data(DataRequest::Scan(ScanRequest {
//!     locator: collection.clone(), filter: Some(FilterExpr::eq("published", true)),
//!     projection: Projection::Columns(vec!["title".into()]), limit: Some(10),
//!     offset: None, page_token: None,
//! })).await?;
//! let DataResponse::Records(page) = page else { panic!("scan returns records") };
//! assert_eq!(page.records.len(), 1);
//! assert_eq!(page.records[0].fields["title"], "Introduction");
//!
//! let nearest = database.query(QueryRequest::Nearest(NearestRequest {
//!     query: ScopedQuery::new(QueryScope::new(collection.clone()), VectorQueryOptions {
//!         vector_field: Some("embedding".into()),
//!         params: VectorSearchParams { limit: 1, distance: Some(DistanceMetric::L2), ..Default::default() },
//!     }),
//!     vector: DenseVector::F32(vec![1.0, 0.0]),
//! })).await?;
//! assert_eq!(nearest.hits.len(), 1);
//! assert_eq!(nearest.hits[0].record.fields["article"], "intro");
//! assert_eq!(nearest.hits[0].score_kind, ScoreKind::Distance);
//! assert_eq!(nearest.hits[0].raw_score, Some(0.0));
//!
//! database.index(IndexRequest::Create(CreateIndexRequest {
//!     locator: collection.clone(), columns: vec!["title".into()], kind: IndexKind::Scalar,
//!     wait_timeout_ms: None,
//! })).await?;
//! let IndexResponse::Indexes(indexes) = database.index(IndexRequest::List {
//!     locator: collection.clone(),
//! }).await? else { panic!("index listing returns metadata") };
//! let name = indexes.iter().find(|index| index.columns == ["title"])
//!     .and_then(|index| index.name.clone()).expect("LanceDB reports its scalar index name");
//! database.index(IndexRequest::WaitFor(WaitForIndexRequest {
//!     locator: collection.clone(), index_names: vec![name.clone()], timeout_ms: 5_000,
//! })).await?;
//! database.index(IndexRequest::Drop { locator: collection.clone(), name }).await?;
//!
//! database.data(DataRequest::Delete(DeleteRequest {
//!     locator: collection.clone(), filter: FilterExpr::eq("published", false),
//! })).await?;
//! let DataResponse::Records(remaining) = database.data(DataRequest::Scan(ScanRequest {
//!     locator: collection.clone(), filter: Some(FilterExpr::eq("article", "intro")),
//!     projection: Projection::All, limit: Some(10), offset: None, page_token: None,
//! })).await? else { panic!("scan returns records") };
//! assert_eq!(remaining.records.len(), 1);
//! assert!(matches!(database.data(DataRequest::Count(CountRequest {
//!     locator: collection.clone(), filter: None,
//! })).await?, DataResponse::Count(1)));
//! database.catalog(CatalogRequest::DropCollection {
//!     locator: collection, ignore_missing: false,
//! }).await?;
//! drop(database);
//! # Ok::<(), storage_sdk::StorageError>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Database has no multi-operation transaction or explicit shutdown method. A
//! failed ordinary write can have an uncertain outcome; reconcile before replaying.
//! Drop all owners before removing a caller-owned directory. For control-plane
//! atomicity, use the separate [StateStore transaction workflow](crate::state).

/// Built-in database construction data.
///
/// Constructors build configuration without I/O. Validation and connection
/// reject invalid settings; [`crate::Database::connect`] opens the selected
/// backend. Qdrant configuration is available with the `qdrant` feature.
pub mod config {
    pub use crate::credentials::{ApiKey, Password};
    pub use crate::databases::config::{
        CommonConfig, DatabaseConfig, LanceConfig, SurrealConfig, SurrealModeConfig,
    };
    #[cfg(feature = "qdrant")]
    pub use crate::databases::config::{QdrantConfig, QdrantModeConfig};
}

/// Portable collection catalog and schema alteration.
///
/// Execute [`CatalogRequest`](crate::database::catalog::CatalogRequest) through [`crate::DatabaseBackend::catalog`].
/// Catalog overwrite recreates the collection; replacing records while keeping
/// its schema and indexes belongs to [`WriteMode::Overwrite`](crate::database::data::WriteMode::Overwrite).
pub mod catalog {
    pub use crate::core::{
        AddColumnsRequest, AlterColumnsRequest, CatalogRequest, CatalogResponse, CollectionInfo,
        CollectionSchema, ColumnAlterationSpec, ComputedColumn, CreateCollectionMode,
        CreateCollectionRequest, DropColumnsRequest, FieldSchema, FieldType,
        ListCollectionsRequest, SchemaEvolutionRequest, VectorElementType,
    };
}

/// Portable records and data mutations.
///
/// Execute [`DataRequest`](crate::database::data::DataRequest) through [`crate::DatabaseBackend::data`]. Optional
/// mutation counts and continuation values preserve backend observations.
/// Ordinary writes may have uncertain outcomes after native failures;
/// [`ReplacePartitionRequest`](crate::database::data::ReplacePartitionRequest) has its own stronger atomic contract.
pub mod data {
    pub use crate::core::{
        ComparisonOp, CountRequest, DataRecord, DataRequest, DataResponse, DeleteRequest,
        DenseVector, FilterExpr, InsertRequest, LookupRequest, MergeActions, MergeRequest,
        MutationResult, PageToken, Projection, RecordId, RecordsPage, ReplacePartitionRequest,
        ScanRequest, UpdateAssignment, UpdateRequest, WriteMode,
    };
}

/// Portable retained search operations.
///
/// Build a [`QueryScope`](crate::database::query::QueryScope) and [`ScopedQuery`](crate::database::query::ScopedQuery) with vector or text options, then
/// execute a [`QueryRequest`](crate::database::query::QueryRequest) through [`crate::DatabaseBackend::query`]. Read
/// [`ScoredRecord::score_kind`](crate::database::query::ScoredRecord::score_kind) before interpreting scores and return page
/// tokens unchanged to the same applicable operation.
pub mod query {
    pub use crate::core::{
        DenseVector, DistanceMetric, NearestRequest, Projection, QueryRequest, QueryResponse,
        QueryScope, RandomSampleRequest, ScopedQuery, ScoreKind, ScoredRecord, ScrollRequest,
        SearchByIdRequest, TextQueryOptions, TextSearchRequest, VectorCoercionPolicy,
        VectorQueryOptions, VectorSearchParams,
    };
}

/// Portable index lifecycle.
///
/// Execute [`IndexRequest`](crate::database::index::IndexRequest) through [`crate::DatabaseBackend::index`]. Creation
/// and readiness observation are separate; unsupported kinds fail explicitly
/// and optional metadata remains absent when it cannot be reported.
pub mod index {
    pub use crate::core::{
        CreateIndexRequest, DistanceMetric, IndexInfo, IndexKind, IndexRequest, IndexResponse,
        WaitForIndexRequest,
    };
}

pub use crate::core::{CapabilitySet, StorageLocator};
