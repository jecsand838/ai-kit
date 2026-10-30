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

//! # Write and filter a local collection
//!
//! This executable example uses a tiny process-local LanceDB collection. Logical
//! identity lives in an application field: this LanceDB adapter does not support
//! portable `DataRecord.id` or ID lookup. Query and index operations have separate
//! request types; optional capabilities never guarantee every request is supported.
//!
//! ```
//! use storage_kit::{Database, DatabaseBackend};
//! use storage_kit::database::{StorageLocator, config::{DatabaseConfig, LanceConfig}};
//! use storage_kit::database::catalog::{CatalogRequest, CreateCollectionMode, CreateCollectionRequest};
//! use storage_kit::database::data::{DataRecord, DataRequest, DataResponse, DeleteRequest,
//!     FilterExpr, InsertRequest, ScanRequest, CountRequest, Projection};
//! # tokio::runtime::Runtime::new()?.block_on(async {
//! let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
//! let collection = StorageLocator::new("articles");
//! let first = DataRecord::without_id().field("article", "intro")?.field("title", "Introduction")?
//!     .field("published", true)?;
//! database.catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
//!     locator: collection.clone(), schema: None, records: vec![first],
//!     mode: CreateCollectionMode::Create,
//! })).await?;
//! database.data(DataRequest::Insert(InsertRequest::append(collection.clone(), vec![
//!     DataRecord::without_id().field("article", "draft")?
//!         .field("title", "Draft")?.field("published", false)?,
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
//! database.data(DataRequest::Delete(DeleteRequest {
//!     locator: collection.clone(), filter: FilterExpr::eq("published", false),
//! })).await?;
//! assert!(matches!(database.data(DataRequest::Count(CountRequest {
//!     locator: collection.clone(), filter: None,
//! })).await?, DataResponse::Count(1)));
//! database.catalog(CatalogRequest::DropCollection {
//!     locator: collection, ignore_missing: false,
//! }).await?;
//! drop(database);
//! # Ok::<(), storage_kit::StorageError>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! For vector retrieval, build a [`query::NearestRequest`] and inspect each
//! [`query::ScoredRecord::score_kind`] before interpreting its score. Vector
//! queries do not require a trained vector index. Index creation and readiness
//! are separate operations; see [`crate::DatabaseBackend::index`] and
//! [`index::WaitForIndexRequest`]. Unsupported kinds or options fail explicitly.
//!
//! Database has no multi-operation transaction or explicit shutdown method. A
//! failed ordinary write can have an uncertain outcome; reconcile before replaying.
//! Drop all owners before removing a caller-owned directory. For control-plane
//! atomicity, use the separate [StateStore transaction workflow](crate::state).

/// Built-in database construction data.
///
/// Constructors build configuration without I/O. Validation and connection
/// reject invalid settings; [`crate::Database::connect`] opens the selected
/// backend.
pub mod config {
    pub use crate::credentials::Password;
    pub use crate::databases::config::{
        CommonConfig, DatabaseConfig, LanceConfig, SurrealConfig, SurrealModeConfig,
    };
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
        DenseVector, FilterExpr, InsertRequest, MutationResult, PageToken, Projection, RecordId,
        RecordsPage, ReplacePartitionRequest, ScanRequest, UpdateAssignment, UpdateRequest,
        WriteMode,
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
        QueryScope, ScopedQuery, ScoreKind, ScoredRecord, ScrollRequest, TextQueryOptions,
        TextSearchRequest, VectorCoercionPolicy, VectorQueryOptions, VectorSearchParams,
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
