//! Portable database and transactional state-store abstractions.
//!
//! Built-in databases are opened with [`Database::connect`], while built-in
//! state stores are opened with [`StateStore::connect`]. Caller-defined roots
//! implement [`DatabaseBackend`] or [`StateStoreBackend`] directly. Associated
//! state transactions remain owned by their parent backend.
//!
//! Supporting request, response, and configuration types live in the
//! functional [`database`] and [`state`] modules.
//!
//! [`Database`] owns application data and retrieval. [`StateStore`] owns
//! revisioned control-plane records, with a unique transaction for atomic writes.
//! A StateStore transaction cannot include Database effects. Bring the relevant
//! backend traits into scope to invoke behavior on the supplied handles.
//!
//! # Choose a workflow
//!
//! - Application builders: [collection writes, filtered/vector retrieval and
//!   index cleanup](database), then [transactions and durable reopen](state).
//! - Custom implementers: [concrete Database composition](DatabaseBackend) and
//!   [associated StateStore/Transaction composition](StateStoreBackend).
//!   These examples delegate real storage semantics without an erased ingress.
//!
//! # Transactional state
//!
//! This example runs against process-local Lance and requires no service:
//!
//! ```
//! use storage_kit::{StateStore, StateStoreBackend, StateTransactionBackend};
//! use storage_kit::state::{
//!     config::{LanceStateStoreConfig, StateStoreConfig},
//!     record::{StateKey, StateRecord},
//! };
//! # tokio::runtime::Runtime::new()?.block_on(async {
//! let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
//! let key = StateKey::new("workflow", "run/1")?;
//! let mut transaction = store.begin_transaction().await?;
//! transaction.put(StateRecord::new(key.clone(), serde_json::json!({"status": "ready"}))).await?;
//! transaction.commit().await?;
//! let observed = store.load(key).await?.expect("committed record");
//! assert_eq!(observed.revision.get(), 1);
//! # Ok::<(), storage_kit::StateStoreError>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Conditional writes must read their precondition through the same transaction
//! that commits them. Separate reads or listing pages do not establish a fence
//! or historical snapshot. After an uncertain commit result, reconcile durable
//! application state before retrying; losing a reply does not establish rollback.
//!
//! # Application data
//!
//! This example opens an in-memory LanceDB instance and performs a catalog read:
//!
//! ```
//! use storage_kit::{Database, DatabaseBackend};
//! use storage_kit::database::{
//!     config::{DatabaseConfig, LanceConfig},
//!     catalog::{CatalogRequest, CatalogResponse, ListCollectionsRequest},
//! };
//! # tokio::runtime::Runtime::new()?.block_on(async {
//! let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path("memory://"))).await?;
//! let response = database.catalog(CatalogRequest::ListCollections(ListCollectionsRequest {
//!     namespace: Vec::new(), limit: Some(10), start_after: None,
//! })).await?;
//! assert!(matches!(response, CatalogResponse::Collections(_)));
//! # Ok::<(), storage_kit::StorageError>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Use [`database::catalog`] for collections and schemas, [`database::data`] for
//! records and writes, [`database::query`] for retrieval, and [`database::index`]
//! for index lifecycle. Optional capabilities describe supported families;
//! operation results remain authoritative for each request.
//!
//! # Features
//!
//! LanceDB Database and the embedded Lance StateStore are always compiled as
//! distinct products. Cargo defaults are empty (`default = []`). Alternative backends
//! require explicit `redb`, `postgres`, `sqlite`, or `surrealdb`
//! features. Default and no-default builds both provide Lance alone.
//!
//! # Static composition
//!
//! Advanced DTOs are intentionally absent from the crate root:
//!
//! ```compile_fail
//! use storage_kit::DataRecord;
//! ```
//!
//! Each [`StateStoreBackend`] returns its own associated transaction type.
//! The built-in [`StateTransaction`] remains a unique owner:
//!
//! ```compile_fail
//! let _ = storage_kit::StateTransaction::from_backend;
//! ```
//!
//! ```compile_fail
//! fn require_clone<T: Clone>() {}
//! require_clone::<storage_kit::StateTransaction>();
//! ```
//!
//! RPITIT backend traits are implemented directly rather than exposed as
//! public trait objects:
//!
//! ```compile_fail
//! fn accept_erased(_: &dyn storage_kit::DatabaseBackend) {}
//! ```
#![doc(html_logo_url = "../assets/ai-kit-logo.png")]
#![forbid(unsafe_code)]

mod core;
mod credentials;
pub mod database;
mod database_handle;
mod databases;
pub mod state;
mod state_handle;
mod state_stores;

pub use crate::core::StorageError;
pub use crate::database_handle::{Database, DatabaseBackend};
pub use crate::state_handle::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
};

pub(crate) use crate::core::*;
pub(crate) use crate::credentials::Password;
pub(crate) use crate::databases::config::*;
#[cfg(feature = "redb")]
pub(crate) use crate::state::record::StateNamespace;
pub(crate) use crate::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot, StateRevision,
};
