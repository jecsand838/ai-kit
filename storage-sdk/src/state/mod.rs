//! Revisioned control-plane state and owned transactions.
//!
//! Open a supplied [`crate::StateStore`] from [`config::StateStoreConfig`],
//! import [`crate::StateStoreBackend`] for reads and transaction creation, and
//! import [`crate::StateTransactionBackend`] to stage and finalize mutations.
//! [`record`] contains validated identities, write inputs, observations, and
//! pagination. Transactions remain unique, lifetime-free values and are
//! consumed by commit or rollback.
//!
//! Read conditional-write preconditions inside the committing transaction.
//! Ordinary reads and separate listing pages do not establish a snapshot.
//! A lost commit reply may require durable reconciliation before retrying.

//! # Preconditions, revisions and pagination
//!
//! This runs on embedded Lance without a service. Conditional writes validate their
//! precondition inside the same transaction. Rollback consumes the transaction and
//! discards its staged revision. Do not automatically retry an ambiguous commit.
//!
//! ```
//! use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
//! use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
//! use storage_sdk::state::record::{StateKey, StateListRequest, StateRecord};
//! # tokio::runtime::Runtime::new()?.block_on(async {
//! let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
//! let key = StateKey::new("jobs", "a")?;
//! let mut create = store.begin_transaction().await?;
//! assert!(create.load(key.clone()).await?.is_none());
//! create.put(StateRecord::new(key.clone(), serde_json::json!({"status":"ready"}))).await?;
//! create.put(StateRecord::new(StateKey::new("jobs", "b")?, serde_json::json!({"status":"ready"}))).await?;
//! create.commit().await?;
//!
//! let mut update = store.begin_transaction().await?;
//! let before = update.load(key.clone()).await?.expect("created job");
//! assert_eq!(before.revision.get(), 1);
//! assert_eq!(before.record.value["status"], "ready");
//! update.put(StateRecord::new(key.clone(), serde_json::json!({"status":"running"}))).await?;
//! assert_eq!(update.load(key.clone()).await?.unwrap().revision.get(), 2);
//! update.rollback().await?;
//! assert_eq!(store.load(key.clone()).await?.unwrap().revision.get(), 1);
//!
//! let first = store.list(StateListRequest::new("jobs")?.with_limit(1)?).await?;
//! assert_eq!(first.records.len(), 1);
//! let cursor = first.next_start_after.expect("one remaining job");
//! let second = store.list(StateListRequest::new("jobs")?.with_limit(1)?.start_after(cursor)?).await?;
//! assert_eq!(second.records.len(), 1);
//! assert!(second.next_start_after.is_none());
//! // Separate pages observe committed state independently, not one frozen snapshot.
//! let mut replace = store.begin_transaction().await?;
//! replace.delete(key.clone()).await?;
//! assert!(replace.load(key.clone()).await?.is_none());
//! replace.put(StateRecord::new(key.clone(), serde_json::json!({"status":"new"}))).await?;
//! replace.commit().await?;
//! assert_eq!(store.load(key).await?.unwrap().revision.get(), 3);
//! drop(store);
//! # Ok::<(), storage_sdk::StateStoreError>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! A caller detecting a stale application precondition should roll back instead of
//! writing. Native serialization conflicts use [`crate::StateStoreError::TransactionConflict`];
//! a timeout or lost commit reply does not establish that the write was rolled back.
//!
//! # Directory-backed reopen
//!
//! Memory belongs to one connected owner and its clones. Directory connections
//! reopen persisted records. This example owns its temporary directory, consumes
//! all transactions and drops both store handles before removing that directory.
//! StateStore has no public shutdown operation; final-handle cleanup belongs to
//! the selected implementation.
//!
//! ```
//! use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
//! use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};
//! use storage_sdk::state::record::{StateKey, StateRecord};
//! let directory = std::env::temp_dir().join(format!("ai-kit-state-doc-{}", uuid::Uuid::new_v4()));
//! std::fs::create_dir(&directory)?;
//! let result = tokio::runtime::Runtime::new()?.block_on(async {
//!     let config = StateStoreConfig::lance(LanceStateStoreConfig::directory(&directory));
//!     let store = StateStore::connect(config.clone()).await?;
//!     let key = StateKey::new("settings", "theme")?;
//!     let mut transaction = store.begin_transaction().await?;
//!     transaction.put(StateRecord::new(key.clone(), serde_json::json!("dark"))).await?;
//!     transaction.commit().await?;
//!     drop(store);
//!     let reopened = StateStore::connect(config).await?;
//!     let observed = reopened.load(key).await?.expect("durable record");
//!     assert_eq!(observed.record.value.as_ref(), &serde_json::json!("dark"));
//!     assert_eq!(observed.revision.get(), 1);
//!     drop(reopened);
//!     Ok::<(), storage_sdk::StateStoreError>(())
//! });
//! let cleanup = std::fs::remove_dir_all(&directory);
//! result?; // Preserve the operation error if cleanup also failed.
//! cleanup?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Custom stores retain their own concrete associated transaction. The complete
//! [composition example](crate::StateStoreBackend) delegates native commit and
//! rollback instead of inventing transaction guarantees.

/// Built-in state-store construction data.
///
/// Lance is always available. Redb, PostgreSQL, and SQLite/Turso require
/// explicit `redb`, `postgres`, and `sqlite` features. Constructors and validation perform no connection I/O;
/// [`crate::StateStore::connect`] opens the selected store.
pub mod config {
    pub use crate::credentials::Password;
    pub use crate::state_stores::config::{
        LanceStateStoreConfig, LanceStateStoreLocation, PostgresStateStoreConfig,
        PostgresStateStoreTlsMode, RedbStateStoreConfig, RedbStateStoreLocation, StateStoreConfig,
    };
    #[cfg(feature = "sqlite")]
    pub use crate::state_stores::config::{SqliteStateStoreConfig, SqliteStateStoreLocation};
}

/// Revisioned records, keys, and pagination.
pub mod record;

/// The unique owned transaction handle.
///
/// [`StateTransaction`](crate::state::transaction::StateTransaction) implements [`crate::StateTransactionBackend`]. Obtain
/// it from [`crate::StateStoreBackend::begin_transaction`]; it has no public
/// constructor. Caller-written stores return their own associated transaction
/// type instead of constructing this supplied handle.
pub mod transaction {
    pub use crate::state_handle::StateTransaction;
}
