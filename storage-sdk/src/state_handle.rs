//! Private implementation of root state-store owners.

use std::{fmt, future::Future, sync::Arc};

use futures::future::Either;

use crate::state_stores::{self, config::StateStoreConfig};

use crate::state::record::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot,
};

/// Transactional control-plane storage with caller-owned associated transactions.
///
/// Implement this trait directly; the backend need not implement `Clone`.
/// A direct [`Self::load`] or [`Self::list`] observes committed data but is not
/// a fence for a later mutation. Conditional writes must read and validate
/// their preconditions inside the same associated transaction that commits
/// the writes. App and Agent own their record schemas, histories and receipts.
///
/// # Examples
///
/// ```
/// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
/// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
/// use storage_sdk::state::record::{StateKey, StateRecord};
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
/// let key = StateKey::new("workflow", "run/1")?;
/// let mut transaction = store.begin_transaction().await?;
/// transaction.put(StateRecord::new(key.clone(), serde_json::json!({"status": "ready"}))).await?;
/// transaction.commit().await?;
/// assert!(store.load(key).await?.is_some());
/// # Ok::<(), storage_sdk::StateStoreError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Caller-owned store and transaction enums
///
/// This complete composition keeps real embedded-Lance semantics. The caller's
/// `ApplicationStore` selects an application role; its associated `Transaction`
/// selects the matching child. Neither enum implements Clone. Commit and rollback
/// consume the transaction and delegate finalization to its actual owner. This is
/// an illustrative caller composition, not an alternative transaction engine.
///
/// ```
/// use storage_sdk::{StateStore, StateStoreBackend, StateTransaction, StateTransactionBackend, StateStoreError};
/// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
/// use storage_sdk::state::record::{StateKey, StateRecord, StateRecordSnapshot, StateListRequest, StateListResponse};
///
/// enum ApplicationStore { Live(StateStore), Scratch(StateStore) }
/// enum Transaction { Live(StateTransaction), Scratch(StateTransaction) }
/// impl StateStoreBackend for ApplicationStore {
///     type Transaction = Transaction;
///     async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
///         match self { Self::Live(store) => store.load(key).await, Self::Scratch(store) => store.load(key).await }
///     }
///     async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
///         match self { Self::Live(store) => store.list(request).await, Self::Scratch(store) => store.list(request).await }
///     }
///     async fn begin_transaction(&self) -> Result<Transaction, StateStoreError> {
///         match self {
///             Self::Live(store) => Ok(Transaction::Live(store.begin_transaction().await?)),
///             Self::Scratch(store) => Ok(Transaction::Scratch(store.begin_transaction().await?)),
///         }
///     }
/// }
/// impl StateTransactionBackend for Transaction {
///     async fn load(&mut self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
///         match self { Self::Live(tx) => tx.load(key).await, Self::Scratch(tx) => tx.load(key).await }
///     }
///     async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
///         match self { Self::Live(tx) => tx.put(record).await, Self::Scratch(tx) => tx.put(record).await }
///     }
///     async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
///         match self { Self::Live(tx) => tx.delete(key).await, Self::Scratch(tx) => tx.delete(key).await }
///     }
///     async fn commit(self) -> Result<(), StateStoreError> {
///         match self { Self::Live(tx) => tx.commit().await, Self::Scratch(tx) => tx.commit().await }
///     }
///     async fn rollback(self) -> Result<(), StateStoreError> {
///         match self { Self::Live(tx) => tx.rollback().await, Self::Scratch(tx) => tx.rollback().await }
///     }
/// }
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let live = ApplicationStore::Live(StateStore::connect(
///     StateStoreConfig::lance(LanceStateStoreConfig::memory()),
/// ).await?);
/// let scratch = ApplicationStore::Scratch(StateStore::connect(
///     StateStoreConfig::lance(LanceStateStoreConfig::memory()),
/// ).await?);
/// let key = StateKey::new("jobs", "one")?;
/// let mut accepted = live.begin_transaction().await?;
/// accepted.put(StateRecord::new(key.clone(), serde_json::json!("ready"))).await?;
/// accepted.commit().await?;
/// assert_eq!(live.load(key.clone()).await?.unwrap().revision.get(), 1);
/// let mut discarded = scratch.begin_transaction().await?;
/// discarded.put(StateRecord::new(key.clone(), serde_json::json!("draft"))).await?;
/// assert!(discarded.load(key.clone()).await?.is_some());
/// discarded.rollback().await?;
/// assert!(scratch.load(key).await?.is_none());
/// drop((live, scratch));
/// # Ok::<(), StateStoreError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait StateStoreBackend: Send + Sync + 'static {
    /// Unique owned transaction returned by this store.
    type Transaction: StateTransactionBackend;

    /// Loads the current non-deleted value for a key.
    ///
    /// # Errors
    ///
    /// Returns the owning [`StateStoreError`] when the read cannot complete,
    /// including unavailable storage, timeout, or record decoding failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::StateKey;
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// assert!(store.load(key).await?.is_none());
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn load(
        &self,
        key: StateKey,
    ) -> impl Future<Output = Result<Option<StateRecordSnapshot>, StateStoreError>> + Send + '_;

    /// Lists present records in canonical ascending UTF-8 key order.
    ///
    /// Namespace and prefix matching are exact and case-sensitive; wildcard
    /// characters in a prefix are literal. The cursor is exclusive. A
    /// continuation is the last returned key only when another matching
    /// record exists in this page's view. Separate pages are not a snapshot;
    /// callers must fence/revalidate inventories that can change concurrently.
    ///
    /// # Errors
    ///
    /// Returns the owning [`StateStoreError`] when the page cannot be read or
    /// decoded, or the configured operation deadline expires.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::{StateKey, StateRecord, StateListRequest};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// let mut transaction = store.begin_transaction().await?;
    /// transaction.put(StateRecord::new(key, serde_json::json!(42))).await?;
    /// transaction.commit().await?;
    /// let page = store.list(StateListRequest::new("workflow")?.with_limit(10)?).await?;
    /// assert_eq!(page.records.len(), 1);
    /// assert!(page.next_start_after.is_none());
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn list(
        &self,
        request: StateListRequest,
    ) -> impl Future<Output = Result<StateListResponse, StateStoreError>> + Send + '_;

    /// Opens an owned transaction whose successful outcomes are serializable.
    ///
    /// Admission may wait or subsequent operations may report an exact
    /// [`StateStoreError::TransactionConflict`]. The returned transaction's
    /// reads, including absent keys, participate in its serializable outcome.
    /// The native transaction may begin lazily at the first operation.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError`] if admission or native transaction setup fails,
    /// times out, or encounters a serialization conflict.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let transaction = store.begin_transaction().await?;
    /// transaction.rollback().await?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn begin_transaction(
        &self,
    ) -> impl Future<Output = Result<Self::Transaction, StateStoreError>> + Send + '_;
}

/// A unique, lifetime-free transaction; commit and rollback consume the owner.
///
/// Successful commits atomically publish every staged mutation with a
/// serializable read/write outcome, including missing-key observations.
/// Reads see this transaction's preceding writes and tombstones. A failed or
/// cancelled nonterminal operation invalidates further writes/commit and
/// requires rollback cleanup. Dropping an uncommitted owner never commits.
/// An admitted commit retains finalization authority after waiter loss.
///
/// The trait deliberately has no list or native snapshot operation. Upper
/// consumers build history from their own immutable records and fenced heads.
///
/// # Examples
///
/// ```
/// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
/// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
/// use storage_sdk::state::record::{StateKey, StateRecord};
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
/// let key = StateKey::new("workflow", "run/1")?;
/// let mut transaction = store.begin_transaction().await?;
/// transaction.put(StateRecord::new(key.clone(), serde_json::json!(42))).await?;
/// assert!(transaction.load(key.clone()).await?.is_some());
/// transaction.rollback().await?;
/// assert!(store.load(key).await?.is_none());
/// # Ok::<(), storage_sdk::StateStoreError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait StateTransactionBackend: Send + 'static {
    /// Loads through the transaction, including earlier writes and tombstones.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError`] for a conflict, timeout, decoding failure, or
    /// unavailable transaction. A failed nonterminal operation invalidates the
    /// transaction; roll it back or drop it instead of attempting commit.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::{StateKey, StateRecord};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// let mut transaction = store.begin_transaction().await?;
    /// transaction.put(StateRecord::new(key.clone(), serde_json::json!(42))).await?;
    /// let observed = transaction.load(key).await?.expect("staged record is visible");
    /// assert_eq!(observed.record.value.as_ref(), &serde_json::json!(42));
    /// transaction.rollback().await?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn load(
        &mut self,
        key: StateKey,
    ) -> impl Future<Output = Result<Option<StateRecordSnapshot>, StateStoreError>> + Send + '_;

    /// Stages a present record and advances its key revision by exactly one.
    ///
    /// The first put or delete starts at revision one. Repeated writes count
    /// individually, including equal values; a retained tombstone is not a
    /// fresh key. Rollback discards staged revisions with the staged values.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError`] for a revision boundary, serialization failure,
    /// conflict, timeout, or unavailable transaction. A failure invalidates the
    /// transaction, so earlier staged writes cannot subsequently commit.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::{StateKey, StateRecord};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// let mut transaction = store.begin_transaction().await?;
    /// transaction.put(StateRecord::new(key.clone(), serde_json::json!(42))).await?;
    /// assert_eq!(transaction.load(key).await?.expect("staged record").revision.get(), 1);
    /// transaction.commit().await?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn put(
        &mut self,
        record: StateRecord,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send + '_;

    /// Stages a tombstone and advances the revision, including for an absent key.
    ///
    /// Loads then return `None`. A later put increments the retained revision
    /// instead of resetting it, preventing delete/recreate ABA equality.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError`] for a revision boundary, conflict, timeout, or
    /// unavailable transaction. A failure invalidates the transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::{StateKey, StateRecord};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// let mut transaction = store.begin_transaction().await?;
    /// transaction.delete(key.clone()).await?;
    /// assert!(transaction.load(key.clone()).await?.is_none());
    /// transaction.put(StateRecord::new(key.clone(), serde_json::json!(42))).await?;
    /// assert_eq!(transaction.load(key).await?.expect("recreated record").revision.get(), 2);
    /// transaction.commit().await?;
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn delete(
        &mut self,
        key: StateKey,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send + '_;

    /// Atomically commits all staged mutations and completes native cleanup.
    ///
    /// `Ok(())` proves commit. A conflict proves no mutations committed. A
    /// timeout, transport/finalization error or lost waiter can leave the
    /// outcome uncertain; it does not prove rollback. Reconcile an upper
    /// consumer's durable receipt before retrying potentially committed work.
    /// Dropping the future after admission cannot revoke commit authority.
    ///
    /// # Errors
    ///
    /// [`StateStoreError::TransactionConflict`] proves this transaction did not
    /// publish. Timeout, transport, or finalization errors can leave the outcome
    /// uncertain; inspect a durable application receipt before retrying. A
    /// previously failed nonterminal operation cannot publish its staged writes.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::{StateKey, StateRecord};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// let mut transaction = store.begin_transaction().await?;
    /// transaction.put(StateRecord::new(key.clone(), serde_json::json!(42))).await?;
    /// transaction.commit().await?;
    /// assert!(store.load(key).await?.is_some());
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn commit(self) -> impl Future<Output = Result<(), StateStoreError>> + Send
    where
        Self: Sized;

    /// Rolls back all staged mutations and completes rollback cleanup.
    ///
    /// Success leaves every committed key and revision unchanged. A primary
    /// failure must remain distinguishable from a cleanup failure.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError`] if native rollback or resource cleanup fails.
    /// Preserve any earlier operation failure as the primary error when reporting
    /// a separate rollback failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::{StateKey, StateRecord};
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// let key = StateKey::new("workflow", "run/1")?;
    /// let mut transaction = store.begin_transaction().await?;
    /// transaction.put(StateRecord::new(key.clone(), serde_json::json!(42))).await?;
    /// transaction.rollback().await?;
    /// assert!(store.load(key).await?.is_none());
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn rollback(self) -> impl Future<Output = Result<(), StateStoreError>> + Send
    where
        Self: Sized;
}

/// Cloneable opaque state-store handle.
///
/// Clones share the same store. Bring [`StateStoreBackend`] into scope for
/// reads, listing, and transaction creation; transaction behavior is provided
/// by [`StateTransactionBackend`]. This handle contains built-ins only.
///
/// # Examples
///
/// ```
/// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
/// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
/// use storage_sdk::state::record::{StateKey, StateRecord};
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
/// let shared = store.clone();
/// let key = StateKey::new("workflow", "run/1")?;
/// let mut transaction = shared.begin_transaction().await?;
/// transaction.put(StateRecord::new(key.clone(), serde_json::json!(42))).await?;
/// transaction.commit().await?;
/// assert!(store.load(key).await?.is_some());
/// # Ok::<(), storage_sdk::StateStoreError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct StateStore {
    inner: Arc<StateStoreInner>,
}

enum StateStoreInner {
    Lance(state_stores::lance::LanceStateStore),
    #[cfg(feature = "redb")]
    Redb(state_stores::redb::RedbStateStore),
    #[cfg(feature = "postgres")]
    Postgres(state_stores::postgres::PostgresStateStore),
    #[cfg(feature = "sqlite")]
    Sqlite(state_stores::sqlite::SqliteStateStore),
}

impl StateStore {
    /// Opens a built-in state store.
    ///
    /// Configuration is validated before opening the selected backend. Lance
    /// is always available. Redb, PostgreSQL, and SQLite/Turso require `redb`,
    /// `postgres`, and `sqlite`, respectively. A disabled implementation returns
    /// [`StateStoreError::InvalidConfig`] before opening native resources.
    ///
    /// # Errors
    ///
    /// Returns [`StateStoreError::InvalidConfig`] for invalid configuration, or
    /// the owning I/O, connection, setup, or timeout error when opening fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
    /// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
    /// use storage_sdk::state::record::StateKey;
    /// # tokio::runtime::Runtime::new()?.block_on(async {
    /// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
    /// assert!(store.load(StateKey::new("workflow", "run/1")?).await?.is_none());
    /// # Ok::<(), storage_sdk::StateStoreError>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn connect(config: StateStoreConfig) -> Result<Self, StateStoreError> {
        config.validate()?;
        match config {
            StateStoreConfig::Lance(config) => {
                state_stores::lance::LanceStateStore::connect(config)
                    .await
                    .map(Self::from_lance)
            }
            StateStoreConfig::Redb(config) => {
                if cfg!(feature = "redb") {
                    #[cfg(feature = "redb")]
                    return state_stores::redb::RedbStateStore::connect(config)
                        .await
                        .map(Self::from_redb);
                }
                drop(config);
                Err(StateStoreError::InvalidConfig(
                    "Redb requires the `redb` feature".into(),
                ))
            }
            StateStoreConfig::Postgres(config) => {
                if cfg!(feature = "postgres") {
                    #[cfg(feature = "postgres")]
                    return state_stores::postgres::PostgresStateStore::connect(config)
                        .await
                        .map(Self::from_postgres);
                }
                drop(config);
                Err(StateStoreError::InvalidConfig(
                    "PostgreSQL requires the `postgres` feature".into(),
                ))
            }
            #[cfg(feature = "sqlite")]
            StateStoreConfig::Sqlite(config) => {
                state_stores::sqlite::SqliteStateStore::connect(config)
                    .await
                    .map(Self::from_sqlite)
            }
        }
    }

    #[cfg(feature = "redb")]
    fn from_redb(store: state_stores::redb::RedbStateStore) -> Self {
        Self {
            inner: Arc::new(StateStoreInner::Redb(store)),
        }
    }

    fn from_lance(store: state_stores::lance::LanceStateStore) -> Self {
        Self {
            inner: Arc::new(StateStoreInner::Lance(store)),
        }
    }

    #[cfg(feature = "postgres")]
    fn from_postgres(store: state_stores::postgres::PostgresStateStore) -> Self {
        Self {
            inner: Arc::new(StateStoreInner::Postgres(store)),
        }
    }

    #[cfg(feature = "sqlite")]
    fn from_sqlite(store: state_stores::sqlite::SqliteStateStore) -> Self {
        Self {
            inner: Arc::new(StateStoreInner::Sqlite(store)),
        }
    }
}

impl fmt::Debug for StateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StateStore { .. }")
    }
}

impl StateStoreBackend for StateStore {
    type Transaction = StateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.inner.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        self.inner.begin_transaction().await
    }
}

impl StateStoreBackend for StateStoreInner {
    type Transaction = StateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::Lance(store) => store.load(key).await,
            #[cfg(feature = "redb")]
            Self::Redb(store) => store.load(key).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.load(key).await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => store.load(key).await,
        }
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        match self {
            Self::Lance(store) => store.list(request).await,
            #[cfg(feature = "redb")]
            Self::Redb(store) => store.list(request).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.list(request).await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => store.list(request).await,
        }
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        match self {
            Self::Lance(store) => store
                .begin_transaction()
                .await
                .map(StateTransaction::from_lance),
            #[cfg(feature = "redb")]
            Self::Redb(store) => store
                .begin_transaction()
                .await
                .map(StateTransaction::from_redb),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store
                .begin_transaction()
                .await
                .map(StateTransaction::from_postgres),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(store) => store
                .begin_transaction()
                .await
                .map(StateTransaction::from_sqlite),
        }
    }
}

/// Unique, lifetime-free opaque state transaction.
///
/// Obtain this handle from [`StateStoreBackend::begin_transaction`]. Import
/// [`StateTransactionBackend`] to read, stage writes, and consume it through
/// commit or rollback. It is not cloneable; dropping an uncommitted handle
/// settles rollback cleanup, while an admitted commit retains finalization
/// authority even if its reply is lost.
///
/// # Examples
///
/// ```
/// use storage_sdk::{StateStore, StateStoreBackend, StateTransactionBackend};
/// use storage_sdk::state::config::{StateStoreConfig, LanceStateStoreConfig};
/// # tokio::runtime::Runtime::new()?.block_on(async {
/// let store = StateStore::connect(StateStoreConfig::lance(LanceStateStoreConfig::memory())).await?;
/// let transaction = store.begin_transaction().await?;
/// transaction.rollback().await?;
/// # Ok::<(), storage_sdk::StateStoreError>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct StateTransaction {
    inner: StateTransactionInner,
}

enum StateTransactionInner {
    Lance(state_stores::lance::LanceStateTransaction),
    #[cfg(feature = "redb")]
    Redb(state_stores::redb::RedbStateTransaction),
    #[cfg(feature = "postgres")]
    Postgres(state_stores::postgres::PostgresStateTransaction),
    #[cfg(feature = "sqlite")]
    Sqlite(state_stores::sqlite::SqliteStateTransaction),
}

impl StateTransaction {
    fn from_lance(transaction: state_stores::lance::LanceStateTransaction) -> Self {
        Self {
            inner: StateTransactionInner::Lance(transaction),
        }
    }

    #[cfg(feature = "redb")]
    fn from_redb(transaction: state_stores::redb::RedbStateTransaction) -> Self {
        Self {
            inner: StateTransactionInner::Redb(transaction),
        }
    }

    #[cfg(feature = "postgres")]
    fn from_postgres(transaction: state_stores::postgres::PostgresStateTransaction) -> Self {
        Self {
            inner: StateTransactionInner::Postgres(transaction),
        }
    }

    #[cfg(feature = "sqlite")]
    fn from_sqlite(transaction: state_stores::sqlite::SqliteStateTransaction) -> Self {
        Self {
            inner: StateTransactionInner::Sqlite(transaction),
        }
    }
}

impl fmt::Debug for StateTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StateTransaction { .. }")
    }
}

impl StateTransactionBackend for StateTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.inner.load(key).await
    }

    fn put(
        &mut self,
        record: StateRecord,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send {
        self.inner.put(record)
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.inner.delete(key).await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        self.inner.rollback().await
    }
}

impl StateTransactionBackend for StateTransactionInner {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self {
            Self::Lance(transaction) => transaction.load(key).await,
            #[cfg(feature = "redb")]
            Self::Redb(transaction) => transaction.load(key).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(transaction) => transaction.load(key).await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(transaction) => transaction.load(key).await,
        }
    }

    fn put(
        &mut self,
        record: StateRecord,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send {
        // Construct Lance's future before returning so it owns deep JSON even
        // if the caller drops this operation without polling it.
        let prepared: Either<_, (&mut Self, StateRecord)> = match self {
            Self::Lance(transaction) => Either::Left(transaction.put(record)),
            #[cfg(feature = "redb")]
            transaction @ Self::Redb(_) => Either::Right((transaction, record)),
            #[cfg(feature = "postgres")]
            transaction @ Self::Postgres(_) => Either::Right((transaction, record)),
            #[cfg(feature = "sqlite")]
            transaction @ Self::Sqlite(_) => Either::Right((transaction, record)),
        };
        async move {
            match prepared {
                Either::Left(operation) => operation.await,
                Either::Right((transaction, record)) => match transaction {
                    Self::Lance(transaction) => transaction.put(record).await,
                    #[cfg(feature = "redb")]
                    Self::Redb(transaction) => transaction.put(record).await,
                    #[cfg(feature = "postgres")]
                    Self::Postgres(transaction) => transaction.put(record).await,
                    #[cfg(feature = "sqlite")]
                    Self::Sqlite(transaction) => transaction.put(record).await,
                },
            }
        }
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        match self {
            Self::Lance(transaction) => transaction.delete(key).await,
            #[cfg(feature = "redb")]
            Self::Redb(transaction) => transaction.delete(key).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(transaction) => transaction.delete(key).await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(transaction) => transaction.delete(key).await,
        }
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        match self {
            Self::Lance(transaction) => transaction.commit().await,
            #[cfg(feature = "redb")]
            Self::Redb(transaction) => transaction.commit().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(transaction) => transaction.commit().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(transaction) => transaction.commit().await,
        }
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        match self {
            Self::Lance(transaction) => transaction.rollback().await,
            #[cfg(feature = "redb")]
            Self::Redb(transaction) => transaction.rollback().await,
            #[cfg(feature = "postgres")]
            Self::Postgres(transaction) => transaction.rollback().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(transaction) => transaction.rollback().await,
        }
    }
}

/// Portable state-store error.
///
/// A [`Self::TransactionConflict`] establishes non-publication of that
/// transaction. A timeout, unavailable connection, or backend finalization
/// error does not by itself establish rollback. Reconcile durable application
/// state before retrying a potentially committed mutation. Error messages
/// provided by caller backends must not contain credentials or native payloads.
///
/// # Examples
///
/// ```
/// use storage_sdk::{StateStoreError};
/// let error = StateStoreError::transaction_conflict("commit", "concurrent write");
/// assert!(matches!(error, StateStoreError::TransactionConflict { .. }));
/// ```
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StateStoreError {
    /// Invalid configuration.
    #[error("invalid state store configuration: {0}")]
    InvalidConfig(String),
    /// Invalid caller request.
    #[error("invalid state store request: {0}")]
    InvalidRequest(String),
    /// Backend unavailable.
    #[error("state store is unavailable during `{operation}`: {message}")]
    Unavailable {
        /// Portable operation.
        operation: String,
        /// Secret-safe message.
        message: String,
    },
    /// Backend operation failure.
    #[error("state store operation `{operation}` failed: {message}")]
    BackendOperation {
        /// Portable operation.
        operation: String,
        /// Secret-safe message.
        message: String,
    },
    /// Exact transaction conflict.
    #[error("state store transaction `{operation}` conflicted: {message}")]
    TransactionConflict {
        /// Portable operation.
        operation: String,
        /// Secret-safe message.
        message: String,
    },
    /// Configured timeout elapsed.
    #[error("state store operation `{operation}` timed out after {timeout_ms} ms")]
    TimedOut {
        /// Portable operation.
        operation: String,
        /// Timeout in milliseconds.
        timeout_ms: u64,
    },
    /// JSON serialization failed.
    #[error("state store serialization failed: {0}")]
    Serialization(String),
}

impl StateStoreError {
    /// Creates an invalid-config error.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStoreError};
    /// let error = StateStoreError::invalid_config("timeout must be positive");
    /// assert!(matches!(error, StateStoreError::InvalidConfig(_)));
    /// ```
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig(message.into())
    }

    /// Creates an invalid-request error.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStoreError};
    /// let error = StateStoreError::invalid_request("key is empty");
    /// assert!(matches!(error, StateStoreError::InvalidRequest(_)));
    /// ```
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest(message.into())
    }

    /// Creates an unavailable error.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStoreError};
    /// let error = StateStoreError::unavailable("load", "connection is closed");
    /// assert!(matches!(error, StateStoreError::Unavailable { .. }));
    /// ```
    pub fn unavailable(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Unavailable {
            operation: operation.into(),
            message: message.into(),
        }
    }

    /// Creates a backend-operation error.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStoreError};
    /// let error = StateStoreError::backend_operation("load", "record could not be read");
    /// assert!(matches!(error, StateStoreError::BackendOperation { .. }));
    /// ```
    pub fn backend_operation(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::BackendOperation {
            operation: operation.into(),
            message: message.into(),
        }
    }

    /// Creates an exact transaction-conflict error.
    ///
    /// Use this category only when the transaction did not publish its mutations.
    /// Do not convert every concurrent timeout or transport failure into conflict.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStoreError};
    /// let error = StateStoreError::transaction_conflict("commit", "concurrent write");
    /// assert!(matches!(error, StateStoreError::TransactionConflict { .. }));
    /// ```
    pub fn transaction_conflict(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::TransactionConflict {
            operation: operation.into(),
            message: message.into(),
        }
    }

    /// Creates a timeout error.
    ///
    /// This records a deadline failure; it does not establish whether a commit
    /// admitted before the timeout eventually published.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStoreError};
    /// let error = StateStoreError::timed_out("commit", 500);
    /// assert!(matches!(error, StateStoreError::TimedOut { timeout_ms: 500, .. }));
    /// ```
    pub fn timed_out(operation: impl Into<String>, timeout_ms: u64) -> Self {
        Self::TimedOut {
            operation: operation.into(),
            timeout_ms,
        }
    }

    /// Creates a serialization error.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::{StateStoreError};
    /// let error = StateStoreError::serialization("invalid record encoding");
    /// assert!(matches!(error, StateStoreError::Serialization(_)));
    /// ```
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization(message.into())
    }
}

impl From<serde_json::Error> for StateStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::serialization(error.to_string())
    }
}
