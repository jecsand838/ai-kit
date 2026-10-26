use std::{fmt, future::Future, sync::Arc, time::Duration};

use crate::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot, StateRevision,
    StateStoreBackend, StateStoreError, state_stores::config::SqliteStateStoreConfig,
};

use super::{
    SqliteStateTransaction, live_transaction::SqliteTransactionActors, schema::SqliteSchema,
};

pub(crate) struct SqliteStateStore {
    database: turso::Database,
    connection: turso::Connection,
    transaction_actors: Arc<SqliteTransactionActors>,
    operation_timeout: Duration,
    operation_timeout_ms: u64,
}

impl fmt::Debug for SqliteStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStateStore")
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl SqliteStateStore {
    pub(crate) async fn connect(config: SqliteStateStoreConfig) -> Result<Self, StateStoreError> {
        config.validate()?;
        let path = config.path_for_turso()?.to_owned();
        let operation_timeout = config.operation_timeout();
        let operation_timeout_ms = config.operation_timeout_ms();
        let should_enable_wal = !config.location().is_memory();
        let error = SqliteErrorContext::new("connect", operation_timeout_ms);

        let database = turso::Builder::new_local(&path)
            .build()
            .await
            .map_err(|source| error.map_turso(source))?;
        let connection = database
            .connect()
            .map_err(|source| error.map_turso(source))?;
        connection
            .busy_timeout(operation_timeout)
            .map_err(|source| error.map_turso(source))?;
        if should_enable_wal {
            Self::initialize_wal(&connection, error).await?;
        }
        SqliteSchema::initialize(&connection, operation_timeout_ms).await?;

        Ok(Self {
            database,
            connection,
            transaction_actors: Arc::new(SqliteTransactionActors::new()),
            operation_timeout,
            operation_timeout_ms,
        })
    }

    async fn with_timeout<T, F>(&self, op: &'static str, fut: F) -> Result<T, StateStoreError>
    where
        F: Future<Output = Result<T, StateStoreError>>,
    {
        match tokio::time::timeout(self.operation_timeout, fut).await {
            Ok(result) => result,
            Err(_) => Err(StateStoreError::timed_out(op, self.operation_timeout_ms)),
        }
    }

    async fn load_record(
        &self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.with_timeout("load", self.load_record_inner(key)).await
    }

    async fn list_records(
        &self,
        request: StateListRequest,
    ) -> Result<StateListResponse, StateStoreError> {
        self.with_timeout("list", self.list_records_inner(request))
            .await
    }

    fn begin_transaction_inner(&self) -> Result<SqliteStateTransaction, StateStoreError> {
        let error = SqliteErrorContext::new("begin_transaction", self.operation_timeout_ms);
        let connection = self
            .database
            .connect()
            .map_err(|source| error.map_turso(source))?;
        connection
            .busy_timeout(self.operation_timeout)
            .map_err(|source| error.map_turso(source))?;
        Ok(SqliteStateTransaction::new(
            connection,
            self.operation_timeout,
            self.operation_timeout_ms,
            &self.transaction_actors,
        ))
    }

    async fn load_record_inner(
        &self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let mut stmt = self
            .connection
            .prepare(
                "SELECT revision, value_json
                 FROM state_records
                 WHERE namespace = ?1 AND key = ?2 AND deleted = 0",
            )
            .await
            .map_err(|source| self.operation_error("load", source))?;
        let mut rows = stmt
            .query((key.namespace().as_str(), key.key()))
            .await
            .map_err(|source| self.operation_error("load", source))?;
        let row = rows
            .next()
            .await
            .map_err(|source| self.operation_error("load", source))?;
        match row {
            Some(row) => {
                let revision = row
                    .get::<i64>(0)
                    .map_err(|source| self.operation_error("load", source))?;
                let value_json = row
                    .get::<String>(1)
                    .map_err(|source| self.operation_error("load", source))?;
                Ok(Some(StateRecordSnapshot {
                    record: StateRecord::new(key, serde_json::from_str(&value_json)?),
                    revision: StateRevision::from_i64(revision)?,
                }))
            }
            None => Ok(None),
        }
    }

    async fn list_records_inner(
        &self,
        request: StateListRequest,
    ) -> Result<StateListResponse, StateStoreError> {
        let mut sql = String::from(
            "SELECT key, revision, value_json
             FROM state_records
             WHERE namespace = ? AND deleted = 0",
        );
        let mut params = Vec::new();
        params.push(turso::Value::Text(request.namespace().as_str().to_owned()));

        if let Some(prefix) = request.key_prefix() {
            sql.push_str(" AND substr(key, 1, length(?2)) = ?2 COLLATE BINARY");
            params.push(turso::Value::Text(prefix.as_str().to_owned()));
        }

        if let Some(start_after) = request.start_after_key() {
            sql.push_str(" AND key > ?");
            params.push(turso::Value::Text(start_after.to_owned()));
        }

        sql.push_str(" ORDER BY key ASC LIMIT ?");
        let limit_with_extra = request.limit().checked_add(1).ok_or_else(|| {
            StateStoreError::InvalidRequest("state list limit overflow".to_owned())
        })?;
        let sqlite_limit = i64::try_from(limit_with_extra).map_err(|err| {
            let mut message = String::from("state list limit conversion failed: ");
            message.push_str(&err.to_string());
            StateStoreError::InvalidRequest(message)
        })?;
        params.push(turso::Value::Integer(sqlite_limit));

        let mut stmt = self
            .connection
            .prepare(&sql)
            .await
            .map_err(|source| self.operation_error("list", source))?;
        let mut rows = stmt
            .query(params)
            .await
            .map_err(|source| self.operation_error("list", source))?;
        let mut records = Vec::new();
        while let Some(row) = rows
            .next()
            .await
            .map_err(|source| self.operation_error("list", source))?
        {
            let key_fragment = row
                .get::<String>(0)
                .map_err(|source| self.operation_error("list", source))?;
            let revision = row
                .get::<i64>(1)
                .map_err(|source| self.operation_error("list", source))?;
            let value_json = row
                .get::<String>(2)
                .map_err(|source| self.operation_error("list", source))?;
            let key = StateKey::new(request.namespace().as_str().to_owned(), key_fragment)?;
            records.push(StateRecordSnapshot {
                record: StateRecord::new(key, serde_json::from_str(&value_json)?),
                revision: StateRevision::from_i64(revision)?,
            });
        }

        let has_more = records.len() > request.limit();
        if has_more {
            records.truncate(request.limit());
        }
        let next_start_after = if has_more {
            records
                .last()
                .map(|snapshot| snapshot.record.key.key().to_owned())
        } else {
            None
        };

        Ok(StateListResponse {
            records,
            next_start_after,
        })
    }

    fn operation_error(&self, op: &'static str, error: turso::Error) -> StateStoreError {
        SqliteErrorContext::new(op, self.operation_timeout_ms).map_turso(error)
    }

    async fn initialize_wal(
        conn: &turso::Connection,
        error: SqliteErrorContext,
    ) -> Result<(), StateStoreError> {
        let mut rows = conn
            .query("PRAGMA journal_mode = 'wal'", ())
            .await
            .map_err(|source| error.map_turso(source))?;
        let row = rows
            .next()
            .await
            .map_err(|source| error.map_turso(source))?;
        let Some(row) = row else {
            return Err(error.backend_failed());
        };
        let journal_mode = row
            .get::<String>(0)
            .map_err(|source| error.map_turso(source))?;
        if journal_mode.eq_ignore_ascii_case("wal") {
            Ok(())
        } else {
            Err(error.backend_failed())
        }
    }
}

impl StateStoreBackend for SqliteStateStore {
    type Transaction = SqliteStateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.load_record(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.list_records(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        self.begin_transaction_inner()
    }
}

#[derive(Clone, Copy)]
pub(super) struct SqliteErrorContext {
    operation: &'static str,
    timeout_ms: u64,
}

impl SqliteErrorContext {
    pub(super) fn new(operation: &'static str, timeout_ms: u64) -> Self {
        Self {
            operation,
            timeout_ms,
        }
    }

    pub(super) fn map_turso(self, error: turso::Error) -> StateStoreError {
        match error {
            turso::Error::Busy(_) => StateStoreError::timed_out(self.operation, self.timeout_ms),
            _ => self.backend_failed(),
        }
    }

    pub(super) fn backend_failed(self) -> StateStoreError {
        StateStoreError::backend_operation(self.operation, "SQLite backend operation failed")
    }

    pub(super) fn actor_unavailable(self) -> StateStoreError {
        StateStoreError::unavailable(self.operation, "SQLite transaction actor is unavailable")
    }
}
