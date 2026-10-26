use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use secrecy::ExposeSecret;
use tokio::task::JoinHandle;
use tokio_postgres::error::SqlState;

use crate::{
    StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot, StateRevision,
    StateStoreBackend, StateStoreError, state_stores::config::PostgresStateStoreConfig,
};

use super::{
    PostgresStateTransaction, live_transaction::PostgresTransactionActors, schema::PostgresSchema,
};

pub(crate) struct PostgresStateStore {
    settings: PostgresConnectionSettings,
    connection: PostgresConnection,
    transaction_actors: Arc<PostgresTransactionActors>,
}

impl fmt::Debug for PostgresStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresStateStore")
            .field("operation_timeout", &self.settings.operation_timeout())
            .finish_non_exhaustive()
    }
}

pub(super) struct PostgresConnection {
    client: tokio_postgres::Client,
    driver: Option<JoinHandle<Result<(), StateStoreError>>>,
}

impl PostgresConnection {
    fn new(
        client: tokio_postgres::Client,
        driver: JoinHandle<Result<(), StateStoreError>>,
    ) -> Self {
        Self {
            client,
            driver: Some(driver),
        }
    }

    pub(super) fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    pub(super) fn driver(&mut self) -> Option<&mut JoinHandle<Result<(), StateStoreError>>> {
        self.driver.as_mut()
    }

    pub(super) fn observe_driver_completion(
        &mut self,
        result: Result<Result<(), StateStoreError>, tokio::task::JoinError>,
    ) {
        let _completed_driver = self.driver.take();
        let _driver_result = result;
    }

    pub(super) async fn shutdown(mut self) {
        if let Some(driver) = self.driver.take() {
            driver.abort();
            let _driver_result = driver.await;
        }
    }
}

impl Drop for PostgresConnection {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.as_ref() {
            driver.abort();
        }
    }
}

struct PostgresConnectionSettings {
    config: Box<PostgresStateStoreConfig>,
}

impl PostgresConnectionSettings {
    fn new(config: PostgresStateStoreConfig) -> Result<Self, StateStoreError> {
        config.validate()?;
        Ok(Self {
            config: Box::new(config),
        })
    }

    fn operation_timeout(&self) -> Duration {
        self.config.operation_timeout()
    }

    fn operation_timeout_ms(&self) -> u64 {
        self.config.operation_timeout_ms()
    }

    async fn open(&self, operation: &'static str) -> Result<PostgresConnection, StateStoreError> {
        let mut config = tokio_postgres::Config::new();
        config.host(self.config.host());
        config.port(self.config.port());
        config.dbname(self.config.database());
        config.user(self.config.username());
        config.connect_timeout(self.operation_timeout());
        if let Some(password) = self.config.password() {
            config.password(password.as_secret().expose_secret());
        }

        let error_context = PostgresErrorContext::new(operation, self.operation_timeout_ms());
        let (client, connection) = config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(|error| error_context.postgres(error))?;
        let driver_error_context =
            PostgresErrorContext::new("connection_driver", self.operation_timeout_ms());
        let driver = tokio::spawn(async move {
            connection
                .await
                .map_err(|error| driver_error_context.postgres(error))
        });

        Ok(PostgresConnection::new(client, driver))
    }
}

impl Clone for PostgresConnectionSettings {
    fn clone(&self) -> Self {
        Self {
            config: Box::new((*self.config).clone()),
        }
    }
}

impl fmt::Debug for PostgresConnectionSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnectionSettings")
            .field("operation_timeout", &self.operation_timeout())
            .finish_non_exhaustive()
    }
}

pub(super) struct PostgresErrorContext {
    operation: &'static str,
    timeout_ms: u64,
}

impl PostgresErrorContext {
    pub(super) fn new(operation: &'static str, timeout_ms: u64) -> Self {
        Self {
            operation,
            timeout_ms,
        }
    }

    pub(super) fn postgres(&self, error: tokio_postgres::Error) -> StateStoreError {
        if let Some(database_error) = error.as_db_error() {
            let code = database_error.code();
            if code == &SqlState::QUERY_CANCELED || code == &SqlState::LOCK_NOT_AVAILABLE {
                return StateStoreError::timed_out(self.operation, self.timeout_ms);
            }
            if code == &SqlState::T_R_SERIALIZATION_FAILURE
                || code == &SqlState::T_R_DEADLOCK_DETECTED
                || code == &SqlState::UNIQUE_VIOLATION
            {
                return StateStoreError::transaction_conflict(
                    self.operation,
                    "state transaction conflict",
                );
            }
        }
        if error.is_closed() {
            return StateStoreError::unavailable(
                self.operation,
                "state store connection is unavailable",
            );
        }
        self.backend_failure()
    }

    pub(super) fn backend_failure(&self) -> StateStoreError {
        StateStoreError::backend_operation(self.operation, "state store backend operation failed")
    }

    pub(super) fn current_time_ms(&self) -> Result<i64, StateStoreError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| self.backend_failure())?;
        i64::try_from(duration.as_millis()).map_err(|_| self.backend_failure())
    }
}

impl PostgresStateStore {
    pub(crate) async fn connect(config: PostgresStateStoreConfig) -> Result<Self, StateStoreError> {
        let settings = PostgresConnectionSettings::new(config)?;
        let connection = settings.open("connect").await?;
        let store = Self {
            settings,
            connection,
            transaction_actors: Arc::new(PostgresTransactionActors::new()),
        };

        store.configure_session_timeouts().await?;
        PostgresSchema::initialize(
            store.connection.client(),
            store.settings.operation_timeout_ms(),
        )
        .await?;

        Ok(store)
    }

    async fn configure_session_timeouts(&self) -> Result<(), StateStoreError> {
        let timeout = self.settings.operation_timeout_ms().to_string();
        let error_context = self.error_context("connect");
        self.connection
            .client()
            .execute(
                "SELECT set_config('statement_timeout', $1, false)",
                &[&timeout],
            )
            .await
            .map_err(|error| error_context.postgres(error))?;
        self.connection
            .client()
            .execute("SELECT set_config('lock_timeout', $1, false)", &[&timeout])
            .await
            .map_err(|error| error_context.postgres(error))?;
        Ok(())
    }

    async fn load_record(
        &self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.load_record_inner(key).await
    }

    async fn list_records(
        &self,
        request: StateListRequest,
    ) -> Result<StateListResponse, StateStoreError> {
        self.list_records_inner(request).await
    }

    async fn begin_transaction_inner(&self) -> Result<PostgresStateTransaction, StateStoreError> {
        let connection = self.settings.open("begin_transaction").await?;
        PostgresStateTransaction::start(
            connection,
            self.settings.operation_timeout_ms(),
            &self.transaction_actors,
        )
        .await
    }

    async fn load_record_inner(
        &self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let row = self
            .connection
            .client()
            .query_opt(
                "SELECT revision, value_json
                 FROM storage_sdk_state_records
                 WHERE namespace = $1 AND key = $2 AND deleted = FALSE",
                &[&key.namespace().as_str(), &key.key()],
            )
            .await
            .map_err(|error| self.error_context("load").postgres(error))?;
        match row {
            Some(row) => {
                let revision = row
                    .try_get::<_, i64>(0)
                    .map_err(|error| self.error_context("load").postgres(error))?;
                let value_json = row
                    .try_get::<_, String>(1)
                    .map_err(|error| self.error_context("load").postgres(error))?;
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
        let query = PostgresListQuery::new(&request);
        let prefix = query.escaped_prefix();
        let cursor = request.start_after_key();
        let limit = query.limit_with_extra()?;
        // Compare the same UTF-8 bytes for both pagination and ordering,
        // independent of the database's collation and storage encoding.
        let rows = self
            .connection
            .client()
            .query(
                "SELECT key, revision, value_json
                 FROM storage_sdk_state_records
                 WHERE namespace = $1
                   AND deleted = FALSE
                   AND ($2::TEXT IS NULL OR key LIKE $2 ESCAPE '\\')
                   AND ($3::TEXT IS NULL
                        OR convert_to(key, 'UTF8') > convert_to($3, 'UTF8'))
                 ORDER BY convert_to(key, 'UTF8') ASC
                 LIMIT $4",
                &[&request.namespace().as_str(), &prefix, &cursor, &limit],
            )
            .await
            .map_err(|error| self.error_context("list").postgres(error))?;

        let mut records = Vec::with_capacity(rows.len().min(request.limit()));
        for row in rows {
            let key_fragment = row
                .try_get::<_, String>(0)
                .map_err(|error| self.error_context("list").postgres(error))?;
            let revision = row
                .try_get::<_, i64>(1)
                .map_err(|error| self.error_context("list").postgres(error))?;
            let value_json = row
                .try_get::<_, String>(2)
                .map_err(|error| self.error_context("list").postgres(error))?;
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

    fn error_context(&self, operation: &'static str) -> PostgresErrorContext {
        PostgresErrorContext::new(operation, self.settings.operation_timeout_ms())
    }
}

impl StateStoreBackend for PostgresStateStore {
    type Transaction = PostgresStateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.load_record(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.list_records(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        self.begin_transaction_inner().await
    }
}

struct PostgresListQuery<'a> {
    request: &'a StateListRequest,
}

impl<'a> PostgresListQuery<'a> {
    fn new(request: &'a StateListRequest) -> Self {
        Self { request }
    }

    fn escaped_prefix(&self) -> Option<String> {
        self.request.key_prefix().map(|prefix| {
            let raw = prefix.as_str();
            let mut escaped = String::with_capacity(raw.len() + 1);
            for character in raw.chars() {
                match character {
                    '\\' | '%' | '_' => {
                        escaped.push('\\');
                        escaped.push(character);
                    }
                    other => escaped.push(other),
                }
            }
            escaped.push('%');
            escaped
        })
    }

    fn limit_with_extra(&self) -> Result<i64, StateStoreError> {
        let limit_with_extra = self
            .request
            .limit()
            .checked_add(1)
            .ok_or_else(|| StateStoreError::invalid_request("state list limit overflow"))?;
        i64::try_from(limit_with_extra).map_err(|_| {
            StateStoreError::invalid_request("state list limit is not representable by the backend")
        })
    }
}
