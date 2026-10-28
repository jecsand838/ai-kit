use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

use crate::{
    StateKey, StateRecord, StateRecordSnapshot, StateRevision, StateStoreError,
    StateTransactionBackend,
};

use super::store::SqliteErrorContext;

pub(crate) struct SqliteStateTransaction {
    commands: Option<mpsc::Sender<SqliteTransactionCommand>>,
    actor_state: watch::Receiver<SqliteActorState>,
    state: SqliteTransactionState,
    admission_timeout: Duration,
    admission_timeout_ms: u64,
}

enum SqliteTransactionState {
    Active,
    CleanupPending,
    CleanupConfirmed,
    CleanupUnknown,
}

impl SqliteTransactionState {
    fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    fn ensure_active(&self) -> Result<(), StateStoreError> {
        match self {
            Self::Active => Ok(()),
            Self::CleanupPending | Self::CleanupConfirmed | Self::CleanupUnknown => {
                Err(Self::inactive_error())
            }
        }
    }

    fn inactive_error() -> StateStoreError {
        StateStoreError::invalid_request("state transaction has failed and must be rolled back")
    }

    fn mark_cleanup_pending(&mut self) {
        *self = Self::CleanupPending;
    }

    fn mark_cleanup(&mut self, confirmed: bool) {
        *self = if confirmed {
            Self::CleanupConfirmed
        } else {
            Self::CleanupUnknown
        };
    }
}

enum SqliteActorState {
    Running,
    Finished(Result<(), StateStoreError>),
}

impl SqliteActorState {
    fn result(&self) -> Option<Result<(), StateStoreError>> {
        match self {
            Self::Running => None,
            Self::Finished(result) => Some(result.clone()),
        }
    }
}

impl SqliteStateTransaction {
    const COMMAND_CAPACITY: usize = 8;

    pub(super) fn new(
        connection: turso::Connection,
        operation_timeout: Duration,
        operation_timeout_ms: u64,
        actors: &Arc<SqliteTransactionActors>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(Self::COMMAND_CAPACITY);
        let actor_state = actors.spawn(SqliteTransactionActor::new(
            connection,
            receiver,
            operation_timeout_ms,
        ));
        Self {
            commands: Some(sender),
            actor_state,
            state: SqliteTransactionState::Active,
            admission_timeout: operation_timeout,
            admission_timeout_ms: operation_timeout_ms,
        }
    }

    async fn execute_nonterminal<T>(
        &mut self,
        operation: &'static str,
        command: SqliteTransactionCommand,
        reply: oneshot::Receiver<SqliteOperationReply<T>>,
    ) -> Result<T, StateStoreError> {
        self.state.ensure_active()?;
        let timeout = self.admission_timeout;
        let timeout_ms = self.admission_timeout_ms;
        let deadline = tokio::time::Instant::now() + timeout;
        let unavailable = self.error(operation).actor_unavailable();
        let observation = SqliteCommandObservation::new(&mut self.commands, &mut self.state);
        observation
            .enqueue(operation, deadline, timeout_ms, command)
            .await?;
        match tokio::time::timeout_at(deadline, reply).await {
            Ok(reply) => observation.finish(reply, unavailable),
            Err(_) => Err(StateStoreError::timed_out(operation, timeout_ms)),
        }
    }

    async fn enqueue_terminal(
        &self,
        operation: &'static str,
        command: SqliteTransactionCommand,
    ) -> Result<(), StateStoreError> {
        let commands = self
            .commands
            .as_ref()
            .ok_or_else(|| self.error(operation).actor_unavailable())?;
        commands
            .send(command)
            .await
            .map_err(|_| self.error(operation).actor_unavailable())
    }

    fn error(&self, operation: &'static str) -> SqliteErrorContext {
        SqliteErrorContext::new(operation, self.admission_timeout_ms)
    }

    async fn await_actor(&mut self, operation: &'static str) -> Result<(), StateStoreError> {
        let unavailable = self.error(operation).actor_unavailable();
        loop {
            if let Some(result) = self.actor_state.borrow_and_update().result() {
                return result;
            }
            if self.actor_state.changed().await.is_err() {
                return Err(unavailable);
            }
        }
    }
}

impl Drop for SqliteStateTransaction {
    fn drop(&mut self) {
        self.commands.take();
    }
}

pub(super) struct SqliteTransactionActors {
    next_id: AtomicU64,
    tasks: Mutex<BTreeMap<u64, JoinHandle<()>>>,
}

impl SqliteTransactionActors {
    pub(super) fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            tasks: Mutex::new(BTreeMap::new()),
        }
    }

    fn spawn(self: &Arc<Self>, actor: SqliteTransactionActor) -> watch::Receiver<SqliteActorState> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (registration, registered) = oneshot::channel();
        let (state, actor_state) = watch::channel(SqliteActorState::Running);
        let actors = Arc::clone(self);
        let task = tokio::spawn(async move {
            let _registration = SqliteTransactionActorRegistration::new(id, actors);
            if registered.await.is_ok() {
                let result = actor.run().await;
                let _prior_state = state.send_replace(SqliteActorState::Finished(result));
            }
        });
        let _prior_task = self.tasks().insert(id, task);
        let _registration_result = registration.send(());
        actor_state
    }

    fn remove(&self, id: u64) {
        let _task = self.tasks().remove(&id);
    }

    fn tasks(&self) -> MutexGuard<'_, BTreeMap<u64, JoinHandle<()>>> {
        self.tasks.lock().unwrap_or_else(|error| error.into_inner())
    }
}

struct SqliteTransactionActorRegistration {
    id: u64,
    actors: Arc<SqliteTransactionActors>,
}

impl SqliteTransactionActorRegistration {
    fn new(id: u64, actors: Arc<SqliteTransactionActors>) -> Self {
        Self { id, actors }
    }
}

impl Drop for SqliteTransactionActorRegistration {
    fn drop(&mut self) {
        self.actors.remove(self.id);
    }
}

impl StateTransactionBackend for SqliteStateTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let (reply, receiver) = oneshot::channel();
        self.execute_nonterminal(
            "load",
            SqliteTransactionCommand::Load { key, reply },
            receiver,
        )
        .await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let (reply, receiver) = oneshot::channel();
        self.execute_nonterminal(
            "put",
            SqliteTransactionCommand::Put { record, reply },
            receiver,
        )
        .await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        let (reply, receiver) = oneshot::channel();
        self.execute_nonterminal(
            "delete",
            SqliteTransactionCommand::Delete { key, reply },
            receiver,
        )
        .await
    }

    async fn commit(mut self) -> Result<(), StateStoreError> {
        if let Err(inactive) = self.state.ensure_active() {
            self.commands.take();
            return match self.await_actor("commit").await {
                Ok(()) => Err(inactive),
                Err(cleanup_error) => Err(cleanup_error),
            };
        }
        let (reply, receiver) = oneshot::channel();
        if let Err(error) = self
            .enqueue_terminal("commit", SqliteTransactionCommand::Commit { reply })
            .await
        {
            self.commands.take();
            return match self.await_actor("commit").await {
                Ok(()) => Err(error),
                Err(actor_error) => Err(actor_error),
            };
        }
        self.commands.take();
        let terminal_result = receiver.await;
        let actor_result = self.await_actor("commit").await;
        match terminal_result {
            Ok(result) => result,
            Err(_closed) => match actor_result {
                Ok(()) => Err(self.error("commit").actor_unavailable()),
                Err(cleanup_error) => Err(cleanup_error),
            },
        }
    }

    async fn rollback(mut self) -> Result<(), StateStoreError> {
        if !self.state.is_active() {
            self.commands.take();
            return self.await_actor("rollback").await;
        }
        let (reply, receiver) = oneshot::channel();
        if let Err(error) = self
            .enqueue_terminal("rollback", SqliteTransactionCommand::Rollback { reply })
            .await
        {
            self.commands.take();
            return match self.await_actor("rollback").await {
                Ok(()) => Err(error),
                Err(actor_error) => Err(actor_error),
            };
        }
        self.commands.take();
        let terminal_result = receiver.await;
        let actor_result = self.await_actor("rollback").await;
        match terminal_result {
            Ok(result) => result,
            Err(_closed) => match actor_result {
                Ok(()) => Err(self.error("rollback").actor_unavailable()),
                Err(cleanup_error) => Err(cleanup_error),
            },
        }
    }
}

struct SqliteCommandObservation<'transaction> {
    commands: &'transaction mut Option<mpsc::Sender<SqliteTransactionCommand>>,
    state: &'transaction mut SqliteTransactionState,
    close_on_drop: bool,
}

impl<'transaction> SqliteCommandObservation<'transaction> {
    fn new(
        commands: &'transaction mut Option<mpsc::Sender<SqliteTransactionCommand>>,
        state: &'transaction mut SqliteTransactionState,
    ) -> Self {
        Self {
            commands,
            state,
            close_on_drop: true,
        }
    }

    async fn enqueue(
        &self,
        operation: &'static str,
        deadline: tokio::time::Instant,
        timeout_ms: u64,
        command: SqliteTransactionCommand,
    ) -> Result<(), StateStoreError> {
        let commands = self.commands.as_ref().ok_or_else(|| {
            StateStoreError::invalid_request("state transaction has failed and must be rolled back")
        })?;
        match tokio::time::timeout_at(deadline, commands.send(command)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(SqliteErrorContext::new(operation, timeout_ms).actor_unavailable()),
            Err(_) => Err(StateStoreError::timed_out(operation, timeout_ms)),
        }
    }

    fn finish<T>(
        mut self,
        reply: Result<SqliteOperationReply<T>, oneshot::error::RecvError>,
        unavailable: StateStoreError,
    ) -> Result<T, StateStoreError> {
        let result = match reply {
            Ok(SqliteOperationReply::Completed(output)) => Ok(output),
            Ok(SqliteOperationReply::Failed {
                primary,
                cleanup_confirmed,
            }) => {
                self.commands.take();
                self.state.mark_cleanup(cleanup_confirmed);
                Err(primary)
            }
            Err(_closed) => {
                self.commands.take();
                self.state.mark_cleanup_pending();
                Err(unavailable)
            }
        };
        self.close_on_drop = false;
        result
    }
}

impl Drop for SqliteCommandObservation<'_> {
    fn drop(&mut self) {
        if self.close_on_drop {
            self.commands.take();
            self.state.mark_cleanup_pending();
        }
    }
}

enum SqliteOperationReply<T> {
    Completed(T),
    Failed {
        primary: StateStoreError,
        cleanup_confirmed: bool,
    },
}

enum SqliteTransactionCommand {
    Load {
        key: StateKey,
        reply: oneshot::Sender<SqliteOperationReply<Option<StateRecordSnapshot>>>,
    },
    Put {
        record: StateRecord,
        reply: oneshot::Sender<SqliteOperationReply<()>>,
    },
    Delete {
        key: StateKey,
        reply: oneshot::Sender<SqliteOperationReply<()>>,
    },
    Commit {
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
    Rollback {
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
}

struct SqliteTransactionActor {
    connection: turso::Connection,
    receiver: mpsc::Receiver<SqliteTransactionCommand>,
    started: bool,
    operation_timeout_ms: u64,
}

impl SqliteTransactionActor {
    fn new(
        connection: turso::Connection,
        receiver: mpsc::Receiver<SqliteTransactionCommand>,
        operation_timeout_ms: u64,
    ) -> Self {
        Self {
            connection,
            receiver,
            started: false,
            operation_timeout_ms,
        }
    }

    async fn run(mut self) -> Result<(), StateStoreError> {
        while let Some(command) = self.receiver.recv().await {
            let keep_running = match command {
                SqliteTransactionCommand::Load { key, reply } => {
                    let result = self.load(key).await;
                    self.send_nonterminal(reply, result).await?
                }
                SqliteTransactionCommand::Put { record, reply } => {
                    let result = self.put(record).await;
                    self.send_nonterminal(reply, result).await?
                }
                SqliteTransactionCommand::Delete { key, reply } => {
                    let result = self.delete(key).await;
                    self.send_nonterminal(reply, result).await?
                }
                SqliteTransactionCommand::Commit { reply } => {
                    let result = self.commit().await;
                    let _ = reply.send(result);
                    return Ok(());
                }
                SqliteTransactionCommand::Rollback { reply } => {
                    let result = self.rollback().await;
                    let _ = reply.send(result);
                    return Ok(());
                }
            };
            if !keep_running {
                return Ok(());
            }
        }
        self.rollback().await
    }

    async fn send_nonterminal<T>(
        &mut self,
        reply: oneshot::Sender<SqliteOperationReply<T>>,
        result: Result<T, StateStoreError>,
    ) -> Result<bool, StateStoreError> {
        match result {
            Ok(output) => {
                if reply.send(SqliteOperationReply::Completed(output)).is_ok() {
                    Ok(true)
                } else {
                    self.rollback().await?;
                    Ok(false)
                }
            }
            Err(primary) => {
                let cleanup = self.rollback().await;
                let cleanup_confirmed = cleanup.is_ok();
                let _ = reply.send(SqliteOperationReply::Failed {
                    primary,
                    cleanup_confirmed,
                });
                cleanup.map(|()| false)
            }
        }
    }

    async fn ensure_started(&mut self, operation: &'static str) -> Result<(), StateStoreError> {
        if self.started {
            return Ok(());
        }
        self.connection
            .execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(|error| self.error(operation).map_turso(error))?;
        self.started = true;
        Ok(())
    }

    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.ensure_started("load").await?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT revision, value_json
                 FROM state_records
                 WHERE namespace = ?1 AND key = ?2 AND deleted = 0",
            )
            .await
            .map_err(|error| self.error("load").map_turso(error))?;
        let mut rows = statement
            .query((key.namespace().as_str(), key.key()))
            .await
            .map_err(|error| self.error("load").map_turso(error))?;
        let row = rows
            .next()
            .await
            .map_err(|error| self.error("load").map_turso(error))?;
        match row {
            Some(row) => {
                let revision = row
                    .get::<i64>(0)
                    .map_err(|error| self.error("load").map_turso(error))?;
                let value_json = row
                    .get::<String>(1)
                    .map_err(|error| self.error("load").map_turso(error))?;
                Ok(Some(StateRecordSnapshot::new(
                    StateRecord::new(key, serde_json::from_str(&value_json)?),
                    StateRevision::from_i64(revision)?,
                )))
            }
            None => Ok(None),
        }
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.ensure_started("put").await?;
        let revision = self.next_revision(&record.key, "put").await?;
        let value_json = serde_json::to_string(record.value.as_ref())?;
        self.upsert_record(&record.key, revision, Some(value_json), false, "put")
            .await?;
        Ok(())
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.ensure_started("delete").await?;
        let revision = self.next_revision(&key, "delete").await?;
        self.upsert_record(&key, revision, None, true, "delete")
            .await?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), StateStoreError> {
        self.ensure_started("commit").await?;
        if let Err(error) = self.connection.execute("COMMIT", ()).await {
            let primary = self.error("commit").map_turso(error);
            let _ = self.rollback().await;
            return Err(primary);
        }
        self.started = false;
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), StateStoreError> {
        if !self.started {
            return Ok(());
        }
        let result = self
            .connection
            .execute("ROLLBACK", ())
            .await
            .map(|_| ())
            .map_err(|error| self.error("rollback").map_turso(error));
        self.started = false;
        result
    }

    async fn next_revision(
        &self,
        key: &StateKey,
        operation: &'static str,
    ) -> Result<StateRevision, StateStoreError> {
        match self.status_for(key, operation).await? {
            SqliteRecordStatus::Missing => Ok(StateRevision::first()),
            SqliteRecordStatus::Deleted { revision } | SqliteRecordStatus::Present { revision } => {
                revision.next()
            }
        }
    }

    async fn status_for(
        &self,
        key: &StateKey,
        operation: &'static str,
    ) -> Result<SqliteRecordStatus, StateStoreError> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT revision, deleted
                 FROM state_records
                 WHERE namespace = ?1 AND key = ?2",
            )
            .await
            .map_err(|error| self.error(operation).map_turso(error))?;
        let mut rows = statement
            .query((key.namespace().as_str(), key.key()))
            .await
            .map_err(|error| self.error(operation).map_turso(error))?;
        let row = rows
            .next()
            .await
            .map_err(|error| self.error(operation).map_turso(error))?;
        match row {
            Some(row) => {
                let revision = row
                    .get::<i64>(0)
                    .map_err(|error| self.error(operation).map_turso(error))?;
                let deleted = row
                    .get::<i64>(1)
                    .map_err(|error| self.error(operation).map_turso(error))?;
                let revision = StateRevision::from_i64(revision)?;
                if deleted == 0 {
                    Ok(SqliteRecordStatus::Present { revision })
                } else {
                    Ok(SqliteRecordStatus::Deleted { revision })
                }
            }
            None => Ok(SqliteRecordStatus::Missing),
        }
    }

    async fn upsert_record(
        &self,
        key: &StateKey,
        revision: StateRevision,
        value_json: Option<String>,
        deleted: bool,
        operation: &'static str,
    ) -> Result<(), StateStoreError> {
        let now_ms = self.current_time_ms(operation)?;
        let mut statement = self
            .connection
            .prepare(
                "INSERT INTO state_records (
                    namespace,
                    key,
                    revision,
                    value_json,
                    deleted,
                    created_at_ms,
                    updated_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(namespace, key) DO UPDATE SET
                    revision = excluded.revision,
                    value_json = excluded.value_json,
                    deleted = excluded.deleted,
                    updated_at_ms = excluded.updated_at_ms",
            )
            .await
            .map_err(|error| self.error(operation).map_turso(error))?;
        let value = match value_json {
            Some(value) => turso::Value::Text(value),
            None => turso::Value::Null,
        };
        let deleted_value = if deleted { 1 } else { 0 };
        statement
            .execute(Vec::from([
                turso::Value::Text(key.namespace().as_str().to_owned()),
                turso::Value::Text(key.key().to_owned()),
                turso::Value::Integer(revision.to_backend_i64()?),
                value,
                turso::Value::Integer(deleted_value),
                turso::Value::Integer(now_ms),
                turso::Value::Integer(now_ms),
            ]))
            .await
            .map_err(|error| self.error(operation).map_turso(error))?;
        Ok(())
    }

    fn current_time_ms(&self, operation: &'static str) -> Result<i64, StateStoreError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| self.error(operation).backend_failed())?;
        i64::try_from(duration.as_millis()).map_err(|_| self.error(operation).backend_failed())
    }

    fn error(&self, operation: &'static str) -> SqliteErrorContext {
        SqliteErrorContext::new(operation, self.operation_timeout_ms)
    }
}

enum SqliteRecordStatus {
    Missing,
    Deleted { revision: StateRevision },
    Present { revision: StateRevision },
}
