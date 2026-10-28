use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{
    StateKey, StateRecord, StateRecordSnapshot, StateRevision, StateStoreError,
    StateTransactionBackend,
};

use super::store::{PostgresConnection, PostgresErrorContext};

const TRANSACTION_COMMAND_CAPACITY: usize = 1;

pub(crate) struct PostgresStateTransaction {
    commands: Option<mpsc::Sender<PostgresTransactionCommand>>,
    state: PostgresTransactionState,
}

enum PostgresTransactionState {
    Active,
    Cleanup(Result<(), StateStoreError>),
    WorkerUnavailable,
}

impl PostgresTransactionState {
    fn ensure_active(&self) -> Result<(), StateStoreError> {
        match self {
            Self::Active => Ok(()),
            Self::Cleanup(_) | Self::WorkerUnavailable => Err(StateStoreError::invalid_request(
                "state transaction has failed and must be rolled back",
            )),
        }
    }

    fn mark_worker_unavailable(&mut self) {
        *self = Self::WorkerUnavailable;
    }

    fn observe_operation_reply<T>(
        &mut self,
        operation: &'static str,
        reply: Result<PostgresOperationReply<T>, oneshot::error::RecvError>,
    ) -> Result<T, StateStoreError> {
        match reply {
            Ok(PostgresOperationReply::Output(output)) => Ok(output),
            Ok(PostgresOperationReply::Failed { primary, cleanup }) => {
                *self = Self::Cleanup(cleanup);
                Err(primary)
            }
            Err(_closed) => {
                *self = Self::WorkerUnavailable;
                Err(StateStoreError::unavailable(
                    operation,
                    "state transaction worker reply is unavailable",
                ))
            }
        }
    }

    fn rollback_result(&self) -> Option<Result<(), StateStoreError>> {
        match self {
            Self::Active => None,
            Self::Cleanup(result) => Some(result.clone()),
            Self::WorkerUnavailable => Some(Err(StateStoreError::unavailable(
                "rollback",
                "state transaction worker cleanup is unknown",
            ))),
        }
    }
}

impl PostgresStateTransaction {
    pub(super) async fn start(
        connection: PostgresConnection,
        operation_timeout_ms: u64,
        actors: &Arc<PostgresTransactionActors>,
    ) -> Result<Self, StateStoreError> {
        let (commands, actor) =
            PostgresTransactionActor::start(connection, operation_timeout_ms).await?;
        actors.spawn(actor);
        Ok(Self {
            commands: Some(commands),
            state: PostgresTransactionState::Active,
        })
    }

    async fn send_terminal(
        &self,
        operation: &'static str,
        command: PostgresTransactionCommand,
    ) -> Result<(), StateStoreError> {
        let commands = self.commands.as_ref().ok_or_else(|| {
            StateStoreError::unavailable(operation, "state transaction worker is unavailable")
        })?;
        let permit = commands.reserve().await.map_err(|_| {
            StateStoreError::unavailable(operation, "state transaction worker is unavailable")
        })?;
        permit.send(command);
        Ok(())
    }

    async fn receive<T>(
        operation: &'static str,
        reply: oneshot::Receiver<Result<T, StateStoreError>>,
    ) -> Result<T, StateStoreError> {
        reply.await.map_err(|_| {
            StateStoreError::unavailable(operation, "state transaction worker reply is unavailable")
        })?
    }

    async fn execute<T>(
        &mut self,
        operation: &'static str,
        command: PostgresTransactionCommand,
        reply: oneshot::Receiver<PostgresOperationReply<T>>,
    ) -> Result<T, StateStoreError> {
        let guard = PostgresOperationGuard::new(&mut self.commands, &mut self.state, operation)?;
        guard.send(command).await?;
        guard.finish(reply.await)
    }
}

struct PostgresOperationGuard<'transaction> {
    commands: &'transaction mut Option<mpsc::Sender<PostgresTransactionCommand>>,
    state: &'transaction mut PostgresTransactionState,
    operation: &'static str,
    close_on_drop: bool,
}

impl<'transaction> PostgresOperationGuard<'transaction> {
    fn new(
        commands: &'transaction mut Option<mpsc::Sender<PostgresTransactionCommand>>,
        state: &'transaction mut PostgresTransactionState,
        operation: &'static str,
    ) -> Result<Self, StateStoreError> {
        state.ensure_active()?;
        if commands.is_none() {
            state.mark_worker_unavailable();
            return Err(StateStoreError::unavailable(
                operation,
                "state transaction worker is unavailable",
            ));
        }
        Ok(Self {
            commands,
            state,
            operation,
            close_on_drop: true,
        })
    }

    async fn send(&self, command: PostgresTransactionCommand) -> Result<(), StateStoreError> {
        let commands = self.commands.as_ref().cloned().ok_or_else(|| {
            StateStoreError::unavailable(self.operation, "state transaction worker is unavailable")
        })?;
        let permit = commands.reserve().await.map_err(|_| {
            StateStoreError::unavailable(self.operation, "state transaction worker is unavailable")
        })?;
        permit.send(command);
        Ok(())
    }

    fn finish<T>(
        mut self,
        reply: Result<PostgresOperationReply<T>, oneshot::error::RecvError>,
    ) -> Result<T, StateStoreError> {
        self.close_on_drop = false;
        self.state.observe_operation_reply(self.operation, reply)
    }
}

impl Drop for PostgresOperationGuard<'_> {
    fn drop(&mut self) {
        if self.close_on_drop {
            self.state.mark_worker_unavailable();
            self.commands.take();
        }
    }
}

pub(super) struct PostgresTransactionActors {
    next_id: AtomicU64,
    tasks: Mutex<BTreeMap<u64, JoinHandle<()>>>,
}

impl PostgresTransactionActors {
    pub(super) fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            tasks: Mutex::new(BTreeMap::new()),
        }
    }

    fn spawn(self: &Arc<Self>, actor: PostgresTransactionActor) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (registration, registered) = oneshot::channel();
        let actors = Arc::clone(self);
        let task = tokio::spawn(async move {
            let _registration = PostgresTransactionActorRegistration::new(id, actors);
            if registered.await.is_ok() {
                actor.run().await;
            }
        });
        self.tasks().insert(id, task);
        let _registration_result = registration.send(());
    }

    fn remove(&self, id: u64) {
        let _task = self.tasks().remove(&id);
    }

    fn tasks(&self) -> MutexGuard<'_, BTreeMap<u64, JoinHandle<()>>> {
        self.tasks.lock().unwrap_or_else(|error| error.into_inner())
    }
}

struct PostgresTransactionActorRegistration {
    id: u64,
    actors: Arc<PostgresTransactionActors>,
}

impl PostgresTransactionActorRegistration {
    fn new(id: u64, actors: Arc<PostgresTransactionActors>) -> Self {
        Self { id, actors }
    }
}

impl Drop for PostgresTransactionActorRegistration {
    fn drop(&mut self) {
        self.actors.remove(self.id);
    }
}

enum PostgresOperationReply<T> {
    Output(T),
    Failed {
        primary: StateStoreError,
        cleanup: Result<(), StateStoreError>,
    },
}

enum PostgresTransactionCommand {
    Load {
        key: StateKey,
        reply: oneshot::Sender<PostgresOperationReply<Option<StateRecordSnapshot>>>,
    },
    Put {
        record: StateRecord,
        reply: oneshot::Sender<PostgresOperationReply<()>>,
    },
    Delete {
        key: StateKey,
        reply: oneshot::Sender<PostgresOperationReply<()>>,
    },
    Commit {
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
    Rollback {
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
}

impl PostgresTransactionCommand {
    fn send_error(self, primary: StateStoreError, cleanup: Result<(), StateStoreError>) {
        match self {
            Self::Load { reply, .. } => {
                let _reply_result = reply.send(PostgresOperationReply::Failed { primary, cleanup });
            }
            Self::Put { reply, .. } | Self::Delete { reply, .. } => {
                let _reply_result = reply.send(PostgresOperationReply::Failed { primary, cleanup });
            }
            Self::Commit { reply } | Self::Rollback { reply } => {
                let _reply_result = reply.send(Err(primary));
            }
        }
    }
}

struct PostgresTransactionActor {
    connection: PostgresConnection,
    commands: mpsc::Receiver<PostgresTransactionCommand>,
    operation_timeout_ms: u64,
}

impl PostgresTransactionActor {
    async fn start(
        connection: PostgresConnection,
        operation_timeout_ms: u64,
    ) -> Result<(mpsc::Sender<PostgresTransactionCommand>, Self), StateStoreError> {
        if let Err(error) = Self::begin_transaction(&connection, operation_timeout_ms).await {
            let _rollback_result =
                Self::rollback_connection(&connection, operation_timeout_ms).await;
            connection.shutdown().await;
            return Err(error);
        }

        let (commands, receiver) = mpsc::channel(TRANSACTION_COMMAND_CAPACITY);
        let actor = Self {
            connection,
            commands: receiver,
            operation_timeout_ms,
        };
        Ok((commands, actor))
    }

    async fn begin_transaction(
        connection: &PostgresConnection,
        operation_timeout_ms: u64,
    ) -> Result<(), StateStoreError> {
        let error_context = PostgresErrorContext::new("begin_transaction", operation_timeout_ms);
        connection
            .client()
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .map_err(|error| error_context.postgres(error))
    }

    async fn configure_transaction_timeouts(&self) -> Result<(), StateStoreError> {
        let error_context =
            PostgresErrorContext::new("begin_transaction", self.operation_timeout_ms);
        let operation_timeout_ms = self.operation_timeout_ms;
        let timeout_configuration = format!(
            "SET LOCAL statement_timeout = {operation_timeout_ms}; \
             SET LOCAL lock_timeout = {operation_timeout_ms}"
        );
        self.connection
            .client()
            .batch_execute(&timeout_configuration)
            .await
            .map_err(|error| error_context.postgres(error))
    }

    async fn run(mut self) {
        let mut transaction_timeouts_pending = true;
        loop {
            let (command, driver_result) = if let Some(driver) = self.connection.driver() {
                tokio::select! {
                    command = self.commands.recv() => (command, None),
                    driver_result = driver => {
                        (None, Some(driver_result))
                    }
                }
            } else {
                return;
            };
            if let Some(driver_result) = driver_result {
                self.connection.observe_driver_completion(driver_result);
                self.shutdown().await;
                return;
            }

            let Some(command) = command else {
                self.rollback_and_shutdown().await;
                return;
            };
            if transaction_timeouts_pending {
                if let Err(error) = self.configure_transaction_timeouts().await {
                    self.rollback_and_finish_command(command, error).await;
                    return;
                }
                transaction_timeouts_pending = false;
            }
            match command {
                PostgresTransactionCommand::Load { key, reply } => {
                    match self.load_record(key).await {
                        Ok(snapshot) => {
                            if reply
                                .send(PostgresOperationReply::Output(snapshot))
                                .is_err()
                            {
                                self.rollback_and_shutdown().await;
                                return;
                            }
                        }
                        Err(error) => {
                            self.rollback_and_finish(reply, error).await;
                            return;
                        }
                    }
                }
                PostgresTransactionCommand::Put { record, reply } => {
                    match self.put_record(record).await {
                        Ok(()) => {
                            if reply.send(PostgresOperationReply::Output(())).is_err() {
                                self.rollback_and_shutdown().await;
                                return;
                            }
                        }
                        Err(error) => {
                            self.rollback_and_finish(reply, error).await;
                            return;
                        }
                    }
                }
                PostgresTransactionCommand::Delete { key, reply } => {
                    match self.delete_record(key).await {
                        Ok(()) => {
                            if reply.send(PostgresOperationReply::Output(())).is_err() {
                                self.rollback_and_shutdown().await;
                                return;
                            }
                        }
                        Err(error) => {
                            self.rollback_and_finish(reply, error).await;
                            return;
                        }
                    }
                }
                PostgresTransactionCommand::Commit { reply } => {
                    let result = self.commit_transaction().await;
                    self.finish(reply, result).await;
                    return;
                }
                PostgresTransactionCommand::Rollback { reply } => {
                    let result = self.rollback_transaction().await;
                    self.finish(reply, result).await;
                    return;
                }
            }
        }
    }

    async fn rollback_and_finish<T>(
        self,
        reply: oneshot::Sender<PostgresOperationReply<T>>,
        primary: StateStoreError,
    ) {
        let cleanup = self.rollback_transaction().await;
        self.shutdown().await;
        let _reply_result = reply.send(PostgresOperationReply::Failed { primary, cleanup });
    }

    async fn rollback_and_finish_command(
        self,
        command: PostgresTransactionCommand,
        error: StateStoreError,
    ) {
        let cleanup = self.rollback_transaction().await;
        self.shutdown().await;
        command.send_error(error, cleanup);
    }

    async fn finish<T>(
        self,
        reply: oneshot::Sender<Result<T, StateStoreError>>,
        result: Result<T, StateStoreError>,
    ) {
        self.shutdown().await;
        let _reply_result = reply.send(result);
    }

    async fn rollback_and_shutdown(self) {
        let _cleanup_result = self.rollback_transaction().await;
        self.shutdown().await;
    }

    async fn shutdown(self) {
        self.connection.shutdown().await;
    }

    async fn load_record(
        &self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let row = self
            .connection
            .client()
            .query_opt(
                "SELECT revision, value_json
                 FROM storage_kit_state_records
                 WHERE namespace = $1 AND key = $2 AND deleted = FALSE
                 FOR UPDATE",
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

    async fn put_record(&self, record: StateRecord) -> Result<(), StateStoreError> {
        let value_json = serde_json::to_string(record.value.as_ref())?;
        let error_context = self.error_context("put");
        let now_ms = error_context.current_time_ms()?;
        self.connection
            .client()
            .execute(
                "INSERT INTO storage_kit_state_records (
                    namespace, key, revision, value_json, deleted, created_at_ms, updated_at_ms
                 )
                 VALUES ($1, $2, 1, $3, FALSE, $4, $4)
                 ON CONFLICT(namespace, key) DO UPDATE SET
                    revision = storage_kit_state_records.revision + 1,
                    value_json = EXCLUDED.value_json,
                    deleted = FALSE,
                    updated_at_ms = EXCLUDED.updated_at_ms
                 ",
                &[
                    &record.key.namespace().as_str(),
                    &record.key.key(),
                    &value_json,
                    &now_ms,
                ],
            )
            .await
            .map_err(|error| error_context.postgres(error))?;

        Ok(())
    }

    async fn delete_record(&self, key: StateKey) -> Result<(), StateStoreError> {
        let error_context = self.error_context("delete");
        let now_ms = error_context.current_time_ms()?;
        self.connection
            .client()
            .execute(
                "INSERT INTO storage_kit_state_records (
                    namespace, key, revision, value_json, deleted, created_at_ms, updated_at_ms
                 )
                 VALUES ($1, $2, 1, NULL, TRUE, $3, $3)
                 ON CONFLICT(namespace, key) DO UPDATE SET
                    revision = storage_kit_state_records.revision + 1,
                    value_json = NULL,
                    deleted = TRUE,
                    updated_at_ms = EXCLUDED.updated_at_ms
                 ",
                &[&key.namespace().as_str(), &key.key(), &now_ms],
            )
            .await
            .map_err(|error| error_context.postgres(error))?;

        Ok(())
    }

    async fn commit_transaction(&mut self) -> Result<(), StateStoreError> {
        let error_context = self.error_context("commit");
        match self.connection.client().batch_execute("COMMIT").await {
            Ok(()) => Ok(()),
            Err(error) => {
                let mapped = error_context.postgres(error);
                let _cleanup_result = self.rollback_transaction().await;
                Err(mapped)
            }
        }
    }

    async fn rollback_transaction(&self) -> Result<(), StateStoreError> {
        self.connection
            .client()
            .batch_execute("ROLLBACK")
            .await
            .map_err(|error| self.error_context("rollback").postgres(error))
    }

    async fn rollback_connection(
        connection: &PostgresConnection,
        operation_timeout_ms: u64,
    ) -> Result<(), StateStoreError> {
        connection
            .client()
            .batch_execute("ROLLBACK")
            .await
            .map_err(|error| {
                PostgresErrorContext::new("rollback", operation_timeout_ms).postgres(error)
            })
    }

    fn error_context(&self, operation: &'static str) -> PostgresErrorContext {
        PostgresErrorContext::new(operation, self.operation_timeout_ms)
    }
}

impl StateTransactionBackend for PostgresStateTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let (reply, receiver) = oneshot::channel();
        self.execute(
            "load",
            PostgresTransactionCommand::Load { key, reply },
            receiver,
        )
        .await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let (reply, receiver) = oneshot::channel();
        self.execute(
            "put",
            PostgresTransactionCommand::Put { record, reply },
            receiver,
        )
        .await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        let (reply, receiver) = oneshot::channel();
        self.execute(
            "delete",
            PostgresTransactionCommand::Delete { key, reply },
            receiver,
        )
        .await
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        self.state.ensure_active()?;
        let (reply, receiver) = oneshot::channel();
        self.send_terminal("commit", PostgresTransactionCommand::Commit { reply })
            .await?;
        Self::receive("commit", receiver).await
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        if let Some(result) = self.state.rollback_result() {
            return result;
        }
        let (reply, receiver) = oneshot::channel();
        self.send_terminal("rollback", PostgresTransactionCommand::Rollback { reply })
            .await?;
        Self::receive("rollback", receiver).await
    }
}
