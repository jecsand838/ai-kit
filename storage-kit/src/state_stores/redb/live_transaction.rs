use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch};

use crate::{StateKey, StateRecord, StateRecordSnapshot, StateStoreError, StateTransactionBackend};

use super::{
    executor::{RedbTimeoutPolicy, RedbWorkerRuntime},
    transaction_session::{RedbSessionStatus, RedbTransactionCommand},
};

pub(crate) struct RedbStateTransaction {
    commands: Option<mpsc::Sender<RedbTransactionCommand>>,
    timeout: RedbTimeoutPolicy,
    status: Arc<RedbSessionStatus>,
    shutdown: watch::Receiver<bool>,
    runtime: Arc<RedbWorkerRuntime>,
}

impl RedbStateTransaction {
    pub(super) fn new(
        commands: mpsc::Sender<RedbTransactionCommand>,
        timeout: RedbTimeoutPolicy,
        status: Arc<RedbSessionStatus>,
        runtime: Arc<RedbWorkerRuntime>,
    ) -> Self {
        let shutdown = runtime.subscribe_shutdown();
        Self {
            commands: Some(commands),
            timeout,
            status,
            shutdown,
            runtime,
        }
    }

    async fn execute_non_final<T>(
        &mut self,
        op: &'static str,
        command: RedbTransactionCommand,
        reply: oneshot::Receiver<Result<T, StateStoreError>>,
    ) -> Result<T, StateStoreError> {
        self.ensure_available(op)?;
        let timeout = self.timeout;
        let status = self.status.clone();
        let shutdown = self.shutdown.clone();
        let runtime = self.runtime.clone();
        let guard = RedbCommandGuard::new(&mut self.commands, op, status.clone(), runtime.clone())?;
        let commands = guard.sender()?;
        if let Err(error) = timeout.enqueue(op, &commands, command).await {
            let error = runtime.session_failure(&status).unwrap_or(error);
            return guard.finish(Err(error));
        }

        let result = timeout
            .read_transaction_reply(op, reply, shutdown, &runtime, &status)
            .await;
        guard.finish(result)
    }

    fn ensure_available(&mut self, operation: &'static str) -> Result<(), StateStoreError> {
        if self.commands.is_none() {
            return Err(Self::transaction_failed_error());
        }
        if let Err(error) = self.runtime.session_available(operation, &self.status) {
            self.status.fail_pending(error.clone());
            self.commands.take();
            return Err(error);
        }
        Ok(())
    }

    async fn terminal_reply<T>(
        &self,
        operation: &'static str,
        reply: oneshot::Receiver<Result<T, StateStoreError>>,
    ) -> Result<T, StateStoreError> {
        let context = super::error::RedbErrorContext::new(operation);
        let mut reply = Box::pin(reply);
        let mut shutdown = self.shutdown.clone();
        loop {
            tokio::select! {
                result = &mut reply => {
                    return result.map_err(|_| {
                        self.runtime
                            .session_failure(&self.status)
                            .unwrap_or_else(|| context.closed())
                    })?;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        return Err(context.closed());
                    }
                    if let Err(error) =
                        self.runtime.session_available(operation, &self.status)
                    {
                        match self.status.terminal_result() {
                            Some(Ok(())) => {}
                            Some(Err(error)) => return Err(error),
                            None => return Err(error),
                        }
                    }
                }
            }
        }
    }

    fn transaction_failed_error() -> StateStoreError {
        StateStoreError::invalid_request("state transaction has failed and must be rolled back")
    }
}

impl StateTransactionBackend for RedbStateTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RedbTransactionCommand::Load {
            key,
            reply: reply_tx,
        };
        self.execute_non_final("load", command, reply_rx).await
    }

    async fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RedbTransactionCommand::Put {
            record,
            reply: reply_tx,
        };
        self.execute_non_final("put", command, reply_rx).await
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RedbTransactionCommand::Delete {
            key,
            reply: reply_tx,
        };
        self.execute_non_final("delete", command, reply_rx).await
    }

    async fn commit(mut self) -> Result<(), StateStoreError> {
        self.ensure_available("commit")?;
        let Some(commands) = self.commands.as_ref() else {
            return Err(Self::transaction_failed_error());
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RedbTransactionCommand::Commit { reply: reply_tx };
        if let Err(error) = self.timeout.enqueue("commit", commands, command).await {
            return Err(self.runtime.session_failure(&self.status).unwrap_or(error));
        }
        self.commands.take();
        self.terminal_reply("commit", reply_rx).await
    }

    async fn rollback(mut self) -> Result<(), StateStoreError> {
        match self.runtime.cancel_pending("rollback", &self.status) {
            Ok(true) => {
                self.commands.take();
                return self
                    .timeout
                    .wait_for_cleanup("rollback", self.status.subscribe_completion())
                    .await;
            }
            Ok(false) => {}
            Err(error) => {
                self.commands.take();
                return Err(error);
            }
        }
        if self.commands.is_none() {
            return self
                .timeout
                .wait_for_cleanup("rollback", self.status.subscribe_completion())
                .await;
        }
        let commands = self
            .commands
            .as_ref()
            .cloned()
            .ok_or_else(Self::transaction_failed_error)?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RedbTransactionCommand::Rollback { reply: reply_tx };
        if self
            .timeout
            .enqueue("rollback", &commands, command)
            .await
            .is_err()
        {
            self.commands.take();
            return self
                .timeout
                .wait_for_cleanup("rollback", self.status.subscribe_completion())
                .await;
        }
        self.commands.take();
        self.terminal_reply("rollback", reply_rx).await
    }
}

impl Drop for RedbStateTransaction {
    fn drop(&mut self) {
        if self.commands.is_some() {
            let _ = self.runtime.cancel_pending("drop", &self.status);
        }
        self.commands.take();
    }
}

struct RedbCommandGuard<'transaction> {
    commands: &'transaction mut Option<mpsc::Sender<RedbTransactionCommand>>,
    operation: &'static str,
    status: Arc<RedbSessionStatus>,
    runtime: Arc<RedbWorkerRuntime>,
    close_on_drop: bool,
}

impl<'transaction> RedbCommandGuard<'transaction> {
    fn new(
        commands: &'transaction mut Option<mpsc::Sender<RedbTransactionCommand>>,
        operation: &'static str,
        status: Arc<RedbSessionStatus>,
        runtime: Arc<RedbWorkerRuntime>,
    ) -> Result<Self, StateStoreError> {
        if commands.is_none() {
            return Err(RedbStateTransaction::transaction_failed_error());
        }
        Ok(Self {
            commands,
            operation,
            status,
            runtime,
            close_on_drop: true,
        })
    }

    fn sender(&self) -> Result<mpsc::Sender<RedbTransactionCommand>, StateStoreError> {
        self.commands
            .as_ref()
            .cloned()
            .ok_or_else(RedbStateTransaction::transaction_failed_error)
    }

    fn finish<T>(
        mut self,
        delivered: Result<Result<T, StateStoreError>, StateStoreError>,
    ) -> Result<T, StateStoreError> {
        match delivered {
            Ok(result) => {
                self.close_on_drop = false;
                result
            }
            Err(error) => Err(error),
        }
    }
}

impl Drop for RedbCommandGuard<'_> {
    fn drop(&mut self) {
        if self.close_on_drop {
            let _ = self.runtime.cancel_pending(self.operation, &self.status);
            self.commands.take();
        }
    }
}
