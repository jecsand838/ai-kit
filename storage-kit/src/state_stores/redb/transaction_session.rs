use std::{
    ops::ControlFlow,
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use redb::ReadableTable;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{StateKey, StateRecord, StateRecordSnapshot, StateStoreError};

use super::{
    codec::{RECORDS, RedbReadOperation, RedbRecordBody, RedbStateKeyCodec},
    error::RedbErrorContext,
};

pub(super) enum RedbTransactionCommand {
    Load {
        key: StateKey,
        reply: oneshot::Sender<Result<Option<StateRecordSnapshot>, StateStoreError>>,
    },
    Put {
        record: StateRecord,
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
    Delete {
        key: StateKey,
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
    Commit {
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
    Rollback {
        reply: oneshot::Sender<Result<(), StateStoreError>>,
    },
}

impl RedbTransactionCommand {
    pub(super) fn reply_is_closed(&self) -> bool {
        match self {
            Self::Load { reply, .. } => reply.is_closed(),
            Self::Put { reply, .. } | Self::Delete { reply, .. } | Self::Rollback { reply } => {
                reply.is_closed()
            }
            Self::Commit { reply } => reply.is_closed(),
        }
    }
}

pub(super) struct RedbTransactionCommands {
    receiver: mpsc::Receiver<RedbTransactionCommand>,
}

impl RedbTransactionCommands {
    pub(super) fn new(receiver: mpsc::Receiver<RedbTransactionCommand>) -> Self {
        Self { receiver }
    }

    pub(super) fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }

    pub(super) fn preflight_closed(
        &mut self,
        status: &RedbSessionStatus,
    ) -> ControlFlow<(), Option<RedbTransactionCommand>> {
        match self.receiver.try_recv() {
            Ok(command @ RedbTransactionCommand::Commit { .. }) => {
                ControlFlow::Continue(Some(command))
            }
            Ok(RedbTransactionCommand::Rollback { reply }) => {
                let result = status.finish_without_native();
                let _ = reply.send(result);
                ControlFlow::Break(())
            }
            Ok(command) if command.reply_is_closed() => {
                let _ = status.finish_without_native();
                ControlFlow::Break(())
            }
            Ok(command) => ControlFlow::Continue(Some(command)),
            Err(mpsc::error::TryRecvError::Empty)
            | Err(mpsc::error::TryRecvError::Disconnected) => {
                let _ = status.finish_without_native();
                ControlFlow::Break(())
            }
        }
    }

    fn blocking_recv(&mut self) -> Option<RedbTransactionCommand> {
        self.receiver.blocking_recv()
    }
}

pub(super) struct RedbSessionStatus {
    phase: Mutex<RedbSessionPhase>,
    completion: watch::Sender<Option<Result<(), StateStoreError>>>,
}

impl RedbSessionStatus {
    pub(super) fn new() -> Self {
        let (completion, _) = watch::channel(None);
        Self {
            phase: Mutex::new(RedbSessionPhase::Pending),
            completion,
        }
    }

    pub(super) fn activate(&self) -> bool {
        let mut phase = self.lock_phase();
        if !phase.is_pending() {
            return false;
        }
        *phase = RedbSessionPhase::Active;
        true
    }

    pub(super) fn cancel_pending(
        &self,
        accepting: bool,
        unavailable: StateStoreError,
    ) -> Result<bool, StateStoreError> {
        let mut phase = self.lock_phase();
        if phase.is_pending() {
            if accepting {
                *phase = RedbSessionPhase::Cancelled;
                self.completion.send_replace(Some(Ok(())));
                return Ok(true);
            }
            *phase = RedbSessionPhase::Failed;
            self.completion.send_replace(Some(Err(unavailable.clone())));
            return Err(unavailable);
        }
        if phase.is_active() {
            return Ok(false);
        }
        self.completion_result()
            .unwrap_or(Err(unavailable))
            .map(|()| true)
    }

    pub(super) fn fail_pending(&self, error: StateStoreError) -> bool {
        let mut phase = self.lock_phase();
        if !phase.is_pending() {
            return false;
        }
        *phase = RedbSessionPhase::Failed;
        self.completion.send_replace(Some(Err(error)));
        true
    }

    pub(super) fn finish_active(&self, result: Result<(), StateStoreError>) {
        let terminal = if result.is_ok() {
            RedbSessionPhase::Complete
        } else {
            RedbSessionPhase::Failed
        };
        let mut phase = self.lock_phase();
        if !phase.is_active() {
            return;
        }
        *phase = terminal;
        self.completion.send_replace(Some(result));
    }

    fn finish_without_native(&self) -> Result<(), StateStoreError> {
        let mut phase = self.lock_phase();
        if phase.is_pending() {
            *phase = RedbSessionPhase::Cancelled;
            self.completion.send_replace(Some(Ok(())));
            return Ok(());
        }
        if phase.is_active() {
            *phase = RedbSessionPhase::Complete;
            self.completion.send_replace(Some(Ok(())));
            return Ok(());
        }
        self.completion_result().unwrap_or(Ok(()))
    }

    pub(super) fn ensure_available(
        &self,
        accepting: bool,
        unavailable: StateStoreError,
    ) -> Result<(), StateStoreError> {
        let phase = self.lock_phase();
        if phase.is_active() || (accepting && phase.is_pending()) {
            return Ok(());
        }
        if phase.is_failed() {
            return self.completion_result().unwrap_or(Err(unavailable));
        }
        Err(unavailable)
    }

    pub(super) fn failure(&self) -> Option<StateStoreError> {
        self.terminal_result().and_then(|result| result.err())
    }

    pub(super) fn terminal_result(&self) -> Option<Result<(), StateStoreError>> {
        let _phase = self.lock_phase();
        self.completion_result()
    }

    fn completion_result(&self) -> Option<Result<(), StateStoreError>> {
        let completion = self.completion.subscribe();
        let current = completion.borrow();
        current.clone()
    }

    pub(super) fn subscribe_completion(
        &self,
    ) -> watch::Receiver<Option<Result<(), StateStoreError>>> {
        self.completion.subscribe()
    }

    fn lock_phase(&self) -> MutexGuard<'_, RedbSessionPhase> {
        match self.phase.lock() {
            Ok(phase) => phase,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

enum RedbSessionPhase {
    Pending,
    Active,
    Cancelled,
    Complete,
    Failed,
}

impl RedbSessionPhase {
    fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    fn is_failed(&self) -> bool {
        matches!(self, Self::Failed)
    }
}

pub(super) struct RedbTransactionSession {
    tx: redb::WriteTransaction,
    reads: Arc<RedbReadOperation>,
    commands: RedbTransactionCommands,
    pending: Option<RedbTransactionCommand>,
    initial_recovery_savepoint: Option<redb::Savepoint>,
    quick_repair: bool,
    failed: bool,
}

impl RedbTransactionSession {
    pub(super) fn begin(
        db: &redb::Database,
        quick_repair: bool,
        commands: RedbTransactionCommands,
        pending: Option<RedbTransactionCommand>,
        status: Arc<RedbSessionStatus>,
        reads: Arc<RedbReadOperation>,
    ) -> RedbTransactionSessionStarter<'_> {
        RedbTransactionSessionStarter {
            db,
            quick_repair,
            commands,
            pending,
            status,
            reads,
        }
    }

    fn run(mut self) -> Result<(), StateStoreError> {
        while let Some(command) = self.next_command() {
            match command {
                RedbTransactionCommand::Load { key, reply } => {
                    let result = self.load(key);
                    if !self.send_non_final_reply(reply, result) {
                        return self.rollback();
                    }
                }
                RedbTransactionCommand::Put { record, reply } => {
                    let result = self.put(record);
                    if !self.send_non_final_reply(reply, result) {
                        return self.rollback();
                    }
                }
                RedbTransactionCommand::Delete { key, reply } => {
                    let result = self.delete(key);
                    if !self.send_non_final_reply(reply, result) {
                        return self.rollback();
                    }
                }
                RedbTransactionCommand::Commit { reply } => {
                    let result = self.commit();
                    let completion = match &result {
                        Ok(_) => Ok(()),
                        Err(error) => Err(error.clone()),
                    };
                    let _ = reply.send(result);
                    return completion;
                }
                RedbTransactionCommand::Rollback { reply } => {
                    let result = self.rollback();
                    let completion = result.clone();
                    let _ = reply.send(result);
                    return completion;
                }
            }
        }
        self.rollback()
    }

    fn next_command(&mut self) -> Option<RedbTransactionCommand> {
        if let Some(command) = self.pending.take() {
            Some(command)
        } else {
            self.commands.blocking_recv()
        }
    }

    fn load(&mut self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.ensure_active()?;
        match self.apply_load(key) {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => {
                self.restore_initial_savepoint_and_fail();
                Err(error)
            }
        }
    }

    fn put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        self.ensure_active()?;
        match self.apply_put(record) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.restore_initial_savepoint_and_fail();
                Err(error)
            }
        }
    }

    fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.ensure_active()?;
        match self.apply_delete(key) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.restore_initial_savepoint_and_fail();
                Err(error)
            }
        }
    }

    fn commit(mut self) -> Result<(), StateStoreError> {
        self.ensure_active()?;
        drop(self.initial_recovery_savepoint.take());
        if self.quick_repair {
            self.tx.set_quick_repair(true);
        }
        self.tx
            .commit()
            .map_err(|err| RedbErrorContext::new("commit").backend(err))?;
        Ok(())
    }

    fn rollback(self) -> Result<(), StateStoreError> {
        self.tx
            .abort()
            .map_err(|err| RedbErrorContext::new("rollback").backend(err))
    }

    fn ensure_active(&self) -> Result<(), StateStoreError> {
        if self.failed {
            return Err(StateStoreError::invalid_request(
                "state transaction has failed and must be rolled back",
            ));
        }
        Ok(())
    }

    fn apply_load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match self.read_body(&key, "load")? {
            Some(body) => body.into_state_snapshot(key),
            None => Ok(None),
        }
    }

    fn apply_put(&mut self, record: StateRecord) -> Result<(), StateStoreError> {
        let previous = self.read_body(&record.key, "put")?;
        let revision = RedbRecordBody::next_revision(previous.as_ref())?;
        let now_ms = Self::current_time_ms("put")?;
        let body = RedbRecordBody::new_present(revision, record.value, now_ms, previous.as_ref());
        self.write_body(&record.key, body, "put")?;
        Ok(())
    }

    fn apply_delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        let previous = self.read_body(&key, "delete")?;
        let revision = RedbRecordBody::next_revision(previous.as_ref())?;
        let now_ms = Self::current_time_ms("delete")?;
        let body = RedbRecordBody::new_tombstone(revision, now_ms, previous.as_ref());
        self.write_body(&key, body, "delete")?;
        Ok(())
    }

    fn read_body(
        &self,
        key: &StateKey,
        op: &'static str,
    ) -> Result<Option<RedbRecordBody>, StateStoreError> {
        let context = RedbErrorContext::new(op);
        let encoded = RedbStateKeyCodec::encode_key(key);
        let table = self
            .tx
            .open_table(RECORDS)
            .map_err(|err| context.backend(err))?;
        match table
            .get(encoded.as_slice())
            .map_err(|err| context.backend(err))?
        {
            Some(value) => self
                .reads
                .decode_body(encoded.as_slice(), value.value(), false)
                .map(Some),
            None => Ok(None),
        }
    }

    fn write_body(
        &mut self,
        key: &StateKey,
        body: RedbRecordBody,
        op: &'static str,
    ) -> Result<(), StateStoreError> {
        let context = RedbErrorContext::new(op);
        let encoded = RedbStateKeyCodec::encode_key(key);
        let bytes = body.encode()?;
        let mut table = self
            .tx
            .open_table(RECORDS)
            .map_err(|err| context.backend(err))?;
        table
            .insert(encoded.as_slice(), bytes.as_slice())
            .map_err(|err| context.backend(err))?;
        Ok(())
    }

    fn send_non_final_reply<T>(
        &mut self,
        reply: oneshot::Sender<Result<T, StateStoreError>>,
        result: Result<T, StateStoreError>,
    ) -> bool {
        if reply.send(result).is_err() {
            self.restore_initial_savepoint_and_fail();
            return false;
        }

        true
    }

    fn restore_initial_savepoint_and_fail(&mut self) {
        self.failed = true;
        if let Some(initial_recovery_savepoint) = self.initial_recovery_savepoint.take() {
            let _ = self.tx.restore_savepoint(&initial_recovery_savepoint);
        }
    }

    fn current_time_ms(op: &'static str) -> Result<i64, StateStoreError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| RedbErrorContext::new(op).backend(err))?;
        i64::try_from(duration.as_millis()).map_err(|err| RedbErrorContext::new(op).backend(err))
    }
}

pub(super) struct RedbTransactionSessionStarter<'db> {
    db: &'db redb::Database,
    reads: Arc<RedbReadOperation>,
    quick_repair: bool,
    commands: RedbTransactionCommands,
    pending: Option<RedbTransactionCommand>,
    status: Arc<RedbSessionStatus>,
}

impl RedbTransactionSessionStarter<'_> {
    pub(super) fn run(self) {
        let context = RedbErrorContext::new("begin_transaction");
        let tx = match self.db.begin_write().map_err(|err| context.backend(err)) {
            Ok(tx) => tx,
            Err(error) => {
                self.status.finish_active(Err(error));
                return;
            }
        };
        // Redb only allows ephemeral savepoints before the transaction opens any
        // table, so recovery uses one initial checkpoint instead of per-operation
        // savepoints after put/delete has touched RECORDS.
        let initial_recovery_savepoint = match tx.ephemeral_savepoint() {
            Ok(savepoint) => Some(savepoint),
            Err(error) => {
                let error = context.backend(error);
                drop(tx);
                self.status.finish_active(Err(error));
                return;
            }
        };
        let session = RedbTransactionSession {
            tx,
            reads: self.reads,
            commands: self.commands,
            pending: self.pending,
            initial_recovery_savepoint,
            quick_repair: self.quick_repair,
            failed: false,
        };
        let result = session.run();
        self.status.finish_active(result);
    }
}
