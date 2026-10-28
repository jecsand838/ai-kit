use std::{ops::ControlFlow, sync::Arc, thread};

use tokio::sync::{mpsc, oneshot};

use crate::{StateKey, StateListRequest, StateListResponse, StateRecordSnapshot, StateStoreError};

use super::{
    codec::RedbReadOperation,
    error::RedbErrorContext,
    executor::RedbRuntimeLifecycle,
    transaction_session::{RedbSessionStatus, RedbTransactionCommands, RedbTransactionSession},
};

pub(super) enum RedbReadCommand {
    Load {
        key: StateKey,
        reply: oneshot::Sender<Result<Option<StateRecordSnapshot>, StateStoreError>>,
    },
    List {
        request: StateListRequest,
        reply: oneshot::Sender<Result<StateListResponse, StateStoreError>>,
    },
}

impl RedbReadCommand {
    fn execute(self, reads: &RedbReadOperation) {
        match self {
            Self::Load { key, reply } => {
                let _ = reply.send(reads.load(key));
            }
            Self::List { request, reply } => {
                let _ = reply.send(reads.list(request));
            }
        }
    }
}

pub(super) enum RedbWriteCommand {
    Open {
        quick_repair: bool,
        commands: RedbTransactionCommands,
        status: Arc<RedbSessionStatus>,
    },
}

impl RedbWriteCommand {
    fn execute(
        self,
        db: &redb::Database,
        lifecycle: &RedbRuntimeLifecycle,
        reads: Arc<RedbReadOperation>,
    ) {
        match self {
            Self::Open {
                quick_repair,
                mut commands,
                status,
            } => {
                if !lifecycle.admit(&status) {
                    status.fail_pending(RedbErrorContext::new("begin_transaction").closed());
                    return;
                }
                let mut pending = None;
                if commands.is_closed() {
                    match commands.preflight_closed(&status) {
                        ControlFlow::Break(()) => return,
                        ControlFlow::Continue(command) => pending = command,
                    }
                }
                RedbTransactionSession::begin(db, quick_repair, commands, pending, status, reads)
                    .run();
            }
        }
    }
}

pub(super) struct RedbReadWorker {
    reads: Arc<RedbReadOperation>,
    receiver: mpsc::Receiver<RedbReadCommand>,
}

impl RedbReadWorker {
    pub(super) fn new(
        reads: Arc<RedbReadOperation>,
        receiver: mpsc::Receiver<RedbReadCommand>,
    ) -> Self {
        Self { reads, receiver }
    }

    pub(super) fn run(mut self) {
        while let Some(command) = self.receiver.blocking_recv() {
            command.execute(&self.reads);
        }
    }
}

pub(super) struct RedbWriteWorkerLoop {
    db: Arc<redb::Database>,
    receiver: mpsc::Receiver<RedbWriteCommand>,
    lifecycle: Arc<RedbRuntimeLifecycle>,
    reads: Arc<RedbReadOperation>,
}

impl RedbWriteWorkerLoop {
    pub(super) fn new(
        db: Arc<redb::Database>,
        receiver: mpsc::Receiver<RedbWriteCommand>,
        lifecycle: Arc<RedbRuntimeLifecycle>,
        reads: Arc<RedbReadOperation>,
    ) -> Self {
        Self {
            db,
            receiver,
            lifecycle,
            reads,
        }
    }

    pub(super) fn run(mut self) {
        while let Some(command) = self.receiver.blocking_recv() {
            command.execute(&self.db, &self.lifecycle, self.reads.clone());
        }
    }
}

pub(super) struct RedbWorkerThreads {
    handles: Vec<thread::JoinHandle<()>>,
}

impl RedbWorkerThreads {
    pub(super) fn new(handles: Vec<thread::JoinHandle<()>>) -> Self {
        Self { handles }
    }

    pub(super) fn join_all(self) {
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}
