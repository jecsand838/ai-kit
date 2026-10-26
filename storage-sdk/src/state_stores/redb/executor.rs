use std::{
    fmt,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    StateKey, StateListRequest, StateListResponse, StateRecordSnapshot, StateStoreError,
    state_stores::config::{RedbStateStoreConfig, RedbStateStoreLocation},
};

use super::{
    codec::RedbReadOperation,
    error::RedbErrorContext,
    live_transaction::RedbStateTransaction,
    transaction::RedbSchemaInitializer,
    transaction_session::{RedbSessionStatus, RedbTransactionCommands},
    worker::{
        RedbReadCommand, RedbReadWorker, RedbWorkerThreads, RedbWriteCommand, RedbWriteWorkerLoop,
    },
};

const REDB_READ_QUEUE_CAPACITY: usize = 16;
const REDB_WRITE_QUEUE_CAPACITY: usize = 8;
const REDB_TRANSACTION_QUEUE_CAPACITY: usize = 8;

pub(crate) struct RedbExecutor {
    inner: Arc<RedbExecutorInner>,
}

impl RedbExecutor {
    pub(crate) async fn start(config: RedbStateStoreConfig) -> Result<Self, StateStoreError> {
        let context = RedbErrorContext::new("connect");
        let timeout = config.operation_timeout();
        let timeout_ms = config.operation_timeout_ms();
        let (ready_tx, ready_rx) = oneshot::channel();
        let _open_thread = RedbOpenThread::spawn(config, ready_tx)?;

        match tokio::time::timeout(timeout, ready_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(context.closed()),
            Err(_) => Err(context.timeout(timeout_ms)),
        }
    }

    pub(crate) async fn load(
        &self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RedbReadCommand::Load {
            key,
            reply: reply_tx,
        };
        let sender = self.inner.reads.sender()?;
        self.inner.timeout.enqueue("load", &sender, command).await?;
        self.inner.timeout.read_reply("load", reply_rx).await
    }

    pub(crate) async fn list(
        &self,
        request: StateListRequest,
    ) -> Result<StateListResponse, StateStoreError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RedbReadCommand::List {
            request,
            reply: reply_tx,
        };
        let sender = self.inner.reads.sender()?;
        self.inner.timeout.enqueue("list", &sender, command).await?;
        self.inner.timeout.read_reply("list", reply_rx).await
    }

    pub(crate) async fn begin_transaction(&self) -> Result<RedbStateTransaction, StateStoreError> {
        let (command_tx, command_rx) = mpsc::channel(REDB_TRANSACTION_QUEUE_CAPACITY);
        let status = Arc::new(RedbSessionStatus::new());
        let command = RedbWriteCommand::Open {
            quick_repair: self.inner.summary.quick_repair,
            commands: RedbTransactionCommands::new(command_rx),
            status: status.clone(),
        };
        let sender = self.inner.write.sender("begin_transaction")?;
        self.inner
            .timeout
            .enqueue("begin_transaction", &sender, command)
            .await?;
        Ok(RedbStateTransaction::new(
            command_tx,
            self.inner.timeout,
            status,
            self.inner.runtime.clone(),
        ))
    }

    fn start_sync(config: RedbStateStoreConfig) -> Result<Self, StateStoreError> {
        let context = RedbErrorContext::new("connect");
        let mut builder = redb::Builder::new();
        builder.set_cache_size(config.cache_size_bytes());
        let quick_repair = !config.location().is_memory();
        let database = match config.location() {
            RedbStateStoreLocation::Memory => builder
                .create_with_backend(redb::backends::InMemoryBackend::new())
                .map_err(|err| context.backend(err))?,
            RedbStateStoreLocation::File(path) => {
                builder.create(path).map_err(|err| context.backend(err))?
            }
        };
        let database = Arc::new(database);
        RedbSchemaInitializer::new(&database, quick_repair).initialize()?;

        let reads = Arc::new(RedbReadOperation::new(database.clone()));
        let read_worker_count = Self::read_worker_count();
        let mut handles = Vec::with_capacity(read_worker_count + 1);
        let mut read_senders = Vec::with_capacity(read_worker_count);
        for index in 0..read_worker_count {
            let (sender, receiver) = mpsc::channel(REDB_READ_QUEUE_CAPACITY);
            let worker = RedbReadWorker::new(reads.clone(), receiver);
            match thread::Builder::new()
                .name(format!("storage-sdk-redb-read-{index}"))
                .spawn(move || worker.run())
            {
                Ok(handle) => {
                    read_senders.push(sender);
                    handles.push(handle);
                }
                Err(err) => {
                    drop(read_senders);
                    RedbWorkerThreads::new(handles).join_all();
                    return Err(context.backend(err));
                }
            }
        }

        let lifecycle = Arc::new(RedbRuntimeLifecycle::new());
        let (write_sender, write_receiver) = mpsc::channel(REDB_WRITE_QUEUE_CAPACITY);
        let write_worker =
            RedbWriteWorkerLoop::new(database, write_receiver, lifecycle.clone(), reads);
        match thread::Builder::new()
            .name("storage-sdk-redb-write".to_owned())
            .spawn(move || write_worker.run())
        {
            Ok(handle) => handles.push(handle),
            Err(err) => {
                drop(read_senders);
                RedbWorkerThreads::new(handles).join_all();
                return Err(context.backend(err));
            }
        }

        let runtime = Arc::new(RedbWorkerRuntime::new(
            lifecycle,
            RedbWorkerThreads::new(handles),
        ));
        Ok(Self {
            inner: Arc::new(RedbExecutorInner {
                reads: RedbReadWorkers::new(read_senders),
                write: RedbWriteWorker::new(write_sender),
                timeout: RedbTimeoutPolicy::new(
                    config.operation_timeout(),
                    config.operation_timeout_ms(),
                ),
                summary: RedbExecutorSummary::new(
                    config.location(),
                    config.operation_timeout(),
                    config.cache_size_bytes(),
                    read_worker_count,
                    quick_repair,
                ),
                runtime,
            }),
        })
    }

    fn read_worker_count() -> usize {
        thread::available_parallelism().map_or(2, |parallelism| parallelism.get().clamp(1, 4))
    }
}

struct RedbOpenThread {
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RedbOpenThread {
    fn spawn(
        config: RedbStateStoreConfig,
        ready: oneshot::Sender<Result<RedbExecutor, StateStoreError>>,
    ) -> Result<Arc<Self>, StateStoreError> {
        let context = RedbErrorContext::new("connect");
        let owner = Arc::new(Self {
            handle: Mutex::new(None),
        });
        let thread_owner = owner.clone();
        let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
        let handle = thread::Builder::new()
            .name("storage-sdk-redb-open".to_owned())
            .spawn(move || {
                if start_rx.recv().is_ok() {
                    let result = RedbExecutor::start_sync(config);
                    let _ = ready.send(result);
                }
                thread_owner.release();
            })
            .map_err(|err| context.backend(err))?;
        owner.install(handle);
        if start_tx.send(()).is_err() {
            owner.settle();
            return Err(context.closed());
        }
        Ok(owner)
    }

    fn install(&self, handle: thread::JoinHandle<()>) {
        let mut slot = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *slot = Some(handle);
    }

    fn release(&self) {
        let _handle = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }

    fn settle(&self) {
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }
}

impl Clone for RedbExecutor {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl fmt::Debug for RedbExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedbExecutor")
            .field("summary", &self.inner.summary)
            .finish()
    }
}

struct RedbExecutorInner {
    reads: RedbReadWorkers,
    write: RedbWriteWorker,
    timeout: RedbTimeoutPolicy,
    summary: RedbExecutorSummary,
    runtime: Arc<RedbWorkerRuntime>,
}

impl Drop for RedbExecutorInner {
    fn drop(&mut self) {
        self.runtime.seal();
        self.reads.close();
        self.write.close();
    }
}

struct RedbExecutorSummary {
    location: RedbExecutorLocation,
    operation_timeout: Duration,
    cache_size_bytes: usize,
    read_worker_count: usize,
    quick_repair: bool,
}

impl RedbExecutorSummary {
    fn new(
        location: &RedbStateStoreLocation,
        operation_timeout: Duration,
        cache_size_bytes: usize,
        read_worker_count: usize,
        quick_repair: bool,
    ) -> Self {
        Self {
            location: RedbExecutorLocation::from_config(location),
            operation_timeout,
            cache_size_bytes,
            read_worker_count,
            quick_repair,
        }
    }
}

impl fmt::Debug for RedbExecutorSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedbExecutorSummary")
            .field("location", &self.location)
            .field("operation_timeout", &self.operation_timeout)
            .field("cache_size_bytes", &self.cache_size_bytes)
            .field("read_worker_count", &self.read_worker_count)
            .field("quick_repair", &self.quick_repair)
            .finish()
    }
}

enum RedbExecutorLocation {
    Memory,
    File(PathBuf),
}

impl RedbExecutorLocation {
    fn from_config(location: &RedbStateStoreLocation) -> Self {
        match location {
            RedbStateStoreLocation::Memory => Self::Memory,
            RedbStateStoreLocation::File(path) => Self::File(path.clone()),
        }
    }
}

impl fmt::Debug for RedbExecutorLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("Memory"),
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
        }
    }
}

struct RedbReadWorkers {
    senders: Mutex<Vec<mpsc::Sender<RedbReadCommand>>>,
    next: AtomicUsize,
}

impl RedbReadWorkers {
    fn new(senders: Vec<mpsc::Sender<RedbReadCommand>>) -> Self {
        Self {
            senders: Mutex::new(senders),
            next: AtomicUsize::new(0),
        }
    }

    fn sender(&self) -> Result<mpsc::Sender<RedbReadCommand>, StateStoreError> {
        let context = RedbErrorContext::new("read");
        let senders = self.senders.lock().map_err(|_| context.closed())?;
        if senders.is_empty() {
            return Err(context.closed());
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) % senders.len();
        Ok(senders[index].clone())
    }

    fn close(&self) {
        if let Ok(mut senders) = self.senders.lock() {
            senders.clear();
        }
    }
}

struct RedbWriteWorker {
    sender: Mutex<Option<mpsc::Sender<RedbWriteCommand>>>,
}

impl RedbWriteWorker {
    fn new(sender: mpsc::Sender<RedbWriteCommand>) -> Self {
        Self {
            sender: Mutex::new(Some(sender)),
        }
    }

    fn sender(
        &self,
        operation: &'static str,
    ) -> Result<mpsc::Sender<RedbWriteCommand>, StateStoreError> {
        let context = RedbErrorContext::new(operation);
        let guard = self.sender.lock().map_err(|_| context.closed())?;
        guard.as_ref().cloned().ok_or_else(|| context.closed())
    }

    fn close(&self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
    }
}

pub(super) struct RedbTimeoutPolicy {
    operation_timeout: Duration,
    operation_timeout_ms: u64,
}

impl RedbTimeoutPolicy {
    fn new(operation_timeout: Duration, operation_timeout_ms: u64) -> Self {
        Self {
            operation_timeout,
            operation_timeout_ms,
        }
    }

    pub(super) async fn enqueue<T>(
        &self,
        op: &'static str,
        sender: &mpsc::Sender<T>,
        command: T,
    ) -> Result<(), StateStoreError> {
        let context = RedbErrorContext::new(op);
        match tokio::time::timeout(self.operation_timeout, sender.reserve()).await {
            Ok(Ok(permit)) => {
                permit.send(command);
                Ok(())
            }
            Ok(Err(_)) => Err(context.closed()),
            Err(_) => Err(context.timeout(self.operation_timeout_ms)),
        }
    }

    pub(super) async fn read_reply<T>(
        &self,
        op: &'static str,
        reply: oneshot::Receiver<Result<T, StateStoreError>>,
    ) -> Result<T, StateStoreError> {
        let context = RedbErrorContext::new(op);
        match tokio::time::timeout(self.operation_timeout, reply).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(context.closed()),
            Err(_) => Err(context.timeout(self.operation_timeout_ms)),
        }
    }

    pub(super) async fn read_transaction_reply<T>(
        &self,
        op: &'static str,
        reply: oneshot::Receiver<Result<T, StateStoreError>>,
        mut shutdown: watch::Receiver<bool>,
        runtime: &RedbWorkerRuntime,
        status: &RedbSessionStatus,
    ) -> Result<Result<T, StateStoreError>, StateStoreError> {
        let context = RedbErrorContext::new(op);
        let mut reply = Box::pin(reply);
        let response = async {
            loop {
                tokio::select! {
                    result = &mut reply => {
                        return result.map_err(|_| {
                            runtime
                                .session_failure(status)
                                .unwrap_or_else(|| context.closed())
                        });
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() {
                            return Err(context.closed());
                        }
                        runtime.session_available(op, status)?;
                    }
                }
            }
        };
        match tokio::time::timeout(self.operation_timeout, response).await {
            Ok(result) => result,
            Err(_) => Err(context.timeout(self.operation_timeout_ms)),
        }
    }

    pub(super) async fn wait_for_cleanup(
        &self,
        op: &'static str,
        mut completion: watch::Receiver<Option<Result<(), StateStoreError>>>,
    ) -> Result<(), StateStoreError> {
        let context = RedbErrorContext::new(op);
        if let Some(result) = completion.borrow().clone() {
            return result;
        }
        let wait = async {
            loop {
                completion.changed().await.map_err(|_| context.closed())?;
                if let Some(result) = completion.borrow().clone() {
                    return result;
                }
            }
        };
        match tokio::time::timeout(self.operation_timeout, wait).await {
            Ok(result) => result,
            Err(_) => Err(context.timeout(self.operation_timeout_ms)),
        }
    }
}

impl Clone for RedbTimeoutPolicy {
    fn clone(&self) -> Self {
        *self
    }
}

impl Copy for RedbTimeoutPolicy {}

pub(super) struct RedbWorkerRuntime {
    lifecycle: Arc<RedbRuntimeLifecycle>,
    threads: Mutex<Option<RedbWorkerThreads>>,
}

impl RedbWorkerRuntime {
    fn new(lifecycle: Arc<RedbRuntimeLifecycle>, threads: RedbWorkerThreads) -> Self {
        Self {
            lifecycle,
            threads: Mutex::new(Some(threads)),
        }
    }

    fn seal(&self) {
        self.lifecycle.seal();
    }

    pub(super) fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.lifecycle.subscribe_shutdown()
    }

    pub(super) fn session_available(
        &self,
        operation: &'static str,
        status: &RedbSessionStatus,
    ) -> Result<(), StateStoreError> {
        self.lifecycle.session_available(operation, status)
    }

    pub(super) fn session_failure(&self, status: &RedbSessionStatus) -> Option<StateStoreError> {
        status.failure()
    }

    pub(super) fn cancel_pending(
        &self,
        operation: &'static str,
        status: &RedbSessionStatus,
    ) -> Result<bool, StateStoreError> {
        self.lifecycle.cancel_pending(operation, status)
    }
}

impl Drop for RedbWorkerRuntime {
    fn drop(&mut self) {
        let threads = self
            .threads
            .lock()
            .ok()
            .and_then(|mut threads| threads.take());
        let Some(threads) = threads else {
            return;
        };
        threads.join_all();
    }
}

pub(super) struct RedbRuntimeLifecycle {
    accepting: Mutex<bool>,
    shutdown: watch::Sender<bool>,
}

impl RedbRuntimeLifecycle {
    fn new() -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            accepting: Mutex::new(true),
            shutdown,
        }
    }

    fn seal(&self) {
        let mut accepting = match self.accepting.lock() {
            Ok(accepting) => accepting,
            Err(poisoned) => poisoned.into_inner(),
        };
        if *accepting {
            *accepting = false;
            self.shutdown.send_replace(true);
        }
    }

    pub(super) fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub(super) fn admit(&self, status: &RedbSessionStatus) -> bool {
        let accepting = match self.accepting.lock() {
            Ok(accepting) => accepting,
            Err(_) => return false,
        };
        if !*accepting {
            return false;
        }
        status.activate()
    }

    fn cancel_pending(
        &self,
        operation: &'static str,
        status: &RedbSessionStatus,
    ) -> Result<bool, StateStoreError> {
        let context = RedbErrorContext::new(operation);
        let accepting = match self.accepting.lock() {
            Ok(accepting) => accepting,
            Err(_) => {
                let error = context.closed();
                status.fail_pending(error.clone());
                return status
                    .terminal_result()
                    .unwrap_or(Err(error))
                    .map(|()| true);
            }
        };
        status.cancel_pending(*accepting, context.closed())
    }

    fn session_available(
        &self,
        operation: &'static str,
        status: &RedbSessionStatus,
    ) -> Result<(), StateStoreError> {
        let context = RedbErrorContext::new(operation);
        let accepting = self.accepting.lock().map_err(|_| context.closed())?;
        status.ensure_available(*accepting, context.closed())
    }
}
