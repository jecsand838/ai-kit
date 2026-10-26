mod coordination;
mod durability;
mod engine;
mod json;
mod records;

use std::{
    collections::HashMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use tokio::{
    runtime::{Builder, Runtime},
    sync::{mpsc, oneshot},
};

use crate::{
    StateStoreBackend, StateStoreError, StateTransactionBackend,
    state::{
        config::LanceStateStoreConfig,
        record::{StateKey, StateListRequest, StateListResponse, StateRecord, StateRecordSnapshot},
    },
};

use self::{
    engine::LanceEngine,
    json::LanceJson,
    records::{LanceSnapshot, StoredPage, StoredRecord},
};

pub(crate) use engine::STATE_TABLE;

pub(crate) struct LanceStateStore {
    owner: Arc<LanceOwner>,
}

impl LanceStateStore {
    pub(crate) async fn connect(config: LanceStateStoreConfig) -> Result<Self, StateStoreError> {
        Ok(Self {
            owner: LanceOwner::connect(config).await?,
        })
    }
}

impl StateStoreBackend for LanceStateStore {
    type Transaction = LanceStateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        let deadline = self.owner.deadline("load")?;
        let (response, receiver) = oneshot::channel();
        self.owner
            .admit(LanceCommand::Load { key, response }, deadline, "load")
            .await?;
        Ok(self
            .owner
            .observe(receiver, deadline, "load")
            .await?
            .as_ref()
            .and_then(StoredRecord::snapshot))
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        let deadline = self.owner.deadline("list")?;
        let (response, receiver) = oneshot::channel();
        self.owner
            .admit(LanceCommand::List { request, response }, deadline, "list")
            .await?;
        Ok(self
            .owner
            .observe(receiver, deadline, "list")
            .await?
            .into_response())
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        let deadline = self.owner.deadline("begin_transaction")?;
        let (response, receiver) = oneshot::channel();
        self.owner
            .admit(
                LanceCommand::Begin { response },
                deadline,
                "begin_transaction",
            )
            .await?;
        let snapshot = self
            .owner
            .observe(receiver, deadline, "begin_transaction")
            .await?;
        Ok(LanceStateTransaction {
            owner: Arc::clone(&self.owner),
            state: TransactionState::Active {
                snapshot,
                overlay: HashMap::new(),
            },
        })
    }
}

/// The validated image has no live native read tasks. Staging is entirely
/// memory-owned and never creates files; only an admitted Commit may publish.
pub(crate) struct LanceStateTransaction {
    // Dispose snapshot/overlay protection before the final joining owner.
    state: TransactionState,
    owner: Arc<LanceOwner>,
}

enum TransactionState {
    Active {
        overlay: HashMap<StateKey, StoredRecord>,
        snapshot: LanceSnapshot,
    },
    Invalid(StateStoreError),
}

impl LanceStateTransaction {
    fn stage(&mut self, key: StateKey, value: Option<LanceJson>) -> Result<(), StateStoreError> {
        let result = match &mut self.state {
            TransactionState::Active { snapshot, overlay } => {
                let revision = overlay
                    .get(&key)
                    .or_else(|| snapshot.record(&key))
                    .map_or(0, |record| record.revision)
                    .checked_add(1)
                    .ok_or_else(|| {
                        StateStoreError::invalid_request(
                            "state revision exceeds the supported u64 boundary",
                        )
                    });
                match revision {
                    Ok(revision) => {
                        overlay.insert(
                            key.clone(),
                            StoredRecord {
                                key,
                                revision,
                                value,
                            },
                        );
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
            TransactionState::Invalid(error) => Err(error.clone()),
        };
        if let Err(error) = &result {
            // Drop both the earlier overlay and the old image: a subsequent
            // commit cannot resurrect the partial authored mutation set.
            self.state = TransactionState::Invalid(error.clone());
        }
        result
    }
}

impl StateTransactionBackend for LanceStateTransaction {
    async fn load(
        &mut self,
        key: StateKey,
    ) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        match &self.state {
            TransactionState::Active { snapshot, overlay } => Ok(overlay
                .get(&key)
                .or_else(|| snapshot.record(&key))
                .and_then(StoredRecord::snapshot)),
            TransactionState::Invalid(error) => Err(error.clone()),
        }
    }

    fn put(
        &mut self,
        record: StateRecord,
    ) -> impl Future<Output = Result<(), StateStoreError>> + Send {
        // Adopt before even the first poll: abandoning the operation must also
        // release a uniquely owned deep input without recursive destruction.
        let value = LanceJson::adopt(record.value);
        async move { self.stage(record.key, Some(value)) }
    }

    async fn delete(&mut self, key: StateKey) -> Result<(), StateStoreError> {
        self.stage(key, None)
    }

    async fn commit(self) -> Result<(), StateStoreError> {
        match self.state {
            TransactionState::Invalid(error) => Err(error),
            TransactionState::Active { snapshot, overlay } => {
                if overlay.is_empty() {
                    // A read-only transaction can serialize at its original
                    // snapshot, regardless of subsequent committed versions.
                    return Ok(());
                }
                let deadline = self.owner.deadline("commit")?;
                let (response, receiver) = oneshot::channel();
                self.owner
                    .admit(
                        LanceCommand::Commit {
                            snapshot,
                            overlay,
                            response,
                        },
                        deadline,
                        "commit",
                    )
                    .await?;
                self.owner.observe(receiver, deadline, "commit").await
            }
        }
    }

    async fn rollback(self) -> Result<(), StateStoreError> {
        // There is no native mutation or outstanding read associated with
        // this unpublished overlay. Dropping it is the complete rollback.
        drop(self.state);
        Ok(())
    }
}

/// The worker owns no Arc back to this joining owner. Transactions retain this
/// owner, and the final owner closes admission, drains commands, then joins.
struct LanceOwner {
    sender: Option<mpsc::Sender<LanceCommand>>,
    worker: Option<JoinHandle<()>>,
    timeout: Duration,
}

impl LanceOwner {
    async fn connect(config: LanceStateStoreConfig) -> Result<Arc<Self>, StateStoreError> {
        config.validate()?;
        let timeout = config.operation_timeout();
        let (sender, mut receiver) = mpsc::channel::<LanceCommand>(32);
        let (initialized, initialization) = oneshot::channel();
        let worker = thread::Builder::new()
            .name("storage-lance-state".to_owned())
            .spawn(move || {
                let opened = LanceEngine::new(config).and_then(|engine| {
                    // Retain the I/O ledger on every initialization outcome until
                    // native child cleanup and runtime shutdown have both settled.
                    LanceCommand::maintain(&engine)?;
                    let protection = engine.shared("connect")?;
                    LanceCommand::run(
                        &engine,
                        "connect",
                        engine.initialize(Arc::clone(&protection)),
                    )?;
                    engine.completed_job(false);
                    Ok(engine)
                });
                let engine = match opened {
                    Ok(engine) => engine,
                    Err(error) => {
                        let _ = initialized.send(Err(error));
                        return;
                    }
                };
                if initialized.send(Ok(())).is_err() {
                    return;
                }
                while let Some(command) = receiver.blocking_recv() {
                    command.execute(&engine);
                }
            })
            .map_err(|_| {
                StateStoreError::unavailable("connect", "could not start native state owner")
            })?;
        // Establish the joining owner before the first cancellation point.
        let owner = Arc::new(Self {
            sender: Some(sender),
            worker: Some(worker),
            timeout,
        });
        let deadline = owner.deadline("connect")?;
        owner.observe(initialization, deadline, "connect").await?;
        Ok(owner)
    }

    fn runtime() -> Result<Runtime, StateStoreError> {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| {
                StateStoreError::unavailable("runtime", "could not start native state runtime")
            })
    }

    fn drain(runtime: &Runtime) {
        // Explicitly poll child cleanup before shutdown. Runtime destruction
        // then joins any blocking native work. It must never be detached to
        // satisfy the observer deadline. Buffered uploads own no hidden disk
        // abort; admitted filesystem writes finish on this owner thread.
        while runtime.metrics().num_alive_tasks() != 0 {
            runtime.block_on(tokio::task::yield_now());
        }
    }

    fn deadline(&self, operation: &str) -> Result<Instant, StateStoreError> {
        Instant::now().checked_add(self.timeout).ok_or_else(|| {
            StateStoreError::backend_operation(
                operation,
                "operation deadline exceeds the clock range",
            )
        })
    }

    fn timed_out(&self, operation: &str) -> StateStoreError {
        StateStoreError::TimedOut {
            operation: operation.to_owned(),
            timeout_ms: self.timeout.as_millis() as u64,
        }
    }

    async fn admit(
        &self,
        command: LanceCommand,
        deadline: Instant,
        operation: &str,
    ) -> Result<(), StateStoreError> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| StateStoreError::unavailable(operation, "state admission is closed"))?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            sender.send(command),
        )
        .await
        .map_err(|_| self.timed_out(operation))?
        .map_err(|_| StateStoreError::unavailable(operation, "native state owner stopped"))
    }

    async fn observe<T>(
        &self,
        receiver: oneshot::Receiver<Result<T, StateStoreError>>,
        deadline: Instant,
        operation: &str,
    ) -> Result<T, StateStoreError> {
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), receiver)
            .await
            .map_err(|_| self.timed_out(operation))?
            .map_err(|_| {
                StateStoreError::unavailable(operation, "native state owner lost its reply")
            })?
    }
}

impl Drop for LanceOwner {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

enum LanceCommand {
    Load {
        key: StateKey,
        response: oneshot::Sender<Result<Option<StoredRecord>, StateStoreError>>,
    },
    List {
        request: StateListRequest,
        response: oneshot::Sender<Result<StoredPage, StateStoreError>>,
    },
    Begin {
        response: oneshot::Sender<Result<LanceSnapshot, StateStoreError>>,
    },
    Commit {
        overlay: HashMap<StateKey, StoredRecord>,
        snapshot: LanceSnapshot,
        response: oneshot::Sender<Result<(), StateStoreError>>,
    },
}

impl LanceCommand {
    fn execute(self, engine: &LanceEngine) {
        // Receiver loss is never a cancellation signal for an admitted job.
        // Every response follows native completion, child drain, runtime join,
        // and the durable wrapper's cleanup/error receipt.
        match self {
            Self::Load { key, response } => {
                let result = (|| {
                    Self::maintain(engine)?;
                    let protection = engine.shared("load")?;
                    Self::run(engine, "load", async {
                        let snapshot = engine.snapshot(Arc::clone(&protection)).await?;
                        Ok(snapshot
                            .record(&key)
                            .filter(|record| record.value.is_some())
                            .cloned())
                    })
                })();
                engine.completed_job(false);
                let _ = response.send(result);
            }
            Self::List { request, response } => {
                let result = (|| {
                    Self::maintain(engine)?;
                    let protection = engine.shared("list")?;
                    Self::run(engine, "list", async {
                        Ok(engine
                            .snapshot(Arc::clone(&protection))
                            .await?
                            .list(&request))
                    })
                })();
                engine.completed_job(false);
                let _ = response.send(result);
            }
            Self::Begin { response } => {
                let result = (|| {
                    Self::maintain(engine)?;
                    let protection = engine.shared("begin_transaction")?;
                    Self::run(
                        engine,
                        "begin_transaction",
                        engine.snapshot(Arc::clone(&protection)),
                    )
                })();
                engine.completed_job(false);
                let _ = response.send(result);
            }
            Self::Commit {
                snapshot,
                overlay,
                response,
            } => {
                // Keep the original dataset, image, and lease outside the
                // future until child drain, runtime destruction, and cleanup.
                let result = Self::run(engine, "commit", engine.commit(&snapshot, overlay))
                    .and_then(|image| {
                        // Publication alone is insufficient: run has drained
                        // native children, destroyed their runtime, and checked
                        // durable cleanup before this cache promotion.
                        if let Some(image) = image {
                            engine
                                .remember(&image)
                                .inspect_err(|_| engine.invalidate_image())?;
                        }
                        Ok(())
                    });
                drop(snapshot);
                engine.completed_job(result.is_err());
                let _ = response.send(result);
            }
        }
    }

    fn maintain(engine: &LanceEngine) -> Result<(), StateStoreError> {
        let result = (|| {
            let Some(protection) = engine.maintenance_protection()? else {
                return Ok(());
            };
            let inspected = Self::run(
                engine,
                "maintenance",
                engine.maintenance_snapshot(Arc::clone(&protection)),
            )?;
            if let Some((snapshot, full)) = inspected {
                if full {
                    // Reclamation starts only after the rewrite's native
                    // children, runtime and durable cleanup have settled.
                    Self::run(engine, "maintenance", engine.maintenance_rewrite(&snapshot))?;
                }
                // Keep only immutable payloads through reclamation. The native
                // snapshot is released at the original ownership boundary.
                let image = Arc::clone(snapshot.image());
                drop(snapshot);
                Self::run(
                    engine,
                    "maintenance",
                    engine.maintenance_cleanup(full, Arc::clone(&protection), &image),
                )?;
            }
            engine.settled_maintenance(true);
            Ok(())
        })();
        result.map_err(|error: StateStoreError| {
            engine.invalidate_image();
            engine.settled_maintenance(false);
            StateStoreError::backend_operation("maintenance", error.to_string())
        })
    }

    fn run<T>(
        engine: &LanceEngine,
        operation: &str,
        future: impl Future<Output = Result<T, StateStoreError>>,
    ) -> Result<T, StateStoreError> {
        engine
            .begin_job()
            .inspect_err(|_| engine.invalidate_image())?;
        let runtime = LanceOwner::runtime().inspect_err(|_| engine.invalidate_image())?;
        let result =
            catch_unwind(AssertUnwindSafe(|| runtime.block_on(future))).unwrap_or_else(|_| {
                Err(StateStoreError::backend_operation(
                    operation,
                    "native state operation panicked; publication may be uncertain",
                ))
            });
        LanceOwner::drain(&runtime);
        drop(runtime);
        let result = match (result, engine.finish_job(operation)) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Ok(())) => Err(primary),
            (Err(primary), Err(cleanup)) => Err(Self::cleanup_context(primary, cleanup)),
        };
        if result.is_err() {
            engine.invalidate_image();
        }
        result
    }

    fn cleanup_context(mut primary: StateStoreError, cleanup: StateStoreError) -> StateStoreError {
        match &mut primary {
            StateStoreError::InvalidConfig(message)
            | StateStoreError::InvalidRequest(message)
            | StateStoreError::Serialization(message)
            | StateStoreError::Unavailable { message, .. }
            | StateStoreError::BackendOperation { message, .. }
            | StateStoreError::TransactionConflict { message, .. } => {
                message.push_str("; cleanup also failed: ");
                message.push_str(&cleanup.to_string());
            }
            StateStoreError::TimedOut { .. } => {
                // The only job-side timeout is bootstrap-lock admission,
                // before acquiring the lock or starting native I/O. Operation
                // observation deadlines run outside this receipt path.
            }
        }
        primary
    }
}
