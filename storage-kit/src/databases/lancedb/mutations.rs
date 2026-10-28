use std::{
    fs::{File, OpenOptions, TryLockError},
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
    CatalogRequest, CatalogResponse, CommonConfig, DataRequest, DataResponse, IndexRequest,
    IndexResponse, LanceConfig, StorageError,
};

use super::manager::LanceManager;

/// Owns admitted native mutations independently of their calling runtime.
/// The worker never owns this owner: final drop closes admission and joins it.
#[derive(Debug)]
pub(super) struct LanceMutations {
    sender: Option<mpsc::Sender<LanceMutation>>,
    worker: Option<JoinHandle<()>>,
    timeout: Duration,
}

impl LanceMutations {
    pub(super) async fn connect(
        common: CommonConfig,
        config: LanceConfig,
    ) -> Result<(Arc<Self>, LanceManager), StorageError> {
        let (sender, mut receiver) = mpsc::channel::<LanceMutation>(32);
        let (initialized, initialization) = oneshot::channel();
        let timeout = common.request_timeout();
        let connect_timeout = common.connect_timeout();
        let worker = thread::Builder::new()
            .name("storage-lance-mutations".to_owned())
            .spawn(move || {
                let manager = match Self::runtime() {
                    Ok(runtime) => {
                        let result = runtime.block_on(LanceManager::connect(common, config));
                        // File and memory object stores have no reactor-bound transport.
                        // Settle native startup work before publishing the connection.
                        drop(runtime);
                        result
                    }
                    Err(error) => Err(error),
                };
                let manager = match manager {
                    Ok(manager) => manager,
                    Err(error) => {
                        let _ = initialized.send(Err(error));
                        return;
                    }
                };
                if initialized.send(Ok(manager.clone())).is_err() {
                    return;
                }
                while let Some(mutation) = receiver.blocking_recv() {
                    mutation.execute(&manager);
                }
            })
            .map_err(|error| StorageError::backend_op("connect", error.to_string()))?;
        // This owner exists before the first await, so cancelled construction joins.
        let owner = Arc::new(Self {
            sender: Some(sender),
            worker: Some(worker),
            timeout,
        });
        let manager = tokio::time::timeout(connect_timeout, initialization)
            .await
            .map_err(|_| StorageError::backend_op("connect", "LanceDB initialization timed out"))?
            .map_err(|_| {
                StorageError::backend_op("connect", "LanceDB worker stopped during initialization")
            })??;
        Ok((owner, manager))
    }

    fn runtime() -> Result<Runtime, StorageError> {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| StorageError::backend_op("lance.runtime", error.to_string()))
    }

    async fn admit(&self, mutation: LanceMutation) -> Result<(), StorageError> {
        let deadline = tokio::time::Instant::from_std(mutation.deadline());
        let sender = self.sender.as_ref().ok_or_else(|| {
            StorageError::backend_op("lance.mutation", "LanceDB mutation admission is closed")
        })?;
        tokio::time::timeout_at(deadline, sender.send(mutation))
            .await
            .map_err(|_| LanceMutation::timed_out())?
            .map_err(|_| {
                StorageError::backend_op("lance.mutation", "LanceDB mutation worker stopped")
            })
    }

    pub(super) async fn catalog(
        &self,
        request: CatalogRequest,
    ) -> Result<CatalogResponse, StorageError> {
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            StorageError::InvalidConfig(
                "LanceDB request timeout exceeds the clock range".to_owned(),
            )
        })?;
        let (response, receiver) = oneshot::channel();
        self.admit(LanceMutation::Catalog {
            request,
            response,
            deadline,
        })
        .await?;
        Self::observe(receiver, deadline).await
    }

    pub(super) async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            StorageError::InvalidConfig(
                "LanceDB request timeout exceeds the clock range".to_owned(),
            )
        })?;
        let (response, receiver) = oneshot::channel();
        self.admit(LanceMutation::Data {
            request,
            response,
            deadline,
        })
        .await?;
        Self::observe(receiver, deadline).await
    }

    pub(super) async fn index(&self, request: IndexRequest) -> Result<IndexResponse, StorageError> {
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            StorageError::InvalidConfig(
                "LanceDB request timeout exceeds the clock range".to_owned(),
            )
        })?;
        let (response, receiver) = oneshot::channel();
        self.admit(LanceMutation::Index {
            request,
            response,
            deadline,
        })
        .await?;
        Self::observe(receiver, deadline).await
    }

    async fn observe<T>(
        receiver: oneshot::Receiver<Result<T, StorageError>>,
        deadline: Instant,
    ) -> Result<T, StorageError> {
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), receiver)
            .await
            .map_err(|_| LanceMutation::timed_out())?
            .map_err(|_| {
                StorageError::backend_op("lance.mutation", "LanceDB mutation worker stopped")
            })?
    }
}

impl Drop for LanceMutations {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            // No worker operation requires progress from a caller's Tokio runtime.
            // Native blocking I/O cannot be preempted: retain ownership until it ends.
            let _ = worker.join();
        }
    }
}

#[derive(Debug)]
enum LanceMutation {
    Catalog {
        request: CatalogRequest,
        response: oneshot::Sender<Result<CatalogResponse, StorageError>>,
        deadline: Instant,
    },
    Data {
        request: DataRequest,
        response: oneshot::Sender<Result<DataResponse, StorageError>>,
        deadline: Instant,
    },
    Index {
        request: IndexRequest,
        response: oneshot::Sender<Result<IndexResponse, StorageError>>,
        deadline: Instant,
    },
}

impl LanceMutation {
    fn deadline(&self) -> Instant {
        match self {
            Self::Catalog { deadline, .. }
            | Self::Data { deadline, .. }
            | Self::Index { deadline, .. } => *deadline,
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            Self::Catalog { response, .. } => response.is_closed(),
            Self::Data { response, .. } => response.is_closed(),
            Self::Index { response, .. } => response.is_closed(),
        }
    }

    fn timed_out() -> StorageError {
        StorageError::backend_op(
            "lance.mutation",
            "LanceDB mutation admission or observation timed out",
        )
    }

    fn file_lock(manager: &LanceManager, deadline: Instant) -> Result<Option<File>, StorageError> {
        if manager.config().path() == std::path::Path::new("memory://") {
            return Ok(None);
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(
                manager
                    .config()
                    .path()
                    .join(".storage-kit-atomic-partition-replace.lock"),
            )
            .map_err(|error| manager.map_err("lance.mutation.lock", error))?;
        loop {
            if Instant::now() >= deadline {
                return Err(Self::timed_out());
            }
            match file.try_lock() {
                Ok(()) => return Ok(Some(file)),
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(2))
                }
                Err(TryLockError::WouldBlock) => return Err(Self::timed_out()),
                Err(TryLockError::Error(error)) => {
                    return Err(manager.map_err("lance.mutation.lock", error));
                }
            }
        }
    }

    fn execute(self, manager: &LanceManager) {
        if self.is_closed() {
            return;
        }
        let deadline = self.deadline();
        let process_lock = manager.mutation_process_lock();
        let guards = (|| {
            let process_guard = loop {
                if self.is_closed() || Instant::now() >= deadline {
                    return Err(Self::timed_out());
                }
                match Arc::clone(&process_lock).try_lock_owned() {
                    Ok(guard) => break guard,
                    Err(_) => thread::sleep(Duration::from_millis(2)),
                }
            };
            let file_guard = Self::file_lock(manager, deadline)?;
            Ok((process_guard, file_guard))
        })();
        match self {
            Self::Catalog {
                request, response, ..
            } => Self::settle(manager.catalog(request), response, guards, deadline),
            Self::Data {
                request, response, ..
            } => Self::settle(manager.data(request), response, guards, deadline),
            Self::Index {
                request, response, ..
            } => Self::settle(manager.index(request), response, guards, deadline),
        }
    }

    fn settle<T>(
        operation: impl Future<Output = Result<T, StorageError>>,
        response: oneshot::Sender<Result<T, StorageError>>,
        guards: Result<(tokio::sync::OwnedMutexGuard<()>, Option<File>), StorageError>,
        deadline: Instant,
    ) {
        let result = match guards {
            Err(error) => Err(error),
            Ok(guards) => {
                let result = match LanceMutations::runtime() {
                    Err(error) => Err(error),
                    Ok(runtime) => {
                        // Native Lance JoinSet drops abort child tasks on an error.
                        // Retain both mutation guards until this runtime drains local
                        // tasks and blocking file writes, even when the future panics.
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            runtime.block_on(async {
                                if response.is_closed() || Instant::now() >= deadline {
                                    return Err(Self::timed_out());
                                }
                                tokio::time::timeout_at(
                                    tokio::time::Instant::from_std(deadline),
                                    operation,
                                )
                                .await
                                .map_err(|_| Self::timed_out())?
                            })
                        }));
                        drop(runtime);
                        result.unwrap_or_else(|_| {
                            Err(StorageError::backend_op(
                                "lance.mutation",
                                "native LanceDB mutation panicked",
                            ))
                        })
                    }
                };
                drop(guards);
                result
            }
        };
        let _ = response.send(result);
    }
}
