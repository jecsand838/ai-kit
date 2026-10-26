use std::{
    fs::{File, OpenOptions, TryLockError},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::StateStoreError;

/// An independently opened descriptor is never upgraded, duplicated, or
/// relocked. Its final owner releases protection only after native cleanup.
pub(super) enum LanceProtection {
    Memory { snapshots: Arc<AtomicUsize> },
    Directory { _file: File },
}

impl Drop for LanceProtection {
    fn drop(&mut self) {
        match self {
            Self::Memory { snapshots } => {
                snapshots.fetch_sub(1, Ordering::SeqCst);
            }
            Self::Directory { .. } => {}
        }
    }
}

pub(super) enum LanceCoordination {
    Memory { snapshots: Arc<AtomicUsize> },
    Directory { path: PathBuf, timeout: Duration },
}

impl LanceCoordination {
    pub(super) fn memory() -> Self {
        Self::Memory {
            snapshots: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn directory(path: PathBuf, timeout: Duration) -> Self {
        Self::Directory { path, timeout }
    }

    fn open(&self, operation: &str) -> Result<Option<File>, StateStoreError> {
        match self {
            Self::Memory { .. } => Ok(None),
            Self::Directory { path, .. } => OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map(Some)
                .map_err(|error| {
                    StateStoreError::backend_operation(
                        operation,
                        format!(
                            "state coordination file could not be opened ({:?})",
                            error.kind()
                        ),
                    )
                }),
        }
    }

    pub(super) fn shared(&self, operation: &str) -> Result<Arc<LanceProtection>, StateStoreError> {
        match self {
            Self::Memory { snapshots } => {
                snapshots.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(LanceProtection::Memory {
                    snapshots: Arc::clone(snapshots),
                }))
            }
            Self::Directory { timeout, .. } => {
                let file = self.open(operation)?.ok_or_else(|| {
                    StateStoreError::backend_operation(
                        operation,
                        "state coordination descriptor is absent",
                    )
                })?;
                let deadline = Instant::now().checked_add(*timeout).ok_or_else(|| {
                    StateStoreError::invalid_config("state coordination deadline overflow")
                })?;
                loop {
                    match file.try_lock_shared() {
                        Ok(()) => return Ok(Arc::new(LanceProtection::Directory { _file: file })),
                        Err(TryLockError::WouldBlock) => {
                            let Some(remaining) = deadline.checked_duration_since(Instant::now())
                            else {
                                return Err(StateStoreError::timed_out(
                                    operation,
                                    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                                ));
                            };
                            // This is the dedicated synchronous owner thread,
                            // before creating a native runtime or doing I/O.
                            thread::sleep(remaining.min(Duration::from_millis(5)));
                        }
                        Err(TryLockError::Error(error)) => {
                            return Err(StateStoreError::backend_operation(
                                operation,
                                format!("state shared coordination failed ({:?})", error.kind()),
                            ));
                        }
                    }
                }
            }
        }
    }

    pub(super) fn exclusive(&self) -> Result<Option<Arc<LanceProtection>>, StateStoreError> {
        match self {
            Self::Memory { snapshots } => {
                // Only the single worker admits new snapshots. Existing
                // snapshots may be dropped concurrently, never created here.
                if snapshots
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    Ok(Some(Arc::new(LanceProtection::Memory {
                        snapshots: Arc::clone(snapshots),
                    })))
                } else {
                    Ok(None)
                }
            }
            Self::Directory { .. } => {
                let file = self.open("maintenance")?.ok_or_else(|| {
                    StateStoreError::backend_operation(
                        "maintenance",
                        "state coordination descriptor is absent",
                    )
                })?;
                match file.try_lock() {
                    Ok(()) => Ok(Some(Arc::new(LanceProtection::Directory { _file: file }))),
                    Err(TryLockError::WouldBlock) => Ok(None),
                    Err(TryLockError::Error(error)) => Err(StateStoreError::backend_operation(
                        "maintenance",
                        format!("state exclusive coordination failed ({:?})", error.kind()),
                    )),
                }
            }
        }
    }
}

/// Scheduling estimates never authorize deletion: exact retention is checked
/// again under exclusive protection before any rewrite or reclamation.
pub(super) struct LanceMaintenance {
    head: Option<u64>,
    retained: usize,
    deferred_jobs: u32,
    failed_mutations: u32,
    orphan_cleanup: bool,
}

impl LanceMaintenance {
    pub(super) fn new() -> Self {
        Self {
            head: None,
            retained: 0,
            deferred_jobs: 0,
            failed_mutations: 0,
            orphan_cleanup: true,
        }
    }

    pub(super) fn observe(&mut self, version: u64) {
        if let Some(previous) = self.head {
            self.retained = self.retained.saturating_add(
                usize::try_from(version.saturating_sub(previous)).unwrap_or(usize::MAX),
            );
        }
        self.head = Some(version);
    }

    pub(super) fn retention(&mut self, version: u64, retained: usize) {
        self.head = Some(version);
        self.retained = retained;
    }

    pub(super) fn due(&self) -> bool {
        self.deferred_jobs == 0 && (self.orphan_cleanup || self.retained >= 64)
    }

    pub(super) fn defer(&mut self) {
        // The triggering public job will also report its completion; retain
        // another 32 completed jobs after that one before retrying.
        self.deferred_jobs = 33;
    }

    pub(super) fn completed_job(&mut self) {
        self.deferred_jobs = self.deferred_jobs.saturating_sub(1);
    }

    pub(super) fn failed_mutation(&mut self) {
        self.failed_mutations = self.failed_mutations.saturating_add(1);
        if self.failed_mutations >= 64 {
            self.orphan_cleanup = true;
        }
    }

    pub(super) fn settled(&mut self) {
        self.orphan_cleanup = false;
        self.failed_mutations = 0;
        self.deferred_jobs = 0;
    }
}
