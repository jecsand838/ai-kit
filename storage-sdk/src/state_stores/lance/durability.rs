//! Local durability for the native Lance object-store extension point.
//!
//! Native writes run synchronously on the StateStore's dedicated owner thread.
//! There is no suspension between creating a staging file and settling its
//! publication and cleanup. Native multipart parts remain memory-owned until
//! completion, avoiding the upstream local upload's detached cleanup tasks.

use std::{
    collections::BTreeSet,
    fmt,
    fs::{File, OpenOptions},
    future::Future,
    io::{self, Write},
    path::{Path as FilePath, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
};

use futures::{Stream, StreamExt};
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    RenameTargetMode, local::LocalFileSystem, path::Path,
};

use crate::StateStoreError;

use super::engine::STATE_TABLE;

#[derive(Debug, Default)]
struct IoLedger {
    uploads: usize,
    staging: BTreeSet<PathBuf>,
    failures: Vec<String>,
    reclamation: Reclamation,
}

#[derive(Debug, Default)]
enum Reclamation {
    #[default]
    Immediate,
    Armed,
    Pending {
        parent: PathBuf,
        files: Vec<File>,
    },
}

impl IoLedger {
    fn flush_reclamation(&mut self) -> object_store::Result<()> {
        if let Reclamation::Pending { parent, files } = &self.reclamation {
            // Keep descriptors in the ledger throughout every fallible step.
            // A directory failure must not skip another ancestor or any file.
            let mut next = Some(parent.as_path());
            while let Some(path) = next {
                if let Err(error) = DurableIo::sync_directory(path) {
                    self.failures.push(format!(
                        "reclamation directory durability failed ({:?})",
                        error.kind()
                    ));
                }
                next = path.parent();
            }
            for file in files {
                if let Err(error) = file.sync_all() {
                    self.failures.push(format!(
                        "reclamation file durability failed ({:?})",
                        error.kind()
                    ));
                }
            }
            // Even a successful reobservation cannot erase a prior failure or
            // release failed descriptors before the final job sweep.
            if self.failures.is_empty() {
                self.reclamation = Reclamation::Armed;
            }
        }
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(DurableIo::error(io::Error::other(
                "reclamation I/O has an unsettled failure",
            )))
        }
    }
}

#[derive(Debug)]
pub(super) struct DurableIo {
    root: PathBuf,
    table: Path,
    ledger: Mutex<IoLedger>,
}

enum WritePolicy {
    Preparation,
    Create,
    Overwrite,
}

impl DurableIo {
    pub(super) fn new(root: PathBuf) -> Result<Self, StateStoreError> {
        if !cfg!(any(target_os = "macos", target_os = "linux")) {
            return Err(StateStoreError::InvalidConfig(
                "Lance StateStore directory durability requires macOS or Linux".into(),
            ));
        }
        std::fs::create_dir_all(&root)
            .map_err(|error| Self::io_failure("connect", "directory creation", error))?;
        let root = root
            .canonicalize()
            .map_err(|error| Self::io_failure("connect", "directory resolution", error))?;
        Self::sync_ancestors(&root)
            .map_err(|error| Self::io_failure("connect", "directory durability", error))?;
        let table = Path::from_absolute_path(root.join(format!("{STATE_TABLE}.lance")))
            .map_err(|_| Self::backend("connect", "native table path is invalid"))?;
        Ok(Self {
            root,
            table,
            ledger: Mutex::new(IoLedger::default()),
        })
    }

    pub(super) fn root(&self) -> &FilePath {
        &self.root
    }

    pub(super) fn wrap(self: &Arc<Self>, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(DurableStore {
            original,
            io: Arc::clone(self),
        })
    }

    pub(super) fn native_file(table: &Path, path: &Path) -> bool {
        let Some(name) = path.filename() else {
            return false;
        };
        let parent = path.parent();
        if parent.as_ref() == Some(&table.clone().join("data")) {
            return name.strip_suffix(".lance").is_some_and(|stem| {
                let bytes = stem.as_bytes();
                bytes.len() == 50
                    && bytes[..24].iter().all(|byte| matches!(byte, b'0' | b'1'))
                    && bytes[24..]
                        .iter()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            });
        }
        if parent.as_ref() == Some(&table.clone().join("_deletions")) {
            return name
                .strip_suffix(".arrow")
                .or_else(|| name.strip_suffix(".bin"))
                .is_some_and(|stem| {
                    let parts = stem.split('-').collect::<Vec<_>>();
                    parts.len() == 3
                        && parts.iter().all(|part| {
                            part.parse::<u64>()
                                .is_ok_and(|value| value.to_string() == *part)
                        })
                });
        }
        if parent.as_ref() == Some(&table.clone().join("_transactions")) {
            return name
                .strip_suffix(".txn")
                .and_then(|stem| stem.split_once('-'))
                .is_some_and(|(version, identity)| {
                    version
                        .parse::<u64>()
                        .is_ok_and(|value| value.to_string() == version)
                        && uuid::Uuid::parse_str(identity)
                            .is_ok_and(|value| value.to_string() == identity)
                });
        }
        false
    }

    /// Native cleanup owns a broader filename space than this StateStore.
    /// While its exclusive reclamation lease is held, expose only the payload
    /// names our writer owns. Real manifests and branch/detached metadata stay
    /// visible; unsupported references were rejected before reclamation began.
    /// This changes candidate discovery, never a deletion acknowledgement.
    fn visible_during_reclamation(&self, path: &Path) -> bool {
        if matches!(self.lock().reclamation, Reclamation::Immediate) {
            return true;
        }
        let Some(mut parts) = path.prefix_match(&self.table) else {
            return true;
        };
        let Some(directory) = parts.next() else {
            return true;
        };
        match directory.as_ref() {
            "data" | "_deletions" | "_transactions" => Self::native_file(&self.table, path),
            // StateStore never creates indices or native temporary manifests.
            // Native empty-directory cleanup may still remove empty index
            // directories, but it preserves directories containing these files.
            "_indices" => false,
            "_versions" => !parts
                .next()
                .is_some_and(|name| name.as_ref().starts_with(".tmp")),
            _ => true,
        }
    }

    /// The caller holds exclusive StateStore protection. A missing hint is
    /// ordinary state, not an I/O failure in the publication ledger.
    pub(super) fn remove_hint(&self, table: &str) -> Result<(), StateStoreError> {
        self.remove_if_present(
            &self
                .root
                .join(table)
                .join("_versions/latest_version_hint.json"),
        )
    }

    fn remove_if_present(&self, path: &FilePath) -> Result<(), StateStoreError> {
        let retained = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(Self::backend(
                    "maintenance",
                    format!("cleanup open failed ({:?})", error.kind()),
                ));
            }
        };
        std::fs::remove_file(path).map_err(|error| {
            Self::backend(
                "maintenance",
                format!("cleanup removal failed ({:?})", error.kind()),
            )
        })?;
        let parent = path
            .parent()
            .ok_or_else(|| Self::backend("maintenance", "cleanup object has no parent"))?;
        Self::sync_ancestors(parent)
            .and_then(|()| retained.sync_all())
            .map_err(|error| {
                Self::backend(
                    "maintenance",
                    format!("cleanup durability failed ({:?})", error.kind()),
                )
            })
    }

    /// Only SDK staging names under this private table are eligible. Directory
    /// entries are inspected without following links, and all public/native
    /// owners must participate in the exclusive lease protocol.
    pub(super) fn remove_orphans(&self, table: &str) -> Result<(), StateStoreError> {
        let root = self.root.join(table);
        let metadata = std::fs::symlink_metadata(&root).map_err(|error| {
            Self::backend(
                "maintenance",
                format!("staging root inspection failed ({:?})", error.kind()),
            )
        })?;
        if !metadata.is_dir() {
            return Err(Self::backend(
                "maintenance",
                "staging root is not a directory",
            ));
        }
        let mut directories = vec![root];
        while let Some(directory) = directories.pop() {
            let entries = std::fs::read_dir(directory).map_err(|error| {
                Self::backend(
                    "maintenance",
                    format!("staging inspection failed ({:?})", error.kind()),
                )
            })?;
            for entry in entries {
                let entry = entry.map_err(|error| {
                    Self::backend(
                        "maintenance",
                        format!("staging entry failed ({:?})", error.kind()),
                    )
                })?;
                let kind = entry.file_type().map_err(|error| {
                    Self::backend(
                        "maintenance",
                        format!("staging type inspection failed ({:?})", error.kind()),
                    )
                })?;
                if kind.is_dir() {
                    directories.push(entry.path());
                } else if kind.is_file()
                    && let Some(name) = entry.file_name().to_str()
                    && let Some(suffix) = name.strip_prefix(".state-stage-")
                    && uuid::Uuid::parse_str(suffix).is_ok_and(|id| id.to_string() == suffix)
                {
                    self.remove_if_present(&entry.path())?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn begin_job(&self) -> Result<(), StateStoreError> {
        let mut ledger = self.lock();
        if self.ledger.is_poisoned()
            || ledger.uploads != 0
            || !ledger.staging.is_empty()
            || !matches!(ledger.reclamation, Reclamation::Immediate)
        {
            return Err(Self::backend(
                "begin operation",
                "the previous native I/O operation has not settled",
            ));
        }
        ledger.failures.clear();
        Ok(())
    }

    pub(super) fn begin_reclamation(&self) -> Result<(), StateStoreError> {
        self.ensure_clean("maintenance")?;
        self.lock().reclamation = Reclamation::Armed;
        Ok(())
    }

    pub(super) fn flush_reclamation(&self) -> Result<(), StateStoreError> {
        let mut ledger = self.lock();
        ledger
            .flush_reclamation()
            .map_err(|_| Self::backend("maintenance", ledger.failures.join("; ")))
    }

    pub(super) fn end_reclamation(&self) -> Result<(), StateStoreError> {
        self.flush_reclamation()?;
        self.lock().reclamation = Reclamation::Immediate;
        Ok(())
    }

    /// Called only after native children drain and their runtime is destroyed.
    /// The maintenance caller still holds exclusive protection during this
    /// finite final sweep, including when its native future returned or panicked.
    pub(super) fn finish_job(&self, operation: &str) -> Result<(), StateStoreError> {
        {
            let mut ledger = self.lock();
            // Each barrier failure is retained in the existing ledger. Always
            // close descriptors after these attempts; failed durability remains
            // an error, but does not require an indefinitely retained lease.
            let _ = ledger.flush_reclamation();
            ledger.reclamation = Reclamation::Immediate;
        }
        self.ensure_clean(operation)
    }

    pub(super) fn ensure_clean(&self, operation: &str) -> Result<(), StateStoreError> {
        // A failed cleanup remains owned. Retry after native writers have
        // drained, retaining the original failure even when cleanup recovers.
        let abandoned = {
            let ledger = self.lock();
            if ledger.uploads == 0 {
                ledger.staging.iter().cloned().collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        };
        for path in abandoned {
            if let Err(error) = self.cleanup_staging(&path) {
                self.lock().failures.push(Self::description(&error));
            }
        }
        let ledger = self.lock();
        if ledger.uploads == 0
            && ledger.staging.is_empty()
            && ledger.failures.is_empty()
            && matches!(ledger.reclamation, Reclamation::Immediate)
        {
            return Ok(());
        }
        Err(Self::backend(
            operation,
            format!(
                "native I/O did not finish cleanly ({} uploads, {} staging files, reclamation active: {}): {}",
                ledger.uploads,
                ledger.staging.len(),
                !matches!(ledger.reclamation, Reclamation::Immediate),
                ledger.failures.join("; ")
            ),
        ))
    }

    fn lock(&self) -> MutexGuard<'_, IoLedger> {
        match self.ledger.lock() {
            Ok(ledger) => ledger,
            Err(poisoned) => {
                let mut ledger = poisoned.into_inner();
                ledger
                    .failures
                    .push("native I/O ledger was poisoned".into());
                ledger
            }
        }
    }

    fn backend(operation: &str, message: impl fmt::Display) -> StateStoreError {
        StateStoreError::BackendOperation {
            operation: operation.into(),
            message: message.to_string(),
        }
    }

    fn io_failure(operation: &str, phase: &str, error: io::Error) -> StateStoreError {
        Self::backend(operation, format!("{phase} failed ({:?})", error.kind()))
    }

    fn description(error: &object_store::Error) -> String {
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(cause) = source {
            if let Some(error) = cause.downcast_ref::<io::Error>() {
                return format!("filesystem I/O failed ({:?})", error.kind());
            }
            source = cause.source();
        }
        // Native error Display values can include paths and payload details.
        // Keep their classification without copying those values into receipts.
        match error {
            object_store::Error::NotFound { .. } => "native object was not found",
            object_store::Error::AlreadyExists { .. } => "native object already exists",
            object_store::Error::NotImplemented { .. } => {
                "native filesystem operation is unsupported"
            }
            _ => "native filesystem operation failed",
        }
        .to_owned()
    }

    fn error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> object_store::Error {
        object_store::Error::Generic {
            store: "Lance StateStore durable filesystem",
            source: error.into(),
        }
    }

    fn file_error(path: &Path, error: io::Error) -> object_store::Error {
        match error.kind() {
            io::ErrorKind::NotFound => object_store::Error::NotFound {
                path: path.to_string(),
                source: Box::new(error),
            },
            _ => Self::error(error),
        }
    }

    fn create_error(path: &Path, error: io::Error) -> object_store::Error {
        if error.kind() == io::ErrorKind::AlreadyExists {
            object_store::Error::AlreadyExists {
                path: path.to_string(),
                source: Box::new(error),
            }
        } else {
            Self::file_error(path, error)
        }
    }

    fn record<T>(&self, result: object_store::Result<T>) -> object_store::Result<T> {
        if let Err(error) = &result
            && !matches!(error, object_store::Error::AlreadyExists { .. })
        {
            self.lock().failures.push(Self::description(error));
        }
        result
    }

    fn unsupported(&self, operation: &str) -> object_store::Error {
        object_store::Error::NotImplemented {
            operation: operation.into(),
            implementer: "Lance StateStore durable local filesystem".into(),
        }
    }

    fn local_path(&self, location: &Path) -> object_store::Result<PathBuf> {
        let path = LocalFileSystem::new().path_to_filesystem(location)?;
        if !path.starts_with(&self.root) {
            return Err(Self::error(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "native object path is outside the StateStore directory",
            )));
        }
        Ok(path)
    }

    fn prepare_parent(&self, path: &FilePath) -> object_store::Result<PathBuf> {
        let parent = path
            .parent()
            .ok_or_else(|| Self::error(io::Error::other("object has no parent directory")))?;
        std::fs::create_dir_all(parent).map_err(Self::error)?;
        let resolved = parent.canonicalize().map_err(Self::error)?;
        if !resolved.starts_with(&self.root) {
            return Err(Self::error(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "native object parent resolves outside the StateStore directory",
            )));
        }
        Ok(parent.to_path_buf())
    }

    fn sync_ancestors(directory: &FilePath) -> io::Result<()> {
        let mut next = Some(directory);
        while let Some(path) = next {
            Self::sync_directory(path)?;
            next = path.parent();
        }
        Ok(())
    }

    #[cfg(unix)]
    fn sync_directory(path: &FilePath) -> io::Result<()> {
        let directory = File::open(path)?;
        rustix::fs::fsync(&directory)?;
        Ok(())
    }

    #[cfg(windows)]
    fn sync_directory(_path: &FilePath) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Lance StateStore directory durability is not supported on Windows",
        ))
    }

    fn cleanup_staging(&self, path: &FilePath) -> object_store::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(Self::error(error)),
        }
        let parent = path
            .parent()
            .ok_or_else(|| Self::error(io::Error::other("staging file has no parent")))?;
        Self::sync_ancestors(parent).map_err(Self::error)?;
        self.lock().staging.remove(path);
        Ok(())
    }

    fn write(
        &self,
        location: &Path,
        payloads: &[PutPayload],
        mode: PutMode,
    ) -> object_store::Result<PutResult> {
        let policy = match mode {
            PutMode::Create => WritePolicy::Create,
            PutMode::Overwrite => WritePolicy::Overwrite,
            PutMode::Update(_) => {
                return self.record(Err(self.unsupported("put_opts with PutMode::Update")));
            }
        };
        let preparation = Self::native_file(&self.table, location);
        let policy = if preparation {
            WritePolicy::Preparation
        } else {
            policy
        };
        let result = self.write_inner(location, payloads, policy);
        // Only manifest publication uses AlreadyExists as an ordinary conflict.
        // A collided preparation was never ours and cannot authorize publication.
        if preparation && let Err(error @ object_store::Error::AlreadyExists { .. }) = &result {
            self.lock().failures.push(Self::description(error));
        }
        self.record(result)
    }

    fn write_inner(
        &self,
        location: &Path,
        payloads: &[PutPayload],
        policy: WritePolicy,
    ) -> object_store::Result<PutResult> {
        let destination = self.local_path(location)?;
        let parent = self.prepare_parent(&destination)?;
        let staging = parent.join(format!(".state-stage-{}", uuid::Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
            .map_err(Self::error)?;
        self.lock().staging.insert(staging.clone());

        // No await occurs while a staging file or publication is in flight.
        let publication = (|| {
            for payload in payloads {
                for bytes in payload.iter() {
                    file.write_all(bytes).map_err(Self::error)?;
                }
            }
            // Existing objects retain both barriers. Preparation objects are
            // create-only and remain unreferenced until the final barrier below.
            // Rust uses F_FULLFSYNC for File::sync_all on macOS.
            if matches!(policy, WritePolicy::Create | WritePolicy::Overwrite) {
                file.sync_all().map_err(Self::error)?;
            }
            match policy {
                WritePolicy::Preparation | WritePolicy::Create => {
                    std::fs::hard_link(&staging, &destination)
                        .map_err(|error| Self::create_error(location, error))?
                }
                WritePolicy::Overwrite => {
                    std::fs::rename(&staging, &destination)
                        .map_err(|error| Self::file_error(location, error))?;
                    self.lock().staging.remove(&staging);
                }
            }
            if matches!(policy, WritePolicy::Create | WritePolicy::Overwrite) {
                Self::sync_ancestors(&parent).map_err(Self::error)?;
            }
            Ok(())
        })();

        // Preparation's one directory walk follows both installation and unlink.
        let cleanup = if self.lock().staging.contains(&staging) {
            self.cleanup_staging(&staging)
        } else {
            Ok(())
        };
        let settled = self.settle(publication, cleanup, "staging cleanup");
        // fsync(directory) orders metadata into the device cache. The final
        // file sync asks macOS to flush that cache as well, after publication
        // and unlink; the retained descriptor is valid even after rename or
        // removal of its staging name.
        let barrier = file.sync_all().map_err(Self::error);
        self.settle(settled, barrier, "final filesystem barrier")?;
        Ok(PutResult {
            e_tag: None,
            version: None,
            extensions: Default::default(),
        })
    }

    fn settle(
        &self,
        primary: object_store::Result<()>,
        supplementary: object_store::Result<()>,
        phase: &str,
    ) -> object_store::Result<()> {
        match (primary, supplementary) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(primary), Err(supplementary)) => {
                // In particular, keep AlreadyExists for native conflict
                // classification. Later failures remain in the job ledger.
                self.lock().failures.push(format!(
                    "{phase} also failed: {}",
                    Self::description(&supplementary)
                ));
                Err(primary)
            }
        }
    }

    fn copy(&self, from: &Path, to: &Path, mode: CopyMode) -> object_store::Result<()> {
        let result = (|| {
            let source = self.local_path(from)?;
            let bytes = std::fs::read(source).map_err(|error| Self::file_error(from, error))?;
            let policy = match mode {
                CopyMode::Create => WritePolicy::Create,
                CopyMode::Overwrite => WritePolicy::Overwrite,
            };
            self.write_inner(to, &[bytes.into()], policy)?;
            Ok(())
        })();
        self.record(result)
    }

    fn rename(&self, from: &Path, to: &Path, mode: RenameTargetMode) -> object_store::Result<()> {
        let result = (|| {
            let source = self.local_path(from)?;
            let destination = self.local_path(to)?;
            let parent = self.prepare_parent(&destination)?;
            let file = File::open(&source).map_err(|error| Self::file_error(from, error))?;
            file.sync_all()
                .map_err(|error| Self::file_error(from, error))?;
            match mode {
                RenameTargetMode::Overwrite => std::fs::rename(&source, &destination)
                    .map_err(|error| Self::file_error(to, error))?,
                RenameTargetMode::Create => {
                    std::fs::hard_link(&source, &destination)
                        .map_err(|error| Self::create_error(to, error))?;
                    std::fs::remove_file(&source).map_err(|error| Self::file_error(from, error))?;
                }
            }
            Self::sync_ancestors(&parent).map_err(Self::error)?;
            let source_parent = source
                .parent()
                .ok_or_else(|| Self::error(io::Error::other("source has no parent directory")))?;
            Self::sync_ancestors(source_parent).map_err(Self::error)?;
            file.sync_all().map_err(Self::error)?;
            Ok(())
        })();
        self.record(result)
    }

    fn delete(&self, location: &Path) -> object_store::Result<()> {
        let result = (|| {
            let path = self.local_path(location)?;
            let parent = path
                .parent()
                .ok_or_else(|| Self::error(io::Error::other("object has no parent directory")))?;
            {
                let mut ledger = self.lock();
                if !matches!(ledger.reclamation, Reclamation::Immediate) {
                    if !ledger.failures.is_empty() {
                        return Err(Self::error(io::Error::other(
                            "reclamation stopped after an earlier I/O failure",
                        )));
                    }
                    let eligible = Self::native_file(&self.table, location);
                    if !eligible
                        || matches!(&ledger.reclamation, Reclamation::Pending { parent: pending, .. } if pending != parent)
                    {
                        // In particular, payload barriers precede old-manifest
                        // deletion in the pinned native cleanup stream.
                        ledger.flush_reclamation()?;
                    }
                    if eligible {
                        let file =
                            File::open(&path).map_err(|error| Self::file_error(location, error))?;
                        if matches!(ledger.reclamation, Reclamation::Armed) {
                            ledger.reclamation = Reclamation::Pending {
                                parent: parent.to_path_buf(),
                                files: Vec::with_capacity(64),
                            };
                        }
                        if let Reclamation::Pending { files, .. } = &mut ledger.reclamation {
                            // Adopt before unlink so an error or native unwind
                            // cannot discard the descriptor awaiting settlement.
                            files.push(file);
                            std::fs::remove_file(&path)
                                .map_err(|error| Self::file_error(location, error))?;
                            if files.len() == 64 {
                                ledger.flush_reclamation()?;
                            }
                            return Ok(());
                        }
                        return Err(Self::error(io::Error::other(
                            "reclamation deletion was not armed",
                        )));
                    }
                }
            }
            let file = File::open(&path).map_err(|error| Self::file_error(location, error))?;
            std::fs::remove_file(&path).map_err(|error| Self::file_error(location, error))?;
            Self::sync_ancestors(parent).map_err(Self::error)?;
            file.sync_all().map_err(Self::error)
        })();
        self.record(result)
    }
}

#[derive(Debug)]
struct DurableStore {
    original: Arc<dyn ObjectStore>,
    io: Arc<DurableIo>,
}

impl fmt::Display for DurableStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Lance StateStore durable local filesystem")
    }
}

impl ObjectStore for DurableStore {
    fn put_opts<'store, 'path, 'future>(
        &'store self,
        location: &'path Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'future>>
    where
        'store: 'future,
        'path: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            if !options.attributes.is_empty() {
                return self.io.record(Err(self.io.unsupported("put attributes")));
            }
            self.io.write(location, &[payload], options.mode)
        })
    }

    fn put_multipart_opts<'store, 'path, 'future>(
        &'store self,
        location: &'path Path,
        options: PutMultipartOptions,
    ) -> Pin<
        Box<dyn Future<Output = object_store::Result<Box<dyn MultipartUpload>>> + Send + 'future>,
    >
    where
        'store: 'future,
        'path: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            if !options.attributes.is_empty() {
                return self
                    .io
                    .record(Err(self.io.unsupported("multipart attributes")));
            }
            self.io.lock().uploads += 1;
            Ok(Box::new(BufferedUpload {
                io: Arc::clone(&self.io),
                location: location.clone(),
                state: UploadState::Active(Vec::new()),
            }) as Box<dyn MultipartUpload>)
        })
    }

    fn get_opts<'store, 'path, 'future>(
        &'store self,
        location: &'path Path,
        options: GetOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<GetResult>> + Send + 'future>>
    where
        'store: 'future,
        'path: 'future,
        Self: 'future,
    {
        self.original.get_opts(location, options)
    }

    fn delete_stream(
        &self,
        locations: Pin<Box<dyn Stream<Item = object_store::Result<Path>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = object_store::Result<Path>> + Send>> {
        let io = Arc::clone(&self.io);
        Box::pin(locations.map(move |location| {
            let location = location?;
            io.delete(&location)?;
            Ok(location)
        }))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Pin<Box<dyn Stream<Item = object_store::Result<ObjectMeta>> + Send>> {
        let io = Arc::clone(&self.io);
        Box::pin(self.original.list(prefix).filter(move |entry| {
            std::future::ready(match entry {
                Ok(object) => io.visible_during_reclamation(&object.location),
                Err(_) => true,
            })
        }))
    }

    fn list_with_delimiter<'store, 'path, 'future>(
        &'store self,
        prefix: Option<&'path Path>,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<ListResult>> + Send + 'future>>
    where
        'store: 'future,
        'path: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let mut result = self.original.list_with_delimiter(prefix).await?;
            result
                .objects
                .retain(|object| self.io.visible_during_reclamation(&object.location));
            // Preserve common prefixes: reference discovery remains native-owned.
            Ok(result)
        })
    }

    fn copy_opts<'store, 'from, 'to, 'future>(
        &'store self,
        from: &'from Path,
        to: &'to Path,
        options: CopyOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'future>>
    where
        'store: 'future,
        'from: 'future,
        'to: 'future,
        Self: 'future,
    {
        Box::pin(async move { self.io.copy(from, to, options.mode) })
    }

    fn rename_opts<'store, 'from, 'to, 'future>(
        &'store self,
        from: &'from Path,
        to: &'to Path,
        options: RenameOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'future>>
    where
        'store: 'future,
        'from: 'future,
        'to: 'future,
        Self: 'future,
    {
        Box::pin(async move { self.io.rename(from, to, options.target_mode) })
    }
}

#[derive(Debug)]
enum UploadState {
    Active(Vec<PutPayload>),
    Completed,
    Aborted,
}

#[derive(Debug)]
struct BufferedUpload {
    io: Arc<DurableIo>,
    location: Path,
    state: UploadState,
}

impl MultipartUpload for BufferedUpload {
    fn put_part(
        &mut self,
        payload: PutPayload,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send>> {
        let result = match &mut self.state {
            UploadState::Active(parts) => {
                parts.push(payload);
                Ok(())
            }
            UploadState::Completed | UploadState::Aborted => self.io.record(Err(DurableIo::error(
                io::Error::other("multipart upload is already terminal"),
            ))),
        };
        Box::pin(std::future::ready(result))
    }

    fn complete<'upload, 'future>(
        &'upload mut self,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'future>>
    where
        'upload: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            match std::mem::replace(&mut self.state, UploadState::Aborted) {
                UploadState::Active(parts) => {
                    let result = self.io.write(&self.location, &parts, PutMode::Overwrite)?;
                    self.state = UploadState::Completed;
                    Ok(result)
                }
                UploadState::Completed | UploadState::Aborted => self.io.record(Err(
                    DurableIo::error(io::Error::other("multipart upload is already terminal")),
                )),
            }
        })
    }

    fn abort<'upload, 'future>(
        &'upload mut self,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'future>>
    where
        'upload: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            match std::mem::replace(&mut self.state, UploadState::Aborted) {
                UploadState::Active(_) => Ok(()),
                UploadState::Completed | UploadState::Aborted => self.io.record(Err(
                    DurableIo::error(io::Error::other("multipart upload is already terminal")),
                )),
            }
        })
    }
}

impl Drop for BufferedUpload {
    fn drop(&mut self) {
        self.io.lock().uploads -= 1;
    }
}
