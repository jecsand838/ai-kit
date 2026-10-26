use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions, TryLockError},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::RecordBatchIterator;
use futures::TryStreamExt;
use lance::{
    Dataset,
    dataset::{
        CommitBuilder, InsertBuilder, MergeInsertBuilder, ReadParams, WhenMatched, WhenNotMatched,
        WhenNotMatchedBySource, WriteMode, WriteParams, builder::DatasetBuilder,
        cleanup::CleanupPolicy, transaction::Transaction,
    },
    io::{ObjectStore, ObjectStoreParams, WrappingObjectStore},
};
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction as ManifestTransaction},
    io::{
        commit::{
            CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation,
            ManifestNamingScheme, ManifestWriter,
        },
        deletion::deletion_file_path,
    },
};
use object_store::{
    ObjectStore as NativeObjectStore, ObjectStoreExt, memory::InMemory, path::Path,
};

use crate::{
    StateKey, StateStoreError,
    state::config::{LanceStateStoreConfig, LanceStateStoreLocation},
};

use super::{
    coordination::{LanceCoordination, LanceMaintenance, LanceProtection},
    durability::DurableIo,
    records::{EXPECTED_VERSION, LanceBatches, LanceImage, LanceSnapshot, StoredRecord},
};

pub(crate) const STATE_TABLE: &str = "__ai_kit_state";

/// One connected store owns its memory objects or its local durability ledger.
#[derive(Debug)]
enum LanceObjects {
    Memory(Arc<InMemory>),
    Directory(Arc<DurableIo>),
}

impl LanceObjects {
    fn begin_job(&self) -> Result<(), StateStoreError> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Directory(io) => io.begin_job(),
        }
    }

    fn ensure_clean(&self, operation: &str) -> Result<(), StateStoreError> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Directory(io) => io.ensure_clean(operation),
        }
    }
}

impl WrappingObjectStore for LanceObjects {
    fn wrap(
        &self,
        _prefix: &str,
        original: Arc<dyn NativeObjectStore>,
    ) -> Arc<dyn NativeObjectStore> {
        match self {
            Self::Memory(memory) => Arc::clone(memory) as Arc<dyn NativeObjectStore>,
            Self::Directory(io) => io.wrap(original),
        }
    }
    fn wrap_paginated(
        &self,
        _prefix: &str,
        _original: Arc<dyn object_store::list::PaginatedListStore>,
    ) -> Option<Arc<dyn object_store::list::PaginatedListStore>> {
        // Listing must use the same replacement/observed store as ordinary I/O.
        None
    }
}

#[derive(Debug)]
struct LanceCommit {
    objects: Arc<LanceObjects>,
}

impl CommitHandler for LanceCommit {
    fn is_version_not_found_definitive(&self) -> bool {
        // This handler resolves only strongly consistent local/memory stores,
        // with no external or eventually consistent manifest authority.
        true
    }

    fn propagate_commit_error_after_success(&self) -> bool {
        // A landed manifest does not erase a failed durability barrier.
        true
    }

    fn commit<'owner, 'manifest, 'path, 'store, 'future>(
        &'owner self,
        manifest: &'manifest mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &'path Path,
        object_store: &'store ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<ManifestTransaction>,
    ) -> Pin<Box<dyn Future<Output = Result<ManifestLocation, CommitError>> + Send + 'future>>
    where
        'owner: 'future,
        'manifest: 'future,
        'path: 'future,
        'store: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            self.objects.ensure_clean("commit").map_err(|error| {
                CommitError::OtherError(lance::Error::external(Box::new(error)))
            })?;
            let expected = transaction
                .as_ref()
                .and_then(|transaction| {
                    transaction
                        .inner
                        .transaction_properties
                        .get(EXPECTED_VERSION)
                })
                .and_then(|version| version.parse::<u64>().ok())
                .ok_or_else(|| {
                    CommitError::OtherError(lance::Error::invalid_input(
                        "state commit has no original target version",
                    ))
                })?;
            if manifest.version != expected {
                return Err(CommitError::OtherError(
                    lance::Error::retryable_commit_conflict_source(
                        expected,
                        "state commit cannot rebase its original snapshot".into(),
                    ),
                ));
            }
            let location = ConditionalPutCommitHandler
                .commit(
                    manifest,
                    indices,
                    base_path,
                    object_store,
                    manifest_writer,
                    naming_scheme,
                    transaction,
                )
                .await?;
            // A native best-effort hint write must not hide a failed barrier.
            self.objects.ensure_clean("commit").map_err(|error| {
                CommitError::OtherError(lance::Error::external(Box::new(error)))
            })?;
            Ok(location)
        })
    }
}

pub(super) struct LanceEngine {
    uri: String,
    objects: Arc<LanceObjects>,
    handler: Arc<LanceCommit>,
    initialization_lock: Mutex<Option<File>>,
    operation_timeout: Duration,
    coordination: LanceCoordination,
    maintenance: Mutex<LanceMaintenance>,
    image: Mutex<Option<Arc<LanceImage>>>,
    mutation_started: AtomicBool,
}

impl LanceEngine {
    pub(super) fn new(config: LanceStateStoreConfig) -> Result<Self, StateStoreError> {
        config.validate()?;
        let (uri, objects, coordination) = match config.location() {
            LanceStateStoreLocation::Memory => (
                format!("memory:///{STATE_TABLE}.lance"),
                LanceObjects::Memory(Arc::new(InMemory::new())),
                LanceCoordination::memory(),
            ),
            LanceStateStoreLocation::Directory(directory) => {
                let io = Arc::new(DurableIo::new(directory.clone())?);
                let path = io.root().join(format!("{STATE_TABLE}.lance"));
                let uri = url::Url::from_file_path(path)
                    .map_err(|_| StateStoreError::invalid_config("state directory URL is invalid"))?
                    .to_string()
                    .replacen("file:", "file-object-store:", 1);
                let coordination = LanceCoordination::directory(
                    io.root().join(format!("{STATE_TABLE}.maintenance.lock")),
                    config.operation_timeout(),
                );
                (uri, LanceObjects::Directory(io), coordination)
            }
        };
        let objects = Arc::new(objects);
        Ok(Self {
            uri,
            handler: Arc::new(LanceCommit {
                objects: Arc::clone(&objects),
            }),
            objects,
            initialization_lock: Mutex::new(None),
            operation_timeout: config.operation_timeout(),
            coordination,
            maintenance: Mutex::new(LanceMaintenance::new()),
            image: Mutex::new(None),
            mutation_started: AtomicBool::new(false),
        })
    }

    pub(super) async fn initialize(
        &self,
        protection: Arc<LanceProtection>,
    ) -> Result<(), StateStoreError> {
        self.lock_initialization().await?;
        match self.open().await {
            Ok(dataset) => {
                self.retention(&dataset).await?;
                self.read_image(dataset, Arc::clone(&protection)).await?;
            }
            Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. }) => {
                self.require_fresh_table()?;
                self.create_table(protection).await?;
            }
            Err(_) => return Err(Self::failure("connect", "state table could not be opened")),
        }
        Ok(())
    }

    fn params(&self) -> ObjectStoreParams {
        ObjectStoreParams {
            object_store_wrapper: Some(Arc::clone(&self.objects) as Arc<dyn WrappingObjectStore>),
            ..ObjectStoreParams::default()
        }
    }

    fn builder(&self) -> DatasetBuilder {
        DatasetBuilder::from_uri(&self.uri).with_read_params(ReadParams {
            store_options: Some(self.params()),
            commit_handler: Some(Arc::clone(&self.handler) as Arc<dyn CommitHandler>),
            ..ReadParams::default()
        })
    }

    async fn open(&self) -> Result<Arc<Dataset>, lance::Error> {
        let dataset = self.builder().load().await?;
        LanceSnapshot::validate_storage(&dataset)
            .map_err(|error| lance::Error::external(Box::new(error)))?;
        Ok(Arc::new(dataset))
    }

    pub(super) fn begin_job(&self) -> Result<(), StateStoreError> {
        self.mutation_started.store(false, Ordering::Relaxed);
        self.objects.begin_job()
    }

    pub(super) fn finish_job(&self, operation: &str) -> Result<(), StateStoreError> {
        let cleanup = match self.objects.as_ref() {
            LanceObjects::Memory(_) => Ok(()),
            LanceObjects::Directory(io) => io.finish_job(operation),
        };
        // Initialization keeps this file owned until the worker has drained its
        // runtime and checked native cleanup. The commit guard never calls here.
        let release = self
            .initialization_lock
            .lock()
            .map(|mut lock| drop(lock.take()))
            .map_err(|_| Self::failure(operation, "state initialization lock was poisoned"));
        cleanup.and(release)
    }

    async fn lock_initialization(&self) -> Result<(), StateStoreError> {
        let LanceObjects::Directory(io) = self.objects.as_ref() else {
            return Ok(());
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(io.root().join(format!("{STATE_TABLE}.bootstrap.lock")))
            .map_err(|_| {
                Self::failure("connect", "state initialization lock could not be opened")
            })?;
        let deadline = Instant::now()
            .checked_add(self.operation_timeout)
            .ok_or_else(|| {
                StateStoreError::invalid_config("state initialization deadline overflow")
            })?;
        loop {
            match file.try_lock() {
                Ok(()) => {
                    let mut lock = self.initialization_lock.lock().map_err(|_| {
                        Self::failure("connect", "state initialization lock was poisoned")
                    })?;
                    *lock = Some(file);
                    return Ok(());
                }
                Err(TryLockError::WouldBlock) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(StateStoreError::timed_out(
                            "connect",
                            u64::try_from(self.operation_timeout.as_millis()).unwrap_or(u64::MAX),
                        ));
                    };
                    tokio::time::sleep(remaining.min(Duration::from_millis(5))).await;
                }
                Err(TryLockError::Error(_)) => {
                    return Err(Self::failure("connect", "state initialization lock failed"));
                }
            }
        }
    }

    fn require_fresh_table(&self) -> Result<(), StateStoreError> {
        let LanceObjects::Directory(io) = self.objects.as_ref() else {
            return Ok(());
        };
        let path = io.root().join(format!("{STATE_TABLE}.lance"));
        match std::fs::read_dir(path) {
            Ok(mut entries) => match entries.next() {
                None => Ok(()),
                Some(Ok(_)) => Err(Self::failure(
                    "connect",
                    "state manifest is missing from an existing table; automatic reinitialization is refused",
                )),
                Some(Err(_)) => Err(Self::failure(
                    "connect",
                    "state table could not be inspected",
                )),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(Self::failure(
                "connect",
                "state table could not be inspected",
            )),
        }
    }

    pub(super) async fn snapshot(
        &self,
        protection: Arc<LanceProtection>,
    ) -> Result<LanceSnapshot, StateStoreError> {
        let dataset = self
            .open()
            .await
            .map_err(|_| Self::failure("read", "state table could not be opened"))?;
        self.read_image(dataset, protection).await
    }

    async fn read_image(
        &self,
        dataset: Arc<Dataset>,
        protection: Arc<LanceProtection>,
    ) -> Result<LanceSnapshot, StateStoreError> {
        let identity = LanceSnapshot::identity(&dataset).await?;
        self.scheduling()?.observe(dataset.version_id());
        let cached = self
            .image
            .lock()
            .map_err(|_| Self::failure("read", "state image cache was poisoned"))?
            .as_ref()
            .filter(|image| image.matches_identity(&dataset, &identity))
            .cloned();
        if let Some(image) = cached {
            return Ok(LanceSnapshot::from_image(dataset, image, protection));
        }
        let snapshot = LanceSnapshot::read(dataset, protection, None).await?;
        self.remember(snapshot.image())?;
        Ok(snapshot)
    }

    async fn create_table(&self, protection: Arc<LanceProtection>) -> Result<(), StateStoreError> {
        let uuid = uuid::Uuid::new_v4().to_string();
        let params = WriteParams {
            mode: WriteMode::Create,
            // Pin the supported native encoder to this owner's runtime. Delta
            // writes inherit this format from their original Dataset.
            data_storage_version: Some("2.0".parse().map_err(|_| {
                Self::failure("connect", "required state storage format is unavailable")
            })?),
            enable_stable_row_ids: false,
            store_params: Some(self.params()),
            commit_handler: Some(Arc::clone(&self.handler) as Arc<dyn CommitHandler>),
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        let batches = vec![LanceSnapshot::initial_batch(&uuid)?];
        let prepared = InsertBuilder::new(self.uri.as_str())
            .with_params(&params)
            .execute_uncommitted(batches)
            .await;
        let mut transaction = match prepared {
            Ok(transaction) => transaction,
            Err(error) => return self.initialization_error(error, &uuid, protection).await,
        };
        Self::identify(&mut transaction, uuid.clone(), 1);
        match CommitBuilder::new(self.uri.as_str())
            .use_stable_row_ids(false)
            .with_store_params(self.params())
            .with_commit_handler(Arc::clone(&self.handler) as Arc<dyn CommitHandler>)
            .with_max_retries(0)
            // The admitted owner retains finalization; native timeout must not
            // drop publication work after the manifest has landed.
            .with_timeout(None)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
        {
            Ok(dataset) => {
                if dataset.version_id() != 1 {
                    return Err(Self::failure("connect", "initial state version is invalid"));
                }
                self.retention(&dataset).await?;
                let snapshot = LanceSnapshot::read(Arc::new(dataset), protection, None).await?;
                self.remember(snapshot.image())?;
                Ok(())
            }
            Err(error) => self.initialization_error(error, &uuid, protection).await,
        }
    }

    async fn initialization_error(
        &self,
        error: lance::Error,
        uuid: &str,
        protection: Arc<LanceProtection>,
    ) -> Result<(), StateStoreError> {
        match error {
            lance::Error::DatasetAlreadyExists { .. }
            | lance::Error::CommitConflict { .. }
            | lance::Error::DatasetNotFound { .. }
            | lance::Error::NotFound { .. }
                // Only a proven different creator can turn this initialization
                // race into an open. An own UUID after an error stays an error.
                if self
                    .canonical_uuid(1)
                    .await
                    .is_some_and(|canonical| canonical != uuid) =>
            {
                self.snapshot(protection).await?;
                return Ok(());
            }
            _ => {}
        }
        Err(Self::failure(
            "connect",
            "state initialization failed; publication may have occurred",
        ))
    }

    pub(super) async fn commit(
        &self,
        snapshot: &LanceSnapshot,
        overlay: HashMap<StateKey, StoredRecord>,
    ) -> Result<Option<Arc<LanceImage>>, StateStoreError> {
        if overlay.is_empty() {
            return Ok(None);
        }
        let head = self.open().await.map_err(|_| {
            Self::failure(
                "commit",
                "state head could not be resolved before preparation",
            )
        })?;
        self.scheduling()?.observe(head.version_id());
        if head.version_id() > snapshot.dataset().version_id() {
            return Err(StateStoreError::transaction_conflict(
                "commit",
                "state snapshot changed before preparation",
            ));
        }
        let identity = LanceSnapshot::identity(&head).await?;
        if !snapshot.matches_identity(&head, &identity) {
            return Err(Self::failure(
                "commit",
                "state head identity regressed or changed within one version",
            ));
        }
        let uuid = uuid::Uuid::new_v4().to_string();
        // Clones share iterative JSON owners; keep the authored records until
        // exact publication can derive an image from the validated snapshot.
        let version = self
            .publish(
                snapshot,
                LanceBatches::delta(overlay.clone(), uuid.clone()),
                uuid.clone(),
            )
            .await?;
        Ok(Some(snapshot.image().committed(overlay, version, uuid)))
    }

    async fn publish(
        &self,
        snapshot: &LanceSnapshot,
        batches: LanceBatches,
        uuid: String,
    ) -> Result<u64, StateStoreError> {
        let dataset = Arc::clone(snapshot.dataset());
        let target = dataset
            .version_id()
            .checked_add(1)
            .ok_or_else(|| StateStoreError::invalid_request("state commit version overflow"))?;
        let receipt = batches.failure_receipt();
        let source = RecordBatchIterator::new(batches, LanceSnapshot::schema());
        let mut builder = MergeInsertBuilder::try_new(
            Arc::clone(&dataset),
            vec!["kind".to_owned(), "namespace".to_owned(), "key".to_owned()],
        )
        .map_err(|_| Self::failure("commit", "state merge could not be prepared"))?;
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
            .use_index(false);
        let job = builder
            .try_build()
            .map_err(|_| Self::failure("commit", "state merge could not be prepared"))?;
        self.mutation_started.store(true, Ordering::Relaxed);
        let prepared = job.execute_uncommitted(source).await;
        if let Some(error) = receipt
            .lock()
            .map_err(|_| Self::failure("commit", "state encoding receipt was poisoned"))?
            .take()
        {
            return Err(error);
        }
        let mut delta = prepared.map_err(Self::preparation_error)?;
        Self::identify(&mut delta.transaction, uuid.clone(), target);
        let mut commit = CommitBuilder::new(dataset)
            .with_max_retries(0)
            // Preserve the same admitted-finalization authority as initialization.
            .with_timeout(None)
            .with_skip_auto_cleanup(true);
        if let Some(affected_rows) = delta.affected_rows {
            commit = commit.with_affected_rows(affected_rows);
        }
        match commit.execute(delta.transaction).await {
            Ok(committed) if committed.version_id() == target => {
                self.scheduling()?.observe(target);
                Ok(target)
            }
            Ok(_) => Err(Self::failure(
                "commit",
                "state commit returned an unexpected version",
            )),
            Err(error) => Err(self.commit_error(error, target, &uuid).await),
        }
    }

    fn preparation_error(error: lance::Error) -> StateStoreError {
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(cause) = source {
            if let Some(error) = cause.downcast_ref::<StateStoreError>() {
                return error.clone();
            }
            source = cause.source();
        }
        Self::failure("commit", "state data could not be prepared")
    }

    fn scheduling(&self) -> Result<MutexGuard<'_, LanceMaintenance>, StateStoreError> {
        self.maintenance
            .lock()
            .map_err(|_| Self::failure("maintenance", "state scheduling owner was poisoned"))
    }

    pub(super) fn remember(&self, image: &Arc<LanceImage>) -> Result<(), StateStoreError> {
        *self
            .image
            .lock()
            .map_err(|_| Self::failure("read", "state image cache was poisoned"))? =
            Some(Arc::clone(image));
        Ok(())
    }

    pub(super) fn invalidate_image(&self) {
        match self.image.lock() {
            Ok(mut image) => {
                image.take();
            }
            Err(poisoned) => {
                poisoned.into_inner().take();
            }
        }
    }

    async fn retention(&self, dataset: &Dataset) -> Result<usize, StateStoreError> {
        let retained = dataset
            .versions()
            .await
            .map_err(|_| {
                Self::failure("maintenance", "retained state versions could not be listed")
            })?
            .len();
        self.scheduling()?.retention(dataset.version_id(), retained);
        Ok(retained)
    }

    pub(super) fn shared(&self, operation: &str) -> Result<Arc<LanceProtection>, StateStoreError> {
        self.coordination.shared(operation)
    }

    pub(super) fn maintenance_protection(
        &self,
    ) -> Result<Option<Arc<LanceProtection>>, StateStoreError> {
        if !self.scheduling()?.due() {
            return Ok(None);
        }
        let protection = self.coordination.exclusive()?;
        if protection.is_none() {
            self.scheduling()?.defer();
        }
        Ok(protection)
    }

    pub(super) fn completed_job(&self, failed_commit: bool) {
        if let Ok(mut maintenance) = self.maintenance.lock() {
            maintenance.completed_job();
            if failed_commit && self.mutation_started.load(Ordering::Relaxed) {
                maintenance.failed_mutation();
            }
        }
    }

    pub(super) fn settled_maintenance(&self, success: bool) {
        if let Ok(mut maintenance) = self.maintenance.lock() {
            if success {
                maintenance.settled();
            } else {
                maintenance.defer();
            }
        }
    }

    async fn remove_hint(&self) -> Result<(), StateStoreError> {
        match self.objects.as_ref() {
            LanceObjects::Directory(io) => io.remove_hint(&format!("{STATE_TABLE}.lance")),
            LanceObjects::Memory(memory) => {
                let path = Path::from(format!(
                    "{STATE_TABLE}.lance/_versions/latest_version_hint.json"
                ));
                match memory.delete(&path).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(_) => Err(Self::failure(
                        "maintenance",
                        "state version hint could not be removed",
                    )),
                }
            }
        }
    }

    pub(super) async fn maintenance_snapshot(
        &self,
        protection: Arc<LanceProtection>,
    ) -> Result<Option<(LanceSnapshot, bool)>, StateStoreError> {
        let previous = self
            .image
            .lock()
            .map_err(|_| Self::failure("maintenance", "state image cache was poisoned"))?
            .take();
        self.remove_hint().await?;
        let dataset = match self.open().await {
            Ok(dataset) => dataset,
            Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. }) => {
                self.require_fresh_table()?;
                return Ok(None);
            }
            Err(_) => {
                return Err(Self::failure(
                    "maintenance",
                    "state table could not be opened",
                ));
            }
        };
        let retained = self.retention(&dataset).await?;
        // Inspect every native row before reclamation. An earlier image can
        // only reuse payloads whose persisted bytes still match exactly.
        let snapshot = LanceSnapshot::read(dataset, protection, previous.as_deref()).await?;
        Ok(Some((snapshot, retained >= 64)))
    }

    pub(super) async fn maintenance_rewrite(
        &self,
        snapshot: &LanceSnapshot,
    ) -> Result<(), StateStoreError> {
        let uuid = uuid::Uuid::new_v4().to_string();
        self.publish(
            snapshot,
            LanceBatches::image(Arc::clone(snapshot.image()), uuid.clone()),
            uuid,
        )
        .await
        .map(|_| ())
    }

    pub(super) async fn maintenance_cleanup(
        &self,
        full: bool,
        protection: Arc<LanceProtection>,
        previous: &LanceImage,
    ) -> Result<(), StateStoreError> {
        // No retained hint may refer into a manifest history with gaps, even
        // when process death interrupts the following native deletions.
        self.remove_hint().await?;
        let dataset = self.open().await.map_err(|_| {
            Self::failure(
                "maintenance",
                "state table could not be reopened before cleanup",
            )
        })?;
        // Native 13 scans all data when deleting unverified files, but still
        // filters transaction/deletion listings by retained-manifest timestamps.
        // Validate references before history deletion, then supplement only
        // those two scans without assuming filesystem and manifest clocks agree.
        let mut retained_files = self.retained_files(&dataset).await?;
        if let LanceObjects::Directory(io) = self.objects.as_ref() {
            io.begin_reclamation()?;
        }
        dataset
            .cleanup_with_policy(CleanupPolicy {
                before_version: Some(if full { dataset.version_id() } else { 0 }),
                delete_unverified: true,
                ..CleanupPolicy::default()
            })
            .await
            .map_err(|_| Self::failure("maintenance", "native state reclamation failed"))?;
        if let LanceObjects::Directory(io) = self.objects.as_ref() {
            io.flush_reclamation()?;
        }
        if full {
            // Recompute from the manifests actually retained by native GC;
            // references from removed versions must not pin obsolete files.
            retained_files = self.retained_files(&dataset).await?;
        }
        self.remove_recent_orphans(&dataset, &retained_files)
            .await?;
        if let LanceObjects::Directory(io) = self.objects.as_ref() {
            io.end_reclamation()?;
            io.remove_orphans(&format!("{STATE_TABLE}.lance"))?;
        }
        self.objects.ensure_clean("maintenance")?;
        let dataset = self.open().await.map_err(|_| {
            Self::failure(
                "maintenance",
                "state table could not be reopened after cleanup",
            )
        })?;
        self.retention(&dataset).await?;
        let snapshot = LanceSnapshot::read(dataset, protection, Some(previous)).await?;
        self.remember(snapshot.image())?;
        Ok(())
    }

    async fn retained_files(&self, dataset: &Dataset) -> Result<HashSet<Path>, StateStoreError> {
        let branches = dataset.branches().list().await.map_err(|_| {
            Self::failure(
                "maintenance",
                "state branch references could not be inspected",
            )
        })?;
        let detached = dataset.list_detached_manifests().await.map_err(|_| {
            Self::failure(
                "maintenance",
                "state detached references could not be inspected",
            )
        })?;
        if !branches.is_empty() || !detached.is_empty() {
            return Err(Self::failure(
                "maintenance",
                "state history has unsupported references",
            ));
        }
        let versions = dataset.versions().await.map_err(|_| {
            Self::failure(
                "maintenance",
                "retained state history could not be inspected",
            )
        })?;
        if versions.last().map(|version| version.version) != Some(dataset.version_id())
            || versions
                .windows(2)
                .any(|pair| pair[0].version >= pair[1].version)
        {
            return Err(Self::failure(
                "maintenance",
                "retained state history is inconsistent",
            ));
        }
        let mut retained = HashSet::new();
        for version in versions {
            let historical = dataset
                .checkout_version(version.version)
                .await
                .map_err(|_| {
                    Self::failure("maintenance", "retained state version could not be opened")
                })?;
            if historical.version_id() != version.version {
                return Err(Self::failure(
                    "maintenance",
                    "retained state version is inconsistent",
                ));
            }
            LanceSnapshot::identity(&historical).await?;
            let manifest = historical.manifest();
            if manifest.branch.is_some()
                || !manifest.base_paths.is_empty()
                || manifest.index_section.is_some()
            {
                return Err(Self::failure(
                    "maintenance",
                    "state history has unsupported references",
                ));
            }
            let base = historical
                .data_dir()
                .parent()
                .ok_or_else(|| Self::failure("maintenance", "state history has no native root"))?;
            for fragment in manifest.fragments.iter() {
                if !fragment.overlays.is_empty()
                    || fragment.row_id_meta.is_some()
                    || fragment.last_updated_at_version_meta.is_some()
                    || fragment.created_at_version_meta.is_some()
                {
                    return Err(Self::failure(
                        "maintenance",
                        "state fragment has unsupported references",
                    ));
                }
                for file in &fragment.files {
                    let relative = Path::parse(&file.path).map_err(|_| {
                        Self::failure("maintenance", "state data reference is invalid")
                    })?;
                    if file.base_id.is_some()
                        || relative.parts_count() != 1
                        || file.file_major_version != 2
                        || file.file_minor_version != 0
                    {
                        return Err(Self::failure(
                            "maintenance",
                            "state data reference is unsupported",
                        ));
                    }
                }
                if let Some(deletion) = &fragment.deletion_file {
                    if deletion.base_id.is_some() {
                        return Err(Self::failure(
                            "maintenance",
                            "state deletion reference is unsupported",
                        ));
                    }
                    retained.insert(deletion_file_path(&base, fragment.id, deletion));
                }
            }
            if let Some(transaction) = &manifest.transaction_file {
                let relative = Path::parse(transaction).map_err(|_| {
                    Self::failure("maintenance", "state transaction reference is invalid")
                })?;
                if relative.parts_count() != 1 {
                    return Err(Self::failure(
                        "maintenance",
                        "state transaction reference is unsupported",
                    ));
                }
                retained.insert(historical.transactions_dir().join(transaction.as_str()));
            }
        }
        Ok(retained)
    }

    async fn remove_recent_orphans(
        &self,
        dataset: &Dataset,
        retained: &HashSet<Path>,
    ) -> Result<(), StateStoreError> {
        let store = dataset
            .object_store(None)
            .await
            .map_err(|_| Self::failure("maintenance", "state object owner could not be opened"))?;
        let mut orphans = Vec::new();
        for directory in [dataset.deletions_dir(), dataset.transactions_dir()] {
            // List after native cleanup: it may already have deleted older
            // candidates, which must not become false NotFound ledger errors.
            let mut entries = store.inner.list(Some(&directory));
            while let Some(entry) = entries.try_next().await.map_err(|_| {
                Self::failure("maintenance", "state orphan files could not be inspected")
            })? {
                if !retained.contains(&entry.location)
                    && dataset
                        .data_dir()
                        .parent()
                        .is_some_and(|table| DurableIo::native_file(&table, &entry.location))
                {
                    orphans.push(entry.location);
                }
            }
        }
        for path in orphans {
            store.inner.delete(&path).await.map_err(|_| {
                Self::failure("maintenance", "state orphan file could not be removed")
            })?;
        }
        Ok(())
    }

    fn identify(transaction: &mut Transaction, uuid: String, target: u64) {
        transaction.uuid = uuid;
        transaction.transaction_properties = Some(Arc::new(HashMap::from([(
            EXPECTED_VERSION.to_owned(),
            target.to_string(),
        )])));
    }

    async fn canonical_uuid(&self, version: u64) -> Option<String> {
        let dataset = self.builder().with_version(version).load().await.ok()?;
        LanceSnapshot::validate_storage(&dataset).ok()?;
        let transaction = dataset.read_transaction().await.ok()??;
        // A native transaction stores an unchecked string. Only a valid,
        // canonical identity can establish that another writer published.
        uuid::Uuid::parse_str(&transaction.uuid)
            .ok()
            .map(|identity| identity.to_string())
    }

    async fn commit_error(&self, error: lance::Error, target: u64, uuid: &str) -> StateStoreError {
        let unpublished = match error {
            lance::Error::RetryableCommitConflict { .. }
            | lance::Error::IncompatibleTransaction { .. } => true,
            lance::Error::CommitConflict { .. } => self
                .canonical_uuid(target)
                .await
                .is_some_and(|canonical| canonical != uuid),
            _ => false,
        };
        if unpublished {
            StateStoreError::transaction_conflict(
                "commit",
                "state snapshot changed before publication",
            )
        } else {
            Self::failure(
                "commit",
                "state finalization failed; publication may have occurred",
            )
        }
    }

    fn failure(operation: &str, message: &str) -> StateStoreError {
        StateStoreError::backend_operation(operation, message)
    }
}
