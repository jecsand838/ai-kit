use std::{
    fmt,
    fs::File,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::Stream;
use lance::{
    Dataset,
    dataset::{
        CommitBuilder, InsertBuilder, ReadParams, WriteMode, WriteParams, builder::DatasetBuilder,
        transaction::Transaction,
    },
    io::{ObjectStoreParams, WrappingObjectStore},
};
use lance_table::{
    format::{IndexMetadata, Manifest, Transaction as ManifestTransaction},
    io::commit::{
        CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation,
        ManifestNamingScheme, ManifestWriter,
    },
};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    local::LocalFileSystem, path::Path,
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileRole {
    Data,
    Transaction,
    Deletion,
    Manifest,
    VersionHint,
    Other,
}

impl FileRole {
    fn of(path: &Path) -> Self {
        if path.as_ref().ends_with("latest_version_hint.json") {
            Self::VersionHint
        } else if path.as_ref().ends_with(".manifest") {
            Self::Manifest
        } else if path.as_ref().ends_with(".lance") {
            Self::Data
        } else if path.as_ref().ends_with(".txn") {
            Self::Transaction
        } else if path.as_ref().contains("/_deletions/")
            && (path.as_ref().ends_with(".arrow") || path.as_ref().ends_with(".bin"))
        {
            Self::Deletion
        } else {
            Self::Other
        }
    }
}

#[derive(Debug, Default)]
struct Observations {
    create_only_preparations: bool,
    failure: Option<FileRole>,
    read_failure: Option<FileRole>,
    synchronized: Vec<FileRole>,
    failed: Vec<FileRole>,
    failed_reads: Vec<FileRole>,
    fail_collision_barrier: bool,
    manifest_collisions: usize,
    collision_failures: Vec<String>,
    active_uploads: usize,
    completed_uploads: usize,
    aborted_uploads: usize,
    abort_errors: Vec<String>,
    unfinished_drops: usize,
    hold_abort: bool,
    abort_started: bool,
    hold_commit: bool,
    commit_entered: bool,
    completed_held_commits: usize,
}

/// A test-local native object-store hook. It never supplies a successful write
/// response itself: successes come from the real local filesystem and barriers.
#[derive(Debug)]
pub struct DurabilityProbe {
    root: PathBuf,
    observations: Mutex<Observations>,
    changed: Notify,
    abort_release: Notify,
    commit_release: Notify,
}

impl DurabilityProbe {
    pub fn new(root: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            root,
            observations: Mutex::new(Observations::default()),
            changed: Notify::new(),
            abort_release: Notify::new(),
            commit_release: Notify::new(),
        })
    }

    pub fn params(self: &Arc<Self>) -> ObjectStoreParams {
        ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(ProbeWrapper {
                probe: Arc::clone(self),
            })),
            ..ObjectStoreParams::default()
        }
    }

    pub fn hold_commit(&self) {
        self.observations.lock().expect("probe lock").hold_commit = true;
    }

    pub fn release_commit(&self) {
        self.commit_release.notify_one();
    }

    pub fn completed_held_commits(&self) -> usize {
        self.observations
            .lock()
            .expect("probe lock")
            .completed_held_commits
    }

    pub async fn wait_for_commit(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut changed = Box::pin(self.changed.notified());
                changed.as_mut().enable();
                if self.observations.lock().expect("probe lock").commit_entered {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("native conditional publication must reach the held result");
    }

    async fn after_commit(&self) {
        {
            let mut state = self.observations.lock().expect("probe lock");
            if !state.hold_commit {
                return;
            }
            state.commit_entered = true;
        }
        self.changed.notify_waiters();
        self.commit_release.notified().await;
        self.observations
            .lock()
            .expect("probe lock")
            .completed_held_commits += 1;
    }

    pub fn fail_next(&self, role: FileRole) {
        self.observations.lock().expect("probe lock").failure = Some(role);
    }

    /// Opt into the native create-only protocol for small preparation puts.
    /// This does not substitute for the supplied adapter's private classifier
    /// or multipart implementation; ordinary native multipart stays unchanged.
    pub fn create_only_preparations(&self) {
        self.observations
            .lock()
            .expect("probe lock")
            .create_only_preparations = true;
    }

    pub fn fail_next_read(&self, role: FileRole) {
        self.observations.lock().expect("probe lock").read_failure = Some(role);
    }

    pub fn fail_barrier_after_manifest_collision(&self) {
        self.observations
            .lock()
            .expect("probe lock")
            .fail_collision_barrier = true;
    }

    pub fn assert_collision_failure_retained(&self) {
        let state = self.observations.lock().expect("probe lock");
        assert!(!state.fail_collision_barrier);
        assert_eq!(state.manifest_collisions, 1);
        assert_eq!(state.collision_failures.len(), 1);
        assert!(!state.collision_failures[0].is_empty());
    }

    async fn after_collision(&self, path: &Path) {
        {
            let mut state = self.observations.lock().expect("probe lock");
            if FileRole::of(path) != FileRole::Manifest || !state.fail_collision_barrier {
                return;
            }
            state.fail_collision_barrier = false;
            state.manifest_collisions += 1;
            state.failure = Some(FileRole::Manifest);
        }
        // The genuine conditional-create error already exists. This
        // supported native hook retains a separate barrier error without
        // replacing that error or simulating the Kit's private cleanup.
        let failure = self
            .synchronize(path)
            .await
            .expect_err("armed secondary native-hook barrier must fail");
        self.observations
            .lock()
            .expect("probe lock")
            .collision_failures
            .push(failure.to_string());
    }

    pub fn failed_reads(&self, role: FileRole) -> usize {
        self.observations
            .lock()
            .expect("probe lock")
            .failed_reads
            .iter()
            .filter(|observed| **observed == role)
            .count()
    }

    fn before_read(&self, path: &Path) -> object_store::Result<()> {
        let role = FileRole::of(path);
        let mut state = self.observations.lock().expect("probe lock");
        if state.read_failure == Some(role) {
            state.read_failure = None;
            state.failed_reads.push(role);
            return Err(Self::error(std::io::Error::other(format!(
                "native test object-store hook denied {role:?} read"
            ))));
        }
        Ok(())
    }

    pub fn synchronized(&self, role: FileRole) -> usize {
        self.observations
            .lock()
            .expect("probe lock")
            .synchronized
            .iter()
            .filter(|observed| **observed == role)
            .count()
    }

    pub fn failures(&self, role: FileRole) -> usize {
        self.observations
            .lock()
            .expect("probe lock")
            .failed
            .iter()
            .filter(|observed| **observed == role)
            .count()
    }

    pub fn hold_aborts(&self) {
        self.observations.lock().expect("probe lock").hold_abort = true;
    }

    pub fn release_abort(&self) {
        self.abort_release.notify_one();
    }

    pub async fn wait_for_abort(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut changed = Box::pin(self.changed.notified());
                changed.as_mut().enable();
                if self.observations.lock().expect("probe lock").abort_started {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("native writer must request multipart abort");
    }

    pub async fn wait_for_cleanup(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut changed = Box::pin(self.changed.notified());
                changed.as_mut().enable();
                if self.observations.lock().expect("probe lock").active_uploads == 0 {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("multipart cleanup must settle");
    }

    pub fn assert_aborted(&self) {
        let state = self.observations.lock().expect("probe lock");
        assert_eq!(state.active_uploads, 0);
        assert_eq!(state.aborted_uploads, 1);
        assert_eq!(state.unfinished_drops, 0);
    }

    pub fn assert_shutdown_interrupted_abort(&self) {
        let state = self.observations.lock().expect("probe lock");
        assert!(state.abort_started);
        assert_eq!(state.aborted_uploads, 0);
        assert_eq!(state.unfinished_drops, 1);
    }

    pub fn assert_multipart_completed(&self) {
        let state = self.observations.lock().expect("probe lock");
        assert_eq!(state.active_uploads, 0);
        assert_eq!(state.completed_uploads, 1);
        assert_eq!(state.unfinished_drops, 0);
    }

    pub fn assert_abort_failed(&self) {
        let state = self.observations.lock().expect("probe lock");
        assert_eq!(state.active_uploads, 0);
        assert_eq!(state.aborted_uploads, 0);
        assert_eq!(state.abort_errors.len(), 1);
        assert_eq!(state.unfinished_drops, 1);
    }

    pub fn assert_unfinished_upload(&self) {
        let state = self.observations.lock().expect("probe lock");
        assert_eq!(state.active_uploads, 0);
        assert_eq!(state.unfinished_drops, 1);
    }

    async fn synchronize(&self, path: &Path) -> object_store::Result<()> {
        let role = FileRole::of(path);
        let fail = {
            let mut state = self.observations.lock().expect("probe lock");
            if state.failure == Some(role) {
                state.failure = None;
                state.failed.push(role);
                true
            } else {
                false
            }
        };
        if fail {
            return Err(Self::error(std::io::Error::other(format!(
                "injected {role:?} synchronization failure after native publication"
            ))));
        }

        let local = LocalFileSystem::new().path_to_filesystem(path)?;
        assert!(
            local.starts_with(&self.root),
            "probe escaped its owned root"
        );
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            File::open(&local)?.sync_all()?;
            let mut parent = local.parent();
            while let Some(directory) = parent {
                let file = File::open(directory)?;
                rustix::fs::fsync(&file)?;
                if directory == root {
                    break;
                }
                parent = directory.parent();
            }
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|error| Self::error(std::io::Error::other(error)))?
        .map_err(Self::error)?;
        self.observations
            .lock()
            .expect("probe lock")
            .synchronized
            .push(role);
        Ok(())
    }

    fn error(error: std::io::Error) -> object_store::Error {
        object_store::Error::Generic {
            store: "lance-durability-feasibility",
            source: Box::new(error),
        }
    }

    fn upload_started(&self) {
        self.observations.lock().expect("probe lock").active_uploads += 1;
    }

    fn upload_finished(&self, phase: UploadPhase) {
        let mut state = self.observations.lock().expect("probe lock");
        state.active_uploads -= 1;
        match phase {
            UploadPhase::Active => state.unfinished_drops += 1,
            UploadPhase::Completed => state.completed_uploads += 1,
            UploadPhase::Aborted => state.aborted_uploads += 1,
        }
        drop(state);
        self.changed.notify_waiters();
    }

    async fn before_abort(&self) {
        let hold = {
            let mut state = self.observations.lock().expect("probe lock");
            state.abort_started = true;
            state.hold_abort
        };
        self.changed.notify_waiters();
        if hold {
            self.abort_release.notified().await;
        }
    }
}

// Native 13's built-in handler may recover an ambiguous write error as success
// after reading its own manifest. This probe deliberately exercises the stricter
// custom-handler policy required by the StateStore's durability owner. The
// native conditional write, conflict rules and manifest bytes remain unchanged.
impl CommitHandler for DurabilityProbe {
    fn propagate_commit_error_after_success(&self) -> bool {
        true
    }

    fn commit<'owner, 'manifest, 'path, 'store, 'future>(
        &'owner self,
        manifest: &'manifest mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &'path Path,
        object_store: &'store lance::io::ObjectStore,
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
            self.after_commit().await;
            Ok(location)
        })
    }
}

#[derive(Debug)]
struct ProbeWrapper {
    probe: Arc<DurabilityProbe>,
}

impl WrappingObjectStore for ProbeWrapper {
    fn wrap(&self, _: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(ProbeStore {
            original,
            probe: Arc::clone(&self.probe),
        })
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
struct ProbeStore {
    original: Arc<dyn ObjectStore>,
    probe: Arc<DurabilityProbe>,
}

impl fmt::Display for ProbeStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("real local durability probe")
    }
}

impl ObjectStore for ProbeStore {
    fn put_opts<'store, 'path, 'future>(
        &'store self,
        location: &'path Path,
        payload: PutPayload,
        mut options: PutOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'future>>
    where
        'store: 'future,
        'path: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            if self
                .probe
                .observations
                .lock()
                .expect("probe lock")
                .create_only_preparations
                && matches!(
                    FileRole::of(location),
                    FileRole::Data | FileRole::Transaction | FileRole::Deletion
                )
            {
                options.mode = PutMode::Create;
            }
            let result = match self.original.put_opts(location, payload, options).await {
                Ok(result) => result,
                Err(error @ object_store::Error::AlreadyExists { .. }) => {
                    self.probe.after_collision(location).await;
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
            self.probe.synchronize(location).await?;
            Ok(result)
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
            let original = self.original.put_multipart_opts(location, options).await?;
            self.probe.upload_started();
            Ok(Box::new(ProbeUpload {
                original,
                probe: Arc::clone(&self.probe),
                location: location.clone(),
                phase: UploadPhase::Active,
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
        Box::pin(async move {
            self.probe.before_read(location)?;
            self.original.get_opts(location, options).await
        })
    }

    fn delete_stream(
        &self,
        locations: Pin<Box<dyn Stream<Item = object_store::Result<Path>> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = object_store::Result<Path>> + Send>> {
        self.original.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Pin<Box<dyn Stream<Item = object_store::Result<ObjectMeta>> + Send>> {
        self.original.list(prefix)
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
        self.original.list_with_delimiter(prefix)
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
        Box::pin(async move {
            self.original.copy_opts(from, to, options).await?;
            self.probe.synchronize(to).await
        })
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
        Box::pin(async move {
            self.original.rename_opts(from, to, options).await?;
            self.probe.synchronize(to).await
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum UploadPhase {
    Active,
    Completed,
    Aborted,
}

#[derive(Debug)]
struct ProbeUpload {
    original: Box<dyn MultipartUpload>,
    probe: Arc<DurabilityProbe>,
    location: Path,
    phase: UploadPhase,
}

impl MultipartUpload for ProbeUpload {
    fn put_part(
        &mut self,
        data: PutPayload,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send>> {
        self.original.put_part(data)
    }

    fn complete<'upload, 'future>(
        &'upload mut self,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'future>>
    where
        'upload: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let result = self.original.complete().await?;
            self.phase = UploadPhase::Completed;
            self.probe.synchronize(&self.location).await?;
            Ok(result)
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
            self.probe.before_abort().await;
            if let Err(error) = self.original.abort().await {
                self.probe
                    .observations
                    .lock()
                    .expect("probe lock")
                    .abort_errors
                    .push(error.to_string());
                return Err(error);
            }
            self.phase = UploadPhase::Aborted;
            Ok(())
        })
    }
}

impl Drop for ProbeUpload {
    fn drop(&mut self) {
        self.probe.upload_finished(self.phase);
    }
}

pub struct NativeFixture {
    directory: tempfile::TempDir,
    probe: Arc<DurabilityProbe>,
}

impl NativeFixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().expect("owned temporary dataset");
        let root = directory.path().canonicalize().expect("canonical root");
        Self {
            directory,
            probe: DurabilityProbe::new(root),
        }
    }

    pub fn uri(&self, object_store_path: bool) -> String {
        let root = self
            .directory
            .path()
            .canonicalize()
            .expect("canonical root");
        let mut uri = url::Url::from_directory_path(root)
            .expect("absolute directory URL")
            .join("state.lance")
            .expect("dataset URL")
            .to_string();
        if object_store_path {
            uri = uri.replacen("file:", "file-object-store:", 1);
        }
        uri
    }

    pub fn probe(&self) -> &Arc<DurabilityProbe> {
        &self.probe
    }

    pub fn multipart_payload(&self) -> Vec<u8> {
        assert!(
            std::env::var_os("LANCE_INITIAL_UPLOAD_SIZE").is_none(),
            "unset LANCE_INITIAL_UPLOAD_SIZE for the native multipart feasibility probes"
        );
        vec![42; 6 * 1024 * 1024]
    }

    pub fn local_files(&self) -> Vec<PathBuf> {
        let mut directories = vec![self.directory.path().to_path_buf()];
        let mut files = Vec::new();
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(directory).expect("owned fixture directory") {
                let entry = entry.expect("owned fixture entry");
                let kind = entry.file_type().expect("owned fixture entry type");
                if kind.is_dir() {
                    directories.push(entry.path());
                } else if kind.is_file() {
                    files.push(entry.path());
                } else {
                    panic!("unexpected entry in isolated native fixture");
                }
            }
        }
        files
    }

    pub fn occupy_destination(&self, path: &Path) {
        let local = LocalFileSystem::new()
            .path_to_filesystem(path)
            .expect("native destination");
        assert!(local.starts_with(&self.probe.root));
        std::fs::create_dir(local).expect("directory prevents native rename");
    }

    pub async fn write(
        &self,
        value: i64,
        mode: WriteMode,
        object_store_path: bool,
    ) -> lance::Result<Dataset> {
        let batch = Self::batch(value);
        let schema = batch.schema();
        let input = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let params = WriteParams {
            mode,
            store_params: Some(self.probe.params()),
            commit_handler: Some(Arc::clone(&self.probe) as Arc<dyn CommitHandler>),
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        Dataset::write(input, self.uri(object_store_path).as_str(), Some(params)).await
    }

    fn batch(value: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .expect("valid real record batch")
    }

    pub async fn prepare_overwrite(&self, value: i64) -> (Arc<Dataset>, Transaction) {
        let snapshot = Arc::new(self.open_version(1).await.expect("original native version"));
        let params = WriteParams {
            mode: WriteMode::Overwrite,
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        let transaction = InsertBuilder::new(Arc::clone(&snapshot))
            .with_params(&params)
            .execute_uncommitted(vec![Self::batch(value)])
            .await
            .expect("prepare identified native transaction without publication");
        (snapshot, transaction)
    }

    pub async fn commit(
        snapshot: Arc<Dataset>,
        transaction: Transaction,
    ) -> lance::Result<Dataset> {
        CommitBuilder::new(snapshot)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
    }

    pub async fn commit_with_timeout(
        snapshot: Arc<Dataset>,
        transaction: Transaction,
        timeout: Option<Duration>,
    ) -> lance::Result<Dataset> {
        CommitBuilder::new(snapshot)
            .with_max_retries(0)
            .with_skip_auto_cleanup(true)
            .with_timeout(timeout)
            .execute(transaction)
            .await
    }

    pub async fn open_version(&self, version: u64) -> lance::Result<Dataset> {
        DatasetBuilder::from_uri(self.uri(true))
            .with_read_params(ReadParams {
                store_options: Some(self.probe.params()),
                commit_handler: Some(Arc::clone(&self.probe) as Arc<dyn CommitHandler>),
                ..ReadParams::default()
            })
            .with_version(version)
            .load()
            .await
    }

    pub async fn reopen(&self) -> Dataset {
        Dataset::open(&self.uri(true)).await.expect("fresh dataset")
    }

    pub async fn object_store(&self) -> (Arc<lance::io::ObjectStore>, Path) {
        lance::io::ObjectStore::from_uri_and_params(
            Arc::new(lance::io::ObjectStoreRegistry::default()),
            &self.uri(true),
            &self.probe.params(),
        )
        .await
        .expect("wrapped real object store")
    }
}
