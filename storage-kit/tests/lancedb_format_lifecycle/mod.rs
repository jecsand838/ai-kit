//! Native format-selection and actual multipart-child failure probes.
//! These use the public native object-store extension, not the supplied adapter.
//! They exercise a real ObjectWriter child error/panic after real local I/O;
//! they do not inject an encoder panic or qualify StateStore durability.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::{
    BooleanArray, LargeBinaryArray, RecordBatch, RecordBatchIterator, StringArray, UInt8Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::Stream;
use lance::{
    Dataset,
    dataset::{WriteMode, WriteParams},
    io::{ObjectStoreParams, WrappingObjectStore},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, path::Path,
};

#[derive(Clone, Copy, Debug)]
enum ChildFailure {
    Error,
    Panic,
}

#[derive(Debug)]
struct PartProbe {
    failure: ChildFailure,
    armed: AtomicBool,
    injected: AtomicUsize,
    started: AtomicUsize,
    settled: AtomicUsize,
    native_parts_written: AtomicUsize,
    uploads: AtomicUsize,
    aborted: AtomicUsize,
}

impl PartProbe {
    fn new(failure: ChildFailure) -> Arc<Self> {
        Arc::new(Self {
            failure,
            armed: AtomicBool::new(false),
            injected: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            settled: AtomicUsize::new(0),
            native_parts_written: AtomicUsize::new(0),
            uploads: AtomicUsize::new(0),
            aborted: AtomicUsize::new(0),
        })
    }

    fn params(self: &Arc<Self>) -> ObjectStoreParams {
        ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(PartWrapper {
                probe: Arc::clone(self),
            })),
            ..ObjectStoreParams::default()
        }
    }

    fn assert_settled(&self) {
        assert_eq!(self.injected.load(Ordering::SeqCst), 1);
        assert!(self.native_parts_written.load(Ordering::SeqCst) > 0);
        assert_eq!(
            self.started.load(Ordering::SeqCst),
            self.settled.load(Ordering::SeqCst)
        );
        assert_eq!(self.uploads.load(Ordering::SeqCst), 0);
        assert_eq!(self.aborted.load(Ordering::SeqCst), 1);
    }
}

#[derive(Debug)]
struct PartWrapper {
    probe: Arc<PartProbe>,
}

impl WrappingObjectStore for PartWrapper {
    fn wrap(&self, _prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(PartStore {
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
struct PartStore {
    original: Arc<dyn ObjectStore>,
    probe: Arc<PartProbe>,
}

impl fmt::Display for PartStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("native-format-child-failure-probe")
    }
}

impl ObjectStore for PartStore {
    fn put_opts<'a, 'b, 'f>(
        &'a self,
        path: &'b Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        Self: 'f,
    {
        self.original.put_opts(path, payload, options)
    }

    fn put_multipart_opts<'a, 'b, 'f>(
        &'a self,
        path: &'b Path,
        options: PutMultipartOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Box<dyn MultipartUpload>>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            let original = self.original.put_multipart_opts(path, options).await?;
            self.probe.uploads.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(PartUpload {
                original,
                probe: Arc::clone(&self.probe),
            }) as Box<dyn MultipartUpload>)
        })
    }

    fn get_opts<'a, 'b, 'f>(
        &'a self,
        path: &'b Path,
        options: GetOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<GetResult>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        Self: 'f,
    {
        self.original.get_opts(path, options)
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

    fn list_with_delimiter<'a, 'b, 'f>(
        &'a self,
        prefix: Option<&'b Path>,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<ListResult>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        Self: 'f,
    {
        self.original.list_with_delimiter(prefix)
    }

    fn copy_opts<'a, 'b, 'c, 'f>(
        &'a self,
        from: &'b Path,
        to: &'c Path,
        options: CopyOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        Self: 'f,
    {
        self.original.copy_opts(from, to, options)
    }

    fn rename_opts<'a, 'b, 'c, 'f>(
        &'a self,
        from: &'b Path,
        to: &'c Path,
        options: RenameOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        Self: 'f,
    {
        self.original.rename_opts(from, to, options)
    }
}

#[derive(Debug)]
struct PartUpload {
    original: Box<dyn MultipartUpload>,
    probe: Arc<PartProbe>,
}

struct PartLifetime {
    probe: Arc<PartProbe>,
}

impl Drop for PartLifetime {
    fn drop(&mut self) {
        self.probe.settled.fetch_add(1, Ordering::SeqCst);
    }
}

impl MultipartUpload for PartUpload {
    fn put_part(
        &mut self,
        payload: PutPayload,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send>> {
        let native = self.original.put_part(payload);
        let probe = Arc::clone(&self.probe);
        Box::pin(async move {
            assert!(
                tokio::task::try_id().is_some(),
                "part must run in an actual native child task"
            );
            probe.started.fetch_add(1, Ordering::SeqCst);
            let _lifetime = PartLifetime {
                probe: Arc::clone(&probe),
            };
            native.await?;
            probe.native_parts_written.fetch_add(1, Ordering::SeqCst);
            if probe.armed.swap(false, Ordering::SeqCst) {
                probe.injected.fetch_add(1, Ordering::SeqCst);
                match probe.failure {
                    ChildFailure::Error => {
                        return Err(object_store::Error::Generic {
                            store: "native-format-child-failure-probe",
                            source: Box::new(std::io::Error::other(
                                "part child failed after actual native write",
                            )),
                        });
                    }
                    ChildFailure::Panic => {
                        panic!("native part child panic after actual local write")
                    }
                }
            }
            Ok(())
        })
    }

    fn complete<'a, 'f>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'f>>
    where
        'a: 'f,
        Self: 'f,
    {
        self.original.complete()
    }

    fn abort<'a, 'f>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'f>>
    where
        'a: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            self.original.abort().await?;
            self.probe.aborted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

impl Drop for PartUpload {
    fn drop(&mut self) {
        self.probe.uploads.fetch_sub(1, Ordering::SeqCst);
    }
}

struct FormatFixture {
    directory: tempfile::TempDir,
    probe: Arc<PartProbe>,
}

impl FormatFixture {
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("kind", DataType::UInt8, false),
            Field::new("namespace", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("revision", DataType::UInt64, false),
            Field::new("present", DataType::Boolean, false),
            Field::new("value", DataType::LargeBinary, true),
            Field::new("token", DataType::Utf8, false),
        ]))
    }

    fn uri(&self) -> String {
        url::Url::from_file_path(self.directory.path().join("native.lance"))
            .expect("owned local URI")
            .to_string()
            .replacen("file:", "file-object-store:", 1)
    }

    fn payload() -> Vec<u8> {
        let mut seed = 0x1357_9bdf_2468_ace1_u64;
        let mut text = String::with_capacity(12 * 1024 * 1024);
        for _ in 0..12 * 1024 * 1024 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            text.push(char::from(b'!' + (seed % 90) as u8));
        }
        serde_json::to_vec(&text).expect("actual JSON payload")
    }

    async fn write(&self, key: &str, value: Vec<u8>, mode: WriteMode) -> lance::Result<Dataset> {
        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt8Array::from(vec![0])),
                Arc::new(StringArray::from(vec!["native-test"])),
                Arc::new(StringArray::from(vec![key])),
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(LargeBinaryArray::from(vec![Some(value.as_slice())])),
                Arc::new(StringArray::from(vec![""])),
            ],
        )
        .expect("native state-shaped batch");
        let params = WriteParams {
            mode,
            store_params: Some(self.probe.params()),
            data_storage_version: Some("2.0".parse().expect("supported native format")),
            skip_auto_cleanup: true,
            ..WriteParams::default()
        };
        Dataset::write(
            RecordBatchIterator::new([Ok(batch)], Self::schema()),
            self.uri().as_str(),
            Some(params),
        )
        .await
    }

    fn drain(runtime: &tokio::runtime::Runtime) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while runtime.metrics().num_alive_tasks() != 0 {
            assert!(
                Instant::now() < deadline,
                "native child drain did not finish"
            );
            runtime.block_on(tokio::task::yield_now());
        }
    }

    fn run(failure: ChildFailure) {
        assert!(
            std::env::var_os("LANCE_INITIAL_UPLOAD_SIZE").is_none(),
            "unset native multipart sizing override for this probe"
        );
        let fixture = Self {
            directory: tempfile::tempdir().expect("owned native directory"),
            probe: PartProbe::new(failure),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owned native runtime");
        let original = runtime
            .block_on(fixture.write("initial", b"null".to_vec(), WriteMode::Create))
            .expect("initial real native table");
        assert_eq!(
            original
                .manifest()
                .data_storage_format
                .lance_file_format()
                .to_data_file_numbers(),
            (2, 0)
        );
        assert!(!original.manifest().uses_stable_row_ids());
        drop(original);
        Self::drain(&runtime);

        fixture.probe.armed.store(true, Ordering::SeqCst);
        let result = runtime.block_on(fixture.write("failed", Self::payload(), WriteMode::Append));
        assert!(
            result.is_err(),
            "native child failure must reach the parent write result"
        );
        drop(result);
        Self::drain(&runtime);
        fixture.probe.assert_settled();
        drop(runtime);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fresh native runtime");
        runtime.block_on(async {
            let observed = Dataset::open(&fixture.uri())
                .await
                .expect("fresh native reopen");
            assert_eq!(observed.version_id(), 1);
            assert_eq!(
                observed
                    .count_rows(None)
                    .await
                    .expect("count unchanged rows"),
                1
            );
            let progressed = fixture
                .write("later", b"true".to_vec(), WriteMode::Append)
                .await
                .expect("progress after failed child and runtime join");
            assert_eq!(progressed.version_id(), 2);
            assert_eq!(
                progressed.count_rows(None).await.expect("count later rows"),
                2
            );
            assert_eq!(
                progressed
                    .manifest()
                    .data_storage_format
                    .lance_file_format()
                    .to_data_file_numbers(),
                (2, 0)
            );
        });
        Self::drain(&runtime);
        drop(runtime);
    }
}

#[test]
fn native_format_two_zero_propagates_actual_part_child_error_and_drains() {
    FormatFixture::run(ChildFailure::Error);
}

#[test]
fn native_format_two_zero_propagates_actual_part_child_panic_and_drains() {
    FormatFixture::run(ChildFailure::Panic);
}
