use std::{
    fs::File,
    future::Future,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use storage_kit::{
    Database, DatabaseBackend, StateStore, StateStoreBackend, StateStoreError, StateTransaction,
    StateTransactionBackend,
    database::{
        StorageLocator,
        catalog::{CatalogRequest, CreateCollectionMode, CreateCollectionRequest},
        config::{DatabaseConfig, LanceConfig},
        data::{CountRequest, DataRecord, DataRequest, DataResponse, InsertRequest},
    },
    state::{
        config::{LanceStateStoreConfig, StateStoreConfig},
        record::{StateKey, StateKeyPrefix, StateListRequest, StateRecord},
    },
};

struct Fixture {
    directory: tempfile::TempDir,
}

enum ProcessIo {
    Measured {
        write_bytes: u64,
        cancelled_write_bytes: u64,
        written_characters: u64,
    },
    Unavailable,
}

impl ProcessIo {
    fn capture() -> Self {
        if cfg!(target_os = "linux") {
            let counters = std::fs::read_to_string("/proc/self/io")
                .expect("Linux workload process I/O counters must be readable");
            let mut write_bytes = None;
            let mut cancelled_write_bytes = None;
            let mut written_characters = None;
            for line in counters.lines() {
                let mut fields = line.split_whitespace();
                let target = match fields.next() {
                    Some("write_bytes:") => &mut write_bytes,
                    Some("cancelled_write_bytes:") => &mut cancelled_write_bytes,
                    Some("wchar:") => &mut written_characters,
                    _ => continue,
                };
                assert!(target.is_none(), "duplicate process I/O counter");
                *target = Some(
                    fields
                        .next()
                        .expect("process I/O counter value")
                        .parse::<u64>()
                        .expect("valid process I/O counter"),
                );
            }
            Self::Measured {
                write_bytes: write_bytes.expect("Linux write_bytes counter"),
                cancelled_write_bytes: cancelled_write_bytes
                    .expect("Linux cancelled_write_bytes counter"),
                written_characters: written_characters.expect("Linux wchar counter"),
            }
        } else {
            Self::Unavailable
        }
    }

    fn report(&self, baseline: &Self, authored_bytes: u64) -> serde_json::Value {
        match (self, baseline) {
            (
                Self::Measured {
                    write_bytes,
                    cancelled_write_bytes,
                    written_characters,
                },
                Self::Measured {
                    write_bytes: initial_write_bytes,
                    cancelled_write_bytes: initial_cancelled_write_bytes,
                    written_characters: initial_written_characters,
                },
            ) => {
                let written = write_bytes
                    .checked_sub(*initial_write_bytes)
                    .expect("process write_bytes counter must not decrease");
                let cancelled = cancelled_write_bytes
                    .checked_sub(*initial_cancelled_write_bytes)
                    .expect("process cancelled_write_bytes counter must not decrease");
                let characters = written_characters
                    .checked_sub(*initial_written_characters)
                    .expect("process wchar counter must not decrease");
                serde_json::json!({
                    "available": true,
                    "source": "/proc/self/io",
                    "write_bytes": write_bytes,
                    "cancelled_write_bytes": cancelled_write_bytes,
                    "wchar": written_characters,
                    "delta_write_bytes": written,
                    "delta_cancelled_write_bytes": cancelled,
                    "delta_wchar": characters,
                    "process_write_bytes_per_authored_byte": (authored_bytes != 0)
                        .then(|| written as f64 / authored_bytes as f64),
                    "scope": "whole integration process and waited-for children; includes knowledge, metadata and other process writes; kernel-accounted I/O, not device durable-write amplification"
                })
            }
            (Self::Unavailable, Self::Unavailable) => serde_json::json!({
                "available": false,
                "reason": "Linux /proc/self/io counters are unavailable on this platform; device-write amplification is unmeasured"
            }),
            _ => panic!("process I/O measurement availability changed"),
        }
    }
}

impl Fixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("owned lifecycle fixture"),
        }
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }

    fn runtime(&self) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test caller runtime")
    }

    async fn connect(root: &Path) -> StateStore {
        StateStore::connect(StateStoreConfig::Lance(
            LanceStateStoreConfig::directory(root.join("database"))
                .with_operation_timeout(Duration::from_secs(20))
                .expect("operation timeout"),
        ))
        .await
        .expect("real Lance StateStore")
    }

    fn key(namespace: &str, key: &str) -> StateKey {
        StateKey::new(namespace, key).expect("valid lifecycle key")
    }

    fn accepted(result: Result<(), StateStoreError>) -> bool {
        match result {
            Ok(()) => true,
            Err(StateStoreError::TransactionConflict { .. }) => false,
            Err(error) => panic!("unexpected transaction result: {error:?}"),
        }
    }

    fn filesystem_bytes(&self) -> (u64, u64) {
        let mut pending = vec![self.root().join("database")];
        let mut bytes = 0;
        let mut files = 0;
        while let Some(directory) = pending.pop() {
            for entry in std::fs::read_dir(directory).expect("measure dataset directory") {
                let entry = entry.expect("dataset entry");
                let metadata = entry.metadata().expect("dataset metadata");
                if metadata.is_dir() {
                    pending.push(entry.path());
                } else if metadata.is_file() {
                    bytes += metadata.len();
                    files += 1;
                } else {
                    panic!("unexpected non-file in isolated workload directory");
                }
            }
        }
        (files, bytes)
    }

    fn memory_kib(&self) -> serde_json::Value {
        let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
            return serde_json::json!({
                "available": false,
                "reason": "optional process memory counters are unavailable; no external process-inspection tool is required"
            });
        };
        let mut rss = None;
        let mut peak = None;
        for line in status.lines() {
            let mut fields = line.split_whitespace();
            match fields.next() {
                Some("VmRSS:") => rss = fields.next().and_then(|value| value.parse::<u64>().ok()),
                Some("VmHWM:") => peak = fields.next().and_then(|value| value.parse::<u64>().ok()),
                _ => {}
            }
        }
        serde_json::json!({
            "available": rss.is_some(), "rss_kib": rss, "peak_rss_kib": peak,
            "scope": "whole test process, including concurrent tests and native caches"
        })
    }

    fn latency_report(samples: &[Duration]) -> serde_json::Value {
        let mut micros = samples.iter().map(Duration::as_micros).collect::<Vec<_>>();
        micros.sort_unstable();
        assert!(!micros.is_empty(), "a workload must actually run");
        serde_json::json!({
            "samples": micros.len(),
            "min_us": micros[0],
            "median_us": micros[micros.len() / 2],
            "p95_us": micros[(micros.len() * 95).div_ceil(100) - 1],
            "max_us": micros[micros.len() - 1]
        })
    }

    fn payload(bytes: usize) -> String {
        let mut state = 0x4d59_5df4_d0f3_3173_u64;
        (0..bytes)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                char::from(b'a' + (state % 26) as u8)
            })
            .collect()
    }

    async fn stage_authority(
        store: &StateStore,
        namespace: &str,
        payload: &str,
    ) -> StateTransaction {
        let mut transaction = store.begin_transaction().await.expect("idle transaction");
        transaction
            .put(StateRecord::new(
                Self::key(namespace, "head"),
                serde_json::json!("earlier staged value"),
            ))
            .await
            .expect("first authored mutation");
        transaction
            .put(StateRecord::new(
                Self::key(namespace, "head"),
                serde_json::json!({"payload": payload}),
            ))
            .await
            .expect("second authored mutation");
        transaction
            .delete(Self::key(namespace, "retired"))
            .await
            .expect("authored absent-key tombstone");
        transaction
            .put(StateRecord::new(
                Self::key(namespace, "receipt"),
                serde_json::json!({"namespace": namespace, "bytes": payload.len()}),
            ))
            .await
            .expect("atomic transaction receipt");
        transaction
    }

    async fn verify_authority(store: &StateStore, namespace: &str, payload: &str) {
        // One transaction is the atomicity oracle; unrelated ordinary loads
        // could legally observe different versions.
        let mut observed = store.begin_transaction().await.expect("fresh receipt view");
        let head = observed
            .load(Self::key(namespace, "head"))
            .await
            .expect("head read")
            .expect("accepted mutation published");
        assert_eq!(head.revision.get(), 2);
        assert_eq!(head.record.value["payload"].as_str(), Some(payload));
        let receipt = observed
            .load(Self::key(namespace, "receipt"))
            .await
            .expect("receipt read")
            .expect("complete atomic receipt");
        assert_eq!(receipt.revision.get(), 1);
        assert_eq!(
            receipt.record.value.as_ref(),
            &serde_json::json!({"namespace": namespace, "bytes": payload.len()})
        );
        assert!(
            observed
                .load(Self::key(namespace, "retired"))
                .await
                .expect("tombstone read")
                .is_none()
        );
        observed
            .put(StateRecord::new(
                Self::key(namespace, "retired"),
                serde_json::json!("uncommitted recreation"),
            ))
            .await
            .expect("probe retained tombstone revision");
        assert_eq!(
            observed
                .load(Self::key(namespace, "retired"))
                .await
                .expect("recreation read")
                .expect("staged recreation")
                .revision
                .get(),
            2
        );
        observed.rollback().await.expect("probe rollback");
        let mut progress = store
            .begin_transaction()
            .await
            .expect("subsequent admission");
        progress
            .put(StateRecord::new(
                Self::key(namespace, "progress"),
                serde_json::json!(true),
            ))
            .await
            .expect("subsequent write");
        progress.commit().await.expect("subsequent commit");
    }
}

/// Owns only a test child. Every exit path kills any live process and reaps it.
struct Process {
    child: Option<Child>,
    root: PathBuf,
    name: String,
    log: PathBuf,
}

impl Process {
    fn spawn(root: &Path, name: &str, mode: &str) -> Self {
        let log = root.join(format!("{name}.log"));
        let output = File::create(&log).expect("owned child log");
        let child = Command::new(std::env::current_exe().expect("current integration executable"))
            .args([
                "lancedb_state_lifecycle::owned_process_entry",
                "--exact",
                "--nocapture",
            ])
            .env("AI_KIT_LANCE_STATE_LIFECYCLE_ROOT", root)
            .env("AI_KIT_LANCE_STATE_LIFECYCLE_NAME", name)
            .env("AI_KIT_LANCE_STATE_LIFECYCLE_MODE", mode)
            .stdin(Stdio::null())
            .stdout(output.try_clone().expect("child stdout"))
            .stderr(output)
            .spawn()
            .expect("owned real StateStore subprocess");
        Self {
            child: Some(child),
            root: root.to_path_buf(),
            name: name.to_owned(),
            log,
        }
    }

    fn output(&self) -> String {
        std::fs::read_to_string(&self.log).expect("child log remains readable")
    }

    fn await_marker(&mut self, marker: &str) {
        let path = self.root.join(format!("{}.{marker}", self.name));
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if path.is_file() {
                return;
            }
            if let Some(status) = self
                .child
                .as_mut()
                .expect("owned child")
                .try_wait()
                .expect("observe child")
            {
                panic!(
                    "child exited before actual {marker} activity: {status}\n{}",
                    self.output()
                );
            }
            assert!(
                Instant::now() < deadline,
                "child did not reach {marker}; it will be killed and reaped\n{}",
                self.output()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn finish(&mut self) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("owned child")
                .try_wait()
                .expect("observe child completion")
            {
                self.child
                    .take()
                    .expect("completed child")
                    .wait()
                    .expect("reap completed child");
                assert!(
                    status.success(),
                    "child failed: {status}\n{}",
                    self.output()
                );
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "child watchdog elapsed; it will be killed and reaped\n{}",
                self.output()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn kill_after_marker(&mut self) {
        let child = self.child.as_mut().expect("owned child to kill");
        assert!(
            child.try_wait().expect("child still running").is_none(),
            "post-commit child must still own its live store"
        );
        child.kill().expect("kill only the owned child");
        let status = child.wait().expect("reap killed child");
        self.child.take();
        assert!(!status.success(), "test must actually kill the process");
    }

    fn receipt(&self) -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(self.root.join(format!("{}.json", self.name)))
                .expect("child wrote a receipt"),
        )
        .expect("valid child receipt")
    }

    fn mark(root: &Path, name: &str, marker: &str) {
        std::fs::write(root.join(format!("{name}.{marker}")), [])
            .expect("publish test-only process barrier");
    }

    fn finish_case(root: &Path, name: &str, evidence: serde_json::Value) {
        std::fs::write(
            root.join(format!("{name}.json")),
            serde_json::to_vec(&evidence).expect("lifecycle evidence JSON"),
        )
        .expect("persist lifecycle evidence");
    }

    fn run_case(mode: &str) -> serde_json::Value {
        let fixture = Fixture::new();
        let mut child = Self::spawn(fixture.root(), mode, mode);
        child.finish();
        assert!(
            fixture.root().join(format!("{mode}.joined")).is_file(),
            "child must finish its owned stores and runtimes before its receipt"
        );
        let mut receipt = child.receipt();
        if mode == "last-owner-drop" {
            let attempt = receipt["attempt"].as_u64().expect("witnessed attempt");
            let namespace = format!("{mode}/{attempt}");
            let payload = Fixture::payload(2 * 1024 * 1024);
            // The child has already exited and been reaped. This independent
            // parent process now verifies publication through a fresh store.
            fixture.runtime().block_on(async {
                let reopened = Fixture::connect(fixture.root()).await;
                Fixture::verify_authority(&reopened, &namespace, &payload).await;
            });
            receipt["parent_reopen_after_reap"] = serde_json::json!(true);
        }
        println!("ordinary Lance lifecycle evidence: {receipt}");
        receipt
    }

    fn authority(root: &Path, name: &str, mode: &str) {
        let payload = Fixture::payload(2 * 1024 * 1024);
        for attempt in 0..4 {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("original caller runtime");
            let store = runtime.block_on(Fixture::connect(root));
            let namespace = format!("{mode}/{attempt}");
            let transaction =
                runtime.block_on(Fixture::stage_authority(&store, &namespace, &payload));
            let mut commit = Box::pin(transaction.commit());
            let observation = {
                let _entered = runtime.enter();
                let mut context = Context::from_waker(Waker::noop());
                commit.as_mut().poll(&mut context)
            };
            match observation {
                Poll::Ready(result) => {
                    result.expect("immediate commit outcome");
                    drop(commit);
                    runtime.block_on(Fixture::verify_authority(&store, &namespace, &payload));
                    drop(store);
                    drop(runtime);
                    // Immediate completion is valid but does not witness
                    // cancellation of an accepted, still-pending observer.
                    continue;
                }
                Poll::Pending => {}
            }
            // The store was idle and these are synchronous staging methods.
            // Its first commit poll can admit into the empty bounded queue;
            // Pending therefore witnesses an accepted reply wait here. It
            // makes no assertion about a private filesystem phase.
            match mode {
                "observer-cancel" => {
                    drop(commit);
                    runtime.block_on(Fixture::verify_authority(&store, &namespace, &payload));
                    drop(store);
                    drop(runtime);
                }
                "caller-runtime-loss" => {
                    drop(runtime);
                    drop(commit);
                    let observer = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("replacement caller runtime");
                    observer.block_on(Fixture::verify_authority(&store, &namespace, &payload));
                    drop(store);
                    drop(observer);
                }
                "last-owner-drop" => {
                    drop(store);
                    // This future contains the final public owner. Its drop
                    // must finish accepted work and join the worker.
                    drop(commit);
                    drop(runtime);
                    let observer = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("fresh reopen runtime");
                    let reopened = observer.block_on(Fixture::connect(root));
                    observer.block_on(Fixture::verify_authority(&reopened, &namespace, &payload));
                    drop(reopened);
                    drop(observer);
                }
                _ => panic!("unknown authority scenario"),
            }
            Self::finish_case(
                root,
                name,
                serde_json::json!({
                    "scenario": mode, "attempt": attempt, "pending_witness": true,
                    "receipt_verified": true, "joined": true,
                    "scope": "accepted public future; no native I/O phase interception"
                }),
            );
            return;
        }
        panic!("no pending admitted commit was observed in four finite attempts");
    }

    async fn bootstrap_timeout(root: &Path, name: &str) {
        drop(Fixture::connect(root).await);
        let bootstrap = root.join("database").join("__ai_kit_state.bootstrap.lock");
        let lock = File::options()
            .read(true)
            .write(true)
            .open(bootstrap)
            .expect("open only the owned initialization lock");
        lock.lock().expect("hold initialization admission");
        let result = StateStore::connect(StateStoreConfig::Lance(
            LanceStateStoreConfig::directory(root.join("database"))
                .with_operation_timeout(Duration::from_millis(50))
                .expect("finite initialization timeout"),
        ))
        .await;
        assert!(
            matches!(result, Err(StateStoreError::TimedOut { .. })),
            "an observed lock deadline must be a timeout"
        );
        drop(lock);
        let store = Fixture::connect(root).await;
        let transaction = Fixture::stage_authority(&store, "after-lock-timeout", "value").await;
        transaction
            .commit()
            .await
            .expect("progress after releasing lock");
        Fixture::verify_authority(&store, "after-lock-timeout", "value").await;
        drop(store);
        Self::finish_case(
            root,
            name,
            serde_json::json!({"scenario": name, "timed_out": true, "joined": true}),
        );
    }

    async fn observation_timeout(root: &Path, name: &str) {
        let mut setup_timeouts = 0;
        let mut completed_commits = 0;
        let mut commit_timeouts = 0;
        let mut verified_operations = 0;
        for (attempt, mebibytes) in [8, 16, 32, 64].into_iter().enumerate() {
            let attempt_root = root.join(format!("attempt-{attempt}"));
            drop(Fixture::connect(&attempt_root).await);
            let short = StateStore::connect(StateStoreConfig::Lance(
                LanceStateStoreConfig::directory(attempt_root.join("database"))
                    .with_operation_timeout(Duration::from_millis(100))
                    .expect("finite observer timeout"),
            ))
            .await;
            let store = match short {
                Ok(store) => store,
                Err(StateStoreError::TimedOut { .. }) => {
                    setup_timeouts += 1;
                    continue;
                }
                Err(error) => panic!("unexpected setup outcome: {error}"),
            };
            let payload = Fixture::payload(mebibytes * 1024 * 1024);
            let namespace = format!("observed-timeout/{attempt}");
            let transaction = Fixture::stage_authority(&store, &namespace, &payload).await;
            let result = transaction.commit().await;
            let timed_out = match result {
                Err(StateStoreError::TimedOut { .. }) => true,
                Ok(()) => false,
                Err(error) => panic!("unexpected finalization outcome: {error}"),
            };
            // Final drop joins the admitted operation. A normal independent
            // connection can then reconcile its complete receipt regardless
            // of whether the original caller kept its response.
            drop(store);
            let reopened = Fixture::connect(&attempt_root).await;
            Fixture::verify_authority(&reopened, &namespace, &payload).await;
            drop(reopened);
            verified_operations += 1;
            if timed_out {
                commit_timeouts += 1;
                break;
            }
            completed_commits += 1;
        }
        assert!(
            verified_operations > 0,
            "no commit reached verification in four finite ordinary workloads"
        );
        assert_eq!(verified_operations, completed_commits + commit_timeouts);
        Self::finish_case(
            root,
            name,
            serde_json::json!({
                "scenario": name, "setup_timeouts": setup_timeouts,
                "completed_commits": completed_commits, "commit_timeouts": commit_timeouts,
                "verified_operations": verified_operations, "timed_out": commit_timeouts > 0,
                "receipt_verified": true, "joined": true,
                "scope": "commit authority for either configured observation outcome"
            }),
        );
    }

    async fn observer_pressure(root: &Path, name: &str) {
        let store = Fixture::connect(root).await;
        let key = Fixture::key("observer-pressure", "record");
        let mut seed = store.begin_transaction().await.expect("pressure seed");
        seed.put(StateRecord::new(key.clone(), serde_json::json!(true)))
            .await
            .expect("seed write");
        seed.commit().await.expect("seed commit");
        let mut observers = (0..96)
            .map(|_| Some(Box::pin(store.load(key.clone()))))
            .collect::<Vec<_>>();
        let mut pending = 0;
        let mut completed = 0;
        let mut cancelled = 0;
        for (index, observer) in observers.iter_mut().enumerate() {
            let mut context = Context::from_waker(Waker::noop());
            match observer
                .as_mut()
                .expect("unpolled observer")
                .as_mut()
                .poll(&mut context)
            {
                Poll::Ready(result) => {
                    assert_eq!(
                        result
                            .expect("load outcome")
                            .expect("record")
                            .revision
                            .get(),
                        1
                    );
                    completed += 1;
                    observer.take();
                }
                Poll::Pending => {
                    pending += 1;
                    if index % 3 == 0 {
                        observer.take();
                        cancelled += 1;
                    }
                }
            }
        }
        assert!(
            pending > 0 && cancelled > 0,
            "observe actual pending and cancelled requests"
        );
        for observer in observers.into_iter().flatten() {
            let observed = observer
                .await
                .expect("remaining observer completes")
                .expect("record");
            assert_eq!(observed.revision.get(), 1);
            assert_eq!(observed.record.value.as_ref(), &serde_json::json!(true));
            completed += 1;
        }
        assert_eq!(completed + cancelled, 96);
        let transaction = Fixture::stage_authority(&store, "after-observers", "progress").await;
        transaction
            .commit()
            .await
            .expect("admission remains usable");
        Fixture::verify_authority(&store, "after-observers", "progress").await;
        drop(store);
        Self::finish_case(
            root,
            name,
            serde_json::json!({
                "scenario": name, "observers": 96, "pending": pending,
                "cancelled": cancelled, "completed": completed, "joined": true,
                "scope": "public observer pressure; exact queue occupancy is not exposed"
            }),
        );
    }

    async fn wait_for_release(root: &Path) {
        tokio::time::timeout(Duration::from_secs(60), async {
            while !root.join("release").is_file() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent must release or kill its child before watchdog");
    }

    async fn contend(root: &Path, name: &str) {
        assert!(matches!(name, "left" | "right"));
        let store = Fixture::connect(root).await;
        let mut transaction = store.begin_transaction().await.expect("child transaction");
        for side in ["left", "right"] {
            let observed = transaction
                .load(Fixture::key("process-condition", side))
                .await
                .expect("cross-process condition read")
                .expect("seeded condition");
            assert_eq!(observed.record.value.as_ref(), &serde_json::json!(false));
            assert_eq!(observed.revision.get(), 1);
        }
        Self::mark(root, name, "read");
        Self::wait_for_release(root).await;
        transaction
            .put(StateRecord::new(
                Fixture::key("process-condition", name),
                serde_json::json!(true),
            ))
            .await
            .expect("stage cross-process decision");
        transaction
            .put(StateRecord::new(
                Fixture::key("process-receipts", name),
                serde_json::json!({"decision": name}),
            ))
            .await
            .expect("stage atomic receipt");
        let committed = Fixture::accepted(transaction.commit().await);
        std::fs::write(
            root.join(format!("{name}.json")),
            serde_json::to_vec(&serde_json::json!({
                "pid": std::process::id(), "committed": committed, "read_revision": 1
            }))
            .expect("child receipt JSON"),
        )
        .expect("persist test-only receipt");
        Self::mark(root, name, "finished");
    }

    async fn acknowledge_then_wait(root: &Path, name: &str) {
        let store = Fixture::connect(root).await;
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("durable child transaction");
        for key in ["head", "history/0001", "receipt/0001"] {
            transaction
                .put(StateRecord::new(
                    Fixture::key("crash-reopen", key),
                    serde_json::json!({"step": 1, "pid": std::process::id()}),
                ))
                .await
                .expect("stage durable atomic publication");
        }
        transaction.commit().await.expect("acknowledged commit");
        std::fs::write(
            root.join(format!("{name}.json")),
            serde_json::to_vec(&serde_json::json!({"pid": std::process::id()}))
                .expect("acknowledgement receipt"),
        )
        .expect("test-only acknowledgement receipt");
        Self::mark(root, name, "acknowledged");
        Self::wait_for_release(root).await;
        panic!("parent must kill this process while the acknowledged store remains live");
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn owned_process_entry() {
    let Some(root) = std::env::var_os("AI_KIT_LANCE_STATE_LIFECYCLE_ROOT") else {
        return;
    };
    let name = std::env::var("AI_KIT_LANCE_STATE_LIFECYCLE_NAME").expect("selected child name");
    let mode = std::env::var("AI_KIT_LANCE_STATE_LIFECYCLE_MODE").expect("selected child mode");
    let root = PathBuf::from(root);
    if matches!(
        mode.as_str(),
        "observer-cancel" | "caller-runtime-loss" | "last-owner-drop"
    ) {
        Process::authority(&root, &name, &mode);
        Process::mark(&root, &name, "joined");
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("independent child caller runtime");
    runtime.block_on(async {
        match mode.as_str() {
            "contend" => Process::contend(&root, &name).await,
            "acknowledge" => Process::acknowledge_then_wait(&root, &name).await,
            "bootstrap-timeout" => Process::bootstrap_timeout(&root, &name).await,
            "observation-timeout" => Process::observation_timeout(&root, &name).await,
            "observer-pressure" => Process::observer_pressure(&root, &name).await,
            _ => panic!("unknown selected child mode"),
        }
    });
    drop(runtime);
    Process::mark(&root, &name, "joined");
}

#[test]
fn admitted_commit_finishes_after_its_pending_observer_is_cancelled() {
    let receipt = Process::run_case("observer-cancel");
    assert_eq!(receipt["pending_witness"], true);
    assert_eq!(receipt["receipt_verified"], true);
}

#[test]
fn admitted_commit_finishes_after_the_originating_caller_runtime_is_lost() {
    let receipt = Process::run_case("caller-runtime-loss");
    assert_eq!(receipt["pending_witness"], true);
    assert_eq!(receipt["receipt_verified"], true);
}

#[test]
fn last_owner_drop_joins_an_admitted_commit_before_reopen() {
    let receipt = Process::run_case("last-owner-drop");
    assert_eq!(receipt["pending_witness"], true);
    assert_eq!(receipt["receipt_verified"], true);
    assert_eq!(receipt["parent_reopen_after_reap"], true);
}

#[test]
fn held_initialization_lock_times_out_without_detaching_the_owner() {
    let receipt = Process::run_case("bootstrap-timeout");
    assert_eq!(receipt["timed_out"], true);
}

#[test]
fn commit_observation_preserves_atomic_publication_authority() {
    let receipt = Process::run_case("observation-timeout");
    assert!(
        receipt["verified_operations"]
            .as_u64()
            .expect("verified operations")
            > 0
    );
    assert_eq!(receipt["receipt_verified"], true);
    assert_eq!(receipt["joined"], true);
}

#[test]
fn ninety_six_real_observers_preserve_progress_after_cancellation() {
    let receipt = Process::run_case("observer-pressure");
    assert_eq!(receipt["observers"], 96);
    assert!(receipt["cancelled"].as_u64().expect("cancellation count") > 0);
}

#[tokio::test]
async fn unpolled_transaction_operations_and_commit_publish_nothing() {
    let fixture = Fixture::new();
    let store = Fixture::connect(fixture.root()).await;
    let key = Fixture::key("unpolled", "record");
    let mut seed = store.begin_transaction().await.expect("seed");
    seed.put(StateRecord::new(key.clone(), serde_json::json!("seed")))
        .await
        .expect("seed put");
    seed.commit().await.expect("seed commit");
    drop(store.begin_transaction());
    let mut transaction = store.begin_transaction().await.expect("transaction");
    drop(transaction.put(StateRecord::new(key.clone(), serde_json::json!("unpolled"))));
    drop(transaction.delete(key.clone()));
    drop(transaction.load(key.clone()));
    let observed = transaction
        .load(key.clone())
        .await
        .expect("read")
        .expect("record");
    assert_eq!(observed.record.value.as_ref(), &serde_json::json!("seed"));
    assert_eq!(observed.revision.get(), 1);
    transaction
        .put(StateRecord::new(
            key.clone(),
            serde_json::json!("uncommitted"),
        ))
        .await
        .expect("actual staging");
    drop(transaction.commit());
    let mut next = store
        .begin_transaction()
        .await
        .expect("progress after future drop");
    assert_eq!(next.load(key.clone()).await.expect("read"), Some(observed));
    next.put(StateRecord::new(
        key.clone(),
        serde_json::json!("committed"),
    ))
    .await
    .expect("replacement");
    next.commit().await.expect("replacement commit");
    assert_eq!(
        store
            .load(key)
            .await
            .expect("read")
            .expect("record")
            .revision
            .get(),
        2
    );
}

#[tokio::test]
async fn transaction_retains_the_store_after_the_last_store_handle_is_dropped() {
    let fixture = Fixture::new();
    let store = Fixture::connect(fixture.root()).await;
    let mut transaction = store
        .begin_transaction()
        .await
        .expect("retained transaction");
    drop(store);
    let key = Fixture::key("retained-owner", "record");
    transaction
        .put(StateRecord::new(key.clone(), serde_json::json!("owned")))
        .await
        .expect("stage after store drop");
    assert_eq!(
        transaction
            .load(key.clone())
            .await
            .expect("read")
            .expect("staged")
            .revision
            .get(),
        1
    );
    transaction
        .commit()
        .await
        .expect("commit through retained owner");
    let reopened = Fixture::connect(fixture.root()).await;
    assert_eq!(
        reopened
            .load(key)
            .await
            .expect("reopen read")
            .expect("durable")
            .record
            .value
            .as_ref(),
        &serde_json::json!("owned")
    );
}

#[tokio::test]
async fn dropped_and_rolled_back_transactions_do_not_consume_revisions() {
    let fixture = Fixture::new();
    let store = Fixture::connect(fixture.root()).await;
    let key = Fixture::key("rollback-revisions", "record");
    let abandoned = Fixture::key("rollback-revisions", "abandoned");
    let mut dropped = store
        .begin_transaction()
        .await
        .expect("dropped transaction");
    dropped
        .delete(key.clone())
        .await
        .expect("first absent delete");
    dropped
        .delete(key.clone())
        .await
        .expect("second absent delete");
    dropped
        .put(StateRecord::new(abandoned.clone(), serde_json::json!(true)))
        .await
        .expect("abandoned record");
    drop(dropped);
    let mut first = store.begin_transaction().await.expect("first commit");
    first
        .put(StateRecord::new(key.clone(), serde_json::json!(1)))
        .await
        .expect("first value");
    first.commit().await.expect("first committed value");
    let mut rollback = store.begin_transaction().await.expect("rollback");
    rollback
        .delete(key.clone())
        .await
        .expect("uncommitted delete");
    rollback
        .put(StateRecord::new(key.clone(), serde_json::json!(2)))
        .await
        .expect("uncommitted recreate");
    rollback.rollback().await.expect("rollback complete");
    assert!(
        store
            .load(abandoned)
            .await
            .expect("abandoned read")
            .is_none()
    );
    assert_eq!(
        store
            .load(key.clone())
            .await
            .expect("retained value")
            .expect("record")
            .revision
            .get(),
        1
    );
    let mut next = store.begin_transaction().await.expect("next transaction");
    next.put(StateRecord::new(key.clone(), serde_json::json!(3)))
        .await
        .expect("next write");
    next.commit().await.expect("next commit");
    assert_eq!(
        store
            .load(key)
            .await
            .expect("next read")
            .expect("record")
            .revision
            .get(),
        2
    );
}

#[test]
fn independent_processes_preserve_read_conditions_and_atomic_receipts() {
    let fixture = Fixture::new();
    fixture.runtime().block_on(async {
        let store = Fixture::connect(fixture.root()).await;
        let mut seed = store.begin_transaction().await.expect("seed decisions");
        for side in ["left", "right"] {
            seed.put(StateRecord::new(
                Fixture::key("process-condition", side),
                serde_json::json!(false),
            ))
            .await
            .expect("seed decision");
        }
        seed.commit().await.expect("seed commit");
    });
    let mut left = Process::spawn(fixture.root(), "left", "contend");
    let mut right = Process::spawn(fixture.root(), "right", "contend");
    left.await_marker("read");
    right.await_marker("read");
    std::fs::write(fixture.root().join("release"), []).expect("release both observed snapshots");
    left.await_marker("finished");
    right.await_marker("finished");
    left.finish();
    right.finish();
    let receipts = [left.receipt(), right.receipt()];
    assert_ne!(receipts[0]["pid"], receipts[1]["pid"]);
    for receipt in &receipts {
        assert_ne!(receipt["pid"].as_u64(), Some(u64::from(std::process::id())));
        assert_eq!(receipt["read_revision"], 1);
    }
    let committed = receipts.each_ref().map(|receipt| {
        receipt["committed"]
            .as_bool()
            .expect("actual commit receipt")
    });
    assert!(
        !(committed[0] && committed[1]),
        "both stale conditional decisions cannot publish"
    );
    fixture.runtime().block_on(async {
        let store = Fixture::connect(fixture.root()).await;
        for (index, side) in ["left", "right"].into_iter().enumerate() {
            let decision = store
                .load(Fixture::key("process-condition", side))
                .await
                .expect("decision")
                .expect("seeded decision");
            assert_eq!(decision.record.value.as_bool(), Some(committed[index]));
            assert_eq!(decision.revision.get(), 1 + u64::from(committed[index]));
            assert_eq!(
                store
                    .load(Fixture::key("process-receipts", side))
                    .await
                    .expect("receipt")
                    .is_some(),
                committed[index]
            );
        }
        let mut next = store
            .begin_transaction()
            .await
            .expect("progress after process conflict");
        next.put(StateRecord::new(
            Fixture::key("process-condition", "fresh"),
            serde_json::json!(true),
        ))
        .await
        .expect("fresh write");
        next.commit().await.expect("fresh commit");
    });
}

#[test]
fn acknowledged_commit_survives_abrupt_process_kill_and_reopen() {
    let fixture = Fixture::new();
    let mut child = Process::spawn(fixture.root(), "acknowledged", "acknowledge");
    child.await_marker("acknowledged");
    let receipt = child.receipt();
    let pid = receipt["pid"].as_u64().expect("real child pid");
    assert_ne!(pid, u64::from(std::process::id()));
    child.kill_after_marker();
    fixture.runtime().block_on(async {
        let store = Fixture::connect(fixture.root()).await;
        for key in ["head", "history/0001", "receipt/0001"] {
            let observed = store
                .load(Fixture::key("crash-reopen", key))
                .await
                .expect("post-crash read")
                .expect("acknowledged record");
            assert_eq!(observed.revision.get(), 1);
            assert_eq!(
                observed.record.value.as_ref(),
                &serde_json::json!({"step": 1, "pid": pid})
            );
        }
        let mut next = store
            .begin_transaction()
            .await
            .expect("progress after crash");
        next.put(StateRecord::new(
            Fixture::key("crash-reopen", "head"),
            serde_json::json!({"step": 2}),
        ))
        .await
        .expect("advance head");
        next.delete(Fixture::key("crash-reopen", "history/0001"))
            .await
            .expect("delete retained history");
        next.commit().await.expect("post-crash commit");
        assert_eq!(
            store
                .load(Fixture::key("crash-reopen", "head"))
                .await
                .expect("new head")
                .expect("head")
                .revision
                .get(),
            2
        );
        assert!(
            store
                .load(Fixture::key("crash-reopen", "history/0001"))
                .await
                .expect("retired history")
                .is_none()
        );
    });
}

#[tokio::test]
async fn representative_state_and_knowledge_workloads_report_measurements() {
    let fixture = Fixture::new();
    let store = Fixture::connect(fixture.root()).await;
    let competitor = Fixture::connect(fixture.root()).await;
    let database = Database::connect(DatabaseConfig::lance(LanceConfig::embedded_path(
        fixture.root().join("database"),
    )))
    .await
    .expect("knowledge database beside state");
    let knowledge = StorageLocator::new("workload_knowledge");
    database
        .catalog(CatalogRequest::CreateCollection(CreateCollectionRequest {
            locator: knowledge.clone(),
            schema: None,
            records: vec![DataRecord::without_id().json_field("body", serde_json::json!("seed"))],
            mode: CreateCollectionMode::Create,
        }))
        .await
        .expect("knowledge collection");
    let (initial_files, initial_bytes) = fixture.filesystem_bytes();
    let initial_memory = fixture.memory_kib();
    let initial_process_io = ProcessIo::capture();
    let checkpoint_barrier = tokio::sync::Barrier::new(2);
    let knowledge_value_bytes = AtomicU64::new(0);
    let started = Instant::now();
    let state_work = async {
        let checkpoint = "c".repeat(256 * 1024);
        let notification = "n".repeat(64 * 1024);
        let history = "h".repeat(8 * 1024);
        let mut commit_times = Vec::new();
        let mut authored_value_bytes = 0_u64;
        let mut growth = Vec::new();
        for sequence in 0..32 {
            let start = Instant::now();
            let mut transaction = store
                .begin_transaction()
                .await
                .expect("workload transaction");
            let records = [
                StateRecord::new(
                    Fixture::key("workflow-workload", "head"),
                    serde_json::json!({"sequence": sequence, "checkpoint": &checkpoint}),
                ),
                StateRecord::new(
                    Fixture::key("agent-workload", "notifications"),
                    serde_json::json!({"sequence": sequence, "message": &notification}),
                ),
                StateRecord::new(
                    Fixture::key("workflow-workload", &format!("history/{sequence:04}")),
                    serde_json::json!({"sequence": sequence, "history": &history}),
                ),
                StateRecord::new(
                    Fixture::key("workflow-workload", &format!("receipt/{sequence:04}")),
                    serde_json::json!({"sequence": sequence, "applied": true}),
                ),
            ];
            for record in records {
                authored_value_bytes += serde_json::to_vec(record.value.as_ref())
                    .expect("serialize authored workload value")
                    .len() as u64;
                transaction
                    .put(record)
                    .await
                    .expect("authored workload record");
            }
            if sequence >= 3 {
                for prefix in ["history", "receipt"] {
                    transaction
                        .delete(Fixture::key(
                            "workflow-workload",
                            &format!("{prefix}/{:04}", sequence - 3),
                        ))
                        .await
                        .expect("bounded history retirement");
                }
            }
            transaction
                .commit()
                .await
                .expect("atomic workload publication");
            commit_times.push(start.elapsed());
            if (sequence + 1) % 8 == 0 {
                // Both operation streams have acknowledged their current
                // writes. Pause knowledge until the filesystem scan finishes,
                // so directory rename/unlink races cannot skew the checkpoint.
                checkpoint_barrier.wait().await;
                let process_io = ProcessIo::capture();
                let (files, bytes) = fixture.filesystem_bytes();
                let knowledge_bytes = knowledge_value_bytes.load(Ordering::SeqCst);
                let logical_bytes = authored_value_bytes + knowledge_bytes;
                let growth_bytes = i128::from(bytes) - i128::from(initial_bytes);
                growth.push(serde_json::json!({
                    "completed_state_transactions": sequence + 1,
                    "unique_retired_keys": (sequence - 2) * 2,
                    "serialized_state_value_bytes": authored_value_bytes,
                    "serialized_knowledge_value_bytes": knowledge_bytes,
                    "serialized_authored_bytes": logical_bytes,
                    "shared_directory_files": files,
                    "shared_directory_bytes": bytes,
                    "physical_growth_bytes": growth_bytes,
                    "physical_growth_per_authored_byte": growth_bytes as f64 / logical_bytes as f64,
                    "process_io": process_io.report(&initial_process_io, logical_bytes)
                }));
                checkpoint_barrier.wait().await;
            }
        }
        (commit_times, authored_value_bytes, growth)
    };
    let knowledge_work = async {
        let mut insert_times = Vec::new();
        for batch in 0..4 {
            let mut batch_value_bytes = 0;
            let records = (0..16)
                .map(|row| {
                    let body = serde_json::json!(format!(
                        "batch {batch} document {row}: {}",
                        "knowledge ".repeat(64)
                    ));
                    batch_value_bytes += serde_json::to_vec(&body)
                        .expect("serialize authored knowledge value")
                        .len() as u64;
                    DataRecord::without_id().json_field("body", body)
                })
                .collect();
            let start = Instant::now();
            database
                .data(DataRequest::Insert(InsertRequest::append(
                    knowledge.clone(),
                    records,
                )))
                .await
                .expect("concurrent real knowledge insert");
            insert_times.push(start.elapsed());
            knowledge_value_bytes.fetch_add(batch_value_bytes, Ordering::SeqCst);
            checkpoint_barrier.wait().await;
            checkpoint_barrier.wait().await;
        }
        insert_times
    };
    let ((commit_times, authored_value_bytes, growth), insert_times) =
        tokio::join!(state_work, knowledge_work);
    let concurrent_elapsed = started.elapsed();
    let workload_process_io = ProcessIo::capture();
    let visible_history = store
        .list(
            StateListRequest::new("workflow-workload")
                .expect("namespace")
                .with_key_prefix(StateKeyPrefix::new("history/").expect("history prefix")),
        )
        .await
        .expect("retained history page");
    assert_eq!(visible_history.records.len(), 3);
    assert!(visible_history.next_start_after.is_none());
    assert_eq!(
        visible_history
            .records
            .iter()
            .map(|record| record.record.key.key())
            .collect::<Vec<_>>(),
        ["history/0029", "history/0030", "history/0031"]
    );
    for (namespace, key, field, bytes) in [
        ("workflow-workload", "head", "checkpoint", 256 * 1024),
        ("agent-workload", "notifications", "message", 64 * 1024),
    ] {
        let observed = store
            .load(Fixture::key(namespace, key))
            .await
            .expect("final authored payload")
            .expect("retained head");
        assert_eq!(observed.revision.get(), 32);
        assert_eq!(observed.record.value["sequence"], 31);
        assert_eq!(
            observed.record.value[field]
                .as_str()
                .expect("payload")
                .len(),
            bytes
        );
    }
    assert_eq!(
        database
            .data(DataRequest::Count(CountRequest {
                locator: knowledge,
                filter: None
            }))
            .await
            .expect("knowledge count"),
        DataResponse::Count(65)
    );
    let mut recreate = store
        .begin_transaction()
        .await
        .expect("tombstone continuity");
    let mut verified_tombstones = 0;
    for sequence in 0..29 {
        for prefix in ["history", "receipt"] {
            let retired = Fixture::key("workflow-workload", &format!("{prefix}/{sequence:04}"));
            assert!(
                recreate
                    .load(retired.clone())
                    .await
                    .expect("retired record")
                    .is_none()
            );
            recreate
                .put(StateRecord::new(
                    retired.clone(),
                    serde_json::json!("probe"),
                ))
                .await
                .expect("stage retired record recreation");
            assert_eq!(
                recreate
                    .load(retired)
                    .await
                    .expect("staged recreation")
                    .expect("recreated record")
                    .revision
                    .get(),
                3
            );
            verified_tombstones += 1;
        }
    }
    recreate
        .rollback()
        .await
        .expect("retain retired tombstones");
    assert_eq!(verified_tombstones, 58);

    let counter = Fixture::key("contention-workload", "counter");
    let mut seed = store.begin_transaction().await.expect("counter seed");
    seed.put(StateRecord::new(counter.clone(), serde_json::json!(0)))
        .await
        .expect("counter");
    seed.commit().await.expect("counter seed commit");
    let mut contention_times = Vec::new();
    let mut successes = 0_u64;
    for _ in 0..4 {
        let mut left = store.begin_transaction().await.expect("first contender");
        let mut right = competitor
            .begin_transaction()
            .await
            .expect("second contender");
        for transaction in [&mut left, &mut right] {
            let before = transaction
                .load(counter.clone())
                .await
                .expect("contender read")
                .expect("counter");
            assert_eq!(before.record.value.as_u64(), Some(successes));
            transaction
                .put(StateRecord::new(
                    counter.clone(),
                    serde_json::json!(successes + 1),
                ))
                .await
                .expect("stage increment");
        }
        let start = Instant::now();
        let (left, right) = tokio::join!(left.commit(), right.commit());
        let accepted = u64::from(Fixture::accepted(left)) + u64::from(Fixture::accepted(right));
        assert!(
            accepted <= 1,
            "same-snapshot increments cannot both publish"
        );
        successes += accepted;
        contention_times.push(start.elapsed());
    }
    let observed = store
        .load(counter)
        .await
        .expect("contention result")
        .expect("counter");
    assert_eq!(observed.record.value.as_u64(), Some(successes));
    assert_eq!(observed.revision.get(), successes + 1);
    let (final_files, final_bytes) = fixture.filesystem_bytes();
    eprintln!(
        "LANCE_STATE_WORKLOAD {}",
        serde_json::json!({
            "state_transactions": 32, "checkpoint_bytes": 256 * 1024, "notification_bytes": 64 * 1024,
            "history_bytes": 8 * 1024, "unique_retired_keys": 58, "knowledge_rows_inserted": 64,
            "serialized_state_value_bytes": authored_value_bytes,
            "serialized_knowledge_value_bytes": knowledge_value_bytes.load(Ordering::SeqCst),
            "serialized_authored_bytes": authored_value_bytes + knowledge_value_bytes.load(Ordering::SeqCst),
            "initial_process_io": initial_process_io.report(&initial_process_io, 0),
            "workload_process_io": workload_process_io.report(
                &initial_process_io,
                authored_value_bytes + knowledge_value_bytes.load(Ordering::SeqCst)
            ),
            "retained_tombstone_revisions_verified": verified_tombstones,
            "growth_checkpoints": growth,
            "state_transaction_latency": Fixture::latency_report(&commit_times),
            "knowledge_insert_latency": Fixture::latency_report(&insert_times),
            "concurrent_elapsed_ms": concurrent_elapsed.as_millis(),
            "state_transactions_per_second": 32.0 / concurrent_elapsed.as_secs_f64(),
            "contention_round_latency": Fixture::latency_report(&contention_times),
            "contention_attempts": 8, "contention_commits": successes, "contention_conflicts": 8 - successes,
            "initial_shared_directory_files": initial_files, "final_shared_directory_files": final_files,
            "initial_shared_directory_bytes": initial_bytes, "final_shared_directory_bytes": final_bytes,
            "initial_process_memory": initial_memory, "final_process_memory": fixture.memory_kib(),
            "logical_byte_definition": "UTF-8 serde_json encodings of authored state and knowledge values; excludes keys, tombstone requests, baseline data, and native metadata",
            "interpretation": "bounded growth measurements with compressible payloads and retained native versions; physical/logical ratio includes same-directory knowledge, metadata and old versions; Linux process I/O counters separately measure kernel-accounted write amplification and cancelled writeback, not device durability or isolated state writes; every retired tombstone revision is checked through a rolled-back recreation; no performance acceptance threshold; process kill does not simulate power loss"
        })
    );
}
