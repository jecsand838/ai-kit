use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::FutureExt;
use storage_sdk::{
    StateStore, StateStoreBackend, StateStoreError, StateTransaction, StateTransactionBackend,
    state::{
        config::{PostgresStateStoreConfig, StateStoreConfig},
        record::StateRecord,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::watch,
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep, timeout, timeout_at},
};

use super::PostgresCases;

#[derive(Clone, Copy)]
enum RollbackReceipt {
    Forward,
    Lose,
}

#[derive(Default)]
struct RelayEvidence {
    conflicts: usize,
    rollbacks: usize,
    lost_receipts: usize,
    backend_pids: BTreeSet<i32>,
}

// This bridge never manufactures a PostgreSQL reply. The negative case cuts
// only after the real server reports ROLLBACK on the connection that emitted
// SQLSTATE 40001. It proves lost cleanup acknowledgment, not failed rollback.
struct RollbackRelay {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Vec<String>>>,
    evidence: Arc<Mutex<RelayEvidence>>,
}

impl RollbackRelay {
    async fn start(host: String, port: u16, receipt: RollbackReceipt) -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let (stop, stopping) = watch::channel(false);
        let evidence = Arc::new(Mutex::new(RelayEvidence::default()));
        let observed = Arc::clone(&evidence);
        let task = tokio::spawn(Self::run(listener, host, port, receipt, stopping, observed));
        Ok(Self {
            address,
            stop,
            task: Some(task),
            evidence,
        })
    }

    async fn run(
        listener: TcpListener,
        host: String,
        port: u16,
        receipt: RollbackReceipt,
        mut stopping: watch::Receiver<bool>,
        evidence: Arc<Mutex<RelayEvidence>>,
    ) -> Vec<String> {
        let mut connections = JoinSet::new();
        let mut errors = Vec::new();
        loop {
            tokio::select! {
                _ = stopping.changed() => break,
                accepted = listener.accept() => match accepted {
                    Ok((client, _)) => {
                        connections.spawn(Self::connection(
                            client, host.clone(), port, receipt, stopping.clone(), Arc::clone(&evidence),
                        ));
                    }
                    Err(error) => { errors.push(format!("relay accept: {error}")); break; }
                },
                joined = connections.join_next(), if !connections.is_empty() => {
                    Self::record_join(joined, &mut errors);
                }
            }
        }
        // The sender remains on the fixture while every connection observes
        // stop and is joined. No relay task is detached on the normal path.
        while let Some(joined) = connections.join_next().await {
            Self::record_join(Some(joined), &mut errors);
        }
        errors
    }

    fn record_join(
        result: Option<Result<io::Result<()>, tokio::task::JoinError>>,
        errors: &mut Vec<String>,
    ) {
        match result {
            Some(Ok(Ok(()))) | None => {}
            Some(Ok(Err(error))) => errors.push(format!("relay connection: {error}")),
            Some(Err(error)) => errors.push(format!("relay connection task: {error}")),
        }
    }

    async fn connection(
        client: TcpStream,
        host: String,
        port: u16,
        receipt: RollbackReceipt,
        mut stopping: watch::Receiver<bool>,
        evidence: Arc<Mutex<RelayEvidence>>,
    ) -> io::Result<()> {
        if *stopping.borrow() {
            return Ok(());
        }
        let backend = tokio::select! {
            result = timeout(Duration::from_secs(5), TcpStream::connect((host.as_str(), port))) => {
                result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "relay connect deadline"))??
            }
            _ = stopping.changed() => return Ok(()),
        };
        let (mut client_read, client_write) = client.into_split();
        let (backend_read, mut backend_write) = backend.into_split();
        tokio::select! {
            _ = stopping.changed() => Ok(()),
            copied = tokio::io::copy(&mut client_read, &mut backend_write) => copied.map(|_| ()),
            copied = Self::backend_frames(backend_read, client_write, receipt, evidence) => copied,
        }
    }

    async fn backend_frames(
        mut backend: OwnedReadHalf,
        mut client: OwnedWriteHalf,
        receipt: RollbackReceipt,
        evidence: Arc<Mutex<RelayEvidence>>,
    ) -> io::Result<()> {
        let mut conflict = false;
        loop {
            let mut tag = [0_u8; 1];
            if backend.read(&mut tag).await? == 0 {
                return Ok(());
            }
            let mut length = [0_u8; 4];
            backend.read_exact(&mut length).await?;
            let length_value = u32::from_be_bytes(length) as usize;
            if !(4..=1024 * 1024).contains(&length_value) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected native frame size",
                ));
            }
            let mut payload = vec![0; length_value - 4];
            backend.read_exact(&mut payload).await?;
            let lose = {
                let mut observed = evidence.lock().expect("relay evidence");
                if tag[0] == b'K' && payload.len() >= 4 {
                    observed.backend_pids.insert(i32::from_be_bytes(
                        payload[..4].try_into().expect("PID bytes"),
                    ));
                }
                if tag[0] == b'E'
                    && payload
                        .split(|byte| *byte == 0)
                        .any(|field| field == b"C40001")
                {
                    conflict = true;
                    observed.conflicts += 1;
                }
                if conflict && tag[0] == b'C' && payload == b"ROLLBACK\0" {
                    observed.rollbacks += 1;
                    if matches!(receipt, RollbackReceipt::Lose) {
                        observed.lost_receipts += 1;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if lose {
                return Ok(());
            }
            client.write_all(&tag).await?;
            client.write_all(&length).await?;
            client.write_all(&payload).await?;
        }
    }

    async fn finish(&mut self) -> Vec<String> {
        self.stop.send_replace(true);
        let Some(task) = self.task.as_mut() else {
            return Vec::new();
        };
        let mut errors = match timeout(Duration::from_secs(5), &mut *task).await {
            Ok(Ok(errors)) => {
                self.task.take();
                return errors;
            }
            Ok(Err(error)) => {
                self.task.take();
                return vec![format!("relay owner: {error}")];
            }
            Err(_) => vec!["relay owner did not join before cleanup deadline".to_owned()],
        };
        task.abort();
        match timeout(Duration::from_secs(5), &mut *task).await {
            Ok(Err(error)) if error.is_cancelled() => {
                self.task.take();
            }
            Ok(result) => {
                self.task.take();
                errors.push(format!("relay fallback: {result:?}"));
            }
            Err(_) => errors.push("relay fallback join remains unconfirmed".to_owned()),
        }
        errors
    }
}

impl Drop for RollbackRelay {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct CleanupReceiptCase {
    case: PostgresCases,
    receipt: RollbackReceipt,
    relay: Option<RollbackRelay>,
    client: Option<tokio_postgres::Client>,
    driver: Option<JoinHandle<Result<(), tokio_postgres::Error>>>,
    stores: Vec<StateStore>,
    stale: Option<StateTransaction>,
    runtime_baseline: usize,
}

impl CleanupReceiptCase {
    async fn run(receipt: RollbackReceipt) {
        let mut case = Self {
            case: PostgresCases::from_env(),
            receipt,
            relay: None,
            client: None,
            driver: None,
            stores: Vec::new(),
            stale: None,
            runtime_baseline: 0,
        };
        let observed = AssertUnwindSafe(timeout(Duration::from_secs(30), case.observe()))
            .catch_unwind()
            .await;
        let cleanup = case.cleanup().await;
        if !cleanup.is_empty() {
            eprintln!("PostgreSQL cleanup receipt fixture cleanup: {cleanup:?}");
        }
        let result = match observed {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                panic!("PostgreSQL receipt observation timed out: {error}; cleanup={cleanup:?}")
            }
            Err(panic) => std::panic::resume_unwind(panic),
        };
        let (primary, rollback) = result;
        assert!(
            matches!(&primary, StateStoreError::TransactionConflict { operation, .. } if operation == "load"),
            "{primary:?}"
        );
        match receipt {
            RollbackReceipt::Forward => assert_eq!(rollback, Ok(()), "confirmed native rollback"),
            RollbackReceipt::Lose => assert!(
                matches!(&rollback, Err(StateStoreError::Unavailable { operation, .. }) if operation == "rollback"),
                "lost cleanup acknowledgment must remain unknown; primary={primary:?}, rollback={rollback:?}"
            ),
        }
        assert!(cleanup.is_empty(), "{cleanup:?}");
    }

    async fn observe(&mut self) -> (StateStoreError, Result<(), StateStoreError>) {
        let (client, connection) = self
            .case
            .native_config
            .connect(tokio_postgres::NoTls)
            .await
            .expect("native observer");
        self.client = Some(client);
        self.driver = Some(tokio::spawn(connection));
        self.runtime_baseline = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        self.relay = Some(
            RollbackRelay::start(
                self.case.config.host().to_owned(),
                self.case.config.port(),
                self.receipt,
            )
            .await
            .expect("native relay"),
        );
        let relay = self.relay.as_ref().expect("relay");
        let mut config = PostgresStateStoreConfig::new(
            "127.0.0.1",
            self.case.config.database(),
            self.case.config.username(),
        )
        .with_port(relay.address.port());
        if let Some(password) = self.case.config.password() {
            config = config.with_password(password.clone());
        }
        for _ in 0..2 {
            self.stores.push(
                StateStore::connect(StateStoreConfig::postgres(config.clone()))
                    .await
                    .expect("public PostgreSQL store through native relay"),
            );
        }
        let head = self.case.key("head");
        let anchor = self.case.key("snapshot-anchor");
        let losing = self.case.key("losing-write");
        let mut seed = self.stores[0].begin_transaction().await.expect("seed");
        seed.put(StateRecord::new(head.clone(), serde_json::json!(0)))
            .await
            .expect("seed head");
        seed.put(StateRecord::new(anchor.clone(), serde_json::json!(0)))
            .await
            .expect("seed anchor");
        seed.commit().await.expect("seed commit");
        self.stale = Some(self.stores[1].begin_transaction().await.expect("stale"));
        let stale = self.stale.as_mut().expect("stale");
        stale
            .load(anchor)
            .await
            .expect("establish old native snapshot")
            .expect("anchor");
        stale
            .put(StateRecord::new(losing.clone(), serde_json::json!(true)))
            .await
            .expect("stage losing write");
        let mut winner = self.stores[0].begin_transaction().await.expect("winner");
        winner
            .put(StateRecord::new(head.clone(), serde_json::json!(1)))
            .await
            .expect("winner write");
        winner.commit().await.expect("winner commit");
        let primary = stale
            .load(head.clone())
            .await
            .expect_err("real PostgreSQL serialization conflict");
        let rollback = self.stale.take().expect("stale").rollback().await;
        assert!(
            self.stores[0]
                .load(losing)
                .await
                .expect("losing write readback")
                .is_none()
        );
        assert_eq!(
            self.stores[0]
                .load(head)
                .await
                .expect("winner readback")
                .expect("winner")
                .record
                .value
                .as_ref(),
            &serde_json::json!(1)
        );
        let evidence = self
            .relay
            .as_ref()
            .expect("relay")
            .evidence
            .lock()
            .expect("relay evidence");
        assert_eq!(evidence.conflicts, 1, "actual SQLSTATE 40001");
        assert_eq!(evidence.rollbacks, 1, "actual native ROLLBACK completion");
        assert_eq!(
            evidence.lost_receipts,
            usize::from(matches!(self.receipt, RollbackReceipt::Lose))
        );
        assert!(
            !evidence.backend_pids.is_empty(),
            "real native connection identities"
        );
        (primary, rollback)
    }

    async fn cleanup(&mut self) -> Vec<String> {
        self.stale.take();
        let initialized = !self.stores.is_empty();
        self.stores.clear();
        let mut errors = Vec::new();
        let pids = if let Some(relay) = self.relay.as_mut() {
            errors.extend(relay.finish().await);
            relay
                .evidence
                .lock()
                .expect("relay evidence")
                .backend_pids
                .iter()
                .copied()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        self.relay.take();
        if let Some(client) = &self.client {
            let deadline = Instant::now() + Duration::from_secs(5);
            let absent = timeout_at(deadline, async {
                loop {
                    let count: i64 = client
                        .query_one(
                            "SELECT count(*) FROM pg_stat_activity WHERE pid = ANY($1)",
                            &[&pids],
                        )
                        .await?
                        .get(0);
                    if count == 0 {
                        return Ok::<(), tokio_postgres::Error>(());
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            if !matches!(absent, Ok(Ok(()))) {
                errors.push(format!("relay backend sessions did not settle: {absent:?}"));
            }
            if initialized {
                let removed = timeout(
                    Duration::from_secs(5),
                    client.execute(
                        "DELETE FROM storage_sdk_state_records WHERE namespace = $1",
                        &[&self.case.namespace],
                    ),
                )
                .await;
                if !matches!(removed, Ok(Ok(_))) {
                    errors.push(format!("owned namespace cleanup: {removed:?}"));
                }
            }
            // Relative task quiescence is supplementary lifetime evidence,
            // not a claim that the test possesses private actor JoinHandles.
            let quiet = timeout(Duration::from_secs(5), async {
                while tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks()
                    > self.runtime_baseline
                {
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            if quiet.is_err() {
                errors.push(
                    "public store tasks did not return to the stable observer baseline".to_owned(),
                );
            }
        }
        self.client.take();
        if let Some(driver) = self.driver.as_mut() {
            match timeout(Duration::from_secs(5), &mut *driver).await {
                Ok(Ok(Ok(()))) => {
                    self.driver.take();
                }
                Ok(result) => {
                    errors.push(format!("native observer driver: {result:?}"));
                    self.driver.take();
                }
                Err(_) => {
                    errors.push("native observer driver cleanup deadline".to_owned());
                    driver.abort();
                    match timeout(Duration::from_secs(5), &mut *driver).await {
                        Ok(Err(error)) if error.is_cancelled() => {
                            self.driver.take();
                        }
                        Ok(result) => {
                            errors.push(format!("native observer fallback: {result:?}"));
                            self.driver.take();
                        }
                        Err(_) => errors
                            .push("native observer fallback join remains unconfirmed".to_owned()),
                    }
                }
            }
        }
        errors
    }
}

impl Drop for CleanupReceiptCase {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}

#[tokio::test]
async fn postgres_conflict_preserves_confirmed_cleanup_receipt() {
    CleanupReceiptCase::run(RollbackReceipt::Forward).await;
}

#[tokio::test]
async fn postgres_conflict_preserves_lost_cleanup_receipt() {
    CleanupReceiptCase::run(RollbackReceipt::Lose).await;
}
