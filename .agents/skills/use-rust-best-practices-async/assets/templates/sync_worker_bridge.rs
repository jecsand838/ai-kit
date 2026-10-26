use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    thread::{self, JoinHandle},
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerCommand>,
    thread: Option<JoinHandle<()>>,
    enqueue_timeout: Duration,
    read_timeout: Duration,
}

impl WorkerHandle {
    pub fn start(
        queue_capacity: usize,
        enqueue_timeout: Duration,
        read_timeout: Duration,
    ) -> Result<Self, WorkerError> {
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let engine = SyncEngine::open()?;
        let thread = thread::Builder::new()
            .name("sync-worker".to_owned())
            .spawn(move || Worker::new(engine, receiver).run())
            .map_err(WorkerError::spawn_failed)?;

        Ok(Self {
            sender,
            thread: Some(thread),
            enqueue_timeout,
            read_timeout,
        })
    }

    pub async fn read(&self, key: String) -> Result<Option<String>, WorkerError> {
        let (reply, result) = oneshot::channel();
        self.send_with_timeout(WorkerCommand::Read { key, reply })
            .await?;

        time::timeout(self.read_timeout, result)
            .await
            .map_err(|_| WorkerError::timed_out("read"))?
            .map_err(|_| WorkerError::closed())?
    }

    pub async fn write(&self, key: String, value: String) -> Result<(), WorkerError> {
        let (reply, result) = oneshot::channel();
        self.send_with_timeout(WorkerCommand::Write { key, value, reply })
            .await?;

        // After a side-effectful operation is queued, wait for its definitive
        // result instead of returning an ambiguous timeout while it may commit.
        result.await.map_err(|_| WorkerError::closed())?
    }

    pub async fn shutdown(mut self) -> Result<(), WorkerError> {
        let _ = self.send_with_timeout(WorkerCommand::Shutdown).await;
        drop(self.sender);

        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| WorkerError::panicked())?;
        }

        Ok(())
    }

    async fn send_with_timeout(&self, command: WorkerCommand) -> Result<(), WorkerError> {
        let permit = time::timeout(self.enqueue_timeout, self.sender.reserve())
            .await
            .map_err(|_| WorkerError::timed_out("enqueue"))?
            .map_err(|_| WorkerError::closed())?;
        permit.send(command);
        Ok(())
    }
}

enum WorkerCommand {
    Read {
        key: String,
        reply: oneshot::Sender<Result<Option<String>, WorkerError>>,
    },
    Write {
        key: String,
        value: String,
        reply: oneshot::Sender<Result<(), WorkerError>>,
    },
    Shutdown,
}

impl WorkerCommand {
    fn execute(self, engine: &mut SyncEngine) -> bool {
        match self {
            Self::Read { key, reply } => {
                let _ = reply.send(engine.read(&key));
                true
            }
            Self::Write { key, value, reply } => {
                let _ = reply.send(engine.write(key, value));
                true
            }
            Self::Shutdown => false,
        }
    }
}

struct Worker {
    engine: SyncEngine,
    receiver: mpsc::Receiver<WorkerCommand>,
}

impl Worker {
    fn new(engine: SyncEngine, receiver: mpsc::Receiver<WorkerCommand>) -> Self {
        Self { engine, receiver }
    }

    fn run(mut self) {
        while let Some(command) = self.receiver.blocking_recv() {
            if !command.execute(&mut self.engine) {
                break;
            }
        }
    }
}

struct SyncEngine {
    values: BTreeMap<String, String>,
}

impl SyncEngine {
    fn open() -> Result<Self, WorkerError> {
        Ok(Self {
            values: BTreeMap::new(),
        })
    }

    fn read(&self, key: &str) -> Result<Option<String>, WorkerError> {
        Ok(self.values.get(key).cloned())
    }

    fn write(&mut self, key: String, value: String) -> Result<(), WorkerError> {
        self.values.insert(key, value);
        Ok(())
    }
}

#[derive(Debug)]
pub struct WorkerError {
    message: String,
}

impl WorkerError {
    fn closed() -> Self {
        Self {
            message: "sync worker closed".to_owned(),
        }
    }

    fn panicked() -> Self {
        Self {
            message: "sync worker panicked".to_owned(),
        }
    }

    fn timed_out(op: &'static str) -> Self {
        Self {
            message: format!("{op} timed out before the worker accepted the request"),
        }
    }

    fn spawn_failed(error: std::io::Error) -> Self {
        Self {
            message: format!("failed to spawn sync worker: {error}"),
        }
    }
}

impl fmt::Display for WorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for WorkerError {}
