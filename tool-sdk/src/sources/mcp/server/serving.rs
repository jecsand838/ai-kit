use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::Duration,
};

use rmcp::ErrorData as McpError;
use tokio::{
    sync::{Notify, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::{self, Instant},
};

use crate::ToolError;

const REQUEST_CAPACITY: usize = 64;
const SESSION_CAPACITY: usize = 64;

#[derive(Clone)]
enum ServingState {
    Open,
    Closing { deadline: Instant },
    Settled(Result<(), ToolError>),
}

/// Owns admitted caller work independently of protocol and shutdown observers.
pub(super) struct Serving {
    state: watch::Sender<ServingState>,
    requests: Mutex<JoinSet<()>>,
    sessions: Mutex<JoinSet<()>>,
    protocol_retired: AtomicBool,
    failure: Mutex<Option<ToolError>>,
    changed: Notify,
    supervisor: Mutex<Option<JoinHandle<Result<(), ToolError>>>>,
    observed: AtomicBool,
    timeout: Duration,
}

impl Serving {
    pub(super) fn new(timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            state: watch::channel(ServingState::Open).0,
            requests: Mutex::new(JoinSet::new()),
            sessions: Mutex::new(JoinSet::new()),
            protocol_retired: AtomicBool::new(false),
            failure: Mutex::new(None),
            changed: Notify::new(),
            supervisor: Mutex::new(None),
            observed: AtomicBool::new(true),
            timeout,
        })
    }

    pub(super) fn start<F>(self: &Arc<Self>, endpoint: F)
    where
        F: Future<Output = std::io::Result<()>> + Send + 'static,
    {
        let owner = Arc::clone(self);
        // Install the JoinHandle before its future may publish completion.
        let (activate, activation) = oneshot::channel();
        let task = tokio::spawn(async move {
            if activation.await.is_err() {
                owner.record(ToolError::Transport {
                    tool: None,
                    message: "MCP server activation was lost".to_owned(),
                });
                owner.seal();
            } else {
                owner.drive(endpoint).await;
            }
            owner.finish()
        });
        *self
            .supervisor
            .lock()
            .expect("server task registration never polls caller code") = Some(task);
        let _runtime_still_running = activate.send(()).is_ok();
    }

    pub(super) fn admit<R, F>(&self, operation: F) -> Result<oneshot::Receiver<R>, McpError>
    where
        R: Send + 'static,
        F: Future<Output = R> + Send + 'static,
    {
        // Keep the read guard through insertion: seal needs the corresponding
        // write guard, so admission cannot pass its check and spawn after seal.
        let state = self.state.borrow();
        if !matches!(*state, ServingState::Open) {
            return Err(McpError::internal_error("MCP server is closing", None));
        }
        let mut requests = self
            .requests
            .lock()
            .expect("server admission never polls caller code");
        let mut failure = None;
        while let Some(result) = requests.try_join_next() {
            if let Err(error) = result
                && failure.is_none()
            {
                failure = Some(ToolError::Transport {
                    tool: None,
                    message: format!("MCP server accepted request task failed: {error}"),
                });
            }
        }
        let observation = if requests.len() >= REQUEST_CAPACITY {
            Err(McpError::internal_error(
                "MCP server request capacity is exhausted",
                None,
            ))
        } else {
            let (reply, observation) = oneshot::channel();
            requests.spawn(async move {
                let result = operation.await;
                // The operation, including its unique handler, has settled before
                // publishing a response. Losing this observer cannot drop the work.
                let _observed = reply.send(result).is_ok();
            });
            Ok(observation)
        };
        drop(requests);
        if let Some(error) = failure {
            self.record(error);
        }
        drop(state);
        self.changed.notify_one();
        observation
    }

    pub(super) fn admit_session<F>(&self, operation: F) -> Result<(), std::io::Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let state = self.state.borrow();
        if !matches!(*state, ServingState::Open) {
            return Err(std::io::Error::other("MCP server is closing"));
        }
        let mut sessions = self
            .sessions
            .lock()
            .expect("native session admission never polls caller code");
        while let Some(result) = sessions.try_join_next() {
            if let Err(error) = result {
                self.record(ToolError::Transport {
                    tool: None,
                    message: format!("MCP native session settlement task failed: {error}"),
                });
            }
        }
        if sessions.len() >= SESSION_CAPACITY {
            return Err(std::io::Error::other(
                "MCP server session capacity is exhausted",
            ));
        }
        sessions.spawn(operation);
        drop(sessions);
        drop(state);
        self.changed.notify_one();
        Ok(())
    }

    pub(super) fn protocol_retired(&self) {
        self.protocol_retired.store(true, Ordering::SeqCst);
        self.changed.notify_one();
    }

    pub(super) fn closing(&self) -> bool {
        !matches!(*self.state.borrow(), ServingState::Open)
    }

    pub(super) async fn stopped(&self) {
        let mut state = self.state.subscribe();
        let _stopped = state
            .wait_for(|state| !matches!(state, ServingState::Open))
            .await;
    }

    fn seal(&self) {
        self.state.send_if_modified(|state| {
            if matches!(state, ServingState::Open) {
                *state = ServingState::Closing {
                    deadline: Instant::now() + self.timeout,
                };
                true
            } else {
                false
            }
        });
        self.changed.notify_one();
    }

    pub(super) fn record(&self, error: ToolError) {
        let mut failure = self
            .failure
            .lock()
            .expect("server failure recording never polls caller code");
        // Keep the first failure authoritative and retention bounded. Individual
        // calls still receive their own complete primary/cleanup diagnostics.
        if failure.is_none() {
            *failure = Some(error);
        }
    }

    async fn drive<F>(&self, endpoint: F)
    where
        F: Future<Output = std::io::Result<()>>,
    {
        // Concrete boxing is only to drop a completed endpoint before the loop
        // waits for native manager-reference retirement. No future erasure.
        let mut endpoint = Some(Box::pin(endpoint));
        loop {
            let requests_empty = self
                .requests
                .lock()
                .expect("server completion check does not poll caller code")
                .is_empty();
            let sessions_empty = self
                .sessions
                .lock()
                .expect("server completion check does not poll caller code")
                .is_empty();
            if endpoint.is_none()
                && requests_empty
                && sessions_empty
                && self.protocol_retired.load(Ordering::SeqCst)
            {
                break;
            }
            tokio::select! {
                result = async {
                    endpoint.as_mut().expect("guarded endpoint future").as_mut().await
                }, if endpoint.is_some() => {
                    // Drop Router/StreamableHttpService/factory references now.
                    // Keeping the completed future here would retain Sessions
                    // while this loop waits for Protocol retirement: a real deadlock.
                    endpoint.take();
                    if let Err(error) = result {
                        self.record(ToolError::Transport {
                            tool: None,
                            message: format!("MCP server endpoint failed: {error}"),
                        });
                    }
                    self.seal();
                }
                result = poll_fn(|context| {
                    let mut requests = self.requests.lock()
                        .expect("server reaping never polls caller code");
                    match requests.poll_join_next(context) {
                        Poll::Ready(None) => Poll::Pending,
                        Poll::Ready(Some(result)) => Poll::Ready(result),
                        Poll::Pending => Poll::Pending,
                    }
                }) => {
                    if let Err(error) = result {
                        self.record(ToolError::Transport {
                            tool: None,
                            message: format!("MCP server accepted request task failed: {error}"),
                        });
                    }
                }
                result = poll_fn(|context| {
                    let mut sessions = self.sessions.lock()
                        .expect("native session reaping never polls caller code");
                    match sessions.poll_join_next(context) {
                        Poll::Ready(None) => Poll::Pending,
                        Poll::Ready(Some(result)) => Poll::Ready(result),
                        Poll::Pending => Poll::Pending,
                    }
                }) => {
                    if let Err(error) = result {
                        self.record(ToolError::Transport {
                            tool: None,
                            message: format!("MCP native session settlement task failed: {error}"),
                        });
                    }
                }
                _ = self.changed.notified() => {}
            }
        }
    }

    fn finish(&self) -> Result<(), ToolError> {
        let result = self
            .failure
            .lock()
            .expect("server completion never polls caller code")
            .clone()
            .map_or(Ok(()), Err);
        self.state
            .send_replace(ServingState::Settled(result.clone()));
        if !self.observed.load(Ordering::SeqCst) {
            // No observer remains to join this final return. All owned children
            // have already settled, and there is no await after this handoff.
            self.supervisor
                .lock()
                .expect("server completion never polls caller code")
                .take();
        }
        result
    }

    pub(super) async fn shutdown(&self) -> Result<(), ToolError> {
        self.seal();
        let deadline = match &*self.state.borrow() {
            ServingState::Open => unreachable!("seal changes the state synchronously"),
            ServingState::Closing { deadline } => *deadline,
            ServingState::Settled(_) => Instant::now() + self.timeout,
        };
        match time::timeout_at(deadline, self.join()).await {
            Ok(result) => result,
            Err(_) => {
                let primary = self
                    .failure
                    .lock()
                    .expect("server timeout never polls caller code")
                    .as_ref()
                    .map(|error| format!("{error}; "))
                    .unwrap_or_default();
                Err(ToolError::Transport {
                    tool: None,
                    message: format!(
                        "{primary}MCP server shutdown deadline elapsed; accepted cleanup remains owned"
                    ),
                })
            }
        }
    }

    async fn join(&self) -> Result<(), ToolError> {
        poll_fn(|context| {
            let mut supervisor = self
                .supervisor
                .lock()
                .expect("server observation never polls caller code");
            let Some(task) = supervisor.as_mut() else {
                return Poll::Ready(match &*self.state.borrow() {
                    ServingState::Settled(result) => result.clone(),
                    ServingState::Open | ServingState::Closing { .. } => {
                        Err(ToolError::Transport {
                            tool: None,
                            message: "MCP server supervisor is unavailable".to_owned(),
                        })
                    }
                });
            };
            match Pin::new(task).poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    supervisor.take();
                    Poll::Ready(result.unwrap_or_else(|error| {
                        Err(ToolError::Transport {
                            tool: None,
                            message: format!("MCP server supervisor failed: {error}"),
                        })
                    }))
                }
            }
        })
        .await
    }

    pub(super) fn release_observer(&self) {
        self.seal();
        self.observed.store(false, Ordering::SeqCst);
        if matches!(*self.state.borrow(), ServingState::Settled(_)) {
            self.supervisor
                .lock()
                .expect("server observer release never polls caller code")
                .take();
        }
    }
}
