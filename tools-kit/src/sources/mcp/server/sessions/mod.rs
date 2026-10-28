use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_core::Stream;
use rmcp::{
    RoleServer,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage, ServerResult},
    transport::{
        Transport, WorkerTransport,
        streamable_http_server::session::{
            ServerSseMessage, SessionId, SessionManager,
            local::{LocalSessionManager, LocalSessionWorker, SessionError},
        },
    },
};
use tokio::sync::{oneshot, watch};

use super::serving::Serving;

/// The native HTTP service and its outer session tasks are the ONLY strong
/// owners of this manager. Serving and native settlers must never retain it.
pub(super) struct Sessions {
    native: Arc<LocalSessionManager>,
    serving: Arc<Serving>,
    entries: Arc<Mutex<HashMap<SessionId, Arc<Session>>>>,
    _protocol: Arc<Protocol>,
}

impl Sessions {
    pub(super) fn new(serving: Arc<Serving>, protocol: Arc<Protocol>) -> Self {
        Self {
            native: Arc::new(LocalSessionManager::default()),
            serving,
            entries: Arc::new(Mutex::new(HashMap::new())),
            _protocol: protocol,
        }
    }
}

/// Shared only by the native manager and every concrete server handler clone.
/// The final field of each owner drops after its Toolset/native references.
/// Neither Serving nor native settlement tasks hold this lifetime token.
pub(super) struct Protocol {
    serving: Arc<Serving>,
}

impl Protocol {
    pub(super) fn new(serving: Arc<Serving>) -> Self {
        Self { serving }
    }
}

impl Drop for Protocol {
    fn drop(&mut self) {
        // Final reference retirement, NOT possession of rmcp's outer task handle.
        self.serving.protocol_retired();
    }
}

impl SessionManager for Sessions {
    type Error = io::Error;
    type Transport = SessionTransport;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        let (activate, activation) = oneshot::channel::<(
            Arc<Session>,
            oneshot::Receiver<WorkerTransport<LocalSessionWorker>>,
        )>();
        let native = Arc::clone(&self.native);
        let entries = Arc::clone(&self.entries);
        let serving = Arc::clone(&self.serving);

        // Atomic sealed/capacity check + JoinSet insertion happens before any
        // native creation. Losing this request while native creation is pending
        // closes activation; its already-owned empty settler then terminates.
        self.serving.admit_session(async move {
            if let Ok((session, returned)) = activation.await {
                session.settle(native, &serving, returned).await;
                entries
                    .lock()
                    .expect("session removal does not poll caller code")
                    .remove(&session.id);
            }
        })?;

        let (id, transport) = self
            .native
            .create_session()
            .await
            .map_err(io::Error::other)?;
        // Pinned native create_session has no await between spawning its worker
        // and returning. This synchronous tail installs its return owner before
        // this outer future can next yield or be dropped.
        let session = Arc::new(Session {
            id: id.clone(),
            closing: watch::channel(false).0,
            result: watch::channel(None).0,
            negotiated: AtomicBool::new(false),
            failure: Mutex::new(None),
        });
        let (return_transport, returned) = oneshot::channel();
        self.entries
            .lock()
            .expect("session admission does not poll caller code")
            .insert(id.clone(), Arc::clone(&session));
        // The owned settler cannot finish or be aborted before activation;
        // Serving never aborts its native JoinSet, including on observer timeout.
        activate
            .send((Arc::clone(&session), returned))
            .unwrap_or_else(|_| panic!("registered session settlement owner terminated"));
        Ok((
            id,
            SessionTransport {
                session,
                state: TransportState::Open {
                    transport,
                    return_transport,
                },
            },
        ))
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        // Native LocalSessionManager keeps a map read lock while awaiting the
        // handshake response. Clone the public native handle under the lock,
        // then release it before awaiting, so removal can wake initial receives.
        let handle = self
            .native
            .sessions
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "MCP session not found"))?;
        handle.initialize(message).await.map_err(io::Error::other)
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        self.native.has_session(id).await.map_err(io::Error::other)
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        let session = self
            .entries
            .lock()
            .expect("session lookup does not poll caller code")
            .get(id)
            .cloned();
        if let Some(session) = session {
            session.closing.send_replace(true);
            session.completed().await.map_err(io::Error::other)?;
        }
        // Native LocalSessionManager also treats an absent session as closed.
        Ok(())
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.native
            .create_stream(id, message)
            .await
            .map_err(io::Error::other)
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.native
            .accept_message(id, message)
            .await
            .map_err(io::Error::other)
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.native
            .create_standalone_stream(id)
            .await
            .map_err(io::Error::other)
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.native
            .resume(id, last_event_id)
            .await
            .map_err(io::Error::other)
    }
}

/// One native session's close request and terminal receipt. The exact native
/// transport remains unique and moves into its pre-reserved settler once.
struct Session {
    id: SessionId,
    closing: watch::Sender<bool>,
    result: watch::Sender<Option<Result<(), String>>>,
    negotiated: AtomicBool,
    failure: Mutex<Option<String>>,
}

impl Session {
    async fn completed(&self) -> Result<(), String> {
        let mut result = self.result.subscribe();
        let result = result
            .wait_for(Option::is_some)
            .await
            .map_err(|_| "native session settlement receipt was lost".to_owned())?;
        result
            .as_ref()
            .expect("wait_for required a receipt")
            .clone()
    }

    async fn settle(
        &self,
        native: Arc<LocalSessionManager>,
        serving: &Serving,
        mut returned: oneshot::Receiver<WorkerTransport<LocalSessionWorker>>,
    ) {
        let mut closing = self.closing.subscribe();
        let early = tokio::select! {
            transport = &mut returned => Some(transport),
            _ = serving.stopped() => None,
            _ = closing.wait_for(|closing| *closing) => None,
        };
        // Remove and wake before waiting for transport return/close. In the
        // rejected-first-request path the worker has not reached its ct select.
        let handle = native.sessions.write().await.remove(&self.id);
        let close = match handle {
            Some(handle) => handle.close().await,
            None => Ok(()),
        };
        let returned = match early {
            Some(returned) => returned,
            None => returned.await,
        };
        let joined = match returned {
            Ok(mut transport) => transport.close().await.map_err(|error| error.to_string()),
            Err(_) => Err("native session transport return owner was lost".to_owned()),
        };
        // SessionServiceTerminated alone says Close could not be queued because
        // the target already ended. It is accepted ONLY after the exact worker
        // has actually joined successfully; a failed join still fails cleanup.
        // Other close failures remain visible and never skip the join above.
        let result = match (close, joined) {
            (Ok(()) | Err(SessionError::SessionServiceTerminated), Ok(())) => Ok(()),
            (Err(close), Ok(())) => Err(format!("native session close failed: {close}")),
            (Ok(()), Err(join)) => Err(join),
            (Err(close), Err(join)) => Err(format!(
                "native session close failed: {close}; join failed: {join}"
            )),
        };
        let result = match self
            .failure
            .lock()
            .expect("session diagnostic observation does not poll caller code")
            .take()
        {
            Some(primary) => match result {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(format!("{primary}; cleanup failed: {cleanup}")),
            },
            None => result,
        };
        if let Err(error) = &result {
            serving.record(crate::ToolError::Transport {
                tool: None,
                message: error.clone(),
            });
        }
        self.result.send_replace(Some(result));
        // No owned transport or native close future survives this receipt.
    }
}

/// Faithful transport adaptation plus cancellation-safe transfer on close/Drop.
/// No private native messages, WorkerContext construction or early receive EOF.
pub(super) struct SessionTransport {
    session: Arc<Session>,
    state: TransportState,
}

enum TransportState {
    Open {
        transport: WorkerTransport<LocalSessionWorker>,
        return_transport: oneshot::Sender<WorkerTransport<LocalSessionWorker>>,
    },
    Returned,
}

impl SessionTransport {
    fn release(&mut self) {
        if let TransportState::Open {
            transport,
            return_transport,
        } = std::mem::replace(&mut self.state, TransportState::Returned)
        {
            return_transport
                .send(transport)
                .unwrap_or_else(|_| panic!("registered native transport owner terminated"));
        }
    }
}

impl Drop for SessionTransport {
    fn drop(&mut self) {
        if matches!(self.state, TransportState::Open { .. })
            && self.session.negotiated.load(Ordering::SeqCst)
        {
            *self
                .session
                .failure
                .lock()
                .expect("session retirement does not poll caller code") = Some(
                "MCP native service stopped without closing its accepted transport".to_owned(),
            );
        }
        // No spawn, blocking wait, or fallible bounded-queue admission in Drop.
        self.release();
    }
}

impl Transport<RoleServer> for SessionTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        message: ServerJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        // Native send returns a 'static future, so no guard/transport borrow is
        // held across caller work. Send after close remains an honest error.
        let initialization = matches!(&message,
            ServerJsonRpcMessage::Response(response)
                if matches!(&response.result, ServerResult::InitializeResult(_)));
        let session = Arc::clone(&self.session);
        let operation = match &mut self.state {
            TransportState::Open { transport, .. } => Some(transport.send(message)),
            TransportState::Returned => None,
        };
        async move {
            match operation {
                Some(operation) => {
                    operation.await.map_err(io::Error::other)?;
                    if initialization {
                        // No await separates native successful init send from
                        // entering serve_inner in the pinned server adapter.
                        session.negotiated.store(true, Ordering::SeqCst);
                    }
                    Ok(())
                }
                None => Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "MCP session is closed",
                )),
            }
        }
    }

    async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
        match &mut self.state {
            TransportState::Open { transport, .. } => transport.receive().await,
            TransportState::Returned => None,
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.release();
        // Dropping this observer cannot drop native close/join: those futures
        // are polled exclusively in Serving's pre-reserved native JoinSet task.
        self.session.completed().await.map_err(io::Error::other)
    }
}
