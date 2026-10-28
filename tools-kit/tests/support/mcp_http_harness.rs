use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::{Request, State},
    http::{Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tools_kit::{
    Toolset,
    mcp::{McpClientConfig, McpConnectionConfig},
};

use super::repo_managed_mcp_service::RepoManagedMcpServer;

const AUTH_TOKEN: &str = "tools-kit-mcp-secret";
const CUSTOM_HEADER_NAME: &str = "x-tools-kit-proof";
const CUSTOM_HEADER_VALUE: &str = "strict-live";

#[derive(Clone, Copy, Debug, Default)]
pub struct HttpServerOptions {
    pub fail_list_tools: bool,
    pub extra_echo_tools: usize,
    pub stall_delete_session: bool,
}

pub struct HttpServerHandle {
    endpoint: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl HttpServerHandle {
    pub async fn spawn() -> Self {
        Self::spawn_with_options(HttpServerOptions::default()).await
    }

    pub async fn spawn_with_options(options: HttpServerOptions) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("repo-managed streamable HTTP MCP listener should bind");
        let address = listener
            .local_addr()
            .expect("repo-managed streamable HTTP MCP listener address should be readable");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let router = streamable_http_router(options);

        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(wait_for_http_shutdown(shutdown_rx))
                .await
                .expect("repo-managed streamable HTTP MCP server should run");
        });

        Self {
            endpoint: format!("http://{address}/mcp"),
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
        }
    }

    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(shutdown) = self.shutdown_tx.take() {
            let _receiver_closed = shutdown.send(());
        }
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(result) => result.map_err(|error| format!("native MCP fixture join: {error}")),
            Err(_) => {
                task.abort();
                let terminal = task.await;
                Err(format!(
                    "native MCP fixture cleanup deadline; aborted and joined: {terminal:?}"
                ))
            }
        }
    }

    pub fn source(&self, namespace: Option<&str>) -> McpClientConfig {
        let config = McpClientConfig::new(McpConnectionConfig::StreamableHttp {
            endpoint: self
                .endpoint
                .parse()
                .expect("repo-managed MCP endpoint should parse"),
            headers: BTreeMap::from([
                ("Authorization".to_owned(), format!("Bearer {}", AUTH_TOKEN)),
                (
                    CUSTOM_HEADER_NAME.to_owned(),
                    CUSTOM_HEADER_VALUE.to_owned(),
                ),
            ]),
        })
        .with_request_timeout(Duration::from_secs(2))
        .expect("test timeout should validate");
        match namespace {
            Some(namespace) => config
                .with_namespace(namespace)
                .expect("test namespace should validate"),
            None => config,
        }
    }

    pub async fn runtime(&self, namespace: Option<&str>) -> Toolset {
        Toolset::mcp(self.source(namespace))
            .await
            .expect("repo-managed streamable HTTP MCP runtime should build")
    }
}

impl Drop for HttpServerHandle {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            request_http_shutdown(shutdown_tx);
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn wait_for_http_shutdown(shutdown: oneshot::Receiver<()>) {
    if shutdown.await.is_err() {
        // Dropping the repo-managed MCP server owner is also a shutdown path.
    }
}

fn request_http_shutdown(shutdown: oneshot::Sender<()>) {
    if shutdown.send(()).is_err() {
        // The repo-managed MCP server task already exited.
    }
}

fn streamable_http_router(options: HttpServerOptions) -> Router {
    let service: StreamableHttpService<RepoManagedMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || {
                Ok(RepoManagedMcpServer::new(
                    None,
                    options.fail_list_tools,
                    None,
                    options.extra_echo_tools,
                ))
            },
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default(),
        );

    Router::new()
        .route("/healthz", axum::routing::get(|| async { StatusCode::OK }))
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(
            HttpMiddlewareState {
                auth: HttpAuthConfig {
                    token: AUTH_TOKEN.to_owned(),
                    custom_header: Some((
                        CUSTOM_HEADER_NAME.to_owned(),
                        CUSTOM_HEADER_VALUE.to_owned(),
                    )),
                },
                stall_delete_session: options.stall_delete_session,
            },
            enforce_headers,
        ))
}

#[derive(Clone, Debug)]
struct HttpAuthConfig {
    token: String,
    custom_header: Option<(String, String)>,
}

#[derive(Clone, Debug)]
struct HttpMiddlewareState {
    auth: HttpAuthConfig,
    stall_delete_session: bool,
}

async fn enforce_headers(
    State(state): State<HttpMiddlewareState>,
    request: Request,
    next: Next,
) -> Response {
    if request.uri().path() == "/healthz" {
        return next.run(request).await;
    }

    if state.stall_delete_session && request.method() == Method::DELETE {
        return std::future::pending().await;
    }

    let authorization = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let expected_authorization = format!("Bearer {}", state.auth.token);
    if authorization != Some(expected_authorization.as_str()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    if let Some((name, value)) = &state.auth.custom_header {
        let observed = request
            .headers()
            .get(name.as_str())
            .and_then(|value| value.to_str().ok());
        if observed != Some(value.as_str()) {
            return StatusCode::BAD_REQUEST.into_response();
        }
    }

    next.run(request).await
}
