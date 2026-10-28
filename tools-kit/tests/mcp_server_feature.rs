#![cfg(feature = "mcp-server")]

mod server_capacity_regressions;
mod server_regressions;
mod server_startup_regressions;
pub mod support;

use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

use rmcp::{
    ServiceExt,
    model::{CallToolRequestParams, ErrorCode, PaginatedRequestParams},
    service::ServiceError,
    transport::{
        StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tools_kit::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::{ToolInfo, ToolName},
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{
        ToolInteractionId, ToolInteractionKind, ToolInteractionRequest, ToolInteractionResponse,
    },
    local::ToolRegistration,
    mcp::{
        McpClientConfig, McpConnectionConfig, McpServer, McpServerConfig,
        McpServerInteractionPolicy,
    },
    schema::JsonSchema,
};

#[derive(Deserialize, schemars::JsonSchema)]
struct EchoArgs {
    message: String,
}

#[derive(Serialize, schemars::JsonSchema)]
struct EchoOutput {
    echoed: String,
}

#[tokio::test]
async fn independent_server_serves_any_toolset_without_owning_its_shutdown() {
    let toolset = Toolset::local(vec![
        ToolRegistration::typed("echo", |args: EchoArgs| async move {
            Ok(EchoOutput {
                echoed: args.message,
            })
        })
        .expect("typed registration should validate"),
    ])
    .expect("local toolset should build");
    let bind = free_loopback_addr().await;
    let server = McpServer::start(Arc::new(toolset.clone()), server_config(bind))
        .await
        .expect("MCP server should start eagerly");
    let downstream = downstream_toolset(bind, false).await;

    let tools = downstream
        .list_tools()
        .await
        .expect("downstream inventory should load");
    assert!(
        tools
            .iter()
            .any(|tool| tool.name.as_str() == "served__echo")
    );
    let result = support::invoke_tool(
        &downstream,
        "served__echo",
        Some("call-served"),
        json!({ "message": "hello" }),
    )
    .await
    .expect("served echo should execute");
    assert_eq!(
        result.structured_content,
        Some(json!({ "echoed": "hello" }))
    );

    server.shutdown().await.expect("server should join cleanly");
    assert_downstream_cannot_connect(bind).await;
    assert_eq!(
        toolset
            .list_tools()
            .await
            .expect("source remains open")
            .len(),
        1
    );
}

#[tokio::test]
async fn served_inventory_supports_cursor_pagination() {
    let upstream = support::mcp_http_harness::HttpServerHandle::spawn_with_options(
        support::mcp_http_harness::HttpServerOptions {
            extra_echo_tools: 70,
            ..Default::default()
        },
    )
    .await;
    let toolset = upstream.runtime(Some("repo")).await;
    let bind = free_loopback_addr().await;
    let server = McpServer::start(Arc::new(toolset), server_config(bind))
        .await
        .expect("MCP server should start");
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{bind}/mcp")),
    );
    let mut downstream = ().serve(transport).await.expect("client should connect");

    let first = downstream
        .peer()
        .list_tools(Some(PaginatedRequestParams::default()))
        .await
        .expect("first tools/list page should load");
    assert_eq!(first.tools.len(), 64);
    let cursor = first.next_cursor.expect("large inventory should paginate");
    let second = downstream
        .peer()
        .list_tools(Some(
            PaginatedRequestParams::default().with_cursor(Some(cursor)),
        ))
        .await
        .expect("second tools/list page should load");
    assert!(!second.tools.is_empty());
    assert!(second.next_cursor.is_none());

    let name = second
        .tools
        .iter()
        .find(|tool| tool.name.starts_with("repo__extra_echo_"))
        .expect("second page should contain generated tool")
        .name
        .to_string();
    let result = downstream
        .peer()
        .call_tool(CallToolRequestParams::new(name.clone()))
        .await
        .expect("paginated tool should execute");
    assert_eq!(
        result.structured_content,
        Some(json!({ "tool": name.trim_start_matches("repo__") }))
    );

    downstream.close().await.expect("client should close");
    server.shutdown().await.expect("server should join");
}

#[tokio::test]
async fn serving_preserves_model_visible_failure_results() {
    let upstream = support::mcp_http_harness::HttpServerHandle::spawn().await;
    let toolset = upstream.runtime(Some("repo")).await;
    let bind = free_loopback_addr().await;
    let server = McpServer::start(Arc::new(toolset), server_config(bind))
        .await
        .expect("MCP server should start");
    let downstream = downstream_toolset(bind, false).await;

    let result = support::invoke_tool(
        &downstream,
        "served__repo__error_metadata",
        Some("call-error"),
        json!({}),
    )
    .await
    .expect("model-visible failure remains a completed result");
    assert!(result.is_error);
    assert_eq!(
        result.metadata.get("trace_id"),
        Some(&json!("mcp-error-result"))
    );

    server.shutdown().await.expect("server should join");
}

#[tokio::test]
async fn server_maps_outer_tool_errors_to_mcp_error_codes() {
    let failing_info = ToolInfo::new(
        ToolName::new("transport_failure").unwrap(),
        JsonSchema::strict(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })),
    );
    let toolset = Toolset::local(vec![
        ToolRegistration::typed("echo", |args: EchoArgs| async move {
            Ok(EchoOutput {
                echoed: args.message,
            })
        })
        .expect("typed registration should validate"),
        ToolRegistration::function(failing_info, |_| async {
            Err(ToolError::Transport {
                tool: None,
                message: "fixture transport failed".to_owned(),
            })
        }),
    ])
    .expect("local toolset should build");
    let bind = free_loopback_addr().await;
    let server = McpServer::start(Arc::new(toolset), server_config(bind))
        .await
        .expect("MCP server should start");
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{bind}/mcp")),
    );
    let mut client = ().serve(transport).await.expect("client should connect");

    let unknown = client
        .peer()
        .call_tool(CallToolRequestParams::new("missing".to_owned()))
        .await
        .expect_err("unknown tools must be MCP request errors");
    assert!(matches!(
        unknown,
        ServiceError::McpError(error) if error.code == ErrorCode::INVALID_PARAMS
    ));

    let invalid_arguments = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("echo".to_owned()).with_arguments(
                serde_json::Map::from_iter([("message".to_owned(), json!(42))]),
            ),
        )
        .await
        .expect_err("invalid arguments must be MCP request errors");
    assert!(matches!(
        invalid_arguments,
        ServiceError::McpError(error) if error.code == ErrorCode::INVALID_PARAMS
    ));

    let transport_failure = client
        .peer()
        .call_tool(CallToolRequestParams::new("transport_failure".to_owned()))
        .await
        .expect_err("exceptional tool failures must be MCP server errors");
    assert!(matches!(
        transport_failure,
        ServiceError::McpError(error) if error.code == ErrorCode::INTERNAL_ERROR
    ));

    client.close().await.expect("client should close");
    server.shutdown().await.expect("server should join");
}

#[tokio::test]
async fn server_interaction_policy_rejects_or_elicits_without_leaking_handlers() {
    let upstream = support::mcp_http_harness::HttpServerHandle::spawn().await;
    let config = upstream.source(Some("repo")).with_form_validation(true);
    let toolset = Toolset::mcp(config)
        .await
        .expect("interaction-capable upstream should build");

    let reject_bind = free_loopback_addr().await;
    let reject = McpServer::start(Arc::new(toolset.clone()), server_config(reject_bind))
        .await
        .expect("rejecting server should start");
    let rejecting_client = downstream_toolset(reject_bind, false).await;
    let rejected = support::invoke_tool(
        &rejecting_client,
        "served__repo__form_elicitation",
        Some("reject"),
        json!({}),
    )
    .await
    .expect_err("interaction policy rejection is an outer MCP failure");
    assert!(matches!(rejected, ToolError::Protocol { .. }));
    reject
        .shutdown()
        .await
        .expect("rejecting server should join");

    let elicit_bind = free_loopback_addr().await;
    let config = server_config(elicit_bind)
        .with_interaction_policy(McpServerInteractionPolicy::ElicitFormIfClientSupports);
    let elicit = McpServer::start(Arc::new(toolset), config)
        .await
        .expect("eliciting server should start");
    let client = downstream_toolset(elicit_bind, true).await;
    let mut handler = client
        .tool_handler(
            "served__repo__form_elicitation"
                .try_into()
                .expect("tool name should parse"),
        )
        .await
        .expect("served form handler should build");
    let output = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("elicit".to_owned()),
            arguments: json!({}),
        }))
        .await
        .expect("served form should request interaction");
    let ToolHandlerOutput::Interaction(request) = output else {
        panic!("served form should emit interaction");
    };
    assert!(matches!(
        &request.kind,
        ToolInteractionKind::Form {
            schema_validation: true,
            ..
        }
    ));
    let completed = handler
        .step(ToolHandlerInput::Interaction(
            ToolInteractionResponse::submit(request.id, json!({ "choice": "yes" })),
        ))
        .await
        .expect("served form should resume");
    assert!(matches!(completed, ToolHandlerOutput::Completed(_)));
    elicit
        .shutdown()
        .await
        .expect("eliciting server should join");
}

#[tokio::test]
async fn server_config_rejects_non_listening_or_non_loopback_addresses() {
    let zero = McpServerConfig::new(
        "127.0.0.1:0".parse().expect("literal should parse"),
        "/mcp",
        "tools:",
    )
    .expect_err("port zero must fail");
    assert!(matches!(zero, ToolError::InvalidConfiguration { .. }));

    let public = McpServerConfig::new(
        "0.0.0.0:12345".parse().expect("literal should parse"),
        "/mcp",
        "tools:",
    )
    .expect_err("non-loopback bind must fail");
    assert!(matches!(public, ToolError::InvalidConfiguration { .. }));
}

#[tokio::test]
async fn cancelling_server_shutdown_aborts_the_still_owned_task() {
    let toolset = Toolset::local(vec![
        ToolRegistration::typed("echo", |args: EchoArgs| async move {
            Ok(EchoOutput {
                echoed: args.message,
            })
        })
        .expect("typed registration should validate"),
    ])
    .expect("local toolset should build");
    let bind = free_loopback_addr().await;
    let server = McpServer::start(Arc::new(toolset), server_config(bind))
        .await
        .expect("MCP server should start");
    let shutdown = tokio::spawn(server.shutdown());
    shutdown.abort();
    assert!(
        shutdown
            .await
            .expect_err("shutdown waiter should cancel")
            .is_cancelled()
    );
    assert_downstream_cannot_connect(bind).await;
}

// The backend and its unique associated handler deliberately do not implement
// Clone. Only the caller's observation state is shared.
struct ServedTools {
    inventory: Vec<ToolInfo>,
    observation: Arc<ServeObservation>,
}

#[derive(Default)]
struct ServeObservation {
    closed: AtomicBool,
    shutdowns: AtomicUsize,
    handlers_created: AtomicUsize,
    handlers_dropped: AtomicUsize,
}

struct ServedHandler {
    name: ToolName,
    state: ServedHandlerState,
    observation: Arc<ServeObservation>,
}

enum ServedHandlerState {
    New,
    Form(ToolInteractionId),
    Completed,
}

impl ServedTools {
    fn new() -> Arc<Self> {
        let inventory = ["caller_echo", "caller_form"]
            .into_iter()
            .map(|name| {
                let mut info = ToolInfo::new(
                    ToolName::new(name).expect("caller name"),
                    JsonSchema::strict(json!({"type": "object"})),
                );
                info.description = Some(format!("Direct caller {name}"));
                info.interactions.form = name == "caller_form";
                info.interactions.form_schema_validation = name == "caller_form";
                info
            })
            .collect();
        Arc::new(Self {
            inventory,
            observation: Arc::new(ServeObservation::default()),
        })
    }

    fn ensure_open(&self) -> Result<(), ToolError> {
        if self.observation.closed.load(Ordering::SeqCst) {
            Err(ToolError::Closed {
                operation: "caller admission".to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

impl ToolsetBackend for ServedTools {
    type Handler = ServedHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        self.ensure_open()?;
        Ok(self.inventory.clone())
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        self.ensure_open()?;
        if !self.inventory.iter().any(|info| info.name == name) {
            return Err(ToolError::UnknownTool { name });
        }
        self.observation
            .handlers_created
            .fetch_add(1, Ordering::SeqCst);
        Ok(ServedHandler {
            name,
            state: ServedHandlerState::New,
            observation: Arc::clone(&self.observation),
        })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        // This source has no asynchronous cleanup: admission sealing and its
        // complete cleanup occur in the same poll, and repeated calls replay.
        if !self.observation.closed.swap(true, Ordering::SeqCst) {
            self.observation.shutdowns.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

impl ToolHandlerBackend for ServedHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        let state = std::mem::replace(&mut self.state, ServedHandlerState::Completed);
        match (state, input) {
            (ServedHandlerState::New, ToolHandlerInput::Invoke(invocation)) => {
                if self.name.as_str() == "caller_form" {
                    let id = ToolInteractionId::new();
                    self.state = ServedHandlerState::Form(id.clone());
                    Ok(ToolHandlerOutput::Interaction(ToolInteractionRequest {
                        id,
                        tool_name: self.name.clone(),
                        call_id: invocation.call_id,
                        prompt: "Choose the caller value".to_owned(),
                        kind: ToolInteractionKind::Form {
                            schema: json!({
                                "type": "object",
                                "properties": {"choice": {"type": "string"}},
                                "required": ["choice"],
                                "additionalProperties": false
                            }),
                            schema_validation: true,
                        },
                    }))
                } else {
                    Ok(ToolHandlerOutput::Completed(ToolResult::json(
                        invocation.arguments,
                    )))
                }
            }
            (
                ServedHandlerState::Form(expected),
                ToolHandlerInput::Interaction(ToolInteractionResponse::Submit { id, content }),
            ) if id == expected => Ok(ToolHandlerOutput::Completed(
                ToolResult::json(content).with_error(true),
            )),
            (_, ToolHandlerInput::Cancel { reason }) => Err(ToolError::Cancelled {
                tool: self.name.clone(),
                reason,
            }),
            _ => Err(ToolError::InvalidConfiguration {
                message: "caller handler transition rejected".to_owned(),
            }),
        }
    }
}

impl Drop for ServedHandler {
    fn drop(&mut self) {
        self.observation
            .handlers_dropped
            .fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn direct_non_clone_toolset_serves_associated_handlers_over_mcp() {
    let source = ServedTools::new();
    let bind = free_loopback_addr().await;
    let server = McpServer::start(
        Arc::clone(&source),
        server_config(bind)
            .with_interaction_policy(McpServerInteractionPolicy::ElicitFormIfClientSupports),
    )
    .await
    .expect("direct caller source must serve without normalization");
    let client = downstream_toolset(bind, true).await;
    let inventory = client.list_tools().await.expect("wire inventory");
    assert_eq!(
        inventory
            .iter()
            .map(|info| info.name.as_str())
            .collect::<Vec<_>>(),
        ["served__caller_echo", "served__caller_form"]
    );
    let result = support::invoke_tool(
        &client,
        "served__caller_echo",
        Some("caller-echo"),
        json!({"message": "direct"}),
    )
    .await
    .expect("foreign associated handler executes over MCP");
    assert_eq!(
        result.structured_content,
        Some(json!({"message": "direct"}))
    );
    assert!(!result.is_error);
    let mut handler = client
        .tool_handler(ToolName::new("served__caller_form").expect("name"))
        .await
        .expect("foreign interactive handler");
    let output = handler
        .step(ToolHandlerInput::Invoke(ToolInvocation {
            call_id: Some("caller-form".to_owned()),
            arguments: json!({}),
        }))
        .await
        .expect("foreign form is elicited");
    let ToolHandlerOutput::Interaction(request) = output else {
        panic!("foreign form must retain its interaction");
    };
    assert!(matches!(
        request.kind,
        ToolInteractionKind::Form {
            schema_validation: true,
            ..
        }
    ));
    let output = handler
        .step(ToolHandlerInput::Interaction(
            ToolInteractionResponse::submit(request.id, json!({"choice": "foreign result"})),
        ))
        .await
        .expect("foreign form result remains completed");
    let ToolHandlerOutput::Completed(result) = output else {
        panic!("foreign handler must complete");
    };
    assert!(result.is_error);
    assert_eq!(
        result.structured_content,
        Some(json!({"choice": "foreign result"}))
    );
    drop(handler);
    client.shutdown().await.expect("close wire client");
    server.shutdown().await.expect("join independent server");
    assert_downstream_cannot_connect(bind).await;
    assert_eq!(
        source.observation.handlers_created.load(Ordering::SeqCst),
        2
    );
    assert_eq!(
        source.observation.handlers_dropped.load(Ordering::SeqCst),
        2
    );
    assert_eq!(source.observation.shutdowns.load(Ordering::SeqCst), 0);
    assert_eq!(
        source
            .list_tools()
            .await
            .expect("caller remains open")
            .len(),
        2
    );
    source
        .shutdown()
        .await
        .expect("caller owns source shutdown");
    source.shutdown().await.expect("caller shutdown replays");
    assert_eq!(source.observation.shutdowns.load(Ordering::SeqCst), 1);
    assert!(matches!(
        source.list_tools().await,
        Err(ToolError::Closed { .. })
    ));
}

#[tokio::test]
async fn direct_non_clone_toolset_outlives_server_drop_and_cancelled_shutdown() {
    for cancel_shutdown in [false, true] {
        let source = ServedTools::new();
        let bind = free_loopback_addr().await;
        let server = McpServer::start(Arc::clone(&source), server_config(bind))
            .await
            .expect("direct server");
        let client = downstream_toolset(bind, false).await;
        assert_eq!(client.list_tools().await.expect("inventory").len(), 2);
        if cancel_shutdown {
            let mut shutdown = Box::pin(server.shutdown());
            std::future::poll_fn(|context| {
                // This current-thread test cannot run the serving task between
                // sending shutdown and polling its join. Pending proves shutdown
                // still retains its unfinished task.
                assert!(shutdown.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(shutdown);
        } else {
            drop(server);
        }
        client.shutdown().await.expect("close client");
        assert_downstream_cannot_connect(bind).await;
        assert_eq!(source.observation.shutdowns.load(Ordering::SeqCst), 0);
        assert_eq!(
            source
                .list_tools()
                .await
                .expect("caller remains open")
                .len(),
            2
        );
        source.shutdown().await.expect("caller owns cleanup");
        assert_eq!(source.observation.shutdowns.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn direct_non_clone_toolset_survives_failed_server_bind() {
    let source = ServedTools::new();
    let occupied = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("occupied port");
    let result = McpServer::start(
        Arc::clone(&source),
        server_config(occupied.local_addr().expect("occupied address")),
    )
    .await;
    assert!(matches!(result, Err(ToolError::Transport { .. })));
    assert_eq!(source.observation.shutdowns.load(Ordering::SeqCst), 0);
    assert_eq!(
        source
            .list_tools()
            .await
            .expect("caller remains open")
            .len(),
        2
    );
    source.shutdown().await.expect("caller owns cleanup");
    assert_eq!(source.observation.shutdowns.load(Ordering::SeqCst), 1);
}

fn server_config(bind: SocketAddr) -> McpServerConfig {
    McpServerConfig::new(bind, "/mcp", "tools-kit-test-tools-v1:")
        .expect("test server config should validate")
}

async fn downstream_toolset(bind: SocketAddr, forms: bool) -> Toolset {
    let config = McpClientConfig::new(McpConnectionConfig::StreamableHttp {
        endpoint: format!("http://{bind}/mcp")
            .parse()
            .expect("served endpoint should parse"),
        headers: BTreeMap::new(),
    })
    .with_namespace("served")
    .expect("test namespace should validate");
    let config = if forms {
        config.with_form_validation(true)
    } else {
        config
    };
    Toolset::mcp(config)
        .await
        .expect("downstream MCP client should connect")
}

async fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("free loopback port probe should bind");
    let address = listener
        .local_addr()
        .expect("free loopback port probe should expose its address");
    drop(listener);
    address
}

async fn assert_downstream_cannot_connect(bind: SocketAddr) {
    let error = Toolset::mcp(
        McpClientConfig::new(McpConnectionConfig::StreamableHttp {
            endpoint: format!("http://{bind}/mcp")
                .parse()
                .expect("served endpoint should parse"),
            headers: BTreeMap::new(),
        })
        .with_namespace("served")
        .expect("test namespace should validate"),
    )
    .await
    .expect_err("served endpoint should be closed");
    assert!(matches!(
        error,
        ToolError::Transport { .. } | ToolError::Protocol { .. } | ToolError::Timeout { .. }
    ));
}
