//! Independent bounded HTTP peers exercise the shipped client, not McpServer.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};
use tools_kit::{
    ToolError, ToolHandlerBackend, Toolset, ToolsetBackend,
    declaration::ToolName,
    execution::{ToolContent, ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    mcp::{McpClientConfig, McpConnectionConfig},
};

const IO_BOUND: Duration = Duration::from_secs(5);
const EARLY_REJECTION_BOUND: Duration = Duration::from_secs(2);
const JSON_BODY_LIMIT: usize = 16 * 1024 * 1024;
const ERROR_BODY_LIMIT: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
enum ResponseCase {
    DeclaredOversize,
    ChunkedJsonOversize,
    ChunkedErrorOversize,
    JsonControl,
    ErrorControl,
    ProtocolRevision(&'static str),
    ContentControl,
    AudioMetadataControl,
}

impl ResponseCase {
    fn held(self) -> bool {
        matches!(
            self,
            Self::DeclaredOversize | Self::ChunkedJsonOversize | Self::ChunkedErrorOversize
        )
    }

    async fn send(
        self,
        wire: &mut HttpPeer,
        target: Target,
        id: Value,
        delivered: &watch::Sender<bool>,
        mut released: watch::Receiver<bool>,
    ) -> Result<(), String> {
        match self {
            Self::DeclaredOversize => {
                wire.headers(200, Some(JSON_BODY_LIMIT + 1), false).await?;
            }
            Self::ChunkedJsonOversize | Self::ChunkedErrorOversize => {
                let (status, limit) = match self {
                    Self::ChunkedJsonOversize => (200, JSON_BODY_LIMIT),
                    Self::ChunkedErrorOversize => (400, ERROR_BODY_LIMIT),
                    Self::DeclaredOversize
                    | Self::JsonControl
                    | Self::ErrorControl
                    | Self::ProtocolRevision(_)
                    | Self::ContentControl
                    | Self::AudioMetadataControl => {
                        unreachable!("only chunked cases reach this arm")
                    }
                };
                wire.headers(status, None, true).await?;
                // One finite cap-plus-one body. Its final HTTP chunk is withheld,
                // so EOF/JSON parsing cannot masquerade as a response-size bound.
                wire.chunk(&vec![b' '; limit + 1]).await?;
            }
            Self::JsonControl => {
                wire.json(200, target.success(id)).await?;
            }
            Self::ProtocolRevision(version) => {
                let mut result = target.success(id);
                result["result"]["protocolVersion"] = json!(version);
                wire.json(200, result).await?;
            }
            Self::ContentControl => {
                wire.json(200, json!({"jsonrpc":"2.0", "id":id, "result":{
                    "content":[
                        {"type":"text", "text":"typed text", "_meta":{"trace":"text"}, "annotations":{"priority":0.25}},
                        {"type":"image", "data":"aW1hZ2U=", "mimeType":"image/png", "_meta":{"trace":"image"}},
                        {"type":"audio", "data":"YXVkaW8=", "mimeType":"audio/wav", "annotations":{"priority":0.5}},
                        {"type":"resource_link", "uri":"memory://reference", "name":"reference", "_meta":{"trace":"link"}},
                        {"type":"resource", "resource":{"uri":"memory://embedded", "mimeType":"text/plain", "text":"embedded"}, "_meta":{"trace":"embedded"}}
                    ], "structuredContent":{"complete":true}, "isError":false,
                    "_meta":{"trace":"result"}
                }})).await?;
            }
            Self::AudioMetadataControl => {
                wire.json(200, json!({"jsonrpc":"2.0", "id":id, "result":{
                    "content":[{"type":"audio", "data":"YXVkaW8=", "mimeType":"audio/wav",
                        "_meta":{"trace":"audio"}, "annotations":{"priority":0.5}}], "isError":false
                }})).await?;
            }
            Self::ErrorControl => {
                wire.json(
                    400,
                    json!({"jsonrpc":"2.0", "id":id, "error":{
                        "code":-32602, "message":"bounded deliberate rejection"
                    }}),
                )
                .await?;
            }
        }
        delivered.send_replace(true);
        if self.held() {
            released
                .wait_for(|released| *released)
                .await
                .map_err(|error| error.to_string())?;
            // Closing the finite response releases the old unbounded reader only
            // after the test has recorded whether it rejected before EOF.
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Target {
    Initialize,
    ToolCall,
}

impl Target {
    fn method(self) -> &'static str {
        match self {
            Self::Initialize => "initialize",
            Self::ToolCall => "tools/call",
        }
    }

    fn success(self, id: Value) -> Value {
        let result = match self {
            Self::Initialize => json!({
                "protocolVersion":"2025-11-25", "capabilities":{"tools":{}},
                "serverInfo":{"name":"bounded-response-fixture", "version":"1"},
                "instructions":"within-limit initialization"
            }),
            Self::ToolCall => json!({
                "content":[{"type":"text", "text":"bounded response"}], "isError":false
            }),
        };
        json!({"jsonrpc":"2.0", "id":id, "result":result})
    }

    async fn observe(self, config: McpClientConfig) -> ClientObservation {
        let toolset = match Toolset::mcp(config).await {
            Ok(toolset) => toolset,
            Err(error) => {
                return ClientObservation {
                    result: Err(error),
                    cleanup: Ok(()),
                };
            }
        };
        let result = match self {
            Self::Initialize => Ok(None),
            Self::ToolCall => {
                async {
                    let mut handler = toolset.tool_handler(ToolName::new("echo")?).await?;
                    let result = handler
                        .step(ToolHandlerInput::Invoke(ToolInvocation {
                            call_id: Some("bounded-call".to_owned()),
                            arguments: json!({}),
                        }))
                        .await?;
                    match result {
                        ToolHandlerOutput::Completed(result) => Ok(Some(result)),
                        unexpected => Err(ToolError::Protocol {
                            message: format!("unexpected bounded control result: {unexpected:?}"),
                        }),
                    }
                }
                .await
            }
        };
        ClientObservation {
            result,
            cleanup: toolset.shutdown().await,
        }
    }
}

#[derive(Debug)]
struct ClientObservation {
    result: Result<Option<ToolResult>, ToolError>,
    cleanup: Result<(), ToolError>,
}

struct ResponseLimitPeer {
    address: SocketAddr,
    stopped: watch::Sender<bool>,
    released: watch::Sender<bool>,
    delivered: watch::Receiver<bool>,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<Result<(), String>>,
}

impl ResponseLimitPeer {
    async fn start(target: Target, response: ResponseCase) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind raw peer");
        let address = listener.local_addr().expect("raw peer address");
        let (stopped, mut stop) = watch::channel(false);
        let (released, release) = watch::channel(false);
        let (delivered, delivery) = watch::channel(false);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            // This fixture has one stateless client and no SSE/background work.
            // One tracked owner serially serves its finite requests and stops
            // even while a response intentionally withholds EOF.
            for _ in 0..16 {
                let stream = tokio::select! {
                    _ = stop.wait_for(|stop| *stop) => return Ok(()),
                    accepted = listener.accept() => accepted.map_err(|error| error.to_string())?.0,
                };
                tokio::select! {
                    _ = stop.wait_for(|stop| *stop) => return Ok(()),
                    result = HttpPeer::new(stream).respond(
                        target, response, Arc::clone(&observed), delivered.clone(), release.clone()
                    ) => result?,
                }
            }
            Err("raw peer exceeded its finite request bound".to_owned())
        });
        Self {
            address,
            stopped,
            released,
            delivered: delivery,
            requests,
            task,
        }
    }

    fn config(&self) -> McpClientConfig {
        McpClientConfig::new(McpConnectionConfig::StreamableHttp {
            endpoint: format!("http://{}/mcp", self.address)
                .parse()
                .expect("raw endpoint"),
            headers: BTreeMap::new(),
        })
        .with_request_timeout(Duration::from_secs(15))
        .expect("finite request timeout")
    }

    async fn check(target: Target, response: ResponseCase) {
        let mut peer = Self::start(target, response).await;
        let config = peer.config();
        let mut client = tokio::spawn(target.observe(config));
        let delivered = timeout(IO_BOUND, peer.delivered.wait_for(|delivered| *delivered)).await;
        let delivered = matches!(delivered, Ok(Ok(_)));
        let before_release = timeout(EARLY_REJECTION_BOUND, &mut client).await;
        peer.released.send_replace(true);
        let (early, mut observed) = match before_release {
            Ok(observed) => (true, Some(observed)),
            Err(_) => (false, timeout(IO_BOUND, &mut client).await.ok()),
        };
        peer.stopped.send_replace(true);
        let cleanup = match timeout(IO_BOUND, &mut peer.task).await {
            Ok(cleanup) => cleanup,
            Err(_) => {
                peer.task.abort();
                peer.task.await
            }
        };
        if observed.is_none() {
            observed = timeout(IO_BOUND, &mut client).await.ok();
        }
        let stalled = observed.is_none();
        let observed = match observed {
            Some(observed) => observed,
            None => {
                client.abort();
                client.await
            }
        };
        let requests = peer.requests.lock().expect("request log").clone();
        eprintln!(
            "{target:?}/{response:?}: delivered={delivered}, completed_before_eof={early}, client_join_stalled={stalled}, requests={requests:?}, result={observed:?}"
        );
        assert!(
            !stalled,
            "{target:?}/{response:?}: delivered={delivered}, completed_before_eof={early}; client failed to join after peer cleanup: {requests:?}; {observed:?}"
        );
        let observed = observed.expect("client observation owner joins");

        assert!(
            delivered,
            "selected response was not delivered: {requests:?}; {observed:?}"
        );
        assert!(
            cleanup.is_ok_and(|result| result.is_ok()),
            "raw peer cleanup failed"
        );
        assert!(
            observed.cleanup.is_ok(),
            "client cleanup failed: {observed:?}"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request["method"] == target.method())
                .count(),
            1,
            "response failure must never resend an operation: {requests:?}"
        );
        assert!(
            early,
            "{target:?}/{response:?} waited for withheld EOF instead of bounded rejection: {observed:?}"
        );
        match response {
            ResponseCase::JsonControl => match target {
                Target::Initialize => assert!(
                    matches!(observed.result, Ok(None)),
                    "initialization control: {observed:?}"
                ),
                Target::ToolCall => assert!(
                    matches!(observed.result, Ok(Some(ref result))
                    if !result.is_error && result.content == vec![ToolContent::Text {
                        text:"bounded response".to_owned(), metadata:None
                    }]),
                    "exact JSON result control: {observed:?}"
                ),
            },
            ResponseCase::ProtocolRevision(version) => {
                let initialize = requests
                    .iter()
                    .find(|request| request["method"] == "initialize")
                    .expect("observed initialize");
                assert_eq!(
                    initialize["params"]["protocolVersion"], "2025-11-25",
                    "offered revision must remain explicit"
                );
                if version == "2026-07-28" {
                    assert!(
                        matches!(
                            observed.result,
                            Err(ToolError::Protocol { .. } | ToolError::Unsupported { .. })
                        ),
                        "unsupported selection must fail before inventory: {observed:?}"
                    );
                    assert!(
                        !requests
                            .iter()
                            .any(|request| request["method"] == "tools/list"
                                || request["method"] == "tools/call"),
                        "unsupported selection admitted work: {requests:?}"
                    );
                } else {
                    assert!(
                        matches!(observed.result, Ok(None)),
                        "retained revision {version}: {observed:?}"
                    );
                    assert_eq!(
                        requests
                            .iter()
                            .filter(|request| request["method"] == "tools/list")
                            .count(),
                        1
                    );
                }
            }
            ResponseCase::ContentControl => {
                let result = observed
                    .result
                    .expect("content control result")
                    .expect("tool result");
                assert!(!result.is_error);
                assert_eq!(result.structured_content, Some(json!({"complete":true})));
                assert_eq!(
                    result.metadata,
                    BTreeMap::from([("trace".to_owned(), json!("result"))])
                );
                assert_eq!(
                    result.content,
                    vec![
                        ToolContent::Text {
                            text: "typed text".to_owned(),
                            metadata: Some(json!({"trace":"text", "priority":0.25}))
                        },
                        ToolContent::Image {
                            data_base64: "aW1hZ2U=".to_owned(),
                            mime_type: "image/png".to_owned(),
                            metadata: Some(json!({"trace":"image"}))
                        },
                        ToolContent::Audio {
                            data_base64: "YXVkaW8=".to_owned(),
                            mime_type: "audio/wav".to_owned(),
                            metadata: Some(json!({"priority":0.5}))
                        },
                        ToolContent::ResourceLink {
                            uri: "memory://reference".to_owned(),
                            title: Some("reference".to_owned()),
                            metadata: json!({"trace":"link"})
                        },
                        ToolContent::EmbeddedResource {
                            uri: "memory://embedded".to_owned(),
                            contents: json!({"uri":"memory://embedded", "mimeType":"text/plain", "text":"embedded"}),
                            metadata: json!({"trace":"embedded"})
                        },
                    ]
                );
            }
            ResponseCase::AudioMetadataControl => {
                let result = observed.result.expect("audio result").expect("tool result");
                assert_eq!(
                    result.content,
                    vec![ToolContent::Audio {
                        data_base64: "YXVkaW8=".to_owned(),
                        mime_type: "audio/wav".to_owned(),
                        metadata: Some(json!({"trace":"audio", "priority":0.5})),
                    }]
                );
            }
            ResponseCase::ErrorControl => assert!(
                matches!(observed.result,
                Err(ToolError::Protocol { ref message }) if message.contains("bounded deliberate rejection")),
                "correlated JSON-RPC error identity must survive: {observed:?}"
            ),
            ResponseCase::DeclaredOversize
            | ResponseCase::ChunkedJsonOversize
            | ResponseCase::ChunkedErrorOversize => {
                assert!(
                    matches!(
                        observed.result,
                        Err(ToolError::Transport { .. } | ToolError::Protocol { .. })
                    ),
                    "oversized native response must fail honestly: {observed:?}"
                );
            }
        }
    }
}

struct HttpPeer {
    stream: BufReader<TcpStream>,
}

impl HttpPeer {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufReader::new(stream),
        }
    }

    async fn line(&mut self) -> Result<String, String> {
        // Cap the read before allocation, including the request line. One
        // sentinel byte distinguishes an overlong line from an exact limit.
        let mut bytes = Vec::with_capacity(8193);
        let mut bounded = (&mut self.stream).take(8193);
        bounded
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|error| error.to_string())?;
        if bytes.len() > 8192 || !bytes.ends_with(b"\n") {
            return Err("raw request line exceeded its bound or ended early".to_owned());
        }
        String::from_utf8(bytes).map_err(|error| error.to_string())
    }

    async fn request(&mut self) -> Result<Option<Value>, String> {
        let line = self.line().await?;
        if !line.starts_with("POST ") {
            return Ok(None);
        }
        let mut length = None;
        for _ in 0..64 {
            let line = self.line().await?;
            if line == "\r\n" {
                let length = length.ok_or("raw POST omitted Content-Length")?;
                if length > 1024 * 1024 {
                    return Err("raw request exceeded fixture bound".to_owned());
                }
                let mut body = vec![0; length];
                self.stream
                    .read_exact(&mut body)
                    .await
                    .map_err(|error| error.to_string())?;
                return serde_json::from_slice(&body)
                    .map(Some)
                    .map_err(|error| error.to_string());
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = Some(
                    value
                        .trim()
                        .parse::<usize>()
                        .map_err(|error| error.to_string())?,
                );
            }
        }
        Err("raw request header count exceeded fixture bound".to_owned())
    }

    async fn respond(
        mut self,
        target: Target,
        response: ResponseCase,
        requests: Arc<Mutex<Vec<Value>>>,
        delivered: watch::Sender<bool>,
        release: watch::Receiver<bool>,
    ) -> Result<(), String> {
        let Some(request) = self.request().await? else {
            return self.empty(405).await;
        };
        let method = request["method"]
            .as_str()
            .ok_or("raw request omitted method")?;
        requests
            .lock()
            .map_err(|error| error.to_string())?
            .push(request.clone());
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        if method == target.method() {
            response
                .send(&mut self, target, id, &delivered, release)
                .await
        } else {
            match method {
                "initialize" => self.json(200, Target::Initialize.success(id)).await,
                "tools/list" => self.json(200, json!({"jsonrpc":"2.0", "id":id, "result":{
                    "tools":[{"name":"echo", "inputSchema":{"type":"object", "properties":{}},
                        "execution":{"taskSupport":"optional"}}]
                }})).await,
                "notifications/initialized" | "notifications/cancelled" => self.empty(202).await,
                unexpected => Err(format!("unexpected raw fixture method: {unexpected}")),
            }
        }
    }

    async fn headers(
        &mut self,
        status: u16,
        length: Option<usize>,
        chunked: bool,
    ) -> Result<(), String> {
        let framing = if chunked {
            "Transfer-Encoding: chunked\r\n".to_owned()
        } else {
            format!("Content-Length: {}\r\n", length.expect("fixed length"))
        };
        let headers = format!(
            "HTTP/1.1 {status} fixture\r\nContent-Type: application/json\r\n{framing}Connection: close\r\n\r\n"
        );
        self.stream
            .get_mut()
            .write_all(headers.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        self.stream
            .get_mut()
            .flush()
            .await
            .map_err(|error| error.to_string())
    }

    async fn chunk(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stream
            .get_mut()
            .write_all(format!("{:x}\r\n", bytes.len()).as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        self.stream
            .get_mut()
            .write_all(bytes)
            .await
            .map_err(|error| error.to_string())?;
        // Leave the trailing CRLF and final zero chunk withheld. The client
        // already receives exactly cap-plus-one body bytes; requiring another
        // write after that point would race its correct early rejection.
        self.stream
            .get_mut()
            .flush()
            .await
            .map_err(|error| error.to_string())
    }

    async fn json(&mut self, status: u16, body: Value) -> Result<(), String> {
        let body = serde_json::to_vec(&body).map_err(|error| error.to_string())?;
        self.headers(status, Some(body.len()), false).await?;
        self.stream
            .get_mut()
            .write_all(&body)
            .await
            .map_err(|error| error.to_string())
    }

    async fn empty(&mut self, status: u16) -> Result<(), String> {
        self.headers(status, Some(0), false).await
    }
}

#[tokio::test]
async fn declared_oversized_initialization_is_rejected_before_body_arrives() {
    ResponseLimitPeer::check(Target::Initialize, ResponseCase::DeclaredOversize).await;
}

#[tokio::test]
async fn chunked_initialization_is_rejected_at_limit_before_eof() {
    ResponseLimitPeer::check(Target::Initialize, ResponseCase::ChunkedJsonOversize).await;
}

#[tokio::test]
async fn chunked_tool_response_is_rejected_at_limit_before_eof_without_resend() {
    ResponseLimitPeer::check(Target::ToolCall, ResponseCase::ChunkedJsonOversize).await;
}

#[tokio::test]
async fn chunked_error_response_is_rejected_at_its_smaller_limit_before_eof() {
    ResponseLimitPeer::check(Target::ToolCall, ResponseCase::ChunkedErrorOversize).await;
}

#[tokio::test]
async fn bounded_response_controls_preserve_success_and_jsonrpc_rejection() {
    ResponseLimitPeer::check(Target::Initialize, ResponseCase::JsonControl).await;
    ResponseLimitPeer::check(Target::ToolCall, ResponseCase::JsonControl).await;
    ResponseLimitPeer::check(Target::ToolCall, ResponseCase::ErrorControl).await;
}

#[tokio::test]
async fn client_protocol_revision_preserves_initialize_offer_and_retained_versions() {
    for version in ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"] {
        ResponseLimitPeer::check(Target::Initialize, ResponseCase::ProtocolRevision(version)).await;
    }
}

#[tokio::test]
async fn client_protocol_revision_rejects_new_lifecycle_before_inventory() {
    ResponseLimitPeer::check(
        Target::Initialize,
        ResponseCase::ProtocolRevision("2026-07-28"),
    )
    .await;
}

#[tokio::test]
async fn content_wire_control_preserves_all_existing_variants_and_metadata() {
    ResponseLimitPeer::check(Target::ToolCall, ResponseCase::ContentControl).await;
}

#[tokio::test]
async fn audio_content_metadata_is_preserved_with_annotations() {
    ResponseLimitPeer::check(Target::ToolCall, ResponseCase::AudioMetadataControl).await;
}
