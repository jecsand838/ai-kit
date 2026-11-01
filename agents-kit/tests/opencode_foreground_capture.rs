mod native_cleanup;
mod opencode_foreground_evidence;

use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use native_cleanup::NativeCleanup;
use opencode_foreground_evidence::OpenCodeForegroundCapture;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct WireFixture;

impl WireFixture {
    const PROMPT: &'static str = "Return only this exact token: token123";
    const TOKEN: &'static str = "token123";
    const SESSION: &'static str = "ses_000000000001AAAAAAAAAAAAAA";
    const USER: &'static str = "msg_000000000002BBBBBBBBBBBBBB";
    const ASSISTANT: &'static str = "msg_000000000003CCCCCCCCCCCCCC";

    fn request_body() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "noReply":false,"model":{"providerID":"openai","modelID":"model"},
            "parts":[{"id":"prt_request","type":"text","text":Self::PROMPT}]
        }))
        .unwrap()
    }

    fn response_body() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "info":{"id":Self::ASSISTANT,"sessionID":Self::SESSION,"parentID":Self::USER,
                "role":"assistant","providerID":"openai","modelID":"model","time":{"completed":1}},
            "parts":[{"id":"prt_answer","messageID":Self::ASSISTANT,"sessionID":Self::SESSION,
                "type":"text","text":Self::TOKEN}]
        }))
        .unwrap()
    }

    fn parent_body() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "info":{"id":Self::USER,"sessionID":Self::SESSION,"role":"user",
                "model":{"providerID":"openai","modelID":"model"}},
            "parts":[{"id":"prt_request","messageID":Self::USER,"sessionID":Self::SESSION,
                "type":"text","text":Self::PROMPT}]
        }))
        .unwrap()
    }

    fn requests() -> Vec<u8> {
        let body = Self::request_body();
        let mut bytes = format!(
            "POST /session/{}/message?directory=%2Fprivate HTTP/1.1\r\nHost: exact-host\r\nAuthorization: private-test-secret\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            Self::SESSION,
            body.len()
        )
        .into_bytes();
        bytes.extend(body);
        bytes.extend(
            format!(
                "GET /session/{}/message/{} HTTP/1.1\r\nHost: exact-host\r\n\r\n",
                Self::SESSION,
                Self::USER
            )
            .as_bytes(),
        );
        bytes
    }

    fn response(body: &[u8], chunked: bool) -> Vec<u8> {
        if chunked {
            let mut bytes = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
            for chunk in body.chunks(7) {
                bytes.extend(format!("{:x};ignored=extension\r\n", chunk.len()).as_bytes());
                bytes.extend(chunk);
                bytes.extend(b"\r\n");
            }
            bytes.extend(b"0\r\nX-Diagnostic-Trailer: unchanged\r\n\r\n");
            bytes
        } else {
            let mut bytes = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .into_bytes();
            bytes.extend(body);
            bytes
        }
    }

    async fn exchange(request: Vec<u8>, response: Vec<u8>) -> OpenCodeForegroundCapture {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let native = listener.local_addr().unwrap();
        let expected_request = request.clone();
        let expected_response = response.clone();
        let upstream = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut actual = vec![0; expected_request.len()];
            socket.read_exact(&mut actual).await.unwrap();
            assert_eq!(
                actual, expected_request,
                "request wire bytes must be unchanged"
            );
            for chunk in response.chunks(11) {
                socket.write_all(chunk).await.unwrap();
                tokio::task::yield_now().await;
            }
            socket.shutdown().await.unwrap();
        });
        let mut capture = OpenCodeForegroundCapture::start(native).await.unwrap();
        capture
            .expect("stream", Self::PROMPT, "model", Self::TOKEN)
            .unwrap();
        let mut client = TcpStream::connect(Self::address(&capture)).await.unwrap();
        for chunk in request.chunks(13) {
            client.write_all(chunk).await.unwrap();
        }
        client.shutdown().await.unwrap();
        let mut actual = Vec::new();
        client.read_to_end(&mut actual).await.unwrap();
        assert_eq!(
            actual, expected_response,
            "response wire bytes must be unchanged"
        );
        upstream.await.unwrap();
        capture.observe("stream", Self::TOKEN);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
        capture.finish(&mut cleanup).await;
        cleanup.finish(Ok(Ok(())));
        capture
    }

    fn address(capture: &OpenCodeForegroundCapture) -> SocketAddr {
        capture
            .endpoint()
            .strip_prefix("http://")
            .unwrap()
            .parse()
            .unwrap()
    }

    fn phase(summary: &Value) -> &Value {
        &summary["phases"][0]
    }

    fn directory() -> PathBuf {
        std::env::temp_dir().join(format!("foreground-capture-{}", uuid::Uuid::new_v4()))
    }
}

#[tokio::test]
async fn content_length_and_chunked_foreground_are_correlated_without_rewriting() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for chunked in [false, true] {
            let mut response = b"HTTP/1.1 100 Continue\r\n\r\n".to_vec();
            response.extend(WireFixture::response(
                &WireFixture::response_body(),
                chunked,
            ));
            response.extend(WireFixture::response(&WireFixture::parent_body(), !chunked));
            let capture = WireFixture::exchange(WireFixture::requests(), response).await;
            let summary = capture.summary();
            assert_eq!(summary["capture_complete"], true, "{summary}");
            let phase = WireFixture::phase(&summary);
            assert_eq!(phase["correlation_complete"], true);
            assert_eq!(phase["native_equals_expected"], true);
            assert_eq!(phase["native_equals_public"], true);
            assert_eq!(phase["parts"][0]["text_bytes"], 8);
            assert!(!summary.to_string().contains(WireFixture::SESSION));
            assert!(!summary.to_string().contains(WireFixture::TOKEN));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn exact_missing_digits_are_evidence_not_repaired_output() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut body: Value = serde_json::from_slice(&WireFixture::response_body()).unwrap();
        body["parts"][0]["text"] = json!("token");
        let mut response = WireFixture::response(&serde_json::to_vec(&body).unwrap(), true);
        response.extend(WireFixture::response(&WireFixture::parent_body(), false));
        let capture = WireFixture::exchange(WireFixture::requests(), response).await;
        let summary = capture.summary();
        assert_eq!(summary["capture_complete"], true, "{summary}");
        assert_eq!(
            WireFixture::phase(&summary)["native_equals_expected"],
            false
        );
        assert_eq!(WireFixture::phase(&summary)["native_equals_public"], false);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn incomplete_and_unsupported_framing_never_claim_parity() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let body = WireFixture::response_body();
        let candidates = [
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nno".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx".to_vec(),
            format!("HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n{}", body.len(), String::from_utf8(body).unwrap()).into_bytes(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 16777217\r\n\r\n".to_vec(),
        ];
        for response in candidates {
            let capture = WireFixture::exchange(WireFixture::requests(), response).await;
            assert_eq!(capture.summary()["capture_complete"], false);
        }
    }).await.unwrap();
}

#[tokio::test]
async fn malformed_ownership_and_duplicate_parts_are_inconclusive() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for duplicate in [false, true] {
            let mut body: Value = serde_json::from_slice(&WireFixture::response_body()).unwrap();
            if duplicate {
                let part = body["parts"][0].clone();
                body["parts"].as_array_mut().unwrap().push(part);
            } else {
                body["parts"][0]["messageID"] = json!("msg_wrong");
            }
            let mut response = WireFixture::response(&serde_json::to_vec(&body).unwrap(), false);
            response.extend(WireFixture::response(&WireFixture::parent_body(), false));
            let capture = WireFixture::exchange(WireFixture::requests(), response).await;
            assert_eq!(capture.summary()["capture_complete"], false);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn private_staging_contains_entity_bodies_without_headers() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut response = WireFixture::response(&WireFixture::response_body(), true);
        response.extend(WireFixture::response(&WireFixture::parent_body(), false));
        let capture = WireFixture::exchange(WireFixture::requests(), response).await;
        let directory = WireFixture::directory();
        std::fs::create_dir(&directory).unwrap();
        let operation = capture.stage_private_bodies(&directory);
        let manifest = std::fs::read(directory.join("foreground/manifest.json"));
        let request = std::fs::read(directory.join("foreground/stream.request.json"));
        let answer = std::fs::read(directory.join("foreground/stream.response.json"));
        let cleanup = std::fs::remove_dir_all(&directory);
        operation.unwrap();
        let manifest: Value = serde_json::from_slice(&manifest.unwrap()).unwrap();
        assert_eq!(manifest["capture_complete"], true);
        assert_eq!(request.unwrap(), WireFixture::request_body());
        assert_eq!(answer.unwrap(), WireFixture::response_body());
        assert!(!manifest.to_string().contains("private-test-secret"));
        cleanup.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_sse_flows_before_completion_and_shutdown_joins_every_socket() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let mut capture = OpenCodeForegroundCapture::start(listener.local_addr().unwrap()).await.unwrap();
        capture.expect("stream", WireFixture::PROMPT, "model", WireFixture::TOKEN).unwrap();
        let event_request = b"GET /global/event HTTP/1.1\r\nHost: unchanged\r\n\r\n";
        let event_prefix = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n8\r\ndata: x\n\r\n";
        let upstream = tokio::spawn(async move {
            let (mut events, _) = listener.accept().await.unwrap();
            let mut event_input = vec![0; event_request.len()];
            events.read_exact(&mut event_input).await.unwrap();
            assert_eq!(event_input, event_request);
            events.write_all(event_prefix).await.unwrap();
            let (mut foreground, _) = listener.accept().await.unwrap();
            let request = WireFixture::requests();
            let mut input = vec![0; request.len()];
            foreground.read_exact(&mut input).await.unwrap();
            assert_eq!(input, request);
            foreground.write_all(&WireFixture::response(&WireFixture::response_body(), true)).await.unwrap();
            foreground.write_all(&WireFixture::response(&WireFixture::parent_body(), false)).await.unwrap();
            foreground.shutdown().await.unwrap();
            let mut closed = [0; 1];
            assert_eq!(events.read(&mut closed).await.unwrap(), 0, "relay shutdown must close its upstream SSE socket");
        });
        let mut events = TcpStream::connect(WireFixture::address(&capture)).await.unwrap();
        events.write_all(event_request).await.unwrap();
        let mut prefix = vec![0; event_prefix.len()];
        events.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix, event_prefix, "SSE prefix must arrive before the upstream finishes");
        let mut foreground = TcpStream::connect(WireFixture::address(&capture)).await.unwrap();
        foreground.write_all(&WireFixture::requests()).await.unwrap();
        foreground.shutdown().await.unwrap();
        let mut output = Vec::new();
        foreground.read_to_end(&mut output).await.unwrap();
        capture.observe("stream", WireFixture::TOKEN);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
        capture.finish(&mut cleanup).await;
        cleanup.finish(Ok(Ok(())));
        upstream.await.unwrap();
        assert_eq!(capture.summary()["capture_complete"], true, "{}", capture.summary());
        assert_eq!(capture.summary()["connections"], 2);
        assert!(TcpStream::connect(WireFixture::address(&capture)).await.is_err(), "listener must be gone after joined shutdown");
    }).await.unwrap();
}

#[tokio::test]
async fn observation_limits_do_not_truncate_or_rewrite_transport() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let oversized = vec![b'x'; (2 << 20) + 1];
        let mut request = format!(
            "POST /session/{}/message HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            WireFixture::SESSION,
            oversized.len()
        )
        .into_bytes();
        request.extend(oversized);
        let capture = WireFixture::exchange(
            request,
            WireFixture::response(&WireFixture::response_body(), false),
        )
        .await;
        assert_eq!(capture.summary()["capture_complete"], false);
        assert!(
            capture.summary()["reasons"]
                .as_array()
                .unwrap()
                .contains(&json!("http_body_bound"))
        );

        let mut response = b"HTTP/1.1 200 OK\r\nX-Large: ".to_vec();
        response.extend(vec![b'x'; 64 << 10]);
        response.extend(b"\r\nContent-Length: 0\r\n\r\n");
        let capture = WireFixture::exchange(WireFixture::requests(), response).await;
        assert_eq!(capture.summary()["capture_complete"], false);
        assert!(
            capture.summary()["reasons"]
                .as_array()
                .unwrap()
                .contains(&json!("http_header_bound"))
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn complete_close_delimited_body_is_distinct_from_interrupted_body() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let body = WireFixture::response_body();
        let mut response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n".to_vec();
        response.extend(body);
        let capture = WireFixture::exchange(WireFixture::requests(), response).await;
        // The native body is complete at EOF, but the required parent response
        // is absent. It must never be presented as correlated success.
        assert_eq!(capture.summary()["capture_complete"], false);
        assert_eq!(
            WireFixture::phase(&capture.summary())["response_forwarded"],
            true
        );
        assert_eq!(
            WireFixture::phase(&capture.summary())["reason"],
            "foreground_parent_ambiguous_or_missing"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn interrupted_finish_keeps_connection_handles_until_resumed_join() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let mut capture = OpenCodeForegroundCapture::start(listener.local_addr().unwrap())
            .await
            .unwrap();
        let mut client = TcpStream::connect(WireFixture::address(&capture))
            .await
            .unwrap();
        client
            .write_all(b"GET /global/event HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let (mut upstream, _) = listener.accept().await.unwrap();
        let mut input = [0; 30];
        let count = upstream.read(&mut input).await.unwrap();
        assert!(count > 0);
        let mut cleanup = NativeCleanup::new(Duration::from_secs(5));
        // Poll shutdown once, then cancel it at its first pending join. The
        // owner must still retain both listener and connection handles.
        let mut finishing = Box::pin(capture.finish(&mut cleanup));
        let interrupted = std::future::poll_fn(|context| {
            std::task::Poll::Ready(finishing.as_mut().poll(context).is_pending())
        })
        .await;
        drop(finishing);
        capture.finish(&mut cleanup).await;
        cleanup.finish(Ok(Ok(())));
        let mut closed = [0; 1];
        assert_eq!(upstream.read(&mut closed).await.unwrap(), 0);
        assert_eq!(client.read(&mut closed).await.unwrap(), 0);
        let summary = capture.summary();
        assert_eq!(summary["transport_settled"], true);
        assert_eq!(summary["listener_joined"], true);
        assert_eq!(summary["joined_connections"], summary["connections"]);
        assert!(
            interrupted,
            "the first cleanup future must actually be cancelled"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn framed_partial_entity_is_retained_without_claiming_complete_capture() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let prefix = br#"{"info":{"role":"assistant"}"#;
        for chunked in [false, true] {
            let mut response = if chunked {
                format!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
                    prefix.len() + 10
                )
                .into_bytes()
            } else {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                    prefix.len() + 10
                )
                .into_bytes()
            };
            response.extend(prefix);
            let capture = WireFixture::exchange(WireFixture::requests(), response).await;
            let directory = WireFixture::directory();
            std::fs::create_dir(&directory).unwrap();
            let staged = capture.stage_private_bodies(&directory);
            let body = std::fs::read(directory.join("foreground/stream.response.json"));
            let manifest = std::fs::read(directory.join("foreground/manifest.json"));
            let cleanup = std::fs::remove_dir_all(directory);
            staged.unwrap();
            assert_eq!(body.unwrap(), prefix);
            let manifest: Value = serde_json::from_slice(&manifest.unwrap()).unwrap();
            assert_eq!(manifest["capture_complete"], false);
            assert_eq!(manifest["phases"][0]["response_framing_complete"], false);
            assert_eq!(
                manifest["phases"][0]["reason"],
                "foreground_framing_incomplete"
            );
            cleanup.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_part_order_and_synthetic_selection_remain_explicit() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut body: Value = serde_json::from_slice(&WireFixture::response_body()).unwrap();
        let mut first = body["parts"][0].clone();
        first["id"] = json!("prt_a");
        first["text"] = json!("token");
        first["ignored"] = json!(true);
        let mut second = body["parts"][0].clone();
        second["id"] = json!("prt_b");
        second["text"] = json!("123");
        let mut synthetic = body["parts"][0].clone();
        synthetic["id"] = json!("prt_c");
        synthetic["synthetic"] = json!(true);
        synthetic["text"] = json!("synthetic unrelated text");
        body["parts"] = json!([second, synthetic, first]);
        let mut response = WireFixture::response(&serde_json::to_vec(&body).unwrap(), true);
        response.extend(WireFixture::response(&WireFixture::parent_body(), false));
        let capture = WireFixture::exchange(WireFixture::requests(), response).await;
        let summary = capture.summary();
        assert_eq!(summary["capture_complete"], true, "{summary}");
        let phase = WireFixture::phase(&summary);
        assert_eq!(phase["native_equals_expected"], true);
        assert_eq!(phase["parts"][0]["ignored"], true);
        assert_eq!(phase["parts"][0]["selected"], true);
        assert_eq!(phase["parts"][2]["synthetic"], true);
        assert_eq!(phase["parts"][2]["selected"], false);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn informational_headers_share_the_exchange_framing_budget() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut response = Vec::new();
        for _ in 0..5 {
            response.extend(b"HTTP/1.1 100 Continue\r\nX-Padding: ");
            response.extend(vec![b'x'; 60 << 10]);
            response.extend(b"\r\n\r\n");
        }
        response.extend(WireFixture::response(&WireFixture::response_body(), true));
        response.extend(WireFixture::response(&WireFixture::parent_body(), false));
        let capture = WireFixture::exchange(WireFixture::requests(), response).await;
        let summary = capture.summary();
        assert_eq!(summary["capture_complete"], false, "{summary}");
        assert!(
            summary["reasons"]
                .as_array()
                .unwrap()
                .contains(&json!("http_framing_bound"))
        );
    })
    .await
    .unwrap();
}
