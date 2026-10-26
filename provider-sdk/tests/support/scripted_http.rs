use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use url::Url;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct ScriptedResponse {
    status: u16,
    content_type: &'static str,
    headers: BTreeMap<String, String>,
    body: Vec<Vec<u8>>,
    content_length: Option<usize>,
}

impl ScriptedResponse {
    pub fn json(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            headers: BTreeMap::new(),
            body: vec![body.as_bytes().to_vec()],
            content_length: None,
        }
    }

    pub fn sse(body: &str) -> Self {
        Self {
            status: 200,
            content_type: "text/event-stream",
            headers: BTreeMap::new(),
            body: vec![body.as_bytes().to_vec()],
            content_length: None,
        }
    }

    pub fn error(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "application/json",
            headers: BTreeMap::new(),
            body: vec![body.as_bytes().to_vec()],
            content_length: None,
        }
    }

    pub fn chunks(status: u16, content_type: &'static str, body: Vec<Vec<u8>>) -> Self {
        Self {
            status,
            content_type,
            headers: BTreeMap::new(),
            body,
            content_length: None,
        }
    }

    pub fn with_header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.insert(name.to_string(), value.into());
        self
    }

    pub fn advertised_content_length(mut self, length: usize) -> Self {
        self.content_length = Some(length);
        self
    }
}

#[derive(Debug)]
pub struct ScriptedHttpServer {
    base_url: Url,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    tasks: JoinSet<()>,
    state: ScriptedHttpState,
}

#[derive(Debug)]
enum ScriptedHttpState {
    Running,
    Cancelling(String),
    Settled(Result<(), String>),
}

impl ScriptedHttpServer {
    pub fn base_url(&self) -> Url {
        self.base_url.clone()
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests
            .lock()
            .expect("requests mutex should lock")
            .clone()
    }

    /// Observe actual completion; cancelling this borrow keeps task ownership.
    /// Drop requests abort through JoinSet, but does not synchronously join.
    pub(crate) async fn finish(&mut self, wait: Duration) -> Result<(), String> {
        match &self.state {
            ScriptedHttpState::Settled(result) => return result.clone(),
            ScriptedHttpState::Running => {
                match tokio::time::timeout(wait, self.tasks.join_next()).await {
                    Ok(joined) => {
                        let result = joined
                            .ok_or_else(|| "scripted HTTP task ownership missing".to_owned())
                            .and_then(|result| {
                                result
                                    .map_err(|error| format!("scripted HTTP task failed: {error}"))
                            });
                        self.state = ScriptedHttpState::Settled(result.clone());
                        return result;
                    }
                    Err(_) => {
                        self.state = ScriptedHttpState::Cancelling(
                            "scripted HTTP server timed out before completing".into(),
                        );
                        self.tasks.abort_all();
                    }
                }
            }
            ScriptedHttpState::Cancelling(_) => {}
        }
        let ScriptedHttpState::Cancelling(primary) = &self.state else {
            unreachable!("an unsettled server must be cancelling");
        };
        let failure =
            match tokio::time::timeout(Duration::from_secs(1), self.tasks.join_next()).await {
                Ok(Some(Ok(()))) => primary.clone(),
                Ok(Some(Err(error))) if error.is_cancelled() => primary.clone(),
                Ok(Some(Err(error))) => format!("{primary}; cancellation join failed: {error}"),
                Ok(None) => format!("{primary}; cancellation task ownership missing"),
                Err(_) => {
                    // Keep the task and the primary timeout for a later waiter.
                    return Err(format!("{primary}; cancellation join is still unconfirmed"));
                }
            };
        let result = Err(failure);
        self.state = ScriptedHttpState::Settled(result.clone());
        result
    }
}

pub async fn spawn_scripted_http_server(script: Vec<ScriptedResponse>) -> ScriptedHttpServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have local address");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let stored_requests = Arc::clone(&requests);
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        for response in script {
            let (mut stream, _) = listener.accept().await.expect("server should accept");
            let request = RecordedRequest::read(&mut stream)
                .await
                .expect("server should parse request");
            stored_requests
                .lock()
                .expect("requests mutex should lock")
                .push(request);
            response
                .write(&mut stream)
                .await
                .expect("server should write response");
        }
    });

    ScriptedHttpServer {
        base_url: Url::parse(&format!("http://{address}")).expect("base URL should parse"),
        requests,
        tasks,
        state: ScriptedHttpState::Running,
    }
}

impl RecordedRequest {
    pub(crate) async fn read(
        stream: &mut tokio::net::TcpStream,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut buffer = Vec::new();
        let headers_end = loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err("request ended before headers completed".into());
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(position) = buffer.windows(4).position(|part| part == b"\r\n\r\n") {
                break position;
            }
        };

        let header_text = String::from_utf8(buffer[..headers_end].to_vec())?;
        let mut lines = header_text.split("\r\n");
        let mut request_line = lines
            .next()
            .ok_or("request line should exist")?
            .split_whitespace();
        let method = request_line
            .next()
            .ok_or("request method should exist")?
            .to_string();
        let path = request_line
            .next()
            .ok_or("request path should exist")?
            .to_string();

        let mut headers = BTreeMap::new();
        let mut content_length = 0;
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                let name = name.to_ascii_lowercase();
                let value = value.trim().to_string();
                if name == "content-length" {
                    content_length = value.parse()?;
                }
                headers.insert(name, value);
            }
        }

        let mut body = buffer[headers_end + 4..].to_vec();
        while body.len() < content_length {
            let mut chunk = vec![0_u8; content_length - body.len()];
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..read]);
        }
        body.truncate(content_length);

        Ok(Self {
            method,
            path,
            headers,
            body,
        })
    }
}

impl ScriptedResponse {
    pub(crate) async fn write(
        self,
        stream: &mut tokio::net::TcpStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.body.len() == 1 {
            let body = self.body.into_iter().next().unwrap_or_default();
            let content_length = self.content_length.unwrap_or(body.len());
            let extra_headers = self
                .headers
                .into_iter()
                .map(|(name, value)| format!("{name}: {value}\r\n"))
                .collect::<String>();
            let head = format!(
                "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nContent-Type: {}\r\n{extra_headers}Connection: close\r\n\r\n",
                self.status, content_length, self.content_type,
            );
            stream.write_all(head.as_bytes()).await?;
            stream.write_all(&body).await?;
        } else {
            let extra_headers = self
                .headers
                .into_iter()
                .map(|(name, value)| format!("{name}: {value}\r\n"))
                .collect::<String>();
            let head = format!(
                "HTTP/1.1 {} OK\r\nTransfer-Encoding: chunked\r\nContent-Type: {}\r\n{extra_headers}Connection: close\r\n\r\n",
                self.status, self.content_type,
            );
            stream.write_all(head.as_bytes()).await?;
            for chunk in self.body {
                stream
                    .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
                    .await?;
                stream.write_all(&chunk).await?;
                stream.write_all(b"\r\n").await?;
            }
            stream.write_all(b"0\r\n\r\n").await?;
        }
        stream.flush().await?;
        Ok(())
    }
}
