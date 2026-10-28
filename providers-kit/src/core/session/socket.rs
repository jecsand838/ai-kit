use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::error::CapacityError;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message as WebSocketMessage};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::KitError;
use crate::error::ValidationViolation;
use crate::error::{
    ErrorKind, ErrorPhase, NativeErrorDetails, Operation, ResponseDetails, ResponseViolation,
};
use crate::transport::CheckedRequestSpec;

const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 8 * 1_024 * 1_024;

pub(crate) enum SessionSocketMessage {
    Payload(Value),
    Closed(Option<String>),
    Exhausted,
}

pub(crate) struct SessionSocket {
    timeout: Duration,
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    metadata: NativeErrorDetails,
}

impl SessionSocket {
    pub(crate) async fn connect(
        request: http::Request<()>,
        connect_timeout: Duration,
        timeout: Duration,
        requests: CheckedRequestSpec,
    ) -> Result<(Self, Instant), KitError> {
        let deadline = Self::deadline_after(timeout)?;
        let connect_deadline = Self::deadline_after(connect_timeout)?.min(deadline);
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES));
        let connected = tokio::time::timeout_at(
            connect_deadline,
            connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| {
            KitError::new(
                ErrorKind::Timeout,
                Operation::OpenSession,
                ErrorPhase::SessionHandshake,
            )
            .with_timeout(connect_timeout.min(timeout))
        })?
        .map_err(|error| {
            requests.refine_http_error(Self::socket_error(
                error,
                Operation::OpenSession,
                ErrorPhase::SessionHandshake,
            ))
        })?;
        let metadata = NativeErrorDetails {
            request_id: connected
                .1
                .headers()
                .get("x-request-id")
                .or_else(|| connected.1.headers().get("request-id"))
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            ..NativeErrorDetails::default()
        };
        Ok((
            Self {
                timeout,
                socket: connected.0,
                metadata,
            },
            deadline,
        ))
    }

    pub(crate) fn operation_deadline(&self) -> Result<Instant, KitError> {
        Self::deadline_after(self.timeout)
    }

    pub(crate) async fn fail_setup(mut self, error: KitError) -> KitError {
        let error = self.attach_error(error.at_operation(Operation::OpenSession));
        let cleanup = match self.operation_deadline() {
            Ok(deadline) => self.close(deadline).await,
            Err(error) => Err(error.at_operation(Operation::SessionClose)),
        };
        match cleanup {
            Ok(()) => error,
            Err(cleanup) => error.with_supplementary(cleanup.at_phase(ErrorPhase::Cleanup)),
        }
    }

    pub(crate) async fn send(&mut self, payload: Value, deadline: Instant) -> Result<(), KitError> {
        let payload = payload.to_string();
        if payload.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
            return Err(KitError::invalid_input(
                ValidationViolation::LimitExceeded {
                    field: "session.message",
                    limit: MAX_WEBSOCKET_MESSAGE_BYTES,
                },
                "serialized session websocket message exceeds 8 MiB",
            )
            .at_operation(Operation::SessionSend)
            .at_phase(ErrorPhase::InputValidation));
        }
        tokio::time::timeout_at(
            deadline,
            self.socket.send(WebSocketMessage::Text(payload.into())),
        )
        .await
        .map_err(|_| {
            self.attach_error(
                KitError::new(
                    ErrorKind::Timeout,
                    Operation::SessionSend,
                    ErrorPhase::SessionSend,
                )
                .with_timeout(self.timeout),
            )
        })?
        .map_err(|error| {
            self.attach_error(Self::socket_error(
                error,
                Operation::SessionSend,
                ErrorPhase::SessionSend,
            ))
        })
    }

    pub(crate) async fn receive(
        &mut self,
        deadline: Instant,
    ) -> Result<SessionSocketMessage, KitError> {
        tokio::time::timeout_at(deadline, async {
            loop {
                let Some(message) = self.socket.next().await else {
                    return Ok(SessionSocketMessage::Exhausted);
                };
                let message = match message {
                    Ok(message) => message,
                    Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => {
                        return Ok(SessionSocketMessage::Exhausted);
                    }
                    Err(error) => {
                        return Err(Self::socket_error(
                            error,
                            Operation::SessionReceive,
                            ErrorPhase::SessionReceive,
                        ));
                    }
                };
                match message {
                    WebSocketMessage::Text(text) => {
                        if text.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
                            return Err(KitError::invalid_response(
                                ResponseViolation::LimitExceeded {
                                    limit: MAX_WEBSOCKET_MESSAGE_BYTES,
                                },
                            )
                            .at_operation(Operation::SessionReceive)
                            .at_phase(ErrorPhase::SessionReceive));
                        }
                        let payload = serde_json::from_str(text.as_ref()).map_err(|error| {
                            KitError::invalid_response(ResponseViolation::MalformedJson)
                                .with_response_details(ResponseDetails::json(&error))
                                .at_operation(Operation::SessionReceive)
                                .at_phase(ErrorPhase::Decode)
                                .with_message(format!(
                                    "session websocket text was invalid JSON: {error}"
                                ))
                                .with_body(Some(text.to_string()))
                        })?;
                        return Ok(SessionSocketMessage::Payload(payload));
                    }
                    WebSocketMessage::Binary(bytes) => {
                        if bytes.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
                            return Err(KitError::invalid_response(
                                ResponseViolation::LimitExceeded {
                                    limit: MAX_WEBSOCKET_MESSAGE_BYTES,
                                },
                            )
                            .at_operation(Operation::SessionReceive)
                            .at_phase(ErrorPhase::SessionReceive));
                        }
                        let payload = serde_json::from_slice(bytes.as_ref()).map_err(|error| {
                            KitError::invalid_response(
                                if std::str::from_utf8(bytes.as_ref()).is_err() {
                                    ResponseViolation::InvalidUtf8
                                } else {
                                    ResponseViolation::MalformedJson
                                },
                            )
                            .with_response_details(ResponseDetails::json(&error))
                            .at_operation(Operation::SessionReceive)
                            .at_phase(ErrorPhase::Decode)
                            .with_message(format!(
                                "session websocket binary message was invalid JSON: {error}"
                            ))
                            .with_body(Some(String::from_utf8_lossy(bytes.as_ref()).into_owned()))
                        })?;
                        return Ok(SessionSocketMessage::Payload(payload));
                    }
                    WebSocketMessage::Close(frame) => {
                        let details = frame.map(|frame| {
                            let reason = frame.reason.trim();
                            if reason.is_empty() {
                                frame.code.to_string()
                            } else {
                                format!("{} {}", frame.code, reason)
                            }
                        });
                        return Ok(SessionSocketMessage::Closed(details));
                    }
                    WebSocketMessage::Ping(_)
                    | WebSocketMessage::Pong(_)
                    | WebSocketMessage::Frame(_) => {}
                }
            }
        })
        .await
        .map_err(|_| {
            self.attach_error(
                KitError::new(
                    ErrorKind::Timeout,
                    Operation::SessionReceive,
                    ErrorPhase::SessionReceive,
                )
                .with_timeout(self.timeout),
            )
        })?
        .map_err(|error| self.attach_error(error))
    }

    pub(crate) async fn receive_payload(
        &mut self,
        deadline: Instant,
        operation: &'static str,
    ) -> Result<Value, KitError> {
        match self.receive(deadline).await.map_err(|error| {
            let error = if error.context().phase == ErrorPhase::SessionReceive {
                error.at_phase(ErrorPhase::SessionHandshake)
            } else {
                error
            };
            error.at_operation(Operation::OpenSession)
        })? {
            SessionSocketMessage::Payload(payload) => Ok(payload),
            SessionSocketMessage::Closed(Some(details)) => Err(self.attach_error(
                KitError::invalid_response(ResponseViolation::UnexpectedEnd)
                    .at_operation(Operation::OpenSession)
                    .at_phase(ErrorPhase::SessionHandshake)
                    .with_message(format!("{operation}: {details}")),
            )),
            SessionSocketMessage::Closed(None) | SessionSocketMessage::Exhausted => Err(self
                .attach_error(
                    KitError::invalid_response(ResponseViolation::UnexpectedEnd)
                        .at_operation(Operation::OpenSession)
                        .at_phase(ErrorPhase::SessionHandshake)
                        .with_message(operation),
                )),
        }
    }

    pub(crate) async fn close(&mut self, deadline: Instant) -> Result<(), KitError> {
        match tokio::time::timeout_at(deadline, SinkExt::close(&mut self.socket))
            .await
            .map_err(|_| {
                self.attach_error(
                    KitError::new(
                        ErrorKind::Timeout,
                        Operation::SessionClose,
                        ErrorPhase::SessionClose,
                    )
                    .with_timeout(self.timeout),
                )
            })? {
            Ok(()) | Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed) => {
                Ok(())
            }
            Err(error) => Err(self.attach_error(Self::socket_error(
                error,
                Operation::SessionClose,
                ErrorPhase::SessionClose,
            ))),
        }
    }

    fn deadline_after(timeout: Duration) -> Result<Instant, KitError> {
        Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| KitError::invalid_state("session timeout exceeds the clock range"))
    }

    pub(crate) fn attach_error(&self, error: KitError) -> KitError {
        let mut details = error.native_details().clone();
        details.request_id = details
            .request_id
            .or_else(|| self.metadata.request_id.clone());
        error.with_native_capture(details)
    }

    fn socket_error(error: WebSocketError, operation: Operation, phase: ErrorPhase) -> KitError {
        let message = error.to_string();
        match error {
            WebSocketError::Http(response) => {
                let status = response.status().as_u16();
                let body = response.body().as_deref();
                let truncated = response
                    .headers()
                    .get("content-length")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<usize>().ok())
                    .is_some_and(|length| length > body.map_or(0, <[u8]>::len));
                KitError::new(ErrorKind::from_http_status(status), operation, phase)
                    .with_http_status(status)
                    .with_native_details(NativeErrorDetails {
                        message: Some(message),
                        body: body.map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
                        request_id: response
                            .headers()
                            .get("x-request-id")
                            .or_else(|| response.headers().get("request-id"))
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned),
                        retry_after: response
                            .headers()
                            .get("retry-after")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned),
                        truncated,
                        ..NativeErrorDetails::default()
                    })
            }
            WebSocketError::Capacity(CapacityError::MessageTooLong { max_size, .. }) => {
                KitError::invalid_response(ResponseViolation::LimitExceeded { limit: max_size })
                    .at_operation(operation)
                    .at_phase(phase)
                    .with_message(message)
            }
            WebSocketError::Capacity(CapacityError::TooManyHeaders) => {
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "WebSocket response exceeds its header count limit",
                })
                .at_operation(operation)
                .at_phase(phase)
                .with_message(message)
            }
            WebSocketError::Utf8(_) => KitError::invalid_response(ResponseViolation::InvalidUtf8)
                .at_operation(operation)
                .at_phase(phase)
                .with_message(message),
            WebSocketError::Protocol(_) => {
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "invalid WebSocket protocol exchange",
                })
                .at_operation(operation)
                .at_phase(phase)
                .with_message(message)
            }
            WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed => {
                KitError::new(ErrorKind::Closed, operation, phase).with_message(message)
            }
            WebSocketError::WriteBufferFull(_) => {
                KitError::new(ErrorKind::InvalidState, operation, phase).with_message(message)
            }
            WebSocketError::AttackAttempt => {
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "invalid WebSocket handshake sequence",
                })
                .at_operation(operation)
                .at_phase(phase)
                .with_message(message)
            }
            WebSocketError::Url(_) => KitError::new(
                ErrorKind::Configuration,
                operation,
                ErrorPhase::RequestBuild,
            )
            .with_message(message),
            WebSocketError::HttpFormat(_) => {
                // Callers supply an already constructed HTTP request; failures
                // parsing the peer's HTTP response belong to the handshake.
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "invalid HTTP handshake response",
                })
                .at_operation(operation)
                .at_phase(phase)
                .with_message(message)
            }
            WebSocketError::Io(_) | WebSocketError::Tls(_) => {
                KitError::new(ErrorKind::Transport, operation, phase).with_message(message)
            }
        }
    }
}
