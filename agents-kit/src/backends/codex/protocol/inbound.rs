use serde_json::Value;

use crate::KitError;
use crate::backends::codex::protocol::notifications::CodexServerNotification;
use crate::backends::codex::protocol::requests::CodexServerRequest;

pub(crate) enum CodexInbound {
    Request(CodexServerRequest),
    MalformedRequest { request_id: Value, error: KitError },
    Notification(CodexServerNotification),
}

pub(crate) enum CodexInboundFrame {
    ServerRequest {
        request_id: Value,
        method: String,
        params: Value,
    },
    ServerNotification {
        method: String,
        params: Value,
    },
}

impl CodexInbound {
    pub(crate) fn decode(frame: CodexInboundFrame) -> Result<Self, KitError> {
        match frame {
            CodexInboundFrame::ServerRequest {
                request_id,
                method,
                params,
            } => match CodexServerRequest::decode(request_id.clone(), &method, params) {
                Ok(request) => Ok(Self::Request(request)),
                Err(error) => Ok(Self::MalformedRequest { request_id, error }),
            },
            CodexInboundFrame::ServerNotification { method, params } => {
                CodexServerNotification::decode(method, params).map(Self::Notification)
            }
        }
    }
}

impl CodexInboundFrame {
    pub(crate) fn request(request_id: Value, method: String, params: Value) -> Self {
        Self::ServerRequest {
            request_id,
            method,
            params,
        }
    }

    pub(crate) fn notification(method: String, params: Value) -> Self {
        Self::ServerNotification { method, params }
    }
}
