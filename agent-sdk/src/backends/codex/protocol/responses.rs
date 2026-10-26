use serde_json::Value;

use crate::backends::codex::server_requests::{CodexServerRendering, CodexServerRenderingOutcome};

pub(crate) struct CodexServerResponse {
    value: Value,
}

impl CodexServerResponse {
    pub(crate) fn from_rendering(rendering: CodexServerRendering) -> Self {
        let (request_id, outcome) = rendering.into_parts();
        let value = match outcome {
            CodexServerRenderingOutcome::Success(result) => serde_json::json!({
                "id": request_id,
                "result": result,
            }),
            CodexServerRenderingOutcome::Error { code, message } => serde_json::json!({
                "id": request_id,
                "error": {
                    "code": code,
                    "message": message,
                },
            }),
        };
        Self { value }
    }

    pub(crate) fn into_value(self) -> Value {
        self.value
    }
}
