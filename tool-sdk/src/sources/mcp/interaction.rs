use std::sync::{Arc, Mutex, MutexGuard};

use rmcp::model::{CreateElicitationRequestParams, CreateElicitationResult, ElicitationAction};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, mpsc, oneshot};
use uuid::Uuid;

use crate::{
    ToolError,
    interaction::{
        ToolInteractionId, ToolInteractionKind, ToolInteractionRequest, ToolInteractionResponse,
    },
    name::ToolName,
};

#[derive(Clone, Debug)]
pub(crate) struct McpInteractionBroker {
    form_enabled: bool,
    schema_validation: bool,
    call_lock: Arc<AsyncMutex<()>>,
    active: Arc<Mutex<Option<McpActiveContext>>>,
}

pub(crate) struct McpElicitationEnvelope {
    request: ToolInteractionRequest,
    response: oneshot::Sender<ToolInteractionResponse>,
}

impl McpElicitationEnvelope {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ToolInteractionRequest,
        oneshot::Sender<ToolInteractionResponse>,
    ) {
        (self.request, self.response)
    }
}

#[derive(Clone, Debug)]
struct McpActiveContext {
    id: Uuid,
    tool_name: ToolName,
    call_id: Option<String>,
    sender: mpsc::Sender<McpElicitationEnvelope>,
}

pub(crate) struct McpActiveCall {
    broker: McpInteractionBroker,
    id: Option<Uuid>,
    _guard: Option<OwnedMutexGuard<()>>,
}

impl McpInteractionBroker {
    pub(crate) fn new(form_enabled: bool, schema_validation: bool) -> Self {
        Self {
            form_enabled,
            schema_validation,
            call_lock: Arc::new(AsyncMutex::new(())),
            active: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn form_enabled(&self) -> bool {
        self.form_enabled
    }

    pub(crate) fn schema_validation(&self) -> bool {
        self.schema_validation
    }

    pub(crate) async fn begin_call(
        &self,
        tool_name: ToolName,
        call_id: Option<String>,
    ) -> (McpActiveCall, mpsc::Receiver<McpElicitationEnvelope>) {
        let guard = if self.form_enabled {
            Some(self.call_lock.clone().lock_owned().await)
        } else {
            None
        };
        let (tx, rx) = mpsc::channel(1);
        let id = if self.form_enabled {
            let id = Uuid::new_v4();
            *self.active_lock() = Some(McpActiveContext {
                id,
                tool_name,
                call_id,
                sender: tx,
            });
            Some(id)
        } else {
            None
        };
        (
            McpActiveCall {
                broker: self.clone(),
                id,
                _guard: guard,
            },
            rx,
        )
    }

    fn active_lock(&self) -> MutexGuard<'_, Option<McpActiveContext>> {
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn active_context(&self) -> Result<McpActiveContext, ToolError> {
        self.active_lock()
            .as_ref()
            .cloned()
            .ok_or_else(|| ToolError::Protocol {
                message: "MCP server requested elicitation outside an active tool call".to_owned(),
            })
    }

    fn clear_active_if_current(&self, id: Uuid) {
        let mut active = self.active_lock();
        if active.as_ref().is_some_and(|current| current.id == id) {
            *active = None;
        }
    }

    pub(crate) async fn request_elicitation(
        &self,
        params: CreateElicitationRequestParams,
    ) -> Result<CreateElicitationResult, ToolError> {
        if !self.form_enabled {
            return Err(ToolError::Unsupported {
                operation: "MCP form elicitation is not enabled".to_owned(),
            });
        }
        let (prompt, schema) = match params {
            CreateElicitationRequestParams::FormElicitationParams {
                message,
                requested_schema,
                ..
            } => (
                message,
                serde_json::to_value(requested_schema).map_err(|error| ToolError::Protocol {
                    message: format!("failed to serialize MCP elicitation schema: {error}"),
                })?,
            ),
            CreateElicitationRequestParams::UrlElicitationParams { .. } => {
                return Err(ToolError::Unsupported {
                    operation: "MCP URL elicitation".to_owned(),
                });
            }
        };

        let active = self.active_context()?;
        let request = ToolInteractionRequest {
            id: ToolInteractionId::new(),
            tool_name: active.tool_name.clone(),
            call_id: active.call_id.clone(),
            prompt,
            kind: ToolInteractionKind::Form {
                schema,
                schema_validation: self.schema_validation,
            },
        };
        let (tx, rx) = oneshot::channel();
        active
            .sender
            .send(McpElicitationEnvelope {
                request: request.clone(),
                response: tx,
            })
            .await
            .map_err(|_| ToolError::Transport {
                tool: Some(active.tool_name.clone()),
                message: "MCP interaction channel closed".to_owned(),
            })?;
        let response = rx.await.map_err(|_| ToolError::Cancelled {
            tool: active.tool_name.clone(),
            reason: Some("MCP interaction response channel closed".to_owned()),
        })?;
        request.validate_response(&response)?;
        Ok(match response {
            ToolInteractionResponse::Submit { content, .. } => CreateElicitationResult {
                action: ElicitationAction::Accept,
                content: Some(content),
                meta: None,
            },
            ToolInteractionResponse::Decline { .. } => CreateElicitationResult {
                action: ElicitationAction::Decline,
                content: None,
                meta: None,
            },
            ToolInteractionResponse::Cancel { .. } => CreateElicitationResult {
                action: ElicitationAction::Cancel,
                content: None,
                meta: None,
            },
        })
    }
}

impl Drop for McpActiveCall {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.broker.clear_active_if_current(id);
        }
    }
}
