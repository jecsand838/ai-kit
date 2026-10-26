use std::sync::Arc;

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    ToolError, ToolHandlerBackend,
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{ToolInteractionRequest, ToolInteractionResponse},
    sources::mcp::{
        catalog::{McpProtocol, McpToolEntry},
        client::McpRemoteCancelSlot,
        interaction::{McpActiveCall, McpElicitationEnvelope},
    },
};

/// MCP resumable handler.
pub(crate) struct McpToolHandler {
    entry: Arc<McpToolEntry>,
    state: McpHandlerState,
}

enum McpHandlerState {
    Ready,
    Running(McpRunningCall),
    Pending {
        interaction: Box<PendingMcpInteraction>,
        call: McpRunningCall,
    },
    Finished,
}

impl McpHandlerState {
    fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Finished)
    }
}

struct McpRunningCall {
    join: Option<JoinHandle<Result<ToolResult, ToolError>>>,
    interactions: mpsc::Receiver<McpElicitationEnvelope>,
    active: Option<McpActiveCall>,
    remote_cancel: McpRemoteCancelSlot,
}

impl McpRunningCall {
    async fn cancel(mut self, reason: Option<String>) {
        self.remote_cancel.cancel(reason).await;
        self.active.take();
        if let Some(join) = self.join.take() {
            join.abort();
            let _ = join.await;
        }
    }
}

impl Drop for McpRunningCall {
    fn drop(&mut self) {
        self.active.take();
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

struct PendingMcpInteraction {
    request: ToolInteractionRequest,
    response: Option<tokio::sync::oneshot::Sender<ToolInteractionResponse>>,
}

impl PendingMcpInteraction {
    fn respond(&mut self, response: ToolInteractionResponse) -> Result<(), ToolError> {
        if let Some(tx) = self.response.take() {
            tx.send(response).map_err(|_| ToolError::Transport {
                tool: Some(self.request.tool_name.clone()),
                message: "MCP interaction receiver closed".to_owned(),
            })?;
        }
        Ok(())
    }

    fn cancel(mut self, reason: Option<String>) {
        if let Some(tx) = self.response.take() {
            let _ = tx.send(ToolInteractionResponse::cancel(
                self.request.id.clone(),
                reason,
            ));
        }
    }
}

impl Drop for PendingMcpInteraction {
    fn drop(&mut self) {
        if let Some(tx) = self.response.take() {
            let _ = tx.send(ToolInteractionResponse::cancel(
                self.request.id.clone(),
                Some("MCP handler dropped before interaction resolution".to_owned()),
            ));
        }
    }
}

impl ToolHandlerBackend for McpToolHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match self.state.take() {
            McpHandlerState::Ready => self.step_ready(input).await,
            McpHandlerState::Running(call) => self.step_running(call, input).await,
            McpHandlerState::Pending { interaction, call } => {
                self.step_pending(interaction, call, input).await
            }
            McpHandlerState::Finished => Err(ToolError::InvalidConfiguration {
                message: "tool handler has already finished".to_owned(),
            }),
        }
    }
}

impl McpToolHandler {
    pub(crate) fn new(entry: Arc<McpToolEntry>) -> Self {
        Self {
            entry,
            state: McpHandlerState::Ready,
        }
    }

    async fn step_ready(
        &mut self,
        input: ToolHandlerInput,
    ) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Invoke(invocation) => {
                let call = self.start(invocation).await?;
                self.state = McpHandlerState::Running(call);
                self.await_running().await
            }
            ToolHandlerInput::Interaction(response) => Err(ToolError::InvalidInteraction {
                id: response.interaction_id().as_str().to_owned(),
                message: "no interaction is pending".to_owned(),
            }),
            ToolHandlerInput::Cancel { reason } => Err(ToolError::Cancelled {
                tool: self.entry.info().name.clone(),
                reason,
            }),
        }
    }

    async fn step_running(
        &mut self,
        call: McpRunningCall,
        input: ToolHandlerInput,
    ) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Cancel { reason } => {
                call.cancel(reason.clone()).await;
                Err(ToolError::Cancelled {
                    tool: self.entry.info().name.clone(),
                    reason,
                })
            }
            ToolHandlerInput::Invoke(_) => {
                self.state = McpHandlerState::Running(call);
                Err(ToolError::InvalidConfiguration {
                    message: "cannot invoke a handler while it is already running".to_owned(),
                })
            }
            ToolHandlerInput::Interaction(response) => {
                self.state = McpHandlerState::Running(call);
                Err(ToolError::InvalidInteraction {
                    id: response.interaction_id().as_str().to_owned(),
                    message: "no interaction is pending".to_owned(),
                })
            }
        }
    }

    async fn step_pending(
        &mut self,
        mut pending: Box<PendingMcpInteraction>,
        call: McpRunningCall,
        input: ToolHandlerInput,
    ) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Interaction(response) => {
                if let Err(error) = pending.request.validate_response(&response) {
                    self.state = McpHandlerState::Pending {
                        interaction: pending,
                        call,
                    };
                    return Err(error);
                }
                pending.respond(response)?;
                self.state = McpHandlerState::Running(call);
                self.await_running().await
            }
            ToolHandlerInput::Cancel { reason } => {
                pending.cancel(reason.clone());
                call.cancel(reason.clone()).await;
                Err(ToolError::Cancelled {
                    tool: self.entry.info().name.clone(),
                    reason,
                })
            }
            ToolHandlerInput::Invoke(_) => {
                self.state = McpHandlerState::Pending {
                    interaction: pending,
                    call,
                };
                Err(ToolError::InvalidConfiguration {
                    message: "cannot invoke a handler while interaction is pending".to_owned(),
                })
            }
        }
    }

    async fn start(&self, invocation: ToolInvocation) -> Result<McpRunningCall, ToolError> {
        let tool_name = self.entry.info().name.clone();
        let issues = self
            .entry
            .info()
            .input_schema
            .validate(&invocation.arguments)?;
        if !issues.is_empty() {
            return Err(ToolError::InvalidArguments {
                tool: tool_name,
                message: "tool arguments failed schema validation".to_owned(),
                issues,
            });
        }
        let call_id = invocation.call_id.clone();
        let broker = self.entry.client().broker();
        let (active, interactions) = broker.begin_call(tool_name.clone(), call_id.clone()).await;
        let entry = self.entry.clone();
        let remote_cancel = McpRemoteCancelSlot::new();
        let task_remote_cancel = remote_cancel.clone();
        let join = tokio::spawn(async move {
            entry
                .client()
                .start_tool_call(tool_name, invocation.arguments, task_remote_cancel)
                .await?
                .finish()
                .await
                .and_then(McpProtocol::tool_result_from_mcp)
        });
        Ok(McpRunningCall {
            join: Some(join),
            interactions,
            active: Some(active),
            remote_cancel,
        })
    }

    async fn await_running(&mut self) -> Result<ToolHandlerOutput, ToolError> {
        enum RunningEvent {
            Completed(Result<Result<ToolResult, ToolError>, tokio::task::JoinError>),
            Interaction(Option<McpElicitationEnvelope>),
        }

        let event = {
            let McpHandlerState::Running(call) = &mut self.state else {
                return Err(ToolError::InvalidConfiguration {
                    message: "MCP handler running call is missing".to_owned(),
                });
            };
            let McpRunningCall {
                join, interactions, ..
            } = call;
            let Some(join) = join.as_mut() else {
                return Err(ToolError::InvalidConfiguration {
                    message: "MCP running call is missing its worker task".to_owned(),
                });
            };
            tokio::select! {
                result = join => RunningEvent::Completed(result),
                maybe_interaction = interactions.recv() => RunningEvent::Interaction(maybe_interaction),
            }
        };

        match event {
            RunningEvent::Completed(result) => {
                self.state = McpHandlerState::Finished;
                self.output_from_join_result(result)
            }
            RunningEvent::Interaction(Some(envelope)) => {
                let (request, response) = envelope.into_parts();
                let output = request.clone();
                let McpHandlerState::Running(call) = self.state.take() else {
                    return Err(ToolError::InvalidConfiguration {
                        message: "MCP handler running call is missing".to_owned(),
                    });
                };
                self.state = McpHandlerState::Pending {
                    interaction: Box::new(PendingMcpInteraction {
                        request,
                        response: Some(response),
                    }),
                    call,
                };
                Ok(ToolHandlerOutput::Interaction(output))
            }
            RunningEvent::Interaction(None) => {
                let result = {
                    let McpHandlerState::Running(call) = &mut self.state else {
                        return Err(ToolError::InvalidConfiguration {
                            message: "MCP handler running call is missing".to_owned(),
                        });
                    };
                    let Some(join) = call.join.as_mut() else {
                        return Err(ToolError::InvalidConfiguration {
                            message: "MCP running call is missing its worker task".to_owned(),
                        });
                    };
                    join.await
                };
                self.state = McpHandlerState::Finished;
                self.output_from_join_result(result)
            }
        }
    }

    fn output_from_join_result(
        &self,
        result: Result<Result<ToolResult, ToolError>, tokio::task::JoinError>,
    ) -> Result<ToolHandlerOutput, ToolError> {
        match result {
            Ok(Ok(result)) => self.output_from_tool_result(result),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(ToolError::Transport {
                tool: Some(self.entry.info().name.clone()),
                message: format!("MCP tool task failed: {error}"),
            }),
        }
    }

    fn output_from_tool_result(&self, result: ToolResult) -> Result<ToolHandlerOutput, ToolError> {
        self.entry.info().validate_output(&result)?;
        Ok(ToolHandlerOutput::Completed(result))
    }
}
