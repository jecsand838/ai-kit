use std::sync::Arc;

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    ToolError, ToolHandlerBackend,
    execution::{ToolHandlerInput, ToolHandlerOutput, ToolInvocation, ToolResult},
    interaction::{ToolInteractionRequest, ToolInteractionResponse},
    local::{LocalInteractionEnvelope, RegisteredTool, ToolInvocationContext},
};

/// Local resumable handler.
pub(crate) struct LocalToolHandler {
    entry: Arc<RegisteredTool>,
    state: LocalHandlerState,
}

enum LocalHandlerState {
    Ready,
    Running(LocalRunningCall),
    Pending {
        request: Box<ToolInteractionRequest>,
        response: Option<tokio::sync::oneshot::Sender<ToolInteractionResponse>>,
        call: LocalRunningCall,
    },
    Finished,
}

struct LocalRunningCall {
    join: Option<JoinHandle<Result<ToolResult, ToolError>>>,
    interactions: mpsc::Receiver<LocalInteractionEnvelope>,
}

impl LocalRunningCall {
    async fn cancel(mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
            let _ = join.await;
        }
    }
}

impl Drop for LocalRunningCall {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

impl LocalHandlerState {
    fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Finished)
    }
}

impl ToolHandlerBackend for LocalToolHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match self.state.take() {
            LocalHandlerState::Ready => self.step_ready(input).await,
            LocalHandlerState::Running(call) => self.step_running(call, input).await,
            LocalHandlerState::Pending {
                request,
                response,
                call,
            } => self.step_pending(request, response, call, input).await,
            LocalHandlerState::Finished => Err(ToolError::InvalidConfiguration {
                message: "tool handler has already finished".to_owned(),
            }),
        }
    }
}

impl LocalToolHandler {
    pub(crate) fn new(entry: Arc<RegisteredTool>) -> Self {
        Self {
            entry,
            state: LocalHandlerState::Ready,
        }
    }

    async fn step_ready(
        &mut self,
        input: ToolHandlerInput,
    ) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Invoke(invocation) => {
                let call = self.start(invocation)?;
                self.state = LocalHandlerState::Running(call);
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
        call: LocalRunningCall,
        input: ToolHandlerInput,
    ) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Cancel { reason } => {
                call.cancel().await;
                Err(ToolError::Cancelled {
                    tool: self.entry.info().name.clone(),
                    reason,
                })
            }
            ToolHandlerInput::Invoke(_) => {
                self.state = LocalHandlerState::Running(call);
                Err(ToolError::InvalidConfiguration {
                    message: "cannot invoke a handler while it is already running".to_owned(),
                })
            }
            ToolHandlerInput::Interaction(response) => {
                self.state = LocalHandlerState::Running(call);
                Err(ToolError::InvalidInteraction {
                    id: response.interaction_id().as_str().to_owned(),
                    message: "no interaction is pending".to_owned(),
                })
            }
        }
    }

    async fn step_pending(
        &mut self,
        request: Box<ToolInteractionRequest>,
        mut response: Option<tokio::sync::oneshot::Sender<ToolInteractionResponse>>,
        call: LocalRunningCall,
        input: ToolHandlerInput,
    ) -> Result<ToolHandlerOutput, ToolError> {
        match input {
            ToolHandlerInput::Interaction(resolution) => {
                if let Err(error) = request.validate_response(&resolution) {
                    self.state = LocalHandlerState::Pending {
                        request,
                        response,
                        call,
                    };
                    return Err(error);
                }
                if let Some(tx) = response.take() {
                    tx.send(resolution).map_err(|_| ToolError::Transport {
                        tool: Some(self.entry.info().name.clone()),
                        message: "local interaction receiver closed".to_owned(),
                    })?;
                }
                self.state = LocalHandlerState::Running(call);
                self.await_running().await
            }
            ToolHandlerInput::Cancel { reason } => {
                if let Some(tx) = response.take() {
                    let _ = tx.send(ToolInteractionResponse::cancel(
                        request.id.clone(),
                        reason.clone(),
                    ));
                }
                call.cancel().await;
                Err(ToolError::Cancelled {
                    tool: self.entry.info().name.clone(),
                    reason,
                })
            }
            ToolHandlerInput::Invoke(_) => {
                self.state = LocalHandlerState::Pending {
                    request,
                    response,
                    call,
                };
                Err(ToolError::InvalidConfiguration {
                    message: "cannot invoke a handler while interaction is pending".to_owned(),
                })
            }
        }
    }

    fn start(&self, invocation: ToolInvocation) -> Result<LocalRunningCall, ToolError> {
        let (tx, rx) = mpsc::channel(1);
        let entry = Arc::clone(&self.entry);
        let context = ToolInvocationContext::new(
            entry.info().name.clone(),
            invocation.call_id.clone(),
            tx,
            entry.info().interactions.clone(),
        );
        let call = entry.call(invocation, context)?;
        let join = tokio::spawn(call.run());
        Ok(LocalRunningCall {
            join: Some(join),
            interactions: rx,
        })
    }

    async fn await_running(&mut self) -> Result<ToolHandlerOutput, ToolError> {
        enum RunningEvent {
            Completed(Result<Result<ToolResult, ToolError>, tokio::task::JoinError>),
            Interaction(Option<LocalInteractionEnvelope>),
        }

        let event = {
            let LocalHandlerState::Running(call) = &mut self.state else {
                return Err(ToolError::InvalidConfiguration {
                    message: "local handler running call is missing".to_owned(),
                });
            };
            let LocalRunningCall { join, interactions } = call;
            let Some(join) = join.as_mut() else {
                return Err(ToolError::InvalidConfiguration {
                    message: "local running call is missing its worker task".to_owned(),
                });
            };
            tokio::select! {
                result = join => RunningEvent::Completed(result),
                maybe_interaction = interactions.recv() => RunningEvent::Interaction(maybe_interaction),
            }
        };

        match event {
            RunningEvent::Completed(result) => {
                self.state = LocalHandlerState::Finished;
                self.output_from_join_result(result)
            }
            RunningEvent::Interaction(Some(envelope)) => {
                let (request, response) = envelope.into_parts();
                let output = request.clone();
                let LocalHandlerState::Running(call) = self.state.take() else {
                    return Err(ToolError::InvalidConfiguration {
                        message: "local handler running call is missing".to_owned(),
                    });
                };
                self.state = LocalHandlerState::Pending {
                    request: Box::new(request),
                    response: Some(response),
                    call,
                };
                Ok(ToolHandlerOutput::Interaction(output))
            }
            RunningEvent::Interaction(None) => {
                let result = {
                    let LocalHandlerState::Running(call) = &mut self.state else {
                        return Err(ToolError::InvalidConfiguration {
                            message: "local handler running call is missing".to_owned(),
                        });
                    };
                    let Some(join) = call.join.as_mut() else {
                        return Err(ToolError::InvalidConfiguration {
                            message: "local running call is missing its worker task".to_owned(),
                        });
                    };
                    join.await
                };
                self.state = LocalHandlerState::Finished;
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
                message: format!("local tool task failed: {error}"),
            }),
        }
    }

    fn output_from_tool_result(&self, result: ToolResult) -> Result<ToolHandlerOutput, ToolError> {
        self.entry.info().validate_output(&result)?;
        Ok(ToolHandlerOutput::Completed(result))
    }
}
