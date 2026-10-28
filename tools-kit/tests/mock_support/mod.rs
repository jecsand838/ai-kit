use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tools_kit::declaration::{ToolInfo, ToolName};
use tools_kit::execution::{ToolHandlerInput, ToolHandlerOutput};
use tools_kit::interaction::ToolInteractionRequest;
use tools_kit::{ToolError, ToolHandlerBackend, ToolsetBackend};

pub struct Transition {
    pub input: ToolHandlerInput,
    pub output: Result<ToolHandlerOutput, ToolError>,
}

pub struct HandlerScript {
    pub name: ToolName,
    pub transitions: VecDeque<Transition>,
}

#[derive(Clone)]
pub struct Observation(Arc<Mutex<Observed>>);

struct Observed {
    inventory: usize,
    requested: Vec<ToolName>,
    inputs: Vec<(usize, ToolHandlerInput)>,
    remaining: Vec<usize>,
    drops: Vec<usize>,
    shutdowns: usize,
}

impl Observation {
    pub fn inputs(&self) -> Vec<(usize, ToolHandlerInput)> {
        self.0.lock().unwrap().inputs.clone()
    }

    pub fn assert_complete(&self) {
        let complete = self.0.lock().unwrap().remaining.iter().all(|n| *n == 0);
        assert!(complete, "unconsumed handler expectations");
    }

    pub fn assert_cleanup(&self, handlers: usize) {
        let observed = self.0.lock().unwrap();
        assert_eq!(observed.shutdowns, 1);
        assert_eq!(observed.drops.len(), handlers);
    }
}

enum Admission {
    Open,
    Closed(Result<(), ToolError>),
}

struct Inventory {
    admission: Admission,
    handlers: VecDeque<HandlerScript>,
    next: usize,
}

/// A test-local owner, deliberately not Clone. Every accepted handler is unique.
pub struct ScriptedTools {
    inventory: Result<Vec<ToolInfo>, ToolError>,
    state: Mutex<Inventory>,
    shutdown_result: Result<(), ToolError>,
    observed: Observation,
}

impl ScriptedTools {
    pub fn new(
        inventory: Result<Vec<ToolInfo>, ToolError>,
        handlers: Vec<HandlerScript>,
        shutdown_result: Result<(), ToolError>,
    ) -> (Self, Observation) {
        let observed = Observation(Arc::new(Mutex::new(Observed {
            inventory: 0,
            requested: Vec::new(),
            inputs: Vec::new(),
            remaining: handlers.iter().map(|h| h.transitions.len()).collect(),
            drops: Vec::new(),
            shutdowns: 0,
        })));
        (
            Self {
                inventory,
                state: Mutex::new(Inventory {
                    admission: Admission::Open,
                    handlers: handlers.into(),
                    next: 0,
                }),
                shutdown_result,
                observed: observed.clone(),
            },
            observed,
        )
    }

    pub fn assert_complete(&self) {
        let complete = self.state.lock().unwrap().handlers.is_empty();
        assert!(complete, "unconsumed handler acquisitions");
        self.observed.assert_complete();
    }

    fn closed(operation: &str) -> ToolError {
        ToolError::Closed {
            operation: operation.to_owned(),
        }
    }
}

impl ToolsetBackend for ScriptedTools {
    type Handler = ScriptedHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        if matches!(self.state.lock().unwrap().admission, Admission::Closed(_)) {
            return Err(Self::closed("list_tools"));
        }
        self.observed.0.lock().unwrap().inventory += 1;
        self.inventory.clone()
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        let mut state = self.state.lock().unwrap();
        if matches!(state.admission, Admission::Closed(_)) {
            return Err(Self::closed("tool_handler"));
        }
        if !self
            .inventory
            .as_ref()
            .map_err(Clone::clone)?
            .iter()
            .any(|info| info.name == name)
        {
            return Err(ToolError::UnknownTool { name });
        }
        let expected = state
            .handlers
            .front()
            .expect("unexpected handler acquisition");
        assert_eq!(expected.name, name, "wrong handler request");
        let script = state.handlers.pop_front().unwrap();
        let index = state.next;
        state.next += 1;
        self.observed.0.lock().unwrap().requested.push(name.clone());
        Ok(ScriptedHandler {
            name,
            index,
            transitions: script.transitions,
            phase: HandlerPhase::New,
            observed: self.observed.clone(),
            _not_sync: std::marker::PhantomData,
        })
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        let mut state = self.state.lock().unwrap();
        match &state.admission {
            Admission::Closed(result) => result.clone(),
            Admission::Open => {
                // This fixture owns no asynchronous resource. Sealing and cleanup
                // are one immediate transition, so no waiter can detach work.
                self.observed.0.lock().unwrap().shutdowns += 1;
                state.admission = Admission::Closed(self.shutdown_result.clone());
                self.shutdown_result.clone()
            }
        }
    }
}

enum HandlerPhase {
    New,
    Pending(ToolInteractionRequest),
    Terminal,
}

pub struct ScriptedHandler {
    name: ToolName,
    index: usize,
    transitions: VecDeque<Transition>,
    phase: HandlerPhase,
    observed: Observation,
    // ToolHandlerBackend requires Send, never Sync or Clone.
    _not_sync: std::marker::PhantomData<std::cell::Cell<()>>,
}

impl ToolHandlerBackend for ScriptedHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        if self.observed.0.lock().unwrap().shutdowns != 0 {
            return Err(ScriptedTools::closed("step"));
        }
        match (&self.phase, &input) {
            (HandlerPhase::Terminal, _) => return Err(ScriptedTools::closed("step")),
            (HandlerPhase::New, ToolHandlerInput::Interaction(response)) => {
                return Err(ToolError::InvalidInteraction {
                    id: response.interaction_id().as_str().into(),
                    message: "handler has no pending interaction".into(),
                });
            }
            (HandlerPhase::Pending(request), ToolHandlerInput::Interaction(response)) => {
                if request.id != *response.interaction_id() {
                    return Err(ToolError::InvalidInteraction {
                        id: response.interaction_id().as_str().into(),
                        message: "response does not match pending interaction".into(),
                    });
                }
            }
            (HandlerPhase::Pending(_), ToolHandlerInput::Invoke(_)) => {
                return Err(ToolError::Protocol {
                    message: "handler already invoked".into(),
                });
            }
            (HandlerPhase::New, ToolHandlerInput::Invoke(_) | ToolHandlerInput::Cancel { .. })
            | (HandlerPhase::Pending(_), ToolHandlerInput::Cancel { .. }) => {}
        }
        let expected = self.transitions.front().expect("exhausted handler script");
        assert_eq!(expected.input, input, "wrong handler input");
        let transition = self.transitions.pop_front().unwrap();
        {
            let mut observed = self.observed.0.lock().unwrap();
            observed.inputs.push((self.index, input.clone()));
            observed.remaining[self.index] -= 1;
        }
        self.phase = match &transition.output {
            Ok(ToolHandlerOutput::Interaction(request)) => {
                assert_eq!(request.tool_name, self.name);
                if let ToolHandlerInput::Invoke(invocation) = input {
                    assert_eq!(request.call_id, invocation.call_id);
                }
                HandlerPhase::Pending(request.clone())
            }
            Ok(ToolHandlerOutput::Completed(_)) | Err(_) => HandlerPhase::Terminal,
        };
        transition.output
    }
}

impl Drop for ScriptedHandler {
    fn drop(&mut self) {
        self.observed.0.lock().unwrap().drops.push(self.index);
    }
}

/// Caller-owned exhaustive selection remains static at the public seam.
pub enum CallerTools {
    Primary(ScriptedTools),
    Backup(ScriptedTools),
}

pub enum CallerHandler {
    Primary(ScriptedHandler),
    Backup(ScriptedHandler),
}

impl ToolsetBackend for CallerTools {
    type Handler = CallerHandler;

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, ToolError> {
        match self {
            Self::Primary(tools) => tools.list_tools().await,
            Self::Backup(tools) => tools.list_tools().await,
        }
    }

    async fn tool_handler(&self, name: ToolName) -> Result<Self::Handler, ToolError> {
        match self {
            Self::Primary(tools) => tools.tool_handler(name).await.map(CallerHandler::Primary),
            Self::Backup(tools) => tools.tool_handler(name).await.map(CallerHandler::Backup),
        }
    }

    async fn shutdown(&self) -> Result<(), ToolError> {
        match self {
            Self::Primary(tools) => tools.shutdown().await,
            Self::Backup(tools) => tools.shutdown().await,
        }
    }
}

impl ToolHandlerBackend for CallerHandler {
    async fn step(&mut self, input: ToolHandlerInput) -> Result<ToolHandlerOutput, ToolError> {
        match self {
            Self::Primary(handler) => handler.step(input).await,
            Self::Backup(handler) => handler.step(input).await,
        }
    }
}
