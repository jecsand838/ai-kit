use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::task::{JoinError, JoinSet};

use crate::SdkError;
use crate::backends::codex::adapters::prompt::CodexStructuredOutput;
use crate::prompt::PromptOutput;

pub(super) struct CodexPromptContext {
    client_message_id: String,
    validator: Option<CodexStructuredOutput>,
    output: Option<Arc<PromptOutput>>,
    turn: CodexOutputTurn,
}

enum CodexOutputTurn {
    Pending(Option<String>),
    Accepted(String),
    Clearing(String),
}

pub(super) enum CodexPromptTasks {
    Open(JoinSet<()>),
    Sealed,
}

impl CodexPromptContext {
    pub(super) fn begin_clear(
        &mut self,
        client_message_id: &str,
        turn_id: &str,
    ) -> Result<(), SdkError> {
        if !self.owns(client_message_id) {
            return Err(SdkError::conflict(
                "prompt cleanup belongs to another generation",
            ));
        }
        match &self.turn {
            CodexOutputTurn::Accepted(owned) | CodexOutputTurn::Clearing(owned)
                if owned == turn_id =>
            {
                self.turn = CodexOutputTurn::Clearing(turn_id.to_owned());
                Ok(())
            }
            _ => Err(SdkError::invalid_state(
                "prompt cleanup lacks its accepted turn",
            )),
        }
    }

    pub(super) fn clears_turn(&self, turn_id: &str) -> bool {
        matches!(&self.turn, CodexOutputTurn::Clearing(owned) if owned == turn_id)
    }

    pub(super) fn is_clearing(&self) -> bool {
        matches!(self.turn, CodexOutputTurn::Clearing(_))
    }

    pub(super) fn new(
        client_message_id: String,
        validator: Option<CodexStructuredOutput>,
        output: Option<Arc<PromptOutput>>,
    ) -> Self {
        Self {
            client_message_id,
            validator,
            output,
            turn: CodexOutputTurn::Pending(None),
        }
    }

    pub(super) fn owns(&self, client_message_id: &str) -> bool {
        self.client_message_id == client_message_id
    }

    pub(super) fn retain_output(&mut self, output: Option<Arc<PromptOutput>>) {
        if self.output.is_none() {
            self.output = output;
        }
    }

    pub(super) fn validator(
        &mut self,
        schema: &serde_json::Value,
    ) -> Result<CodexStructuredOutput, SdkError> {
        if let Some(validator) = &self.validator {
            if validator.schema() != schema {
                return Err(SdkError::invalid_state(
                    "stored structured output validator disagrees with its schema",
                ));
            }
            return Ok(validator.clone());
        }
        let validator = CodexStructuredOutput::from_schema(schema.clone())
            .map_err(|_| SdkError::invalid_state("stored structured output schema is malformed"))?;
        self.validator = Some(validator.clone());
        Ok(validator)
    }

    pub(super) fn accept_output(&mut self, turn_id: &str) {
        let matches = match &self.turn {
            CodexOutputTurn::Pending(None) => true,
            CodexOutputTurn::Pending(Some(observed))
            | CodexOutputTurn::Accepted(observed)
            | CodexOutputTurn::Clearing(observed) => observed == turn_id,
        };
        if !matches!(self.turn, CodexOutputTurn::Clearing(_)) {
            self.turn = CodexOutputTurn::Accepted(turn_id.to_owned());
        }
        if let Some(output) = &self.output {
            if matches {
                output.admit();
            } else {
                output.finish(Err(SdkError::integration(
                    "prompt output disagreed with its authoritative turn receipt",
                )));
            }
        }
    }

    pub(super) fn fragment(&mut self, turn_id: &str, text: &str) {
        match &mut self.turn {
            CodexOutputTurn::Pending(observed) => match observed {
                Some(observed) if observed != turn_id => {
                    self.fail(&SdkError::integration(
                        "prompt output contained conflicting unaccepted turns",
                    ));
                    return;
                }
                Some(_) => {}
                None => *observed = Some(turn_id.to_owned()),
            },
            CodexOutputTurn::Clearing(_) => return,
            CodexOutputTurn::Accepted(owned) if owned != turn_id => return,
            CodexOutputTurn::Accepted(_) => {}
        }
        if let Some(output) = &self.output {
            output.fragment(text);
        }
    }

    pub(super) fn fail(&self, error: &SdkError) {
        if let Some(output) = &self.output {
            output.finish(Err(error.clone()));
        }
    }
}

impl CodexPromptTasks {
    pub(super) fn new() -> Self {
        Self::Open(JoinSet::new())
    }

    pub(super) fn spawn<F>(&mut self, future: F) -> Result<(), F>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            Self::Open(tasks) => {
                tasks.spawn(future);
                Ok(())
            }
            Self::Sealed => Err(future),
        }
    }

    pub(super) fn poll_exit(&mut self, context: &mut Context<'_>) -> Poll<Result<(), JoinError>> {
        match self {
            Self::Open(tasks) => match tasks.poll_join_next(context) {
                Poll::Ready(Some(result)) => Poll::Ready(result),
                Poll::Ready(None) | Poll::Pending => Poll::Pending,
            },
            Self::Sealed => Poll::Pending,
        }
    }

    pub(super) fn seal(&mut self) -> Option<JoinSet<()>> {
        match std::mem::replace(self, Self::Sealed) {
            Self::Open(tasks) => Some(tasks),
            Self::Sealed => None,
        }
    }
}
