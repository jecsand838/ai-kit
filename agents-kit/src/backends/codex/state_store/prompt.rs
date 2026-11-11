use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::task::{JoinError, JoinSet};

use crate::backends::codex::adapters::prompt::CodexStructuredOutput;
use crate::prompt::{PromptMilestone, PromptOutput};
use crate::{KitError, NotificationId};

pub(super) struct CodexPromptContext {
    client_message_id: String,
    validator: Option<CodexStructuredOutput>,
    output: Option<Arc<PromptOutput>>,
    turn: CodexOutputTurn,
    progress: CodexPromptProgress,
}

enum CodexPromptProgress {
    Ineligible,
    Live {
        thread_id: String,
        deadline: Option<tokio::time::Instant>,
        lifecycle_timeout: std::time::Duration,
        publication: CodexMilestonePublication,
    },
}

enum CodexMilestonePublication {
    Ready(PromptMilestone),
    Pending {
        milestone: PromptMilestone,
        id: NotificationId,
    },
    Complete,
    Failed(KitError),
}

enum CodexOutputTurn {
    Pending(Option<String>),
    PendingTerminal(String),
    Accepted(String),
    Terminal(String),
    Clearing(String),
}

pub(crate) struct CodexPromptEffect {
    context: Arc<Mutex<CodexPromptContext>>,
    turn_id: String,
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
    ) -> Result<(), KitError> {
        if !self.owns(client_message_id) {
            return Err(KitError::conflict(
                "prompt cleanup belongs to another generation",
            ));
        }
        match &self.turn {
            CodexOutputTurn::Accepted(owned)
            | CodexOutputTurn::Terminal(owned)
            | CodexOutputTurn::Clearing(owned)
                if owned == turn_id =>
            {
                self.turn = CodexOutputTurn::Clearing(turn_id.to_owned());
                Ok(())
            }
            _ => Err(KitError::invalid_state(
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
            progress: CodexPromptProgress::Ineligible,
        }
    }

    pub(super) fn enable_progress(
        &mut self,
        thread_id: &str,
        deadline: Option<tokio::time::Instant>,
        lifecycle_timeout: std::time::Duration,
    ) -> Result<(), KitError> {
        if !matches!(self.progress, CodexPromptProgress::Ineligible) {
            return Err(KitError::invalid_state(
                "prompt progress already has its live owner",
            ));
        }
        self.progress = CodexPromptProgress::Live {
            thread_id: thread_id.to_owned(),
            deadline,
            lifecycle_timeout,
            publication: CodexMilestonePublication::Ready(PromptMilestone::Preparing),
        };
        Ok(())
    }

    pub(super) fn claim_progress(
        &mut self,
        milestone: PromptMilestone,
        turn_id: Option<&str>,
    ) -> Option<(NotificationId, tokio::time::Instant)> {
        if matches!(
            self.turn,
            CodexOutputTurn::PendingTerminal(_)
                | CodexOutputTurn::Terminal(_)
                | CodexOutputTurn::Clearing(_)
        ) {
            return None;
        }
        if matches!(milestone, PromptMilestone::ToolCallsRequested)
            && !matches!((&self.turn, turn_id), (CodexOutputTurn::Accepted(owned), Some(turn)) if owned == turn)
        {
            return None;
        }
        let CodexPromptProgress::Live {
            deadline,
            lifecycle_timeout,
            publication,
            ..
        } = &mut self.progress
        else {
            return None;
        };
        let matches = matches!(
            (&publication, milestone),
            (
                CodexMilestonePublication::Ready(PromptMilestone::Preparing),
                PromptMilestone::Preparing
            ) | (
                CodexMilestonePublication::Ready(PromptMilestone::ToolCallsRequested),
                PromptMilestone::ToolCallsRequested
            )
        );
        if !matches {
            return None;
        }
        let lifecycle_deadline = tokio::time::Instant::now() + *lifecycle_timeout;
        let publication_deadline = deadline.map_or(lifecycle_deadline, |deadline| {
            deadline.min(lifecycle_deadline)
        });
        let id = NotificationId::new();
        *publication = CodexMilestonePublication::Pending {
            milestone,
            id: id.clone(),
        };
        Some((id, publication_deadline))
    }

    pub(super) fn settle_progress(&mut self, id: &NotificationId, result: &Result<(), KitError>) {
        let CodexPromptProgress::Live { publication, .. } = &mut self.progress else {
            return;
        };
        let CodexMilestonePublication::Pending {
            milestone,
            id: claimed,
        } = publication
        else {
            return;
        };
        if claimed != id {
            return;
        }
        *publication = match result {
            Err(error) => CodexMilestonePublication::Failed(error.clone()),
            Ok(()) => match milestone {
                PromptMilestone::Preparing => {
                    CodexMilestonePublication::Ready(PromptMilestone::ToolCallsRequested)
                }
                PromptMilestone::ToolCallsRequested => CodexMilestonePublication::Complete,
            },
        };
    }

    pub(super) fn progress_pending(&self) -> bool {
        matches!(
            &self.progress,
            CodexPromptProgress::Live {
                publication: CodexMilestonePublication::Pending { .. },
                ..
            }
        )
    }

    pub(super) fn progress_failure(&self) -> Option<KitError> {
        match &self.progress {
            CodexPromptProgress::Live {
                publication: CodexMilestonePublication::Failed(error),
                ..
            } => Some(error.clone()),
            _ => None,
        }
    }

    pub(super) fn observe_terminal(&mut self, turn_id: &str) {
        match &self.turn {
            CodexOutputTurn::Pending(None) => {
                self.turn = CodexOutputTurn::PendingTerminal(turn_id.to_owned());
            }
            CodexOutputTurn::Pending(Some(owned)) if owned == turn_id => {
                self.turn = CodexOutputTurn::PendingTerminal(turn_id.to_owned());
            }
            CodexOutputTurn::Accepted(owned) if owned == turn_id => {
                self.turn = CodexOutputTurn::Terminal(turn_id.to_owned());
            }
            _ => {}
        }
    }

    fn matches_effect(&self, thread_id: &str, turn_id: &str) -> bool {
        matches!(&self.progress, CodexPromptProgress::Live { thread_id: owned, .. } if owned == thread_id)
            && self.matches_turn(turn_id)
    }

    fn matches_turn(&self, turn_id: &str) -> bool {
        match &self.turn {
            CodexOutputTurn::Pending(None) => true,
            CodexOutputTurn::Pending(Some(owned))
            | CodexOutputTurn::PendingTerminal(owned)
            | CodexOutputTurn::Accepted(owned)
            | CodexOutputTurn::Terminal(owned)
            | CodexOutputTurn::Clearing(owned) => owned == turn_id,
        }
    }

    fn effects_allowed(&self, turn_id: &str) -> bool {
        self.matches_turn(turn_id)
            && !matches!(
                self.turn,
                CodexOutputTurn::PendingTerminal(_)
                    | CodexOutputTurn::Terminal(_)
                    | CodexOutputTurn::Clearing(_)
            )
            && matches!(&self.progress, CodexPromptProgress::Live { deadline, publication, .. }
                if deadline.is_none_or(|deadline| tokio::time::Instant::now() < deadline)
                    && !matches!(publication, CodexMilestonePublication::Failed(_)))
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
    ) -> Result<CodexStructuredOutput, KitError> {
        if let Some(validator) = &self.validator {
            if validator.schema() != schema {
                return Err(KitError::invalid_state(
                    "stored structured output validator disagrees with its schema",
                ));
            }
            return Ok(validator.clone());
        }
        let validator = CodexStructuredOutput::from_schema(schema.clone())
            .map_err(|_| KitError::invalid_state("stored structured output schema is malformed"))?;
        self.validator = Some(validator.clone());
        Ok(validator)
    }

    pub(super) fn accept_output(&mut self, turn_id: &str) {
        let matches = self.matches_turn(turn_id);
        self.turn = match &self.turn {
            CodexOutputTurn::Clearing(owned) => CodexOutputTurn::Clearing(owned.clone()),
            CodexOutputTurn::PendingTerminal(owned) | CodexOutputTurn::Terminal(owned)
                if owned == turn_id =>
            {
                CodexOutputTurn::Terminal(owned.clone())
            }
            _ => CodexOutputTurn::Accepted(turn_id.to_owned()),
        };
        if let Some(output) = &self.output {
            if matches {
                output.admit();
            } else {
                output.finish(Err(KitError::integration(
                    "prompt output disagreed with its authoritative turn receipt",
                )));
            }
        }
    }

    pub(super) fn fragment(&mut self, turn_id: &str, text: &str) {
        match &mut self.turn {
            CodexOutputTurn::Pending(observed) => match observed {
                Some(observed) if observed != turn_id => {
                    self.fail(&KitError::integration(
                        "prompt output contained conflicting unaccepted turns",
                    ));
                    return;
                }
                Some(_) => {}
                None => *observed = Some(turn_id.to_owned()),
            },
            CodexOutputTurn::PendingTerminal(_)
            | CodexOutputTurn::Terminal(_)
            | CodexOutputTurn::Clearing(_) => return,
            CodexOutputTurn::Accepted(owned) if owned != turn_id => return,
            CodexOutputTurn::Accepted(_) => {}
        }
        if let Some(output) = &self.output {
            output.fragment(text);
        }
    }

    pub(super) fn fail(&self, error: &KitError) {
        if let Some(output) = &self.output {
            output.finish(Err(error.clone()));
        }
    }
}

impl CodexPromptEffect {
    pub(super) fn capture(
        context: &Arc<Mutex<CodexPromptContext>>,
        thread_id: &str,
        turn_id: &str,
    ) -> Option<Self> {
        context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .matches_effect(thread_id, turn_id)
            .then(|| Self {
                context: Arc::clone(context),
                turn_id: turn_id.to_owned(),
            })
    }

    pub(crate) fn bound_deadline(&self, proposed: tokio::time::Instant) -> tokio::time::Instant {
        let context = self
            .context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &context.progress {
            CodexPromptProgress::Live {
                deadline: Some(deadline),
                ..
            } => proposed.min(*deadline),
            _ => proposed,
        }
    }

    pub(crate) fn interruption_error(&self) -> KitError {
        let context = self
            .context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(error) = context.progress_failure() {
            return error;
        }
        if matches!(&context.progress, CodexPromptProgress::Live { deadline: Some(deadline), .. }
            if tokio::time::Instant::now() >= *deadline)
        {
            KitError::timeout("prompt deadline elapsed")
        } else {
            KitError::cancelled("managed tool generation ended")
        }
    }

    pub(crate) fn allows_effects(&self) -> bool {
        self.context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .effects_allowed(&self.turn_id)
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
