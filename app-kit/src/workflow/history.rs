use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::workflow::run::RunRecord;
use crate::workflow::{RunId, RunSelector, RunStatus, WorkflowId, WorkflowState};
use crate::{AppError, WorkspaceId};

/// The result of one successfully committed action, distinct from user state.
///
/// See [the execution and history workflow](crate::workflow#execute-observe-restore-and-delete).
#[non_exhaustive]
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct ActionOutcome {
    /// Node that produced this result.
    pub node: String,
    /// Semantic action ordinal, independent of control-checkpoint sequence.
    pub ordinal: u64,
    /// Exact result fields, distinct from durable workflow state.
    pub result: Map<String, Value>,
}

/// The boundary represented by one retained checkpoint.
pub enum CheckpointKind {
    /// Initial state admitted before any action executes.
    Initial,
    /// State-preserving control transition.
    Control,
    /// Successful action commit.
    Action,
    /// A failed action observation; committed state and position remain unchanged.
    Failed {
        /// Failed activation, or the canonical representative of a failed BSP wave.
        node: String,
        /// Proposed action ordinal of that failed activation.
        ordinal: u64,
    },
    /// Terminal workflow transition.
    Terminal,
}

/// A committed workflow boundary and its original run identity.
///
/// See [the execution and history workflow](crate::workflow#execute-observe-restore-and-delete).
#[non_exhaustive]
pub struct Checkpoint {
    /// Run containing this committed boundary.
    pub run: RunId,
    /// Exact admitted occurrence containing this boundary.
    pub occurrence: String,
    /// Monotonic checkpoint sequence within the run.
    pub sequence: u64,
    /// Kind of committed boundary.
    pub kind: CheckpointKind,
    /// Logical workflow identity.
    pub workflow: WorkflowId,
    /// Exact workflow revision used by this run.
    pub revision: String,
    /// Last successfully committed action ordinal, if any.
    pub action_ordinal: Option<u64>,
    /// Last successfully committed action position, if any.
    pub position: Option<String>,
    /// Immutable committed user state.
    pub state: WorkflowState,
    /// Durable lifecycle at this boundary. Historical reads do not reconstruct
    /// actionable targets; a running boundary may contain a pending control.
    pub status: RunStatus,
    /// Results committed at this boundary; control transitions invent none.
    pub outcomes: Vec<ActionOutcome>,
    /// Source boundary when this run was restored into a new identity.
    pub lineage: Option<RunLineage>,
}

/// The source boundary from which a fresh run was restored.
///
/// See [the execution and history workflow](crate::workflow#execute-observe-restore-and-delete).
#[non_exhaustive]
pub struct RunLineage {
    /// Original run; restoring does not overwrite this identity.
    pub source: RunId,
    /// Exact occurrence of the selected source boundary.
    pub source_occurrence: String,
    /// Selected committed source checkpoint.
    pub checkpoint: u64,
    /// Selected action ordinal, distinct from the source's last committed ordinal.
    ///
    /// An explicit action selector preserves its ordinal. Other selectors use
    /// a failed observation's attempted ordinal or the last committed ordinal.
    pub action_ordinal: Option<u64>,
}

/// One scoped retained-history selection, independent of storage revisions.
///
/// Its serialized form uses `kind` and, for sequence/action selections, `value`.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CheckpointSelector {
    /// The latest eligible observation in the selected operation.
    ///
    /// Run checkpoint reads use the current App checkpoint. Restore and child
    /// initializers use the latest saved successful or failed action.
    Latest,
    /// One exact immutable checkpoint sequence.
    Sequence(u64),
    /// The latest successful or failed observation of this action ordinal.
    Action(u64),
}

/// The saved execution boundary used when restoring a fresh run.
///
/// Serializes as `after_completed`, `at_saved`, or `entrypoint`.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumePosition {
    /// Continue after the selected successful action or complete BSP wave.
    AfterCompleted,
    /// Retry the saved action or complete uncommitted BSP wave.
    AtSaved,
    /// Restart from the definition's entrypoint without prior-action selection.
    Entrypoint,
}

/// Selects and copies clean retained state into a new, parked run.
///
/// A fallback is used only when the source or selected checkpoint is absent; it
/// does not silently replace uncertainty or receive overrides intended for loaded
/// state. A successful restore copies clean state into a fresh paused run, so
/// later source deletion cannot strand the copy.
///
/// See [the execution and history workflow](crate::workflow#execute-observe-restore-and-delete).
pub struct RestoreOptions {
    /// Original run, which is never overwritten by restore.
    pub source: RunSelector,
    /// Exact source selection, or the latest saved successful or failed action.
    pub checkpoint: CheckpointSelector,
    /// State overrides validated before admission of the copy.
    pub overrides: WorkflowState,
    /// Where explicit execution of the copied run begins.
    pub position: ResumePosition,
    /// Explicit initial state used only when the requested checkpoint is absent.
    pub fallback: Option<WorkflowState>,
}

/// One bounded page of immutable checkpoints in ascending sequence order.
///
/// See [the execution and history workflow](crate::workflow#execute-observe-restore-and-delete).
#[non_exhaustive]
pub struct HistoryPage {
    /// Complete retained checkpoints from the captured head cutoff.
    pub checkpoints: Vec<Checkpoint>,
    /// Continues the same cutoff and scope; absent when that history is exhausted.
    pub next: Option<HistoryCursor>,
}

/// An opaque continuation of a captured run-history view.
///
/// Serialization preserves its App generation, run creation scope and committed
/// head cutoff. It is not a native storage cursor and cannot cross run scopes.
///
/// See [the execution and history workflow](crate::workflow#execute-observe-restore-and-delete).
#[derive(Clone, Debug)]
pub struct HistoryCursor {
    workspace: WorkspaceId,
    workflow: WorkflowId,
    run: RunId,
    generation: u64,
    creation: String,
    cutoff: u64,
    after: u64,
}

impl HistoryCursor {
    pub(crate) fn issue(
        head: &RunRecord,
        generation: u64,
        creation: &str,
        after: u64,
    ) -> Result<Self, AppError> {
        Self {
            workspace: head.workspace_id.clone(),
            workflow: head.workflow_id.clone(),
            run: head.id.clone(),
            generation,
            creation: creation.to_owned(),
            cutoff: head.checkpoint.sequence,
            after,
        }
        .validated()
    }

    pub(crate) fn continue_from(
        &self,
        head: &RunRecord,
        generation: u64,
        creation: &str,
    ) -> Result<(u64, u64), AppError> {
        if self.workspace != head.workspace_id
            || self.workflow != head.workflow_id
            || self.run != head.id
            || self.generation != generation
            || self.creation != creation
            || self.cutoff > head.checkpoint.sequence
        {
            return Err(AppError::InvalidInput(
                "history cursor scope is invalid".into(),
            ));
        }
        Ok((self.cutoff, self.after))
    }

    pub(crate) fn advance_to(&self, after: u64) -> Result<Self, AppError> {
        if after <= self.after {
            return Err(AppError::Integration("history page did not advance".into()));
        }
        Self {
            after,
            ..self.clone()
        }
        .validated()
    }

    fn validated(self) -> Result<Self, AppError> {
        if self.generation == 0
            || self.creation.len() != 32
            || !self
                .creation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.after >= self.cutoff
        {
            return Err(AppError::InvalidInput("invalid history cursor".into()));
        }
        Ok(self)
    }
}

impl Serialize for HistoryCursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (
            1u8,
            &self.workspace,
            &self.workflow,
            &self.run,
            self.generation,
            &self.creation,
            self.cutoff,
            self.after,
        )
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HistoryCursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (version, workspace, workflow, run, generation, creation, cutoff, after) =
            <(u8, WorkspaceId, WorkflowId, RunId, u64, String, u64, u64)>::deserialize(
                deserializer,
            )?;
        if version != 1 {
            return Err(serde::de::Error::custom("invalid history cursor"));
        }
        Self {
            workspace,
            workflow,
            run,
            generation,
            creation,
            cutoff,
            after,
        }
        .validated()
        .map_err(serde::de::Error::custom)
    }
}
