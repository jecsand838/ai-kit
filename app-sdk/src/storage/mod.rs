use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage_sdk::state::record::{
    StateKey, StateKeyPrefix, StateListRequest, StateRecord, StateRecordSnapshot,
};
use storage_sdk::{StateStoreBackend, StateStoreError, StateTransactionBackend};
use uuid::Uuid;

use crate::app::{AppRecord, AppTrace, MAX_AGENTS, MAX_RUNS, ShutdownRecord};
use crate::workflow::MAX_FRONTIER;
use crate::workflow::definition::{CompiledDefinition, SchemaText};
use crate::workflow::run::{
    CheckpointPhaseRecord, ControlRecordState, InteractionSettlement, MAX_INTERACTION_SETTLEMENTS,
    MAX_LIST_LIMIT, RunKey, RunRecord, RuntimeOperationFailure,
};
use crate::workflow::{
    CheckpointKind, CheckpointSelector, HistoryCursor, RunId, RunSelector, StagedOutcomeRecord,
    WorkflowId,
};
use crate::workspace::{MAX_WORKSPACES, WorkspaceRecord};
use crate::{AppError, WorkspaceId};

mod migration;
mod occurrence;

pub(crate) use migration::AppMigration;
pub(crate) use occurrence::RunCopy;
use occurrence::{RunIndex, RunProjection};

pub(crate) const EPOCH: &str = "app/v7";
const LEGACY_EPOCH: &str = "app/v6";
const NAMESPACE: &str = "app-sdk";
const PAGE_SIZE: usize = 256;
pub(crate) const MAX_RECORD_BYTES: usize = 4 * 1_048_576;

pub(crate) struct AppStore<CS: StateStoreBackend> {
    inner: CS,
    decoded_runs: Mutex<Vec<DecodedRun>>,
    retired_receipts: Mutex<Vec<String>>,
}

/// Intrinsic decoding evidence only; callers still freshly read every referenced row.
struct DecodedRun {
    source: Arc<Value>,
    pages: Vec<Arc<Value>>,
    record: RunRecord,
}

impl DecodedRun {
    fn matches(&self, source: &Arc<Value>, pages: &[Arc<Value>]) -> bool {
        Arc::ptr_eq(&self.source, source)
            && self.pages.len() == pages.len()
            && self
                .pages
                .iter()
                .zip(pages)
                .all(|(left, right)| Arc::ptr_eq(left, right))
    }
}

pub(crate) struct AppMutation {
    id: String,
    kind: AppMutationKind,
}

pub(crate) enum StorageFailure {
    Recoverable(AppError),
    Permanent(AppError),
    CleanupFailed(AppError),
}

pub(crate) enum AppCommitOutcome {
    Applied,
    Rejected(StorageFailure),
    Unknown(StorageFailure),
}

enum AppMutationKind {
    ClaimApp {
        fence: AppClaimFence,
        app: AppStorageRecord,
        definitions: Vec<AppStorageRecord>,
    },
    AdvanceLifecycle {
        expected_app_revision: u64,
        generation: u64,
        shutdown: ShutdownRecord,
    },
    AdmitWorkspace {
        expected_app_revision: u64,
        app: AppStorageRecord,
        workspace: AppStorageRecord,
    },
    AdvanceWorkspace {
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        workspace: AppStorageRecord,
    },
    AdvanceWorkspaceWithApp {
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        app: AppStorageRecord,
        workspace: AppStorageRecord,
    },
    DeleteWorkspace {
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        workspace: RecordKey,
    },
    AdmitRun {
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        app: AppStorageRecord,
        workspace: AppStorageRecord,
        run: AppStorageRecord,
        head: AppStorageRecord,
        source: Option<Box<RunCopy>>,
    },
    AdvanceRun {
        expected_app_revision: u64,
        expected_run_revision: u64,
        app: AppStorageRecord,
        run: AppStorageRecord,
        predecessor: RunRecord,
        children: Vec<(RunKey, String)>,
    },
    AdmitChildren {
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        expected_parent_revision: u64,
        app: AppStorageRecord,
        workspace: AppStorageRecord,
        parent: AppStorageRecord,
        predecessor: RunRecord,
        children: Vec<(AppStorageRecord, AppStorageRecord, Option<RunCopy>)>,
    },
    RetainRun {
        expected_app_revision: u64,
        expected_run_revision: u64,
        app: AppStorageRecord,
        run: AppStorageRecord,
    },
    ReadmitRun {
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        expected_run_revision: u64,
        app: AppStorageRecord,
        workspace: AppStorageRecord,
        run: AppStorageRecord,
        predecessor: RunRecord,
    },
    SealRun {
        expected_app_revision: u64,
        expected_run_revision: u64,
        run: AppStorageRecord,
        parent: Option<(AppStorageRecord, u64)>,
    },
    DeleteRunPage {
        expected_app_revision: u64,
        expected_run_revision: u64,
        head: AppStorageRecord,
        keys: Vec<RecordKey>,
        complete: bool,
    },
}

enum AppClaimFence {
    Missing,
    Revision(u64),
}

enum AppStorageRecord {
    Value {
        key: RecordKey,
        value: Value,
    },
    Checkpoint {
        key: RecordKey,
        value: Value,
        record: Box<RunRecord>,
        pages: Vec<StateRecord>,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SettlementPages {
    count: usize,
    pages: Vec<u64>,
}

#[derive(Serialize)]
struct SettlementPage<'record> {
    epoch: String,
    workspace_id: WorkspaceId,
    workflow_id: WorkflowId,
    id: RunId,
    occurrence: String,
    index: usize,
    sequence: u64,
    entries: &'record [InteractionSettlement],
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
enum RecordKey {
    App,
    Definition {
        workflow: WorkflowId,
        revision: String,
    },
    Workspace(WorkspaceId),
    Run {
        workspace: WorkspaceId,
        workflow: WorkflowId,
        run: RunId,
        occurrence: String,
    },
    Checkpoint {
        workspace: WorkspaceId,
        workflow: WorkflowId,
        run: RunId,
        occurrence: String,
        sequence: u64,
    },
    Ledger {
        workspace: WorkspaceId,
        workflow: WorkflowId,
        run: RunId,
        occurrence: String,
        index: usize,
        sequence: u64,
    },
    ActionObservation {
        workspace: WorkspaceId,
        workflow: WorkflowId,
        run: RunId,
        occurrence: String,
        ordinal: u64,
        sequence: u64,
    },
    ActiveRun {
        workspace: WorkspaceId,
        workflow: WorkflowId,
        run: RunId,
        occurrence: String,
    },
    ProjectionTail {
        workspace: WorkspaceId,
        workflow: WorkflowId,
        run: RunId,
        projection: RunProjection,
    },
    ProjectionLink {
        key: RunKey,
        projection: RunProjection,
        sequence: u64,
    },
    Receipt(String),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DefinitionRecord {
    epoch: String,
    workflow_id: WorkflowId,
    revision: String,
    canonical: String,
}

impl SettlementPages {
    fn validate(&self, sequence: u64) -> Result<(), AppError> {
        if self.count > MAX_INTERACTION_SETTLEMENTS
            || self.pages.len() != self.count.div_ceil(64)
            || self.pages.iter().any(|published| *published > sequence)
        {
            return Err(AppError::Integration(
                "settlement page references are invalid".into(),
            ));
        }
        Ok(())
    }
}

impl SettlementPage<'_> {
    fn matches(key: &RecordKey, value: &Value) -> bool {
        let RecordKey::Ledger {
            workspace,
            workflow,
            run,
            occurrence,
            index,
            sequence,
        } = key
        else {
            return false;
        };
        value.as_object().is_some_and(|value| value.len() == 8)
            && value.get("epoch").and_then(Value::as_str) == Some(EPOCH)
            && value.get("workspace_id").and_then(Value::as_str) == Some(workspace.as_str())
            && value.get("workflow_id").and_then(Value::as_str) == Some(workflow.as_str())
            && value.get("id").and_then(Value::as_str) == Some(run.as_str())
            && value.get("occurrence").and_then(Value::as_str) == Some(occurrence.as_str())
            && value.get("index").and_then(Value::as_u64) == Some(*index as u64)
            && value.get("sequence").and_then(Value::as_u64) == Some(*sequence)
            && value
                .get("entries")
                .and_then(Value::as_array)
                .is_some_and(|entries| !entries.is_empty() && entries.len() <= 64)
    }
}

impl DefinitionRecord {
    fn key(&self) -> RecordKey {
        RecordKey::Definition {
            workflow: self.workflow_id.clone(),
            revision: self.revision.clone(),
        }
    }

    fn validate(&self) -> Result<(), AppError> {
        let compiled = CompiledDefinition::parse(&self.canonical)
            .map_err(|_| AppError::Integration("durable workflow definition is corrupt".into()))?;
        if self.epoch != EPOCH
            || self.workflow_id.as_str() != compiled.id()
            || self.revision != compiled.revision()
            || self.canonical != compiled.fingerprint()
        {
            return Err(AppError::Integration(
                "durable workflow definition identity is inconsistent".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RunHead {
    epoch: String,
    id: RunId,
    workspace_id: WorkspaceId,
    workflow_id: WorkflowId,
    definition_revision: String,
    child_revisions: BTreeMap<WorkflowId, String>,
    agent_revisions: BTreeMap<String, String>,
    creation: String,
    sequence: u64,
    unresolved_failures: BTreeMap<u64, u64>,
    retention: RunRetention,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum RunRetention {
    Active {},
    Retained {},
    Deleting { after: Option<String> },
}

#[derive(Deserialize, Serialize)]
struct ReceiptRecord {
    epoch: String,
    id: String,
}

pub(crate) struct RecordCodec {
    bytes: usize,
}

impl RunHead {
    fn definition_pins(&self) -> impl Iterator<Item = (&WorkflowId, &str)> {
        std::iter::once((&self.workflow_id, self.definition_revision.as_str())).chain(
            self.child_revisions
                .iter()
                .map(|(id, revision)| (id, revision.as_str())),
        )
    }

    fn advance(&mut self, record: &RunRecord, prior: &RunRecord) -> Result<(), AppError> {
        if !matches!(self.retention, RunRetention::Active {})
            || self.sequence.checked_add(1) != Some(record.checkpoint.sequence)
            || self.id != record.id
            || self.creation != record.occurrence
            || self.workspace_id != record.workspace_id
            || self.workflow_id != record.workflow_id
        {
            return Err(AppError::Conflict(
                "run head cannot accept this checkpoint".into(),
            ));
        }
        if let Some(committed) = record.checkpoint.committed_ordinal() {
            self.unresolved_failures
                .retain(|ordinal, _| *ordinal > committed);
        }
        if let Some(observation) = record.checkpoint.failed_observation()
            && prior.checkpoint.failed_observation() != Some(observation)
        {
            self.unresolved_failures
                .insert(observation.ordinal, record.checkpoint.sequence);
        }
        self.sequence = record.checkpoint.sequence;
        self.validate()?;
        self.validate_row(record)
    }

    fn head_key(&self) -> RecordKey {
        RecordKey::Run {
            workspace: self.workspace_id.clone(),
            workflow: self.workflow_id.clone(),
            run: self.id.clone(),
            occurrence: self.creation.clone(),
        }
    }

    fn checkpoint_key(&self, sequence: u64) -> RecordKey {
        RecordKey::Checkpoint {
            workspace: self.workspace_id.clone(),
            workflow: self.workflow_id.clone(),
            run: self.id.clone(),
            occurrence: self.creation.clone(),
            sequence,
        }
    }

    fn active_key(&self) -> RecordKey {
        RecordKey::ActiveRun {
            workspace: self.workspace_id.clone(),
            workflow: self.workflow_id.clone(),
            run: self.id.clone(),
            occurrence: self.creation.clone(),
        }
    }

    fn history_prefix(&self) -> String {
        format!(
            "history/{}/{}/{}/{}/",
            self.workspace_id, self.workflow_id, self.id, self.creation
        )
    }

    fn ensure_readable(&self) -> Result<(), AppError> {
        if matches!(self.retention, RunRetention::Deleting { .. }) {
            Err(AppError::Closed(
                "run history deletion has been admitted".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn admitted(record: &RunRecord) -> Result<Self, AppError> {
        if record.checkpoint.sequence != 0 || record.previous.is_some() {
            return Err(AppError::Integration(
                "run admission must publish checkpoint zero".into(),
            ));
        }
        let head = Self {
            epoch: record.epoch.clone(),
            id: record.id.clone(),
            workspace_id: record.workspace_id.clone(),
            workflow_id: record.workflow_id.clone(),
            definition_revision: record.definition_revision.clone(),
            child_revisions: record.child_revisions.clone(),
            agent_revisions: record.agent_revisions.clone(),
            creation: record.occurrence.clone(),
            sequence: record.checkpoint.sequence,
            unresolved_failures: BTreeMap::new(),
            retention: RunRetention::Active {},
        };
        head.validate()?;
        head.validate_row(record)?;
        Ok(head)
    }

    fn decode(value: &Value) -> Result<Self, AppError> {
        RecordCodec::preflight_epoch(value)?;
        let head: Self = RecordCodec::decode(value)?;
        head.validate()?;
        Ok(head)
    }

    fn validate(&self) -> Result<(), AppError> {
        if self.epoch != EPOCH
            || self.definition_pins().any(|(_, revision)| {
                revision.is_empty()
                    || revision.len() > 256
                    || revision.trim() != revision
                    || revision.chars().any(char::is_control)
            })
            || self.agent_revisions.len() > MAX_AGENTS
            || self.agent_revisions.iter().any(|(key, revision)| {
                SchemaText::node_or_state("agent key", key).is_err()
                    || SchemaText::revision(revision).is_err()
            })
            || self.creation.len() != 32
            || !self
                .creation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(AppError::Integration("durable run head is invalid".into()));
        }
        if self.unresolved_failures.len() > MAX_FRONTIER
            || self
                .unresolved_failures
                .values()
                .any(|sequence| *sequence == 0 || *sequence > self.sequence)
            || self
                .unresolved_failures
                .values()
                .collect::<BTreeSet<_>>()
                .len()
                != self.unresolved_failures.len()
        {
            return Err(AppError::Integration(
                "durable failed observation evidence is invalid".into(),
            ));
        }
        if let RunRetention::Deleting { after: Some(after) } = &self.retention {
            match RecordKey::history_key(after)? {
                RecordKey::Checkpoint {
                    workspace,
                    workflow,
                    run,
                    occurrence,
                    sequence,
                }
                | RecordKey::Ledger {
                    workspace,
                    workflow,
                    run,
                    occurrence,
                    sequence,
                    ..
                }
                | RecordKey::ActionObservation {
                    workspace,
                    workflow,
                    run,
                    occurrence,
                    sequence,
                    ..
                } if workspace == self.workspace_id
                    && workflow == self.workflow_id
                    && run == self.id
                    && occurrence == self.creation
                    && sequence <= self.sequence => {}
                RecordKey::ProjectionLink { key, sequence, .. }
                    if key.workspace() == &self.workspace_id
                        && key.workflow() == &self.workflow_id
                        && key.run() == &self.id
                        && key.occurrence() == self.creation
                        && sequence <= self.sequence => {}
                _ => {
                    return Err(AppError::Integration(
                        "run deletion cursor has foreign scope".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_row(&self, record: &RunRecord) -> Result<(), AppError> {
        if record.epoch != self.epoch
            || record.id != self.id
            || record.occurrence != self.creation
            || record.workspace_id != self.workspace_id
            || record.workflow_id != self.workflow_id
            || self.definition_revision != record.definition_revision
            || self.child_revisions != record.child_revisions
            || self.agent_revisions != record.agent_revisions
            || record.checkpoint.sequence != self.sequence
            || self.unresolved_failures.keys().any(|ordinal| {
                match record.checkpoint.committed_ordinal() {
                    Some(committed) => ordinal
                        .checked_sub(committed)
                        .is_none_or(|offset| offset == 0 || offset > MAX_FRONTIER as u64),
                    None => *ordinal >= MAX_FRONTIER as u64,
                }
            })
            || record
                .checkpoint
                .failed_observation()
                .is_some_and(|observation| {
                    !self.unresolved_failures.contains_key(&observation.ordinal)
                })
            || (self.sequence == 0 && !self.unresolved_failures.is_empty())
            || (!matches!(self.retention, RunRetention::Active {})
                && !record.checkpoint.is_terminal())
        {
            return Err(AppError::Integration(
                "durable run head does not identify its checkpoint".into(),
            ));
        }
        Ok(())
    }

    fn record(&self) -> Result<StateRecord, AppError> {
        self.validate()?;
        self.head_key().record(self)
    }
}

impl StorageFailure {
    fn from_app(error: AppError) -> Self {
        if matches!(error, AppError::Busy(_) | AppError::Timeout(_)) {
            Self::Recoverable(error)
        } else {
            Self::Permanent(error)
        }
    }

    pub(crate) fn permanent(error: AppError) -> Self {
        Self::Permanent(error)
    }

    pub(crate) fn is_recoverable(&self) -> bool {
        matches!(self, Self::Recoverable(_))
    }

    pub(crate) fn is_cleanup_failed(&self) -> bool {
        matches!(self, Self::CleanupFailed(_))
    }

    pub(crate) fn is_retryable_conflict(&self) -> bool {
        !self.is_cleanup_failed() && matches!(self.error(), AppError::Conflict(_))
    }

    pub(crate) fn error(&self) -> &AppError {
        match self {
            Self::Recoverable(error) | Self::Permanent(error) | Self::CleanupFailed(error) => error,
        }
    }

    pub(crate) fn into_error(self) -> AppError {
        match self {
            Self::Recoverable(error) | Self::Permanent(error) | Self::CleanupFailed(error) => error,
        }
    }
}

impl<CS: StateStoreBackend> AppStore<CS> {
    pub(crate) fn new(inner: CS) -> Self {
        Self {
            inner,
            decoded_runs: Mutex::new(Vec::new()),
            retired_receipts: Mutex::new(Vec::new()),
        }
    }

    async fn decode_run<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        snapshot: StateRecordSnapshot,
        admit: bool,
        observed_pages: &mut Vec<StateRecordSnapshot>,
    ) -> Result<RunRecord, StorageFailure> {
        let key = snapshot.record.key;
        let value = snapshot.record.value;
        RecordCodec::preflight_epoch(&value).map_err(StorageFailure::permanent)?;
        RecordKey::validate_value(key.key(), &value).map_err(StorageFailure::permanent)?;
        let RecordKey::Checkpoint {
            workspace,
            workflow,
            run,
            occurrence,
            sequence,
        } = RecordKey::history_key(key.key()).map_err(StorageFailure::permanent)?
        else {
            return Err(StorageFailure::permanent(AppError::Integration(
                "expected checkpoint evidence".into(),
            )));
        };
        let descriptor: SettlementPages = RecordCodec::decode(&value["checkpoint"]["settlements"])
            .map_err(StorageFailure::permanent)?;
        descriptor
            .validate(sequence)
            .map_err(StorageFailure::permanent)?;
        let mut pages = Vec::with_capacity(descriptor.pages.len());
        for (index, sequence) in descriptor.pages.iter().copied().enumerate() {
            let key = RecordKey::Ledger {
                workspace: workspace.clone(),
                workflow: workflow.clone(),
                run: run.clone(),
                occurrence: occurrence.clone(),
                index,
                sequence,
            }
            .state_key()
            .map_err(StorageFailure::permanent)?;
            let snapshot = match observed_pages
                .iter()
                .find(|snapshot| snapshot.record.key == key)
            {
                Some(snapshot) => snapshot.clone(),
                None => {
                    let snapshot = transaction
                        .load(key.clone())
                        .await
                        .map_err(StorageFailure::from)?
                        .ok_or_else(|| {
                            StorageFailure::permanent(AppError::Integration(
                                "referenced settlement page is missing".into(),
                            ))
                        })?;
                    // Current and predecessor reference at most 128 distinct
                    // leaves. Longer history/child batches evict old observations;
                    // this is a reuse bound, never an operation or record limit.
                    if observed_pages.len() == 128 {
                        observed_pages.remove(0);
                    }
                    observed_pages.push(snapshot.clone());
                    snapshot
                }
            };
            if snapshot.record.key != key {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "settlement page has foreign storage identity".into(),
                )));
            }
            RecordKey::validate_value(key.key(), &snapshot.record.value)
                .map_err(StorageFailure::permanent)?;
            if snapshot
                .record
                .value
                .get("entries")
                .and_then(Value::as_array)
                .map(Vec::len)
                != Some((descriptor.count - index * 64).min(64))
            {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "settlement page count is inconsistent".into(),
                )));
            }
            pages.push(snapshot.record.value);
        }
        let cached = {
            let mut rows = self
                .decoded_runs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            rows.iter()
                .position(|row| row.matches(&value, &pages))
                .map(|index| {
                    let row = rows.remove(index);
                    let cached = row.record.clone();
                    rows.push(row);
                    cached
                })
        };
        if let Some(record) = cached {
            return Ok(record);
        }
        // Reconstitute the original inline byte charge without allocating its
        // JSON array. Descriptor and leaf metadata are physical bookkeeping,
        // not a reason to narrow the unchanged logical four-MiB contract.
        let mut entries_size = 2usize;
        for (index, page) in pages.iter().enumerate() {
            let size =
                RecordCodec::value_len(&page["entries"]).map_err(StorageFailure::permanent)?;
            entries_size = entries_size
                .checked_add(size.saturating_sub(2))
                .and_then(|size| size.checked_add(usize::from(index > 0)))
                .ok_or_else(|| {
                    StorageFailure::permanent(AppError::Integration(
                        "app durable record encoding failed".into(),
                    ))
                })?;
        }
        let size = RecordCodec::value_len(&value)
            .map_err(StorageFailure::permanent)?
            .checked_sub(
                RecordCodec::value_len(&value["checkpoint"]["settlements"])
                    .map_err(StorageFailure::permanent)?,
            )
            .and_then(|size| size.checked_add(entries_size))
            .ok_or_else(|| {
                StorageFailure::permanent(AppError::Integration(
                    "app durable record encoding failed".into(),
                ))
            })?;
        if size > MAX_RECORD_BYTES {
            return Err(StorageFailure::permanent(AppError::Integration(
                "app durable record exceeds the private byte bound".into(),
            )));
        }
        let mut record = RecordCodec::parse_run(&value).map_err(StorageFailure::permanent)?;
        record
            .checkpoint
            .hydrate_settlements(&pages, descriptor.pages, descriptor.count)
            .map_err(StorageFailure::permanent)?;
        RecordCodec::validate_run(&record, size).map_err(StorageFailure::permanent)?;
        record
            .checkpoint
            .validate_settlements()
            .map_err(StorageFailure::permanent)?;
        if admit {
            let mut rows = self
                .decoded_runs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !rows.iter().any(|row| row.matches(&value, &pages)) {
                if rows.len() == 4 {
                    rows.remove(0);
                }
                rows.push(DecodedRun {
                    source: value,
                    pages,
                    record: record.clone(),
                });
            }
        }
        Ok(record)
    }

    pub(crate) async fn validate_definition_inventory(
        &self,
        definitions: &BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
        application: Option<&AppRecord>,
        configured_agents: &BTreeSet<(&str, &str)>,
    ) -> Result<(), AppError> {
        for workspace in self.list_workspaces().await? {
            workspace.validate(&workspace.id, configured_agents)?;
        }
        if let Some(application) = application {
            for (id, revision) in &application.definitions {
                let key = RecordKey::Definition {
                    workflow: id.clone(),
                    revision: revision.clone(),
                };
                if self
                    .load_record::<DefinitionRecord>(key)
                    .await
                    .map_err(StorageFailure::into_error)?
                    .is_none()
                {
                    return Err(AppError::Integration(
                        "current workflow revision has no durable definition".into(),
                    ));
                }
            }
        }
        // Configured pairs are checked once, independently of how many retained
        // runs pin them. Unused durable pairs never become supply obligations.
        let mut catalogued = BTreeSet::new();
        for (id, revisions) in definitions {
            for definition in revisions {
                let key = RecordKey::Definition {
                    workflow: id.clone(),
                    revision: definition.revision().to_owned(),
                };
                if let Some((record, _)) = self
                    .load_record::<DefinitionRecord>(key)
                    .await
                    .map_err(StorageFailure::into_error)?
                {
                    if record.canonical != definition.fingerprint() {
                        return Err(AppError::Conflict(
                            "configured workflow revision differs from its durable definition"
                                .into(),
                        ));
                    }
                    catalogued.insert((id, definition.revision()));
                }
            }
        }
        let mut after = None;
        loop {
            let (page, next) = self
                .list_page(Some("run/"), after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in page {
                let head = RunHead::decode(&snapshot.record.value)?;
                if matches!(head.retention, RunRetention::Deleting { .. }) {
                    continue;
                }
                if head.agent_revisions.iter().any(|(key, revision)| {
                    !configured_agents.contains(&(key.as_str(), revision.as_str()))
                }) {
                    return Err(AppError::Conflict(
                        "retained agent revision is not configured".into(),
                    ));
                }
                let latest = self
                    .load_checkpoint(&head, head.sequence)
                    .await
                    .map_err(StorageFailure::into_error)?;
                head.validate_row(&latest)?;
                // Validate exact selected task bindings from the same already
                // required latest row; explicit history reads validate older rows.
                for (id, revision) in latest.definition_pins() {
                    if !catalogued.contains(&(id, revision)) {
                        let key = RecordKey::Definition {
                            workflow: id.clone(),
                            revision: revision.to_owned(),
                        };
                        if self
                            .load_record::<DefinitionRecord>(key)
                            .await
                            .map_err(StorageFailure::into_error)?
                            .is_none()
                        {
                            return Err(AppError::Integration(
                                "retained workflow revision has no durable definition".into(),
                            ));
                        }
                        return Err(AppError::Conflict(
                            "retained workflow revision is not configured".into(),
                        ));
                    }
                }
                let definition = definitions
                    .get(&head.workflow_id)
                    .and_then(|revisions| {
                        revisions
                            .iter()
                            .find(|definition| definition.revision() == head.definition_revision)
                    })
                    .ok_or_else(|| {
                        AppError::Conflict("retained workflow revision is not configured".into())
                    })?;
                definition
                    .validate_child_relationships(definitions, &head.child_revisions)
                    .map_err(|_| {
                        AppError::Integration(
                            "retained workflow child revision relationships are inconsistent"
                                .into(),
                        )
                    })?;
                latest.validate_recovery(definition)?;
                if !self
                    .head_unchanged(&head, snapshot.revision.get())
                    .await
                    .map_err(StorageFailure::into_error)?
                {
                    return Err(AppError::Busy(
                        "retained run changed during startup validation".into(),
                    ));
                }
            }
            match next {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
    }

    pub(crate) async fn load_app(&self) -> Result<Option<(AppRecord, u64)>, AppError> {
        self.load_app_for_recovery()
            .await
            .map_err(StorageFailure::into_error)
    }

    pub(crate) async fn load_app_for_recovery(
        &self,
    ) -> Result<Option<(AppRecord, u64)>, StorageFailure> {
        self.load_record(RecordKey::App).await
    }

    pub(crate) async fn load_workspace(
        &self,
        id: &WorkspaceId,
    ) -> Result<Option<(WorkspaceRecord, u64)>, AppError> {
        self.load_workspace_for_recovery(id)
            .await
            .map_err(StorageFailure::into_error)
    }

    pub(crate) async fn load_workspace_for_recovery(
        &self,
        id: &WorkspaceId,
    ) -> Result<Option<(WorkspaceRecord, u64)>, StorageFailure> {
        self.load_record(RecordKey::Workspace(id.clone())).await
    }

    pub(crate) async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, AppError> {
        let mut after = None;
        let mut records = Vec::new();
        loop {
            let (page, next) = self
                .list_page(Some("workspace/"), after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in page {
                if records.len() == MAX_WORKSPACES {
                    return Err(AppError::Integration(
                        "durable workspace inventory exceeds the private bound".into(),
                    ));
                }
                records.push(RecordCodec::decode(&snapshot.record.value)?);
            }
            match next {
                Some(next) => after = Some(next),
                None => return Ok(records),
            }
        }
    }

    pub(crate) async fn load_run(
        &self,
        key: &RunKey,
    ) -> Result<Option<(RunRecord, u64)>, AppError> {
        self.load_run_for_recovery(key)
            .await
            .map_err(StorageFailure::into_error)
    }

    pub(crate) async fn load_run_for_recovery(
        &self,
        key: &RunKey,
    ) -> Result<Option<(RunRecord, u64)>, StorageFailure> {
        let state_key = RecordKey::exact_run(key)
            .state_key()
            .map_err(StorageFailure::permanent)?;
        for _ in 0..8 {
            let mut transaction = self
                .inner
                .begin_transaction()
                .await
                .map_err(StorageFailure::from)?;
            let mut observed_pages = Vec::new();
            let mut missing_row_head = None;
            let observed: Result<Option<(RunHead, u64, RunRecord)>, StorageFailure> = async {
                let Some(snapshot) = transaction
                    .load(state_key.clone())
                    .await
                    .map_err(StorageFailure::from)?
                else {
                    return Ok(None);
                };
                RecordKey::validate_value(state_key.key(), &snapshot.record.value)
                    .map_err(StorageFailure::permanent)?;
                let head =
                    RunHead::decode(&snapshot.record.value).map_err(StorageFailure::permanent)?;
                let revision = snapshot.revision.get();
                head.ensure_readable().map_err(StorageFailure::permanent)?;
                let mut current: Option<RunRecord> = None;
                for selected in std::iter::once(head.sequence).chain(head.sequence.checked_sub(1)) {
                    let key = head
                        .checkpoint_key(selected)
                        .state_key()
                        .map_err(StorageFailure::permanent)?;
                    let Some(snapshot) = transaction
                        .load(key.clone())
                        .await
                        .map_err(StorageFailure::from)?
                    else {
                        missing_row_head = Some(head);
                        return Err(StorageFailure::permanent(AppError::Integration(
                            if current.is_some() {
                                "retained checkpoint predecessor is missing"
                            } else {
                                "retained checkpoint is missing"
                            }
                            .into(),
                        )));
                    };
                    current = Some(
                        self.decode_checkpoint_row(
                            &mut transaction,
                            &key,
                            snapshot,
                            current,
                            &mut observed_pages,
                        )
                        .await?,
                    );
                }
                let record = current.expect("the selected checkpoint is always observed");
                head.validate_row(&record)
                    .map_err(StorageFailure::permanent)?;
                Ok(Some((head, revision, record)))
            }
            .await;
            let observed = match observed {
                Ok(observed) => match transaction.commit().await {
                    Ok(()) => observed,
                    // A concurrent run transition can invalidate this pure
                    // snapshot. Discard it and observe again within the bound.
                    Err(StateStoreError::TransactionConflict { .. }) => continue,
                    Err(error) => return Err(StorageFailure::from(error)),
                },
                Err(primary) => {
                    if transaction.rollback().await.is_err() {
                        AppTrace::event(
                            || tracing::warn!(target: "app_sdk", event = "app.storage.read_rollback_failed"),
                        );
                        // Failed cleanup vetoes retry without replacing the primary public error.
                        return Err(StorageFailure::CleanupFailed(primary.into_error()));
                    }
                    if let Some(head) = missing_row_head {
                        self.validate_history_head(&head).await?;
                    }
                    return Err(primary);
                }
            };
            let Some((head, revision, record)) = observed else {
                return Ok(None);
            };
            // Snapshot consistency does not replace this fresh deletion/advance fence.
            if self.head_unchanged(&head, revision).await? {
                return Ok(Some((record, revision)));
            }
        }
        Err(StorageFailure::Recoverable(AppError::Busy(
            "run head changed during observation".into(),
        )))
    }

    async fn load_head(&self, key: RecordKey) -> Result<Option<(RunHead, u64)>, StorageFailure> {
        let state_key = key.state_key().map_err(StorageFailure::permanent)?;
        let snapshot = self
            .inner
            .load(state_key.clone())
            .await
            .map_err(StorageFailure::from)?;
        snapshot
            .map(|snapshot| {
                RecordKey::validate_value(state_key.key(), &snapshot.record.value)
                    .map_err(StorageFailure::permanent)?;
                RunHead::decode(&snapshot.record.value)
                    .map(|head| (head, snapshot.revision.get()))
                    .map_err(StorageFailure::permanent)
            })
            .transpose()
    }

    async fn head_unchanged(&self, head: &RunHead, revision: u64) -> Result<bool, StorageFailure> {
        let Some((current, observed)) = self.load_head(head.head_key()).await? else {
            return Err(StorageFailure::permanent(AppError::Closed(
                "workflow run was removed during observation".into(),
            )));
        };
        current
            .ensure_readable()
            .map_err(StorageFailure::permanent)?;
        Ok(observed == revision
            && current.creation == head.creation
            && current.sequence == head.sequence)
    }

    async fn validate_history_head(&self, head: &RunHead) -> Result<(), StorageFailure> {
        let Some((current, _)) = self.load_head(head.head_key()).await? else {
            return Err(StorageFailure::permanent(AppError::Closed(
                "workflow run was removed during observation".into(),
            )));
        };
        current
            .ensure_readable()
            .map_err(StorageFailure::permanent)?;
        if current.creation != head.creation || current.sequence < head.sequence {
            return Err(StorageFailure::permanent(AppError::Conflict(
                "run history scope changed during observation".into(),
            )));
        }
        Ok(())
    }

    async fn decode_checkpoint_row<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        key: &StateKey,
        snapshot: StateRecordSnapshot,
        current: Option<RunRecord>,
        observed_pages: &mut Vec<StateRecordSnapshot>,
    ) -> Result<RunRecord, StorageFailure> {
        RecordCodec::preflight_epoch(&snapshot.record.value).map_err(StorageFailure::permanent)?;
        RecordKey::validate_value(key.key(), &snapshot.record.value)
            .map_err(StorageFailure::permanent)?;
        let record = self
            .decode_run(transaction, snapshot, true, observed_pages)
            .await?;
        match current {
            Some(mut current) => {
                if !current.same_run(&record) {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "retained checkpoint predecessor has foreign identity".into(),
                    )));
                }
                current.previous = Some(record.checkpoint);
                Ok(current)
            }
            None => Ok(record),
        }
    }

    async fn load_checkpoint(
        &self,
        head: &RunHead,
        sequence: u64,
    ) -> Result<RunRecord, StorageFailure> {
        let mut transaction = self
            .inner
            .begin_transaction()
            .await
            .map_err(StorageFailure::from)?;
        let mut observed_pages = Vec::new();
        let mut missing = false;
        let observed = async {
            let mut current: Option<RunRecord> = None;
            for selected in std::iter::once(sequence).chain(sequence.checked_sub(1)) {
                let key = head
                    .checkpoint_key(selected)
                    .state_key()
                    .map_err(StorageFailure::permanent)?;
                let Some(snapshot) = transaction
                    .load(key.clone())
                    .await
                    .map_err(StorageFailure::from)?
                else {
                    missing = true;
                    return Err(StorageFailure::permanent(AppError::Integration(
                        if current.is_some() {
                            "retained checkpoint predecessor is missing"
                        } else {
                            "retained checkpoint is missing"
                        }
                        .into(),
                    )));
                };
                current = Some(
                    self.decode_checkpoint_row(
                        &mut transaction,
                        &key,
                        snapshot,
                        current,
                        &mut observed_pages,
                    )
                    .await?,
                );
            }
            Ok(current.expect("the selected checkpoint is always observed"))
        }
        .await;
        match observed {
            Ok(record) => {
                transaction.commit().await.map_err(StorageFailure::from)?;
                Ok(record)
            }
            Err(primary) => {
                if transaction.rollback().await.is_err() {
                    AppTrace::event(
                        || tracing::warn!(target: "app_sdk", event = "app.storage.read_rollback_failed"),
                    );
                    return Err(StorageFailure::CleanupFailed(primary.into_error()));
                }
                if missing {
                    self.validate_history_head(head).await?;
                }
                Err(primary)
            }
        }
    }

    pub(crate) async fn run_is_active(&self, record: &RunRecord) -> Result<bool, StorageFailure> {
        let Some((head, revision)) = self.load_head(RecordKey::run(record)).await? else {
            return Ok(false);
        };
        head.ensure_readable().map_err(StorageFailure::permanent)?;
        let active = matches!(head.retention, RunRetention::Active {});
        let marker: Option<(Value, u64)> = self.load_record(head.active_key()).await?;
        if !self.head_unchanged(&head, revision).await? {
            return Err(StorageFailure::Recoverable(AppError::Busy(
                "run capacity changed during observation".into(),
            )));
        }
        if active != marker.is_some() {
            return Err(StorageFailure::permanent(AppError::Integration(
                "run capacity index is inconsistent".into(),
            )));
        }
        Ok(active)
    }

    pub(crate) async fn checkpoint(
        &self,
        current: &RunRecord,
        selector: &CheckpointSelector,
        definition: &CompiledDefinition,
    ) -> Result<Option<(RunRecord, u64)>, RuntimeOperationFailure> {
        let Some((head, revision)) = self.load_head(RecordKey::run(current)).await? else {
            return Ok(None);
        };
        head.ensure_readable()?;
        let validate = |record: &RunRecord| {
            if !record.same_run(current) {
                return Err(AppError::Integration(
                    "retained checkpoint has foreign identity".into(),
                ));
            }
            record.validate_recovery(definition)
        };
        let latest = self.load_checkpoint(&head, head.sequence).await?;
        head.validate_row(&latest)?;
        validate(&latest)?;
        let selected = match selector {
            CheckpointSelector::Latest => Some(latest),
            CheckpointSelector::Sequence(sequence) if *sequence == head.sequence => Some(latest),
            CheckpointSelector::Sequence(sequence) if *sequence <= head.sequence => {
                Some(self.load_checkpoint(&head, *sequence).await?)
            }
            CheckpointSelector::Sequence(_) => None,
            CheckpointSelector::Action(ordinal) => {
                self.action_checkpoint(&head, &latest, *ordinal, definition)
                    .await?
            }
        };
        if let Some(record) = &selected {
            validate(record)?;
        }
        self.validate_history_head(&head).await?;
        Ok(selected.map(|record| (record, revision)))
    }

    async fn action_checkpoint(
        &self,
        head: &RunHead,
        latest: &RunRecord,
        ordinal: u64,
        definition: &CompiledDefinition,
    ) -> Result<Option<RunRecord>, RuntimeOperationFailure> {
        let ordinal = &ordinal;
        let validate = |record: &RunRecord| -> Result<(), RuntimeOperationFailure> {
            if !record.same_run(latest) {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "retained checkpoint has foreign identity".into(),
                ))
                .into());
            }
            record
                .validate_recovery(definition)
                .map_err(|error| StorageFailure::permanent(error).into())
        };

        let prefix = format!("{}action/{ordinal:020}/", head.history_prefix());
        let reverse = u64::MAX - head.sequence;
        let after = (reverse > 0).then(|| format!("{prefix}{:020}", reverse - 1));
        let (rows, _) = self.list_page(Some(&prefix), after.as_deref(), 1).await?;
        let indexed = rows
            .first()
            .map(
                |snapshot| match RecordKey::history_key(snapshot.record.key.key())? {
                    RecordKey::ActionObservation {
                        workspace,
                        workflow,
                        run,
                        occurrence,
                        ordinal: actual,
                        sequence,
                    } if workspace == head.workspace_id
                        && workflow == head.workflow_id
                        && run == head.id
                        && occurrence == head.creation
                        && actual == *ordinal
                        && sequence <= head.sequence =>
                    {
                        Ok(sequence)
                    }
                    _ => Err(AppError::Integration(
                        "action observation index is invalid".into(),
                    )),
                },
            )
            .transpose()
            .map_err(StorageFailure::permanent)?;
        let committed = latest
            .checkpoint
            .committed_ordinal()
            .is_some_and(|position| *ordinal <= position);
        let expected = if committed {
            let initial = self.load_checkpoint(head, 0).await?;
            validate(&initial)?;
            if initial
                .checkpoint
                .committed_ordinal()
                .is_some_and(|inherited| *ordinal <= inherited)
            {
                // A restored initial position is not a local observation.
                None
            } else {
                // Committed positions are monotone and local successful
                // ordinals are contiguous. Find their first publication;
                // even u64::MAX checkpoints require at most 64 probes.
                let mut lower = 1;
                let mut upper = head.sequence;
                let mut boundary = latest.clone();
                for _ in 0..u64::BITS {
                    if lower >= upper {
                        break;
                    }
                    let middle = lower + (upper - lower) / 2;
                    let record = self.load_checkpoint(head, middle).await?;
                    validate(&record)?;
                    if record
                        .checkpoint
                        .committed_ordinal()
                        .is_some_and(|position| position >= *ordinal)
                    {
                        upper = middle;
                        boundary = record;
                    } else {
                        lower = middle + 1;
                    }
                }
                if lower != upper
                    || boundary.checkpoint.sequence != lower
                    || !matches!(boundary.checkpoint_kind(), CheckpointKind::Action)
                    || !boundary
                        .checkpoint
                        .outcomes()
                        .iter()
                        .any(|outcome| outcome.ordinal == *ordinal)
                {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "committed action has no valid observation boundary".into(),
                    ))
                    .into());
                }
                Some(boundary)
            }
        } else if let Some(sequence) = head.unresolved_failures.get(ordinal) {
            let record = self.load_checkpoint(head, *sequence).await?;
            validate(&record)?;
            if !matches!(record.checkpoint_kind(),
                        CheckpointKind::Failed { ordinal: actual, .. } if actual == *ordinal)
            {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "failed observation evidence identifies another checkpoint".into(),
                ))
                .into());
            }
            Some(record)
        } else {
            None
        };
        if indexed != expected.as_ref().map(|record| record.checkpoint.sequence) {
            // A legitimate seal/removal outranks an apparent missing
            // index in this captured, otherwise immutable history.
            self.validate_history_head(head).await?;
            return Err(StorageFailure::permanent(AppError::Integration(
                "action observation index is inconsistent".into(),
            ))
            .into());
        }
        Ok(expected)
    }

    pub(crate) async fn history(
        &self,
        current: &RunRecord,
        generation: u64,
        after: Option<&HistoryCursor>,
        limit: NonZeroUsize,
    ) -> Result<(Vec<RunRecord>, Option<HistoryCursor>), AppError> {
        if limit.get() > MAX_LIST_LIMIT {
            return Err(AppError::InvalidInput(
                "history page limit exceeds the supported bound".into(),
            ));
        }
        let Some((head, _)) = self
            .load_head(RecordKey::run(current))
            .await
            .map_err(StorageFailure::into_error)?
        else {
            return Err(AppError::NotFound("workflow run does not exist".into()));
        };
        head.ensure_readable()?;
        let latest = self
            .load_checkpoint(&head, head.sequence)
            .await
            .map_err(StorageFailure::into_error)?;
        head.validate_row(&latest)?;
        if !latest.same_run(current) {
            return Err(AppError::Integration(
                "run history has foreign identity".into(),
            ));
        }
        let (cutoff, mut next) = match after {
            Some(cursor) => {
                let (cutoff, after) = cursor.continue_from(&latest, generation, &head.creation)?;
                (
                    cutoff,
                    after.checked_add(1).ok_or_else(|| {
                        AppError::InvalidInput("history cursor is exhausted".into())
                    })?,
                )
            }
            None => (head.sequence, 0),
        };
        let prefix = format!("{}checkpoint/", head.history_prefix());
        let mut rows: Vec<RunRecord> = Vec::new();
        let mut previous = if next > 0 {
            Some(
                self.load_checkpoint(&head, next - 1)
                    .await
                    .map_err(StorageFailure::into_error)?
                    .checkpoint,
            )
        } else {
            None
        };
        while next <= cutoff && rows.len() < limit.get() {
            let after_key = (next > 0).then(|| format!("{prefix}{:020}", next - 1));
            let count = (limit.get() - rows.len()).min(PAGE_SIZE).min(
                usize::try_from(cutoff - next)
                    .unwrap_or(usize::MAX)
                    .saturating_add(1),
            );
            let (page, _) = self
                .list_page(Some(&prefix), after_key.as_deref(), count)
                .await
                .map_err(StorageFailure::into_error)?;
            if page.is_empty() {
                self.validate_history_head(&head)
                    .await
                    .map_err(StorageFailure::into_error)?;
                return Err(AppError::Integration(
                    "retained history contains a gap".into(),
                ));
            }
            let mut transaction = self
                .inner
                .begin_transaction()
                .await
                .map_err(AppError::from)?;
            let mut observed_pages = Vec::new();
            let mut missing = false;
            let hydrated = async {
                for listed in page {
                    let Some(snapshot) = transaction
                        .load(listed.record.key.clone())
                        .await
                        .map_err(AppError::from)?
                    else {
                        missing = true;
                        return Err(AppError::Integration(
                            "retained history contains a gap".into(),
                        ));
                    };
                    RecordKey::validate_value(snapshot.record.key.key(), &snapshot.record.value)?;
                    let mut record = self
                        .decode_run(&mut transaction, snapshot, true, &mut observed_pages)
                        .await
                        .map_err(StorageFailure::into_error)?;
                    if record.checkpoint.sequence != next || !record.same_run(current) {
                        return Err(AppError::Integration(
                            "retained history is not contiguous".into(),
                        ));
                    }
                    record.previous = previous.take();
                    previous = Some(record.checkpoint.clone());
                    rows.push(record);
                    if next == cutoff {
                        break;
                    }
                    next += 1;
                }
                Ok(())
            }
            .await;
            match hydrated {
                Ok(()) => transaction.commit().await.map_err(AppError::from)?,
                Err(primary) => {
                    if transaction.rollback().await.is_err() {
                        AppTrace::event(
                            || tracing::warn!(target: "app_sdk", event = "app.storage.read_rollback_failed"),
                        );
                        return Err(primary);
                    }
                    if missing {
                        self.validate_history_head(&head)
                            .await
                            .map_err(StorageFailure::into_error)?;
                    }
                    return Err(primary);
                }
            }
            if rows
                .last()
                .is_some_and(|record| record.checkpoint.sequence == cutoff)
            {
                break;
            }
        }
        self.validate_history_head(&head)
            .await
            .map_err(StorageFailure::into_error)?;
        let last = rows.last().map(|record| record.checkpoint.sequence);
        let cursor = match last {
            Some(last) if last < cutoff => Some(match after {
                Some(cursor) => cursor.advance_to(last)?,
                None => HistoryCursor::issue(&latest, generation, &head.creation, last)?,
            }),
            _ => None,
        };
        Ok((rows, cursor))
    }

    async fn list_page(
        &self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<StateRecordSnapshot>, Option<String>), StorageFailure> {
        let page = self.list_scoped_page(prefix, after, limit).await?;
        for snapshot in &page.0 {
            RecordCodec::preflight_epoch(&snapshot.record.value)
                .map_err(StorageFailure::permanent)?;
            RecordKey::validate_value(snapshot.record.key.key(), &snapshot.record.value)
                .map_err(StorageFailure::permanent)?;
        }
        Ok(page)
    }

    async fn list_scoped_page(
        &self,
        prefix: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<StateRecordSnapshot>, Option<String>), StorageFailure> {
        let mut request = StateListRequest::new(NAMESPACE)
            .map_err(StorageFailure::from)?
            .with_limit(limit)
            .map_err(StorageFailure::from)?;
        if let Some(prefix) = prefix {
            request =
                request.with_key_prefix(StateKeyPrefix::new(prefix).map_err(StorageFailure::from)?);
        }
        if let Some(after) = after {
            request = request.start_after(after).map_err(StorageFailure::from)?;
        }
        let page = self
            .inner
            .list(request)
            .await
            .map_err(StorageFailure::from)?;
        if page.records.len() > limit {
            return Err(StorageFailure::permanent(AppError::Integration(
                "storage page exceeds its requested bound".into(),
            )));
        }
        let mut previous = after;
        for snapshot in &page.records {
            if snapshot.record.key.namespace().as_str() != NAMESPACE
                || prefix.is_some_and(|prefix| !snapshot.record.key.key().starts_with(prefix))
                || previous.is_some_and(|previous| previous >= snapshot.record.key.key())
            {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "storage page has invalid scope or order".into(),
                )));
            }
            previous = Some(snapshot.record.key.key());
        }
        if page
            .next_start_after
            .as_deref()
            .is_some_and(|next| Some(next) != previous)
            || (page.records.is_empty() && page.next_start_after.is_some())
        {
            return Err(StorageFailure::permanent(AppError::Integration(
                "storage page cursor does not advance".into(),
            )));
        }
        Ok((page.records, page.next_start_after))
    }

    pub(crate) async fn select_run(
        &self,
        workspace: &WorkspaceId,
        workflow: &WorkflowId,
        selector: &RunSelector,
    ) -> Result<Option<RunKey>, AppError> {
        selector.validate()?;
        let occurrence = match selector {
            RunSelector::Exact { occurrence, .. } => occurrence.clone(),
            RunSelector::Latest(id) => {
                let Some(last) = self
                    .projection_tail(workspace, workflow, id, RunProjection::Checkpoint)
                    .await
                    .map_err(StorageFailure::into_error)?
                else {
                    return Ok(None);
                };
                last.occurrence
            }
        };
        let key = RunKey::new(
            workspace.clone(),
            workflow.clone(),
            selector.id().clone(),
            occurrence,
        );
        let Some((head, _)) = self
            .load_head(RecordKey::exact_run(&key))
            .await
            .map_err(StorageFailure::into_error)?
        else {
            return match selector {
                RunSelector::Exact { .. } => Ok(None),
                RunSelector::Latest(_) => Err(AppError::Integration(
                    "saved logical run points to a missing occurrence".into(),
                )),
            };
        };
        head.ensure_readable()?;
        Ok(Some(key))
    }

    pub(crate) async fn list_runs(
        &self,
        workspace: &WorkspaceId,
        workflow: &WorkflowId,
        after: Option<&RunSelector>,
        limit: NonZeroUsize,
    ) -> Result<Vec<RunRecord>, AppError> {
        if limit.get() > MAX_LIST_LIMIT {
            return Err(AppError::InvalidInput(
                "run list limit exceeds the supported bound".into(),
            ));
        }
        let prefix = format!("run/{workspace}/{workflow}/");
        let mut cursor = match after {
            Some(selector) => {
                self.select_run(workspace, workflow, selector)
                    .await?
                    .ok_or_else(|| AppError::NotFound("run cursor does not exist".into()))?;
                Some(match selector {
                    RunSelector::Latest(id) => format!("{prefix}{id}/~"),
                    RunSelector::Exact { id, occurrence } => format!("{prefix}{id}/{occurrence}"),
                })
            }
            None => None,
        };
        let mut records = Vec::new();
        loop {
            let (page, next) = self
                .list_page(
                    Some(&prefix),
                    cursor.as_deref(),
                    (limit.get() - records.len()).min(PAGE_SIZE),
                )
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in page {
                let head = RunHead::decode(&snapshot.record.value)?;
                if matches!(head.retention, RunRetention::Deleting { .. }) {
                    continue;
                }
                if let Some((record, _)) = self
                    .load_run(&RunKey::new(
                        workspace.clone(),
                        workflow.clone(),
                        head.id.clone(),
                        head.creation.clone(),
                    ))
                    .await?
                {
                    records.push(record);
                }
            }
            if records.len() == limit.get() || next.is_none() {
                return Ok(records);
            }
            cursor = next;
        }
    }

    pub(crate) async fn has_retained_history(
        &self,
        workspace: &WorkspaceId,
        workflow: Option<&WorkflowId>,
    ) -> Result<bool, AppError> {
        let prefix = match workflow {
            Some(workflow) => format!("run/{workspace}/{workflow}/"),
            None => format!("run/{workspace}/"),
        };
        let mut after = None;
        loop {
            let (page, next) = self
                .list_page(Some(&prefix), after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in page {
                let revision = snapshot.revision.get();
                let head = RunHead::decode(&snapshot.record.value)?;
                if !matches!(head.retention, RunRetention::Retained {}) {
                    continue;
                }
                let record = self
                    .load_checkpoint(&head, head.sequence)
                    .await
                    .map_err(StorageFailure::into_error)?;
                head.validate_row(&record)?;
                if !self
                    .head_unchanged(&head, revision)
                    .await
                    .map_err(StorageFailure::into_error)?
                {
                    return Err(AppError::Busy(
                        "workspace history changed during observation".into(),
                    ));
                }
                return Ok(true);
            }
            match next {
                Some(next) => after = Some(next),
                None => return Ok(false),
            }
        }
    }

    pub(crate) async fn list_all_runs(&self) -> Result<Vec<RunRecord>, AppError> {
        self.list_all_runs_for_recovery()
            .await
            .map_err(StorageFailure::into_error)
    }

    pub(crate) async fn list_all_runs_for_recovery(
        &self,
    ) -> Result<Vec<RunRecord>, StorageFailure> {
        let mut after = None;
        let mut records = Vec::new();
        loop {
            let (page, next) = self
                .list_page(Some("active/"), after.as_deref(), PAGE_SIZE)
                .await?;
            for snapshot in page {
                if records.len() == MAX_RUNS {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "durable run inventory exceeds the private bound".into(),
                    )));
                }
                let RecordKey::ActiveRun {
                    workspace,
                    workflow,
                    run,
                    occurrence,
                } = RecordKey::history_key(snapshot.record.key.key())
                    .map_err(StorageFailure::permanent)?
                else {
                    unreachable!()
                };
                let observed = async {
                    let Some((record, _)) = self
                        .load_run_for_recovery(&RunKey::new(
                            workspace.clone(),
                            workflow.clone(),
                            run.clone(),
                            occurrence.clone(),
                        ))
                        .await?
                    else {
                        return Ok(None);
                    };
                    if !self.run_is_active(&record).await? {
                        return Err(StorageFailure::Recoverable(AppError::Busy(
                            "active run inventory changed".into(),
                        )));
                    }
                    Ok(Some(record))
                }
                .await;
                match observed {
                    Ok(Some(record)) => records.push(record),
                    Ok(None)
                    | Err(StorageFailure::Permanent(AppError::Closed(_) | AppError::NotFound(_))) =>
                    {
                        // A captured list page can outlive both capacity
                        // release and explicit deletion. Disappearance or
                        // replacement of this exact marker proves inventory movement.
                        let marker: Option<(Value, u64)> = self
                            .load_record(RecordKey::ActiveRun {
                                workspace,
                                workflow,
                                run,
                                occurrence,
                            })
                            .await?;
                        if marker.is_some_and(|(_, revision)| revision == snapshot.revision.get()) {
                            return Err(StorageFailure::permanent(AppError::Integration(
                                "active run head is missing or inconsistent".into(),
                            )));
                        }
                        return Err(StorageFailure::Recoverable(AppError::Busy(
                            "active run inventory changed".into(),
                        )));
                    }
                    Err(failure) => return Err(failure),
                }
            }
            match next {
                Some(next) => after = Some(next),
                None => return Ok(records),
            }
        }
    }

    pub(crate) async fn preflight_records(&self) -> Result<(), AppError> {
        let mut after = None;
        loop {
            let (page, next) = self
                .list_page(None, after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in page {
                if snapshot.record.key.key().starts_with("run/") {
                    RunHead::decode(&snapshot.record.value)?;
                }
            }
            match next {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
    }

    pub(crate) async fn pending_deletions(
        &self,
        after: Option<&str>,
    ) -> Result<(Vec<RunKey>, Option<String>), AppError> {
        let (page, next) = self
            .list_page(Some("run/"), after, PAGE_SIZE)
            .await
            .map_err(StorageFailure::into_error)?;
        let mut deletions = Vec::new();
        for snapshot in page {
            let head = RunHead::decode(&snapshot.record.value)?;
            if matches!(head.retention, RunRetention::Deleting { .. }) {
                deletions.push(RunKey::new(
                    head.workspace_id,
                    head.workflow_id,
                    head.id,
                    head.creation,
                ));
            }
        }
        Ok((deletions, next))
    }

    pub(crate) async fn deletion_checkpoint(
        &self,
        key: &RunKey,
    ) -> Result<Option<u64>, StorageFailure> {
        let key = RecordKey::exact_run(key);
        Ok(self.load_head(key).await?.and_then(|(head, _)| {
            matches!(head.retention, RunRetention::Deleting { .. }).then_some(head.sequence)
        }))
    }

    pub(crate) async fn delete_run_page(
        &self,
        expected_app_revision: u64,
        run: &RunKey,
    ) -> Result<Option<AppMutation>, StorageFailure> {
        let key = RecordKey::exact_run(run);
        let Some((head, revision)) = self.load_head(key.clone()).await? else {
            return Ok(None);
        };
        let RunRetention::Deleting { after } = &head.retention else {
            return Err(StorageFailure::permanent(AppError::Conflict(
                "run deletion has not been sealed".into(),
            )));
        };
        // Listing the first remaining page also detects a forged progress key
        // which would otherwise hide undeleted earlier records.
        let (page, next) = self
            .list_page(Some(&head.history_prefix()), None, PAGE_SIZE)
            .await?;
        let mut keys = Vec::with_capacity(page.len());
        for snapshot in page {
            if after
                .as_ref()
                .is_some_and(|after| snapshot.record.key.key() <= after.as_str())
            {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "run deletion progress skipped a retained record".into(),
                )));
            }
            keys.push(
                RecordKey::history_key(snapshot.record.key.key())
                    .map_err(StorageFailure::permanent)?,
            );
        }
        Ok(Some(AppMutation::new(AppMutationKind::DeleteRunPage {
            expected_app_revision,
            expected_run_revision: revision,
            head: AppStorageRecord::new(key, &head, 0).map_err(StorageFailure::permanent)?,
            keys,
            complete: next.is_none(),
        })))
    }

    pub(crate) async fn commit(&self, mutation: AppMutation) -> AppCommitOutcome {
        let AppMutation { id, kind } = mutation;
        let receipt = match RecordKey::Receipt(id.clone()).record(&ReceiptRecord {
            epoch: EPOCH.to_owned(),
            id: id.clone(),
        }) {
            Ok(receipt) => receipt,
            Err(error) => {
                return AppCommitOutcome::Rejected(StorageFailure::permanent(error));
            }
        };
        let mut transaction = match self.inner.begin_transaction().await {
            Ok(transaction) => transaction,
            Err(error) => return AppCommitOutcome::Rejected(StorageFailure::from(error)),
        };
        let prepared: Result<(), StorageFailure> = async {
            if transaction
                .load(receipt.key.clone())
                .await
                .map_err(StorageFailure::from)?
                .is_some()
            {
                return Err(StorageFailure::permanent(AppError::Conflict(
                    "mutation identity already exists".to_owned(),
                )));
            }
            kind.stage(&mut transaction, self).await?;
            transaction.put(receipt).await.map_err(StorageFailure::from)
        }
        .await;
        if let Err(primary) = prepared {
            if transaction.rollback().await.is_err() {
                AppTrace::event(
                    || tracing::warn!(target: "app_sdk", event = "app.storage.mutation_rollback_failed"),
                );
                return AppCommitOutcome::Rejected(StorageFailure::CleanupFailed(
                    primary.into_error(),
                ));
            }
            return AppCommitOutcome::Rejected(primary);
        }
        match transaction.commit().await {
            Ok(()) => {
                self.cleanup_receipt(&id).await;
                AppCommitOutcome::Applied
            }
            Err(
                error @ (StateStoreError::TransactionConflict { .. }
                | StateStoreError::InvalidConfig(_)
                | StateStoreError::InvalidRequest(_)
                | StateStoreError::Serialization(_)),
            ) => AppCommitOutcome::Rejected(StorageFailure::from(error)),
            Err(
                error @ (StateStoreError::Unavailable { .. }
                | StateStoreError::BackendOperation { .. }
                | StateStoreError::TimedOut { .. }),
            ) => {
                let unknown = StorageFailure::from(error);
                let key = match RecordKey::Receipt(id.clone()).state_key() {
                    Ok(key) => key,
                    Err(error) => {
                        return AppCommitOutcome::Unknown(StorageFailure::permanent(error));
                    }
                };
                let snapshot = match self.inner.load(key).await {
                    Ok(Some(snapshot)) => snapshot,
                    Ok(None) => return AppCommitOutcome::Unknown(unknown),
                    Err(error) => {
                        return AppCommitOutcome::Unknown(StorageFailure::from(error));
                    }
                };
                if let Err(error) = RecordCodec::preflight_epoch(&snapshot.record.value) {
                    return AppCommitOutcome::Unknown(StorageFailure::permanent(error));
                }
                match RecordCodec::decode::<ReceiptRecord>(&snapshot.record.value) {
                    Ok(receipt) if receipt.id == id => {
                        self.cleanup_receipt(&receipt.id).await;
                        AppCommitOutcome::Applied
                    }
                    Ok(_) | Err(_) => AppCommitOutcome::Unknown(StorageFailure::permanent(
                        AppError::Integration("mutation receipt is invalid".to_owned()),
                    )),
                }
            }
        }
    }

    async fn load_record<T>(&self, key: RecordKey) -> Result<Option<(T, u64)>, StorageFailure>
    where
        T: DeserializeOwned,
    {
        let state_key = key.state_key().map_err(StorageFailure::permanent)?;
        let snapshot = self
            .inner
            .load(state_key.clone())
            .await
            .map_err(StorageFailure::from)?;
        snapshot
            .map(|snapshot| {
                RecordCodec::preflight_epoch(&snapshot.record.value)
                    .map_err(StorageFailure::permanent)?;
                RecordKey::validate_value(state_key.key(), &snapshot.record.value)
                    .map_err(StorageFailure::permanent)?;
                let revision = snapshot.revision.get();
                RecordCodec::decode(&snapshot.record.value)
                    .map(|record| (record, revision))
                    .map_err(StorageFailure::permanent)
            })
            .transpose()
    }

    pub(crate) async fn cleanup_receipts(&self) -> Result<(), AppError> {
        let mut after = None;
        loop {
            let (page, next) = self
                .list_page(Some("receipt/"), after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            if !page.is_empty() {
                let mut transaction = self
                    .inner
                    .begin_transaction()
                    .await
                    .map_err(AppError::from)?;
                for snapshot in page {
                    transaction
                        .delete(snapshot.record.key)
                        .await
                        .map_err(AppError::from)?;
                }
                transaction.commit().await.map_err(AppError::from)?;
            }
            match next {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
    }

    async fn cleanup_receipt(&self, id: &str) {
        // Only Applied mutations retire evidence. An idle partial page may
        // remain until reopen; uncertain owners' receipts never enter this queue.
        let retired = {
            let Ok(mut retired) = self.retired_receipts.lock() else {
                return;
            };
            retired.push(id.to_owned());
            if retired.len() < PAGE_SIZE {
                return;
            }
            std::mem::take(&mut *retired)
        };
        let Ok(mut transaction) = self.inner.begin_transaction().await else {
            return;
        };
        for id in retired {
            let Ok(key) = RecordKey::Receipt(id.clone()).state_key() else {
                continue;
            };
            let snapshot = match transaction.load(key.clone()).await {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => continue,
                Err(_) => {
                    let _ = transaction.rollback().await;
                    return;
                }
            };
            if RecordCodec::preflight_epoch(&snapshot.record.value).is_err() {
                continue;
            }
            let Ok(receipt) = RecordCodec::decode::<ReceiptRecord>(&snapshot.record.value) else {
                continue;
            };
            if receipt.id != id {
                continue;
            }
            if transaction.delete(key).await.is_err() {
                let _ = transaction.rollback().await;
                return;
            }
        }
        // This separate best-effort transaction cannot reject the already
        // applied mutation. Failed or cancelled cleanup is retried on reopen.
        let _ = transaction.commit().await;
    }
}

impl AppMutationKind {
    fn stage<'a, T: StateTransactionBackend, CS: StateStoreBackend>(
        self,
        transaction: &'a mut T,
        store: &'a AppStore<CS>,
    ) -> Pin<Box<impl Future<Output = Result<(), StorageFailure>> + 'a>> {
        Box::pin(async move {
            let mut observed_pages = Vec::new();
            match self {
                Self::ClaimApp {
                    fence,
                    app,
                    definitions,
                } => {
                    match fence {
                        AppClaimFence::Missing => {
                            app.key().expect_missing(transaction).await?;
                            let intended = RecordCodec::decode::<AppRecord>(app.value())
                                .map_err(StorageFailure::permanent)?;
                            if intended.epoch != EPOCH
                                || intended.generation != 2
                                || intended.run_count != 0
                                || !matches!(&intended.shutdown, ShutdownRecord::Active)
                            {
                                return Err(StorageFailure::permanent(AppError::Conflict(
                                    "initial application generation claim is invalid".to_owned(),
                                )));
                            }
                        }
                        AppClaimFence::Revision(expected) => {
                            let snapshot = transaction
                                .load(app.key().state_key().map_err(StorageFailure::permanent)?)
                                .await
                                .map_err(StorageFailure::from)?
                                .ok_or_else(|| {
                                    StorageFailure::permanent(AppError::NotFound(
                                        "durable record does not exist".to_owned(),
                                    ))
                                })?;
                            if snapshot.revision.get() != expected {
                                return Err(StorageFailure::permanent(AppError::Conflict(
                                    "durable record revision fence is stale".to_owned(),
                                )));
                            }
                            RecordCodec::preflight_epoch(&snapshot.record.value)
                                .map_err(StorageFailure::permanent)?;
                            RecordKey::validate_value(
                                snapshot.record.key.key(),
                                &snapshot.record.value,
                            )
                            .map_err(StorageFailure::permanent)?;
                            let current = RecordCodec::decode::<AppRecord>(&snapshot.record.value)
                                .map_err(StorageFailure::permanent)?;
                            let intended = RecordCodec::decode::<AppRecord>(app.value())
                                .map_err(StorageFailure::permanent)?;
                            let next_generation =
                                current.generation.checked_add(1).ok_or_else(|| {
                                    StorageFailure::permanent(AppError::Conflict(
                                        "application generation is exhausted".to_owned(),
                                    ))
                                })?;
                            let next_shutdown =
                                if matches!(&current.shutdown, ShutdownRecord::Closed) {
                                    ShutdownRecord::Active
                                } else {
                                    current.shutdown.clone()
                                };
                            if intended.epoch != current.epoch
                                || intended.generation != next_generation
                                || intended.run_count != current.run_count
                                || intended.shutdown != next_shutdown
                            {
                                return Err(StorageFailure::permanent(AppError::Conflict(
                                    "application generation claim changed durable authority"
                                        .to_owned(),
                                )));
                            }
                        }
                    }
                    for definition in definitions {
                        definition.catalog(transaction).await?;
                    }
                    app.put(transaction).await
                }
                Self::AdvanceLifecycle {
                    expected_app_revision,
                    generation,
                    shutdown,
                } => {
                    let key = RecordKey::App
                        .state_key()
                        .map_err(StorageFailure::permanent)?;
                    let snapshot = transaction
                        .load(key.clone())
                        .await
                        .map_err(StorageFailure::from)?
                        .ok_or_else(|| {
                            StorageFailure::permanent(AppError::NotFound(
                                "durable record does not exist".to_owned(),
                            ))
                        })?;
                    if snapshot.revision.get() != expected_app_revision {
                        return Err(StorageFailure::permanent(AppError::Conflict(
                            "durable record revision fence is stale".to_owned(),
                        )));
                    }
                    RecordCodec::preflight_epoch(&snapshot.record.value)
                        .map_err(StorageFailure::permanent)?;
                    RecordKey::validate_value(key.key(), &snapshot.record.value)
                        .map_err(StorageFailure::permanent)?;
                    let mut record = RecordCodec::decode::<AppRecord>(&snapshot.record.value)
                        .map_err(StorageFailure::permanent)?;
                    if record.generation != generation {
                        return Err(StorageFailure::permanent(AppError::Closed(
                            "application generation has been superseded".to_owned(),
                        )));
                    }
                    let legal = matches!(
                        (&record.shutdown, &shutdown),
                        (ShutdownRecord::Active, ShutdownRecord::Draining)
                            | (ShutdownRecord::Active, ShutdownRecord::Cancelling)
                            | (ShutdownRecord::Draining, ShutdownRecord::Cancelling)
                            | (ShutdownRecord::Draining, ShutdownRecord::Closed)
                            | (ShutdownRecord::Cancelling, ShutdownRecord::Closed)
                    );
                    if !legal {
                        return Err(StorageFailure::permanent(AppError::Conflict(
                            "application lifecycle transition is invalid".to_owned(),
                        )));
                    }
                    record.shutdown = shutdown;
                    AppStorageRecord::app(&record)
                        .map_err(StorageFailure::permanent)?
                        .put(transaction)
                        .await
                }
                Self::AdmitWorkspace {
                    expected_app_revision,
                    app,
                    workspace,
                } => {
                    app.key()
                        .expect_revision(transaction, expected_app_revision)
                        .await?;
                    workspace.key().expect_missing(transaction).await?;
                    app.put(transaction).await?;
                    workspace.put(transaction).await
                }
                Self::AdvanceWorkspace {
                    expected_app_revision,
                    expected_workspace_revision,
                    workspace,
                } => {
                    RecordKey::App
                        .expect_revision(transaction, expected_app_revision)
                        .await?;
                    workspace
                        .key()
                        .expect_revision(transaction, expected_workspace_revision)
                        .await?;
                    workspace.put(transaction).await
                }
                Self::AdvanceWorkspaceWithApp {
                    expected_app_revision,
                    expected_workspace_revision,
                    app,
                    workspace,
                } => {
                    app.key()
                        .expect_revision(transaction, expected_app_revision)
                        .await?;
                    workspace
                        .key()
                        .expect_revision(transaction, expected_workspace_revision)
                        .await?;
                    app.put(transaction).await?;
                    workspace.put(transaction).await
                }
                Self::DeleteWorkspace {
                    expected_app_revision,
                    expected_workspace_revision,
                    workspace,
                } => {
                    RecordKey::App
                        .expect_revision(transaction, expected_app_revision)
                        .await?;
                    workspace
                        .expect_revision(transaction, expected_workspace_revision)
                        .await?;
                    workspace.delete(transaction).await
                }
                Self::AdmitRun {
                    expected_app_revision,
                    expected_workspace_revision,
                    mut app,
                    workspace,
                    run,
                    head,
                    source,
                } => {
                    app.rebase_capacity(transaction, expected_app_revision, 1)
                        .await?;
                    workspace
                        .key()
                        .expect_revision(transaction, expected_workspace_revision)
                        .await?;
                    if let Some(source) = source {
                        source.validate(transaction, store).await?;
                    }
                    app.put(transaction).await?;
                    run.admit(transaction, head).await
                }
                Self::AdvanceRun {
                    expected_app_revision,
                    expected_run_revision,
                    mut app,
                    run,
                    predecessor,
                    children,
                } => {
                    app.rebase_capacity(transaction, expected_app_revision, 0)
                        .await?;
                    let head = run.load_head(transaction, expected_run_revision).await?;
                    let mut released = 0_usize;
                    if !children.is_empty() {
                        let parent: RunRecord =
                            run.decode_run().map_err(StorageFailure::permanent)?;
                        let previous = &predecessor;
                        let deleting = matches!(
                            parent.checkpoint.phase,
                            CheckpointPhaseRecord::Routing { .. }
                        ) && parent.checkpoint.base_sequence
                            == parent.checkpoint.sequence;
                        let mut admitted = BTreeMap::new();
                        if deleting {
                            for activation in &previous.checkpoint.staged {
                                if let Some((sequence, members)) = &activation.children {
                                    for (index, member) in members.iter().enumerate() {
                                        admitted.insert(
                                            &member.key,
                                            (
                                                *sequence,
                                                index,
                                                member.revision.as_str(),
                                                member.history,
                                            ),
                                        );
                                    }
                                }
                            }
                        } else {
                            // Release only the original complete Child group
                            // transferred by this Stage, before its parent barrier.
                            for activation in &parent.checkpoint.staged {
                                if !matches!(
                                    activation.outcome,
                                    StagedOutcomeRecord::Complete { .. }
                                ) {
                                    continue;
                                }
                                let Some((sequence, members)) = &activation.children else {
                                    continue;
                                };
                                let Some(control) = previous.checkpoint.control(*sequence) else {
                                    continue;
                                };
                                let ControlRecordState::Child {
                                    inputs,
                                    admitted: original,
                                } = &control.state
                                else {
                                    continue;
                                };
                                if activation.node != control.node
                                    || activation.lineage != control.lineage
                                    || members != original
                                    || members.len() != inputs.len()
                                    || parent.checkpoint.control(*sequence).is_some()
                                    || parent.checkpoint.base_sequence
                                        != previous.checkpoint.base_sequence
                                {
                                    return Err(StorageFailure::permanent(AppError::Conflict(
                                        "child capacity requires its completed original Stage"
                                            .into(),
                                    )));
                                }
                                for (index, member) in members.iter().enumerate() {
                                    admitted.insert(
                                        &member.key,
                                        (
                                            *sequence,
                                            index,
                                            member.revision.as_str(),
                                            member.history,
                                        ),
                                    );
                                }
                            }
                        }
                        for (child, revision) in children {
                            let key = RecordKey::Run {
                                workspace: child.workspace().clone(),
                                workflow: child.workflow().clone(),
                                run: child.run().clone(),
                                occurrence: child.occurrence().to_owned(),
                            }
                            .state_key()
                            .map_err(StorageFailure::permanent)?;
                            let snapshot = transaction
                                .load(key.clone())
                                .await
                                .map_err(StorageFailure::from)?
                                .ok_or_else(|| {
                                    StorageFailure::permanent(AppError::Integration(
                                        "copied child head is missing".into(),
                                    ))
                                })?;
                            RecordKey::validate_value(key.key(), &snapshot.record.value)
                                .map_err(StorageFailure::permanent)?;
                            let mut child_head = RunHead::decode(&snapshot.record.value)
                                .map_err(StorageFailure::permanent)?;
                            let key = child_head
                                .checkpoint_key(child_head.sequence)
                                .state_key()
                                .map_err(StorageFailure::permanent)?;
                            let snapshot = transaction
                                .load(key.clone())
                                .await
                                .map_err(StorageFailure::from)?
                                .ok_or_else(|| {
                                    StorageFailure::permanent(AppError::Integration(
                                        "copied child checkpoint is missing".into(),
                                    ))
                                })?;
                            RecordKey::validate_value(key.key(), &snapshot.record.value)
                                .map_err(StorageFailure::permanent)?;
                            let record: RunRecord = store
                                .decode_run(transaction, snapshot, false, &mut observed_pages)
                                .await?;
                            child_head
                                .validate_row(&record)
                                .map_err(StorageFailure::permanent)?;
                            let relation = record.parent.as_ref().ok_or_else(|| {
                                StorageFailure::permanent(AppError::Conflict(
                                    "copied child has no original parent".into(),
                                ))
                            })?;
                            let membership = admitted.remove(&child);
                            if !matches!(record.checkpoint.phase, CheckpointPhaseRecord::Succeeded)
                                || record.workspace_id != parent.workspace_id
                                || relation.workflow_id != parent.workflow_id || relation.run_id != parent.id
                                || relation.occurrence != parent.occurrence
                                || relation.base_checkpoint != previous.checkpoint.base_sequence
                                || previous.parent.as_ref().map_or(0, |parent| parent.depth).checked_add(1) != Some(relation.depth)
                                || relation.depth > crate::workflow::run::MAX_CHILD_DEPTH
                                || membership.is_none_or(|(sequence, index, selected, history)| {
                                    sequence != relation.control_sequence || index != relation.index || selected != revision
                                        || deleting && history != crate::workflow::definition::ChildHistory::DeleteAfterReduction
                                })
                                || record.definition_revision != revision
                                || record.child_revisions != previous.child_revisions
                                || record.agent_revisions.iter().any(|(key, revision)| previous.agent_revisions.get(key) != Some(revision))
                            {
                                return Err(StorageFailure::permanent(AppError::Conflict("copied child authority changed".into())));
                            }
                            match child_head.retention {
                                RunRetention::Active {} => {
                                    child_head.active_key().expect_present(transaction).await?;
                                    child_head.active_key().delete(transaction).await?;
                                    released += 1;
                                }
                                RunRetention::Retained {} => {
                                    child_head.active_key().expect_missing(transaction).await?;
                                    if !deleting {
                                        continue;
                                    }
                                }
                                RunRetention::Deleting { .. } => {
                                    return Err(StorageFailure::permanent(AppError::Conflict(
                                        "copied child is already sealed".into(),
                                    )));
                                }
                            }
                            child_head.retention = if deleting {
                                RunRetention::Deleting { after: None }
                            } else {
                                RunRetention::Retained {}
                            };
                            transaction
                                .put(child_head.record().map_err(StorageFailure::permanent)?)
                                .await
                                .map_err(StorageFailure::from)?;
                        }
                    }
                    if released != 0 {
                        let mut app: AppRecord =
                            RecordCodec::decode(app.value()).map_err(StorageFailure::permanent)?;
                        app.run_count = app.run_count.checked_sub(released).ok_or_else(|| {
                            StorageFailure::permanent(AppError::Integration(
                                "copied child capacity is inconsistent".into(),
                            ))
                        })?;
                        AppStorageRecord::app(&app)
                            .map_err(StorageFailure::permanent)?
                            .put(transaction)
                            .await?;
                    }
                    run.append(transaction, head, predecessor, store, &mut observed_pages)
                        .await
                }
                Self::AdmitChildren {
                    expected_app_revision,
                    expected_workspace_revision,
                    expected_parent_revision,
                    mut app,
                    workspace,
                    parent,
                    predecessor,
                    children,
                } => {
                    app.rebase_capacity(
                        transaction,
                        expected_app_revision,
                        children.len() as isize,
                    )
                    .await?;
                    workspace
                        .key()
                        .expect_revision(transaction, expected_workspace_revision)
                        .await?;
                    let head = parent
                        .load_head(transaction, expected_parent_revision)
                        .await?;
                    for (_, _, source) in &children {
                        if let Some(source) = source {
                            source.validate(transaction, store).await?;
                        }
                    }
                    app.put(transaction).await?;
                    parent
                        .append(transaction, head, predecessor, store, &mut observed_pages)
                        .await?;
                    for (child, head, _) in children {
                        child.admit(transaction, head).await?;
                    }
                    Ok(())
                }
                Self::RetainRun {
                    expected_app_revision,
                    expected_run_revision,
                    mut app,
                    run,
                } => {
                    app.rebase_capacity(transaction, expected_app_revision, -1)
                        .await?;
                    let mut head = run.load_head(transaction, expected_run_revision).await?;
                    let record: RunRecord = run.decode_run().map_err(StorageFailure::permanent)?;
                    head.validate_row(&record)
                        .map_err(StorageFailure::permanent)?;
                    if !record.checkpoint.is_terminal()
                        || !matches!(head.retention, RunRetention::Active {})
                    {
                        return Err(StorageFailure::permanent(AppError::Conflict(
                            "run capacity is not eligible for release".into(),
                        )));
                    }
                    run.expect_value(transaction, store, &mut observed_pages)
                        .await?;
                    head.active_key().expect_present(transaction).await?;
                    head.retention = RunRetention::Retained {};
                    app.put(transaction).await?;
                    head.active_key().delete(transaction).await?;
                    transaction
                        .put(head.record().map_err(StorageFailure::permanent)?)
                        .await
                        .map_err(StorageFailure::from)
                }
                Self::ReadmitRun {
                    expected_app_revision,
                    expected_workspace_revision,
                    expected_run_revision,
                    mut app,
                    workspace,
                    run,
                    predecessor,
                } => {
                    app.rebase_capacity(transaction, expected_app_revision, 1)
                        .await?;
                    workspace
                        .key()
                        .expect_revision(transaction, expected_workspace_revision)
                        .await?;
                    let mut head = run.load_head(transaction, expected_run_revision).await?;
                    if !matches!(head.retention, RunRetention::Retained {}) {
                        return Err(StorageFailure::permanent(AppError::Conflict(
                            "run capacity is already admitted".into(),
                        )));
                    }
                    let prior = transaction
                        .load(
                            head.checkpoint_key(head.sequence)
                                .state_key()
                                .map_err(StorageFailure::permanent)?,
                        )
                        .await
                        .map_err(StorageFailure::from)?
                        .ok_or_else(|| {
                            StorageFailure::permanent(AppError::Integration(
                                "retained run checkpoint is missing".into(),
                            ))
                        })?;
                    RecordCodec::preflight_epoch(&prior.record.value)
                        .map_err(StorageFailure::permanent)?;
                    RecordKey::validate_value(prior.record.key.key(), &prior.record.value)
                        .map_err(StorageFailure::permanent)?;
                    let prior: RunRecord = store
                        .decode_run(transaction, prior, false, &mut observed_pages)
                        .await?;
                    head.validate_row(&prior)
                        .map_err(StorageFailure::permanent)?;
                    head.active_key().expect_missing(transaction).await?;
                    head.retention = RunRetention::Active {};
                    let active = head
                        .active_key()
                        .index_record()
                        .map_err(StorageFailure::permanent)?;
                    app.put(transaction).await?;
                    run.append(transaction, head, predecessor, store, &mut observed_pages)
                        .await?;
                    transaction.put(active).await.map_err(StorageFailure::from)
                }
                Self::SealRun {
                    expected_app_revision,
                    expected_run_revision,
                    run,
                    parent,
                } => {
                    RecordKey::App
                        .expect_revision(transaction, expected_app_revision)
                        .await?;
                    let mut head = run.load_head(transaction, expected_run_revision).await?;
                    let record: RunRecord = run.decode_run().map_err(StorageFailure::permanent)?;
                    head.validate_row(&record)
                        .map_err(StorageFailure::permanent)?;
                    if !matches!(head.retention, RunRetention::Retained {})
                        || !record.checkpoint.is_terminal()
                    {
                        return Err(StorageFailure::permanent(AppError::Conflict(
                            "run history is not eligible for deletion".into(),
                        )));
                    }
                    run.expect_value(transaction, store, &mut observed_pages)
                        .await?;
                    match (&record.parent, parent) {
                        (Some(relation), Some((parent, revision))) => {
                            let parent_record: RunRecord =
                                parent.decode_run().map_err(StorageFailure::permanent)?;
                            if parent_record.workspace_id != record.workspace_id
                                || parent_record.workflow_id != relation.workflow_id
                                || parent_record.id != relation.run_id
                                || parent_record.occurrence != relation.occurrence
                            {
                                return Err(StorageFailure::permanent(AppError::Conflict(
                                    "deletion parent fence has foreign identity".into(),
                                )));
                            }
                            let parent_head = parent.load_head(transaction, revision).await?;
                            parent_head
                                .ensure_readable()
                                .map_err(StorageFailure::permanent)?;
                            parent_head
                                .validate_row(&parent_record)
                                .map_err(StorageFailure::permanent)?;
                            parent
                                .expect_value(transaction, store, &mut observed_pages)
                                .await?;
                        }
                        (Some(relation), None) => {
                            RecordKey::Run {
                                workspace: record.workspace_id.clone(),
                                workflow: relation.workflow_id.clone(),
                                run: relation.run_id.clone(),
                                occurrence: relation.occurrence.clone(),
                            }
                            .expect_missing(transaction)
                            .await?;
                        }
                        (None, None) => {}
                        (None, Some(_)) => {
                            return Err(StorageFailure::permanent(AppError::Conflict(
                                "root deletion cannot fence a parent".into(),
                            )));
                        }
                    }
                    head.active_key().expect_missing(transaction).await?;
                    head.retention = RunRetention::Deleting { after: None };
                    transaction
                        .put(head.record().map_err(StorageFailure::permanent)?)
                        .await
                        .map_err(StorageFailure::from)
                }
                Self::DeleteRunPage {
                    expected_app_revision,
                    expected_run_revision,
                    head,
                    keys,
                    complete,
                } => {
                    RecordKey::App
                        .expect_revision(transaction, expected_app_revision)
                        .await?;
                    head.key()
                        .expect_revision(transaction, expected_run_revision)
                        .await?;
                    head.expect_value(transaction, store, &mut observed_pages)
                        .await?;
                    let mut decoded =
                        RunHead::decode(head.value()).map_err(StorageFailure::permanent)?;
                    let RunRetention::Deleting { after } = &decoded.retention else {
                        return Err(StorageFailure::permanent(AppError::Conflict(
                            "run history deletion is not admitted".into(),
                        )));
                    };
                    let mut previous = after.clone();
                    for key in &keys {
                        let text = key
                            .state_key()
                            .map_err(StorageFailure::permanent)?
                            .key()
                            .to_owned();
                        let valid = match key {
                            RecordKey::Checkpoint {
                                workspace,
                                workflow,
                                run,
                                occurrence,
                                sequence,
                            }
                            | RecordKey::Ledger {
                                workspace,
                                workflow,
                                run,
                                occurrence,
                                sequence,
                                ..
                            }
                            | RecordKey::ActionObservation {
                                workspace,
                                workflow,
                                run,
                                occurrence,
                                sequence,
                                ..
                            } => {
                                workspace == &decoded.workspace_id
                                    && workflow == &decoded.workflow_id
                                    && run == &decoded.id
                                    && occurrence == &decoded.creation
                                    && *sequence <= decoded.sequence
                            }
                            RecordKey::ProjectionLink { key, sequence, .. } => {
                                key.workspace() == &decoded.workspace_id
                                    && key.workflow() == &decoded.workflow_id
                                    && key.run() == &decoded.id
                                    && key.occurrence() == decoded.creation
                                    && *sequence <= decoded.sequence
                            }
                            _ => false,
                        };
                        if !valid || previous.as_ref().is_some_and(|previous| previous >= &text) {
                            return Err(StorageFailure::permanent(AppError::Integration(
                                "run deletion page is invalid".into(),
                            )));
                        }
                        previous = Some(text);
                    }
                    for key in keys {
                        match &key {
                            RecordKey::ActionObservation {
                                workspace,
                                workflow,
                                run,
                                occurrence,
                                ordinal,
                                sequence,
                            } => {
                                RunProjection::Action(*ordinal)
                                    .remove(
                                        transaction,
                                        &RunKey::new(
                                            workspace.clone(),
                                            workflow.clone(),
                                            run.clone(),
                                            occurrence.clone(),
                                        ),
                                        *sequence,
                                        EPOCH,
                                    )
                                    .await?;
                            }
                            RecordKey::ProjectionLink {
                                key,
                                projection,
                                sequence,
                            } => {
                                projection
                                    .remove(transaction, key, *sequence, EPOCH)
                                    .await?;
                            }
                            _ => key.delete(transaction).await?,
                        }
                    }
                    if complete {
                        decoded.head_key().delete(transaction).await
                    } else {
                        decoded.retention = RunRetention::Deleting { after: previous };
                        transaction
                            .put(decoded.record().map_err(StorageFailure::permanent)?)
                            .await
                            .map_err(StorageFailure::from)
                    }
                }
            }
        })
    }
}

impl AppStorageRecord {
    fn key(&self) -> &RecordKey {
        match self {
            Self::Value { key, .. } | Self::Checkpoint { key, .. } => key,
        }
    }

    fn value(&self) -> &Value {
        match self {
            Self::Value { value, .. } | Self::Checkpoint { value, .. } => value,
        }
    }

    fn definitions(
        definitions: &BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
    ) -> Result<Vec<Self>, AppError> {
        definitions
            .iter()
            .flat_map(|(id, revisions)| {
                revisions.iter().map(move |definition| {
                    let record = DefinitionRecord {
                        epoch: EPOCH.into(),
                        workflow_id: id.clone(),
                        revision: definition.revision().to_owned(),
                        canonical: definition.fingerprint().to_owned(),
                    };
                    Self::new(record.key(), &record, 0)
                })
            })
            .collect()
    }

    async fn catalog<T: StateTransactionBackend>(
        self,
        transaction: &mut T,
    ) -> Result<(), StorageFailure> {
        let key = self.key().state_key().map_err(StorageFailure::permanent)?;
        if let Some(current) = transaction
            .load(key.clone())
            .await
            .map_err(StorageFailure::from)?
        {
            RecordKey::validate_value(key.key(), &current.record.value)
                .map_err(StorageFailure::permanent)?;
            let current: DefinitionRecord =
                RecordCodec::decode(&current.record.value).map_err(StorageFailure::permanent)?;
            let intended: DefinitionRecord =
                RecordCodec::decode(self.value()).map_err(StorageFailure::permanent)?;
            if current.canonical != intended.canonical {
                return Err(StorageFailure::permanent(AppError::Conflict(
                    "configured workflow revision differs from its durable definition".into(),
                )));
            }
            Ok(())
        } else {
            self.put(transaction).await
        }
    }

    async fn rebase_capacity<T: StateTransactionBackend>(
        &mut self,
        transaction: &mut T,
        expected: u64,
        change: isize,
    ) -> Result<(), StorageFailure> {
        let snapshot = transaction
            .load(self.key().state_key().map_err(StorageFailure::permanent)?)
            .await
            .map_err(StorageFailure::from)?
            .ok_or_else(|| {
                StorageFailure::permanent(AppError::NotFound(
                    "durable record does not exist".into(),
                ))
            })?;
        RecordCodec::preflight_epoch(&snapshot.record.value).map_err(StorageFailure::permanent)?;
        RecordKey::validate_value("app", &snapshot.record.value)
            .map_err(StorageFailure::permanent)?;
        let mut current: AppRecord =
            RecordCodec::decode(&snapshot.record.value).map_err(StorageFailure::permanent)?;
        let intended = AppRecord::deserialize(self.value()).map_err(|_| {
            StorageFailure::permanent(AppError::Integration(
                "app durable record is corrupt".into(),
            ))
        })?;
        let count = current.run_count;
        // Only the shared count can commute with another accepted run mutation.
        // Generation, lifecycle and every retained pin remain exact authority.
        current.run_count = intended.run_count;
        if current != intended {
            return Err(StorageFailure::permanent(AppError::Conflict(
                "run capacity mutation changes unrelated application authority".into(),
            )));
        }
        let count = count.checked_add_signed(change).ok_or_else(|| {
            StorageFailure::permanent(AppError::Conflict(
                "run capacity transition is invalid".into(),
            ))
        })?;
        if snapshot.revision.get() < expected
            || snapshot.revision.get() == expected && count != intended.run_count
        {
            return Err(StorageFailure::permanent(AppError::Conflict(
                "run capacity revision or count fence is stale".into(),
            )));
        }
        if count > MAX_RUNS {
            return Err(StorageFailure::permanent(AppError::Busy(
                "application run capacity is exhausted".into(),
            )));
        }
        current.run_count = count;
        *self = Self::app(&current).map_err(StorageFailure::permanent)?;
        Ok(())
    }

    fn app(record: &AppRecord) -> Result<Self, AppError> {
        Self::new(RecordKey::App, record, 0)
    }

    fn workspace(record: &WorkspaceRecord) -> Result<Self, AppError> {
        Self::new(RecordKey::Workspace(record.id.clone()), record, 0)
    }

    fn predecessor(record: &RunRecord) -> Result<RunRecord, AppError> {
        let predecessor = record.observed_predecessor()?;
        let key = RecordKey::checkpoint(&predecessor);
        let reserved_bytes = predecessor.checkpoint.prospective_settlement_bytes()?;
        let state_key = key.state_key()?;
        let size = RecordCodec::logical_run_len(&predecessor)?;
        if size.saturating_add(reserved_bytes) > MAX_RECORD_BYTES {
            return Err(AppError::InvalidInput(
                "app durable record exceeds the private byte bound".into(),
            ));
        }
        // The derived key and typed predecessor share their exact identity.
        // Keep key validation after the complete encoded-size check; `previous`
        // is omitted by RunRecord serialization and is never stored here.
        if RecordKey::history_key(state_key.key())? != key {
            return Err(AppError::Integration(
                "durable history value does not match its key".into(),
            ));
        }
        Ok(predecessor)
    }

    fn head(record: &RunRecord) -> Result<Self, AppError> {
        let head = RunHead::admitted(record)?;
        Self::new(head.head_key(), &head, 0)
    }

    async fn load_head<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        expected: u64,
    ) -> Result<RunHead, StorageFailure> {
        let key = self
            .key()
            .head_key()
            .map_err(StorageFailure::permanent)?
            .state_key()
            .map_err(StorageFailure::permanent)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(StorageFailure::from)?
            .ok_or_else(|| {
                StorageFailure::permanent(AppError::NotFound("workflow run does not exist".into()))
            })?;
        if snapshot.revision.get() != expected {
            return Err(StorageFailure::permanent(AppError::Conflict(
                "run head revision fence is stale".into(),
            )));
        }
        RecordKey::validate_value(key.key(), &snapshot.record.value)
            .map_err(StorageFailure::permanent)?;
        RunHead::decode(&snapshot.record.value).map_err(StorageFailure::permanent)
    }

    async fn expect_value<T: StateTransactionBackend, CS: StateStoreBackend>(
        &self,
        transaction: &mut T,
        store: &AppStore<CS>,
        observed_pages: &mut Vec<StateRecordSnapshot>,
    ) -> Result<(), StorageFailure> {
        let snapshot = transaction
            .load(self.key().state_key().map_err(StorageFailure::permanent)?)
            .await
            .map_err(StorageFailure::from)?;
        let matches = match snapshot {
            Some(snapshot) if matches!(self.key(), RecordKey::Checkpoint { .. }) => {
                let actual: RunRecord = store
                    .decode_run(transaction, snapshot, false, observed_pages)
                    .await?;
                self.matches_run(&actual)
                    .map_err(StorageFailure::permanent)?
            }
            Some(snapshot) => snapshot.record.value.as_ref() == self.value(),
            None => false,
        };
        if !matches {
            return Err(StorageFailure::permanent(AppError::Conflict(
                "immutable checkpoint evidence changed".into(),
            )));
        }
        Ok(())
    }

    fn decode_run(&self) -> Result<RunRecord, AppError> {
        match self {
            Self::Checkpoint { record, .. } => Ok(record.as_ref().clone()),
            Self::Value { .. } => Err(AppError::Integration("expected checkpoint evidence".into())),
        }
    }

    fn matches_run(&self, actual: &RunRecord) -> Result<bool, AppError> {
        match self {
            Self::Checkpoint { record, value, .. } => {
                let expected: SettlementPages =
                    RecordCodec::decode(&value["checkpoint"]["settlements"])?;
                Ok(actual.same_persisted_row(record)?
                    && actual.checkpoint.settlement_references() == expected.pages)
            }
            Self::Value { .. } => Err(AppError::Integration("expected checkpoint evidence".into())),
        }
    }

    fn append<'a, T: StateTransactionBackend, CS: StateStoreBackend>(
        self,
        transaction: &'a mut T,
        mut head: RunHead,
        predecessor: RunRecord,
        store: &'a AppStore<CS>,
        observed_pages: &'a mut Vec<StateRecordSnapshot>,
    ) -> Pin<Box<impl Future<Output = Result<(), StorageFailure>> + 'a>> {
        Box::pin(async move {
            let record: RunRecord = self.decode_run().map_err(StorageFailure::permanent)?;
            let prior = transaction
                .load(
                    head.checkpoint_key(head.sequence)
                        .state_key()
                        .map_err(StorageFailure::permanent)?,
                )
                .await
                .map_err(StorageFailure::from)?
                .ok_or_else(|| {
                    StorageFailure::permanent(AppError::Integration(
                        "current checkpoint is missing".into(),
                    ))
                })?;
            RecordCodec::preflight_epoch(&prior.record.value).map_err(StorageFailure::permanent)?;
            RecordKey::validate_value(prior.record.key.key(), &prior.record.value)
                .map_err(StorageFailure::permanent)?;
            let prior: RunRecord = store
                .decode_run(transaction, prior, false, observed_pages)
                .await?;
            if RecordKey::checkpoint(&predecessor) != head.checkpoint_key(head.sequence)
                || prior.checkpoint.settlement_references()
                    != predecessor.checkpoint.settlement_references()
                || !prior
                    .same_persisted_row(&predecessor)
                    .map_err(StorageFailure::permanent)?
            {
                return Err(StorageFailure::permanent(AppError::Conflict(
                    "immutable checkpoint predecessor changed".into(),
                )));
            }
            head.validate_row(&prior)
                .map_err(StorageFailure::permanent)?;
            if !record.same_run(&prior) {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "checkpoint changes immutable run identity".into(),
                )));
            }
            head.advance(&record, &prior)
                .map_err(StorageFailure::permanent)?;
            self.key().expect_missing(transaction).await?;
            for projection in RunProjection::for_checkpoint(&record, Some(&prior)) {
                projection
                    .append(transaction, &record, record.checkpoint.sequence)
                    .await?;
            }
            let before = prior.checkpoint.is_copy_clean();
            let after = record.checkpoint.is_copy_clean();
            if before && !after {
                RunProjection::Unsettled
                    .append(transaction, &record, 0)
                    .await?;
            } else if !before && after {
                RunProjection::Unsettled
                    .remove(transaction, &record.key(), 0, EPOCH)
                    .await?;
            }
            self.put(transaction).await?;
            transaction
                .put(head.record().map_err(StorageFailure::permanent)?)
                .await
                .map_err(StorageFailure::from)
        })
    }

    async fn admit<T: StateTransactionBackend>(
        self,
        transaction: &mut T,
        head: Self,
    ) -> Result<(), StorageFailure> {
        let decoded = RunHead::decode(head.value()).map_err(StorageFailure::permanent)?;
        let record: RunRecord = self.decode_run().map_err(StorageFailure::permanent)?;
        decoded
            .validate_row(&record)
            .map_err(StorageFailure::permanent)?;
        head.key().expect_missing(transaction).await?;
        self.key().expect_missing(transaction).await?;
        decoded.active_key().expect_missing(transaction).await?;
        for projection in RunProjection::for_checkpoint(&record, None) {
            projection
                .append(transaction, &record, record.checkpoint.sequence)
                .await?;
        }
        if !record.checkpoint.is_copy_clean() {
            RunProjection::Unsettled
                .append(transaction, &record, 0)
                .await?;
        }
        self.put(transaction).await?;
        head.put(transaction).await?;
        transaction
            .put(
                decoded
                    .active_key()
                    .index_record()
                    .map_err(StorageFailure::permanent)?,
            )
            .await
            .map_err(StorageFailure::from)
    }

    fn new<T>(key: RecordKey, record: &T, reserved_bytes: usize) -> Result<Self, AppError>
    where
        T: Serialize,
    {
        let state_key = key.state_key()?;
        let value = RecordCodec::encode(record, reserved_bytes)?;
        RecordKey::validate_value(state_key.key(), &value)?;
        Ok(Self::Value { key, value })
    }

    async fn put<T: StateTransactionBackend>(
        self,
        transaction: &mut T,
    ) -> Result<(), StorageFailure> {
        let (key, value) = match self {
            Self::Value { key, value } => (key, value),
            Self::Checkpoint {
                key, value, pages, ..
            } => {
                for page in pages {
                    if transaction
                        .load(page.key.clone())
                        .await
                        .map_err(StorageFailure::from)?
                        .is_some()
                    {
                        return Err(StorageFailure::permanent(AppError::Conflict(
                            "immutable settlement page already exists".into(),
                        )));
                    }
                    transaction.put(page).await.map_err(StorageFailure::from)?;
                }
                (key, value)
            }
        };
        transaction
            .put(StateRecord::new(
                key.state_key().map_err(StorageFailure::permanent)?,
                value,
            ))
            .await
            .map_err(StorageFailure::from)
    }
}

impl AppMutation {
    pub(crate) fn claim_new_app(
        record: &AppRecord,
        definitions: &BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
    ) -> Result<Self, AppError> {
        Ok(Self::new(AppMutationKind::ClaimApp {
            fence: AppClaimFence::Missing,
            app: AppStorageRecord::app(record)?,
            definitions: AppStorageRecord::definitions(definitions)?,
        }))
    }

    pub(crate) fn claim_app_revision(
        expected_app_revision: u64,
        record: &AppRecord,
        definitions: &BTreeMap<WorkflowId, Vec<Arc<CompiledDefinition>>>,
    ) -> Result<Self, AppError> {
        Ok(Self::new(AppMutationKind::ClaimApp {
            fence: AppClaimFence::Revision(expected_app_revision),
            app: AppStorageRecord::app(record)?,
            definitions: AppStorageRecord::definitions(definitions)?,
        }))
    }

    pub(crate) fn advance_lifecycle(
        expected_app_revision: u64,
        generation: u64,
        shutdown: ShutdownRecord,
    ) -> Result<Self, AppError> {
        if matches!(&shutdown, ShutdownRecord::Active) {
            return Err(AppError::Integration(
                "application lifecycle mutation cannot publish active state".to_owned(),
            ));
        }
        Ok(Self::new(AppMutationKind::AdvanceLifecycle {
            expected_app_revision,
            generation,
            shutdown,
        }))
    }

    pub(crate) fn admit_workspace(
        expected_app_revision: u64,
        app: &AppRecord,
        workspace: &WorkspaceRecord,
    ) -> Result<Self, AppError> {
        Ok(Self::new(AppMutationKind::AdmitWorkspace {
            expected_app_revision,
            app: AppStorageRecord::app(app)?,
            workspace: AppStorageRecord::workspace(workspace)?,
        }))
    }

    pub(crate) fn advance_workspace(
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        workspace: &WorkspaceRecord,
    ) -> Result<Self, AppError> {
        Ok(Self::new(AppMutationKind::AdvanceWorkspace {
            expected_app_revision,
            expected_workspace_revision,
            workspace: AppStorageRecord::workspace(workspace)?,
        }))
    }

    pub(crate) fn advance_workspace_with_app(
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        app: &AppRecord,
        workspace: &WorkspaceRecord,
    ) -> Result<Self, AppError> {
        Ok(Self::new(AppMutationKind::AdvanceWorkspaceWithApp {
            expected_app_revision,
            expected_workspace_revision,
            app: AppStorageRecord::app(app)?,
            workspace: AppStorageRecord::workspace(workspace)?,
        }))
    }

    pub(crate) fn delete_workspace(
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        workspace: &WorkspaceId,
    ) -> Result<Self, AppError> {
        RecordKey::Workspace(workspace.clone()).state_key()?;
        Ok(Self::new(AppMutationKind::DeleteWorkspace {
            expected_app_revision,
            expected_workspace_revision,
            workspace: RecordKey::Workspace(workspace.clone()),
        }))
    }

    pub(crate) fn admit_run(
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        app: &AppRecord,
        workspace: &WorkspaceRecord,
        run: &RunRecord,
    ) -> Result<Self, AppError> {
        if workspace.id != run.workspace_id || workspace.revision != run.workspace_revision {
            return Err(AppError::Integration(
                "run admission workspace identity is inconsistent".to_owned(),
            ));
        }
        Ok(Self::new(AppMutationKind::AdmitRun {
            expected_app_revision,
            expected_workspace_revision,
            app: AppStorageRecord::app(app)?,
            workspace: AppStorageRecord::workspace(workspace)?,
            run: RecordCodec::run(run)?,
            head: AppStorageRecord::head(run)?,
            source: None,
        }))
    }

    pub(crate) fn admit_restore(
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        app: &AppRecord,
        workspace: &WorkspaceRecord,
        run: &RunRecord,
        selected: &RunCopy,
    ) -> Result<Self, AppError> {
        if selected.source.as_ref().is_some_and(|(source, _)| {
            source.workspace_id != run.workspace_id || source.workflow_id != run.workflow_id
        }) {
            return Err(AppError::InvalidInput(
                "restore source is outside the workflow scope".into(),
            ));
        }
        let mut mutation = Self::admit_run(
            expected_app_revision,
            expected_workspace_revision,
            app,
            workspace,
            run,
        )?;
        let AppMutationKind::AdmitRun { source, .. } = &mut mutation.kind else {
            unreachable!()
        };
        *source = Some(Box::new(selected.clone()));
        Ok(mutation)
    }

    pub(crate) fn advance_run(
        expected_app_revision: u64,
        expected_run_revision: u64,
        app: &AppRecord,
        run: &RunRecord,
        children: &[(RunKey, String)],
    ) -> Result<Self, AppError> {
        let mut children = children.to_vec();
        if children.is_empty()
            && let Some(previous) = &run.previous
        {
            for activation in &run.checkpoint.staged {
                if matches!(activation.outcome, StagedOutcomeRecord::Complete { .. })
                    && let Some((sequence, members)) = &activation.children
                    && previous.control(*sequence).is_some()
                {
                    children.extend(
                        members
                            .iter()
                            .map(|member| (member.key.clone(), member.revision.clone())),
                    );
                }
            }
        }
        Ok(Self::new(AppMutationKind::AdvanceRun {
            expected_app_revision,
            expected_run_revision,
            app: AppStorageRecord::app(app)?,
            run: RecordCodec::run(run)?,
            predecessor: AppStorageRecord::predecessor(run)?,
            children,
        }))
    }

    pub(crate) fn admit_children<'a>(
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        expected_parent_revision: u64,
        app: &AppRecord,
        workspace: &WorkspaceRecord,
        parent: &RunRecord,
        children: impl ExactSizeIterator<Item = (&'a RunRecord, Option<RunCopy>)>,
    ) -> Result<Self, AppError> {
        if children.len() == 0 {
            return Err(AppError::Integration(
                "child admission mutation requires at least one child".to_owned(),
            ));
        }
        if workspace.id != parent.workspace_id || workspace.revision != parent.workspace_revision {
            return Err(AppError::Integration(
                "child admission workspace identity is inconsistent".to_owned(),
            ));
        }
        let mut identities = BTreeSet::new();
        let mut stored_children = Vec::with_capacity(children.len());
        for (child, source) in children {
            let relation_matches = child.parent.as_ref().is_some_and(|relation| {
                relation.workflow_id == parent.workflow_id
                    && relation.run_id == parent.id
                    && relation.occurrence == parent.occurrence
                    && parent
                        .parent
                        .as_ref()
                        .map_or(0, |parent| parent.depth)
                        .checked_add(1)
                        == Some(relation.depth)
                    && relation.depth <= crate::workflow::run::MAX_CHILD_DEPTH
            });
            if child.workspace_id != parent.workspace_id
                || child.workspace_revision != parent.workspace_revision
                || !relation_matches
            {
                return Err(AppError::Integration(
                    "child admission run identity is inconsistent".to_owned(),
                ));
            }
            let stored = RecordCodec::run(child)?;
            if !identities.insert(stored.key().clone()) {
                return Err(AppError::Integration(
                    "child admission contains duplicate run identities".to_owned(),
                ));
            }
            stored_children.push((stored, AppStorageRecord::head(child)?, source));
        }
        Ok(Self::new(AppMutationKind::AdmitChildren {
            expected_app_revision,
            expected_workspace_revision,
            expected_parent_revision,
            app: AppStorageRecord::app(app)?,
            workspace: AppStorageRecord::workspace(workspace)?,
            parent: RecordCodec::run(parent)?,
            predecessor: AppStorageRecord::predecessor(parent)?,
            children: stored_children,
        }))
    }

    pub(crate) fn retain_run(
        expected_app_revision: u64,
        expected_run_revision: u64,
        app: &AppRecord,
        run: &RunRecord,
    ) -> Result<Self, AppError> {
        Ok(Self::new(AppMutationKind::RetainRun {
            expected_app_revision,
            expected_run_revision,
            app: AppStorageRecord::app(app)?,
            run: RecordCodec::run(run)?,
        }))
    }

    pub(crate) fn readmit_run(
        expected_app_revision: u64,
        expected_workspace_revision: u64,
        expected_run_revision: u64,
        app: &AppRecord,
        workspace: &WorkspaceRecord,
        run: &RunRecord,
    ) -> Result<Self, AppError> {
        if workspace.id != run.workspace_id || workspace.revision != run.workspace_revision {
            return Err(AppError::Conflict(
                "run readmission workspace identity is stale".into(),
            ));
        }
        Ok(Self::new(AppMutationKind::ReadmitRun {
            expected_app_revision,
            expected_workspace_revision,
            expected_run_revision,
            app: AppStorageRecord::app(app)?,
            workspace: AppStorageRecord::workspace(workspace)?,
            run: RecordCodec::run(run)?,
            predecessor: AppStorageRecord::predecessor(run)?,
        }))
    }

    pub(crate) fn seal_run(
        expected_app_revision: u64,
        expected_run_revision: u64,
        run: &RunRecord,
        parent: Option<(&RunRecord, u64)>,
    ) -> Result<Self, AppError> {
        Ok(Self::new(AppMutationKind::SealRun {
            expected_app_revision,
            expected_run_revision,
            run: RecordCodec::run(run)?,
            parent: parent
                .map(|(record, revision)| RecordCodec::run(record).map(|record| (record, revision)))
                .transpose()?,
        }))
    }

    fn new(kind: AppMutationKind) -> Self {
        Self {
            id: Uuid::new_v4().simple().to_string(),
            kind,
        }
    }
}

impl RecordKey {
    fn ledger(record: &RunRecord, index: usize, sequence: u64) -> Self {
        Self::Ledger {
            workspace: record.workspace_id.clone(),
            workflow: record.workflow_id.clone(),
            run: record.id.clone(),
            occurrence: record.occurrence.clone(),
            index,
            sequence,
        }
    }

    fn exact_run(key: &RunKey) -> Self {
        Self::Run {
            workspace: key.workspace().clone(),
            workflow: key.workflow().clone(),
            run: key.run().clone(),
            occurrence: key.occurrence().to_owned(),
        }
    }

    async fn expect_present<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
    ) -> Result<(), StorageFailure> {
        let key = self.state_key().map_err(StorageFailure::permanent)?;
        let snapshot = transaction
            .load(key.clone())
            .await
            .map_err(StorageFailure::from)?
            .ok_or_else(|| {
                StorageFailure::permanent(AppError::Integration("run index is missing".into()))
            })?;
        RecordCodec::preflight_epoch(&snapshot.record.value).map_err(StorageFailure::permanent)?;
        Self::validate_value(key.key(), &snapshot.record.value).map_err(StorageFailure::permanent)
    }

    fn run(record: &RunRecord) -> Self {
        Self::Run {
            workspace: record.workspace_id.clone(),
            workflow: record.workflow_id.clone(),
            run: record.id.clone(),
            occurrence: record.occurrence.clone(),
        }
    }

    fn checkpoint(record: &RunRecord) -> Self {
        Self::Checkpoint {
            workspace: record.workspace_id.clone(),
            workflow: record.workflow_id.clone(),
            run: record.id.clone(),
            occurrence: record.occurrence.clone(),
            sequence: record.checkpoint.sequence,
        }
    }

    fn head_key(&self) -> Result<Self, AppError> {
        match self {
            Self::Run {
                workspace,
                workflow,
                run,
                occurrence,
            }
            | Self::Checkpoint {
                workspace,
                workflow,
                run,
                occurrence,
                ..
            }
            | Self::Ledger {
                workspace,
                workflow,
                run,
                occurrence,
                ..
            }
            | Self::ActionObservation {
                workspace,
                workflow,
                run,
                occurrence,
                ..
            }
            | Self::ActiveRun {
                workspace,
                workflow,
                run,
                occurrence,
            } => Ok(Self::Run {
                workspace: workspace.clone(),
                workflow: workflow.clone(),
                run: run.clone(),
                occurrence: occurrence.clone(),
            }),
            Self::ProjectionLink { key, .. } => Ok(Self::exact_run(key)),
            Self::App
            | Self::Definition { .. }
            | Self::Workspace(_)
            | Self::Receipt(_)
            | Self::ProjectionTail { .. } => {
                Err(AppError::Integration("record has no run scope".into()))
            }
        }
    }

    fn history_key(key: &str) -> Result<Self, AppError> {
        let parts = key.split('/').collect::<Vec<_>>();
        let parsed = (|| match parts.as_slice() {
            [
                "history",
                workspace,
                workflow,
                run,
                occurrence,
                "checkpoint",
                sequence,
            ] => Some(Self::Checkpoint {
                workspace: workspace.parse().ok()?,
                workflow: workflow.parse().ok()?,
                run: run.parse().ok()?,
                occurrence: (*occurrence).to_owned(),
                sequence: Self::sequence(sequence)?,
            }),
            [
                "history",
                workspace,
                workflow,
                run,
                occurrence,
                "ledger",
                index,
                sequence,
            ] if index.len() == 2 && index.bytes().all(|byte| byte.is_ascii_digit()) => {
                let index = index.parse::<usize>().ok()?;
                if index >= MAX_INTERACTION_SETTLEMENTS.div_ceil(64) {
                    return None;
                }
                Some(Self::Ledger {
                    workspace: workspace.parse().ok()?,
                    workflow: workflow.parse().ok()?,
                    run: run.parse().ok()?,
                    occurrence: (*occurrence).to_owned(),
                    index,
                    sequence: Self::sequence(sequence)?,
                })
            }
            [
                "history",
                workspace,
                workflow,
                run,
                occurrence,
                "action",
                ordinal,
                reverse,
            ] => Some(Self::ActionObservation {
                workspace: workspace.parse().ok()?,
                workflow: workflow.parse().ok()?,
                run: run.parse().ok()?,
                occurrence: (*occurrence).to_owned(),
                ordinal: Self::sequence(ordinal)?,
                sequence: u64::MAX - Self::sequence(reverse)?,
            }),
            ["active", workspace, workflow, run, occurrence] => Some(Self::ActiveRun {
                workspace: workspace.parse().ok()?,
                workflow: workflow.parse().ok()?,
                run: run.parse().ok()?,
                occurrence: (*occurrence).to_owned(),
            }),
            [
                "history",
                workspace,
                workflow,
                run,
                occurrence,
                "projection",
                rest @ ..,
            ] if rest.len() >= 2 => {
                let (sequence, projection) = rest.split_last()?;
                let projection = RunProjection::parse(projection)?;
                if matches!(projection, RunProjection::Action(_)) {
                    return None;
                }
                RunId::validate_occurrence(occurrence).ok()?;
                Some(Self::ProjectionLink {
                    key: RunKey::new(
                        workspace.parse().ok()?,
                        workflow.parse().ok()?,
                        run.parse().ok()?,
                        (*occurrence).into(),
                    ),
                    projection,
                    sequence: Self::sequence(sequence)?,
                })
            }
            _ => None,
        })();
        let parsed =
            parsed.ok_or_else(|| AppError::Integration("durable history key is invalid".into()))?;
        match &parsed {
            Self::Checkpoint { occurrence, .. }
            | Self::Ledger { occurrence, .. }
            | Self::ActionObservation { occurrence, .. }
            | Self::ActiveRun { occurrence, .. } => RunId::validate_occurrence(occurrence)
                .map_err(|_| {
                    AppError::Integration("durable history occurrence is invalid".into())
                })?,
            Self::ProjectionLink { .. } => {}
            _ => {
                return Err(AppError::Integration(
                    "durable history key is invalid".into(),
                ));
            }
        }
        Ok(parsed)
    }

    fn sequence(value: &str) -> Option<u64> {
        (value.len() == 20 && value.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| value.parse().ok())
            .flatten()
    }

    fn index_record(&self) -> Result<StateRecord, AppError> {
        if !matches!(self, Self::ActiveRun { .. }) {
            return Err(AppError::Integration(
                "record is not a history index".into(),
            ));
        }
        self.record(&serde_json::json!({ "epoch": EPOCH }))
    }

    async fn expect_missing<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
    ) -> Result<(), StorageFailure> {
        let key = self.state_key().map_err(StorageFailure::permanent)?;
        if transaction
            .load(key)
            .await
            .map_err(StorageFailure::from)?
            .is_some()
        {
            Err(StorageFailure::permanent(AppError::Conflict(
                "durable record already exists".to_owned(),
            )))
        } else {
            Ok(())
        }
    }

    async fn expect_revision<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        expected: u64,
    ) -> Result<(), StorageFailure> {
        let key = self.state_key().map_err(StorageFailure::permanent)?;
        let Some(snapshot) = transaction.load(key).await.map_err(StorageFailure::from)? else {
            return Err(StorageFailure::permanent(AppError::NotFound(
                "durable record does not exist".to_owned(),
            )));
        };
        if matches!(self, Self::App) {
            RecordCodec::preflight_epoch(&snapshot.record.value)
                .map_err(StorageFailure::permanent)?;
            Self::validate_value("app", &snapshot.record.value)
                .map_err(StorageFailure::permanent)?;
        }
        if snapshot.revision.get() != expected {
            return Err(StorageFailure::permanent(AppError::Conflict(
                "durable record revision fence is stale".to_owned(),
            )));
        }
        Ok(())
    }

    async fn delete<T: StateTransactionBackend>(
        self,
        transaction: &mut T,
    ) -> Result<(), StorageFailure> {
        let key = self.state_key().map_err(StorageFailure::permanent)?;
        transaction.delete(key).await.map_err(StorageFailure::from)
    }

    fn record<T>(&self, record: &T) -> Result<StateRecord, AppError>
    where
        T: Serialize,
    {
        let key = self.state_key()?;
        let value = RecordCodec::encode(record, 0)?;
        Self::validate_value(key.key(), &value)?;
        Ok(StateRecord::new(key, value))
    }

    fn validate_value(key: &str, value: &Value) -> Result<(), AppError> {
        if key.starts_with("definition/") {
            let record: DefinitionRecord = RecordCodec::decode(value)?;
            record.validate()?;
            return if record.key().state_key()?.key() == key {
                Ok(())
            } else {
                Err(AppError::Integration(
                    "durable workflow definition key is inconsistent".into(),
                ))
            };
        }
        if key.starts_with("projection/") {
            let parts = key.split('/').collect::<Vec<_>>();
            let ["projection", workspace, workflow, run, projection @ ..] = parts.as_slice() else {
                return Err(AppError::Integration(
                    "saved projection key is invalid".into(),
                ));
            };
            let parsed = Self::ProjectionTail {
                workspace: workspace.parse()?,
                workflow: workflow.parse()?,
                run: run.parse()?,
                projection: RunProjection::parse(projection)
                    .ok_or_else(|| AppError::Integration("saved projection is invalid".into()))?,
            };
            let index: RunIndex = RecordCodec::decode(value)?;
            index.validate()?;
            let valid = match (&parsed, &index) {
                (Self::ProjectionTail { projection, .. }, RunIndex::Tail { last, .. }) => {
                    match projection {
                        RunProjection::Sequence(sequence) => last.sequence == *sequence,
                        RunProjection::Unsettled => last.sequence == 0,
                        RunProjection::Checkpoint
                        | RunProjection::Saved
                        | RunProjection::Action(_) => true,
                    }
                }
                _ => false,
            };
            if parsed.state_key()?.key() != key || !valid {
                return Err(AppError::Integration(
                    "saved projection tail is invalid".into(),
                ));
            }
            return Ok(());
        }
        if key.starts_with("history/") || key.starts_with("active/") {
            let matches = match Self::history_key(key)? {
                Self::Checkpoint {
                    workspace,
                    workflow,
                    run,
                    occurrence,
                    sequence,
                } => {
                    value.get("workspace_id").and_then(Value::as_str) == Some(workspace.as_str())
                        && value.get("previous").is_none()
                        && value.get("workflow_id").and_then(Value::as_str)
                            == Some(workflow.as_str())
                        && value.get("id").and_then(Value::as_str) == Some(run.as_str())
                        && value.get("occurrence").and_then(Value::as_str)
                            == Some(occurrence.as_str())
                        && value
                            .get("checkpoint")
                            .and_then(|checkpoint| checkpoint.get("sequence"))
                            .and_then(Value::as_u64)
                            == Some(sequence)
                }
                ledger @ Self::Ledger { .. } => SettlementPage::matches(&ledger, value),
                Self::ActionObservation {
                    occurrence,
                    sequence,
                    ..
                } => {
                    let index: RunIndex = RecordCodec::decode(value)?;
                    index.validate()?;
                    matches!(index, RunIndex::Link { previous, next, .. }
                        if previous.iter().chain(&next).all(|neighbor|
                            neighbor.occurrence != occurrence || neighbor.sequence != sequence))
                }
                Self::ProjectionLink {
                    key,
                    projection,
                    sequence,
                } => {
                    let index: RunIndex = RecordCodec::decode(value)?;
                    index.validate()?;
                    let scope = match projection {
                        RunProjection::Sequence(expected) => sequence == expected,
                        RunProjection::Unsettled => sequence == 0,
                        RunProjection::Checkpoint | RunProjection::Saved => true,
                        RunProjection::Action(_) => false,
                    };
                    scope
                        && matches!(index, RunIndex::Link { previous, next, .. }
                        if previous.iter().chain(&next).all(|neighbor|
                            (neighbor.occurrence != key.occurrence() || neighbor.sequence != sequence)
                            && match projection {
                                RunProjection::Sequence(expected) => neighbor.sequence == expected,
                                RunProjection::Unsettled => neighbor.sequence == 0,
                                _ => true,
                            }))
                }
                Self::ActiveRun { .. } => {
                    value.as_object().is_some_and(|object| object.len() == 1)
                        && value.get("epoch").and_then(Value::as_str) == Some(EPOCH)
                }
                _ => false,
            };
            return if matches {
                Ok(())
            } else {
                Err(AppError::Integration(
                    "durable history value does not match its key".into(),
                ))
            };
        }
        let mut parts = key.split('/');
        let matches = match (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) {
            (Some("app"), None, None, None, None, None) => {
                let migration = value.get("migration").ok_or_else(|| {
                    AppError::Integration("app migration state is missing".into())
                })?;
                matches!(
                    RecordCodec::decode::<AppMigration>(migration)?,
                    AppMigration::Ready
                )
            }
            (Some("workspace"), Some(id), None, None, None, None) => {
                value.get("id").and_then(Value::as_str) == Some(id)
            }
            (Some("run"), Some(workspace), Some(workflow), Some(run), Some(occurrence), None) => {
                value.get("workspace_id").and_then(Value::as_str) == Some(workspace)
                    && value.get("workflow_id").and_then(Value::as_str) == Some(workflow)
                    && value.get("id").and_then(Value::as_str) == Some(run)
                    && value.get("creation").and_then(Value::as_str) == Some(occurrence)
            }
            (Some("receipt"), Some(id), None, None, None, None) => {
                value.get("id").and_then(Value::as_str) == Some(id)
            }
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(AppError::Integration(
                "durable record identity does not match its storage key".to_owned(),
            ))
        }
    }

    fn state_key(&self) -> Result<StateKey, AppError> {
        let key = match self {
            Self::App => "app".to_owned(),
            Self::Definition { workflow, revision } => {
                let mut encoded = String::with_capacity(revision.len() * 2);
                for byte in revision.bytes() {
                    encoded.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
                    encoded.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
                }
                format!("definition/{workflow}/{encoded}")
            }
            Self::Workspace(id) => format!("workspace/{id}"),
            Self::Run {
                workspace,
                workflow,
                run,
                occurrence,
            } => format!("run/{workspace}/{workflow}/{run}/{occurrence}"),
            Self::Checkpoint {
                workspace,
                workflow,
                run,
                occurrence,
                sequence,
            } => {
                format!(
                    "history/{workspace}/{workflow}/{run}/{occurrence}/checkpoint/{sequence:020}"
                )
            }
            Self::Ledger {
                workspace,
                workflow,
                run,
                occurrence,
                index,
                sequence,
            } => {
                format!(
                    "history/{workspace}/{workflow}/{run}/{occurrence}/ledger/{index:02}/{sequence:020}"
                )
            }
            Self::ActionObservation {
                workspace,
                workflow,
                run,
                occurrence,
                ordinal,
                sequence,
            } => {
                format!(
                    "history/{workspace}/{workflow}/{run}/{occurrence}/action/{ordinal:020}/{:020}",
                    u64::MAX - sequence
                )
            }
            Self::ActiveRun {
                workspace,
                workflow,
                run,
                occurrence,
            } => {
                format!("active/{workspace}/{workflow}/{run}/{occurrence}")
            }
            Self::ProjectionTail {
                workspace,
                workflow,
                run,
                projection,
            } => {
                format!(
                    "projection/{workspace}/{workflow}/{run}/{}",
                    projection.text()
                )
            }
            Self::ProjectionLink {
                key,
                projection,
                sequence,
            } => {
                format!(
                    "history/{}/{}/{}/{}/projection/{}/{sequence:020}",
                    key.workspace(),
                    key.workflow(),
                    key.run(),
                    key.occurrence(),
                    projection.text()
                )
            }
            Self::Receipt(id) => format!("receipt/{id}"),
        };
        StateKey::new(NAMESPACE, key).map_err(AppError::from)
    }
}

impl std::io::Write for RecordCodec {
    fn write(&mut self, bytes: &[u8]) -> Result<usize, std::io::Error> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("app durable record encoded length overflowed"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl RecordCodec {
    fn run(record: &RunRecord) -> Result<AppStorageRecord, AppError> {
        let reserved = record.checkpoint.prospective_settlement_bytes()?;
        let key = RecordKey::checkpoint(record);
        let state_key = key.state_key()?;
        let size = RecordCodec::logical_run_len(record)?;
        if size.saturating_add(reserved) > MAX_RECORD_BYTES {
            return Err(AppError::InvalidInput(
                "app durable record exceeds the private byte bound".into(),
            ));
        }
        let count = record.checkpoint.settlement_count();
        if count > MAX_INTERACTION_SETTLEMENTS {
            return Err(AppError::InvalidInput(
                "durable interaction settlement capacity is insufficient".into(),
            ));
        }
        let mut normalized = record.clone();
        if normalized.checkpoint.settlement_references().len() != count.div_ceil(64) {
            normalized
                .checkpoint
                .prepare_settlement_pages(record.previous.as_ref(), record.checkpoint.sequence)?;
        }
        // Prepared records previously came from their persisted body, which
        // never includes this separate observed predecessor.
        normalized.previous = None;
        let references = normalized.checkpoint.settlement_references().to_vec();
        let mut pages = Vec::new();
        for (index, sequence) in references.iter().copied().enumerate() {
            if sequence == record.checkpoint.sequence {
                let entries = record.checkpoint.settlement_page(index)?;
                let page = SettlementPage {
                    epoch: EPOCH.into(),
                    workspace_id: record.workspace_id.clone(),
                    workflow_id: record.workflow_id.clone(),
                    id: record.id.clone(),
                    occurrence: record.occurrence.clone(),
                    index,
                    sequence,
                    entries,
                };
                let key = RecordKey::ledger(record, index, sequence);
                let value = serde_json::to_value(&page).map_err(|_| {
                    AppError::Integration("app durable record encoding failed".into())
                })?;
                let state_key = key.state_key()?;
                RecordKey::validate_value(state_key.key(), &value)?;
                pages.push(StateRecord::new(state_key, value));
            }
        }
        let descriptor = SettlementPages {
            count,
            pages: references,
        };
        descriptor.validate(record.checkpoint.sequence)?;
        let mut value = serde_json::to_value(record)
            .map_err(|_| AppError::Integration("app durable record encoding failed".into()))?;
        value
            .get_mut("checkpoint")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| AppError::Integration("checkpoint body encoding is invalid".into()))?
            .insert(
                "settlements".into(),
                serde_json::to_value(&descriptor).map_err(|_| {
                    AppError::Integration("app durable record encoding failed".into())
                })?,
            );
        RecordKey::validate_value(state_key.key(), &value)?;
        Ok(AppStorageRecord::Checkpoint {
            key,
            value,
            record: Box::new(normalized),
            pages,
        })
    }

    pub(crate) fn encoded_len<T: Serialize>(record: &T) -> Result<usize, AppError> {
        let mut encoded = Self { bytes: 0 };
        serde_json::to_writer(&mut encoded, record)
            .map_err(|_| AppError::Integration("app durable record encoding failed".into()))?;
        Ok(encoded.bytes)
    }

    fn value_len(value: &Value) -> Result<usize, AppError> {
        let checked = |size: Option<usize>| {
            size.ok_or_else(|| {
                AppError::Integration("app durable record encoding failed".to_owned())
            })
        };
        Ok(match value {
            Value::Null => 4,
            Value::Bool(true) => 4,
            Value::Bool(false) => 5,
            Value::Number(number) => {
                if let Some(value) = number.as_u64() {
                    value.checked_ilog10().unwrap_or(0) as usize + 1
                } else if let Some(value) = number.as_i64() {
                    value.unsigned_abs().checked_ilog10().unwrap_or(0) as usize + 2
                } else {
                    Self::encoded_len(number)?
                }
            }
            Value::String(text) => checked(Self::string_len(text))?,
            Value::Array(values) => {
                let mut size = 2usize;
                for (index, value) in values.iter().enumerate() {
                    size = checked(size.checked_add(usize::from(index != 0)))?;
                    size = checked(size.checked_add(Self::value_len(value)?))?;
                }
                size
            }
            Value::Object(values) => {
                let mut size = 2usize;
                for (index, (key, value)) in values.iter().enumerate() {
                    size = checked(size.checked_add(if index == 0 { 1 } else { 2 }))?;
                    size = checked(size.checked_add(checked(Self::string_len(key))?))?;
                    size = checked(size.checked_add(Self::value_len(value)?))?;
                }
                size
            }
        })
    }

    fn string_len(text: &str) -> Option<usize> {
        let mut size = text.len().checked_add(2)?;
        for byte in text.bytes() {
            let extra = match byte {
                b'"' | b'\\' | b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 1,
                0..=0x1f => 5,
                _ => continue,
            };
            size = size.checked_add(extra)?;
        }
        Some(size)
    }

    fn parse_run(value: &Value) -> Result<RunRecord, AppError> {
        RunRecord::deserialize(value)
            .map_err(|_| AppError::Integration("app durable record is corrupt".into()))
    }

    fn parse_legacy_run(value: &Value) -> Result<RunRecord, AppError> {
        if value.get("epoch").and_then(Value::as_str) != Some(LEGACY_EPOCH) {
            return Err(AppError::Integration(
                "unsupported app record schema version".into(),
            ));
        }
        let size = Self::value_len(value)?;
        if size > MAX_RECORD_BYTES {
            return Err(AppError::Integration(
                "app durable record exceeds the private byte bound".into(),
            ));
        }
        let mut record = Self::parse_run(value)?;
        let entries = value
            .get("checkpoint")
            .and_then(|checkpoint| checkpoint.get("settlements"))
            .ok_or_else(|| AppError::Integration("app durable record is corrupt".into()))?;
        record.checkpoint.hydrate_legacy_settlements(entries)?;
        Self::validate_run(&record, size)?;
        record.checkpoint.validate_settlements()?;
        Ok(record)
    }

    pub(crate) fn logical_run_len(record: &RunRecord) -> Result<usize, AppError> {
        // The canonical body omits only this final checkpoint field. Counting
        // its existing exact array encoding restores the complete inline size
        // without materializing that potentially large JSON Value.
        let body = Self::encoded_len(record)?;
        let entries = record.checkpoint.settlement_bytes()?;
        body.checked_add(
            Self::string_len("settlements").ok_or_else(|| {
                AppError::Integration("app durable record encoding failed".into())
            })?,
        )
        .and_then(|size| size.checked_add(2))
        .and_then(|size| size.checked_add(entries))
        .ok_or_else(|| AppError::Integration("app durable record encoding failed".into()))
    }

    pub(crate) fn same_run(left: &RunRecord, right: &RunRecord) -> Result<bool, AppError> {
        let encode = |record: &RunRecord| {
            serde_json::to_vec(record).map_err(|_| {
                AppError::Integration("workflow checkpoint evidence encoding failed".into())
            })
        };
        // Both ledgers are encoded even if the bodies differ, preserving
        // validation of exact ordered maps and numeric representations.
        let left_body = encode(left)?;
        let left_entries = left.checkpoint.settlement_evidence()?;
        let right_body = encode(right)?;
        let right_entries = right.checkpoint.settlement_evidence()?;
        Ok(left_body == right_body && left_entries == right_entries)
    }

    fn validate_run(record: &RunRecord, size: usize) -> Result<(), AppError> {
        RunId::validate_occurrence(&record.occurrence)
            .map_err(|_| AppError::Integration("durable run occurrence is invalid".into()))?;
        let reserve = record
            .checkpoint
            .prospective_settlement_bytes()
            .map_err(|_| {
                AppError::Integration(
                    "durable interaction settlement capacity is insufficient".into(),
                )
            })?;
        if size.saturating_add(reserve) > MAX_RECORD_BYTES {
            return Err(AppError::Integration(
                "durable interaction settlement capacity is insufficient".into(),
            ));
        }
        Ok(())
    }

    fn preflight_epoch(value: &Value) -> Result<(), AppError> {
        if value.get("epoch").and_then(Value::as_str) == Some(EPOCH) {
            Ok(())
        } else {
            Err(AppError::Integration(
                "unsupported app record schema version".to_owned(),
            ))
        }
    }

    fn encode<T>(record: &T, reserved_bytes: usize) -> Result<Value, AppError>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(record)
            .map_err(|_| AppError::Integration("app durable record encoding failed".to_owned()))?;
        let size = Self::value_len(&value)?;
        if size.saturating_add(reserved_bytes) > MAX_RECORD_BYTES {
            return Err(AppError::InvalidInput(
                "app durable record exceeds the private byte bound".to_owned(),
            ));
        }
        Ok(value)
    }

    fn decode<T>(value: &Value) -> Result<T, AppError>
    where
        T: DeserializeOwned,
    {
        let size = Self::value_len(value)
            .map_err(|_| AppError::Integration("app durable record decoding failed".to_owned()))?;
        if size > MAX_RECORD_BYTES {
            return Err(AppError::Integration(
                "app durable record exceeds the private byte bound".to_owned(),
            ));
        }
        T::deserialize(value)
            .map_err(|_| AppError::Integration("app durable record is corrupt".to_owned()))
    }
}

impl From<AppError> for StorageFailure {
    fn from(error: AppError) -> Self {
        Self::from_app(error)
    }
}

impl From<StateStoreError> for StorageFailure {
    fn from(error: StateStoreError) -> Self {
        let recoverable = matches!(
            error,
            StateStoreError::TimedOut { .. }
                | StateStoreError::Unavailable { .. }
                | StateStoreError::BackendOperation { .. }
        );
        let error = AppError::from(error);
        if recoverable {
            Self::Recoverable(error)
        } else {
            Self::Permanent(error)
        }
    }
}

impl From<StateStoreError> for AppError {
    fn from(error: StateStoreError) -> Self {
        match error {
            StateStoreError::InvalidConfig(_) | StateStoreError::InvalidRequest(_) => {
                Self::InvalidInput("app state-store input is invalid".to_owned())
            }
            StateStoreError::TransactionConflict { .. } => {
                Self::Conflict("app state transaction conflicted".to_owned())
            }
            StateStoreError::TimedOut { .. } => {
                Self::Timeout("app state-store operation timed out".to_owned())
            }
            StateStoreError::Unavailable { .. }
            | StateStoreError::BackendOperation { .. }
            | StateStoreError::Serialization(_) => {
                Self::Integration("app state-store operation failed".to_owned())
            }
        }
    }
}
