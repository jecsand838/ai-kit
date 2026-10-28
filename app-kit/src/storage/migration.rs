use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage_kit::state::record::{StateKey, StateRecord, StateRecordSnapshot};
use storage_kit::{StateStoreBackend, StateStoreError, StateTransactionBackend};
use uuid::Uuid;

use crate::AppError;
use crate::app::{AppRecord, AppTrace};
use crate::workflow::definition::CompiledDefinition;
use crate::workflow::run::{RunKey, RunRecord};
use crate::workspace::WorkspaceRecord;

use super::occurrence::{RunIndex, RunObservation, RunProjection};
use super::{
    AppStorageRecord, AppStore, DefinitionRecord, EPOCH, LEGACY_EPOCH, NAMESPACE, PAGE_SIZE,
    RecordCodec, RecordKey, RunHead, RunRetention, StorageFailure,
};

/// Only this root authorizes the interrupted, one-way format conversion.
#[derive(Clone, Default, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum AppMigration {
    #[default]
    Ready,
    Migrating {
        id: String,
        phase: MigrationPhase,
    },
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MigrationPhase {
    Deletions { after: Option<String> },
    Checkpoints { after: Option<String> },
    Records { after: Option<String> },
    Audit,
}

impl AppMigration {
    fn validate(&self) -> Result<(), AppError> {
        if let Self::Migrating { id, phase } = self {
            if id.len() != 32
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(AppError::Integration(
                    "invalid App migration identity".into(),
                ));
            }
            let after = match phase {
                MigrationPhase::Deletions { after }
                | MigrationPhase::Checkpoints { after }
                | MigrationPhase::Records { after } => after,
                MigrationPhase::Audit => return Ok(()),
            };
            if let Some(after) = after {
                StateKey::new(NAMESPACE, after.clone())
                    .map_err(|_| AppError::Integration("invalid App migration progress".into()))?;
                if matches!(phase, MigrationPhase::Deletions { .. }) && !after.starts_with("run/")
                    || matches!(phase, MigrationPhase::Checkpoints { .. })
                        && !after.starts_with("history/")
                {
                    return Err(AppError::Integration(
                        "foreign App migration progress".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl<CS: StateStoreBackend> AppStore<CS> {
    pub(crate) async fn migrate(
        &self,
        definitions: &BTreeMap<crate::workflow::WorkflowId, Vec<Arc<CompiledDefinition>>>,
        configured_agents: &BTreeSet<(&str, &str)>,
    ) -> Result<(), AppError> {
        let Some((mut root, mut snapshot)) = self.migration_root().await? else {
            return Ok(());
        };
        if root.epoch == LEGACY_EPOCH {
            self.preflight_migration(&root, &snapshot, definitions, configured_agents)
                .await?;
            root.epoch = EPOCH.into();
            root.migration = AppMigration::Migrating {
                id: Uuid::new_v4().simple().to_string(),
                phase: MigrationPhase::Deletions { after: None },
            };
            self.publish_migration_root(&snapshot, &root).await?;
            (root, snapshot) = self
                .migration_root()
                .await?
                .ok_or_else(|| AppError::Conflict("App migration root disappeared".into()))?;
        }
        loop {
            let AppMigration::Migrating { id, phase } = root.migration.clone() else {
                return Ok(());
            };
            match phase {
                MigrationPhase::Deletions { after } => {
                    self.migrate_deletion(&root, &snapshot, after.as_deref())
                        .await?;
                }
                MigrationPhase::Checkpoints { after } => {
                    self.migrate_checkpoint(&root, &snapshot, after.as_deref())
                        .await?;
                }
                MigrationPhase::Records { after } => {
                    self.migrate_records(&root, &snapshot, after.as_deref())
                        .await?;
                }
                MigrationPhase::Audit => {
                    self.audit_migration(&root, configured_agents).await?;
                    let mut ready = root.clone();
                    ready.migration = AppMigration::Ready;
                    self.publish_migration_root(&snapshot, &ready).await?;
                    return Ok(());
                }
            }
            let current = self
                .migration_root()
                .await?
                .ok_or_else(|| AppError::Conflict("App migration root disappeared".into()))?;
            if !matches!(&current.0.migration, AppMigration::Migrating { id: current, .. } if current == &id)
            {
                return Err(AppError::Conflict("App migration ownership changed".into()));
            }
            (root, snapshot) = current;
        }
    }

    async fn migration_root(&self) -> Result<Option<(AppRecord, StateRecordSnapshot)>, AppError> {
        let key = RecordKey::App.state_key()?;
        let Some(snapshot) = self.inner.load(key.clone()).await? else {
            return Ok(None);
        };
        if snapshot.record.key != key {
            return Err(AppError::Integration(
                "App migration root has foreign identity".into(),
            ));
        }
        let root: AppRecord = RecordCodec::decode(&snapshot.record.value)?;
        match root.epoch.as_str() {
            LEGACY_EPOCH
                if snapshot.record.value.get("migration").is_none()
                    && matches!(root.migration, AppMigration::Ready) => {}
            EPOCH if snapshot.record.value.get("migration").is_some() => {
                root.migration.validate()?
            }
            _ => {
                return Err(AppError::Integration(
                    "unsupported app record schema version".into(),
                ));
            }
        }
        Ok(Some((root, snapshot)))
    }

    /// Legacy parsing is limited to migration. The ordinary decoder never
    /// accepts an older epoch or falls back after a failed normalized decode.
    fn migration_value(snapshot: &StateRecordSnapshot) -> Result<Cow<'_, Value>, AppError> {
        let value = snapshot.record.value.as_ref();
        if !matches!(
            value.get("epoch").and_then(Value::as_str),
            Some(LEGACY_EPOCH | EPOCH)
        ) {
            return Err(AppError::Integration(
                "unsupported app record schema version".into(),
            ));
        }
        let key = snapshot.record.key.key();
        let normalized_payload = value.get("epoch").and_then(Value::as_str) == Some(EPOCH)
            && key.starts_with("history/")
            && matches!(
                RecordKey::history_key(key)?,
                RecordKey::Checkpoint { .. } | RecordKey::Ledger { .. }
            );
        if !normalized_payload && RecordCodec::value_len(value)? > super::MAX_RECORD_BYTES {
            return Err(AppError::Integration(
                "app durable record exceeds the private byte bound".into(),
            ));
        }
        // Checkpoint identity validation does not depend on its epoch. The
        // canonical legacy parser owns the original full payload and ledger.
        if key != "app"
            && (value.get("epoch").and_then(Value::as_str) == Some(EPOCH)
                || key.starts_with("history/")
                    && matches!(RecordKey::history_key(key)?, RecordKey::Checkpoint { .. }))
        {
            RecordKey::validate_value(key, value)?;
            return Ok(Cow::Borrowed(value));
        }
        let mut value = value.clone();
        value["epoch"] = Value::String(EPOCH.into());
        if key == "app" {
            value["migration"] = serde_json::to_value(AppMigration::Ready)
                .map_err(|_| AppError::Integration("App migration encoding failed".into()))?;
        }
        RecordKey::validate_value(key, &value)?;
        Ok(Cow::Owned(value))
    }

    /// A read-only pass covers every surviving row before the first write.
    /// Counters, rather than retained payloads, bound the root revision reserve.
    async fn preflight_migration(
        &self,
        root: &AppRecord,
        root_snapshot: &StateRecordSnapshot,
        definitions: &BTreeMap<crate::workflow::WorkflowId, Vec<Arc<CompiledDefinition>>>,
        configured_agents: &BTreeSet<(&str, &str)>,
    ) -> Result<(), AppError> {
        root.generation
            .checked_add(1)
            .ok_or_else(|| AppError::Conflict("application generation is exhausted".into()))?;
        let mut after = None;
        let mut checkpoints = 0u64;
        let mut other = 0u64;
        let mut deletion_jobs = 0u64;
        let mut removed_other = 0u64;
        let mut all_history = 0u64;
        let mut listed_history = 0u64;
        let mut active_runs = 0usize;
        let mut longest_key = String::new();
        for (id, revision) in &root.definitions {
            let key = RecordKey::Definition {
                workflow: id.clone(),
                revision: revision.clone(),
            };
            if self.inner.load(key.state_key()?).await?.is_none() {
                return Err(AppError::Integration(
                    "current workflow revision has no durable definition".into(),
                ));
            }
        }
        for (id, selected) in definitions {
            for definition in selected {
                let key = RecordKey::Definition {
                    workflow: id.clone(),
                    revision: definition.revision().into(),
                };
                if let Some(snapshot) = self.inner.load(key.state_key()?).await? {
                    let catalog: DefinitionRecord =
                        RecordCodec::decode(Self::migration_value(&snapshot)?.as_ref())?;
                    if catalog.canonical != definition.fingerprint() {
                        return Err(AppError::Conflict(
                            "configured workflow revision differs from its durable definition"
                                .into(),
                        ));
                    }
                }
            }
        }
        loop {
            let (page, next) = self
                .list_scoped_page(None, after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in page {
                if snapshot.record.value.get("epoch").and_then(Value::as_str) != Some(LEGACY_EPOCH)
                {
                    return Err(AppError::Integration(
                        "mixed epochs outside App migration".into(),
                    ));
                }
                let required = if snapshot.record.key.key().starts_with("receipt/") {
                    2
                } else {
                    1
                };
                snapshot
                    .revision
                    .get()
                    .checked_add(required)
                    .ok_or_else(|| {
                        AppError::Conflict("App migration record revision is exhausted".into())
                    })?;
                let value = Self::migration_value(&snapshot)?;
                let key = snapshot.record.key.key();
                if key.len() > longest_key.len() {
                    longest_key = key.to_owned();
                }
                if key.starts_with("active/") {
                    let parsed = RecordKey::history_key(key)?;
                    let owner = self
                        .inner
                        .load(parsed.head_key()?.state_key()?)
                        .await?
                        .ok_or_else(|| {
                            AppError::Integration("active run head is missing".into())
                        })?;
                    let owner = RunHead::decode(Self::migration_value(&owner)?.as_ref())?;
                    if !matches!(owner.retention, RunRetention::Active {}) {
                        return Err(AppError::Integration(
                            "active marker has inactive head".into(),
                        ));
                    }
                    active_runs = active_runs.checked_add(1).ok_or_else(|| {
                        AppError::Conflict("App migration inventory is exhausted".into())
                    })?;
                } else if key.starts_with("history/") {
                    listed_history = listed_history.checked_add(1).ok_or_else(|| {
                        AppError::Conflict("App migration inventory is exhausted".into())
                    })?;
                }
                if key.starts_with("run/") {
                    let head = RunHead::decode(&value)?;
                    if self
                        .inner
                        .load(head.active_key().state_key()?)
                        .await?
                        .is_some()
                        != matches!(head.retention, RunRetention::Active {})
                    {
                        return Err(AppError::Integration(
                            "run head active capacity is inconsistent".into(),
                        ));
                    }
                    let (rows, history_checkpoints, deleting) = self
                        .preflight_migration_history(&head, Some(definitions))
                        .await?;
                    all_history = all_history.checked_add(rows).ok_or_else(|| {
                        AppError::Conflict("App migration inventory is exhausted".into())
                    })?;
                    if !deleting {
                        checkpoints =
                            checkpoints
                                .checked_add(history_checkpoints)
                                .ok_or_else(|| {
                                    AppError::Conflict(
                                        "App migration inventory is exhausted".into(),
                                    )
                                })?;
                        for (id, revision) in head.definition_pins() {
                            if !definitions.get(id).is_some_and(|definitions| {
                                definitions
                                    .iter()
                                    .any(|definition| definition.revision() == revision)
                            }) {
                                return Err(AppError::Conflict(
                                    "retained workflow revision is not configured".into(),
                                ));
                            }
                        }
                        if !head.agent_revisions.iter().all(|(key, revision)| {
                            configured_agents.contains(&(key.as_str(), revision.as_str()))
                        }) {
                            return Err(AppError::Conflict(
                                "retained agent revision is not configured".into(),
                            ));
                        }
                        let definition = definitions
                            .get(&head.workflow_id)
                            .and_then(|definitions| {
                                definitions.iter().find(|definition| {
                                    definition.revision() == head.definition_revision
                                })
                            })
                            .ok_or_else(|| {
                                AppError::Conflict(
                                    "retained workflow revision is not configured".into(),
                                )
                            })?;
                        definition.validate_child_relationships(definitions, &head.child_revisions).map_err(|_| AppError::Integration("retained workflow child revision relationships are inconsistent".into()))?;
                    }
                    if deleting {
                        removed_other = removed_other
                            .checked_add(
                                rows.checked_sub(history_checkpoints)
                                    .and_then(|rows| rows.checked_add(1))
                                    .ok_or_else(|| {
                                        AppError::Conflict(
                                            "App migration progress is exhausted".into(),
                                        )
                                    })?,
                            )
                            .ok_or_else(|| {
                                AppError::Conflict("App migration progress is exhausted".into())
                            })?;
                        deletion_jobs = deletion_jobs
                            .checked_add(rows.div_ceil(PAGE_SIZE as u64).max(1))
                            .ok_or_else(|| {
                                AppError::Conflict("App migration progress is exhausted".into())
                            })?;
                        snapshot
                            .revision
                            .get()
                            .checked_add(rows.div_ceil(PAGE_SIZE as u64).max(1))
                            .ok_or_else(|| {
                                AppError::Conflict(
                                    "App migration deletion revision is exhausted".into(),
                                )
                            })?;
                    }
                } else if key.starts_with("workspace/") {
                    let workspace: WorkspaceRecord = RecordCodec::decode(&value)?;
                    workspace.validate(&workspace.id, configured_agents)?;
                }
                if key.starts_with("history/") {
                    match RecordKey::history_key(key)? {
                        RecordKey::Checkpoint { .. } => {}
                        RecordKey::Ledger { .. } => {
                            return Err(AppError::Integration(
                                "legacy inventory contains settlement pages".into(),
                            ));
                        }
                        _ => {
                            other = other.checked_add(1).ok_or_else(|| {
                                AppError::Conflict("App migration progress is exhausted".into())
                            })?
                        }
                    }
                } else if key != "app" {
                    other = other.checked_add(1).ok_or_else(|| {
                        AppError::Conflict("App migration progress is exhausted".into())
                    })?;
                }
            }
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        if all_history != listed_history || active_runs != root.run_count {
            return Err(AppError::Integration(
                "App migration inventory ownership is inconsistent".into(),
            ));
        }
        other = other.checked_sub(removed_other).ok_or_else(|| {
            AppError::Integration("App migration inventory count is inconsistent".into())
        })?;
        let removed_tails = self
            .validate_migration_projections(all_history, true)
            .await?;
        other = other.checked_sub(removed_tails).ok_or_else(|| {
            AppError::Integration("App migration projection count is inconsistent".into())
        })?;
        // Claim, three phase transitions, Ready, and the normal generation claim.
        let publications = checkpoints
            .checked_add(other.div_ceil(PAGE_SIZE as u64))
            .and_then(|count| count.checked_add(deletion_jobs))
            .and_then(|count| count.checked_add(6))
            .ok_or_else(|| AppError::Conflict("App migration progress is exhausted".into()))?;
        root_snapshot
            .revision
            .get()
            .checked_add(publications)
            .ok_or_else(|| AppError::Conflict("App migration root revision is exhausted".into()))?;
        let mut prospective = root.clone();
        prospective.epoch = EPOCH.into();
        prospective.migration = AppMigration::Migrating {
            id: "0".repeat(32),
            phase: MigrationPhase::Checkpoints {
                after: Some(longest_key),
            },
        };
        RecordCodec::encode(&prospective, 0)?;
        Ok(())
    }

    async fn preflight_migration_history(
        &self,
        head: &RunHead,
        definitions: Option<&BTreeMap<crate::workflow::WorkflowId, Vec<Arc<CompiledDefinition>>>>,
    ) -> Result<(u64, u64, bool), AppError> {
        let deleting = matches!(head.retention, RunRetention::Deleting { .. });
        let deleted_after = match &head.retention {
            RunRetention::Deleting { after } => after.as_deref(),
            RunRetention::Active {} | RunRetention::Retained {} => None,
        };
        let mut after = None;
        let mut rows = 0u64;
        let mut last_checkpoint: Option<u64> = None;
        let mut checkpoints = 0u64;
        loop {
            let (page, next) = self
                .list_scoped_page(Some(&head.history_prefix()), after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in page {
                if deleted_after.is_some_and(|cursor| snapshot.record.key.key() <= cursor) {
                    return Err(AppError::Integration(
                        "sealed deletion skipped a surviving record".into(),
                    ));
                }
                rows = rows.checked_add(1).ok_or_else(|| {
                    AppError::Conflict("App migration progress is exhausted".into())
                })?;
                let parsed = RecordKey::history_key(snapshot.record.key.key())?;
                let sequence = match &parsed {
                    RecordKey::Checkpoint { sequence, .. }
                    | RecordKey::ActionObservation { sequence, .. }
                    | RecordKey::ProjectionLink { sequence, .. }
                    | RecordKey::Ledger { sequence, .. } => *sequence,
                    _ => {
                        return Err(AppError::Integration(
                            "history record has invalid scope".into(),
                        ));
                    }
                };
                if sequence > head.sequence {
                    return Err(AppError::Integration(
                        "history observation exceeds its head".into(),
                    ));
                }
                if let RecordKey::Checkpoint { sequence, .. } = parsed {
                    let expected = match last_checkpoint {
                        Some(previous) => previous.checked_add(1),
                        None if deleting => {
                            match deleted_after.map(RecordKey::history_key).transpose()? {
                                Some(RecordKey::Checkpoint { sequence, .. }) => {
                                    sequence.checked_add(1)
                                }
                                Some(_) | None => Some(0),
                            }
                        }
                        None => Some(0),
                    };
                    if expected != Some(sequence) || sequence > head.sequence {
                        return Err(AppError::Integration(
                            "retained checkpoint inventory has a gap".into(),
                        ));
                    }
                    let mut transaction = self.inner.begin_transaction().await?;
                    let result = async {
                        Self::migration_sources(&mut transaction, std::slice::from_ref(&snapshot))
                            .await?;
                        self.migration_checkpoint_row(
                            &mut transaction,
                            &snapshot,
                            head,
                            deleting && last_checkpoint.is_none(),
                        )
                        .await
                    }
                    .await;
                    match result {
                        Ok(row) => {
                            transaction.commit().await.map_err(AppError::from)?;
                            if sequence == head.sequence
                                && !deleting
                                && let Some(definitions) = definitions
                            {
                                for (id, revision) in row.definition_pins() {
                                    if !definitions.get(id).is_some_and(|rows| {
                                        rows.iter()
                                            .any(|definition| definition.revision() == revision)
                                    }) {
                                        return Err(AppError::Conflict(
                                            "retained workflow revision is not configured".into(),
                                        ));
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            return Err(Self::rollback_migration(transaction, error).await);
                        }
                    }
                    last_checkpoint = Some(sequence);
                    checkpoints = checkpoints.checked_add(1).ok_or_else(|| {
                        AppError::Conflict("App migration inventory is exhausted".into())
                    })?;
                }
            }
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        if last_checkpoint != Some(head.sequence)
            && !(deleting
                && deleted_after.is_some_and(|cursor| {
                    head.checkpoint_key(head.sequence)
                        .state_key()
                        .is_ok_and(|key| cursor >= key.key())
                }))
        {
            return Err(AppError::Integration(
                "retained latest checkpoint is missing".into(),
            ));
        }
        Ok((rows, checkpoints, deleting))
    }

    async fn migration_checkpoint_row<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        snapshot: &StateRecordSnapshot,
        head: &RunHead,
        first_surviving: bool,
    ) -> Result<RunRecord, AppError> {
        RecordKey::validate_value(snapshot.record.key.key(), &snapshot.record.value)?;
        let legacy =
            snapshot.record.value.get("epoch").and_then(Value::as_str) == Some(LEGACY_EPOCH);
        let mut pages = Vec::new();
        let mut row = if legacy {
            RecordCodec::parse_legacy_run(&snapshot.record.value)?
        } else {
            self.decode_run(transaction, snapshot.clone(), false, &mut pages)
                .await
                .map_err(StorageFailure::into_error)?
        };
        row.epoch = EPOCH.into();
        let mut normalized_head = RecordCodec::decode::<RunHead>(&RecordCodec::encode(head, 0)?)?;
        normalized_head.epoch = EPOCH.into();
        if row.checkpoint.sequence == head.sequence {
            normalized_head.validate_row(&row)?;
        } else if row.id != head.id
            || row.workspace_id != head.workspace_id
            || row.workflow_id != head.workflow_id
            || row.occurrence != head.creation
            || row.definition_revision != head.definition_revision
            || row.child_revisions != head.child_revisions
            || row.agent_revisions != head.agent_revisions
            || row.checkpoint.sequence > head.sequence
        {
            return Err(AppError::Integration(
                "retained checkpoint has foreign head identity".into(),
            ));
        }
        if let Some(sequence) = row.checkpoint.sequence.checked_sub(1) {
            let key = head.checkpoint_key(sequence).state_key()?;
            if let Some(previous) = transaction.load(key.clone()).await? {
                if previous.record.key != key {
                    return Err(AppError::Integration(
                        "retained checkpoint predecessor has foreign identity".into(),
                    ));
                }
                RecordKey::validate_value(key.key(), &previous.record.value)?;
                let mut previous = if previous.record.value.get("epoch").and_then(Value::as_str)
                    == Some(LEGACY_EPOCH)
                {
                    RecordCodec::parse_legacy_run(&previous.record.value)?
                } else {
                    self.decode_run(transaction, previous, false, &mut pages)
                        .await
                        .map_err(StorageFailure::into_error)?
                };
                previous.epoch = EPOCH.into();
                if !row.same_run(&previous) {
                    return Err(AppError::Integration(
                        "retained checkpoint predecessor has foreign identity".into(),
                    ));
                }
                row.previous = Some(previous.checkpoint);
            } else if !first_surviving {
                return Err(AppError::Integration(
                    "retained checkpoint predecessor is missing".into(),
                ));
            }
        }
        let definition = RecordKey::Definition {
            workflow: row.workflow_id.clone(),
            revision: row.definition_revision.clone(),
        };
        let snapshot = transaction
            .load(definition.state_key()?)
            .await?
            .ok_or_else(|| {
                AppError::Integration("retained workflow definition is missing".into())
            })?;
        let definition: DefinitionRecord =
            RecordCodec::decode(Self::migration_value(&snapshot)?.as_ref())?;
        definition.validate()?;
        let definition = CompiledDefinition::parse(&definition.canonical)?;
        for (id, revision) in row.definition_pins() {
            if id == &row.workflow_id && revision == row.definition_revision {
                continue;
            }
            let key = RecordKey::Definition {
                workflow: id.clone(),
                revision: revision.into(),
            };
            let snapshot = transaction.load(key.state_key()?).await?.ok_or_else(|| {
                AppError::Integration("retained workflow revision has no durable definition".into())
            })?;
            let catalog: DefinitionRecord =
                RecordCodec::decode(Self::migration_value(&snapshot)?.as_ref())?;
            catalog.validate()?;
        }
        if first_surviving && row.previous.is_none() && row.checkpoint.sequence > 0 {
            row.validate_retained_fragment(&definition, row.checkpoint.sequence)?;
        } else {
            row.validate_recovery(&definition)?;
        }
        Ok(row)
    }

    fn migration_equal(left: &Value, right: &Value) -> Result<bool, AppError> {
        let left = serde_json::to_vec(left)
            .map_err(|_| AppError::Integration("App migration evidence encoding failed".into()))?;
        let right = serde_json::to_vec(right)
            .map_err(|_| AppError::Integration("App migration evidence encoding failed".into()))?;
        Ok(left == right)
    }

    async fn migration_sources<T: StateTransactionBackend>(
        transaction: &mut T,
        sources: &[StateRecordSnapshot],
    ) -> Result<(), AppError> {
        for expected in sources {
            let actual = transaction
                .load(expected.record.key.clone())
                .await?
                .ok_or_else(|| AppError::Conflict("App migration source disappeared".into()))?;
            if actual.record.key != expected.record.key
                || actual.revision != expected.revision
                || !Self::migration_equal(&actual.record.value, &expected.record.value)?
            {
                return Err(AppError::Conflict("App migration source changed".into()));
            }
        }
        Ok(())
    }

    async fn rollback_migration<T: StateTransactionBackend>(
        transaction: T,
        primary: AppError,
    ) -> AppError {
        if transaction.rollback().await.is_err() {
            AppTrace::event(
                || tracing::warn!(target: "app_kit", event = "app.migration.rollback_failed"),
            );
        }
        // No retry follows this path, including when rollback fails.
        primary
    }

    async fn publish_migration_root(
        &self,
        expected: &StateRecordSnapshot,
        intended: &AppRecord,
    ) -> Result<(), AppError> {
        let root = StateRecord::new(
            RecordKey::App.state_key()?,
            RecordCodec::encode(intended, 0)?,
        );
        let mut transaction = self.inner.begin_transaction().await?;
        let result = async {
            Self::migration_sources(&mut transaction, std::slice::from_ref(expected)).await?;
            transaction.put(root.clone()).await?;
            Ok::<(), AppError>(())
        }
        .await;
        if let Err(error) = result {
            return Err(Self::rollback_migration(transaction, error).await);
        }
        self.commit_migration(transaction, expected, root, Vec::new())
            .await
    }

    async fn commit_migration<T: StateTransactionBackend>(
        &self,
        transaction: T,
        expected: &StateRecordSnapshot,
        root: StateRecord,
        rows: Vec<(StateKey, Option<StateRecordSnapshot>)>,
    ) -> Result<(), AppError> {
        match transaction.commit().await {
            Ok(()) => Ok(()),
            Err(
                error @ (StateStoreError::InvalidConfig(_)
                | StateStoreError::InvalidRequest(_)
                | StateStoreError::Serialization(_)
                | StateStoreError::TransactionConflict { .. }),
            ) => Err(error.into()),
            Err(error) => {
                // Only exact same-publication evidence can turn an uncertain
                // consumed commit into success. Later progress is not a receipt.
                let observed = async {
                    let mut transaction = self.inner.begin_transaction().await?;
                    let result = async {
                        let actual =
                            transaction.load(root.key.clone()).await?.ok_or_else(|| {
                                AppError::Conflict("App migration publication is unobserved".into())
                            })?;
                        if actual.record.key != root.key
                            || Some(actual.revision.get()) != expected.revision.get().checked_add(1)
                            || !Self::migration_equal(&actual.record.value, &root.value)?
                        {
                            return Err(AppError::Conflict(
                                "App migration publication does not match".into(),
                            ));
                        }
                        for (key, value) in rows {
                            let actual = transaction.load(key).await?;
                            let matches = match (&actual, &value) {
                                (Some(actual), Some(expected)) => {
                                    actual.record.key == expected.record.key
                                        && actual.revision == expected.revision
                                        && Self::migration_equal(
                                            &actual.record.value,
                                            &expected.record.value,
                                        )?
                                }
                                (None, None) => true,
                                _ => false,
                            };
                            if !matches {
                                return Err(AppError::Conflict(
                                    "App migration intended rows do not match".into(),
                                ));
                            }
                        }
                        Ok::<(), AppError>(())
                    }
                    .await;
                    match result {
                        Ok(()) => transaction.commit().await.map_err(AppError::from),
                        Err(error) => Err(Self::rollback_migration(transaction, error).await),
                    }
                }
                .await;
                match observed {
                    Ok(()) => Ok(()),
                    Err(_) => {
                        AppTrace::event(
                            || tracing::warn!(target: "app_kit", event = "app.migration.readback_failed"),
                        );
                        Err(error.into())
                    }
                }
            }
        }
    }

    fn migration_progress(root: &AppRecord, phase: MigrationPhase) -> Result<AppRecord, AppError> {
        let AppMigration::Migrating { id, .. } = &root.migration else {
            return Err(AppError::Conflict("App migration is not owned".into()));
        };
        let mut next = root.clone();
        next.migration = AppMigration::Migrating {
            id: id.clone(),
            phase,
        };
        Ok(next)
    }

    async fn migrate_checkpoint(
        &self,
        root: &AppRecord,
        expected: &StateRecordSnapshot,
        after: Option<&str>,
    ) -> Result<(), AppError> {
        let mut cursor = after.map(str::to_owned);
        loop {
            let (rows, next) = self
                .list_scoped_page(Some("history/"), cursor.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for source in rows {
                if !matches!(
                    RecordKey::history_key(source.record.key.key())?,
                    RecordKey::Checkpoint { .. }
                ) {
                    continue;
                }
                if source.record.value.get("epoch").and_then(Value::as_str) != Some(LEGACY_EPOCH) {
                    return Err(AppError::Integration(
                        "checkpoint advanced beyond migration progress".into(),
                    ));
                }
                let mut transaction = self.inner.begin_transaction().await?;
                let staged = async {
                    Self::migration_sources(&mut transaction, &[expected.clone(), source.clone()])
                        .await?;
                    let head_key = RecordKey::history_key(source.record.key.key())?
                        .head_key()?
                        .state_key()?;
                    let head = transaction.load(head_key).await?.ok_or_else(|| {
                        AppError::Integration("checkpoint has no run head".into())
                    })?;
                    let head = RunHead::decode(Self::migration_value(&head)?.as_ref())?;
                    head.ensure_readable()?;
                    let row = self
                        .migration_checkpoint_row(&mut transaction, &source, &head, false)
                        .await?;
                    if row.previous.as_ref().is_some_and(|previous| {
                        previous.settlement_references().len()
                            != previous.settlement_count().div_ceil(64)
                    }) {
                        return Err(AppError::Integration(
                            "migration predecessor is not converted".into(),
                        ));
                    }
                    let prepared = RecordCodec::run(&row)?;
                    let mut keys = Vec::new();
                    if let AppStorageRecord::Checkpoint { pages, key, .. } = &prepared {
                        keys.extend(pages.iter().map(|page| page.key.clone()));
                        keys.push(key.state_key()?);
                    }
                    prepared
                        .put(&mut transaction)
                        .await
                        .map_err(StorageFailure::into_error)?;
                    let mut intended = Vec::with_capacity(keys.len());
                    for key in keys {
                        intended.push((key.clone(), transaction.load(key).await?));
                    }
                    let next = Self::migration_progress(
                        root,
                        MigrationPhase::Checkpoints {
                            after: Some(source.record.key.key().into()),
                        },
                    )?;
                    let next = StateRecord::new(
                        RecordKey::App.state_key()?,
                        RecordCodec::encode(&next, 0)?,
                    );
                    transaction.put(next.clone()).await?;
                    Ok::<_, AppError>((next, intended))
                }
                .await;
                return match staged {
                    Ok((next, intended)) => {
                        self.commit_migration(transaction, expected, next, intended)
                            .await
                    }
                    Err(error) => Err(Self::rollback_migration(transaction, error).await),
                };
            }
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        self.publish_migration_root(
            expected,
            &Self::migration_progress(root, MigrationPhase::Records { after: None })?,
        )
        .await
    }

    async fn migrate_records(
        &self,
        root: &AppRecord,
        expected: &StateRecordSnapshot,
        after: Option<&str>,
    ) -> Result<(), AppError> {
        let mut cursor = after.map(str::to_owned);
        let mut selected = Vec::with_capacity(PAGE_SIZE);
        let mut exhausted = false;
        while selected.len() < PAGE_SIZE && !exhausted {
            let (rows, next) = self
                .list_scoped_page(None, cursor.as_deref(), PAGE_SIZE - selected.len())
                .await
                .map_err(StorageFailure::into_error)?;
            for source in rows {
                cursor = Some(source.record.key.key().into());
                match source.record.value.get("epoch").and_then(Value::as_str) {
                    Some(LEGACY_EPOCH) => {
                        if source.record.key.key().starts_with("history/")
                            && matches!(
                                RecordKey::history_key(source.record.key.key())?,
                                RecordKey::Checkpoint { .. } | RecordKey::Ledger { .. }
                            )
                        {
                            return Err(AppError::Integration(
                                "unconverted checkpoint at record migration".into(),
                            ));
                        }
                        selected.push(source);
                    }
                    Some(EPOCH) => {}
                    _ => {
                        return Err(AppError::Integration(
                            "unsupported app record schema version".into(),
                        ));
                    }
                }
            }
            exhausted = next.is_none();
            if let Some(next) = next {
                cursor = Some(next);
            }
        }
        let next = Self::migration_progress(
            root,
            if selected.is_empty() {
                MigrationPhase::Audit
            } else {
                MigrationPhase::Records { after: cursor }
            },
        )?;
        let next = StateRecord::new(RecordKey::App.state_key()?, RecordCodec::encode(&next, 0)?);
        let mut transaction = self.inner.begin_transaction().await?;
        let staged = async {
            Self::migration_sources(&mut transaction, std::slice::from_ref(expected)).await?;
            Self::migration_sources(&mut transaction, &selected).await?;
            let mut intended = Vec::with_capacity(selected.len());
            for source in selected {
                let value = Self::migration_value(&source)?.into_owned();
                let record = StateRecord::new(source.record.key, value);
                let key = record.key.clone();
                transaction.put(record).await?;
                intended.push((key.clone(), transaction.load(key).await?));
            }
            transaction.put(next.clone()).await?;
            Ok::<_, AppError>(intended)
        }
        .await;
        match staged {
            Ok(intended) => {
                self.commit_migration(transaction, expected, next, intended)
                    .await
            }
            Err(error) => Err(Self::rollback_migration(transaction, error).await),
        }
    }

    async fn migrate_deletion(
        &self,
        root: &AppRecord,
        expected: &StateRecordSnapshot,
        after: Option<&str>,
    ) -> Result<(), AppError> {
        let mut cursor = after.map(str::to_owned);
        loop {
            let (rows, next) = self
                .list_scoped_page(Some("run/"), cursor.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for source in rows {
                let mut head = RunHead::decode(Self::migration_value(&source)?.as_ref())?;
                let RunRetention::Deleting {
                    after: deleted_after,
                } = &head.retention
                else {
                    continue;
                };
                let mut selected = Vec::with_capacity(PAGE_SIZE);
                let mut page_after = None;
                let mut complete = false;
                while selected.len() < PAGE_SIZE && !complete {
                    let (page, next) = self
                        .list_scoped_page(
                            Some(&head.history_prefix()),
                            page_after.as_deref(),
                            PAGE_SIZE - selected.len(),
                        )
                        .await
                        .map_err(StorageFailure::into_error)?;
                    for row in page {
                        if deleted_after
                            .as_ref()
                            .is_some_and(|after| row.record.key.key() <= after.as_str())
                        {
                            return Err(AppError::Integration(
                                "sealed deletion skipped a surviving record".into(),
                            ));
                        }
                        selected.push(row);
                    }
                    complete = next.is_none();
                    page_after = next;
                }
                let mut transaction = self.inner.begin_transaction().await?;
                let staged = async {
                    Self::migration_sources(&mut transaction, &[expected.clone(), source.clone()])
                        .await?;
                    Self::migration_sources(&mut transaction, &selected).await?;
                    if transaction
                        .load(head.active_key().state_key()?)
                        .await?
                        .is_some()
                    {
                        return Err(AppError::Integration(
                            "sealed deletion retains active capacity".into(),
                        ));
                    }
                    let mut touched = Vec::new();
                    for row in &selected {
                        let key = RecordKey::history_key(row.record.key.key())?;
                        match key {
                            RecordKey::ActionObservation {
                                workspace,
                                workflow,
                                run,
                                occurrence,
                                ordinal,
                                sequence,
                            } => touched.extend(
                                RunProjection::Action(ordinal)
                                    .remove(
                                        &mut transaction,
                                        &RunKey::new(workspace, workflow, run, occurrence),
                                        sequence,
                                        LEGACY_EPOCH,
                                    )
                                    .await
                                    .map_err(StorageFailure::into_error)?,
                            ),
                            RecordKey::ProjectionLink {
                                key,
                                projection,
                                sequence,
                            } => touched.extend(
                                projection
                                    .remove(&mut transaction, &key, sequence, LEGACY_EPOCH)
                                    .await
                                    .map_err(StorageFailure::into_error)?,
                            ),
                            RecordKey::Checkpoint { sequence, .. } if sequence <= head.sequence => {
                                transaction.delete(row.record.key.clone()).await?;
                                touched.push(row.record.key.clone());
                            }
                            _ => {
                                return Err(AppError::Integration(
                                    "invalid legacy deletion record".into(),
                                ));
                            }
                        }
                    }
                    if complete {
                        transaction.delete(source.record.key.clone()).await?;
                    } else {
                        head.epoch = LEGACY_EPOCH.into();
                        head.retention = RunRetention::Deleting {
                            after: selected.last().map(|row| row.record.key.key().into()),
                        };
                        transaction
                            .put(StateRecord::new(
                                source.record.key.clone(),
                                RecordCodec::encode(&head, 0)?,
                            ))
                            .await?;
                    }
                    touched.push(source.record.key.clone());
                    touched.sort_by(|left, right| left.key().cmp(right.key()));
                    touched.dedup();
                    let mut intended = Vec::with_capacity(touched.len());
                    for key in touched {
                        let value = transaction.load(key.clone()).await?;
                        intended.push((key, value));
                    }
                    let next = Self::migration_progress(
                        root,
                        MigrationPhase::Deletions {
                            after: if complete {
                                Some(source.record.key.key().into())
                            } else {
                                after.map(str::to_owned)
                            },
                        },
                    )?;
                    let next = StateRecord::new(
                        RecordKey::App.state_key()?,
                        RecordCodec::encode(&next, 0)?,
                    );
                    transaction.put(next.clone()).await?;
                    Ok::<_, AppError>((next, intended))
                }
                .await;
                match staged {
                    Ok((next, intended)) => {
                        self.commit_migration(transaction, expected, next, intended)
                            .await?
                    }
                    Err(error) => return Err(Self::rollback_migration(transaction, error).await),
                }
                return Ok(());
            }
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        self.publish_migration_root(
            expected,
            &Self::migration_progress(root, MigrationPhase::Checkpoints { after: None })?,
        )
        .await?;
        Ok(())
    }

    fn migration_projection(
        snapshot: &StateRecordSnapshot,
    ) -> Result<Option<(RunProjection, RunKey, Option<RunObservation>)>, AppError> {
        let text = snapshot.record.key.key();
        if text.starts_with("history/") {
            return match RecordKey::history_key(text)? {
                RecordKey::ActionObservation {
                    workspace,
                    workflow,
                    run,
                    occurrence,
                    ordinal,
                    sequence,
                } => Ok(Some((
                    RunProjection::Action(ordinal),
                    RunKey::new(workspace, workflow, run, occurrence.clone()),
                    Some(RunObservation {
                        occurrence,
                        sequence,
                    }),
                ))),
                RecordKey::ProjectionLink {
                    key,
                    projection,
                    sequence,
                } => {
                    let observation = RunObservation {
                        occurrence: key.occurrence().into(),
                        sequence,
                    };
                    Ok(Some((projection, key, Some(observation))))
                }
                _ => Ok(None),
            };
        }
        if !text.starts_with("projection/") {
            return Ok(None);
        }
        let parts = text.split('/').collect::<Vec<_>>();
        let ["projection", workspace, workflow, run, projection @ ..] = parts.as_slice() else {
            return Err(AppError::Integration(
                "saved projection key is invalid".into(),
            ));
        };
        let projection = RunProjection::parse(projection)
            .ok_or_else(|| AppError::Integration("saved projection is invalid".into()))?;
        let index: RunIndex = RecordCodec::decode(Self::migration_value(snapshot)?.as_ref())?;
        let RunIndex::Tail { last, .. } = index else {
            return Err(AppError::Integration("saved tail contains a link".into()));
        };
        Ok(Some((
            projection,
            RunKey::new(
                workspace.parse()?,
                workflow.parse()?,
                run.parse()?,
                last.occurrence,
            ),
            None,
        )))
    }

    async fn migration_index<T: StateTransactionBackend>(
        transaction: &mut T,
        key: &RecordKey,
    ) -> Result<RunIndex, AppError> {
        let source = transaction
            .load(key.state_key()?)
            .await?
            .ok_or_else(|| AppError::Integration("saved observation neighbor is missing".into()))?;
        RecordCodec::decode(Self::migration_value(&source)?.as_ref())
    }

    async fn validate_migration_projections(
        &self,
        history_rows: u64,
        reserve_revisions: bool,
    ) -> Result<u64, AppError> {
        let mut after = None;
        let mut removed_tails = 0u64;
        let mut listed_links = 0u64;
        let mut reachable_links = 0u64;
        loop {
            let (rows, next) = self
                .list_scoped_page(None, after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for snapshot in rows {
                let Some((projection, key, observation)) = Self::migration_projection(&snapshot)?
                else {
                    continue;
                };
                let mut transaction = self.inner.begin_transaction().await?;
                let checked = async {
                    Self::migration_sources(&mut transaction, std::slice::from_ref(&snapshot)).await?;
                    let index: RunIndex = RecordCodec::decode(Self::migration_value(&snapshot)?.as_ref())?;
                    match (index, observation) {
                        (RunIndex::Tail { last, .. }, None) => {
                            if !matches!(Self::migration_index(&mut transaction, &projection.link_key(&key, &last)).await?, RunIndex::Link { next: None, .. }) {
                                return Err(AppError::Integration("saved tail does not own its last observation".into()));
                            }
                            let mut prior = Some(last);
                            let mut remaining = history_rows;
                            let mut removed = true;
                            while let Some(observation) = prior {
                                reachable_links = reachable_links.checked_add(1).ok_or_else(|| AppError::Conflict("App migration inventory is exhausted".into()))?;
                                remaining = remaining.checked_sub(1).ok_or_else(|| AppError::Integration("saved observation chain is cyclic".into()))?;
                                let link = projection.link_key(&key, &observation);
                                let head = transaction.load(link.head_key()?.state_key()?).await?.ok_or_else(|| AppError::Integration("saved observation head is missing".into()))?;
                                let head = RunHead::decode(Self::migration_value(&head)?.as_ref())?;
                                removed &= matches!(head.retention, RunRetention::Deleting { .. });
                                let RunIndex::Link { previous, .. } = Self::migration_index(&mut transaction, &link).await? else { return Err(AppError::Integration("saved observation contains a tail".into())); };
                                prior = previous;
                            }
                            if removed { removed_tails = removed_tails.checked_add(1).ok_or_else(|| AppError::Conflict("App migration inventory is exhausted".into()))?; }
                        }
                        (RunIndex::Link { previous, next, .. }, Some(observed)) => {
                            listed_links = listed_links.checked_add(1).ok_or_else(|| AppError::Conflict("App migration inventory is exhausted".into()))?;
                            if let Some(prior) = previous
                                && !matches!(Self::migration_index(&mut transaction, &projection.link_key(&key, &prior)).await?, RunIndex::Link { next: Some(actual), .. } if actual == observed) {
                                    return Err(AppError::Integration("saved previous neighbor is inconsistent".into()));
                            }
                            if let Some(following) = &next {
                                if !matches!(Self::migration_index(&mut transaction, &projection.link_key(&key, following)).await?, RunIndex::Link { previous: Some(actual), .. } if actual == observed) {
                                    return Err(AppError::Integration("saved next neighbor is inconsistent".into()));
                                }
                            } else if !matches!(Self::migration_index(&mut transaction, &projection.tail_key(key.workspace(), key.workflow(), key.run())).await?, RunIndex::Tail { last, .. } if last == observed) {
                                return Err(AppError::Integration("saved tail is inconsistent".into()));
                            }

                        }
                        _ => return Err(AppError::Integration("saved observation kind is inconsistent".into())),
                    }
                    Ok::<(), AppError>(())
                }.await;
                match checked {
                    Ok(()) => transaction.commit().await.map_err(AppError::from)?,
                    Err(error) => return Err(Self::rollback_migration(transaction, error).await),
                }
                // Ordinary revisions need only this cheap proven upper bound.
                // Near exhaustion, count each actual future staged mutation.
                let maximum = history_rows.checked_add(1).ok_or_else(|| {
                    AppError::Conflict("App migration progress is exhausted".into())
                })?;
                if reserve_revisions && snapshot.revision.get().checked_add(maximum).is_none() {
                    let touches = self
                        .migration_projection_touches(&snapshot, history_rows)
                        .await?;
                    snapshot
                        .revision
                        .get()
                        .checked_add(touches)
                        .ok_or_else(|| {
                            AppError::Conflict(
                                "App migration projection revision is exhausted".into(),
                            )
                        })?;
                }
            }
            match next {
                Some(next) => after = Some(next),
                None => {
                    if listed_links != reachable_links {
                        return Err(AppError::Integration(
                            "saved observation inventory has unreachable links".into(),
                        ));
                    }
                    return Ok(removed_tails);
                }
            }
        }
    }

    /// This rare near-u64::MAX path streams the accepted deletion order for one
    /// key. It retains one bounded page, never a second inventory or write log.
    async fn migration_projection_touches(
        &self,
        target: &StateRecordSnapshot,
        history_rows: u64,
    ) -> Result<u64, AppError> {
        let mut run_after = None;
        let mut touches = 0u64;
        let mut deleted = false;
        loop {
            let (runs, next) = self
                .list_scoped_page(Some("run/"), run_after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for source in runs {
                let head = RunHead::decode(Self::migration_value(&source)?.as_ref())?;
                if !matches!(head.retention, RunRetention::Deleting { .. }) {
                    continue;
                }
                let mut after = None;
                let mut complete = false;
                while !complete {
                    let mut page = Vec::with_capacity(PAGE_SIZE);
                    while page.len() < PAGE_SIZE && !complete {
                        let (rows, next) = self
                            .list_scoped_page(
                                Some(&head.history_prefix()),
                                after.as_deref(),
                                PAGE_SIZE - page.len(),
                            )
                            .await
                            .map_err(StorageFailure::into_error)?;
                        page.extend(rows);
                        complete = next.is_none();
                        after = next;
                    }
                    let mut touched = 0u64;
                    let mut transaction = self.inner.begin_transaction().await?;
                    let result = async {
                        for row in &page {
                            if row.record.key == target.record.key {
                                touched += 1;
                                deleted = true;
                            }
                            let Some((projection, key, Some(observed))) =
                                Self::migration_projection(row)?
                            else {
                                continue;
                            };
                            let index: RunIndex =
                                RecordCodec::decode(Self::migration_value(row)?.as_ref())?;
                            let RunIndex::Link { previous, next, .. } = index else {
                                unreachable!()
                            };
                            let previous = Self::migration_remaining_neighbor(
                                &mut transaction,
                                &projection,
                                &key,
                                previous,
                                true,
                                row,
                                history_rows,
                            )
                            .await?;
                            let next = Self::migration_remaining_neighbor(
                                &mut transaction,
                                &projection,
                                &key,
                                next,
                                false,
                                row,
                                history_rows,
                            )
                            .await?;
                            for neighbor in previous.iter().chain(&next) {
                                touched += u64::from(
                                    projection.link_key(&key, neighbor).state_key()?
                                        == target.record.key,
                                );
                            }
                            if next.is_none()
                                && projection
                                    .tail_key(key.workspace(), key.workflow(), key.run())
                                    .state_key()?
                                    == target.record.key
                            {
                                touched += 1;
                                deleted |= previous.is_none();
                            }
                            if observed.occurrence != key.occurrence() {
                                return Err(AppError::Integration(
                                    "saved observation scope is inconsistent".into(),
                                ));
                            }
                        }
                        Ok::<(), AppError>(())
                    }
                    .await;
                    match result {
                        Ok(()) => transaction.commit().await.map_err(AppError::from)?,
                        Err(error) => {
                            return Err(Self::rollback_migration(transaction, error).await);
                        }
                    }
                    touches = touches.checked_add(touched).ok_or_else(|| {
                        AppError::Conflict("App migration progress is exhausted".into())
                    })?;
                }
            }
            match next {
                Some(next) => run_after = Some(next),
                None => break,
            }
        }
        touches
            .checked_add(u64::from(!deleted))
            .ok_or_else(|| AppError::Conflict("App migration progress is exhausted".into()))
    }

    async fn migration_remaining_neighbor<T: StateTransactionBackend>(
        transaction: &mut T,
        projection: &RunProjection,
        key: &RunKey,
        mut neighbor: Option<RunObservation>,
        previous: bool,
        current: &StateRecordSnapshot,
        mut remaining: u64,
    ) -> Result<Option<RunObservation>, AppError> {
        let current_key = current.record.key.key();
        let current_head = RecordKey::history_key(current_key)?
            .head_key()?
            .state_key()?;
        while let Some(observation) = &neighbor {
            let link = projection.link_key(key, observation);
            let head_key = link.head_key()?.state_key()?;
            let source = transaction
                .load(head_key.clone())
                .await?
                .ok_or_else(|| AppError::Integration("saved observation head is missing".into()))?;
            let head = RunHead::decode(Self::migration_value(&source)?.as_ref())?;
            if !matches!(head.retention, RunRetention::Deleting { .. })
                || head_key.key() > current_head.key()
                || head_key.key() == current_head.key() && link.state_key()?.key() >= current_key
            {
                return Ok(neighbor);
            }
            remaining = remaining
                .checked_sub(1)
                .ok_or_else(|| AppError::Integration("saved observation chain is cyclic".into()))?;
            let RunIndex::Link {
                previous: earlier,
                next: later,
                ..
            } = Self::migration_index(transaction, &link).await?
            else {
                return Err(AppError::Integration(
                    "saved observation contains a tail".into(),
                ));
            };
            neighbor = if previous { earlier } else { later };
        }
        Ok(None)
    }

    async fn audit_migration(
        &self,
        root: &AppRecord,
        configured_agents: &BTreeSet<(&str, &str)>,
    ) -> Result<(), AppError> {
        let mut after = None;
        let mut history_rows = 0u64;
        let mut owned_history = 0u64;
        let mut active_runs = 0usize;
        loop {
            let (rows, next) = self
                .list_scoped_page(None, after.as_deref(), PAGE_SIZE)
                .await
                .map_err(StorageFailure::into_error)?;
            for source in rows {
                RecordCodec::preflight_epoch(&source.record.value)?;
                let value = Self::migration_value(&source)?;
                if source.record.key.key() == "app" {
                    let actual: AppRecord = RecordCodec::decode(&source.record.value)?;
                    if actual != *root {
                        return Err(AppError::Conflict(
                            "App migration audit lost ownership".into(),
                        ));
                    }
                } else if source.record.key.key().starts_with("run/") {
                    let head = RunHead::decode(&value)?;
                    head.ensure_readable()?;
                    let (rows, _, _) = self.preflight_migration_history(&head, None).await?;
                    owned_history = owned_history.checked_add(rows).ok_or_else(|| {
                        AppError::Integration("App migration inventory is exhausted".into())
                    })?;
                    if self
                        .inner
                        .load(head.active_key().state_key()?)
                        .await?
                        .is_some()
                        != matches!(head.retention, RunRetention::Active {})
                    {
                        return Err(AppError::Integration(
                            "run head active capacity is inconsistent".into(),
                        ));
                    }
                } else if source.record.key.key().starts_with("active/") {
                    let head = self
                        .inner
                        .load(
                            RecordKey::history_key(source.record.key.key())?
                                .head_key()?
                                .state_key()?,
                        )
                        .await?
                        .ok_or_else(|| {
                            AppError::Integration("active run head is missing".into())
                        })?;
                    let head = RunHead::decode(Self::migration_value(&head)?.as_ref())?;
                    if !matches!(head.retention, RunRetention::Active {}) {
                        return Err(AppError::Integration(
                            "active marker has inactive head".into(),
                        ));
                    }
                    active_runs = active_runs.checked_add(1).ok_or_else(|| {
                        AppError::Integration("App migration inventory is exhausted".into())
                    })?;
                } else if source.record.key.key().starts_with("history/") {
                    history_rows = history_rows.checked_add(1).ok_or_else(|| {
                        AppError::Integration("App migration inventory is exhausted".into())
                    })?;
                    let key = RecordKey::history_key(source.record.key.key())?;
                    let mut transaction = self.inner.begin_transaction().await?;
                    let checked = async {
                        Self::migration_sources(&mut transaction, std::slice::from_ref(&source))
                            .await?;
                        let owner = transaction
                            .load(key.head_key()?.state_key()?)
                            .await?
                            .ok_or_else(|| {
                                AppError::Integration("retained history has no run head".into())
                            })?;
                        RunHead::decode(Self::migration_value(&owner)?.as_ref())?
                            .ensure_readable()?;
                        if let RecordKey::Ledger {
                            workspace,
                            workflow,
                            run,
                            occurrence,
                            index,
                            sequence,
                        } = key
                        {
                            let published = RecordKey::Checkpoint {
                                workspace,
                                workflow,
                                run,
                                occurrence,
                                sequence,
                            };
                            let published_key = published.state_key()?;
                            let published = transaction
                                .load(published_key.clone())
                                .await?
                                .ok_or_else(|| {
                                    AppError::Integration(
                                        "settlement page has no publication checkpoint".into(),
                                    )
                                })?;
                            if published.record.key != published_key {
                                return Err(AppError::Integration(
                                    "settlement publication has foreign identity".into(),
                                ));
                            }
                            RecordCodec::preflight_epoch(&published.record.value)?;
                            RecordKey::validate_value(
                                published.record.key.key(),
                                &published.record.value,
                            )?;
                            let descriptor: super::SettlementPages = RecordCodec::decode(
                                &published.record.value["checkpoint"]["settlements"],
                            )?;
                            descriptor.validate(sequence)?;
                            if descriptor.pages.get(index) != Some(&sequence) {
                                return Err(AppError::Integration(
                                    "settlement page is not referenced by its publication".into(),
                                ));
                            }
                        }
                        Ok::<(), AppError>(())
                    }
                    .await;
                    match checked {
                        Ok(()) => transaction.commit().await.map_err(AppError::from)?,
                        Err(error) => {
                            return Err(Self::rollback_migration(transaction, error).await);
                        }
                    }
                } else if source.record.key.key().starts_with("workspace/") {
                    let workspace: WorkspaceRecord = RecordCodec::decode(&value)?;
                    workspace.validate(&workspace.id, configured_agents)?;
                }
            }
            match next {
                Some(next) => after = Some(next),
                None => {
                    if history_rows != owned_history || active_runs != root.run_count {
                        return Err(AppError::Integration(
                            "App migration inventory ownership is inconsistent".into(),
                        ));
                    }
                    if self
                        .validate_migration_projections(history_rows, false)
                        .await?
                        != 0
                    {
                        return Err(AppError::Integration(
                            "sealed deletion survived migration".into(),
                        ));
                    }
                    return Ok(());
                }
            }
        }
    }
}
