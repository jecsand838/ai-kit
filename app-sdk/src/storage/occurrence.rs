use serde::{Deserialize, Serialize};
use storage_sdk::state::record::{StateKey, StateRecord};
use storage_sdk::{StateStoreBackend, StateTransactionBackend};

use crate::workflow::run::{RunKey, RunRecord, RuntimeOperationFailure};
use crate::workflow::{CheckpointKind, RunId, WorkflowId};
use crate::{AppError, WorkspaceId};

use super::{AppStore, EPOCH, LEGACY_EPOCH, RecordCodec, RecordKey, StorageFailure};

/// Different saved-observation projections share the same native link owner.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum RunProjection {
    Checkpoint,
    Saved,
    Sequence(u64),
    Action(u64),
    Unsettled,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunObservation {
    pub(super) occurrence: String,
    pub(super) sequence: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum RunIndex {
    Tail {
        epoch: String,
        last: RunObservation,
    },
    Link {
        epoch: String,
        previous: Option<RunObservation>,
        next: Option<RunObservation>,
    },
}

impl RunObservation {
    fn new(record: &RunRecord, sequence: u64) -> Self {
        Self {
            occurrence: record.occurrence.clone(),
            sequence,
        }
    }

    fn validate(&self) -> Result<(), AppError> {
        RunId::validate_occurrence(&self.occurrence)
            .map_err(|_| AppError::Integration("saved observation occurrence is invalid".into()))
    }
}

impl RunIndex {
    pub(super) fn validate(&self) -> Result<(), AppError> {
        match self {
            Self::Tail { epoch, last } => {
                if epoch != EPOCH {
                    return Err(AppError::Integration(
                        "saved observation epoch is invalid".into(),
                    ));
                }
                last.validate()
            }
            Self::Link {
                epoch,
                previous,
                next,
            } => {
                if epoch != EPOCH {
                    return Err(AppError::Integration(
                        "saved observation epoch is invalid".into(),
                    ));
                }
                for observed in previous.iter().chain(next) {
                    observed.validate()?;
                }
                if previous.is_some() && previous == next {
                    return Err(AppError::Integration(
                        "saved observation neighbors are identical".into(),
                    ));
                }
                Ok(())
            }
        }
    }
}

impl RunProjection {
    pub(super) fn text(&self) -> String {
        match self {
            Self::Checkpoint => "checkpoint".into(),
            Self::Saved => "saved".into(),
            Self::Sequence(sequence) => format!("sequence/{sequence:020}"),
            Self::Action(ordinal) => format!("action/{ordinal:020}"),
            Self::Unsettled => "unsettled".into(),
        }
    }

    pub(super) fn parse(parts: &[&str]) -> Option<Self> {
        match parts {
            ["checkpoint"] => Some(Self::Checkpoint),
            ["saved"] => Some(Self::Saved),
            ["sequence", sequence] => Some(Self::Sequence(RecordKey::sequence(sequence)?)),
            ["action", ordinal] => Some(Self::Action(RecordKey::sequence(ordinal)?)),
            ["unsettled"] => Some(Self::Unsettled),
            _ => None,
        }
    }

    pub(super) fn tail_key(
        &self,
        workspace: &WorkspaceId,
        workflow: &WorkflowId,
        run: &RunId,
    ) -> RecordKey {
        RecordKey::ProjectionTail {
            workspace: workspace.clone(),
            workflow: workflow.clone(),
            run: run.clone(),
            projection: self.clone(),
        }
    }

    pub(super) fn link_key(&self, key: &RunKey, observation: &RunObservation) -> RecordKey {
        match self {
            Self::Action(ordinal) => RecordKey::ActionObservation {
                workspace: key.workspace().clone(),
                workflow: key.workflow().clone(),
                run: key.run().clone(),
                occurrence: observation.occurrence.clone(),
                ordinal: *ordinal,
                sequence: observation.sequence,
            },
            _ => RecordKey::ProjectionLink {
                key: RunKey::new(
                    key.workspace().clone(),
                    key.workflow().clone(),
                    key.run().clone(),
                    observation.occurrence.clone(),
                ),
                projection: self.clone(),
                sequence: observation.sequence,
            },
        }
    }

    pub(super) fn for_checkpoint(record: &RunRecord, previous: Option<&RunRecord>) -> Vec<Self> {
        let mut projections = vec![Self::Checkpoint, Self::Sequence(record.checkpoint.sequence)];
        match record.checkpoint_kind() {
            CheckpointKind::Action => {
                projections.push(Self::Saved);
                projections.extend(
                    record
                        .checkpoint
                        .outcomes()
                        .iter()
                        .map(|outcome| Self::Action(outcome.ordinal)),
                );
            }
            CheckpointKind::Failed { ordinal, .. }
                if previous.is_none_or(|previous| {
                    previous.checkpoint.failed_observation()
                        != record.checkpoint.failed_observation()
                }) =>
            {
                projections.push(Self::Saved);
                projections.push(Self::Action(ordinal));
            }
            CheckpointKind::Initial
            | CheckpointKind::Control
            | CheckpointKind::Terminal
            | CheckpointKind::Failed { .. } => {}
        }
        projections
    }

    async fn read<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        key: &RecordKey,
        epoch: &str,
    ) -> Result<Option<RunIndex>, StorageFailure> {
        let state_key = key.state_key().map_err(StorageFailure::permanent)?;
        let Some(row) = transaction
            .load(state_key.clone())
            .await
            .map_err(StorageFailure::from)?
        else {
            return Ok(None);
        };
        if !matches!(epoch, EPOCH | LEGACY_EPOCH)
            || row
                .record
                .value
                .get("epoch")
                .and_then(serde_json::Value::as_str)
                != Some(epoch)
        {
            return Err(StorageFailure::permanent(AppError::Integration(
                "saved observation epoch is invalid".into(),
            )));
        }
        let legacy = (epoch == LEGACY_EPOCH).then(|| {
            let mut value = row.record.value.as_ref().clone();
            value["epoch"] = serde_json::Value::String(EPOCH.into());
            value
        });
        let value = legacy.as_ref().unwrap_or(&row.record.value);
        RecordKey::validate_value(state_key.key(), value).map_err(StorageFailure::permanent)?;
        let index: RunIndex = RecordCodec::decode(value).map_err(StorageFailure::permanent)?;
        Ok(Some(index))
    }

    pub(super) async fn append<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        record: &RunRecord,
        sequence: u64,
    ) -> Result<(), StorageFailure> {
        let key = record.key();
        let observed = RunObservation::new(record, sequence);
        let tail = self.tail_key(key.workspace(), key.workflow(), key.run());
        let previous = match self.read(transaction, &tail, EPOCH).await? {
            Some(RunIndex::Tail { last, .. }) => Some(last),
            None => None,
            Some(RunIndex::Link { .. }) => {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "saved tail contains a link".into(),
                )));
            }
        };
        let link = self.link_key(&key, &observed);
        link.expect_missing(transaction).await?;
        if let Some(prior) = &previous {
            let prior_key = self.link_key(&key, prior);
            let Some(RunIndex::Link {
                previous,
                next: None,
                ..
            }) = self.read(transaction, &prior_key, EPOCH).await?
            else {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "saved tail does not own its last observation".into(),
                )));
            };
            transaction
                .put(
                    prior_key
                        .record(&RunIndex::Link {
                            epoch: EPOCH.into(),
                            previous,
                            next: Some(observed.clone()),
                        })
                        .map_err(StorageFailure::permanent)?,
                )
                .await
                .map_err(StorageFailure::from)?;
        }
        transaction
            .put(
                link.record(&RunIndex::Link {
                    epoch: EPOCH.into(),
                    previous,
                    next: None,
                })
                .map_err(StorageFailure::permanent)?,
            )
            .await
            .map_err(StorageFailure::from)?;
        transaction
            .put(
                tail.record(&RunIndex::Tail {
                    epoch: EPOCH.into(),
                    last: observed,
                })
                .map_err(StorageFailure::permanent)?,
            )
            .await
            .map_err(StorageFailure::from)
    }

    pub(super) async fn remove<T: StateTransactionBackend>(
        &self,
        transaction: &mut T,
        key: &RunKey,
        sequence: u64,
        epoch: &str,
    ) -> Result<Vec<StateKey>, StorageFailure> {
        let record = |key: &RecordKey, index: RunIndex| {
            let mut record = key.record(&index).map_err(StorageFailure::permanent)?;
            if epoch == LEGACY_EPOCH {
                std::sync::Arc::make_mut(&mut record.value)["epoch"] =
                    serde_json::Value::String(epoch.into());
            }
            Ok::<StateRecord, StorageFailure>(record)
        };
        let mut touched = Vec::with_capacity(4);
        let observed = RunObservation {
            occurrence: key.occurrence().to_owned(),
            sequence,
        };
        let link = self.link_key(key, &observed);
        touched.push(link.state_key().map_err(StorageFailure::permanent)?);
        let Some(RunIndex::Link { previous, next, .. }) =
            self.read(transaction, &link, epoch).await?
        else {
            return Err(StorageFailure::permanent(AppError::Integration(
                "saved observation link is missing".into(),
            )));
        };
        if let Some(prior) = &previous {
            let prior_key = self.link_key(key, prior);
            let Some(RunIndex::Link {
                previous: earlier,
                next: Some(actual),
                ..
            }) = self.read(transaction, &prior_key, epoch).await?
            else {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "saved previous neighbor is missing".into(),
                )));
            };
            if actual != observed {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "saved previous neighbor is inconsistent".into(),
                )));
            }
            touched.push(prior_key.state_key().map_err(StorageFailure::permanent)?);
            transaction
                .put(record(
                    &prior_key,
                    RunIndex::Link {
                        epoch: EPOCH.into(),
                        previous: earlier,
                        next: next.clone(),
                    },
                )?)
                .await
                .map_err(StorageFailure::from)?;
        }
        match &next {
            Some(following) => {
                let next_key = self.link_key(key, following);
                let Some(RunIndex::Link {
                    previous: Some(actual),
                    next: later,
                    ..
                }) = self.read(transaction, &next_key, epoch).await?
                else {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "saved next neighbor is missing".into(),
                    )));
                };
                if actual != observed {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "saved next neighbor is inconsistent".into(),
                    )));
                }
                touched.push(next_key.state_key().map_err(StorageFailure::permanent)?);
                transaction
                    .put(record(
                        &next_key,
                        RunIndex::Link {
                            epoch: EPOCH.into(),
                            previous,
                            next: later,
                        },
                    )?)
                    .await
                    .map_err(StorageFailure::from)?;
            }
            None => {
                let tail = self.tail_key(key.workspace(), key.workflow(), key.run());
                if !matches!(self.read(transaction, &tail, epoch).await?, Some(RunIndex::Tail { last, .. }) if last == observed)
                {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "saved tail is inconsistent".into(),
                    )));
                }
                touched.push(tail.state_key().map_err(StorageFailure::permanent)?);
                match previous {
                    Some(last) => transaction
                        .put(record(
                            &tail,
                            RunIndex::Tail {
                                epoch: EPOCH.into(),
                                last,
                            },
                        )?)
                        .await
                        .map_err(StorageFailure::from)?,
                    None => tail.delete(transaction).await?,
                }
            }
        }
        link.delete(transaction).await?;
        Ok(touched)
    }
}

impl<CS: StateStoreBackend> AppStore<CS> {
    pub(super) async fn projection_tail(
        &self,
        workspace: &WorkspaceId,
        workflow: &WorkflowId,
        run: &RunId,
        projection: RunProjection,
    ) -> Result<Option<RunObservation>, StorageFailure> {
        let key = projection.tail_key(workspace, workflow, run);
        let loaded: Option<(RunIndex, u64)> = self.load_record(key).await?;
        match loaded {
            Some((RunIndex::Tail { last, .. }, _)) => Ok(Some(last)),
            None => Ok(None),
            Some((RunIndex::Link { .. }, _)) => Err(StorageFailure::permanent(
                AppError::Integration("saved tail contains a link".into()),
            )),
        }
    }
}

/// Captured source and absence/cleanliness fences belong to one original copy admission.
#[derive(Clone)]
pub(crate) struct RunCopy {
    pub(crate) source: Option<(RunRecord, u64)>,
    pub(crate) saved: Option<RunRecord>,
    fences: Vec<(RecordKey, Option<u64>)>,
}

impl RunCopy {
    pub(super) async fn validate<T: StateTransactionBackend, CS: StateStoreBackend>(
        &self,
        transaction: &mut T,
        store: &AppStore<CS>,
    ) -> Result<(), StorageFailure> {
        for (key, revision) in &self.fences {
            match revision {
                Some(revision) => key.expect_revision(transaction, *revision).await?,
                None => key.expect_missing(transaction).await?,
            }
        }
        let mut observed_pages = Vec::new();
        if let Some((record, revision)) = &self.source {
            let stored = RecordCodec::run(record).map_err(StorageFailure::permanent)?;
            let head = stored.load_head(transaction, *revision).await?;
            head.ensure_readable().map_err(StorageFailure::permanent)?;
            if record.checkpoint.sequence > head.sequence || !record.checkpoint.is_copy_clean() {
                return Err(StorageFailure::permanent(AppError::Conflict(
                    "copy source is no longer a clean retained boundary".into(),
                )));
            }
            let latest_key = head
                .checkpoint_key(head.sequence)
                .state_key()
                .map_err(StorageFailure::permanent)?;
            let latest = transaction
                .load(latest_key.clone())
                .await
                .map_err(StorageFailure::from)?
                .ok_or_else(|| {
                    StorageFailure::permanent(AppError::Integration(
                        "copy source head checkpoint is missing".into(),
                    ))
                })?;
            RecordKey::validate_value(latest_key.key(), &latest.record.value)
                .map_err(StorageFailure::permanent)?;
            let latest = store
                .decode_run(transaction, latest, false, &mut observed_pages)
                .await?;
            head.validate_row(&latest)
                .map_err(StorageFailure::permanent)?;
            if !record.same_run(&latest) {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "copy source has foreign retained identity".into(),
                )));
            }
            stored
                .expect_value(transaction, store, &mut observed_pages)
                .await?;
            if let Some(saved) = &self.saved {
                if !saved.same_run(record) || saved.checkpoint.sequence > record.checkpoint.sequence
                {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "copy saved activation proof has foreign identity".into(),
                    )));
                }
                RecordCodec::run(saved)
                    .map_err(StorageFailure::permanent)?
                    .expect_value(transaction, store, &mut observed_pages)
                    .await?;
            }
        }
        Ok(())
    }
}

impl<CS: StateStoreBackend> AppStore<CS> {
    pub(crate) async fn copy_source(
        &self,
        workspace: &WorkspaceId,
        workflow: &WorkflowId,
        source: &crate::workflow::RunSelector,
        selection: (
            &crate::workflow::CheckpointSelector,
            &crate::workflow::ResumePosition,
        ),
        definition: impl FnOnce(
            &RunRecord,
        ) -> Result<
            std::sync::Arc<crate::workflow::definition::CompiledDefinition>,
            AppError,
        >,
    ) -> Result<RunCopy, RuntimeOperationFailure> {
        use crate::workflow::{CheckpointSelector, RunSelector};
        let (selector, position) = selection;
        source.validate()?;
        let mut captured = RunCopy {
            source: None,
            saved: None,
            fences: Vec::new(),
        };
        let (head, revision, selected_sequence) = match source {
            RunSelector::Exact { id, occurrence } => {
                let key = RunKey::new(
                    workspace.clone(),
                    workflow.clone(),
                    id.clone(),
                    occurrence.clone(),
                );
                let exact = RecordKey::exact_run(&key);
                let Some((head, revision)) = self.load_head(exact.clone()).await? else {
                    captured.fences.push((exact, None));
                    return Ok(captured);
                };
                head.ensure_readable()?;
                let sequence = match selector {
                    CheckpointSelector::Sequence(sequence) => {
                        (*sequence <= head.sequence).then_some(*sequence)
                    }
                    CheckpointSelector::Latest | CheckpointSelector::Action(_) => None,
                };
                (head, revision, sequence)
            }
            RunSelector::Latest(id) => {
                if matches!(selector, CheckpointSelector::Latest) {
                    let unresolved = RunProjection::Unsettled.tail_key(workspace, workflow, id);
                    let current: Option<(RunIndex, u64)> =
                        self.load_record(unresolved.clone()).await?;
                    if current.is_some() {
                        return Err(AppError::Conflict(
                            "logical copy source has unsettled occurrences".into(),
                        )
                        .into());
                    }
                    captured.fences.push((unresolved, None));
                }
                let projection = match selector {
                    CheckpointSelector::Latest => RunProjection::Saved,
                    CheckpointSelector::Sequence(sequence) => RunProjection::Sequence(*sequence),
                    CheckpointSelector::Action(ordinal) => RunProjection::Action(*ordinal),
                };
                let tail = projection.tail_key(workspace, workflow, id);
                let saved: Option<(RunIndex, u64)> = self.load_record(tail.clone()).await?;
                let (last, tail_revision) = match saved {
                    Some((RunIndex::Tail { last, .. }, revision)) => (last, revision),
                    Some(_) => {
                        return Err(StorageFailure::permanent(AppError::Integration(
                            "copy source tail is corrupt".into(),
                        ))
                        .into());
                    }
                    None => {
                        captured.fences.push((tail, None));
                        return Ok(captured);
                    }
                };
                captured.fences.push((tail, Some(tail_revision)));
                let key = RunKey::new(
                    workspace.clone(),
                    workflow.clone(),
                    id.clone(),
                    last.occurrence,
                );
                let Some((head, revision)) = self.load_head(RecordKey::exact_run(&key)).await?
                else {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "copy source observation has no retained occurrence".into(),
                    ))
                    .into());
                };
                head.ensure_readable()?;
                if last.sequence > head.sequence {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "copy source is beyond its retained head".into(),
                    ))
                    .into());
                }
                (head, revision, Some(last.sequence))
            }
        };
        let latest = self.load_checkpoint(&head, head.sequence).await?;
        head.validate_row(&latest)
            .map_err(StorageFailure::permanent)?;
        let definition = definition(&latest)?;
        latest
            .validate_recovery(&definition)
            .map_err(StorageFailure::permanent)?;
        if matches!(selector, CheckpointSelector::Latest) && !latest.checkpoint.is_copy_clean() {
            return Err(
                AppError::Conflict("copy source has unsettled execution work".into()).into(),
            );
        }
        let selected = match (source, selector) {
            (RunSelector::Exact { .. }, CheckpointSelector::Latest) => {
                // The existing committed Action index excludes inherited initial
                // positions; unresolved Failed observations own their exact rows.
                let mut saved = match latest.checkpoint.committed_ordinal() {
                    Some(ordinal) => {
                        self.action_checkpoint(&head, &latest, ordinal, &definition)
                            .await?
                    }
                    None => None,
                };
                if let Some((ordinal, _)) = head
                    .unresolved_failures
                    .iter()
                    .max_by_key(|(_, sequence)| *sequence)
                {
                    let failed = self
                        .action_checkpoint(&head, &latest, *ordinal, &definition)
                        .await?;
                    if failed.as_ref().is_some_and(|record| {
                        saved.as_ref().is_none_or(|saved| {
                            record.checkpoint.sequence > saved.checkpoint.sequence
                        })
                    }) {
                        saved = failed;
                    }
                }
                saved
            }
            (RunSelector::Exact { .. }, CheckpointSelector::Action(ordinal)) => {
                self.action_checkpoint(&head, &latest, *ordinal, &definition)
                    .await?
            }
            _ => match selected_sequence {
                Some(sequence) if sequence == head.sequence => Some(latest.clone()),
                Some(sequence) => Some(self.load_checkpoint(&head, sequence).await?),
                None => None,
            },
        };
        if let Some(record) = selected {
            if !record.same_run(&latest) {
                return Err(StorageFailure::permanent(AppError::Integration(
                    "copy source has foreign retained identity".into(),
                ))
                .into());
            }
            record
                .validate_recovery(&definition)
                .map_err(StorageFailure::permanent)?;
            if !record.checkpoint.is_copy_clean() {
                return Err(
                    AppError::Conflict("copy source has unsettled execution work".into()).into(),
                );
            }
            match selector {
                CheckpointSelector::Latest
                    if !matches!(
                        record.checkpoint_kind(),
                        CheckpointKind::Action | CheckpointKind::Failed { .. }
                    ) =>
                {
                    return Err(StorageFailure::permanent(AppError::Integration(
                        "copy latest source is not a saved action observation".into(),
                    ))
                    .into());
                }
                CheckpointSelector::Action(ordinal) if matches!(source, RunSelector::Latest(_)) => {
                    // Aggregate lookup still validates the exact occurrence's
                    // required local index; missing evidence is not absence.
                    let local = self
                        .action_checkpoint(&head, &latest, *ordinal, &definition)
                        .await?;
                    if local
                        .as_ref()
                        .is_none_or(|local| local.checkpoint.sequence != record.checkpoint.sequence)
                    {
                        return Err(StorageFailure::permanent(AppError::Integration(
                            "copy action source has foreign ordinal".into(),
                        ))
                        .into());
                    }
                }
                _ => {}
            }
            if !matches!(position, crate::workflow::ResumePosition::Entrypoint)
                && record.checkpoint.failed_observation().is_none()
            {
                captured.saved = if let Some(action) = record.saved_action() {
                    if action.sequence == record.checkpoint.sequence {
                        None
                    } else {
                        let mut saved = record.clone();
                        saved.checkpoint = action.clone();
                        saved.previous = None;
                        Some(saved)
                    }
                } else if record.checkpoint.sequence > 0 {
                    match record.checkpoint.committed_ordinal() {
                        Some(ordinal) => {
                            let saved = match self
                                .action_checkpoint(&head, &latest, ordinal, &definition)
                                .await?
                            {
                                Some(saved) => saved,
                                None => self.load_checkpoint(&head, 0).await?,
                            };
                            if !saved.same_run(&record)
                                || saved.checkpoint.sequence > record.checkpoint.sequence
                            {
                                return Err(StorageFailure::permanent(AppError::Integration(
                                    "copy saved activation proof is inconsistent".into(),
                                ))
                                .into());
                            }
                            saved
                                .validate_recovery(&definition)
                                .map_err(StorageFailure::permanent)?;
                            Some(saved)
                        }
                        None => None,
                    }
                } else {
                    None
                };
            }
            captured.source = Some((record, revision));
        } else {
            captured.fences.push((head.head_key(), Some(revision)));
        }
        self.validate_history_head(&head).await?;
        Ok(captured)
    }
}
