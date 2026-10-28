use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use agents_kit::identity::NotificationId;
use agents_kit::notification::{
    ConversationNotification, ConversationNotificationKind, NotificationResolution,
};
use serde::de::{Error, IgnoredAny, SeqAccess, Visitor, value::SeqAccessDeserializer};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

use crate::AppError;
use crate::storage::RecordCodec;
use crate::workflow::Resolution;
use crate::workflow::run::{
    ActionPosition, AttemptPhaseRecord, CheckpointPhaseRecord, CheckpointRecord, ControlRecord,
    ControlRecordState, FailureCategory, MAX_FRONTIER, ReconciliationRecord,
    StagedActivationRecord, StagedOutcomeRecord,
};

pub(crate) const MAX_INTERACTION_SETTLEMENTS: usize = MAX_FRONTIER;

#[derive(Clone, PartialEq)]
pub(crate) struct InteractionSettlement {
    pub(super) sequence: u64,
    pub(super) generation: u64,
    pub(super) control_sequence: u64,
    pub(super) target: SettlementTarget,
    pub(super) state: SettlementState,
}

#[derive(Clone, PartialEq)]
pub(super) enum SettlementTarget {
    Input,
    Notification {
        attempt_number: u32,
        notification: ConversationNotification,
    },
    Unknown {
        attempt_number: u32,
    },
    Inputs,
}

#[derive(Clone, PartialEq)]
pub(super) enum SettlementState {
    Pending,
    Intended {
        decision: SettlementDecision,
    },
    Applied {
        decision: SettlementDecision,
        checkpoint: u64,
    },
    Uncertain {
        decision: SettlementDecision,
    },
}

#[derive(Clone, PartialEq)]
pub(super) enum SettlementDecision {
    Input {
        value: Value,
    },
    Allow,
    Deny {
        reason: String,
    },
    Submit {
        content: Value,
    },
    Dismiss {
        reason: String,
    },
    Accept {
        contribution: Map<String, Value>,
    },
    Fail,
    Retry,
    Inputs {
        keys: Vec<String>,
        app_generation: u64,
        execution_generation: u64,
    },
}

impl Serialize for InteractionSettlement {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (
            &self.sequence,
            &self.generation,
            &self.control_sequence,
            &self.target,
            &self.state,
        )
            .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for InteractionSettlement {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (sequence, generation, control_sequence, target, state) =
            Deserialize::deserialize(deserializer)?;
        Ok(Self {
            sequence,
            generation,
            control_sequence,
            target,
            state,
        })
    }
}

impl Serialize for SettlementTarget {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Input => (0u8,).serialize(serializer),
            Self::Notification {
                attempt_number,
                notification,
            } => {
                let ConversationNotificationKind::Interaction {
                    title,
                    body,
                    response_schema,
                    dismissible,
                } = notification.kind()
                else {
                    return Err(<S::Error as serde::ser::Error>::custom(
                        "notification settlement requires an actual interaction",
                    ));
                };
                let time = notification
                    .inserted_at()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| {
                        <S::Error as serde::ser::Error>::custom(
                            "SystemTime must be later than UNIX_EPOCH",
                        )
                    })?;
                (
                    1u8,
                    attempt_number,
                    notification.id(),
                    (time.as_secs(), time.subsec_nanos()),
                    title,
                    body,
                    response_schema,
                    dismissible,
                )
                    .serialize(serializer)
            }
            Self::Unknown { attempt_number } => (2u8, attempt_number).serialize(serializer),
            Self::Inputs => (3u8,).serialize(serializer),
        }
    }
}
impl<'de> Deserialize<'de> for SettlementTarget {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(Self::Input)
    }
}
impl<'de> Visitor<'de> for SettlementTarget {
    type Value = Self;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("positional settlement target")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self, A::Error> {
        let tag = seq
            .next_element::<u8>()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let result = match tag {
            0 => Self::Input,
            1 => {
                let (
                    attempt_number,
                    notification_id,
                    (secs, nanos),
                    title,
                    body,
                    response_schema,
                    dismissible,
                ): (
                    u32,
                    NotificationId,
                    (u64, u32),
                    String,
                    String,
                    Option<Value>,
                    bool,
                ) = Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                // Same overflow, normalization and platform bound as pinned Serde SystemTime.
                secs.checked_add(u64::from(nanos / 1_000_000_000))
                    .ok_or_else(|| {
                        A::Error::custom("overflow deserializing SystemTime epoch offset")
                    })?;
                let inserted_at = UNIX_EPOCH
                    .checked_add(Duration::new(secs, nanos))
                    .ok_or_else(|| A::Error::custom("overflow deserializing SystemTime"))?;
                Self::Notification {
                    attempt_number,
                    notification: ConversationNotification::new(
                        notification_id,
                        inserted_at,
                        ConversationNotificationKind::Interaction {
                            title,
                            body,
                            response_schema,
                            dismissible,
                        },
                    )
                    .map_err(|_| {
                        A::Error::custom("durable notification interaction evidence is invalid")
                    })?,
                }
            }
            2 => {
                let (attempt_number,) =
                    Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Unknown { attempt_number }
            }
            3 => Self::Inputs,
            _ => return Err(A::Error::custom("unknown settlement target tag")),
        };
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("extra settlement target field"));
        }
        Ok(result)
    }
}

impl Serialize for SettlementState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Pending => (0u8,).serialize(serializer),
            Self::Intended { decision } => (1u8, decision).serialize(serializer),
            Self::Applied {
                decision,
                checkpoint,
            } => (2u8, decision, checkpoint).serialize(serializer),
            Self::Uncertain { decision } => (3u8, decision).serialize(serializer),
        }
    }
}
impl<'de> Deserialize<'de> for SettlementState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(Self::Pending)
    }
}
impl<'de> Visitor<'de> for SettlementState {
    type Value = Self;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("positional settlement state")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self, A::Error> {
        let tag = seq
            .next_element::<u8>()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let result = match tag {
            0 => Self::Pending,
            1 => {
                let (decision,) = Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Intended { decision }
            }
            2 => {
                let (decision, checkpoint) =
                    Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Applied {
                    decision,
                    checkpoint,
                }
            }
            3 => {
                let (decision,) = Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Uncertain { decision }
            }
            _ => return Err(A::Error::custom("unknown settlement state tag")),
        };
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("extra settlement state field"));
        }
        Ok(result)
    }
}

impl Serialize for SettlementDecision {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Input { value } => (0u8, value).serialize(serializer),
            Self::Allow => (1u8,).serialize(serializer),
            Self::Deny { reason } => (2u8, reason).serialize(serializer),
            Self::Submit { content } => (3u8, content).serialize(serializer),
            Self::Dismiss { reason } => (4u8, reason).serialize(serializer),
            Self::Accept { contribution } => (5u8, contribution).serialize(serializer),
            Self::Fail => (6u8,).serialize(serializer),
            Self::Retry => (7u8,).serialize(serializer),
            Self::Inputs {
                keys,
                app_generation,
                execution_generation,
            } => (8u8, keys, app_generation, execution_generation).serialize(serializer),
        }
    }
}
impl<'de> Deserialize<'de> for SettlementDecision {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(Self::Allow)
    }
}
impl<'de> Visitor<'de> for SettlementDecision {
    type Value = Self;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("positional settlement decision")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self, A::Error> {
        let tag = seq
            .next_element::<u8>()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let result = match tag {
            0 => {
                let (value,) = Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Input { value }
            }
            1 => Self::Allow,
            2 => {
                let (reason,) = Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Deny { reason }
            }
            3 => {
                let (content,) = Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Submit { content }
            }
            4 => {
                let (reason,) = Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Dismiss { reason }
            }
            5 => {
                let (contribution,) =
                    Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Accept { contribution }
            }
            6 => Self::Fail,
            7 => Self::Retry,
            8 => {
                let (keys, app_generation, execution_generation) =
                    Deserialize::deserialize(SeqAccessDeserializer::new(&mut seq))?;
                Self::Inputs {
                    keys,
                    app_generation,
                    execution_generation,
                }
            }
            _ => return Err(A::Error::custom("unknown settlement decision tag")),
        };
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("extra settlement decision field"));
        }
        Ok(result)
    }
}

impl From<&NotificationResolution> for SettlementDecision {
    fn from(resolution: &NotificationResolution) -> Self {
        match resolution {
            NotificationResolution::Allow => Self::Allow,
            NotificationResolution::Deny { reason } => Self::Deny {
                reason: reason.clone(),
            },
            NotificationResolution::Submit { content } => Self::Submit {
                content: content.clone(),
            },
            NotificationResolution::Dismiss { reason } => Self::Dismiss {
                reason: reason.clone(),
            },
        }
    }
}

impl SettlementDecision {
    pub(super) fn matches_resolution(&self, resolution: &Resolution) -> bool {
        match (self, resolution) {
            (Self::Input { value }, Resolution::Input(other)) => value == other,
            (Self::Allow, Resolution::Notification(NotificationResolution::Allow)) => true,
            (
                Self::Deny { reason },
                Resolution::Notification(NotificationResolution::Deny { reason: other }),
            )
            | (
                Self::Dismiss { reason },
                Resolution::Notification(NotificationResolution::Dismiss { reason: other }),
            ) => reason == other,
            (
                Self::Submit { content },
                Resolution::Notification(NotificationResolution::Submit { content: other }),
            ) => content == other,
            (Self::Accept { contribution }, Resolution::Accept(other)) => contribution == other,
            (Self::Fail, Resolution::Fail) | (Self::Retry, Resolution::Retry) => true,
            // Inputs values are compared only by the retained execution-options owner.
            _ => false,
        }
    }
}

impl SettlementTarget {
    pub(super) fn from_notification(
        control: &ControlRecord,
        notification: &ConversationNotification,
    ) -> Result<Self, AppError> {
        if !matches!(
            notification.kind(),
            ConversationNotificationKind::Interaction { .. }
        ) {
            return Err(AppError::Integration(
                "notification settlement requires an actual interaction".into(),
            ));
        }
        Ok(Self::Notification {
            attempt_number: control.attempt_number()?,
            notification: notification.clone(),
        })
    }

    pub(super) fn notification(&self) -> Result<ConversationNotification, AppError> {
        let Self::Notification { notification, .. } = self else {
            return Err(AppError::Integration(
                "interaction target has no notification evidence".into(),
            ));
        };
        if !matches!(
            notification.kind(),
            ConversationNotificationKind::Interaction { .. }
        ) {
            return Err(AppError::Integration(
                "durable notification interaction evidence is invalid".into(),
            ));
        }
        Ok(notification.clone())
    }

    fn owns(&self, control: &ControlRecord) -> bool {
        match (self, &control.state) {
            (Self::Input, ControlRecordState::Input { .. })
            | (Self::Inputs, ControlRecordState::Inputs { .. }) => true,
            (
                Self::Notification { attempt_number, .. } | Self::Unknown { attempt_number },
                ControlRecordState::Attempt(attempt),
            ) => *attempt_number == attempt.number,
            _ => false,
        }
    }

    fn accepts(&self, decision: &SettlementDecision) -> bool {
        match (self, decision) {
            (Self::Input, SettlementDecision::Input { .. })
            | (
                Self::Notification { .. },
                SettlementDecision::Allow
                | SettlementDecision::Deny { .. }
                | SettlementDecision::Submit { .. }
                | SettlementDecision::Dismiss { .. },
            )
            | (
                Self::Unknown { .. },
                SettlementDecision::Accept { .. }
                | SettlementDecision::Fail
                | SettlementDecision::Retry,
            ) => true,
            (
                Self::Inputs,
                SettlementDecision::Inputs {
                    keys,
                    app_generation,
                    execution_generation,
                },
            ) => {
                *app_generation > 0
                    && *execution_generation > 0
                    && !keys.is_empty()
                    && keys.iter().all(|key| !key.is_empty())
                    && keys.windows(2).all(|pair| pair[0] < pair[1])
            }
            _ => false,
        }
    }
}

impl InteractionSettlement {
    pub(super) fn pending(
        checkpoint: &CheckpointRecord,
        control: &ControlRecord,
        target: SettlementTarget,
    ) -> Result<Self, AppError> {
        if checkpoint.settlements.len() >= MAX_INTERACTION_SETTLEMENTS {
            return Err(AppError::InvalidInput(
                "workflow interaction settlement limit exceeded".into(),
            ));
        }
        if !target.owns(control) {
            return Err(AppError::Conflict(
                "interaction target does not own its control".into(),
            ));
        }
        let generation = checkpoint.next_control()?;
        Ok(Self {
            sequence: if matches!(target, SettlementTarget::Notification { .. }) {
                generation
            } else {
                control.sequence
            },
            generation,
            control_sequence: control.sequence,
            target,
            state: SettlementState::Pending,
        })
    }

    pub(super) fn replacement_unknown(
        &self,
        checkpoint: &CheckpointRecord,
        control: &ControlRecord,
    ) -> Result<Self, AppError> {
        if !self.owns(control) || matches!(self.state, SettlementState::Applied { .. }) {
            return Err(AppError::Conflict(
                "unknown replacement does not own its prior target".into(),
            ));
        }
        let mut replacement = Self::pending(
            checkpoint,
            control,
            SettlementTarget::Unknown {
                attempt_number: control.attempt_number()?,
            },
        )?;
        replacement.sequence = self.sequence;
        Ok(replacement)
    }

    pub(super) fn owns(&self, control: &ControlRecord) -> bool {
        self.control_sequence == control.sequence && self.target.owns(control)
    }

    fn replaces(&self, previous: &Self) -> bool {
        self.sequence == previous.sequence
            && self.control_sequence == previous.control_sequence
            && self.generation > previous.generation
            && match (&previous.target, &self.target) {
                (
                    SettlementTarget::Notification {
                        attempt_number: before,
                        ..
                    }
                    | SettlementTarget::Unknown {
                        attempt_number: before,
                    },
                    SettlementTarget::Unknown {
                        attempt_number: after,
                    },
                ) => before == after,
                (SettlementTarget::Inputs, SettlementTarget::Inputs) => true,
                _ => false,
            }
    }

    pub(super) fn same_target(&self, other: &Self) -> bool {
        self.sequence == other.sequence
            && self.generation == other.generation
            && self.control_sequence == other.control_sequence
            && self.target == other.target
    }

    pub(super) fn allows(&self, next: &Self) -> bool {
        if !self.same_target(next) {
            return false;
        }
        if self.state == next.state {
            return true;
        }
        match (&self.state, &next.state) {
            (SettlementState::Pending, SettlementState::Intended { decision }) => {
                matches!(self.target, SettlementTarget::Notification { .. })
                    && self.target.accepts(decision)
            }
            (SettlementState::Pending, SettlementState::Applied { decision, .. }) => {
                !matches!(self.target, SettlementTarget::Notification { .. })
                    && self.target.accepts(decision)
            }
            (
                SettlementState::Intended { decision: before },
                SettlementState::Applied {
                    decision: after, ..
                }
                | SettlementState::Uncertain { decision: after },
            )
            | (
                SettlementState::Uncertain { decision: before },
                SettlementState::Applied {
                    decision: after, ..
                },
            ) => before == after,
            _ => false,
        }
    }

    pub(super) fn intend(&mut self, decision: SettlementDecision) -> Result<(), AppError> {
        if !matches!(self.state, SettlementState::Pending)
            || !matches!(self.target, SettlementTarget::Notification { .. })
            || !self.target.accepts(&decision)
        {
            return Err(AppError::Conflict(
                "interaction target cannot accept the intended decision".into(),
            ));
        }
        self.state = SettlementState::Intended { decision };
        Ok(())
    }

    pub(super) fn settle(
        &mut self,
        applied: bool,
        checkpoint: &CheckpointRecord,
    ) -> Result<(), AppError> {
        let SettlementState::Intended { decision } = &self.state else {
            return Err(AppError::Conflict(
                "interaction target has no intended decision".into(),
            ));
        };
        self.state = if applied {
            SettlementState::Applied {
                decision: decision.clone(),
                checkpoint: checkpoint.next_control()?,
            }
        } else {
            SettlementState::Uncertain {
                decision: decision.clone(),
            }
        };
        Ok(())
    }

    pub(super) fn apply(
        &mut self,
        decision: SettlementDecision,
        checkpoint: &CheckpointRecord,
    ) -> Result<(), AppError> {
        if !matches!(self.state, SettlementState::Pending)
            || matches!(self.target, SettlementTarget::Notification { .. })
            || !self.target.accepts(&decision)
        {
            return Err(AppError::Conflict(
                "interaction target cannot apply this decision".into(),
            ));
        }
        self.state = SettlementState::Applied {
            decision,
            checkpoint: checkpoint.next_control()?,
        };
        Ok(())
    }
}

impl CheckpointRecord {
    pub(crate) fn prospective_settlement_bytes(&self) -> Result<usize, AppError> {
        let mut rows = self.settlements.len();
        let mut bytes = 0usize;
        let mut has_attempt = false;
        let mut largest_node_bytes = 0;
        let (category, message) = match &self.phase {
            CheckpointPhaseRecord::Failing {
                category, message, ..
            }
            | CheckpointPhaseRecord::Failed {
                category, message, ..
            } => (*category, message.as_str()),
            _ => (
                FailureCategory::Agent,
                "caller failed an external effect with unknown outcome",
            ),
        };
        let message_bytes = RecordCodec::encoded_len(&message)?.saturating_sub(2);
        let failed_stage =
            StagedActivationRecord::failed(String::new(), String::new(), category, String::new());
        let failed_stage_bytes =
            RecordCodec::encoded_len(&failed_stage)?.saturating_add(message_bytes);
        let mut staged_count = self.staged.len();
        for control in self.controls.values() {
            let ControlRecordState::Attempt(attempt) = &control.state else {
                continue;
            };
            has_attempt = true;
            let node_bytes = RecordCodec::encoded_len(&control.node)?;
            if node_bytes > largest_node_bytes {
                largest_node_bytes = node_bytes;
            }
            let current = self.current_settlement(control);
            if !current
                .is_some_and(|target| matches!(target.target, SettlementTarget::Unknown { .. }))
            {
                let next_sequence = if control.is_retry() {
                    u64::MAX
                } else {
                    control.sequence
                };
                let replacement = InteractionSettlement {
                    sequence: current
                        .filter(|target| !matches!(target.state, SettlementState::Applied { .. }))
                        .map_or(next_sequence, |target| target.sequence),
                    generation: u64::MAX,
                    control_sequence: next_sequence,
                    target: SettlementTarget::Unknown {
                        attempt_number: attempt.number,
                    },
                    state: SettlementState::Applied {
                        decision: SettlementDecision::Fail,
                        checkpoint: u64::MAX,
                    },
                };
                bytes = bytes.saturating_add(RecordCodec::encoded_len(&replacement)?);
                if rows != 0 {
                    bytes = bytes.saturating_add(1);
                }
                rows = rows.saturating_add(1);
            }
            if control.is_retry() {
                // RetryReady changes both the map key and its control sequence.
                let width = RecordCodec::encoded_len(&u64::MAX)?
                    .saturating_sub(RecordCodec::encoded_len(&control.sequence)?);
                bytes = bytes.saturating_add(width.saturating_mul(2));
            }
            let mut unknown = attempt.clone();
            unknown.phase = AttemptPhaseRecord::Unknown {
                notification_id: control.notification_id().ok().cloned(),
            };
            // Intended is the longest abort state, including the transition
            // immediately before either confirmed or uncertain abort evidence.
            unknown.abort = ReconciliationRecord::Intended;
            let unknown = ControlRecordState::Attempt(unknown);
            bytes = bytes.saturating_add(
                RecordCodec::encoded_len(&unknown)?
                    .saturating_sub(RecordCodec::encoded_len(&control.state)?),
            );
            let settled_control = ControlRecord {
                sequence: control.sequence,
                node: String::new(),
                lineage: String::new(),
                deadline_millis: control.deadline_millis,
                state: unknown,
            };
            // Unknown Fail substitutes staging for this map entry. Node and
            // lineage bytes occur once in either shape and cancel exactly.
            let removed_bytes = RecordCodec::encoded_len(&settled_control)?
                .saturating_add(RecordCodec::encoded_len(&control.sequence)?)
                .saturating_add(3);
            bytes = bytes.saturating_add(
                failed_stage_bytes
                    .saturating_add(usize::from(staged_count != 0))
                    .saturating_sub(removed_bytes),
            );
            staged_count = staged_count.saturating_add(1);
        }
        if rows > MAX_INTERACTION_SETTLEMENTS {
            return Err(AppError::InvalidInput(
                "workflow interaction settlement limit exceeded".into(),
            ));
        }
        for target in self.settlements.iter() {
            let decision = match target.target {
                SettlementTarget::Notification { .. } => SettlementDecision::Allow,
                SettlementTarget::Unknown { .. } => SettlementDecision::Fail,
                SettlementTarget::Input | SettlementTarget::Inputs => continue,
            };
            let state = match &target.state {
                SettlementState::Applied { .. } => continue,
                SettlementState::Pending => {
                    if self
                        .control(target.control_sequence)
                        .and_then(|control| self.current_settlement(control))
                        != Some(target)
                    {
                        continue;
                    }
                    SettlementState::Pending
                }
                SettlementState::Intended { .. } => SettlementState::Intended {
                    decision: decision.clone(),
                },
                SettlementState::Uncertain { .. } => SettlementState::Uncertain {
                    decision: decision.clone(),
                },
            };
            // An accepted historical decision may still return after its control
            // disappears. Its payload is unchanged; only state/checkpoint syntax grows.
            let applied = SettlementState::Applied {
                decision,
                checkpoint: u64::MAX,
            };
            bytes = bytes.saturating_add(
                RecordCodec::encoded_len(&applied)?
                    .saturating_sub(RecordCodec::encoded_len(&state)?),
            );
        }
        let current_phase_bytes = RecordCodec::encoded_len(&self.phase)?;
        let mut future_phase_bytes = current_phase_bytes;
        if !self.is_terminal() {
            let phase_bytes =
                |category, message: &str, node_bytes: Option<usize>| -> Result<usize, AppError> {
                    let failure = CheckpointPhaseRecord::Failing {
                        category,
                        message: String::new(),
                        observation: node_bytes.map(|_| ActionPosition {
                            node: String::new(),
                            ordinal: u64::MAX,
                        }),
                        cancellation_checkpoint: Some(u64::MAX),
                    };
                    Ok(RecordCodec::encoded_len(&failure)?
                        .saturating_add(RecordCodec::encoded_len(&message)?.saturating_sub(2))
                        .saturating_add(node_bytes.map_or(0, |bytes| bytes.saturating_sub(2))))
                };
            if has_attempt {
                future_phase_bytes = future_phase_bytes.max(phase_bytes(
                    category,
                    message,
                    Some(largest_node_bytes),
                )?);
            }
            if let CheckpointPhaseRecord::Failing {
                category,
                message,
                observation,
                ..
            } = &self.phase
            {
                let node_bytes = observation
                    .as_ref()
                    .map(|position| RecordCodec::encoded_len(&position.node))
                    .transpose()?;
                future_phase_bytes =
                    future_phase_bytes.max(phase_bytes(*category, message, node_bytes)?);
            }
            // Unknown Fail removes its Attempt before publishing the failure.
            // Existing failed staging retains that same phase headroom meanwhile.
            for activation in &self.staged {
                if let StagedOutcomeRecord::Failed { category, message } = &activation.outcome {
                    future_phase_bytes = future_phase_bytes.max(phase_bytes(
                        *category,
                        message,
                        Some(RecordCodec::encoded_len(&activation.node)?),
                    )?);
                }
            }
        }
        bytes = bytes.saturating_add(future_phase_bytes.saturating_sub(current_phase_bytes));
        if bytes != 0 {
            bytes = bytes.saturating_add(
                RecordCodec::encoded_len(&u64::MAX)?
                    .saturating_sub(RecordCodec::encoded_len(&self.sequence)?),
            );
        }
        Ok(bytes)
    }

    pub(super) fn settlement(
        &self,
        sequence: u64,
        generation: u64,
    ) -> Option<&InteractionSettlement> {
        self.settlements
            .iter()
            .rev()
            .find(|target| target.sequence == sequence && target.generation == generation)
    }

    pub(super) fn current_settlement(
        &self,
        control: &ControlRecord,
    ) -> Option<&InteractionSettlement> {
        self.settlements.iter().rev().find(|target| {
            target.owns(control) && match (&target.target, &control.state) {
                (SettlementTarget::Input, ControlRecordState::Input { .. })
                | (SettlementTarget::Inputs, ControlRecordState::Inputs { .. }) => true,
                (SettlementTarget::Notification { notification, .. }, ControlRecordState::Attempt(attempt)) => matches!(&attempt.phase, AttemptPhaseRecord::Waiting { notification_id: id } if id == notification.id()),
                (SettlementTarget::Unknown { .. }, ControlRecordState::Attempt(attempt)) => matches!(attempt.phase, AttemptPhaseRecord::Unknown { .. }),
                _ => false,
            }
        })
    }

    pub(super) fn notification_target(
        &self,
        control: &ControlRecord,
        id: &NotificationId,
    ) -> Option<&InteractionSettlement> {
        self.settlements.iter().rev().find(|target| {
            target.owns(control) && matches!(&target.target, SettlementTarget::Notification { notification, .. } if notification.id() == id)
        })
    }

    pub(super) fn settlement_transition_allowed(
        &self,
        owner: &ControlRecord,
        next: &InteractionSettlement,
    ) -> bool {
        if !next.owns(owner) {
            return false;
        }
        let current = self.settlement(next.sequence, next.generation);
        let owns_current = self.control(owner.sequence).is_some_and(|control| {
            control.same_owner(owner)
                && next.owns(control)
                && (!matches!(owner.state, ControlRecordState::Attempt(_))
                    || control.same_attempt(owner))
        });
        if !owns_current {
            // An already accepted lower receipt can settle its old evidence after
            // explicit Unknown settlement removed or replaced the native control.
            return current.is_some_and(|prior| {
                prior.allows(next)
                    && matches!(
                        (&prior.state, &next.state),
                        (
                            SettlementState::Intended { .. } | SettlementState::Uncertain { .. },
                            SettlementState::Applied { .. } | SettlementState::Uncertain { .. }
                        )
                    )
            });
        }
        match current {
            Some(current) => current.allows(next),
            None => self.can_reserve_settlement(next),
        }
    }

    fn can_reserve_settlement(&self, next: &InteractionSettlement) -> bool {
        if self.settlements.len() >= MAX_INTERACTION_SETTLEMENTS
            || !matches!(next.state, SettlementState::Pending)
            || !self
                .next_control()
                .is_ok_and(|generation| generation == next.generation)
        {
            return false;
        }
        if let Some(previous) = self
            .settlements
            .iter()
            .rev()
            .find(|target| target.sequence == next.sequence)
        {
            return next.replaces(previous)
                && (matches!(next.target, SettlementTarget::Inputs)
                    || !matches!(previous.state, SettlementState::Applied { .. }));
        }
        match &next.target {
            SettlementTarget::Notification { attempt_number, notification } => {
                next.sequence == next.generation && next.control_sequence < next.sequence
                    && !self.settlements.iter().any(|previous| {
                        previous.control_sequence == next.control_sequence
                            && matches!(&previous.target, SettlementTarget::Notification { attempt_number: prior_number, notification: prior, .. } if prior_number == attempt_number && prior.id() == notification.id())
                    })
            }
            _ => next.sequence == next.control_sequence && next.control_sequence <= next.generation,
        }
    }

    pub(super) fn apply_settlement(
        &mut self,
        mut next: InteractionSettlement,
        applied_checkpoint: u64,
    ) -> Result<(), AppError> {
        if applied_checkpoint != self.next_control()? {
            return Err(AppError::Conflict(
                "interaction application checkpoint is stale".into(),
            ));
        }
        if let Some(index) = self.settlements.iter().rposition(|target| {
            target.sequence == next.sequence && target.generation == next.generation
        }) {
            let current = &self.settlements[index];
            if !matches!(current.state, SettlementState::Applied { .. })
                && let SettlementState::Applied { checkpoint, .. } = &mut next.state
            {
                *checkpoint = applied_checkpoint;
            }
            if !current.allows(&next) {
                return Err(AppError::Conflict(
                    "interaction settlement evidence conflicted".into(),
                ));
            }
            Arc::make_mut(&mut self.settlements)[index] = next;
        } else {
            if self.settlements.len() >= MAX_INTERACTION_SETTLEMENTS {
                return Err(AppError::InvalidInput(
                    "workflow interaction settlement limit exceeded".into(),
                ));
            }
            if !self.can_reserve_settlement(&next) {
                return Err(AppError::Conflict(
                    "interaction target reservation is stale".into(),
                ));
            }
            Arc::make_mut(&mut self.settlements).push(next);
        }
        Ok(())
    }

    pub(super) fn mark_settlement_uncertain(&mut self, control: &ControlRecord) {
        for index in 0..self.settlements.len() {
            let target = &self.settlements[index];
            if target.owns(control)
                && let SettlementState::Intended { decision } = &target.state
            {
                let decision = decision.clone();
                Arc::make_mut(&mut self.settlements)[index].state =
                    SettlementState::Uncertain { decision };
            }
        }
    }

    pub(super) fn validate_settlement_successor(&self, previous: &Self) -> Result<(), AppError> {
        if previous.settlements.len() > self.settlements.len()
            || previous.settlements.iter().zip(self.settlements.iter()).any(|(before, after)| {
                !before.allows(after) || (!matches!(before.state, SettlementState::Applied { .. }) && matches!(after.state, SettlementState::Applied { checkpoint, .. } if checkpoint != self.sequence))
            })
            || self.settlements[previous.settlements.len()..].iter().any(|target| {
                target.generation != self.sequence
                    || !previous.can_reserve_settlement(target)
                    || self.control(target.control_sequence)
                        .and_then(|control| self.current_settlement(control)) != Some(target)
            })
        {
            return Err(AppError::Integration("durable interaction settlement predecessor is inconsistent".into()));
        }
        Ok(())
    }

    pub(crate) fn validate_settlements(&self) -> Result<(), AppError> {
        if self.settlements.len() > MAX_INTERACTION_SETTLEMENTS {
            return Err(AppError::Integration(
                "durable interaction settlement limit exceeded".into(),
            ));
        }
        let mut latest = BTreeMap::<u64, &InteractionSettlement>::new();
        let mut notifications = BTreeSet::new();
        let mut prior_generation = 0;
        for target in self.settlements.iter() {
            let target_valid = match &target.target {
                SettlementTarget::Notification {
                    attempt_number,
                    notification,
                } => {
                    *attempt_number > 0
                        && matches!(
                            notification.kind(),
                            ConversationNotificationKind::Interaction { .. }
                        )
                        && target.control_sequence < target.sequence
                        && notifications.insert((
                            target.control_sequence,
                            *attempt_number,
                            notification.id(),
                        ))
                }
                SettlementTarget::Unknown { attempt_number } => *attempt_number > 0,
                SettlementTarget::Input | SettlementTarget::Inputs => {
                    target.sequence == target.control_sequence
                }
            };
            let state_valid = match &target.state {
                SettlementState::Pending => true,
                SettlementState::Intended { decision }
                | SettlementState::Uncertain { decision } => {
                    matches!(target.target, SettlementTarget::Notification { .. })
                        && target.target.accepts(decision)
                }
                SettlementState::Applied {
                    decision,
                    checkpoint,
                } => {
                    target.target.accepts(decision)
                        && *checkpoint > target.generation
                        && *checkpoint <= self.sequence
                }
            };
            let identity_valid = match latest.insert(target.sequence, target) {
                Some(previous) => target.replaces(previous),
                None => match target.target {
                    SettlementTarget::Notification { .. } => target.sequence == target.generation,
                    _ => target.sequence == target.control_sequence,
                },
            };
            if target.sequence == 0
                || target.control_sequence == 0
                || target.control_sequence > target.sequence
                || target.sequence > target.generation
                || target.generation > self.sequence
                || target.generation < prior_generation
                || !target_valid
                || !state_valid
                || !identity_valid
            {
                return Err(AppError::Integration(
                    "durable interaction target evidence is inconsistent".into(),
                ));
            }
            prior_generation = target.generation;
        }
        for control in self.controls.values() {
            let interactive = matches!(
                &control.state,
                ControlRecordState::Input { .. }
                    | ControlRecordState::Inputs { .. }
                    | ControlRecordState::Attempt(crate::workflow::run::AttemptRecord {
                        phase: AttemptPhaseRecord::Waiting { .. }
                            | AttemptPhaseRecord::Unknown { .. },
                        ..
                    })
            );
            if interactive
                && !self.current_settlement(control).is_some_and(|target| {
                    matches!(
                        target.state,
                        SettlementState::Pending | SettlementState::Intended { .. }
                    )
                })
            {
                return Err(AppError::Integration(
                    "durable interaction has no unsettled target".into(),
                ));
            }
        }
        Ok(())
    }
}
