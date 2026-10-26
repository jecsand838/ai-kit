use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tool_sdk::ToolsetBackend;

use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::backends::custom::notifications::{CustomInteraction, CustomNotifications};
use crate::backends::custom::state_store::CustomStateStore;
use crate::backends::custom::tools::CustomTools;
use crate::core::notification_store::{
    ResolutionDisposition, ResolutionIntent, ResolutionSettlement,
};
use crate::{ConversationId, NotificationId, NotificationResolution, SdkError};
use storage_sdk::{DatabaseBackend, StateStoreBackend};

const MAX_LIVE_ATTACHMENTS: usize = 4_096;

pub(crate) struct CustomServerRequests<S: StateStoreBackend, D: DatabaseBackend, T: ToolsetBackend>
{
    state: CustomStateStore<S, D>,
    notifications: CustomNotifications,
    tools: CustomTools<T>,
    lifecycle_timeout: Duration,
    attachments: Arc<Mutex<CustomHookAttachments>>,
    changed: Arc<Notify>,
}

struct CustomHookAttachments {
    closed: BTreeSet<ConversationId>,
    attachments: BTreeMap<NotificationId, CustomHookAttachment>,
}

struct CustomHookAttachment {
    conversation_id: ConversationId,
    request: CustomRequest,
    continuation: Option<oneshot::Sender<Result<CustomHookDelivery, SdkError>>>,
    state: CustomHookAttachmentState,
}

enum CustomHookAttachmentState {
    Publishing {
        handle: Option<JoinHandle<()>>,
    },
    Pending,
    Resolving {
        handle: Option<JoinHandle<()>>,
    },
    Deferred {
        reconciliation: CustomReconciliation,
    },
    RecoveryRequired,
    Reconciling {
        handle: Option<JoinHandle<()>>,
    },
}

#[derive(Clone)]
enum CustomRequest {
    ToolApproval {
        visible_name: String,
        arguments: serde_json::Value,
    },
    ToolInteraction(tool_sdk::interaction::ToolInteractionRequest),
}

enum CustomHookResponse {
    Approval(CustomApprovalDecision),
    ToolInteraction(tool_sdk::interaction::ToolInteractionResponse),
}

pub(crate) enum CustomApprovalDecision {
    Allow,
    Deny { reason: Option<String> },
}

struct CustomHookDelivery {
    response: CustomHookResponse,
    consumed: oneshot::Sender<()>,
    settled: oneshot::Receiver<()>,
}

struct CustomReconciliation {
    intent: ResolutionIntent,
    settlement: ResolutionSettlement,
    confirmed_ambiguous: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CustomAcceptance {
    Accepted,
    Ambiguous,
    Cancelled,
}

enum CustomCloseStep {
    Reconcile {
        notification_id: NotificationId,
        reconciliation: CustomReconciliation,
    },
    Ambiguous,
    Failed,
    Wait,
    Ready,
}

impl<S: StateStoreBackend, D: DatabaseBackend, T: ToolsetBackend> Clone
    for CustomServerRequests<S, D, T>
{
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            notifications: self.notifications,
            tools: self.tools.clone(),
            lifecycle_timeout: self.lifecycle_timeout,
            attachments: Arc::clone(&self.attachments),
            changed: Arc::clone(&self.changed),
        }
    }
}

impl<S: StateStoreBackend, D: DatabaseBackend, T: ToolsetBackend> CustomServerRequests<S, D, T> {
    pub(crate) fn new(
        state: CustomStateStore<S, D>,
        tools: CustomTools<T>,
        lifecycle_timeout: Duration,
    ) -> Self {
        Self {
            notifications: CustomNotifications::new(),
            state,
            tools,
            lifecycle_timeout,
            attachments: Arc::new(Mutex::new(CustomHookAttachments {
                closed: BTreeSet::new(),
                attachments: BTreeMap::new(),
            })),
            changed: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn start_resolution(
        &self,
        conversation_id: &ConversationId,
        notification_id: NotificationId,
        resolution: NotificationResolution,
    ) -> Result<oneshot::Receiver<Result<(), SdkError>>, SdkError> {
        resolution.validate()?;
        let request = {
            let mut requests = self.hook_attachments();
            requests.claim_resolution(conversation_id, &notification_id)?
        };
        let (completion, completed) = oneshot::channel();
        let owner = self.clone();
        let conversation_id = conversation_id.clone();
        let task_notification = notification_id.clone();
        let (start, ready) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if ready.await.is_err() {
                owner.finish_resolution_attempt(&task_notification);
                return;
            }
            let result = match request.response(&resolution).await {
                Ok(response) => {
                    owner
                        .drive_resolution(
                            conversation_id,
                            task_notification.clone(),
                            resolution,
                            response,
                        )
                        .await
                }
                Err(error) => {
                    owner.finish_resolution_attempt(&task_notification);
                    Err(error)
                }
            };
            let _ = completion.send(result);
        });
        self.hook_attachments()
            .install_resolution(&notification_id, handle)?;
        start
            .send(())
            .map_err(|_| SdkError::invalid_state("interaction resolution task could not start"))?;
        Ok(completed)
    }

    pub(crate) fn ensure_quiescent_reopen(
        &self,
        conversation_id: &ConversationId,
    ) -> Result<(), SdkError> {
        self.hook_attachments().ensure_no_live(conversation_id)
    }

    pub(crate) async fn cancel_conversation(
        &self,
        conversation_id: &ConversationId,
        deadline: Instant,
        observation_deadline: Option<Instant>,
    ) -> Result<(), SdkError> {
        let continuations = {
            let mut requests = self.hook_attachments();
            requests.begin_cancellation(conversation_id)
        };
        for continuation in continuations {
            let _ = continuation.send(Err(Self::cancelled_error()));
        }
        self.changed.notify_waiters();
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let step = {
                let mut requests = self.hook_attachments();
                requests.close_step(conversation_id)
            };
            match step {
                CustomCloseStep::Reconcile {
                    notification_id,
                    reconciliation,
                } => {
                    if Instant::now() >= deadline {
                        self.restore_reconciliation(notification_id, reconciliation);
                        return Err(SdkError::timeout(
                            "interaction reconciliation did not settle before cancellation",
                        ));
                    }
                    let owner = self.clone();
                    let task_notification = notification_id.clone();
                    let (start, ready) = oneshot::channel();
                    let handle = tokio::spawn(async move {
                        if ready.await.is_err() {
                            owner.restore_reconciliation(task_notification, reconciliation);
                            return;
                        }
                        let _ = owner
                            .reconcile_attachment(task_notification, reconciliation, deadline)
                            .await;
                    });
                    self.hook_attachments()
                        .install_reconciliation(&notification_id, handle)?;
                    start.send(()).map_err(|_| {
                        SdkError::invalid_state("interaction reconciliation task could not start")
                    })?;
                }
                CustomCloseStep::Wait => {
                    tokio::time::timeout_at(deadline, changed)
                        .await
                        .map_err(|_| {
                            SdkError::timeout("interaction cancellation did not settle")
                        })?;
                }
                CustomCloseStep::Ambiguous => {
                    return Err(SdkError::conflict(
                        "interaction settlement is still in progress",
                    ));
                }
                CustomCloseStep::Failed => {
                    return Err(SdkError::invalid_state(
                        "interaction settlement task terminated unexpectedly",
                    ));
                }
                CustomCloseStep::Ready => break,
            }
        }
        let owner = self.clone();
        let completion_id = conversation_id.clone();
        self.state
            .cancel_interactions_before(
                conversation_id.clone(),
                deadline,
                observation_deadline,
                move || {
                    owner.finish_cancellation(&completion_id);
                },
            )
            .await
    }

    fn finish_cancellation(&self, conversation_id: &ConversationId) {
        self.hook_attachments().finish_cancellation(conversation_id);
        self.changed.notify_waiters();
    }

    pub(crate) async fn wait_for_tool_approval(
        &self,
        conversation_id: ConversationId,
        visible_name: String,
        arguments: serde_json::Value,
    ) -> Result<CustomApprovalDecision, SdkError> {
        match self
            .wait_for_response(
                conversation_id,
                CustomRequest::ToolApproval {
                    visible_name,
                    arguments,
                },
            )
            .await?
        {
            CustomHookResponse::Approval(decision) => Ok(decision),
            CustomHookResponse::ToolInteraction(_) => Err(SdkError::invalid_state(
                "tool approval produced a contradictory response",
            )),
        }
    }

    pub(crate) async fn wait_for_tool_interaction(
        &self,
        conversation_id: ConversationId,
        request: tool_sdk::interaction::ToolInteractionRequest,
    ) -> Result<tool_sdk::interaction::ToolInteractionResponse, SdkError> {
        match self
            .wait_for_response(conversation_id, CustomRequest::ToolInteraction(request))
            .await?
        {
            CustomHookResponse::ToolInteraction(response) => Ok(response),
            CustomHookResponse::Approval(_) => Err(SdkError::invalid_state(
                "tool interaction produced a contradictory response",
            )),
        }
    }

    async fn wait_for_response(
        &self,
        conversation_id: ConversationId,
        request: CustomRequest,
    ) -> Result<CustomHookResponse, SdkError> {
        let interaction = request.interaction(&self.tools).await?;
        let kind = self.notifications.interaction(interaction).await?;
        let notification_id = NotificationId::new();
        let (continuation, receiver) = oneshot::channel();
        {
            let mut requests = self.hook_attachments();
            requests.insert(
                notification_id.clone(),
                CustomHookAttachment {
                    conversation_id: conversation_id.clone(),
                    request,
                    continuation: Some(continuation),
                    state: CustomHookAttachmentState::Publishing { handle: None },
                },
            )?;
        }
        let (publication, published) = oneshot::channel();
        let owner = self.clone();
        let task_notification = notification_id.clone();
        let (start, ready) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if ready.await.is_err() {
                return;
            }
            let result = owner
                .publish_interaction(conversation_id, task_notification, kind)
                .await;
            let _ = publication.send(result);
        });
        self.hook_attachments()
            .install_publication(&notification_id, handle)?;
        start
            .send(())
            .map_err(|_| SdkError::invalid_state("interaction publication task could not start"))?;
        published.await.unwrap_or_else(|_| {
            Err(SdkError::invalid_state(
                "interaction publication task ended unexpectedly",
            ))
        })?;
        let delivery = receiver
            .await
            .unwrap_or_else(|_| Err(Self::cancelled_error()))?;
        delivery.consume().await
    }

    async fn publish_interaction(
        &self,
        conversation_id: ConversationId,
        notification_id: NotificationId,
        kind: crate::ConversationNotificationKind,
    ) -> Result<(), SdkError> {
        let now = Instant::now();
        let deadline = now.checked_add(self.lifecycle_timeout).unwrap_or(now);
        let publication = self
            .state
            .publish_interaction(&conversation_id, notification_id.clone(), kind, deadline)
            .await;
        let cancelled = {
            let mut requests = self.hook_attachments();
            requests.complete_publication(&notification_id, publication.is_ok())?
        };
        self.changed.notify_waiters();
        match publication {
            Ok(()) => Ok(()),
            Err(_) if cancelled => Err(Self::cancelled_error()),
            Err(error) => Err(error),
        }
    }

    async fn drive_resolution(
        &self,
        conversation_id: ConversationId,
        notification_id: NotificationId,
        resolution: NotificationResolution,
        response: CustomHookResponse,
    ) -> Result<(), SdkError> {
        let now = Instant::now();
        let deadline = now.checked_add(self.lifecycle_timeout).unwrap_or(now);
        let intent = match self
            .state
            .begin_resolution(
                &conversation_id,
                notification_id.clone(),
                resolution,
                deadline,
            )
            .await
        {
            Ok(intent) => intent,
            Err(error) if error.kind() == crate::error::ErrorKind::Conflict => {
                self.require_resolution_recovery(&notification_id);
                return Err(error);
            }
            Err(error) => {
                self.finish_resolution_attempt(&notification_id);
                return Err(error);
            }
        };
        let (acceptance, settlement_ready) =
            self.dispatch_response(&notification_id, response).await;
        let mut reconciliation = CustomReconciliation {
            intent,
            settlement: acceptance.settlement(),
            confirmed_ambiguous: false,
        };
        if let Err(error) = self.settle_bounded(&mut reconciliation, deadline).await {
            let mut requests = self.hook_attachments();
            requests.defer_reconciliation(&notification_id, reconciliation);
            drop(requests);
            self.changed.notify_waiters();
            return Err(error);
        }
        if let Some(settlement_ready) = settlement_ready {
            let _ = settlement_ready.send(());
        }
        if reconciliation.mark_confirmed_ambiguous() {
            let mut requests = self.hook_attachments();
            requests.defer_reconciliation(&notification_id, reconciliation);
            drop(requests);
            self.changed.notify_waiters();
            return acceptance.public_result();
        }
        self.finish_resolution(&notification_id);
        acceptance.public_result()
    }

    async fn dispatch_response(
        &self,
        notification_id: &NotificationId,
        response: CustomHookResponse,
    ) -> (CustomAcceptance, Option<oneshot::Sender<()>>) {
        let continuation = {
            let mut requests = self.hook_attachments();
            requests.take_continuation(notification_id)
        };
        let Some(continuation) = continuation else {
            return (CustomAcceptance::Cancelled, None);
        };
        let (consumed, observed) = oneshot::channel();
        let (settlement_ready, settled) = oneshot::channel();
        if continuation
            .send(Ok(CustomHookDelivery {
                response,
                consumed,
                settled,
            }))
            .is_err()
        {
            return (CustomAcceptance::Cancelled, None);
        }
        let acceptance = match observed.await {
            Ok(()) => CustomAcceptance::Accepted,
            Err(_) => CustomAcceptance::Ambiguous,
        };
        (acceptance, Some(settlement_ready))
    }

    async fn settle_bounded(
        &self,
        reconciliation: &mut CustomReconciliation,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let disposition = self
            .state
            .settle_resolution(
                &mut reconciliation.intent,
                reconciliation.settlement,
                deadline,
            )
            .await?;
        reconciliation.confirm(disposition)
    }

    async fn reconcile_attachment(
        &self,
        notification_id: NotificationId,
        mut reconciliation: CustomReconciliation,
        deadline: Instant,
    ) -> Result<(), SdkError> {
        let settlement = self.settle_bounded(&mut reconciliation, deadline).await;
        match settlement {
            Ok(()) => {
                if reconciliation.mark_confirmed_ambiguous() {
                    let mut attachments = self.hook_attachments();
                    attachments.restore_reconciliation(&notification_id, reconciliation);
                    drop(attachments);
                    self.changed.notify_waiters();
                    Err(SdkError::conflict(
                        "interaction settlement is still in progress",
                    ))
                } else {
                    self.finish_resolution(&notification_id);
                    Ok(())
                }
            }
            Err(error) => {
                let mut attachments = self.hook_attachments();
                attachments.restore_reconciliation(&notification_id, reconciliation);
                drop(attachments);
                self.changed.notify_waiters();
                Err(error)
            }
        }
    }

    fn finish_resolution_attempt(&self, notification_id: &NotificationId) {
        let mut requests = self.hook_attachments();
        requests.finish_resolution_attempt(notification_id);
        drop(requests);
        self.changed.notify_waiters();
    }

    fn require_resolution_recovery(&self, notification_id: &NotificationId) {
        let mut requests = self.hook_attachments();
        requests.require_resolution_recovery(notification_id);
        drop(requests);
        self.changed.notify_waiters();
    }

    fn finish_resolution(&self, notification_id: &NotificationId) {
        self.hook_attachments().attachments.remove(notification_id);
        self.changed.notify_waiters();
    }

    fn restore_reconciliation(
        &self,
        notification_id: NotificationId,
        reconciliation: CustomReconciliation,
    ) {
        self.hook_attachments()
            .restore_reconciliation(&notification_id, reconciliation);
        self.changed.notify_waiters();
    }

    fn hook_attachments(&self) -> MutexGuard<'_, CustomHookAttachments> {
        self.attachments
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn cancelled_error() -> SdkError {
        SdkError::cancelled("interaction response was cancelled")
    }
}

impl CustomHookAttachments {
    fn insert(
        &mut self,
        notification_id: NotificationId,
        attachment: CustomHookAttachment,
    ) -> Result<(), SdkError> {
        if self.closed.contains(&attachment.conversation_id) {
            return Err(SdkError::closed(
                "conversation no longer accepts interactions",
            ));
        }
        if self.attachments.len() >= MAX_LIVE_ATTACHMENTS {
            return Err(SdkError::busy("live interaction capacity is saturated"));
        }
        if self.attachments.contains_key(&notification_id) {
            return Err(SdkError::invalid_state("interaction identity was reused"));
        }
        self.attachments.insert(notification_id, attachment);
        Ok(())
    }

    fn complete_publication(
        &mut self,
        notification_id: &NotificationId,
        succeeded: bool,
    ) -> Result<bool, SdkError> {
        let cancelled = {
            let attachment = self
                .attachments
                .get_mut(notification_id)
                .ok_or_else(|| SdkError::invalid_state("interaction state is inconsistent"))?;
            if !matches!(
                attachment.state,
                CustomHookAttachmentState::Publishing { .. }
            ) {
                return Err(SdkError::invalid_state(
                    "interaction publication state is inconsistent",
                ));
            }
            attachment.state = CustomHookAttachmentState::Pending;
            attachment.continuation.is_none()
        };
        if !succeeded {
            self.attachments.remove(notification_id);
        }
        Ok(cancelled)
    }

    fn install_publication(
        &mut self,
        notification_id: &NotificationId,
        handle: JoinHandle<()>,
    ) -> Result<(), SdkError> {
        let attachment = self
            .attachments
            .get_mut(notification_id)
            .ok_or_else(|| SdkError::invalid_state("interaction state is inconsistent"))?;
        match &mut attachment.state {
            CustomHookAttachmentState::Publishing {
                handle: slot @ None,
            } => {
                *slot = Some(handle);
                Ok(())
            }
            _ => Err(SdkError::invalid_state(
                "interaction publication task changed before installation",
            )),
        }
    }

    fn install_resolution(
        &mut self,
        notification_id: &NotificationId,
        handle: JoinHandle<()>,
    ) -> Result<(), SdkError> {
        let attachment = self
            .attachments
            .get_mut(notification_id)
            .ok_or_else(|| SdkError::invalid_state("interaction state is inconsistent"))?;
        match &mut attachment.state {
            CustomHookAttachmentState::Resolving {
                handle: slot @ None,
            } => {
                *slot = Some(handle);
                Ok(())
            }
            _ => Err(SdkError::invalid_state(
                "interaction resolution task changed before installation",
            )),
        }
    }

    fn install_reconciliation(
        &mut self,
        notification_id: &NotificationId,
        handle: JoinHandle<()>,
    ) -> Result<(), SdkError> {
        let attachment = self
            .attachments
            .get_mut(notification_id)
            .ok_or_else(|| SdkError::invalid_state("interaction state is inconsistent"))?;
        match &mut attachment.state {
            CustomHookAttachmentState::Reconciling {
                handle: slot @ None,
            } => {
                *slot = Some(handle);
                Ok(())
            }
            _ => Err(SdkError::invalid_state(
                "interaction reconciliation task changed before installation",
            )),
        }
    }

    fn claim_resolution(
        &mut self,
        conversation_id: &ConversationId,
        notification_id: &NotificationId,
    ) -> Result<CustomRequest, SdkError> {
        if self.closed.contains(conversation_id) {
            return Err(SdkError::closed(
                "conversation no longer accepts interaction resolution",
            ));
        }
        self.owned_attachment_mut(conversation_id, notification_id)?
            .claim_resolution()
    }

    fn take_continuation(
        &mut self,
        notification_id: &NotificationId,
    ) -> Option<oneshot::Sender<Result<CustomHookDelivery, SdkError>>> {
        self.attachments
            .get_mut(notification_id)
            .and_then(|attachment| attachment.continuation.take())
    }

    fn defer_reconciliation(
        &mut self,
        notification_id: &NotificationId,
        reconciliation: CustomReconciliation,
    ) {
        if let Some(attachment) = self.attachments.get_mut(notification_id) {
            attachment.state = CustomHookAttachmentState::Deferred { reconciliation };
        }
    }

    fn restore_reconciliation(
        &mut self,
        notification_id: &NotificationId,
        reconciliation: CustomReconciliation,
    ) {
        if let Some(attachment) = self.attachments.get_mut(notification_id) {
            attachment.state = CustomHookAttachmentState::Deferred { reconciliation };
        }
    }

    fn finish_resolution_attempt(&mut self, notification_id: &NotificationId) {
        if let Some(attachment) = self.attachments.get_mut(notification_id) {
            attachment.state = CustomHookAttachmentState::Pending;
        }
    }

    fn require_resolution_recovery(&mut self, notification_id: &NotificationId) {
        if let Some(attachment) = self.attachments.get_mut(notification_id) {
            attachment.state = CustomHookAttachmentState::RecoveryRequired;
        }
    }

    fn begin_cancellation(
        &mut self,
        conversation_id: &ConversationId,
    ) -> Vec<oneshot::Sender<Result<CustomHookDelivery, SdkError>>> {
        self.closed.insert(conversation_id.clone());
        self.attachments
            .values_mut()
            .filter(|attachment| &attachment.conversation_id == conversation_id)
            .filter_map(|attachment| attachment.continuation.take())
            .collect()
    }

    fn close_step(&mut self, conversation_id: &ConversationId) -> CustomCloseStep {
        for (notification_id, attachment) in &mut self.attachments {
            if &attachment.conversation_id != conversation_id {
                continue;
            }
            match &attachment.state {
                CustomHookAttachmentState::Publishing { handle }
                | CustomHookAttachmentState::Resolving { handle }
                | CustomHookAttachmentState::Reconciling { handle } => {
                    if handle.as_ref().is_some_and(JoinHandle::is_finished) {
                        return CustomCloseStep::Failed;
                    }
                    return CustomCloseStep::Wait;
                }
                CustomHookAttachmentState::Pending => {}
                CustomHookAttachmentState::RecoveryRequired => {
                    return CustomCloseStep::Ambiguous;
                }
                CustomHookAttachmentState::Deferred { reconciliation }
                    if reconciliation.is_confirmed_ambiguous() =>
                {
                    return CustomCloseStep::Ambiguous;
                }
                CustomHookAttachmentState::Deferred { .. } => {
                    let state = std::mem::replace(
                        &mut attachment.state,
                        CustomHookAttachmentState::Reconciling { handle: None },
                    );
                    let CustomHookAttachmentState::Deferred { reconciliation } = state else {
                        return CustomCloseStep::Failed;
                    };
                    return CustomCloseStep::Reconcile {
                        notification_id: notification_id.clone(),
                        reconciliation,
                    };
                }
            }
        }
        CustomCloseStep::Ready
    }

    fn finish_cancellation(&mut self, conversation_id: &ConversationId) {
        self.attachments
            .retain(|_, attachment| &attachment.conversation_id != conversation_id);
        self.closed.remove(conversation_id);
    }

    fn ensure_no_live(&self, conversation_id: &ConversationId) -> Result<(), SdkError> {
        if self.closed.contains(conversation_id)
            || self
                .attachments
                .values()
                .any(|attachment| &attachment.conversation_id == conversation_id)
        {
            return Err(SdkError::busy(
                "conversation still owns live interaction responses",
            ));
        }
        Ok(())
    }

    fn owned_attachment_mut(
        &mut self,
        conversation_id: &ConversationId,
        notification_id: &NotificationId,
    ) -> Result<&mut CustomHookAttachment, SdkError> {
        self.attachments
            .get_mut(notification_id)
            .filter(|attachment| &attachment.conversation_id == conversation_id)
            .ok_or_else(|| SdkError::not_found("notification is not a live interaction"))
    }
}

impl CustomHookAttachment {
    fn claim_resolution(&mut self) -> Result<CustomRequest, SdkError> {
        match &self.state {
            CustomHookAttachmentState::Publishing { .. } => {
                return Err(SdkError::busy("interaction publication is still settling"));
            }
            CustomHookAttachmentState::Resolving { handle }
            | CustomHookAttachmentState::Reconciling { handle } => {
                return if handle.as_ref().is_some_and(JoinHandle::is_finished) {
                    Err(SdkError::invalid_state(
                        "interaction settlement task terminated unexpectedly",
                    ))
                } else {
                    Err(SdkError::conflict(
                        "interaction resolution is already in progress",
                    ))
                };
            }
            CustomHookAttachmentState::Deferred { .. } => {
                return Err(SdkError::conflict(
                    "interaction settlement is still in progress",
                ));
            }
            CustomHookAttachmentState::RecoveryRequired => {
                return Err(SdkError::conflict(
                    "interaction intent requires durable recovery",
                ));
            }
            CustomHookAttachmentState::Pending => {}
        }
        if self.continuation.is_none() {
            return Err(SdkError::conflict(
                "interaction response is no longer available",
            ));
        }
        self.state = CustomHookAttachmentState::Resolving { handle: None };
        Ok(self.request.clone())
    }
}

impl CustomHookDelivery {
    async fn consume(self) -> Result<CustomHookResponse, SdkError> {
        let Self {
            response,
            consumed,
            settled,
        } = self;
        let _ = consumed.send(());
        settled
            .await
            .map_err(|_| SdkError::conflict("interaction settlement is still being reconciled"))?;
        Ok(response)
    }
}

impl CustomReconciliation {
    fn mark_confirmed_ambiguous(&mut self) -> bool {
        if self.settlement == ResolutionSettlement::Ambiguous {
            self.confirmed_ambiguous = true;
        }
        self.confirmed_ambiguous
    }

    fn is_confirmed_ambiguous(&self) -> bool {
        self.confirmed_ambiguous
    }

    fn confirm(&self, disposition: ResolutionDisposition) -> Result<(), SdkError> {
        match disposition {
            ResolutionDisposition::Applied(actual) if actual == self.settlement => Ok(()),
            ResolutionDisposition::AlreadyAccepted
                if self.settlement == ResolutionSettlement::Accepted =>
            {
                Ok(())
            }
            ResolutionDisposition::Applied(_) | ResolutionDisposition::AlreadyAccepted => Err(
                SdkError::invalid_state("interaction settlement contradicts stored state"),
            ),
        }
    }
}

impl CustomRequest {
    async fn interaction<T: ToolsetBackend>(
        &self,
        tools: &CustomTools<T>,
    ) -> Result<CustomInteraction, SdkError> {
        let interaction = match self {
            Self::ToolApproval {
                visible_name,
                arguments,
            } => CustomInteraction::tool_approval(
                visible_name.clone(),
                tools
                    .present_approval_arguments(visible_name, arguments)
                    .await?,
            ),
            Self::ToolInteraction(request) => CustomInteraction::tool_interaction(request),
        };
        Ok(interaction)
    }

    async fn response(
        &self,
        resolution: &NotificationResolution,
    ) -> Result<CustomHookResponse, SdkError> {
        match (self, resolution) {
            (Self::ToolApproval { .. }, NotificationResolution::Allow) => {
                Ok(CustomHookResponse::Approval(CustomApprovalDecision::Allow))
            }
            (Self::ToolApproval { .. }, NotificationResolution::Deny { reason }) => {
                Ok(CustomHookResponse::Approval(CustomApprovalDecision::Deny {
                    reason: Some(reason.clone()),
                }))
            }
            (
                Self::ToolApproval { .. },
                NotificationResolution::Submit { .. } | NotificationResolution::Dismiss { .. },
            ) => Err(SdkError::invalid_input(
                "approval interaction requires Allow or Deny",
            )),
            (Self::ToolInteraction(request), NotificationResolution::Submit { content }) => {
                CustomInteraction::validate_submission(request.clone(), content.clone()).await?;
                Ok(CustomHookResponse::ToolInteraction(
                    tool_sdk::interaction::ToolInteractionResponse::submit(
                        request.id.clone(),
                        content.clone(),
                    ),
                ))
            }
            (Self::ToolInteraction(request), NotificationResolution::Dismiss { reason }) => {
                Ok(CustomHookResponse::ToolInteraction(
                    tool_sdk::interaction::ToolInteractionResponse::cancel(
                        request.id.clone(),
                        Some(reason.clone()),
                    ),
                ))
            }
            (
                Self::ToolInteraction(_),
                NotificationResolution::Allow | NotificationResolution::Deny { .. },
            ) => Err(SdkError::invalid_input(
                "tool interaction requires Submit or Dismiss",
            )),
        }
    }
}

impl CustomAcceptance {
    fn settlement(self) -> ResolutionSettlement {
        match self {
            Self::Accepted => ResolutionSettlement::Accepted,
            Self::Ambiguous => ResolutionSettlement::Ambiguous,
            Self::Cancelled => ResolutionSettlement::Cancelled,
        }
    }

    fn public_result(self) -> Result<(), SdkError> {
        match self {
            Self::Accepted => Ok(()),
            Self::Ambiguous => Err(SdkError::conflict(
                "interaction settlement is still in progress",
            )),
            Self::Cancelled => Err(SdkError::conflict(
                "interaction response is no longer available",
            )),
        }
    }
}
