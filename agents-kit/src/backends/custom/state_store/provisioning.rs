use std::collections::BTreeMap;
use std::sync::Mutex;

use tokio::sync::Notify;
use tokio::time::{Instant, timeout_at};

use crate::{ConversationId, KitError, WorkspaceId};

pub(super) struct CustomProvisioningOwners {
    active: Mutex<BTreeMap<WorkspaceId, BTreeMap<ConversationId, CustomProvisioningStatus>>>,
    changed: Notify,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CustomProvisioningStatus {
    Active,
    RecoveryRequired,
}

impl CustomProvisioningOwners {
    pub(super) fn new() -> Self {
        Self {
            active: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
        }
    }

    pub(super) fn begin(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) -> Result<(), KitError> {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let conversations = active.entry(workspace_id.clone()).or_default();
        if conversations.contains_key(conversation_id) {
            return Err(KitError::invalid_state(
                "conversation provisioning is already active",
            ));
        }
        conversations.insert(conversation_id.clone(), CustomProvisioningStatus::Active);
        drop(active);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(super) fn finish(&self, workspace_id: &WorkspaceId, conversation_id: &ConversationId) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(conversations) = active.get_mut(workspace_id) {
            conversations.remove(conversation_id);
            if conversations.is_empty() {
                active.remove(workspace_id);
            }
        }
        drop(active);
        self.changed.notify_waiters();
    }

    pub(super) fn require_recovery(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &ConversationId,
    ) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active.entry(workspace_id.clone()).or_default().insert(
            conversation_id.clone(),
            CustomProvisioningStatus::RecoveryRequired,
        );
        drop(active);
        self.changed.notify_waiters();
    }

    pub(super) fn contains_workspace(&self, workspace_id: &WorkspaceId) -> bool {
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(workspace_id)
    }

    pub(super) async fn settle_before(
        &self,
        workspace_id: Option<&WorkspaceId>,
        deadline: Instant,
    ) -> Result<(), KitError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let status = {
                let active = self
                    .active
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let mut has_active = false;
                let mut recovery_required = false;
                for (active_workspace, conversations) in &*active {
                    if workspace_id.is_some_and(|expected| expected != active_workspace) {
                        continue;
                    }
                    for status in conversations.values() {
                        has_active |= *status == CustomProvisioningStatus::Active;
                        recovery_required |= *status == CustomProvisioningStatus::RecoveryRequired;
                    }
                }
                (has_active, recovery_required)
            };
            if status.1 {
                return Err(KitError::integration(
                    "conversation provisioning cleanup requires recovery",
                ));
            }
            if !status.0 {
                return Ok(());
            }
            timeout_at(deadline, changed)
                .await
                .map_err(|_| KitError::busy("conversation provisioning is still active"))?;
        }
    }
}
