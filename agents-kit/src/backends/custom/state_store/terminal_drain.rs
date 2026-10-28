use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use storage_kit::{DatabaseBackend, StateStoreBackend};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use crate::backends::custom::knowledge_base::CustomKnowledgeBase;
use crate::core::state::ConversationRecord;
use crate::{ConversationId, KitError, KnowledgeOwnerId, WorkspaceId};

use super::CustomStateStore;

struct CustomTerminalDrainOwner<S: StateStoreBackend, D: DatabaseBackend> {
    state: CustomStateStore<S, D>,
    knowledge: CustomKnowledgeBase<S, D>,
    workspace_id: WorkspaceId,
    workspace_revision: u64,
    conversation_id: ConversationId,
    lifecycle_timeout: Duration,
    executor: tokio::runtime::Handle,
    slot: Mutex<CustomTerminalDrainState>,
    changed: Notify,
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct CustomTerminalDrainKey {
    workspace_id: WorkspaceId,
    conversation_id: ConversationId,
}

pub(super) struct CustomTerminalDrains<S: StateStoreBackend, D: DatabaseBackend> {
    active: Mutex<BTreeMap<CustomTerminalDrainKey, Arc<CustomTerminalDrainOwner<S, D>>>>,
}

enum CustomTerminalDrainState {
    Ready {
        attempt_id: u64,
    },
    Task {
        attempt_id: u64,
        handle: JoinHandle<()>,
        exit: CustomTerminalDrainExit,
    },
    Reclaimed,
    Poisoned,
}

enum CustomTerminalDrainExit {
    Running,
    Completed(Result<(), KitError>),
    Interrupted,
}

pub(crate) struct CustomTerminalDrain<S: StateStoreBackend, D: DatabaseBackend> {
    owner: Arc<CustomTerminalDrainOwner<S, D>>,
}

struct CustomTerminalDrainGuard<S: StateStoreBackend, D: DatabaseBackend> {
    owner: Arc<CustomTerminalDrainOwner<S, D>>,
    attempt_id: u64,
    outcome: Option<Result<(), KitError>>,
}

impl<S, D> CustomTerminalDrains<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    pub(super) fn new() -> Self {
        Self {
            active: Mutex::new(BTreeMap::new()),
        }
    }

    fn begin(&self, owner: &Arc<CustomTerminalDrainOwner<S, D>>) -> Result<(), KitError> {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = owner.key();
        if active.contains_key(&key) {
            return Err(KitError::invalid_state(
                "terminal notification cleanup is already active",
            ));
        }
        active.insert(key, Arc::clone(owner));
        Ok(())
    }

    fn finish(&self, owner: &Arc<CustomTerminalDrainOwner<S, D>>) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = owner.key();
        if active
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, owner))
        {
            active.remove(&key);
        }
    }

    pub(super) async fn settle_before(
        &self,
        workspace_id: &WorkspaceId,
        deadline: Instant,
    ) -> Result<(), KitError> {
        self.settle_scope_before(Some(workspace_id), deadline).await
    }

    pub(super) async fn settle_all_before(&self, deadline: Instant) -> Result<(), KitError> {
        self.settle_scope_before(None, deadline).await
    }

    async fn settle_scope_before(
        &self,
        workspace_id: Option<&WorkspaceId>,
        deadline: Instant,
    ) -> Result<(), KitError> {
        let mut first_error = None;
        loop {
            let owners = {
                let active = self
                    .active
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                active
                    .iter()
                    .filter(|(key, _)| {
                        workspace_id.is_none_or(|workspace_id| &key.workspace_id == workspace_id)
                    })
                    .map(|(_, owner)| Arc::clone(owner))
                    .collect::<Vec<_>>()
            };
            if owners.is_empty() {
                return first_error.map_or(Ok(()), Err);
            }
            let mut expired = false;
            for owner in &owners {
                match timeout_at(deadline, owner.wait_before(deadline)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => KitError::record_cleanup(&mut first_error, error),

                    Err(_) => {
                        KitError::record_cleanup(
                            &mut first_error,
                            KitError::busy("terminal conversation cleanup is still active"),
                        );
                        expired = true;
                        break;
                    }
                }
            }
            if expired {
                let joining = owners
                    .into_iter()
                    .filter(|owner| owner.abort_for_deadline())
                    .collect::<Vec<_>>();
                for owner in joining {
                    owner.join_after_deadline().await;
                }
                return first_error.map_or(Ok(()), Err);
            }
        }
    }
}

impl<S, D> CustomTerminalDrainOwner<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    fn key(&self) -> CustomTerminalDrainKey {
        CustomTerminalDrainKey {
            workspace_id: self.workspace_id.clone(),
            conversation_id: self.conversation_id.clone(),
        }
    }

    fn new(
        state: CustomStateStore<S, D>,
        knowledge: CustomKnowledgeBase<S, D>,
        record: &ConversationRecord,
        lifecycle_timeout: Duration,
    ) -> Self {
        let executor = state.notifications().executor();
        Self {
            state,
            knowledge,
            workspace_id: record.workspace_id().clone(),
            workspace_revision: record.workspace_revision(),
            conversation_id: record.id().clone(),
            lifecycle_timeout,
            executor,
            slot: Mutex::new(CustomTerminalDrainState::Ready { attempt_id: 1 }),
            changed: Notify::new(),
        }
    }

    fn cleanup_deadline(&self) -> Instant {
        let now = Instant::now();
        now.checked_add(self.lifecycle_timeout).unwrap_or(now)
    }

    fn start_before(self: &Arc<Self>, deadline: Instant) -> Result<(), KitError> {
        let executor = self.executor.clone();
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let attempt_id = match &*slot {
            CustomTerminalDrainState::Ready { attempt_id } => *attempt_id,
            CustomTerminalDrainState::Task { .. } | CustomTerminalDrainState::Reclaimed => {
                return Ok(());
            }
            CustomTerminalDrainState::Poisoned => {
                return Err(KitError::invalid_state(
                    "terminal notification cleanup owner is poisoned",
                ));
            }
        };
        self.state.terminal_drains.begin(self)?;
        let state = self.state.clone();
        let knowledge = self.knowledge.clone();
        let workspace_id = self.workspace_id.clone();
        let workspace_revision = self.workspace_revision;
        let conversation_id = self.conversation_id.clone();
        let guard = CustomTerminalDrainGuard {
            owner: Arc::clone(self),
            attempt_id,
            outcome: None,
        };
        let handle = executor.spawn(async move {
            let outcome = timeout_at(
                deadline,
                state.complete_terminal_drain(
                    &workspace_id,
                    workspace_revision,
                    &conversation_id,
                    &knowledge,
                    deadline,
                ),
            )
            .await
            .unwrap_or_else(|_| {
                Err(KitError::timeout(
                    "terminal conversation cleanup deadline elapsed",
                ))
            });
            guard.complete(outcome);
        });
        *slot = CustomTerminalDrainState::Task {
            attempt_id,
            handle,
            exit: CustomTerminalDrainExit::Running,
        };
        drop(slot);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn wait_before(self: &Arc<Self>, deadline: Instant) -> Result<(), KitError> {
        if let Err(error) = self.start_before(deadline) {
            self.finish_registration();
            return Err(error);
        }
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let (restart, yield_before_wait) = {
                let mut slot = self
                    .slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let joined = match &mut *slot {
                    CustomTerminalDrainState::Task { handle, .. } if handle.is_finished() => {
                        let waker = std::task::Waker::noop();
                        let mut context = Context::from_waker(waker);
                        match Pin::new(handle).poll(&mut context) {
                            Poll::Ready(joined) => Some(joined.is_ok()),
                            Poll::Pending => None,
                        }
                    }
                    _ => None,
                };
                if let Some(joined) = joined {
                    let current = std::mem::replace(&mut *slot, CustomTerminalDrainState::Poisoned);
                    match current {
                        CustomTerminalDrainState::Task {
                            attempt_id,
                            exit: CustomTerminalDrainExit::Completed(outcome),
                            ..
                        } if joined => match outcome {
                            Ok(()) => {
                                *slot = CustomTerminalDrainState::Reclaimed;
                                self.finish_registration();
                                return Ok(());
                            }
                            Err(error) => {
                                let Some(attempt_id) = attempt_id.checked_add(1) else {
                                    self.finish_registration();
                                    return Err(KitError::invalid_state(
                                        "terminal notification cleanup identity is exhausted",
                                    ));
                                };
                                *slot = CustomTerminalDrainState::Ready { attempt_id };
                                self.finish_registration();
                                return Err(error);
                            }
                        },
                        CustomTerminalDrainState::Ready { .. }
                        | CustomTerminalDrainState::Task { .. }
                        | CustomTerminalDrainState::Reclaimed
                        | CustomTerminalDrainState::Poisoned => {
                            self.finish_registration();
                            return Err(KitError::invalid_state(
                                "terminal notification cleanup task terminated unexpectedly",
                            ));
                        }
                    }
                }
                match &*slot {
                    CustomTerminalDrainState::Ready { .. } => (true, false),
                    CustomTerminalDrainState::Task {
                        exit: CustomTerminalDrainExit::Running,
                        ..
                    } => (false, false),
                    CustomTerminalDrainState::Task {
                        exit:
                            CustomTerminalDrainExit::Completed(_) | CustomTerminalDrainExit::Interrupted,
                        ..
                    } => (false, true),
                    CustomTerminalDrainState::Reclaimed => return Ok(()),
                    CustomTerminalDrainState::Poisoned => {
                        return Err(KitError::invalid_state(
                            "terminal notification cleanup owner is poisoned",
                        ));
                    }
                }
            };
            if restart {
                if let Err(error) = self.start_before(deadline) {
                    self.finish_registration();
                    return Err(error);
                }
            } else if yield_before_wait {
                tokio::task::yield_now().await;
            } else {
                changed.await;
            }
        }
    }

    fn abort_for_deadline(&self) -> bool {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let active = if let CustomTerminalDrainState::Task { handle, exit, .. } = &mut *slot {
            if matches!(exit, CustomTerminalDrainExit::Running) {
                *exit = CustomTerminalDrainExit::Interrupted;
                handle.abort();
            }
            true
        } else {
            false
        };
        drop(slot);
        if active {
            self.changed.notify_waiters();
        }
        active
    }

    async fn join_after_deadline(self: &Arc<Self>) {
        loop {
            let finished = {
                let mut slot = self
                    .slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let joined = match &mut *slot {
                    CustomTerminalDrainState::Task { handle, .. } if handle.is_finished() => {
                        let waker = std::task::Waker::noop();
                        let mut context = Context::from_waker(waker);
                        matches!(Pin::new(handle).poll(&mut context), Poll::Ready(_))
                    }
                    CustomTerminalDrainState::Ready { .. }
                    | CustomTerminalDrainState::Reclaimed
                    | CustomTerminalDrainState::Poisoned => return,
                    CustomTerminalDrainState::Task { .. } => false,
                };
                if joined {
                    *slot = CustomTerminalDrainState::Poisoned;
                }
                joined
            };
            if finished {
                self.finish_registration();
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    fn finish_registration(self: &Arc<Self>) {
        self.state.terminal_drains.finish(self);
    }

    fn finish_attempt(&self, attempt_id: u64, outcome: Option<Result<(), KitError>>) {
        let matched = {
            let mut slot = self
                .slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let current = std::mem::replace(&mut *slot, CustomTerminalDrainState::Poisoned);
            match current {
                CustomTerminalDrainState::Task {
                    attempt_id: current,
                    handle,
                    exit: CustomTerminalDrainExit::Running,
                } if current == attempt_id => {
                    let exit = match outcome {
                        Some(outcome) => CustomTerminalDrainExit::Completed(outcome),
                        None => CustomTerminalDrainExit::Interrupted,
                    };
                    *slot = CustomTerminalDrainState::Task {
                        attempt_id,
                        handle,
                        exit,
                    };
                    true
                }
                current => {
                    *slot = current;
                    false
                }
            }
        };
        if matched {
            self.changed.notify_waiters();
        }
    }
}

impl<S, D> CustomTerminalDrain<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    pub(super) fn new(
        state: CustomStateStore<S, D>,
        record: &ConversationRecord,
        knowledge: CustomKnowledgeBase<S, D>,
        lifecycle_timeout: Duration,
    ) -> Self {
        Self {
            owner: Arc::new(CustomTerminalDrainOwner::new(
                state,
                knowledge,
                record,
                lifecycle_timeout,
            )),
        }
    }

    pub(crate) async fn complete(&self) -> Result<(), KitError> {
        let deadline = self.owner.cleanup_deadline();
        self.owner.wait_before(deadline).await
    }

    pub(crate) fn schedule(&self) {
        let deadline = self.owner.cleanup_deadline();
        let _ = self.owner.start_before(deadline);
    }
}

impl<S, D> CustomTerminalDrainGuard<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    fn complete(mut self, result: Result<(), KitError>) {
        self.outcome = Some(result);
    }
}

impl<S, D> Drop for CustomTerminalDrainGuard<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    fn drop(&mut self) {
        self.owner
            .finish_attempt(self.attempt_id, self.outcome.take());
    }
}

impl<S, D> CustomStateStore<S, D>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
{
    async fn complete_terminal_drain(
        &self,
        workspace_id: &WorkspaceId,
        workspace_revision: u64,
        conversation_id: &ConversationId,
        knowledge: &CustomKnowledgeBase<S, D>,
        deadline: Instant,
    ) -> Result<(), KitError> {
        self.notifications
            .wait_for_terminal_reclamation(conversation_id)
            .await?;
        knowledge
            .settle_forget(
                &KnowledgeOwnerId::Conversation {
                    agent_id: self.agent_id.clone(),
                    workspace_id: workspace_id.clone(),
                    conversation_id: conversation_id.clone(),
                },
                deadline,
            )
            .await?;
        self.remove_closed_conversation(workspace_id, workspace_revision, conversation_id)
            .await
    }
}
