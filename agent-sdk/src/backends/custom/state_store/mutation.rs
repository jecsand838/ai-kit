use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

use crate::SdkError;

pub(super) struct CustomMutationOwner {
    slot: Mutex<CustomMutationSlot>,
    changed: Notify,
}

enum CustomMutationSlot {
    Idle {
        next_id: u64,
    },
    Installing {
        id: u64,
        next_id: u64,
    },
    ExitedBeforeInstall {
        id: u64,
        next_id: u64,
        exit: CustomMutationTaskExit,
    },
    Running {
        id: u64,
        next_id: u64,
        task: CustomMutationTask,
    },
    Poisoned,
}

struct CustomMutationTask {
    handle: JoinHandle<()>,
    phase: CustomMutationTaskPhase,
}

enum CustomMutationTaskPhase {
    Running,
    Exited(CustomMutationTaskExit),
}

enum CustomMutationTaskExit {
    Complete,
    Interrupted,
}

enum CustomMutationObservation {
    Complete,
    Notify,
    Yield,
}

struct CustomMutationGuard {
    owner: Arc<CustomMutationOwner>,
    id: u64,
    state: CustomMutationGuardState,
}

enum CustomMutationGuardState {
    Active,
    Complete,
}

impl CustomMutationOwner {
    pub(super) fn new() -> Self {
        Self {
            slot: Mutex::new(CustomMutationSlot::Idle { next_id: 1 }),
            changed: Notify::new(),
        }
    }

    pub(super) async fn run_before<T, F>(
        self: &Arc<Self>,
        admission_deadline: Instant,
        observation_deadline: Option<Instant>,
        future: F,
    ) -> Result<T, SdkError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, SdkError>> + Send + 'static,
    {
        let id = loop {
            self.settle_before(admission_deadline, None).await?;
            if let Some(id) = self.reserve()? {
                break id;
            }
        };
        if Instant::now() >= admission_deadline {
            self.release_installation(id)?;
            return Err(SdkError::timeout(
                "state mutation could not start before its deadline",
            ));
        }
        let (outcome, result) = oneshot::channel();
        let owner = Arc::clone(self);
        let (start, ready) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let guard = CustomMutationGuard {
                owner,
                id,
                state: CustomMutationGuardState::Active,
            };
            if ready.await.is_err() {
                return;
            }
            let result = future.await;
            let _ = outcome.send(result);
            guard.complete();
        });
        self.install(id, handle)?;
        start
            .send(())
            .map_err(|_| SdkError::invalid_state("state mutation task could not start"))?;
        self.settle(observation_deadline, Some(id)).await?;
        result
            .await
            .map_err(|_| SdkError::invalid_state("state mutation completed without a result"))?
    }

    pub(super) async fn settle_before(
        &self,
        deadline: Instant,
        expected: Option<u64>,
    ) -> Result<(), SdkError> {
        self.settle(Some(deadline), expected).await
    }

    async fn settle(
        &self,
        deadline: Option<Instant>,
        expected: Option<u64>,
    ) -> Result<(), SdkError> {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            let observation = {
                let mut slot = self
                    .slot
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match &mut *slot {
                    CustomMutationSlot::Idle { .. } => CustomMutationObservation::Complete,
                    CustomMutationSlot::Poisoned => {
                        return Err(SdkError::invalid_state("state mutation owner is poisoned"));
                    }
                    CustomMutationSlot::Installing { id, .. }
                    | CustomMutationSlot::ExitedBeforeInstall { id, .. }
                        if expected.is_some_and(|expected| expected != *id) =>
                    {
                        CustomMutationObservation::Complete
                    }
                    CustomMutationSlot::Installing { .. }
                    | CustomMutationSlot::ExitedBeforeInstall { .. } => {
                        CustomMutationObservation::Notify
                    }
                    CustomMutationSlot::Running { id, .. }
                        if expected.is_some_and(|expected| expected != *id) =>
                    {
                        CustomMutationObservation::Complete
                    }
                    CustomMutationSlot::Running { task, .. } if task.handle().is_finished() => {
                        let joined = task.poll_join();
                        let Some(joined) = joined else {
                            return Err(SdkError::invalid_state(
                                "finished state mutation task could not be joined",
                            ));
                        };
                        let previous = std::mem::replace(&mut *slot, CustomMutationSlot::Poisoned);
                        let CustomMutationSlot::Running { next_id, task, .. } = previous else {
                            return Err(SdkError::invalid_state(
                                "state mutation owner changed during settlement",
                            ));
                        };
                        if joined.is_err() || !task.completed() {
                            return Err(SdkError::integration(
                                "state mutation task terminated unexpectedly",
                            ));
                        }
                        *slot = CustomMutationSlot::Idle { next_id };
                        CustomMutationObservation::Complete
                    }
                    CustomMutationSlot::Running { task, .. }
                        if matches!(&task.phase, CustomMutationTaskPhase::Exited(_)) =>
                    {
                        CustomMutationObservation::Yield
                    }
                    CustomMutationSlot::Running { .. } => CustomMutationObservation::Notify,
                }
            };
            match observation {
                CustomMutationObservation::Complete => {
                    self.changed.notify_waiters();
                    return Ok(());
                }
                CustomMutationObservation::Notify => match deadline {
                    Some(deadline) => timeout_at(deadline, changed).await.map_err(|_| {
                        SdkError::timeout("state mutation did not settle before its deadline")
                    })?,
                    None => changed.await,
                },
                CustomMutationObservation::Yield => {
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return Err(SdkError::timeout(
                            "state mutation did not settle before its deadline",
                        ));
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    fn reserve(&self) -> Result<Option<u64>, SdkError> {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let CustomMutationSlot::Idle { next_id } = *slot else {
            return if matches!(*slot, CustomMutationSlot::Poisoned) {
                Err(SdkError::invalid_state("state mutation owner is poisoned"))
            } else {
                Ok(None)
            };
        };
        let following = next_id
            .checked_add(1)
            .ok_or_else(|| SdkError::invalid_state("state mutation identity exhausted"))?;
        *slot = CustomMutationSlot::Installing {
            id: next_id,
            next_id: following,
        };
        Ok(Some(next_id))
    }

    fn install(&self, id: u64, handle: JoinHandle<()>) -> Result<(), SdkError> {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *slot, CustomMutationSlot::Poisoned);
        match previous {
            CustomMutationSlot::Installing {
                id: installing,
                next_id,
            } if installing == id => {
                *slot = CustomMutationSlot::Running {
                    id,
                    next_id,
                    task: CustomMutationTask {
                        handle,
                        phase: CustomMutationTaskPhase::Running,
                    },
                };
            }
            CustomMutationSlot::ExitedBeforeInstall {
                id: installing,
                next_id,
                exit,
            } if installing == id => {
                *slot = CustomMutationSlot::Running {
                    id,
                    next_id,
                    task: CustomMutationTask {
                        handle,
                        phase: CustomMutationTaskPhase::Exited(exit),
                    },
                };
            }
            previous => {
                *slot = previous;
                return Err(SdkError::invalid_state(
                    "state mutation owner changed during task installation",
                ));
            }
        }
        drop(slot);
        self.changed.notify_waiters();
        Ok(())
    }

    fn release_installation(&self, id: u64) -> Result<(), SdkError> {
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *slot, CustomMutationSlot::Poisoned);
        let CustomMutationSlot::Installing {
            id: installing,
            next_id,
        } = previous
        else {
            return Err(SdkError::invalid_state(
                "state mutation owner changed while releasing an installation",
            ));
        };
        if installing != id {
            return Err(SdkError::invalid_state(
                "state mutation identity changed while releasing an installation",
            ));
        }
        *slot = CustomMutationSlot::Idle { next_id };
        drop(slot);
        self.changed.notify_waiters();
        Ok(())
    }

    fn finish(&self, id: u64, guard: CustomMutationGuardState) {
        let exit = match guard {
            CustomMutationGuardState::Active => CustomMutationTaskExit::Interrupted,
            CustomMutationGuardState::Complete => CustomMutationTaskExit::Complete,
        };
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::mem::replace(&mut *slot, CustomMutationSlot::Poisoned);
        *slot = match previous {
            CustomMutationSlot::Installing {
                id: installing,
                next_id,
            } if installing == id => CustomMutationSlot::ExitedBeforeInstall { id, next_id, exit },
            CustomMutationSlot::Running {
                id: active,
                next_id,
                task:
                    CustomMutationTask {
                        handle,
                        phase: CustomMutationTaskPhase::Running,
                    },
            } if active == id => CustomMutationSlot::Running {
                id,
                next_id,
                task: CustomMutationTask {
                    handle,
                    phase: CustomMutationTaskPhase::Exited(exit),
                },
            },
            previous => previous,
        };
        drop(slot);
        self.changed.notify_waiters();
    }
}

impl CustomMutationTask {
    fn handle(&self) -> &JoinHandle<()> {
        &self.handle
    }

    fn poll_join(&mut self) -> Option<Result<(), tokio::task::JoinError>> {
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        match Pin::new(&mut self.handle).poll(&mut context) {
            Poll::Ready(joined) => Some(joined),
            Poll::Pending => None,
        }
    }

    fn completed(&self) -> bool {
        matches!(
            &self.phase,
            CustomMutationTaskPhase::Exited(CustomMutationTaskExit::Complete)
        )
    }
}

impl CustomMutationGuard {
    fn complete(mut self) {
        self.state = CustomMutationGuardState::Complete;
    }
}

impl Drop for CustomMutationGuard {
    fn drop(&mut self) {
        let state = std::mem::replace(&mut self.state, CustomMutationGuardState::Active);
        self.owner.finish(self.id, state);
    }
}
