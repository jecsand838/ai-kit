use tokio::sync::watch;

use crate::app::FinalResult;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum DrainPolicy {
    RejectIfActive,
    Wait,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ShutdownMode {
    Drain,
    Cancel,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ShutdownRequest {
    Drain(DrainPolicy),
    Cancel,
}

impl ShutdownRequest {
    pub(super) fn mode(self) -> ShutdownMode {
        match self {
            Self::Drain(_) => ShutdownMode::Drain,
            Self::Cancel => ShutdownMode::Cancel,
        }
    }

    pub(super) fn rejects_active(self) -> bool {
        matches!(self, Self::Drain(DrainPolicy::RejectIfActive))
    }
}

impl From<ShutdownMode> for ShutdownRequest {
    fn from(mode: ShutdownMode) -> Self {
        match mode {
            ShutdownMode::Drain => Self::Drain(DrainPolicy::Wait),
            ShutdownMode::Cancel => Self::Cancel,
        }
    }
}

pub(super) struct Finalization {
    pub(super) failure: Option<FinalResult>,
    pub(super) stage: FinalizationStage,
    pub(super) published_mode: Option<ShutdownMode>,
    pub(super) superseded: bool,
    pub(super) dropping: bool,
}

#[derive(Clone, Copy)]
pub(super) enum FinalizationStage {
    Tasks,
    Agents,
    Publication,
    Publishing,
    Complete,
}

impl Finalization {
    pub(super) fn new(dropping: bool) -> Self {
        Self {
            failure: None,
            stage: FinalizationStage::Tasks,
            published_mode: None,
            superseded: false,
            dropping,
        }
    }
}

pub(super) enum AppLifecycle {
    Active,
    Draining {
        policy: DrainPolicy,
        root_dropped: bool,
        completion: watch::Receiver<Option<FinalResult>>,
    },
    Cancelling {
        completion: watch::Receiver<Option<FinalResult>>,
    },
    Finalizing {
        mode: ShutdownMode,
        completion: watch::Receiver<Option<FinalResult>>,
        progress: Finalization,
    },
    Closed {
        outcome: FinalResult,
        dropping: bool,
    },
}

impl AppLifecycle {
    pub(super) fn transition_to_finalizing(&mut self) -> Result<ShutdownMode, FinalResult> {
        let current = std::mem::replace(self, Self::Active);
        match current {
            Self::Draining {
                policy: _,
                completion,
                root_dropped: _,
            } => {
                *self = Self::Finalizing {
                    mode: ShutdownMode::Drain,
                    completion,
                    progress: Finalization::new(false),
                };
                Ok(ShutdownMode::Drain)
            }
            Self::Cancelling { completion } => {
                *self = Self::Finalizing {
                    mode: ShutdownMode::Cancel,
                    completion,
                    progress: Finalization::new(false),
                };
                Ok(ShutdownMode::Cancel)
            }
            current @ Self::Finalizing { mode, .. } => {
                *self = current;
                Ok(mode)
            }
            current @ (Self::Active | Self::Closed { .. }) => {
                *self = current;
                Err(FinalResult::Conflict(
                    "application finalizer lost lifecycle ownership".to_owned(),
                ))
            }
        }
    }

    pub(super) fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    pub(super) fn is_closed(&self) -> bool {
        matches!(self, Self::Closed { .. })
    }

    pub(super) fn accepts_runtime_work(&self) -> bool {
        matches!(
            self,
            Self::Active | Self::Draining { .. } | Self::Cancelling { .. }
        )
    }

    pub(super) fn allows_snapshot_read(&self) -> bool {
        match self {
            Self::Active | Self::Draining { .. } | Self::Cancelling { .. } => true,
            Self::Finalizing { progress, .. } => progress.dropping && progress.failure.is_none(),
            Self::Closed { outcome, dropping } => {
                *dropping && matches!(outcome, FinalResult::Closed(_))
            }
        }
    }

    pub(super) fn shutdown_mode(&self) -> Option<ShutdownMode> {
        match self {
            Self::Draining { .. } => Some(ShutdownMode::Drain),
            Self::Cancelling { .. } => Some(ShutdownMode::Cancel),
            Self::Finalizing { mode, .. } => Some(*mode),
            Self::Active | Self::Closed { .. } => None,
        }
    }

    pub(super) fn shutdown_request(&self) -> Option<ShutdownRequest> {
        match self {
            Self::Draining { policy, .. } => Some(ShutdownRequest::Drain(*policy)),
            Self::Cancelling { .. } => Some(ShutdownRequest::Cancel),
            Self::Finalizing { mode, .. } => Some((*mode).into()),
            Self::Active | Self::Closed { .. } => None,
        }
    }
}
