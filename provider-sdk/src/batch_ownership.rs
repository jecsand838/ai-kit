//! Private ownership of supplied inline batch submissions. No backend erasure.

use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, oneshot};
use tokio::task::{JoinError, JoinSet};

use crate::SdkError;
use crate::batch::BatchJob;
use crate::error::{ErrorPhase, NativeDetail, Operation};
use crate::provider::ProviderAdmission;
use crate::providers::openai::OpenAiBatchSubmission;

pub(crate) struct BatchRegistry {
    tasks: JoinSet<()>,
    receipts: Vec<Arc<BatchReceipt>>,
    payload_bytes: usize,
    failure: Option<SdkError>,
}

impl BatchRegistry {
    const MAX_TASKS: usize = 16;
    const MAX_BYTES: usize = 400 * 1024 * 1024;
    const MAX_RECEIPTS: usize = 64;

    pub(crate) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
            receipts: Vec::new(),
            payload_bytes: 0,
            failure: None,
        }
    }

    pub(crate) fn reserve(&mut self, bytes: usize) -> Result<(), SdkError> {
        while let Some(result) = self.tasks.try_join_next() {
            self.joined(result);
        }
        self.receipts.retain(|receipt| !receipt.claimed());
        if self.tasks.len() >= Self::MAX_TASKS
            || self.receipts.len() >= Self::MAX_RECEIPTS
            || bytes > Self::MAX_BYTES.saturating_sub(self.payload_bytes)
        {
            return Err(
                SdkError::invalid_state("inline batch admission capacity is exhausted")
                    .at_operation(Operation::SubmitBatch),
            );
        }
        self.payload_bytes += bytes;
        Ok(())
    }

    pub(crate) fn release(&mut self, bytes: usize) {
        debug_assert!(self.payload_bytes >= bytes);
        self.payload_bytes = self.payload_bytes.saturating_sub(bytes);
    }

    pub(crate) fn register(
        &mut self,
        submission: OpenAiBatchSubmission,
        runtime: &tokio::runtime::Handle,
    ) -> (BatchObserver, oneshot::Sender<ProviderAdmission>) {
        let receipt = Arc::new(BatchReceipt::new());
        self.receipts.push(Arc::clone(&receipt));
        let (activate, activated) = oneshot::channel();
        let publication = BatchPublication {
            receipt: Arc::clone(&receipt),
            admission: None,
            activation: activated,
            published: false,
        };
        // The publication guard exists before spawning, including when a dying
        // runtime destroys the task before its first poll. Admission is handed
        // over only after registration locks have been released: a synchronous
        // task destruction during spawn cannot recursively lock the runtime.
        self.tasks.spawn_on(
            async move {
                let mut publication = publication;
                let Ok(admission) = (&mut publication.activation).await else {
                    return;
                };
                publication.admission = Some(admission);
                let result = submission.execute(&publication.receipt).await;
                publication.complete(result);
            },
            runtime,
        );
        (BatchObserver { receipt }, activate)
    }

    pub(crate) fn take_tasks(&mut self) -> JoinSet<()> {
        std::mem::take(&mut self.tasks)
    }

    pub(crate) fn joined(&mut self, result: Result<(), JoinError>) {
        if result.is_err() {
            self.record(
                SdkError::invalid_state("inline batch owner could not be joined")
                    .at_operation(Operation::SubmitBatch)
                    .at_phase(ErrorPhase::Cleanup),
            );
        }
    }

    fn record(&mut self, error: SdkError) {
        self.failure = Some(match self.failure.take() {
            Some(primary) => primary.with_supplementary(error),
            None => error,
        });
    }

    pub(crate) fn terminal(&mut self) -> Result<(), SdkError> {
        let receipts = std::mem::take(&mut self.receipts);
        let mut resources = Vec::new();
        for receipt in receipts {
            if let Some((error, resource)) = receipt.report() {
                resources.extend(resource);
                self.record(error);
            }
        }
        match self.failure.take() {
            Some(error) if !resources.is_empty() => {
                // The public error tree bounds supplementary nodes below the
                // receipt count. Native resource records retain all ordinary
                // file/job IDs under the existing independent record bound.
                let mut details = error.native_details().clone();
                resources.append(&mut details.details);
                details.details = resources;
                Err(error.with_native_capture(details))
            }
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

pub(crate) struct BatchReceipt {
    state: Mutex<ReceiptState>,
    changed: Notify,
}

enum ReceiptState {
    Pending {
        observed: bool,
        resource: Option<SdkError>,
    },
    Ready {
        observed: bool,
        result: Result<BatchJob, SdkError>,
        resource: Option<SdkError>,
    },
    Claimed,
    Reported(SdkError),
}

impl BatchReceipt {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ReceiptState::Pending {
                observed: true,
                resource: None,
            }),
            changed: Notify::new(),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ReceiptState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn abandoned(&self) -> bool {
        match &*self.state() {
            ReceiptState::Pending { observed, .. } | ReceiptState::Ready { observed, .. } => {
                !observed
            }
            ReceiptState::Claimed | ReceiptState::Reported(_) => true,
        }
    }

    pub(crate) fn checkpoint(&self, resource: Option<SdkError>) {
        if let ReceiptState::Pending {
            resource: stored, ..
        } = &mut *self.state()
        {
            *stored = resource;
        }
    }

    fn publish(&self, result: Result<BatchJob, SdkError>) {
        let mut state = self.state();
        let ReceiptState::Pending { observed, resource } = &mut *state else {
            return;
        };
        let observed = *observed;
        let resource = resource.take();
        *state = ReceiptState::Ready {
            observed,
            result,
            resource,
        };
        drop(state);
        self.changed.notify_waiters();
    }

    fn failed_worker(&self) {
        let mut state = self.state();
        let ReceiptState::Pending { observed, resource } = &mut *state else {
            return;
        };
        let observed = *observed;
        let resource = resource.take();
        let error =
            SdkError::invalid_state("inline batch owner ended before publishing its outcome")
                .at_operation(Operation::SubmitBatch)
                .at_phase(ErrorPhase::Cleanup);
        let error = match resource.as_ref() {
            Some(resource) => error.with_supplementary(resource.clone()),
            None => error,
        };
        *state = ReceiptState::Ready {
            observed,
            result: Err(error),
            resource,
        };
        drop(state);
        self.changed.notify_waiters();
    }

    fn claimed(&self) -> bool {
        matches!(*self.state(), ReceiptState::Claimed)
    }

    fn abandon(&self) {
        match &mut *self.state() {
            ReceiptState::Pending { observed, .. } | ReceiptState::Ready { observed, .. } => {
                *observed = false
            }
            ReceiptState::Claimed | ReceiptState::Reported(_) => {}
        }
    }

    fn claim(&self) -> Option<Result<BatchJob, SdkError>> {
        let mut state = self.state();
        match &*state {
            ReceiptState::Pending { .. } => return None,
            ReceiptState::Reported(error) => return Some(Err(error.clone())),
            ReceiptState::Claimed => {
                return Some(Err(SdkError::invalid_state(
                    "batch receipt was already claimed",
                )
                .at_operation(Operation::SubmitBatch)));
            }
            ReceiptState::Ready { .. } => {}
        }
        let ReceiptState::Ready { result, .. } =
            std::mem::replace(&mut *state, ReceiptState::Claimed)
        else {
            unreachable!()
        };
        Some(result)
    }

    fn report(&self) -> Option<(SdkError, Vec<NativeDetail>)> {
        let mut state = self.state();
        let resource = match &*state {
            ReceiptState::Pending { resource, .. } | ReceiptState::Ready { resource, .. } => {
                resource
                    .as_ref()
                    .map(|error| error.native_details().details.clone())
                    .unwrap_or_default()
            }
            ReceiptState::Claimed | ReceiptState::Reported(_) => Vec::new(),
        };
        let error = match &*state {
            ReceiptState::Claimed => return None,
            ReceiptState::Reported(error) => return Some((error.clone(), resource)),
            ReceiptState::Pending { resource, .. } => {
                let error =
                    SdkError::invalid_state("batch owner joined without a terminal receipt")
                        .at_operation(Operation::SubmitBatch)
                        .at_phase(ErrorPhase::Cleanup);
                match resource {
                    Some(resource) => error.with_supplementary(resource.clone()),
                    None => error,
                }
            }
            ReceiptState::Ready {
                result: Err(error), ..
            } => error.clone(),
            ReceiptState::Ready {
                result: Ok(_),
                resource: Some(resource),
                ..
            } => resource.clone(),
            ReceiptState::Ready {
                result: Ok(_),
                resource: None,
                ..
            } => SdkError::invalid_state("batch result was not observed before provider shutdown")
                .at_operation(Operation::SubmitBatch)
                .at_phase(ErrorPhase::Cleanup),
        };
        // A receipt reported by shutdown cannot subsequently become a claimed
        // success that contradicts the already replayable shutdown terminal.
        *state = ReceiptState::Reported(error.clone());
        drop(state);
        self.changed.notify_waiters();
        Some((error, resource))
    }
}

pub(crate) struct BatchObserver {
    receipt: Arc<BatchReceipt>,
}

impl BatchObserver {
    pub(crate) async fn wait(self) -> Result<BatchJob, SdkError> {
        loop {
            let notified = self.receipt.changed.notified();
            if let Some(result) = self.receipt.claim() {
                return result;
            }
            notified.await;
        }
    }
}

impl Drop for BatchObserver {
    fn drop(&mut self) {
        self.receipt.abandon();
    }
}

struct BatchPublication {
    receipt: Arc<BatchReceipt>,
    admission: Option<ProviderAdmission>,
    // Own the receiver inside the guard as well: if activation was sent but
    // never polled, Drop publishes before this receiver releases its admission.
    activation: oneshot::Receiver<ProviderAdmission>,
    published: bool,
}

impl BatchPublication {
    fn complete(mut self, result: Result<BatchJob, SdkError>) {
        self.receipt.publish(result);
        self.published = true;
    }
}

impl Drop for BatchPublication {
    fn drop(&mut self) {
        if !self.published {
            self.receipt.failed_worker();
        }
    }
}
