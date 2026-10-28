//! Model-bound batch submission and provider-owned lifecycle values.
//!
//! Submit [`BatchSubmitRequest`] through an exact [`crate::ModelBackend`]; use the
//! Provider for status, cancellation, listing, and bounded result retrieval.
//! [`BatchInput::custom_id`](crate::batch::BatchInput::custom_id) preserves caller correlation. A completed [`BatchJob`]
//! does not imply every [`BatchResult`] succeeded. Failed rows retain explicit
//! remote evidence as data; download and normalization failures remain outer errors.
//!
//! # Observe or cancel an owned job
//!
//! Submission belongs to [`crate::ModelBackend::submit_batch`]; observation belongs
//! to its Provider. Retain the exact returned job id in application durable state
//! before waiting. The following service example resumes **an already owned job**
//! without creating another. It requires OpenAI credentials and that exact job id.
//!
//! Set `cancel_requested` to request cancellation once. The returned acknowledgement
//! is not proof of terminality: both paths keep polling the exact job under one
//! application deadline. Completion is per job; each result row has its own outcome.
//!
//! ```no_run
//! use std::{num::NonZeroUsize, time::Duration};
//! use providers_kit::{Provider, ProviderBackend, ProviderConfig};
//! use providers_kit::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
//! use providers_kit::batch::{BatchResult, BatchStatus};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
//!     TransportConfig::default(),
//! ))?;
//! let owned_job_id = "replace-with-your-recorded-job-id".to_owned();
//! let cancel_requested = false;
//! let work = tokio::time::timeout(Duration::from_secs(24 * 60 * 60), async {
//!     if cancel_requested {
//!         let acknowledgement = provider.cancel_batch(owned_job_id.clone()).await?;
//!         assert_eq!(acknowledgement.id, owned_job_id);
//!     }
//!     let terminal = loop {
//!         let job = provider.batch_status(owned_job_id.clone()).await?;
//!         assert_eq!(job.id, owned_job_id);
//!         match job.status {
//!             BatchStatus::Queued | BatchStatus::Running => {
//!                 tokio::time::sleep(Duration::from_secs(60)).await;
//!             }
//!             _ => break job,
//!         }
//!     };
//!     match terminal.status {
//!         BatchStatus::Completed => {
//!             let rows = provider.batch_results(owned_job_id.clone(), NonZeroUsize::new(8 * 1024 * 1024).unwrap()).await?;
//!             for row in rows {
//!                 match row {
//!                     BatchResult::Succeeded { custom_id, output } => {
//!                         // Reconcile custom_id against the retained submission inventory.
//!                         println!("{custom_id}: {:?}", output.finish_reason);
//!                     }
//!                     BatchResult::Failed { custom_id, error } => println!("{custom_id}: {:?}", error.kind()),
//!                     BatchResult::Cancelled { custom_id } => println!("{custom_id}: cancelled"),
//!                     BatchResult::Expired { custom_id } => println!("{custom_id}: expired"),
//!                 }
//!             }
//!         }
//!         status => println!("Owned job reached {status:?}"),
//!     }
//!     Ok::<(), providers_kit::KitError>(())
//! }).await;
//! let shutdown = provider.shutdown().await;
//! // On timeout/error, retain owned_job_id for later reconciliation. Never infer
//! // remote deletion or cancellation from shutdown or from dropping this waiter.
//! work??;
//! shutdown?;
//! # Ok(())
//! # }
//! ```
//!
//! The portable job does not expose every provider-generated file identity. Remote
//! artifact reconciliation remains separate; delete only independently established
//! owned file ids via [`crate::ProviderBackend::delete_file`]. No example here claims
//! that terminal job status or local Provider shutdown deletes those files.
//!
//! OpenAI `batch_status` uses the configured bounded transport retry policy for
//! eligible status-read failures. All attempts share the original request deadline;
//! zero retries disables them. The internal lookup in `batch_results` remains
//! fail-fast, as do mutations. Polling above is an application lifecycle decision,
//! not a generation retry or a reset of a request deadline.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use serde::{Deserialize, Deserializer, Serialize};

use crate::KitError;
use crate::error::RemoteFailure;
use crate::error::ValidationViolation;
use crate::generation::{GenerateRequest, GenerateResponse, GenerationContinuation, Usage};

/// One validated generation row in an inline batch.
///
/// # Examples
///
/// ```
/// use providers_kit::{batch::BatchInput, generation::{GenerateRequest, Message}};
/// let row = BatchInput::new("row-1", GenerateRequest::<()>::new(vec![Message::user_text("Summarize")?])?)?;
/// assert_eq!(row.custom_id(), "row-1");
/// assert_eq!(row.request().messages().len(), 1);
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(bound(serialize = ""), deny_unknown_fields)]
pub struct BatchInput<C = GenerationContinuation> {
    custom_id: String,
    request: GenerateRequest<C>,
}

impl<'de, C> Deserialize<'de> for BatchInput<C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(bound(deserialize = ""), deny_unknown_fields)]
        struct Wire<C> {
            custom_id: String,
            request: GenerateRequest<C>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.custom_id, wire.request).map_err(serde::de::Error::custom)
    }
}

impl<C> BatchInput<C> {
    /// Creates a row with a non-empty caller correlation identifier.
    ///
    /// # Errors
    ///
    /// Returns an input error for an empty or whitespace-only correlation id. Submission additionally validates uniqueness across rows.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::{batch::BatchInput, generation::{GenerateRequest, Message}};
    /// let row = BatchInput::new("row-1", GenerateRequest::<()>::new(vec![Message::user_text("Summarize")?])?)?;
    /// assert_eq!(row.custom_id(), "row-1");
    /// assert_eq!(row.request().messages().len(), 1);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn new(
        custom_id: impl Into<String>,
        request: GenerateRequest<C>,
    ) -> Result<Self, KitError> {
        let custom_id = custom_id.into();
        if custom_id.trim().is_empty() {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "batch.custom_id",
                },
                "batch custom id must not be empty",
            ));
        }
        Ok(Self { custom_id, request })
    }

    #[must_use]
    /// Returns the caller-provided correlation identifier.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::{batch::BatchInput, generation::{GenerateRequest, Message}};
    /// let row = BatchInput::new("row-1", GenerateRequest::<()>::new(vec![Message::user_text("Summarize")?])?)?;
    /// assert_eq!(row.custom_id(), "row-1");
    /// assert_eq!(row.request().messages().len(), 1);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn custom_id(&self) -> &str {
        &self.custom_id
    }

    #[must_use]
    /// Returns the generation request for this row.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::{batch::BatchInput, generation::{GenerateRequest, Message}};
    /// let row = BatchInput::new("row-1", GenerateRequest::<()>::new(vec![Message::user_text("Summarize")?])?)?;
    /// assert_eq!(row.custom_id(), "row-1");
    /// assert_eq!(row.request().messages().len(), 1);
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub const fn request(&self) -> &GenerateRequest<C> {
        &self.request
    }

    fn try_map_continuation<D>(
        self,
        map: &mut impl FnMut(C) -> Result<D, KitError>,
    ) -> Result<BatchInput<D>, KitError> {
        Ok(BatchInput {
            custom_id: self.custom_id,
            request: self.request.try_map_continuation(map)?,
        })
    }
}

/// Model-bound batch input source.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(
    bound(serialize = ""),
    tag = "source",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum BatchSubmitRequest<C = GenerationContinuation> {
    /// Inline normalized rows.
    Inline {
        /// Validated rows to submit.
        rows: Vec<BatchInput<C>>,
    },
    /// Existing provider-native input file.
    ProviderFile {
        /// Exact provider-native file identifier.
        file_id: String,
    },
}

impl<'de, C> Deserialize<'de> for BatchSubmitRequest<C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(
            bound(deserialize = ""),
            tag = "source",
            rename_all = "snake_case",
            deny_unknown_fields
        )]
        enum Wire<C> {
            Inline { rows: Vec<BatchInput<C>> },
            ProviderFile { file_id: String },
        }
        let request = match Wire::deserialize(deserializer)? {
            Wire::Inline { rows } => Self::Inline { rows },
            Wire::ProviderFile { file_id } => Self::ProviderFile { file_id },
        };
        request.validate().map_err(serde::de::Error::custom)?;
        Ok(request)
    }
}

impl<C> BatchSubmitRequest<C> {
    /// Maps continuation values carried by inline rows.
    ///
    /// # Errors
    ///
    /// Returns the first mapping error unchanged. File-backed requests have no inline continuation values to map.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::{batch::BatchInput, generation::{GenerateRequest, Message}};
    /// let row = BatchInput::new("row-1", GenerateRequest::<()>::new(vec![Message::user_text("Summarize")?])?)?;
    /// use providers_kit::batch::BatchSubmitRequest;
    /// let batch = BatchSubmitRequest::Inline { rows: vec![row] };
    /// let mapped: BatchSubmitRequest<String> = batch.try_map_continuation(|()| Ok("state".into()))?;
    /// assert!(matches!(mapped, BatchSubmitRequest::Inline { rows } if rows[0].custom_id() == "row-1"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn try_map_continuation<D>(
        self,
        mut map: impl FnMut(C) -> Result<D, KitError>,
    ) -> Result<BatchSubmitRequest<D>, KitError> {
        Ok(match self {
            Self::Inline { rows } => BatchSubmitRequest::Inline {
                rows: rows
                    .into_iter()
                    .map(|row| row.try_map_continuation(&mut map))
                    .collect::<Result<_, _>>()?,
            },
            Self::ProviderFile { file_id } => BatchSubmitRequest::ProviderFile { file_id },
        })
    }

    pub(crate) fn validate(&self) -> Result<(), KitError> {
        match self {
            Self::Inline { rows } => {
                if rows.is_empty() {
                    return Err(KitError::invalid_input(
                        ValidationViolation::Empty {
                            field: "batch.rows",
                        },
                        "inline batch rows must not be empty",
                    ));
                }
                let mut ids = BTreeSet::new();
                for row in rows {
                    if !ids.insert(row.custom_id()) {
                        return Err(KitError::invalid_input(
                            ValidationViolation::Duplicate {
                                field: "batch.custom_id",
                            },
                            format!("duplicate batch custom id `{}`", row.custom_id()),
                        ));
                    }
                }
                Ok(())
            }
            Self::ProviderFile { file_id } if file_id.trim().is_empty() => {
                Err(KitError::invalid_input(
                    ValidationViolation::Empty {
                        field: "batch.file_id",
                    },
                    "batch file id must not be empty",
                ))
            }
            Self::ProviderFile { .. } => Ok(()),
        }
    }
}

/// Provider-authored batch lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BatchStatus {
    /// The provider accepted the batch and has not started processing it.
    Queued,
    /// The provider is processing the batch.
    Running,
    /// Every batch row reached a terminal state.
    Completed,
    /// The provider failed the batch.
    Failed {
        /// Provider-authored failures retained without exposing their native envelope.
        errors: Vec<RemoteFailure>,
    },
    /// The batch was cancelled.
    Cancelled,
    /// The batch expired before completion.
    Expired,
}

/// Provider-owned batch job metadata.
///
/// A terminal job can contain failed, cancelled, or expired rows. Inspect each
/// [`BatchResult`] separately; cancellation success does not prove row success.
///
/// # Examples
///
/// ```
/// use providers_kit::batch::{BatchJob, BatchStatus};
/// let job = BatchJob { id: "owned-job".into(), status: BatchStatus::Queued, created_at: None, completed_at: None, usage: None };
/// assert!(matches!(job.status, BatchStatus::Queued));
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchJob {
    /// Exact provider-native batch identifier.
    pub id: String,
    /// Current provider-authored lifecycle status.
    pub status: BatchStatus,
    /// Provider creation timestamp, when reported.
    pub created_at: Option<String>,
    /// Provider completion timestamp, when reported.
    pub completed_at: Option<String>,
    /// Aggregate token usage, when reported.
    pub usage: Option<Usage>,
}

/// One terminal batch row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    bound(serialize = "", deserialize = ""),
    tag = "status",
    rename_all = "snake_case"
)]
pub enum BatchResult<C = GenerationContinuation> {
    /// A row completed successfully.
    Succeeded {
        /// Caller-provided row correlation identifier.
        custom_id: String,
        /// Normalized generation output.
        output: Box<GenerateResponse<C>>,
    },
    /// A row failed.
    Failed {
        /// Caller-provided row correlation identifier.
        custom_id: String,
        /// Provider-authored row failure with explicitly accessed native evidence.
        error: RemoteFailure,
    },
    /// A row was cancelled.
    Cancelled {
        /// Caller-provided row correlation identifier.
        custom_id: String,
    },
    /// A row expired before completion.
    Expired {
        /// Caller-provided row correlation identifier.
        custom_id: String,
    },
}

impl<C> BatchResult<C> {
    /// Maps continuation state carried by a successful row.
    ///
    /// # Errors
    ///
    /// Returns the mapping error for a successful row continuation. Failure, cancellation, and expiration rows retain their exact correlation and evidence.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::batch::BatchResult;
    /// let row: BatchResult<()> = BatchResult::Cancelled { custom_id: "row-1".into() };
    /// let mapped: BatchResult<String> = row.try_map_continuation(|()| Ok("state".into()))?;
    /// assert!(matches!(mapped, BatchResult::Cancelled { custom_id } if custom_id == "row-1"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn try_map_continuation<D>(
        self,
        map: impl FnMut(C) -> Result<D, KitError>,
    ) -> Result<BatchResult<D>, KitError> {
        Ok(match self {
            Self::Succeeded { custom_id, output } => BatchResult::Succeeded {
                custom_id,
                output: Box::new((*output).try_map_continuation(map)?),
            },
            Self::Failed { custom_id, error } => BatchResult::Failed { custom_id, error },
            Self::Cancelled { custom_id } => BatchResult::Cancelled { custom_id },
            Self::Expired { custom_id } => BatchResult::Expired { custom_id },
        })
    }
}

/// Validated batch-list pagination request.
///
/// Pagination cursors are opaque provider-owned values; a requested limit is not a
/// guarantee that every provider supports the same pagination controls.
///
/// # Examples
///
/// ```
/// use providers_kit::batch::BatchListRequest;
/// let request = BatchListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
/// assert_eq!(request.limit().unwrap().get(), 10);
/// assert_eq!(request.cursor(), Some("opaque-next"));
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct BatchListRequest {
    limit: Option<NonZeroUsize>,
    cursor: Option<String>,
}

impl BatchListRequest {
    /// Sets a non-zero result limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::batch::BatchListRequest;
    /// let request = BatchListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
    /// assert_eq!(request.limit().unwrap().get(), 10);
    /// assert_eq!(request.cursor(), Some("opaque-next"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets a non-empty provider-native continuation cursor.
    ///
    /// # Errors
    ///
    /// Returns an input error when the cursor is empty or whitespace-only. Nonblank cursor bytes are retained exactly; use only a cursor returned by the same provider operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::batch::BatchListRequest;
    /// let request = BatchListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
    /// assert_eq!(request.limit().unwrap().get(), 10);
    /// assert_eq!(request.cursor(), Some("opaque-next"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Result<Self, KitError> {
        let cursor = cursor.into();
        if cursor.trim().is_empty() {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "batch.cursor",
                },
                "batch cursor must not be empty",
            ));
        }
        self.cursor = Some(cursor);
        Ok(self)
    }

    /// Returns the requested result limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::batch::BatchListRequest;
    /// let request = BatchListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
    /// assert_eq!(request.limit().unwrap().get(), 10);
    /// assert_eq!(request.cursor(), Some("opaque-next"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub const fn limit(&self) -> Option<NonZeroUsize> {
        self.limit
    }

    /// Returns the provider-native continuation cursor.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::batch::BatchListRequest;
    /// let request = BatchListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
    /// assert_eq!(request.limit().unwrap().get(), 10);
    /// assert_eq!(request.cursor(), Some("opaque-next"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Consumes the request into its limit and continuation cursor.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::batch::BatchListRequest;
    /// let request = BatchListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
    /// let (limit, cursor) = request.into_parts();
    /// assert_eq!(limit.unwrap().get(), 10);
    /// assert_eq!(cursor.as_deref(), Some("opaque-next"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn into_parts(self) -> (Option<NonZeroUsize>, Option<String>) {
        (self.limit, self.cursor)
    }
}

impl<'de> Deserialize<'de> for BatchListRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            limit: Option<NonZeroUsize>,
            cursor: Option<String>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut request = Self {
            limit: wire.limit,
            cursor: None,
        };
        if let Some(cursor) = wire.cursor {
            request = request
                .with_cursor(cursor)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(request)
    }
}

/// One page of provider batch jobs.
///
/// # Examples
///
/// ```
/// let page = providers_kit::batch::BatchPage { jobs: Vec::new(), next_cursor: None };
/// assert!(page.jobs.is_empty());
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchPage {
    /// Batch jobs returned in this page.
    pub jobs: Vec<BatchJob>,
    /// Provider-native cursor for a subsequent page.
    pub next_cursor: Option<String>,
}
