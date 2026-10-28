//! Exact model identity, discovery values, and the opaque model handle.
//!
//! [`ModelId`] is validated but never normalized, aliased, or replaced by a default.
//! Select a model through [`crate::ProviderBackend::model`]; [`ModelBackend`] owns
//! generation, embeddings, batch submission, and native session opening.
//! Operation errors, not a capability matrix, establish unsupported combinations.
//! Supplied Model clones share their Provider lifecycle and have no separate shutdown.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};

use crate::KitError;
use crate::batch::{BatchJob, BatchSubmitRequest};
use crate::embedding::{EmbedRequest, EmbedResponse};
use crate::error::Operation;
use crate::error::ValidationViolation;
use crate::generation::{
    GenerateEventStream, GenerateRequest, GenerateResponse, GenerationContinuation,
};
use crate::provider::ProviderRuntime;
use crate::providers::{
    anthropic::AnthropicModel, gemini::GeminiModel, ollama::OllamaModel, openai::OpenAiModel,
};
use crate::session::{Session, SessionConfig, SessionInner};

/// Validated exact provider-local model identity.
///
/// Identity is exact and provider-local. No alias, normalization, fallback, or default
/// selection is performed, and a valid id does not prove model availability.
///
/// # Examples
///
/// ```
/// use providers_kit::ModelId;
/// let id = ModelId::new("team/model:revision")?;
/// assert_eq!(id.as_str(), "team/model:revision");
/// assert!(ModelId::new(" padded ").is_err());
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ModelId(String);

impl ModelId {
    /// Validates an exact model id without normalizing it.
    ///
    /// # Errors
    ///
    /// Returns an input error unless the value is 1 through 256 UTF-8 bytes, unpadded, and free of control characters.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::ModelId;
    /// let id = ModelId::new("team/model:revision")?;
    /// assert_eq!(id.as_str(), "team/model:revision");
    /// assert!(ModelId::new(" padded ").is_err());
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn new(value: impl Into<String>) -> Result<Self, KitError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 256
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "model_id",
                    rule: "1..=256 unpadded UTF-8 bytes without control characters",
                },
                "model id must contain 1..=256 unpadded UTF-8 bytes without control characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the exact id.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::ModelId;
    /// let id = ModelId::new("team/model:revision")?;
    /// assert_eq!(id.as_str(), "team/model:revision");
    /// assert!(ModelId::new(" padded ").is_err());
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModelId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ModelId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Validated model-list pagination request.
///
/// Pagination cursors are opaque provider-owned values; a requested limit is not a
/// guarantee that every provider supports the same pagination controls.
///
/// # Examples
///
/// ```
/// use providers_kit::models::ModelListRequest;
/// let request = ModelListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
/// assert_eq!(request.limit().unwrap().get(), 10);
/// assert_eq!(request.cursor(), Some("opaque-next"));
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelListRequest {
    limit: Option<NonZeroUsize>,
    cursor: Option<String>,
}

impl ModelListRequest {
    /// Sets a non-zero result limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::models::ModelListRequest;
    /// let request = ModelListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
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

    /// Sets a non-empty native continuation cursor.
    ///
    /// # Errors
    ///
    /// Returns an input error when the cursor is empty or whitespace-only. Nonblank cursor bytes are retained exactly; use only a cursor returned by the same provider operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::models::ModelListRequest;
    /// let request = ModelListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
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
                    field: "models.cursor",
                },
                "model cursor must not be empty",
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
    /// use providers_kit::models::ModelListRequest;
    /// let request = ModelListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
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
    /// use providers_kit::models::ModelListRequest;
    /// let request = ModelListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
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
    /// use providers_kit::models::ModelListRequest;
    /// let request = ModelListRequest::default().with_limit(std::num::NonZeroUsize::new(10).unwrap()).with_cursor("opaque-next")?;
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

impl<'de> Deserialize<'de> for ModelListRequest {
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

/// Descriptive model metadata.
///
/// # Examples
///
/// ```
/// use providers_kit::models::{ModelId, ModelSummary};
/// let summary = ModelSummary { id: ModelId::new("exact-model")?, display_name: Some("Example".into()), description: None, context_window_tokens: None };
/// assert_eq!(summary.id.as_str(), "exact-model");
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelSummary {
    /// Exact provider-local model identifier.
    pub id: ModelId,
    /// Human-readable model name.
    pub display_name: Option<String>,
    /// Human-readable model description.
    pub description: Option<String>,
    /// Reported maximum context window.
    pub context_window_tokens: Option<u64>,
}

/// One page of provider model metadata.
///
/// # Examples
///
/// ```
/// let page = providers_kit::models::ModelPage { models: Vec::new(), next_cursor: None };
/// assert!(page.models.is_empty());
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelPage {
    /// Models returned in this page.
    pub models: Vec<ModelSummary>,
    /// Native cursor for a subsequent page.
    pub next_cursor: Option<String>,
}

/// Public implementation contract for one exact selected model.
///
/// One model owns an exact [`ModelId`], an associated continuation type, and an
/// associated Session type. Requests do not repeat model selection. Caller models
/// remain concrete; supplied Model clones share their Provider lifecycle.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), providers_kit::KitError> {
/// use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId, config::{ProviderKind, OllamaConfig, TransportConfig}};
/// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
/// let model = provider.model(ModelId::new("exact-model")?)?;
/// assert_eq!(model.model_id().as_str(), "exact-model");
/// provider.shutdown().await?;
///
/// # Ok(())
/// # }
/// ```
///
/// # A sessionless caller model
///
/// Use `Infallible` when no Session can be constructed; its `SessionBackend`
/// implementation is supplied by this crate. Keep the default `open_session`
/// Unsupported result instead of fabricating a session. This deliberately limited
/// caller model rejects generation as well; it demonstrates the associated-type
/// choice, not an implemented model capability. See [`crate::ProviderBackend`] for
/// an executable text-generating caller hierarchy.
///
/// ```
/// use std::convert::Infallible;
/// use providers_kit::{ModelBackend, ModelId, KitError};
/// use providers_kit::generation::{GenerateEventStream, GenerateRequest, GenerateResponse};
/// use providers_kit::error::{ErrorKind, ErrorPhase, Operation};
/// struct Sessionless { id: ModelId }
/// impl ModelBackend for Sessionless {
///     type Continuation = ();
///     type Session = Infallible;
///     fn model_id(&self) -> &ModelId { &self.id }
///     async fn generate(&self, _: &GenerateRequest<()>) -> Result<GenerateResponse<()>, KitError> {
///         Err(KitError::new(ErrorKind::Unsupported, Operation::Generate, ErrorPhase::InputValidation))
///     }
///     async fn generate_stream(&self, _: &GenerateRequest<()>) -> Result<GenerateEventStream<()>, KitError> {
///         Err(KitError::new(ErrorKind::Unsupported, Operation::GenerateStream, ErrorPhase::InputValidation))
///     }
/// }
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), KitError> {
/// let model = Sessionless { id: ModelId::new("sessionless")? };
/// let error = model.open_session(Default::default()).await.unwrap_err();
/// assert_eq!(error.kind(), ErrorKind::Unsupported);
/// # Ok(())
/// # }
/// ```
///
pub trait ModelBackend: Send + Sync + 'static {
    /// Opaque model-authored state retained between generation turns.
    type Continuation: Clone + Send + Sync + 'static;

    /// Stateful child returned by this model.
    type Session: crate::session::SessionBackend;

    /// Exact selected model id.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId, config::{ProviderKind, OllamaConfig, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// let model = provider.model(ModelId::new("exact-model")?)?;
    /// assert_eq!(model.model_id().as_str(), "exact-model");
    /// provider.shutdown().await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn model_id(&self) -> &ModelId;

    /// Runs non-streaming generation.
    ///
    /// This example requires a real service, valid credentials, and an exact model
    /// supporting this operation.
    /// Append the returned assistant message unchanged when continuing with tool results.
    /// Its exact associated continuation is model-owned and remains in that message.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for closed admission, unsupported selected-model behavior, invalid request combinations, transport/deadline failures, service rejection, or malformed response. The operation is authoritative; no capability matrix or fallback model supplies success.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use providers_kit::{ModelBackend, ModelId, generation::{GenerateRequest, Message}};
    /// let model = provider.model(ModelId::new("your-exact-model-id")?)?;
    /// let request = GenerateRequest::new(vec![Message::user_text("Summarize the task")?])?;
    /// let result = model.generate(&request).await;
    /// let cleanup = provider.shutdown().await;
    /// let _response = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn generate<'a>(
        &'a self,
        request: &'a GenerateRequest<Self::Continuation>,
    ) -> impl Future<Output = Result<GenerateResponse<Self::Continuation>, KitError>> + Send + 'a;

    /// Opens a normalized generation stream.
    ///
    /// This example requires a real service, valid credentials, and an exact model
    /// supporting this operation.
    /// The returned stream is demand-driven and bounded, with no forwarding task.
    /// Completion, error, or drop releases admission; no event follows Completed.
    /// The final response is authoritative; partial deltas do not replace it.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for closed admission, unsupported selected-model behavior, invalid request combinations, transport/deadline failures, service rejection, or malformed response. The operation is authoritative; no capability matrix or fallback model supplies success.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use providers_kit::{ModelBackend, ModelId, generation::{GenerateRequest, Message}};
    /// let model = provider.model(ModelId::new("your-exact-model-id")?)?;
    /// let request = GenerateRequest::new(vec![Message::user_text("Summarize the task")?])?;
    /// let result = model.generate_stream(&request).await;
    /// let observed = async {
    ///     use futures_util::StreamExt;
    ///     let mut stream = result?;
    ///     while let Some(event) = stream.next().await { let _event = event?; }
    ///     Ok::<(), providers_kit::KitError>(())
    /// }.await;
    /// let cleanup = provider.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn generate_stream<'a>(
        &'a self,
        request: &'a GenerateRequest<Self::Continuation>,
    ) -> impl Future<Output = Result<GenerateEventStream<Self::Continuation>, KitError>> + Send + 'a;

    /// Runs embeddings when supported.
    ///
    /// This example requires a real service, valid credentials, and an exact model
    /// supporting this operation.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for closed admission, unsupported selected-model behavior, invalid request combinations, transport/deadline failures, service rejection, or malformed response. The operation is authoritative; no capability matrix or fallback model supplies success.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use providers_kit::{ModelBackend, ModelId, generation::{GenerateRequest, Message}};
    /// let model = provider.model(ModelId::new("your-exact-model-id")?)?;
    /// let result = model.embed(&providers_kit::embedding::EmbedRequest::new(vec!["document".into()])?).await;
    /// let cleanup = provider.shutdown().await;
    /// let _response = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn embed<'a>(
        &'a self,
        _request: &'a EmbedRequest,
    ) -> impl Future<Output = Result<EmbedResponse, KitError>> + Send + 'a {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "embedding",
                    rule: "operation is not implemented",
                },
                "model embeddings",
            )
            .at_operation(Operation::Embed))
        }
    }

    /// Submits a model-bound batch when supported.
    ///
    /// Retain the exact returned job id to observe or cancel the remote job. Shared
    /// provider shutdown closes local resources; it does not cancel remote jobs.
    ///
    /// This example requires a real service, valid credentials, and an exact model
    /// supporting this operation.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for closed admission, unsupported selected-model behavior, invalid request combinations, transport/deadline failures, service rejection, or malformed response. The operation is authoritative; no capability matrix or fallback model supplies success.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use providers_kit::{ModelBackend, ModelId, generation::{GenerateRequest, Message}};
    /// let model = provider.model(ModelId::new("your-exact-model-id")?)?;
    /// let request = GenerateRequest::new(vec![Message::user_text("Summarize the task")?])?;
    /// let result = model.submit_batch(&providers_kit::batch::BatchSubmitRequest::Inline { rows: vec![providers_kit::batch::BatchInput::new("row-1", request)?] }).await;
    /// use providers_kit::batch::BatchStatus;
    /// use providers_kit::error::{ErrorKind, ErrorPhase, Operation};
    /// use std::time::Duration;
    /// let observed = match result {
    ///     Ok(job) => {
    ///         let owned_id = job.id; // Persist this id in the application's job inventory.
    ///         let settlement = tokio::time::timeout(Duration::from_secs(11 * 60), async {
    ///             let acknowledgement = provider.cancel_batch(owned_id.clone()).await?;
    ///             assert_eq!(acknowledgement.id, owned_id);
    ///             loop {
    ///                 let status = provider.batch_status(owned_id.clone()).await?;
    ///                 assert_eq!(status.id, owned_id);
    ///                 match status.status {
    ///                     BatchStatus::Queued | BatchStatus::Running =>
    ///                         tokio::time::sleep(Duration::from_secs(5)).await,
    ///                     terminal => break Ok::<_, providers_kit::KitError>(terminal),
    ///                 }
    ///             }
    ///         }).await;
    ///         match settlement {
    ///             Ok(Ok(terminal)) => {
    ///                 // Completed may win the race: this does not prove cancellation.
    ///                 println!("{owned_id}: {terminal:?}");
    ///                 Ok(())
    ///             }
    ///             Ok(Err(error)) => Err(error.with_correlation_id(owned_id)),
    ///             Err(_) => Err(providers_kit::KitError::new(ErrorKind::Timeout,
    ///                 Operation::BatchStatus, ErrorPhase::Cleanup).with_correlation_id(owned_id)),
    ///         }
    ///     }
    ///     Err(error) => Err(error),
    /// };
    /// let cleanup = provider.shutdown().await;
    /// // Keep an unresolved id/error for later reconciliation. Shutdown does not
    /// // delete remote files; see the batch module's artifact-ownership limits.
    /// match (observed, cleanup) {
    ///     (Err(primary), Err(cleanup)) => return Err(primary.with_supplementary(cleanup)),
    ///     (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
    ///     (Ok(()), Ok(())) => {}
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn submit_batch<'a>(
        &'a self,
        _request: &'a BatchSubmitRequest<Self::Continuation>,
    ) -> impl Future<Output = Result<BatchJob, KitError>> + Send + 'a {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "batch.submit",
                    rule: "operation is not implemented",
                },
                "model batch submission",
            )
            .at_operation(Operation::SubmitBatch))
        }
    }

    /// Opens a stateful model session.
    ///
    /// Supplied Sessions support OpenAI only; Gemini, Anthropic and Ollama return
    /// Unsupported. Caller Models retain their own associated Session semantics.
    /// See the complete [Session progression](crate::session) and consuming cleanup.
    ///
    /// This example requires a real service, valid credentials, and an exact model
    /// supporting this operation.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] for closed admission, unsupported selected-model behavior, invalid request combinations, transport/deadline failures, service rejection, or malformed response. The operation is authoritative; no capability matrix or fallback model supplies success.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{Provider, ProviderBackend, ProviderConfig, config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig}};
    /// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)), TransportConfig::default()))?;
    /// use providers_kit::{ModelBackend, ModelId, generation::{GenerateRequest, Message}};
    /// let model = provider.model(ModelId::new("your-exact-model-id")?)?;
    /// let result = model.open_session(providers_kit::session::SessionConfig::default()).await;
    /// use providers_kit::SessionBackend;
    /// let observed = match result { Ok(session) => session.close().await, Err(error) => Err(error) };
    /// let cleanup = provider.shutdown().await;
    /// observed?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn open_session(
        &self,
        _config: SessionConfig,
    ) -> impl Future<Output = Result<Self::Session, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "session",
                    rule: "operation is not implemented",
                },
                "model interactive sessions",
            )
            .at_operation(Operation::OpenSession))
        }
    }
}

#[derive(Clone)]
pub(crate) enum ModelInner {
    OpenAi(OpenAiModel),
    Anthropic(AnthropicModel),
    Gemini(GeminiModel),
    Ollama(OllamaModel),
}

/// Cloneable opaque handle for one exact selected model.
///
/// One model owns an exact [`ModelId`], an associated continuation type, and an
/// associated Session type. Requests do not repeat model selection. Caller models
/// remain concrete; supplied Model clones share their Provider lifecycle.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), providers_kit::KitError> {
/// use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId, config::{ProviderKind, OllamaConfig, TransportConfig}};
/// let provider = Provider::from_config(ProviderConfig::new(ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
/// let model = provider.model(ModelId::new("exact-model")?)?;
/// assert_eq!(model.model_id().as_str(), "exact-model");
/// provider.shutdown().await?;
///
/// # Ok(())
/// # }
/// ```
///
#[derive(Clone)]
pub struct Model {
    inner: ModelInner,
    provider: Arc<ProviderRuntime>,
}

impl Model {
    pub(crate) fn new(inner: ModelInner, provider: Arc<ProviderRuntime>) -> Self {
        Self { inner, provider }
    }
}

impl ModelBackend for ModelInner {
    type Continuation = GenerationContinuation;
    type Session = SessionInner;

    fn model_id(&self) -> &ModelId {
        match self {
            Self::OpenAi(model) => model.model_id(),
            Self::Anthropic(model) => model.model_id(),
            Self::Gemini(model) => model.model_id(),
            Self::Ollama(model) => model.model_id(),
        }
    }

    async fn generate(
        &self,
        request: &GenerateRequest<GenerationContinuation>,
    ) -> Result<GenerateResponse<GenerationContinuation>, KitError> {
        match self {
            Self::OpenAi(model) => model.generate(request).await,
            Self::Anthropic(model) => model.generate(request).await,
            Self::Gemini(model) => model.generate(request).await,
            Self::Ollama(model) => model.generate(request).await,
        }
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<GenerationContinuation>,
    ) -> Result<GenerateEventStream<GenerationContinuation>, KitError> {
        match self {
            Self::OpenAi(model) => model.generate_stream(request).await,
            Self::Anthropic(model) => model.generate_stream(request).await,
            Self::Gemini(model) => model.generate_stream(request).await,
            Self::Ollama(model) => model.generate_stream(request).await,
        }
    }

    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, KitError> {
        match self {
            Self::OpenAi(model) => model.embed(request).await,
            Self::Anthropic(model) => model.embed(request).await,
            Self::Gemini(model) => model.embed(request).await,
            Self::Ollama(model) => model.embed(request).await,
        }
    }

    async fn submit_batch(
        &self,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<BatchJob, KitError> {
        match self {
            Self::OpenAi(model) => model.submit_batch(request).await,
            Self::Anthropic(model) => model.submit_batch(request).await,
            Self::Gemini(model) => model.submit_batch(request).await,
            Self::Ollama(model) => model.submit_batch(request).await,
        }
    }

    async fn open_session(&self, config: SessionConfig) -> Result<SessionInner, KitError> {
        match self {
            Self::OpenAi(model) => model.open_session(config).await.map(SessionInner::OpenAi),
            Self::Anthropic(model) => model.open_session(config).await.map(|never| match never {}),
            Self::Gemini(model) => model.open_session(config).await.map(|never| match never {}),
            Self::Ollama(model) => model.open_session(config).await.map(|never| match never {}),
        }
    }
}

impl ModelBackend for Model {
    type Continuation = GenerationContinuation;
    type Session = Session;

    fn model_id(&self) -> &ModelId {
        self.inner.model_id()
    }

    async fn generate(
        &self,
        request: &GenerateRequest<GenerationContinuation>,
    ) -> Result<GenerateResponse<GenerationContinuation>, KitError> {
        let _admission = self.provider.admit(Operation::Generate)?;
        self.inner
            .generate(request)
            .await
            .map_err(|error| error.at_operation(Operation::Generate))
    }

    async fn generate_stream(
        &self,
        request: &GenerateRequest<GenerationContinuation>,
    ) -> Result<GenerateEventStream<GenerationContinuation>, KitError> {
        let admission = self.provider.admit(Operation::GenerateStream)?;
        let stream = self
            .inner
            .generate_stream(request)
            .await
            .map_err(|error| error.at_operation(Operation::GenerateStream))?;
        Ok(admission.bind(stream))
    }

    async fn embed(&self, request: &EmbedRequest) -> Result<EmbedResponse, KitError> {
        let _admission = self.provider.admit(Operation::Embed)?;
        self.inner
            .embed(request)
            .await
            .map_err(|error| error.at_operation(Operation::Embed))
    }

    async fn submit_batch(
        &self,
        request: &BatchSubmitRequest<GenerationContinuation>,
    ) -> Result<BatchJob, KitError> {
        let admission = self.provider.admit(Operation::SubmitBatch)?;
        if let (ModelInner::OpenAi(model), BatchSubmitRequest::Inline { .. }) =
            (&self.inner, request)
        {
            let submission = model
                .prepare_owned_batch(request)
                .map_err(|error| error.at_operation(Operation::SubmitBatch))?;
            return self.provider.run_owned_batch(submission, admission).await;
        }
        let _admission = admission;
        self.inner
            .submit_batch(request)
            .await
            .map_err(|error| error.at_operation(Operation::SubmitBatch))
    }

    async fn open_session(&self, config: SessionConfig) -> Result<Session, KitError> {
        let _admission = self.provider.admit(Operation::OpenSession)?;
        let session = self
            .inner
            .open_session(config)
            .await
            .map_err(|error| error.at_operation(Operation::OpenSession))?;
        Ok(Session::new(session, Arc::clone(&self.provider)))
    }
}

impl fmt::Debug for Model {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Model")
            .field("model_id", self.model_id())
            .finish()
    }
}
