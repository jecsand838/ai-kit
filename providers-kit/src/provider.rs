//! Root provider contract and opaque provider handle.

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::Stream;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::KitError;
use crate::batch::{BatchJob, BatchListRequest, BatchPage, BatchResult};
use crate::batch_ownership::BatchRegistry;
use crate::config::{ProviderConfig, ProviderKind};
use crate::error::Operation;
use crate::error::ValidationViolation;
use crate::files::{FileListRequest, FileObject, FilePage, FileUploadRequest};
use crate::generation::{GenerateEvent, GenerateEventStream, GenerationContinuation};
use crate::models::{Model, ModelBackend, ModelId, ModelInner};
use crate::models::{ModelListRequest, ModelPage, ModelSummary};
use crate::providers::{
    anthropic::AnthropicBackend,
    gemini::GeminiBackend,
    ollama::OllamaBackend,
    openai::{OpenAiBackend, OpenAiBatchSubmission},
};
use crate::runtime::HealthStatus;

/// Public implementation contract for one provider root.
///
/// Provider and Model clones share one lifecycle. Shutdown seals model admission,
/// drains accepted work, and publishes one terminal result to every waiter; cancelling
/// a waiter cannot cancel owned cleanup. Models have no independent shutdown.
/// Caller implementations retain their concrete associated children directly.
///
/// # Examples
///
/// ```
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), providers_kit::KitError> {
/// use providers_kit::{
///     ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig,
///     config::{OllamaConfig, ProviderKind, TransportConfig},
/// };
/// let provider = Provider::from_config(ProviderConfig::new(
///     ProviderKind::Ollama(OllamaConfig::local()),
///     TransportConfig::default(),
/// ))?;
/// let model = provider.model(ModelId::new("exact-model")?)?;
/// assert_eq!(model.model_id().as_str(), "exact-model");
/// provider.shutdown().await?;
///
/// # Ok(())
/// # }
/// ```
///
/// # Implement a concrete local hierarchy
///
/// This runnable caller implementation is a deliberately limited, in-process text
/// echo service, **not** a language model or proof of any supplied provider. It
/// supports one text message with default settings and text-only Sessions. Optional
/// operations retain their Unsupported defaults. Its work is synchronous and finite:
/// there is no transport, spawned task or asynchronous resource to drain. A real
/// network implementation must additionally own admitted work and shutdown cleanup.
///
/// The continuation is caller-owned, cloneable, and deliberately has no serialization
/// contract. Finite streams yield on demand without a forwarding task. Shared closure
/// seals the root and existing children; consuming Session close releases its local queue.
///
/// ```
/// use std::{collections::VecDeque, sync::{Arc, atomic::{AtomicBool, Ordering}}};
/// use futures_util::{stream, StreamExt};
/// use providers_kit::{ProviderBackend, ModelBackend, SessionBackend, ModelId, KitError};
/// use providers_kit::error::{ErrorKind, ErrorPhase, Operation};
/// use providers_kit::generation::{ContentPart, FinishReason, GenerateEvent,
///     GenerateEventStream, GenerateRequest, GenerateResponse, GenerationConfig, Message, Role, Usage};
/// use providers_kit::session::{SessionConfig, SessionEvent, SessionInput};
///
/// #[derive(Clone, Default)]
/// struct EchoProvider { closed: Arc<AtomicBool> }
/// #[derive(Clone)]
/// struct EchoContinuation(Arc<()>);
/// struct EchoModel { id: ModelId, root: EchoProvider, replay: EchoContinuation }
/// struct EchoSession { model: ModelId, root: EchoProvider, events: VecDeque<SessionEvent> }
///
/// impl EchoProvider {
///     fn check(&self, operation: Operation) -> Result<(), KitError> {
///         if self.closed.load(Ordering::SeqCst) {
///             return Err(KitError::new(ErrorKind::Closed, operation, ErrorPhase::Lifecycle));
///         }
///         Ok(())
///     }
/// }
/// impl ProviderBackend for EchoProvider {
///     type Model = EchoModel;
///     fn model(&self, id: ModelId) -> Result<EchoModel, KitError> {
///         self.check(Operation::Generate)?;
///         if id.as_str() != "local-echo" {
///             return Err(KitError::new(ErrorKind::Unsupported, Operation::Generate,
///                 ErrorPhase::InputValidation));
///         }
///         Ok(EchoModel { id, root: self.clone(), replay: EchoContinuation(Arc::new(())) })
///     }
///     async fn shutdown(&self) -> Result<(), KitError> {
///         // No await: sealing and this resource-free cleanup complete together.
///         self.closed.store(true, Ordering::SeqCst);
///         Ok(())
///     }
/// }
/// impl EchoModel {
///     fn response(&self, request: &GenerateRequest<EchoContinuation>)
///         -> Result<GenerateResponse<EchoContinuation>, KitError>
///     {
///         self.root.check(Operation::Generate)?;
///         let unsupported = || KitError::new(ErrorKind::Unsupported,
///             Operation::Generate, ErrorPhase::InputValidation);
///         if !request.tools().is_empty() || request.reasoning().is_some()
///             || request.response_format().is_some()
///             || request.generation() != &GenerationConfig::default()
///             || request.messages().len() != 1 {
///             return Err(unsupported());
///         }
///         let message = &request.messages()[0];
///         if message.role() != Role::User { return Err(unsupported()); }
///         let [ContentPart::Text { text }] = message.content() else {
///             return Err(unsupported());
///         };
///         if text.len() > 64 * 1024 { return Err(unsupported()); }
///         Ok(GenerateResponse {
///             model: self.id.clone(),
///             message: Message::assistant_text(text.clone())?
///                 .with_continuation(self.replay.clone())?,
///             finish_reason: Some(FinishReason::Stop),
///             citations: Vec::new(), artifacts: Vec::new(), hosted_tool_outputs: Vec::new(),
///             usage: Usage::default(), // No token accounting is invented.
///         })
///     }
/// }
/// impl ModelBackend for EchoModel {
///     type Continuation = EchoContinuation;
///     type Session = EchoSession;
///     fn model_id(&self) -> &ModelId { &self.id }
///     async fn generate(&self, request: &GenerateRequest<EchoContinuation>)
///         -> Result<GenerateResponse<EchoContinuation>, KitError> { self.response(request) }
///     async fn generate_stream(&self, request: &GenerateRequest<EchoContinuation>)
///         -> Result<GenerateEventStream<EchoContinuation>, KitError>
///     {
///         let response = self.response(request)?;
///         let text: String = response.message.content().iter().filter_map(|part| match part {
///             ContentPart::Text { text } => Some(text.as_str()), _ => None,
///         }).collect();
///         Ok(GenerateEventStream::new(stream::iter([
///             Ok(GenerateEvent::Started { model: self.id.clone() }),
///             Ok(GenerateEvent::ContentDelta { delta: text }),
///             Ok(GenerateEvent::Completed(Box::new(response))),
///         ])))
///     }
///     async fn open_session(&self, config: SessionConfig) -> Result<EchoSession, KitError> {
///         self.root.check(Operation::OpenSession)?;
///         if config != SessionConfig::default() {
///             return Err(KitError::new(ErrorKind::Unsupported, Operation::OpenSession,
///                 ErrorPhase::InputValidation));
///         }
///         Ok(EchoSession { model: self.id.clone(), root: self.root.clone(), events: VecDeque::new() })
///     }
/// }
/// impl SessionBackend for EchoSession {
///     fn id(&self) -> &str { "local-echo-session" }
///     fn model_id(&self) -> &ModelId { &self.model }
///     async fn send(&mut self, input: SessionInput) -> Result<(), KitError> {
///         self.root.check(Operation::SessionSend)?;
///         if !self.events.is_empty() {
///             return Err(KitError::new(ErrorKind::InvalidState, Operation::SessionSend,
///                 ErrorPhase::Lifecycle));
///         }
///         match input {
///             SessionInput::Text { text } if !text.trim().is_empty() && text.len() <= 64 * 1024 => {
///                 self.events.push_back(SessionEvent::TextDelta { delta: text });
///                 self.events.push_back(SessionEvent::Completed);
///                 Ok(())
///             }
///             _ => Err(KitError::new(ErrorKind::Unsupported, Operation::SessionSend,
///                 ErrorPhase::InputValidation)),
///         }
///     }
///     async fn recv(&mut self) -> Result<SessionEvent, KitError> {
///         self.root.check(Operation::SessionReceive)?;
///         self.events.pop_front().ok_or_else(|| KitError::new(ErrorKind::InvalidState,
///             Operation::SessionReceive, ErrorPhase::Lifecycle))
///     }
///     async fn close(self) -> Result<(), KitError> { Ok(()) }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), KitError> {
/// let provider = EchoProvider::default();
/// let model = provider.model(ModelId::new("local-echo")?)?;
/// let request = GenerateRequest::new(vec![Message::user_text("hello")?])?;
/// let response = model.generate(&request).await?;
/// assert!(matches!(response.message.content(), [ContentPart::Text { text }] if text == "hello"));
/// assert!(Arc::ptr_eq(&response.message.continuation().unwrap().0, &model.replay.0));
/// let mut events = model.generate_stream(&request).await?;
/// assert!(matches!(events.next().await.transpose()?, Some(GenerateEvent::Started { .. })));
/// assert!(
///     matches!(events.next().await.transpose()?, Some(GenerateEvent::ContentDelta { delta }) if delta == "hello")
/// );
/// assert!(matches!(events.next().await.transpose()?, Some(GenerateEvent::Completed(_))));
/// assert!(events.next().await.is_none());
/// let mut session = model.open_session(SessionConfig::default()).await?;
/// session.send(SessionInput::Text { text: "hello again".into() }).await?;
/// assert!(
///     matches!(session.recv().await?, SessionEvent::TextDelta { delta } if delta == "hello again")
/// );
/// assert_eq!(session.recv().await?, SessionEvent::Completed);
/// session.close().await?;
/// provider.shutdown().await?;
/// assert_eq!(model.generate(&request).await.err().unwrap().kind(), ErrorKind::Closed);
/// provider.shutdown().await?; // Replays the same resource-free terminal result.
/// # Ok(())
/// # }
/// ```
///
pub trait ProviderBackend: Send + Sync + 'static {
    /// Exact model child returned by this provider.
    type Model: ModelBackend;

    /// Selects one exact provider-local model.
    ///
    /// # Errors
    ///
    /// Returns an error when root admission is closed or the selected implementation cannot construct this exact model. Selection never silently falls back to another model.
    ///
    /// See [`crate::ProviderBackend`] for a complete example.
    ///
    fn model(&self, id: ModelId) -> Result<Self::Model, KitError>;

    /// Lists provider-visible models.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.list_models(providers_kit::models::ModelListRequest::default()).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let page = result?;
    /// cleanup?;
    /// for model in page.models {
    ///     println!("{}", model.id.as_str());
    /// }
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn list_models(
        &self,
        _request: ModelListRequest,
    ) -> impl Future<Output = Result<ModelPage, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "models.list",
                    rule: "operation is not implemented",
                },
                "provider model discovery",
            )
            .at_operation(Operation::ListModels))
        }
    }

    /// Fetches descriptive metadata for one exact provider-local model.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.model_info(providers_kit::ModelId::new("your-exact-model-id")?).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn model_info(
        &self,
        _id: ModelId,
    ) -> impl Future<Output = Result<ModelSummary, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "models.info",
                    rule: "operation is not implemented",
                },
                "provider model metadata",
            )
            .at_operation(Operation::ModelInfo))
        }
    }

    /// Uploads one provider-managed file.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider
    ///     .upload_file(providers_kit::files::FileUploadRequest::new(
    ///         "notes.txt",
    ///         "text/plain",
    ///         b"notes".to_vec(),
    ///     )?)
    ///     .await;
    /// let deletion = match &result {
    ///     Ok(file) => provider.delete_file(file.id.clone()).await,
    ///     Err(_) => Ok(()),
    /// };
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// deletion?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn upload_file(
        &self,
        _request: FileUploadRequest,
    ) -> impl Future<Output = Result<FileObject, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "files.upload",
                    rule: "operation is not implemented",
                },
                "provider file upload",
            )
            .at_operation(Operation::UploadFile))
        }
    }

    /// Lists provider-managed files.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.list_files(providers_kit::files::FileListRequest::default()).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn list_files(
        &self,
        _request: FileListRequest,
    ) -> impl Future<Output = Result<FilePage, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "files.list",
                    rule: "operation is not implemented",
                },
                "provider file listing",
            )
            .at_operation(Operation::ListFiles))
        }
    }

    /// Fetches metadata for one provider-managed file.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.file_info("owned-file-id".into()).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn file_info(
        &self,
        _id: String,
    ) -> impl Future<Output = Result<FileObject, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "files.info",
                    rule: "operation is not implemented",
                },
                "provider file metadata",
            )
            .at_operation(Operation::FileInfo))
        }
    }

    /// Deletes one provider-managed file.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.delete_file("owned-file-id".into()).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn delete_file(&self, _id: String) -> impl Future<Output = Result<(), KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "files.delete",
                    rule: "operation is not implemented",
                },
                "provider file deletion",
            )
            .at_operation(Operation::DeleteFile))
        }
    }

    /// Downloads one provider-managed file under the caller's byte limit.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider
    ///     .download_file(
    ///         "owned-file-id".into(),
    ///         std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
    ///     )
    ///     .await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn download_file(
        &self,
        _id: String,
        _max_bytes: NonZeroUsize,
    ) -> impl Future<Output = Result<Vec<u8>, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "files.download",
                    rule: "operation is not implemented",
                },
                "provider file download",
            )
            .at_operation(Operation::DownloadFile))
        }
    }

    /// Fetches one batch job.
    ///
    /// Supplied OpenAI status reads opt into the configured bounded transport
    /// retries; every attempt shares the original absolute request deadline.
    /// This does not retry decoding or successful-response body failures. The
    /// lookup used by `batch_results` is excluded. See the [batch workflow](crate::batch).
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.batch_status("owned-batch-id".into()).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn batch_status(
        &self,
        _id: String,
    ) -> impl Future<Output = Result<BatchJob, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "batch.status",
                    rule: "operation is not implemented",
                },
                "provider batch status",
            )
            .at_operation(Operation::BatchStatus))
        }
    }

    /// Requests cancellation of one batch job.
    ///
    /// Acknowledgement is not terminality: the returned job may still be running.
    /// Observe its status until terminal before cleaning up owned input files; see
    /// the [bounded batch workflow](crate::batch).
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.cancel_batch("owned-batch-id".into()).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn cancel_batch(
        &self,
        _id: String,
    ) -> impl Future<Output = Result<BatchJob, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "batch.cancel",
                    rule: "operation is not implemented",
                },
                "provider batch cancellation",
            )
            .at_operation(Operation::CancelBatch))
        }
    }

    /// Downloads and normalizes batch result rows under the caller's byte limit.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    /// The byte bound covers the downloaded result; each terminal row must be inspected
    /// independently. Row failures are result data, while download/normalization failures
    /// are outer errors.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider
    ///     .batch_results(
    ///         "owned-batch-id".into(),
    ///         std::num::NonZeroUsize::new(1024 * 1024).unwrap(),
    ///     )
    ///     .await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn batch_results(
        &self,
        _id: String,
        _max_bytes: NonZeroUsize,
    ) -> impl Future<
        Output = Result<Vec<BatchResult<<Self::Model as ModelBackend>::Continuation>>, KitError>,
    > + Send
    + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "batch.results",
                    rule: "operation is not implemented",
                },
                "provider batch results",
            )
            .at_operation(Operation::BatchResults))
        }
    }

    /// Lists provider batch jobs.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown. Only delete or cancel resources the caller owns.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.list_batches(providers_kit::batch::BatchListRequest::default()).await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn list_batches(
        &self,
        _request: BatchListRequest,
    ) -> impl Future<Output = Result<BatchPage, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "batch.list",
                    rule: "operation is not implemented",
                },
                "provider batch listing",
            )
            .at_operation(Operation::ListBatches))
        }
    }

    /// Probes provider runtime health.
    ///
    /// This example requires a real configured service, valid credentials, and any exact
    /// resource ids shown.
    /// The default trait implementation returns Unsupported.
    ///
    /// # Errors
    ///
    /// Returns [`KitError`] classified as Unsupported when unavailable, Closed after sealing, or the owning input, transport, deadline, service-rejection, or response-validation category. Inspect `kind()` and `context()` rather than diagnostic prose.
    ///
    /// # Examples
    ///
    /// ```no_run
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     Provider, ProviderBackend, ProviderConfig,
    ///     config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
    ///     TransportConfig::default(),
    /// ))?;
    /// let result = provider.health().await;
    /// let cleanup = provider.shutdown().await;
    /// if let Err(error) = &cleanup {
    ///     eprintln!("Cleanup failed: {error}");
    /// }
    /// let _value = result?;
    /// cleanup?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn health(&self) -> impl Future<Output = Result<HealthStatus, KitError>> + Send + '_ {
        async {
            Err(KitError::unsupported(
                ValidationViolation::Unsupported {
                    field: "runtime.health",
                    rule: "operation is not implemented",
                },
                "provider runtime health",
            )
            .at_operation(Operation::Health))
        }
    }

    /// Shuts down backend-owned resources.
    ///
    /// Retain cleanup independently of each borrowed waiter. Repeated and concurrent
    /// calls replay the same terminal result; last-handle drop is not an async cleanup API.
    ///
    /// # Errors
    ///
    /// Returns the shared backend cleanup failure, if any. A cancelled waiter does not reset shutdown or abandon cleanup.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), providers_kit::KitError> {
    /// use providers_kit::{
    ///     ModelBackend, ModelId, Provider, ProviderBackend, ProviderConfig,
    ///     config::{OllamaConfig, ProviderKind, TransportConfig},
    /// };
    /// let provider = Provider::from_config(ProviderConfig::new(
    ///     ProviderKind::Ollama(OllamaConfig::local()),
    ///     TransportConfig::default(),
    /// ))?;
    /// let clone = provider.clone();
    /// provider.shutdown().await?;
    /// clone.shutdown().await?;
    /// assert!(clone.model(ModelId::new("exact-model")?).is_err());
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    fn shutdown(&self) -> impl Future<Output = Result<(), KitError>> + Send + '_;
}

enum ProviderInner {
    OpenAi(OpenAiBackend),
    Anthropic(AnthropicBackend),
    Gemini(GeminiBackend),
    Ollama(OllamaBackend),
}

impl ProviderBackend for ProviderInner {
    type Model = ModelInner;

    fn model(&self, id: ModelId) -> Result<ModelInner, KitError> {
        match self {
            Self::OpenAi(backend) => backend.model(id).map(ModelInner::OpenAi),
            Self::Anthropic(backend) => backend.model(id).map(ModelInner::Anthropic),
            Self::Gemini(backend) => backend.model(id).map(ModelInner::Gemini),
            Self::Ollama(backend) => backend.model(id).map(ModelInner::Ollama),
        }
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, KitError> {
        match self {
            Self::OpenAi(backend) => backend.list_models(request).await,
            Self::Anthropic(backend) => backend.list_models(request).await,
            Self::Gemini(backend) => backend.list_models(request).await,
            Self::Ollama(backend) => backend.list_models(request).await,
        }
    }

    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, KitError> {
        match self {
            Self::OpenAi(backend) => backend.model_info(id).await,
            Self::Anthropic(backend) => backend.model_info(id).await,
            Self::Gemini(backend) => backend.model_info(id).await,
            Self::Ollama(backend) => backend.model_info(id).await,
        }
    }

    async fn upload_file(&self, request: FileUploadRequest) -> Result<FileObject, KitError> {
        match self {
            Self::OpenAi(backend) => backend.upload_file(request).await,
            Self::Anthropic(backend) => backend.upload_file(request).await,
            Self::Gemini(backend) => backend.upload_file(request).await,
            Self::Ollama(backend) => backend.upload_file(request).await,
        }
    }

    async fn list_files(&self, request: FileListRequest) -> Result<FilePage, KitError> {
        match self {
            Self::OpenAi(backend) => backend.list_files(request).await,
            Self::Anthropic(backend) => backend.list_files(request).await,
            Self::Gemini(backend) => backend.list_files(request).await,
            Self::Ollama(backend) => backend.list_files(request).await,
        }
    }

    async fn file_info(&self, id: String) -> Result<FileObject, KitError> {
        match self {
            Self::OpenAi(backend) => backend.file_info(id).await,
            Self::Anthropic(backend) => backend.file_info(id).await,
            Self::Gemini(backend) => backend.file_info(id).await,
            Self::Ollama(backend) => backend.file_info(id).await,
        }
    }

    async fn delete_file(&self, id: String) -> Result<(), KitError> {
        match self {
            Self::OpenAi(backend) => backend.delete_file(id).await,
            Self::Anthropic(backend) => backend.delete_file(id).await,
            Self::Gemini(backend) => backend.delete_file(id).await,
            Self::Ollama(backend) => backend.delete_file(id).await,
        }
    }

    async fn download_file(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<u8>, KitError> {
        match self {
            Self::OpenAi(backend) => backend.download_file(id, max_bytes).await,
            Self::Anthropic(backend) => backend.download_file(id, max_bytes).await,
            Self::Gemini(backend) => backend.download_file(id, max_bytes).await,
            Self::Ollama(backend) => backend.download_file(id, max_bytes).await,
        }
    }

    async fn batch_status(&self, id: String) -> Result<BatchJob, KitError> {
        match self {
            Self::OpenAi(backend) => backend.batch_status(id).await,
            Self::Anthropic(backend) => backend.batch_status(id).await,
            Self::Gemini(backend) => backend.batch_status(id).await,
            Self::Ollama(backend) => backend.batch_status(id).await,
        }
    }

    async fn cancel_batch(&self, id: String) -> Result<BatchJob, KitError> {
        match self {
            Self::OpenAi(backend) => backend.cancel_batch(id).await,
            Self::Anthropic(backend) => backend.cancel_batch(id).await,
            Self::Gemini(backend) => backend.cancel_batch(id).await,
            Self::Ollama(backend) => backend.cancel_batch(id).await,
        }
    }

    async fn batch_results(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<GenerationContinuation>>, KitError> {
        match self {
            Self::OpenAi(backend) => backend.batch_results(id, max_bytes).await,
            Self::Anthropic(backend) => backend.batch_results(id, max_bytes).await,
            Self::Gemini(backend) => backend.batch_results(id, max_bytes).await,
            Self::Ollama(backend) => backend.batch_results(id, max_bytes).await,
        }
    }

    async fn list_batches(&self, request: BatchListRequest) -> Result<BatchPage, KitError> {
        match self {
            Self::OpenAi(backend) => backend.list_batches(request).await,
            Self::Anthropic(backend) => backend.list_batches(request).await,
            Self::Gemini(backend) => backend.list_batches(request).await,
            Self::Ollama(backend) => backend.list_batches(request).await,
        }
    }

    async fn health(&self) -> Result<HealthStatus, KitError> {
        match self {
            Self::OpenAi(backend) => backend.health().await,
            Self::Anthropic(backend) => backend.health().await,
            Self::Gemini(backend) => backend.health().await,
            Self::Ollama(backend) => backend.health().await,
        }
    }

    async fn shutdown(&self) -> Result<(), KitError> {
        match self {
            Self::OpenAi(backend) => backend.shutdown().await,
            Self::Anthropic(backend) => backend.shutdown().await,
            Self::Gemini(backend) => backend.shutdown().await,
            Self::Ollama(backend) => backend.shutdown().await,
        }
    }
}

/// Cloneable opaque provider handle.
///
/// Provider and Model clones share one lifecycle. Shutdown seals model admission,
/// drains accepted work, and publishes one terminal result to every waiter; cancelling
/// a waiter cannot cancel owned cleanup. Models have no independent shutdown.
/// Caller implementations retain their concrete associated children directly.
///
/// See [`crate::ProviderBackend`] for a complete example.
///
#[derive(Clone)]
pub struct Provider {
    inner: Arc<ProviderRuntime>,
}

pub(crate) struct ProviderRuntime {
    backend: ProviderInner,
    state: Mutex<ProviderLifecycleState>,
    batches: Mutex<BatchRegistry>,
    changed: Notify,
}

enum ProviderLifecycleState {
    Open {
        admitted: usize,
    },
    Closing {
        admitted: usize,
        task: Option<JoinHandle<Result<(), KitError>>>,
    },
    Closed {
        terminal: Result<(), KitError>,
    },
}

pub(crate) struct ProviderAdmission {
    runtime: Arc<ProviderRuntime>,
    batch_bytes: Option<usize>,
}

struct ProviderAdmittedStream {
    inner: GenerateEventStream,
    admission: Option<ProviderAdmission>,
}

struct ShutdownTaskLease<'a> {
    runtime: &'a ProviderRuntime,
    task: Option<JoinHandle<Result<(), KitError>>>,
}

impl ProviderRuntime {
    fn new(backend: ProviderInner) -> Self {
        Self {
            backend,
            state: Mutex::new(ProviderLifecycleState::Open { admitted: 0 }),
            batches: Mutex::new(BatchRegistry::new()),
            changed: Notify::new(),
        }
    }

    pub(crate) fn admit(
        self: &Arc<Self>,
        operation: Operation,
    ) -> Result<ProviderAdmission, KitError> {
        let mut state = self.state();
        match &mut *state {
            ProviderLifecycleState::Open { admitted } => {
                *admitted = admitted.checked_add(1).ok_or_else(|| {
                    KitError::invalid_state("provider admission is exhausted")
                        .at_operation(operation)
                })?;
                Ok(ProviderAdmission {
                    runtime: Arc::clone(self),
                    batch_bytes: None,
                })
            }
            ProviderLifecycleState::Closing { .. } | ProviderLifecycleState::Closed { .. } => {
                Err(KitError::closed("provider admission closed").at_operation(operation))
            }
        }
    }

    pub(crate) async fn run_owned_batch(
        &self,
        submission: OpenAiBatchSubmission,
        mut admission: ProviderAdmission,
    ) -> Result<BatchJob, KitError> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            KitError::invalid_state("inline batch ownership requires a Tokio runtime")
                .at_operation(Operation::SubmitBatch)
        })?;
        let bytes = submission.payload_bytes();
        let (observer, activate) = {
            let mut batches = self.batches();
            batches.reserve(bytes)?;
            batches.register(submission, &handle)
        };
        // Preserve the original admission acquired before preparation. Even
        // shutdown sealing during rendering cannot harvest tasks until this
        // admission is released. Activation must follow registry lock release.
        admission.batch_bytes = Some(bytes);
        if let Err(admission) = activate.send(admission) {
            // The pre-spawn publication guard has already reported worker loss.
            // This release also returns the byte charge exactly once.
            drop(admission);
        }
        observer
            .wait()
            .await
            .map_err(|error| error.at_operation(Operation::SubmitBatch))
    }

    async fn shutdown(self: &Arc<Self>) -> Result<(), KitError> {
        self.start_shutdown()?;
        self.terminal().await
    }

    fn start_shutdown(self: &Arc<Self>) -> Result<(), KitError> {
        let mut state = self.state();
        let admitted = match &*state {
            ProviderLifecycleState::Open { admitted } => *admitted,
            ProviderLifecycleState::Closing { .. } | ProviderLifecycleState::Closed { .. } => {
                return Ok(());
            }
        };
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| KitError::invalid_state("provider shutdown requires a Tokio runtime"))?;
        let runtime = Arc::clone(self);
        let task = handle.spawn(async move { runtime.finish_shutdown().await });
        *state = ProviderLifecycleState::Closing {
            admitted,
            task: Some(task),
        };
        Ok(())
    }

    async fn finish_shutdown(self: Arc<Self>) -> Result<(), KitError> {
        self.drain().await;
        // Admissions cover preparation, registration, execution and cleanup.
        // Only after draining can no previously admitted call add another task.
        let mut tasks = self.batches().take_tasks();
        while let Some(result) = tasks.join_next().await {
            self.batches().joined(result);
        }
        let ownership = self.batches().terminal();
        let backend = self.backend.shutdown().await;
        match (ownership, backend) {
            (Ok(()), result) => result,
            (Err(primary), Ok(())) => Err(primary),
            (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
        }
    }

    fn batches(&self) -> std::sync::MutexGuard<'_, BatchRegistry> {
        self.batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ProviderLifecycleState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn release(&self) {
        let should_notify = {
            let mut state = self.state();
            match &mut *state {
                ProviderLifecycleState::Open { admitted } => {
                    debug_assert_ne!(*admitted, 0);
                    *admitted = admitted.saturating_sub(1);
                    false
                }
                ProviderLifecycleState::Closing { admitted, .. } => {
                    debug_assert_ne!(*admitted, 0);
                    *admitted = admitted.saturating_sub(1);
                    *admitted == 0
                }
                ProviderLifecycleState::Closed { .. } => {
                    debug_assert!(false, "closed provider retained an admission");
                    false
                }
            }
        };
        if should_notify {
            self.changed.notify_waiters();
        }
    }

    async fn drain(&self) {
        loop {
            let notified = self.changed.notified();
            let drained = match &*self.state() {
                ProviderLifecycleState::Open { admitted }
                | ProviderLifecycleState::Closing { admitted, .. } => *admitted == 0,
                ProviderLifecycleState::Closed { .. } => true,
            };
            if drained {
                return;
            }
            notified.await;
        }
    }

    async fn terminal(&self) -> Result<(), KitError> {
        loop {
            let notified = self.changed.notified();
            let mut lease = {
                let mut state = self.state();
                match &mut *state {
                    ProviderLifecycleState::Closed { terminal } => return terminal.clone(),
                    ProviderLifecycleState::Closing { task, .. } => {
                        task.take().map(|task| ShutdownTaskLease::new(self, task))
                    }
                    ProviderLifecycleState::Open { .. } => {
                        return Err(KitError::invalid_state(
                            "provider shutdown lost lifecycle ownership",
                        ));
                    }
                }
            };
            let Some(lease) = lease.as_mut() else {
                notified.await;
                continue;
            };
            let terminal = lease.join().await;
            lease.complete(terminal.clone());
            return terminal;
        }
    }
}

impl<'a> ShutdownTaskLease<'a> {
    fn new(runtime: &'a ProviderRuntime, task: JoinHandle<Result<(), KitError>>) -> Self {
        Self {
            runtime,
            task: Some(task),
        }
    }

    async fn join(&mut self) -> Result<(), KitError> {
        let Some(task) = self.task.as_mut() else {
            return Err(KitError::invalid_state(
                "provider shutdown task lease was empty",
            ));
        };
        task.await.unwrap_or_else(|_| {
            Err(KitError::invalid_state(
                "provider shutdown owner could not be joined",
            ))
        })
    }

    fn complete(&mut self, terminal: Result<(), KitError>) {
        self.task = None;
        let mut state = self.runtime.state();
        if matches!(&*state, ProviderLifecycleState::Closing { task: None, .. }) {
            *state = ProviderLifecycleState::Closed { terminal };
            drop(state);
            self.runtime.changed.notify_waiters();
        }
    }
}

impl Drop for ShutdownTaskLease<'_> {
    fn drop(&mut self) {
        let Some(task) = self.task.take() else {
            return;
        };
        let mut state = self.runtime.state();
        if let ProviderLifecycleState::Closing { task: slot, .. } = &mut *state
            && slot.is_none()
        {
            *slot = Some(task);
            drop(state);
            self.runtime.changed.notify_waiters();
            return;
        }
        debug_assert!(false, "shutdown task lease lost lifecycle ownership");
    }
}

impl Drop for ProviderAdmission {
    fn drop(&mut self) {
        if let Some(bytes) = self.batch_bytes {
            self.runtime.batches().release(bytes);
        }
        self.runtime.release();
    }
}

impl ProviderAdmission {
    pub(crate) fn bind(self, stream: GenerateEventStream) -> GenerateEventStream {
        GenerateEventStream::new(ProviderAdmittedStream {
            inner: stream,
            admission: Some(self),
        })
    }
}

impl Stream for ProviderAdmittedStream {
    type Item = Result<GenerateEvent, KitError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = Pin::new(&mut self.inner).poll_next(context);
        if matches!(
            &polled,
            Poll::Ready(None)
                | Poll::Ready(Some(Err(_)))
                | Poll::Ready(Some(Ok(GenerateEvent::Completed(_))))
        ) {
            drop(self.admission.take());
        }
        polled.map(|item| {
            item.map(|result| result.map_err(|error| error.at_operation(Operation::GenerateStream)))
        })
    }
}

impl Provider {
    /// Constructs a built-in provider from validated configuration.
    ///
    /// Construction creates clients locally and does not probe credentials or model availability.
    ///
    /// # Errors
    ///
    /// Returns a configuration or client-construction error for invalid endpoints, transport settings, or HTTP/TLS client setup. Service authentication is checked by subsequent operations.
    ///
    /// See [`crate::ProviderBackend`] for a complete example.
    ///
    pub fn from_config(config: ProviderConfig) -> Result<Self, KitError> {
        let (kind, transport) = config.into_parts();
        let backend = match kind {
            ProviderKind::OpenAi(config) => ProviderInner::OpenAi(
                OpenAiBackend::from_config(config, transport)
                    .map_err(|error| error.at_operation(Operation::Configure))?,
            ),
            ProviderKind::Anthropic(config) => ProviderInner::Anthropic(
                AnthropicBackend::from_config(config, transport)
                    .map_err(|error| error.at_operation(Operation::Configure))?,
            ),
            ProviderKind::Gemini(config) => ProviderInner::Gemini(
                GeminiBackend::from_config(config, transport)
                    .map_err(|error| error.at_operation(Operation::Configure))?,
            ),
            ProviderKind::Ollama(config) => ProviderInner::Ollama(
                OllamaBackend::from_config(config, transport)
                    .map_err(|error| error.at_operation(Operation::Configure))?,
            ),
        };
        Ok(Self {
            inner: Arc::new(ProviderRuntime::new(backend)),
        })
    }
}

impl ProviderBackend for Provider {
    type Model = Model;

    fn model(&self, id: ModelId) -> Result<Model, KitError> {
        let _admission = self.inner.admit(Operation::SelectModel)?;
        let model = self
            .inner
            .backend
            .model(id)
            .map_err(|error| error.at_operation(Operation::SelectModel))?;
        Ok(Model::new(model, Arc::clone(&self.inner)))
    }

    async fn list_models(&self, request: ModelListRequest) -> Result<ModelPage, KitError> {
        let _admission = self.inner.admit(Operation::ListModels)?;
        self.inner
            .backend
            .list_models(request)
            .await
            .map_err(|error| error.at_operation(Operation::ListModels))
    }

    async fn model_info(&self, id: ModelId) -> Result<ModelSummary, KitError> {
        let _admission = self.inner.admit(Operation::ModelInfo)?;
        self.inner
            .backend
            .model_info(id)
            .await
            .map_err(|error| error.at_operation(Operation::ModelInfo))
    }

    async fn upload_file(&self, request: FileUploadRequest) -> Result<FileObject, KitError> {
        let _admission = self.inner.admit(Operation::UploadFile)?;
        self.inner
            .backend
            .upload_file(request)
            .await
            .map_err(|error| error.at_operation(Operation::UploadFile))
    }

    async fn list_files(&self, request: FileListRequest) -> Result<FilePage, KitError> {
        let _admission = self.inner.admit(Operation::ListFiles)?;
        self.inner
            .backend
            .list_files(request)
            .await
            .map_err(|error| error.at_operation(Operation::ListFiles))
    }

    async fn file_info(&self, id: String) -> Result<FileObject, KitError> {
        let _admission = self.inner.admit(Operation::FileInfo)?;
        self.inner
            .backend
            .file_info(id)
            .await
            .map_err(|error| error.at_operation(Operation::FileInfo))
    }

    async fn delete_file(&self, id: String) -> Result<(), KitError> {
        let _admission = self.inner.admit(Operation::DeleteFile)?;
        self.inner
            .backend
            .delete_file(id)
            .await
            .map_err(|error| error.at_operation(Operation::DeleteFile))
    }

    async fn download_file(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<u8>, KitError> {
        let _admission = self.inner.admit(Operation::DownloadFile)?;
        self.inner
            .backend
            .download_file(id, max_bytes)
            .await
            .map_err(|error| error.at_operation(Operation::DownloadFile))
    }

    async fn batch_status(&self, id: String) -> Result<BatchJob, KitError> {
        let _admission = self.inner.admit(Operation::BatchStatus)?;
        self.inner
            .backend
            .batch_status(id)
            .await
            .map_err(|error| error.at_operation(Operation::BatchStatus))
    }

    async fn cancel_batch(&self, id: String) -> Result<BatchJob, KitError> {
        let _admission = self.inner.admit(Operation::CancelBatch)?;
        self.inner
            .backend
            .cancel_batch(id)
            .await
            .map_err(|error| error.at_operation(Operation::CancelBatch))
    }

    async fn batch_results(
        &self,
        id: String,
        max_bytes: NonZeroUsize,
    ) -> Result<Vec<BatchResult<GenerationContinuation>>, KitError> {
        let _admission = self.inner.admit(Operation::BatchResults)?;
        self.inner
            .backend
            .batch_results(id, max_bytes)
            .await
            .map_err(|error| error.at_operation(Operation::BatchResults))
    }

    async fn list_batches(&self, request: BatchListRequest) -> Result<BatchPage, KitError> {
        let _admission = self.inner.admit(Operation::ListBatches)?;
        self.inner
            .backend
            .list_batches(request)
            .await
            .map_err(|error| error.at_operation(Operation::ListBatches))
    }

    async fn health(&self) -> Result<HealthStatus, KitError> {
        let _admission = self.inner.admit(Operation::Health)?;
        self.inner
            .backend
            .health()
            .await
            .map_err(|error| error.at_operation(Operation::Health))
    }

    async fn shutdown(&self) -> Result<(), KitError> {
        self.inner
            .shutdown()
            .await
            .map_err(|error| error.at_operation(Operation::Shutdown))
    }
}

impl fmt::Debug for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Provider").finish_non_exhaustive()
    }
}
