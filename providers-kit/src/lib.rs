//! Portable provider resources with exact model selection.
//!
//! The hierarchy is [`Provider`] → [`Model`] → [`Session`]. Import
//! [`ProviderBackend`], [`ModelBackend`], and [`SessionBackend`] to call behavior
//! on supplied handles. Caller implementations use these traits directly with
//! their own associated children; no normalization constructor is required.
//!
//! [`config`] owns credentials and transport settings; [`models`] owns exact
//! model identity and discovery. Requests and returned values live in
//! [`generation`], [`embedding`], [`files`], [`batch`], [`hosted`], and [`session`].
//! Inspect typed failures through [`error`] and health results through [`runtime`].
//!
//! Provider and Model clones share shutdown. Sessions are unique and close by
//! consumption. Keep the runtime driven until accepted cleanup settles.
//!
//! # Local construction
//!
//! Creating a provider and selecting an exact model performs no service request:
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), providers_kit::KitError> {
//! use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId};
//! use providers_kit::config::{ProviderKind, OllamaConfig, TransportConfig};
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default(),
//! ))?;
//! let model = provider.model(ModelId::new("exact-local-model-id")?)?;
//! assert_eq!(model.model_id().as_str(), "exact-local-model-id");
//! provider.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Generation
//!
//! This example requires a running Ollama service with the exact model installed.
//! Selection does not fall back or prove availability; the operation is authoritative.
//!
//! ```no_run
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), providers_kit::KitError> {
//! use providers_kit::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId};
//! use providers_kit::config::{ProviderKind, OllamaConfig, TransportConfig};
//! use providers_kit::generation::{GenerateRequest, Message};
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default(),
//! ))?;
//! let result = async {
//!     let model = provider.model(ModelId::new("exact-local-model-id")?)?;
//!     let request = GenerateRequest::new(vec![Message::user_text("Hello")?])?;
//!     model.generate(&request).await
//! }.await;
//! let cleanup = provider.shutdown().await;
//! let response = result?;
//! cleanup?;
//! // Preserve response.message unchanged for subsequent function continuation.
//! let _assistant_message = response.message;
//! # Ok(())
//! # }
//! ```
//!
//! # Choose a workflow
//!
//! | Audience | Start here |
//! | --- | --- |
//! | Application builders | [Generation and exact assistant replay](generation), [embeddings](embedding), [owned files](files), [batch lifecycle](batch), and [interactive Sessions](session) |
//! | Custom-backend implementers | The complete local hierarchy on [`ProviderBackend`], then the sessionless [`ModelBackend`] companion |
//!
//! The supplied Session dispatcher supports **OpenAI only**. Supplied Gemini,
//! Anthropic and Ollama Models return Unsupported from `open_session`. Stateless
//! OpenAI audio input/audio-chat models are also unsupported; this does not remove
//! OpenAI Realtime Session support. These supplied-backend restrictions do not
//! constrain an independently implemented [`SessionBackend`].
//!
//! The examples demonstrate API use, not current account/model availability or
//! native qualification. Model ids are exact, and the selected operation decides
//! whether a requested combination is supported.
#![doc(html_logo_url = "../assets/ai-kit-logo.png")]
#![forbid(unsafe_code)]

pub mod batch;
mod batch_ownership;
pub mod config;
mod core;
pub mod embedding;
pub mod error;
pub mod files;
pub mod generation;
pub mod hosted;
pub mod models;
mod provider;
mod providers;
pub mod runtime;
pub mod session;
mod transport;

pub use crate::config::ProviderConfig;
pub use crate::error::KitError;
pub use crate::models::{Model, ModelBackend, ModelId};
pub use crate::provider::{Provider, ProviderBackend};
pub use crate::session::{Session, SessionBackend};
