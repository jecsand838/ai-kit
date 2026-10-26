//! Portable embedding requests and results.
//!
//! Select one exact Model before submitting an [`EmbedRequest`]. Input order maps
//! directly to [`EmbedResponse::vectors`](crate::embedding::EmbedResponse::vectors). Requested dimensions are optional and
//! model support is authoritative at the operation boundary.
//!
//! # Embed an ordered collection
//!
//! This example needs an OpenAI key and access to the selected embedding model.
//! Only compare or store vectors using the same selected model/dimension convention.
//! The OpenAI operation validates vector count and index correlation. This example
//! separately checks that each vector has the requested dimensions; neither check
//! establishes semantic retrieval quality.
//!
//! ```no_run
//! use provider_sdk::{Provider, ProviderBackend, ProviderConfig, ModelBackend, ModelId};
//! use provider_sdk::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
//! use provider_sdk::embedding::EmbedRequest;
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), provider_sdk::SdkError> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
//!     TransportConfig::default(),
//! ))?;
//! let work = async {
//!     let model = provider.model(ModelId::new("text-embedding-3-small")?)?;
//!     let request = EmbedRequest::new(vec!["first document".into(), "second document".into()])?
//!         .with_dimensions(std::num::NonZeroU32::new(256).unwrap());
//!     let response = model.embed(&request).await?;
//!     assert_eq!(response.vectors.len(), request.inputs().len());
//!     assert!(response.vectors.iter().all(|vector| vector.len() == 256));
//!     Ok::<_, provider_sdk::SdkError>(response)
//! }.await;
//! let cleanup = provider.shutdown().await;
//! let _ordered_vectors = work?;
//! cleanup?;
//! # Ok(())
//! # }
//! ```

use std::num::NonZeroU32;

use serde::{Deserialize, Deserializer, Serialize};

use crate::SdkError;
use crate::error::ValidationViolation;
use crate::generation::Usage;

/// Validated text-embedding request.
///
/// # Examples
///
/// ```
/// use provider_sdk::embedding::EmbedRequest;
/// let request = EmbedRequest::new(vec!["search document".into()])?
///     .with_dimensions(std::num::NonZeroU32::new(128).unwrap());
/// assert_eq!(request.inputs(), &["search document"]);
/// assert_eq!(request.dimensions().unwrap().get(), 128);
///
/// # Ok::<(), provider_sdk::SdkError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EmbedRequest {
    inputs: Vec<String>,
    dimensions: Option<NonZeroU32>,
}

impl EmbedRequest {
    /// Creates a request containing non-empty text inputs.
    ///
    /// # Errors
    ///
    /// Returns an input error if the input list is empty or any text is whitespace-only.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::embedding::EmbedRequest;
    /// let request = EmbedRequest::new(vec!["search document".into()])?
    ///     .with_dimensions(std::num::NonZeroU32::new(128).unwrap());
    /// assert_eq!(request.inputs(), &["search document"]);
    /// assert_eq!(request.dimensions().unwrap().get(), 128);
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    pub fn new(inputs: Vec<String>) -> Result<Self, SdkError> {
        if inputs.is_empty() || inputs.iter().any(|value| value.trim().is_empty()) {
            return Err(SdkError::invalid_input(
                ValidationViolation::Empty {
                    field: "embedding.inputs",
                },
                "embedding inputs must contain non-empty text",
            ));
        }
        Ok(Self {
            inputs,
            dimensions: None,
        })
    }

    /// Requests a non-zero output dimensionality.
    ///
    /// The Model is already selected; dimensionality support is checked by the model operation.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::embedding::EmbedRequest;
    /// let request = EmbedRequest::new(vec!["search document".into()])?
    ///     .with_dimensions(std::num::NonZeroU32::new(128).unwrap());
    /// assert_eq!(request.inputs(), &["search document"]);
    /// assert_eq!(request.dimensions().unwrap().get(), 128);
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn with_dimensions(mut self, dimensions: NonZeroU32) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Returns text inputs in caller order.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::embedding::EmbedRequest;
    /// let request = EmbedRequest::new(vec!["search document".into()])?
    ///     .with_dimensions(std::num::NonZeroU32::new(128).unwrap());
    /// assert_eq!(request.inputs(), &["search document"]);
    /// assert_eq!(request.dimensions().unwrap().get(), 128);
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub fn inputs(&self) -> &[String] {
        &self.inputs
    }

    /// Returns the requested output dimensionality.
    ///
    /// # Examples
    ///
    /// ```
    /// use provider_sdk::embedding::EmbedRequest;
    /// let request = EmbedRequest::new(vec!["search document".into()])?
    ///     .with_dimensions(std::num::NonZeroU32::new(128).unwrap());
    /// assert_eq!(request.inputs(), &["search document"]);
    /// assert_eq!(request.dimensions().unwrap().get(), 128);
    ///
    /// # Ok::<(), provider_sdk::SdkError>(())
    /// ```
    ///
    #[must_use]
    pub const fn dimensions(&self) -> Option<NonZeroU32> {
        self.dimensions
    }
}

impl<'de> Deserialize<'de> for EmbedRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            inputs: Vec<String>,
            dimensions: Option<NonZeroU32>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut request = Self::new(wire.inputs).map_err(serde::de::Error::custom)?;
        request.dimensions = wire.dimensions;
        Ok(request)
    }
}

/// Embedding vectors returned in request-input order.
///
/// # Examples
///
/// ```
/// use provider_sdk::{embedding::EmbedResponse, generation::Usage};
/// let response = EmbedResponse { vectors: vec![vec![0.25, 0.75]], usage: Usage::default() };
/// assert_eq!(response.vectors.len(), 1);
/// assert!(response.usage.total_tokens.is_none());
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EmbedResponse {
    /// Dense vectors corresponding exactly to request inputs.
    pub vectors: Vec<Vec<f32>>,
    /// Provider usage accounting.
    pub usage: Usage,
}
