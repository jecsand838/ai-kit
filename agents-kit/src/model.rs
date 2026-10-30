//! Backend-advertised model, personality, and reasoning selectors.
//!
//! Selectors preserve their exact strings. Construction validates shape, not
//! availability; use [`crate::AgentBackend::list_models`] for the selected
//! backend's inventory. [`ModelInfo`] reports optional backend metadata without
//! turning it into a cross-provider capability promise.

//!
//! # Supplied selector restrictions
//!
//! | Backend | Reasoning | Personality |
//! | --- | --- | --- |
//! | Codex | Exact effort advertised for the selected model | Rejected: the selected native protocol retired personality selection |
//! | OpenCode | Portable override is Unsupported | Portable override is Unsupported |
//! | Custom | `minimal`, `low`, `medium`, `high`, `maximum`, mapped to Provider's portable reasoning enum | Unsupported |
//!
//! Custom's selected Provider Model still validates whether it can honor the
//! mapped effort. Arbitrary native efforts such as `xhigh` are not a Custom
//! escape hatch. An omitted override stays omitted at the Agent request boundary;
//! effective values also depend on conversation/workspace defaults. Availability
//! of a model name or a syntactically valid selector is not execution evidence.
//! Caller implementations may support their own advertised selectors.
//!
//! # Exact Codex selection
//!
//! This example requires the supported Codex executable on `PATH`, native
//! authentication and access to the exact model. Building starts a subprocess;
//! listing and prompting use the real native service, so this compiles without
//! running as a doctest. No caller Provider is constructed for a native Agent.
//!
//! ```no_run,standalone_crate
//! #![recursion_limit = "256"] // Nested concrete async futures need this on Linux.
//! use std::{num::NonZeroUsize, time::Duration};
//! use agents_kit::{AgentBackend, AgentBuilder, AgentStorage, ConversationBackend, WorkspaceBackend};
//! use agents_kit::config::{AgentConfig, CodexConfig};
//! use agents_kit::conversation::NewConversation;
//! use agents_kit::model::{ModelSelector, ReasoningEffort};
//! use agents_kit::prompt::PromptRequest;
//! use agents_kit::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
//!     WorkspaceExecutionPolicy, WorkspaceNetworkAccess, WorkspaceRoot,
//!     WorkspaceRootAccess, WorkspaceSandboxMode};
//! use storage_kit::{StateStore,
//!     state::config::{LanceStateStoreConfig, StateStoreConfig}};
//! # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
//! # runtime.block_on(async {
//! let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
//! let config = AgentConfig::new(CodexConfig::spawn(Duration::from_secs(15),
//!     Duration::from_secs(90))?.into(), Duration::from_secs(60), NonZeroUsize::MIN)?;
//! let agent = AgentBuilder::create(config, AgentStorage::new(store)).build().await?;
//! let outcome = async {
//!     let selected = ModelSelector::new("gpt-6-luna")?;
//!     let effort = ReasoningEffort::medium();
//!     let inventory = agent.list_models().await?;
//!     let model = inventory.iter().find(|item| item.selector() == &selected)
//!         .ok_or_else(|| agents_kit::KitError::unsupported("required exact model is unavailable"))?;
//!     if !model.reasoning_efforts().contains(&effort) {
//!         return Err(agents_kit::KitError::unsupported("required effort is unavailable").into());
//!     }
//!     let cwd = std::env::current_dir()?;
//!     let context = WorkspaceContext::new(cwd.clone(),
//!         vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
//!         WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
//!     let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
//!     let work = async {
//!         let request = PromptRequest::new("Describe the current task without changing files.")?
//!             .with_model(selected).with_reasoning_effort(effort)
//!             .with_timeout(Duration::from_secs(90))?;
//!         let conversation = workspace.new_conversation(NewConversation::new(None, None, vec![])?).await?;
//!         let response = conversation.prompt(request).await;
//!         let close = conversation.close().await;
//!         let response = match (response, close) {
//!             (Ok(response), Ok(())) => Ok(response),
//!             (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!             (Err(error), _) | (_, Err(error)) => Err(error),
//!         }?;
//!         assert!(!response.output().trim().is_empty()); // progress, not a prescribed wording grade
//!         Ok::<(), agents_kit::KitError>(())
//!     }.await;
//!     let close = workspace.close().await; // also runs if conversation creation failed
//!     match (work, close) {
//!         (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!         (Err(error), _) | (_, Err(error)) => Err(error),
//!         (Ok(()), Ok(())) => Ok(()),
//!     }
//! }.await;
//! let shutdown = agent.shutdown().await;
//! match (outcome, shutdown) {
//!     (Err(primary), Err(cleanup)) => Err(primary.with_supplementary(cleanup)),
//!     (Err(error), _) | (_, Err(error)) => Err(error),
//!     (Ok(()), Ok(())) => Ok(()),
//! }?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::KitError;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Selects one backend model by its canonical opaque value.
///
/// ```
/// use agents_kit::model::ModelSelector;
///
/// let selector = ModelSelector::new("model-slug")?;
/// assert_eq!(selector.as_str(), "model-slug");
/// # Ok::<(), agents_kit::KitError>(())
/// ```
pub struct ModelSelector {
    value: String,
}

impl ModelSelector {
    /// Validates and constructs a nonempty, unpadded selector.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for an empty or padded value.
    ///
    /// ```
    /// use agents_kit::model::ModelSelector;
    /// let model = ModelSelector::new("exact-model-id")?;
    /// assert_eq!(model.as_str(), "exact-model-id");
    /// assert!(ModelSelector::new(" model ").is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, KitError> {
        let value = value.into();
        if value.is_empty() || value.trim() != value {
            return Err(KitError::invalid_input(
                "model selector must be nonempty and unpadded",
            ));
        }
        Ok(Self { value })
    }

    /// Returns the canonical selector value.
    ///
    /// ```
    /// use agents_kit::model::ModelSelector;
    /// assert_eq!(ModelSelector::new("model-id")?.as_str(), "model-id");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Display for ModelSelector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.value)
    }
}

impl Serialize for ModelSelector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de> Deserialize<'de> for ModelSelector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Selects one backend-advertised personality by its canonical opaque value.
///
/// ```
/// use agents_kit::model::PersonalitySelector;
/// let personality = PersonalitySelector::new("concise")?;
/// assert_eq!(personality.as_str(), "concise");
/// # Ok::<(), agents_kit::KitError>(())
/// ```
pub struct PersonalitySelector {
    value: String,
}

impl PersonalitySelector {
    /// Validates and constructs a nonempty, unpadded personality selector.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for an empty or padded value.
    /// This does not check whether a backend advertises the personality.
    ///
    /// ```
    /// use agents_kit::model::PersonalitySelector;
    /// assert!(PersonalitySelector::new("").is_err());
    /// assert_eq!(PersonalitySelector::new("concise")?.as_str(), "concise");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, KitError> {
        let value = value.into();
        if value.is_empty() || value.trim() != value {
            return Err(KitError::invalid_input(
                "personality selector must be nonempty and unpadded",
            ));
        }
        Ok(Self { value })
    }

    /// Returns the canonical selector value.
    ///
    /// ```
    /// use agents_kit::model::PersonalitySelector;
    /// assert_eq!(PersonalitySelector::new("concise")?.as_str(), "concise");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Display for PersonalitySelector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.value)
    }
}

impl Serialize for PersonalitySelector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de> Deserialize<'de> for PersonalitySelector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Describes an open-ended backend reasoning-effort value.
///
/// The conventional constructors do not establish support on a selected model.
/// See the [supplied selector restrictions](self#supplied-selector-restrictions).
///
/// ```
/// use agents_kit::model::ReasoningEffort;
/// assert_eq!(ReasoningEffort::high().as_str(), "high");
/// ```
pub struct ReasoningEffort {
    value: String,
}

impl ReasoningEffort {
    /// Validates and constructs a nonempty, unpadded effort value.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for an empty or padded value.
    ///
    /// ```
    /// use agents_kit::model::ReasoningEffort;
    /// assert_eq!(ReasoningEffort::new("custom-effort")?.as_str(), "custom-effort");
    /// assert!(ReasoningEffort::new(" high").is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, KitError> {
        let value = value.into();
        if value.is_empty() || value.trim() != value {
            return Err(KitError::invalid_input(
                "reasoning effort must be nonempty and unpadded",
            ));
        }
        Ok(Self { value })
    }

    /// Constructs the conventional low effort value.
    ///
    /// ```
    /// assert_eq!(agents_kit::model::ReasoningEffort::low().as_str(), "low");
    /// ```
    pub fn low() -> Self {
        Self {
            value: "low".to_owned(),
        }
    }

    /// Constructs the conventional medium effort value.
    ///
    /// ```
    /// assert_eq!(agents_kit::model::ReasoningEffort::medium().as_str(), "medium");
    /// ```
    pub fn medium() -> Self {
        Self {
            value: "medium".to_owned(),
        }
    }

    /// Constructs the conventional high effort value.
    ///
    /// ```
    /// assert_eq!(agents_kit::model::ReasoningEffort::high().as_str(), "high");
    /// ```
    pub fn high() -> Self {
        Self {
            value: "high".to_owned(),
        }
    }

    /// Returns the canonical effort value.
    ///
    /// ```
    /// use agents_kit::model::ReasoningEffort;
    /// let effort = ReasoningEffort::low();
    /// assert_eq!(effort.as_str(), "low");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Display for ReasoningEffort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.value)
    }
}

impl Serialize for ReasoningEffort {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de> Deserialize<'de> for ReasoningEffort {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Describes one selectable model returned by agent inventory.
///
/// ```
/// use agents_kit::model::{ModelInfo, ModelSelector};
/// let info = ModelInfo::new(ModelSelector::new("model-id")?, None, vec![], true)?;
/// assert!(info.is_default());
/// assert_eq!(info.selector().as_str(), "model-id");
/// # Ok::<(), agents_kit::KitError>(())
/// ```
pub struct ModelInfo {
    selector: ModelSelector,
    display_name: Option<String>,
    reasoning_efforts: Vec<ReasoningEffort>,
    personalities: Vec<PersonalitySelector>,
    is_default: bool,
}

impl ModelInfo {
    /// Constructs validated model inventory information.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for a present empty or
    /// padded display name. It performs no model discovery or network request.
    ///
    /// ```
    /// use agents_kit::model::{ModelInfo, ModelSelector, ReasoningEffort};
    /// let info = ModelInfo::new(ModelSelector::new("model-id")?,
    ///     Some("Example model".into()), vec![ReasoningEffort::low()], false)?;
    /// assert_eq!(info.display_name(), Some("Example model"));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn new(
        selector: ModelSelector,
        display_name: Option<String>,
        reasoning_efforts: Vec<ReasoningEffort>,
        is_default: bool,
    ) -> Result<Self, KitError> {
        if display_name
            .as_deref()
            .is_some_and(|name| name.is_empty() || name.trim() != name)
        {
            return Err(KitError::invalid_input(
                "model display name must be nonempty and unpadded when present",
            ));
        }
        Ok(Self {
            selector,
            display_name,
            reasoning_efforts,
            personalities: Vec::new(),
            is_default,
        })
    }

    /// Replaces the ordered backend-advertised personality values.
    ///
    /// ```
    /// use agents_kit::model::{ModelInfo, ModelSelector, PersonalitySelector};
    /// let info = ModelInfo::new(ModelSelector::new("model-id")?, None, vec![], false)?
    ///     .with_personalities(vec![PersonalitySelector::new("concise")?]);
    /// assert_eq!(info.personalities()[0].as_str(), "concise");
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    #[must_use]
    pub fn with_personalities(mut self, personalities: Vec<PersonalitySelector>) -> Self {
        self.personalities = personalities;
        self
    }

    /// Returns the selector callers can round-trip into prompts and defaults.
    ///
    /// ```
    /// use agents_kit::model::{ModelInfo, ModelSelector};
    /// let info = ModelInfo::new(ModelSelector::new("model-id")?, None, vec![], false)?;
    /// let request = agents_kit::prompt::PromptRequest::new("Hello")?.with_model(info.selector().clone());
    /// assert_eq!(request.model(), Some(info.selector()));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn selector(&self) -> &ModelSelector {
        &self.selector
    }

    /// Returns the optional caller-facing model name.
    ///
    /// ```
    /// use agents_kit::model::{ModelInfo, ModelSelector};
    /// let info = ModelInfo::new(ModelSelector::new("model-id")?, None, vec![], false)?;
    /// assert_eq!(info.display_name(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }

    /// Returns the advertised open-ended reasoning-effort values.
    ///
    /// ```
    /// use agents_kit::model::{ModelInfo, ModelSelector, ReasoningEffort};
    /// let info = ModelInfo::new(ModelSelector::new("model-id")?, None,
    ///     vec![ReasoningEffort::low()], false)?;
    /// assert_eq!(info.reasoning_efforts(), &[ReasoningEffort::low()]);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn reasoning_efforts(&self) -> &[ReasoningEffort] {
        &self.reasoning_efforts
    }

    /// Returns the ordered backend-advertised personality values.
    ///
    /// ```
    /// use agents_kit::model::{ModelInfo, ModelSelector};
    /// let info = ModelInfo::new(ModelSelector::new("model-id")?, None, vec![], false)?;
    /// assert!(info.personalities().is_empty());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn personalities(&self) -> &[PersonalitySelector] {
        &self.personalities
    }

    /// Reports whether this is the sourced agent-wide default model.
    ///
    /// ```
    /// use agents_kit::model::{ModelInfo, ModelSelector};
    /// let info = ModelInfo::new(ModelSelector::new("model-id")?, None, vec![], true)?;
    /// assert!(info.is_default());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn is_default(&self) -> bool {
        self.is_default
    }
}
