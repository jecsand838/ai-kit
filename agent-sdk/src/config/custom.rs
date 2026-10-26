use std::num::NonZeroUsize;

use crate::{ModelSelector, SdkError, ToolSelection};

/// Bounded work admitted by one Custom prompt.
///
/// ```
/// use agent_sdk::config::CustomLoopLimits;
/// let limits = CustomLoopLimits::new(8, 0, 1)?;
/// assert_eq!(limits.max_tool_actions(), 0);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CustomLoopLimits {
    max_model_turns: NonZeroUsize,
    max_tool_actions: usize,
    max_parallel_tool_actions: NonZeroUsize,
}

impl CustomLoopLimits {
    /// Validates the model-turn, aggregate-action, and parallel-action bounds.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidConfig`] for zero model turns or
    /// parallel actions, or more than 65,536 aggregate actions. Zero aggregate
    /// actions disables tools without disabling model turns.
    ///
    /// ```
    /// use agent_sdk::config::CustomLoopLimits;
    /// let limits = CustomLoopLimits::new(8, 0, 1)?;
    /// assert_eq!(limits.max_tool_actions(), 0);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(
        max_model_turns: usize,
        max_tool_actions: usize,
        max_parallel_tool_actions: usize,
    ) -> Result<Self, SdkError> {
        let max_model_turns = NonZeroUsize::new(max_model_turns)
            .ok_or_else(|| SdkError::invalid_config("Custom model-turn limit must be positive"))?;
        let max_parallel_tool_actions =
            NonZeroUsize::new(max_parallel_tool_actions).ok_or_else(|| {
                SdkError::invalid_config("Custom parallel-tool limit must be positive")
            })?;
        if max_tool_actions > 65_536 {
            return Err(SdkError::invalid_config(
                "Custom aggregate tool-action limit exceeds the portable bound",
            ));
        }
        Ok(Self {
            max_model_turns,
            max_tool_actions,
            max_parallel_tool_actions,
        })
    }

    /// Returns the maximum number of model turns admitted by one prompt.
    ///
    /// ```
    /// use agent_sdk::config::CustomLoopLimits;
    ///
    /// let limits = CustomLoopLimits::new(8, 16, 2)?;
    /// assert_eq!(limits.max_model_turns(), 8);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    #[must_use]
    pub fn max_model_turns(self) -> usize {
        self.max_model_turns.get()
    }

    /// Returns the aggregate tool-action limit for one prompt; zero disables actions.
    ///
    /// ```
    /// use agent_sdk::config::CustomLoopLimits;
    ///
    /// let limits = CustomLoopLimits::new(8, 0, 2)?;
    /// assert_eq!(limits.max_tool_actions(), 0);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    #[must_use]
    pub fn max_tool_actions(self) -> usize {
        self.max_tool_actions
    }

    /// Returns the maximum number of tool actions that may execute concurrently.
    ///
    /// ```
    /// use agent_sdk::config::CustomLoopLimits;
    ///
    /// let limits = CustomLoopLimits::new(8, 16, 2)?;
    /// assert_eq!(limits.max_parallel_tool_actions(), 2);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    #[must_use]
    pub fn max_parallel_tool_actions(self) -> usize {
        self.max_parallel_tool_actions.get()
    }

    pub(crate) fn validate(self) -> Result<(), SdkError> {
        if self.max_tool_actions > 65_536 {
            return Err(SdkError::invalid_state(
                "stored Custom tool-action limit exceeds the portable bound",
            ));
        }
        Ok(())
    }
}

/// Approval behavior for model-requested tool actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CustomToolApproval {
    /// Execute admitted actions without a caller interaction.
    Allow,
    /// Reject every model-requested action.
    Deny,
    /// Publish one durable notification and await its resolution.
    Interaction,
}

/// Passive policy for the provider-backed Custom runtime.
///
/// ```
/// use agent_sdk::{config::{CustomAgentConfig, CustomLoopLimits, CustomToolApproval},
///     model::ModelSelector, tool::ToolSelection};
/// let config = CustomAgentConfig::new(ModelSelector::new("exact-model-id")?,
///     ToolSelection::None, CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
/// assert_eq!(config.default_tool_selection(), &ToolSelection::None);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
#[derive(Clone, Debug)]
pub struct CustomAgentConfig {
    default_model: ModelSelector,
    default_tool_selection: ToolSelection,
    loop_limits: CustomLoopLimits,
    tool_approval: CustomToolApproval,
}

impl CustomAgentConfig {
    /// Constructs a complete Custom policy with no runtime resources.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for an empty named tool
    /// selection, duplicate names, or names outside the portable tool vocabulary.
    /// Model availability is checked by the selected backend during use.
    ///
    /// ```
    /// use agent_sdk::{config::{CustomAgentConfig, CustomLoopLimits, CustomToolApproval},
    ///     model::ModelSelector, tool::ToolSelection};
    /// let config = CustomAgentConfig::new(ModelSelector::new("exact-model-id")?,
    ///     ToolSelection::None, CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// assert_eq!(config.default_tool_selection(), &ToolSelection::None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(
        default_model: ModelSelector,
        default_tool_selection: ToolSelection,
        loop_limits: CustomLoopLimits,
        tool_approval: CustomToolApproval,
    ) -> Result<Self, SdkError> {
        default_tool_selection.validate()?;
        Ok(Self {
            default_model,
            default_tool_selection,
            loop_limits,
            tool_approval,
        })
    }

    /// Returns the configured default model selector.
    ///
    /// ```
    /// use agent_sdk::{
    ///     config::{CustomAgentConfig, CustomLoopLimits, CustomToolApproval},
    ///     model::ModelSelector,
    ///     tool::ToolSelection,
    /// };
    ///
    /// let config = CustomAgentConfig::new(
    ///     ModelSelector::new("model-slug")?, ToolSelection::Auto,
    ///     CustomLoopLimits::new(8, 16, 2)?, CustomToolApproval::Allow,
    /// )?;
    /// assert_eq!(config.default_model().as_str(), "model-slug");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    #[must_use]
    pub fn default_model(&self) -> &ModelSelector {
        &self.default_model
    }

    /// Returns the configured default tool selection.
    ///
    /// ```
    /// use agent_sdk::{
    ///     config::{CustomAgentConfig, CustomLoopLimits, CustomToolApproval},
    ///     model::ModelSelector,
    ///     tool::ToolSelection,
    /// };
    ///
    /// let config = CustomAgentConfig::new(
    ///     ModelSelector::new("model-slug")?, ToolSelection::Auto,
    ///     CustomLoopLimits::new(8, 16, 2)?, CustomToolApproval::Allow,
    /// )?;
    /// assert_eq!(config.default_tool_selection(), &ToolSelection::Auto);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    #[must_use]
    pub fn default_tool_selection(&self) -> &ToolSelection {
        &self.default_tool_selection
    }

    /// Returns the configured bounds for one prompt's model and tool work.
    ///
    /// ```
    /// use agent_sdk::{
    ///     config::{CustomAgentConfig, CustomLoopLimits, CustomToolApproval},
    ///     model::ModelSelector,
    ///     tool::ToolSelection,
    /// };
    ///
    /// let limits = CustomLoopLimits::new(8, 16, 2)?;
    /// let config = CustomAgentConfig::new(
    ///     ModelSelector::new("model-slug")?, ToolSelection::Auto,
    ///     limits, CustomToolApproval::Allow,
    /// )?;
    /// assert_eq!(config.loop_limits(), limits);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    #[must_use]
    pub fn loop_limits(&self) -> CustomLoopLimits {
        self.loop_limits
    }

    /// Returns the approval policy for model-requested tool actions.
    ///
    /// ```
    /// use agent_sdk::{
    ///     config::{CustomAgentConfig, CustomLoopLimits, CustomToolApproval},
    ///     model::ModelSelector,
    ///     tool::ToolSelection,
    /// };
    ///
    /// let config = CustomAgentConfig::new(
    ///     ModelSelector::new("model-slug")?, ToolSelection::Auto,
    ///     CustomLoopLimits::new(8, 16, 2)?, CustomToolApproval::Interaction,
    /// )?;
    /// assert_eq!(config.tool_approval(), CustomToolApproval::Interaction);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    #[must_use]
    pub fn tool_approval(&self) -> CustomToolApproval {
        self.tool_approval
    }

    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        self.default_tool_selection.validate()?;
        self.loop_limits.validate()
    }
}
