use serde::{Deserialize, Serialize};

use crate::config::GeminiServiceTier;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum GeminiNativeServiceTier {
    Standard,
    Flex,
    Priority,
}

impl GeminiNativeServiceTier {
    pub(crate) fn as_rest_value(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Flex => "flex",
            Self::Priority => "priority",
        }
    }
}

impl From<GeminiServiceTier> for GeminiNativeServiceTier {
    fn from(value: GeminiServiceTier) -> Self {
        match value {
            GeminiServiceTier::Standard => Self::Standard,
            GeminiServiceTier::Flex => Self::Flex,
            GeminiServiceTier::Priority => Self::Priority,
        }
    }
}

pub(crate) fn render_service_tier(value: GeminiServiceTier) -> Option<String> {
    match value {
        GeminiServiceTier::Standard => None,
        GeminiServiceTier::Flex => Some(GeminiNativeServiceTier::Flex.as_rest_value().to_string()),
        GeminiServiceTier::Priority => Some(
            GeminiNativeServiceTier::Priority
                .as_rest_value()
                .to_string(),
        ),
    }
}
