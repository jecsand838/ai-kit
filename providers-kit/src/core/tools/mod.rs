//! Shared tool selection and provider rendering support.

mod render;
mod selection;

use serde::{Deserialize, Serialize};

pub(crate) use render::tool_result_text;
pub(crate) use selection::{filter_tools_for_choice, selected_tools_for_choice, tool_name};

/// Execution mode used when validating provider-hosted tool support.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostedToolMode {
    /// Non-streaming generation request.
    Generate,
    /// Streaming generation request.
    GenerateStream,
}
