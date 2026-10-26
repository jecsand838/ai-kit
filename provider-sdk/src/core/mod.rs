//! Crate-private logic shared by the concrete provider implementations.

mod attachments;
pub(crate) mod content;
mod files;
mod generation;
mod session;
mod tools;

pub(crate) use content::text_parts;
pub(crate) use files::{limit_after_path, multipart_file_part};
pub(crate) use generation::{
    FrameDecoder, GenerateRequestView, GenerateResponseOptions, HostedJsonCursor,
    NormalizedGenerationParts, StreamFrame, StreamingAccumulator, artifact,
    ensure_nonempty_generation_parts, file_reference, file_reference_part, generate_response,
    hosted_url_artifact, inline_media_part, push_provider_thinking_content, push_text_part,
    tool_call,
};
pub(crate) use session::{SessionSocket, SessionSocketMessage};
pub(crate) use tools::{
    HostedToolMode, filter_tools_for_choice, selected_tools_for_choice, tool_name, tool_result_text,
};
