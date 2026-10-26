//! Shared generation rendering, normalization, and stream framing.

mod hosted;
mod normalize;
mod stream;

use crate::generation::{
    GenerationConfig, GenerationContinuation, Message, ProviderToolSpec, ReasoningConfig,
    ResponseFormat,
};

pub(crate) use hosted::{HostedJsonCursor, hosted_url_artifact};
pub(crate) use normalize::{
    GenerateResponseOptions, NormalizedGenerationParts, artifact, ensure_nonempty_generation_parts,
    file_reference, file_reference_part, generate_response, inline_media_part,
    push_provider_thinking_content, push_text_part, tool_call,
};
pub(crate) use stream::{FrameDecoder, StreamFrame, StreamingAccumulator};

#[derive(Debug)]
pub(crate) struct GenerateRequestView<'a, C = GenerationContinuation> {
    pub(crate) messages: &'a [Message<C>],
    pub(crate) tools: &'a [ProviderToolSpec],
    pub(crate) response_format: Option<&'a ResponseFormat>,
    pub(crate) reasoning: Option<&'a ReasoningConfig>,
    pub(crate) generation: &'a GenerationConfig,
}

impl<C> Copy for GenerateRequestView<'_, C> {}

impl<C> Clone for GenerateRequestView<'_, C> {
    fn clone(&self) -> Self {
        *self
    }
}
