use std::time::Duration;

use futures_util::{Stream, StreamExt};
use reqwest::Response;

use crate::KitError;
use crate::error::{ErrorPhase, NativeErrorDetails, Operation, ResponseViolation};
use crate::generation::{ContentPart, FunctionCall, GenerateEvent, GenerateEventStream, Usage};
use crate::models::ModelId;
use crate::transport::{CheckedRequestSpec, RequestExecution, SseEvent, SseParser};

const FRAME_LIMIT: usize = 8 * 1024 * 1024;
const STATE_LIMIT: usize = 32 * 1024 * 1024;
const STATE_ITEM_LIMIT: usize = 4_096;

#[derive(Clone, Debug, Default)]
pub(crate) struct StreamingAccumulator {
    model: Option<ModelId>,
    usage: Usage,
    content: String,
    tool_calls: Vec<FunctionCall>,
    retained: (usize, usize),
    started: bool,
}

impl StreamingAccumulator {
    pub(crate) fn with_model(model: ModelId) -> Self {
        Self {
            model: Some(model),
            ..Self::default()
        }
    }

    pub(crate) fn started_event(&mut self, model: ModelId) -> Option<GenerateEvent> {
        self.model = Some(model.clone());
        (!std::mem::replace(&mut self.started, true)).then_some(GenerateEvent::Started { model })
    }

    pub(crate) fn model(&self) -> Result<&ModelId, KitError> {
        self.model.as_ref().ok_or_else(|| {
            KitError::invalid_response(ResponseViolation::MissingField { field: "model" })
                .at_phase(ErrorPhase::Stream)
                .with_message("stream completed before the provider supplied a model id")
        })
    }

    pub(crate) fn set_usage(&mut self, usage: Usage) {
        self.usage = usage;
    }

    pub(crate) fn usage(&self) -> &Usage {
        &self.usage
    }

    pub(crate) fn retain(&mut self, bytes: usize, items: usize) -> Result<(), KitError> {
        let retained = (
            self.retained.0.saturating_add(bytes),
            self.retained.1.saturating_add(items),
        );
        if retained.0 > STATE_LIMIT {
            return Err(
                KitError::invalid_response(ResponseViolation::LimitExceeded { limit: STATE_LIMIT })
                    .at_phase(ErrorPhase::Stream)
                    .with_message("normalized stream state exceeded its byte budget"),
            );
        }
        if retained.1 > STATE_ITEM_LIMIT {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "normalized stream state exceeded its item budget",
                })
                .at_phase(ErrorPhase::Stream),
            );
        }
        self.retained = retained;
        Ok(())
    }

    pub(crate) fn push_text(&mut self, delta: &str) -> Result<Option<GenerateEvent>, KitError> {
        if delta.is_empty() {
            return Ok(None);
        }
        self.retain(delta.len(), 0)?;
        self.content.push_str(delta);
        Ok(Some(GenerateEvent::ContentDelta {
            delta: delta.to_string(),
        }))
    }

    pub(crate) fn take_output(&mut self) -> (Vec<ContentPart>, Vec<FunctionCall>) {
        let content = (!self.content.is_empty())
            .then(|| ContentPart::Text {
                text: std::mem::take(&mut self.content),
            })
            .into_iter()
            .collect();
        (content, std::mem::take(&mut self.tool_calls))
    }

    pub(crate) fn push_call(&mut self, call: FunctionCall) -> Result<(), KitError> {
        let bytes =
            call.id().map_or(0, str::len) + call.name().len() + call.arguments().to_string().len();
        self.retain(bytes, 1)?;
        self.tool_calls.push(call);
        Ok(())
    }
}

pub(crate) enum StreamFrame {
    Sse(SseEvent),
    Line(String),
    End,
}

#[derive(Default)]
pub(crate) struct FrameDecoder {
    parser: Option<SseParser>,
    bytes: usize,
}

impl FrameDecoder {
    pub(crate) fn sse() -> Self {
        Self {
            parser: Some(SseParser::default()),
            ..Self::default()
        }
    }

    pub(crate) fn into_stream<C, F>(
        self,
        response: Response,
        operation: &'static str,
        timeout: Duration,
        normalize: F,
    ) -> Result<GenerateEventStream<C>, KitError>
    where
        C: Send + 'static,
        F: FnMut(StreamFrame) -> Result<Vec<GenerateEvent<C>>, KitError> + Send + 'static,
    {
        let metadata = NativeErrorDetails {
            request_id: response
                .headers()
                .get("x-request-id")
                .or_else(|| response.headers().get("request-id"))
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            retry_after: response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            ..NativeErrorDetails::default()
        };
        let state = FramedStreamState {
            body: Some(response.bytes_stream()),
            chunk: None,
            cursor: 0,
            line: String::new(),
            utf8_suffix: Vec::new(),
            decoder: self,
            normalize,
            events: Vec::new().into_iter(),
            context: (operation, timeout),
            completed: false,
            metadata,
        };
        Ok(GenerateEventStream::new(futures_util::stream::try_unfold(
            state,
            |state| state.next_event(),
        )))
    }

    fn push(&mut self, line: String) -> Result<Option<StreamFrame>, KitError> {
        let Some(parser) = self.parser.as_mut() else {
            let line = line.trim();
            return Ok((!line.is_empty()).then(|| StreamFrame::Line(line.to_string())));
        };
        self.bytes = self
            .bytes
            .checked_add(line.len() + 1)
            .ok_or_else(Self::limit)?;
        if self.bytes > FRAME_LIMIT {
            return Err(Self::limit());
        }
        let complete = line.trim_end_matches('\r').is_empty();
        let event = parser.push_line(&line).map(StreamFrame::Sse);
        if complete {
            self.bytes = 0;
        }
        Ok(event)
    }

    fn limit() -> KitError {
        KitError::invalid_response(ResponseViolation::LimitExceeded { limit: FRAME_LIMIT })
            .at_phase(ErrorPhase::Stream)
            .with_message(format!(
                "stream frame exceeded the {FRAME_LIMIT}-byte limit"
            ))
    }

    fn finish(&self) -> Result<(), KitError> {
        if self
            .parser
            .as_ref()
            .is_some_and(SseParser::has_pending_data)
        {
            return Err(KitError::invalid_response(ResponseViolation::UnexpectedEnd)
                .at_phase(ErrorPhase::Stream)
                .with_message("generation stream ended within an SSE event"));
        }
        Ok(())
    }
}

struct FramedStreamState<S, F, B, C> {
    body: Option<S>,
    chunk: Option<B>,
    cursor: usize,
    line: String,
    utf8_suffix: Vec<u8>,
    decoder: FrameDecoder,
    normalize: F,
    events: std::vec::IntoIter<GenerateEvent<C>>,
    context: (&'static str, Duration),
    completed: bool,
    metadata: NativeErrorDetails,
}

impl<S, F, B, C> FramedStreamState<S, F, B, C>
where
    S: Stream<Item = Result<B, reqwest::Error>> + Send + Unpin + 'static,
    F: FnMut(StreamFrame) -> Result<Vec<GenerateEvent<C>>, KitError> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
    C: Send + 'static,
{
    async fn next_event(self) -> Result<Option<(GenerateEvent<C>, Self)>, KitError> {
        let metadata = self.metadata.clone();
        self.next_event_inner().await.map_err(|error| {
            let mut details = error.native_details().clone();
            details.request_id = details.request_id.or(metadata.request_id);
            details.retry_after = details.retry_after.or(metadata.retry_after);
            error
                .at_operation(Operation::GenerateStream)
                .with_native_capture(details)
        })
    }

    async fn next_event_inner(mut self) -> Result<Option<(GenerateEvent<C>, Self)>, KitError> {
        loop {
            if let Some(event) = self.events.next() {
                return Ok(Some((event, self)));
            }
            if self.completed {
                return Ok(None);
            }
            if self.consume_chunk()? {
                continue;
            }
            if let Some(body) = self.body.as_mut() {
                match body.next().await {
                    Some(Ok(chunk)) => {
                        self.chunk = Some(chunk);
                    }
                    Some(Err(error)) => {
                        return Err(RequestExecution::transport_error(
                            self.context.0,
                            self.context.1,
                            error,
                        )
                        .at_phase(ErrorPhase::ReadBody));
                    }
                    None => {
                        self.body = None;
                        if !self.utf8_suffix.is_empty() {
                            return Err(Self::utf8_error());
                        }
                        if !self.line.is_empty() {
                            self.finish_line()?;
                        }
                        self.decoder.finish()?;
                    }
                }
                continue;
            }
            self.normalize(StreamFrame::End)?;
            if self.completed {
                continue;
            }
            return Err(KitError::invalid_response(ResponseViolation::UnexpectedEnd)
                .at_phase(ErrorPhase::Stream)
                .with_message("generation stream ended before a completed event"));
        }
    }

    fn consume_chunk(&mut self) -> Result<bool, KitError> {
        let Some(chunk) = self.chunk.take() else {
            return Ok(false);
        };
        let bytes = chunk.as_ref();
        if self.cursor == bytes.len() {
            self.cursor = 0;
            return Ok(true);
        }
        if let Some(offset) = bytes[self.cursor..].iter().position(|byte| *byte == b'\n') {
            let end = self.cursor + offset;
            self.push_bytes(&bytes[self.cursor..end])?;
            self.cursor = end + 1;
            self.chunk = Some(chunk);
            self.finish_line()?;
        } else {
            self.push_bytes(&bytes[self.cursor..])?;
            self.cursor = 0;
        }
        Ok(true)
    }

    fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), KitError> {
        let buffered = self.line.len() + self.utf8_suffix.len();
        if bytes.len() > FRAME_LIMIT.saturating_sub(buffered) {
            return Err(FrameDecoder::limit());
        }

        self.utf8_suffix.extend_from_slice(bytes);
        let valid = match std::str::from_utf8(&self.utf8_suffix) {
            Ok(_) => self.utf8_suffix.len(),
            Err(error) if error.error_len().is_none() => error.valid_up_to(),
            Err(_) => return Err(Self::utf8_error()),
        };
        let suffix = self.utf8_suffix.split_off(valid);
        let decoded = String::from_utf8(std::mem::replace(&mut self.utf8_suffix, suffix))
            .map_err(|_| Self::utf8_error())?;
        self.line.push_str(&decoded);
        Ok(())
    }

    fn finish_line(&mut self) -> Result<(), KitError> {
        if !self.utf8_suffix.is_empty() {
            return Err(Self::utf8_error());
        }
        if let Some(frame) = self.decoder.push(std::mem::take(&mut self.line))? {
            self.normalize(frame)?;
        }
        Ok(())
    }

    fn normalize(&mut self, frame: StreamFrame) -> Result<(), KitError> {
        let (event_id, diagnostic) = match &frame {
            StreamFrame::Sse(event) => (
                event.id.clone(),
                Some(CheckedRequestSpec::diagnostic_body(event.data.as_bytes())),
            ),
            StreamFrame::Line(line) => (
                None,
                Some(CheckedRequestSpec::diagnostic_body(line.as_bytes())),
            ),
            StreamFrame::End => (None, None),
        };
        let mut events = (self.normalize)(frame).map_err(|error| {
            let mut details = error.native_details().clone();
            details.event_id = details.event_id.or(event_id);
            if details.body.is_none()
                && let Some(diagnostic) = &diagnostic
            {
                details.body = diagnostic.body.clone();
                details.truncated |= diagnostic.truncated;
            }
            error.with_native_capture(details)
        })?;
        if events.len() > 128 {
            return Err(
                KitError::invalid_response(ResponseViolation::ProtocolViolation {
                    rule: "one native stream frame expanded into too many portable events",
                })
                .at_phase(ErrorPhase::Stream)
                .with_diagnostic(diagnostic),
            );
        }
        if let Some(index) = events
            .iter()
            .position(|event| matches!(event, GenerateEvent::Completed(_)))
        {
            events.truncate(index + 1);
            self.completed = true;
            self.body = None;
            self.chunk = None;
        }
        self.events = events.into_iter();
        Ok(())
    }

    fn utf8_error() -> KitError {
        KitError::invalid_response(ResponseViolation::InvalidUtf8)
            .at_phase(ErrorPhase::Decode)
            .with_message("generation stream contained invalid UTF-8")
    }
}
