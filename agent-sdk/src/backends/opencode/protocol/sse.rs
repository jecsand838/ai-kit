use std::collections::VecDeque;
use std::time::Duration;

use crate::SdkError;

const MAX_SSE_BUFFER_BYTES: usize = 1 << 20;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenCodeSseFrame {
    event: Option<String>,
    data: String,
    id: Option<String>,
}

impl OpenCodeSseFrame {
    pub(crate) fn new(event: Option<String>, data: String, id: Option<String>) -> Self {
        Self { event, data, id }
    }

    pub(crate) fn into_parts(self) -> (Option<String>, String, Option<String>) {
        (self.event, self.data, self.id)
    }
}

#[derive(Debug, Default)]
pub(crate) struct OpenCodeSseDecoder {
    buffer: Vec<u8>,
    data: String,
    event: Option<String>,
    id: Option<String>,
    skip_leading_lf: bool,
}

pub(crate) struct OpenCodeEventStream {
    response: reqwest::Response,
    decoder: OpenCodeSseDecoder,
    pending: VecDeque<OpenCodeSseFrame>,
    idle_timeout: Duration,
}

impl OpenCodeEventStream {
    pub(crate) fn new(
        response: reqwest::Response,
        idle_timeout: Duration,
    ) -> Result<Self, SdkError> {
        if idle_timeout < Duration::from_secs(30) {
            return Err(SdkError::invalid_state(
                "event-stream silence deadline is below its validated minimum",
            ));
        }
        Ok(Self {
            response,
            decoder: OpenCodeSseDecoder::new(),
            pending: VecDeque::new(),
            idle_timeout,
        })
    }

    pub(crate) async fn next_complete(&mut self) -> Result<OpenCodeSseFrame, SdkError> {
        if let Some(frame) = self.pending.pop_front() {
            return Ok(frame);
        }
        let deadline = tokio::time::Instant::now() + self.idle_timeout;
        loop {
            let chunk = tokio::time::timeout_at(deadline, self.response.chunk())
                .await
                .map_err(|_| SdkError::timeout("event stream became silent"))?
                .map_err(|_| SdkError::integration("event stream read failed"))?
                .ok_or_else(|| SdkError::integration("event stream ended"))?;
            self.pending.extend(self.decoder.push(&chunk)?);
            if let Some(frame) = self.pending.pop_front() {
                return Ok(frame);
            }
        }
    }
}

impl OpenCodeSseDecoder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Vec<OpenCodeSseFrame>, SdkError> {
        if self
            .buffer
            .len()
            .checked_add(self.data.len())
            .and_then(|length| length.checked_add(bytes.len()))
            .is_none_or(|length| length > MAX_SSE_BUFFER_BYTES)
        {
            return Err(SdkError::protocol("SSE frame exceeds its configured bound"));
        }
        self.buffer.extend_from_slice(bytes);
        let mut frames = Vec::new();
        while let Some(line) = self.take_line()? {
            if let Some(frame) = self.process_line(&line) {
                frames.push(frame);
            }
        }
        Ok(frames)
    }

    fn take_line(&mut self) -> Result<Option<String>, SdkError> {
        if self.skip_leading_lf {
            let Some(first) = self.buffer.first() else {
                return Ok(None);
            };
            if *first == b'\n' {
                self.buffer.remove(0);
            }
            self.skip_leading_lf = false;
        }
        let Some((line_end, terminator_len)) = Self::line_ending(&self.buffer) else {
            return Ok(None);
        };
        let trailing_cr = self.buffer[line_end] == b'\r' && line_end + 1 == self.buffer.len();
        let line = std::str::from_utf8(&self.buffer[..line_end])
            .map_err(|_| crate::SdkError::protocol("event stream text is not valid UTF-8"))?
            .to_owned();
        self.buffer.drain(..line_end + terminator_len);
        self.skip_leading_lf = trailing_cr;
        Ok(Some(line))
    }

    fn process_line(&mut self, line: &str) -> Option<OpenCodeSseFrame> {
        if line.is_empty() {
            return self.dispatch();
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => {
                let value = value.strip_prefix(' ').unwrap_or(value);
                (field, value)
            }
            None => (line, ""),
        };
        match field {
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            "event" => self.event = Some(value.to_owned()),
            "id" if !value.contains('\0') => self.id = Some(value.to_owned()),
            "retry" if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) => {
                let _ = value.parse::<u64>();
            }
            "retry" => {}
            _ => {}
        }
        None
    }

    fn dispatch(&mut self) -> Option<OpenCodeSseFrame> {
        if self.data.is_empty() {
            self.event = None;
            return None;
        }
        if self.data.ends_with('\n') {
            self.data.pop();
        }
        let data = std::mem::take(&mut self.data);
        let event = self.event.take();
        Some(OpenCodeSseFrame::new(event, data, self.id.clone()))
    }

    fn line_ending(buffer: &[u8]) -> Option<(usize, usize)> {
        for (index, byte) in buffer.iter().enumerate() {
            match byte {
                b'\n' => return Some((index, 1)),
                b'\r' => {
                    let terminator_len = if buffer.get(index + 1) == Some(&b'\n') {
                        2
                    } else {
                        1
                    };
                    return Some((index, terminator_len));
                }
                _ => {}
            }
        }
        None
    }
}
