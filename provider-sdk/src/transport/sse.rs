#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SseEvent {
    pub(crate) event: Option<String>,
    pub(crate) data: String,
    pub(crate) id: Option<String>,
    pub(crate) retry: Option<u64>,
}

#[derive(Clone, Debug, Default)]
struct PartialEvent {
    event: Option<String>,
    data_lines: Vec<String>,
    id: Option<String>,
    retry: Option<u64>,
}

impl PartialEvent {
    fn finalize(&mut self) -> Option<SseEvent> {
        if self.event.is_none()
            && self.data_lines.is_empty()
            && self.id.is_none()
            && self.retry.is_none()
        {
            return None;
        }
        let event = SseEvent {
            event: self.event.take(),
            data: self.data_lines.join("\n"),
            id: self.id.take(),
            retry: self.retry.take(),
        };
        self.data_lines.clear();
        Some(event)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SseParser {
    current: PartialEvent,
}

impl SseParser {
    pub(crate) fn has_pending_data(&self) -> bool {
        !self.current.data_lines.is_empty()
    }

    pub(crate) fn push_line(&mut self, line: &str) -> Option<SseEvent> {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            return self.current.finalize();
        }
        if line.starts_with(':') {
            return None;
        }

        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "event" => self.current.event = Some(value.to_string()),
            "data" => self.current.data_lines.push(value.to_string()),
            "id" => self.current.id = Some(value.to_string()),
            "retry" => {
                if let Ok(retry) = value.parse::<u64>() {
                    self.current.retry = Some(retry);
                }
            }
            _ => {}
        }
        None
    }
}
