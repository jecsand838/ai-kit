use serde_json::Value;

use crate::SdkError;
use crate::core::{HostedJsonCursor, hosted_url_artifact};
use crate::error::{
    ClassificationBasis, ErrorKind, ErrorPhase, NativeErrorDetails, RemoteFailure, ResponseDetails,
    ResponseViolation,
};
use crate::hosted::{
    HostedCodeExecutionResult, HostedExecutableCode, HostedFileSearchResult, HostedToolOutput,
    HostedToolStatus, HostedWebSearchAction, HostedWebSearchResult, HostedWebSearchSource,
};
use crate::providers::openai::wire::{OpenAiErrorObject, OpenAiOutputItem};
use crate::transport::CheckedRequestSpec;

pub(crate) struct OpenAiHostedOutput<'a> {
    item: &'a OpenAiOutputItem,
    body: NativeErrorDetails,
}

impl<'a> OpenAiHostedOutput<'a> {
    pub(crate) fn new(item: &'a OpenAiOutputItem) -> Self {
        Self {
            item,
            body: CheckedRequestSpec::diagnostic_json(item).unwrap_or_default(),
        }
    }

    pub(crate) fn web_search(&self) -> Result<HostedToolOutput, SdkError> {
        let cursor = HostedJsonCursor::for_object(
            "OpenAI web_search_call",
            self.body.clone(),
            &self.item.extra,
        );
        let action = cursor
            .object()?
            .get("action")
            .map(|value| self.web_search_action(value))
            .transpose()?;
        let results = cursor
            .optional_array_field("results")?
            .unwrap_or_default()
            .iter()
            .enumerate()
            .map(|(index, value)| self.web_search_result(value, index))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(HostedToolOutput::WebSearch {
            status: self.status()?,
            action,
            results,
            citations: Vec::new(),
        })
    }

    pub(crate) fn file_search(&self) -> Result<HostedToolOutput, SdkError> {
        let cursor = HostedJsonCursor::for_object(
            "OpenAI file_search_call",
            self.body.clone(),
            &self.item.extra,
        );
        let results = match cursor.object()?.get("results") {
            None | Some(Value::Null) => None,
            Some(Value::Array(values)) => Some(
                values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| self.file_search_result(value, index))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some(value) => {
                return Err(cursor
                    .invalid(
                        ResponseViolation::InvalidField { field: "results" },
                        "results must be an array or null",
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: crate::error::JsonValueKind::Array,
                        actual: crate::error::JsonValueKind::of(Some(value)),
                    }));
            }
        };
        Ok(HostedToolOutput::FileSearch {
            status: self.status()?,
            queries: cursor.string_array_field("queries")?,
            results,
            citations: Vec::new(),
        })
    }

    pub(crate) fn code_interpreter(&self) -> Result<HostedToolOutput, SdkError> {
        let cursor = HostedJsonCursor::for_object(
            "OpenAI code_interpreter_call",
            self.body.clone(),
            &self.item.extra,
        );
        let code = self
            .item
            .code
            .as_ref()
            .map(|code| HostedExecutableCode {
                language: None,
                code: code.clone(),
            })
            .into_iter()
            .collect();
        let mut results = Vec::new();
        let mut artifacts = Vec::new();
        let outputs = match cursor.object()?.get("outputs") {
            None | Some(Value::Null) => &[][..],
            Some(Value::Array(outputs)) => outputs.as_slice(),
            Some(value) => {
                return Err(cursor
                    .invalid(
                        ResponseViolation::InvalidField { field: "outputs" },
                        "outputs must be an array or null",
                    )
                    .with_response_details(ResponseDetails::FieldType {
                        expected: crate::error::JsonValueKind::Array,
                        actual: crate::error::JsonValueKind::of(Some(value)),
                    }));
            }
        };
        for output in outputs {
            let output_cursor = HostedJsonCursor::new(
                "OpenAI code_interpreter_call output",
                self.body.clone(),
                output,
            );
            match output_cursor.required_str_field("type")? {
                "logs" => results.push(HostedCodeExecutionResult {
                    status: None,
                    logs: Some(output_cursor.required_string_field("logs")?),
                    stdout: None,
                    stderr: None,
                    exit_code: None,
                }),
                "image" => artifacts.push(hosted_url_artifact(
                    output_cursor.required_str_field("url")?,
                    self.body.clone(),
                )?),
                other => return Err(self.unknown("outputs.type", other)),
            }
        }
        Ok(HostedToolOutput::CodeExecution {
            status: self.status()?,
            code,
            results,
            artifacts,
        })
    }

    fn web_search_action(&self, value: &Value) -> Result<HostedWebSearchAction, SdkError> {
        let cursor =
            HostedJsonCursor::new("OpenAI web_search_call.action", self.body.clone(), value);
        match cursor.required_str_field("type")? {
            "search" => {
                let mut sources = Vec::new();
                for (index, source) in cursor
                    .optional_array_field("sources")?
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                {
                    if let Some(source) = self.web_search_source(source, index)? {
                        sources.push(source);
                    }
                }
                Ok(HostedWebSearchAction::Search {
                    query: cursor.optional_str_field("query")?,
                    queries: cursor.string_array_field("queries")?,
                    sources,
                })
            }
            "open_page" => Ok(HostedWebSearchAction::OpenPage {
                url: cursor.optional_str_field("url")?,
            }),
            "find_in_page" => Ok(HostedWebSearchAction::FindInPage {
                url: cursor.required_string_field("url")?,
                pattern: cursor.required_string_field("pattern")?,
            }),
            other => Err(self.unknown("action.type", other)),
        }
    }

    fn web_search_source(
        &self,
        value: &Value,
        index: usize,
    ) -> Result<Option<HostedWebSearchSource>, SdkError> {
        let cursor = HostedJsonCursor::new(
            format!("OpenAI web_search_call.action.sources[{index}]"),
            self.body.clone(),
            value,
        );
        let object = cursor.object()?;
        if object.get("type").and_then(Value::as_str) == Some("api") {
            if cursor.required_str_field("name")?.trim().is_empty() {
                return Err(cursor.invalid(
                    ResponseViolation::InvalidField { field: "name" },
                    "name must not be blank",
                ));
            }
            return Ok(None);
        }
        Ok(Some(HostedWebSearchSource {
            title: cursor.optional_str_field("title")?,
            url: cursor.required_string_field("url")?,
        }))
    }

    fn web_search_result(
        &self,
        value: &Value,
        index: usize,
    ) -> Result<HostedWebSearchResult, SdkError> {
        let cursor = HostedJsonCursor::new(
            format!("OpenAI web_search_call.results[{index}]"),
            self.body.clone(),
            value,
        );
        Ok(HostedWebSearchResult {
            title: cursor.optional_str_field("title")?,
            url: cursor.required_string_field("url")?,
            snippet: cursor.optional_str_field("snippet")?,
        })
    }

    fn file_search_result(
        &self,
        value: &Value,
        index: usize,
    ) -> Result<HostedFileSearchResult, SdkError> {
        let cursor = HostedJsonCursor::new(
            format!("OpenAI file_search_call.results[{index}]"),
            self.body.clone(),
            value,
        );
        Ok(HostedFileSearchResult {
            file_id: cursor.required_string_field("file_id")?,
            file_name: cursor.optional_str_field("filename")?,
            text: cursor.optional_str_field("text")?,
            score: cursor.optional_number_field("score")?,
        })
    }

    fn status(&self) -> Result<Option<HostedToolStatus>, SdkError> {
        let Some(value) = self.item.status.as_deref() else {
            return Ok(None);
        };
        let status = match value {
            "in_progress" => HostedToolStatus::InProgress,
            "searching"
                if matches!(
                    self.item.kind.as_str(),
                    "web_search_call" | "file_search_call"
                ) =>
            {
                HostedToolStatus::InProgress
            }
            "interpreting" if self.item.kind == "code_interpreter_call" => {
                HostedToolStatus::InProgress
            }
            "completed" => HostedToolStatus::Completed,
            "incomplete" => HostedToolStatus::Incomplete,
            "failed" => {
                let mut details = self.body.clone();
                details.native_status = Some(value.to_owned());
                details.tool_call_id = self.item.id.clone();
                let error = match self.item.extra.get("error") {
                    Some(value) if !value.is_null() => {
                        serde_json::from_value::<OpenAiErrorObject>(value.clone())
                            .map_err(|error| {
                                SdkError::invalid_response(ResponseViolation::from_json_error(
                                    &error, "error",
                                ))
                                .at_phase(ErrorPhase::Normalize)
                                .with_response_details(ResponseDetails::json(&error))
                                .with_diagnostic(Some(self.body.clone()))
                            })?
                            .remote_failure(Some(details))?
                    }
                    _ => RemoteFailure::from_native(
                        ErrorKind::ProviderRejected,
                        ClassificationBasis::Unclassified,
                        details,
                    )?,
                };
                HostedToolStatus::Failed { error }
            }
            other => return Err(self.unknown("status", other)),
        };
        Ok(Some(status))
    }

    fn unknown(&self, field: &'static str, value: &str) -> SdkError {
        SdkError::invalid_response(ResponseViolation::UnknownVariant { field })
            .at_phase(ErrorPhase::Normalize)
            .with_response_details(ResponseDetails::Discriminator {
                actual: value.to_owned(),
            })
            .with_diagnostic(Some(self.body.clone()))
    }
}
