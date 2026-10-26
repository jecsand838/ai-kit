use std::collections::BTreeSet;

use provider_sdk::SdkError;
use provider_sdk::error::{ClassificationBasis, ErrorKind, ErrorPhase, Operation};
use provider_sdk::generation::{Citation, ContentPart, FileReference, GenerateResponse};
use provider_sdk::hosted::{
    HostedCodeExecutionResult, HostedExecutableCode, HostedToolOutput, HostedToolStatus,
    HostedWebSearchAction,
};
use serde_json::{Value, json};
use url::Url;

use crate::live_assertions::{ErrorEvidence, GenerationEvidence};
use crate::live_failures::CaseFailure;

#[derive(Clone, Copy)]
pub enum HostedProvider {
    OpenAi,
    Anthropic,
    Gemini,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileSearchContract {
    Exclusive,
    GeminiStreaming,
}

impl FileSearchContract {
    pub fn for_request(provider: HostedProvider, operation: Operation, model: &str) -> Self {
        if matches!(provider, HostedProvider::Gemini)
            && operation == Operation::GenerateStream
            && model == "models/gemini-2.5-flash"
        {
            Self::GeminiStreaming
        } else {
            Self::Exclusive
        }
    }

    pub fn observation(self, response: &GenerateResponse) -> Value {
        json!({
            "contract": match self {
                Self::Exclusive => "exclusive",
                Self::GeminiStreaming => "gemini_streaming_unpaired_file_search_declaration",
            },
            "strict_file_search_only_requirement_met": response.hosted_tool_outputs.iter()
                .all(|output| matches!(output, HostedToolOutput::FileSearch { .. })),
            "unpaired_file_search_declaration_observed": response.hosted_tool_outputs.iter()
                .any(HostedEvidence::unpaired_file_search_declaration),
            "code_execution_claimed": false,
            "response": CaseFailure::response_evidence(response),
        })
    }
}

pub enum HostedAnswer<'a> {
    Organization(&'a str),
    Marker(&'a str),
    Integer(u64),
}

impl HostedAnswer<'_> {
    pub fn verify(&self, response: &GenerateResponse) -> Result<(), String> {
        let text = GenerationEvidence::text(response)
            .map_err(|error| format!("answer-format: {error}"))?;
        let part_starts = response
            .message
            .content()
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text),
                _ => None,
            })
            .scan(0, |offset, part| {
                let start = *offset;
                *offset += part.len();
                Some(start)
            });
        // Part starts may anchor a field, but never terminate its value. Keep
        // the original bytes so split prefixes, suffixes and decoration count.
        let answer_starts: BTreeSet<usize> = part_starts
            .chain(text.match_indices('\n').map(|(offset, _)| offset + 1))
            .filter_map(|start| {
                let line = text[start..].lines().next()?;
                let trimmed = line.trim_ascii_start();
                trimmed
                    .starts_with("Answer:")
                    .then_some(start + line.len() - trimmed.len())
            })
            .collect();
        let mut answers = answer_starts.into_iter().map(|start| {
            text[start + "Answer:".len()..]
                .lines()
                .next()
                .unwrap_or_default()
        });
        let value = answers
            .next()
            .ok_or("answer-format: missing Answer: line")?
            .trim_ascii();
        if answers.next().is_some() {
            return Err("answer-format: duplicate Answer: lines".into());
        }
        if value.is_empty() {
            return Err("answer-format: empty Answer: value".into());
        }
        match self {
            Self::Organization(expected) | Self::Marker(expected) => {
                if value == *expected || value.strip_suffix('.') == Some(*expected) {
                    Ok(())
                } else if value.chars().any(char::is_whitespace)
                    || value.contains(['[', ']', '(', ')', '<', '>'])
                {
                    Err("answer-format: Answer: value contains extra text or citation decoration; put citations on another line".into())
                } else {
                    Err(
                        "answer-incorrect: Answer: value differs from the expected complete token"
                            .into(),
                    )
                }
            }
            Self::Integer(expected) => {
                if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(
                        "answer-format: integer answer must contain only ASCII decimal digits"
                            .into(),
                    );
                }
                let actual = value
                    .parse::<u64>()
                    .map_err(|_| "answer-format: integer answer exceeds u64".to_owned())?;
                if actual == *expected {
                    Ok(())
                } else {
                    Err(format!(
                        "answer-incorrect: expected integer {expected}, received {actual}"
                    ))
                }
            }
        }
    }
}

pub struct HostedEvidence;

impl HostedEvidence {
    pub fn assess(
        response: &GenerateResponse,
        answer: Option<HostedAnswer<'_>>,
        evidence: Result<(), String>,
    ) -> Result<(), String> {
        let mut failures = Vec::new();
        if let Err(error) = GenerationEvidence::completed(response) {
            failures.push(format!("generation-completion: {error}"));
        }
        if let Some(answer) = answer
            && let Err(error) = answer.verify(response)
        {
            failures.push(error);
        }
        if let Err(error) = evidence {
            failures.push(format!("hosted-evidence: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    pub fn missing_store_id(provider: HostedProvider, nonce: &str) -> Result<String, String> {
        let suffix = nonce
            .strip_prefix("strict-")
            .ok_or("fixture: missing-store nonce must start with strict-")?;
        if suffix.len() != 32
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(
                "fixture: missing-store nonce needs exactly 32 lowercase hexadecimal digits".into(),
            );
        }
        match provider {
            HostedProvider::OpenAi => Ok(format!("vs_{nonce}")),
            HostedProvider::Gemini => Ok(format!("fileSearchStores/{nonce}")),
            HostedProvider::Anthropic => Err("fixture: Anthropic has no file-search store".into()),
        }
    }

    pub fn failure_source(provider: HostedProvider, sentinel: &str) -> Result<String, String> {
        if sentinel.is_empty()
            || !sentinel
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err("fixture: execution sentinel must be a nonempty ASCII identifier".into());
        }
        match provider {
            HostedProvider::OpenAi => Ok(format!(
                "import subprocess, sys\nresult = subprocess.run([sys.executable, \"-c\", 'raise RuntimeError(\"{sentinel}\")'], capture_output=True, text=True, timeout=10)\nprint(result.stderr, end=\"\")\nresult.check_returncode()"
            )),
            HostedProvider::Gemini => Ok(format!("raise RuntimeError(\"{sentinel}\")")),
            HostedProvider::Anthropic => {
                Ok(format!("python3 -c 'raise RuntimeError(\"{sentinel}\")'"))
            }
        }
    }

    fn positive_outcomes(response: &GenerateResponse) -> Result<(), String> {
        for output in &response.hosted_tool_outputs {
            let status = match output {
                HostedToolOutput::WebSearch { status, .. }
                | HostedToolOutput::FileSearch { status, .. }
                | HostedToolOutput::UrlContext { status, .. } => status,
                HostedToolOutput::CodeExecution {
                    status, results, ..
                } => {
                    if results.iter().any(|result| {
                        matches!(
                            result.status,
                            Some(HostedToolStatus::Failed { .. } | HostedToolStatus::Incomplete)
                        ) || result.exit_code.is_some_and(|code| code != 0)
                    }) {
                        return Err(
                            "positive scenario contains an unsuccessful hosted execution result"
                                .into(),
                        );
                    }
                    status
                }
            };
            if matches!(
                status,
                Some(HostedToolStatus::Failed { .. } | HostedToolStatus::Incomplete)
            ) {
                return Err(
                    "positive scenario contains a failed or incomplete hosted operation".into(),
                );
            }
        }
        Ok(())
    }

    pub fn web_search(response: &GenerateResponse, provider: HostedProvider) -> Result<(), String> {
        let mut proven = false;
        let mut failures = Self::positive_outcomes(response)
            .err()
            .into_iter()
            .collect::<Vec<_>>();
        for output in &response.hosted_tool_outputs {
            let HostedToolOutput::WebSearch {
                status,
                action,
                results,
                citations,
            } = output
            else {
                continue;
            };
            if let Err(error) = Self::search_status(provider, status) {
                failures.push(error);
            }
            if !Self::qualified_search(provider, status) {
                continue;
            }
            let Some(action) = action else {
                continue;
            };
            let mut sources = Vec::new();
            match action {
                HostedWebSearchAction::Search {
                    sources: action_sources,
                    ..
                } => {
                    sources.extend(action_sources.iter().map(|source| source.url.as_str()));
                    sources.extend(results.iter().map(|result| result.url.as_str()));
                }
                HostedWebSearchAction::OpenPage { url } => {
                    if matches!(provider, HostedProvider::OpenAi) {
                        sources.extend(url.as_deref());
                    } else {
                        failures.push(
                            "provider returned a web action it does not natively support".into(),
                        );
                    }
                }
                HostedWebSearchAction::FindInPage { url, pattern } => {
                    if matches!(provider, HostedProvider::OpenAi) && !pattern.trim().is_empty() {
                        sources.push(url.as_str());
                    } else {
                        failures.push(
                            "provider returned an unsupported or empty find-in-page action".into(),
                        );
                    }
                }
            }
            proven |= sources.into_iter().any(|source| {
                Self::usable_url(source)
                    && (Self::cites_url(citations, source)
                        || Self::cites_url(&response.citations, source))
            });
        }
        if !proven {
            failures.push("web search needs a qualified action with a usable native source and matching citation from that operation or the final answer".into());
        }
        Self::finish_checks(failures)
    }

    pub fn file_search(
        response: &GenerateResponse,
        provider: HostedProvider,
        contract: FileSearchContract,
    ) -> Result<(), String> {
        let mut proven = false;
        let mut included = false;
        let mut result_count = 0;
        let mut citation_count = 0;
        let mut matching_count = 0;
        let mut declarations = 0;
        let mut failures = Self::positive_outcomes(response)
            .err()
            .into_iter()
            .collect::<Vec<_>>();
        if contract == FileSearchContract::GeminiStreaming
            && !matches!(provider, HostedProvider::Gemini)
        {
            failures.push("qualified File Search declarations are Gemini-only".into());
        }
        for output in &response.hosted_tool_outputs {
            if contract == FileSearchContract::GeminiStreaming
                && matches!(provider, HostedProvider::Gemini)
                && Self::unpaired_file_search_declaration(output)
            {
                declarations += 1;
                continue;
            }
            let HostedToolOutput::FileSearch {
                status,
                results,
                citations,
                ..
            } = output
            else {
                failures.push("file search returned an unexpected hosted family".into());
                continue;
            };
            if let Err(error) = Self::search_status(provider, status) {
                failures.push(error);
            }
            if !Self::qualified_search(provider, status) {
                continue;
            }
            if results
                .as_ref()
                .is_some_and(|rows| rows.iter().any(|row| row.file_id.trim().is_empty()))
            {
                failures.push("file search returned a blank source identity".into());
            }
            if citations.iter().chain(&response.citations).any(|citation| matches!(citation, Citation::File { reference: FileReference::Provider { id, .. }, .. } if id.trim().is_empty())) {
                failures.push("file search returned a blank citation identity".into());
            }
            // Identity-free Gemini retrievedContext is honest grounding data. The
            // independent Answer: check still requires the undisclosed stored marker.
            if matches!(provider, HostedProvider::Gemini) {
                included |= results.is_some();
                proven |= results.is_some();
                continue;
            }
            let Some(results) = results else {
                continue;
            };
            included = true;
            result_count += results.len();
            let file_citations = citations
                .iter()
                .chain(&response.citations)
                .filter_map(|citation| match citation {
                    Citation::File {
                        reference: FileReference::Provider { id, .. },
                        ..
                    } => Some(id.as_str()),
                    Citation::File { .. } | Citation::Url { .. } => None,
                })
                .collect::<Vec<_>>();
            citation_count += file_citations.len();
            let matching = file_citations
                .iter()
                .filter(|id| {
                    results
                        .iter()
                        .any(|result| result.file_id == **id && !id.trim().is_empty())
                })
                .count();
            matching_count += matching;
            proven |= !results.is_empty() && matching != 0;
        }
        if declarations > 1 {
            failures.push("File Search qualification permits only one unpaired declaration".into());
        }
        if declarations != 0 && !response.artifacts.is_empty() {
            failures.push("unpaired File Search declaration returned unexpected artifacts".into());
        }
        if !proven {
            let results = if !included {
                "omitted"
            } else if result_count == 0 {
                "empty"
            } else {
                "included"
            };
            failures.push(format!("file search evidence: results={results}, result_ids={result_count}, file_citations={citation_count}, matching_citations={matching_count}; no qualified same-record retrieval witness"));
        }
        Self::finish_checks(failures)
    }

    fn unpaired_file_search_declaration(output: &HostedToolOutput) -> bool {
        let HostedToolOutput::CodeExecution {
            status: None,
            code,
            results,
            artifacts,
        } = output
        else {
            return false;
        };
        let [declaration] = code.as_slice() else {
            return false;
        };
        if declaration.language.as_deref() != Some("PYTHON")
            || !results.is_empty()
            || !artifacts.is_empty()
        {
            return false;
        }
        // Recognize only the observed declaration. Never execute it, repair it,
        // or replace the original source in the SDK response or evidence.
        let Some(argument) = declaration
            .code
            .trim_ascii()
            .strip_prefix("print(file_search.query(")
            .and_then(|source| source.strip_suffix("))"))
        else {
            return false;
        };
        let mut argument = argument.trim_ascii();
        if let Some(keyword) = argument.strip_prefix("query") {
            let Some(value) = keyword.trim_ascii_start().strip_prefix('=') else {
                return false;
            };
            argument = value.trim_ascii_start();
        }
        let mut chars = argument.chars();
        let Some(quote @ ('\'' | '"')) = chars.next() else {
            return false;
        };
        let mut escaped = false;
        let mut nonblank = false;
        while let Some(character) = chars.next() {
            if escaped {
                if !matches!(character, '\\' | '\'' | '"' | 'n' | 'r' | 't') {
                    return false;
                }
                nonblank |= matches!(character, '\\' | '\'' | '"');
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == quote {
                return nonblank && chars.as_str().trim_ascii().is_empty();
            } else if character.is_control() {
                return false;
            } else {
                nonblank |= !character.is_whitespace();
            }
        }
        false
    }

    pub fn url_context(
        response: &GenerateResponse,
        provider: HostedProvider,
        expected_url: &str,
    ) -> Result<(), String> {
        let mut proven = false;
        let mut failures = Self::positive_outcomes(response)
            .err()
            .into_iter()
            .collect::<Vec<_>>();
        for output in &response.hosted_tool_outputs {
            let HostedToolOutput::UrlContext {
                status,
                requested_url,
                retrieved_url,
                ..
            } = output
            else {
                continue;
            };
            match status {
                Some(HostedToolStatus::Failed { .. } | HostedToolStatus::Incomplete) => {
                    failures.push("URL context reported failed or incomplete retrieval".into())
                }
                Some(HostedToolStatus::Completed) => {
                    proven |= match provider {
                        HostedProvider::Anthropic => {
                            requested_url.as_deref() == Some(expected_url)
                                && retrieved_url.as_deref().is_some_and(Self::usable_url)
                        }
                        HostedProvider::Gemini => {
                            requested_url.is_none()
                                && retrieved_url.as_deref() == Some(expected_url)
                                && Self::usable_url(expected_url)
                        }
                        HostedProvider::OpenAi => false,
                    };
                }
                Some(HostedToolStatus::InProgress) | None => {}
            }
        }
        if !proven {
            failures.push(
                "URL context omitted successful retrieval correlated to the exact requested URL"
                    .into(),
            );
        }
        Self::finish_checks(failures)
    }

    pub fn url_failure(
        response: &GenerateResponse,
        provider: HostedProvider,
        expected_url: &str,
    ) -> Result<(), String> {
        let mut proven = false;
        let mut failures = Vec::new();
        for output in &response.hosted_tool_outputs {
            let HostedToolOutput::UrlContext {
                status,
                requested_url,
                retrieved_url,
                ..
            } = output
            else {
                continue;
            };
            match status {
                Some(HostedToolStatus::Completed) => {
                    failures.push("unreachable URL scenario reported completed retrieval".into())
                }
                Some(HostedToolStatus::Failed { error }) => {
                    let details = error.native_details();
                    proven |= match provider {
                        HostedProvider::Anthropic => {
                            requested_url.as_deref() == Some(expected_url)
                                && details.code.as_deref() == Some("url_not_accessible")
                                && error.kind() == ErrorKind::ProviderRejected
                        }
                        HostedProvider::Gemini => {
                            requested_url.is_none()
                                && retrieved_url.as_deref() == Some(expected_url)
                                && details.outcome.as_deref() == Some("URL_RETRIEVAL_STATUS_ERROR")
                                && error.kind() == ErrorKind::ProviderRejected
                        }
                        HostedProvider::OpenAi => false,
                    };
                }
                Some(HostedToolStatus::InProgress | HostedToolStatus::Incomplete) | None => {}
            }
        }
        if !proven {
            failures.push("unreachable URL coverage unproven: no eligible native retrieval failure for the exact target".into());
        }
        Self::finish_checks(failures)
    }

    pub fn code_success(
        response: &GenerateResponse,
        provider: HostedProvider,
        expected_line: &str,
    ) -> Result<(), String> {
        let mut proven = false;
        let mut failures = Self::positive_outcomes(response)
            .err()
            .into_iter()
            .collect::<Vec<_>>();
        for output in &response.hosted_tool_outputs {
            let HostedToolOutput::CodeExecution {
                status,
                code,
                results,
                ..
            } = output
            else {
                continue;
            };
            if let Err(error) = Self::code_declaration_status(provider, status, !code.is_empty()) {
                failures.push(error);
            }
            let source = match Self::paired_source(provider, code, results.len()) {
                Ok(source) => source,
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            if matches!(status, Some(HostedToolStatus::Failed { .. })) {
                failures.push("positive code execution returned an explicit failed record".into());
            }
            if matches!(status, Some(HostedToolStatus::Incomplete)) {
                failures.push("positive code execution returned an incomplete record".into());
            }
            for result in results {
                if matches!(
                    result.status,
                    Some(HostedToolStatus::Failed { .. } | HostedToolStatus::Incomplete)
                ) {
                    failures.push(
                        "positive code execution returned a failed or incomplete result".into(),
                    );
                }
                if result.exit_code.is_some_and(|exit| exit != 0) {
                    failures.push("positive code execution returned a nonzero exit".into());
                }
                let native_fields = match provider {
                    HostedProvider::OpenAi => {
                        result.exit_code.is_none()
                            && result.stdout.is_none()
                            && result.stderr.is_none()
                    }
                    HostedProvider::Gemini => {
                        result.exit_code.is_none()
                            && result.logs.is_none()
                            && result.stderr.is_none()
                    }
                    HostedProvider::Anthropic => {
                        result.exit_code.is_some() && result.logs.is_none()
                    }
                };
                if !native_fields {
                    failures.push(
                        "execution result violates provider-native exit or output-channel evidence"
                            .into(),
                    );
                }
                let (completed, output) = match provider {
                    HostedProvider::OpenAi => (
                        matches!(status, Some(HostedToolStatus::Completed))
                            && result.status.is_none()
                            && result.exit_code.is_none()
                            && result.stdout.is_none()
                            && result.stderr.is_none(),
                        result.logs.as_deref(),
                    ),
                    HostedProvider::Anthropic => (
                        matches!(result.status, Some(HostedToolStatus::Completed))
                            && result.exit_code == Some(0)
                            && result.logs.is_none()
                            && result.stderr.as_ref().is_none_or(|text| text.is_empty()),
                        result.stdout.as_deref(),
                    ),
                    HostedProvider::Gemini => (
                        matches!(result.status, Some(HostedToolStatus::Completed))
                            && result.exit_code.is_none()
                            && result.logs.is_none()
                            && result.stderr.is_none(),
                        result.stdout.as_deref(),
                    ),
                };
                proven |= source.is_some_and(|source| !source.trim().is_empty())
                    && completed
                    && Self::has_line(output, expected_line);
            }
        }
        if !proven {
            failures.push("code execution needs a paired executed source and provider-appropriate successful output with the exact expected line".into());
        }
        Self::finish_checks(failures)
    }

    pub fn code_failure(
        response: &GenerateResponse,
        provider: HostedProvider,
        sentinel: &str,
    ) -> Result<(), String> {
        let primary_source = Self::failure_source(provider, sentinel)?;
        let alternative_source = match provider {
            HostedProvider::OpenAi => None,
            HostedProvider::Gemini => Some(format!("raise RuntimeError('{sentinel}')")),
            HostedProvider::Anthropic => {
                Some(format!("python3 -c \"raise RuntimeError('{sentinel}')\""))
            }
        };
        let mut paired_sources = 0;
        let mut execution_items = 0;
        let mut matching_sources = 0;
        let mut exception_logs = 0;
        let mut native_failures = 0;
        let mut proven = false;
        let mut failures = Vec::new();
        for output in &response.hosted_tool_outputs {
            if let HostedToolOutput::CodeExecution {
                status,
                code,
                results,
                ..
            } = output
            {
                execution_items += 1;
                if let Err(error) =
                    Self::code_declaration_status(provider, status, !code.is_empty())
                {
                    failures.push(error);
                }
                let source = match Self::paired_source(provider, code, results.len()) {
                    Ok(source) => source,
                    Err(error) => {
                        failures.push(error);
                        None
                    }
                };
                paired_sources += usize::from(source.is_some());
                let source_matches = source.is_some_and(|source| {
                    let source = source.trim_ascii();
                    source == primary_source || alternative_source.as_deref() == Some(source)
                });
                matching_sources += usize::from(source_matches);
                for result in results {
                    let exception = Self::exception_line(result, provider, sentinel);
                    exception_logs += usize::from(exception);
                    let native_failure = match provider {
                        HostedProvider::OpenAi => {
                            matches!(
                                status,
                                Some(HostedToolStatus::Completed | HostedToolStatus::Failed { .. })
                            ) && result.status.is_none()
                                && result.exit_code.is_none()
                                && result.stdout.is_none()
                                && result.stderr.is_none()
                        }
                        HostedProvider::Anthropic => {
                            matches!(result.status, Some(HostedToolStatus::Failed { .. }))
                                && result.exit_code.is_some_and(|code| code != 0)
                                && result.logs.is_none()
                        }
                        HostedProvider::Gemini => {
                            matches!(&result.status, Some(HostedToolStatus::Failed { error }) if error.native_details().outcome.as_deref() == Some("OUTCOME_FAILED") && error.kind() == ErrorKind::ProviderRejected)
                                && result.exit_code.is_none()
                                && result.stdout.is_none()
                                && result.stderr.is_none()
                        }
                    };
                    native_failures += usize::from(native_failure);
                    proven |= source_matches && exception && native_failure;
                }
            }
        }
        if matches!(provider, HostedProvider::OpenAi) && execution_items != 1 {
            failures
                .push("OpenAI exception fixture requires exactly one interpreter execution".into());
        }
        if !proven || !failures.is_empty() {
            failures.push(format!(
                "execution-error coverage unproven: execution_items={execution_items}, paired_sources={paired_sources}, matching_sources={matching_sources}, exception_logs={exception_logs}, native_failure_results={native_failures}; expected_source={primary_source:?}; all evidence must belong to one execution"
            ));
        }
        Self::finish_checks(failures)
    }

    pub fn missing_store(
        error: &SdkError,
        operation: Operation,
        provider: HostedProvider,
        missing: &str,
    ) -> Result<(), String> {
        let codes: &[Option<&str>] = match provider {
            HostedProvider::OpenAi => &[None, Some("not_found"), Some("resource_not_found")],
            HostedProvider::Gemini => &[Some("NOT_FOUND"), Some("404")],
            HostedProvider::Anthropic => {
                return Err("fixture: Anthropic has no file-search store".into());
            }
        };
        let classified = ErrorEvidence::not_found(error, operation, missing, codes);
        if classified.is_ok() || !matches!(provider, HostedProvider::OpenAi) {
            return classified;
        }

        // OpenAI also reports this exact missing-store diagnostic as a generic
        // in-stream invalid request. Recognize that observed scenario without
        // changing the SDK's classification or accepting incidental ID mentions.
        // Structured detail records were absent in this observed envelope.
        let details = error.native_details();
        let context = error.context();
        let fixture_matches = missing
            .strip_prefix("vs_")
            .is_some_and(|nonce| Self::missing_store_id(HostedProvider::OpenAi, nonce).is_ok());
        if operation == Operation::GenerateStream
            && context.operation == operation
            && context.phase == ErrorPhase::Stream
            && context.row.is_none()
            && error.kind() == ErrorKind::InvalidRequest
            && error.classification_basis() == ClassificationBasis::NativeErrorType
            && error.remote_failure().is_some()
            && error.http_status().is_none()
            && details.error_type.as_deref() == Some("invalid_request_error")
            && details.event_type.as_deref() == Some("error")
            && details.code.is_none()
            && details.parameter.is_none()
            && details.native_status.is_none()
            && details.reported_http_status.is_none()
            && details.rpc_code.is_none()
            && details.outcome.is_none()
            && details.execution_exit_code.is_none()
            && !details.truncated
            && details.omitted_details == 0
            && fixture_matches
            && details.message.as_ref().is_some_and(|message| {
                message == &format!("Vector store with id ['{missing}'] not found.")
            })
            && details.details.is_empty()
        {
            return Ok(());
        }
        classified
    }

    fn finish_checks(failures: Vec<String>) -> Result<(), String> {
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    fn paired_source(
        provider: HostedProvider,
        code: &[HostedExecutableCode],
        result_count: usize,
    ) -> Result<Option<&str>, String> {
        if result_count == 0 {
            return Ok(None);
        }
        if code.len() != 1 {
            return Err(
                "execution result must contain exactly one source in the same normalized record"
                    .into(),
            );
        }
        if !matches!(provider, HostedProvider::OpenAi) && result_count != 1 {
            return Err(
                "execution must contain exactly one result for its same-record source".into(),
            );
        }
        Ok(Some(code[0].code.as_str()))
    }

    fn search_status(
        provider: HostedProvider,
        status: &Option<HostedToolStatus>,
    ) -> Result<(), String> {
        if matches!(provider, HostedProvider::Gemini) && status.is_some() {
            return Err(
                "Gemini grounding has no execution-status field; status must remain absent".into(),
            );
        }
        if matches!(
            status,
            Some(HostedToolStatus::Failed { .. } | HostedToolStatus::Incomplete)
        ) {
            return Err("hosted search reported failed or incomplete activity".into());
        }
        Ok(())
    }

    fn code_declaration_status(
        provider: HostedProvider,
        status: &Option<HostedToolStatus>,
        has_code: bool,
    ) -> Result<(), String> {
        if matches!(provider, HostedProvider::Gemini) && has_code && status.is_some() {
            return Err("Gemini executableCode does not report a lifecycle status".into());
        }
        Ok(())
    }

    fn qualified_search(provider: HostedProvider, status: &Option<HostedToolStatus>) -> bool {
        match provider {
            HostedProvider::Gemini => status.is_none(),
            HostedProvider::OpenAi | HostedProvider::Anthropic => {
                matches!(status, Some(HostedToolStatus::Completed))
            }
        }
    }

    fn usable_url(value: &str) -> bool {
        !value.chars().any(char::is_whitespace)
            && Url::parse(value).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
            })
    }

    fn cites_url(citations: &[Citation], expected: &str) -> bool {
        citations
            .iter()
            .any(|citation| matches!(citation, Citation::Url { url, .. } if url == expected))
    }

    fn has_line(text: Option<&str>, expected: &str) -> bool {
        text.is_some_and(|text| text.lines().any(|line| line == expected))
    }

    fn exception_line(
        result: &HostedCodeExecutionResult,
        provider: HostedProvider,
        sentinel: &str,
    ) -> bool {
        let text = match provider {
            HostedProvider::Anthropic => result.stderr.as_deref(),
            HostedProvider::OpenAi | HostedProvider::Gemini => result.logs.as_deref(),
        };
        let mut witness = false;
        if let Some(text) = text {
            let Some(plain) = Self::without_ansi_color(text) else {
                return false;
            };
            witness |= plain.lines().any(|line| {
                line.trim_matches([' ', '\t'])
                    .strip_prefix("RuntimeError:")
                    .is_some_and(|value| value.trim_matches([' ', '\t']) == sentinel)
            });
        }
        witness
    }

    fn without_ansi_color(text: &str) -> Option<String> {
        let mut plain = String::with_capacity(text.len());
        let mut characters = text.chars();
        while let Some(character) = characters.next() {
            if character != '\u{1b}' {
                if character == '\r' {
                    if characters.next() != Some('\n') {
                        return None;
                    }
                    plain.push('\n');
                    continue;
                }
                if character.is_control() && !matches!(character, '\n' | '\t') {
                    return None;
                }
                plain.push(character);
                continue;
            }
            if characters.next() != Some('[') {
                return None;
            }
            loop {
                match characters.next()? {
                    'm' => break,
                    '0'..='9' | ';' => {}
                    _ => return None,
                }
            }
        }
        Some(plain)
    }
}
