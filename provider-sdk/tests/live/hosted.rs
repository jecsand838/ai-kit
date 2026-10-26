use crate::live_failures::CaseFailure;
use provider_sdk::error::{ErrorKind, Operation};
use std::num::NonZeroU32;
use std::time::Duration;

use provider_sdk::generation::{GenerateRequest, GenerationConfig, ProviderToolSpec, ToolChoice};
use provider_sdk::hosted::HostedTool;

use crate::hosted_assertions::{FileSearchContract, HostedAnswer, HostedEvidence, HostedProvider};
use crate::live::{LiveProvider, Mode, Service};
use crate::live_assertions::FileSearchStage;
use provider_sdk::generation::GenerateResponse;
use provider_sdk::hosted::{HostedToolOutput, HostedToolStatus};

enum HostedScenario {
    WebSearch,
    FileSearch { current: bool },
    UrlContext,
    CodeExecution,
    CodeFailure,
    UnreachableUrl,
    MissingStore,
    DeletedStoreConsistency,
    Unsupported(HostedTool),
}

struct HostedCase {
    service: Service,
    mode: Mode,
    scenario: HostedScenario,
}

impl HostedCase {
    const SOURCE: &str = "https://www.iana.org/help/example-domains";
    const FILE_MARKER: &str = "ai-kit-provider-strict-file-search-v1";

    fn new(service: Service, mode: Mode, scenario: HostedScenario) -> Self {
        Self {
            service,
            mode,
            scenario,
        }
    }

    async fn run(self) -> Result<(), CaseFailure> {
        let selected_file_search = if self.service == Service::Gemini {
            match &self.scenario {
                HostedScenario::FileSearch { current: false }
                | HostedScenario::DeletedStoreConsistency => {
                    let model = LiveProvider::required("TEST_GEMINI_MODEL_FILE_SEARCH")?;
                    if model != "models/gemini-2.5-flash" {
                        return Err(
                            "legacy File Search qualification requires models/gemini-2.5-flash"
                                .into(),
                        );
                    }
                    Some(model)
                }
                HostedScenario::FileSearch { current: true } => {
                    Some("models/gemini-3.8-flash".to_owned())
                }
                _ => None,
            }
        } else {
            None
        };
        let mut live = LiveProvider::new(self.service)?;
        if let Some(model) = selected_file_search {
            live.default_model = model;
        }
        let result = self.exercise(&live).await;
        live.finish(result).await;
        Ok(())
    }

    fn protocol(&self) -> Result<HostedProvider, CaseFailure> {
        match self.service {
            Service::OpenAi => Ok(HostedProvider::OpenAi),
            Service::Anthropic => Ok(HostedProvider::Anthropic),
            Service::Gemini => Ok(HostedProvider::Gemini),
            Service::Ollama => Err("Ollama has no provider-hosted execution protocol".into()),
        }
    }

    fn request(
        &self,
        live: &LiveProvider,
        prompt: &str,
        tool: HostedTool,
        force: bool,
    ) -> Result<GenerateRequest, CaseFailure> {
        let forcing_supported = !(matches!(self.service, Service::Gemini)
            || self.service == Service::Anthropic
                && matches!(
                    live.default_model(),
                    "claude-fable-5-1" | "claude-opus-5-5" | "claude-sonnet-5-5"
                ));
        let choice = if force && forcing_supported {
            ToolChoice::Required
        } else {
            ToolChoice::Auto
        };
        LiveProvider::text_request(prompt)?
            .with_tools(vec![ProviderToolSpec::Hosted(tool)])
            .map_err(CaseFailure::from)?
            .with_generation(
                GenerationConfig::default()
                    .with_max_output_tokens(NonZeroU32::new(4_096).ok_or("nonzero output budget")?)
                    .with_tool_choice(choice),
            )
            .map_err(CaseFailure::from)
    }

    fn assess(
        response: &GenerateResponse,
        answer: Option<HostedAnswer<'_>>,
        evidence: Result<(), String>,
    ) -> Result<(), CaseFailure> {
        let Err(message) = HostedEvidence::assess(response, answer, evidence) else {
            return Ok(());
        };
        let mut failure = CaseFailure::from(message);
        for output in &response.hosted_tool_outputs {
            let status = match output {
                HostedToolOutput::WebSearch { status, .. }
                | HostedToolOutput::FileSearch { status, .. }
                | HostedToolOutput::UrlContext { status, .. } => status,
                HostedToolOutput::CodeExecution {
                    status, results, ..
                } => {
                    for result in results {
                        if let Some(HostedToolStatus::Failed { error }) = &result.status {
                            failure = failure.with_remote(error.clone());
                        }
                    }
                    status
                }
            };
            if let Some(HostedToolStatus::Failed { error }) = status {
                failure = failure.with_remote(error.clone());
            }
        }
        Err(failure)
    }

    async fn unavailable_url() -> Result<String, CaseFailure> {
        let url = format!(
            "https://httpbin.org/status/404?strict_live={}",
            LiveProvider::nonce()
        );
        // This checks only the external fixture. The hosted response must still
        // independently prove the provider's failed retrieval of this exact URL.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| format!("HTTP-404 fixture client: {error}"))?;
        let response = client
            .get(&url)
            .send()
            .await
            .map_err(|error| format!("HTTP-404 fixture preflight: {error}"))?;
        if response.status() != reqwest::StatusCode::NOT_FOUND || response.url().as_str() != url {
            return Err(format!(
                "HTTP-404 fixture preflight returned {}; expected exact URL and HTTP 404",
                response.status()
            )
            .into());
        }
        // No response body is needed or consumed.
        Ok(url)
    }

    async fn exercise(&self, live: &LiveProvider) -> Result<(), CaseFailure> {
        match &self.scenario {
            HostedScenario::WebSearch => {
                let request = self.request(live,
                    "Use the hosted web research tool to find the authoritative reserved-domain documentation for example.com and example.org. In that document's main page title, extract only the abbreviation immediately before \"-managed Reserved Domains\". Write the answer line as plain text with the literal prefix Answer:, one space, and only the extracted abbreviation; use no Markdown on that line. Put supporting explanation and native source citations on other lines, outside the answer line.",
                 HostedTool::WebSearch, true)?;
                let response = live.generate_hosted(&request, self.mode).await?;
                Self::assess(
                    &response,
                    Some(HostedAnswer::Organization("IANA")),
                    HostedEvidence::web_search(&response, self.protocol()?),
                )
            }
            HostedScenario::FileSearch { .. } => {
                let store = match self.service {
                    Service::OpenAi => LiveProvider::required("TEST_OPENAI_FILE_SEARCH_STORE_ID")?,
                    Service::Gemini => LiveProvider::required("TEST_GEMINI_FILE_SEARCH_STORE_ID")?,
                    Service::Anthropic | Service::Ollama => {
                        return Err("unsupported file-search positive case".into());
                    }
                };
                let request = self.request(live,
                    "Search the configured provider store for the provider strict-test fixture. Read the document and put its exact marker on one line formatted Answer: <value>. The marker must be obtained from the stored document; it is not provided in this prompt. Cite the document you read using native citation metadata. Put any explanation and citation text on other lines, outside the answer line.",
                    HostedTool::FileSearch { store_ids: vec![store] }, true,
                )?;
                let response = live
                    .generate_file_search(&request, self.mode, FileSearchStage::Retrieval)
                    .await?;
                let contract = FileSearchContract::for_request(
                    self.protocol()?,
                    match self.mode {
                        Mode::Unary => Operation::Generate,
                        Mode::Streaming => Operation::GenerateStream,
                    },
                    live.default_model(),
                );
                let assessment = Self::assess(
                    &response,
                    Some(HostedAnswer::Marker(Self::FILE_MARKER)),
                    HostedEvidence::file_search(&response, self.protocol()?, contract),
                );
                if contract == FileSearchContract::GeminiStreaming {
                    let report =
                        live.write_file_search_observation(contract, &response, &assessment);
                    CaseFailure::combine(assessment, report)
                } else {
                    assessment
                }
            }
            HostedScenario::UrlContext => {
                let request = self.request(live, &format!(
                    "Use the hosted URL retrieval tool to read {source}. In the page's Further Reading section, find the link title ending in \"-managed Reserved Domains\". Extract only the abbreviation immediately before \"-managed\" in that link title. Your entire visible answer must be exactly one line: Answer: <abbreviation>. Start with Answer:; provide no preface, explanation, markdown, or rendered citation text. Preserve native source citation metadata.", source = Self::SOURCE,
                ), HostedTool::UrlContext, true)?;
                let response = live.generate_hosted(&request, self.mode).await?;
                Self::assess(
                    &response,
                    Some(HostedAnswer::Organization("IANA")),
                    HostedEvidence::url_context(&response, self.protocol()?, Self::SOURCE),
                )
            }
            HostedScenario::CodeExecution => {
                let request = self.request(live,
                    "Use the hosted code-execution tool to execute Python print(sum(i*i for i in range(1, 101))). Execute the code and read its stdout. Put the resulting unformatted decimal integer on one line formatted Answer: <value>. Put any explanation on other lines, outside the answer line.",
                    HostedTool::CodeExecution, true,
                )?;
                let response = live.generate_hosted(&request, self.mode).await?;
                let expected = (1_u64..=100).map(|value| value * value).sum();
                Self::assess(
                    &response,
                    Some(HostedAnswer::Integer(expected)),
                    HostedEvidence::code_success(
                        &response,
                        self.protocol()?,
                        &expected.to_string(),
                    ),
                )
            }
            HostedScenario::CodeFailure => {
                let sentinel = format!("STRICT_HOSTED_FAILURE_{}", LiveProvider::nonce());
                let protocol = self.protocol()?;
                let source = HostedEvidence::failure_source(protocol, &sentinel)?;
                let language = match protocol {
                    HostedProvider::Anthropic => "Bash command",
                    HostedProvider::OpenAi | HostedProvider::Gemini => "Python source",
                };
                let restriction = match protocol {
                    HostedProvider::OpenAi => {
                        "The child exception must remain uncaught; forward only its actual stderr and propagate its failed exit. Do not add or change statements."
                    }
                    HostedProvider::Anthropic | HostedProvider::Gemini => {
                        "Do not add imports, comments, commands, or lines."
                    }
                };
                let prompt = format!(
                    "This is a harmless execution-error propagation test. Use the hosted code-execution tool exactly once with this exact {language}:\n{source}\n{restriction} Do not catch the exception, repair the code, or print a fabricated traceback. After the tool returns the real failure, briefly report what happened."
                );
                let request = self.request(live, &prompt, HostedTool::CodeExecution, true)?;
                if let Some(directory) = std::env::var_os("PROVIDER_SDK_EVIDENCE_DIR") {
                    live.capture_observation(
                        directory,
                        "code-failure-input",
                        serde_json::json!({
                            "boundary": "before_generate_hosted",
                            "selected_model": live.default_model(),
                            "mode": match self.mode {
                                Mode::Unary => "unary",
                                Mode::Streaming => "streaming",
                            },
                            "expected_sentinel": &sentinel,
                            "requested_source": &source,
                            "prompt": &prompt,
                        }),
                    );
                }
                let response = live.generate_hosted(&request, self.mode).await?;
                Self::assess(
                    &response,
                    None,
                    HostedEvidence::code_failure(&response, protocol, &sentinel),
                )
            }
            HostedScenario::UnreachableUrl => {
                let url = Self::unavailable_url().await?;
                let final_answer = if self.service == Service::Gemini {
                    " After the tool attempt, reply only RETRIEVAL_FAILED if retrieval failed. Do not add explanatory prose or rendered citations. Preserve native retrieval metadata."
                } else {
                    ""
                };
                let request = self.request(live, &format!(
                    "Use the hosted URL retrieval tool to retrieve this exact URL: {url}. This is a harmless retrieval-error test. Make the real retrieval attempt and report its failure. Do not use web search, substitute another URL, or describe a fabricated retrieval.{final_answer}"
                ), HostedTool::UrlContext, true)?;
                let response = live.generate_hosted(&request, self.mode).await?;
                Self::assess(
                    &response,
                    None,
                    HostedEvidence::url_failure(&response, self.protocol()?, &url),
                )
            }
            HostedScenario::DeletedStoreConsistency => {
                live.gemini_deleted_store_consistency(self.mode).await
            }
            HostedScenario::MissingStore => {
                let missing =
                    HostedEvidence::missing_store_id(self.protocol()?, &LiveProvider::nonce())?;
                let request = self.request(
                    live,
                    "Search the configured store for its strict-test fixture marker.",
                    HostedTool::FileSearch {
                        store_ids: vec![missing.clone()],
                    },
                    false,
                )?;
                self.reject(live, &request, Some(&missing)).await
            }
            HostedScenario::Unsupported(tool) => {
                let request = self.request(
                    live,
                    "Exercise this hosted tool declaration.",
                    tool.clone(),
                    false,
                )?;
                self.reject(live, &request, None).await
            }
        }
    }

    async fn reject(
        &self,
        live: &LiveProvider,
        request: &GenerateRequest,
        missing_store: Option<&str>,
    ) -> Result<(), CaseFailure> {
        let error = live.rejected(&live.model()?, request, self.mode).await?;
        if let Some(missing) = missing_store {
            let operation = match self.mode {
                Mode::Unary => Operation::Generate,
                Mode::Streaming => Operation::GenerateStream,
            };
            HostedEvidence::missing_store(&error, operation, self.protocol()?, missing)
                .map_err(|failure| CaseFailure::from(failure).with_sdk(error))?;
        } else if !matches!(error.kind(), ErrorKind::Unsupported) {
            return Err(
                CaseFailure::from("hosted negative returned the wrong error class").with_sdk(error),
            );
        }
        Ok(())
    }
}

#[tokio::test]
async fn openai_web_search_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::OpenAi, Mode::Unary, HostedScenario::WebSearch)
        .run()
        .await
}

#[tokio::test]
async fn openai_web_search_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(Service::OpenAi, Mode::Streaming, HostedScenario::WebSearch)
        .run()
        .await
}

#[tokio::test]
async fn anthropic_web_search_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Anthropic, Mode::Unary, HostedScenario::WebSearch)
        .run()
        .await
}

#[tokio::test]
async fn anthropic_web_search_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Streaming,
        HostedScenario::WebSearch,
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_web_search_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Gemini, Mode::Unary, HostedScenario::WebSearch)
        .run()
        .await
}

#[tokio::test]
async fn gemini_web_search_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Gemini, Mode::Streaming, HostedScenario::WebSearch)
        .run()
        .await
}

#[tokio::test]
async fn openai_file_search_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::OpenAi,
        Mode::Unary,
        HostedScenario::FileSearch { current: false },
    )
    .run()
    .await
}

#[tokio::test]
async fn openai_file_search_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::OpenAi,
        Mode::Streaming,
        HostedScenario::FileSearch { current: false },
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_file_search_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Unary,
        HostedScenario::FileSearch { current: false },
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_file_search_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Streaming,
        HostedScenario::FileSearch { current: false },
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_url_context_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Anthropic, Mode::Unary, HostedScenario::UrlContext)
        .run()
        .await
}

#[tokio::test]
async fn anthropic_url_context_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Streaming,
        HostedScenario::UrlContext,
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_url_context_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Gemini, Mode::Unary, HostedScenario::UrlContext)
        .run()
        .await
}

#[tokio::test]
async fn gemini_url_context_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Gemini, Mode::Streaming, HostedScenario::UrlContext)
        .run()
        .await
}

#[tokio::test]
async fn openai_code_execution_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::OpenAi, Mode::Unary, HostedScenario::CodeExecution)
        .run()
        .await
}

#[tokio::test]
async fn openai_code_execution_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::OpenAi,
        Mode::Streaming,
        HostedScenario::CodeExecution,
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_code_execution_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Unary,
        HostedScenario::CodeExecution,
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_code_execution_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Streaming,
        HostedScenario::CodeExecution,
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_code_execution_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Gemini, Mode::Unary, HostedScenario::CodeExecution)
        .run()
        .await
}

#[tokio::test]
async fn gemini_code_execution_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Streaming,
        HostedScenario::CodeExecution,
    )
    .run()
    .await
}

#[tokio::test]
async fn openai_code_failure_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::OpenAi, Mode::Unary, HostedScenario::CodeFailure)
        .run()
        .await
}

#[tokio::test]
async fn openai_code_failure_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::OpenAi,
        Mode::Streaming,
        HostedScenario::CodeFailure,
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_code_failure_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Anthropic, Mode::Unary, HostedScenario::CodeFailure)
        .run()
        .await
}

#[tokio::test]
async fn anthropic_code_failure_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Streaming,
        HostedScenario::CodeFailure,
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_code_failure_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Gemini, Mode::Unary, HostedScenario::CodeFailure)
        .run()
        .await
}

#[tokio::test]
async fn gemini_code_failure_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Streaming,
        HostedScenario::CodeFailure,
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_unreachable_url_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Unary,
        HostedScenario::UnreachableUrl,
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_unreachable_url_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Streaming,
        HostedScenario::UnreachableUrl,
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_unreachable_url_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::Gemini, Mode::Unary, HostedScenario::UnreachableUrl)
        .run()
        .await
}

#[tokio::test]
async fn gemini_unreachable_url_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Streaming,
        HostedScenario::UnreachableUrl,
    )
    .run()
    .await
}

#[tokio::test]
async fn openai_missing_store_unary() -> Result<(), CaseFailure> {
    HostedCase::new(Service::OpenAi, Mode::Unary, HostedScenario::MissingStore)
        .run()
        .await
}

#[tokio::test]
async fn openai_missing_store_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::OpenAi,
        Mode::Streaming,
        HostedScenario::MissingStore,
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_deleted_store_consistency_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Unary,
        HostedScenario::DeletedStoreConsistency,
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_deleted_store_consistency_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Streaming,
        HostedScenario::DeletedStoreConsistency,
    )
    .run()
    .await
}

#[tokio::test]
async fn openai_url_context_is_unsupported_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::OpenAi,
        Mode::Unary,
        HostedScenario::Unsupported(HostedTool::UrlContext),
    )
    .run()
    .await
}

#[tokio::test]
async fn openai_url_context_is_unsupported_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::OpenAi,
        Mode::Streaming,
        HostedScenario::Unsupported(HostedTool::UrlContext),
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_file_search_is_unsupported_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Unary,
        HostedScenario::Unsupported(HostedTool::FileSearch {
            store_ids: vec!["strict-unused-store".into()],
        }),
    )
    .run()
    .await
}

#[tokio::test]
async fn anthropic_file_search_is_unsupported_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Anthropic,
        Mode::Streaming,
        HostedScenario::Unsupported(HostedTool::FileSearch {
            store_ids: vec!["strict-unused-store".into()],
        }),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_web_search_is_unsupported_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Unary,
        HostedScenario::Unsupported(HostedTool::WebSearch),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_web_search_is_unsupported_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Streaming,
        HostedScenario::Unsupported(HostedTool::WebSearch),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_file_search_is_unsupported_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Unary,
        HostedScenario::Unsupported(HostedTool::FileSearch {
            store_ids: vec!["strict-unused-store".into()],
        }),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_file_search_is_unsupported_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Streaming,
        HostedScenario::Unsupported(HostedTool::FileSearch {
            store_ids: vec!["strict-unused-store".into()],
        }),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_url_context_is_unsupported_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Unary,
        HostedScenario::Unsupported(HostedTool::UrlContext),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_url_context_is_unsupported_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Streaming,
        HostedScenario::Unsupported(HostedTool::UrlContext),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_code_execution_is_unsupported_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Unary,
        HostedScenario::Unsupported(HostedTool::CodeExecution),
    )
    .run()
    .await
}

#[tokio::test]
async fn ollama_code_execution_is_unsupported_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Ollama,
        Mode::Streaming,
        HostedScenario::Unsupported(HostedTool::CodeExecution),
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_current_file_search_unary() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Unary,
        HostedScenario::FileSearch { current: true },
    )
    .run()
    .await
}

#[tokio::test]
async fn gemini_current_file_search_streaming() -> Result<(), CaseFailure> {
    HostedCase::new(
        Service::Gemini,
        Mode::Streaming,
        HostedScenario::FileSearch { current: true },
    )
    .run()
    .await
}
