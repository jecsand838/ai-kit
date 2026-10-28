use std::num::NonZeroU32;
use std::time::Duration;

use providers_kit::error::{
    ErrorKind, ErrorPhase, NativeErrorDetails, Operation, ResponseDetails, ResponseViolation,
};
use providers_kit::files::FileUploadRequest;
use providers_kit::generation::{GenerationConfig, ProviderToolSpec, ToolChoice};
use providers_kit::hosted::HostedTool;
use providers_kit::{KitError, ProviderBackend};
use reqwest::{Client, RequestBuilder};
use serde_json::json;

use super::{LiveProvider, Mode, Service};
use crate::hosted_assertions::{FileSearchContract, HostedProvider};
use crate::live_assertions::{BatchEvidence, FileSearchStage, StoreFixture};
use crate::live_failures::CaseFailure;

impl LiveProvider {
    pub(super) async fn gemini_deleted_store_consistency(
        &self,
        mode: Mode,
    ) -> Result<(), CaseFailure> {
        if self.service() != Service::Gemini {
            return Err("owned store prerequisite is Gemini-only".into());
        }
        if Self::required("TEST_GEMINI_BASE_URL")?.trim_end_matches('/')
            != "https://generativelanguage.googleapis.com"
        {
            return Err("owned store prerequisite requires the official Gemini endpoint".into());
        }
        let key = Self::required("TEST_GEMINI_API_KEY")?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| {
                CaseFailure::from(
                    KitError::new(
                        ErrorKind::Configuration,
                        Operation::Unspecified,
                        ErrorPhase::Configuration,
                    )
                    .with_native_details(NativeErrorDetails {
                        message: Some(error.to_string()),
                        ..Default::default()
                    }),
                )
                .context("native owned-store client construction")
            })?;
        let operation = match mode {
            Mode::Unary => Operation::Generate,
            Mode::Streaming => Operation::GenerateStream,
        };
        let contract = FileSearchContract::for_request(
            HostedProvider::Gemini,
            operation,
            self.default_model(),
        );
        let mut fixture = StoreFixture::new(Self::nonce(), contract);
        // The marker appears only in uploaded bytes, never in names or the prompt.
        let secret = Self::nonce();
        let journey = async {
            let request = client
                .post("https://generativelanguage.googleapis.com/v1beta/fileSearchStores")
                .header("x-goog-api-key", &key)
                .json(&json!({"displayName": fixture.display_name()}));
            fixture.begin_create()?;
            let body = fixture
                .native_bytes(request, None)
                .await
                .map_err(|error| CaseFailure::from(error).context("native owned-store creation"))?;
            fixture.accept_created(&body).map_err(|error| {
                CaseFailure::from(error).context("native owned-store creation acknowledgement")
            })?;
            fixture.upload_source(self, &secret).await?;
            fixture.import_source(&client, &key).await?;
            let target = fixture.target()?.to_owned();
            let request = Self::text_request(
                "Use the configured File Search tool to read the strict-test fixture document in the configured store. \
                 What is its strict-test fixture marker? Do not guess. Return exactly one \
                 answer line, Answer: <the complete stored marker>. Put any citations on \
                 a separate line.",
            )?
            .with_tools(vec![ProviderToolSpec::Hosted(HostedTool::FileSearch {
                store_ids: vec![target],
            })])?
            .with_generation(
                GenerationConfig::default()
                    .with_max_output_tokens(NonZeroU32::new(4096).expect("nonzero output budget"))
                    .with_tool_choice(ToolChoice::Auto),
            )?;
            let response = self
                .generate_file_search(&request, mode, FileSearchStage::Preflight)
                .await
                .map_err(|error| {
                    error.context(
                        "owned-store Kit retrieval preflight failed; fixture coverage unproven",
                    )
                })?;
            fixture
                .accept_preflight(&response, &secret)
                .map_err(|error| {
                    error.context("owned-store Kit preflight did not prove marker retrieval")
                })?;
            fixture.delete_once(&client, &key).await?;
            let deleted_at_unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| CaseFailure::from("store deletion timestamp precedes the Unix epoch"))?
                .as_millis();
            let target = fixture.negative_target()?.to_owned();
            let lookup = client
                .get(format!(
                    "https://generativelanguage.googleapis.com/v1beta/{target}"
                ))
                .header("x-goog-api-key", &key);
            match fixture.native_bytes(lookup, Some(&target)).await {
                Ok(_) => {
                    return Err(CaseFailure::from(
                        "deleted store still exists in its management lookup",
                    ));
                }
                Err(error) => fixture.accept_absence(error)?,
            }
            // Remove the separate source before observing retrieval. Native store
            // absence is a prerequisite, not a synthetic Kit generation error.
            fixture.delete_source_once(self).await?;
            let result = self
                .generate_file_search(&request, mode, FileSearchStage::PostDelete)
                .await;
            let mut probe = fixture.consistency_probe(result, operation, &secret)?;
            probe["deletion_acknowledged_at_unix_ms"] = json!(deleted_at_unix_ms);
            probe["observed_at_unix_ms"] = json!(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| CaseFailure::from(
                        "store observation timestamp precedes the Unix epoch"
                    ))?
                    .as_millis()
            );
            Ok(probe)
        }
        .await;
        if let Some(directory) = std::env::var_os("PROVIDERS_KIT_EVIDENCE_DIR") {
            self.capture_observation(directory, "store-before-cleanup", fixture.observation());
        }
        let cleanup = fixture.delete_once(&client, &key).await;
        let source_cleanup = fixture.delete_source_once(self).await;
        let source_residual = fixture
            .finish_input()
            .map_err(|error| CaseFailure::from(error).context("owned store source residual state"));
        let residual = fixture
            .finish()
            .map_err(|error| CaseFailure::from(error).context("owned store residual state"));
        if let Some(directory) = std::env::var_os("PROVIDERS_KIT_EVIDENCE_DIR") {
            self.capture_observation(directory, "store-after-cleanup", fixture.observation());
        }
        let cleanup = CaseFailure::combine(
            CaseFailure::combine(cleanup, residual),
            CaseFailure::combine(source_cleanup, source_residual),
        );
        let probe = journey.as_ref().ok().cloned();
        let result = CaseFailure::combine(journey.map(|_| ()), cleanup);
        let report = fixture.write_consistency_report(json!({
            "mode": match mode { Mode::Unary => "unary", Mode::Streaming => "streaming" },
            "fixture": fixture.observation(),
            "probe": probe,
            "failure": result.as_ref().err().map(CaseFailure::evidence),
            "coverage": "provider_consistency_observation",
            "guaranteed_deleted_store_rejection": false,
        }));
        CaseFailure::combine(result, report)
    }
}

impl StoreFixture {
    fn write_consistency_report(&self, observation: serde_json::Value) -> Result<(), CaseFailure> {
        let directory = match std::env::var_os("PROVIDERS_KIT_EVIDENCE_DIR") {
            Some(directory) => std::path::PathBuf::from(directory),
            None => {
                let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../target/providers-kit-consistency-probes")
                    .join(LiveProvider::nonce());
                let mut builder = std::fs::DirBuilder::new();
                builder.recursive(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                builder.create(&path).map_err(|error| {
                    CaseFailure::from(format!(
                        "private consistency directory creation failed: {:?}",
                        error.kind()
                    ))
                })?;
                path
            }
        };
        let path = directory.join(format!("{}.consistency.json", self.display_name()));
        let outcome = observation
            .pointer("/probe/outcome")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("probe_failed")
            .to_owned();
        let exclusion = observation
            .pointer("/fixture/file_search_observations")
            .and_then(serde_json::Value::as_array)
            .filter(|observations| !observations.is_empty())
            .map(|observations| {
                observations
                    .iter()
                    .all(|entry| entry["strict_file_search_only_requirement_met"] == true)
            });
        CaseFailure::write_required_observation(
            &path,
            observation,
            &LiveProvider::evidence_secrets(),
        )
        .map_err(|error| {
            CaseFailure::from(format!(
                "private consistency report failed: {:?}",
                error.kind()
            ))
        })?;
        eprintln!(
            "Gemini deleted-store consistency: {outcome}; File Search-only exclusion={}; no code execution claim; guaranteed rejection and retrieval erasure remain unproven; evidence={}",
            exclusion.map_or_else(|| "unproven".into(), |met| met.to_string()),
            path.display()
        );
        Ok(())
    }

    async fn upload_source(
        &mut self,
        live: &LiveProvider,
        secret: &str,
    ) -> Result<(), CaseFailure> {
        let request = FileUploadRequest::new(
            format!("{}.txt", self.display_name()),
            "text/plain",
            format!("This is the ai-kit strict-test fixture document.\nThe strict-test fixture marker is {secret}.\n").into_bytes(),
        )?;
        self.begin_upload()?;
        let uploaded = tokio::time::timeout(
            Duration::from_secs(60),
            live.provider().upload_file(request.clone()),
        )
        .await
        .map_err(|_| {
            CaseFailure::from("owned store source upload exceeded 60 seconds; outcome uncertain")
        })?
        .map_err(|error| CaseFailure::from(error).context("Kit upload of owned store source"))?;
        self.observe_source_file(&request, &uploaded);
        self.accept_uploaded(uploaded.id.clone())?;
        BatchEvidence::fixture_file_ready(&uploaded, &uploaded, &request, HostedProvider::Gemini)?;
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let file = live
                    .provider()
                    .file_info(uploaded.id.clone())
                    .await
                    .map_err(|error| {
                        CaseFailure::from(error).context("Kit readiness of owned store source")
                    })?;
                self.observe_source_file(&request, &file);
                if BatchEvidence::fixture_file_ready(
                    &file,
                    &uploaded,
                    &request,
                    HostedProvider::Gemini,
                )? {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
        .await
        .map_err(|_| CaseFailure::from("owned store source readiness exceeded 90 seconds"))?
    }

    async fn import_source(&mut self, client: &Client, key: &str) -> Result<(), CaseFailure> {
        let target = self.target()?.to_owned();
        let file = self.begin_import()?;
        tokio::time::timeout(Duration::from_secs(180), async {
            let request = client
                .post(format!(
                    "https://generativelanguage.googleapis.com/v1beta/{target}:importFile"
                ))
                .header("x-goog-api-key", key)
                .json(&json!({"fileName": file}));
            let mut body = self
                .native_bytes(request, Some(&target))
                .await
                .map_err(|error| {
                    CaseFailure::from(error)
                        .context("native owned-store source import; mutation is not retried")
                })?;
            loop {
                if self.accept_import(&body).map_err(|error| {
                    CaseFailure::from(error).context("native owned-store import readiness")
                })? {
                    return Ok(());
                }
                let operation = self.import_operation()?.to_owned();
                tokio::time::sleep(Duration::from_secs(5)).await;
                let request = client
                    .get(format!(
                        "https://generativelanguage.googleapis.com/v1beta/{operation}"
                    ))
                    .header("x-goog-api-key", key);
                body = self
                    .native_bytes(request, Some(&operation))
                    .await
                    .map_err(|error| {
                        CaseFailure::from(error)
                            .context("native owned-store import operation lookup")
                    })?;
            }
        })
        .await
        .map_err(|_| {
            CaseFailure::from("owned-store import exceeded 180 seconds; import completion unproven")
        })?
    }

    async fn delete_source_once(&mut self, live: &LiveProvider) -> Result<(), CaseFailure> {
        let Some(id) = self.begin_input_delete() else {
            return Ok(());
        };
        tokio::time::timeout(
            Duration::from_secs(60),
            live.provider().delete_file(id.clone()),
        )
        .await
        .map_err(|_| {
            CaseFailure::from("owned store source deletion exceeded 60 seconds; outcome uncertain")
                .with_owner(id.clone())
        })?
        .map_err(|error| {
            CaseFailure::from(error.with_request_target(id.clone()))
                .context("Kit deletion of exact owned store source")
                .with_owner(id)
        })?;
        self.accept_input_deleted().map_err(CaseFailure::from)
    }

    async fn delete_once(&mut self, client: &Client, key: &str) -> Result<(), CaseFailure> {
        let Some(target) = self.begin_delete()? else {
            return Ok(());
        };
        let request = client
            .delete(format!(
                "https://generativelanguage.googleapis.com/v1beta/{target}"
            ))
            .header("x-goog-api-key", key)
            .query(&[("force", "true")]);
        let body = self
            .native_bytes(request, Some(&target))
            .await
            .map_err(|error| {
                CaseFailure::from(error)
                    .context("native owned-store deletion with document cascade")
            })?;
        self.accept_deleted(&body).map_err(|error| {
            CaseFailure::from(error.with_request_target(target))
                .context("native store deletion acknowledgement")
        })
    }

    async fn native_bytes(
        &self,
        request: RequestBuilder,
        target: Option<&str>,
    ) -> Result<Vec<u8>, KitError> {
        const LIMIT: usize = 65_536;
        let mut response = request.send().await.map_err(|error| {
            self.native_transport_error(error, ErrorPhase::RequestSend, target, None, &[])
        })?;
        let status = response.status();
        let request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            self.native_transport_error(
                error,
                ErrorPhase::ReadBody,
                target,
                (!status.is_success()).then_some(status.as_u16()),
                &bytes,
            )
        })? {
            let observed = bytes.len().saturating_add(chunk.len());
            if observed > LIMIT {
                bytes.extend_from_slice(&chunk[..LIMIT - bytes.len()]);
                let mut error = KitError::new(
                    ErrorKind::InvalidResponse(ResponseViolation::LimitExceeded { limit: LIMIT }),
                    Operation::Unspecified,
                    ErrorPhase::ReadBody,
                )
                .with_correlation_id(self.display_name())
                .with_response_details(ResponseDetails::Size { observed })
                .with_native_details(NativeErrorDetails {
                    request_id,
                    body: Some(String::from_utf8_lossy(&bytes).into_owned()),
                    truncated: true,
                    ..Default::default()
                });
                if !status.is_success() {
                    error = error.with_http_status(status.as_u16());
                }
                if let Some(target) = target {
                    error = error.with_request_target(target);
                }
                return Err(error);
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            let mut error = KitError::new(
                ErrorKind::ProviderRejected,
                Operation::Unspecified,
                ErrorPhase::ResponseHeaders,
            )
            .with_http_status(status.as_u16())
            .with_correlation_id(self.display_name())
            .with_native_details(NativeErrorDetails {
                request_id,
                body: Some(String::from_utf8_lossy(&bytes).into_owned()),
                ..Default::default()
            });
            if let Some(target) = target {
                error = error.with_request_target(target);
            }
            return Err(error);
        }
        Ok(bytes)
    }

    fn native_transport_error(
        &self,
        error: reqwest::Error,
        phase: ErrorPhase,
        target: Option<&str>,
        status: Option<u16>,
        body: &[u8],
    ) -> KitError {
        let timed_out = error.is_timeout();
        let mut error = KitError::new(
            if timed_out {
                ErrorKind::Timeout
            } else {
                ErrorKind::Transport
            },
            Operation::Unspecified,
            phase,
        )
        .with_correlation_id(self.display_name())
        .with_native_details(NativeErrorDetails {
            message: Some(error.to_string()),
            body: (!body.is_empty()).then(|| String::from_utf8_lossy(body).into_owned()),
            ..Default::default()
        });
        if timed_out {
            error = error.with_timeout(Duration::from_secs(60));
        }
        if let Some(status) = status {
            error = error.with_http_status(status);
        }
        if let Some(target) = target {
            error = error.with_request_target(target);
        }
        error
    }
}
