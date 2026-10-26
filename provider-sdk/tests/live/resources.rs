use std::num::NonZeroUsize;
use std::time::Duration;

use provider_sdk::ProviderBackend;
use provider_sdk::embedding::EmbedRequest;
use provider_sdk::error::ErrorKind;
use provider_sdk::files::{FileListRequest, FileUploadRequest};
use provider_sdk::generation::{
    ContentPart, FunctionResult, GenerateRequest, JsonSchema, Message, ProviderFunctionTool,
    ToolChoice,
};
use provider_sdk::models::{ModelBackend, ModelListRequest};
use provider_sdk::session::{
    SessionBackend, SessionConfig, SessionEvent, SessionInput, SessionTurnDetection,
};
use serde_json::json;

use super::{LiveProvider, Service};
use crate::hosted_assertions::HostedProvider;
use crate::live_assertions::{BatchEvidence, GenerationEvidence, SessionTurnEvidence};
use crate::live_failures::CaseFailure;

impl LiveProvider {
    async fn discovery(&self) -> Result<(), CaseFailure> {
        let page = self
            .provider()
            .list_models(
                ModelListRequest::default()
                    .with_limit(NonZeroUsize::new(10).expect("nonzero page size")),
            )
            .await
            .map_err(Self::error)?;
        if page.models.is_empty() {
            return Err("model discovery returned no models".to_owned().into());
        }
        Ok(())
    }

    async fn health(&self) -> Result<(), CaseFailure> {
        let health = self.provider().health().await.map_err(Self::error)?;
        if health.latency > Duration::from_secs(180) {
            return Err("health latency exceeded configured request deadline"
                .to_owned()
                .into());
        }
        Ok(())
    }

    async fn embeddings(&self) -> Result<(), CaseFailure> {
        let variable = match self.service() {
            Service::OpenAi => "TEST_OPENAI_MODEL_EMBEDDINGS",
            Service::Gemini => "TEST_GEMINI_MODEL_EMBEDDINGS",
            Service::Ollama => "TEST_OLLAMA_MODEL_EMBEDDINGS",
            Service::Anthropic => return Err("no Anthropic embedding fixture".to_owned().into()),
        };
        let model = self.selected(&Self::required(variable)?)?;
        let request = EmbedRequest::new(vec![
            "provider-sdk strict live embedding".to_owned(),
            "portable exact model selection".to_owned(),
        ])
        .map_err(Self::error)?;
        let response = model.embed(&request).await.map_err(Self::error)?;
        if response.vectors.len() != 2
            || response.vectors[0].is_empty()
            || response.vectors[0].len() != response.vectors[1].len()
            || response
                .vectors
                .iter()
                .flatten()
                .any(|value| !value.is_finite())
        {
            return Err("embedding result omitted ordered, finite, equally sized vectors".into());
        }
        Ok(())
    }

    async fn files_list(&self) -> Result<(), CaseFailure> {
        let request = FileListRequest::default()
            .with_limit(NonZeroUsize::new(10).expect("nonzero page size"));
        if self.service() == Service::Ollama {
            return match self.provider().list_files(request).await {
                Err(ref error) if error.kind() == ErrorKind::Unsupported => Ok(()),
                Err(error) => {
                    Err(Self::error(error).context("Ollama file listing expected Unsupported"))
                }
                Ok(_) => Err("Ollama file listing did not return Unsupported".into()),
            };
        }
        let page = self
            .provider()
            .list_files(request)
            .await
            .map_err(Self::error)?;
        if page.files.len() > 10 {
            return Err("file listing exceeded requested limit".to_owned().into());
        }
        Ok(())
    }

    async fn upload_lifecycle(&self) -> Result<(), CaseFailure> {
        let nonce = Self::nonce();
        // The expected answer belongs only in file bytes, never in metadata.
        let filename = format!("{}.txt", Self::nonce());
        let upload = FileUploadRequest::new(
            filename,
            "text/plain",
            format!("Record: delivery confirmation\nConfirmation code: {nonce}\n").into_bytes(),
        )
        .map_err(Self::error)?;
        let provider = match self.service() {
            Service::OpenAi => HostedProvider::OpenAi,
            Service::Anthropic => HostedProvider::Anthropic,
            Service::Gemini => HostedProvider::Gemini,
            Service::Ollama => return Err("Ollama has no upload lifecycle fixture".into()),
        };
        let uploaded = self
            .provider()
            .upload_file(upload.clone())
            .await
            .map_err(Self::error)?;
        // Keep the original ownership target even if a later response is invalid.
        let id = uploaded.id.clone();
        let mut observation = json!({
            "stage": "ordinary-upload-lifecycle",
            "selected_model": self.default_model(),
            "upload_request": {
                "file_name": upload.file_name(),
                "media_type": upload.media_type(),
                "size_bytes": upload.data().len(),
                "content": String::from_utf8_lossy(upload.data()),
            },
            "uploaded": uploaded,
            "owned_id": id,
        });
        let mut lookups = Vec::new();
        let journey = async {
            BatchEvidence::fixture_file_ready(&uploaded, &uploaded, &upload, provider)?;
            let ready = tokio::time::timeout(Duration::from_secs(90), async {
                loop {
                    let file = self.provider().file_info(id.clone()).await.map_err(Self::error)?;
                    lookups.push(json!({"requested_id": id, "file": file}));
                    if BatchEvidence::fixture_file_ready(&file, &uploaded, &upload, provider)? {
                        return Ok::<_, CaseFailure>(file);
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }).await.map_err(|_| "uploaded file did not become ready within 90 seconds".to_owned())??;
            let mut cursor = None;
            let mut found = false;
            for _ in 0..20 {
                let mut request = FileListRequest::default().with_limit(NonZeroUsize::new(100).expect("nonzero page size"));
                if let Some(value) = cursor.take() { request = request.with_cursor(value).map_err(Self::error)?; }
                let page = self.provider().list_files(request).await.map_err(Self::error)?;
                found = page.files.iter().any(|file| file.id == id);
                if found || page.next_cursor.is_none() { break; }
                cursor = page.next_cursor;
            }
            observation["listed"] = json!(found);
            if !found { return Err("file listing omitted newly uploaded file within pagination bound".to_owned().into()); }
            let reference = ready.input_reference.ok_or_else(|| "uploaded file omitted generation reference".to_owned())?;
            let prompt = "What is the confirmation code in the attached delivery confirmation record? Reply with exactly the confirmation code, and nothing else.";
            observation["generation_input"] = json!({"prompt": prompt, "reference": reference});
            let request = GenerateRequest::new(vec![Message::user(vec![
                ContentPart::Text { text: prompt.to_owned() },
                ContentPart::File { reference },
            ]).map_err(Self::error)?]).map_err(Self::error)?
                .with_generation(Self::generation_config()).map_err(Self::error)?;
            let response = self.model()?.generate(&request).await.map_err(Self::error)?;
            observation["generation_response"] = CaseFailure::response_evidence(&response);
            GenerationEvidence::exact_text(&response, &nonce, false)?;
            if self.service() == Service::OpenAi {
                // OpenAI purpose=user_data inputs do not establish file-content download
                // support. Exact bytes and bounded downloads use owned batch input files.
                observation["download"] = json!("user_data download not asserted; separate batch input proves download");
                return Ok(());
            }
            match self.provider().download_file(id.clone(), NonZeroUsize::new(4096).expect("nonzero bound")).await {
                Err(error) if error.kind() == ErrorKind::Unsupported => {
                    observation["download"] = Self::error(error).evidence();
                    Ok(())
                },
                Ok(_) => Err("input-file download succeeded despite unsupported provider semantics".into()),
                Err(error) => Err(Self::error(error).context("input-file download expected Unsupported")),
            }
        }.await;
        let cleanup = self
            .provider()
            .delete_file(id.clone())
            .await
            .map_err(|error| {
                Self::error(error.with_request_target(id.clone()))
                    .context("owned input file deletion failed")
            });
        observation["readiness_lookups"] = json!(lookups);
        observation["journey_passed"] = json!(journey.is_ok());
        observation["journey_failure"] = json!(journey.as_ref().err().map(CaseFailure::evidence));
        observation["deleted"] = json!(cleanup.is_ok());
        observation["cleanup_failure"] = json!(cleanup.as_ref().err().map(CaseFailure::evidence));
        let export = Self::write_required_observation("upload-lifecycle", observation)
            .map(|path| eprintln!("Ordinary upload lifecycle evidence={}", path.display()));
        CaseFailure::combine(CaseFailure::combine(journey, cleanup), export)
    }

    async fn send_session_input(
        session: &mut provider_sdk::Session,
        evidence: &mut SessionTurnEvidence,
        input: SessionInput,
    ) -> Result<(), CaseFailure> {
        evidence.input(&input)?;
        let result = session.send(input).await.map_err(Self::error);
        let recorded = evidence.sent(&result).map_err(CaseFailure::from);
        CaseFailure::combine(result, recorded)
    }

    async fn interactive(&mut self, interrupt: bool) -> Result<(), CaseFailure> {
        let mut observation = json!({"stage":"session_setup", "boundary":"public_session", "native_frames_captured":false});
        let mut turns = Vec::new();
        let mut owned_session = None;
        let journey = async {
            let input_audio = "audio/pcm;rate=24000";
            let audio = include_bytes!("../fixtures/input-speech-24000.pcm");
            let id = Self::required("TEST_OPENAI_MODEL_INTERACTIVE")?;
            observation["selected_model"] = json!(id);
            let function = ProviderFunctionTool::new(
                "strict_live_session_probe",
                Some("Returns one session fixture value.".to_owned()),
                JsonSchema::new(
                    serde_json::json!({"type":"object", "properties":{}, "additionalProperties":false}),
                )
                .map_err(Self::error)?,
            )
            .map_err(Self::error)?;
            let instructions = "Follow each turn independently and speak the exact requested answer, adding nothing. Use tools only when explicitly requested. When the speech input asks you to say audio input received, the full output transcription must be exactly: Audio input received.";
            let config = SessionConfig::default()
                .with_instructions(instructions)
                .map_err(Self::error)?
                .with_input_audio(input_audio, true)
                .map_err(Self::error)?
                .with_output_audio("audio/pcm;rate=24000", true)
                .map_err(Self::error)?
                .with_turn_detection(SessionTurnDetection::Manual)
                .with_functions(vec![function], Some(ToolChoice::Auto))
                .map_err(Self::error)?;
            observation["configuration"] = json!({
                "instructions":config.instructions(), "input_audio_format":config.input_audio_format(),
                "input_transcription":config.input_transcription(), "output_audio_format":config.output_audio_format(),
                "output_transcription":config.output_transcription(), "functions":config.functions(),
                "tool_choice":config.tool_choice(), "turn_detection":format!("{:?}", config.turn_detection()),
            });
            owned_session = Some(self.selected(&id)?.open_session(config).await.map_err(Self::error)?);
            let session = owned_session.as_mut().expect("opened session remains owned");
            observation["session_id"] = json!(session.id());
            if session.id().is_empty() || session.model_id().as_str() != id {
                return Err("session identity differed from exact selection".to_owned().into());
            }
            if interrupt {
                observation["stage"] = json!("interruption");
                let mut evidence = SessionTurnEvidence::interrupting_audio();
                let interrupted = async {
                    Self::send_session_input(session, &mut evidence, SessionInput::Text {
                        text: "Count aloud from one to one hundred, one number at a time. Add nothing else and do not call tools.".to_owned(),
                    }).await?;
                    let mut interruption_sent = false;
                    loop {
                        let event = session.recv().await.map_err(Self::error)?;
                        evidence.observe(&event)?;
                        if let SessionEvent::Audio { ref data, .. } = event
                            && !data.is_empty() && !interruption_sent
                        {
                            Self::send_session_input(session, &mut evidence, SessionInput::CancelTurn).await?;
                            interruption_sent = true;
                        }
                        if evidence.ready() { break; }
                    }
                    if !interruption_sent {
                        return Err("session cancelled without the requested interruption input".into());
                    }
                    evidence.verify()?;
                    Ok::<(), CaseFailure>(())
                };
                let result = tokio::time::timeout(Duration::from_secs(90), interrupted).await
                    .map_err(|_| CaseFailure::from("session interruption exceeded 90 seconds before required evidence"))
                    .and_then(|result| result);
                evidence.finish(&result);
                turns.push(evidence);
                result?;
            }
            let audio_turns: &[bool] = if interrupt { &[false] } else { &[false, true] };
            for &audio_turn in audio_turns {
                observation["stage"] = json!(if audio_turn { "audio_turn" } else { "function_turn" });
                // The turn owner outlives the observation timeout and retains partial evidence.
                let mut evidence = if audio_turn {
                    SessionTurnEvidence::new("Audio input received.", true)
                } else {
                    SessionTurnEvidence::awaiting_function()
                };
                let turn = async {
                    if audio_turn {
                        Self::send_session_input(session, &mut evidence, SessionInput::TurnStart).await?;
                        Self::send_session_input(session, &mut evidence, SessionInput::Audio { media_type: input_audio.to_owned(), data: audio.to_vec() }).await?;
                        Self::send_session_input(session, &mut evidence, SessionInput::TurnEnd).await?;
                    } else {
                        let mut text = "Call strict_live_session_probe exactly once with an empty object. Then speak exactly its entire returned value, preserving words, capitalization, and the final period; add nothing.".to_owned();
                        text.push_str(" Do not speak before calling the tool or while waiting for its result. After the result arrives, speak only its value field; do not narrate the tool call.");
                        Self::send_session_input(session, &mut evidence, SessionInput::Text { text }).await?;
                    }
                    loop {
                        let event = session.recv().await.map_err(Self::error)?;
                        evidence.observe(&event)?;
                        if let SessionEvent::FunctionCall(call) = event {
                            // Generate only after the exact expected call has been accepted.
                            let words = ["amber", "birch", "cedar", "dawn", "elm", "fern", "garden", "harbor", "iris", "jade", "kite", "lemon", "maple", "north", "oak", "pine"];
                            let nonce = uuid::Uuid::new_v4();
                            let phrase = format!("The phrase is {}.", nonce.as_bytes()[..8].iter().flat_map(|byte| [words[usize::from(byte >> 4)], words[usize::from(byte & 15)]]).collect::<Vec<_>>().join(" "));
                            evidence.expect_after_function(phrase.clone())?;
                            let result = FunctionResult::new(call.id().map(str::to_owned), call.name().to_owned(),
                                vec![ContentPart::Json { value: serde_json::json!({"value":phrase}) }], false).map_err(Self::error)?;
                            Self::send_session_input(session, &mut evidence, SessionInput::FunctionResult(result)).await?;
                        }
                        if evidence.ready() { break; }
                    }
                    evidence.verify()?;
                    Ok::<(), CaseFailure>(())
                };
                let result = tokio::time::timeout(Duration::from_secs(90), turn).await
                    .map_err(|_| CaseFailure::from("session turn exceeded 90 seconds before required evidence"))
                    .and_then(|result| result);
                evidence.finish(&result);
                turns.push(evidence);
                result?;
            }
            observation["stage"] = json!("complete");
            Ok(())
        }.await;
        let close_attempted = owned_session.is_some();
        let cleanup = match owned_session {
            Some(session) => tokio::time::timeout(Duration::from_secs(90), session.close())
                .await
                .map_err(|_| CaseFailure::from("session close exceeded 90 seconds"))
                .and_then(|result| result.map_err(Self::error)),
            None => Ok(()),
        };
        observation["journey"] = json!({"succeeded":journey.is_ok(), "failure":journey.as_ref().err().map(CaseFailure::evidence)});
        observation["session_close"] = json!({"attempted":close_attempted, "succeeded":close_attempted && cleanup.is_ok(), "failure":cleanup.as_ref().err().map(CaseFailure::evidence)});
        // Existing LiveProvider owns this until shared Provider shutdown settles.
        self.session_observation = Some((observation, turns));
        CaseFailure::combine(journey, cleanup)
    }
}

#[tokio::test]
async fn openai_discovery() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.discovery().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_discovery() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.discovery().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_discovery() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.discovery().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_discovery() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.discovery().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_health() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.health().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_health() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.health().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_health() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.health().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_health() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.health().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_embeddings() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.embeddings().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_embeddings() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.embeddings().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_embeddings() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.embeddings().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_files_list() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.files_list().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_files_list() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.files_list().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_files_list() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.files_list().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn ollama_files_unsupported() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Ollama)?;
    let result = live.files_list().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_upload_lifecycle() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::OpenAi)?;
    let result = live.upload_lifecycle().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn anthropic_upload_lifecycle() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Anthropic)?;
    let result = live.upload_lifecycle().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn gemini_upload_lifecycle() -> Result<(), CaseFailure> {
    let live = LiveProvider::new(Service::Gemini)?;
    let result = live.upload_lifecycle().await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_interactive_session() -> Result<(), CaseFailure> {
    let mut live = LiveProvider::new(Service::OpenAi)?;
    let result = live.interactive(false).await;
    live.finish(result).await;
    Ok(())
}
#[tokio::test]
async fn openai_interactive_cancellation_and_subsequent_progress() -> Result<(), CaseFailure> {
    let mut live = LiveProvider::new(Service::OpenAi)?;
    let result = live.interactive(true).await;
    live.finish(result).await;
    Ok(())
}
