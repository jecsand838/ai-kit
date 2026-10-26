use provider_sdk::generation::FunctionCall;
use provider_sdk::session::{SessionEvent, SessionInput};

use crate::live_failures::CaseFailure;

use crate::live_assertions::SessionTurnEvidence;

struct SessionFixture;

impl SessionFixture {
    const PHRASE: &str = "Amber willow cedar river.";

    fn call() -> SessionEvent {
        SessionEvent::FunctionCall(
            FunctionCall::new(
                Some("fixture-call".into()),
                "strict_live_session_probe",
                serde_json::json!({}),
            )
            .unwrap(),
        )
    }

    fn audio() -> SessionEvent {
        SessionEvent::Audio {
            media_type: "audio/pcm;rate=24000".into(),
            data: vec![1, 0, 2, 0],
        }
    }

    fn result() -> SessionInput {
        SessionInput::FunctionResult(
            provider_sdk::generation::FunctionResult::new(
                Some("fixture-call".into()),
                "strict_live_session_probe",
                vec![provider_sdk::generation::ContentPart::Json {
                    value: serde_json::json!({"value":Self::PHRASE}),
                }],
                false,
            )
            .unwrap(),
        )
    }

    fn text_events() -> Vec<SessionEvent> {
        vec![
            Self::call(),
            Self::audio(),
            SessionEvent::OutputTranscriptionDelta {
                delta: Self::PHRASE.into(),
            },
            SessionEvent::Completed,
        ]
    }

    fn evidence(events: &[SessionEvent], audio_input: bool) -> Result<SessionTurnEvidence, String> {
        let mut evidence = SessionTurnEvidence::new(Self::PHRASE, audio_input);
        for event in events {
            evidence.observe(event)?;
            if matches!(event, SessionEvent::FunctionCall(_)) {
                evidence.input(&Self::result())?;
                evidence.sent(&Ok(()))?;
            }
        }
        Ok(evidence)
    }

    fn check(events: &[SessionEvent], audio_input: bool) -> Result<(), String> {
        Self::evidence(events, audio_input)?.verify()
    }
}

#[test]
fn session_copy_quality_is_separate_from_sdk_progress() {
    let mut evidence = SessionTurnEvidence::awaiting_function();
    evidence
        .observe(&SessionEvent::OutputTranscriptionDelta {
            delta: "I will check that value first. ".into(),
        })
        .unwrap();
    evidence.observe(&SessionFixture::call()).unwrap();
    evidence
        .expect_after_function(SessionFixture::PHRASE.into())
        .unwrap();
    evidence.input(&SessionFixture::result()).unwrap();
    evidence.sent(&Ok(())).unwrap();
    for event in &SessionFixture::text_events()[1..] {
        evidence.observe(event).unwrap();
    }
    evidence.verify().unwrap();
    let observation = evidence.observation();
    assert_eq!(observation["model_exact_copy"], false);
    assert_eq!(
        observation["output_transcript"],
        "I will check that value first. Amber willow cedar river."
    );
}

#[test]
fn session_requires_progress_after_successful_function_result() {
    let mut evidence = SessionTurnEvidence::awaiting_function();
    evidence.observe(&SessionFixture::call()).unwrap();
    evidence
        .expect_after_function(SessionFixture::PHRASE.into())
        .unwrap();
    for event in &SessionFixture::text_events()[1..3] {
        evidence.observe(event).unwrap();
    }
    evidence.input(&SessionFixture::result()).unwrap();
    evidence.sent(&Ok(())).unwrap();
    evidence.observe(&SessionEvent::Completed).unwrap();
    assert!(
        evidence.verify().is_err(),
        "pre-result transcription and audio cannot prove continuation progress"
    );
}

#[test]
fn session_completed_invalid_evidence_has_no_copy_grade() {
    for omitted in [1, 2] {
        let mut events = SessionFixture::text_events();
        events.remove(omitted);
        let evidence = SessionFixture::evidence(&events, false).unwrap();
        assert!(evidence.verify().is_err());
        assert_eq!(evidence.observation()["completed"], true);
        assert_eq!(evidence.observation()["model_exact_copy"], "unavailable");
    }
    let mut missing_checkpoint = SessionTurnEvidence::new(SessionFixture::PHRASE, false);
    for event in SessionFixture::text_events() {
        missing_checkpoint.observe(&event).unwrap();
    }
    assert!(missing_checkpoint.verify().is_err());
    assert_eq!(
        missing_checkpoint.observation()["model_exact_copy"],
        "unavailable"
    );
    let missing_input =
        SessionFixture::evidence(&SessionFixture::text_events()[1..], true).unwrap();
    assert!(missing_input.verify().is_err());
    assert_eq!(
        missing_input.observation()["model_exact_copy"],
        "unavailable"
    );
    let mut failed_capture =
        SessionFixture::evidence(&SessionFixture::text_events(), false).unwrap();
    assert!(
        failed_capture
            .input(&SessionInput::Text {
                text: "x".repeat(384 * 1024),
            })
            .is_err()
    );
    assert!(failed_capture.verify().is_err());
    assert_eq!(
        failed_capture.observation()["model_exact_copy"],
        "unavailable"
    );
    let valid = SessionFixture::evidence(&SessionFixture::text_events(), false).unwrap();
    assert_eq!(valid.observation()["model_exact_copy"], true);
}

#[test]
fn session_result_checkpoint_requires_success_and_exact_call_identity() {
    for send_outcome in [None, Some(false), Some(true)] {
        let mut evidence = SessionTurnEvidence::awaiting_function();
        evidence.observe(&SessionFixture::call()).unwrap();
        evidence
            .expect_after_function(SessionFixture::PHRASE.into())
            .unwrap();
        for (id, name) in [
            (Some("wrong-call"), "strict_live_session_probe"),
            (None, "strict_live_session_probe"),
            (Some("fixture-call"), "wrong-function"),
        ] {
            let wrong = SessionInput::FunctionResult(
                provider_sdk::generation::FunctionResult::new(
                    id.map(str::to_owned),
                    name,
                    vec![provider_sdk::generation::ContentPart::Text {
                        text: SessionFixture::PHRASE.into(),
                    }],
                    false,
                )
                .unwrap(),
            );
            assert!(evidence.input(&wrong).is_err());
        }
        evidence.input(&SessionFixture::result()).unwrap();
        assert!(evidence.input(&SessionFixture::result()).is_err());
        if let Some(succeeded) = send_outcome {
            let sent = if succeeded {
                Ok(())
            } else {
                Err(CaseFailure::from("original result-send failure"))
            };
            evidence.sent(&sent).unwrap();
            assert!(evidence.input(&SessionFixture::result()).is_err());
        }
        for event in &SessionFixture::text_events()[1..] {
            evidence.observe(event).unwrap();
        }
        assert_eq!(evidence.verify().is_ok(), send_outcome == Some(true));
        assert_eq!(
            evidence.observation()["function_result_checkpoint"].is_object(),
            send_outcome == Some(true)
        );
        assert!(evidence.input(&SessionFixture::result()).is_err());
    }
}

#[test]
fn session_post_result_progress_requires_both_audio_and_nonblank_transcription() {
    for (audio_after, transcript_after) in [
        (false, "continued"),
        (true, ""),
        (true, " \t\n"),
        (true, "continued"),
    ] {
        let mut evidence = SessionTurnEvidence::awaiting_function();
        evidence.observe(&SessionFixture::call()).unwrap();
        evidence
            .expect_after_function(SessionFixture::PHRASE.into())
            .unwrap();
        evidence.observe(&SessionFixture::audio()).unwrap();
        evidence
            .observe(&SessionEvent::OutputTranscriptionDelta {
                delta: SessionFixture::PHRASE.into(),
            })
            .unwrap();
        evidence.input(&SessionFixture::result()).unwrap();
        evidence.sent(&Ok(())).unwrap();
        if audio_after {
            evidence.observe(&SessionFixture::audio()).unwrap();
        }
        evidence
            .observe(&SessionEvent::OutputTranscriptionDelta {
                delta: transcript_after.into(),
            })
            .unwrap();
        evidence.observe(&SessionEvent::Completed).unwrap();
        assert_eq!(
            evidence.verify().is_ok(),
            audio_after && transcript_after == "continued"
        );
        assert_eq!(
            evidence.observation()["output_transcript"],
            format!("{}{transcript_after}", SessionFixture::PHRASE)
        );
    }
}

#[test]
fn session_receipts_require_three_boolean_grades_without_raw_output() {
    let mut changed = SessionFixture::text_events();
    changed.insert(
        0,
        SessionEvent::OutputTranscriptionDelta {
            delta: "Preamble. ".into(),
        },
    );
    let audio_events = [
        SessionFixture::audio(),
        SessionEvent::OutputTranscriptionDelta {
            delta: "A different answer.".into(),
        },
        SessionEvent::Completed,
        SessionEvent::InputTranscriptionDelta {
            delta: "Actual caller speech".into(),
        },
    ];
    let ordinary = [
        SessionFixture::evidence(&SessionFixture::text_events(), false).unwrap(),
        SessionFixture::evidence(&audio_events, true).unwrap(),
    ];
    let receipt = SessionTurnEvidence::realtime_grade_receipt(
        "live::resources::openai_interactive_session",
        "gpt-realtime-2.1",
        "gpt-realtime-2.1",
        &ordinary,
    )
    .unwrap();
    assert_eq!(
        receipt["model_exact_copy"],
        serde_json::json!({"function_turn":true,"audio_turn":false})
    );
    assert_eq!(receipt["sdk_contract"], true);
    assert_eq!(receipt["version"], 1);
    assert!(!receipt.to_string().contains(SessionFixture::PHRASE));
    let mut cancelled = SessionTurnEvidence::interrupting_audio();
    cancelled.observe(&SessionFixture::audio()).unwrap();
    cancelled.observe(&SessionEvent::TurnCancelled).unwrap();
    let cancellation = [
        cancelled,
        SessionFixture::evidence(&changed, false).unwrap(),
    ];
    let receipt = SessionTurnEvidence::realtime_grade_receipt(
        "live::resources::openai_interactive_cancellation_and_subsequent_progress",
        "gpt-realtime-2.1",
        "gpt-realtime-2.1",
        &cancellation,
    )
    .unwrap();
    assert_eq!(
        receipt["model_exact_copy"],
        serde_json::json!({"post_cancellation_function_turn":false})
    );
    assert_eq!(
        cancellation[0].observation()["model_exact_copy"],
        "not_applicable"
    );
    for (case, model, turns) in [
        ("unknown", "gpt-realtime-2.1", ordinary.as_slice()),
        (
            "live::resources::openai_interactive_session",
            "wrong",
            ordinary.as_slice(),
        ),
        (
            "live::resources::openai_interactive_session",
            "gpt-realtime-2.1",
            &ordinary[..1],
        ),
        (
            "live::resources::openai_interactive_session",
            "gpt-realtime-2.1",
            cancellation.as_slice(),
        ),
    ] {
        assert!(
            SessionTurnEvidence::realtime_grade_receipt(case, model, "gpt-realtime-2.1", turns)
                .is_err()
        );
    }
    let partial = [
        SessionTurnEvidence::awaiting_function(),
        SessionTurnEvidence::new(SessionFixture::PHRASE, true),
    ];
    assert_eq!(partial[0].observation()["model_exact_copy"], "unavailable");
    assert!(
        SessionTurnEvidence::realtime_grade_receipt(
            "live::resources::openai_interactive_session",
            "gpt-realtime-2.1",
            "gpt-realtime-2.1",
            &partial,
        )
        .is_err()
    );
}

#[test]
fn reaudit_session_requires_every_text_turn_obligation() {
    let events = SessionFixture::text_events();
    SessionFixture::check(&events, false).unwrap();
    for omitted in 0..events.len() {
        let incomplete = events
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != omitted)
            .map(|(_, event)| event.clone())
            .collect::<Vec<_>>();
        assert!(
            SessionFixture::check(&incomplete, false).is_err(),
            "missing obligation {omitted}"
        );
    }
}

#[test]
fn reaudit_session_raw_copy_grade_preserves_full_phrase_comparison() {
    for incorrect in [
        "",
        "amber willow cedar river.",
        "Amber willow cedar river",
        " Amber willow cedar river.",
        "Amber willow cedar river. ",
        "Here is Amber willow cedar river.",
        "Amber willow cedar river. Done.",
        "Amber willow",
    ] {
        let mut events = SessionFixture::text_events();
        events[2] = SessionEvent::OutputTranscriptionDelta {
            delta: incorrect.into(),
        };
        let evidence = SessionFixture::evidence(&events, false).unwrap();
        assert_eq!(evidence.verify().is_ok(), !incorrect.is_empty());
        assert_eq!(
            evidence.observation()["model_exact_copy"],
            if incorrect.is_empty() {
                serde_json::json!("unavailable")
            } else {
                serde_json::json!(false)
            }
        );
        assert_eq!(evidence.observation()["output_transcript"], incorrect);
    }
    for substitute in [
        SessionEvent::TextDelta {
            delta: SessionFixture::PHRASE.into(),
        },
        SessionEvent::InputTranscriptionDelta {
            delta: SessionFixture::PHRASE.into(),
        },
    ] {
        let mut events = SessionFixture::text_events();
        events[2] = substitute;
        assert!(SessionFixture::check(&events, false).is_err());
    }
    let events = [
        SessionFixture::call(),
        SessionFixture::audio(),
        SessionEvent::OutputTranscriptionDelta {
            delta: "Amber wil".into(),
        },
        SessionEvent::OutputTranscriptionDelta {
            delta: "low cedar river.".into(),
        },
        SessionEvent::Completed,
    ];
    SessionFixture::check(&events, false).unwrap();
    let mut late = SessionFixture::text_events();
    late.push(SessionEvent::OutputTranscriptionDelta {
        delta: "late".into(),
    });
    assert!(SessionFixture::check(&late, false).is_err());
}

#[test]
fn session_answer_assignment_preserves_pre_call_transcription() {
    for preamble in [None, Some("I will check that value first. ")] {
        let mut evidence = SessionTurnEvidence::awaiting_function();
        if let Some(preamble) = preamble {
            evidence
                .observe(&SessionEvent::OutputTranscriptionDelta {
                    delta: preamble.into(),
                })
                .unwrap();
        }
        evidence.observe(&SessionFixture::call()).unwrap();
        evidence
            .expect_after_function(SessionFixture::PHRASE.into())
            .unwrap();
        evidence.input(&SessionFixture::result()).unwrap();
        evidence.sent(&Ok(())).unwrap();
        evidence.observe(&SessionFixture::audio()).unwrap();
        for delta in ["Amber willow ", "cedar river."] {
            evidence
                .observe(&SessionEvent::OutputTranscriptionDelta {
                    delta: delta.into(),
                })
                .unwrap();
        }
        evidence.observe(&SessionEvent::Completed).unwrap();
        assert!(evidence.ready());
        evidence.verify().unwrap();
        assert_eq!(
            evidence.observation()["model_exact_copy"],
            preamble.is_none()
        );
    }
}

#[test]
fn reaudit_session_rejects_duplicate_wrong_and_cancelled_functions() {
    for bad in [
        SessionFixture::call(),
        SessionEvent::FunctionCall(
            FunctionCall::new(Some("wrong".into()), "wrong_tool", serde_json::json!({})).unwrap(),
        ),
        SessionEvent::FunctionCall(
            FunctionCall::new(
                Some("wrong".into()),
                "strict_live_session_probe",
                serde_json::json!({"unexpected":true}),
            )
            .unwrap(),
        ),
        SessionEvent::TurnCancelled,
        SessionEvent::FunctionCallsCancelled {
            call_ids: vec!["fixture-call".into()],
        },
    ] {
        let mut events = SessionFixture::text_events();
        events.insert(1, bad);
        assert!(SessionFixture::check(&events, false).is_err());
    }
}

#[test]
fn reaudit_session_audio_turn_requires_its_own_output_and_allows_late_input_transcription() {
    SessionFixture::check(&SessionFixture::text_events(), false).unwrap();
    let events = vec![
        SessionFixture::audio(),
        SessionEvent::OutputTranscriptionDelta {
            delta: SessionFixture::PHRASE.into(),
        },
        SessionEvent::Completed,
        SessionEvent::InputTranscriptionDelta {
            delta: "Please say the phrase.".into(),
        },
    ];
    SessionFixture::check(&events, true).unwrap();
    let mut evidence = SessionTurnEvidence::new(SessionFixture::PHRASE, true);
    for event in &events[..3] {
        evidence.observe(event).unwrap();
    }
    assert!(
        !evidence.ready(),
        "input transcription may legitimately arrive after completion"
    );
    evidence.observe(&events[3]).unwrap();
    assert!(evidence.ready());
    evidence.verify().unwrap();
    for omitted in 0..events.len() {
        let incomplete = events
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != omitted)
            .map(|(_, event)| event.clone())
            .collect::<Vec<_>>();
        assert!(
            SessionFixture::check(&incomplete, true).is_err(),
            "previous text turn cannot supply missing audio-turn obligation {omitted}"
        );
    }
    let mut unsolicited = events.clone();
    unsolicited.insert(0, SessionFixture::call());
    assert!(SessionFixture::check(&unsolicited, true).is_err());
}

#[test]
fn session_evidence_assigns_expected_answer_only_after_the_validated_call() {
    let mut evidence = SessionTurnEvidence::awaiting_function();
    assert!(
        evidence
            .expect_after_function(SessionFixture::PHRASE.into())
            .is_err()
    );
    evidence.observe(&SessionFixture::call()).unwrap();
    evidence
        .expect_after_function(SessionFixture::PHRASE.into())
        .unwrap();
    assert!(
        evidence
            .expect_after_function("another answer".into())
            .is_err()
    );
    evidence.input(&SessionFixture::result()).unwrap();
    evidence.sent(&Ok(())).unwrap();
    for event in &SessionFixture::text_events()[1..] {
        evidence.observe(event).unwrap();
    }
    evidence.verify().unwrap();
    assert_eq!(evidence.observation()["expected"], SessionFixture::PHRASE);
}

#[tokio::test]
async fn session_evidence_retains_partial_turn_after_observation_timeout() {
    let mut evidence = SessionTurnEvidence::awaiting_function();
    let result = tokio::time::timeout(std::time::Duration::from_millis(10), async {
        evidence.observe(&SessionFixture::call()).unwrap();
        evidence
            .expect_after_function(SessionFixture::PHRASE.into())
            .unwrap();
        let input = SessionInput::FunctionResult(
            provider_sdk::generation::FunctionResult::new(
                Some("fixture-call".into()),
                "strict_live_session_probe",
                vec![provider_sdk::generation::ContentPart::Json {
                    value: serde_json::json!({"value":SessionFixture::PHRASE}),
                }],
                false,
            )
            .unwrap(),
        );
        evidence.input(&input).unwrap();
        evidence.sent(&Ok(())).unwrap();
        evidence.observe(&SessionFixture::audio()).unwrap();
        evidence
            .observe(&SessionEvent::OutputTranscriptionDelta {
                delta: "Amber ".into(),
            })
            .unwrap();
        std::future::pending::<()>().await;
    })
    .await
    .map_err(|_| CaseFailure::from("observation timeout"));
    assert!(result.is_err());
    evidence.finish(&result);
    let observed = evidence.observation();
    assert_eq!(observed["function_calls"], 1);
    assert_eq!(observed["output_transcript"], "Amber ");
    assert_eq!(observed["completed"], false);
    assert_eq!(observed["model_exact_copy"], "unavailable");
    assert_eq!(
        observed["outcome"]["failure"]["assertion"],
        "observation timeout"
    );
    assert_eq!(
        observed["events"][0]["received"],
        serde_json::to_value(SessionFixture::call()).unwrap()
    );
    let result_value = observed["events"][1]["send_attempt"].clone();
    let input: SessionInput = serde_json::from_value(result_value).unwrap();
    let SessionInput::FunctionResult(result) = input else {
        panic!("complete function result retained");
    };
    assert_eq!(result.call_id(), Some("fixture-call"));
    assert_eq!(result.name(), "strict_live_session_probe");
    assert_eq!(
        result.content(),
        &[provider_sdk::generation::ContentPart::Json {
            value: serde_json::json!({"value":SessionFixture::PHRASE})
        }]
    );
    assert!(!result.is_error());
    assert!(evidence.verify().is_err());
}

#[test]
fn session_pcm_sidecars_preserve_complete_bytes_without_json_expansion() {
    let directory = tempfile::tempdir().unwrap();
    let mut evidence = SessionTurnEvidence::new("Audio input received.", true);
    let audio = vec![0x5a; 4_320_000]; // Ninety seconds of 24-kHz, 16-bit mono PCM.
    evidence
        .input(&SessionInput::Audio {
            media_type: "audio/pcm;rate=24000".into(),
            data: audio.clone(),
        })
        .unwrap();
    evidence
        .observe(&SessionEvent::Audio {
            media_type: "audio/pcm;rate=24000".into(),
            data: audio.clone(),
        })
        .unwrap();
    let (observed, exported) = evidence.export(directory.path(), "complete");
    exported.unwrap();
    assert_eq!(
        std::fs::read(directory.path().join("complete-input.pcm")).unwrap(),
        audio
    );
    assert_eq!(
        std::fs::read(directory.path().join("complete-output.pcm")).unwrap(),
        audio
    );
    assert_eq!(observed["pcm"].as_array().unwrap().len(), 2);
    assert_eq!(observed["events"][0]["send_attempt"]["byte_len"], 4_320_000);
    assert_eq!(observed["events"][1]["received"]["pcm_offset"], 0);
    assert!(serde_json::to_vec(&observed).unwrap().len() < 4096);
    let path = directory.path().join("complete.json");
    CaseFailure::write_required_observation(&path, observed, &[]).unwrap();
    assert!(!std::fs::read_to_string(path).unwrap().contains("Wlpa"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for direction in ["input", "output"] {
            assert_eq!(
                std::fs::metadata(directory.path().join(format!("complete-{direction}.pcm")))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
    // A representative ten-second response delivered in 20-ms chunks also fits
    // the independent chronology bound and retains every ordered sample.
    let mut chunked = SessionTurnEvidence::new("Audio input received.", true);
    let mut expected_audio = Vec::new();
    for index in 0..500 {
        let data = vec![u8::try_from(index % 251).unwrap(); 960];
        expected_audio.extend_from_slice(&data);
        chunked
            .observe(&SessionEvent::Audio {
                media_type: "audio/pcm;rate=24000".into(),
                data,
            })
            .unwrap();
    }
    chunked
        .observe(&SessionEvent::OutputTranscriptionDelta {
            delta: "Audio input received.".into(),
        })
        .unwrap();
    chunked.observe(&SessionEvent::Completed).unwrap();
    chunked
        .observe(&SessionEvent::InputTranscriptionDelta {
            delta: "Please say audio input received.".into(),
        })
        .unwrap();
    chunked.verify().unwrap();
    chunked.finish(&Ok(()));
    let (observed, exported) = chunked.export(directory.path(), "chunked");
    exported.unwrap();
    assert_eq!(
        std::fs::read(directory.path().join("chunked-output.pcm")).unwrap(),
        expected_audio
    );
    assert_eq!(observed["events"].as_array().unwrap().len(), 503);
    assert_eq!(observed["events"][499]["received"]["pcm_offset"], 499 * 960);
    CaseFailure::write_required_observation(&directory.path().join("chunked.json"), observed, &[])
        .unwrap();
}

#[test]
fn session_pcm_export_reports_each_failure_and_preserves_primary_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let mut evidence = SessionTurnEvidence::new("expected", true);
    evidence
        .input(&SessionInput::Audio {
            media_type: "audio/pcm;rate=16000".into(),
            data: vec![1, 2],
        })
        .unwrap();
    evidence.observe(&SessionFixture::audio()).unwrap();
    let primary = Err(CaseFailure::from("original session failure"));
    evidence.finish(&primary);
    // create_new must preserve existing ownership and still attempt the other direction.
    std::fs::write(directory.path().join("occupied-input.pcm"), b"original").unwrap();
    let (observed, exported) = evidence.export(directory.path(), "occupied");
    assert!(exported.is_err());
    assert_eq!(observed["pcm"][0]["exported"], false);
    assert_eq!(observed["pcm"][1]["exported"], true);
    assert_eq!(
        std::fs::read(directory.path().join("occupied-input.pcm")).unwrap(),
        b"original"
    );
    assert_eq!(
        std::fs::read(directory.path().join("occupied-output.pcm")).unwrap(),
        vec![1, 0, 2, 0]
    );
    let combined = CaseFailure::combine(primary, exported)
        .unwrap_err()
        .evidence();
    assert_eq!(
        combined["failures"][0]["assertion"],
        "original session failure"
    );
    let (observed, exported) = evidence.export(&directory.path().join("missing"), "both");
    assert!(exported.is_err());
    assert!(
        observed["pcm"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["exported"] == false)
    );
}

#[test]
fn session_evidence_limits_fail_visibly_without_silent_truncation() {
    let mut evidence = SessionTurnEvidence::awaiting_function();
    evidence.observe(&SessionFixture::call()).unwrap();
    let error = evidence
        .observe(&SessionEvent::Audio {
            media_type: "audio/pcm;rate=24000".into(),
            data: vec![0; 8 * 1024 * 1024 + 1],
        })
        .unwrap_err();
    assert!(error.contains("8 MiB"));
    assert_eq!(evidence.observation()["capture_failure"], error);
    assert!(evidence.verify().is_err());
    assert!(evidence.observe(&SessionEvent::Completed).is_err());
    let mut text = SessionTurnEvidence::new("expected", true);
    assert!(
        text.observe(&SessionEvent::OutputTranscriptionDelta {
            delta: "x".repeat(32 * 1024 + 1)
        })
        .is_err()
    );
    assert!(!text.observation()["capture_failure"].is_null());
    let mut events = SessionTurnEvidence::new("expected", true);
    let mut limited = false;
    for _ in 0..=8192 {
        if events
            .observe(&SessionEvent::TextDelta {
                delta: "small".into(),
            })
            .is_err()
        {
            limited = true;
            break;
        }
    }
    assert!(limited);
    assert!(events.observation()["event_bytes"].as_u64().unwrap() <= 384 * 1024);
    assert!(!events.observation()["capture_failure"].is_null());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("oversized.pcm");
    assert_eq!(
        CaseFailure::write_session_pcm(&path, &vec![0; 8 * 1024 * 1024 + 1])
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(!path.exists());
}

#[test]
fn session_interruption_requires_actual_audio_cancellation() {
    let mut evidence = SessionTurnEvidence::interrupting_audio();
    evidence.observe(&SessionFixture::audio()).unwrap();
    assert!(!evidence.ready());
    evidence.observe(&SessionEvent::TurnCancelled).unwrap();
    assert!(evidence.ready());
    evidence.verify().unwrap();
    assert_eq!(evidence.observation()["model_exact_copy"], "not_applicable");
    assert!(evidence.observe(&SessionEvent::Completed).is_err());
    assert!(evidence.observe(&SessionFixture::audio()).is_err());
}

#[test]
fn session_interruption_rejects_completion_and_missing_progress() {
    let mut no_progress = SessionTurnEvidence::interrupting_audio();
    let cancelled = no_progress.observe(&SessionEvent::TurnCancelled);
    assert!(cancelled.is_err() || no_progress.verify().is_err());
    let mut completed = SessionTurnEvidence::interrupting_audio();
    completed.observe(&SessionFixture::audio()).unwrap();
    let terminal = completed.observe(&SessionEvent::Completed);
    assert!(terminal.is_err() || completed.verify().is_err());
}
