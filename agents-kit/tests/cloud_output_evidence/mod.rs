use std::future::Future;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use agents_kit::prompt::PromptRequest;
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::native_cleanup::NativeCleanup;

/// Test-local observations. No diagnostic value is used to construct a result.
pub struct CloudOutputEvidence {
    expected: String,
    prompt: String,
    model: String,
    directory: PathBuf,
    attempt: String,
    unary: UnaryObservation,
    stream: StreamObservation,
    native: Value,
    fused: bool,
}

enum UnaryObservation {
    NotStarted,
    Bounded {
        expected: String,
        prompt: String,
        output_bytes: usize,
    },
    Awaiting {
        expected: String,
        prompt: String,
    },
    Completed {
        expected: String,
        prompt: String,
        output: String,
    },
}

enum StreamObservation {
    Awaiting,
    Bounded {
        text: String,
        deltas: usize,
        invalid_delta: bool,
        output_bytes: usize,
    },
    Partial {
        text: String,
        deltas: usize,
        invalid_delta: bool,
    },
    Completed {
        text: String,
        deltas: usize,
        invalid_delta: bool,
        output: String,
    },
}

impl CloudOutputEvidence {
    pub fn new(model: String, expected: String, directory: PathBuf) -> io::Result<Self> {
        Self::private_directory(&directory)?;
        Ok(Self {
            prompt: format!("Do not call tools. Return only this exact token: {expected}"),
            expected,
            model,
            directory: directory.canonicalize()?,
            attempt: std::env::var("AI_KIT_CLOUD_CAPTURE_ATTEMPT_ID").unwrap_or_default(),
            unary: UnaryObservation::NotStarted,
            stream: StreamObservation::Awaiting,
            native: json!({"capture":"incomplete","reason":"not_observed"}),
            fused: false,
        })
    }

    pub fn private_directory(path: &Path) -> io::Result<()> {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(path)?;
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(io::Error::other("private evidence directory is a symlink"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    fn write_private(path: &Path, value: &Value) -> io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn expected(&self) -> &str {
        &self.expected
    }
    pub fn prompt(&self) -> &str {
        &self.prompt
    }
    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn configure(&self, config: &mut Value) {
        let plugin = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/strict_live_suite/diagnostics/opencode-boundary.mjs")
            .canonicalize()
            .expect("native observer must exist");
        let plugin = reqwest::Url::from_file_path(plugin).expect("observer file URL");
        config["plugin"].as_array_mut().expect("plugin inventory").push(json!([
            plugin.as_str(), {"outputDirectory":self.directory,"expectedProvider":"openai",
                "expectedModelName":self.model,"freshOutput":{"prompt":self.prompt,"expected":self.expected}}
        ]));
    }

    pub fn request(&self) -> PromptRequest {
        PromptRequest::new(self.prompt.clone()).expect("original cloud prompt remains valid")
    }

    pub fn begin_unary(&mut self, expected: &str) -> String {
        let prompt = format!("Do not call tools. Return only this exact token: {expected}");
        self.unary = UnaryObservation::Awaiting {
            expected: expected.to_owned(),
            prompt: prompt.clone(),
        };
        prompt
    }

    pub fn unary_result(&mut self, output: &str) {
        if let UnaryObservation::Awaiting { expected, prompt } = &self.unary {
            if output.len() > 64 * 1024 {
                self.unary = UnaryObservation::Bounded {
                    expected: expected.clone(),
                    prompt: prompt.clone(),
                    output_bytes: output.len(),
                };
                return;
            }
            self.unary = UnaryObservation::Completed {
                expected: expected.clone(),
                prompt: prompt.clone(),
                output: output.to_owned(),
            };
        }
    }

    pub fn delta(&mut self, delta: &str) {
        if matches!(self.stream, StreamObservation::Awaiting) {
            self.stream = StreamObservation::Partial {
                text: String::new(),
                deltas: 0,
                invalid_delta: false,
            };
        }
        if let StreamObservation::Partial {
            text,
            deltas,
            invalid_delta,
        } = &mut self.stream
        {
            *deltas += 1;
            if delta.is_empty() || text.len().saturating_add(delta.len()) > 64 * 1024 {
                *invalid_delta = true;
            } else {
                text.push_str(delta);
            }
        }
    }

    pub fn terminal(&mut self, output: &str) {
        let previous = std::mem::replace(&mut self.stream, StreamObservation::Awaiting);
        let (text, deltas, invalid_delta) = match previous {
            StreamObservation::Awaiting => (String::new(), 0, false),
            StreamObservation::Partial {
                text,
                deltas,
                invalid_delta,
            }
            | StreamObservation::Completed {
                text,
                deltas,
                invalid_delta,
                ..
            }
            | StreamObservation::Bounded {
                text,
                deltas,
                invalid_delta,
                ..
            } => (text, deltas, invalid_delta),
        };
        if output.len() > 64 * 1024 {
            self.stream = StreamObservation::Bounded {
                text,
                deltas,
                invalid_delta,
                output_bytes: output.len(),
            };
            return;
        }
        self.stream = StreamObservation::Completed {
            text,
            deltas,
            invalid_delta,
            output: output.to_owned(),
        };
    }

    pub fn fused(&mut self, fused: bool) {
        self.fused = fused;
    }

    pub async fn observe_native(
        &mut self,
        deadline: Instant,
        observation: impl Future<Output = Result<Value, &'static str>>,
    ) {
        let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
        self.native = match NativeCleanup::observe(deadline, observation).await {
            Ok(Ok(Ok(mut value))) => {
                if let Some(fields) = value.as_object_mut() {
                    for key in ["session", "user", "assistant"] {
                        fields.remove(key);
                    }
                }
                if value.to_string().len() > 4 * 1024 {
                    json!({"capture":"incomplete","reason":"diagnostic_bound"})
                } else {
                    value
                }
            }
            Ok(Ok(Err(reason))) => json!({"capture":"incomplete","reason":reason}),
            Ok(Err(_)) => json!({"capture":"incomplete","reason":"deadline"}),
            Err(_) => json!({"capture":"incomplete","reason":"readback_panic"}),
        };
    }

    pub fn public_summary(&self) -> Value {
        let unary = match &self.unary {
            UnaryObservation::NotStarted => json!({"capture":"incomplete","state":"not_started"}),
            UnaryObservation::Bounded { output_bytes, .. } => {
                json!({"capture":"incomplete","state":"terminal_bound","portable_bytes":output_bytes})
            }
            UnaryObservation::Awaiting { expected, .. } => {
                json!({"capture":"incomplete","state":"awaiting","expected_bytes":expected.len()})
            }
            UnaryObservation::Completed {
                expected, output, ..
            } => {
                json!({"capture":"complete","expected_bytes":expected.len(),"portable_bytes":output.len(),"portable_equals_expected":output==expected})
            }
        };
        let stream = match &self.stream {
            StreamObservation::Awaiting => json!({"capture":"incomplete","state":"awaiting"}),
            StreamObservation::Bounded {
                text,
                deltas,
                invalid_delta,
                output_bytes,
            } => {
                json!({"capture":"incomplete","state":"terminal_bound","portable_bytes":output_bytes,"incremental_bytes":text.len(),"deltas":deltas,"invalid_delta":invalid_delta})
            }
            StreamObservation::Partial {
                text,
                deltas,
                invalid_delta,
            } => {
                json!({"capture":"incomplete","state":"partial","incremental_bytes":text.len(),"deltas":deltas,"invalid_delta":invalid_delta})
            }
            StreamObservation::Completed {
                text,
                deltas,
                invalid_delta,
                output,
            } => json!({
                "capture":"complete","state":"completed","expected_bytes":self.expected.len(),"portable_bytes":output.len(),
                "incremental_bytes":text.len(),"deltas":deltas,"invalid_delta":invalid_delta,"stream_fused":self.fused,
                "portable_equals_expected":output==&self.expected,"incremental_equals_portable":text==output,"incremental_equals_expected":text==&self.expected
            }),
        };
        json!({"unary":unary,"stream":stream,"native":self.native})
    }

    fn private_text(&self) -> Value {
        let unary = match &self.unary {
            UnaryObservation::NotStarted => Value::Null,
            UnaryObservation::Awaiting { expected, prompt }
            | UnaryObservation::Bounded {
                expected, prompt, ..
            } => {
                json!({"expected":expected,"prompt":prompt})
            }
            UnaryObservation::Completed {
                expected,
                prompt,
                output,
            } => json!({"expected":expected,"prompt":prompt,"public_terminal":output}),
        };
        let mut stream = json!({"expected":self.expected,"prompt":self.prompt});
        match &self.stream {
            StreamObservation::Awaiting => {}
            StreamObservation::Partial { text, .. } | StreamObservation::Bounded { text, .. } => {
                stream["public_deltas"] = json!(text);
            }
            StreamObservation::Completed { text, output, .. } => {
                stream["public_deltas"] = json!(text);
                stream["public_terminal"] = json!(output);
            }
        }
        json!({"version":1,"campaign_attempt_id":self.attempt,"model":{"providerID":"openai","modelID":self.model},"phases":{"unary":unary,"stream":stream}})
    }

    fn provider(&self) -> io::Result<Value> {
        let mut rows = Vec::new();
        let mut files = 0;
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("opencode-boundary-") || !name.ends_with(".jsonl") {
                continue;
            }
            files += 1;
            if files > 1 {
                return Err(io::Error::other("ambiguous provider receipts"));
            }
            let mut bytes = Vec::new();
            std::fs::File::open(entry.path())?
                .take(128 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 128 * 1024 {
                return Err(io::Error::other("provider receipt bound"));
            }
            for line in bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                rows.push(serde_json::from_slice::<Value>(line)?);
            }
        }
        Ok(json!({"receipts":rows}))
    }

    /// Staged text remains private until the campaign validates every boundary.
    pub fn publish(
        &self,
        case_passed: bool,
        cleanup_passed: bool,
        foreground: Value,
        diagnostic_errors: &[&str],
    ) -> io::Result<()> {
        let pending = self.directory.join("private-pending");
        Self::private_directory(&pending)?;
        Self::write_private(&pending.join("case-text.json"), &self.private_text())?;
        let provider = self
            .provider()
            .unwrap_or_else(|_| json!({"capture":"incomplete","reason":"receipt_read"}));
        let summary = json!({"version":3,"campaign_attempt_id":self.attempt,
            "case":"strict_live_suite::prompt_and_io::opencode_cloud_generation_and_streaming_are_live",
            "case_passed":case_passed,"cleanup_passed":cleanup_passed,
            "public":self.public_summary(),"foreground":foreground,"provider":provider,
            "diagnostic_errors":diagnostic_errors,"raw_body_disposition":"pending_campaign_validation"});
        Self::write_private(&self.directory.join("summary.json"), &summary)?;
        let marker = json!({"version":3,"campaign_attempt_id":self.attempt,
            "evidence_directory":self.directory,"case_passed":case_passed,"cleanup_passed":cleanup_passed});
        writeln!(
            io::stderr().lock(),
            "\nAGENTS_KIT_CLOUD_OUTPUT_DIAGNOSTIC={marker}"
        )
    }
}
