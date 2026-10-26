use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

struct CompileCase {
    name: &'static str,
    source: &'static str,
    expected: &'static [&'static str],
}

impl CompileCase {
    fn run(&self) {
        let project = tempfile::tempdir().expect("compile-contract temp directory should build");
        fs::create_dir(project.path().join("src"))
            .expect("compile-contract src directory should build");
        fs::write(project.path().join("src/main.rs"), self.source)
            .expect("compile-contract source should write");
        fs::write(
            project.path().join("Cargo.toml"),
            format!(
                "[package]\nname = \"provider-contract-{}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\nprovider-sdk = {{ path = \"{}\" }}\n",
                self.name,
                Self::escaped_path(&Self::manifest_dir())
            ),
        )
        .expect("compile-contract manifest should write");
        fs::create_dir(project.path().join(".cargo"))
            .expect("compile-contract cargo directory should build");
        fs::write(
            project.path().join(".cargo/config.toml"),
            "[net]\noffline = true\n",
        )
        .expect("compile-contract cargo config should write");

        let output = Command::new("cargo")
            .args(["check", "--quiet", "--offline"])
            .current_dir(project.path())
            .env("CARGO_TARGET_DIR", Self::target_dir())
            .output()
            .expect("compile-contract cargo check should run");
        assert!(
            !output.status.success(),
            "{} unexpectedly compiled",
            self.name
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        for fragment in self.expected {
            assert!(
                stderr.contains(fragment),
                "{} stderr omitted `{fragment}`:\n{stderr}",
                self.name
            );
        }
    }

    fn manifest_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn target_dir() -> PathBuf {
        Self::manifest_dir()
            .parent()
            .expect("workspace root should exist")
            .join("target/compile-contracts")
    }

    fn escaped_path(path: &Path) -> String {
        path.display().to_string().replace('\\', "\\\\")
    }
}

#[test]
fn removed_ingress_routers_mutable_fields_and_synthetic_sessions_are_rejected() {
    for case in [
        CompileCase {
            name: "private-core",
            source: "use provider_sdk::core; fn main() {}\n",
            expected: &["core", "private"],
        },
        CompileCase {
            name: "provider-ingress",
            source: "fn main() { let _ = provider_sdk::Provider::from_backend(()); }\n",
            expected: &["Provider", "from_backend"],
        },
        CompileCase {
            name: "model-ingress",
            source: "fn main() { let _ = provider_sdk::Model::from_extension(()); }\n",
            expected: &["Model", "from_extension"],
        },
        CompileCase {
            name: "session-ingress",
            source: "fn main() { let _ = provider_sdk::Session::from_extension(()); }\n",
            expected: &["Session", "from_extension"],
        },
        CompileCase {
            name: "implementation-identity-type",
            source: "fn main() { let _ = provider_sdk::ProviderImplementationId::new(\"custom\"); }\n",
            expected: &["ProviderImplementationId"],
        },
        CompileCase {
            name: "advanced-root-dtos",
            source: "use provider_sdk::{GenerateRequest, ProviderKind}; fn main() {}\n",
            expected: &["GenerateRequest", "ProviderKind"],
        },
        CompileCase {
            name: "removed-router-envelopes",
            source: "use provider_sdk::models::ModelsRequest; use provider_sdk::files::FilesRequest; use provider_sdk::batch::BatchRequest; use provider_sdk::runtime::RuntimeRequest; fn main() {}\n",
            expected: &[
                "ModelsRequest",
                "FilesRequest",
                "BatchRequest",
                "RuntimeRequest",
            ],
        },
        CompileCase {
            name: "removed-synthetic-session-types",
            source: "use provider_sdk::session::{SessionKind, SessionOpenRequest, SessionState}; fn main() {}\n",
            expected: &["SessionKind", "SessionOpenRequest", "SessionState"],
        },
        CompileCase {
            name: "removed-session-decomposition",
            source: "use provider_sdk::session::SessionConfig; fn main() { let _ = SessionConfig::default().into_parts(); }\n",
            expected: &["SessionConfig", "into_parts"],
        },
        CompileCase {
            name: "removed-session-generation",
            source: "use provider_sdk::session::SessionBackend; fn forbidden<B: SessionBackend>(session: &mut B) { let _ = session.generate; } fn main() {}\n",
            expected: &["generate"],
        },
        CompileCase {
            name: "private-request-fields",
            source: "use provider_sdk::generation::{GenerateRequest, Message}; fn main() { let mut request = GenerateRequest::<()>::new(vec![Message::user_text(\"hi\").unwrap()]).unwrap(); request.messages.clear(); }\n",
            expected: &["messages", "private"],
        },
        CompileCase {
            name: "private-message-fields",
            source: "use provider_sdk::generation::Message; fn main() { let mut message = Message::<()>::user_text(\"hi\").unwrap(); message.content.clear(); }\n",
            expected: &["content", "private"],
        },
        CompileCase {
            name: "private-generation-config-fields",
            source: "use provider_sdk::generation::GenerationConfig; fn main() { let mut config = GenerationConfig::default(); config.temperature = Some(0.5); }\n",
            expected: &["temperature", "private"],
        },
        CompileCase {
            name: "removed-content-and-hosted-options",
            source: "use provider_sdk::generation::ContentPart; use provider_sdk::hosted::HostedTool; fn main() { let _ = ContentPart::Binary; let _ = ContentPart::ImageBase64; let _ = ContentPart::AudioBase64; let _ = ContentPart::FileRef; let _ = HostedTool::ImageGeneration; let _ = HostedTool::LocationGrounding; }\n",
            expected: &[
                "Binary",
                "ImageBase64",
                "AudioBase64",
                "FileRef",
                "ImageGeneration",
                "LocationGrounding",
            ],
        },
        CompileCase {
            name: "opaque-runtime-error",
            source: "fn main() { let _ = provider_sdk::SdkError {}; }\n",
            expected: &["SdkError", "private"],
        },
        CompileCase {
            name: "opaque-remote-failure",
            source: "fn main() { let _ = provider_sdk::error::RemoteFailure {}; }\n",
            expected: &["RemoteFailure", "private"],
        },
        CompileCase {
            name: "removed-error-categories",
            source: "fn main() { let _ = provider_sdk::SdkError::NotImplemented(\"x\".to_string()); let _ = provider_sdk::SdkError::Streaming(\"x\".to_string()); let _ = provider_sdk::SdkError::WebSocket(\"x\".to_string()); let _ = provider_sdk::SdkError::Serialization(\"x\".to_string()); let _ = provider_sdk::SdkError::Io(\"x\".to_string()); }\n",
            expected: &[
                "NotImplemented",
                "Streaming",
                "WebSocket",
                "Serialization",
                "Io",
            ],
        },
        CompileCase {
            name: "trait-import-required",
            source: "use provider_sdk::{ModelId, Provider}; fn main() { let provider: Provider = todo!(); let _ = provider.model(ModelId::new(\"m\").unwrap()); }\n",
            expected: &["model", "ProviderBackend"],
        },
        CompileCase {
            name: "session-not-clone",
            source: "fn assert_clone<T: Clone>() {} fn main() { assert_clone::<provider_sdk::Session>(); }\n",
            expected: &["Session", "Clone"],
        },
    ] {
        case.run();
    }
}
