struct StrictLiveEnvironment;

mod build;
mod custom;
mod cwd_alias;
mod delivery_and_events;
mod fixture;
mod hierarchy;
mod knowledge_tools;
mod lance_conflict;
mod prompt_and_io;
mod resolution_attempts;

impl StrictLiveEnvironment {
    const REQUIRED: [&'static str; 21] = [
        "TEST_CODEX_WS_URL",
        "TEST_CODEX_HOME",
        "TEST_OPENCODE_URL",
        "TEST_CUSTOM_OLLAMA_URL",
        "TEST_CODEX_MODEL_DEFAULT",
        "TEST_CODEX_REASONING_EFFORT",
        "TEST_OLLAMA_MODEL_OPENCODE_DEFAULT",
        "TEST_OLLAMA_MODEL_OPENCODE_ALTERNATE",
        "TEST_OLLAMA_MODEL_OPENCODE_SHELL",
        "TEST_OLLAMA_MODEL_CUSTOM_DEFAULT",
        "TEST_GEMINI_API_KEY",
        "TEST_GEMINI_BASE_URL",
        "TEST_GEMINI_SERVICE_TIER",
        "TEST_GEMINI_MODEL_DEFAULT",
        "TEST_OPENAI_API_KEY",
        "TEST_OPENAI_BASE_URL",
        "TEST_ANTHROPIC_API_KEY",
        "TEST_ANTHROPIC_BASE_URL",
        "TEST_CUSTOM_OPENAI_MODEL",
        "TEST_CUSTOM_ANTHROPIC_MODEL",
        "TEST_OPENCODE_CLOUD_MODEL",
    ];

    fn require_all() {
        assert!(
            std::env::var_os("TEST_MODEL_CONFORMANCE_PROFILE").is_none(),
            "TEST_MODEL_CONFORMANCE_PROFILE is retired; strict-live assertions are unconditional"
        );
        for name in Self::REQUIRED {
            let value = std::env::var(name)
                .unwrap_or_else(|_| panic!("missing required strict-live environment: {name}"));
            assert_eq!(
                value.trim(),
                value,
                "strict-live environment contains surrounding whitespace: {name}"
            );
            assert!(
                !value.is_empty(),
                "missing required strict-live environment: {name}"
            );
        }
    }
}

#[test]
fn strict_live_environment_is_explicit_and_complete() {
    StrictLiveEnvironment::require_all();
}
