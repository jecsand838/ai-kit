use std::fs;
use std::path::PathBuf;

mod live_assertions;

use live_assertions::ExpectedToolResult;
use tool_sdk::execution::ToolResult;

#[test]
fn strict_live_result_rejects_success_with_incorrect_content() {
    let expected = ToolResult::text("independent fixture contents\n");
    let oracle = ExpectedToolResult::parse(&serde_json::to_string(&expected).unwrap()).unwrap();
    assert!(oracle.verify(&expected).is_ok());
    assert!(
        oracle
            .verify(&ToolResult::text("unrelated success"))
            .is_err()
    );
}

#[test]
fn strict_live_result_preserves_complete_result_equality() {
    let expected =
        ToolResult::text("answer").with_structured_content(serde_json::json!({"answer": 42}));
    let oracle = ExpectedToolResult::parse(&serde_json::to_string(&expected).unwrap()).unwrap();
    let mut actual = expected.clone();
    actual.structured_content = None;
    assert!(oracle.verify(&actual).is_err());
    actual = expected.clone();
    actual
        .metadata
        .insert("extra".into(), serde_json::json!(true));
    assert!(oracle.verify(&actual).is_err());
    actual = expected.clone();
    actual.content.push(tool_sdk::execution::ToolContent::Text {
        text: "extra".into(),
        metadata: None,
    });
    assert!(oracle.verify(&actual).is_err());
    assert!(oracle.verify(&expected.with_error(true)).is_err());
}

#[test]
fn strict_live_result_rejects_missing_malformed_or_failure_expectations() {
    for value in ["", "not JSON", "{}", "null"] {
        assert!(ExpectedToolResult::parse(value).is_err());
    }
    let failed = serde_json::to_string(&ToolResult::text("failed").with_error(true)).unwrap();
    assert!(ExpectedToolResult::parse(&failed).is_err());
}

fn env_template() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("strict-tests.env.example");
    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

#[test]
fn template_requires_an_independently_configured_real_mcp_service() {
    let template = env_template();
    assert!(
        template.contains("caller-selected real stdio MCP server"),
        "strict env template should require a real caller-selected service"
    );
    assert!(
        template.contains("tool-sdk/scripts/verify_strict_live.sh"),
        "strict env template should reference the crate-owned verify script"
    );
    for variable in [
        "TEST_TOOL_SDK_MCP_PROGRAM",
        "TEST_TOOL_SDK_MCP_ARGS_JSON",
        "TEST_TOOL_SDK_MCP_ENV_JSON",
        "TEST_TOOL_SDK_MCP_TIMEOUT_SECS",
        "TEST_TOOL_SDK_MCP_TOOL_NAME",
        "TEST_TOOL_SDK_MCP_TOOL_ARGUMENTS_JSON",
        "TEST_TOOL_SDK_MCP_EXPECTED_RESULT_JSON",
    ] {
        assert!(
            template.contains(variable),
            "strict env template should define `{variable}`"
        );
    }
    assert!(
        template.contains("never substitutes a repository-authored"),
        "strict env template must explicitly reject repository-authored fixtures"
    );
    assert!(
        template.contains("@modelcontextprotocol/server-filesystem@2026.7.10"),
        "the canonical live example must pin the independently researched MCP server release"
    );
}

#[test]
fn template_does_not_require_provider_credentials() {
    let template = env_template();
    for variable in [
        "TEST_OPENAI_API_KEY",
        "TEST_OPENAI_BASE_URL",
        "TEST_OPENAI_MODEL_WEB_SEARCH",
        "TEST_OPENAI_MODEL_CODE_EXECUTION",
        "TEST_ANTHROPIC_API_KEY",
        "TEST_ANTHROPIC_BASE_URL",
        "TEST_ANTHROPIC_VERSION",
        "TEST_ANTHROPIC_MODEL_DEFAULT",
        "TEST_GEMINI_API_KEY",
        "TEST_GEMINI_BASE_URL",
        "TEST_GEMINI_MODEL_DEFAULT",
        "TEST_GEMINI_SERVICE_TIER",
    ] {
        assert!(
            !template.contains(variable),
            "tool-sdk strict env template should not require provider variable `{variable}`"
        );
    }
}

#[test]
fn strict_live_verifier_requires_expected_result_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let scripts = directory.path().join("tool-sdk/scripts");
    fs::create_dir_all(&scripts).unwrap();
    fs::create_dir(directory.path().join("scripts")).unwrap();
    fs::copy(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("scripts/release_stage.sh"),
        directory.path().join("scripts/release_stage.sh"),
    )
    .unwrap();
    fs::copy(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/verify_strict_live.sh"),
        scripts.join("verify_strict_live.sh"),
    )
    .unwrap();
    fs::write(
        scripts.join("check.sh"),
        "#!/usr/bin/env bash\nprintf reached > \"${DISPATCH_MARKER}\"\nexit 91\n",
    )
    .unwrap();
    let marker = directory.path().join("dispatch");
    let output = std::process::Command::new("bash")
        .arg(scripts.join("verify_strict_live.sh"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap())
        .env("DISPATCH_MARKER", &marker)
        .env("TEST_TOOL_SDK_MCP_PROGRAM", "unused")
        .env("TEST_TOOL_SDK_MCP_ARGS_JSON", "[]")
        .env("TEST_TOOL_SDK_MCP_ENV_JSON", "{}")
        .env("TEST_TOOL_SDK_MCP_TIMEOUT_SECS", "30")
        .env("TEST_TOOL_SDK_MCP_TOOL_NAME", "live__read_text_file")
        .env("TEST_TOOL_SDK_MCP_TOOL_ARGUMENTS_JSON", "{}")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(
        !marker.exists(),
        "missing expected output must fail before any gate"
    );
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("TEST_TOOL_SDK_MCP_EXPECTED_RESULT_JSON")
    );
}
