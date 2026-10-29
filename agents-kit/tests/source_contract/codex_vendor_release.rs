use crate::support;

struct CodexAuthority {
    specifications: String,
    vendor_lock: String,
    protocol: String,
}

impl CodexAuthority {
    fn load() -> Self {
        Self {
            specifications: support::read_workspace_file("agents-kit/specs/SPECIFICATIONS.md"),
            vendor_lock: support::read_workspace_file("agents-kit/vendor/vendor-lock.toml"),
            protocol: support::read_workspace_file("agents-kit/specs/codex/PROTOCOL.md"),
        }
    }
}

#[test]
fn selected_codex_release_has_one_pinned_authority() {
    let authority = CodexAuthority::load();

    assert!(
        authority
            .specifications
            .contains("Codex App Server: `rust-v0.163.0`")
    );
    for fact in [
        "tag = \"rust-v0.163.0\"",
        "archive = \"codex-0.163.0.zip\"",
        "extracted_dir = \"codex-0.163.0\"",
        "sha256 = \"36fda4e61c251d805c2d68203c3a34b0f6881a88846e12776ca02fb13d6e67c7\"",
    ] {
        assert!(
            authority.vendor_lock.contains(fact),
            "Codex vendor lock lost `{fact}`"
        );
    }
    assert!(
        !authority.protocol.contains("rust-v0.163.0"),
        "the protocol contract must defer release selection to SPECIFICATIONS.md"
    );
}

#[test]
fn codex_contract_owns_semantics_not_private_rust_choreography() {
    let protocol = CodexAuthority::load().protocol;

    for forbidden in [
        "src/backends/codex/",
        "CodexAgentRuntime::",
        "CodexConversationRuntime::",
        "fn handle_",
        "match AgentInner",
    ] {
        assert!(
            !protocol.contains(forbidden),
            "Codex contract freezes private implementation spelling `{forbidden}`"
        );
    }
}
