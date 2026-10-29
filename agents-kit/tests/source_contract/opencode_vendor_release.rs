use crate::support;

struct OpenCodeAuthority {
    specifications: String,
    vendor_lock: String,
    protocol: String,
    backend: String,
}

impl OpenCodeAuthority {
    fn load() -> Self {
        Self {
            specifications: support::read_workspace_file("agents-kit/specs/SPECIFICATIONS.md"),
            vendor_lock: support::read_workspace_file("agents-kit/vendor/vendor-lock.toml"),
            protocol: support::read_workspace_file("agents-kit/specs/opencode/PROTOCOL.md"),
            backend: support::read_workspace_file("agents-kit/specs/opencode/BACKEND.md"),
        }
    }
}

#[test]
fn selected_opencode_release_has_one_pinned_authority() {
    let authority = OpenCodeAuthority::load();

    assert!(
        authority
            .specifications
            .contains("OpenCode Server: `v1.19.3`")
    );
    for fact in [
        "tag = \"v1.19.3\"",
        "archive = \"opencode-1.19.3.zip\"",
        "extracted_dir = \"opencode-1.19.3\"",
        "sha256 = \"662ef890f51c49c364bc8e91ab04f25c8a3abf8833c86d42d9d0e5c31df34b0b\"",
    ] {
        assert!(
            authority.vendor_lock.contains(fact),
            "OpenCode vendor lock lost `{fact}`"
        );
    }
    assert!(
        !authority.protocol.contains("v1.19.3"),
        "the protocol contract must defer release selection to SPECIFICATIONS.md"
    );
}

#[test]
fn opencode_contract_owns_semantics_not_private_rust_choreography() {
    let authority = OpenCodeAuthority::load();

    for forbidden in [
        "src/backends/opencode/",
        "OpenCodeAgentRuntime::",
        "OpenCodeConversationRuntime::",
        "fn handle_",
        "match AgentInner",
    ] {
        assert!(
            !authority.protocol.contains(forbidden) && !authority.backend.contains(forbidden),
            "OpenCode contracts freeze private implementation spelling `{forbidden}`"
        );
    }
}
