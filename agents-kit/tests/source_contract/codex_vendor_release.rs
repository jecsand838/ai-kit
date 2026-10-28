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

    fn vendor_source(&self, path: &str) -> String {
        support::read_workspace_file(format!("agents-kit/vendor/codex-0.163.0/codex-rs/{path}"))
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
fn selected_codex_wire_facts_are_sourced_without_becoming_public_api() {
    let authority = CodexAuthority::load();
    let rpc = authority.vendor_source("app-server-protocol/src/rpc.rs");
    let methods = authority.vendor_source("app-server-protocol/src/protocol/common.rs");
    let stdio = authority.vendor_source("app-server-transport/src/transport/stdio.rs");
    let websocket = authority.vendor_source("app-server-transport/src/transport/websocket.rs");
    let tools = authority.vendor_source("protocol/src/dynamic_tools.rs");
    let items = authority.vendor_source("app-server-protocol/src/protocol/v2/item.rs");

    for sourced in ["pub enum RequestId", "pub enum JSONRPCMessage"] {
        assert!(
            rpc.contains(sourced),
            "selected RPC authority lost `{sourced}`"
        );
    }
    for sourced in [
        "ThreadStart => \"thread/start\"",
        "TurnStart => \"turn/start\"",
        "TurnInterrupt => \"turn/interrupt\"",
        "ThreadDelete => \"thread/delete\"",
    ] {
        assert!(
            methods.contains(sourced),
            "selected method authority lost `{sourced}`"
        );
    }
    assert!(stdio.contains("std::io::stdin().lock().lines()"));
    assert!(websocket.contains("IncomingWebSocketMessage::Text(text)"));
    assert!(tools.contains("pub enum DynamicToolSpec"));
    assert!(items.contains("pub struct DynamicToolCallParams"));

    for contract in [
        "Typed wire decode and routing end at the protocol boundary.",
        "Kit-spawned stdio connections carry one JSON value per line.",
        "TCP and Unix-socket connections carry JSON-RPC values in WebSocket text",
        "Wire request identifiers, tool-call payloads, and response DTOs remain private.",
    ] {
        assert!(
            authority.protocol.contains(contract),
            "Codex protocol contract lost `{contract}`"
        );
    }
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
