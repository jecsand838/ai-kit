use std::fs;
use std::path::{Path, PathBuf};

struct AgentSource {
    crate_root: PathBuf,
}

impl AgentSource {
    fn load() -> Self {
        Self {
            crate_root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        }
    }

    fn read(&self, path: &str) -> String {
        fs::read_to_string(self.crate_root.join(path))
            .unwrap_or_else(|error| panic!("read {path}: {error}"))
    }

    fn assert_public_generics(&self, name: &str, expected: &[&str]) {
        let source = self.production();
        let declaration = format!("pub struct {name}<");
        let mut declarations = source.split(&declaration).skip(1);
        let parameters = declarations
            .next()
            .unwrap_or_else(|| panic!("missing public declaration `{declaration}`"))
            .split_once('>')
            .expect("public generic declaration must close")
            .0
            .split(',')
            .filter(|parameter| !parameter.trim().is_empty())
            .map(|parameter| parameter.split_whitespace().collect::<String>())
            .collect::<Vec<_>>();
        assert!(
            declarations.next().is_none(),
            "multiple public declarations for `{name}`"
        );
        assert_eq!(
            parameters, expected,
            "{name} has the wrong generic contract"
        );
    }

    fn production(&self) -> String {
        let mut files = Vec::new();
        Self::rust_files(&self.crate_root.join("src"), &mut files);
        files.sort();
        files
            .into_iter()
            .map(|path| {
                fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
        {
            let path = entry.expect("source entry must be readable").path();
            if path.is_dir() {
                Self::rust_files(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
}

#[test]
fn root_exports_only_primary_runtime_owners() {
    let lib = AgentSource::load().read("src/lib.rs");
    for required in [
        "pub use crate::agent::{Agent, AgentBackend};",
        "pub use crate::agent_storage::AgentStorage;",
        "pub use crate::builder::AgentBuilder;",
        "pub use crate::conversation::{Conversation, ConversationBackend};",
        "pub use crate::error::SdkError;",
        "pub use crate::workspace::{Workspace, WorkspaceBackend};",
        "pub mod config;",
        "pub mod error;",
        "pub mod conversation;",
        "pub mod identity;",
        "pub mod knowledge;",
        "pub mod model;",
        "pub mod notification;",
        "pub mod prompt;",
        "pub mod tool;",
        "pub mod workspace;",
    ] {
        assert!(lib.contains(required), "missing root contract `{required}`");
    }
}

#[test]
fn integration_composition_is_static_and_preserves_public_generic_parameters() {
    let source = AgentSource::load();
    let storage = source.read("src/agent_storage.rs");
    let builder = source.read("src/builder.rs");

    assert!(storage.contains("pub struct AgentStorage<"));
    assert!(storage.contains("S: StateStoreBackend = storage_sdk::StateStore"));
    assert!(builder.contains("pub struct AgentBuilder<"));
    assert!(builder.contains("S: StateStoreBackend = StateStore"));
    assert!(builder.contains("D: DatabaseBackend = Database"));
    assert!(builder.contains("P: ProviderBackend = provider_sdk::Provider"));
    assert!(!builder.contains("create_for_provider"));
    assert!(!builder.contains("reopen_for_provider"));
    assert!(!builder.contains("toolset: tool_sdk::Toolset"));
    assert!(!builder.contains("Toolset::from_backend"));

    source.assert_public_generics(
        "AgentBuilder",
        &[
            "S:StateStoreBackend=StateStore",
            "D:DatabaseBackend=Database",
            "P:ProviderBackend=provider_sdk::Provider",
            "T:ToolsetBackend=tool_sdk::Toolset",
        ],
    );
    for handle in ["Agent", "Workspace", "Conversation"] {
        source.assert_public_generics(
            handle,
            &[
                "S:StateStoreBackend=storage_sdk::StateStore",
                "D:DatabaseBackend=storage_sdk::Database",
                "P:ProviderBackend=provider_sdk::Provider",
                "T:ToolsetBackend=tool_sdk::Toolset",
            ],
        );
    }
}

#[test]
fn production_contains_no_agent_behavior_erasure() {
    let production = AgentSource::load().production();
    for forbidden in [
        "ErasedAgentBackend",
        "ErasedWorkspaceBackend",
        "ErasedConversationBackend",
        "dyn AgentBackend",
        "dyn WorkspaceBackend",
        "dyn ConversationBackend",
    ] {
        assert!(
            !production.contains(forbidden),
            "agent behavior erasure returned: {forbidden}"
        );
    }
}

#[test]
fn production_contains_no_storage_erasure_or_normalization_route() {
    let production = AgentSource::load().production();
    for forbidden in [
        "DatabaseImplementationId",
        "ErasedDatabaseBackend",
        "ErasedStateStoreBackend",
        "ErasedStateTransactionBackend",
        "dyn DatabaseBackend",
        "dyn StateStoreBackend",
        "dyn StateTransactionBackend",
        "Database::from_backend",
        "StateStore::from_backend",
    ] {
        assert!(
            !production.contains(forbidden),
            "storage erasure or normalization returned: {forbidden}"
        );
    }
}

#[test]
fn production_contains_no_provider_erasure_identity_or_normalization_route() {
    let production = AgentSource::load().production();
    for forbidden in [
        "ProviderImplementationId",
        "ErasedProviderBackend",
        "ErasedModelBackend",
        "ErasedSessionBackend",
        "dyn ProviderBackend",
        "dyn ModelBackend",
        "dyn SessionBackend",
        "Provider::from_backend",
        "Model::from_extension",
        "Session::from_extension",
        "implementation_id",
    ] {
        assert!(
            !production.contains(forbidden),
            "provider erasure, identity, or normalization returned: {forbidden}"
        );
    }
}

#[test]
fn retired_product_frameworks_are_absent_from_production() {
    let production = AgentSource::load().production();
    for forbidden in [
        "KnowledgeBaseBackend",
        "pub struct KnowledgeBase",
        "AgentFactory",
        "KnowledgeBaseFactory",
        "AgentConformance",
        "KnowledgeBaseConformance",
        "LoopDriver",
        "LoopDriveHooks",
        "ProviderHostedToolDeclaration",
        "with_hosted_tools",
        "pub fn hosted_tools",
        "provider_sdk::hosted::HostedTool",
        "ProviderToolSpec::Hosted",
        "WorkspaceExtension<",
        "ConversationExtension<",
        "AgentExtension<",
    ] {
        assert!(
            !production.contains(forbidden),
            "retired production symbol remains: {forbidden}"
        );
    }
}

#[test]
fn production_has_no_test_only_module_or_public_erased_backend() {
    let production = AgentSource::load().production();
    assert!(!production.contains("#[cfg(test)]"));
    assert!(!production.contains("pub trait Erased"));
    assert!(!production.contains("pub dyn "));
}
