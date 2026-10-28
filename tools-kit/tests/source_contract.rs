use std::{fs, path::Path};

fn source(relative: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(relative))
        .unwrap_or_else(|error| panic!("failed to read `{relative}`: {error}"))
}

fn rust_sources(root: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(root).expect("source directory should read") {
        let path = entry.expect("source entry should read").path();
        if path.is_dir() {
            rust_sources(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[test]
fn crate_root_is_tight_and_supporting_types_live_in_functional_modules() {
    let root = source("src/lib.rs");
    for module in [
        "pub mod declaration;",
        "pub mod execution;",
        "pub mod interaction;",
        "pub mod local;",
        "pub mod mcp;",
        "pub mod schema;",
    ] {
        assert!(
            root.contains(module),
            "missing functional module `{module}`"
        );
    }
    assert!(root.contains("handler::{ToolHandler, ToolHandlerBackend},"));
    assert!(root.contains("toolset::{Toolset, ToolsetBackend},"));
    assert!(!root.contains("name::ToolName"));
    assert!(source("src/declaration.rs").contains("pub use crate::name::ToolName;"));

    for forbidden in [
        "ToolsetBuilder",
        "ToolsetConfig",
        "ToolsetSourceConfig",
        "LocalSourceConfig",
        "McpSourceConfig",
        "ToolsetFactory",
        "ToolsetConformance",
        "ToolConformanceFailure",
        "ToolDeclaration",
        "ToolList",
    ] {
        assert!(
            !root.contains(forbidden),
            "crate root must not flatten or retain `{forbidden}`"
        );
    }
}

#[test]
fn hierarchy_uses_associated_handlers_and_only_root_custom_ingress() {
    let toolset = source("src/toolset.rs");
    let handler = source("src/handler.rs");
    assert!(toolset.contains("type Handler: crate::ToolHandlerBackend;"));
    assert!(toolset.contains("pub fn from_backend<B: ToolsetBackend>(backend: B) -> Self"));
    assert!(!handler.contains("pub fn from_backend"));

    for arm in [
        "Local(LocalToolset)",
        "Mcp(McpToolset)",
        "Extension(Arc<dyn ErasedToolsetBackend>)",
    ] {
        assert!(
            toolset.contains(arm),
            "Toolset dispatcher must retain `{arm}`"
        );
    }
    for arm in [
        "Local(local::handler::LocalToolHandler)",
        "Mcp(mcp::handler::McpToolHandler)",
        "Extension(Box<dyn ErasedToolHandlerBackend>)",
    ] {
        assert!(
            handler.contains(arm),
            "handler dispatcher must retain `{arm}`"
        );
    }
}

#[test]
fn deleted_product_and_compatibility_symbols_are_absent_from_production() {
    let mut files = Vec::new();
    rust_sources(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let deleted = [
        "ToolsetBuilder",
        "ToolsetConfig",
        "ToolsetSourceConfig",
        "LocalSourceConfig",
        "McpSourceConfig",
        "McpServeConfig",
        "ToolsetFactory",
        "ToolsetFactoryRequest",
        "ToolsetConformance",
        "ToolsetConformanceInputs",
        "ToolConformanceFailure",
        "ToolDeclaration",
        "ToolList",
        "FunctionToolRegistration",
        "InteractiveToolRegistration",
        "ToolFuture",
        "FormInteractionCapability",
        "TextInteractionCapability",
        "ToolInteractionIntent",
        "ToolInteractionDecision",
        "ToolHandlerOutput::Error",
        "ToolError::Execution",
        "source_id",
    ];
    for path in files {
        let contents = fs::read_to_string(&path).expect("Rust source should read");
        for symbol in deleted {
            assert!(
                !contents.contains(symbol),
                "deleted symbol `{symbol}` remains in {}",
                path.display()
            );
        }
        assert!(
            !contents.contains("#[cfg(test)]"),
            "production test code remains in {}",
            path.display()
        );
    }
}

#[test]
fn docs_teach_direct_construction_and_independent_server_ownership() {
    let docs = [
        source("README.md"),
        source("ARCHITECTURE.md"),
        source("docs/PORTABLE_API.md"),
        source("docs/STRICT_TESTING.md"),
    ]
    .join("\n");
    for required in [
        "Toolset::local",
        "Toolset::mcp",
        "Toolset::from_backend",
        "ToolsetBackend",
        "ToolHandlerBackend",
        "McpServer",
    ] {
        assert!(docs.contains(required), "docs omit `{required}`");
    }
    for stale in [
        "ToolsetFactoryRequest",
        "ToolsetConformanceInputs",
        "ToolHandlerOutput::Error",
        "McpSourceConfig::serve",
        "Toolset::serve",
        "Toolset::stop",
    ] {
        assert!(!docs.contains(stale), "docs still teach `{stale}`");
    }
}
