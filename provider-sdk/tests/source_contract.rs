use std::fs;
use std::path::{Path, PathBuf};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).expect("source directory should be readable") {
            let path = entry.expect("source entry should be readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("contract source should be readable")
}

#[test]
fn production_source_uses_the_required_safety_and_building_rules() {
    let root = manifest_dir().join("src");
    for path in rust_files(&root) {
        let source = read(&path);
        let relative = path
            .strip_prefix(manifest_dir())
            .expect("source should be inside the crate")
            .display();
        for forbidden in [
            "unsafe {",
            "unsafe fn",
            "#[cfg(test)]",
            "async_trait",
            "macro_rules!",
            "include!(",
            "#[path",
            "std::env::",
            "env::var(",
            "option_env!(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} contains forbidden production pattern `{forbidden}`"
            );
        }
        for line in source.lines() {
            let top_level_alias = line.starts_with("type ")
                || line.starts_with("pub type ")
                || line.starts_with("pub(crate) type ");
            assert!(
                !top_level_alias,
                "{relative} contains forbidden Rust item type alias `{line}`"
            );
        }
    }
}

#[test]
fn public_namespace_is_functional_and_root_is_tight() {
    let lib = read(manifest_dir().join("src/lib.rs"));
    for module in [
        "config",
        "generation",
        "embedding",
        "hosted",
        "files",
        "batch",
        "models",
        "session",
        "runtime",
        "error",
    ] {
        assert!(
            lib.contains(&format!("pub mod {module}")),
            "lib.rs must expose functional module `{module}`"
        );
    }
    for root_export in [
        "pub use crate::config::ProviderConfig;",
        "pub use crate::models::{Model, ModelBackend, ModelId};",
        "pub use crate::provider::{Provider, ProviderBackend};",
        "pub use crate::session::{Session, SessionBackend};",
    ] {
        assert!(
            lib.contains(root_export),
            "lib.rs is missing exact primary root export `{root_export}`"
        );
    }
    let root_exports = lib
        .lines()
        .filter(|line| line.starts_with("pub use "))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in [
        "pub use crate::config::{ProviderConfig, ProviderKind}",
        "GenerateRequest",
        "GenerateResponse",
        "ModelListRequest",
        "SessionConfig",
        "HostedToolOutput",
    ] {
        assert!(
            !root_exports.contains(forbidden),
            "lib.rs flattens an advanced DTO family through `{forbidden}`"
        );
    }
}

#[test]
fn public_declarations_are_owned_by_root_files_or_configuration() {
    let root = manifest_dir().join("src");
    let config = root.join("config");
    for path in rust_files(&root) {
        if path.parent() == Some(root.as_path()) || path.starts_with(&config) {
            continue;
        }
        for (index, line) in read(&path).lines().enumerate() {
            assert!(
                !line.trim_start().starts_with("pub "),
                "{}:{} declares public API outside root source files or config",
                path.display(),
                index + 1,
            );
        }
    }
}

#[test]
fn provider_hierarchy_has_no_identity_erasure_or_normalization_route() {
    let source = rust_files(&manifest_dir().join("src"))
        .into_iter()
        .map(read)
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in [
        "ProviderImplementationId",
        "implementation_id",
        "ErasedProviderBackend",
        "ErasedModelBackend",
        "ErasedSessionBackend",
        "pub fn from_backend",
        "fn from_extension",
        "ProviderInner::Extension",
        "ModelInner::Extension",
        "SessionInner::Extension",
        "dyn ProviderBackend",
        "dyn ModelBackend",
        "dyn SessionBackend",
    ] {
        assert!(
            !source.contains(forbidden),
            "provider hierarchy retains forbidden normalization pattern `{forbidden}`"
        );
    }
}

#[test]
fn deleted_product_surfaces_and_dependency_edges_stay_deleted() {
    let crate_dir = manifest_dir();
    let source = rust_files(&crate_dir.join("src"))
        .into_iter()
        .map(read)
        .collect::<Vec<_>>()
        .join("\n");
    for deleted in [
        "LoopDriver",
        "ProviderFactory",
        "ProviderConformance",
        "CapabilityMatrix",
        "GenerateRequestTemplate",
        "ProviderHostedToolsBackend",
        "pub enum ModelsRequest",
        "pub enum ModelsResponse",
        "pub enum FilesRequest",
        "pub enum FilesResponse",
        "pub enum BatchRequest",
        "pub enum BatchResponse",
        "pub enum RuntimeRequest",
        "pub enum RuntimeResponse",
        "pub struct RuntimeInventoryResponse",
        "pub enum SessionKind",
        "pub enum SessionState",
        "pub struct SessionConfigParts",
        "pub enum Attachment",
        "pub struct SessionOpenRequest",
        "pub enum AttachmentSource",
        "pub enum AttachmentPurpose",
        "pub struct EmbeddedVector",
        "pub struct FileError",
        "pub enum EmbeddingInput",
        "pub enum BatchInputSource",
        "pub struct HostedLocationGrounding",
        "pub struct HostedImageGeneration",
        "SdkError::NotImplemented",
        "SdkError::Streaming",
        "SdkError::WebSocket",
        "SdkError::Serialization",
        "SdkError::Io",
        "ReceiverStream",
        "RegisteredTool",
        "ShellExecutor",
        "tool_sdk",
    ] {
        assert!(
            !source.contains(deleted),
            "deleted product surface `{deleted}` remains in production source"
        );
    }
    for path in [
        "src/builder.rs",
        "src/factory.rs",
        "src/conformance.rs",
        "src/driver.rs",
        "src/core/capability.rs",
    ] {
        assert!(
            !crate_dir.join(path).is_file(),
            "deleted product path `{path}` still exists"
        );
    }
    for path in ["src/core/driver", "src/core/tools/registered"] {
        assert!(
            !crate_dir.join(path).is_dir() || rust_files(&crate_dir.join(path)).is_empty(),
            "deleted product directory `{path}` still contains Rust source"
        );
    }
    assert!(
        !read(crate_dir.join("Cargo.toml")).contains("tool-sdk"),
        "provider-sdk must not depend on tool-sdk"
    );
}
