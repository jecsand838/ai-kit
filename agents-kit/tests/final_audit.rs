use std::fs;
use std::path::{Path, PathBuf};

struct ReductionAudit {
    workspace: PathBuf,
}

impl ReductionAudit {
    fn load() -> Self {
        Self {
            workspace: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("agents-kit must live beneath the workspace")
                .to_path_buf(),
        }
    }

    fn read(&self, path: &str) -> String {
        fs::read_to_string(self.workspace.join(path))
            .unwrap_or_else(|error| panic!("read {path}: {error}"))
    }

    fn files(&self, root: &str, extension: &str) -> Vec<PathBuf> {
        let mut files = Vec::new();
        Self::collect(&self.workspace.join(root), extension, &mut files);
        files.sort();
        files
    }

    fn collect(directory: &Path, extension: &str, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
        {
            let path = entry.expect("audit entry must be readable").path();
            if path.is_dir() {
                Self::collect(&path, extension, files);
            } else if path
                .extension()
                .is_some_and(|candidate| candidate == extension)
            {
                files.push(path);
            }
        }
    }
}

#[test]
fn retired_agent_product_files_are_deleted() {
    let audit = ReductionAudit::load();
    for path in [
        "agents-kit/src/conformance.rs",
        "agents-kit/src/factory.rs",
        "agents-kit/tests/agent_conformance.rs",
        "agents-kit/tests/factory_contract.rs",
        "agents-kit/tests/knowledge_conformance.rs",
    ] {
        assert!(
            !audit.workspace.join(path).exists(),
            "retired path remains: {path}"
        );
    }
}

#[test]
fn active_agent_sources_contain_no_refactor_markers_or_compatibility_surface() {
    let audit = ReductionAudit::load();
    let production = audit
        .files("agents-kit/src", "rs")
        .into_iter()
        .map(|path| {
            fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(!production.contains("PUBLIC_TRAIT_REFACTOR_PLAN"));
    for forbidden in [
        "#[deprecated",
        "compatibility shim",
        "KnowledgeBaseBackend",
        "LoopDriver",
        "AgentFactory",
        "AgentConformance",
    ] {
        assert!(
            !production.contains(forbidden),
            "retired production contract remains: {forbidden}"
        );
    }
}

#[test]
fn governing_agent_documents_name_the_reduced_hierarchy() {
    let audit = ReductionAudit::load();
    for path in [
        "AGENTS.md",
        "ARCHITECTURE.md",
        "agents-kit/AGENTS.md",
        "agents-kit/ARCHITECTURE.md",
        "agents-kit/README.md",
        "agents-kit/specs/API.md",
    ] {
        let document = audit.read(path);
        assert!(
            document.contains("Agent")
                && document.contains("Workspace")
                && document.contains("Conversation"),
            "{path} must teach the retained hierarchy"
        );
    }
}
