use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

struct DownstreamCase {
    name: &'static str,
    source: &'static str,
    expected: &'static [&'static str],
}

struct DownstreamProject {
    path: PathBuf,
}

impl DownstreamProject {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ai-kit-{name}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&path).expect("downstream project directory must build");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DownstreamProject {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl DownstreamCase {
    fn run(self) {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir.parent().expect("workspace root must exist");
        let project = DownstreamProject::new(self.name);
        fs::create_dir_all(project.path().join("src")).expect("source directory must build");
        fs::create_dir_all(project.path().join(".cargo")).expect("Cargo directory must build");
        fs::write(project.path().join("src/main.rs"), self.source)
            .expect("downstream source must write");
        fs::write(
            project.path().join("Cargo.toml"),
            format!(
                "[package]\nname = \"{}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n\n[dependencies]\nagents-kit = {{ path = {} }}\n",
                self.name.replace('-', "_"),
                serde_json::to_string(&manifest_dir).expect("Agent path must serialize"),
            ),
        )
        .expect("downstream manifest must write");
        fs::write(
            project.path().join(".cargo/config.toml"),
            "[net]\noffline = true\n",
        )
        .expect("downstream Cargo config must write");
        let lock = workspace_root.join("Cargo.lock");
        if lock.is_file() {
            fs::copy(lock, project.path().join("Cargo.lock"))
                .expect("workspace lockfile must copy");
        }
        let target_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("compile-contracts");

        let output = Command::new("cargo")
            .arg("check")
            .arg("--offline")
            .arg("--quiet")
            .current_dir(project.path())
            .env("CARGO_TARGET_DIR", target_dir)
            .output()
            .expect("downstream Cargo check must run");
        assert!(
            !output.status.success(),
            "`{}` unexpectedly compiled",
            self.name
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("error[E"),
            "`{}` must reach rustc diagnostics, not fail dependency resolution:\n{stderr}",
            self.name
        );
        for expected in self.expected {
            assert!(
                stderr.contains(expected),
                "`{}` stderr lacked `{expected}`:\n{stderr}",
                self.name
            );
        }
    }
}

#[test]
fn hierarchy_handles_have_no_backend_ingress() {
    for case in [
        DownstreamCase {
            name: "agent-ingress-absent",
            source: r#"
use agents_kit::{Agent, AgentBackend};

fn forbidden<B: AgentBackend>(backend: B) {
    let _: Agent = Agent::from_backend(backend);
}

fn main() {}
"#,
            expected: &["from_backend"],
        },
        DownstreamCase {
            name: "workspace-ingress-absent",
            source: r#"
use agents_kit::{Workspace, WorkspaceBackend};

fn forbidden<B: WorkspaceBackend>(backend: B) {
    let _: Workspace = Workspace::from_backend(backend);
}

fn main() {}
"#,
            expected: &["from_backend"],
        },
        DownstreamCase {
            name: "conversation-ingress-absent",
            source: r#"
use agents_kit::{Conversation, ConversationBackend};

fn forbidden<B: ConversationBackend>(backend: B) {
    let _: Conversation = Conversation::from_backend(backend);
}

fn main() {}
"#,
            expected: &["from_backend"],
        },
    ] {
        case.run();
    }
}

#[test]
fn unique_runtime_handles_are_not_cloneable() {
    DownstreamCase {
        name: "agent-hierarchy-unique",
        source: r#"
use agents_kit::{Agent, Conversation, Workspace};

fn require_clone<T: Clone>() {}

fn main() {
    require_clone::<Agent>();
    require_clone::<Workspace>();
    require_clone::<Conversation>();
}
"#,
        expected: &["Clone"],
    }
    .run();
}

#[test]
fn supporting_types_are_module_scoped() {
    DownstreamCase {
        name: "agent-functional-modules-private-fields",
        source: r#"
use agents_kit::{AgentInfo, ModelInfo};
use agents_kit::identity::AgentId;

fn main() {
    let id: AgentId = "agent_00000000000000000000000000000001".parse().unwrap();
    let _ = AgentInfo { id, display_name: None };
    let _ = std::mem::size_of::<ModelInfo>();
}
"#,
        expected: &[
            "error[E0603]",
            "struct `AgentInfo` is private",
            "struct `ModelInfo` is private",
        ],
    }
    .run();
}

#[test]
fn agent_info_invariant_fields_are_private() {
    DownstreamCase {
        name: "agent-info-private-fields",
        source: r#"
use agents_kit::identity::{AgentId, AgentInfo};

fn forbidden(id: AgentId) {
    let _ = AgentInfo { id, display_name: None };
}

fn main() {}
"#,
        expected: &[
            "error[E0451]",
            "fields `id` and `display_name` of struct `agents_kit::identity::AgentInfo` are private",
        ],
    }
    .run();
}

#[test]
fn supplied_hierarchy_behavior_requires_its_public_traits_in_scope() {
    for case in [
        DownstreamCase {
            name: "agent-behavior-requires-trait",
            source: r#"
use agents_kit::Agent;
use agents_kit::identity::WorkspaceId;
use agents_kit::knowledge::IngestKnowledgeRequest;
use agents_kit::workspace::{DeleteWorkspaceRequest, NewWorkspace, UpdateWorkspaceRequest};

fn behavior(
    agent: Agent,
    id: WorkspaceId,
    new: NewWorkspace,
    update: UpdateWorkspaceRequest,
    delete: DeleteWorkspaceRequest,
    knowledge: IngestKnowledgeRequest,
) {
    let _ = agent.id();
    let _ = agent.info();
    let _ = agent.new_workspace(new);
    let _ = agent.list_workspaces();
    let _ = agent.workspace_info(id.clone());
    let _ = agent.get_workspace(id);
    let _ = agent.check_workspace_update(&update);
    let _ = agent.update_workspace(update);
    let _ = agent.delete_workspace(delete);
    let _ = agent.list_models();
    let _ = agent.ingest_knowledge(knowledge);
    let _ = agent.shutdown();
}

fn main() {}
"#,
            expected: &[
                "no method named `id`",
                "no method named `info`",
                "no method named `new_workspace`",
                "no method named `list_workspaces`",
                "no method named `workspace_info`",
                "no method named `get_workspace`",
                "no method named `check_workspace_update`",
                "no method named `update_workspace`",
                "no method named `delete_workspace`",
                "no method named `list_models`",
                "no method named `ingest_knowledge`",
                "no method named `shutdown`",
            ],
        },
        DownstreamCase {
            name: "workspace-behavior-requires-trait",
            source: r#"
use agents_kit::Workspace;
use agents_kit::conversation::NewConversation;
use agents_kit::identity::ConversationId;
use agents_kit::knowledge::IngestKnowledgeRequest;

fn behavior(workspace: Workspace, id: ConversationId, new: NewConversation, knowledge: IngestKnowledgeRequest) {
    let _ = workspace.id();
    let _ = workspace.info();
    let _ = workspace.new_conversation(new);
    let _ = workspace.list_conversations();
    let _ = workspace.conversation_info(id.clone());
    let _ = workspace.get_conversation(id);
    let _ = workspace.list_tools();
    let _ = workspace.ingest_knowledge(knowledge);
    let _ = workspace.close();
}

fn main() {}
"#,
            expected: &[
                "no method named `id`",
                "no method named `info`",
                "no method named `new_conversation`",
                "no method named `list_conversations`",
                "no method named `conversation_info`",
                "no method named `get_conversation`",
                "no method named `list_tools`",
                "no method named `ingest_knowledge`",
                "no method named `close`",
            ],
        },
        DownstreamCase {
            name: "conversation-behavior-requires-trait",
            source: r#"
use agents_kit::Conversation;
use agents_kit::identity::NotificationId;
use agents_kit::notification::NotificationResolution;
use agents_kit::prompt::PromptRequest;

fn behavior(conversation: Conversation, request: PromptRequest, id: NotificationId, resolution: NotificationResolution) {
    let _ = conversation.id();
    let _ = conversation.info();
    let _ = conversation.prompt(request);
    let _ = conversation.abort();
    let _ = conversation.read_file(std::path::PathBuf::new());
    let _ = conversation.notification_stream();
    let _ = conversation.resolve_notification(id, resolution);
    let _ = conversation.close();
}

fn main() {}
"#,
            expected: &[
                "no method named `id`",
                "no method named `info`",
                "no method named `prompt`",
                "no method named `abort`",
                "no method named `read_file`",
                "no method named `notification_stream`",
                "no method named `resolve_notification`",
                "no method named `close`",
            ],
        },
    ] {
        case.run();
    }
}
