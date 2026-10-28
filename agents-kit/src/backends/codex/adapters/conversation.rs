use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use crate::backends::codex::protocol::requests::CodexThreadWirePolicy;
use crate::{
    KitError, WorkspaceContext, WorkspaceNetworkAccess, WorkspaceRootAccess, WorkspaceSandboxMode,
};

const CODEX_PAGE_LIMIT: usize = 100;
const READ_ONLY_NETWORK_PROFILE: &str = "agents_kit_read_only_network";
const WORKSPACE_WRITE_PROFILE: &str = "agents_kit_workspace_write";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CodexThread {
    id: String,
    cwd: PathBuf,
    thread_source: Option<String>,
    materialization: Option<CodexThreadMaterialization>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CodexThreadMaterialization {
    runtime_workspace_roots: Vec<PathBuf>,
    sandbox: CodexMaterializedSandbox,
    active_permission_profile: Option<CodexPermissionProfile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CodexMaterializedSandbox {
    DangerFullAccess,
    ReadOnly {
        network_access: bool,
    },
    ExternalSandbox {
        network_access: CodexExternalNetworkAccess,
    },
    WorkspaceWrite {
        writable_roots: Vec<PathBuf>,
        network_access: bool,
        exclude_tmpdir_env_var: bool,
        exclude_slash_tmp: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodexExternalNetworkAccess {
    Restricted,
    Enabled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CodexPermissionProfile {
    id: String,
    extends: Option<String>,
}

#[derive(Clone)]
pub(crate) struct CodexWorkspacePolicy {
    cwd: PathBuf,
    runtime_workspace_roots: Vec<PathBuf>,
    permission_profile: CodexPermissionProfile,
    config: Map<String, Value>,
    expected_sandbox: CodexMaterializedSandbox,
    requires_empty_mcp: bool,
}

pub(crate) struct CodexThreadList {
    entries: Vec<CodexThreadIdentity>,
    next_cursor: Option<String>,
}

pub(crate) struct CodexThreadIdentity {
    id: String,
    thread_source: Option<String>,
}

pub(crate) struct CodexFeatureList {
    entries: Vec<(String, bool)>,
    next_cursor: Option<String>,
}

pub(crate) struct CodexMcpStatusList {
    empty: bool,
    next_cursor: Option<String>,
}

pub(crate) struct CodexTerminalList {
    empty: bool,
    next_cursor: Option<String>,
}

struct CodexPageContract;

impl CodexWorkspacePolicy {
    pub(crate) fn new(context: &WorkspaceContext, has_tools: bool) -> Result<Self, KitError> {
        let cwd = context.cwd().to_path_buf();
        let runtime_workspace_roots = context
            .roots()
            .iter()
            .map(|root| root.path().to_path_buf())
            .collect::<Vec<_>>();
        let network_enabled = matches!(
            context.execution_policy().network_access(),
            WorkspaceNetworkAccess::Enabled
        );
        let mut config = Map::new();
        let (permission_profile, expected_sandbox, requires_empty_mcp) =
            match context.execution_policy().sandbox_mode() {
                WorkspaceSandboxMode::ReadOnly if network_enabled => {
                    config.insert(
                        "default_permissions".to_owned(),
                        Value::String(READ_ONLY_NETWORK_PROFILE.to_owned()),
                    );
                    config.insert(
                        format!("permissions.{READ_ONLY_NETWORK_PROFILE}.extends"),
                        Value::String(":read-only".to_owned()),
                    );
                    config.insert(
                        format!("permissions.{READ_ONLY_NETWORK_PROFILE}.network.enabled"),
                        Value::Bool(true),
                    );
                    (
                        CodexPermissionProfile {
                            id: READ_ONLY_NETWORK_PROFILE.to_owned(),
                            extends: Some(":read-only".to_owned()),
                        },
                        CodexMaterializedSandbox::ReadOnly {
                            network_access: true,
                        },
                        true,
                    )
                }
                WorkspaceSandboxMode::ReadOnly => {
                    config.insert(
                        "web_search".to_owned(),
                        Value::String("disabled".to_owned()),
                    );
                    (
                        CodexPermissionProfile {
                            id: ":read-only".to_owned(),
                            extends: None,
                        },
                        CodexMaterializedSandbox::ReadOnly {
                            network_access: false,
                        },
                        true,
                    )
                }
                WorkspaceSandboxMode::WorkspaceWrite => {
                    if context
                        .roots()
                        .iter()
                        .any(|root| root.access() != WorkspaceRootAccess::ReadWrite)
                    {
                        return Err(KitError::unsupported(
                            "workspace-write requires every declared root to be writable",
                        ));
                    }
                    config.insert(
                        "default_permissions".to_owned(),
                        Value::String(WORKSPACE_WRITE_PROFILE.to_owned()),
                    );
                    config.insert(
                        format!("permissions.{WORKSPACE_WRITE_PROFILE}.extends"),
                        Value::String(":workspace".to_owned()),
                    );
                    config.insert(
                        format!("permissions.{WORKSPACE_WRITE_PROFILE}.filesystem.:tmpdir"),
                        Value::String("read".to_owned()),
                    );
                    config.insert(
                        format!("permissions.{WORKSPACE_WRITE_PROFILE}.filesystem.:slash_tmp"),
                        Value::String("read".to_owned()),
                    );
                    config.insert(
                        format!("permissions.{WORKSPACE_WRITE_PROFILE}.network.enabled"),
                        Value::Bool(network_enabled),
                    );
                    if !network_enabled {
                        config.insert(
                            "web_search".to_owned(),
                            Value::String("disabled".to_owned()),
                        );
                    }
                    (
                        CodexPermissionProfile {
                            id: WORKSPACE_WRITE_PROFILE.to_owned(),
                            extends: Some(":workspace".to_owned()),
                        },
                        CodexMaterializedSandbox::WorkspaceWrite {
                            writable_roots: runtime_workspace_roots
                                .iter()
                                .filter(|root| *root != &cwd)
                                .cloned()
                                .collect(),
                            network_access: network_enabled,
                            exclude_tmpdir_env_var: true,
                            exclude_slash_tmp: true,
                        },
                        true,
                    )
                }
                WorkspaceSandboxMode::FullAccess if network_enabled => (
                    CodexPermissionProfile {
                        id: ":danger-full-access".to_owned(),
                        extends: None,
                    },
                    CodexMaterializedSandbox::DangerFullAccess,
                    false,
                ),
                WorkspaceSandboxMode::FullAccess => {
                    return Err(KitError::unsupported(
                        "cannot represent full filesystem access with network disabled",
                    ));
                }
            };
        if has_tools && requires_empty_mcp {
            return Err(KitError::unsupported(
                "managed tools require full filesystem and network access",
            ));
        }
        Ok(Self {
            cwd,
            runtime_workspace_roots,
            permission_profile,
            config,
            expected_sandbox,
            requires_empty_mcp,
        })
    }

    pub(crate) fn requires_empty_mcp(&self) -> bool {
        self.requires_empty_mcp
    }

    pub(crate) fn wire_policy(&self) -> Result<CodexThreadWirePolicy, KitError> {
        let cwd = self
            .cwd
            .to_str()
            .ok_or_else(|| KitError::unsupported("workspace cwd must be representable as UTF-8"))?;
        let runtime_workspace_roots = self
            .runtime_workspace_roots
            .iter()
            .map(|root| {
                root.to_str().map(str::to_owned).ok_or_else(|| {
                    KitError::unsupported("workspace roots must be representable as UTF-8")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        CodexThreadWirePolicy::new(
            cwd.to_owned(),
            runtime_workspace_roots,
            None,
            Some(self.permission_profile.id.clone()),
            self.config.clone(),
        )
    }

    pub(crate) fn verify_materialized(&self, thread: &CodexThread) -> Result<(), KitError> {
        if thread.cwd != self.cwd {
            return Err(KitError::invalid_state(
                "materialized a different workspace cwd",
            ));
        }
        let materialization = thread.materialization.as_ref().ok_or_else(|| {
            KitError::protocol("conversation response omitted its materialized policy")
        })?;
        if materialization.runtime_workspace_roots != self.runtime_workspace_roots {
            return Err(KitError::invalid_state(
                "materialized different runtime workspace roots",
            ));
        }
        if materialization.sandbox != self.expected_sandbox {
            return Err(KitError::invalid_state(
                "materialized a different workspace execution policy",
            ));
        }
        if materialization.active_permission_profile.as_ref() != Some(&self.permission_profile) {
            return Err(KitError::invalid_state(
                "materialized a different workspace execution policy",
            ));
        }
        Ok(())
    }

    pub(crate) fn verify_correlated_creation(&self, thread: &CodexThread) -> Result<(), KitError> {
        if thread.cwd != self.cwd {
            return Err(KitError::invalid_state(
                "conversation creation state has a different workspace cwd",
            ));
        }
        if thread.materialization.is_some() {
            return Err(KitError::invalid_state(
                "conversation creation policy state is contradictory",
            ));
        }
        Ok(())
    }
}

impl CodexThread {
    pub(crate) fn from_start(value: Value) -> Result<Self, KitError> {
        Self::from_materialized_response(value)
    }

    pub(crate) fn from_resume(value: Value) -> Result<Self, KitError> {
        Self::from_materialized_response(value)
    }

    pub(crate) fn from_read(value: Value) -> Result<Self, KitError> {
        let thread = value
            .get("thread")
            .and_then(Value::as_object)
            .ok_or_else(|| KitError::protocol("conversation reload omitted conversation"))?;
        let (id, cwd, thread_source) = Self::binding(
            thread.get("id"),
            thread.get("cwd"),
            thread.get("parentThreadId"),
            thread.get("threadSource"),
        )?;
        Ok(Self {
            id,
            cwd,
            thread_source,
            materialization: None,
        })
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub(crate) fn has_same_durable_binding(&self, other: &Self) -> bool {
        self.id == other.id && self.cwd == other.cwd
    }

    pub(crate) fn require_thread_source(&self, expected: &str) -> Result<(), KitError> {
        if self.thread_source.as_deref() == Some(expected) {
            Ok(())
        } else {
            Err(KitError::invalid_state(
                "conversation creation state contradicts durable state",
            ))
        }
    }

    fn from_materialized_response(value: Value) -> Result<Self, KitError> {
        let object = value
            .as_object()
            .ok_or_else(|| KitError::protocol("conversation creation response is malformed"))?;
        let thread = object
            .get("thread")
            .and_then(Value::as_object)
            .ok_or_else(|| KitError::protocol("conversation creation response omitted state"))?;
        let (id, cwd, thread_source) = Self::binding(
            thread.get("id"),
            object.get("cwd"),
            thread.get("parentThreadId"),
            thread.get("threadSource"),
        )?;
        let runtime_workspace_roots = object
            .get("runtimeWorkspaceRoots")
            .and_then(Value::as_array)
            .ok_or_else(|| KitError::protocol("workspace execution policy omitted its roots"))?
            .iter()
            .map(|root| {
                root.as_str()
                    .map(PathBuf::from)
                    .ok_or_else(|| KitError::protocol("runtime workspace root is malformed"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sandbox = CodexMaterializedSandbox::decode(object.get("sandbox"))?;
        let active_permission_profile =
            CodexPermissionProfile::decode_optional(object.get("activePermissionProfile"))?;
        Ok(Self {
            id,
            cwd,
            thread_source,
            materialization: Some(CodexThreadMaterialization {
                runtime_workspace_roots,
                sandbox,
                active_permission_profile,
            }),
        })
    }

    fn binding(
        id: Option<&Value>,
        cwd: Option<&Value>,
        parent_thread_id: Option<&Value>,
        thread_source: Option<&Value>,
    ) -> Result<(String, PathBuf, Option<String>), KitError> {
        let id = id
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && id.trim() == *id)
            .ok_or_else(|| KitError::protocol("conversation id is malformed"))?;
        let cwd = cwd
            .and_then(Value::as_str)
            .filter(|cwd| !cwd.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| KitError::protocol("conversation cwd is malformed"))?;
        let parent_thread_id = match parent_thread_id {
            Some(Value::Null) => None,
            Some(parent_thread_id) => Some(
                parent_thread_id
                    .as_str()
                    .filter(|id| !id.is_empty() && id.trim() == *id)
                    .map(str::to_owned)
                    .ok_or_else(|| KitError::protocol("parent conversation id is malformed"))?,
            ),
            None => {
                return Err(KitError::protocol(
                    "conversation response omitted its parent identity",
                ));
            }
        };
        if parent_thread_id.is_some() {
            return Err(KitError::invalid_state(
                "conversation attachment contradicts portable ownership",
            ));
        }
        let thread_source = match thread_source {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_str()
                    .filter(|source| !source.is_empty() && source.trim() == *source)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        KitError::protocol("conversation creation state is malformed")
                    })?,
            ),
        };
        Ok((id.to_owned(), cwd, thread_source))
    }
}

impl CodexMaterializedSandbox {
    fn decode(value: Option<&Value>) -> Result<Self, KitError> {
        let object = value
            .and_then(Value::as_object)
            .ok_or_else(|| KitError::protocol("workspace execution policy is malformed"))?;
        match object.get("type").and_then(Value::as_str) {
            Some("dangerFullAccess") => Ok(Self::DangerFullAccess),
            Some("readOnly") => Ok(Self::ReadOnly {
                network_access: Self::defaulted_bool(object, "networkAccess")?,
            }),
            Some("externalSandbox") => {
                let network_access = match object.get("networkAccess") {
                    None => CodexExternalNetworkAccess::Restricted,
                    Some(Value::String(value)) if value == "restricted" => {
                        CodexExternalNetworkAccess::Restricted
                    }
                    Some(Value::String(value)) if value == "enabled" => {
                        CodexExternalNetworkAccess::Enabled
                    }
                    _ => {
                        return Err(KitError::protocol(
                            "external workspace execution policy network policy is malformed",
                        ));
                    }
                };
                Ok(Self::ExternalSandbox { network_access })
            }
            Some("workspaceWrite") => {
                let writable_roots = match object.get("writableRoots") {
                    None => Vec::new(),
                    Some(value) => value
                        .as_array()
                        .ok_or_else(|| KitError::protocol("writable root inventory is malformed"))?
                        .iter()
                        .map(|root| {
                            root.as_str()
                                .map(PathBuf::from)
                                .ok_or_else(|| KitError::protocol("writable root is malformed"))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                };
                Ok(Self::WorkspaceWrite {
                    writable_roots,
                    network_access: Self::defaulted_bool(object, "networkAccess")?,
                    exclude_tmpdir_env_var: Self::defaulted_bool(object, "excludeTmpdirEnvVar")?,
                    exclude_slash_tmp: Self::defaulted_bool(object, "excludeSlashTmp")?,
                })
            }
            Some(_) => Err(KitError::unsupported(
                "workspace execution policy is unsupported",
            )),
            None => Err(KitError::protocol(
                "workspace execution policy omitted its type",
            )),
        }
    }

    fn defaulted_bool(object: &Map<String, Value>, key: &str) -> Result<bool, KitError> {
        match object.get(key) {
            None => Ok(false),
            Some(value) => value.as_bool().ok_or_else(|| {
                KitError::protocol("workspace execution policy boolean is malformed")
            }),
        }
    }
}

impl CodexPermissionProfile {
    fn decode_optional(value: Option<&Value>) -> Result<Option<Self>, KitError> {
        let Some(value) = value else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }
        let object = value
            .as_object()
            .ok_or_else(|| KitError::protocol("active workspace execution policy is malformed"))?;
        let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
            KitError::protocol("active workspace execution policy omitted its id")
        })?;
        let extends = match object.get("extends") {
            None | Some(Value::Null) => None,
            Some(value) => Some(value.as_str().map(str::to_owned).ok_or_else(|| {
                KitError::protocol("workspace execution policy parent is malformed")
            })?),
        };
        Ok(Some(Self {
            id: id.to_owned(),
            extends,
        }))
    }
}

impl CodexThreadList {
    pub(crate) fn decode(value: Value) -> Result<Self, KitError> {
        let object = value
            .as_object()
            .ok_or_else(|| KitError::protocol("conversation inventory response is malformed"))?;
        let data = object
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| KitError::protocol("conversation inventory omitted data"))?;
        CodexPageContract::require_bound(data.len())?;
        let mut entries = Vec::with_capacity(data.len());
        for thread in data {
            let id = thread
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty() && id.trim() == *id)
                .ok_or_else(|| KitError::protocol("listed conversation id is malformed"))?;
            let thread_source = match thread.get("threadSource") {
                None | Some(Value::Null) => None,
                Some(value) => Some(
                    value
                        .as_str()
                        .filter(|source| !source.is_empty() && source.trim() == *source)
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            KitError::protocol("conversation creation inventory is malformed")
                        })?,
                ),
            };
            entries.push(CodexThreadIdentity {
                id: id.to_owned(),
                thread_source,
            });
        }
        Ok(Self {
            entries,
            next_cursor: CodexPageContract::cursor(object)?,
        })
    }

    pub(crate) fn into_parts(self) -> (Vec<CodexThreadIdentity>, Option<String>) {
        (self.entries, self.next_cursor)
    }
}

impl CodexThreadIdentity {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn has_thread_source(&self, expected: &str) -> bool {
        self.thread_source.as_deref() == Some(expected)
    }
}

impl CodexFeatureList {
    pub(crate) fn decode(value: Value) -> Result<Self, KitError> {
        let object = value.as_object().ok_or_else(|| {
            KitError::protocol("workspace execution policy inventory is malformed")
        })?;
        let data = object
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                KitError::protocol("workspace execution policy inventory omitted data")
            })?;
        CodexPageContract::require_bound(data.len())?;
        let mut entries = Vec::with_capacity(data.len());
        for feature in data {
            let name = feature
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    KitError::protocol("workspace execution policy entry is malformed")
                })?;
            let enabled = feature
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or_else(|| {
                    KitError::protocol("workspace execution policy state is malformed")
                })?;
            entries.push((name.to_owned(), enabled));
        }
        Ok(Self {
            entries,
            next_cursor: CodexPageContract::cursor(object)?,
        })
    }

    pub(crate) fn into_parts(self) -> (Vec<(String, bool)>, Option<String>) {
        (self.entries, self.next_cursor)
    }
}

impl CodexMcpStatusList {
    pub(crate) fn decode(value: Value) -> Result<Self, KitError> {
        let object = value.as_object().ok_or_else(|| {
            KitError::protocol("configured tool source status result is malformed")
        })?;
        let data = object
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                KitError::protocol("configured tool source status result omitted data")
            })?;
        CodexPageContract::require_bound(data.len())?;
        Ok(Self {
            empty: data.is_empty(),
            next_cursor: CodexPageContract::cursor(object)?,
        })
    }

    pub(crate) fn into_parts(self) -> (bool, Option<String>) {
        (self.empty, self.next_cursor)
    }
}

impl CodexTerminalList {
    pub(crate) fn decode(value: Value) -> Result<Self, KitError> {
        let object = value
            .as_object()
            .ok_or_else(|| KitError::protocol("background work inventory is malformed"))?;
        let data = object
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| KitError::protocol("background work inventory omitted data"))?;
        CodexPageContract::require_bound(data.len())?;
        Ok(Self {
            empty: data.is_empty(),
            next_cursor: CodexPageContract::cursor(object)?,
        })
    }

    pub(crate) fn into_parts(self) -> (bool, Option<String>) {
        (self.empty, self.next_cursor)
    }
}

impl CodexPageContract {
    fn require_bound(len: usize) -> Result<(), KitError> {
        if len <= CODEX_PAGE_LIMIT {
            Ok(())
        } else {
            Err(KitError::protocol(
                "runtime inventory page exceeds the requested bound",
            ))
        }
    }

    fn cursor(object: &Map<String, Value>) -> Result<Option<String>, KitError> {
        match object.get("nextCursor") {
            None | Some(Value::Null) => Ok(None),
            Some(cursor) => cursor
                .as_str()
                .filter(|cursor| !cursor.is_empty() && cursor.trim() == *cursor)
                .map(|cursor| Some(cursor.to_owned()))
                .ok_or_else(|| KitError::protocol("page cursor is malformed")),
        }
    }
}
