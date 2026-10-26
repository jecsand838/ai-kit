//! Workspace contexts, durable snapshots, revision-checked requests, and live handles.
//!
//! Context paths describe the backend host, which may differ from the client.
//! Construction validates lexical confinement without probing that filesystem.
//! [`WorkspaceBackend`] owns its associated Conversation; close releases runtime
//! ownership while preserving reopenable records. Use [`crate::AgentBackend`]
//! for atomic replacement/deletion, supplying the last committed revision.

use std::ffi::OsString;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use tool_sdk::ToolsetBackend;

use provider_sdk::ProviderBackend;
use storage_sdk::{DatabaseBackend, StateStoreBackend};

use crate::backends::{codex, custom, opencode};
use crate::{
    Conversation, ConversationId, ConversationInfo, IngestKnowledgeRequest, KnowledgePolicy,
    ModelSelector, NewConversation, PersonalitySelector, ReasoningEffort, SdkError, ToolInfo,
    WorkspaceId,
};

/// Filesystem access declared for one workspace root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceRootAccess {
    /// Model-visible actions may read but not write beneath the root.
    ReadOnly,
    /// Model-visible actions may read and write beneath the root when policy permits.
    ReadWrite,
}

/// One normalized absolute backend-host workspace root.
#[derive(Clone, Debug, Eq, PartialEq)]
///
/// ```
/// use agent_sdk::workspace::{WorkspaceRoot, WorkspaceRootAccess};
/// let value = WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?;
/// assert_eq!(value.path(), std::path::Path::new("/workspace"));
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct WorkspaceRoot {
    path: BackendHostPath,
    access: WorkspaceRootAccess,
}

#[derive(Clone)]
struct BackendHostPath {
    original: PathBuf,
    anchor: BackendHostPathAnchor,
    components: Vec<BackendHostPathSegment>,
}

impl PartialEq for BackendHostPath {
    fn eq(&self, other: &Self) -> bool {
        self.anchor == other.anchor && self.components == other.components
    }
}

impl Eq for BackendHostPath {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BackendHostPathAnchor {
    Posix,
    WindowsDrive(u8),
    WindowsUnc {
        server: BackendHostPathSegment,
        share: BackendHostPathSegment,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendHostPathFlavor {
    Posix,
    Windows,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BackendHostPathSegment(Box<[u8]>);

impl fmt::Debug for BackendHostPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("BackendHostPath")
            .field(&self.original)
            .finish()
    }
}

impl BackendHostPathSegment {
    fn new(value: &[u8], label: &str) -> Result<Self, SdkError> {
        if value.is_empty() || value == b"." || value == b".." {
            return Err(SdkError::invalid_input(format!(
                "{label} must already be lexically normalized"
            )));
        }
        Ok(Self(value.into()))
    }
}

impl BackendHostPathAnchor {
    fn is_windows(&self) -> bool {
        !matches!(self, Self::Posix)
    }

    fn flavor(&self) -> BackendHostPathFlavor {
        if self.is_windows() {
            BackendHostPathFlavor::Windows
        } else {
            BackendHostPathFlavor::Posix
        }
    }
}

impl BackendHostPath {
    fn parse_absolute(label: &str, original: PathBuf) -> Result<Self, SdkError> {
        let bytes = original.as_os_str().as_encoded_bytes();
        if bytes.is_empty() || bytes.contains(&0) {
            return Err(SdkError::invalid_input(format!(
                "{label} must be a nonempty absolute backend-host path"
            )));
        }

        let (anchor, components) = if bytes.starts_with(b"\\\\") {
            let mut components = Self::parse_components(&bytes[2..], true, label)?;
            if components.len() < 2 || components[0].0.as_ref() == b"?" {
                return Err(SdkError::invalid_input(format!(
                    "{label} must use a normalized UNC share path"
                )));
            }
            let share = components.remove(1);
            let server = components.remove(0);
            (
                BackendHostPathAnchor::WindowsUnc { server, share },
                components,
            )
        } else if bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\')
        {
            (
                BackendHostPathAnchor::WindowsDrive(bytes[0].to_ascii_lowercase()),
                Self::parse_components(&bytes[3..], true, label)?,
            )
        } else if bytes[0] == b'/' && !bytes.starts_with(b"//") {
            (
                BackendHostPathAnchor::Posix,
                Self::parse_components(&bytes[1..], false, label)?,
            )
        } else {
            return Err(SdkError::invalid_input(format!(
                "{label} must be a nonempty absolute backend-host path"
            )));
        };

        Ok(Self {
            original,
            anchor,
            components,
        })
    }

    fn parse_components(
        bytes: &[u8],
        windows: bool,
        label: &str,
    ) -> Result<Vec<BackendHostPathSegment>, SdkError> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let mut components = Vec::new();
        let mut start = 0;
        for (index, byte) in bytes.iter().copied().enumerate() {
            if byte == b'/' || (windows && byte == b'\\') {
                components.push(BackendHostPathSegment::new(&bytes[start..index], label)?);
                start = index + 1;
            }
        }
        components.push(BackendHostPathSegment::new(&bytes[start..], label)?);
        Ok(components)
    }

    fn validate_portable_relative(path: &Path) -> Result<PathBuf, SdkError> {
        let bytes = path.as_os_str().as_encoded_bytes();
        if bytes.is_empty()
            || bytes.contains(&0)
            || matches!(bytes[0], b'/' | b'\\')
            || (bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
        {
            return Err(SdkError::invalid_input(
                "workspace path must be nonempty and relative",
            ));
        }
        if bytes
            .split(|byte| matches!(*byte, b'/' | b'\\'))
            .any(|component| component.is_empty() || component == b"." || component == b"..")
        {
            return Err(SdkError::invalid_input(
                "workspace path must already be lexically normalized",
            ));
        }
        Ok(path.to_path_buf())
    }

    fn resolve(&self, relative: &Path) -> Result<Self, SdkError> {
        let relative_components = Self::parse_components(
            relative.as_os_str().as_encoded_bytes(),
            self.anchor.is_windows(),
            "workspace path",
        )?;
        let mut raw = OsString::from(self.original.as_os_str());
        let terminated = raw
            .as_encoded_bytes()
            .last()
            .is_some_and(|byte| *byte == b'/' || (self.anchor.is_windows() && *byte == b'\\'));
        if !terminated {
            raw.push(if self.anchor.is_windows() { "\\" } else { "/" });
        }
        raw.push(relative.as_os_str());
        let resolved = Self::parse_absolute("resolved workspace path", PathBuf::from(raw))?;
        let mut expected = self.components.clone();
        expected.extend(relative_components);
        if resolved.anchor != self.anchor || resolved.components != expected {
            return Err(SdkError::invalid_input(
                "workspace path must remain beneath its backend-host cwd",
            ));
        }
        Ok(resolved)
    }

    fn relative_native_file(&self, native_path: &Path) -> Result<PathBuf, SdkError> {
        let native = native_path
            .to_str()
            .filter(|path| !path.is_empty() && !path.contains('\0'))
            .ok_or_else(|| SdkError::invalid_input("native file path must be nonempty UTF-8"))?;
        let windows = self.anchor.is_windows();
        let native = if windows {
            native.replace('\\', "/")
        } else {
            native.to_owned()
        };
        let mut parts = native
            .split('/')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        let root = if windows && native.starts_with("//") {
            if parts.len() < 2 {
                return Err(SdkError::invalid_input("native file path has no UNC share"));
            }
            let root = Self::parse_absolute(
                "native file root",
                PathBuf::from(format!(r"\\{}\{}", parts[0], parts[1])),
            )?;
            parts.drain(..2);
            Some(root)
        } else if native.as_bytes().get(1) == Some(&b':')
            && native.as_bytes()[0].is_ascii_alphabetic()
        {
            if !windows || native.as_bytes().get(2) != Some(&b'/') {
                return Err(SdkError::invalid_input(
                    "native file path has an incompatible drive",
                ));
            }
            let root = Self::parse_absolute("native file root", PathBuf::from(&native[..3]))?;
            parts.remove(0);
            Some(root)
        } else if native.starts_with('/') {
            if windows {
                return Err(SdkError::invalid_input(
                    "native file path requires an explicit Windows root",
                ));
            }
            Some(Self::parse_absolute(
                "native file root",
                PathBuf::from("/"),
            )?)
        } else {
            if native.starts_with('\\') {
                return Err(SdkError::invalid_input(
                    "native file path has an incompatible root",
                ));
            }
            None
        };
        let mut components = match root {
            Some(root) if root.anchor == self.anchor => Vec::new(),
            Some(_) => {
                return Err(SdkError::invalid_input(
                    "native file path has a different root",
                ));
            }
            None => self.components.clone(),
        };
        for part in parts {
            match part {
                "." => {}
                ".." => {
                    if components.pop().is_none() {
                        return Err(SdkError::invalid_input("native file path escapes its root"));
                    }
                }
                part => components.push(BackendHostPathSegment::new(
                    part.as_bytes(),
                    "native file path",
                )?),
            }
        }
        if !components.starts_with(&self.components) || components.len() == self.components.len() {
            return Err(SdkError::invalid_input(
                "native file path does not identify a workspace file",
            ));
        }
        let relative = components[self.components.len()..]
            .iter()
            .map(|part| {
                std::str::from_utf8(&part.0)
                    .map_err(|_| SdkError::invalid_input("native file path is not UTF-8"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join("/");
        Self::validate_portable_relative(Path::new(&relative))
    }

    fn same_flavor(&self, other: &Self) -> bool {
        self.anchor.is_windows() == other.anchor.is_windows()
    }

    fn contains(&self, candidate: &Self) -> bool {
        self.anchor == candidate.anchor && candidate.components.starts_with(&self.components)
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.contains(other) || other.contains(self)
    }

    fn as_path(&self) -> &Path {
        &self.original
    }

    fn into_path_buf(self) -> PathBuf {
        self.original
    }
}

impl WorkspaceRoot {
    /// Validates and constructs a declared backend-host root.
    ///
    /// # Errors
    /// Returns InvalidInput for a nonabsolute path or invalid backend-host path components.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceRoot, WorkspaceRootAccess};
    /// let value = WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?;
    /// assert_eq!(value.path(), std::path::Path::new("/workspace"));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(path: PathBuf, access: WorkspaceRootAccess) -> Result<Self, SdkError> {
        Ok(Self {
            path: BackendHostPath::parse_absolute("workspace root", path)?,
            access,
        })
    }

    pub(crate) fn relative_native_file(&self, path: &Path) -> Result<PathBuf, SdkError> {
        self.path.relative_native_file(path)
    }

    /// Returns the normalized absolute backend-host path.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceRoot, WorkspaceRootAccess};
    /// let value = WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?;
    /// assert_eq!(value.path(), std::path::Path::new("/workspace"));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Returns the declared access level.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceRoot, WorkspaceRootAccess};
    /// let value = WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?;
    /// assert_eq!(value.access(), WorkspaceRootAccess::ReadOnly);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn access(&self) -> WorkspaceRootAccess {
        self.access
    }
}

/// Filesystem confinement selected for every model-visible workspace action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceSandboxMode {
    /// Allows reads only through every installed action path.
    ReadOnly,
    /// Allows writes only beneath roots declared read-write.
    WorkspaceWrite,
    /// Allows the backend's unrestricted filesystem behavior.
    FullAccess,
}

/// Arbitrary network access selected for model-visible workspace actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceNetworkAccess {
    /// Forbids model-visible arbitrary network access.
    Disabled,
    /// Permits model-visible network access.
    Enabled,
}

/// Complete execution policy for one workspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
///
/// ```
/// use agent_sdk::workspace::{WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
/// let value = WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled);
/// assert_eq!(value.sandbox_mode(), WorkspaceSandboxMode::ReadOnly);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct WorkspaceExecutionPolicy {
    sandbox_mode: WorkspaceSandboxMode,
    network_access: WorkspaceNetworkAccess,
}

impl WorkspaceExecutionPolicy {
    /// Constructs an explicit workspace execution policy.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let value = WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled);
    /// assert_eq!(value.sandbox_mode(), WorkspaceSandboxMode::ReadOnly);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(sandbox_mode: WorkspaceSandboxMode, network_access: WorkspaceNetworkAccess) -> Self {
        Self {
            sandbox_mode,
            network_access,
        }
    }

    /// Returns the selected filesystem sandbox mode.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let value = WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled);
    /// assert_eq!(value.sandbox_mode(), WorkspaceSandboxMode::ReadOnly);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn sandbox_mode(&self) -> WorkspaceSandboxMode {
        self.sandbox_mode
    }

    /// Returns the selected network access policy.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let value = WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled);
    /// assert_eq!(value.network_access(), WorkspaceNetworkAccess::Disabled);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn network_access(&self) -> WorkspaceNetworkAccess {
        self.network_access
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Declares the complete portable context for a workspace.
///
/// ```
/// use agent_sdk::workspace::{
///     WorkspaceContext, WorkspaceExecutionPolicy, WorkspaceNetworkAccess,
///     WorkspaceRoot, WorkspaceRootAccess, WorkspaceSandboxMode,
/// };
///
/// let cwd = std::env::current_dir().expect("current directory");
/// let root = WorkspaceRoot::new(cwd.clone(), WorkspaceRootAccess::ReadWrite)?;
/// let policy = WorkspaceExecutionPolicy::new(
///     WorkspaceSandboxMode::FullAccess,
///     WorkspaceNetworkAccess::Enabled,
/// );
/// let context = WorkspaceContext::new(cwd.clone(), vec![root], policy)?;
/// assert_eq!(context.cwd(), cwd);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct WorkspaceContext {
    cwd: BackendHostPath,
    roots: Vec<WorkspaceRoot>,
    execution_policy: WorkspaceExecutionPolicy,
}

impl WorkspaceContext {
    /// Validates and constructs a complete passive workspace context.
    ///
    /// # Errors
    /// Returns InvalidInput for invalid absolute paths, mixed path flavors, no roots, contradictory overlapping access, a cwd outside the roots, or root access inconsistent with the sandbox policy.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// assert_eq!(context.roots().len(), 1);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(
        cwd: PathBuf,
        roots: Vec<WorkspaceRoot>,
        execution_policy: WorkspaceExecutionPolicy,
    ) -> Result<Self, SdkError> {
        let cwd = BackendHostPath::parse_absolute("workspace cwd", cwd)?;
        if roots.is_empty() {
            return Err(SdkError::invalid_input(
                "workspace context requires at least one root",
            ));
        }

        let mut deduplicated = Vec::<WorkspaceRoot>::new();
        for root in roots {
            if !cwd.same_flavor(&root.path) {
                return Err(SdkError::invalid_input(
                    "workspace cwd and roots must use one backend-host path flavor",
                ));
            }
            if let Some(existing) = deduplicated
                .iter()
                .find(|existing| existing.path == root.path)
            {
                if existing.access != root.access {
                    return Err(SdkError::invalid_input(
                        "duplicate workspace roots must not contradict access",
                    ));
                }
                continue;
            }
            if deduplicated.iter().any(|existing| {
                existing.path.overlaps(&root.path) && existing.access != root.access
            }) {
                return Err(SdkError::invalid_input(
                    "overlapping workspace roots must not contradict access",
                ));
            }
            deduplicated.push(root);
        }

        let containing = deduplicated
            .iter()
            .filter(|root| root.path.contains(&cwd))
            .collect::<Vec<_>>();
        if containing.is_empty() {
            return Err(SdkError::invalid_input(
                "workspace cwd must be contained by a declared root",
            ));
        }

        match execution_policy.sandbox_mode {
            WorkspaceSandboxMode::ReadOnly
                if deduplicated
                    .iter()
                    .any(|root| root.access != WorkspaceRootAccess::ReadOnly) =>
            {
                return Err(SdkError::invalid_input(
                    "read-only workspace policy requires every root to be read-only",
                ));
            }
            WorkspaceSandboxMode::WorkspaceWrite
                if containing
                    .iter()
                    .any(|root| root.access != WorkspaceRootAccess::ReadWrite) =>
            {
                return Err(SdkError::invalid_input(
                    "workspace-write policy requires the cwd root to be read-write",
                ));
            }
            WorkspaceSandboxMode::FullAccess
                if deduplicated
                    .iter()
                    .any(|root| root.access != WorkspaceRootAccess::ReadWrite) =>
            {
                return Err(SdkError::invalid_input(
                    "full-access workspace policy requires every root to be read-write",
                ));
            }
            _ => {}
        }

        Ok(Self {
            cwd,
            roots: deduplicated,
            execution_policy,
        })
    }

    /// Returns the absolute backend-host cwd.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// assert_eq!(context.cwd(), std::path::Path::new("/workspace"));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn cwd(&self) -> &Path {
        self.cwd.as_path()
    }

    /// Returns the deduplicated declared workspace roots.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// assert_eq!(context.roots()[0].access(), WorkspaceRootAccess::ReadOnly);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn roots(&self) -> &[WorkspaceRoot] {
        &self.roots
    }

    /// Returns the complete execution policy.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// assert_eq!(context.execution_policy().network_access(), WorkspaceNetworkAccess::Disabled);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn execution_policy(&self) -> &WorkspaceExecutionPolicy {
        &self.execution_policy
    }

    pub(crate) fn normalized_relative(path: &Path) -> Result<PathBuf, SdkError> {
        BackendHostPath::validate_portable_relative(path)
    }

    pub(crate) fn path_flavor(&self) -> BackendHostPathFlavor {
        self.cwd.anchor.flavor()
    }

    pub(crate) fn resolve(&self, path: &Path) -> Result<(PathBuf, PathBuf), SdkError> {
        let relative = Self::normalized_relative(path)?;
        let absolute = self.cwd.resolve(&relative)?;
        if !self.roots.iter().any(|root| root.path.contains(&absolute)) {
            return Err(SdkError::invalid_input(
                "workspace path must remain beneath a declared root",
            ));
        }
        Ok((relative, absolute.into_path_buf()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Supplies optional model, reasoning, and knowledge defaults for a workspace.
///
/// ```
/// use agent_sdk::workspace::WorkspaceDefaults;
/// let value = WorkspaceDefaults::new(None, None, None)?;
/// assert_eq!(value.model(), None);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct WorkspaceDefaults {
    model: Option<ModelSelector>,
    personality: Option<PersonalitySelector>,
    reasoning_effort: Option<ReasoningEffort>,
    knowledge_policy: Option<KnowledgePolicy>,
}

impl WorkspaceDefaults {
    /// Constructs validated workspace defaults.
    ///
    /// # Errors
    /// Returns InvalidInput for an invalid explicit knowledge scope selection. Model and personality availability is checked by the receiving backend.
    ///
    /// ```
    /// use agent_sdk::workspace::WorkspaceDefaults;
    /// let value = WorkspaceDefaults::new(None, None, None)?;
    /// assert_eq!(value.model(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(
        model: Option<ModelSelector>,
        reasoning_effort: Option<ReasoningEffort>,
        knowledge_policy: Option<KnowledgePolicy>,
    ) -> Result<Self, SdkError> {
        if let Some(policy) = &knowledge_policy {
            policy.validate()?;
        }
        Ok(Self {
            model,
            personality: None,
            reasoning_effort,
            knowledge_policy,
        })
    }

    /// Constructs defaults with every optional value absent.
    ///
    /// ```
    /// use agent_sdk::workspace::WorkspaceDefaults;
    /// let value = WorkspaceDefaults::new(None, None, None)?;
    /// let value = WorkspaceDefaults::empty();
    /// assert_eq!(value.knowledge_policy(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn empty() -> Self {
        Self {
            model: None,
            personality: None,
            reasoning_effort: None,
            knowledge_policy: None,
        }
    }

    /// Returns the optional model default.
    ///
    /// ```
    /// use agent_sdk::workspace::WorkspaceDefaults;
    /// let value = WorkspaceDefaults::new(None, None, None)?;
    /// assert_eq!(value.model(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn model(&self) -> Option<&ModelSelector> {
        self.model.as_ref()
    }

    /// Sets the optional personality default.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::WorkspaceDefaults;
    /// let value = WorkspaceDefaults::new(None, None, None)?;
    /// let value = value.with_personality(agent_sdk::model::PersonalitySelector::new("concise")?);
    /// assert_eq!(value.personality().unwrap().as_str(), "concise");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn with_personality(mut self, personality: PersonalitySelector) -> Self {
        self.personality = Some(personality);
        self
    }

    /// Returns the optional personality default.
    #[must_use]
    ///
    /// ```
    /// use agent_sdk::workspace::WorkspaceDefaults;
    /// let value = WorkspaceDefaults::new(None, None, None)?;
    /// assert_eq!(value.personality(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn personality(&self) -> Option<&PersonalitySelector> {
        self.personality.as_ref()
    }

    /// Returns the optional reasoning-effort default.
    ///
    /// ```
    /// use agent_sdk::workspace::WorkspaceDefaults;
    /// let value = WorkspaceDefaults::new(None, None, None)?;
    /// assert_eq!(value.reasoning_effort(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn reasoning_effort(&self) -> Option<&ReasoningEffort> {
        self.reasoning_effort.as_ref()
    }

    /// Returns the optional managed-knowledge policy default.
    ///
    /// ```
    /// use agent_sdk::workspace::WorkspaceDefaults;
    /// let value = WorkspaceDefaults::new(None, None, None)?;
    /// assert_eq!(value.knowledge_policy(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn knowledge_policy(&self) -> Option<&KnowledgePolicy> {
        self.knowledge_policy.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Describes one workspace to create.
///
/// ```
/// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
/// let context = WorkspaceContext::new("/workspace".into(),
///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
/// use agent_sdk::workspace::{NewWorkspace, WorkspaceDefaults};
/// let value = NewWorkspace::new("review", context, WorkspaceDefaults::empty())?;
/// assert_eq!(value.name(), "review");
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct NewWorkspace {
    name: String,
    context: WorkspaceContext,
    defaults: WorkspaceDefaults,
}

impl NewWorkspace {
    /// Validates and constructs a new-workspace request.
    ///
    /// # Errors
    /// Returns InvalidInput for an empty or padded name.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{NewWorkspace, WorkspaceDefaults};
    /// let value = NewWorkspace::new("review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.name(), "review");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(
        name: impl Into<String>,
        context: WorkspaceContext,
        defaults: WorkspaceDefaults,
    ) -> Result<Self, SdkError> {
        let name = name.into();
        Self::validate_name(&name)?;
        Ok(Self {
            name,
            context,
            defaults,
        })
    }

    /// Returns the workspace name.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{NewWorkspace, WorkspaceDefaults};
    /// let value = NewWorkspace::new("review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.name(), "review");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the complete workspace context.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{NewWorkspace, WorkspaceDefaults};
    /// let value = NewWorkspace::new("review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.context().cwd(), std::path::Path::new("/workspace"));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn context(&self) -> &WorkspaceContext {
        &self.context
    }

    /// Returns the complete workspace defaults.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{NewWorkspace, WorkspaceDefaults};
    /// let value = NewWorkspace::new("review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.defaults().model(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn defaults(&self) -> &WorkspaceDefaults {
        &self.defaults
    }

    fn validate_name(name: &str) -> Result<(), SdkError> {
        if name.is_empty() || name.trim() != name {
            return Err(SdkError::invalid_input(
                "workspace name must be nonempty and unpadded",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Contains one committed workspace snapshot.
///
/// ```
/// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
/// let context = WorkspaceContext::new("/workspace".into(),
///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
/// use agent_sdk::workspace::{WorkspaceInfo, WorkspaceDefaults};
/// let value = WorkspaceInfo::new("workspace_00000000000000000000000000000000".parse()?, "review".into(), context, WorkspaceDefaults::empty(), 1)?;
/// assert_eq!(value.id().as_str(), "workspace_00000000000000000000000000000000");
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct WorkspaceInfo {
    id: WorkspaceId,
    name: String,
    context: WorkspaceContext,
    defaults: WorkspaceDefaults,
    revision: u64,
}

impl WorkspaceInfo {
    /// Validates and constructs one committed workspace snapshot.
    ///
    /// The name must be nonempty and unpadded, and `revision` must be
    /// nonzero.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for an invalid name or
    /// [`crate::error::ErrorKind::InvalidState`] for a zero revision.
    ///
    /// # Examples
    ///
    /// ```
    /// use agent_sdk::{
    ///     identity::WorkspaceId,
    ///     workspace::{
    ///     WorkspaceContext, WorkspaceDefaults, WorkspaceExecutionPolicy,
    ///     WorkspaceInfo, WorkspaceNetworkAccess, WorkspaceRoot,
    ///     WorkspaceRootAccess, WorkspaceSandboxMode,
    ///     },
    /// };
    ///
    /// let cwd = std::env::current_dir()?;
    /// let context = WorkspaceContext::new(
    ///     cwd.clone(),
    ///     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadWrite)?],
    ///     WorkspaceExecutionPolicy::new(
    ///         WorkspaceSandboxMode::FullAccess,
    ///         WorkspaceNetworkAccess::Enabled,
    ///     ),
    /// )?;
    /// let id: WorkspaceId =
    ///     "workspace_00000000000000000000000000000000".parse()?;
    /// let info = WorkspaceInfo::new(
    ///     id,
    ///     "release".to_owned(),
    ///     context,
    ///     WorkspaceDefaults::empty(),
    ///     1,
    /// )?;
    /// assert_eq!(info.revision(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(
        id: WorkspaceId,
        name: String,
        context: WorkspaceContext,
        defaults: WorkspaceDefaults,
        revision: u64,
    ) -> Result<Self, SdkError> {
        NewWorkspace::validate_name(&name)?;
        if revision == 0 {
            return Err(SdkError::invalid_state(
                "workspace revision must be nonzero",
            ));
        }
        Ok(Self {
            id,
            name,
            context,
            defaults,
            revision,
        })
    }

    /// Returns the portable workspace identity.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{WorkspaceInfo, WorkspaceDefaults};
    /// let value = WorkspaceInfo::new("workspace_00000000000000000000000000000000".parse()?, "review".into(), context, WorkspaceDefaults::empty(), 1)?;
    /// assert_eq!(value.id().as_str(), "workspace_00000000000000000000000000000000");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn id(&self) -> &WorkspaceId {
        &self.id
    }

    /// Returns the committed workspace name.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{WorkspaceInfo, WorkspaceDefaults};
    /// let value = WorkspaceInfo::new("workspace_00000000000000000000000000000000".parse()?, "review".into(), context, WorkspaceDefaults::empty(), 1)?;
    /// assert_eq!(value.name(), "review");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the committed workspace context.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{WorkspaceInfo, WorkspaceDefaults};
    /// let value = WorkspaceInfo::new("workspace_00000000000000000000000000000000".parse()?, "review".into(), context, WorkspaceDefaults::empty(), 1)?;
    /// assert_eq!(value.context().cwd(), std::path::Path::new("/workspace"));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn context(&self) -> &WorkspaceContext {
        &self.context
    }

    /// Returns the committed workspace defaults.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{WorkspaceInfo, WorkspaceDefaults};
    /// let value = WorkspaceInfo::new("workspace_00000000000000000000000000000000".parse()?, "review".into(), context, WorkspaceDefaults::empty(), 1)?;
    /// assert_eq!(value.defaults().model(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn defaults(&self) -> &WorkspaceDefaults {
        &self.defaults
    }

    /// Returns the nonzero committed revision.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{WorkspaceInfo, WorkspaceDefaults};
    /// let value = WorkspaceInfo::new("workspace_00000000000000000000000000000000".parse()?, "review".into(), context, WorkspaceDefaults::empty(), 1)?;
    /// assert_eq!(value.revision(), 1);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn revision(&self) -> u64 {
        self.revision
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Proposes a revision-checked complete workspace replacement.
///
/// ```
/// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
/// let context = WorkspaceContext::new("/workspace".into(),
///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
/// use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceDefaults};
/// let value = UpdateWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1, "review", context, WorkspaceDefaults::empty())?;
/// assert_eq!(value.expected_revision(), 1);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct UpdateWorkspaceRequest {
    id: WorkspaceId,
    expected_revision: u64,
    name: String,
    context: WorkspaceContext,
    defaults: WorkspaceDefaults,
}

impl UpdateWorkspaceRequest {
    /// Validates and constructs a workspace update request.
    ///
    /// # Errors
    /// Returns InvalidInput for an empty/padded name or zero expected revision. Constructing a request does not reserve or change that revision.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceDefaults};
    /// let value = UpdateWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1, "review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.expected_revision(), 1);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(
        id: WorkspaceId,
        expected_revision: u64,
        name: impl Into<String>,
        context: WorkspaceContext,
        defaults: WorkspaceDefaults,
    ) -> Result<Self, SdkError> {
        let name = name.into();
        NewWorkspace::validate_name(&name)?;
        if expected_revision == 0 {
            return Err(SdkError::invalid_input(
                "expected workspace revision must be nonzero",
            ));
        }
        Ok(Self {
            id,
            expected_revision,
            name,
            context,
            defaults,
        })
    }

    /// Returns the workspace identity to update.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceDefaults};
    /// let value = UpdateWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1, "review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.id().as_str(), "workspace_00000000000000000000000000000000");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn id(&self) -> &WorkspaceId {
        &self.id
    }

    /// Returns the revision that must still be current.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceDefaults};
    /// let value = UpdateWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1, "review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.expected_revision(), 1);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    /// Returns the proposed replacement name.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceDefaults};
    /// let value = UpdateWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1, "review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.name(), "review");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the proposed replacement context.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceDefaults};
    /// let value = UpdateWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1, "review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.context().cwd(), std::path::Path::new("/workspace"));
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn context(&self) -> &WorkspaceContext {
        &self.context
    }

    /// Returns the proposed replacement defaults.
    ///
    /// ```
    /// use agent_sdk::workspace::{WorkspaceContext, WorkspaceRoot, WorkspaceRootAccess,
    ///     WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// let context = WorkspaceContext::new("/workspace".into(),
    ///     vec![WorkspaceRoot::new("/workspace".into(), WorkspaceRootAccess::ReadOnly)?],
    ///     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// use agent_sdk::workspace::{UpdateWorkspaceRequest, WorkspaceDefaults};
    /// let value = UpdateWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1, "review", context, WorkspaceDefaults::empty())?;
    /// assert_eq!(value.defaults().model(), None);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn defaults(&self) -> &WorkspaceDefaults {
        &self.defaults
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Reports advisory readiness for a workspace update.
pub enum WorkspaceUpdateReadiness {
    /// The current snapshot has no known blocker.
    Ready,
    /// Current admitted or descendant work blocks mutation.
    Busy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Requests revision-checked deletion of one quiescent workspace.
///
/// ```
/// use agent_sdk::workspace::DeleteWorkspaceRequest;
/// let value = DeleteWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1)?;
/// assert_eq!(value.expected_revision(), 1);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
pub struct DeleteWorkspaceRequest {
    id: WorkspaceId,
    expected_revision: u64,
}

impl DeleteWorkspaceRequest {
    /// Validates and constructs a workspace deletion request.
    ///
    /// # Errors
    /// Returns InvalidInput for a zero expected revision. Actual deletion also requires current revision and quiescence.
    ///
    /// ```
    /// use agent_sdk::workspace::DeleteWorkspaceRequest;
    /// let value = DeleteWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1)?;
    /// assert_eq!(value.expected_revision(), 1);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn new(id: WorkspaceId, expected_revision: u64) -> Result<Self, SdkError> {
        if expected_revision == 0 {
            return Err(SdkError::invalid_input(
                "expected workspace revision must be nonzero",
            ));
        }
        Ok(Self {
            id,
            expected_revision,
        })
    }

    /// Returns the workspace identity to delete.
    ///
    /// ```
    /// use agent_sdk::workspace::DeleteWorkspaceRequest;
    /// let value = DeleteWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1)?;
    /// assert_eq!(value.id().as_str(), "workspace_00000000000000000000000000000000");
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn id(&self) -> &WorkspaceId {
        &self.id
    }

    /// Returns the revision that must still be current.
    ///
    /// ```
    /// use agent_sdk::workspace::DeleteWorkspaceRequest;
    /// let value = DeleteWorkspaceRequest::new("workspace_00000000000000000000000000000000".parse()?, 1)?;
    /// assert_eq!(value.expected_revision(), 1);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn expected_revision(&self) -> u64 {
        self.expected_revision
    }
}

/// Public implementation contract for one live portable workspace.
///
/// A caller agent returns its own concrete workspace implementation through
/// [`AgentBackend::Workspace`](crate::AgentBackend::Workspace), and this workspace
/// returns its own concrete [`Conversation`](Self::Conversation) implementation.
/// Runtime composition can instead use a caller-owned exhaustive enum hierarchy.
/// Closing consumes the implementation, seals new work, and invalidates live
/// conversations after accepted cleanup settles. Drop alone carries no
/// asynchronous cleanup guarantee.
///
/// # Examples
///
/// This local Custom agent uses in-memory Lance and sends no network requests.
///
/// ```
/// # use std::{num::NonZeroUsize, time::Duration};
/// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
/// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
/// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
/// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
/// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
/// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
/// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
/// # let provider = Provider::from_config(ProviderConfig::new(
/// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
/// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
/// # let outcome = async {
/// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
/// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
/// # let cwd = std::env::current_dir()?;
/// # let context = WorkspaceContext::new(cwd.clone(),
/// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
/// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
/// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
/// assert!(workspace.list_conversations().await?.is_empty());
/// assert_eq!(workspace.info().await?.id(), &workspace.id());
/// workspace.close().await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # }.await;
/// # let cleanup = agent.shutdown().await;
/// # outcome?;
/// # cleanup?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait WorkspaceBackend: Send + Sync + 'static {
    /// Concrete conversation implementation produced by this workspace.
    type Conversation: crate::ConversationBackend;

    /// Returns the durable portable workspace identity.
    ///
    /// This is the durable workspace identity. A handle also carries a runtime
    /// revision binding, so a stable id does not make an old handle current.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// let id = workspace.id();
    /// assert_eq!(workspace.info().await?.id(), &id);
    /// # workspace.close().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn id(&self) -> WorkspaceId;

    /// Reads the current committed workspace information.
    ///
    /// The result contains the complete committed context/defaults and revision.
    /// It does not refresh this handle after a concurrent revision change.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] for a closed parent/workspace, stale revision,
    /// state-read failure, inconsistent durable state, or deadline expiry.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// let info = workspace.info().await?;
    /// assert_eq!(info.name(), "review");
    /// assert_eq!(info.revision(), 1);
    /// # workspace.close().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn info(&self) -> impl Future<Output = Result<WorkspaceInfo, SdkError>> + Send + '_;

    /// Creates and publishes one conversation under this workspace.
    ///
    /// The returned associated child is published only after resolved defaults,
    /// initial knowledge, backend attachment, and durable ownership agree.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] for closed or stale runtime, busy admission, invalid
    /// input or unsupported policy/integrations, and dependency/deadline failures.
    /// Accepted native setup remains durably attached or recovery-owned on failure.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// use agent_sdk::ConversationBackend;
    /// use agent_sdk::conversation::NewConversation;
    /// let conversation = workspace.new_conversation(NewConversation::new(
    ///     Some("Review".into()), None, Vec::new(),
    /// )?).await?;
    /// assert_eq!(conversation.info().await?.title(), Some("Review"));
    /// // Parent close preserves this quiescent durable conversation for reopen.
    /// workspace.close().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn new_conversation(
        &self,
        request: NewConversation,
    ) -> impl Future<Output = Result<Self::Conversation, SdkError>> + Send + '_;

    /// Lists live or reopenable conversations in deterministic identity order.
    ///
    /// Inventory comes from ownership-valid durable records; it is not limited
    /// to handles currently open in this process. Terminally closed conversations
    /// are excluded. Listing does not claim live handles.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] for closed or stale runtime, state/dependency read
    /// failures, inconsistent inventory, or deadline expiry.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// assert!(workspace.list_conversations().await?.is_empty());
    /// # workspace.close().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn list_conversations(
        &self,
    ) -> impl Future<Output = Result<Vec<ConversationInfo>, SdkError>> + Send + '_;

    /// Reads committed information for one conversation identity.
    ///
    /// This reads committed metadata without claiming a live child handle.
    /// Missing and foreign identities do not reveal a foreign owner.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorKind::NotFound`] for an unavailable or foreign
    /// conversation, or the owning closed/stale runtime, state-read, integrity, or
    /// deadline error.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// use agent_sdk::ConversationBackend;
    /// use agent_sdk::conversation::NewConversation;
    /// let conversation = workspace.new_conversation(NewConversation::new(
    ///     Some("Review".into()), None, Vec::new(),
    /// )?).await?;
    /// let info = workspace.conversation_info(conversation.id()).await?;
    /// assert_eq!(info.id(), &conversation.id());
    /// assert_eq!(info.title(), Some("Review"));
    /// # workspace.close().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn conversation_info(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<ConversationInfo, SdkError>> + Send + '_;

    /// Opens the current live handle for one conversation identity.
    ///
    /// A current conversation has at most one live public handle; repeated lookup
    /// is busy until its claim is released. Reopen verifies the current workspace
    /// revision, defaults, attachments, tools, knowledge, and recovery state. It
    /// does not create a replacement session for a missing identity.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] for a missing/foreign or terminal conversation, an
    /// existing live claim, closed or stale runtime, incompatible reopen state,
    /// or dependency/deadline failure.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// use agent_sdk::ConversationBackend;
    /// use agent_sdk::conversation::NewConversation;
    /// let conversation = workspace.new_conversation(NewConversation::new(None, None, Vec::new())?).await?;
    /// let id = conversation.id();
    /// let workspace_id = workspace.id();
    /// workspace.close().await?;
    /// let workspace = agent.get_workspace(workspace_id).await?;
    /// let reopened = workspace.get_conversation(id.clone()).await?;
    /// assert_eq!(reopened.id(), id);
    /// workspace.close().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn get_conversation(
        &self,
        id: ConversationId,
    ) -> impl Future<Output = Result<Self::Conversation, SdkError>> + Send + '_;

    /// Lists agent-sdk-managed tools installed for this workspace.
    ///
    /// Returns the complete source-neutral catalog the Agent owns and can expose
    /// in this workspace, subject to its execution policy. An unavailable source
    /// must not be silently omitted from a purportedly complete result.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] for closed or stale runtime, unsupported or incomplete
    /// catalog exposure, malformed source data, or dependency/deadline failure.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// // This Agent was constructed without Toolsets.
    /// assert!(workspace.list_tools().await?.is_empty());
    /// # workspace.close().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn list_tools(&self) -> impl Future<Output = Result<Vec<ToolInfo>, SdkError>> + Send + '_;

    /// Atomically replaces managed knowledge from this workspace.
    ///
    /// An omitted scope selects this Workspace. Explicit Agent or Database scope
    /// is validated under the shared knowledge rules; Conversation scope is not
    /// accepted here. Empty sources are a no-op after scope validation, not deletion.
    /// Updating workspace context/defaults does not move sources. See [`crate::knowledge`]
    /// for local ingestion and policy examples.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] for invalid sources or scope, missing/incompatible
    /// knowledge integrations, closed or stale runtime, busy admission, or
    /// dependency/deadline failure. Unsupported semantics fail explicitly.
    ///
    /// # Examples
    ///
    /// This example requires a live Workspace under an open Agent, with configured
    /// backend and storage integrations. It is compiled without starting services.
    ///
    /// ```no_run
    /// use agent_sdk::{WorkspaceBackend, SdkError};
    /// use agent_sdk::knowledge::IngestKnowledgeRequest;
    ///
    /// async fn replace_sources<W: WorkspaceBackend>(workspace: &W, request: IngestKnowledgeRequest)
    ///     -> Result<(), SdkError>
    /// {
    ///     workspace.ingest_knowledge(request).await
    /// }
    /// ```
    fn ingest_knowledge(
        &self,
        request: IngestKnowledgeRequest,
    ) -> impl Future<Output = Result<(), SdkError>> + Send + '_;

    /// Consumes the implementation, seals its runtime, and settles accepted work.
    ///
    /// Close cascade-invalidates live descendants after accepted work and
    /// interactions settle. Quiescent durable conversations, notifications, and
    /// knowledge remain for reopen. This does not terminally close conversations
    /// or acknowledge caller events. Drop alone provides no async cleanup guarantee.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError`] when an owned cleanup responsibility fails or cannot
    /// settle within the lifecycle deadline. The consumed handle is not restored;
    /// durable recovery owns later reconciliation and supplementary failures do
    /// not replace the primary error.
    ///
    /// # Examples
    ///
    /// This local Custom agent uses in-memory Lance and sends no network requests.
    ///
    /// ```
    /// # use std::{num::NonZeroUsize, time::Duration};
    /// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
    /// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
    /// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
    /// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
    /// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
    /// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    /// # runtime.block_on(async {
    /// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
    /// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
    /// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
    /// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
    /// # let provider = Provider::from_config(ProviderConfig::new(
    /// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
    /// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
    /// # let outcome = async {
    /// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
    /// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
    /// # let cwd = std::env::current_dir()?;
    /// # let context = WorkspaceContext::new(cwd.clone(),
    /// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
    /// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
    /// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
    /// let id = workspace.id();
    /// workspace.close().await?;
    /// // Runtime closure does not delete the durable workspace.
    /// assert_eq!(agent.workspace_info(id).await?.name(), "review");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # }.await;
    /// # let cleanup = agent.shutdown().await;
    /// # outcome?;
    /// # cleanup?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn close(self) -> impl Future<Output = Result<(), SdkError>> + Send
    where
        Self: Sized;
}

/// Owns one live workspace runtime and its conversation handles.
///
/// Obtain this unique, non-Clone handle from [`crate::AgentBackend`]. Import
/// [`WorkspaceBackend`] for behavior. A committed workspace update invalidates
/// prior-revision handles; reopen by typed id instead of expecting in-place
/// refresh. Explicit close settles the runtime while preserving quiescent
/// durable children for later reopen.
///
/// # Examples
///
/// This local Custom agent uses in-memory Lance and sends no network requests.
///
/// ```
/// # use std::{num::NonZeroUsize, time::Duration};
/// # use agent_sdk::{AgentBackend, AgentBuilder, AgentStorage, WorkspaceBackend};
/// # use agent_sdk::config::{AgentConfig, CustomAgentConfig, CustomLoopLimits, CustomToolApproval};
/// # use agent_sdk::{model::ModelSelector, tool::ToolSelection};
/// # use provider_sdk::{Provider, ProviderConfig, config::{OllamaConfig, ProviderKind, TransportConfig}};
/// # use storage_sdk::{StateStore, state::config::{LanceStateStoreConfig, StateStoreConfig}};
/// # let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
/// # runtime.block_on(async {
/// # let store = StateStore::connect(StateStoreConfig::Lance(LanceStateStoreConfig::memory())).await?;
/// # let custom = CustomAgentConfig::new(ModelSelector::new("exact-model")?, ToolSelection::None,
/// #     CustomLoopLimits::new(4, 0, 1)?, CustomToolApproval::Deny)?;
/// # let config = AgentConfig::new(custom.into(), Duration::from_secs(10), NonZeroUsize::MIN)?;
/// # let provider = Provider::from_config(ProviderConfig::new(
/// #     ProviderKind::Ollama(OllamaConfig::local()), TransportConfig::default()))?;
/// # let agent = AgentBuilder::create(config, AgentStorage::new(store)).with_provider(provider).build().await?;
/// # let outcome = async {
/// # use agent_sdk::workspace::{NewWorkspace, WorkspaceContext, WorkspaceDefaults,
/// #     WorkspaceRoot, WorkspaceRootAccess, WorkspaceExecutionPolicy, WorkspaceSandboxMode, WorkspaceNetworkAccess};
/// # let cwd = std::env::current_dir()?;
/// # let context = WorkspaceContext::new(cwd.clone(),
/// #     vec![WorkspaceRoot::new(cwd, WorkspaceRootAccess::ReadOnly)?],
/// #     WorkspaceExecutionPolicy::new(WorkspaceSandboxMode::ReadOnly, WorkspaceNetworkAccess::Disabled))?;
/// # let workspace = agent.new_workspace(NewWorkspace::new("review", context, WorkspaceDefaults::empty())?).await?;
/// let id = workspace.id();
/// assert_eq!(workspace.info().await?.revision(), 1);
/// workspace.close().await?;
/// let reopened = agent.get_workspace(id.clone()).await?;
/// assert_eq!(reopened.id(), id);
/// reopened.close().await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # }.await;
/// # let cleanup = agent.shutdown().await;
/// # outcome?;
/// # cleanup?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Workspace<
    S: StateStoreBackend = storage_sdk::StateStore,
    D: DatabaseBackend = storage_sdk::Database,
    P: ProviderBackend = provider_sdk::Provider,
    T: ToolsetBackend = tool_sdk::Toolset,
> {
    inner: WorkspaceInner<S, D, P, T>,
}

enum WorkspaceInner<S: StateStoreBackend, D: DatabaseBackend, P: ProviderBackend, T: ToolsetBackend>
{
    Codex(codex::workspace::CodexWorkspace<S, D, T>),
    OpenCode(opencode::workspace::OpenCodeWorkspace<S, D>),
    Custom(custom::CustomWorkspace<S, D, P, T>),
}

impl<S, D, P, T> Workspace<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    pub(crate) fn codex(backend: codex::workspace::CodexWorkspace<S, D, T>) -> Self {
        Self {
            inner: WorkspaceInner::Codex(backend),
        }
    }

    pub(crate) fn opencode(backend: opencode::workspace::OpenCodeWorkspace<S, D>) -> Self {
        Self {
            inner: WorkspaceInner::OpenCode(backend),
        }
    }

    pub(crate) fn custom(backend: custom::CustomWorkspace<S, D, P, T>) -> Self {
        Self {
            inner: WorkspaceInner::Custom(backend),
        }
    }
}

impl<S, D, P, T> WorkspaceBackend for WorkspaceInner<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    type Conversation = Conversation<S, D, P, T>;

    fn id(&self) -> WorkspaceId {
        match self {
            WorkspaceInner::Codex(backend) => backend.id(),
            WorkspaceInner::OpenCode(backend) => backend.id(),
            WorkspaceInner::Custom(backend) => backend.id(),
        }
    }

    async fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => backend.info().await,
            WorkspaceInner::OpenCode(backend) => backend.info().await,
            WorkspaceInner::Custom(backend) => backend.info().await,
        }
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<Conversation<S, D, P, T>, SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => backend
                .new_conversation(request)
                .await
                .map(Conversation::codex),
            WorkspaceInner::OpenCode(backend) => backend
                .new_conversation(request)
                .await
                .map(Conversation::opencode),
            WorkspaceInner::Custom(backend) => backend
                .new_conversation(request)
                .await
                .map(Conversation::custom),
        }
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => backend.list_conversations().await,
            WorkspaceInner::OpenCode(backend) => backend.list_conversations().await,
            WorkspaceInner::Custom(backend) => backend.list_conversations().await,
        }
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => backend.conversation_info(id).await,
            WorkspaceInner::OpenCode(backend) => backend.conversation_info(id).await,
            WorkspaceInner::Custom(backend) => backend.conversation_info(id).await,
        }
    }

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<Conversation<S, D, P, T>, SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => {
                backend.get_conversation(id).await.map(Conversation::codex)
            }
            WorkspaceInner::OpenCode(backend) => backend
                .get_conversation(id)
                .await
                .map(Conversation::opencode),
            WorkspaceInner::Custom(backend) => {
                backend.get_conversation(id).await.map(Conversation::custom)
            }
        }
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => backend.list_tools().await,
            WorkspaceInner::OpenCode(backend) => backend.list_tools().await,
            WorkspaceInner::Custom(backend) => backend.list_tools().await,
        }
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => backend.ingest_knowledge(request).await,
            WorkspaceInner::OpenCode(backend) => backend.ingest_knowledge(request).await,
            WorkspaceInner::Custom(backend) => backend.ingest_knowledge(request).await,
        }
    }

    async fn close(self) -> Result<(), SdkError> {
        match self {
            WorkspaceInner::Codex(backend) => backend.close().await,
            WorkspaceInner::OpenCode(backend) => backend.close().await,
            WorkspaceInner::Custom(backend) => backend.close().await,
        }
    }
}

impl<S, D, P, T> WorkspaceBackend for Workspace<S, D, P, T>
where
    S: StateStoreBackend,
    D: DatabaseBackend,
    P: ProviderBackend,
    T: ToolsetBackend,
{
    type Conversation = Conversation<S, D, P, T>;

    fn id(&self) -> WorkspaceId {
        self.inner.id()
    }

    async fn info(&self) -> Result<WorkspaceInfo, SdkError> {
        self.inner.info().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::WorkspaceInfo)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    async fn new_conversation(
        &self,
        request: NewConversation,
    ) -> Result<Conversation<S, D, P, T>, SdkError> {
        self.inner.new_conversation(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::NewConversation)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    async fn list_conversations(&self) -> Result<Vec<ConversationInfo>, SdkError> {
        self.inner.list_conversations().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::ListConversations)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    async fn conversation_info(&self, id: ConversationId) -> Result<ConversationInfo, SdkError> {
        self.inner.conversation_info(id).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::ConversationInfo)
                .in_phase(crate::error::ErrorPhase::StateRead)
        })
    }

    async fn get_conversation(
        &self,
        id: ConversationId,
    ) -> Result<Conversation<S, D, P, T>, SdkError> {
        self.inner.get_conversation(id).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::GetConversation)
                .in_phase(crate::error::ErrorPhase::Admission)
        })
    }

    async fn list_tools(&self) -> Result<Vec<ToolInfo>, SdkError> {
        self.inner.list_tools().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::ListTools)
                .in_phase(crate::error::ErrorPhase::InventoryVerification)
        })
    }

    async fn ingest_knowledge(&self, request: IngestKnowledgeRequest) -> Result<(), SdkError> {
        self.inner.ingest_knowledge(request).await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::IngestKnowledge)
                .in_phase(crate::error::ErrorPhase::KnowledgeWrite)
        })
    }

    async fn close(self) -> Result<(), SdkError> {
        self.inner.close().await.map_err(|error| {
            error
                .at_operation(crate::error::Operation::CloseWorkspace)
                .in_phase(crate::error::ErrorPhase::Lifecycle)
        })
    }
}
