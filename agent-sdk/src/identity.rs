//! Opaque durable identities and descriptive agent inventory.
//!
//! Persist identities through their canonical string representation and parse
//! them through `FromStr`. Parsing validates shape; it does not establish that
//! the referenced resource exists. Identity creation is owned by the runtime.

use std::fmt;
use std::str::FromStr;

use crate::SdkError;

pub use crate::agent::AgentInfo;

#[derive(Clone, Copy)]
enum IdentityKind {
    Agent,
    Workspace,
    Conversation,
    Notification,
}

impl IdentityKind {
    fn prefix(self) -> &'static str {
        match self {
            Self::Agent => "agent_",
            Self::Workspace => "workspace_",
            Self::Conversation => "conversation_",
            Self::Notification => "notification_",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct Identity {
    canonical: String,
}

impl Identity {
    fn new(kind: IdentityKind) -> Self {
        Self {
            canonical: format!("{}{}", kind.prefix(), uuid::Uuid::new_v4().simple()),
        }
    }

    fn parse(kind: IdentityKind, value: String) -> Result<Self, SdkError> {
        let payload = value
            .strip_prefix(kind.prefix())
            .ok_or_else(|| SdkError::invalid_input("SDK identity category is invalid"))?;
        if payload.len() != 32
            || !payload
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(SdkError::invalid_input(
                "SDK identity canonical form is invalid",
            ));
        }
        let parsed = uuid::Uuid::parse_str(payload)
            .map_err(|_| SdkError::invalid_input("SDK identity canonical form is invalid"))?;
        if parsed.simple().to_string() != payload {
            return Err(SdkError::invalid_input(
                "SDK identity canonical form is invalid",
            ));
        }
        Ok(Self { canonical: value })
    }

    fn as_str(&self) -> &str {
        &self.canonical
    }
}

/// Validated identity of an agent, parseable from its canonical string.
///
/// ```
/// use agent_sdk::identity::AgentId;
///
/// let value = "agent_0123456789abcdef0123456789abcdef";
/// let id: AgentId = value.parse()?;
/// assert_eq!(id.to_string(), value);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AgentId(Identity);

impl AgentId {
    pub(crate) fn new() -> Self {
        Self(Identity::new(IdentityKind::Agent))
    }

    /// Borrows the canonical agent identity string.
    ///
    /// ```
    /// use agent_sdk::identity::AgentId;
    ///
    /// let value = "agent_0123456789abcdef0123456789abcdef";
    /// let id: AgentId = value.parse()?;
    /// assert_eq!(id.as_str(), value);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AgentId {
    type Err = SdkError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Identity::parse(IdentityKind::Agent, value.to_owned()).map(Self)
    }
}

impl serde::Serialize for AgentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for AgentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// Validated identity of a workspace, parseable from its canonical string.
///
/// ```
/// use agent_sdk::identity::WorkspaceId;
///
/// let value = "workspace_0123456789abcdef0123456789abcdef";
/// let id: WorkspaceId = value.parse()?;
/// assert_eq!(id.to_string(), value);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkspaceId(Identity);

impl WorkspaceId {
    pub(crate) fn new() -> Self {
        Self(Identity::new(IdentityKind::Workspace))
    }

    /// Borrows the canonical workspace identity string.
    ///
    /// ```
    /// use agent_sdk::identity::WorkspaceId;
    ///
    /// let value = "workspace_0123456789abcdef0123456789abcdef";
    /// let id: WorkspaceId = value.parse()?;
    /// assert_eq!(id.as_str(), value);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for WorkspaceId {
    type Err = SdkError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Identity::parse(IdentityKind::Workspace, value.to_owned()).map(Self)
    }
}

impl serde::Serialize for WorkspaceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for WorkspaceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// Validated identity of a conversation, parseable from its canonical string.
///
/// ```
/// use agent_sdk::identity::ConversationId;
///
/// let value = "conversation_0123456789abcdef0123456789abcdef";
/// let id: ConversationId = value.parse()?;
/// assert_eq!(id.to_string(), value);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ConversationId(Identity);

impl ConversationId {
    pub(crate) fn new() -> Self {
        Self(Identity::new(IdentityKind::Conversation))
    }

    /// Borrows the canonical conversation identity string.
    ///
    /// ```
    /// use agent_sdk::identity::ConversationId;
    ///
    /// let value = "conversation_0123456789abcdef0123456789abcdef";
    /// let id: ConversationId = value.parse()?;
    /// assert_eq!(id.as_str(), value);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for ConversationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ConversationId {
    type Err = SdkError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Identity::parse(IdentityKind::Conversation, value.to_owned()).map(Self)
    }
}

impl serde::Serialize for ConversationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for ConversationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}

/// Validated identity of a notification, parseable from its canonical string.
///
/// ```
/// use agent_sdk::identity::NotificationId;
///
/// let value = "notification_0123456789abcdef0123456789abcdef";
/// let id: NotificationId = value.parse()?;
/// assert_eq!(id.to_string(), value);
/// # Ok::<(), agent_sdk::SdkError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NotificationId(Identity);

impl NotificationId {
    pub(crate) fn new() -> Self {
        Self(Identity::new(IdentityKind::Notification))
    }

    /// Borrows the canonical notification identity string.
    ///
    /// ```
    /// use agent_sdk::identity::NotificationId;
    ///
    /// let value = "notification_0123456789abcdef0123456789abcdef";
    /// let id: NotificationId = value.parse()?;
    /// assert_eq!(id.as_str(), value);
    /// # Ok::<(), agent_sdk::SdkError>(())
    /// ```
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for NotificationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for NotificationId {
    type Err = SdkError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Identity::parse(IdentityKind::Notification, value.to_owned()).map(Self)
    }
}

impl serde::Serialize for NotificationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for NotificationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_str(&value).map_err(serde::de::Error::custom)
    }
}
