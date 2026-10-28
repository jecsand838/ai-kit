use std::{collections::BTreeSet, fmt, hash::Hash, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::ToolError;

const MAX_TOOL_NAME_BYTES: usize = 128;
pub(crate) const TOOL_NAME_NAMESPACE_SEPARATOR: &str = "__";

/// Portable executable tool identifier.
///
/// # Examples
///
/// ```
/// use tools_kit::declaration::ToolName;
/// let name = ToolName::new("lookup_record")?;
/// assert_eq!(name.as_str(), "lookup_record");
/// assert!(ToolName::new("bad name").is_err());
///
/// # Ok::<(), tools_kit::ToolError>(())
/// ```
///
#[derive(Clone, Debug)]
pub struct ToolName {
    inner: ToolNameInner,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ToolNameInner {
    Local { rendered: String },
    Mcp { rendered: String, native: String },
}

/// Private context for allocating rendered MCP names.
#[derive(Debug)]
pub(crate) struct McpToolNameContext {
    namespace: Option<String>,
    rendered_names: BTreeSet<String>,
}

impl McpToolNameContext {
    pub(crate) fn new(namespace: Option<&ToolName>) -> Result<Self, ToolError> {
        let namespace = namespace.map(|name| name.as_str().to_owned());
        if let Some(namespace) = &namespace {
            Self::validate_namespace(namespace)?;
        }
        Ok(Self {
            namespace,
            rendered_names: BTreeSet::new(),
        })
    }

    fn allocate_rendered_name(&mut self, native_name: &str) -> Result<String, ToolError> {
        let preferred_leaf = Self::sanitize_native_leaf(native_name);
        if let Some(rendered) = self.try_reserve_leaf(&preferred_leaf)? {
            return Ok(rendered);
        }

        let hash = Self::stable_short_hash(native_name);
        for counter in 0_u32.. {
            let suffix = if counter == 0 {
                format!("_{hash}")
            } else {
                format!("_{hash}_{counter}")
            };
            if let Some(leaf) =
                Self::truncate_leaf_with_suffix(&preferred_leaf, &suffix, self.leaf_budget())
                && let Some(rendered) = self.try_reserve_leaf(&leaf)?
            {
                return Ok(rendered);
            }
        }

        Err(ToolError::InvalidConfiguration {
            message: "failed to allocate a rendered MCP tool name".to_owned(),
        })
    }

    fn leaf_budget(&self) -> usize {
        match &self.namespace {
            Some(namespace) => MAX_TOOL_NAME_BYTES
                .saturating_sub(namespace.len())
                .saturating_sub(TOOL_NAME_NAMESPACE_SEPARATOR.len()),
            None => MAX_TOOL_NAME_BYTES,
        }
    }

    fn try_reserve_leaf(&mut self, leaf: &str) -> Result<Option<String>, ToolError> {
        if leaf.is_empty() {
            return Ok(None);
        }
        let rendered = match &self.namespace {
            Some(namespace) => {
                format!("{namespace}{TOOL_NAME_NAMESPACE_SEPARATOR}{leaf}")
            }
            None => leaf.to_owned(),
        };
        ToolName::validate_rendered(&rendered)?;
        if self.rendered_names.insert(rendered.clone()) {
            Ok(Some(rendered))
        } else {
            Ok(None)
        }
    }

    fn validate_namespace(value: &str) -> Result<(), ToolError> {
        ToolName::validate_rendered(value)?;
        let minimum_fallback_leaf = "tool_00000000";
        if value.len() + TOOL_NAME_NAMESPACE_SEPARATOR.len() + minimum_fallback_leaf.len()
            > MAX_TOOL_NAME_BYTES
        {
            return Err(ToolName::invalid(
                "tool namespaces must leave room for fallback tool names",
            ));
        }
        Ok(())
    }

    fn sanitize_native_leaf(native_name: &str) -> String {
        let leaf = native_name
            .rsplit(['/', ':', '.'])
            .next()
            .unwrap_or(native_name)
            .trim();
        let mut sanitized = String::with_capacity(leaf.len());
        let mut previous_separator = false;
        for byte in leaf.bytes() {
            let rendered = if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
                previous_separator = false;
                byte as char
            } else {
                if previous_separator {
                    continue;
                }
                previous_separator = true;
                '_'
            };
            sanitized.push(rendered);
        }
        let sanitized = sanitized.trim_matches('_').to_owned();
        if sanitized.is_empty() {
            "tool".to_owned()
        } else {
            sanitized
        }
    }

    fn stable_short_hash(value: &str) -> String {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in value.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("{:08x}", hash as u32)
    }

    fn truncate_leaf_with_suffix(leaf: &str, suffix: &str, budget: usize) -> Option<String> {
        if suffix.len() > budget {
            return None;
        }
        let max_prefix = budget - suffix.len();
        let mut prefix = String::new();
        for ch in leaf.chars() {
            if prefix.len() + ch.len_utf8() > max_prefix {
                break;
            }
            prefix.push(ch);
        }
        let prefix = prefix.trim_end_matches(['_', '-']);
        let prefix = if prefix.is_empty() { "tool" } else { prefix };
        Some(format!("{prefix}{suffix}"))
    }
}

impl ToolName {
    /// Creates a caller-authored executable tool name.
    ///
    /// # Errors
    ///
    /// Returns an error when the rendered name is empty, too long, or contains
    /// characters outside the Kit's portable tool-name subset.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::declaration::ToolName;
    /// let name = ToolName::new("lookup_record")?;
    /// assert_eq!(name.as_str(), "lookup_record");
    /// assert!(ToolName::new("bad name").is_err());
    ///
    /// # Ok::<(), tools_kit::ToolError>(())
    /// ```
    ///
    pub fn new(value: impl Into<String>) -> Result<Self, ToolError> {
        Self::local(value)
    }

    pub(crate) fn local(value: impl Into<String>) -> Result<Self, ToolError> {
        let rendered = value.into();
        Self::validate_rendered(&rendered)?;
        Ok(Self {
            inner: ToolNameInner::Local { rendered },
        })
    }

    pub(crate) fn mcp(
        context: &mut McpToolNameContext,
        native_name: impl Into<String>,
    ) -> Result<Self, ToolError> {
        let native = native_name.into();
        if native.trim().is_empty() {
            return Err(ToolError::InvalidConfiguration {
                message: "MCP tool names must not be empty".to_owned(),
            });
        }
        let rendered = context.allocate_rendered_name(&native)?;
        Ok(Self {
            inner: ToolNameInner::Mcp { rendered, native },
        })
    }

    /// Returns the provider-safe rendered name.
    ///
    /// # Examples
    ///
    /// ```
    /// use tools_kit::declaration::ToolName;
    /// let name = ToolName::new("lookup_record")?;
    /// assert_eq!(name.as_str(), "lookup_record");
    /// assert!(ToolName::new("bad name").is_err());
    ///
    /// # Ok::<(), tools_kit::ToolError>(())
    /// ```
    ///
    #[must_use]
    pub fn as_str(&self) -> &str {
        match &self.inner {
            ToolNameInner::Local { rendered } | ToolNameInner::Mcp { rendered, .. } => rendered,
        }
    }

    pub(crate) fn mcp_native_name(&self) -> Option<&str> {
        match &self.inner {
            ToolNameInner::Local { .. } => None,
            ToolNameInner::Mcp { native, .. } => Some(native),
        }
    }

    fn validate_rendered(value: &str) -> Result<(), ToolError> {
        if value.is_empty() {
            return Err(Self::invalid("tool names must not be empty"));
        }
        if value.len() > MAX_TOOL_NAME_BYTES {
            return Err(Self::invalid("tool names must not exceed 128 bytes"));
        }
        if value.trim() != value {
            return Err(Self::invalid(
                "tool names must not contain leading or trailing whitespace",
            ));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(Self::invalid(
                "tool names may contain only ASCII letters, digits, `_`, and `-`",
            ));
        }
        Ok(())
    }

    fn invalid(message: impl Into<String>) -> ToolError {
        ToolError::InvalidConfiguration {
            message: message.into(),
        }
    }
}

impl PartialEq for ToolName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for ToolName {}

impl PartialOrd for ToolName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ToolName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for ToolName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl fmt::Display for ToolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ToolName {
    type Err = ToolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::local(value)
    }
}

impl TryFrom<String> for ToolName {
    type Error = ToolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::local(value)
    }
}

impl TryFrom<&str> for ToolName {
    type Error = ToolError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::local(value)
    }
}

impl Serialize for ToolName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ToolName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::local(value).map_err(D::Error::custom)
    }
}
