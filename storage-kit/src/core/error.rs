use thiserror::Error;

/// Portable error type returned by the storage Kit.
///
/// Portable request families report unsupported backend behavior explicitly
/// instead of synthesizing successful responses. Use [`StorageError::InvalidRequest`]
/// for semantically invalid caller input that is already structurally valid Rust.
///
/// # Examples
///
/// ```
/// let error = storage_kit::StorageError::InvalidRequest(
///     "vector value cannot be coerced without precision loss".into(),
/// );
///
/// assert!(error.to_string().contains("invalid request"));
/// ```
///
#[derive(Debug, Clone, Error)]
pub enum StorageError {
    /// The supplied configuration is structurally invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A portable request is structurally valid Rust but semantically invalid.
    ///
    /// Vector coercion policy failures and malformed typed requests use this
    /// variant before a backend operation is attempted.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// The selected backend does not expose the requested operation yet.
    #[error("database operation is not implemented yet: {note}")]
    NotYetImplemented {
        /// Human-readable note describing the missing support.
        note: String,
    },

    /// The backend or mode does not support a requested capability.
    #[error("unsupported capability `{capability}`{context}")]
    UnsupportedCapability {
        /// Missing or unsupported capability name.
        capability: String,
        /// Additional human-readable context.
        context: String,
    },

    /// A backend-specific operation failed.
    #[error("database operation `{operation}` failed: {message}")]
    BackendOperation {
        /// Portable operation label used by the adapter.
        operation: String,
        /// Human-readable backend error message.
        message: String,
    },

    /// Serialization or wire-format conversion failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Filesystem or OS I/O failed.
    #[error("io error: {0}")]
    Io(String),
}

impl StorageError {
    /// Creates a [`StorageError::NotYetImplemented`] value.
    ///
    /// # Examples
    ///
    /// ```
    /// let error = storage_kit::StorageError::not_yet_implemented(
    ///     "retained operation mapping is pending",
    /// );
    /// assert!(matches!(
    ///     error,
    ///     storage_kit::StorageError::NotYetImplemented { .. }
    /// ));
    /// ```
    pub fn not_yet_implemented(note: impl Into<String>) -> Self {
        Self::NotYetImplemented { note: note.into() }
    }

    /// Creates a [`StorageError::UnsupportedCapability`] value.
    ///
    /// # Examples
    ///
    /// ```
    /// let error = storage_kit::StorageError::unsupported_capability(
    ///     "FULL_TEXT",
    ///     "selected backend cannot provide this full-text query",
    /// );
    /// assert!(error.to_string().contains("FULL_TEXT"));
    /// ```
    pub fn unsupported_capability(
        capability: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        let context = context.into();
        Self::UnsupportedCapability {
            capability: capability.into(),
            context: if context.is_empty() {
                String::new()
            } else {
                format!(" ({context})")
            },
        }
    }

    /// Creates a [`StorageError::BackendOperation`] value.
    ///
    /// # Examples
    ///
    /// ```
    /// let error = storage_kit::StorageError::backend_op(
    ///     "query",
    ///     "provider returned an error",
    /// );
    /// let cloned = error.clone();
    /// assert_eq!(error.to_string(), cloned.to_string());
    /// ```
    pub fn backend_op(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::BackendOperation {
            operation: operation.into(),
            message: message.into(),
        }
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}
