//! Provider-owned file upload, metadata, pagination, and bounded downloads.
//!
//! [`FileUploadRequest`] owns validated bytes. Submit it through
//! [`crate::ProviderBackend::upload_file`], retain the returned lifecycle id, and
//! explicitly delete only caller-owned files. [`FileObject::input_reference`](crate::files::FileObject::input_reference) may
//! differ from the lifecycle id. Processing readiness and operation support are
//! provider-authored facts rather than assumptions following upload.
//!
//! # Retain ownership through deletion
//!
//! This requires an OpenAI key and upload/read/delete access. The exact upload id
//! is the cleanup authority; neither a filename nor a list-difference establishes
//! ownership. If upload fails without a returned id, retain the error's evidence of
//! any uncertain remote outcome rather than deleting a guessed resource.
//!
//! ```no_run
//! use std::num::NonZeroUsize;
//! use providers_kit::{Provider, ProviderBackend, ProviderConfig};
//! use providers_kit::config::{ApiKey, OpenAiConfig, ProviderKind, TransportConfig};
//! use providers_kit::files::FileUploadRequest;
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), providers_kit::KitError> {
//! let provider = Provider::from_config(ProviderConfig::new(
//!     ProviderKind::OpenAi(OpenAiConfig::new(ApiKey::new("replace-with-your-key")?)),
//!     TransportConfig::default(),
//! ))?;
//! let uploaded = provider
//!     .upload_file(FileUploadRequest::new(
//!         "note.txt",
//!         "text/plain",
//!         b"owned note".to_vec(),
//!     )?)
//!     .await;
//! let work = async {
//!     let file = uploaded.as_ref().map_err(Clone::clone)?;
//!     let info = provider.file_info(file.id.clone()).await?;
//!     assert_eq!(info.id, file.id);
//!     let bytes = provider.download_file(file.id.clone(), NonZeroUsize::new(1024).unwrap()).await?;
//!     assert_eq!(bytes, b"owned note");
//!     Ok::<(), providers_kit::KitError>(())
//! }.await;
//! let deletion = match uploaded {
//!     Ok(file) => provider.delete_file(file.id).await,
//!     Err(_) => Ok(()),
//! };
//! let shutdown = provider.shutdown().await;
//! // Retain cleanup errors as supplementary evidence on an earlier failure.
//! let mut outcome = work;
//! for cleanup in [deletion, shutdown] {
//!     if let Err(error) = cleanup {
//!         outcome = Err(match outcome { Err(primary) => primary.with_supplementary(error), Ok(()) => error });
//!     }
//! }
//! outcome?;
//! # Ok(())
//! # }
//! ```
//!
//! Provider shutdown settles local resources only. Failed deletion remains a remote
//! cleanup obligation; it is not converted into success by shutdown.

use std::num::NonZeroUsize;

use reqwest::multipart::Part;
use serde::{Deserialize, Deserializer, Serialize};

use crate::KitError;
use crate::core::content::base64_bytes;
use crate::error::ValidationViolation;
use crate::generation::FileReference;

/// Validated provider-file upload request.
///
/// This owns bytes without uploading them. Submit it through [`crate::ProviderBackend::upload_file`]
/// and retain the returned lifecycle id for explicit owned-resource cleanup.
///
/// # Examples
///
/// ```
/// use providers_kit::files::FileUploadRequest;
/// let request = FileUploadRequest::new("notes.txt", "text/plain", b"notes".to_vec())?;
/// assert_eq!(request.file_name(), "notes.txt");
/// assert_eq!(request.media_type(), "text/plain");
/// assert_eq!(request.data(), b"notes");
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileUploadRequest {
    file_name: String,
    media_type: String,
    #[serde(with = "base64_bytes")]
    data: Vec<u8>,
}

impl FileUploadRequest {
    /// Creates a non-empty upload with a file name and media type.
    ///
    /// # Errors
    ///
    /// Returns an input error for an empty, padded, or control-containing file name; malformed or padded MIME type/parameters; or empty file bytes.
    ///
    /// See [`crate::files::FileUploadRequest`] for a complete example.
    ///
    pub fn new(
        file_name: impl Into<String>,
        media_type: impl Into<String>,
        data: Vec<u8>,
    ) -> Result<Self, KitError> {
        let file_name = file_name.into();
        let media_type = media_type.into();
        if file_name.is_empty()
            || file_name.trim() != file_name
            || file_name.chars().any(char::is_control)
        {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "file.name",
                    rule: "nonblank, unpadded, without control characters",
                },
                "upload file name must be nonblank, unpadded, and contain no control characters",
            ));
        }
        let parameters_valid = media_type.split(';').skip(1).all(|parameter| {
            parameter
                .split_once('=')
                .is_some_and(|(name, value)| !name.trim().is_empty() && !value.trim().is_empty())
        });
        if media_type.is_empty()
            || media_type.trim() != media_type
            || media_type.chars().any(char::is_control)
            || !parameters_valid
            || Part::bytes(Vec::new()).mime_str(&media_type).is_err()
        {
            return Err(KitError::invalid_input(
                ValidationViolation::InvalidFormat {
                    field: "file.media_type",
                    rule: "valid type/subtype",
                },
                "upload media type must be a valid type/subtype",
            ));
        }
        if data.is_empty() {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty { field: "file.data" },
                "upload data must not be empty",
            ));
        }
        Ok(Self {
            file_name,
            media_type,
            data,
        })
    }

    #[must_use]
    /// Returns the file name presented to the provider.
    ///
    /// See [`crate::files::FileUploadRequest`] for a complete example.
    ///
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    #[must_use]
    /// Returns the declared media type.
    ///
    /// See [`crate::files::FileUploadRequest`] for a complete example.
    ///
    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    #[must_use]
    /// Returns the raw file bytes.
    ///
    /// See [`crate::files::FileUploadRequest`] for a complete example.
    ///
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[must_use]
    /// Consumes the request into its file name, media type, and bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::files::FileUploadRequest;
    /// let request = FileUploadRequest::new("notes.txt", "text/plain", b"notes".to_vec())?;
    /// let (name, media_type, bytes) = request.into_parts();
    /// assert_eq!(name, "notes.txt");
    /// assert_eq!(media_type, "text/plain");
    /// assert_eq!(bytes, b"notes");
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    pub fn into_parts(self) -> (String, String, Vec<u8>) {
        (self.file_name, self.media_type, self.data)
    }
}

impl<'de> Deserialize<'de> for FileUploadRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            file_name: String,
            media_type: String,
            #[serde(with = "base64_bytes")]
            data: Vec<u8>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.file_name, wire.media_type, wire.data).map_err(serde::de::Error::custom)
    }
}

/// Validated file-list pagination request.
///
/// Pagination cursors are opaque provider-owned values; a requested limit is not a
/// guarantee that every provider supports the same pagination controls.
///
/// # Examples
///
/// ```
/// use providers_kit::files::FileListRequest;
/// let request = FileListRequest::default()
///     .with_limit(std::num::NonZeroUsize::new(10).unwrap())
///     .with_cursor("opaque-next")?;
/// assert_eq!(request.limit().unwrap().get(), 10);
/// assert_eq!(request.cursor(), Some("opaque-next"));
///
/// # Ok::<(), providers_kit::KitError>(())
/// ```
///
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct FileListRequest {
    limit: Option<NonZeroUsize>,
    cursor: Option<String>,
}

impl FileListRequest {
    /// Sets a non-zero result limit.
    ///
    /// See [`crate::files::FileListRequest`] for a complete example.
    ///
    #[must_use]
    pub fn with_limit(mut self, limit: NonZeroUsize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets a non-empty provider-native continuation cursor.
    ///
    /// # Errors
    ///
    /// Returns an input error when the cursor is empty or whitespace-only. Nonblank cursor bytes are retained exactly; use only a cursor returned by the same provider operation.
    ///
    /// See [`crate::files::FileListRequest`] for a complete example.
    ///
    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Result<Self, KitError> {
        let cursor = cursor.into();
        if cursor.trim().is_empty() {
            return Err(KitError::invalid_input(
                ValidationViolation::Empty {
                    field: "files.cursor",
                },
                "file cursor must not be empty",
            ));
        }
        self.cursor = Some(cursor);
        Ok(self)
    }

    /// Returns the requested result limit.
    ///
    /// See [`crate::files::FileListRequest`] for a complete example.
    ///
    #[must_use]
    pub const fn limit(&self) -> Option<NonZeroUsize> {
        self.limit
    }

    /// Returns the provider-native continuation cursor.
    ///
    /// See [`crate::files::FileListRequest`] for a complete example.
    ///
    #[must_use]
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Consumes the request into its limit and continuation cursor.
    ///
    /// # Examples
    ///
    /// ```
    /// use providers_kit::files::FileListRequest;
    /// let request = FileListRequest::default()
    ///     .with_limit(std::num::NonZeroUsize::new(10).unwrap())
    ///     .with_cursor("opaque-next")?;
    /// let (limit, cursor) = request.into_parts();
    /// assert_eq!(limit.unwrap().get(), 10);
    /// assert_eq!(cursor.as_deref(), Some("opaque-next"));
    ///
    /// # Ok::<(), providers_kit::KitError>(())
    /// ```
    ///
    #[must_use]
    pub fn into_parts(self) -> (Option<NonZeroUsize>, Option<String>) {
        (self.limit, self.cursor)
    }
}

impl<'de> Deserialize<'de> for FileListRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            limit: Option<NonZeroUsize>,
            cursor: Option<String>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut request = Self {
            limit: wire.limit,
            cursor: None,
        };
        if let Some(cursor) = wire.cursor {
            request = request
                .with_cursor(cursor)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(request)
    }
}

/// Provider-managed file metadata.
///
/// The lifecycle `id` can differ from `input_reference`; use the returned generation
/// reference where supplied. Upload success does not establish processing readiness.
///
/// # Examples
///
/// ```
/// use providers_kit::files::{FileObject, FileState};
/// let file = FileObject {
///     id: "owned-file".into(),
///     file_name: Some("notes.txt".into()),
///     size_bytes: Some(5),
///     media_type: Some("text/plain".into()),
///     created_at: None,
///     state: Some(FileState::Processing),
///     input_reference: None,
/// };
/// assert!(matches!(file.state, Some(FileState::Processing)));
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileObject {
    /// Exact provider-native lifecycle identifier.
    pub id: String,
    /// Provider-reported file name, when available.
    pub file_name: Option<String>,
    /// Provider-reported size in bytes, when available.
    pub size_bytes: Option<u64>,
    /// Provider-reported media type, when available.
    pub media_type: Option<String>,
    /// Provider-reported creation timestamp, when available.
    pub created_at: Option<String>,
    /// Current provider-authored lifecycle state, when available.
    pub state: Option<FileState>,
    /// Exact reference usable as generation input, when distinct from lifecycle `id`.
    pub input_reference: Option<FileReference>,
}

/// Provider-managed file lifecycle state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum FileState {
    /// The provider is processing the uploaded file.
    Processing,
    /// The file is ready for supported provider operations.
    Active,
    /// The provider failed to process the file.
    Failed {
        /// Provider error code, when available.
        code: Option<String>,
        /// Provider error message, when available.
        message: Option<String>,
    },
}

/// One page of provider-managed files.
///
/// # Examples
///
/// ```
/// let page = providers_kit::files::FilePage { files: Vec::new(), next_cursor: None };
/// assert!(page.files.is_empty());
/// assert!(page.next_cursor.is_none());
/// ```
///
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilePage {
    /// Files returned in this page.
    pub files: Vec<FileObject>,
    /// Provider-native cursor for a subsequent page.
    pub next_cursor: Option<String>,
}
