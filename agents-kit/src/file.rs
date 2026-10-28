use std::path::{Path, PathBuf};

use crate::{KitError, WorkspaceContext};

/// Exact content returned by a portable workspace-relative file read.
///
/// # Examples
///
/// ```
/// use std::path::{Path, PathBuf};
///
/// use agents_kit::conversation::FileRead;
///
/// let file = FileRead::from_bytes(
///     PathBuf::from("notes.txt"),
///     b"ready\n".to_vec(),
/// )?;
/// assert_eq!(file.path(), Path::new("notes.txt"));
/// assert_eq!(file.text(), Some("ready\n"));
/// assert_eq!(file.bytes(), None);
/// # Ok::<(), agents_kit::KitError>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileRead {
    path: PathBuf,
    content: FileReadContent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FileReadContent {
    Text(String),
    Binary { bytes: Vec<u8>, mime_type: String },
}

impl FileRead {
    /// Validates the portable relative path and preserves the exact file content.
    ///
    /// This constructs a returned value; it does not read the filesystem.
    /// UTF-8 bytes become text, while invalid UTF-8 remains exact binary data.
    ///
    /// # Errors
    /// Returns [`crate::error::ErrorKind::InvalidInput`] for absolute paths,
    /// parent traversal, or invalid portable path components.
    ///
    /// ```
    /// use agents_kit::conversation::FileRead;
    /// let file = FileRead::from_bytes("notes.txt".into(), b"ready".to_vec())?;
    /// assert_eq!(file.text(), Some("ready"));
    /// assert!(FileRead::from_bytes("../secret".into(), vec![]).is_err());
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn from_bytes(path: PathBuf, bytes: Vec<u8>) -> Result<Self, KitError> {
        let path = WorkspaceContext::normalized_relative(&path)?;
        let content = match String::from_utf8(bytes) {
            Ok(text) => FileReadContent::Text(text),
            Err(error) => FileReadContent::Binary {
                bytes: error.into_bytes(),
                mime_type: "application/octet-stream".to_owned(),
            },
        };
        Ok(Self { path, content })
    }

    /// Returns the validated workspace-relative path.
    ///
    /// ```
    /// use agents_kit::conversation::FileRead;
    /// let file = FileRead::from_bytes("notes.txt".into(), vec![])?;
    /// assert_eq!(file.path(), std::path::Path::new("notes.txt"));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the exact text when the file bytes are valid UTF-8.
    ///
    /// ```
    /// use agents_kit::conversation::FileRead;
    /// let file = FileRead::from_bytes("notes.txt".into(), b"ready\n".to_vec())?;
    /// assert_eq!(file.text(), Some("ready\n"));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn text(&self) -> Option<&str> {
        match &self.content {
            FileReadContent::Text(text) => Some(text),
            FileReadContent::Binary { .. } => None,
        }
    }

    /// Returns the exact bytes when the file content is not valid UTF-8.
    ///
    /// ```
    /// use agents_kit::conversation::FileRead;
    /// let file = FileRead::from_bytes("data.bin".into(), vec![0xff, 0x00])?;
    /// assert_eq!(file.bytes(), Some([0xff, 0x00].as_slice()));
    /// assert_eq!(file.text(), None);
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn bytes(&self) -> Option<&[u8]> {
        match &self.content {
            FileReadContent::Text(_) => None,
            FileReadContent::Binary { bytes, .. } => Some(bytes),
        }
    }

    /// Returns the MIME type when the file content is not valid UTF-8.
    ///
    /// ```
    /// use agents_kit::conversation::FileRead;
    /// let file = FileRead::from_bytes("data.bin".into(), vec![0xff])?;
    /// assert_eq!(file.mime_type(), Some("application/octet-stream"));
    /// # Ok::<(), agents_kit::KitError>(())
    /// ```
    pub fn mime_type(&self) -> Option<&str> {
        match &self.content {
            FileReadContent::Text(_) => None,
            FileReadContent::Binary { mime_type, .. } => Some(mime_type),
        }
    }
}
