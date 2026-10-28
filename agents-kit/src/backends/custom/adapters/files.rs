use std::path::PathBuf;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::path::{Component, Path};

use crate::workspace::BackendHostPathFlavor;
use crate::{FileRead, KitError, WorkspaceRoot};

pub(crate) struct CustomFileAdapter {
    relative_path: PathBuf,
    root_path: PathBuf,
    root_relative_path: PathBuf,
}

impl CustomFileAdapter {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn validate_workspace_path_flavor(
        flavor: BackendHostPathFlavor,
    ) -> Result<(), KitError> {
        if flavor == BackendHostPathFlavor::Posix {
            Ok(())
        } else {
            Err(KitError::unsupported(
                "workspace path flavor is unsupported on this platform",
            ))
        }
    }

    #[cfg(target_os = "windows")]
    pub(crate) fn validate_workspace_path_flavor(
        flavor: BackendHostPathFlavor,
    ) -> Result<(), KitError> {
        if flavor == BackendHostPathFlavor::Windows {
            Ok(())
        } else {
            Err(KitError::unsupported(
                "workspace path flavor is unsupported on this platform",
            ))
        }
    }

    pub(crate) fn from_resolved(
        relative_path: PathBuf,
        absolute_path: PathBuf,
        roots: &[WorkspaceRoot],
    ) -> Result<Self, KitError> {
        let (root_path, root_relative_path) = roots
            .iter()
            .filter_map(|root| {
                absolute_path
                    .strip_prefix(root.path())
                    .ok()
                    .map(|root_relative_path| (root.path(), root_relative_path))
            })
            .max_by_key(|(root_path, _)| root_path.components().count())
            .ok_or_else(|| {
                KitError::invalid_state(
                    "file admission did not identify a containing workspace root",
                )
            })?;
        if root_relative_path.as_os_str().is_empty() {
            return Err(KitError::integration("file target is not a regular file"));
        }
        Ok(Self {
            relative_path,
            root_path: root_path.to_path_buf(),
            root_relative_path: root_relative_path.to_path_buf(),
        })
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) async fn read(self) -> Result<FileRead, KitError> {
        tokio::task::spawn_blocking(move || self.read_rooted())
            .await
            .map_err(|_| {
                KitError::integration("contained file read task failed")
                    .with_detail(crate::error::ErrorDetail::TaskTerminated)
            })?
    }

    #[cfg(target_os = "windows")]
    pub(crate) async fn read(self) -> Result<FileRead, KitError> {
        let Self {
            relative_path: _,
            root_path: _,
            root_relative_path: _,
        } = self;
        Err(KitError::unsupported(
            "contained file reads are unsupported on this platform",
        ))
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn read_rooted(self) -> Result<FileRead, KitError> {
        use rustix::fs::{FileType, Mode, OFlags};

        let directory_flags =
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let mut root_components = self.root_path.components();
        if !matches!(root_components.next(), Some(Component::RootDir)) {
            return Err(KitError::invalid_state(
                "workspace root is unavailable to the current process",
            ));
        }
        let mut directory = rustix::fs::open(Path::new("/"), directory_flags, Mode::empty())
            .map_err(|error| self.filesystem_error(error))?;
        for component in root_components {
            let Component::Normal(segment) = component else {
                return Err(KitError::invalid_state(
                    "workspace root is not a normalized local path",
                ));
            };
            directory = rustix::fs::openat(
                &directory,
                Path::new(segment),
                directory_flags,
                Mode::empty(),
            )
            .map_err(|error| self.descendant_error(error))?;
        }
        let parent = self
            .root_relative_path
            .parent()
            .ok_or_else(|| KitError::invalid_state("file target has no rooted parent"))?;
        for component in parent.components() {
            let Component::Normal(segment) = component else {
                return Err(KitError::invalid_state(
                    "file target is not a normalized rooted path",
                ));
            };
            directory = rustix::fs::openat(
                &directory,
                Path::new(segment),
                directory_flags,
                Mode::empty(),
            )
            .map_err(|error| self.descendant_error(error))?;
        }

        let file_name = self
            .root_relative_path
            .file_name()
            .ok_or_else(|| KitError::invalid_state("file target has no final path component"))?;
        let file_flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        let file = rustix::fs::openat(&directory, Path::new(file_name), file_flags, Mode::empty())
            .map_err(|error| self.descendant_error(error))?;
        let metadata = rustix::fs::fstat(&file).map_err(|error| self.filesystem_error(error))?;
        if !FileType::from_raw_mode(metadata.st_mode).is_file() {
            return Err(KitError::integration("file target is not a regular file"));
        }

        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0_u8; 8192];
            match rustix::io::read(&file, &mut chunk) {
                Ok(0) => break,
                Ok(count) => {
                    bytes.try_reserve(count).map_err(|_| {
                        KitError::integration("file content could not be allocated")
                    })?;
                    bytes.extend_from_slice(&chunk[..count]);
                }
                Err(error) if error == rustix::io::Errno::INTR => {}
                Err(error) => return Err(self.filesystem_error(error)),
            }
        }
        FileRead::from_bytes(self.relative_path, bytes)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn filesystem_error(&self, error: rustix::io::Errno) -> KitError {
        if error == rustix::io::Errno::NOENT {
            KitError::not_found("file path was not found")
        } else if error == rustix::io::Errno::LOOP {
            KitError::invalid_input("file path contains a symbolic link")
        } else {
            KitError::integration("contained file read failed")
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn descendant_error(&self, error: rustix::io::Errno) -> KitError {
        if error == rustix::io::Errno::LOOP || error == rustix::io::Errno::NOTDIR {
            KitError::invalid_input("file path contains an invalid path component")
        } else {
            self.filesystem_error(error)
        }
    }
}
