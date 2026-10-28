use std::path::Path;
use std::time::Duration;

use tokio::io::AsyncReadExt;

use super::CodexInputObservation;

impl CodexInputObservation {
    pub(crate) async fn read_stderr(
        path: &Path,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<u8>, &'static str> {
        if tokio::time::Instant::now() >= deadline {
            return Err("rollout_snapshot_deadline");
        }
        tokio::time::timeout_at(deadline, async {
            let file = tokio::fs::File::open(path)
                .await
                .map_err(|_| "stderr_unavailable")?;
            let mut bytes = Vec::new();
            file.take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| "stderr_read_failed")?;
            if bytes.len() > 1024 * 1024 {
                return Err("stderr_byte_bound");
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| "rollout_snapshot_deadline")?
    }

    /// One bounded snapshot of the native-owned file. Identity checks bracket
    /// the read; bytes from a changed or redirected file never reach projection.
    pub(crate) async fn read_rollout(
        home: &Path,
        path: &Path,
        scenario_deadline: tokio::time::Instant,
    ) -> Result<Vec<u8>, &'static str> {
        let deadline = scenario_deadline.min(tokio::time::Instant::now() + Duration::from_secs(10));
        if tokio::time::Instant::now() >= deadline {
            return Err("rollout_snapshot_deadline");
        }
        tokio::time::timeout_at(deadline, async {
            if !path.is_absolute() {
                return Err("rollout_path_not_absolute");
            }
            let home = tokio::fs::canonicalize(home)
                .await
                .map_err(|_| "rollout_home_unavailable")?;
            let sessions = home.join("sessions");
            if tokio::fs::canonicalize(&sessions)
                .await
                .map_err(|_| "rollout_sessions_unavailable")?
                != sessions
            {
                return Err("rollout_sessions_redirected");
            }
            let canonical = tokio::fs::canonicalize(path)
                .await
                .map_err(|_| "rollout_path_unavailable")?;
            if canonical != path || !canonical.starts_with(&sessions) {
                return Err("rollout_path_foreign_or_redirected");
            }
            let before = tokio::fs::symlink_metadata(path)
                .await
                .map_err(|_| "rollout_metadata_unavailable")?;
            if !before.file_type().is_file() || before.file_type().is_symlink() {
                return Err("rollout_not_regular");
            }
            if before.len() > 8 * 1024 * 1024 {
                return Err("rollout_byte_bound");
            }
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(
                    (rustix::fs::OFlags::CLOEXEC
                        | rustix::fs::OFlags::NOFOLLOW
                        | rustix::fs::OFlags::NONBLOCK)
                        .bits() as i32,
                );
            }
            let mut file =
                tokio::fs::File::from_std(options.open(path).map_err(|_| "rollout_open_failed")?);
            let opened = file
                .metadata()
                .await
                .map_err(|_| "rollout_metadata_unavailable")?;
            if !Self::same_rollout_metadata(&before, &opened) {
                return Err("rollout_changed");
            }
            let mut bytes = Vec::with_capacity(before.len() as usize);
            (&mut file)
                .take(8 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| "rollout_read_failed")?;
            if bytes.len() > 8 * 1024 * 1024 {
                return Err("rollout_byte_bound");
            }
            let after_file = file
                .metadata()
                .await
                .map_err(|_| "rollout_metadata_unavailable")?;
            let after_path = tokio::fs::symlink_metadata(path)
                .await
                .map_err(|_| "rollout_metadata_unavailable")?;
            if !Self::same_rollout_metadata(&before, &after_file)
                || !Self::same_rollout_metadata(&before, &after_path)
                || bytes.len() as u64 != before.len()
                || tokio::fs::canonicalize(path)
                    .await
                    .map_err(|_| "rollout_path_unavailable")?
                    != canonical
            {
                return Err("rollout_changed");
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| "rollout_snapshot_deadline")?
    }

    pub(crate) fn same_rollout_metadata(
        left: &std::fs::Metadata,
        right: &std::fs::Metadata,
    ) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if left.dev() != right.dev()
                || left.ino() != right.ino()
                || left.ctime() != right.ctime()
                || left.ctime_nsec() != right.ctime_nsec()
            {
                return false;
            }
        }
        left.file_type().is_file()
            && right.file_type().is_file()
            && left.len() == right.len()
            && left
                .modified()
                .ok()
                .zip(right.modified().ok())
                .is_some_and(|(left, right)| left == right)
    }
}
