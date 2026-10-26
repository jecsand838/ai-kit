use std::path::PathBuf;

use storage_sdk::state::config::{LanceStateStoreConfig, StateStoreConfig};

/// Owns an isolated persistent store until the consumer journey has joined.
pub(crate) struct LanceStateDirectory {
    path: PathBuf,
}

impl LanceStateDirectory {
    pub(crate) fn new() -> Self {
        Self {
            path: std::env::temp_dir()
                .join(format!("ai-kit-lance-consumer-{}", uuid::Uuid::new_v4())),
        }
    }

    pub(crate) fn config(&self) -> StateStoreConfig {
        StateStoreConfig::Lance(LanceStateStoreConfig::directory(&self.path))
    }
}

impl Drop for LanceStateDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
