use crate::{CapabilitySet, SurrealConfig, SurrealModeConfig};

use super::manager::SurrealContextKey;

impl SurrealConfig {
    pub(crate) fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::DENSE_VECTOR
            | CapabilitySet::FULL_TEXT
            | CapabilitySet::FILTERING
            | CapabilitySet::SCHEMA_EVOLUTION
            | CapabilitySet::WAIT_FOR_INDEX
            | CapabilitySet::ATOMIC_PARTITION_REPLACE
    }

    pub(crate) fn mode_label(&self) -> &'static str {
        match &self.mode {
            SurrealModeConfig::Mem => "embedded-mem",
            SurrealModeConfig::RocksDb { .. } => "embedded-rocksdb",
            SurrealModeConfig::SurrealKv { .. } => "embedded-surrealkv",
            SurrealModeConfig::RemoteWs { .. } => "remote-ws",
            SurrealModeConfig::RemoteHttp { .. } => "remote-http",
        }
    }

    pub(crate) fn endpoint_string(&self) -> String {
        super::mapper::endpoint_from_mode(&self.mode)
    }

    pub(super) fn context_key(&self) -> Option<SurrealContextKey> {
        match (&self.namespace, &self.database) {
            (Some(namespace), Some(database)) => Some(SurrealContextKey {
                namespace: namespace.clone(),
                database: database.clone(),
            }),
            _ => None,
        }
    }
}
