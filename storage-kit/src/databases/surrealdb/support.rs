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
            SurrealModeConfig::SurrealKv { .. } => "embedded-surrealkv",
            SurrealModeConfig::RemoteWs { .. } => "remote-ws",
            SurrealModeConfig::RemoteHttp { .. } => "remote-http",
        }
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

impl SurrealModeConfig {
    pub(crate) fn endpoint_string(&self) -> String {
        match self {
            Self::Mem => "mem://".to_owned(),
            Self::SurrealKv { path } => format!("surrealkv://{}", path.to_string_lossy()),
            Self::RemoteWs { endpoint } | Self::RemoteHttp { endpoint } => {
                endpoint.trim().to_owned()
            }
        }
    }
}
