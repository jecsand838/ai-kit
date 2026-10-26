use crate::{CapabilitySet, QdrantConfig, QdrantModeConfig};

impl QdrantConfig {
    pub(crate) fn capabilities(&self) -> CapabilitySet {
        match &self.mode {
            QdrantModeConfig::RemoteRest { .. } => {
                CapabilitySet::DENSE_VECTOR
                    | CapabilitySet::FILTERING
                    | CapabilitySet::RANDOM_SAMPLE
                    | CapabilitySet::WAIT_FOR_INDEX
            }
            QdrantModeConfig::Edge { .. } => {
                CapabilitySet::DENSE_VECTOR
                    | CapabilitySet::FILTERING
                    | CapabilitySet::WAIT_FOR_INDEX
            }
        }
    }

    pub(crate) fn mode_label(&self) -> &'static str {
        match &self.mode {
            QdrantModeConfig::Edge { .. } => "embedded-edge",
            QdrantModeConfig::RemoteRest { .. } => "remote-rest",
        }
    }
}
