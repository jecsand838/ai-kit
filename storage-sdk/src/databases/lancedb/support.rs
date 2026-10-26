use crate::{CapabilitySet, LanceConfig};

impl LanceConfig {
    pub(crate) fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::DENSE_VECTOR
            | CapabilitySet::FULL_TEXT
            | CapabilitySet::FILTERING
            | CapabilitySet::SCHEMA_EVOLUTION
            | CapabilitySet::WAIT_FOR_INDEX
            | CapabilitySet::ATOMIC_PARTITION_REPLACE
    }

    pub(crate) fn mode_label(&self) -> &'static str {
        "embedded-path"
    }
}
