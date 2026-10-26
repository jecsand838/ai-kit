use bitflags::bitflags;
use serde::{Deserialize, Serialize};

bitflags! {
    /// Optional behavior within the retained portable database inventory.
    ///
    /// Capabilities describe retained optional behavior, not acceptance of every
    /// request or a backend identity. The operation remains authoritative: inspect
    /// its result and handle unsupported combinations before relying on an effect.
    ///
    /// # Examples
    ///
    /// ```
    /// use storage_sdk::database::CapabilitySet;
    /// let required = CapabilitySet::FILTERING | CapabilitySet::DENSE_VECTOR;
    /// assert!(required.contains(CapabilitySet::FILTERING));
    /// assert!(!required.contains(CapabilitySet::RANDOM_SAMPLE));
    /// # Ok::<(), storage_sdk::StorageError>(())
    /// ```
    #[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    pub struct CapabilitySet: u16 {
        /// Dense vector storage and nearest-neighbor search.
        const DENSE_VECTOR = 1 << 0;
        /// Full-text indexing and text search.
        const FULL_TEXT = 1 << 1;
        /// Structured filter lowering.
        const FILTERING = 1 << 2;
        /// Random sampling.
        const RANDOM_SAMPLE = 1 << 3;
        /// Post-creation schema alteration.
        const SCHEMA_EVOLUTION = 1 << 4;
        /// Waiting for index readiness.
        const WAIT_FOR_INDEX = 1 << 5;
        /// Atomic bounded partition replacement.
        const ATOMIC_PARTITION_REPLACE = 1 << 6;
    }
}
