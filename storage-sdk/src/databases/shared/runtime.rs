use std::fmt;
#[cfg(any(feature = "surrealdb", feature = "qdrant"))]
use std::future::Future;

use crate::{CapabilitySet, CommonConfig, StorageError, core::ApplyDefaultLocator};

#[derive(Clone, Debug)]
pub(crate) struct DatabaseRuntime {
    common: CommonConfig,
    capabilities: CapabilitySet,
    label: &'static str,
    mode: &'static str,
}

impl DatabaseRuntime {
    pub(crate) fn new(
        common: CommonConfig,
        capabilities: CapabilitySet,
        label: &'static str,
        mode: &'static str,
    ) -> Self {
        Self {
            common,
            capabilities,
            label,
            mode,
        }
    }

    pub(crate) fn capabilities(&self) -> CapabilitySet {
        self.capabilities
    }

    pub(crate) fn backend_mode(&self) -> &'static str {
        self.mode
    }

    #[cfg(feature = "surrealdb")]
    pub(crate) fn common_config(&self) -> &CommonConfig {
        &self.common
    }

    pub(crate) fn normalize_request<T: ApplyDefaultLocator>(&self, request: T) -> T {
        if let Some(default) = self.common.default_locator.as_ref() {
            request.apply_default_locator(default)
        } else {
            request
        }
    }

    pub(crate) fn ensure_mutable(&self, operation: &'static str) -> Result<(), StorageError> {
        self.common.ensure_mutable(operation)
    }

    pub(crate) fn unsupported(
        &self,
        capability: &'static str,
        context: impl Into<String>,
    ) -> StorageError {
        StorageError::unsupported_capability(capability, context.into())
    }

    pub(crate) fn require_capability(
        &self,
        capability: CapabilitySet,
        label: &'static str,
        context: impl Into<String>,
    ) -> Result<(), StorageError> {
        if self.capabilities.contains(capability) {
            Ok(())
        } else {
            Err(self.unsupported(label, context))
        }
    }

    pub(crate) fn map_err(
        &self,
        operation: &'static str,
        error: impl fmt::Display,
    ) -> StorageError {
        StorageError::backend_op(operation, error.to_string())
    }

    #[cfg(any(feature = "surrealdb", feature = "qdrant"))]
    pub(crate) async fn with_request_timeout<T, F>(
        &self,
        operation: &'static str,
        action: impl fmt::Display,
        future: F,
    ) -> Result<T, StorageError>
    where
        F: Future<Output = Result<T, StorageError>> + Send,
        T: Send,
    {
        tokio::time::timeout(self.common.request_timeout(), future)
            .await
            .map_err(|_| {
                StorageError::backend_op(
                    operation,
                    format!(
                        "timed out after {} ms while {action}",
                        self.common.request_timeout_ms
                    ),
                )
            })?
    }

    pub(crate) fn enforce_required_capabilities(&self) -> Result<(), StorageError> {
        if self
            .capabilities
            .contains(self.common.required_capabilities)
        {
            return Ok(());
        }

        Err(StorageError::InvalidConfig(format!(
            "requested capabilities {:?} are not fully satisfied by {} mode `{}` (available: {:?})",
            self.common.required_capabilities, self.label, self.mode, self.capabilities
        )))
    }
}
