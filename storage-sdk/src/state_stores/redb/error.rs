use std::fmt;

use crate::StateStoreError;

pub(super) struct RedbErrorContext {
    op: &'static str,
}

impl RedbErrorContext {
    pub(super) fn new(op: &'static str) -> Self {
        Self { op }
    }

    pub(super) fn backend<E>(&self, _err: E) -> StateStoreError
    where
        E: fmt::Display,
    {
        StateStoreError::backend_operation(self.op, "state operation failed")
    }

    pub(super) fn serialization<E>(&self, _err: E) -> StateStoreError
    where
        E: fmt::Display,
    {
        StateStoreError::serialization("state record encoding is invalid")
    }

    pub(super) fn timeout(&self, timeout_ms: u64) -> StateStoreError {
        StateStoreError::timed_out(self.op, timeout_ms)
    }

    pub(super) fn closed(&self) -> StateStoreError {
        StateStoreError::unavailable(self.op, "state worker is unavailable")
    }
}
