use crate::{
    StateKey, StateListRequest, StateListResponse, StateRecordSnapshot, StateStoreBackend,
    StateStoreError, state_stores::config::RedbStateStoreConfig,
};

use super::executor::RedbExecutor;

pub(crate) struct RedbStateStore {
    executor: RedbExecutor,
}

impl RedbStateStore {
    pub(crate) async fn connect(config: RedbStateStoreConfig) -> Result<Self, StateStoreError> {
        config.validate()?;
        RedbExecutor::start(config)
            .await
            .map(|executor| Self { executor })
    }
}

impl StateStoreBackend for RedbStateStore {
    type Transaction = super::RedbStateTransaction;

    async fn load(&self, key: StateKey) -> Result<Option<StateRecordSnapshot>, StateStoreError> {
        self.executor.load(key).await
    }

    async fn list(&self, request: StateListRequest) -> Result<StateListResponse, StateStoreError> {
        self.executor.list(request).await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, StateStoreError> {
        self.executor.begin_transaction().await
    }
}
