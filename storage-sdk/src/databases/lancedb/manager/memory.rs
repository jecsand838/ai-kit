use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, providers::memory::MemoryStoreProvider,
};
use object_store::{memory::InMemory, path::Path};
use url::Url;

/// One native backing store shared by this connection's metadata and data sessions.
/// Native registry keys may differ by table, but those differences must not create
/// another memory database or discard bytes when a table handle is released.
#[derive(Debug, Default)]
pub(super) struct LanceMemory {
    storage: Arc<InMemory>,
}

impl ObjectStoreProvider for LanceMemory {
    fn new_store<'store, 'params, 'future>(
        &'store self,
        base_path: Url,
        params: &'params ObjectStoreParams,
    ) -> Pin<Box<dyn Future<Output = lance::Result<ObjectStore>> + Send + 'future>>
    where
        'store: 'future,
        'params: 'future,
        Self: 'future,
    {
        Box::pin(async move {
            let mut store = MemoryStoreProvider.new_store(base_path, params).await?;
            store.inner = self.storage.clone();
            Ok(store)
        })
    }

    fn extract_path(&self, url: &Url) -> lance::Result<Path> {
        MemoryStoreProvider.extract_path(url)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> lance::Result<String> {
        MemoryStoreProvider.calculate_object_store_prefix(url, storage_options)
    }
}
