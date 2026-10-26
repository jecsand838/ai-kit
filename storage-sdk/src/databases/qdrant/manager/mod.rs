use std::{
    collections::{BTreeMap, btree_map::Entry},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::databases::shared::DatabaseRuntime;
use crate::{
    CatalogRequest, CatalogResponse, CommonConfig, DataRequest, DataResponse, IndexRequest,
    IndexResponse, QdrantConfig, QdrantModeConfig, QueryRequest, QueryResponse, StorageError,
};

use super::{
    catalog as catalog_impl, data as data_impl, query as query_impl, rest::RestQdrantClient,
};

#[derive(Clone, Debug)]
pub(super) enum QdrantConnection {
    Edge(EdgeState),
    RemoteRest(RestQdrantClient),
}

#[derive(Clone, Debug)]
pub(super) struct EdgeState {
    pub(super) root: PathBuf,
    shards: Arc<Mutex<BTreeMap<String, Arc<qdrant_edge::EdgeShard>>>>,
}

#[derive(Clone, Debug)]
pub(super) struct EdgeCollectionHandle {
    collection: String,
    path: PathBuf,
    shard: Arc<qdrant_edge::EdgeShard>,
}

impl EdgeCollectionHandle {
    pub(super) fn collection(&self) -> &str {
        &self.collection
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn shard(&self) -> &qdrant_edge::EdgeShard {
        &self.shard
    }
}

impl EdgeState {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            shards: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(super) fn collection_path(&self, collection: &str) -> PathBuf {
        self.root.join(collection)
    }

    pub(super) fn collection(
        &self,
        collection: String,
    ) -> Result<EdgeCollectionHandle, StorageError> {
        let path = self.collection_path(&collection);
        let shard = self.get_or_load(&collection)?;
        Ok(EdgeCollectionHandle {
            collection,
            path,
            shard,
        })
    }

    pub(super) fn insert_created(
        &self,
        collection: String,
        shard: qdrant_edge::EdgeShard,
    ) -> Result<EdgeCollectionHandle, StorageError> {
        let path = self.collection_path(&collection);
        let shard = Arc::new(shard);
        self.shards
            .lock()
            .map_err(edge_cache_error)?
            .insert(collection.clone(), Arc::clone(&shard));
        Ok(EdgeCollectionHandle {
            collection,
            path,
            shard,
        })
    }

    pub(super) fn invalidate(&self, collection: &str) -> Result<(), StorageError> {
        self.shards
            .lock()
            .map_err(edge_cache_error)?
            .remove(collection);
        Ok(())
    }

    fn get_or_load(&self, collection: &str) -> Result<Arc<qdrant_edge::EdgeShard>, StorageError> {
        if let Some(shard) = self
            .shards
            .lock()
            .map_err(edge_cache_error)?
            .get(collection)
            .map(Arc::clone)
        {
            return Ok(shard);
        }

        let path = self.collection_path(collection);
        let shard = Arc::new(
            qdrant_edge::EdgeShard::load(&path, None)
                .map_err(|err| StorageError::backend_op("edge_load", err.to_string()))?,
        );

        let mut shards = self.shards.lock().map_err(edge_cache_error)?;
        match shards.entry(collection.to_string()) {
            Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
            Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&shard));
                Ok(shard)
            }
        }
    }
}

fn edge_cache_error<T>(err: std::sync::PoisonError<T>) -> StorageError {
    StorageError::backend_op("edge_shard_cache", err.to_string())
}

#[derive(Clone, Debug)]
pub(crate) struct QdrantManager {
    connection: QdrantConnection,
    runtime: DatabaseRuntime,
    config: Box<QdrantConfig>,
}

impl QdrantManager {
    pub(crate) async fn connect(
        common: CommonConfig,
        config: QdrantConfig,
    ) -> Result<Self, StorageError> {
        use tokio::fs;

        let mode = config.mode_label();
        let capabilities = config.capabilities();

        let connection = match &config.mode {
            QdrantModeConfig::RemoteRest { endpoint, api_key } => QdrantConnection::RemoteRest(
                RestQdrantClient::new(endpoint.clone(), api_key.clone(), &common)?,
            ),
            QdrantModeConfig::Edge { path } => {
                fs::create_dir_all(path).await?;
                QdrantConnection::Edge(EdgeState::new(path.clone()))
            }
        };

        let manager = Self {
            connection,
            runtime: DatabaseRuntime::new(common, capabilities, "Qdrant", mode),
            config: Box::new(config),
        };
        manager.enforce_required_capabilities()?;
        Ok(manager)
    }
}

impl QdrantManager {
    pub(crate) fn capabilities(&self) -> crate::CapabilitySet {
        self.runtime.capabilities()
    }

    pub(crate) fn backend_mode(&self) -> &'static str {
        self.runtime.backend_mode()
    }

    pub(crate) fn normalize_request<T: crate::core::ApplyDefaultLocator>(&self, request: T) -> T {
        self.runtime.normalize_request(request)
    }

    pub(crate) fn ensure_mutable(&self, operation: &'static str) -> Result<(), StorageError> {
        self.runtime.ensure_mutable(operation)
    }

    pub(crate) fn unsupported(
        &self,
        capability: &'static str,
        context: impl Into<String>,
    ) -> StorageError {
        self.runtime.unsupported(capability, context)
    }

    pub(crate) fn require_capability(
        &self,
        capability: crate::CapabilitySet,
        label: &'static str,
        context: impl Into<String>,
    ) -> Result<(), StorageError> {
        self.runtime.require_capability(capability, label, context)
    }

    pub(crate) fn map_err(
        &self,
        operation: &'static str,
        error: impl std::fmt::Display,
    ) -> StorageError {
        self.runtime.map_err(operation, error)
    }

    fn enforce_required_capabilities(&self) -> Result<(), StorageError> {
        self.runtime.enforce_required_capabilities()
    }
}

impl QdrantManager {
    pub(super) fn connection(&self) -> &QdrantConnection {
        &self.connection
    }

    pub(super) fn config(&self) -> &QdrantConfig {
        &self.config
    }
}

impl QdrantManager {
    pub(super) async fn catalog(
        &self,
        req: CatalogRequest,
    ) -> Result<CatalogResponse, StorageError> {
        self.runtime
            .with_request_timeout(
                "catalog",
                "executing a Qdrant catalog operation",
                catalog_impl::handle(self, req),
            )
            .await
    }

    pub(super) async fn data(&self, req: DataRequest) -> Result<DataResponse, StorageError> {
        self.runtime
            .with_request_timeout(
                "data",
                "executing a Qdrant data operation",
                data_impl::handle(self, req),
            )
            .await
    }

    pub(super) async fn query(&self, req: QueryRequest) -> Result<QueryResponse, StorageError> {
        self.runtime
            .with_request_timeout(
                "query",
                "executing a Qdrant query operation",
                query_impl::handle(self, req),
            )
            .await
    }

    pub(super) async fn index(&self, req: IndexRequest) -> Result<IndexResponse, StorageError> {
        self.runtime
            .with_request_timeout("index", "executing a Qdrant index operation", async {
                match req {
                    IndexRequest::Create(request) => self.create_index(request).await,
                    IndexRequest::List { locator } => Ok(IndexResponse::Indexes(
                        self.list_index_infos(&locator).await?,
                    )),
                    IndexRequest::Drop { locator, name } => self.drop_index(locator, name).await,
                    IndexRequest::WaitFor(request) => self.wait_for_index(request).await,
                }
            })
            .await
    }
}
