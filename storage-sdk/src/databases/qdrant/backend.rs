use crate::{
    CapabilitySet, CatalogRequest, CatalogResponse, CommonConfig, DataRequest, DataResponse,
    DatabaseBackend, IndexRequest, IndexResponse, QdrantConfig, QueryRequest, QueryResponse,
    StorageError,
};

use super::manager::QdrantManager;

#[derive(Clone, Debug)]
pub(crate) struct QdrantBackend {
    manager: Box<QdrantManager>,
}

impl QdrantBackend {
    pub(crate) async fn connect(
        common: CommonConfig,
        config: QdrantConfig,
    ) -> Result<Self, StorageError> {
        QdrantManager::connect(common, config)
            .await
            .map(|manager| Self {
                manager: Box::new(manager),
            })
    }
}

impl DatabaseBackend for QdrantBackend {
    fn capabilities(&self) -> CapabilitySet {
        self.manager.capabilities()
    }

    async fn catalog(&self, req: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        manager.catalog(req).await
    }

    async fn data(&self, req: DataRequest) -> Result<DataResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        req.validate()?;
        manager.data(req).await
    }

    async fn query(&self, req: QueryRequest) -> Result<QueryResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        manager.query(req).await
    }

    async fn index(&self, req: IndexRequest) -> Result<IndexResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        manager.index(req).await
    }
}
