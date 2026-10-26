use crate::{
    CapabilitySet, CatalogRequest, CatalogResponse, CommonConfig, DataRequest, DataResponse,
    DatabaseBackend, IndexRequest, IndexResponse, QueryRequest, QueryResponse, StorageError,
    SurrealConfig,
};

use super::manager::SurrealManager;

#[derive(Clone, Debug)]
pub(crate) struct SurrealBackend {
    manager: Box<SurrealManager>,
}

impl SurrealBackend {
    pub(crate) async fn connect(
        common: CommonConfig,
        config: SurrealConfig,
    ) -> Result<Self, StorageError> {
        SurrealManager::connect(common, config)
            .await
            .map(|manager| Self {
                manager: Box::new(manager),
            })
    }
}

impl DatabaseBackend for SurrealBackend {
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
