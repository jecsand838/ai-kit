use crate::{
    CapabilitySet, CatalogRequest, CatalogResponse, CommonConfig, DataRequest, DataResponse,
    DatabaseBackend, IndexRequest, IndexResponse, LanceConfig, QueryRequest, QueryResponse,
    StorageError,
};

use super::{manager::LanceManager, mutations::LanceMutations};

#[derive(Clone, Debug)]
pub(crate) struct LanceBackend {
    manager: Box<LanceManager>,
    mutations: std::sync::Arc<LanceMutations>,
}

impl LanceBackend {
    pub(crate) async fn connect(
        common: CommonConfig,
        config: LanceConfig,
    ) -> Result<Self, StorageError> {
        let (mutations, manager) = LanceMutations::connect(common, config).await?;
        Ok(Self {
            manager: Box::new(manager),
            mutations,
        })
    }
}

impl DatabaseBackend for LanceBackend {
    fn capabilities(&self) -> CapabilitySet {
        self.manager.capabilities()
    }

    async fn catalog(&self, req: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        match req {
            CatalogRequest::CreateCollection(_)
            | CatalogRequest::DropCollection { .. }
            | CatalogRequest::AlterCollection(_) => self.mutations.catalog(req).await,
            CatalogRequest::OpenCollection { .. } | CatalogRequest::ListCollections(_) => {
                manager.catalog(req).await
            }
        }
    }

    async fn data(&self, req: DataRequest) -> Result<DataResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        req.validate()?;
        match req {
            DataRequest::Insert(_)
            | DataRequest::Update(_)
            | DataRequest::Delete(_)
            | DataRequest::Merge(_)
            | DataRequest::ReplacePartition(_) => self.mutations.data(req).await,
            DataRequest::Lookup(_) | DataRequest::Scan(_) | DataRequest::Count(_) => {
                manager.data(req).await
            }
        }
    }

    async fn query(&self, req: QueryRequest) -> Result<QueryResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        manager.query(req).await
    }

    async fn index(&self, req: IndexRequest) -> Result<IndexResponse, StorageError> {
        let manager = self.manager.as_ref();
        let req = manager.normalize_request(req);
        match req {
            IndexRequest::Create(_) | IndexRequest::Drop { .. } => self.mutations.index(req).await,
            IndexRequest::List { .. } | IndexRequest::WaitFor(_) => manager.index(req).await,
        }
    }
}
