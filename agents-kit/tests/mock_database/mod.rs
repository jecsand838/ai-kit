//! Typed request/response script, deliberately not a database engine.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use storage_kit::database::CapabilitySet;
use storage_kit::database::catalog::{CatalogRequest, CatalogResponse};
use storage_kit::database::data::{DataRequest, DataResponse};
use storage_kit::database::index::{IndexRequest, IndexResponse};
use storage_kit::database::query::{QueryRequest, QueryResponse};
use storage_kit::{DatabaseBackend, StorageError};

pub(super) enum Step {
    Catalog(Result<CatalogResponse, StorageError>),
    Data(Result<DataResponse, StorageError>),
}

pub(super) struct Control {
    steps: Mutex<VecDeque<Step>>,
    catalog: Mutex<Vec<CatalogRequest>>,
    data: Mutex<Vec<DataRequest>>,
}

impl Control {
    pub(super) fn new(steps: Vec<Step>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into()),
            catalog: Mutex::new(Vec::new()),
            data: Mutex::new(Vec::new()),
        })
    }

    pub(super) fn database(self: &Arc<Self>) -> Database {
        Database {
            control: Arc::clone(self),
        }
    }
    pub(super) fn data(&self) -> Vec<DataRequest> {
        self.data.lock().expect("data observations").clone()
    }
    pub(super) fn catalog(&self) -> Vec<CatalogRequest> {
        self.catalog.lock().expect("catalog observations").clone()
    }
    pub(super) fn assert_consumed(&self) {
        assert!(
            self.steps.lock().expect("script").is_empty(),
            "unconsumed required database expectation"
        );
    }
}

pub(super) struct Database {
    control: Arc<Control>,
}

impl DatabaseBackend for Database {
    fn capabilities(&self) -> CapabilitySet {
        CapabilitySet::ATOMIC_PARTITION_REPLACE
    }

    async fn catalog(&self, request: CatalogRequest) -> Result<CatalogResponse, StorageError> {
        self.control
            .catalog
            .lock()
            .expect("catalog observations")
            .push(request);
        match self.control.steps.lock().expect("script").pop_front() {
            Some(Step::Catalog(result)) => result,
            Some(Step::Data(_)) | None => Err(StorageError::InvalidRequest(
                "unexpected catalog operation".into(),
            )),
        }
    }

    async fn data(&self, request: DataRequest) -> Result<DataResponse, StorageError> {
        self.control
            .data
            .lock()
            .expect("data observations")
            .push(request);
        match self.control.steps.lock().expect("script").pop_front() {
            Some(Step::Data(result)) => result,
            Some(Step::Catalog(_)) | None => Err(StorageError::InvalidRequest(
                "unexpected data operation".into(),
            )),
        }
    }

    async fn query(&self, _request: QueryRequest) -> Result<QueryResponse, StorageError> {
        Err(StorageError::unsupported_capability(
            "query",
            "unscripted optional database operation",
        ))
    }

    async fn index(&self, _request: IndexRequest) -> Result<IndexResponse, StorageError> {
        Err(StorageError::unsupported_capability(
            "index",
            "unscripted optional database operation",
        ))
    }
}
