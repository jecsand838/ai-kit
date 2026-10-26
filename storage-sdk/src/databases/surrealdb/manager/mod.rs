use std::{
    collections::HashMap,
    future::IntoFuture,
    sync::{Arc, Mutex},
};

use secrecy::ExposeSecret;
use surrealdb::{Surreal, engine::any::Any};

use crate::databases::shared::DatabaseRuntime;
use crate::{
    CatalogRequest, CatalogResponse, CommonConfig, DataRequest, DataResponse, IndexRequest,
    IndexResponse, QueryRequest, QueryResponse, StorageError, SurrealConfig,
};

use super::{catalog as catalog_impl, data as data_impl, index as index_impl, query as query_impl};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct SurrealContextKey {
    pub(super) namespace: String,
    pub(super) database: String,
}

#[derive(Clone, Debug)]
pub(crate) struct SurrealManager {
    root_client: Arc<Surreal<Any>>,
    runtime: DatabaseRuntime,
    config: SurrealConfig,
    context_clients: Arc<Mutex<HashMap<SurrealContextKey, Arc<Surreal<Any>>>>>,
}

impl SurrealManager {
    pub(crate) async fn connect(
        common: CommonConfig,
        config: SurrealConfig,
    ) -> Result<Self, StorageError> {
        let endpoint = config.endpoint_string();
        let root_client =
            Self::connect_client(&common, &config, &endpoint, config.context_key().as_ref())
                .await?;

        let capabilities = config.capabilities();
        let mode = config.mode_label();
        let manager = Self {
            root_client,
            runtime: DatabaseRuntime::new(common, capabilities, "SurrealDB", mode),
            config,
            context_clients: Arc::new(Mutex::new(Default::default())),
        };
        manager.enforce_required_capabilities()?;
        Ok(manager)
    }

    async fn connect_client(
        common: &CommonConfig,
        config: &SurrealConfig,
        endpoint: &str,
        context: Option<&SurrealContextKey>,
    ) -> Result<Arc<Surreal<Any>>, StorageError> {
        use surrealdb::{engine::any::connect, opt::auth::Root};

        let client = Self::timeboxed_backend_op(
            common,
            "connect",
            "connecting to SurrealDB",
            connect(endpoint),
        )
        .await?;

        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            Self::timeboxed_backend_op(
                common,
                "signin",
                "signing in to SurrealDB",
                client.signin(Root {
                    username: username.clone(),
                    password: password.as_secret().expose_secret().to_owned(),
                }),
            )
            .await?;
        }

        if let Some(context) = context {
            Self::select_client_context(&client, common, context).await?;
        }

        Ok(Arc::new(client))
    }

    pub(super) async fn select_client_context(
        client: &Surreal<Any>,
        common: &CommonConfig,
        context: &SurrealContextKey,
    ) -> Result<(), StorageError> {
        Self::timeboxed_backend_op(
            common,
            "use_ns/use_db",
            "selecting namespace/database in SurrealDB",
            client
                .use_ns(context.namespace.clone())
                .use_db(context.database.clone()),
        )
        .await?;
        Ok(())
    }

    async fn timeboxed_backend_op<T, E, Fut>(
        common: &CommonConfig,
        op: &'static str,
        action: &str,
        future: Fut,
    ) -> Result<T, StorageError>
    where
        Fut: IntoFuture<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        tokio::time::timeout(common.connect_timeout(), future.into_future())
            .await
            .map_err(|_| {
                StorageError::backend_op(
                    op,
                    format!(
                        "timed out after {} ms while {action}",
                        common.connect_timeout_ms
                    ),
                )
            })?
            .map_err(|error| StorageError::backend_op(op, error.to_string()))
    }
}

impl SurrealManager {
    pub(crate) fn capabilities(&self) -> crate::CapabilitySet {
        self.runtime.capabilities()
    }

    pub(crate) fn common_config(&self) -> &CommonConfig {
        self.runtime.common_config()
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

    pub(crate) async fn with_request_timeout<T, F>(
        &self,
        operation: &'static str,
        action: impl std::fmt::Display,
        future: F,
    ) -> Result<T, StorageError>
    where
        F: std::future::Future<Output = Result<T, StorageError>> + Send,
        T: Send,
    {
        self.runtime
            .with_request_timeout(operation, action, future)
            .await
    }

    fn enforce_required_capabilities(&self) -> Result<(), StorageError> {
        self.runtime.enforce_required_capabilities()
    }
}

impl SurrealManager {
    pub(super) const PARTITION_REPLACEMENT_GUARD_TABLE: &'static str =
        "__storage_sdk_atomic_partition_replace_guards";

    pub(super) fn root_client(&self) -> &Arc<Surreal<Any>> {
        &self.root_client
    }

    pub(super) fn config(&self) -> &SurrealConfig {
        &self.config
    }

    pub(super) fn context_clients(&self) -> &Mutex<HashMap<SurrealContextKey, Arc<Surreal<Any>>>> {
        &self.context_clients
    }
}

impl SurrealManager {
    pub(super) async fn catalog(
        &self,
        req: CatalogRequest,
    ) -> Result<CatalogResponse, StorageError> {
        catalog_impl::handle(self, req).await
    }

    pub(super) async fn data(&self, req: DataRequest) -> Result<DataResponse, StorageError> {
        data_impl::handle(self, req).await
    }

    pub(super) async fn query(&self, req: QueryRequest) -> Result<QueryResponse, StorageError> {
        query_impl::handle(self, req).await
    }

    pub(super) async fn index(&self, req: IndexRequest) -> Result<IndexResponse, StorageError> {
        index_impl::handle(self, req).await
    }
}
