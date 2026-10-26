use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, Weak},
};

use lance::session::Session;
use lancedb::{
    connection::Connection, database::namespace::LanceNamespaceDatabase, embeddings::MemoryRegistry,
};

mod memory;

use memory::LanceMemory;

use crate::databases::shared::DatabaseRuntime;
use crate::{
    CommonConfig, DataRequest, DataResponse, IndexRequest, IndexResponse, LanceConfig,
    QueryRequest, QueryResponse, StorageError,
};

use super::{data as data_impl, index as index_impl, query as query_impl};

#[derive(Clone)]
pub(crate) struct LanceManager {
    connection: Connection,
    mutation_lock: Arc<tokio::sync::Mutex<()>>,
    runtime: DatabaseRuntime,
    config: LanceConfig,
}

impl fmt::Debug for LanceManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceManager")
            .field("mode", &self.backend_mode())
            .field("capabilities", &self.capabilities())
            .field("config", &self.config)
            .finish()
    }
}

impl LanceManager {
    pub(crate) async fn connect(
        common: CommonConfig,
        config: LanceConfig,
    ) -> Result<Self, StorageError> {
        use tokio::time::timeout;

        let connection = timeout(common.connect_timeout(), Self::connect_native(&config))
            .await
            .map_err(|_| {
                StorageError::backend_op(
                    "connect",
                    format!(
                        "timed out after {} ms while connecting to LanceDB",
                        common.connect_timeout_ms
                    ),
                )
            })?
            .map_err(|error| StorageError::backend_op("connect", error.to_string()))?;

        let mutation_lock = Self::mutation_lock(&config)?;
        let capabilities = config.capabilities();
        let mode = config.mode_label();
        let manager = Self {
            connection,
            mutation_lock,
            runtime: DatabaseRuntime::new(common, capabilities, "LanceDB", mode),
            config,
        };

        manager.enforce_required_capabilities()?;
        Ok(manager)
    }

    async fn connect_native(config: &LanceConfig) -> lancedb::Result<Connection> {
        let namespace_session = Arc::new(Session::default());
        let table_session = Arc::new(Session::default());
        let uri = config.uri_string();
        let uri = if uri == "memory://" {
            let memory = Arc::new(LanceMemory::default());
            namespace_session
                .store_registry()
                .insert("memory", memory.clone());
            table_session.store_registry().insert("memory", memory);
            // Native namespace manifests trim trailing slashes. Keep memory a
            // hierarchical URL after trimming, with one backing store per owner.
            "memory://storage-sdk".to_owned()
        } else {
            // Native dispatch treats any leading `db` as a cloud route. The
            // portable configuration owns filesystem paths, including new
            // relative directories, so resolve without requiring existence.
            std::path::absolute(config.path())
                .map_err(|error| lancedb::Error::Runtime {
                    message: format!("local database path resolution failed: {error}"),
                })?
                .to_string_lossy()
                .into_owned()
        };
        let namespace_connection = lancedb::connect(&uri)
            .session(namespace_session)
            .execute()
            .await?;
        let namespace = namespace_connection.namespace_client().await?;
        let (implementation, properties) = namespace_connection.namespace_client_config().await?;
        // Table registry keys own namespace accessors. A separate namespace
        // Session prevents those keys from referring back to their own Session.
        let database = LanceNamespaceDatabase::from_namespace_client(
            namespace,
            implementation,
            properties,
            HashMap::new(),
            None,
            Some(table_session),
            HashSet::new(),
        );
        Ok(Connection::new(
            Arc::new(database),
            Arc::new(MemoryRegistry::new()),
        ))
    }
}

impl LanceManager {
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
        error: impl fmt::Display,
    ) -> StorageError {
        self.runtime.map_err(operation, error)
    }

    fn enforce_required_capabilities(&self) -> Result<(), StorageError> {
        self.runtime.enforce_required_capabilities()
    }
}

impl LanceManager {
    pub(super) fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(super) async fn namespace_exists(
        &self,
        namespace: &[String],
    ) -> Result<bool, StorageError> {
        for (index, name) in namespace.iter().enumerate() {
            if !self.namespace_contains(&namespace[..index], name).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) async fn ensure_namespace(&self, namespace: &[String]) -> Result<(), StorageError> {
        // Catalog creation calls this while the existing mutation owner holds
        // admission. Reads never create the native namespace manifest.
        for (index, name) in namespace.iter().enumerate() {
            let parent = &namespace[..index];
            if self.namespace_contains(parent, name).await? {
                continue;
            }
            // LanceDB exposes the operation but does not re-export its request
            // type. Infer that native DTO without adding a direct dependency.
            let request = serde_json::from_value(serde_json::json!({
                "id": &namespace[..=index],
            }))
            .map_err(|error| StorageError::Serialization(error.to_string()))?;
            // Native namespace creation has no idempotent mode. Accept a
            // competing creator only after observing this exact child;
            // a failed observation must not erase the creation failure.
            if let Err(error) = self.connection.create_namespace(request).await
                && !matches!(self.namespace_contains(parent, name).await, Ok(true))
            {
                return Err(self.map_err("create_namespace", error));
            }
        }
        Ok(())
    }

    async fn namespace_contains(
        &self,
        parent: &[String],
        name: &str,
    ) -> Result<bool, StorageError> {
        let mut page_token = None;
        let mut seen_tokens = BTreeSet::new();
        loop {
            let request = serde_json::from_value(serde_json::json!({
                "id": parent,
                "limit": 256,
                "page_token": page_token,
            }))
            .map_err(|error| StorageError::Serialization(error.to_string()))?;
            let response = self
                .connection
                .list_namespaces(request)
                .await
                .map_err(|error| self.map_err("list_namespaces", error))?;
            if response.namespaces.iter().any(|child| child == name) {
                return Ok(true);
            }
            let Some(next) = response.page_token.filter(|token| !token.is_empty()) else {
                return Ok(false);
            };
            if !seen_tokens.insert(next.clone()) {
                return Err(StorageError::backend_op(
                    "list_namespaces",
                    "LanceDB namespace pagination repeated a continuation token",
                ));
            }
            page_token = Some(next);
        }
    }

    pub(super) fn mutation_process_lock(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.mutation_lock)
    }

    fn mutation_lock(config: &LanceConfig) -> Result<Arc<tokio::sync::Mutex<()>>, StorageError> {
        // Memory connections are independent stores; clones share this lock.
        if config.path() == std::path::Path::new("memory://") {
            return Ok(Arc::new(tokio::sync::Mutex::new(())));
        }
        static LOCKS: OnceLock<Mutex<BTreeMap<PathBuf, Weak<tokio::sync::Mutex<()>>>>> =
            OnceLock::new();
        let path = std::fs::canonicalize(config.path())
            .map_err(|error| StorageError::backend_op("connect", error.to_string()))?;
        let mut locks = LOCKS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| {
                StorageError::backend_op("connect", "LanceDB mutation lock registry is poisoned")
            })?;
        locks.retain(|_, lock| lock.strong_count() != 0);
        if let Some(lock) = locks.get(&path).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(path, Arc::downgrade(&lock));
        Ok(lock)
    }

    pub(super) fn config(&self) -> &LanceConfig {
        &self.config
    }
}

impl LanceManager {
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
