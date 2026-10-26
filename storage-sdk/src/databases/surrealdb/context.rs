use std::{collections::BTreeMap, sync::Arc};

use serde_json::{Map, Value};
use surrealdb::types::SurrealValue;
use surrealdb::{IndexedResults as Response, Surreal, engine::any::Any};

use crate::{CollectionInfo, IndexInfo, ListCollectionsRequest, StorageError, StorageLocator};

use super::{manager::SurrealContextKey, manager::SurrealManager, mapper};

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedContext {
    namespace: String,
    database: String,
    key: Option<SurrealContextKey>,
}

impl ResolvedContext {
    fn resolve(
        db: &SurrealManager,
        locator: Option<&StorageLocator>,
    ) -> Result<Self, StorageError> {
        let mut locator = locator.cloned().unwrap_or_default();
        if let Some(default) = db.common_config().default_locator.as_ref() {
            locator = locator.with_defaults_from(default);
        }

        let namespace = locator_namespace(db, &locator)?
            .or_else(|| db.config().namespace.clone())
            .ok_or_else(|| missing_context("namespace"))?;
        let database = locator
            .database
            .or_else(|| db.config().database.clone())
            .ok_or_else(|| missing_context("database"))?;

        let key = SurrealContextKey {
            namespace: namespace.clone(),
            database: database.clone(),
        };
        Ok(Self {
            namespace,
            database,
            key: (db.config().context_key().as_ref() != Some(&key)).then_some(key),
        })
    }

    fn collection_locator(&self, collection: impl Into<String>) -> StorageLocator {
        StorageLocator::new(collection.into())
            .with_namespace([self.namespace.clone()])
            .with_database(self.database.clone())
    }
}

fn locator_namespace(
    db: &SurrealManager,
    locator: &StorageLocator,
) -> Result<Option<String>, StorageError> {
    match locator.namespace.as_slice() {
        [] => Ok(None),
        [namespace] if namespace.trim().is_empty() => Err(StorageError::InvalidConfig(
            "SurrealDB namespace segment must not be empty".into(),
        )),
        [namespace] => Ok(Some(namespace.clone())),
        namespace => Err(db.unsupported(
            "NAMESPACE_PATHS",
            format!(
                "SurrealDB exposes a single native namespace value, not a portable namespace path: {namespace:?}"
            ),
        )),
    }
}

fn missing_context(field: &str) -> StorageError {
    StorageError::InvalidConfig(format!(
        "SurrealDB operations require a {field} either in SurrealConfig.{field} or StorageLocator.{field}"
    ))
}

impl SurrealManager {
    pub(crate) fn scope<'a>(
        &'a self,
        locator: &'a StorageLocator,
    ) -> Result<SurrealScope<'a>, StorageError> {
        Ok(SurrealScope {
            db: self,
            locator,
            table: self.resolve_table(locator)?,
        })
    }

    pub(crate) fn resolve_table(&self, locator: &StorageLocator) -> Result<String, StorageError> {
        let table = mapper::table_name(locator);
        let table = table.trim();
        if table == Self::PARTITION_REPLACEMENT_GUARD_TABLE {
            return Err(StorageError::InvalidRequest(format!(
                "SurrealDB collection name `{table}` is reserved for atomic partition replacement coordination"
            )));
        }
        mapper::sanitize_identifier(table)
    }

    async fn client_for_locator(
        &self,
        locator: Option<&StorageLocator>,
    ) -> Result<Arc<Surreal<Any>>, StorageError> {
        match locator {
            None => Ok(Arc::clone(self.root_client())),
            Some(_) => {
                let resolved = ResolvedContext::resolve(self, locator)?;
                self.client_for_resolved_context(&resolved).await
            }
        }
    }

    async fn client_for_resolved_context(
        &self,
        resolved: &ResolvedContext,
    ) -> Result<Arc<Surreal<Any>>, StorageError> {
        let Some(key) = resolved.key.as_ref() else {
            return Ok(Arc::clone(self.root_client()));
        };

        {
            let cache = self
                .context_clients()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(client) = cache.get(key) {
                return Ok(Arc::clone(client));
            }
        }

        let client = Arc::new(self.root_client().as_ref().clone());
        SurrealManager::select_client_context(client.as_ref(), self.common_config(), key).await?;

        let mut cache = self
            .context_clients()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(Arc::clone(cache.entry(key.clone()).or_insert(client)))
    }

    pub(crate) async fn ensure_namespace_database(
        &self,
        locator: Option<&StorageLocator>,
    ) -> Result<(), StorageError> {
        let resolved = ResolvedContext::resolve(self, locator)?;
        let namespace = mapper::quote_ident(&resolved.namespace);
        let database = mapper::quote_ident(&resolved.database);
        let sql = format!(
            "DEFINE NAMESPACE IF NOT EXISTS {namespace}; USE NS {namespace}; DEFINE DATABASE IF NOT EXISTS {database};"
        );
        let _ = self.run_response(None, sql, Default::default()).await?;
        Ok(())
    }

    async fn table_inventory_for_context(
        &self,
        resolved: &ResolvedContext,
    ) -> Result<Map<String, Value>, StorageError> {
        let client = self.client_for_resolved_context(resolved).await?;
        let mut response = self
            .execute_response(client, "INFO FOR DB;".to_owned(), BTreeMap::new())
            .await?;
        let raw = Self::take_query_value(&mut response, 0)?;
        raw.get("tables")
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| {
                self.map_err(
                    "info_for_db",
                    format!("unexpected INFO FOR DB response: {raw}"),
                )
            })
    }

    pub(crate) async fn table_exists(
        &self,
        locator: &StorageLocator,
    ) -> Result<bool, StorageError> {
        let resolved = ResolvedContext::resolve(self, Some(locator))?;
        Ok(self
            .table_inventory_for_context(&resolved)
            .await?
            .contains_key(&mapper::table_name(locator)))
    }

    fn build_collection_info(
        &self,
        locator: StorageLocator,
        record_count: Option<u64>,
    ) -> CollectionInfo {
        CollectionInfo {
            locator,
            schema: None,
            record_count,
        }
    }

    pub(crate) fn extract_count(&self, value: &Value) -> Result<u64, StorageError> {
        extract_count_value(value)
    }

    pub(crate) fn affected_row_count(&self, value: &Value) -> Result<u64, StorageError> {
        affected_row_count_value(value)
    }

    pub(crate) async fn open_collection_info(
        &self,
        locator: StorageLocator,
    ) -> Result<CollectionInfo, StorageError> {
        Ok(self.build_collection_info(locator.clone(), Some(self.count_rows(&locator).await?)))
    }

    pub(crate) async fn list_collection_infos(
        &self,
        req: &ListCollectionsRequest,
    ) -> Result<Vec<CollectionInfo>, StorageError> {
        let scope = (!req.namespace.is_empty())
            .then(|| StorageLocator::default().with_namespace(req.namespace.clone()));
        let resolved = ResolvedContext::resolve(self, scope.as_ref())?;
        let mut entries = self
            .table_inventory_for_context(&resolved)
            .await?
            .into_iter()
            .filter(|(table_name, _)| table_name != Self::PARTITION_REPLACEMENT_GUARD_TABLE)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(req.apply_to_collections(
            entries
                .into_iter()
                .map(|(table_name, _)| {
                    self.build_collection_info(
                        resolved.collection_locator(table_name.clone()),
                        None,
                    )
                })
                .collect(),
        ))
    }

    pub(crate) async fn list_index_infos(
        &self,
        locator: &StorageLocator,
    ) -> Result<Vec<IndexInfo>, StorageError> {
        let table = self.resolve_table(locator)?;
        let sql = format!("INFO FOR TABLE {table};");
        let raw = self.run_query_value(Some(locator), sql).await?;
        index_infos_from_table_info(&raw)
    }

    pub(crate) async fn count_rows(&self, locator: &StorageLocator) -> Result<u64, StorageError> {
        let table = self.resolve_table(locator)?;
        let sql = format!("SELECT count() AS count FROM {table} GROUP ALL;");
        let value = self.run_query_value(Some(locator), sql).await?;
        self.extract_count(&value)
    }

    pub(crate) fn consume_response_errors(
        op: &'static str,
        response: &mut Response,
    ) -> Result<(), StorageError> {
        let errors = response.take_errors();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(StorageError::backend_op(op, format!("{errors:?}")))
        }
    }

    pub(crate) async fn run_response(
        &self,
        locator: Option<&StorageLocator>,
        sql: impl Into<String>,
        bindings: BTreeMap<String, Value>,
    ) -> Result<Response, StorageError> {
        self.execute_response(
            self.client_for_locator(locator).await?,
            sql.into(),
            bindings,
        )
        .await
    }

    pub(super) async fn run_consuming_statement_errors(
        &self,
        locator: Option<&StorageLocator>,
        sql: impl Into<String>,
        bindings: BTreeMap<String, Value>,
    ) -> Result<Vec<(usize, surrealdb::Error)>, StorageError> {
        let mut response = self
            .execute_response_unchecked(
                self.client_for_locator(locator).await?,
                sql.into(),
                bindings,
            )
            .await?;
        let mut errors = response.take_errors().into_iter().collect::<Vec<_>>();
        errors.sort_by_key(|(statement_index, _)| *statement_index);
        Ok(errors)
    }

    async fn execute_response(
        &self,
        client: Arc<Surreal<Any>>,
        sql: String,
        bindings: BTreeMap<String, Value>,
    ) -> Result<Response, StorageError> {
        let mut response = self
            .execute_response_unchecked(client, sql, bindings)
            .await?;
        Self::consume_response_errors("query", &mut response)?;
        Ok(response)
    }

    async fn execute_response_unchecked(
        &self,
        client: Arc<Surreal<Any>>,
        sql: String,
        bindings: BTreeMap<String, Value>,
    ) -> Result<Response, StorageError> {
        self.with_request_timeout("query", "executing SurrealDB query", async move {
            let query = client.query(sql).bind(bindings);
            query
                .await
                .map_err(|err| StorageError::backend_op("query", err.to_string()))
        })
        .await
    }

    pub(crate) async fn run_query_take(
        &self,
        locator: Option<&StorageLocator>,
        sql: impl Into<String>,
        bindings: BTreeMap<String, Value>,
        statement_index: usize,
    ) -> Result<Value, StorageError> {
        let mut response = self.run_response(locator, sql, bindings).await?;
        Self::take_query_value(&mut response, statement_index)
    }

    fn take_query_value(
        response: &mut Response,
        statement_index: usize,
    ) -> Result<Value, StorageError> {
        let value = response
            .take::<surrealdb::types::Value>(statement_index)
            .map_err(|err| StorageError::backend_op("query.take", err.to_string()))?;
        serde_json::Value::from_value(value)
            .map_err(|err| StorageError::backend_op("query.take", err.to_string()))
    }

    pub(crate) async fn run_query_value(
        &self,
        locator: Option<&StorageLocator>,
        sql: impl Into<String>,
    ) -> Result<Value, StorageError> {
        self.run_query_take(locator, sql, BTreeMap::new(), 0).await
    }
}

fn extract_count_value(value: &Value) -> Result<u64, StorageError> {
    match value {
        Value::Array(rows) => rows
            .last()
            .and_then(|row| row.get("count"))
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                StorageError::Serialization(format!(
                    "Surreal count response is missing numeric `count`: {value}"
                ))
            }),
        Value::Object(obj) => obj.get("count").and_then(Value::as_u64).ok_or_else(|| {
            StorageError::Serialization(format!(
                "Surreal count response object is missing numeric `count`: {value}"
            ))
        }),
        _ => Err(StorageError::Serialization(format!(
            "Surreal count response must be an array or object, got {value}"
        ))),
    }
}

fn affected_row_count_value(value: &Value) -> Result<u64, StorageError> {
    match value {
        Value::Array(values) => Ok(values.len() as u64),
        _ => Err(StorageError::Serialization(format!(
            "Surreal mutation response must be an array, got {value}"
        ))),
    }
}

fn index_infos_from_table_info(raw: &Value) -> Result<Vec<IndexInfo>, StorageError> {
    raw.get("indexes")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            StorageError::Serialization(format!(
                "Surreal `INFO FOR TABLE` response is missing `indexes`: {raw}"
            ))
        })
        .map(|map| {
            map.iter()
                .map(|(name, _value)| IndexInfo {
                    name: Some(name.clone()),
                    columns: Vec::new(),
                    ready: None,
                })
                .collect()
        })
}

pub(crate) struct SurrealScope<'a> {
    db: &'a SurrealManager,
    locator: &'a StorageLocator,
    table: String,
}

impl SurrealScope<'_> {
    pub(crate) fn table(&self) -> &str {
        &self.table
    }

    pub(crate) async fn run_value(&self, sql: String) -> Result<Value, StorageError> {
        self.db.run_query_value(Some(self.locator), sql).await
    }

    pub(crate) async fn run_take(
        &self,
        sql: String,
        binds: BTreeMap<String, Value>,
        take: usize,
    ) -> Result<Value, StorageError> {
        self.db
            .run_query_take(Some(self.locator), sql, binds, take)
            .await
    }
}
