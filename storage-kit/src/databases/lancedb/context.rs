use crate::{
    CollectionInfo, FilterExpr, PageToken, Projection, RecordsPage, StorageError, StorageLocator,
};

use super::{codec, manager::LanceManager, mapper};

impl LanceManager {
    fn ensure_public_collection(&self, locator: &StorageLocator) -> Result<(), StorageError> {
        if locator
            .collection
            .eq_ignore_ascii_case(crate::state_stores::lance::STATE_TABLE)
        {
            return Err(StorageError::InvalidRequest(
                "the StateStore collection is reserved for transactional state".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn table_ops<'a>(&'a self, locator: &'a StorageLocator) -> LanceTableOps<'a> {
        LanceTableOps { db: self, locator }
    }

    pub(crate) fn resolved_name(&self, locator: &StorageLocator) -> LanceName {
        let mut namespace_path = locator.namespace.clone();
        if let Some(database) = &locator.database {
            namespace_path.push(database.clone());
        }
        LanceName {
            namespace_path,
            physical_name: locator.collection.clone(),
        }
    }

    pub(crate) fn default_vector_column<'a>(
        &'a self,
        requested: Option<&'a String>,
    ) -> Result<&'a str, StorageError> {
        if let Some(column) = requested {
            return Ok(column.as_str());
        }
        self.config()
            .default_vector_column
            .as_deref()
            .ok_or_else(|| {
                self.unsupported(
                    "VECTOR_COLUMN_SELECTION",
                    "no vector field was selected and no default vector column is configured",
                )
            })
    }

    pub(crate) async fn open_table(
        &self,
        locator: &StorageLocator,
    ) -> Result<lancedb::table::Table, StorageError> {
        self.ensure_public_collection(locator)?;
        let resolved = self.resolved_name(locator);
        self.connection()
            .open_table(resolved.physical_name)
            .namespace(resolved.namespace_path)
            .execute()
            .await
            .map_err(|error| self.map_err("open_table", error))
    }

    pub(crate) async fn describe_collection(
        &self,
        locator: &StorageLocator,
    ) -> Result<CollectionInfo, StorageError> {
        let table = self.open_table(locator).await?;
        self.collection_info_from_table(locator.clone(), &table)
            .await
    }

    pub(crate) async fn collection_info_from_table(
        &self,
        locator: StorageLocator,
        table: &lancedb::table::Table,
    ) -> Result<CollectionInfo, StorageError> {
        let schema = table
            .schema()
            .await
            .map_err(|error| self.map_err("schema", error))?;
        let record_count = table
            .count_rows(None)
            .await
            .map(|rows| rows as u64)
            .map_err(|error| self.map_err("count_rows", error))?;
        Ok(CollectionInfo {
            locator,
            schema: Some(codec::collection_schema_from_arrow(schema.as_ref())),
            record_count: Some(record_count),
        })
    }

    pub(crate) async fn list_collection_infos(
        &self,
        namespace: Vec<String>,
    ) -> Result<Vec<CollectionInfo>, StorageError> {
        if !self.namespace_exists(&namespace).await? {
            return Ok(Vec::new());
        }
        let names = self
            .connection()
            .table_names()
            .namespace(namespace.clone())
            .execute()
            .await
            .map_err(|error| self.map_err("table_names", error))?;
        Ok(names
            .into_iter()
            .filter(|name| !name.eq_ignore_ascii_case(crate::state_stores::lance::STATE_TABLE))
            .map(|collection| CollectionInfo {
                locator: StorageLocator {
                    namespace: namespace.clone(),
                    database: None,
                    collection,
                },
                schema: None,
                record_count: None,
            })
            .collect())
    }

    pub(crate) async fn collection_exists(
        &self,
        locator: &StorageLocator,
    ) -> Result<bool, StorageError> {
        self.ensure_public_collection(locator)?;
        let resolved = self.resolved_name(locator);
        if !self.namespace_exists(&resolved.namespace_path).await? {
            return Ok(false);
        }
        let names = self
            .connection()
            .table_names()
            .namespace(resolved.namespace_path)
            .execute()
            .await
            .map_err(|error| self.map_err("table_names", error))?;
        Ok(names.iter().any(|name| name == &resolved.physical_name))
    }
}

pub(crate) struct LanceName {
    pub(crate) namespace_path: Vec<String>,
    pub(crate) physical_name: String,
}

pub(crate) struct LanceTableOps<'a> {
    db: &'a LanceManager,
    locator: &'a StorageLocator,
}

impl LanceTableOps<'_> {
    pub(crate) async fn open(&self) -> Result<lancedb::table::Table, StorageError> {
        self.db.open_table(self.locator).await
    }

    pub(crate) async fn count_rows(
        &self,
        filter: Option<&FilterExpr>,
    ) -> Result<u64, StorageError> {
        let table = self.open().await?;
        let schema = table
            .schema()
            .await
            .map_err(|error| self.db.map_err("schema", error))?;
        let filter_sql = mapper::LanceExpressions::new(schema).optional(filter)?;
        table
            .count_rows(filter_sql)
            .await
            .map(|count| count as u64)
            .map_err(|error| self.db.map_err("count_rows", error))
    }

    pub(crate) async fn plain_page(
        &self,
        filter: Option<&FilterExpr>,
        projection: &Projection,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<RecordsPage, StorageError> {
        use futures::TryStreamExt;
        use lancedb::query::ExecutableQuery;

        let table = self.open().await?;
        let schema = table
            .schema()
            .await
            .map_err(|error| self.db.map_err("schema", error))?;
        let filter_sql = mapper::LanceExpressions::new(schema).optional(filter)?;
        let query = mapper::apply_common_query_options(
            table.query(),
            mapper::CommonQueryOptions {
                projection,
                filter_sql: filter_sql.as_deref(),
                limit,
                offset,
            },
        );
        let batches = query
            .execute()
            .await
            .map_err(|error| self.db.map_err("query.execute", error))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| self.db.map_err("query.try_collect", error))?;
        let mut page = codec::decode_batches(&batches)?;
        page.next_page = PageToken::next_offset(limit, offset, page.records.len());
        Ok(page)
    }
}
