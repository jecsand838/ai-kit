use crate::core::CreateCollectionAction;
use crate::{
    CatalogRequest, CatalogResponse, ListCollectionsRequest, SchemaEvolutionRequest, StorageError,
    StorageLocator,
};

use super::{codec, manager::LanceManager};

impl LanceManager {
    pub(super) async fn catalog(
        &self,
        request: CatalogRequest,
    ) -> Result<CatalogResponse, StorageError> {
        match request {
            CatalogRequest::CreateCollection(request) => self.create_collection(request).await,
            CatalogRequest::OpenCollection { locator } => self.open_collection(locator).await,
            CatalogRequest::ListCollections(request) => self.list_collections(request).await,
            CatalogRequest::DropCollection {
                locator,
                ignore_missing,
            } => self.drop_collection(locator, ignore_missing).await,
            CatalogRequest::AlterCollection(request) => self.alter_collection(request).await,
        }
    }

    async fn create_collection(
        &self,
        request: crate::CreateCollectionRequest,
    ) -> Result<CatalogResponse, StorageError> {
        use lancedb::database::CreateTableMode;

        self.ensure_mutable("create_collection")?;
        let resolved = self.resolved_name(&request.locator);
        let exists = self.collection_exists(&request.locator).await?;
        let mode = match request.mode.resolve(exists) {
            CreateCollectionAction::ErrorExists => {
                return Err(request.already_exists_error("table"));
            }
            CreateCollectionAction::Reuse => {
                return self.open_collection(request.locator).await;
            }
            CreateCollectionAction::Recreate => CreateTableMode::Overwrite,
            CreateCollectionAction::Create => CreateTableMode::Create,
        };

        let table = if request.records.is_empty() {
            let schema = request.schema.as_ref().ok_or_else(|| {
                StorageError::InvalidConfig(
                    "schema is required when creating an empty LanceDB table".into(),
                )
            })?;
            self.ensure_namespace(&resolved.namespace_path).await?;
            self.connection()
                .create_empty_table(resolved.physical_name, codec::schema_to_arrow(schema))
                .mode(mode)
                .namespace(resolved.namespace_path)
                .execute()
                .await
                .map_err(|error| self.map_err("create_empty_table", error))?
        } else {
            if request.records.iter().any(|record| record.id.is_some()) {
                return Err(self.unsupported(
                    "LOGICAL_IDS",
                    "LanceDB does not persist portable DataRecord.id values",
                ));
            }
            let batches = codec::encode_records(&request.records, None)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| StorageError::Serialization(error.to_string()))?;
            self.ensure_namespace(&resolved.namespace_path).await?;
            self.connection()
                .create_table(resolved.physical_name, batches)
                .mode(mode)
                .namespace(resolved.namespace_path)
                .execute()
                .await
                .map_err(|error| self.map_err("create_table", error))?
        };

        Ok(CatalogResponse::Collection(
            self.collection_info_from_table(request.locator, &table)
                .await?,
        ))
    }

    async fn open_collection(
        &self,
        locator: StorageLocator,
    ) -> Result<CatalogResponse, StorageError> {
        Ok(CatalogResponse::Collection(
            self.describe_collection(&locator).await?,
        ))
    }

    async fn list_collections(
        &self,
        request: ListCollectionsRequest,
    ) -> Result<CatalogResponse, StorageError> {
        let ListCollectionsRequest {
            namespace,
            limit,
            start_after,
        } = request;
        let collections = self.list_collection_infos(namespace).await?;
        Ok(CatalogResponse::Collections(
            ListCollectionsRequest {
                namespace: Vec::new(),
                limit,
                start_after,
            }
            .apply_to_collections(collections),
        ))
    }

    async fn drop_collection(
        &self,
        locator: StorageLocator,
        ignore_missing: bool,
    ) -> Result<CatalogResponse, StorageError> {
        self.ensure_mutable("drop_collection")?;
        let resolved = self.resolved_name(&locator);
        if !self.collection_exists(&locator).await? {
            return if ignore_missing {
                Ok(CatalogResponse::Completed)
            } else {
                Err(StorageError::backend_op(
                    "drop_table",
                    format!("table `{}` not found", resolved.physical_name),
                ))
            };
        }
        self.connection()
            .drop_table(&resolved.physical_name, &resolved.namespace_path)
            .await
            .map_err(|error| self.map_err("drop_table", error))?;
        Ok(CatalogResponse::Completed)
    }

    async fn alter_collection(
        &self,
        request: SchemaEvolutionRequest,
    ) -> Result<CatalogResponse, StorageError> {
        use lancedb::table::{ColumnAlteration, NewColumnTransform};

        self.ensure_mutable("alter_collection")?;
        match request {
            SchemaEvolutionRequest::AddColumns(request) => {
                if request.read_columns.is_some() {
                    return Err(self.unsupported(
                        "SCHEMA_EVOLUTION_READ_COLUMNS",
                        "LanceDB SQL expressions determine their own source columns",
                    ));
                }
                let table = self.open_table(&request.locator).await?;
                let transforms = NewColumnTransform::SqlExpressions(
                    request
                        .expressions
                        .iter()
                        .map(|expression| {
                            (expression.name.clone(), expression.expression_sql.clone())
                        })
                        .collect(),
                );
                table
                    .add_columns()
                    .transform(transforms)
                    .execute()
                    .await
                    .map_err(|error| self.map_err("add_columns", error))?;
            }
            SchemaEvolutionRequest::AlterColumns(request) => {
                let table = self.open_table(&request.locator).await?;
                if request
                    .alterations
                    .iter()
                    .any(|alteration| alteration.cast_to.is_some())
                {
                    let dataset = table
                        .dataset()
                        .ok_or_else(|| {
                            StorageError::backend_op(
                                "alter_columns",
                                "LanceDB native dataset is unavailable",
                            )
                        })?
                        .get()
                        .await
                        .map_err(|error| self.map_err("alter_columns", error))?;
                    let tightens_nullability = request.alterations.iter().any(|alteration| {
                        alteration.nullable == Some(false)
                            && dataset
                                .schema()
                                .field(&alteration.path)
                                .is_some_and(|field| field.nullable)
                    });
                    if tightens_nullability {
                        return Err(self.unsupported(
                            "SCHEMA_EVOLUTION_CAST_NULLABILITY",
                            "LanceDB cannot atomically cast columns and make a nullable column required; issue separate requests",
                        ));
                    }
                }
                let changes = request
                    .alterations
                    .iter()
                    .map(|alteration| {
                        let mut change = ColumnAlteration::new(alteration.path.clone());
                        if let Some(rename) = &alteration.rename {
                            change = change.rename(rename.clone());
                        }
                        if let Some(nullable) = alteration.nullable {
                            change = change.set_nullable(nullable);
                        }
                        if let Some(field_type) = &alteration.cast_to {
                            change =
                                change.cast_to(codec::field_type_to_arrow_data_type(field_type));
                        }
                        Ok(change)
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?;
                table
                    .alter_columns(&changes)
                    .await
                    .map_err(|error| self.map_err("alter_columns", error))?;
            }
            SchemaEvolutionRequest::DropColumns(request) => {
                let columns = request
                    .columns
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                self.open_table(&request.locator)
                    .await?
                    .drop_columns(&columns)
                    .await
                    .map_err(|error| self.map_err("drop_columns", error))?;
            }
        }
        Ok(CatalogResponse::Completed)
    }
}
