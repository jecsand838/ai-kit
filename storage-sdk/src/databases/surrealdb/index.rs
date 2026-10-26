use std::time::Duration;

use serde_json::Value;
use tokio::time::{Instant, sleep};

use crate::{
    CreateIndexRequest, DistanceMetric, IndexKind, IndexRequest, IndexResponse, StorageError,
    StorageLocator, WaitForIndexRequest,
};

use super::{manager::SurrealManager, mapper};

pub(super) async fn handle(
    db: &SurrealManager,
    request: IndexRequest,
) -> Result<IndexResponse, StorageError> {
    match request {
        IndexRequest::Create(request) => create(db, request).await,
        IndexRequest::List { locator } => {
            Ok(IndexResponse::Indexes(db.list_index_infos(&locator).await?))
        }
        IndexRequest::Drop { locator, name } => drop_index(db, locator, name).await,
        IndexRequest::WaitFor(request) => wait_for_index(db, request).await,
    }
}

async fn create(
    db: &SurrealManager,
    request: CreateIndexRequest,
) -> Result<IndexResponse, StorageError> {
    db.ensure_mutable("create_index")?;
    if request.columns.is_empty() {
        return Err(StorageError::InvalidConfig(
            "SurrealDB index creation requires at least one column".into(),
        ));
    }
    let table = db.resolve_table(&request.locator)?;
    let columns = request
        .columns
        .iter()
        .map(|name| mapper::quote_ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    let index_name = format!(
        "idx_{}_{}",
        request.locator.collection,
        request.columns.join("_")
    );
    let tail = match request.kind {
        IndexKind::Scalar => String::new(),
        IndexKind::FullText => format!(
            "FULLTEXT ANALYZER {} BM25",
            SurrealManager::FULL_TEXT_ANALYZER
        ),
        IndexKind::Vector {
            dimension,
            distance,
        } => {
            if dimension == 0 {
                return Err(StorageError::InvalidConfig(
                    "vector index dimension must be positive".into(),
                ));
            }
            let mut tail = vec!["HNSW".to_owned(), format!("DIMENSION {dimension}")];
            match distance {
                None => {}
                Some(DistanceMetric::L2) => tail.push("DIST EUCLIDEAN".into()),
                Some(DistanceMetric::Cosine) => tail.push("DIST COSINE".into()),
                Some(DistanceMetric::Dot) => {
                    return Err(db.unsupported(
                        "DOT_PRODUCT_VECTOR_INDEX",
                        "SurrealDB vector indexes do not expose portable dot-product indexing",
                    ));
                }
                Some(DistanceMetric::Hamming) => {
                    return Err(db.unsupported(
                        "HAMMING_VECTOR_INDEX",
                        "SurrealDB HNSW indexes do not support Hamming distance",
                    ));
                }
            }
            tail.join(" ")
        }
    };
    let sql = format!(
        "DEFINE INDEX IF NOT EXISTS {} ON TABLE {table} FIELDS {columns} {tail};",
        mapper::quote_ident(&index_name)
    );
    if matches!(request.kind, IndexKind::FullText) {
        db.create_full_text_index(&request.locator, sql).await?;
    } else {
        let _: Value = db.run_query_value(Some(&request.locator), sql).await?;
    }
    if let Some(timeout_ms) = request.wait_timeout_ms {
        wait_for_indexes(
            db,
            &request.locator,
            std::slice::from_ref(&index_name),
            timeout_ms,
        )
        .await?;
    }
    Ok(IndexResponse::Completed)
}

impl SurrealManager {
    const FULL_TEXT_ANALYZER: &str = "storage_sdk_full_text_v1";

    async fn create_full_text_index(
        &self,
        locator: &StorageLocator,
        index_sql: String,
    ) -> Result<(), StorageError> {
        // One bounded, versioned analyzer belongs to the selected database.
        // Never overwrite an existing caller-owned definition at this name.
        let sql = format!(
            "BEGIN TRANSACTION; LET $catalog = (INFO FOR DB STRUCTURE); \
             LET $existing = $catalog.analyzers[WHERE name = $analyzer_name]; \
             IF $existing != [] AND $existing != [$analyzer_definition] {{ \
                 THROW 'full-text analyzer name is occupied by a different definition'; \
             }}; \
             DEFINE ANALYZER IF NOT EXISTS {} TOKENIZERS class FILTERS lowercase COMMENT 'storage-sdk full-text v1'; \
             COMMIT TRANSACTION;",
            Self::FULL_TEXT_ANALYZER,
        );
        let bindings = std::collections::BTreeMap::from([
            (
                "analyzer_name".to_owned(),
                serde_json::json!(Self::FULL_TEXT_ANALYZER),
            ),
            (
                "analyzer_definition".to_owned(),
                serde_json::json!({
                    "name": Self::FULL_TEXT_ANALYZER,
                    "tokenizers": ["CLASS"],
                    "filters": ["LOWERCASE"],
                    "comment": "storage-sdk full-text v1",
                }),
            ),
        ]);
        self.run_response(Some(locator), sql, bindings).await?;
        // SurrealDB resolves the analyzer for a new index through committed
        // catalog state. Publish the shared definition before index creation.
        self.run_response(Some(locator), index_sql, std::collections::BTreeMap::new())
            .await?;
        Ok(())
    }
}

async fn drop_index(
    db: &SurrealManager,
    locator: StorageLocator,
    name: String,
) -> Result<IndexResponse, StorageError> {
    db.ensure_mutable("drop_index")?;
    let table = db.resolve_table(&locator)?;
    let sql = format!(
        "REMOVE INDEX {} ON TABLE {table};",
        mapper::quote_ident(&name)
    );
    let _: Value = db.run_query_value(Some(&locator), sql).await?;
    Ok(IndexResponse::Completed)
}

async fn wait_for_index(
    db: &SurrealManager,
    request: WaitForIndexRequest,
) -> Result<IndexResponse, StorageError> {
    wait_for_indexes(
        db,
        &request.locator,
        &request.index_names,
        request.timeout_ms,
    )
    .await?;
    Ok(IndexResponse::Completed)
}

async fn wait_for_indexes(
    db: &SurrealManager,
    locator: &StorageLocator,
    index_names: &[String],
    timeout_ms: u64,
) -> Result<(), StorageError> {
    if index_names.is_empty() {
        return Ok(());
    }
    let table = db.resolve_table(locator)?;
    let table_info: Value = db
        .run_query_value(Some(locator), format!("INFO FOR TABLE {table};"))
        .await?;
    let indexes = table_info
        .get("indexes")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            StorageError::Serialization(format!(
                "Surreal table info is missing indexes: {table_info}"
            ))
        })?;
    for name in index_names {
        if !indexes.contains_key(name) {
            return Err(db.map_err("wait_for_index", format!("index `{name}` not found")));
        }
    }

    let started = Instant::now();
    let timeout = Duration::from_millis(timeout_ms.max(1));
    loop {
        let mut ready = true;
        for name in index_names {
            let raw: Value = db
                .run_query_value(
                    Some(locator),
                    format!(
                        "INFO FOR INDEX {} ON TABLE {table};",
                        mapper::quote_ident(name)
                    ),
                )
                .await?;
            if let Some(status) = raw
                .get("building")
                .and_then(Value::as_object)
                .and_then(|building| building.get("status"))
                .and_then(Value::as_str)
            {
                if matches!(status.to_ascii_lowercase().as_str(), "error" | "failed") {
                    return Err(db.map_err(
                        "wait_for_index",
                        format!("index `{name}` entered an error state"),
                    ));
                }
                if !matches!(status.to_ascii_lowercase().as_str(), "ready" | "built") {
                    ready = false;
                }
            }
        }
        if ready {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(db.map_err(
                "wait_for_index",
                format!("timed out after {timeout_ms} ms waiting for {index_names:?}"),
            ));
        }
        sleep(Duration::from_millis(250)).await;
    }
}
