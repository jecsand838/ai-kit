use crate::StateStoreError;

use super::store::SqliteErrorContext;

pub(crate) struct SqliteSchema;

impl SqliteSchema {
    pub(crate) async fn initialize(
        conn: &turso::Connection,
        operation_timeout_ms: u64,
    ) -> Result<(), StateStoreError> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS state_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision > 0),
                value_json TEXT,
                deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key),
                CHECK ((deleted = 1 AND value_json IS NULL) OR (deleted = 0 AND value_json IS NOT NULL))
            );
            CREATE INDEX IF NOT EXISTS state_records_list_idx
                ON state_records(namespace, deleted, key);",
        )
        .await
        .map_err(|error| {
            SqliteErrorContext::new("schema", operation_timeout_ms).map_turso(error)
        })
    }
}
