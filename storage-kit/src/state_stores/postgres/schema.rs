use crate::StateStoreError;

use super::store::PostgresErrorContext;

pub(crate) struct PostgresSchema;

impl PostgresSchema {
    pub(crate) async fn initialize(
        client: &tokio_postgres::Client,
        timeout_ms: u64,
    ) -> Result<(), StateStoreError> {
        client
            .batch_execute(
                "
                CREATE TABLE IF NOT EXISTS storage_kit_state_records (
                  namespace TEXT NOT NULL,
                  key TEXT NOT NULL,
                  revision BIGINT NOT NULL CHECK (revision > 0),
                  value_json TEXT,
                  deleted BOOLEAN NOT NULL,
                  created_at_ms BIGINT NOT NULL,
                  updated_at_ms BIGINT NOT NULL,
                  PRIMARY KEY(namespace, key),
                  CHECK (
                    (deleted = TRUE AND value_json IS NULL)
                    OR
                    (deleted = FALSE AND value_json IS NOT NULL)
                  )
                );

                CREATE INDEX IF NOT EXISTS storage_kit_state_records_list_idx
                  ON storage_kit_state_records(namespace, deleted, key);
                ",
            )
            .await
            .map_err(|error| PostgresErrorContext::new("schema", timeout_ms).postgres(error))?;
        Ok(())
    }
}
