use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

pub async fn connect_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .min_connections(4)
        .max_connections(32)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
}
