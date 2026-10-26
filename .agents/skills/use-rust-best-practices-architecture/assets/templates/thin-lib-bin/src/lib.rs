pub mod config;
mod service;

use config::Config;

pub async fn run(config: Config) -> anyhow::Result<()> {
    let app = service::App::new(config)?;
    app.run().await
}
