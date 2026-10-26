use clap::Parser;
use my_app::{config::Config, run};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    config: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::from_path(&cli.config)?;
    run(config).await
}
