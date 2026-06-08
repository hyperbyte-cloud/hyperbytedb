#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use clap::{Parser, Subcommand};

use hyperbytedb::application::backup::{backup, restore};
use hyperbytedb::application::runtime::{init_tracing, serve};
use hyperbytedb::config::HyperbytedbConfig;

#[derive(Parser)]
#[command(
    name = "hyperbytedb",
    version,
    about = "InfluxDB v1-compatible TSDB backed by embedded ClickHouse + Parquet"
)]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Hyperbytedb server
    Serve,
    /// Create a backup
    Backup {
        #[arg(long)]
        output: String,
    },
    /// Restore from a backup
    Restore {
        #[arg(long)]
        input: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = HyperbytedbConfig::load(Some(&cli.config))?;

    let _otel_guard = init_tracing(&config.logging)?;

    match cli.command {
        Commands::Serve => serve(config).await,
        Commands::Backup { output } => backup(config, &output).await,
        Commands::Restore { input } => restore(config, &input).await,
    }
}
