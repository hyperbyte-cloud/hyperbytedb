#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

// jemalloc returns freed heap to the OS via background purging, unlike the
// default glibc malloc which pins RSS at the process's allocation peak. The
// startup series-cache warm and cold WAL→chDB replay allocate (and free) a
// large transient working set at high cardinality; under glibc that peak
// stayed resident for the life of the pod. See `bootstrap::build_services`.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand};

use hyperbytedb::application::backup::{backup, restore};
use hyperbytedb::application::runtime::serve;
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

    let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(f) => f,
        Err(_) => tracing_subscriber::EnvFilter::new(&config.logging.level),
    };

    if config.logging.format == "json" {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|e| anyhow::anyhow!("failed to set logging subscriber: {e}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|e| anyhow::anyhow!("failed to set logging subscriber: {e}"))?;
    }

    match cli.command {
        Commands::Serve => serve(config).await,
        Commands::Backup { output } => backup(config, &output).await,
        Commands::Restore { input } => restore(config, &input).await,
    }
}
