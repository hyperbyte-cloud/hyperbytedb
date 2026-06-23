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

    match cli.command {
        Commands::Serve => serve(config).await,
        Commands::Backup { output } => backup(config, &output).await,
        Commands::Restore { input } => restore(config, &input).await,
    }
}
