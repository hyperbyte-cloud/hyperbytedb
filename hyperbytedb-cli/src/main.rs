#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use std::io::{self, Read};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use hyperbytedb_cli::{
    CliError, ConnectionConfig, HyperbytedbClient, Session,
    client::{PingInfo, WriteOptions},
    config::{ConnectionConfig as Cfg, resolve_host},
    export::{self, ExportOptions},
    import::{self, ImportOptions},
    repl,
    session::OutputFormat,
};

#[derive(Clone, ValueEnum)]
enum FormatArg {
    Json,
    Csv,
    Column,
}

impl From<FormatArg> for OutputFormat {
    fn from(f: FormatArg) -> Self {
        match f {
            FormatArg::Json => OutputFormat::Json,
            FormatArg::Csv => OutputFormat::Csv,
            FormatArg::Column => OutputFormat::Column,
        }
    }
}

#[derive(Parser)]
#[command(
    name = "hyperbytedb-cli",
    disable_version_flag = true,
    about = "Interactive CLI client for HyperbyteDB (InfluxDB v1-compatible)",
    after_help = "Default mode opens an interactive TimeseriesQL shell.\n\
                  Admin/schema operations use TimeseriesQL (CREATE DATABASE, SHOW USERS, etc.).\n\
                  Backup/restore is server-local: hyperbytedb backup --output /path"
)]
struct Cli {
    /// Server host URL or hostname
    #[arg(short = 'H', long = "host")]
    host: Option<String>,

    /// Server port (when host is not a full URL)
    #[arg(long = "port")]
    port: Option<u16>,

    /// Default database
    #[arg(short = 'd', long = "database")]
    database: Option<String>,

    /// Username
    #[arg(short = 'u', long = "username")]
    username: Option<String>,

    /// Password (empty prompts interactively)
    #[arg(short = 'p', long = "password")]
    password: Option<String>,

    /// Use HTTPS
    #[arg(long = "ssl")]
    ssl: bool,

    /// Skip TLS certificate verification
    #[arg(long = "unsafeSsl")]
    unsafe_ssl: bool,

    /// Config profile name from ~/.config/hyperbytedb/config.toml
    #[arg(long = "profile")]
    profile: Option<String>,

    /// Execute TimeseriesQL and exit (batch mode)
    #[arg(short = 'e', long = "execute")]
    execute: Option<String>,

    /// Output format
    #[arg(short = 'f', long = "format", value_enum)]
    format: Option<FormatArg>,

    /// Timestamp precision / epoch param for queries
    #[arg(long = "precision")]
    precision: Option<String>,

    #[arg(long = "epoch")]
    epoch: Option<String>,

    /// Pretty-print JSON output
    #[arg(long = "pretty")]
    pretty: bool,

    /// Verbose HTTP tracing
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    /// Print CLI and server version, then exit
    #[arg(long = "version")]
    show_version: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Write line protocol from stdin or a file
    Write {
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,
        #[arg(short = 'd', long = "database")]
        database: Option<String>,
        #[arg(long = "rp")]
        rp: Option<String>,
        #[arg(long = "precision")]
        precision: Option<String>,
        #[arg(long = "gzip")]
        gzip: bool,
    },
    /// Import DDL+DML file (Influx-compatible format)
    Import {
        #[arg(long = "path")]
        path: String,
        #[arg(long = "compressed")]
        compressed: bool,
        #[arg(long = "pps", default_value = "0")]
        pps: u64,
        #[arg(long = "precision")]
        precision: Option<String>,
    },
    /// Export database to DDL+DML line protocol
    Export {
        #[arg(short = 'd', long = "database")]
        database: String,
        #[arg(long = "rp")]
        rp: Option<String>,
        #[arg(long = "start")]
        start: Option<String>,
        #[arg(long = "end")]
        end: Option<String>,
        #[arg(short = 'o', long = "out")]
        output: Option<String>,
        #[arg(long = "compress")]
        compress: bool,
    },
    /// Ping server and show version
    Ping,
    /// Show health endpoints
    Health {
        #[arg(long = "ready")]
        ready: bool,
    },
    /// Fetch Prometheus metrics
    Metrics,
    /// Show recent query statement summary
    Statements,
    /// Cluster administration
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    /// Schema administration (InfluxDB v1-compatible shortcuts)
    Create {
        #[command(subcommand)]
        target: CreateTarget,
    },
}

#[derive(Subcommand)]
enum CreateTarget {
    /// Create a new database
    Database {
        /// Database name
        name: String,
    },
}

#[derive(Subcommand)]
enum ClusterAction {
    /// List cluster nodes
    Nodes,
    /// Show Raft leader
    Leader,
    /// Cluster metrics
    Metrics,
    /// Initiate graceful drain (admin)
    Drain {
        #[arg(long = "yes", action = clap::ArgAction::SetTrue)]
        confirm: bool,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> hyperbytedb_cli::error::Result<()> {
    let cli = Cli::parse();

    if cli.show_version {
        println!("hyperbytedb-cli {}", env!("CARGO_PKG_VERSION"));
        let conn = build_connection(&cli)?;
        let client = HyperbytedbClient::new(&conn)?;
        match client.ping().await {
            Ok(ping) => {
                if let Some(v) = ping.version {
                    println!("server: {v}");
                }
            }
            Err(e) => eprintln!("server ping failed: {e}"),
        }
        return Ok(());
    }

    let conn = build_connection(&cli)?;

    if let Some(cmd) = cli.command {
        return run_subcommand(&conn, cmd).await;
    }

    if let Some(ref q) = cli.execute {
        let session = build_session(&cli, conn);
        let client = HyperbytedbClient::new(&session.connection)?;
        return repl::execute_query(&session, &client, q).await;
    }

    let session = build_session(&cli, conn);
    repl::run_repl(session).await
}

fn build_connection(cli: &Cli) -> hyperbytedb_cli::error::Result<ConnectionConfig> {
    let mut conn = Cfg::load(cli.profile.as_deref())?;

    if let Some(ref h) = cli.host {
        conn.host = resolve_host(Some(h), cli.port, cli.ssl || conn.ssl);
    } else if cli.port.is_some() {
        conn.host = resolve_host(None, cli.port, cli.ssl || conn.ssl);
    }

    if cli.ssl {
        conn.ssl = true;
    }
    if cli.unsafe_ssl {
        conn.unsafe_ssl = true;
    }
    if cli.database.is_some() {
        conn.database = cli.database.clone();
    }
    if cli.username.is_some() {
        conn.username = cli.username.clone();
    }
    if let Some(ref pw) = cli.password {
        if pw.is_empty() {
            conn.password = Some(prompt_password()?);
        } else {
            conn.password = Some(pw.clone());
        }
    }

    Ok(conn)
}

fn build_session(cli: &Cli, conn: ConnectionConfig) -> Session {
    let mut session = Session::new(conn);
    if let Some(ref f) = cli.format {
        session.format = OutputFormat::from(f.clone());
    }
    session.epoch = cli.epoch.clone().or_else(|| cli.precision.clone());
    session.pretty = cli.pretty;
    session.verbose = cli.verbose;
    session
}

async fn run_subcommand(
    conn: &ConnectionConfig,
    cmd: Commands,
) -> hyperbytedb_cli::error::Result<()> {
    let client = HyperbytedbClient::new(conn)?;

    match cmd {
        Commands::Write {
            file,
            database,
            rp,
            precision,
            gzip,
        } => {
            let db = database
                .or_else(|| conn.database.clone())
                .ok_or_else(|| CliError::Other("--database is required".to_string()))?;
            let body = if let Some(path) = file {
                std::fs::read(&path)
                    .map_err(|e| CliError::Write(format!("read {}: {e}", path.display())))?
            } else {
                let mut buf = Vec::new();
                io::stdin()
                    .read_to_end(&mut buf)
                    .map_err(|e| CliError::Write(e.to_string()))?;
                buf
            };
            let wopts = WriteOptions {
                db,
                rp,
                precision,
                gzip,
            };
            client.write(&body, &wopts).await
        }
        Commands::Import {
            path,
            compressed,
            pps,
            precision,
        } => {
            let opts = ImportOptions {
                path,
                compressed,
                pps,
                precision,
            };
            import::run_import(&client, &opts).await.map(|_| ())
        }
        Commands::Export {
            database,
            rp,
            start,
            end,
            output,
            compress,
        } => {
            let opts = ExportOptions {
                database,
                retention_policy: rp,
                start,
                end,
                output,
                compress,
            };
            export::run_export(&client, &opts).await.map(|_| ())
        }
        Commands::Ping => {
            let info = client.ping().await?;
            print_ping(&info);
            Ok(())
        }
        Commands::Health { ready } => {
            let body = if ready {
                client.health_ready().await?
            } else {
                client.health().await?
            };
            println!("{body}");
            Ok(())
        }
        Commands::Metrics => {
            println!("{}", client.metrics().await?);
            Ok(())
        }
        Commands::Statements => {
            println!("{}", client.statements().await?);
            Ok(())
        }
        Commands::Cluster { action } => match action {
            ClusterAction::Nodes => {
                println!("{}", client.cluster_nodes().await?);
                Ok(())
            }
            ClusterAction::Leader => {
                println!("{}", client.cluster_leader().await?);
                Ok(())
            }
            ClusterAction::Metrics => {
                println!("{}", client.cluster_metrics().await?);
                Ok(())
            }
            ClusterAction::Drain { confirm } => {
                if !confirm {
                    return Err(CliError::Other(
                        "cluster drain requires --yes to confirm".to_string(),
                    ));
                }
                println!("{}", client.cluster_drain().await?);
                Ok(())
            }
        },
        Commands::Create { target } => match target {
            CreateTarget::Database { name } => {
                let session = Session::new(conn.clone());
                let q = format!("CREATE DATABASE {name}");
                repl::execute_query(&session, &client, &q).await
            }
        },
    }
}

fn print_ping(info: &PingInfo) {
    if let Some(ref v) = info.version {
        println!("server version: {v}");
    }
    if let Some(ref b) = info.build {
        println!("build: {b}");
    }
    println!("ok");
}

fn prompt_password() -> hyperbytedb_cli::error::Result<String> {
    eprint!("password: ");
    rpassword::read_password().map_err(|e| CliError::Other(e.to_string()))
}
