#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use std::io::{self, Read};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use hyperbytedb_cli::{
    CliError, ConnectionConfig, HyperbytedbClient, Session,
    args::{decode_data_urlencode_query, normalize_influx_style_args},
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
    #[arg(short = 'H', long = "host", global = true)]
    host: Option<String>,

    /// Server port (when host is not a full URL)
    #[arg(long = "port", global = true)]
    port: Option<u16>,

    /// Default database
    #[arg(short = 'd', long = "database", global = true)]
    database: Option<String>,

    /// Username
    #[arg(short = 'u', long = "username", global = true)]
    username: Option<String>,

    /// Password (empty prompts interactively)
    #[arg(short = 'p', long = "password", global = true)]
    password: Option<String>,

    /// Use HTTPS
    #[arg(long = "ssl", global = true)]
    ssl: bool,

    /// Skip TLS certificate verification
    #[arg(long = "unsafeSsl", global = true)]
    unsafe_ssl: bool,

    /// Path to add after host (InfluxDB v1 `-url-prefix`)
    #[arg(long = "url-prefix", global = true)]
    url_prefix: Option<String>,

    /// Unix domain socket (InfluxDB v1 `-socket`)
    #[arg(long = "socket", global = true)]
    socket: Option<PathBuf>,

    /// Config profile name from ~/.config/hyperbytedb/config.toml
    #[arg(long = "profile", global = true)]
    profile: Option<String>,

    /// Execute TimeseriesQL and exit (batch mode)
    #[arg(short = 'e', long = "execute", global = true)]
    execute: Option<String>,

    /// Output format
    #[arg(short = 'f', long = "format", value_enum, global = true)]
    format: Option<FormatArg>,

    /// Timestamp precision / epoch param for queries
    #[arg(long = "precision", global = true)]
    precision: Option<String>,

    #[arg(long = "epoch", global = true)]
    epoch: Option<String>,

    /// Pretty-print JSON output
    #[arg(long = "pretty", global = true)]
    pretty: bool,

    /// Verbose HTTP tracing
    #[arg(short = 'v', long = "verbose", global = true)]
    verbose: bool,

    /// Query language (only influxql is supported)
    #[arg(long = "type", default_value = "influxql", global = true)]
    query_type: String,

    /// Write consistency level (InfluxDB v1: any, one, quorum, all)
    #[arg(long = "consistency", global = true)]
    consistency: Option<String>,

    /// Bind parameters JSON for queries (`-execute` / REPL)
    #[arg(long = "params", global = true)]
    query_params: Option<String>,

    /// Import a database export file (InfluxDB v1 `-import`)
    #[arg(long = "import", global = true)]
    import_mode: bool,

    /// Path to import file (use with `-import`)
    #[arg(long = "path", global = true)]
    import_path: Option<String>,

    /// Import file is gzip-compressed
    #[arg(long = "compressed", global = true)]
    import_compressed: bool,

    /// Import throttle (points per second; 0 = unlimited)
    #[arg(long = "pps", global = true)]
    import_pps: Option<u64>,

    /// Print CLI and server version, then exit
    #[arg(long = "version", global = true)]
    show_version: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Clone)]
enum Commands {
    /// Write line protocol from stdin, a file, or --data-binary
    Write {
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,
        /// Line protocol payload (InfluxDB v1-compatible)
        #[arg(long = "data-binary")]
        data_binary: Option<String>,
        #[arg(short = 'd', long = "database")]
        database: Option<String>,
        #[arg(long = "rp")]
        rp: Option<String>,
        #[arg(long = "precision")]
        precision: Option<String>,
        #[arg(long = "gzip")]
        gzip: bool,
    },
    /// Run a TimeseriesQL query and exit (InfluxDB v1-compatible)
    Query {
        #[arg(short = 'd', long = "database")]
        database: Option<String>,
        /// Query string (curl-style `q=SELECT ...` or plain TimeseriesQL)
        #[arg(long = "data-urlencode", required = true)]
        data_urlencode: String,
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
    /// Schema administration (InfluxDB v1-compatible shortcuts)
    Drop {
        #[command(subcommand)]
        target: DropTarget,
    },
}

#[derive(Subcommand, Clone)]
enum CreateTarget {
    /// Create a new database
    Database {
        /// Database name
        name: String,
    },
}

#[derive(Subcommand, Clone)]
enum DropTarget {
    /// Drop a database
    Database {
        /// Database name
        name: String,
    },
}

#[derive(Subcommand, Clone)]
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
    let cli = Cli::parse_from(normalize_influx_style_args(std::env::args()));

    if cli.query_type.eq_ignore_ascii_case("flux") {
        return Err(CliError::Other(
            "Flux is not supported; use -type influxql (default)".to_string(),
        ));
    }

    if cli.show_version {
        println!("hyperbytedb-cli {}", env!("CARGO_PKG_VERSION"));
        let conn = build_connection(&cli)?;
        let client = HyperbytedbClient::new(&conn, cli.verbose)?;
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

    if cli.import_mode {
        let path = cli
            .import_path
            .clone()
            .ok_or_else(|| CliError::Other("-import requires -path".to_string()))?;
        let client = HyperbytedbClient::new(&conn, cli.verbose)?;
        let opts = ImportOptions {
            path,
            compressed: cli.import_compressed,
            pps: cli.import_pps.unwrap_or(0),
            precision: cli.precision.clone(),
        };
        return import::run_import(&client, &opts).await.map(|_| ());
    }

    if let Some(cmd) = cli.command.clone() {
        return run_subcommand(&cli, &conn, cmd).await;
    }

    if let Some(ref q) = cli.execute {
        let session = build_session(&cli, conn);
        let client = HyperbytedbClient::new(&session.connection, session.verbose)?;
        return repl::execute_query(&session, &client, q).await;
    }

    let session = build_session(&cli, conn);
    repl::run_repl(session).await
}

fn build_connection(cli: &Cli) -> hyperbytedb_cli::error::Result<ConnectionConfig> {
    let mut conn = Cfg::load(cli.profile.as_deref())?;

    if cli.socket.is_some() {
        conn.socket = cli.socket.clone();
    } else if let Some(ref h) = cli.host {
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
    if cli.url_prefix.is_some() {
        conn.url_prefix = cli.url_prefix.clone();
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
    session.consistency = cli.consistency.clone();
    session.query_params = cli.query_params.clone();
    session
}

async fn run_subcommand(
    cli: &Cli,
    conn: &ConnectionConfig,
    cmd: Commands,
) -> hyperbytedb_cli::error::Result<()> {
    let client = HyperbytedbClient::new(conn, cli.verbose)?;

    match cmd {
        Commands::Write {
            file,
            data_binary,
            database,
            rp,
            precision,
            gzip,
        } => {
            let db = database
                .or_else(|| conn.database.clone())
                .ok_or_else(|| CliError::Other("--database is required".to_string()))?;
            if data_binary.is_some() && file.is_some() {
                return Err(CliError::Other(
                    "cannot use both --data-binary and --file".to_string(),
                ));
            }
            let body = if let Some(data) = data_binary {
                data.into_bytes()
            } else if let Some(path) = file {
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
                consistency: cli.consistency.clone(),
            };
            client.write(&body, &wopts).await
        }
        Commands::Query {
            database,
            data_urlencode,
        } => {
            let mut session = build_session(cli, conn.clone());
            if let Some(db) = database.or_else(|| conn.database.clone()) {
                session.database = Some(db);
            }
            let q = decode_data_urlencode_query(&data_urlencode)?;
            repl::execute_query(&session, &client, &q).await
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
                let session = build_session(cli, conn.clone());
                let q = format!("CREATE DATABASE {name}");
                repl::execute_query(&session, &client, &q).await
            }
        },
        Commands::Drop { target } => match target {
            DropTarget::Database { name } => {
                let session = build_session(cli, conn.clone());
                let q = format!("DROP DATABASE {name}");
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
