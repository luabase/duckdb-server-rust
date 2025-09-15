use anyhow::Result;
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use dirs;
use listenfd::ListenFd;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
};
use tokio::{net, runtime::Builder, sync::Mutex};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::auth::create_auth_config;
use crate::constants::*;
use crate::interfaces::{DbDefaults, DbPath};
use crate::state::AppState;

mod app;
mod auth;
mod cache;
mod constants;
mod db;
mod flight;
mod interfaces;
mod query;
mod sql;
mod state;

unsafe extern "C" {
    pub fn duckdb_library_version() -> *const std::os::raw::c_char;
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
enum Command {
    #[command(about = "Run the DuckDB server")]
    Serve(Args),
    #[command(about = "Print the DuckDB library version")]
    Version,
}

#[derive(Parser, Debug)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// List of databases in the format `id=path`
    #[arg(long = "db", value_parser = parse_db, num_args = 1..)]
    db_paths: Vec<(String, String)>,

    /// Dynamic databases in the format `id1,id2,id3=path`
    #[arg(long = "db-dynamic-root", value_parser = parse_db_dynamic_roots, num_args = 1..)]
    db_dynamic_roots: Vec<(String, Vec<String>, String)>,

    /// HTTP Address
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED.into())]
    address: IpAddr,

    /// HTTP Port
    #[arg(short = 'p', long, default_value_t = 3000)]
    http_port: u16,

    /// gRPC Port
    #[arg(short, long, default_value_t = 3030)]
    grpc_port: u16,

    /// Request timeout
    #[arg(short, long, default_value_t = 60)]
    timeout: u32,

    /// Max connection pool size
    #[arg(long)]
    connection_pool_size: Option<u32>,

    /// Max number of cache entries
    #[arg(long, default_value_t = DEFAULT_CACHE_SIZE)]
    cache_size: usize,

    /// Database access mode
    #[arg(long, default_value = "automatic")]
    access_mode: String,

    /// Default row limit
    #[arg(long, default_value_t = DEFAULT_ROW_LIMIT)]
    row_limit: usize,

    /// Connection pool timeout in seconds
    #[arg(long, default_value_t = 10)]
    pool_timeout: u64,
    
    /// Enable authentication
    #[arg(long)]
    service_auth_enabled: bool,
    
    /// Authentication token
    #[arg(long)]
    service_auth_token: Option<String>,
}

fn parse_db(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        Err(format!("Invalid format for --db argument: {}", s))
    }
    else {
        Ok((parts[0].to_string(), parts[1].to_string()))
    }
}

fn parse_db_dynamic_roots(s: &str) -> Result<(String, Vec<String>, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid format for --db-dynamic-root argument: {}", s));
    }

    let id_parts: Vec<&str> = parts[0].split(',').collect();
    if id_parts.is_empty() {
        return Err(format!("At least one ID is required before '=' in argument: {}", s));
    }

    let primary_id = id_parts[0].to_string();
    let aliases = id_parts[1..].iter().map(|&id| id.to_string()).collect();
    let path = parts[1].to_string();

    Ok((primary_id, aliases, path))
}

fn main() {
    let c_str = unsafe { duckdb_library_version() };
    let duck_version = unsafe { std::ffi::CStr::from_ptr(c_str).to_str().unwrap() };

    println!("DuckDB server version {}.", *FULL_VERSION);
    println!("DuckDB library version: {}", duck_version);

    let platform = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let platform_str = match platform {
        "macos" => "osx",
        other => other,
    };
    let arch_str = match arch {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        other => other,
    };
    let home_dir = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("/root"));
    let ext_path = format!(
        "{}/.duckdb/extensions/{}/{}_{}",
        home_dir.display(),
        duck_version,
        platform_str,
        arch_str
    );
    println!("DuckDB extension path: {}", ext_path);

    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => {
            let worker_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
            println!("Starting server with {} worker threads", worker_threads);
            if let Err(e) = Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .enable_all()
                .build()
                .unwrap()
                .block_on(app_main(args))
            {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
        Command::Version => {
            // noop
        }
    }
}

async fn app_main(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let db_args: Vec<(String, String)> = if args.db_paths.is_empty() {
        vec![(DEFAULT_DB_ID.to_string(), DEFAULT_DB_PATH.to_string())]
    }
    else {
        args.db_paths
    };

    let mut db_paths: Vec<DbPath> = db_args
        .into_iter()
        .map(|(id, path)| DbPath {
            id: id.clone(),
            primary_id: id.clone(),
            path,
            is_dynamic: false,
        })
        .collect();

    for (primary_id, aliases, path) in args.db_dynamic_roots {
        db_paths.push(DbPath {
            id: primary_id.clone(),
            primary_id: primary_id.clone(),
            path: path.clone(),
            is_dynamic: true,
        });

        for alias in aliases {
            db_paths.push(DbPath {
                id: alias,
                primary_id: primary_id.clone(),
                path: path.clone(),
                is_dynamic: true,
            });
        }
    }

    let parallelism = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);

    let db_defaults = DbDefaults {
        access_mode: args.access_mode,
        cache_size: args.cache_size,
        connection_pool_size: args.connection_pool_size.unwrap_or(parallelism as u32),
        row_limit: args.row_limit,
        pool_timeout: args.pool_timeout,
    };

    let app_state = Arc::new(AppState {
        defaults: db_defaults,
        paths: db_paths.into_iter().map(|db| (db.id.clone(), db)).collect(),
        states: Mutex::new(HashMap::new()),
        running_queries: Mutex::new(HashMap::new()),
    });

    tracing::info!("Loaded paths: {:?}", app_state.paths);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "duckdb_server=debug,tower_http=debug,axum::rejection=trace".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let auth_config = if args.service_auth_enabled {
        tracing::info!("Authentication is enabled");
        
        if args.service_auth_token.is_none() {
            return Err(anyhow::anyhow!(
                "Authentication is enabled but no token provided. Use --service-auth-token to specify a token."
            ).into());
        }
        
        Some(create_auth_config(
            true,
            args.service_auth_token,
        ))
    } else {
        None
    };

    let app = app::app(app_state.clone(), args.timeout, auth_config).await?;

    let addr = SocketAddr::new(args.address, args.http_port);
    let mut listenfd = ListenFd::from_env();
    let listener = match listenfd.take_tcp_listener(0)? {
        Some(listener) => {
            listener.set_nonblocking(true)?;
            listener
        }
        None => TcpListener::bind(addr)?,
    };

    // TLS configuration
    let mut config = RustlsConfig::from_pem_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("localhost.pem"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("localhost-key.pem"),
    )
    .await;

    if config.is_err() {
        config = RustlsConfig::from_pem_file("./localhost.pem", "./localhost-key.pem").await;
    }

    let flight_addr = SocketAddr::new(args.address, args.grpc_port);
    let flight_state = app_state.clone();
    std::thread::spawn(move || {
        let flight_rt = tokio::runtime::Runtime::new().unwrap();
        flight_rt.block_on(async move {
            flight::serve(flight_addr, flight_state).await.unwrap();
        });
    });

    match config {
        Err(_) => {
            tracing::warn!("No keys for HTTPS found.");
            tracing::info!(
                "DuckDB Server listening on http://{}. Timeout is {}",
                listener.local_addr()?,
                args.timeout
            );

            let listener = net::TcpListener::from_std(listener)?;

            // Run Axum directly without tokio::spawn
            axum::serve(listener, app.into_make_service()).await?;
        }
        Ok(config) => {
            tracing::info!(
                "DuckDB Server listening with TLS on https://{}. Timeout is {}",
                listener.local_addr()?,
                args.timeout
            );

            axum_server_dual_protocol::from_tcp_dual_protocol(listener, config)
                .serve(app.into_make_service())
                .await?;
        }
    }

    Ok(())
}
