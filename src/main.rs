use anyhow::Result;
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use listenfd::ListenFd;
use std::net::SocketAddr;
use std::{net::IpAddr, net::Ipv4Addr, net::TcpListener, path::PathBuf};
use tokio::{net, runtime::Builder};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::constants::*;
use crate::interfaces::{DbDefaults, DbPath};

mod app;
mod bundle;
mod cache;
mod constants;
mod db;
mod interfaces;
mod query;
mod state;
mod websocket;

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
    #[arg(short, long, default_value_t = Ipv4Addr::LOCALHOST.into())]
    address: IpAddr,

    /// HTTP Port
    #[arg(short, long, default_value_t = 3000)]
    port: u16,

    /// Max connection pool size
    #[arg(long)]
    connection_pool_size: Option<u32>,

    /// Max number of cache entries
    #[arg(long, default_value_t = DEFAULT_CACHE_SIZE)]
    cache_size: usize,

    /// Database access mode
    #[arg(long, default_value = "automatic")]
    access_mode: String,
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
    let worker_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);

    println!("Starting server with {} worker threads", worker_threads);

    let _ = Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .unwrap()
        .block_on(app_main());
}

async fn app_main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

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

    let num_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);

    let db_defaults = DbDefaults {
        access_mode: args.access_mode,
        cache_size: args.cache_size,
        connection_pool_size: args.connection_pool_size.unwrap_or(num_threads as u32),
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "duckdb_server=debug,tower_http=debug,axum::rejection=trace".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = app::app(db_defaults, db_paths).await?;

    // TLS configuration
    let mut config = RustlsConfig::from_pem_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("localhost.pem"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("localhost-key.pem"),
    )
    .await;

    if config.is_err() {
        // try current directory for HTTPS keys if env didn't work
        config = RustlsConfig::from_pem_file("./localhost.pem", "./localhost-key.pem").await;
    }

    // Listenfd setup
    let addr = SocketAddr::new(args.address, args.port);
    let mut listenfd = ListenFd::from_env();
    let listener = match listenfd.take_tcp_listener(0)? {
        // if we are given a tcp listener on listen fd 0, we use that one
        Some(listener) => {
            listener.set_nonblocking(true)?;
            listener
        }
        // otherwise fall back to local listening
        None => TcpListener::bind(addr)?,
    };

    // Run the server
    match config {
        Err(_) => {
            tracing::warn!("No keys for HTTPS found.");
            tracing::info!(
                "DuckDB Server listening on http://{0} and ws://{0}.",
                listener.local_addr()?
            );

            let listener = net::TcpListener::from_std(listener)?;
            axum::serve(listener, app).await?;
        }
        Ok(config) => {
            tracing::info!(
                "DuckDB Server listening on http(s)://{0} and ws://{0}",
                listener.local_addr()?
            );

            axum_server_dual_protocol::from_tcp_dual_protocol(listener, config)
                .serve(app.into_make_service())
                .await?;
        }
    }

    Ok(())
}
