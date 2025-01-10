use anyhow::Result;
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use listenfd::ListenFd;
use std::net::TcpListener;
use std::{net::IpAddr, net::Ipv4Addr, net::SocketAddr, path::PathBuf};
use tokio::net;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::app::DEFAULT_CACHE_SIZE;
use crate::app::DEFAULT_CONNECTION_POOL_SIZE;
use crate::app::DEFAULT_DB_ID;
use crate::app::DEFAULT_DB_PATH;
use crate::interfaces::DbConfig;

mod app;
mod bundle;
mod cache;
mod db;
mod interfaces;
mod query;
mod websocket;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// List of databases in the format `id=path`
    #[arg(long = "db", value_parser = parse_db, num_args = 1..)]
    db_configs: Vec<(String, String)>,

    /// HTTP Address
    #[arg(short, long, default_value_t = Ipv4Addr::LOCALHOST.into())]
    address: IpAddr,

    /// HTTP Port
    #[arg(short, long, default_value_t = 3000)]
    port: u16,

    /// Max connection pool size
    #[arg(long, default_value_t = DEFAULT_CONNECTION_POOL_SIZE)]
    connection_pool_size: u32,

    /// Max number of cache entries
    #[arg(long, default_value_t = DEFAULT_CACHE_SIZE)]
    cache_size: usize,
}

fn parse_db(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        Err(format!("Invalid format for --db argument: {}", s))
    } else {
        Ok((parts[0].to_string(), parts[1].to_string()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db_configs: Vec<DbConfig> = if args.db_configs.is_empty() {
        vec![(
            DEFAULT_DB_ID.to_string(),
            DEFAULT_DB_PATH.to_string()
        )]
    } else {
        args.db_configs
    }
    .into_iter()
    .map(|(id, path)| DbConfig {
        id,
        path,
        cache_size: args.cache_size,
        connection_pool_size: args.connection_pool_size
    })
    .collect();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "duckdb_server=debug,tower_http=debug,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let app = app::app(db_configs).await?;

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
