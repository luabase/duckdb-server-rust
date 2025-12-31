use anyhow::Result;
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use dirs;
use listenfd::ListenFd;
use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::Arc,
};
use tokio::{net, runtime::Builder, sync::Mutex};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::auth::create_auth_config;
use crate::constants::FULL_VERSION;
use crate::interfaces::{CliArgs, Cli, CliCommand, DbDefaults};
use crate::state::AppState;

const PANIC_EXIT_CODE: i32 = 101;
const APPLICATION_ERROR_EXIT_CODE: i32 = 1;

fn log_panic_entry(message: &str, data: serde_json::Value) {
    let log_entry = serde_json::json!({
        "severity": "CRITICAL",
        "message": message,
        "data": data
    });
    eprintln!("{}", log_entry);
}

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

fn main() {
    std::panic::set_hook(Box::new(|panic_info| {
        let backtrace = std::backtrace::Backtrace::capture();

        log_panic_entry("PANIC DETECTED", serde_json::json!({
            "panic_info": panic_info.to_string()
        }));

        if let Some(location) = panic_info.location() {
            log_panic_entry("PANIC LOCATION", serde_json::json!({
                "file": location.file(),
                "line": location.line(),
                "column": location.column()
            }));
        }

        if let Some(payload) = panic_info.payload().downcast_ref::<&str>() {
            log_panic_entry("PANIC PAYLOAD", serde_json::json!({
                "payload": payload
            }));
        }

        log_panic_entry("STACK TRACE", serde_json::json!({
            "backtrace": backtrace.to_string()
        }));

        let rust_backtrace = std::env::var("RUST_BACKTRACE").unwrap_or_else(|_| "not set".to_string());
        log_panic_entry("RUST_BACKTRACE_ENABLED", serde_json::json!({
            "value": rust_backtrace
        }));

        let rust_backtrace_full = std::env::var("RUST_BACKTRACE_FULL").unwrap_or_else(|_| "not set".to_string());
        log_panic_entry("RUST_BACKTRACE_FULL_ENABLED", serde_json::json!({
            "value": rust_backtrace_full
        }));

        // Log additional environment information for debugging
        let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "not set".to_string());
        log_panic_entry("RUST_LOG_LEVEL", serde_json::json!({
            "value": rust_log
        }));

        log_panic_entry("APPLICATION EXITING DUE TO PANIC", serde_json::json!({
            "exit_code": PANIC_EXIT_CODE
        }));

        std::process::exit(PANIC_EXIT_CODE);
    }));

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
        CliCommand::Serve(args) => {
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
                std::process::exit(APPLICATION_ERROR_EXIT_CODE);
            }
        }
        CliCommand::Version => {
            // noop
        }
    }
}

async fn app_main(args: CliArgs) -> Result<(), Box<dyn std::error::Error>> {
    let _sentry_guard = if let Ok(dsn) = std::env::var("SENTRY_DSN") {
        println!("Initializing Sentry with DSN: {}...", &dsn[..dsn.len().min(20)]);
        Some(sentry::init((dsn, sentry::ClientOptions {
            release: Some((*FULL_VERSION).clone().into()),
            traces_sample_rate: 1.0,
            enable_logs: true,
            send_default_pii: true,
            ..Default::default()
        })))
    } else {
        None
    };

    let root = args.db_root;

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
        root: root.clone(),
        states: Mutex::new(HashMap::new()),
        running_queries: Mutex::new(HashMap::new()),
    });

    let fmt_layer = tracing_subscriber::fmt::layer().with_ansi(!args.no_color);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "duckdb_server=debug,tower_http=debug,axum::rejection=trace".into()),
        )
        .with(fmt_layer)
        .with(sentry::integrations::tracing::layer())
        .init();

    tracing::info!("Using database root: {}", root);

    let auth_config = if args.service_auth_enabled {
        tracing::info!("Authentication is enabled");

        let token = args.service_auth_token.or_else(|| std::env::var("SERVICE_AUTH_TOKEN").ok());

        if token.is_none() {
            return Err(anyhow::anyhow!(
                "Authentication is enabled but no token provided. Use --service-auth-token or set \
                SERVICE_AUTH_TOKEN environment variable."
            ).into());
        }

        Some(create_auth_config(
            true,
            token,
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
    tokio::spawn(async move {
        flight::serve(flight_addr, flight_state).await.unwrap();
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
