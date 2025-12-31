use anyhow::Result;
use clap::Parser;
use dirs;
use listenfd::ListenFd;
use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};
use tokio::{net, runtime::Builder, sync::Mutex};
#[cfg(target_os = "linux")]
use tokio::time::interval;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

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

#[cfg(target_os = "linux")]
async fn monitor_memory_pressure(
    warn_threshold: f64,
    critical_threshold: f64,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    use std::io::{Read, Seek, SeekFrom};

    if warn_threshold <= 0.0 && critical_threshold <= 0.0 {
        tracing::info!("Memory pressure monitoring disabled (thresholds set to 0)");
        return;
    }

    if warn_threshold < 0.0 || warn_threshold > 100.0 {
        tracing::warn!(
            warn_threshold = warn_threshold,
            "Memory pressure warn threshold outside typical 0-100 range"
        );
    }
    if critical_threshold < 0.0 || critical_threshold > 100.0 {
        tracing::warn!(
            critical_threshold = critical_threshold,
            "Memory pressure critical threshold outside typical 0-100 range"
        );
    }

    const PSI_PATH: &str = "/proc/pressure/memory";

    let mut file = match std::fs::File::open(PSI_PATH) {
        Ok(f) => f,
        Err(_) => {
            tracing::info!("Memory pressure monitoring unavailable: PSI not supported on this kernel");
            return;
        }
    };

    tracing::info!(
        warn_threshold = warn_threshold,
        critical_threshold = critical_threshold,
        "Memory pressure monitoring enabled (PSI avg10: percentage of time stalled)"
    );
    let mut ticker = interval(Duration::from_secs(5));
    let mut buffer = String::with_capacity(256);
    let mut consecutive_failures: u32 = 0;
    const FAILURE_LOG_INTERVAL: u32 = 12;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::debug!("Memory pressure monitoring stopping");
                return;
            }
            _ = ticker.tick() => {}
        }

        buffer.clear();

        let read_result = file.seek(SeekFrom::Start(0)).and_then(|_| file.read_to_string(&mut buffer));

        if let Err(e) = read_result {
            consecutive_failures += 1;
            tracing::debug!(error = %e, "Failed to read memory pressure file");
            if consecutive_failures % FAILURE_LOG_INTERVAL == 0 {
                tracing::warn!(
                    consecutive_failures = consecutive_failures,
                    error = %e,
                    "Failed to read memory pressure info - monitoring may be impaired"
                );
            }
            continue;
        }

        if consecutive_failures > 0 {
            tracing::info!(
                consecutive_failures = consecutive_failures,
                "Memory pressure monitoring recovered"
            );
            consecutive_failures = 0;
        }

        for line in buffer.lines() {
            if line.starts_with("full") {
                if let Some(avg10) = line
                    .split_whitespace()
                    .find(|s| s.starts_with("avg10="))
                    .and_then(|s| s.strip_prefix("avg10="))
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    if critical_threshold > 0.0 && avg10 > critical_threshold {
                        tracing::error!(
                            memory_pressure_avg10 = avg10,
                            "Critical memory pressure detected"
                        );
                    } else if warn_threshold > 0.0 && avg10 > warn_threshold {
                        tracing::warn!(
                            memory_pressure_avg10 = avg10,
                            "High memory pressure detected"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn monitor_memory_pressure(
    _warn_threshold: f64,
    _critical_threshold: f64,
    _cancel_token: tokio_util::sync::CancellationToken,
) {
    tracing::info!("Memory pressure monitoring unavailable: only supported on Linux with PSI");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C - initiating graceful shutdown");
        }
        _ = terminate => {
            tracing::info!("Received SIGTERM - initiating graceful shutdown");
        }
    }

    if let Some(client) = sentry::Hub::current().client() {
        client.flush(Some(Duration::from_secs(2)));
    }
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
    let sentry_layer = sentry::integrations::tracing::layer()
        .with_filter(tracing_subscriber::filter::LevelFilter::ERROR);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "duckdb_server=debug,tower_http=debug,axum::rejection=trace".into()),
        )
        .with(fmt_layer)
        .with(sentry_layer)
        .init();

    tracing::info!("Using database root: {}", root);

    if args.log_query_memory {
        db::monitoring::set_log_duckdb_memory(true);
        tracing::info!("DuckDB memory logging enabled for queries");
    }

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

    let flight_cancel = tokio_util::sync::CancellationToken::new();
    let flight_addr = SocketAddr::new(args.address, args.grpc_port);
    let flight_state = app_state.clone();
    let flight_cancel_clone = flight_cancel.clone();
    let flight_handle = tokio::spawn(async move {
        if let Err(e) = flight::serve(flight_addr, flight_state, flight_cancel_clone).await {
            tracing::error!("Flight server failed: {}", e);
        }
    });

    let memory_monitor_cancel = tokio_util::sync::CancellationToken::new();
    let memory_monitor_handle = tokio::spawn(monitor_memory_pressure(
        args.memory_pressure_warn,
        args.memory_pressure_critical,
        memory_monitor_cancel.clone(),
    ));

    tracing::info!(
        "DuckDB Server listening on http://{}. Timeout is {}",
        listener.local_addr()?,
        args.timeout
    );

    let listener = net::TcpListener::from_std(listener)?;
    let server = axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!(error = %e, "HTTP server error");
            }
        }
    }

    flight_cancel.cancel();
    memory_monitor_cancel.cancel();

    tokio::select! {
        _ = async {
            let _ = flight_handle.await;
            tracing::debug!("Flight server stopped");
            let _ = memory_monitor_handle.await;
            tracing::debug!("Memory pressure monitor stopped");
        } => {
            tracing::info!("Server shutdown complete");
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            tracing::warn!("Graceful shutdown timed out after 5s");
        }
    }

    Ok(())
}
