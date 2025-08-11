use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::{header::HeaderName, HeaderValue, Method},
    response::Json,
    routing::get,
    Router,
};
use std::{sync::Arc, time::Duration};
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    set_header::SetResponseHeaderLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

use crate::constants::FULL_VERSION;
use crate::interfaces::{AppError, QueryParams, QueryResponse};
use crate::query;
use crate::state::AppState;
use serde::Serialize;

#[derive(Serialize)]
struct PoolStatusResponse {
    id: String,
    db_path: String,
    pool_size: usize,
    access_mode: String,
    in_use: usize,
    idle: usize,
    total: usize,
    timeout: Duration,
}

#[derive(Serialize)]
struct PoolStatusError {
    id: String,
    error: String,
}

#[derive(Serialize)]
#[serde(untagged)]
enum PoolStatusResult {
    Success(PoolStatusResponse),
    Error(PoolStatusError),
}

#[derive(Serialize)]
struct StatusResponse {
    pools: Vec<PoolStatusResult>,
    total_pools: usize,
}

#[axum::debug_handler]
async fn handle_get(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<QueryParams>,
) -> Result<QueryResponse, AppError> {
    let res = query::with_db_retry(&app_state, params, |state, params| {
        Box::pin(query::handle(state, params))
    })
    .await?;

    Ok(res)
}

#[axum::debug_handler]
async fn handle_post(
    State(app_state): State<Arc<AppState>>,
    Json(params): Json<QueryParams>,
) -> Result<QueryResponse, AppError> {
    let res = query::with_db_retry(&app_state, params, |state, params| {
        Box::pin(query::handle(state, params))
    })
    .await?;

    Ok(res)
}

#[axum::debug_handler]
async fn status_handler(
    State(app_state): State<Arc<AppState>>,
) -> Result<Json<StatusResponse>, AppError> {
    let states = app_state.states.lock().await;
    let mut pool_statuses = Vec::new();

    for (id, db_state) in states.iter() {
        match db_state.db.status() {
            Ok(pool_status) => {
                pool_statuses.push(PoolStatusResult::Success(PoolStatusResponse {
                    id: id.clone(),
                    db_path: pool_status.db_path,
                    pool_size: pool_status.pool_size,
                    access_mode: pool_status.access_mode,
                    in_use: pool_status.in_use,
                    idle: pool_status.idle,
                    total: pool_status.total,
                    timeout: pool_status.timeout,
                }));
            }
            Err(error) => {
                pool_statuses.push(PoolStatusResult::Error(PoolStatusError {
                    id: id.clone(),
                    error: error.to_string(),
                }));
            }
        }
    }

    Ok(Json(StatusResponse {
        pools: pool_statuses,
        total_pools: states.len(),
    }))
}

async fn readiness_probe() -> &'static str {
    "OK"
}

async fn version_handler() -> &'static str {
    &FULL_VERSION
}

pub async fn app(app_state: Arc<AppState>, timeout: u32) -> Result<Router> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::OPTIONS, Method::POST, Method::GET])
        .allow_headers(Any)
        .max_age(Duration::from_secs(86400));

    let hostname = hostname::get()?.into_string().unwrap_or_default();
    let hostname = if hostname.is_empty() { "unknown".into() } else { hostname };

    let full_version = if FULL_VERSION.is_empty() { "unknown" } else { &FULL_VERSION };

    let header_layer = ServiceBuilder::new()
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_lowercase(b"x-backend-hostname")?,
            HeaderValue::from_str(&hostname)?,
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            HeaderName::from_lowercase(b"x-server-version")?,
            HeaderValue::from_str(full_version)?,
        ));

    Ok(Router::new()
        .route("/", get(readiness_probe))
        .route("/query", get(handle_get).post(handle_post))
        .route("/query/", get(handle_get).post(handle_post))
        .route("/healthz", get(readiness_probe))
        .route("/version", get(version_handler))
        .route("/status", get(status_handler))
        .with_state(app_state)
        .layer(header_layer)
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(Duration::from_secs(timeout.into()))))
}
