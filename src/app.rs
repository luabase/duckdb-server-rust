use anyhow::Result;
use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderValue, Method, header::HeaderName},
    response::Json,
    routing::{delete, get, post},
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
struct QueryStatus {
    id: String,
    database: String,
    sql: String,
    started_at: String,
}

#[derive(Serialize)]
struct StatusResponse {
    pools: Vec<PoolStatusResult>,
    total_pools: usize,
    running_queries: Vec<QueryStatus>,
    total_running_queries: usize,
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
async fn status_handler(State(app_state): State<Arc<AppState>>) -> Result<Json<StatusResponse>, AppError> {
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

    let running_queries = app_state.get_running_queries().await;
    let query_statuses: Vec<QueryStatus> = running_queries
        .into_iter()
        .map(|query| QueryStatus {
            id: query.id,
            database: query.database,
            sql: query.sql,
            started_at: query.started_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .to_string(),
        })
        .collect();

    let total_running_queries = query_statuses.len();

    Ok(Json(StatusResponse {
        pools: pool_statuses,
        total_pools: states.len(),
        running_queries: query_statuses,
        total_running_queries,
    }))
}

async fn readiness_probe() -> &'static str {
    "OK"
}

async fn version_handler() -> &'static str {
    &FULL_VERSION
}


#[axum::debug_handler]
async fn cancel_query_handler(
    State(app_state): State<Arc<AppState>>,
    Path(query_id): Path<String>,
) -> Result<QueryResponse, AppError> {
    query::cancel_query(&app_state, query_id).await
}

#[axum::debug_handler]
async fn list_queries_handler(State(app_state): State<Arc<AppState>>) -> Result<QueryResponse, AppError> {
    query::list_running_queries(&app_state).await
}

#[axum::debug_handler]
async fn interrupt_all_connections_handler(State(app_state): State<Arc<AppState>>) -> Result<QueryResponse, AppError> {
    query::interrupt_all_connections(&app_state).await
}

pub async fn app(app_state: Arc<AppState>, timeout: u32) -> Result<Router> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::OPTIONS, Method::POST, Method::GET])
        .allow_headers(Any)
        .max_age(Duration::from_secs(86400));

    let hostname = hostname::get()?.into_string().unwrap_or_default();
    let hostname = if hostname.is_empty() {
        "unknown".into()
    }
    else {
        hostname
    };

    let full_version = if FULL_VERSION.is_empty() {
        "unknown"
    }
    else {
        &FULL_VERSION
    };

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
        .route("/query/{query_id}", delete(cancel_query_handler))
        .route("/queries", get(list_queries_handler))
        .route("/interrupt-all", post(interrupt_all_connections_handler))
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
