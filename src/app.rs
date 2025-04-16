use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::{HeaderValue, Method},
    response::Json,
    routing::get,
    Router,
};
use std::{sync::Arc, time::Duration};
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

use crate::hostname::HostnameLayer;
use crate::interfaces::{AppError, QueryParams, QueryResponse};
use crate::query;
use crate::state::AppState;

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

async fn readiness_probe() -> &'static str {
    "OK"
}

pub async fn app(app_state: Arc<AppState>, timeout: u32) -> Result<Router> {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::OPTIONS, Method::POST, Method::GET])
        .allow_headers(Any)
        .max_age(Duration::from_secs(86400));

    let hostname = hostname::get()?.into_string().unwrap_or_else(|_| "unknown".into());
    let hostname_layer = HostnameLayer {
        hostname: HeaderValue::from_str(&hostname)?,
    };

    Ok(Router::new()
        .route("/", get(readiness_probe))
        .route("/query", get(handle_get).post(handle_post))
        .route("/query/", get(handle_get).post(handle_post))
        .route("/healthz", get(readiness_probe))
        .with_state(app_state)
        .layer(ServiceBuilder::new().layer(hostname_layer))
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(Duration::from_secs(timeout.into()))))
}
