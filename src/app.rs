use anyhow::Result;
use axum::{
    extract::{ws::rejection::WebSocketUpgradeRejection, Query, State, WebSocketUpgrade},
    http::Method,
    response::Json,
    routing::get,
    Router,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

use crate::interfaces::{AppError, DbDefaults, DbPath, QueryParams, QueryResponse};
use crate::query;
use crate::state::AppState;
use crate::websocket;

#[axum::debug_handler]
async fn handle_get(
    State(state): State<Arc<AppState>>,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    Query(params): Query<QueryParams>,
) -> Result<QueryResponse, AppError> {
    if let Ok(ws) = ws {
        // WebSocket upgrade
        Ok(QueryResponse::Response(
            ws.on_upgrade(|socket| websocket::handle(socket, state)),
        ))
    }
    else {
        // HTTP request
        query::with_db_retry(&state, params, |state, params| Box::pin(query::handle(state, params))).await
    }
}

pub const DEFAULT_DB_ID: &str = "default";
pub const DEFAULT_DB_PATH: &str = ":memory:";
pub const DEFAULT_CONNECTION_POOL_SIZE: u32 = 10;
pub const DEFAULT_CACHE_SIZE: usize = 1000;

#[axum::debug_handler]
async fn handle_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<QueryParams>,
) -> Result<QueryResponse, AppError> {
    query::with_db_retry(&state, params, |state, params| Box::pin(query::handle(state, params))).await
}

pub async fn app(defaults: DbDefaults, db_paths: Vec<DbPath>) -> Result<Router> {
    let app_state = Arc::new(AppState {
        defaults,
        paths: db_paths.into_iter().map(|db| (db.id.clone(), db)).collect(),
        states: Mutex::new(HashMap::new()),
    });

    // CORS setup
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::OPTIONS, Method::POST, Method::GET])
        .allow_headers(Any)
        .max_age(Duration::from_secs(60 * 60 * 24));

    // Router setup
    Ok(Router::new()
        .route("/", get(handle_get).post(handle_post))
        .with_state(app_state)
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http()))
}
