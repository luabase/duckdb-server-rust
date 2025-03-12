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
use std::thread;
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

#[axum::debug_handler]
async fn handle_post(
    State(state): State<Arc<AppState>>,
    Json(params): Json<QueryParams>,
) -> Result<QueryResponse, AppError> {
    let thread_id = thread::current().id();
    tracing::info!("Handling request on thread {:?}", thread_id);
    query::with_db_retry(&state, params, |state, params| Box::pin(query::handle(state, params))).await
}

pub async fn app(defaults: DbDefaults, db_paths: Vec<DbPath>) -> Result<Router> {
    let app_state = Arc::new(AppState {
        defaults,
        paths: db_paths.into_iter().map(|db| (db.id.clone(), db)).collect(),
        states: Mutex::new(HashMap::new()),
    });

    tracing::info!("Loaded paths: {:?}", app_state.paths);

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::OPTIONS, Method::POST, Method::GET])
        .allow_headers(Any)
        .max_age(Duration::from_secs(60 * 60 * 24));

    Ok(Router::new()
        .route("/", get(handle_get).post(handle_post))
        .with_state(app_state)
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http()))
}
