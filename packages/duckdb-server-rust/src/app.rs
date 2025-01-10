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

use crate::db::{ConnectionPool, Database};
use crate::interfaces::{AppError, AppState, DbConfig, DbState, QueryParams, QueryResponse};
use crate::query;
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

pub async fn app(db_configs: Vec<DbConfig>) -> Result<Router> {
    let mut states = HashMap::new();

    for db_config in db_configs {
        let effective_pool_size = if db_config.path == ":memory:" {
            1
        }
        else {
            db_config.connection_pool_size
        };
        let db = ConnectionPool::new(&db_config.path, effective_pool_size)?;
        let cache = Mutex::new(lru::LruCache::new(db_config.cache_size.try_into()?));

        tracing::info!("Using DuckDB with ID: {}, Path: {}", db_config.id, db_config.path);

        db.execute("INSTALL icu; LOAD icu;").await?;

        states.insert(
            db_config.id.clone(),
            DbState {
                config: db_config,
                db: Box::new(db),
                cache,
            },
        );
    }

    let app_state = Arc::new(AppState {
        states: Mutex::new(states),
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
