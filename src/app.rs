use anyhow::Result;
use axum::{
    extract::{ws::rejection::WebSocketUpgradeRejection, Query, State, WebSocketUpgrade},
    http::{HeaderValue, Method},
    response::Json,
    routing::get,
    Router,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{self, Sender},
        oneshot, Mutex,
    },
    task,
};
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

use crate::hostname::HostnameLayer;
use crate::interfaces::{AppError, DbDefaults, DbPath, QueryParams, QueryResponse};
use crate::state::AppState;
use crate::{query, websocket};

type QueryResultSender = oneshot::Sender<Result<QueryResponse, AppError>>;
type QueryQueueItem = (QueryParams, QueryResultSender);
type QueryReceiver = mpsc::Receiver<QueryQueueItem>;
type SharedQueryReceiver = Arc<Mutex<QueryReceiver>>;

#[derive(Clone)]
pub struct QueueState {
    sender: Sender<(
        QueryParams,
        tokio::sync::oneshot::Sender<Result<QueryResponse, AppError>>,
    )>,
}

#[axum::debug_handler]
async fn handle_get(
    State((queue_state, app_state)): State<(QueueState, Arc<AppState>)>,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    Query(params): Query<QueryParams>,
) -> Result<QueryResponse, AppError> {
    if let Ok(ws) = ws {
        Ok(QueryResponse::Response(
            ws.on_upgrade(|socket| websocket::handle(socket, app_state)),
        ))
    }
    else {
        let (tx, rx) = tokio::sync::oneshot::channel();
        queue_state.sender.send((params, tx)).await.unwrap();
        rx.await.unwrap()
    }
}

#[axum::debug_handler]
async fn handle_post(
    State((queue_state, _app_state)): State<(QueueState, Arc<AppState>)>,
    Json(params): Json<QueryParams>,
) -> Result<QueryResponse, AppError> {
    let (tx, rx) = oneshot::channel();
    queue_state.sender.send((params, tx)).await.unwrap();
    rx.await.unwrap()
}

async fn process_queue(state: Arc<AppState>, receiver: SharedQueryReceiver) {
    loop {
        let msg = {
            let mut receiver_guard = receiver.lock().await;
            receiver_guard.recv().await
        };

        match msg {
            Some((params, response_tx)) => {
                let res =
                    query::with_db_retry(&state, params, |state, params| Box::pin(query::handle(state, params))).await;
                let _ = response_tx.send(res);
            }
            None => break,
        }
    }
}

async fn readiness_probe() -> &'static str {
    "OK"
}

pub async fn app(
    defaults: DbDefaults,
    db_paths: Vec<DbPath>,
    timeout: u32,
    parallelism: usize,
    queue_length: usize,
) -> Result<Router> {
    let app_state = Arc::new(AppState {
        defaults,
        paths: db_paths.into_iter().map(|db| (db.id.clone(), db)).collect(),
        states: Mutex::new(HashMap::new()),
    });

    tracing::info!("Server timeout: {}", timeout);
    tracing::info!("Server parallelism: {}", parallelism);
    tracing::info!("Server connection queue length: {}", queue_length);
    tracing::info!("Loaded paths: {:?}", app_state.paths);

    let (tx, rx): (mpsc::Sender<QueryQueueItem>, QueryReceiver) = mpsc::channel(queue_length);
    let rx: SharedQueryReceiver = Arc::new(Mutex::new(rx));

    for _ in 0..parallelism {
        let processor_state = app_state.clone();
        let rx_clone = Arc::clone(&rx);

        task::spawn(async move {
            process_queue(processor_state, rx_clone).await;
        });
    }

    let queue_state = QueueState { sender: tx };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::OPTIONS, Method::POST, Method::GET])
        .allow_headers(Any)
        .max_age(Duration::from_secs(86400));

    let hostname = hostname::get()?.into_string().unwrap_or_else(|_| "unknown".into());
    let hostname_layer = HostnameLayer { hostname: HeaderValue::from_str(&hostname)? };

    Ok(Router::new()
        .route("/", get(readiness_probe))
        .route("/query", get(handle_get).post(handle_post))
        .route("/query/", get(handle_get).post(handle_post))
        .route("/healthz", get(readiness_probe))
        .with_state((queue_state, app_state))
        .layer(ServiceBuilder::new().layer(hostname_layer))
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(Duration::from_secs(timeout.into()))))
}
