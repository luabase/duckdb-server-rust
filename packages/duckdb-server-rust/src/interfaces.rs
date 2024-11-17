use anyhow::Result;
use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use duckdb::types::ToSql;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::bundle::Query as BundleQuery;
use crate::db::{ConnectionPool, Database};

pub struct AppState {
    pub states: Mutex<HashMap<String, DbState>>,
}

impl AppState {
    pub async fn recreate_db(&self, db_id: &str) -> Result<()> {
        let mut states = self.states.lock().await;

        if let Some(db_state) = states.get(db_id) {
            let config = db_state.config.clone();
            let effective_pool_size = if config.path == ":memory:" { 1 } else { config.pool_size };

            let db = ConnectionPool::new(&config.path, effective_pool_size)?;
            let cache = Mutex::new(lru::LruCache::new(1000.try_into()?));

            db.execute("INSTALL icu; LOAD icu;").await?;

            tracing::info!("Recreated DuckDB with ID: {}, Path: {}", config.id, config.path);

            states.insert(
                db_id.to_string(),
                DbState {
                    config,
                    db: Box::new(db),
                    cache,
                },
            );
        } else {
            return Err(anyhow::anyhow!("Database ID {} not found", db_id));
        }

        Ok(())
    }
}

pub struct DbState {
    pub config: DbConfig,
    pub db: Box<dyn Database>,
    pub cache: Mutex<lru::LruCache<String, Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub id: String,
    pub path: String,
    pub pool_size: u32,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum Command {
    Arrow,
    Exec,
    Json,
    CreateBundle,
    LoadBundle,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum SqlValue {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}

impl SqlValue {
    // Converts SqlValue to a type that implements `ToSql`
    pub fn as_tosql(&self) -> Box<dyn ToSql> {
        match self {
            SqlValue::Int(v) => Box::new(*v),
            SqlValue::Float(v) => Box::new(*v),
            SqlValue::Text(v) => Box::new(v.clone()),
            SqlValue::Bool(v) => Box::new(*v),
            SqlValue::Null => Box::new(None::<i32>),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct QueryParams {
    pub database: String,
    #[serde(rename = "type")]
    pub query_type: Option<Command>,
    pub persist: Option<bool>,
    pub invalidate: Option<bool>,
    pub sql: Option<String>,
    pub args: Option<Vec<SqlValue>>,
    pub name: Option<String>,
    pub queries: Option<Vec<BundleQuery>>,
}

pub enum QueryResponse {
    Arrow(Vec<u8>),
    Json(String),
    Response(Response),
    Empty,
}

impl IntoResponse for QueryResponse {
    fn into_response(self) -> Response {
        match self {
            QueryResponse::Arrow(bytes) => (
                StatusCode::OK,
                [("Content-Type", "application/vnd.apache.arrow.stream")],
                Bytes::from(bytes),
            )
                .into_response(),
            QueryResponse::Json(value) => {
                (StatusCode::OK, [("Content-Type", "application/json")], value).into_response()
            }
            QueryResponse::Response(response) => response,
            QueryResponse::Empty => StatusCode::OK.into_response(),
        }
    }
}

#[derive(Debug)]
pub enum AppError {
    Error(anyhow::Error),
    BadRequest,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::Error(error) => {
                tracing::error!("Error: {:?}", error);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Something went wrong: {error}"),
                )
                    .into_response()
            }
            AppError::BadRequest => (StatusCode::BAD_REQUEST).into_response(),
        }
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        AppError::Error(err.into())
    }
}
