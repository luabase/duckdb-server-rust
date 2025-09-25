use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use duckdb::types::ToSql;
use serde::{Deserialize, Serialize};
use std::fmt;
use tokio::sync::Mutex;

use crate::db::Database;

#[derive(Debug, Clone)]
pub struct DbDefaults {
    pub access_mode: String,
    pub cache_size: usize,
    pub connection_pool_size: u32,
    pub row_limit: usize,
    pub pool_timeout: u64,
}

#[derive(Debug, Clone)]
pub struct DbPath {
    pub id: String,
    pub primary_id: String,
    pub path: String,
    pub is_dynamic: bool,
}

pub struct DbState {
    pub db: Box<dyn Database>,
    pub cache: Mutex<lru::LruCache<String, Vec<u8>>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum Command {
    Arrow,
    Exec,
    Json,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Extension {
    pub name: String,
    pub source: Option<String>,
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
pub struct DucklakeConfig {
    pub connection: String,
    pub alias: String,
    pub data_path: String,
    pub meta_schema: Option<String>,
    pub replace: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct SecretConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub secret_type: String,
    pub key_id: String,
    pub secret: Option<String>,
    pub provider: Option<String>,
    pub region: Option<String>,
    pub token: Option<String>,
    pub scope: Option<String>,
    pub replace: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct QueryParams {
    pub database: String,
    #[serde(rename = "dynamic")]
    pub dynamic_id: Option<String>,
    #[serde(rename = "type")]
    pub query_type: Option<Command>,
    pub persist: Option<bool>,
    pub invalidate: Option<bool>,
    pub sql: Option<String>,
    pub prepare_sql: Option<String>,
    pub args: Option<Vec<SqlValue>>,
    pub name: Option<String>,
    pub limit: Option<usize>,
    pub extensions: Option<Vec<Extension>>,
    pub query_id: Option<String>,
    pub create: Option<bool>,
    pub ducklakes: Option<Vec<DucklakeConfig>>,
    pub secrets: Option<Vec<SecretConfig>>,
}

pub enum QueryResponse {
    Arrow(Vec<u8>),
    Json(String),
    Empty,
    QueryCancelled {
        query_id: String,
    },
    RunningQueries {
        queries: Vec<QueryInfo>,
    },
    QueryWithId {
        query_id: String,
        result: Box<QueryResponse>,
    },
}

#[derive(Serialize, Clone)]
pub struct QueryInfo {
    pub id: String,
    pub database: String,
    pub sql: String,
    pub started_at: String,
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
            QueryResponse::Empty => StatusCode::OK.into_response(),
            QueryResponse::QueryCancelled { query_id } => {
                let response = serde_json::json!({
                    "status": "cancelled",
                    "query_id": query_id
                });
                (
                    StatusCode::OK,
                    [("Content-Type", "application/json")],
                    response.to_string(),
                )
                    .into_response()
            }
            QueryResponse::RunningQueries { queries } => {
                let response = serde_json::json!({
                    "status": "running_queries",
                    "queries": queries
                });
                (
                    StatusCode::OK,
                    [("Content-Type", "application/json")],
                    response.to_string(),
                )
                    .into_response()
            }
            QueryResponse::QueryWithId { query_id, result } => {
                let mut response = (*result).into_response();
                if let Ok(header_value) = query_id.parse() {
                    response.headers_mut().insert("X-Query-ID", header_value);
                }
                response
            }
        }
    }
}

#[derive(Debug)]
pub enum AppError {
    Error(anyhow::Error),
    BadRequest(anyhow::Error),
    Timeout,
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
            AppError::BadRequest(error) => (StatusCode::BAD_REQUEST, format!("Bad request: {error}")).into_response(),
            AppError::Timeout => (StatusCode::REQUEST_TIMEOUT).into_response(),
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::Error(err) => write!(f, "{}", err),
            AppError::BadRequest(err) => write!(f, "Bad request: {}", err),
            AppError::Timeout => write!(f, "Request timed out"),
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
