use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use duckdb::types::ToSql;
use serde::{Deserialize, Serialize};

use super::config::{DucklakeConfig, Extension, SecretConfig};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum Command {
    Arrow,
    Exec,
    Json,
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
pub struct QueryParams {
    pub database: String,
    #[serde(rename = "type")]
    pub query_type: Option<Command>,
    pub persist: Option<bool>,
    pub invalidate: Option<bool>,
    pub sql: Option<String>,
    pub prepare_sql: Option<String>,
    pub args: Option<Vec<SqlValue>>,
    pub name: Option<String>,
    pub limit: Option<usize>,
    pub query_id: Option<String>,
    pub create: Option<bool>,
    pub extensions: Option<Vec<Extension>>,
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
