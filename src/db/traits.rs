use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::interfaces::{AppError, DucklakeConfig, Extension, SecretConfig, SqlValue};

#[async_trait]
pub trait Database: Send + Sync {
    async fn execute(
        &self, sql: &str,
        default_schema: &Option<String>,
        extensions: &Option<Vec<Extension>>
    ) -> Result<()>;
    async fn get_json(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        default_schema: &Option<String>,
        limit: usize,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<u8>>;
    async fn get_arrow(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        default_schema: &Option<String>,
        limit: usize,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<u8>>;
    async fn get_record_batches(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        default_schema: &Option<String>,
        limit: usize,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>>;
    fn reconnect(&self) -> Result<()>;
    fn status(&self) -> Result<PoolStatus, AppError>;
    fn kill_all_connections(&self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct PoolStatus {
    pub db_path: String,
    pub pool_size: usize,
    pub access_mode: String,
    pub in_use: usize,
    pub idle: usize,
    pub total: usize,
    pub timeout: Duration,
}
