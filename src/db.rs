use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb::{params_from_iter, types::ToSql, AccessMode, Config, DuckdbConnectionManager};
use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::log::info;

use crate::interfaces::SqlValue;

#[async_trait]
pub trait Database: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<()>;
    async fn get_json(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>>;
    async fn get_arrow(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>>;
    async fn get_record_batches(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<RecordBatch>>;
}

pub struct ConnectionPool {
    db_path: String,
    pool_size: u32,
    access_mode: AccessMode,
    pool: r2d2::Pool<DuckdbConnectionManager>,
}

impl ConnectionPool {
    pub fn new(db_path: &str, pool_size: u32, access_mode: AccessMode) -> Result<Self> {
        info!(
            "Creating connection pool: db_path={}, pool_size={}, access_mode={:?}",
            db_path, pool_size, access_mode
        );
        let config = Config::default()
            .access_mode(match access_mode {
                AccessMode::ReadOnly => AccessMode::ReadOnly,
                AccessMode::ReadWrite => AccessMode::ReadWrite,
                AccessMode::Automatic => AccessMode::Automatic,
            })?
            .threads(pool_size as i64)?;

        let manager = DuckdbConnectionManager::file_with_flags(db_path, config)?;
        let pool = r2d2::Pool::builder().max_size(pool_size).build(manager)?;

        Ok(Self {
            db_path: db_path.to_string(),
            pool_size,
            access_mode,
            pool,
        })
    }

    pub fn get(&self) -> Result<r2d2::PooledConnection<DuckdbConnectionManager>> {
        info!("Checking out connection from pool: db_path={}", self.db_path);
        Ok(self.pool.get()?)
    }

    pub fn reset_pool(&mut self) -> Result<()> {
        info!(
            "Resetting connection pool: db_path={}, pool_size={}, access_mode={:?}",
            self.db_path, self.pool_size, self.access_mode
        );
        let config = Config::default()
            .access_mode(match self.access_mode {
                AccessMode::ReadOnly => AccessMode::ReadOnly,
                AccessMode::ReadWrite => AccessMode::ReadWrite,
                AccessMode::Automatic => AccessMode::Automatic,
            })?
            .threads(self.pool_size as i64)?;

        let manager = DuckdbConnectionManager::file_with_flags(&self.db_path, config)?;
        self.pool = r2d2::Pool::builder().max_size(self.pool_size).build(manager)?;

        Ok(())
    }
}

pub struct SafeConnectionPool {
    inner: Arc<RwLock<ConnectionPool>>,
}

impl SafeConnectionPool {
    pub fn new(db_path: &str, pool_size: u32, access_mode: AccessMode) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(ConnectionPool::new(db_path, pool_size, access_mode)?)),
        })
    }
}

fn is_writable_sql(sql: &str) -> bool {
    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements.iter().any(|stmt| match stmt {
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::CreateTable { .. }
            | Statement::CreateView { .. }
            | Statement::CreateIndex { .. }
            | Statement::Drop { .. }
            | Statement::AlterTable { .. }
            | Statement::Copy { .. }
            | Statement::Truncate { .. }
            | Statement::Merge { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. } => true,
            Statement::Query(query) => query.with.as_ref().is_some_and(|with| {
                with.cte_tables.iter().any(|cte| {
                    matches!(
                        cte.query.body.as_ref(),
                        sqlparser::ast::SetExpr::Insert { .. } | sqlparser::ast::SetExpr::Update { .. }
                    )
                })
            }),
            _ => false,
        }),
        Err(_) => false,
    }
}

#[async_trait]
impl Database for SafeConnectionPool {
    async fn execute(&self, sql: &str) -> Result<()> {
        {
            let pool = self.inner.read().await;
            let conn = pool.get()?;
            conn.execute_batch(sql)?;
        }

        if is_writable_sql(sql) {
            let mut pool_write = self.inner.write().await;
            pool_write.reset_pool()?;
        }

        Ok(())
    }

    async fn get_json(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>> {
        let sql_owned = sql.to_string();
        let args = args.to_vec();
        let pool = self.inner.clone();

        let result = tokio::task::spawn_blocking({
            let sql = sql_owned.clone();
            move || -> Result<Vec<u8>> {
                let conn = pool.blocking_read().get()?;
                let mut stmt = conn.prepare(&sql)?;
                let tosql_args: Vec<Box<dyn ToSql>> = args.iter().map(|arg| arg.as_tosql()).collect();
                let arrow = stmt.query_arrow(params_from_iter(tosql_args.iter()))?;

                let buf = Vec::new();
                let mut writer = arrow_json::ArrayWriter::new(buf);
                for batch in arrow {
                    writer.write(&batch)?;
                }
                writer.finish()?;
                Ok(writer.into_inner())
            }
        })
        .await??;

        if is_writable_sql(&sql_owned) {
            let mut pool_write = self.inner.write().await;
            pool_write.reset_pool()?;
        }

        Ok(result)
    }

    async fn get_arrow(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>> {
        let sql_owned = sql.to_string();
        let args = args.to_vec();
        let pool = self.inner.clone();

        let result = tokio::task::spawn_blocking({
            let sql = sql_owned.clone();
            move || -> Result<Vec<u8>> {
                let conn = pool.blocking_read().get()?;
                let mut stmt = conn.prepare(&sql)?;
                let tosql_args: Vec<Box<dyn ToSql>> = args.iter().map(|arg| arg.as_tosql()).collect();
                let arrow = stmt.query_arrow(params_from_iter(tosql_args.iter()))?;

                let schema = arrow.get_schema();
                let mut buffer: Vec<u8> = Vec::new();
                let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buffer, schema.as_ref())?;
                for batch in arrow {
                    writer.write(&batch)?;
                }
                writer.finish()?;
                Ok(buffer)
            }
        })
        .await??;

        if is_writable_sql(&sql_owned) {
            let mut pool_write = self.inner.write().await;
            pool_write.reset_pool()?;
        }

        Ok(result)
    }

    async fn get_record_batches(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<RecordBatch>> {
        let sql_owned = sql.to_string();
        let args = args.to_vec();
        let pool = self.inner.clone();

        let result = tokio::task::spawn_blocking({
            let sql = sql_owned.clone();
            move || -> Result<Vec<RecordBatch>> {
                let conn = pool.blocking_read().get()?;
                let mut stmt = conn.prepare(&sql)?;
                let tosql_args: Vec<Box<dyn ToSql>> = args.iter().map(|arg| arg.as_tosql()).collect();
                let arrow = stmt.query_arrow(params_from_iter(tosql_args.iter()))?;
                Ok(arrow.collect())
            }
        })
        .await??;

        if is_writable_sql(&sql_owned) {
            let mut pool_write = self.inner.write().await;
            pool_write.reset_pool()?;
        }

        Ok(result)
    }
}
