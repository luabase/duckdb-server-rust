use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb::{params_from_iter, types::ToSql, AccessMode, Config, DuckdbConnectionManager};
use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
use std::sync::{Arc, Mutex};

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
    pool: Arc<Mutex<r2d2::Pool<DuckdbConnectionManager>>>,
}

impl ConnectionPool {
    pub fn new(db_path: &str, pool_size: u32, access_mode: AccessMode) -> Result<Self> {
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
            pool: Arc::new(Mutex::new(pool)),
        })
    }

    pub fn get(&self) -> Result<r2d2::PooledConnection<DuckdbConnectionManager>> {
        Ok(self.pool.lock().unwrap().get()?)
    }

    fn reset_pool(&self) -> Result<()> {
        let config = Config::default()
            .access_mode(match self.access_mode {
                AccessMode::ReadOnly => AccessMode::ReadOnly,
                AccessMode::ReadWrite => AccessMode::ReadWrite,
                AccessMode::Automatic => AccessMode::Automatic,
            })?
            .threads(self.pool_size as i64)?;
        let manager = DuckdbConnectionManager::file_with_flags(&self.db_path, config)?;
        let new_pool = r2d2::Pool::builder().max_size(self.pool_size).build(manager)?;

        let mut pool_lock = self.pool.lock().unwrap();
        *pool_lock = new_pool;
        Ok(())
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
            | Statement::SetVariable { .. }
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
        Err(_) => true,
    }
}

#[async_trait]
impl Database for ConnectionPool {
    async fn execute(&self, sql: &str) -> Result<()> {
        let conn = self.get()?;
        conn.execute_batch(sql)?;
        drop(conn);

        if is_writable_sql(sql) {
            self.reset_pool()?;
        }

        Ok(())
    }

    async fn get_json(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>> {
        let conn = self.get()?;
        let mut stmt = conn.prepare(sql)?;
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

    async fn get_arrow(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>> {
        let conn = self.get()?;
        let mut stmt = conn.prepare(sql)?;
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

    async fn get_record_batches(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<RecordBatch>> {
        let conn = self.get()?;
        let mut stmt = conn.prepare(sql)?;
        let tosql_args: Vec<Box<dyn ToSql>> = args.iter().map(|arg| arg.as_tosql()).collect();
        let arrow = stmt.query_arrow(params_from_iter(tosql_args.iter()))?;
        Ok(arrow.collect())
    }
}
