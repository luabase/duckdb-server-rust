use anyhow::Result;
use async_trait::async_trait;
use duckdb::{params_from_iter, types::ToSql, DuckdbConnectionManager};

use crate::interfaces::SqlValue;

#[async_trait]
pub trait Database: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<()>;
    async fn get_json(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>>;
    async fn get_arrow(&self, sql: &str, args: &[SqlValue]) -> Result<Vec<u8>>;
}

pub struct ConnectionPool {
    pool: r2d2::Pool<DuckdbConnectionManager>,
}

impl ConnectionPool {
    pub fn new(db_path: &str, pool_size: u32) -> Result<Self> {
        let manager = DuckdbConnectionManager::file(db_path)?;
        let pool = r2d2::Pool::builder().max_size(pool_size).build(manager)?;
        Ok(Self { pool })
    }

    pub fn get(&self) -> Result<r2d2::PooledConnection<DuckdbConnectionManager>> {
        Ok(self.pool.get()?)
    }
}

#[async_trait]
impl Database for ConnectionPool {
    async fn execute(&self, sql: &str) -> Result<()> {
        let conn = self.get()?;
        conn.execute_batch(sql)?;
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
        {
            let schema_ref = schema.as_ref();
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buffer, schema_ref)?;

            for batch in arrow {
                writer.write(&batch)?;
            }

            writer.finish()?;
        }

        Ok(buffer)
    }
}
